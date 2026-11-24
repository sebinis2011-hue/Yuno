package com.yuno

import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.Request
import org.json.JSONObject
import java.io.IOException
import java.util.Locale
import java.util.Random
import java.util.concurrent.ConcurrentHashMap

/** Community estimates and DeArrow branding fetched from their public, read-only APIs. */
data class CommunityVotes(val likes: Long, val dislikes: Long)
data class DeArrowBranding(
    val title: String?,
    val thumbnailUrl: String?,
    /** A community submission explicitly marks the original thumbnail as the preferred one. */
    val useOriginalThumbnail: Boolean = false,
)

object VideoEnhancementApi {
    private const val RYD_BASE = "https://returnyoutubedislikeapi.com"
    private const val DEARROW_BASE = "https://sponsor.ajay.app"
    private const val THUMBNAIL_BASE = "https://dearrow-thumb.ajay.app"
    private const val CACHE_MS = 30 * 60 * 1000L

    private data class Cached<T>(val at: Long, val value: T)
    private val votesCache = ConcurrentHashMap<String, Cached<CommunityVotes>>()
    private val brandingCache = ConcurrentHashMap<String, Cached<DeArrowBranding>>()

    fun fetchVotes(videoId: String): CommunityVotes? {
        requireValidVideoId(videoId)
        votesCache[videoId]?.takeIf { System.currentTimeMillis() - it.at < CACHE_MS }?.let { return it.value }
        val url = "$RYD_BASE/votes".toHttpUrl().newBuilder()
            .addQueryParameter("videoId", videoId)
            .build()
        val json = getJson(url.toString())
        if (json.optBoolean("deleted", false)) return null
        val likes = json.optLong("likes", -1L)
        val dislikes = json.optLong("dislikes", -1L)
        if (likes < 0L || dislikes < 0L) return null
        return CommunityVotes(likes, dislikes).also {
            if (votesCache.size > 256) votesCache.clear()
            votesCache[videoId] = Cached(System.currentTimeMillis(), it)
        }
    }

    /**
     * Mirrors DeArrow's fallback behavior: use a trusted community title/thumbnail when present;
     * otherwise de-clickbait all-caps titles and request a stable random video frame.
     */
    fun fetchBranding(videoId: String, fallbackThumbnail: String?, fallbackDurationSec: Long, originalTitle: String): DeArrowBranding? {
        requireValidVideoId(videoId)
        brandingCache[videoId]?.takeIf { System.currentTimeMillis() - it.at < CACHE_MS }?.let { return it.value }

        val url = "$DEARROW_BASE/api/branding".toHttpUrl().newBuilder()
            .addQueryParameter("videoID", videoId)
            .addQueryParameter("service", "YouTube")
            .addQueryParameter("fetchAll", "false")
            .build()
        // A 404 means this video has no branding record. That is not a fatal error:
        // DeArrow still applies its random-thumbnail and title-formatting fallbacks.
        val json = getJson(url.toString())
        val titles = json.optJSONArray("titles")
        val selectedTitle = titles?.let { list ->
            (0 until list.length()).asSequence()
                .mapNotNull { list.optJSONObject(it) }
                .firstOrNull { !it.optBoolean("original", false) &&
                    (it.optBoolean("locked", false) || it.optInt("votes", -1) >= 0) }
                ?.optString("title")
                ?.replace("‹", "<")
                ?.trim()
                ?.takeIf { it.isNotBlank() }
        }
        val formattedFallbackTitle = fallbackTitle(originalTitle)
        val title = selectedTitle ?: formattedFallbackTitle.takeIf { it.isNotBlank() }

        val thumbnails = json.optJSONArray("thumbnails")
        val selectedThumbnail = thumbnails?.let { list ->
            (0 until list.length()).asSequence()
                .mapNotNull { list.optJSONObject(it) }
                .firstOrNull { it.optBoolean("original", false) ||
                    it.optBoolean("locked", false) || it.optInt("votes", -1) >= 0 }
        }
        val useOriginal = selectedThumbnail?.optBoolean("original", false) == true
        val submittedTime = selectedThumbnail
            ?.takeIf { !it.optBoolean("original", false) && !it.isNull("timestamp") }
            ?.optDouble("timestamp", Double.NaN)
            ?.takeIf { it.isFinite() && it >= 0.0 }

        val apiDuration = json.optDouble("videoDuration", Double.NaN)
        val duration = if (apiDuration.isFinite() && apiDuration > 0.0) apiDuration else fallbackDurationSec.toDouble()
        val apiRandomTime = json.optDouble("randomTime", Double.NaN)
        val randomFraction = if (apiRandomTime.isFinite() && apiRandomTime in 0.0..1.0) {
            if (apiRandomTime > 0.9) apiRandomTime - 0.9 else apiRandomTime
        } else {
            // Stable across launches, matching DeArrow's deterministic random-time fallback.
            Random(videoId.hashCode().toLong()).nextDouble() * 0.9
        }
        val randomTimestamp = if (duration > 0.0) randomFraction * duration else null
        val thumbnailTime = when {
            useOriginal -> null
            submittedTime != null -> submittedTime
            else -> randomTimestamp
        }
        val thumbnail = thumbnailTime?.let { timestamp ->
            "$THUMBNAIL_BASE/api/v1/getThumbnail".toHttpUrl().newBuilder()
                .addQueryParameter("videoID", videoId)
                .addQueryParameter("time", timestamp.toString())
                .build()
                .toString()
        }

        val result = DeArrowBranding(title, thumbnail, useOriginal)
        if (brandingCache.size > 256) brandingCache.clear()
        brandingCache[videoId] = Cached(System.currentTimeMillis(), result)
        return result
    }

    /**
     * Conservative fallback formatting: only normalize titles that are effectively shouting
     * (mostly uppercase). Mixed-case titles keep proper names and acronyms intact.
     */
    private fun fallbackTitle(raw: String): String {
        val value = raw.trim().replace(Regex("\\s+"), " ")
        if (value.isEmpty()) return value
        val letters = value.filter { it.isLetter() }
        if (letters.isEmpty()) return value
        val uppercase = letters.count { it.isUpperCase() }
        if (uppercase.toDouble() / letters.length < 0.80) return value
        val lowered = value.lowercase(Locale.ROOT)
        val firstLetter = lowered.indexOfFirst { it.isLetter() }
        return if (firstLetter < 0) lowered else
            lowered.substring(0, firstLetter) + lowered[firstLetter].uppercase(Locale.ROOT) + lowered.substring(firstLetter + 1)
    }

    private fun getJson(url: String): JSONObject {
        val request = Request.Builder()
            .url(url)
            .header("Accept", "application/json")
            .header("User-Agent", "Yuno/1.1.1 (Android)")
            .get()
            .build()
        Net.client.newCall(request).execute().use { response ->
            if (response.code == 404) return JSONObject()
            if (!response.isSuccessful) throw IOException("Enhancement API returned HTTP ${response.code}")
            return JSONObject(response.body?.string() ?: throw IOException("Empty enhancement API response"))
        }
    }

    private fun requireValidVideoId(id: String) {
        require(id.matches(Regex("[A-Za-z0-9_-]{11}"))) { "Not a YouTube video ID" }
    }
}
