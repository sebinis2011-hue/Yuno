package com.yuno

import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.Request
import org.json.JSONObject
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

/** Community estimates and DeArrow branding fetched from their public, read-only APIs. */
data class CommunityVotes(val likes: Long, val dislikes: Long)
data class DeArrowBranding(val title: String?, val thumbnailUrl: String?)

object VideoEnhancementApi {
    private const val RYD_BASE = "https://returnyoutubedislikeapi.com"
    private const val DEARROW_BASE = "https://sponsor.ajay.app"
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

    fun fetchBranding(videoId: String, fallbackThumbnail: String?): DeArrowBranding? {
        requireValidVideoId(videoId)
        brandingCache[videoId]?.takeIf { System.currentTimeMillis() - it.at < CACHE_MS }?.let { return it.value }
        val url = "$DEARROW_BASE/api/branding".toHttpUrl().newBuilder()
            .addQueryParameter("videoID", videoId)
            .addQueryParameter("service", "YouTube")
            .addQueryParameter("fetchAll", "false")
            .build()
        val json = getJson(url.toString())
        val title = json.optJSONArray("titles")?.let { list ->
            (0 until list.length()).asSequence()
                .mapNotNull { list.optJSONObject(it) }
                .firstOrNull { !it.optBoolean("original", false) && (it.optBoolean("locked", false) || it.optInt("votes", -1) >= 0) }
                ?.optString("title")
                ?.replace(">", "")
                ?.trim()
                ?.takeIf { it.isNotBlank() }
        }
        val thumbnailTime = json.optJSONArray("thumbnails")?.let { list ->
            (0 until list.length()).asSequence()
                .mapNotNull { list.optJSONObject(it) }
                .firstOrNull { !it.optBoolean("original", false) && (it.optBoolean("locked", false) || it.optInt("votes", -1) >= 0) }
                ?.takeIf { !it.isNull("timestamp") }
                ?.optDouble("timestamp", Double.NaN)
                ?.takeIf { it.isFinite() && it >= 0.0 }
        }
        val thumbnail = thumbnailTime?.let { timestamp ->
            val fallback = fallbackThumbnail
                ?.takeIf { it.startsWith("https://i.ytimg.com/") }
                ?: "https://i.ytimg.com/vi/$videoId/hq720.jpg"
            "$DEARROW_BASE/api/v1/getThumbnail".toHttpUrl().newBuilder()
                .addQueryParameter("videoID", videoId)
                .addQueryParameter("time", timestamp.toString())
                .addQueryParameter("redirectUrl", fallback)
                .build()
                .toString()
        }
        val result = DeArrowBranding(title, thumbnail)
        if (brandingCache.size > 256) brandingCache.clear()
        brandingCache[videoId] = Cached(System.currentTimeMillis(), result)
        return result
    }

    private fun getJson(url: String): JSONObject {
        val request = Request.Builder()
            .url(url)
            .header("Accept", "application/json")
            .header("User-Agent", "Yuno/1.1.1 (Android)")
            .get()
            .build()
        Net.client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw IOException("Enhancement API returned HTTP ${response.code}")
            return JSONObject(response.body?.string() ?: throw IOException("Empty enhancement API response"))
        }
    }

    private fun requireValidVideoId(id: String) {
        require(id.matches(Regex("[A-Za-z0-9_-]{11}"))) { "Not a YouTube video ID" }
    }
}
