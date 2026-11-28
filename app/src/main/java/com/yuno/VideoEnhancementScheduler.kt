package com.yuno

import android.content.Context
import android.content.SharedPreferences
import android.os.SystemClock
import android.util.Log
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.util.PriorityQueue

/**
 * One gate for community metadata requests. It keeps results across app restarts, merges duplicate
 * requests, prioritizes the item being watched, and spaces network calls so RYD and DeArrow do not
 * compete with video extraction or flood their services.
 */
internal enum class EnhancementPriority(val weight: Int) {
    Prefetch(0),
    Visible(1),
    Playing(2),
}

internal object VideoEnhancementScheduler {
    internal const val RYD_CACHE_TTL_MS = 30 * 60 * 1000L
    internal const val DEARROW_CACHE_TTL_MS = 24 * 60 * 60 * 1000L
    private const val EMPTY_CACHE_TTL_MS = 5 * 60 * 1000L
    private const val FAILURE_RETRY_MS = 45 * 1000L
    private const val MIN_REQUEST_SPACING_MS = 350L
    private const val MAX_CACHE_ENTRIES = 512
    private const val PREFS = "video_enhancement_cache"
    private const val RYD_PREFIX = "cache_ryd_"
    private const val DEARROW_PREFIX = "cache_dearrow_"
    private const val TAG = "YunoEnhancements"

    private data class CacheLookup<T>(val hit: Boolean, val value: T?)
    private data class CacheEntry(val hit: Boolean, val json: JSONObject?)
    private class Task(
        val key: String,
        var priority: Int,
        val sequence: Long,
        val fetch: () -> Any?,
        val store: (Any?) -> Unit,
        val result: CompletableDeferred<Any?> = CompletableDeferred(),
    )

    private val lock = Any()
    private val queue = PriorityQueue<Task>(
        compareByDescending<Task> { it.priority }.thenBy { it.sequence },
    )
    private val pending = HashMap<String, Task>()
    /** A higher-priority request can arrive while its first caller is still reading disk cache. */
    private val priorityOverrides = HashMap<String, Int>()
    private val retryAfter = HashMap<String, Long>()
    private val wakeups = Channel<Unit>(Channel.CONFLATED)
    private val workerScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var preferences: SharedPreferences? = null
    private var workerStarted = false
    private var sequence = 0L
    private var lastRequestAt = 0L

    fun init(context: Context) {
        synchronized(lock) {
            preferences = context.applicationContext.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            if (workerStarted) return
            workerStarted = true
            workerScope.launch {
                for (ignored in wakeups) drainQueue()
            }
        }
    }

    suspend fun votes(videoId: String, priority: EnhancementPriority): CommunityVotes? =
        request(
            key = RYD_PREFIX + videoId,
            priority = priority,
            readCache = { readVotes(videoId) },
            fetch = { VideoEnhancementApi.fetchVotes(videoId) },
            store = { writeVotes(videoId, it as CommunityVotes?) },
        )

    suspend fun branding(
        videoId: String,
        fallbackThumbnail: String?,
        fallbackDurationSec: Long,
        originalTitle: String,
        priority: EnhancementPriority,
    ): DeArrowBranding? = request(
        key = DEARROW_PREFIX + videoId,
        priority = priority,
        readCache = { readBranding(videoId) },
        fetch = { VideoEnhancementApi.fetchBranding(videoId, fallbackThumbnail, fallbackDurationSec, originalTitle) },
        store = { writeBranding(videoId, it as DeArrowBranding?) },
    )

    /** Raises a queued Shorts vote lookup from speculative to visible without adding another call. */
    fun promoteVotes(videoId: String, priority: EnhancementPriority) {
        val key = RYD_PREFIX + videoId
        synchronized(lock) {
            val task = pending[key]
            if (task == null) {
                priorityOverrides[key] = maxOf(priorityOverrides[key] ?: -1, priority.weight)
            } else if (priority.weight > task.priority && queue.remove(task)) {
                task.priority = priority.weight
                queue.add(task)
                wakeups.trySend(Unit)
            }
        }
    }

    private suspend fun <T : Any> request(
        key: String,
        priority: EnhancementPriority,
        readCache: () -> CacheLookup<T>,
        fetch: () -> T?,
        store: (T?) -> Unit,
    ): T? {
        check(synchronized(lock) { workerStarted }) { "VideoEnhancementScheduler was not initialized" }

        val cached = withContext(Dispatchers.IO) { readCache() }
        if (cached.hit) {
            synchronized(lock) { priorityOverrides.remove(key) }
            return cached.value
        }

        // A failed request is retried after a short cooldown, not on every re-entry to a feed.
        val waitForRetry = synchronized(lock) {
            ((retryAfter[key] ?: 0L) - SystemClock.elapsedRealtime()).coerceAtLeast(0L)
        }
        if (waitForRetry > 0L) delay(waitForRetry)

        val refreshed = withContext(Dispatchers.IO) { readCache() }
        if (refreshed.hit) {
            synchronized(lock) { priorityOverrides.remove(key) }
            return refreshed.value
        }

        val task = synchronized(lock) {
            pending[key]?.also { existing ->
                if (priority.weight > existing.priority && queue.remove(existing)) {
                    existing.priority = priority.weight
                    queue.add(existing)
                }
            } ?: Task(
                key = key,
                priority = maxOf(priority.weight, priorityOverrides.remove(key) ?: -1),
                sequence = ++sequence,
                fetch = fetch,
                store = { value -> @Suppress("UNCHECKED_CAST") store(value as T?) },
            ).also {
                pending[key] = it
                queue.add(it)
            }
        }
        wakeups.trySend(Unit)
        @Suppress("UNCHECKED_CAST")
        return task.result.await() as T?
    }

    private suspend fun drainQueue() {
        while (true) {
            val task = synchronized(lock) { queue.poll() } ?: return
            try {
                val now = SystemClock.elapsedRealtime()
                val wait = if (lastRequestAt == 0L) 0L
                    else (MIN_REQUEST_SPACING_MS - (now - lastRequestAt)).coerceAtLeast(0L)
                if (wait > 0L) delay(wait)
                lastRequestAt = SystemClock.elapsedRealtime()

                Log.d(TAG, "request ${task.key}")
                val value = task.fetch()
                task.store(value)

                synchronized(lock) {
                    retryAfter.remove(task.key)
                    if (pending[task.key] === task) pending.remove(task.key)
                }
                task.result.complete(value)
            } catch (error: Exception) {
                Log.w(TAG, "request failed: ${task.key}", error)
                synchronized(lock) {
                    retryAfter[task.key] = SystemClock.elapsedRealtime() + FAILURE_RETRY_MS
                    if (pending[task.key] === task) pending.remove(task.key)
                }
                task.result.completeExceptionally(error)
            }
        }
    }

    private fun readVotes(videoId: String): CacheLookup<CommunityVotes> {
        val entry = readEntry(RYD_PREFIX + videoId, RYD_CACHE_TTL_MS) ?: return CacheLookup(false, null)
        val json = entry.json ?: return CacheLookup(true, null)
        if (!json.optBoolean("found", false)) return CacheLookup(true, null)
        val likes = json.optLong("likes", -1L)
        val dislikes = json.optLong("dislikes", -1L)
        return if (likes >= 0L && dislikes >= 0L) CacheLookup(true, CommunityVotes(likes, dislikes))
        else CacheLookup(true, null)
    }

    private fun writeVotes(videoId: String, votes: CommunityVotes?) {
        val json = JSONObject()
            .put("at", System.currentTimeMillis())
            .put("found", votes != null)
        if (votes != null) json.put("likes", votes.likes).put("dislikes", votes.dislikes)
        writeEntry(RYD_PREFIX + videoId, json)
    }

    private fun readBranding(videoId: String): CacheLookup<DeArrowBranding> {
        val entry = readEntry(DEARROW_PREFIX + videoId, DEARROW_CACHE_TTL_MS) ?: return CacheLookup(false, null)
        val json = entry.json ?: return CacheLookup(true, null)
        if (!json.optBoolean("found", false)) return CacheLookup(true, null)
        fun optional(name: String) = if (json.isNull(name)) null else json.optString(name).takeIf { it.isNotBlank() }
        return CacheLookup(
            true,
            DeArrowBranding(
                title = optional("title"),
                thumbnailUrl = optional("thumbnail"),
                useOriginalThumbnail = json.optBoolean("useOriginalThumbnail", false),
            ),
        )
    }

    private fun writeBranding(videoId: String, branding: DeArrowBranding?) {
        val json = JSONObject()
            .put("at", System.currentTimeMillis())
            .put("found", branding != null)
        if (branding != null) {
            json.put("title", branding.title ?: JSONObject.NULL)
                .put("thumbnail", branding.thumbnailUrl ?: JSONObject.NULL)
                .put("useOriginalThumbnail", branding.useOriginalThumbnail)
        }
        writeEntry(DEARROW_PREFIX + videoId, json)
    }

    private fun readEntry(key: String, successTtl: Long): CacheEntry? {
        val raw = preferences?.getString(key, null) ?: return null
        val json = runCatching { JSONObject(raw) }.getOrNull() ?: run {
            preferences?.edit()?.remove(key)?.apply()
            return null
        }
        val at = json.optLong("at", 0L)
        val ttl = if (json.optBoolean("found", false)) successTtl else EMPTY_CACHE_TTL_MS
        if (!isCacheFresh(at, System.currentTimeMillis(), ttl)) {
            preferences?.edit()?.remove(key)?.apply()
            return null
        }
        return CacheEntry(true, json)
    }

    private fun writeEntry(key: String, json: JSONObject) {
        val prefs = preferences ?: return
        val entries = prefs.all.entries
            .filter { it.key.startsWith(RYD_PREFIX) || it.key.startsWith(DEARROW_PREFIX) }
        val editor = prefs.edit().putString(key, json.toString())
        val excess = (entries.size + if (key in prefs.all) 0 else 1) - MAX_CACHE_ENTRIES
        if (excess > 0) {
            entries.sortedBy { (_, value) ->
                runCatching { JSONObject(value as? String ?: "").optLong("at", 0L) }.getOrDefault(0L)
            }.take(excess).forEach { editor.remove(it.key) }
        }
        editor.apply()
    }

    internal fun isCacheFresh(savedAt: Long, now: Long, ttl: Long): Boolean =
        savedAt > 0L && now >= savedAt && now - savedAt < ttl
}
