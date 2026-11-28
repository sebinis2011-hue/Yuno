package com.yuno

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class VideoEnhancementSchedulerTest {
    @Test fun rydResultsAreReusedForThirtyMinutes() {
        val now = 10_000_000L
        assertTrue(
            VideoEnhancementScheduler.isCacheFresh(
                savedAt = now - VideoEnhancementScheduler.RYD_CACHE_TTL_MS + 1L,
                now = now,
                ttl = VideoEnhancementScheduler.RYD_CACHE_TTL_MS,
            ),
        )
        assertFalse(
            VideoEnhancementScheduler.isCacheFresh(
                savedAt = now - VideoEnhancementScheduler.RYD_CACHE_TTL_MS,
                now = now,
                ttl = VideoEnhancementScheduler.RYD_CACHE_TTL_MS,
            ),
        )
    }

    @Test fun deArrowResultsAreReusedForTwentyFourHours() {
        val now = 20_000_000L
        assertTrue(
            VideoEnhancementScheduler.isCacheFresh(
                savedAt = now - VideoEnhancementScheduler.DEARROW_CACHE_TTL_MS + 1L,
                now = now,
                ttl = VideoEnhancementScheduler.DEARROW_CACHE_TTL_MS,
            ),
        )
        assertFalse(
            VideoEnhancementScheduler.isCacheFresh(
                savedAt = now - VideoEnhancementScheduler.DEARROW_CACHE_TTL_MS,
                now = now,
                ttl = VideoEnhancementScheduler.DEARROW_CACHE_TTL_MS,
            ),
        )
    }

    @Test fun cacheIsNotTrustedWhenDeviceClockMovesBackwards() {
        assertFalse(
            VideoEnhancementScheduler.isCacheFresh(
                savedAt = 2_000L,
                now = 1_000L,
                ttl = VideoEnhancementScheduler.RYD_CACHE_TTL_MS,
            ),
        )
    }
}
