package com.yuno

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class VideoEnhancementSchedulerTest {
    @Test fun rydResultsAreReusedForTwentyFourHours() {
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

    @Test fun rydRequestSpacingStaysBelowTheDocumentedPerMinuteLimit() {
        assertTrue(VideoEnhancementScheduler.requestSpacingMs(isRyd = true) >= 600L)
        assertTrue(VideoEnhancementScheduler.requestSpacingMs(isRyd = false) < 600L)
    }

    @Test fun rydDailyBudgetLeavesHeadroomBelowProviderLimit() {
        assertTrue(VideoEnhancementScheduler.canUseRydBudget(8_999))
        assertFalse(VideoEnhancementScheduler.canUseRydBudget(9_000))
        assertFalse(VideoEnhancementScheduler.canUseRydBudget(10_000))
        assertFalse(VideoEnhancementScheduler.canUseRydBudget(-1))
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
