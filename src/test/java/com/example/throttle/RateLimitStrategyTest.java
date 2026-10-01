package com.example.throttle;

import static org.junit.jupiter.api.Assertions.*;

import static com.example.throttle.RateLimitStrategy.REJECTED;
import static com.example.throttle.RateLimitStrategy.UNBOUNDED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RateLimitStrategyTest {

    private static final long SEC = 1_000_000_000L;
    private static final Rate THREE_PER_SEC = Rate.of(3, Duration.ofSeconds(1));
    private static final long T = THREE_PER_SEC.nanosPerPermit();

    // ---- TokenBucket ----

    @Test
    void tokenBucket_allowsBurstThenRejects() {
        var b = new TokenBucketRateLimitStrategy(3, THREE_PER_SEC);
        for (int i = 0; i < 3; i++) assertEquals(0, b.reserve(0, 0));
        assertEquals(REJECTED, b.reserve(0, 0));
        assertEquals(T, b.reserve(0, UNBOUNDED));
    }

    @Test
    void tokenBucket_refillsOneTokenPerInterval() {
        var b = new TokenBucketRateLimitStrategy(3, THREE_PER_SEC);
        for (int i = 0; i < 3; i++) b.reserve(0, 0);
        assertEquals(REJECTED, b.reserve(T - 1, 0));
        assertEquals(0, b.reserve(T, 0));
    }

    @Test
    void tokenBucket_rejectionDoesNotConsume() {
        var b = new TokenBucketRateLimitStrategy(3, THREE_PER_SEC);
        for (int i = 0; i < 3; i++) b.reserve(0, 0);
        for (int i = 0; i < 5; i++) assertEquals(REJECTED, b.reserve(0, 0));
        assertEquals(0, b.reserve(T, 0));
    }

    @Test
    void tokenBucket_queuedSlotsDrainAtRefillRate() {
        var b = new TokenBucketRateLimitStrategy(3, THREE_PER_SEC);
        long[] expected = {0, 0, 0, T, 2 * T, 3 * T};
        for (long e : expected) assertEquals(e, b.reserve(0, UNBOUNDED));
    }

    @Test
    void capacityIsIndependentOfRate() {
        var b = new TokenBucketRateLimitStrategy(100, Rate.of(1, Duration.ofSeconds(1)));
        for (int i = 0; i < 100; i++) assertEquals(0, b.reserve(0, 0));
        assertEquals(REJECTED, b.reserve(0, 0));
    }

    // ---- FixedWindow ----

    @Test
    void fixedWindow_allowsNPerWindow() {
        var w = new FixedWindowRateLimitStrategy(THREE_PER_SEC);
        for (int i = 0; i < 3; i++) assertEquals(0, w.reserve(0, 0));
        assertEquals(REJECTED, w.reserve(500_000_000L, 0));
        assertEquals(0, w.reserve(SEC, 0));            // next window
    }

    @Test
    void fixedWindow_spillsIntoFollowingWindows() {
        var w = new FixedWindowRateLimitStrategy(THREE_PER_SEC);
        for (int i = 0; i < 3; i++) w.reserve(0, 0);
        assertEquals(REJECTED, w.reserve(0, 0));
        for (int i = 0; i < 3; i++) assertEquals(SEC, w.reserve(0, UNBOUNDED));
        assertEquals(2 * SEC, w.reserve(0, UNBOUNDED));
    }

    // ---- the boundary case that makes them different contracts ----

    @Test
    void boundaryBurst_fixedWindowAdmitsTwoN_tokenBucketDoesNot() {
        long justBefore = SEC - 1_000_000L;    // 999 ms
        long boundary = SEC;                   // 1000 ms

        var window = new FixedWindowRateLimitStrategy(THREE_PER_SEC);
        int admittedByWindow = 0;
        for (int i = 0; i < 3; i++) if (window.reserve(justBefore, 0) == 0) admittedByWindow++;
        for (int i = 0; i < 3; i++) if (window.reserve(boundary, 0) == 0) admittedByWindow++;
        assertEquals(6, admittedByWindow);     // 2N within 1 ms

        var bucket = new TokenBucketRateLimitStrategy(3, THREE_PER_SEC);
        int admittedByBucket = 0;
        for (int i = 0; i < 3; i++) if (bucket.reserve(justBefore, 0) == 0) admittedByBucket++;
        for (int i = 0; i < 3; i++) if (bucket.reserve(boundary, 0) == 0) admittedByBucket++;
        assertEquals(3, admittedByBucket);     // capacity
    }

    // ---- validation ----

    @Test
    void negativeMaxWaitIsRejectedAsProgrammingError() {
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateLimitStrategy(1, THREE_PER_SEC).reserve(0, -5));
        assertThrows(IllegalArgumentException.class, () -> new FixedWindowRateLimitStrategy(THREE_PER_SEC).reserve(0, -5));
    }

    @Test
    void rateValidation() {
        assertThrows(IllegalArgumentException.class, () -> Rate.of(0, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> Rate.of(1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateLimitStrategy(0, THREE_PER_SEC));
    }
}