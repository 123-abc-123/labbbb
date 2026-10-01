package com.example.throttle;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Token bucket implemented as GCRA.
 */
public final class TokenBucketRateLimitStrategy implements RateLimitStrategy {
    private final long intervalNanos;
    private final long burstToleranceNanos;
    private final AtomicLong tat = new AtomicLong(Long.MIN_VALUE);

    public TokenBucketRateLimitStrategy(long capacity, Rate refill) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, was " + capacity);
        }

        Objects.requireNonNull(refill, "refill");
        this.intervalNanos = refill.nanosPerPermit();
        this.burstToleranceNanos = Math.multiplyExact(capacity - 1, intervalNanos);
    }

    @Override
    public long reserve(long nowNanos, long maxWaitNanos) {
        if (maxWaitNanos < 0) {
            throw new IllegalArgumentException("maxWaitNanos must be >= 0, was " + maxWaitNanos);
        }

        try {
            while (true) {
                long current = tat.get();
                long base = Math.max(current, nowNanos);
                long wait = Math.max(0, Math.subtractExact(Math.subtractExact(base, nowNanos), burstToleranceNanos));

                if (wait > maxWaitNanos) {
                    return REJECTED;
                }

                if (tat.compareAndSet(current, Math.addExact(base, intervalNanos))) {
                    return wait;
                }
            }
        } catch (ArithmeticException overflow) {
            return REJECTED;
        }
    }
}

