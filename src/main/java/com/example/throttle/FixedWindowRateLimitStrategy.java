package com.example.throttle;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public final class FixedWindowRateLimitStrategy implements RateLimitStrategy {
    private record Slot(long windowStart, long used) {}

    private final long permits;
    private final long windowNanos;
    private final AtomicReference<Slot> state = new AtomicReference<>(new Slot(Long.MIN_VALUE, 0));

    public FixedWindowRateLimitStrategy(Rate rate) {
        Objects.requireNonNull(rate, "rate");
        this.permits = rate.permits();
        this.windowNanos = rate.durationNanos();
    }

    @Override
    public long reserve(long nowNanos, long maxWaitNanos) {
        if (maxWaitNanos < 0) {
            throw new IllegalArgumentException("maxWaitNanos must be >= 0, was " + maxWaitNanos);
        }

        try {
            long currentWindow = Math.multiplyExact(Math.floorDiv(nowNanos, windowNanos), windowNanos);

            while (true) {
                Slot s = state.get();
                long ws = s.windowStart();
                long used = s.used();

                if (ws < currentWindow) {
                    ws = currentWindow;
                    used = 0;
                }

                if (used >= permits) {
                    ws = Math.addExact(ws, windowNanos);
                    used = 0;
                }

                long wait = Math.max(0, Math.subtractExact(ws, nowNanos));

                if (wait > maxWaitNanos) {
                    return REJECTED;
                }

                if (state.compareAndSet(s, new Slot(ws, used + 1))) {
                    return wait;
                }
            }
        } catch (ArithmeticException overflow) {
            return REJECTED;
        }
    }
}

