package com.example.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.function.Function;

public final class RetryDelayStrategies {

    private RetryDelayStrategies() {
    }

    public static Function<Long, Duration> constant(Duration delay) {
        requireNonNegative(delay, "delay");
        return attempt -> delay;
    }

    public static Function<Long, Duration> exponential(Duration initialDelay) {
        requireNonNegative(initialDelay, "initialDelay");
        long baseMillis = initialDelay.toMillis();
        return attempt -> exponentialDuration(baseMillis, attempt);
    }

    public static Function<Long, Duration> exponentialJitter(
            Duration initialDelay,
            SplittableRandom random
    ) {
        requireNonNegative(initialDelay, "initialDelay");
        Objects.requireNonNull(random, "random is required");
        long baseMillis = initialDelay.toMillis();

        return attempt -> {
            Duration exponentialDelay = exponentialDuration(baseMillis, attempt);
            long exponentialMillis = exponentialDelay.toMillis();

            if (exponentialMillis <= 0) {
                return Duration.ZERO;
            }

            if (exponentialMillis == Long.MAX_VALUE
                    || exponentialMillis > Long.MAX_VALUE / 2) {
                return Duration.ofMillis(Long.MAX_VALUE);
            }

            // Safe by construction: exponentialMillis is clamped to <= Long.MAX_VALUE / 2 above,
            // so jittered = exponentialMillis/2 + [0, exponentialMillis] is at most
            // 1.5 * exponentialMillis, which cannot overflow or go negative.
            long jitter = random.nextLong(exponentialMillis + 1);
            long jittered = exponentialMillis / 2 + jitter;

            return Duration.ofMillis(jittered);
        };
    }

    private static Duration exponentialDuration(long baseMillis, long retryAttempt) {
        if (baseMillis <= 0) {
            return Duration.ZERO;
        }

        long exponent = Math.max(0L, retryAttempt - 1L);

        if (exponent >= 63) {
            return Duration.ofMillis(Long.MAX_VALUE);
        }

        long multiplier = 1L << exponent;
        long millis;

        try {
            millis = Math.multiplyExact(baseMillis, multiplier);
        } catch (ArithmeticException overflow) {
            millis = Long.MAX_VALUE;
        }

        return Duration.ofMillis(millis);
    }

    private static void requireNonNegative(Duration value, String name) {
        Objects.requireNonNull(value, name + " is required");
        if (value.isNegative()) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
