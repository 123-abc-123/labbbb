package com.example.throttle;

import java.time.Duration;
import java.util.Objects;

public record Rate(long permits, Duration per) {

    public Rate {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be > 0, was " + permits);
        }

        Objects.requireNonNull(per, "per");

        if (per.isZero() || per.isNegative()) {
            throw new IllegalArgumentException("per must be > 0, was " + per);
        }

        if (per.toNanos() < permits) {
            throw new IllegalArgumentException(
                    "rate exceeds one permit per nanosecond: " + permits + " per " + per);
        }
    }

    public static Rate of(long permits, Duration per) {
        return new Rate(permits, per);
    }

    public long durationNanos() {
        return per.toNanos();
    }

    /**
     * Spacing between two permits. Uses ceiling division so the effective rate can only be
     * slightly below the configured one, never above it. (Java 18+)
     */
    public long nanosPerPermit() {
        return Math.ceilDiv(per.toNanos(), permits);
    }
}
