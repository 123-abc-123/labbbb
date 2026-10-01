package com.example.throttle;

public interface RateLimitStrategy {

    long REJECTED = -1;
    long UNBOUNDED = Long.MAX_VALUE;

    /**
     * Reserves one permit.
     *
     * @param nowNanos     current time, supplied by the caller
     * @param maxWaitNanos longest acceptable wait; 0 means "only if available right now"
     * @return nanoseconds to wait before using the permit, or {@link #REJECTED}
     */
    long reserve(long nowNanos, long maxWaitNanos);
}
