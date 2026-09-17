package com.example.time_source;

public interface TimeSource {

    long nowMillis();

    static TimeSource systemNano() {
        return () -> System.nanoTime() / 1_000_000L;
    }
}

