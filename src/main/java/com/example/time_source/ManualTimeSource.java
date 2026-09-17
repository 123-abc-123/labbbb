package com.example.time_source;

import java.time.Duration;

public final class ManualTimeSource implements TimeSource {

    private long now;

    @Override
    public long nowMillis() {
        return now;
    }

    public void advance(Duration duration) {
        now += duration.toMillis();
    }

    public void set(long millis) {
        now = millis;
    }
}
