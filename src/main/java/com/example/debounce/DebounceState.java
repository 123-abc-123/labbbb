package com.example.debounce;

final class DebounceState {

    private final Object lock = new Object();
    private boolean active;
    private boolean subsequent;

    boolean begin(boolean leading) {
        synchronized (lock) {
            if (!active) {
                active = true;
                subsequent = false;
                return leading;
            }
            subsequent = true;
            return false;
        }
    }

    boolean complete(boolean leading, boolean trailing) {
        synchronized (lock) {
            // Emit trailing if:
            // 1. Trailing is enabled AND
            // 2. Either we saw subsequent events, OR leading was disabled (meaning the first event was held back)
            boolean emitTrailing = trailing && (subsequent || !leading);
            active = false;
            subsequent = false;
            return emitTrailing;
        }
    }
}
