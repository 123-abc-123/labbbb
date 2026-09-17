package com.example.exception;

import java.time.Duration;

public final class CallTimeoutException extends RuntimeException {

    private final Duration timeout;

    public CallTimeoutException(Duration timeout, Throwable cause) {
        super("Call timed out after " + timeout, cause);
        this.timeout = timeout;
    }

    public Duration timeout() {
        return timeout;
    }
}
