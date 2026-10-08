package com.example.exception;

import java.util.function.Supplier;

public class OperationTimeoutException extends Throwable {
    private final long timeout;

    public OperationTimeoutException(long timeoutMs) {
        super("Operation timed out after " + timeoutMs + " ms");
        this.timeout = timeoutMs;
    }

    public OperationTimeoutException(long timeoutMs, Throwable cause) {
        super("Operation timed out after " + timeoutMs + " ms", cause);
        this.timeout = timeoutMs;
    }

    public long getTimeout() {
        return timeout;
    }
}
