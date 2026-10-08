package com.example.exception;

public class OperationTimeoutException extends Throwable {
    private final long timeoutMs;

    public OperationTimeoutException(long timeoutMs) {
        super("Operation timed out after " + timeoutMs + " ms");
        this.timeoutMs = timeoutMs;
    }

    public OperationTimeoutException(long timeoutMs, Throwable cause) {
        super("Operation timed out after " + timeoutMs + " ms", cause);
        this.timeoutMs = timeoutMs;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }
}
