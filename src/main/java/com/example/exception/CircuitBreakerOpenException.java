package com.example.exception;

import com.example.circuit_breaker.CircuitBreaker;

public final class CircuitBreakerOpenException extends RuntimeException implements ResilienceRejection {

    private final CircuitBreaker.State state;

    public CircuitBreakerOpenException(CircuitBreaker.State state) {
        super("Circuit breaker is " + state);
        this.state = state;
    }

    public CircuitBreaker.State state() {
        return state;
    }
}
