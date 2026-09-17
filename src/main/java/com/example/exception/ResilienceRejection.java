package com.example.exception;

/**
 * Marker interface for exceptions that represent a resilience-layer rejection — a call that was
 * refused before the underlying operation ever ran — rather than a failure of the operation itself.
 */
public interface ResilienceRejection {
}
