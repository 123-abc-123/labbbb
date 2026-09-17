package com.example.retry;

import com.example.circuit_breaker.CircuitBreaker;
import com.example.exception.CircuitBreakerOpenException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.io.IOException;
import java.time.Duration;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryTest {

    @Test
    void doesNotRetryByDefault() {
        AtomicInteger attempts = new AtomicInteger();
        Retry retry = Retry.builder().maxAttempts(3).build();

        StepVerifier.create(retry.execute(() -> {
                    attempts.incrementAndGet();
                    return Mono.<String>error(new IllegalStateException("boom"));
                }))
                .expectError(IllegalStateException.class).verify();

        assertEquals(1, attempts.get());
    }

    @Test
    void doesNotRetryResilienceRejectionsByDefaultEvenWithRetryAll() {
        AtomicInteger attempts = new AtomicInteger();
        Retry retry = Retry.builder()
                .maxAttempts(3)
                .retryAll()
                .build();

        CircuitBreakerOpenException rejection =
                new CircuitBreakerOpenException(CircuitBreaker.State.OPEN);

        StepVerifier.create(retry.execute(() -> {
                    attempts.incrementAndGet();
                    return Mono.<String>error(rejection);
                }))
                .expectErrorMatches(t -> t == rejection).verify();

        assertEquals(1, attempts.get());
    }

    @Test
    void retryOnRejectionsOptsBackIn() {
        AtomicInteger attempts = new AtomicInteger();
        Retry retry = Retry.builder()
                .maxAttempts(3)
                .retryAll()
                .retryOnRejections()
                .constantDelay(Duration.ZERO)
                .build();

        StepVerifier.create(retry.execute(() -> {
                    int attempt = attempts.incrementAndGet();
                    if (attempt < 2) {
                        return Mono.<String>error(
                                new CircuitBreakerOpenException(CircuitBreaker.State.OPEN));
                    }
                    return Mono.just("ok");
                }))
                .expectNext("ok").verifyComplete();

        assertEquals(2, attempts.get());
    }

    @Test
    void traverseCauseChainFalseOnlyChecksRoot() {
        AtomicInteger attempts = new AtomicInteger();
        Retry retry = Retry.builder()
                .maxAttempts(3)
                .retryOnException(IOException.class)
                .traverseCauseChain(false)
                .build();

        StepVerifier.create(retry.execute(() -> {
                    attempts.incrementAndGet();
                    // Root is RuntimeException, Cause is IOException
                    return Mono.<String>error(new RuntimeException(new IOException("nested")));
                }))
                .expectError(RuntimeException.class).verify();

        assertEquals(1, attempts.get()); // Stopped because root didn't match
    }

    @Test
    void traverseCauseChainTrueChecksHierarchy() {
        AtomicInteger attempts = new AtomicInteger();
        Retry retry = Retry.builder()
                .maxAttempts(3)
                .retryOnException(IOException.class)
                .traverseCauseChain(true)
                .build();

        StepVerifier.create(retry.execute(() -> {
                    int attempt = attempts.incrementAndGet();
                    if (attempt < 2) return Mono.<String>error(new RuntimeException(new IOException("nested")));
                    return Mono.just("ok");
                }))
                .expectNext("ok").verifyComplete();

        assertEquals(2, attempts.get());
    }

    @Test
    void stopsAfterMaxAttemptsAndAppliesDelays() {
        AtomicInteger attempts = new AtomicInteger();

        StepVerifier.withVirtualTime(() -> {
                    VirtualTimeScheduler scheduler = VirtualTimeScheduler.getOrSet();
                    Retry retry = Retry.builder()
                            .maxAttempts(3)
                            .constantDelay(Duration.ofMillis(100))
                            .scheduler(scheduler)
                            .retryOnException(IOException.class)
                            .build();

                    return retry.execute(() -> {
                        attempts.incrementAndGet();
                        return Mono.<String>error(new IOException("always fails"));
                    });
                })
                .thenAwait(Duration.ofMillis(100))
                .thenAwait(Duration.ofMillis(100))
                .expectError(IOException.class)
                .verify();

        assertEquals(3, attempts.get());
    }

    @Test
    void delayStrategiesBehaveCorrectly() {
        Function<Long, Duration> exponential = RetryDelayStrategies.exponential(Duration.ofMillis(100));
        assertEquals(Duration.ofMillis(100), exponential.apply(1L));
        assertEquals(Duration.ofMillis(200), exponential.apply(2L));
        assertEquals(Duration.ofMillis(400), exponential.apply(3L));

        Function<Long, Duration> jitter = RetryDelayStrategies.exponentialJitter(
                Duration.ofMillis(100), new SplittableRandom(42));
        long jittered = jitter.apply(1L).toMillis();
        assertTrue(jittered >= 50 && jittered <= 150);
    }
}