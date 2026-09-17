package com.example.circuit_breaker;

import com.example.exception.CallTimeoutException;
import com.example.exception.CircuitBreakerOpenException;
import com.example.time_source.ManualTimeSource;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CircuitBreakerTest {

    @Test
    void opensAfterConsecutiveFailures() {
        ManualTimeSource time = new ManualTimeSource();
        CircuitBreaker cb = CircuitBreaker.builder()
                .failureThreshold(2)
                .openStateDuration(Duration.ofMillis(1000))
                .timeSource(time)
                .build();

        StepVerifier.create(cb.call(() -> Mono.<String>error(new RuntimeException("failure 1"))))
                .expectError(RuntimeException.class).verify();
        assertEquals(CircuitBreaker.State.CLOSED, cb.state());

        StepVerifier.create(cb.call(() -> Mono.<String>error(new RuntimeException("failure 2"))))
                .expectError(RuntimeException.class).verify();
        assertEquals(CircuitBreaker.State.OPEN, cb.state());
    }

    @Test
    void rejectsCallsImmediatelyWhileOpen() {
        ManualTimeSource time = new ManualTimeSource();
        CircuitBreaker cb = CircuitBreaker.builder()
                .failureThreshold(1)
                .openStateDuration(Duration.ofMillis(1000))
                .timeSource(time)
                .build();

        StepVerifier.create(cb.call(() -> Mono.<String>error(new RuntimeException("boom"))))
                .expectError(RuntimeException.class).verify();
        assertEquals(CircuitBreaker.State.OPEN, cb.state());

        // Well before openStateDuration elapses: fn must be rejected outright, never invoked.
        AtomicInteger invoked = new AtomicInteger();
        StepVerifier.create(cb.call(() -> {
                    invoked.incrementAndGet();
                    return Mono.just("should not run");
                }))
                .expectError(CircuitBreakerOpenException.class).verify();

        assertEquals(0, invoked.get());
        assertEquals(CircuitBreaker.State.OPEN, cb.state());
    }

    @Test
    void stateIsPureDoesNotTransition() {
        ManualTimeSource time = new ManualTimeSource();
        CircuitBreaker cb = CircuitBreaker.builder()
                .failureThreshold(1)
                .openStateDuration(Duration.ofMillis(100))
                .timeSource(time)
                .build();

        StepVerifier.create(cb.call(() -> Mono.<String>error(new RuntimeException("boom"))))
                .expectError(RuntimeException.class).verify();

        assertEquals(CircuitBreaker.State.OPEN, cb.state());
        time.advance(Duration.ofMillis(100));

        assertEquals(CircuitBreaker.State.OPEN, cb.state());
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.stateAfterExpiration());
    }

    @Test
    void halfOpenRequiresMultipleSuccessesToClose() {
        ManualTimeSource time = new ManualTimeSource();
        CircuitBreaker cb = CircuitBreaker.builder()
                .failureThreshold(1)
                .halfOpenMaxCalls(5)
                .halfOpenSuccessThreshold(2)
                .openStateDuration(Duration.ofMillis(100))
                .timeSource(time)
                .build();

        StepVerifier.create(cb.call(() -> Mono.<String>error(new RuntimeException("boom"))))
                .expectError(RuntimeException.class).verify();
        assertEquals(CircuitBreaker.State.OPEN, cb.state());

        time.advance(Duration.ofMillis(100));

        StepVerifier.create(cb.call(() -> Mono.just("probe 1")))
                .expectNext("probe 1").verifyComplete();
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.state());

        StepVerifier.create(cb.call(() -> Mono.just("probe 2")))
                .expectNext("probe 2").verifyComplete();
        assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    }

    @Test
    void halfOpenFailureReopensCircuit() {
        ManualTimeSource time = new ManualTimeSource();
        CircuitBreaker cb = CircuitBreaker.builder()
                .failureThreshold(1)
                .halfOpenMaxCalls(1)
                .openStateDuration(Duration.ofMillis(100))
                .timeSource(time)
                .build();

        StepVerifier.create(cb.call(() -> Mono.<String>error(new RuntimeException("boom"))))
                .expectError(RuntimeException.class).verify();
        assertEquals(CircuitBreaker.State.OPEN, cb.state());

        time.advance(Duration.ofMillis(100));

        StepVerifier.create(cb.call(() -> Mono.<String>error(new RuntimeException("probe failed"))))
                .expectError(RuntimeException.class).verify();

        assertEquals(CircuitBreaker.State.OPEN, cb.state());
    }

    @Test
    void ignoredErrorsDoNotResetConsecutiveFailures() {
        ManualTimeSource time = new ManualTimeSource();
        CircuitBreaker cb = CircuitBreaker.builder()
                .failureThreshold(2)
                .failurePredicate(t -> !(t instanceof IllegalArgumentException))
                .timeSource(time)
                .build();

        StepVerifier.create(cb.call(() -> Mono.<String>error(new RuntimeException("fail 1"))))
                .expectError(RuntimeException.class).verify();

        StepVerifier.create(cb.call(() -> Mono.<String>error(new IllegalArgumentException("bad input"))))
                .expectError(IllegalArgumentException.class).verify();

        StepVerifier.create(cb.call(() -> Mono.<String>error(new RuntimeException("fail 2"))))
                .expectError(RuntimeException.class).verify();

        assertEquals(CircuitBreaker.State.OPEN, cb.state());
    }

    @Test
    void rejectionFromAnotherBreakerNeverCountsAsFailureEvenWithCustomPredicate() {
        ManualTimeSource time = new ManualTimeSource();
        // Deliberately permissive/naive custom predicate: treats every throwable as a failure.
        // Without the unconditional ResilienceRejection veto in recordError, this would have
        // caused a rejection from elsewhere to trip this unrelated breaker too.
        CircuitBreaker outer = CircuitBreaker.builder()
                .failureThreshold(1)
                .failurePredicate(t -> true)
                .timeSource(time)
                .build();

        CircuitBreakerOpenException rejection =
                new CircuitBreakerOpenException(CircuitBreaker.State.OPEN);

        StepVerifier.create(outer.call(() -> Mono.<String>error(rejection)))
                .expectErrorMatches(t -> t == rejection).verify();

        assertEquals(CircuitBreaker.State.CLOSED, outer.state());
    }

    @Test
    void cancellationReleasesHalfOpenPermit() {
        ManualTimeSource time = new ManualTimeSource();
        CircuitBreaker cb = CircuitBreaker.builder()
                .failureThreshold(1)
                .halfOpenMaxCalls(1)
                .openStateDuration(Duration.ofMillis(100))
                .timeSource(time)
                .build();

        StepVerifier.create(cb.call(() -> Mono.<String>error(new RuntimeException("boom"))))
                .expectError(RuntimeException.class).verify();

        time.advance(Duration.ofMillis(100));
        Disposable d1 = cb.call(() -> Mono.<String>never()).subscribe();
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.stateAfterExpiration());

        StepVerifier.create(cb.call(() -> Mono.just("should be rejected")))
                .expectError(CircuitBreakerOpenException.class).verify();

        d1.dispose();

        StepVerifier.create(cb.call(() -> Mono.just("ok")))
                .expectNext("ok").verifyComplete();
        assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    }

    @Test
    void timeoutCountsAsFailure() {
        AtomicReference<CircuitBreaker> cbRef = new AtomicReference<>();

        StepVerifier.withVirtualTime(() -> {
                    VirtualTimeScheduler scheduler = VirtualTimeScheduler.getOrSet();
                    CircuitBreaker cb = CircuitBreaker.builder()
                            .failureThreshold(1)
                            .timeoutPerCall(Duration.ofMillis(100))
                            .timeoutScheduler(scheduler)
                            .timeSource(new ManualTimeSource())
                            .build();
                    cbRef.set(cb);
                    return cb.call(() -> Mono.<String>never());
                })
                .thenAwait(Duration.ofMillis(100))
                .expectError(CallTimeoutException.class)
                .verify();

        assertEquals(CircuitBreaker.State.OPEN, cbRef.get().state());
    }
}