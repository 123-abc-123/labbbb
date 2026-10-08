package com.example.timeout;

import com.example.exception.OperationTimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Timeout")
class TimeoutTest {

    private static final Duration VERIFICATION_TIMEOUT = Duration.ofSeconds(1);

    @Nested
    @DisplayName("Requirement 1: successful operation")
    class SuccessfulOperation {

        @Test
        @DisplayName("should complete when operation finishes before timeout")
        void completesWhenOperationFinishesBeforeTimeout() {
            // Given
            VirtualTimeScheduler time = VirtualTimeScheduler.create();

            Mono<String> operation = Mono.delay(Duration.ofMillis(100), time)
                    .thenReturn("OK");

            // When / Then
            StepVerifier.create(Timeout.withTimeout(() -> operation, 200, time))
                    .then(() -> time.advanceTimeBy(Duration.ofMillis(150)))
                    .expectNext("OK")
                    .expectComplete()
                    .verify(VERIFICATION_TIMEOUT);
        }
    }

    @Nested
    @DisplayName("Requirement 2: timeout exceeded")
    class TimeoutExceeded {

        @Test
        @DisplayName("should return timeout error and cancel outstanding side effects")
        void returnsTimeoutErrorAndCancelsOutstandingSideEffects() {
            // Given
            VirtualTimeScheduler time = VirtualTimeScheduler.create();

            AtomicBoolean sideEffectExecuted = new AtomicBoolean(false);
            AtomicBoolean rollbackExecuted = new AtomicBoolean(false);

            Mono<String> slowOperation = Mono.delay(Duration.ofMillis(500), time)
                    .doOnNext(ignored -> sideEffectExecuted.set(true))
                    .doOnCancel(() -> rollbackExecuted.set(true))
                    .thenReturn("OK");

            // When
            StepVerifier.create(Timeout.withTimeout(() -> slowOperation, 100, time))
                    .then(() -> time.advanceTimeBy(Duration.ofMillis(100)))
                    .expectErrorSatisfies(error -> assertOperationTimeout(error, 100))
                    .verify(VERIFICATION_TIMEOUT);

            // Advance beyond the original operation delay to prove the side effect never happens.
            time.advanceTimeBy(Duration.ofMillis(500));

            // Then
            assertFalse(
                    sideEffectExecuted.get(),
                    "Delayed side effect must not be executed after timeout"
            );

            assertTrue(
                    rollbackExecuted.get(),
                    "Timeout should cancel the operation and trigger rollback/cleanup"
            );
        }
    }

    @Nested
    @DisplayName("Requirement 3: retry combination")
    class RetryCombination {

        @Test
        @DisplayName("should bound total retry time instead of multiplying waits")
        void boundsTotalRetryTime() {
            // Given
            VirtualTimeScheduler time = VirtualTimeScheduler.create();

            AtomicInteger attempts = new AtomicInteger(0);

            Mono<String> failingOperation = Mono.defer(() -> {
                attempts.incrementAndGet();
                return Mono.error(new IllegalStateException("temporary failure"));
            });

            Retry retrySpec = Retry.fixedDelay(10, Duration.ofMillis(100))
                    .scheduler(time);

            Mono<String> retriedOperation = failingOperation.retryWhen(retrySpec);

            /*
             * Timeline:
             *
             * 0ms    attempt 1 fails
             * 100ms  attempt 2 fails
             * 200ms  attempt 3 fails
             * 250ms  total timeout fires
             *
             * Without the total timeout, the retry chain could continue up to 10 retries.
             */

            // When / Then
            StepVerifier.create(Timeout.withTimeout(() -> retriedOperation, 250, time))
                    .then(() -> time.advanceTimeBy(Duration.ofMillis(250)))
                    .expectError(OperationTimeoutException.class)
                    .verify(VERIFICATION_TIMEOUT);

            assertEquals(
                    3,
                    attempts.get(),
                    "Expected attempts at 0ms, 100ms, and 200ms before timeout at 250ms"
            );
        }
    }

    private static void assertOperationTimeout(Throwable error, long expectedTimeoutMs) {
        assertTrue(
                error instanceof OperationTimeoutException,
                () -> "Expected OperationTimeoutException but was: " + error
        );

        assertEquals(
                expectedTimeoutMs,
                ((OperationTimeoutException) error).getTimeoutMs(),
                "Timeout exception should contain the configured timeout value"
        );
    }
}