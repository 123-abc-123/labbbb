package com.example.circuit_breaker;

import com.example.exception.CallTimeoutException;
import com.example.exception.ResilienceRejection;
import com.example.exception.CircuitBreakerOpenException;
import com.example.time_source.TimeSource;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class CircuitBreaker {

    public enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private final int failureThreshold;
    private final int halfOpenMaxCalls;          // Concurrency limit for probes
    private final int halfOpenSuccessThreshold;  // Successes needed to transition to CLOSED
    private final Duration openStateDuration;
    private final Duration timeoutPerCall;
    private final Scheduler timeoutScheduler;
    private final TimeSource timeSource;
    private final Predicate<Throwable> failurePredicate;

    private final Object lock = new Object();

    private State state = State.CLOSED;
    private int consecutiveFailures;

    // Half-open tracking
    private int halfOpenInFlight;
    private int halfOpenSuccesses;

    private long generation;
    private long openUntil;

    private CircuitBreaker(Builder builder) {
        if (builder.failureThreshold <= 0) {
            throw new IllegalArgumentException("failureThreshold must be > 0");
        }
        if (builder.halfOpenMaxCalls <= 0) {
            throw new IllegalArgumentException("halfOpenMaxCalls must be > 0");
        }
        if (builder.halfOpenSuccessThreshold <= 0) {
            throw new IllegalArgumentException("halfOpenSuccessThreshold must be > 0");
        }
        if (builder.halfOpenSuccessThreshold > builder.halfOpenMaxCalls) {
            throw new IllegalArgumentException(
                    "halfOpenSuccessThreshold cannot be greater than halfOpenMaxCalls"
            );
        }
        if (builder.openStateDuration == null || builder.openStateDuration.isNegative()) {
            throw new IllegalArgumentException("openStateDuration must be non-null and non-negative");
        }
        if (builder.timeoutPerCall != null
                && (builder.timeoutPerCall.isZero() || builder.timeoutPerCall.isNegative())) {
            throw new IllegalArgumentException(
                    "timeoutPerCall must be null or positive; zero is ambiguous"
            );
        }

        this.failureThreshold = builder.failureThreshold;
        this.halfOpenMaxCalls = builder.halfOpenMaxCalls;
        this.halfOpenSuccessThreshold = builder.halfOpenSuccessThreshold;
        this.openStateDuration = builder.openStateDuration;
        this.timeoutPerCall = builder.timeoutPerCall;
        this.timeoutScheduler = Objects.requireNonNull(
                builder.timeoutScheduler,
                "timeoutScheduler is required"
        );
        this.timeSource = Objects.requireNonNull(builder.timeSource, "timeSource is required");
        this.failurePredicate = builder.buildFailurePredicate();
    }

    public static Builder builder() {
        return new Builder();
    }

    public State state() {
        synchronized (lock) {
            return state;
        }
    }

    public State stateAfterExpiration() {
        synchronized (lock) {
            expireIfDueLocked();
            return state;
        }
    }

    public <T> Mono<T> call(Supplier<Mono<T>> fn) {
        return Mono.defer(() -> {
            Permit permit;
            State rejectionState;

            synchronized (lock) {
                permit = tryAcquireLocked();
                rejectionState = state;
            }

            if (permit == null) {
                return Mono.error(new CircuitBreakerOpenException(rejectionState));
            }

            Mono<T> source;

            try {
                source = fn.get();
                if (source == null) {
                    throw new NullPointerException("fn returned null");
                }
            } catch (Throwable t) {
                recordError(permit, t);
                return Mono.error(t);
            }

            if (timeoutPerCall != null) {
                source = source
                        .timeout(timeoutPerCall, timeoutScheduler)
                        .onErrorMap(
                                TimeoutException.class,
                                ex -> new CallTimeoutException(timeoutPerCall, ex)
                        );
            }

            return source
                    .doOnSuccess(value -> recordSuccess(permit))
                    .doOnError(error -> recordError(permit, error))
                    .doOnCancel(() -> recordCancel(permit));
        });
    }

    private Permit tryAcquireLocked() {
        expireIfDueLocked();

        switch (state) {
            case CLOSED:
                return new Permit(generation);

            case HALF_OPEN:
                if (halfOpenInFlight < halfOpenMaxCalls) {
                    halfOpenInFlight++;
                    return new Permit(generation);
                }
                return null;

            case OPEN:
            default:
                return null;
        }
    }

    private void expireIfDueLocked() {
        if (state == State.OPEN && timeSource.nowMillis() >= openUntil) {
            toHalfOpenLocked();
        }
    }

    private void recordSuccess(Permit permit) {
        synchronized (lock) {
            if (permit.settled || permit.generation != generation) {
                return;
            }
            permit.settled = true;

            if (state == State.CLOSED) {
                consecutiveFailures = 0;
            } else if (state == State.HALF_OPEN) {
                halfOpenInFlight--;
                halfOpenSuccesses++;

                if (halfOpenSuccesses >= halfOpenSuccessThreshold) {
                    toClosedLocked();
                }
            }
        }
    }

    private void recordError(Permit permit, Throwable error) {
        if (error instanceof CancellationException) {
            recordCancel(permit);
            return;
        }

        boolean isFailure;
        if (error instanceof ResilienceRejection) {
            // A rejection from another resilience primitive (e.g. a nested circuit breaker's own
            // CircuitBreakerOpenException) means the underlying call was never attempted at all, so
            // it can never be a failure of *this* breaker's protected call. This check is
            // unconditional and runs before any configured failurePredicate/failureOn(...), so a
            // custom failure policy can never accidentally reinstate cascading opens across breakers.
            isFailure = false;
        } else {
            try {
                isFailure = failurePredicate.test(error);
            } catch (Throwable predicateError) {
                // A broken failure policy must not disable circuit protection.
                // We fail closed: treat the error as a circuit breaker failure,
                // but we do not replace the application error with the predicate error.
                isFailure = true;
            }
        }

        synchronized (lock) {
            if (permit.settled || permit.generation != generation) {
                return;
            }
            permit.settled = true;

            if (state == State.HALF_OPEN) {
                halfOpenInFlight--;
            }

            if (isFailure) {
                // Outcome: FAILURE
                if (state == State.CLOSED) {
                    consecutiveFailures++;
                    if (consecutiveFailures >= failureThreshold) {
                        toOpenLocked();
                    }
                } else if (state == State.HALF_OPEN) {
                    toOpenLocked();
                }
            }
            // If !isFailure, Outcome: IGNORED.
            // We released the in-flight permit above, but we intentionally do NOT
            // reset consecutiveFailures (which a SUCCESS would do) and we do NOT
            // increment halfOpenSuccesses.
        }
    }

    private void recordCancel(Permit permit) {
        synchronized (lock) {
            if (permit.settled || permit.generation != generation) {
                return;
            }
            permit.settled = true;

            if (state == State.HALF_OPEN && halfOpenInFlight > 0) {
                halfOpenInFlight--;
            }
        }
    }

    private void toOpenLocked() {
        state = State.OPEN;
        generation++;

        long now = timeSource.nowMillis();
        openUntil = safeDeadline(now, openStateDuration);

        consecutiveFailures = 0;
        halfOpenInFlight = 0;
        halfOpenSuccesses = 0;
    }

    private void toHalfOpenLocked() {
        state = State.HALF_OPEN;
        generation++;

        halfOpenInFlight = 0;
        halfOpenSuccesses = 0;
    }

    private void toClosedLocked() {
        state = State.CLOSED;
        generation++;

        consecutiveFailures = 0;
        halfOpenInFlight = 0;
        halfOpenSuccesses = 0;
    }

    private static long safeDeadline(long now, Duration duration) {
        long seconds = duration.getSeconds();

        if (seconds < 0) {
            return now;
        }

        long delay;
        if (seconds > Long.MAX_VALUE / 1000L) {
            delay = Long.MAX_VALUE;
        } else {
            long millis = seconds * 1000L;
            long extraMillis = duration.getNano() / 1_000_000L;

            if (millis > Long.MAX_VALUE - extraMillis) {
                delay = Long.MAX_VALUE;
            } else {
                delay = millis + extraMillis;
            }
        }

        if (delay <= 0) {
            return now;
        }

        if (now > Long.MAX_VALUE - delay) {
            return Long.MAX_VALUE;
        }

        return now + delay;
    }

    private static final class Permit {
        private final long generation;
        private boolean settled;

        private Permit(long generation) {
            this.generation = generation;
        }
    }

    public static final class Builder {
        private int failureThreshold = 5;
        private int halfOpenMaxCalls = 1;
        private int halfOpenSuccessThreshold = 1;
        private Duration openStateDuration = Duration.ofSeconds(5);
        private Duration timeoutPerCall;
        private Scheduler timeoutScheduler = Schedulers.parallel();
        private TimeSource timeSource = TimeSource.systemNano();

        private final List<Class<? extends Throwable>> failureTypes = new ArrayList<>();
        private Predicate<Throwable> failurePredicate;

        public Builder failureThreshold(int failureThreshold) {
            this.failureThreshold = failureThreshold;
            return this;
        }

        public Builder halfOpenMaxCalls(int halfOpenMaxCalls) {
            this.halfOpenMaxCalls = halfOpenMaxCalls;
            return this;
        }

        public Builder halfOpenSuccessThreshold(int halfOpenSuccessThreshold) {
            this.halfOpenSuccessThreshold = halfOpenSuccessThreshold;
            return this;
        }

        public Builder openStateDuration(Duration openStateDuration) {
            this.openStateDuration = openStateDuration;
            return this;
        }

        public Builder timeoutPerCall(Duration timeoutPerCall) {
            this.timeoutPerCall = timeoutPerCall;
            return this;
        }

        public Builder timeoutScheduler(Scheduler timeoutScheduler) {
            this.timeoutScheduler = Objects.requireNonNull(timeoutScheduler, "timeoutScheduler is required");
            return this;
        }

        public Builder timeSource(TimeSource timeSource) {
            this.timeSource = Objects.requireNonNull(timeSource, "timeSource is required");
            return this;
        }

        public Builder failureOn(Class<? extends Throwable> failureType) {
            this.failureTypes.add(Objects.requireNonNull(failureType, "failureType is required"));
            return this;
        }

        public Builder failurePredicate(Predicate<Throwable> failurePredicate) {
            this.failurePredicate = Objects.requireNonNull(failurePredicate, "failurePredicate is required");
            return this;
        }

        public CircuitBreaker build() {
            return new CircuitBreaker(this);
        }

        private Predicate<Throwable> buildFailurePredicate() {
            final Predicate<Throwable> custom = failurePredicate;
            final List<Class<? extends Throwable>> types = List.copyOf(failureTypes);

            if (custom == null && types.isEmpty()) {
                // No custom criteria configured: every non-rejection, non-cancellation error
                // (both are already filtered out upstream in recordError) counts as a failure.
                return error -> true;
            }

            return error -> {
                if (custom != null && custom.test(error)) {
                    return true;
                }

                for (Class<? extends Throwable> type : types) {
                    if (type.isInstance(error)) {
                        return true;
                    }
                }

                return false;
            };
        }
    }
}
