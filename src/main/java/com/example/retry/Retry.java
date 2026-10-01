package com.example.retry;

import com.example.exception.ResilienceRejection;
import com.example.exception.CircuitBreakerOpenException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class Retry {
    private static final int MAX_CAUSE_CHAIN_DEPTH = 32;

    private final int maxAttempts;
    private final Function<Long, Duration> delayStrategy;
    private final Predicate<Throwable> retryOn;
    private final Scheduler scheduler;

    private Retry(Builder builder) {
        this.maxAttempts = builder.maxAttempts;
        this.delayStrategy = builder.delayStrategy;
        this.scheduler = builder.scheduler;
        this.retryOn = builder.buildRetryPredicate();
    }

    public static Builder builder() {
        return new Builder();
    }

    public <T> Mono<T> execute(Supplier<Mono<T>> fn) {
        int maxRetries = Math.max(0, maxAttempts - 1);

        return Mono.defer(() -> {

                    Mono<T> mono;
                    try {
                        mono = fn.get();
                    } catch (Throwable t) {
                        return Mono.error(t);
                    }
                    if (mono == null) {
                        return Mono.error(new NullPointerException("fn returned null"));
                    }
                    return mono;
                })
                .retryWhen(reactor.util.retry.Retry.from(signals -> signals.concatMap(signal -> {
                    long retryNumber = signal.totalRetries() + 1;
                    Throwable failure = signal.failure();

                    boolean allowed;
                    try {
                        allowed = retryOn.test(failure);
                    } catch (Throwable predicateError) {
                        return Mono.<Long>error(predicateError);
                    }

                    if (retryNumber > maxRetries || !allowed) {
                        return Mono.<Long>error(failure);
                    }

                    Duration delay;
                    try {
                        delay = delayStrategy.apply(retryNumber);
                    } catch (Throwable delayError) {
                        return Mono.<Long>error(delayError);
                    }

                    if (delay == null) {
                        return Mono.<Long>error(
                                new IllegalStateException("delay strategy returned null")
                        );
                    }
                    if (delay.isNegative()) {
                        return Mono.<Long>error(
                                new IllegalStateException("delay strategy returned negative delay")
                        );
                    }
                    if (delay.isZero()) {
                        return Mono.just(retryNumber);
                    }

                    return Mono.delay(delay, scheduler)
                            .then(Mono.just(retryNumber));
                })));
    }

    public static final class Builder {
        private int maxAttempts = 3;
        private Function<Long, Duration> delayStrategy = RetryDelayStrategies.constant(Duration.ZERO);
        private Scheduler scheduler = Schedulers.parallel();

        private boolean retryAll = false;
        private boolean retryOnRejections = false;
        private boolean traverseCauseChain = true;

        private final List<Class<? extends Throwable>> retryOnExceptions = new ArrayList<>();
        private final Set<Integer> retryOnCodes = new HashSet<>();
        private Predicate<Throwable> retryPredicate;

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder delayStrategy(Function<Long, Duration> delayStrategy) {
            this.delayStrategy = Objects.requireNonNull(delayStrategy, "delayStrategy is required");
            return this;
        }

        public Builder constantDelay(Duration delay) {
            this.delayStrategy = RetryDelayStrategies.constant(delay);
            return this;
        }

        public Builder exponentialBackoff(Duration initialDelay) {
            this.delayStrategy = RetryDelayStrategies.exponential(initialDelay);
            return this;
        }

        public Builder scheduler(Scheduler scheduler) {
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler is required");
            return this;
        }

        public Builder retryAll() {
            this.retryAll = true;
            return this;
        }

        /**
         * By default, any {@link ResilienceRejection} (e.g. {@link CircuitBreakerOpenException})
         * is never retried, regardless of {@link #retryAll()} or any configured predicate/exception
         * list — a rejection means the call was never attempted, so retrying it blindly would just
         * hammer a breaker that has already decided to back off. Call this to explicitly opt back in.
         *
         * <p>Renamed from the previous {@code retryCircuitBreakerOpen()} now that the check is based
         * on the general {@link ResilienceRejection} marker rather than the concrete circuit-breaker
         * exception type — update any existing call sites accordingly.
         */
        public Builder retryOnRejections() {
            this.retryOnRejections = true;
            return this;
        }

        public Builder traverseCauseChain(boolean traverse) {
            this.traverseCauseChain = traverse;
            return this;
        }

        public Builder retryOnException(Class<? extends Throwable> exceptionType) {
            this.retryOnExceptions.add(
                    Objects.requireNonNull(exceptionType, "exceptionType is required")
            );
            return this;
        }

        @SafeVarargs
        public final Builder retryOnExceptions(Class<? extends Throwable>... exceptionTypes) {
            for (Class<? extends Throwable> type : exceptionTypes) {
                retryOnException(type);
            }
            return this;
        }

        public Builder retryOnCode(int code) {
            this.retryOnCodes.add(code);
            return this;
        }

        public Builder retryOnCodes(int... codes) {
            for (int code : codes) {
                retryOnCode(code);
            }
            return this;
        }

        public Builder retryOn(Predicate<Throwable> predicate) {
            this.retryPredicate = Objects.requireNonNull(predicate, "predicate is required");
            return this;
        }

        public Retry build() {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be >= 1");
            }
            Objects.requireNonNull(delayStrategy, "delayStrategy is required");
            Objects.requireNonNull(scheduler, "scheduler is required");

            return new Retry(this);
        }

        private Predicate<Throwable> buildRetryPredicate() {
            final boolean all = retryAll;
            final boolean includeRejections = retryOnRejections;
            final boolean traverse = traverseCauseChain;
            final Predicate<Throwable> custom = retryPredicate;
            final List<Class<? extends Throwable>> exceptions = List.copyOf(retryOnExceptions);
            final Set<Integer> codes = Set.copyOf(retryOnCodes);

            return error -> {
                if (!includeRejections && error instanceof ResilienceRejection) {
                    return false;
                }

                if (all) {
                    return true;
                }

                if (custom == null && exceptions.isEmpty() && codes.isEmpty()) {
                    return false;
                }

                Throwable current = error;
                int depth = 0;

                while (current != null && depth++ < MAX_CAUSE_CHAIN_DEPTH) {
                    if (custom != null && custom.test(current)) {
                        return true;
                    }

                    for (Class<? extends Throwable> exceptionType : exceptions) {
                        if (exceptionType.isInstance(current)) {
                            return true;
                        }
                    }

                    if (!codes.isEmpty() && current instanceof ErrorCodeProvider provider) {
                        if (codes.contains(provider.errorCode())) {
                            return true;
                        }
                    }

                    if (!traverse) {
                        break;
                    }

                    current = current.getCause();
                }

                return false;
            };
        }
    }
}
