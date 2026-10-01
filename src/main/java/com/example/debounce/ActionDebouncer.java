package com.example.debounce;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

public final class ActionDebouncer<T> implements Disposable {

    private final Sinks.Many<T> sink;
    private final Disposable subscription;
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    // Serializes emissions and disposal to prevent emitting to a completed sink
    private final ReentrantLock emitLock = new ReentrantLock();

    private ActionDebouncer(Function<T, Mono<Void>> action, DebounceOptions options) {
        Objects.requireNonNull(action, "action is required");
        Objects.requireNonNull(options, "options is required");

        this.sink = Sinks.many().multicast().onBackpressureBuffer();
        Flux<T> debounced = Debounce.operator(sink.asFlux(), options);

        this.subscription = debounced
                .concatMap(value -> invoke(action, value, options))
                .subscribe(
                        unused -> {}, // success is handled by the Mono<Void> completion
                        error -> options.errorHandler().accept(error)
                );
    }


    /**
     * Starts building an {@code ActionDebouncer} that invokes {@code action} for each event that
     * survives debouncing, awaiting the returned {@code Mono<Void>} before accepting the next one.
     */
    public static <T> Builder<T> builder(Function<T, Mono<Void>> action) {
        return new Builder<>(action);
    }

    public static Builder<Object> builder(Runnable action) {
        Objects.requireNonNull(action, "action is required");
        return new Builder<>(ignored -> Mono.fromRunnable(action).then());
    }

    /**
     * Builds an {@code ActionDebouncer} from an already-constructed {@link DebounceOptions} —
     * useful when the same options are shared across several debouncers, so each call site doesn't
     * re-enter every field through {@link #builder}. Use this overload when the debounced event
     * carries a value the action needs; use {@link #ofRunnable} when it doesn't.
     */
    public static <T> ActionDebouncer<T> of(Function<T, Mono<Void>> action, DebounceOptions options) {
        return new ActionDebouncer<>(action, options);
    }

    /**
     * {@link #of} counterpart for fire-and-forget actions: use this when you only care that
     * something happened, not what happened, so the action doesn't need the debounced payload.
     */
    public static ActionDebouncer<Object> ofRunnable(Runnable action, DebounceOptions options) {
        Objects.requireNonNull(action, "action is required");
        return of(ignored -> Mono.fromRunnable(action).then(), options);
    }

    public Mono<Void> emit(T value) {
        return Mono.fromRunnable(() -> tryEmit(value)).then();
    }

    /**
     * @return true if the event was successfully accepted by the sink
     */
    public boolean tryEmit(T value) {
        emitLock.lock();

        try {
            if (disposed.get()) {
                return false;
            }

            Sinks.EmitResult result = sink.tryEmitNext(value);
            return result == Sinks.EmitResult.OK;
        } finally {
            emitLock.unlock();
        }
    }

    private static <T> Mono<Void> invoke(Function<T, Mono<Void>> action, T value, DebounceOptions options) {
        Mono<Void> mono = Mono.defer(() -> {
            Mono<Void> result;

            try {
                result = action.apply(value);
            } catch (Throwable t) {
                return Mono.error(t);
            }

            return result != null ? result : Mono.empty();
        });

        if (options.continueOnError()) {
            return mono.onErrorResume(t -> {
                options.errorHandler().accept(t);
                return Mono.empty();
            });
        }

        // If continueOnError is false, errors propagate to the subscribe() error handler and
        // terminate the debouncer's internal pipeline.
        return mono;
    }

    @Override
    public void dispose() {
        emitLock.lock();
        try {
            // compareAndSet ensures disposal logic only runs exactly once
            if (!disposed.compareAndSet(false, true)) {
                return;
            }
            subscription.dispose();
            sink.tryEmitComplete();
        } finally {
            emitLock.unlock();
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }

    /**
     * Delegates all timing/behavior configuration to a {@link DebounceOptions.Builder} internally
     * rather than duplicating those fields, so {@code DebounceOptions} stays the single source of
     * truth for debounce settings.
     */
    public static final class Builder<T> {
        private final Function<T, Mono<Void>> action;
        private final DebounceOptions.Builder optionsBuilder = DebounceOptions.builder();

        private Builder(Function<T, Mono<Void>> action) {
            this.action = Objects.requireNonNull(action, "action is required");
        }

        public Builder<T> delay(Duration delay) {
            optionsBuilder.delay(delay);
            return this;
        }

        public Builder<T> delayMs(long delayMs) {
            optionsBuilder.delayMs(delayMs);
            return this;
        }

        public Builder<T> leading(boolean leading) {
            optionsBuilder.leading(leading);
            return this;
        }

        public Builder<T> trailing(boolean trailing) {
            optionsBuilder.trailing(trailing);
            return this;
        }

        public Builder<T> scheduler(Scheduler scheduler) {
            optionsBuilder.scheduler(scheduler);
            return this;
        }

        public Builder<T> continueOnError(boolean continueOnError) {
            optionsBuilder.continueOnError(continueOnError);
            return this;
        }

        public Builder<T> errorHandler(Consumer<Throwable> errorHandler) {
            optionsBuilder.errorHandler(errorHandler);
            return this;
        }

        public ActionDebouncer<T> build() {
            return new ActionDebouncer<>(action, optionsBuilder.build());
        }
    }
}