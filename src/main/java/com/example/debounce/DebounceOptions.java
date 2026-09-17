package com.example.debounce;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

public final class DebounceOptions {

    private final Duration delay;
    private final boolean leading;
    private final boolean trailing;
    private final Scheduler scheduler;
    private final boolean continueOnError;
    private final Consumer<Throwable> errorHandler;

    private DebounceOptions(Builder builder) {
        this.delay = builder.delay;
        this.leading = builder.leading;
        this.trailing = builder.trailing;
        this.scheduler = builder.scheduler;
        this.continueOnError = builder.continueOnError;
        this.errorHandler = builder.errorHandler;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Duration delay() { return delay; }
    public boolean leading() { return leading; }
    public boolean trailing() { return trailing; }
    public Scheduler scheduler() { return scheduler; }
    public boolean continueOnError() { return continueOnError; }
    public Consumer<Throwable> errorHandler() { return errorHandler; }

    public static final class Builder {
        private Duration delay;
        private boolean leading = false;
        private boolean trailing = true;
        private Scheduler scheduler = Schedulers.parallel();
        private boolean continueOnError = false;
        private Consumer<Throwable> errorHandler = t ->
                System.err.println("Unhandled debounce error: " + t);

        public Builder delay(Duration delay) {
            Objects.requireNonNull(delay, "delay is required");
            if (delay.isNegative()) throw new IllegalArgumentException("delay must be non-negative");
            this.delay = delay;
            return this;
        }

        public Builder delayMs(long delayMs) {
            if (delayMs < 0) throw new IllegalArgumentException("delayMs must be non-negative");
            this.delay = Duration.ofMillis(delayMs);
            return this;
        }

        public Builder leading(boolean leading) { this.leading = leading; return this; }
        public Builder trailing(boolean trailing) { this.trailing = trailing; return this; }

        public Builder scheduler(Scheduler scheduler) {
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler is required");
            return this;
        }

        public Builder continueOnError(boolean continueOnError) {
            this.continueOnError = continueOnError;
            return this;
        }

        public Builder errorHandler(Consumer<Throwable> errorHandler) {
            this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler is required");
            return this;
        }

        public DebounceOptions build() {
            if (delay == null) {
                throw new IllegalArgumentException("delay is required");
            }
            if (!leading && !trailing) {
                throw new IllegalArgumentException(
                        "at least one of leading or trailing must be enabled; "
                                + "with both disabled, the debounced action would never run"
                );
            }
            return new DebounceOptions(this);
        }
    }
}
