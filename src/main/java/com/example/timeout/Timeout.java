package com.example.timeout;

import com.example.exception.OperationTimeoutException;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class Timeout {

    private Timeout() {
    }

    public static <T> Mono<T> withTimeout(Supplier<? extends Mono<? extends T>> fn, long timeoutMs) {
        return withTimeout(fn, timeoutMs, Schedulers.parallel());
    }

    public static <T> Mono<T> withTimeout(Supplier<? extends Mono<? extends T>> fn, long timeoutMs, Scheduler timerScheduler) {
        Objects.requireNonNull(fn, "fn must not be null");
        Objects.requireNonNull(timerScheduler, "timerScheduler must not be null");

        if (timeoutMs <= 0) {
            return Mono.error(() -> new IllegalArgumentException("timeout must be > 0"));
        }

        return Mono.create(sink -> {
            AtomicBoolean terminated = new AtomicBoolean(false);

            AtomicReference<Disposable> sourceReference = new AtomicReference<>();
            AtomicReference<Disposable> timerReference = new AtomicReference<>();

            sink.onCancel(() -> {
                if (terminated.compareAndSet(false, true)) {
                    dispose(sourceReference.get());
                    dispose(timerReference.get());
                }
            });

            sink.onDispose(() -> {
                terminated.set(true);
                dispose(sourceReference.get());
                dispose(timerReference.get());
            });

            Disposable timer;

            try {
                timer = timerScheduler.schedule(() -> {
                    if (terminated.compareAndSet(false, true)) {
                        dispose(sourceReference.get());
                        sink.error(new OperationTimeoutException(timeoutMs));
                    }
                }, timeoutMs, TimeUnit.MILLISECONDS);
            } catch (Throwable scheduleError) {
                Exceptions.throwIfFatal(scheduleError);

                if (terminated.compareAndSet(false, true)) {
                    sink.error(scheduleError);
                }

                return;
            }

            timerReference.set(timer);

            /*
             * The timer may have already fired synchronously,
             * or downstream cancellation may have already happened.
             */
            if (terminated.get()) {
                timer.dispose();
                return;
            }

            Disposable source;

            try {
                /*
                 * Re-check just before starting the source.
                 *
                 * This does not remove every possible race but it avoids
                 * starting the source when termination is already known.
                 */
                if (terminated.get()) {
                    timer.dispose();
                    return;
                }

                source = Mono.defer(fn)
                        /*
                         * Propagate downstream context into the
                         * manually subscribed inner source.
                         */
                        .contextWrite(ctx -> ctx.putAll(sink.contextView()))
                        .subscribe(
                                value -> {
                                    if (terminated.compareAndSet(false, true)) {
                                        dispose(timerReference.get());
                                        sink.success(value);
                                    }
                                },
                                error -> {
                                    Exceptions.throwIfFatal(error);

                                    if (terminated.compareAndSet(false, true)) {
                                        dispose(timerReference.get());
                                        sink.error(error);
                                    }
                                },
                                () -> {
                                    if (terminated.compareAndSet(false, true)) {
                                        dispose(timerReference.get());
                                        sink.success();
                                    }
                                }
                        );
            } catch (Throwable subscriptionError) {
                Exceptions.throwIfFatal(subscriptionError);

                if (terminated.compareAndSet(false, true)) {
                    dispose(timer);
                    sink.error(subscriptionError);
                }

                return;
            }

            sourceReference.set(source);

            /*
             * If timeout/cancel happened while the source subscription
             * was being created, dispose it now.
             */
            if (terminated.get()) {
                source.dispose();
            }
        });
    }

    private static void dispose(Disposable disposable) {
        if (disposable != null) {
            disposable.dispose();
        }
    }
}
