package com.example.throttle;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;


/**
 * Public facade.
 *
 * <pre>{@code
 * var limiter = RateLimiter
 *     .withRateLimitStrategy(new TokenBucket(3, Rate.of(3, Duration.ofSeconds(1))))
 *     .onExcess(Excess.DROP)
 *     .edge(Edge.BOTH)
 *     .build();
 *
 * Flux.range(1, 10)
 *     .flatMapSequential(limiter.throttle(this::callApi));
 * }</pre>
 */
public final class RateLimiter {

    private final OverflowPolicy overflowPolicy;
    private final AdmissionContext context;

    private RateLimiter(OverflowPolicy overflowPolicy, AdmissionContext context) {
        this.overflowPolicy = overflowPolicy;
        this.context = context;
    }

    public static Builder withRateLimitStrategy(RateLimitStrategy strategy) {
        return new Builder(strategy);
    }

    public <T, R> Function<T, Mono<R>> throttle(Function<? super T, ? extends Mono<R>> fn) {
        Objects.requireNonNull(fn, "fn");
        return x -> overflowPolicy.admit(context, () -> fn.apply(x));
    }

    public <T> Function<Flux<T>, Flux<T>> asTransformer() {
        if (overflowPolicy == OverflowPolicy.QUEUE) {
            return upstream -> upstream.concatMap(x -> overflowPolicy.admit(context, () -> Mono.just(x)));
        }

        return upstream -> upstream.flatMapSequential(x -> overflowPolicy.admit(context, () -> Mono.just(x)));
    }

    public static final class Builder {
        private final RateLimitStrategy strategy;
        private OverflowPolicy overflowPolicy = OverflowPolicy.QUEUE;
        private Edge edge;
        private Scheduler scheduler = Schedulers.parallel();

        private Builder(RateLimitStrategy strategy) {
            this.strategy = Objects.requireNonNull(strategy, "strategy");
        }

        public Builder onOverflow(OverflowPolicy overflowPolicy) {
            this.overflowPolicy = Objects.requireNonNull(overflowPolicy, "excess");
            return this;
        }

        public Builder edge(Edge edge) {
            this.edge = Objects.requireNonNull(edge, "edge");
            return this;
        }

        public Builder scheduler(Scheduler scheduler) {
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
            return this;
        }

        public RateLimiter build() {
            Edge effective = (edge != null) ? edge : overflowPolicy.defaultEdge();

            if (!overflowPolicy.allows(effective)) {
                throw new IllegalStateException(
                        "Excess." + overflowPolicy + " does not support Edge." + effective + "; use Excess.DROP");
            }

            return new RateLimiter(overflowPolicy, new AdmissionContext(strategy, scheduler, effective));
        }
    }

    public enum Edge {
        LEADING,
        TRAILING,
        BOTH;

        public boolean isLeading()  { return this == LEADING || this == BOTH; }
        public boolean isTrailing() { return this == TRAILING || this == BOTH; }
    }

    public enum OverflowPolicy {

        QUEUE(Edge.LEADING, EnumSet.of(Edge.LEADING)) {
            @Override
            <R> Mono<R> admit(AdmissionContext ctx, Supplier<Mono<R>> work) {
                return Mono.defer(() -> {
                    long wait = ctx.reserve(RateLimitStrategy.UNBOUNDED);

                    if (wait == RateLimitStrategy.REJECTED) {
                        return Mono.error(new RejectedExecutionException(
                                "rate limit strategy refused an unbounded reservation (arithmetic overflow?)"));
                    }

                    if (wait == 0) {
                        return work.get();
                    }

                    return Mono.delay(Duration.ofNanos(wait), ctx.scheduler).then(Mono.defer(work));
                });
            }
        },

        /**
         * Discard excess calls, optionally keeping the latest one (see {@link Edge}).
         * <p>Note on TRAILING: If the bucket is full, a TRAILING-only configuration will consume
         * <i>two</i> permits. The first burns a token to advance the strategy's internal clock,
         * ensuring the second reservation actually yields a future delay rather than executing immediately.
         */
        DROP(Edge.LEADING, EnumSet.allOf(Edge.class)) {
            @Override
            <R> Mono<R> admit(AdmissionContext ctx, Supplier<Mono<R>> work) {
                return Mono.defer(() -> {
                    long now = ctx.nowNanos();

                    if (!ctx.edge.isTrailing()) {
                        return ctx.reserveAt(now, 0) == 0 ? work.get() : Mono.empty();
                    }

                    boolean runNow = false;
                    Pending<R> mine = null;
                    Pending<?> superseded = null;
                    long armAfter = -1;

                    synchronized (ctx.lock) {
                        if (ctx.pending != null) {
                            superseded = ctx.pending;
                            mine = new Pending<>(work);
                            ctx.pending = mine;
                        } else {
                            long w = ctx.reserveAt(now, 0);

                            if (w == 0 && ctx.edge.isLeading()) {
                                runNow = true;
                            } else {
                                long delay = ctx.reserveAt(now, RateLimitStrategy.UNBOUNDED);

                                if (delay != RateLimitStrategy.REJECTED) {
                                    mine = new Pending<>(work);
                                    ctx.pending = mine;
                                    armAfter = delay;
                                }
                            }
                        }
                    }

                    if (superseded != null) {
                        superseded.supersede();
                    }

                    if (runNow) {
                        return work.get();
                    }

                    if (mine == null) {
                        return Mono.empty();
                    }

                    if (armAfter >= 0) {
                        ctx.armTimer(armAfter);
                    }

                    return mine.asMono();
                });
            }
        };

        private final Edge defaultEdge;
        private final Set<Edge> allowedEdges;

        OverflowPolicy(Edge defaultEdge, Set<Edge> allowedEdges) {
            this.defaultEdge = defaultEdge;
            this.allowedEdges = allowedEdges;
        }

        public Edge defaultEdge() {
            return defaultEdge;
        }

        public boolean allows(Edge edge) {
            return allowedEdges.contains(edge);
        }

        abstract <R> Mono<R> admit(AdmissionContext ctx, Supplier<Mono<R>> work);
    }

    private static final class AdmissionContext {

        final RateLimitStrategy strategy;
        final Scheduler scheduler;
        final Edge edge;

        final Object lock = new Object();
        Pending<?> pending;

        AdmissionContext(RateLimitStrategy strategy, Scheduler scheduler, Edge edge) {
            this.strategy = strategy;
            this.scheduler = scheduler;
            this.edge = edge;
        }

        long nowNanos() {
            return scheduler.now(TimeUnit.NANOSECONDS);
        }

        long reserve(long maxWaitNanos) {
            return strategy.reserve(nowNanos(), maxWaitNanos);
        }

        long reserveAt(long nowNanos, long maxWaitNanos) {
            return strategy.reserve(nowNanos, maxWaitNanos);
        }

        void armTimer(long delayNanos) {
            try {
                scheduler.schedule(this::fire, delayNanos, TimeUnit.NANOSECONDS);
            } catch (RuntimeException e) {
                Pending<?> p = take();

                if (p != null) {
                    p.fail(e);
                }
            }
        }

        void fire() {
            Pending<?> p = take();
            if (p != null) {
                p.run();
            }
        }

        private Pending<?> take() {
            synchronized (lock) {
                Pending<?> p = pending;
                pending = null;
                return p;
            }
        }
    }

    public static final class Pending<R> {

        private enum State { WAITING, RUNNING, CANCELLED }

        private final Supplier<Mono<R>> work;
        private final Sinks.One<R> sink = Sinks.one();
        private final AtomicReference<State> state = new AtomicReference<>(State.WAITING);
        private final Disposable.Swap inner = Disposables.swap();

        Pending(Supplier<Mono<R>> work) {
            this.work = work;
        }

        Mono<R> asMono() {
            return sink.asMono().doOnCancel(this::cancel);
        }

        void supersede() {
            if (state.compareAndSet(State.WAITING, State.CANCELLED)) {
                sink.tryEmitEmpty();
            }
        }

        void cancel() {
            if (state.compareAndSet(State.WAITING, State.CANCELLED)) {
                sink.tryEmitEmpty();
            } else {
                inner.dispose();
            }
        }

        void fail(Throwable error) {
            if (state.compareAndSet(State.WAITING, State.CANCELLED)) {
                sink.tryEmitError(error);
            }
        }

        void run() {
            if (!state.compareAndSet(State.WAITING, State.RUNNING)) {
                return;
            }
            inner.update(Mono.defer(work).subscribe(
                    sink::tryEmitValue, sink::tryEmitError, sink::tryEmitEmpty));
        }

        boolean isCancelled() {
            return state.get() == State.CANCELLED;
        }
    }
}
