package com.example.throttle;

import static org.junit.jupiter.api.Assertions.*;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofNanos;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

class RateLimiterTest {

    private static final long SEC = 1_000_000_000L;
    private static final Rate THREE_PER_SEC = Rate.of(3, ofSeconds(1));
    private static final Rate ONE_PER_SEC = Rate.of(1, ofSeconds(1));
    private static final long T = THREE_PER_SEC.nanosPerPermit();

    record Run(int id, long atNanos) {}

    static final class Outcome {
        Integer value;
        Throwable error;
        boolean done;
    }

    private VirtualTimeScheduler vts;
    private List<Run> runs;

    @BeforeEach
    void setUp() {
        vts = VirtualTimeScheduler.create();
        runs = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        vts.dispose();
    }

    private Mono<Integer> job(Integer id) {
        return Mono.fromSupplier(() -> {
            runs.add(new Run(id, vts.now(NANOSECONDS)));
            return id;
        });
    }

    private Outcome call(Function<Integer, Mono<Integer>> fn, int id) {
        Outcome o = new Outcome();
        fn.apply(id).subscribe(v -> o.value = v, e -> o.error = e, () -> o.done = true);
        return o;
    }

    private List<Integer> ids() {
        return runs.stream().map(Run::id).toList();
    }

    private RateLimiter.Builder limiter(RateLimitStrategy s) {
        return RateLimiter.withRateLimitStrategy(s).scheduler(vts);
    }

    // ---------------- window-based limiting ----------------

    @Test
    void drop_tokenBucket_tenCallsAtOnce_onlyCapacityRun() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(3, THREE_PER_SEC)).onOverflow(RateLimiter.OverflowPolicy.DROP).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        List<Outcome> outs = IntStream.rangeClosed(1, 10).mapToObj(i -> call(fn, i)).toList();

        assertEquals(List.of(1, 2, 3), ids());
        for (Outcome o : outs.subList(3, 10)) {       // dropped calls complete empty
            assertTrue(o.done);
            assertNull(o.value);
        }
    }

    @Test
    void drop_fixedWindow_tenCallsOverOneSecond_onlyThreeRun() {
        var limiter = limiter(new FixedWindowRateLimitStrategy(THREE_PER_SEC)).onOverflow(RateLimiter.OverflowPolicy.DROP).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        for (int i = 1; i <= 10; i++) {
            call(fn, i);
            vts.advanceTimeBy(ofMillis(100));          // calls at 0, 100, ..., 900 ms
        }

        assertEquals(List.of(1, 2, 3), ids());
    }

    // ---------------- queue: order preserved ----------------

    @Test
    void queue_preservesOrder_andSpacesExecutionsAtRefillRate() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(3, THREE_PER_SEC)).onOverflow(RateLimiter.OverflowPolicy.QUEUE).build();
        List<Integer> results = new ArrayList<>();

        Flux.range(1, 10).concatMap(limiter.throttle(this::job)).subscribe(results::add);

        assertEquals(List.of(1, 2, 3), ids());         // the burst runs immediately
        vts.advanceTimeBy(ofNanos(7 * T));

        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), results);
        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), ids());
        assertEquals(List.of(0L, 0L, 0L, T, 2 * T, 3 * T, 4 * T, 5 * T, 6 * T, 7 * T),
                runs.stream().map(Run::atNanos).toList());
    }

    @Test
    void queue_asTransformer_preservesOrder() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(3, THREE_PER_SEC)).onOverflow(RateLimiter.OverflowPolicy.QUEUE).build();
        List<Integer> results = new ArrayList<>();

        Flux.range(1, 10).transform(limiter.asTransformer()).subscribe(results::add);
        vts.advanceTimeBy(ofNanos(7 * T));

        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), results);
    }

    // ---------------- leading / trailing ----------------

    @Test
    void leading_runsFirst_dropsRest() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(1, ONE_PER_SEC)).onOverflow(RateLimiter.OverflowPolicy.DROP).build(); // default LEADING
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        call(fn, 1);
        vts.advanceTimeBy(ofMillis(100));
        call(fn, 2);
        vts.advanceTimeBy(ofMillis(100));
        call(fn, 3);
        vts.advanceTimeBy(ofSeconds(5) .minus(ofMillis(200)));

        assertEquals(List.of(1), ids());
    }

    @Test
    void both_runsFirstImmediately_andLatestDroppedCallAtWindowEnd() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(1, ONE_PER_SEC)).onOverflow(RateLimiter.OverflowPolicy.DROP).edge(RateLimiter.Edge.BOTH).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        Outcome o1 = call(fn, 1);
        vts.advanceTimeBy(ofMillis(100));
        Outcome o2 = call(fn, 2);
        vts.advanceTimeBy(ofMillis(100));
        Outcome o3 = call(fn, 3);
        vts.advanceTimeBy(ofMillis(100));
        Outcome o4 = call(fn, 4);
        assertEquals(List.of(new Run(1, 0)), runs);    // nothing else yet

        vts.advanceTimeBy(ofMillis(900));              // t = 1000 ms

        assertEquals(List.of(new Run(1, 0), new Run(4, SEC)), runs);
        assertEquals(1, o1.value);
        assertEquals(4, o4.value);
        for (Outcome superseded : List.of(o2, o3)) {   // superseded callers complete empty
            assertTrue(superseded.done);
            assertNull(superseded.value);
        }
    }

    @Test
    void trailing_neverRunsImmediately_latestRunsWhenWindowEnds() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(1, ONE_PER_SEC)).onOverflow(RateLimiter.OverflowPolicy.DROP).edge(RateLimiter.Edge.TRAILING).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        call(fn, 1);
        vts.advanceTimeBy(ofMillis(100));
        call(fn, 2);
        vts.advanceTimeBy(ofMillis(100));
        Outcome o3 = call(fn, 3);

        vts.advanceTimeBy(ofMillis(799));              // t = 999 ms
        assertEquals(List.of(), runs);                 // idle limiter, yet nothing ran

        vts.advanceTimeBy(ofMillis(1));                // t = 1000 ms
        assertEquals(List.of(new Run(3, SEC)), runs);
        assertEquals(3, o3.value);
    }

    // ---------------- cancellation ----------------

    @Test
    void trailing_cancelledCall_neverRuns() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(1, ONE_PER_SEC)).onOverflow(RateLimiter.OverflowPolicy.DROP).edge(RateLimiter.Edge.TRAILING).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        Disposable d = fn.apply(1).subscribe();
        d.dispose();
        vts.advanceTimeBy(ofSeconds(3));

        assertEquals(List.of(), runs);
    }

    @Test
    void trailing_laterCallStillRunsAtTheReservedSlot_afterACancelledOne() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(1, ONE_PER_SEC)).onOverflow(RateLimiter.OverflowPolicy.DROP).edge(RateLimiter.Edge.TRAILING).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        fn.apply(1).subscribe().dispose();             // cancelled: stays as a tombstone in the slot
        vts.advanceTimeBy(ofMillis(100));
        call(fn, 2);                                   // replaces the tombstone, same cycle, same slot
        vts.advanceTimeBy(ofSeconds(1));

        assertEquals(List.of(new Run(2, SEC)), runs);
    }

    // ---------------- builder validation ----------------

    @Test
    void builder_rejectsEdgesThatQueueCannotHonour() {
        var strategy = new TokenBucketRateLimitStrategy(1, ONE_PER_SEC);
        assertThrows(IllegalStateException.class,
                () -> RateLimiter.withRateLimitStrategy(strategy).onOverflow(RateLimiter.OverflowPolicy.QUEUE).edge(RateLimiter.Edge.TRAILING).build());
        assertThrows(IllegalStateException.class,
                () -> RateLimiter.withRateLimitStrategy(strategy).onOverflow(RateLimiter.OverflowPolicy.QUEUE).edge(RateLimiter.Edge.BOTH).build());
        RateLimiter.withRateLimitStrategy(strategy).onOverflow(RateLimiter.OverflowPolicy.QUEUE).edge(RateLimiter.Edge.LEADING).build();
        RateLimiter.withRateLimitStrategy(strategy).build();                       // defaults are valid
        RateLimiter.withRateLimitStrategy(strategy).onOverflow(RateLimiter.OverflowPolicy.DROP).edge(RateLimiter.Edge.BOTH).build();
    }



    // Paste these members into the existing RateLimiterTest.
// They rely on its helpers: vts, runs, job(), call(), ids(), limiter(), Run, Outcome, SEC, T, THREE_PER_SEC, ONE_PER_SEC.

    // ---------- helper ----------

    /** Max number of executions inside any half-open window [t, t + windowNanos) starting at an execution. */
    private static int maxRunsInAnyWindow(List<Run> runs, long windowNanos) {
        int max = 0;
        for (Run first : runs) {
            int n = 0;
            for (Run r : runs) {
                if (r.atNanos() >= first.atNanos() && r.atNanos() < first.atNanos() + windowNanos) {
                    n++;
                }
            }
            max = Math.max(max, n);
        }
        return max;
    }

    // ---------- window-based limiting: "10 calls over 1 second, limit 3/sec" ----------

    /**
     * Calls at 0,100,...,900 ms. Bucket(3, 3/s): burst of 3, then one token refills every ~333 ms,
     * so calls 5 (400 ms) and 8 (700 ms) also get through. 5 <= capacity + rate*T = 6.
     */
    @Test
    void drop_tokenBucket_tenCallsSpreadOverOneSecond_burstPlusRefill() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(3, THREE_PER_SEC))
                .onOverflow(RateLimiter.OverflowPolicy.DROP).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        for (int i = 1; i <= 10; i++) {
            call(fn, i);
            vts.advanceTimeBy(ofMillis(100));
        }

        assertEquals(List.of(1, 2, 3, 5, 8), ids());
        assertTrue(maxRunsInAnyWindow(runs, SEC) <= 6);
    }

    /** Queue mode, fixed window: nothing is lost, but executions are released in batches of 3 per window. */
    @Test
    void queue_fixedWindow_tenCalls_releasedThreePerWindow() {
        var limiter = limiter(new FixedWindowRateLimitStrategy(THREE_PER_SEC))
                .onOverflow(RateLimiter.OverflowPolicy.QUEUE).build();
        List<Integer> results = new ArrayList<>();

        Flux.range(1, 10).concatMap(limiter.throttle(this::job)).subscribe(results::add);

        assertEquals(List.of(1, 2, 3), ids());
        vts.advanceTimeBy(ofMillis(999));
        assertEquals(3, runs.size());                  // nothing leaks early
        vts.advanceTimeBy(ofMillis(1));
        assertEquals(6, runs.size());                  // t = 1 s
        vts.advanceTimeBy(ofSeconds(1));
        assertEquals(9, runs.size());                  // t = 2 s
        vts.advanceTimeBy(ofSeconds(1));
        assertEquals(10, runs.size());                 // t = 3 s

        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), results);
        assertEquals(List.of(0L, 0L, 0L, SEC, SEC, SEC, 2 * SEC, 2 * SEC, 2 * SEC, 3 * SEC),
                runs.stream().map(Run::atNanos).toList());
    }

    /** Queue mode, token bucket: the smooth bound capacity + rate*T holds for every 1 s window. */
    @Test
    void queue_tokenBucket_neverExceedsBoundInAnyOneSecondWindow() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(3, THREE_PER_SEC))
                .onOverflow(RateLimiter.OverflowPolicy.QUEUE).build();

        Flux.range(1, 10).concatMap(limiter.throttle(this::job)).subscribe();
        vts.advanceTimeBy(ofNanos(7 * T));

        assertEquals(10, runs.size());
        assertTrue(maxRunsInAnyWindow(runs, SEC) <= 6);
    }

    // ---------- queue: order ----------

    /** Reservation happens at subscription (Mono.defer), so even concurrent flatMap keeps FIFO order. */
    @Test
    void queue_underFlatMap_stillPreservesOrder() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(3, THREE_PER_SEC))
                .onOverflow(RateLimiter.OverflowPolicy.QUEUE).build();
        List<Integer> results = new ArrayList<>();

        Flux.range(1, 10).flatMap(limiter.throttle(this::job)).subscribe(results::add);
        vts.advanceTimeBy(ofNanos(7 * T));

        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), ids());
        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), results);
    }

    @Test
    void queue_failingWork_failsOnlyThatCaller() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(3, THREE_PER_SEC))
                .onOverflow(RateLimiter.OverflowPolicy.QUEUE).build();
        Function<Integer, Mono<Integer>> work =
                id -> id == 2 ? Mono.<Integer>error(new IllegalStateException("boom")) : job(id);
        Function<Integer, Mono<Integer>> fn = limiter.throttle(work);

        Outcome o1 = call(fn, 1);
        Outcome o2 = call(fn, 2);
        Outcome o3 = call(fn, 3);

        assertEquals(1, o1.value);
        assertInstanceOf(IllegalStateException.class, o2.error);
        assertEquals(3, o3.value);
    }

    /** Pins current behaviour: a caller that cancels while waiting does not refund its reserved slot. */
    @Test
    void queue_cancelledWhileWaiting_neverRuns_butSlotIsNotRefunded() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(1, ONE_PER_SEC))
                .onOverflow(RateLimiter.OverflowPolicy.QUEUE).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        call(fn, 1);                                   // runs at 0
        Disposable waiting = fn.apply(2).subscribe();  // reserved for t = 1 s
        waiting.dispose();
        call(fn, 3);                                   // queued behind the cancelled slot -> t = 2 s
        vts.advanceTimeBy(ofSeconds(3));

        assertEquals(List.of(new Run(1, 0), new Run(3, 2 * SEC)), runs);
    }

    // ---------- drop ----------

    @Test
    void drop_recoversAfterRefill() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(1, ONE_PER_SEC))
                .onOverflow(RateLimiter.OverflowPolicy.DROP).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        call(fn, 1);
        vts.advanceTimeBy(ofMillis(500));
        call(fn, 2);                                   // dropped
        vts.advanceTimeBy(ofMillis(500));
        call(fn, 3);                                   // t = 1 s, token is back

        assertEquals(List.of(new Run(1, 0), new Run(3, SEC)), runs);
    }

    @Test
    void drop_asTransformer_emitsOnlyPermittedItems_andCompletes() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(3, THREE_PER_SEC))
                .onOverflow(RateLimiter.OverflowPolicy.DROP).build();
        List<Integer> results = new ArrayList<>();
        boolean[] completed = {false};

        Flux.range(1, 10).transform(limiter.asTransformer())
                .subscribe(results::add, e -> {}, () -> completed[0] = true);

        assertEquals(List.of(1, 2, 3), results);
        assertTrue(completed[0]);
    }

    // ---------- leading / trailing ----------

    /** lodash semantics: with leading+trailing, a single call must not produce a trailing echo. */
    @Test
    void both_singleCall_runsOnce_noTrailingEcho() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(1, ONE_PER_SEC))
                .onOverflow(RateLimiter.OverflowPolicy.DROP).edge(RateLimiter.Edge.BOTH).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        call(fn, 1);
        vts.advanceTimeBy(ofSeconds(5));

        assertEquals(List.of(new Run(1, 0)), runs);
    }

    @Test
    void both_afterQuietPeriod_leadingRunsImmediatelyAgain() {
        var limiter = limiter(new TokenBucketRateLimitStrategy(1, ONE_PER_SEC))
                .onOverflow(RateLimiter.OverflowPolicy.DROP).edge(RateLimiter.Edge.BOTH).build();
        Function<Integer, Mono<Integer>> fn = limiter.throttle(this::job);

        call(fn, 1);
        vts.advanceTimeBy(ofMillis(100));
        call(fn, 2);                                   // trailing, scheduled for t = 1 s
        vts.advanceTimeBy(ofMillis(900));
        assertEquals(List.of(new Run(1, 0), new Run(2, SEC)), runs);

        vts.advanceTimeBy(ofSeconds(4));               // t = 5 s, limiter idle
        call(fn, 3);

        assertEquals(new Run(3, 5 * SEC), runs.get(2)); // leading edge again, no waiting
    }
}