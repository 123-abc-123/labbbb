package com.example.throttle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class PendingTest {

    @Test
    void cancelBeforeRun_workNeverStarts() {
        AtomicInteger runs = new AtomicInteger();
        var p = new RateLimiter.Pending<Integer>(() -> Mono.<Integer>fromRunnable(runs::incrementAndGet));

        p.cancel();
        p.run();

        assertEquals(0, runs.get());
        assertTrue(p.isCancelled());
    }

    @Test
    void supersededCallNeverRuns() {
        AtomicInteger runs = new AtomicInteger();
        var p = new RateLimiter.Pending<Integer>(() -> Mono.<Integer>fromRunnable(runs::incrementAndGet));

        p.supersede();
        p.run();

        assertEquals(0, runs.get());
    }

    @Test
    void runIsAtMostOnce() {
        AtomicInteger runs = new AtomicInteger();
        var p = new RateLimiter.Pending<Integer>(() -> Mono.<Integer>fromRunnable(runs::incrementAndGet));

        p.run();
        p.run();

        assertEquals(1, runs.get());
        assertFalse(p.isCancelled());
    }

    /**
     * Exact invariant under a real race: cancel won the CAS  <=>  the work never started.
     * (If run() won, a synchronous side effect has already happened and cancel cannot undo it.)
     */
    @Test
    void cancelRacingWithRun_exactlyOneWins() {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 20_000; i++) {
                AtomicInteger runs = new AtomicInteger();
                var p = new RateLimiter.Pending<Integer>(() -> Mono.<Integer>fromRunnable(runs::incrementAndGet));
                CountDownLatch go = new CountDownLatch(1);

                CompletableFuture<Void> a = CompletableFuture.runAsync(() -> { await(go); p.run(); }, pool);
                CompletableFuture<Void> b = CompletableFuture.runAsync(() -> { await(go); p.cancel(); }, pool);
                go.countDown();
                a.join();
                b.join();

                assertTrue(runs.get() <= 1, "work ran more than once");
                assertEquals(p.isCancelled(), runs.get() == 0, "cancel and run must be mutually exclusive");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
