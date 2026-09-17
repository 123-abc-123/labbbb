package com.example.debounce;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ActionDebouncerTest {

    @Test
    void wrapperContinuesOnErrorWhenConfigured() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<Throwable> error = new AtomicReference<>();

        ActionDebouncer<String> debouncer = ActionDebouncer.<String>builder(event -> {
                    if ("bad".equals(event)) return Mono.error(new IllegalStateException("bad"));
                    return Mono.fromRunnable(counter::incrementAndGet).then();
                })
                .delay(Duration.ofMillis(100))
                .scheduler(scheduler)
                .continueOnError(true)
                .errorHandler(error::set)
                .build();

        debouncer.tryEmit("bad");
        scheduler.advanceTimeBy(Duration.ofMillis(100));
        assertNotNull(error.get());

        debouncer.tryEmit("good");
        scheduler.advanceTimeBy(Duration.ofMillis(100));
        assertEquals(1, counter.get());

        debouncer.dispose();
        scheduler.dispose();
    }

    @Test
    void disposeCancelsPendingTrailingInvocation() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        AtomicInteger counter = new AtomicInteger();

        ActionDebouncer<String> debouncer = ActionDebouncer.<String>builder(
                        event -> Mono.fromRunnable(counter::incrementAndGet).then())
                .delay(Duration.ofMillis(100))
                .scheduler(scheduler)
                .build();

        debouncer.tryEmit("pending");
        debouncer.dispose();
        scheduler.advanceTimeBy(Duration.ofMillis(200));

        // The trailing timer was still pending when dispose() ran; it must never fire the action.
        assertEquals(0, counter.get());

        scheduler.dispose();
    }

    @Test
    void runnableBuilderIgnoresDebouncedValue() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        AtomicInteger counter = new AtomicInteger();

        ActionDebouncer<Object> debouncer = ActionDebouncer.builder((Runnable) counter::incrementAndGet)
                .delay(Duration.ofMillis(100))
                .scheduler(scheduler)
                .build();

        debouncer.tryEmit(new Object());
        scheduler.advanceTimeBy(Duration.ofMillis(100));
        assertEquals(1, counter.get());

        debouncer.dispose();
        scheduler.dispose();
    }

    @Test
    void ofBuildsFromPreConstructedOptionsAndUsesThePayload() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        AtomicReference<String> received = new AtomicReference<>();
        DebounceOptions options = DebounceOptions.builder()
                .delay(Duration.ofMillis(100))
                .scheduler(scheduler)
                .build();

        ActionDebouncer<String> debouncer = ActionDebouncer.of(
                value -> Mono.fromRunnable(() -> received.set(value)).then(),
                options
        );

        debouncer.tryEmit("payload");
        scheduler.advanceTimeBy(Duration.ofMillis(100));
        assertEquals("payload", received.get());

        debouncer.dispose();
        scheduler.dispose();
    }

    @Test
    void ofRunnableIgnoresPayloadAndUsesPreConstructedOptions() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        AtomicInteger counter = new AtomicInteger();
        DebounceOptions options = DebounceOptions.builder()
                .delay(Duration.ofMillis(100))
                .scheduler(scheduler)
                .build();

        ActionDebouncer<Object> debouncer = ActionDebouncer.ofRunnable(counter::incrementAndGet, options);

        debouncer.tryEmit(new Object());
        scheduler.advanceTimeBy(Duration.ofMillis(100));
        assertEquals(1, counter.get());

        debouncer.dispose();
        scheduler.dispose();
    }
}