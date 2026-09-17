package com.example.debounce;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;

class DebounceTest {

    @Test
    void trailingOnlyOperator() {
        StepVerifier.withVirtualTime(() -> {
                    VirtualTimeScheduler scheduler = VirtualTimeScheduler.getOrSet();
                    DebounceOptions options = DebounceOptions.builder()
                            .delay(Duration.ofMillis(100))
                            .scheduler(scheduler)
                            .build();
                    return Debounce.operator(Flux.just(1, 2, 3), options);
                })
                .thenAwait(Duration.ofMillis(100))
                .expectNext(3)
                .verifyComplete();
    }

    @Test
    void leadingEmitsImmediatelyThenSuppressesUntilPause() {
        StepVerifier.withVirtualTime(() -> {
                    VirtualTimeScheduler scheduler = VirtualTimeScheduler.getOrSet();
                    // 1, 2, 3 arrive back-to-back with no gap: only the first (leading edge) should
                    // emit, and 2/3 should be fully suppressed since trailing is disabled.
                    return Debounce.operator(Flux.just(1, 2, 3), o -> o
                            .delay(Duration.ofMillis(100))
                            .leading(true)
                            .trailing(false)
                            .scheduler(scheduler));
                })
                .expectNext(1)
                .thenAwait(Duration.ofMillis(100))
                .verifyComplete();
    }

    @Test
    void leadingAndTrailingBothFireForASpacedOutBurst() {
        StepVerifier.withVirtualTime(() -> {
                    VirtualTimeScheduler scheduler = VirtualTimeScheduler.getOrSet();
                    // event(1) fires the leading edge; event(2), arriving mid-window, becomes the
                    // trailing emission once the quiet period elapses.
                    Flux<Integer> events = Flux.concat(
                            Mono.just(1),
                            Mono.just(2).delayElement(Duration.ofMillis(10), scheduler)
                    );
                    return Debounce.operator(events, o -> o
                            .delay(Duration.ofMillis(100))
                            .leading(true)
                            .trailing(true)
                            .scheduler(scheduler));
                })
                .expectNext(1)
                .thenAwait(Duration.ofMillis(120))
                .expectNext(2)
                .verifyComplete();
    }
}