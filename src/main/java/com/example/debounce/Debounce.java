package com.example.debounce;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * The stateless debounce operator: {@code Flux<T> -> Flux<T>}. No subscription is held and nothing
 * needs disposing — this is a pure transform, applied fresh each time the returned {@code Flux} is
 * subscribed to (see {@link Flux#defer}).
 *
 * <p>For debouncing calls to a callback/action rather than transforming an existing {@code Flux},
 * use {@link ActionDebouncer} instead — it is a genuinely different shape (it holds a subscription
 * and must be disposed), which is why it is a separate class rather than another method here.
 */
public final class Debounce {

    private Debounce() {
    }

    /**
     * Applies the debounce operator to {@code source} using the given options.
     */
    public static <T> Flux<T> operator(Flux<T> source, DebounceOptions options) {
        Objects.requireNonNull(source, "source is required");
        Objects.requireNonNull(options, "options is required");

        return Flux.defer(() -> {
            DebounceState state = new DebounceState();

            return source.switchMap(event -> {
                boolean emitLeading = state.begin(options.leading());

                Mono<T> trailingSignal = Mono.delay(options.delay(), options.scheduler())
                        .flatMap(tick -> {
                            boolean emitTrailing = state.complete(
                                    options.leading(),
                                    options.trailing()
                            );
                            return emitTrailing ? Mono.just(event) : Mono.<T>empty();
                        });

                if (emitLeading) {
                    return Flux.just(event).concatWith(trailingSignal);
                }
                return trailingSignal.flux();
            });
        });
    }

    /**
     * Convenience overload for configuring options inline at the call site, e.g.:
     * <pre>{@code
     * Debounce.operator(source, o -> o.delay(Duration.ofMillis(100)).scheduler(scheduler));
     * }</pre>
     */
    public static <T> Flux<T> operator(Flux<T> source, Consumer<DebounceOptions.Builder> configurer) {
        Objects.requireNonNull(configurer, "configurer is required");
        DebounceOptions.Builder builder = DebounceOptions.builder();
        configurer.accept(builder);
        return operator(source, builder.build());
    }
}