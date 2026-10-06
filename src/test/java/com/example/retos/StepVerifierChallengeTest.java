package com.example.retos;

import java.time.Duration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Prueba guía de maestría, Estación 2: pruebas reactivas con StepVerifier,
 * incluyendo tiempo virtual.
 *
 * Las pruebas vienen con @Disabled para que "mvn verify" no las reporte en verde
 * mientras están vacías. Al implementar cada una, quita su @Disabled.
 */
class StepVerifierChallengeTest {

    @Test
    @Disabled("Estación 2: implementar")
    void verifiesExactSequence() {
        // TODO: construye un Flux equivalente al pipeline de ReactorExample
        // (Flux.range(1, 10), filtra impares, mapea a String) y valida con
        // StepVerifier la secuencia EXACTA emitida, en orden
        // (expectNext(...) encadenados), terminando en .verifyComplete().
        //
        // Ojo: si incluyes el flatMap con Mono.just(item).subscribeOn(Schedulers.parallel())
        // de ReactorExample, el orden de llegada NO está garantizado y la prueba fallará
        // de forma intermitente. Usa concatMap o flatMapSequential, o deja el pipeline
        // sin ese flatMap asíncrono.
    }

    @Test
    @Disabled("Estación 2: implementar")
    void verifiesDelayedFluxWithVirtualTime() {
        // TODO: usa StepVerifier.withVirtualTime(() -> ...) para probar un
        // Flux que use .delayElements(Duration.ofSeconds(5)) sin que el test
        // tarde realmente 5 segundos por elemento. Usa
        // .thenAwait(Duration.ofSeconds(5)) entre cada expectNext, y
        // .verifyComplete() al final.
    }
}
