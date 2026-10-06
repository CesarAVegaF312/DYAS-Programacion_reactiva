package com.example.Controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import reactor.test.StepVerifier;

/**
 * Pruebas unitarias del controlador reactivo (sin levantar el servidor).
 */
class ReactiveControllerTest {

    private final ReactiveController controller = new ReactiveController();

    @Test
    void helloReturnsPlainString() {
        assertEquals("Hello, Reactive!", controller.hello());
    }

    @Test
    void monoEmitsOneValueAndCompletes() {
        StepVerifier.create(controller.getMono())
                .expectNext("Hello, Mono!")
                .verifyComplete();
    }

    @Test
    void fluxEmitsOneValueAndCompletes() {
        StepVerifier.create(controller.getFlux())
                .expectNext("Hello, Flux!")
                .verifyComplete();
    }
}
