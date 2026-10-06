package com.example.Controllers;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/reactive")
public class ReactiveController {

    @RequestMapping("/hello")
    public String hello() {
        return "Hello, Reactive!";
    }

    @GetMapping("/mono")
    public Mono<String> getMono() {
        return Mono.just("Hello, Mono!");
    }

    @GetMapping("/flux")
    public Flux<String> getFlux() {
        return Flux.just("Hello, Flux!");
    }

    /**
     * Reto (prueba guía de maestría, Estación 5): convierte este endpoint en
     * streaming de Server-Sent Events.
     *
     * TODO:
     *  - Agrega produces = MediaType.TEXT_EVENT_STREAM_VALUE en el @GetMapping
     *    (el tipo de retorno Flux<String> ya es el correcto).
     *  - Usa Flux.interval(Duration.ofSeconds(1)) como fuente y aplícale al
     *    menos un map (para formatear el evento) y un filter.
     *    Necesitarás import java.time.Duration;
     *  - Pruébalo con: curl -N http://localhost:8080/reactive/stream
     *    y confirma que los eventos llegan uno por uno, cada uno como una
     *    línea "data:...".
     */
    @GetMapping("/stream")
    public Flux<String> getStream() {
        throw new UnsupportedOperationException("TODO: implementar streaming con Flux.interval");
    }

}
