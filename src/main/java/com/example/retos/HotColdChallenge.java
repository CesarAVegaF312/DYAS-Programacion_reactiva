package com.example.retos;

import java.time.Duration;

import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;

/**
 * Prueba guía de maestría, Estación 4: publishers Hot y Cold.
 *
 * Escenario: dos paneles de monitoreo (A y B) se suscriben al mismo stream de
 * eventos en momentos distintos.
 *
 * Ejecuta con:
 *   mvn compile exec:java -Dexec.mainClass=com.example.retos.HotColdChallenge
 *
 * Sugerencia: usa una fuente que tarde en emitir, por ejemplo
 * Flux.interval(Duration.ofMillis(100)).take(5), y espera con Thread.sleep(...)
 * entre la suscripción del panel A y la del panel B. Con Flux.range(1, 5) todo se
 * emite de inmediato y no se alcanza a ver la diferencia.
 */
public class HotColdChallenge {

    public static void main(String[] args) throws InterruptedException {
        demonstrateColdBehavior();
        demonstrateHotBehavior();
    }

    /**
     * TODO: crea un Flux "frío" y suscríbete dos veces, con un pequeño delay entre
     * las dos suscripciones. Demuestra (con logs) que cada suscriptor recibe la
     * secuencia completa desde el inicio, de forma independiente.
     */
    private static void demonstrateColdBehavior() throws InterruptedException {
        // TODO
    }

    /**
     * TODO: convierte el Flux en "caliente" y demuestra que el panel que llega tarde
     * NO recibe la secuencia completa, sino que se une a la emisión en curso.
     * Opciones: .publish().autoConnect(1), .share(), o un ConnectableFlux con
     * .connect() antes de que llegue el segundo panel.
     *
     * Prueba también .publish().autoConnect(2) y explica por qué, con esa variante,
     * los dos paneles reciben la secuencia completa y el resultado se parece a Cold.
     */
    private static void demonstrateHotBehavior() throws InterruptedException {
        // TODO
    }
}
