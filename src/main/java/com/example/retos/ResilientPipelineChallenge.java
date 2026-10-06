package com.example.retos;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Prueba guía de maestría, Estación 3: manejo de errores y resiliencia.
 *
 * Escenario: un servicio externo (simulado abajo) falla ~10% de las veces.
 *
 * Ejecuta con:
 *   mvn compile exec:java -Dexec.mainClass=com.example.retos.ResilientPipelineChallenge
 */
public class ResilientPipelineChallenge {

    public static void main(String[] args) {
        // TODO: usa FlakyExternalService.call() y encadena estos tres operadores,
        // en el orden que decidas y puedas justificar:
        //  - onErrorResume(...) con un valor de respaldo,
        //  - retryWhen(Retry.backoff(...)) con backoff exponencial,
        //  - timeout(Duration...).
        // Imprime el resultado final (o el valor de respaldo) al suscribirte.
        //
        // Para comprobar que tu pipeline hace lo que crees:
        //  - Cuenta los intentos (por ejemplo con un AtomicInteger dentro de Mono.defer(...)).
        //  - Prueba también con un servicio que falle SIEMPRE y con uno que tarde más
        //    que el timeout (por ejemplo Mono.just("lento").delayElement(Duration.ofSeconds(2))).
        //  - Imprime la clase del error que llega a onErrorResume.
        //  - Cambia el orden de los operadores y compara el número de intentos.
        // Usa block() solo en este main de prueba; en un servicio real no se bloquea.
    }

    /**
     * Servicio externo simulado: falla ~10% de las veces. Ya está dado.
     * Es perezoso (Mono.fromCallable): el código se ejecuta de nuevo en cada
     * suscripción, que es lo que permite reintentar.
     */
    static class FlakyExternalService {
        static Mono<String> call() {
            return Mono.fromCallable(() -> {
                if (ThreadLocalRandom.current().nextInt(10) == 0) {
                    throw new RuntimeException("Servicio externo no disponible");
                }
                return "resultado-ok";
            });
        }
    }
}
