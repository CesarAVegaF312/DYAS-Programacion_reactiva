package com.example;

import java.util.concurrent.CountDownLatch;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Ejemplo consolidado de operadores con Reactor.
 */
public class ReactorExample {

    public static void main(String[] args) throws InterruptedException {
        // Permite que main() espere a que el flujo termine (complete o error)
        CountDownLatch terminado = new CountDownLatch(1);

        // Creamos un Flux que emita una lista de números de 1 al 10
        Flux<Integer> flux = Flux.range(1, 10);

        // Aplicar operadores
        flux.filter(item -> item % 2 == 1) // Filtrar los números impares
            .map(item -> "Number: " + item) // Mapear los números a un String
            .flatMap(item -> Mono.just(item).subscribeOn(Schedulers.parallel())) // Emitir en un hilo de computación
            .doOnNext(item -> System.out.println("Processing " + item + " on thread: " + Thread.currentThread().getName())) // Loggear el hilo de ejecución
            .subscribeOn(Schedulers.boundedElastic()) // Ejecutar en un hilo elástico
            .doFinally(signal -> terminado.countDown()) // Avisar a main() que el flujo terminó
            .subscribe(
                item -> System.out.println("Received " + item + " on thread: " + Thread.currentThread().getName()), // Imprimir el item recibido
                error -> System.out.println("Error: " + error.getMessage()), // Imprimir el error
                () -> System.out.println("Completed") // Imprimir que se completó
            ); // Suscribirse al Flux

        // Los hilos de Reactor son daemon: si main() termina antes, el programa se cierra sin ver los resultados
        terminado.await();
    }

}
