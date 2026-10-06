package com.example.retos;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Prueba guía de maestría, Estación 1: backpressure.
 *
 * Escenario: un sensor de una planta industrial emite lecturas mucho más rápido
 * de lo que la base de datos alcanza a guardarlas. El consumidor lento se simula
 * con Thread.sleep(1) por elemento (como máximo unas 1.000 lecturas por segundo).
 *
 * Ejecuta con:
 *   mvn compile exec:java -Dexec.mainClass=com.example.retos.BackpressureChallenge
 *
 * Importante: en ninguna de las partes el programa "revienta" por sí solo de forma
 * visible. Para ver lo que pasa tienes que medir: lleva contadores de lecturas
 * emitidas y consumidas (por ejemplo con AtomicLong) e imprime cada segundo las
 * pendientes (emitidas - consumidas) y el heap usado (ver usedHeapMb()).
 */
public class BackpressureChallenge {

    public static void main(String[] args) throws InterruptedException {
        observableWithoutBackpressure();
        // TODO: descomenta cada parte cuando la implementes.
        // flowableFromRange();
        // flowableFromTimedSensor();
    }

    /**
     * Parte A. TODO: usa Observable.range(1, 10_000), .observeOn(Schedulers.computation())
     * y un consumidor lento (Thread.sleep(1) por elemento).
     *  - Cuenta emitidas y consumidas, e imprime cuántas había pendientes en el momento
     *    en que el productor terminó (doOnComplete antes de observeOn) y cada segundo.
     *  - Observable no tiene backpressure: ¿dónde quedan las lecturas que el consumidor
     *    todavía no procesa?
     *  - Con Integer el heap casi no cambia. Para verlo crecer, emite un objeto más pesado
     *    (por ejemplo new byte[1024] por lectura) y compara usedHeapMb() antes y después.
     */
    private static void observableWithoutBackpressure() throws InterruptedException {
        // TODO
    }

    /**
     * Parte B. TODO: repite la parte A con Flowable.range(1, 10_000) + observeOn, SIN
     * ningún operador onBackpressureXxx. Mide de nuevo las pendientes.
     *  - ¿Cuántas lecturas llegan a estar pendientes como máximo? ¿Por qué?
     *  - ¿Por qué con Flowable.range el productor se frena solo y con Observable.range no?
     */
    private static void flowableFromRange() throws InterruptedException {
        // TODO
    }

    /**
     * Parte C. TODO: modela el sensor como una fuente temporizada que no se puede frenar:
     *   Flowable.interval(10, TimeUnit.MICROSECONDS)   // del orden de 100.000 lecturas/s
     * con observeOn y el mismo consumidor lento. Déjalo correr unos segundos
     * (Thread.sleep en main) y luego cancela la suscripción (dispose()).
     *  1. Sin estrategia: ¿qué señal recibe el suscriptor y después de cuánto tiempo?
     *     (imprime el error en el segundo argumento de subscribe).
     *  2. Con onBackpressureDrop, onBackpressureLatest y onBackpressureBuffer acotado
     *     (por ejemplo onBackpressureBuffer(10_000, () -> {...}, BackpressureOverflowStrategy.DROP_OLDEST)):
     *     cuenta consumidas y descartadas en el mismo intervalo de tiempo.
     *  Deja un comentario justificando la estrategia que elegirías para el sensor.
     */
    private static void flowableFromTimedSensor() throws InterruptedException {
        // TODO
    }

    /** Consumidor lento: simula una escritura en base de datos. Ya está dado. */
    static void slowDatabaseWrite() {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Heap usado en MB. Ya está dado. */
    static long usedHeapMb() {
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    }
}
