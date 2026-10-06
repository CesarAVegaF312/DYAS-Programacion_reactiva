package com.example;

import java.util.concurrent.CountDownLatch;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Ejemplo consolidado de operadores con RxJava 3.
 */
public class RxJavaExample
{
    public static void main( String[] args ) throws InterruptedException
    {
        // Permite que main() espere a que el flujo termine (onComplete u onError)
        CountDownLatch terminado = new CountDownLatch(1);

        // Creamos un Observable que emita una lista de números de 1 al 10
        Observable<Integer> observable = Observable.range(1, 10);

        // Aplicar operadores
        observable.filter(item -> item % 2 == 0)  // Filtrar los números pares
            .map(item -> "Number: " + item) // Mapear los números a un String
            .flatMap(item -> Observable.just(item).subscribeOn(Schedulers.computation()))  // Emitir en un hilo de computación
            .doOnNext(item -> System.out.println("Processing " + item + " on thread: " + Thread.currentThread().getName())) // Loggear el hilo de ejecución
            .observeOn(Schedulers.newThread()) // Cambiar de hilo para el suscriptor
            .subscribe(new Observer<String>(){
                @Override
                public void onSubscribe(Disposable d) {
                    System.out.println("Subscribed on thread: " + Thread.currentThread().getName());
                }

                @Override
                public void onNext(String item) {
                    System.out.println("Received " + item + " on thread: " + Thread.currentThread().getName());
                }

                @Override
                public void onError(Throwable e) {
                    System.out.println("Error: " + e.getMessage());
                    terminado.countDown();
                }

                @Override
                public void onComplete() {
                    System.out.println("Completed");
                    terminado.countDown();
                }

            }); // Suscribirse al Observable

        // Los hilos de RxJava son daemon: si main() termina antes, el programa se cierra sin ver los resultados
        terminado.await();
    }
}
