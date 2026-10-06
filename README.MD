# Taller de Operadores de Programación Reactiva (RxJava y Reactor)

Este taller tiene como objetivo aprender a **utilizar operadores de programación reactiva** con **RxJava** y **Reactor** para transformar, filtrar y combinar flujos de datos asíncronos.
Se implementan ejemplos prácticos de cinco operadores (`map`, `filter`, `flatMap`, `merge` y `zip`), se analizan dos ejemplos consolidados que muestran en qué hilos se ejecuta cada etapa y, al final, cada estudiante construye su propio ejemplo y un endpoint reactivo con **Spring WebFlux**.

---

## Objetivo General

Comprender y aplicar los **operadores fundamentales de la programación reactiva** en Java con **RxJava 3** y **Reactor**, identificando cómo se transforman y combinan los flujos de datos, en qué hilos se ejecuta cada etapa (`subscribeOn`, `observeOn`/`publishOn`) y cómo se expone un flujo reactivo sobre HTTP con **Spring WebFlux**.

---

## Índice

- [Conceptos clave](#conceptos-clave)
- [CONOCE EL TALLER](#conoce-el-taller)
  - [Estructura del Proyecto](#estructura-del-proyecto)
  - [Configuración de Dependencias](#configuración-de-dependencias)
  - [El pipeline de CI](#el-pipeline-de-ci)
- [PASO A PASO](#paso-a-paso)
  - [Paso 1: Clona y compila el proyecto](#paso-1-clona-y-compila-el-proyecto)
  - [Paso 2: Operadores en RxJava](#paso-2-operadores-en-rxjava)
  - [Paso 3: Operadores en Reactor](#paso-3-operadores-en-reactor)
  - [Paso 4: Ejecuta los ejemplos consolidados](#paso-4-ejecuta-los-ejemplos-consolidados)
  - [Paso 5: Crea un ejemplo propio](#paso-5-crea-un-ejemplo-propio)
  - [Paso 6 (bono): API reactiva con Spring WebFlux](#paso-6-bono-api-reactiva-con-spring-webflux)
  - [Retos opcionales](#retos-opcionales)
- [Prueba guía de maestría](#prueba-guía-de-maestría)
- [Buenas prácticas](#buenas-prácticas)
- [Para entregar](#para-entregar-con-este-taller)
- [Cómo usar esta guía para tu proyecto](#cómo-usar-esta-guía-para-tu-proyecto)
- [Resumen del Taller](#hagamos-un-resumen)
- [Conclusión](#conclusión)
- [Recursos recomendados](#recursos-recomendados)
- [Créditos y uso académico](#créditos-y-uso-académico)
- [Licencia](#licencia-de-uso)

---

## Conceptos clave

- **Programación reactiva**
Modelo de programación en el que el código **reacciona a los datos a medida que llegan**, en lugar de pedirlos y esperar. Un flujo emite elementos en el tiempo y una cadena de operadores los procesa sin bloquear el hilo.
Ejemplo: un servidor que envía cada lectura de un sensor al navegador en el momento en que se produce.

- **Publicador y suscriptor**
El publicador emite elementos y el suscriptor los recibe. En RxJava el publicador más usado es `Observable` (y `Flowable` cuando hay backpressure); en Reactor son `Flux` (0 a N elementos) y `Mono` (0 o 1 elemento). Nada se ejecuta hasta que alguien se suscribe con `subscribe(...)`.

- **Señales**
Un flujo puede emitir tres tipos de señales: `onNext` (un elemento), `onError` (un error, que termina el flujo) y `onComplete` (fin normal del flujo).

- **Operador**
Método que recibe un flujo y devuelve otro flujo transformado: `map`, `filter`, `flatMap`, `merge`, `zip`, entre muchos otros. Los operadores se encadenan para formar un pipeline.

- **Scheduler**
Grupo de hilos en el que se ejecuta el trabajo. `subscribeOn` indica en qué scheduler arranca la suscripción (y por tanto la fuente); `observeOn` en RxJava y `publishOn` en Reactor cambian el hilo de las etapas que vienen después.

- **Backpressure**
Mecanismo con el que un consumidor lento le indica al productor cuántos elementos puede recibir, para no acumular elementos sin límite. `Flowable` (RxJava) y `Flux` (Reactor) lo soportan; `Observable` no.

- **Publicadores Hot y Cold**
Un publicador **Cold** empieza de cero para cada suscriptor (cada uno recibe la secuencia completa). Uno **Hot** emite aunque nadie esté suscrito, y quien llega tarde solo recibe lo que se emita desde ese momento.

---

## CONOCE EL TALLER

El repositorio trae dos ejemplos de consola que combinan varios operadores, una pequeña aplicación **Spring WebFlux** y los archivos de partida de la [prueba guía de maestría](docs/prueba-guia-maestria.md).

### Estructura del Proyecto

```text
.
├─ .github/workflows/ci.yml                          # pipeline de CI: compila y ejecuta las pruebas en cada push
├─ pom.xml                                           # Spring Boot 4.1 (WebFlux) + RxJava 3 + Reactor + JUnit 5
├─ docs/prueba-guia-maestria.md                      # prueba guía de maestría (5 estaciones)
├─ src/main/java/com/example/
│   ├─ RxJavaExample.java                            # ejemplo consolidado con RxJava 3
│   ├─ ReactorExample.java                           # ejemplo consolidado con Reactor
│   ├─ ReactiveSpringExampleApplication.java         # aplicación Spring Boot (WebFlux)
│   ├─ Controllers/ReactiveController.java           # endpoints /reactive/hello, /mono, /flux y /stream
│   └─ retos/                                        # archivos de partida de la prueba guía de maestría
├─ src/test/java/com/example/
│   ├─ Controllers/ReactiveControllerTest.java       # pruebas del controlador con StepVerifier
│   └─ retos/StepVerifierChallengeTest.java          # archivo de partida de la estación 2 (pruebas deshabilitadas)
└─ README.MD
```

### Configuración de Dependencias

El [`pom.xml`](pom.xml) hereda de `spring-boot-starter-parent`, que fija las versiones compatibles de Reactor, RxJava 3, JUnit y los plugins de Maven. Por eso las dependencias no declaran versión.

| Pieza | Para qué |
|---|---|
| `spring-boot-starter-webflux` | Aplicación web reactiva (servidor Netty). Incluye `reactor-core` (`Flux`, `Mono`). |
| `io.reactivex.rxjava3:rxjava` | RxJava 3 (`Observable`, `Flowable`). RxJava 2 dejó de recibir soporte en 2021. |
| `spring-boot-starter-test` | JUnit 5 (Jupiter), AssertJ y Mockito para las pruebas. |
| `reactor-test` | `StepVerifier` y tiempo virtual para probar flujos de Reactor. |
| `exec-maven-plugin` | Ejecuta las clases con `main` (`mvn exec:java`). |

Estas son las partes principales del `pom.xml`:

```xml
<parent>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-parent</artifactId>
  <version>4.1.1</version>
  <relativePath/>
</parent>

<properties>
  <java.version>21</java.version>
  <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
</properties>

<dependencies>
  <!-- Spring WebFlux: API reactiva (Netty). Incluye reactor-core. -->
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webflux</artifactId>
  </dependency>

  <!-- RxJava 3 (versión gestionada por Spring Boot) -->
  <dependency>
    <groupId>io.reactivex.rxjava3</groupId>
    <artifactId>rxjava</artifactId>
  </dependency>

  <!-- Pruebas: JUnit 5 (Jupiter), AssertJ, Mockito -->
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-test</artifactId>
    <scope>test</scope>
  </dependency>

  <!-- Reactor Test: StepVerifier, tiempo virtual, PublisherProbe -->
  <dependency>
    <groupId>io.projectreactor</groupId>
    <artifactId>reactor-test</artifactId>
    <scope>test</scope>
  </dependency>
</dependencies>

<build>
  <plugins>
    <!-- mvn spring-boot:run -->
    <plugin>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-maven-plugin</artifactId>
    </plugin>

    <!-- mvn exec:java -Dexec.mainClass=... (ejemplos de consola y retos) -->
    <plugin>
      <groupId>org.codehaus.mojo</groupId>
      <artifactId>exec-maven-plugin</artifactId>
      <version>3.6.4</version>
      <configuration>
        <!-- Los schedulers de RxJava y Reactor dejan hilos vivos al terminar main().
             Sin esta opción, exec:java espera unos 15 s intentando detenerlos antes de salir. -->
        <cleanupDaemonThreads>false</cleanupDaemonThreads>
      </configuration>
    </plugin>
  </plugins>
</build>
```

Para ver las versiones exactas que resuelve Maven (por ejemplo, la de RxJava o la de Reactor), ejecuta `mvn dependency:tree`.

### El pipeline de CI

El archivo [`.github/workflows/ci.yml`](.github/workflows/ci.yml) es el mismo tipo de pipeline del taller de Integración Continua:

| Bloque | Qué hace |
|---|---|
| `on: push` / `pull_request` | Corre en cada push y en cada Pull Request hacia `main`. |
| `actions/setup-java@v6` + `cache: maven` | Instala Java 21 (Temurin) y guarda las dependencias de Maven entre ejecuciones. |
| `mvn -B verify` | Compila y ejecuta las pruebas. Si algo falla, el pipeline queda en rojo. |
| `actions/upload-artifact@v7` con `if: always()` | Publica los reportes de pruebas incluso si el build falló. |

---

## PASO A PASO

### Prerrequisitos

- Conocimientos básicos de Java (expresiones lambda y referencias a métodos).
- **JDK 21** y **Maven 3.9**.
- Git y un editor o IDE para Java.
- `curl` para probar los endpoints (en PowerShell escribe `curl.exe`, porque `curl` es un alias de otro comando).

### Paso 1: Clona y compila el proyecto

1. Haz **Fork** de este repositorio en tu cuenta de GitHub y clónalo en tu máquina.
2. Compila y ejecuta las pruebas:

   ```sh
   mvn verify
   ```

   Debes ver `Tests run: 5, Failures: 0, Errors: 0, Skipped: 2` y `BUILD SUCCESS`. Las dos pruebas omitidas son las de la estación 2 de la [prueba guía de maestría](docs/prueba-guia-maestria.md), que vienen deshabilitadas hasta que las implementes.

> El `pom.xml` ya está configurado; no necesitas agregar dependencias para los pasos 2 a 6.

### Paso 2: Operadores en RxJava

Cada ejemplo es una clase independiente. Para ejecutarlos, crea los archivos en `src/main/java/` (por ejemplo en un paquete `com.example.operadores.rxjava`, agregando la línea `package` correspondiente) y ejecútalos con `mvn compile exec:java "-Dexec.mainClass=..."` o desde tu IDE.

#### 1. `map`

Transforma los elementos emitidos aplicando una función.

```java
import io.reactivex.rxjava3.core.Observable;

public class MapOperatorExample {
    public static void main(String[] args) {
        Observable.just(1, 2, 3, 4, 5)
                .map(item -> item * 2)
                .subscribe(System.out::println);
    }
}
```

#### 2. `filter`

Filtra los elementos emitidos en función de una condición.

```java
import io.reactivex.rxjava3.core.Observable;

public class FilterOperatorExample {
    public static void main(String[] args) {
        Observable.just(1, 2, 3, 4, 5)
                .filter(item -> item % 2 == 0)
                .subscribe(System.out::println);
    }
}
```

#### 3. `flatMap`

Transforma cada elemento emitido en un `Observable` y aplana estos `Observables` en una secuencia única.

```java
import io.reactivex.rxjava3.core.Observable;

public class FlatMapOperatorExample {
    public static void main(String[] args) {
        Observable.just(1, 2, 3)
                .flatMap(item -> Observable.just(item * 10, item * 20))
                .subscribe(System.out::println);
    }
}
```

#### 4. `merge`

Combina múltiples `Observables` emitiendo los elementos en el orden en que se producen.

```java
import io.reactivex.rxjava3.core.Observable;

public class MergeOperatorExample {
    public static void main(String[] args) {
        Observable<Integer> observable1 = Observable.just(1, 3, 5);
        Observable<Integer> observable2 = Observable.just(2, 4, 6);

        Observable.merge(observable1, observable2)
                .subscribe(System.out::println);
    }
}
```

#### 5. `zip`

Combina los elementos de múltiples `Observables` emitiéndolos en pares.

```java
import io.reactivex.rxjava3.core.Observable;

public class ZipOperatorExample {
    public static void main(String[] args) {
        Observable<Integer> observable1 = Observable.just(1, 2, 3);
        Observable<String> observable2 = Observable.just("A", "B", "C");

        Observable.zip(observable1, observable2, (num, letter) -> num + letter)
                .subscribe(System.out::println);
    }
}
```

> Si encuentras ejemplos en internet con `import io.reactivex.Observable;`, son de RxJava 2. En RxJava 3 los tipos principales están en `io.reactivex.rxjava3.core` y los schedulers en `io.reactivex.rxjava3.schedulers`.

### Paso 3: Operadores en Reactor

#### 1. `map`

Transforma los elementos emitidos aplicando una función.

```java
import reactor.core.publisher.Flux;

public class MapOperatorExample {
    public static void main(String[] args) {
        Flux.just(1, 2, 3, 4, 5)
                .map(item -> item * 2)
                .subscribe(System.out::println);
    }
}
```

#### 2. `filter`

Filtra los elementos emitidos en función de una condición.

```java
import reactor.core.publisher.Flux;

public class FilterOperatorExample {
    public static void main(String[] args) {
        Flux.just(1, 2, 3, 4, 5)
                .filter(item -> item % 2 == 0)
                .subscribe(System.out::println);
    }
}
```

#### 3. `flatMap`

Transforma cada elemento emitido en un `Publisher` y aplana estos `Publishers` en una secuencia única.

```java
import reactor.core.publisher.Flux;

public class FlatMapOperatorExample {
    public static void main(String[] args) {
        Flux.just(1, 2, 3)
                .flatMap(item -> Flux.just(item * 10, item * 20))
                .subscribe(System.out::println);
    }
}
```

#### 4. `merge`

Combina múltiples `Publishers` emitiendo los elementos en el orden en que se producen.

```java
import reactor.core.publisher.Flux;

public class MergeOperatorExample {
    public static void main(String[] args) {
        Flux<Integer> flux1 = Flux.just(1, 3, 5);
        Flux<Integer> flux2 = Flux.just(2, 4, 6);

        Flux.merge(flux1, flux2)
                .subscribe(System.out::println);
    }
}
```

#### 5. `zip`

Combina los elementos de múltiples `Publishers` emitiéndolos en pares.

```java
import reactor.core.publisher.Flux;

public class ZipOperatorExample {
    public static void main(String[] args) {
        Flux<Integer> flux1 = Flux.just(1, 2, 3);
        Flux<String> flux2 = Flux.just("A", "B", "C");

        Flux.zip(flux1, flux2, (num, letter) -> num + letter)
                .subscribe(System.out::println);
    }
}
```

### Paso 4: Ejecuta los ejemplos consolidados

Los archivos [`RxJavaExample.java`](src/main/java/com/example/RxJavaExample.java) y [`ReactorExample.java`](src/main/java/com/example/ReactorExample.java) combinan `filter`, `map` y `flatMap` con cambios de hilo. Ejecútalos (las comillas son necesarias en PowerShell y funcionan igual en bash):

```sh
mvn compile
mvn exec:java "-Dexec.mainClass=com.example.RxJavaExample"
mvn exec:java "-Dexec.mainClass=com.example.ReactorExample"
```

Cada ejemplo imprime el elemento y el nombre del hilo en dos puntos del pipeline: en `doOnNext` (`Processing ...`) y en el suscriptor (`Received ...`). Una salida posible de `RxJavaExample` es:

```text
Subscribed on thread: com.example.RxJavaExample.main()
Processing Number: 6 on thread: RxComputationThreadPool-3
Processing Number: 2 on thread: RxComputationThreadPool-3
Processing Number: 4 on thread: RxComputationThreadPool-3
Received Number: 6 on thread: RxNewThreadScheduler-1
Processing Number: 8 on thread: RxComputationThreadPool-3
Received Number: 2 on thread: RxNewThreadScheduler-1
...
Completed
```

El orden de los elementos y los números de los hilos cambian de una ejecución a otra. Ejecuta cada ejemplo varias veces y responde:

1. ¿Por qué `flatMap` no conserva el orden de los elementos en `RxJavaExample`? ¿Qué operador usarías para conservarlo?
2. En `RxJavaExample`, ¿qué etapas corren en `RxComputationThreadPool-N` y cuáles en `RxNewThreadScheduler-N`? ¿Qué operador provoca el cambio?
3. En `ReactorExample`, ¿en qué hilos aparecen `Processing` y `Received`? ¿Por qué casi nunca aparece el hilo `boundedElastic` si el pipeline incluye `subscribeOn(Schedulers.boundedElastic())`?

> Los dos ejemplos esperan con un `CountDownLatch` a que el flujo termine. Los hilos de RxJava y Reactor son *daemon*: si `main()` terminara antes, el programa se cerraría sin mostrar los resultados.

### Paso 5: Crea un ejemplo propio

**Instrucciones:**

1. **Define un caso de uso.**
   Piensa en un escenario donde la programación reactiva sea útil: procesamiento de datos, integración con APIs externas, manejo de streams de datos en tiempo real, etc.
2. **Selecciona los operadores.**
   Elige al menos cinco operadores adecuados para tu caso de uso. Puedes usar los vistos (`map`, `filter`, `flatMap`, `merge`, `zip`) u otros de RxJava o Reactor (`reduce`, `scan`, `buffer`, `distinct`, `take`, `onErrorResume`...).
3. **Implementa el ejemplo.**
   Escríbelo en Java con RxJava o Reactor, en una clase propia dentro de `src/main/java/`. Comenta el código para explicar qué hace cada parte y cómo se usa cada operador.
4. **Prueba y depura.**
   Ejecútalo y verifica que funcione como esperas. Maneja los posibles errores (`onError`, `onErrorResume`, `onErrorReturn`).
5. **Comparte y discute.**
   Comparte tu código con los demás estudiantes del taller y discutan las diferentes implementaciones y enfoques.

**Ejemplo para inspirarte.** Se procesa una lista de pedidos para calcular el total de ventas de un producto.

```java
import io.reactivex.rxjava3.core.Observable;

import java.util.Arrays;
import java.util.List;

class Order {
    private final String product;
    private final int quantity;
    private final double price;

    public Order(String product, int quantity, double price) {
        this.product = product;
        this.quantity = quantity;
        this.price = price;
    }

    public String getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }
}

public class CustomExample {
    public static void main(String[] args) {
        List<Order> orders = Arrays.asList(
                new Order("Product A", 2, 50.0),
                new Order("Product B", 1, 30.0),
                new Order("Product A", 1, 50.0),
                new Order("Product C", 3, 20.0)
        );

        Observable.fromIterable(orders)
                .filter(order -> "Product A".equals(order.getProduct())) // Filtrar por Producto A
                .map(order -> order.getQuantity() * order.getPrice()) // Calcular el total por pedido
                .reduce(Double::sum) // Calcular el total de ventas para Producto A
                .subscribe(total -> System.out.println("Total sales for Product A: " + total));
    }
}
```

Salida esperada: `Total sales for Product A: 150.0`. Observa que `reduce` sin valor inicial devuelve un `Maybe` (puede no haber ningún pedido que sume), no un `Observable`.

### Paso 6 (bono): API reactiva con Spring WebFlux

Además de los ejemplos de consola, el repositorio incluye una aplicación Spring Boot (`ReactiveSpringExampleApplication`) con un controlador reactivo ([`ReactiveController`](src/main/java/com/example/Controllers/ReactiveController.java)) que expone endpoints que devuelven `Mono` y `Flux` directamente sobre HTTP. Así se usa la programación reactiva en un servicio web.

Para levantarla:

```sh
mvn spring-boot:run
```

En otra terminal, prueba los endpoints:

```sh
curl http://localhost:8080/reactive/hello   # "Hello, Reactive!"
curl http://localhost:8080/reactive/mono    # "Hello, Mono!"  (Mono<String>)
curl http://localhost:8080/reactive/flux    # "Hello, Flux!"  (Flux<String>)
```

El controlador tiene un cuarto endpoint, `/reactive/stream`, que todavía no está implementado: es la estación 5 de la [prueba guía de maestría](docs/prueba-guia-maestria.md).

**Reto del bono:** agrega un nuevo endpoint que use `Flux` para emitir varios elementos (por ejemplo, una lista de productos) y aplícale alguno de los operadores vistos en el taller (`map`, `filter`, etc.) antes de devolverlo. Escribe una prueba con `StepVerifier` para ese endpoint, siguiendo el ejemplo de [`ReactiveControllerTest`](src/test/java/com/example/Controllers/ReactiveControllerTest.java).

### Retos opcionales

1. **Orden con `concatMap`:** cambia el `flatMap` de `RxJavaExample` por `concatMap` y compara el orden y los hilos de la salida.
2. **Manejo de errores:** en tu ejemplo propio, lanza una excepción dentro de un `map` y compara `onErrorReturn`, `onErrorResume` y `retry(2)`.
3. **Combinar con tiempo:** usa `Flux.interval(Duration.ofMillis(300))` con `zip` para emitir los elementos de una lista uno cada 300 ms.
4. **Badge del pipeline:** agrega al README de tu fork la insignia del estado del pipeline
   (`https://github.com/<usuario>/<repo>/actions/workflows/ci.yml/badge.svg`).

---

## Prueba guía de maestría

La [prueba guía de maestría](docs/prueba-guia-maestria.md) profundiza en los mismos componentes con cinco estaciones: backpressure, pruebas reactivas con `StepVerifier` y tiempo virtual, manejo de errores y reintentos, publicadores Hot y Cold con el análisis de hilos, y streaming con Server-Sent Events. Los archivos de partida están en `src/main/java/com/example/retos/`, `src/test/java/com/example/retos/` y en el método `getStream()` de `ReactiveController`.

---

## Buenas prácticas

1. **Nada pasa sin suscripción:** construir un pipeline no ejecuta nada hasta que se llama a `subscribe(...)`.
2. **No bloquear dentro del pipeline:** evita `Thread.sleep`, `block()` o llamadas bloqueantes en los operadores; si una llamada bloqueante es inevitable, ejecútala en un scheduler para ese fin (`Schedulers.boundedElastic()` en Reactor, `Schedulers.io()` en RxJava).
3. **Manejar siempre los errores:** todo `subscribe` debería tener un manejador de error, o el pipeline un `onErrorResume`/`onErrorReturn`.
4. **Elegir el operador según el orden que se necesita:** `flatMap` procesa en paralelo y no conserva el orden; `concatMap` lo conserva procesando uno a la vez.
5. **Cuidar el consumidor lento:** usa `Flowable`/`Flux` y una estrategia de backpressure cuando el productor puede ir más rápido que el consumidor.
6. **Probar los flujos con `StepVerifier`:** valida elementos, orden, errores y finalización, y usa tiempo virtual para los flujos con retardos.
7. **Usar versiones con soporte:** RxJava 3 en lugar de RxJava 2, y las versiones de Reactor que define Spring Boot.

---

## PARA ENTREGAR CON ESTE TALLER

### 1) Repositorio

- **Fork** de este repositorio con **URL pública o acceso por invitación**.
- Archivo **`integrantes.txt`** o sección en el README con nombres y correos institucionales.
- **Rama principal ejecutable:** `mvn verify` en verde, sin configuraciones manuales adicionales.

### 2) Documentación en Wiki (obligatoria)

> Toda la documentación del taller se entrega en el **Wiki del repositorio**.
> No se requiere PDF; el Wiki es la entrega oficial.

Estructura mínima sugerida del Wiki:

- **Inicio:** integrantes y propósito del taller.
- **Operadores:** explicación, con sus palabras, de `map`, `filter`, `flatMap`, `merge` y `zip`, con la salida de los ejemplos en RxJava y en Reactor.
- **Ejemplos consolidados:** salida de `RxJavaExample` y `ReactorExample` y respuestas a las preguntas del paso 4.
- **Ejemplo propio:** caso de uso, operadores elegidos y salida.
- **API WebFlux:** el endpoint nuevo, su prueba y la salida de `curl`.
- **Reflexión final.**

### 3) Operadores en RxJava y Reactor

- Los cinco operadores del paso 2 ejecutados con RxJava y los cinco del paso 3 con Reactor.
- Captura o copia de la salida de cada uno.

### 4) Ejemplos consolidados

- Salida de `RxJavaExample` y `ReactorExample`, ejecutados al menos dos veces cada uno.
- Respuestas a las tres preguntas del paso 4, apoyadas en los nombres de hilos de la salida.

### 5) Ejemplo propio

- Clase con un caso de uso propio que use **al menos cinco operadores** y maneje errores.
- Código comentado y salida de la ejecución.

### 6) API reactiva con WebFlux

- Endpoint nuevo con `Flux` y al menos un operador.
- Prueba con `StepVerifier` del endpoint y salida de `curl`.

### 7) Reflexión final (en el Wiki)

- ¿Qué diferencia hay entre `map` y `flatMap`? ¿Cuándo usarías cada uno?
- ¿Qué diferencia hay entre `merge` y `zip` cuando uno de los flujos emite más elementos que el otro?
- ¿Qué hace `subscribeOn` y qué hace `observeOn`/`publishOn`, según lo que observaste en los ejemplos?
- ¿En qué parte del proyecto del curso tendría sentido un flujo reactivo y en cuál no?

### 8) Rúbrica – Taller de Operadores de Programación Reactiva

| **Criterios de evaluación** | **Indicadores de cumplimiento** | **Excelente (5 pts)** | **Bueno (4 pts)** | **Necesita mejorar (3.5 pts)** | **Deficiente (2.5 pts)** | **No cumple (0 pts)** |
|-----------------------------|----------------------------------|------------------------|-------------------|-------------------------------|--------------------------|------------------------|
| **Repositorio** | Fork organizado, con integrantes y rama principal ejecutable. | Repositorio ordenado, `mvn verify` en verde, integrantes registrados. | Ejecutable con detalles menores de organización. | Requiere ajustes para ejecutar. | Errores de compilación o estructura desordenada. | No entrega o el código no ejecuta. |
| **Operadores en RxJava y Reactor** | Los cinco operadores ejecutados en ambas librerías. | Diez ejemplos ejecutados, con salida y explicación de cada operador. | Diez ejemplos ejecutados con explicaciones superficiales. | Solo una de las dos librerías o ejemplos incompletos. | Ejemplos copiados sin ejecutar ni explicar. | No realiza el ejercicio. |
| **Ejemplos consolidados e hilos** | Análisis de la salida de `RxJavaExample` y `ReactorExample`. | Responde las tres preguntas del paso 4 apoyado en los nombres de hilos observados. | Respuestas correctas sin relacionarlas con la salida. | Respuestas incompletas o con errores conceptuales. | Solo adjunta la salida, sin análisis. | No realiza el ejercicio. |
| **Ejemplo propio** | Caso de uso con al menos cinco operadores y manejo de errores. | Caso de uso original, cinco o más operadores bien elegidos, errores manejados y código comentado. | Cumple con los operadores, pero el manejo de errores o los comentarios son débiles. | Menos de cinco operadores o caso de uso poco claro. | Ejemplo que no compila o copia del ejemplo de inspiración. | No presenta ejemplo. |
| **API reactiva con WebFlux** | Endpoint nuevo con `Flux`, operadores y prueba. | Endpoint funcional, con operadores, prueba con `StepVerifier` y salida de `curl`. | Endpoint funcional sin prueba. | Endpoint que devuelve datos fijos sin operadores. | Endpoint presente pero con errores. | No realiza el bono. |
| **Documentación en Wiki** | Secciones completas con evidencias. | Wiki completo, claro y con la salida de todos los ejemplos. | Wiki completo con leves omisiones. | Wiki incompleto o con poca claridad. | Wiki muy limitado o confuso. | No hay Wiki o está vacío. |
| **Reflexión técnica** | Análisis de resultados y aprendizajes. | Reflexión profunda, conectada con el proyecto del curso. | Reflexión correcta pero superficial. | Reflexión breve o poco argumentada. | Reflexión vaga o sin relación con el taller. | No presenta reflexión. |

> **Cómo suma**: 7 criterios × 5 pts = **35 puntos**.

| Rango de puntaje | Desempeño                                                |
| ---------------- | -------------------------------------------------------- |
| 31 – 35          | Excelente dominio técnico y metodológico.                |
| 25 – 30          | Buen trabajo con evidencias o documentación parcial.     |
| 21 – 24          | Cumple con lo básico pero sin profundidad.               |
| < 21             | No cumple con los criterios mínimos del taller.          |

---

## Propósito del taller

En este taller se trabajan los operadores con los que se construye cualquier pipeline reactivo y se observa, con los nombres de los hilos, cómo se reparte el trabajo entre schedulers. El mismo modelo se lleva después a un servicio web con Spring WebFlux.

A través de ejemplos pequeños con RxJava y Reactor se conectan los temas de asincronía y concurrencia del curso con una forma de programar en la que el código reacciona a los datos a medida que llegan, que es la base de los sistemas orientados a eventos y del streaming.

---

## Cómo usar esta guía para tu proyecto

1. **Identifica los flujos de datos** del proyecto: eventos, notificaciones, lecturas periódicas o llamadas a servicios externos que hoy se esperan de forma bloqueante.
2. **Elige la librería** según el stack: Reactor si el proyecto usa Spring (WebFlux, R2DBC, WebClient); RxJava si es una aplicación Java o Android sin Spring.
3. **Diseña el pipeline** con los operadores del taller y decide en qué scheduler se ejecuta cada parte.
4. **Maneja los errores y los tiempos de espera** (`onErrorResume`, `timeout`, `retryWhen`) antes de llamar a servicios externos.
5. **Prueba los flujos** con `StepVerifier` e intégralos al pipeline de CI del proyecto.

Checklist para el proyecto:

- [ ] Al menos un flujo del proyecto identificado como candidato a programación reactiva.
- [ ] Pipeline con operadores y manejo de errores.
- [ ] Pruebas con `StepVerifier` que validen elementos, orden y finalización.
- [ ] Las pruebas se ejecutan en el pipeline de CI.

---

> **Resultado esperado:**
> Al finalizar este taller, cada estudiante sabrá usar `map`, `filter`, `flatMap`, `merge` y `zip` en RxJava y en Reactor, interpretar en qué hilos se ejecuta un pipeline reactivo y exponer un flujo reactivo como endpoint HTTP con Spring WebFlux, con su respectiva prueba.

---

## Hagamos un resumen

### Programación reactiva

- **Qué es:** un modelo en el que el código reacciona a los datos a medida que llegan, mediante flujos y operadores.
- **Para qué sirve:** manejar flujos asíncronos y orientados a eventos sin bloquear hilos.
- **Ejemplo típico:** un endpoint que envía al cliente cada evento en cuanto ocurre.

### Operadores

- **`map` y `filter`:** transforman y seleccionan elementos uno a uno.
- **`flatMap`:** convierte cada elemento en un flujo y los aplana; no conserva el orden.
- **`merge` y `zip`:** combinan flujos, intercalando elementos (`merge`) o formando pares (`zip`).

### Schedulers e hilos

- **`subscribeOn`:** define en qué hilo arranca la suscripción y la fuente.
- **`observeOn` (RxJava) y `publishOn` (Reactor):** cambian el hilo de las etapas que vienen después.

### RxJava y Reactor

- **Mismo modelo, distinta API:** `Observable`/`Flowable` en RxJava; `Flux`/`Mono` en Reactor, que es la base de Spring WebFlux.

---

## Conclusión

En conjunto, estas prácticas permiten:

- Transformar, filtrar y combinar flujos de datos con **operadores** encadenados.
- Controlar en qué **hilos** se ejecuta cada parte de un pipeline.
- Exponer flujos reactivos sobre HTTP con **Spring WebFlux**.
- Verificar el comportamiento de los flujos con **pruebas automatizadas** (`StepVerifier`).

Los operadores vistos son una muestra de lo que ofrecen RxJava y Reactor. Seguir practicando con otros operadores y casos de uso permite construir aplicaciones reactivas más robustas y eficientes.

---

## Recursos recomendados

- *Reactive Programming with RxJava* – Tomasz Nurkiewicz & Ben Christensen
- *Hands-On Reactive Programming in Spring 5* – Oleh Dokuka & Igor Lozynskyi
- [ReactiveX – Introducción y catálogo de operadores](https://reactivex.io/intro.html)
- [RxJava – Wiki del proyecto](https://github.com/ReactiveX/RxJava/wiki)
- [Reactor – Reference Guide](https://projectreactor.io/docs/core/release/reference/)
- [Reactor – Which operator do I need?](https://projectreactor.io/docs/core/release/reference/apdx-operatorChoice.html)
- Spring Docs – [Spring WebFlux](https://docs.spring.io/spring-framework/reference/web/webflux.html)
- [Reactive Streams – Especificación](https://www.reactive-streams.org/)

---

## Créditos y uso académico

**Autor:** César Augusto Vega Fernández
**Curso:** Diseño y Arquitectura de Software
**Programa:** Ingeniería Informática – Universidad de La Sabana
**Año:** 2026

Este taller y su contenido fueron diseñados por el profesor **César Augusto Vega Fernández** como material académico para el curso *Diseño y Arquitectura de Software*, impartido en el programa de **Ingeniería Informática de la Universidad de La Sabana**.

Su propósito es exclusivamente educativo y está orientado a fortalecer las competencias de los estudiantes en **programación reactiva, asincronía y concurrencia** aplicadas al diseño de software.

---

### Licencia de uso

Este material se distribuye bajo la licencia [Creative Commons Atribución-NoComercial-CompartirIgual 4.0 Internacional (CC BY-NC-SA 4.0)](https://creativecommons.org/licenses/by-nc-sa/4.0/deed.es).

Puedes **usar, adaptar o compartir** este contenido con fines educativos, siempre que:

1. Se reconozca la autoría del profesor **César Augusto Vega Fernández**.
2. No se utilice con fines comerciales.
3. Las obras derivadas se distribuyan bajo la misma licencia.

---

© Universidad de La Sabana – Facultad de Ingeniería
Ingeniería Informática – 2026
