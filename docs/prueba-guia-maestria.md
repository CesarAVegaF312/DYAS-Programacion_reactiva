# Prueba guía de maestría en programación reactiva (RxJava y Reactor)

## Objetivo

El taller base trabaja el uso de `map`, `filter`, `flatMap`, `merge` y `zip`. Esta prueba va un paso más allá: evalúa si entiendes **por qué y cuándo** usar cada mecanismo reactivo, y si puedes razonar sobre el comportamiento de un pipeline bajo carga, ante fallos y en el tiempo. Son las preguntas que hay que responder antes de poner un sistema reactivo en producción.

## Prerrequisitos

- Haber completado el taller base (`RxJavaExample`, `ReactorExample`, `ReactiveController`).
- JDK 21 y Maven 3.9.
- El `pom.xml` del repositorio ya incluye RxJava 3, Reactor, `reactor-test` (para `StepVerifier`) y JUnit 5.

## Formato

Son 5 estaciones independientes. Cada una se entrega con **código funcional y una justificación escrita** (un párrafo por pregunta). No basta con que compile y "funcione": se evalúa el razonamiento detrás de cada decisión. Tiempo sugerido: 3 horas.

Cada estación tiene un archivo de partida con comentarios `TODO`. Los programas de consola se ejecutan así (las comillas son necesarias en PowerShell):

```sh
mvn compile exec:java "-Dexec.mainClass=com.example.retos.BackpressureChallenge"
```

Y las pruebas con:

```sh
mvn test
```

---

### Estación 1. Backpressure

**Escenario:** un sensor de una planta industrial produce del orden de 100.000 lecturas por segundo y la base de datos solo alcanza a guardar unas 1.000 por segundo. En el archivo de partida el consumidor lento se simula con `Thread.sleep(1)` por lectura. En Windows esa pausa suele durar cerca de 2 ms, así que el consumidor puede ir aún más lento; mide la tasa real en tu máquina.

**Antes de empezar:** en ninguna de las partes el programa falla de forma visible por sí solo, salvo en la parte C sin estrategia. Para entender lo que pasa hay que **medir**: lleva contadores de lecturas emitidas y consumidas (por ejemplo con `AtomicLong`) e imprime, al menos una vez por segundo, las pendientes (emitidas − consumidas) y el heap usado (el método `usedHeapMb()` ya está dado).

**Requisitos:**

- **Parte A. Observable sin backpressure.** Usa `Observable.range(1, 10_000)`, `.observeOn(Schedulers.computation())` y el consumidor lento.
  - Reporta cuánto tardó el productor en terminar y cuántas lecturas estaban pendientes en ese momento (un `doOnComplete` antes del `observeOn` sirve para medirlo).
  - Con `Integer` el heap casi no cambia. Repite la medición emitiendo un objeto más pesado (por ejemplo `new byte[1024]` por lectura) y compara el heap antes y después.
  - ¿Dónde quedan las lecturas que el consumidor todavía no procesa? ¿Qué pasaría si la fuente nunca terminara?
- **Parte B. Flowable con una fuente que se puede frenar.** Repite la parte A con `Flowable.range(1, 10_000)` y `observeOn`, **sin** ningún operador `onBackpressureXxx`.
  - ¿Cuántas lecturas llegan a estar pendientes como máximo? ¿Cuánto tarda ahora el productor en terminar?
  - Explica el resultado en términos de `request(n)`: `Flowable` ya aplica backpressure sin que agregues nada, porque `range` solo emite lo que le piden.
  - Ten en cuenta que `range` no tiene una tasa de emisión: emite todo de inmediato apenas se lo piden. Por eso no representa al sensor del escenario.
- **Parte C. Sensor temporizado que no se puede frenar.** Modela el sensor con `Flowable.interval(10, TimeUnit.MICROSECONDS)` (del orden de 100.000 lecturas por segundo), con `observeOn` y el mismo consumidor lento. Déjalo correr unos segundos y luego cancela la suscripción con `dispose()`.
  1. Sin estrategia: ¿qué señal recibe el suscriptor y después de cuánto tiempo? Imprime el error en el segundo argumento de `subscribe`.
  2. Con `onBackpressureDrop`, `onBackpressureLatest` y `onBackpressureBuffer` acotado (por ejemplo `onBackpressureBuffer(10_000, () -> {...}, BackpressureOverflowStrategy.DROP_OLDEST)`): cuenta consumidas y descartadas durante el mismo intervalo de tiempo.
  - Si aplicas `onBackpressureDrop` sobre un `range`, se descarta casi todo, porque `range` entrega todo en el mismo instante. Por eso esta parte usa una fuente temporizada.
  - Opcional: el equivalente en Reactor (`Flux.interval`, `onBackpressureBuffer/Drop/Latest`, `limitRate`).
- **Justificación:**
  - ¿Por qué elegiste esa estrategia y no las otras, dado que perder una lectura de temperatura de una planta industrial tiene consecuencias distintas a perder un "me gusta" en una red social?
  - Si no se puede perder ninguna lectura y la fuente no se puede frenar, ¿qué alternativas tienes fuera del pipeline?

**Archivo de partida:** [`src/main/java/com/example/retos/BackpressureChallenge.java`](../src/main/java/com/example/retos/BackpressureChallenge.java)

---

### Estación 2. Pruebas reactivas con StepVerifier y tiempo virtual

**Escenario:** un pipeline reactivo sin pruebas automatizadas no es confiable, y esas pruebas no pueden tardar los mismos segundos que espera el código de producción.

**Requisitos:**

- Escribir un `StepVerifier` que valide la secuencia exacta emitida por un pipeline equivalente al de `ReactorExample`.
  - Si incluyes el `flatMap` con `Mono.just(item).subscribeOn(Schedulers.parallel())`, el orden de llegada **no está garantizado** y una prueba con `expectNext` en orden estricto falla de forma intermitente. Usa `concatMap` o `flatMapSequential`, o deja el pipeline sin ese `flatMap` asíncrono, y explica por qué.
- Usar `StepVerifier.withVirtualTime(...)` para probar un `Flux` con `delayElements`, de forma que la prueba corra en milisegundos reales aunque el pipeline "espere" varios segundos virtuales.
- Las dos pruebas del archivo de partida vienen marcadas con `@Disabled("Estación 2: implementar")`, para que `mvn verify` no las muestre en verde mientras están vacías (Maven las reporta como `Skipped`). Quita el `@Disabled` de cada prueba al implementarla; en la entrega el reporte debe mostrar `Skipped: 0`.
- **Justificación:** ¿qué garantiza (y qué no garantiza) un `StepVerifier` que un `assertEquals` tradicional no puede?

**Archivo de partida:** [`src/test/java/com/example/retos/StepVerifierChallengeTest.java`](../src/test/java/com/example/retos/StepVerifierChallengeTest.java)

---

### Estación 3. Manejo de errores y resiliencia

**Escenario:** un servicio externo (simulado en el archivo de partida) falla de forma intermitente cerca del 10% de las veces.

**Requisitos:**

- Implementar `onErrorResume` (con un valor de respaldo), `retryWhen` con backoff exponencial (`Retry.backoff(...)`) y `timeout(Duration)`.
- Comprobar con código, contando los intentos (por ejemplo con un `AtomicInteger` dentro de `Mono.defer(...)`), cada uno de estos puntos:
  1. **El orden de los operadores importa.** Prueba `onErrorResume` antes y después de `retryWhen`. ¿En cuál de los dos órdenes se ejecuta realmente el reintento?
  2. **El error que llega al respaldo.** Con un servicio que falle siempre, imprime la clase del error que recibe `onErrorResume` cuando se agotan los reintentos. Reactor lo envuelve en un `RetryExhaustedException` (la excepción original queda en `getCause()`; `Exceptions.isRetryExhausted(e)` permite reconocerlo). ¿Qué pasa si tu `onErrorResume` filtra por el tipo de la excepción original?
  3. **Dónde va el `timeout`.** Con un servicio que tarde más que el límite (por ejemplo `Mono.just("lento").delayElement(Duration.ofSeconds(2))`), compara `timeout` antes de `retryWhen` (límite por intento) con `timeout` después de `retryWhen` (presupuesto total para todos los intentos y esperas). ¿Cuál necesitas en este escenario y por qué?
  4. **El servicio debe ser perezoso.** `FlakyExternalService.call()` usa `Mono.fromCallable`, así que cada reintento vuelve a ejecutar la llamada. Explica qué pasaría con un `Mono.just(resultadoYaCalculado)` o si la llamada real se hace antes de construir el `Mono`.
- **Justificación:** explica la diferencia entre `retry(n)` y `retryWhen(Retry.backoff(...))`, y describe un caso **concreto** en el que reintentar automáticamente sería peligroso.

**Archivo de partida:** [`src/main/java/com/example/retos/ResilientPipelineChallenge.java`](../src/main/java/com/example/retos/ResilientPipelineChallenge.java) (el servicio simulado `FlakyExternalService` ya está implementado)

---

### Estación 4. Hot y Cold, subscribeOn y publishOn

**Escenario:** dos paneles de monitoreo se suscriben al mismo stream de eventos en momentos distintos.

**Sobre los nombres de los operadores:** en RxJava el operador que cambia el hilo de las etapas siguientes es `observeOn`; en Reactor es `publishOn`. `Flux` no tiene `observeOn` (si lo escribes, `javac` responde `cannot find symbol`). En los dos el operador `subscribeOn` se llama igual.

**Requisitos:**

- Demostrar con código que un `Flux` Cold repite la secuencia completa para cada nuevo suscriptor.
- Demostrar que un `Flux` Hot comparte la misma emisión: el panel que llega tarde **no** recibe los primeros elementos. Usa `.publish().autoConnect(1)`, `.share()` o un `ConnectableFlux` con `.connect()` antes de que llegue el segundo panel.
  - Usa una fuente que tarde en emitir (por ejemplo `Flux.interval(Duration.ofMillis(100)).take(5)`) y una pausa entre las dos suscripciones; con `Flux.range` todo se emite de inmediato y no se ve la diferencia.
  - Prueba también `.publish().autoConnect(2)` y explica por qué, con esa variante, los dos paneles reciben la secuencia completa aunque lleguen en momentos distintos, y el resultado se parece a Cold.
- **Hilos en `ReactorExample`.** El ejemplo imprime el nombre del hilo en `doOnNext` y en el suscriptor. Ejecútalo varias veces y explica lo que observas:
  1. La cadena externa (`range` → `filter` → `map` → `flatMap` → `doOnNext`) tiene un solo `subscribeOn(Schedulers.boundedElastic())`. ¿En qué hilo corren `range`, `filter` y `map`? Agrega impresiones del hilo para comprobarlo.
  2. El `subscribeOn(Schedulers.parallel())` no está en la cadena externa: está aplicado al `Mono` interno que crea el `flatMap`, que es otra suscripción. ¿Por qué los elementos llegan a `doOnNext` y al suscriptor en hilos `parallel-N` (y alguna vez en `boundedElastic-N`)?
  3. ¿Qué pasa si pones dos `subscribeOn` en la misma cadena? Compruébalo y explica cuál de los dos determina el hilo en el que arranca la suscripción.
  4. Agrega un `publishOn(...)` en algún punto del pipeline y muestra qué etapas cambian de hilo. Compáralo con el `observeOn(Schedulers.newThread())` de `RxJavaExample`.
- **Justificación:** con base en la salida que obtuviste, explica la diferencia entre `subscribeOn` y `publishOn`/`observeOn`, y en qué caso usarías cada uno (por ejemplo, una llamada bloqueante a una base de datos frente a un cálculo intensivo).

**Archivo de partida:** [`src/main/java/com/example/retos/HotColdChallenge.java`](../src/main/java/com/example/retos/HotColdChallenge.java)

---

### Estación 5. Streaming con WebFlux (Server-Sent Events)

**Escenario:** esta estación continúa el reto del bono del taller base (un endpoint con un `Flux` de varios elementos) y lo lleva a un caso de streaming.

**Requisitos:**

- Implementar `GET /reactive/stream` para que devuelva `text/event-stream` (`produces = MediaType.TEXT_EVENT_STREAM_VALUE`), emitiendo indefinidamente a partir de `Flux.interval(Duration.ofSeconds(1))` y aplicando al menos `map` y `filter`. Necesitarás `import java.time.Duration;`. Mientras no lo implementes, el endpoint responde con error (`UnsupportedOperationException`).
- Levantar la aplicación (`mvn spring-boot:run`) y probarlo con `curl -N http://localhost:8080/reactive/stream` (en PowerShell escribe `curl.exe`). Confirma que los eventos llegan progresivamente, cada uno como una línea `data:...` seguida de una línea en blanco.
- **Justificación:**
  - Un `Flux<String>` sin `produces` también se envía por partes a medida que se emite. Compruébalo con un endpoint adicional que devuelva, por ejemplo, `Flux.interval(Duration.ofSeconds(1)).take(3).map(...)` y `curl -N`: llega como `text/plain`, con los elementos uno detrás de otro y sin separador. Entonces, ¿qué agrega realmente `text/event-stream`? Piensa en el formato de cada evento, en cómo sabe el cliente dónde termina uno y empieza el siguiente, y en `EventSource` del navegador.
  - Con una fuente infinita, ¿cuándo termina la respuesta? ¿Qué le pasa al `Flux.interval` del servidor cuando el cliente cierra la conexión?

**Archivo a modificar:** [`src/main/java/com/example/Controllers/ReactiveController.java`](../src/main/java/com/example/Controllers/ReactiveController.java) (método `getStream()`)

---

## Qué entregar

- El código de las 5 estaciones en el repositorio, compilando con `mvn verify` y sin pruebas omitidas (`Skipped: 0`).
- En el Wiki del repositorio, por cada estación: la salida de consola o de `curl` que usaste como evidencia y la justificación escrita.

## Rúbrica

| Criterio | Peso |
|---|---|
| Correctitud funcional de las 5 estaciones | 40% |
| Elección justificada de la estrategia (backpressure, reintentos, hot/cold, hilos), apoyada en lo que mediste | 30% |
| Calidad de las pruebas (`StepVerifier`, tiempo virtual, sin pruebas intermitentes) | 20% |
| Claridad de la justificación escrita | 10% |
