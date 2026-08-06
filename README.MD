# Taller sobre Operadores de Programación Reactiva

## Objetivo

El objetivo de este taller es aprender a utilizar al menos cinco operadores de programación reactiva utilizando RxJava o Reactor. Implementaremos ejemplos prácticos que muestran cómo estos operadores transforman y manejan flujos de datos de manera reactiva. Además, los estudiantes crearán su propio ejemplo para aplicar lo aprendido.

## Pre-requisitos

- Conocimientos básicos de Java.
- Familiaridad con conceptos de programación reactiva.
- Tener configurado un entorno de desarrollo con RxJava o Reactor.

## Paso 1: Configuración del Proyecto

### Configuración del `pom.xml` para RxJava

Asegúrate de tener las siguientes dependencias en tu archivo `pom.xml`:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>reactive-operators</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <!-- RxJava -->
        <dependency>
            <groupId>io.reactivex.rxjava2</groupId>
            <artifactId>rxjava</artifactId>
            <version>2.2.21</version>
        </dependency>

        <!-- Reactor Core -->
        <dependency>
            <groupId>io.projectreactor</groupId>
            <artifactId>reactor-core</artifactId>
            <version>3.4.11</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

> **Nota:** el `pom.xml` de este repositorio ya viene configurado (no necesitas crearlo desde cero). Además de RxJava y Reactor, incluye Spring Boot y `spring-boot-starter-webflux`, usados en el ejemplo adicional de la sección "Bono: API reactiva con Spring WebFlux" al final de este documento.

## Paso 2: Ejemplos de Operadores en RxJava

### 1. `map`

Transforma los elementos emitidos aplicando una función.

```java
import io.reactivex.Observable;

public class MapOperatorExample {
    public static void main(String[] args) {
        Observable.just(1, 2, 3, 4, 5)
                .map(item -> item * 2)
                .subscribe(System.out::println);
    }
}
```

### 2. `filter`

Filtra los elementos emitidos en función de una condición.

```java
import io.reactivex.Observable;

public class FilterOperatorExample {
    public static void main(String[] args) {
        Observable.just(1, 2, 3, 4, 5)
                .filter(item -> item % 2 == 0)
                .subscribe(System.out::println);
    }
}
```

### 3. `flatMap`

Transforma cada elemento emitido en un `Observable` y aplana estos `Observables` en una secuencia única.

```java
import io.reactivex.Observable;

public class FlatMapOperatorExample {
    public static void main(String[] args) {
        Observable.just(1, 2, 3)
                .flatMap(item -> Observable.just(item * 10, item * 20))
                .subscribe(System.out::println);
    }
}
```

### 4. `merge`

Combina múltiples `Observables` emitiendo los elementos en el orden en que se producen.

```java
import io.reactivex.Observable;

public class MergeOperatorExample {
    public static void main(String[] args) {
        Observable<Integer> observable1 = Observable.just(1, 3, 5);
        Observable<Integer> observable2 = Observable.just(2, 4, 6);

        Observable.merge(observable1, observable2)
                .subscribe(System.out::println);
    }
}
```

### 5. `zip`

Combina los elementos de múltiples `Observables` emitiéndolos en pares.

```java
import io.reactivex.Observable;

public class ZipOperatorExample {
    public static void main(String[] args) {
        Observable<Integer> observable1 = Observable.just(1, 2, 3);
        Observable<String> observable2 = Observable.just("A", "B", "C");

        Observable.zip(observable1, observable2, (num, letter) -> num + letter)
                .subscribe(System.out::println);
    }
}
```

## Paso 3: Ejemplos de Operadores en Reactor

### 1. `map`

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

### 2. `filter`

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

### 3. `flatMap`

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

### 4. `merge`

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

### 5. `zip`

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

## Paso 4: Crear un Ejemplo Propio

**Instrucciones para los Estudiantes:**

1. **Definir un Caso de Uso:**
   - Piensa en un escenario donde la programación reactiva pueda ser útil. Por ejemplo, procesamiento de datos, integración con APIs externas, manipulación de streams de datos en tiempo real, etc.
2. **Seleccionar Operadores:**
   - Elige al menos cinco operadores que consideres adecuados para tu caso de uso. Puedes usar los operadores que hemos visto (`map`, `filter`, `flatMap`, `merge`, `zip`) u otros operadores disponibles en RxJava o Reactor.
3. **Implementar el Ejemplo:**
   - Implementa tu ejemplo en Java usando RxJava o Reactor.
   - Asegúrate de comentar tu código para explicar qué hace cada parte y cómo los operadores están siendo utilizados.
4. **Probar y Depurar:**
   - Ejecuta tu código y verifica que funcione como esperas. Asegúrate de manejar cualquier posible error.
5. **Compartir y Discutir:**
   - Comparte tu código con los demás estudiantes del taller. Discutan sobre las diferentes implementaciones y enfoques utilizados.

**Ejemplo de Código para los Estudiantes:**

Aquí tienes un ejemplo sencillo para inspirarte. Supongamos que queremos procesar una lista de pedidos y calcular el total de ventas para productos específicos.

```java
import io.reactivex.Observable;

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

## Paso 5: Resumen y Conclusión

### Resumen del Taller

- Aprendimos a utilizar cinco operadores fundamentales en programación reactiva: `map`, `filter`, `flatMap`, `merge`, y `zip`.
- Implementamos ejemplos prácticos en RxJava y Reactor para entender cómo transformar y manejar flujos de datos de manera reactiva.
- Los estudiantes crearon sus propios ejemplos para aplicar lo aprendido.

### Conclusión

La programación reactiva es una poderosa herramienta para manejar flujos de datos asíncronos y event-driven. Los operadores que hemos explorado son solo una muestra de las capacidades de RxJava y Reactor. Continuar explorando y practicando con estos y otros operadores te permitirá construir aplicaciones reactivas más robustas y eficientes.

## Ejecución del Taller

1. Configura tu entorno de desarrollo con las dependencias necesarias (o simplemente clona este repositorio, que ya trae el `pom.xml` configurado).
2. Ejecuta los ejemplos consolidados de operadores incluidos en `src/main/java/com/example/`:

    ```bash
    mvn compile
    mvn exec:java -Dexec.mainClass=com.example.RxJavaExample
    mvn exec:java -Dexec.mainClass=com.example.ReactorExample
    ```

3. Crea y comparte tu propio ejemplo utilizando los operadores aprendidos (ver "Paso 4: Crear un Ejemplo Propio").
4. Experimenta con otros operadores y casos de uso para profundizar en tu comprensión de la programación reactiva.

Este taller proporciona una base sólida para comenzar a trabajar con operadores en programación reactiva y fomenta la creación de ejemplos propios para reforzar el aprendizaje.

## Bono: API reactiva con Spring WebFlux

Además de los ejemplos de consola, este repositorio incluye una pequeña aplicación Spring Boot (`ReactiveSpringExampleApplication`) con un controlador reactivo (`ReactiveController`) que expone tres endpoints usando `Mono` y `Flux` directamente sobre HTTP — la forma en que la programación reactiva se usa típicamente en un servicio web real.

Para levantarla:

```bash
mvn spring-boot:run
```

Y en otra terminal, prueba los endpoints:

```bash
curl http://localhost:8080/reactive/hello   # "Hello, Reactive!"
curl http://localhost:8080/reactive/mono    # "Hello, Mono!"  (Mono<String>)
curl http://localhost:8080/reactive/flux    # "Hello, Flux!"  (Flux<String>)
```

Como reto adicional, intenta agregar un nuevo endpoint que use `Flux` para emitir varios elementos (por ejemplo, una lista) y aplícale alguno de los operadores vistos en este taller (`map`, `filter`, etc.) antes de devolverlo.
