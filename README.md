# Dataflow Kotlin SDK

Idiomatic Kotlin layer over the [Java SDK](../sdk-java): `trace { }` /
`span { }` scoping functions with automatic error recording. The gRPC
pipeline, encryption and metadata conventions come from the Java core.

## Quick start

```kotlin
import dev.huginnlabs.dataflow.kotlin.*

fun main() {
    configure()                          // reads DATAFLOW_* env

    trace("market.Checkout") { root ->
        root.data("cart", cartId)        // AES-256-GCM on the wire
        trace("cart.Total") { it.data("items", 3) }
    }
}
```

Span members (`data`, `attr`, `status`, `callee`, `recordError`) are used
directly — the Java `Span` API carries over one to one.

## Coroutines

Span scoping follows the calling thread. Across `Dispatchers` hops pass
the `Span` explicitly (or re-open nested spans inside the coroutine) —
the core keeps the trace context in a `ThreadLocal` by design.

## Maven

```xml
<dependency>
  <groupId>dev.huginnlabs.dataflow</groupId>
  <artifactId>dataflow-sdk-kotlin</artifactId>
  <version>0.2.0</version>
</dependency>
```

Build from the repo root (installs the Java core first):

```
mvn -f sdk-java/pom.xml install -DskipTests
mvn -f sdk-kotlin/pom.xml install -DskipTests
```

Live example: `example-kotlin/` (marketplace flow, Kotlin DSL end to end).
