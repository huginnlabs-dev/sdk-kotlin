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

## Transports

### OkHttp

Add the interceptor to your client — one `HTTP_CLIENT` span per request
(name `"GET api.example.com/orders"`), callee = host, response status, and
`http.method` / `http.url` metadata; requests carry an
`X-Dataflow-Trace-Id` header so the receiving instrumented service
continues the same trace.

```kotlin
val client = OkHttpClient.Builder()
    .addInterceptor(okhttpInterceptor())
    .build()
```

Best-effort by contract: with the SDK disabled the interceptor is inert,
and instrumentation never breaks the request — transport errors are
recorded on the span, then rethrown unchanged.

### JDBC

Wrap connections to trace statements as `DB_QUERY` spans: name
`"<VERB> <table>"` derived from the SQL (`"SELECT orders"`, `"CREATE
migrations"` — `IF [NOT] EXISTS` skipped, `public.items` reported as
`items`), with `db.system` and `db.statement` metadata (single-spaced,
max 200 chars — parameter values are never captured).

```kotlin
configure()

dataSource.connection.use { raw ->
    wrapConnection(raw, "postgres").use { conn ->
        conn.prepareStatement("SELECT total FROM orders WHERE id = ?").use { st ->
            st.setLong(1, orderId)
            st.executeQuery().use { rs -> /* ... */ }
        }
    }
}
```

`wrapConnection` proxies `java.sql.Connection` (statement creation and
execution only — everything else passes through untouched); SQL exceptions
surface exactly as the driver raised them. Tracing is decided per query, so
connections may be wrapped before `configure()`.

Note: the pinned Java core (dataflow-sdk 0.2.0) predates the
`EVENT_TYPE_DB_QUERY` wire literal, so DB spans are emitted by this wrapper
with the type carried as a proto3 unknown field (wire number 6), which the
server parses back into `DB_QUERY`.

## Route scanning

`ScanCli` is a static route scanner for Kotlin sources: regex extraction
over source lines (no Kotlin compiler dependency) that posts declared HTTP
endpoints to the server catalog (POST `/api/v1/catalog`), where they are
correlated with observed traffic.

Frameworks: Ktor (`routing { get("/x") { } }`, all verbs, nested
`route("/base")` prefixes), Spring (`@GetMapping("/x")` & co plus
`@RequestMapping` as a class-level prefix or method-level with an explicit
method), and Micronaut (`@Controller("/base")` + `@Get/@Post/...`).
http4k and other DSLs are not scanned. Path parameters keep the `{id}`
syntax as written; handlers report as `Class.method`.

```bash
java -cp ... dev.huginnlabs.dataflow.kotlin.ScanCli \
    --dir . --service my-service \
    --url https://dataflow.example.com --api-key $DATAFLOW_API_KEY
```

Flags: `--dir` (source root, required), `--service` (or
`DATAFLOW_SERVICE_NAME`), `--url` (API base; falls back to
`DATAFLOW_HTTP_URL`, then a URL-form `DATAFLOW_ENDPOINT` — a bare
`host:port` gRPC endpoint is skipped with a message), `--api-key` (or
`DATAFLOW_API_KEY`), and `--print` (write the catalog JSON to stdout
instead of posting). At most 1000 routes are reported per service.

## Coroutines

Span scoping follows the calling thread. Across `Dispatchers` hops pass
the `Span` explicitly (or re-open nested spans inside the coroutine) —
the core keeps the trace context in a `ThreadLocal` by design.

## Maven

```xml
<dependency>
  <groupId>dev.huginnlabs.dataflow</groupId>
  <artifactId>dataflow-sdk-kotlin</artifactId>
  <version>0.4.0</version>
</dependency>
```

Build from the repo root (installs the Java core first):

```
mvn -f sdk-java/pom.xml install -DskipTests
mvn -f sdk-kotlin/pom.xml install -DskipTests
```

Live example: `example-kotlin/` (marketplace flow, Kotlin DSL end to end).
