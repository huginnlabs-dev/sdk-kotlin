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

## Ktor middleware

`ktorMiddleware` is an installable Ktor 2.x plugin that records one
`HTTP_SERVER` span per request: named `"GET /hello"` from the request path
and upgraded to the matched route pattern (`"GET /orders/{id}/items"`)
after routing resolves. The response status lands on the span (`5xx` is
also recorded as an error), and handler crashes are recorded with status
500 and an `error.stack` attribute before the exception propagates to the
engine unchanged.

```kotlin
configure()

embeddedServer(CIO, port = 8080) {
    install(ktorMiddleware)
    routing { get("/orders/{id}") { ... } }
}.start(wait = true)
```

Trace propagation works in both directions: an incoming
`X-Dataflow-Trace-Id` request header is adopted as the span's trace id
(joining the caller's trace, like the Java core's filter), and the same
header is injected into the response so downstream callers can continue it.
`trace { }` called inside a route handler joins the server span — span
activation rides the JVM core's ThreadLocal, so across `Dispatchers` hops
pass the span explicitly (see Coroutines).

Ktor itself is a compile-only dependency of this SDK (Kotlin users already
have it); the middleware is inert when the SDK is disabled, and no
instrumentation failure ever breaks the request.

## Crash capture

`capture { }` runs a block and, when it throws, records the crash on the
current span — or a synthetic `exception` span when no trace is active —
and then rethrows the exception unchanged. The recorded span gets status
500, `error_message` = `throwable.toString()` (max 500 chars) and an
`error.stack` attribute with the stack trace (whole lines from the top,
max 8192 chars).

```kotlin
capture {
    processOrder(order)   // a crash here is recorded, then propagates
}
```

`captureUncaught()` installs a default uncaught-exception handler that
records the same fields on a synthetic `uncaught exception` span and then
chains to the handler that was installed before it — nothing is swallowed.
`ignoreUncaught()` removes it again (restoring the previous handler). Both
are idempotent. With the SDK disabled every path is a pure passthrough:
no recording, exceptions and handler chains untouched.

## Log capture

`log` (and the `info` / `warn` / `error` / `debug` shortcuts) buffers
application log lines for shipping with trace correlation: when a span is
active on the current thread, its `trace_id` / `span_id` ride along, so a
line lands next to the trace that produced it in the UI. Fields are
stringified (`toString()`, max 50) and levels normalize to
`debug|info|warn|error` (`"warning"` → `warn`, anything unknown → `info`).

```kotlin
configure()

trace("market.Checkout") { order ->
    info("checkout started")
    log("warn", "slow gateway", mapOf("gateway" to gw.name, "ms" to elapsed))
}
```

A daemon flusher posts buffered lines to `POST /api/v1/logs` in batches —
every 500 ms, or as soon as 50 lines have buffered; `flushLogs()` drains
synchronously (useful before shutdown). At most 1000 lines ship per request;
the buffer holds 1024 lines and drops the oldest when full.

`installLogHandler()` bridges `java.util.logging`: every published record is
forwarded through the same path (JUL severity maps to the wire levels, `{0}`
parameters are substituted, logger name and a thrown throwable ride as
fields). It is idempotent, and `removeLogHandler()` takes it back off.

```kotlin
installLogHandler()          // forward Logger.getGlobal()
installLogHandler(myLogger)  // or a specific JUL logger
removeLogHandler()
```

Best-effort by contract: logging never blocks or throws, and with the SDK
disabled — or on a bare `host:port` endpoint with no HTTP base — every path
is a no-op. Note: in files importing this package `error("…")` refers to
this SDK's shortcut, not `kotlin.error` (which throws).

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
  <version>0.8.0</version>
</dependency>
```

Build from the repo root (installs the Java core first):

```
mvn -f sdk-java/pom.xml install -DskipTests
mvn -f sdk-kotlin/pom.xml install -DskipTests
```

Live example: `example-kotlin/` (marketplace flow, Kotlin DSL end to end).

## Performance

The runtime overhead of every Dataflow SDK is measured with a uniform
benchmark: the same ~1 ms CPU-bound HTTP endpoint in three configs (no
instrumentation / Dataflow SDK / OpenTelemetry), one shared load driver,
spans exported live. Methodology, current numbers and reproduction steps:
Numbers are published in each SDK README as they are measured; the full harness lives in the Dataflow monorepo `bench/`.

Numbers for this SDK: **queued** — the harness follows the same contract
and will land here.
