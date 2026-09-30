package dev.huginnlabs.dataflow.kotlin

import dev.huginnlabs.dataflow.Dataflow
import dev.huginnlabs.dataflow.Span
import dev.huginnlabs.dataflow.gen.DataflowProto
import com.google.protobuf.UnknownFieldSet
import okhttp3.HttpUrl
import okhttp3.Interceptor
import okhttp3.Request
import java.lang.reflect.Field
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.Statement
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

/**
 * Transport instrumentation for the Kotlin SDK: outgoing HTTP (OkHttp) and
 * database access (JDBC), layered over the Java core's delivery pipeline.
 *
 * - [okhttpInterceptor] records one `HTTP_CLIENT` span per request ("GET
 *   api.example.com/orders") and injects `X-Dataflow-Trace-Id` so the
 *   receiving instrumented service continues the same trace.
 * - [wrapConnection] proxies a `java.sql.Connection` and records one
 *   `DB_QUERY` span per executed statement ("<VERB> <table>", e.g.
 *   "SELECT orders") with `db.system` / `db.statement` metadata — the SQL
 *   text single-spaced and capped at 200 chars, parameter values never.
 *
 * Both are best-effort: when the SDK is disabled they stay inert, and no
 * instrumentation failure ever breaks the application's call.
 *
 * Wire note: the Java core pinned by this SDK (dataflow-sdk 0.2.0) predates
 * the `EVENT_TYPE_DB_QUERY` literal (wire enum number 6) — its codegen maps
 * unknown span types to `FUNCTION_CALL`. DB spans are therefore emitted
 * natively with the type carried as a proto3 unknown field, which the
 * server's proto parses back into `EVENT_TYPE_DB_QUERY`. When the core's
 * generated descriptor learns the literal, the typed path is used instead.
 */

/** Wire enum number of `EVENT_TYPE_DB_QUERY` in the server's proto. */
private const val EVENT_TYPE_DB_QUERY = 6

/** `TraceEvent.type` wire field number (`DataflowProto.TraceEvent.TYPE_FIELD_NUMBER`). */
private val TRACE_EVENT_TYPE_FIELD = DataflowProto.TraceEvent.TYPE_FIELD_NUMBER

/** Trace propagation header shared with the Java core's TracedHttp. */
internal const val TRACE_HEADER = "X-Dataflow-Trace-Id"

/** Wire-contract cap for `db.statement` metadata. */
private const val STATEMENT_CAP = 200

// ---------------------------------------------------------------------------
// HTTP: OkHttp interception. The span itself is pure java-core reuse — the
// core maps the "HTTP_CLIENT" type natively.
// ---------------------------------------------------------------------------

/**
 * An OkHttp interceptor recording one `HTTP_CLIENT` span around every
 * request: name "METHOD host[:port]/path", callee = host, status code from
 * the response, `http.method` / `http.url` metadata, and an injected
 * `X-Dataflow-Trace-Id` request header so the downstream instrumented
 * service joins the trace. Errors from the call are recorded before being
 * rethrown unchanged; with the SDK disabled the interceptor is inert.
 *
 * ```
 * val client = OkHttpClient.Builder()
 *     .addInterceptor(okhttpInterceptor())
 *     .build()
 * ```
 */
fun okhttpInterceptor(): Interceptor = Interceptor { chain ->
    val request = chain.request()
    val span = openHttpSpan(request)
    if (span == null) {
        chain.proceed(request)
    } else {
        // Inject the propagation header; header() replaces any prior value.
        val traced = runCatching {
            request.newBuilder().header(TRACE_HEADER, span.traceId()).build()
        }.getOrDefault(request)
        try {
            val response = chain.proceed(traced)
            runCatching {
                span.status(response.code)
                span.attr("http.status_code", response.code.toString())
            }
            response
        } catch (t: Throwable) {
            runCatching { span.recordError(t) }
            throw t
        } finally {
            runCatching { span.end() }
        }
    }
}

/**
 * Opens the HTTP_CLIENT span for [request] (joining the current trace when
 * one is active on this thread); null when the SDK is disabled or anything
 * goes wrong — instrumentation must never fail the request.
 */
private fun openHttpSpan(request: Request): Span? {
    if (!runCatching { Dataflow.enabled() }.getOrDefault(false)) return null
    return runCatching {
        val url = request.url
        val authority = authorityOf(url)
        val span = Dataflow.startSpan(request.method + " " + authority + url.encodedPath, "HTTP_CLIENT")
        span.callee(authority)
        span.attr("http.method", request.method)
        span.attr("http.url", url.toString())
        span
    }.getOrNull()
}

/** "host" for default ports, "host:port" otherwise (same shape as TracedHttp). */
private fun authorityOf(url: HttpUrl): String =
    if (url.port == HttpUrl.defaultPort(url.scheme)) url.host else url.host + ":" + url.port

// ---------------------------------------------------------------------------
// SQL: statement summarizing (mirrors the Go SDK's sqltrace.go rules).
// ---------------------------------------------------------------------------

private val STMT_VERB = Regex(
    """(?is)^\s*\(?\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|""" +
        """WITH|BEGIN|COMMIT|ROLLBACK|SET|CALL|EXEC|SHOW|EXPLAIN)\b""",
)

private val STMT_TABLE = Regex(
    """(?i)\b(?:FROM|INTO|UPDATE|TABLE|JOIN)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?[`"'\[]?([A-Za-z_][\w$.]*)""",
)

/**
 * Short human name for a statement: the verb plus the first table reference
 * when one exists ("SELECT orders", "INSERT users", "CREATE migrations" —
 * `IF [NOT] EXISTS` skipped, "public.items" reported as "items"); bare verbs
 * and non-SQL fall back to the first word ("PRAGMA") or "QUERY".
 */
internal fun stmtSummary(query: String?): String {
    val one = oneLined(query)
    val verb = STMT_VERB.find(one)?.groupValues?.get(1)
    if (verb == null) {
        val i = one.indexOfAny(charArrayOf(' ', '\t', '\n', '('))
        return if (i > 0) one.substring(0, i).uppercase() else "QUERY"
    }
    val table = STMT_TABLE.find(one)?.groupValues?.get(1)
    if (table == null) return verb.uppercase()
    // Schema-qualified names ("public.items") report the bare table.
    return verb.uppercase() + " " + table.substringAfterLast('.').substringAfterLast('$')
}

/** The statement text single-spaced and capped at [STATEMENT_CAP] chars. */
internal fun clipStatement(query: String?): String {
    val one = oneLined(query)
    return if (one.length > STATEMENT_CAP) one.substring(0, STATEMENT_CAP) else one
}

/** Collapses all whitespace runs to single spaces (Go strings.Fields+Join). */
private fun oneLined(query: String?): String =
    query?.trim()
        ?.split(Regex("\\s+"))
        ?.filterTo(ArrayList()) { it.isNotEmpty() }
        ?.joinToString(" ")
        ?: ""

// ---------------------------------------------------------------------------
// SQL: DB_QUERY span emission. Native to this wrapper (see the wire note in
// the file doc) because the pinned java core cannot carry the DB_QUERY type.
// ---------------------------------------------------------------------------

/**
 * One measured database statement. Trace linkage follows the java core: a
 * child of the span active on this thread, or a fresh trace. `end()` is
 * idempotent and best-effort — emission failures are silent.
 */
internal class DbSpan internal constructor(
    private val system: String,
    private val summary: String,
    private val statement: String,
) {
    private val startMillis = System.currentTimeMillis()
    private val startNanos = System.nanoTime()
    private val spanId = newId()
    private val traceId: String
    private val parentSpanId: String
    private val callerPackage: String
    private val emit: Boolean
    private var errorMessage = ""
    private var statusCode = 0
    private var ended = false

    init {
        val parent = runCatching { Span.current() }.getOrNull()
        traceId = parent?.traceId() ?: newId()
        parentSpanId = parent?.spanId() ?: ""
        callerPackage = parent?.let { calleePackageOf(it) } ?: ""
        emit = runCatching { Dataflow.enabled() }.getOrDefault(false) && sampled()
    }

    /** Applies the core's sampling ratio, mirroring the Span constructor. */
    private fun sampled(): Boolean {
        val ratio = runCatching { Dataflow.settings().sampleRatio }.getOrDefault(1.0)
        return ratio >= 1.0 || ThreadLocalRandom.current().nextDouble() < ratio
    }

    fun recordError(t: Throwable) {
        val m = t?.let { it.javaClass.simpleName + ": " + it.message } ?: return
        synchronized(this) {
            errorMessage = if (errorMessage.isEmpty()) m else "$errorMessage; $m"
        }
    }

    fun status(code: Int) {
        synchronized(this) { statusCode = code }
    }

    /** Builds the wire event and hands it to the core's delivery pipeline. */
    @Synchronized
    fun end() {
        if (ended || !emit) return
        ended = true
        val event = runCatching { buildEvent() }.getOrNull() ?: return
        runCatching { coreEnqueue()?.invoke(null, event) }
    }

    private fun buildEvent(): DataflowProto.TraceEvent {
        val b = DataflowProto.TraceEvent.newBuilder()
            .setEventId(newId())
            .setSeq(runCatching { pipelineNextSeq()?.invoke(null) as? Long }.getOrNull() ?: 0L)
            .setTimestamp(startMillis)
            .setDurationMs((System.nanoTime() - startNanos) / 1_000_000L)
            .setServiceName(Dataflow.serviceName())
            .setName(summary)
            .setStatusCode(statusCode)
            .setErrorMessage(errorMessage)
            .setTraceId(traceId)
            .setSpanId(spanId)
            .setParentSpanId(parentSpanId)
        if (callerPackage.isNotEmpty()) b.setCallerPackage(callerPackage)
        if (system.isNotEmpty()) {
            b.setCalleePackage(system)
            b.putMetadata("db.system", system)
        }
        if (statement.isNotEmpty()) b.putMetadata("db.statement", statement)
        applyDbQueryType(b)
        return b.build()
    }
}

/**
 * Sets the event type to `EVENT_TYPE_DB_QUERY` (wire number 6). Prefers the
 * typed enum when the core's generated descriptor knows it; otherwise carries
 * the number as a proto3 unknown field — a stable wire encoding the server's
 * newer proto reads back as the DB_QUERY literal.
 */
private fun applyDbQueryType(b: DataflowProto.TraceEvent.Builder) {
    val known = runCatching {
        val field = DataflowProto.TraceEvent.getDescriptor().findFieldByName("type")
        val value = field?.enumType?.findValueByNumber(EVENT_TYPE_DB_QUERY)
        if (field != null && value != null) {
            b.setField(field, value)
            true
        } else {
            false
        }
    }.getOrDefault(false)
    if (known) return
    b.setUnknownFields(
        UnknownFieldSet.newBuilder()
            .addField(
                TRACE_EVENT_TYPE_FIELD,
                UnknownFieldSet.Field.newBuilder().addVarint(EVENT_TYPE_DB_QUERY.toLong()).build(),
            )
            .build(),
    )
}

/** Same id format as the java core's Span (UUID without dashes). */
private fun newId(): String = UUID.randomUUID().toString().replace("-", "")

/** A span's callee package for caller attribution; private in the core. */
private fun calleePackageOf(span: Span): String =
    runCatching { spanCalleeField()?.get(span) as? String }.getOrNull() ?: ""

// --- memoized reflective bridges into the java core's package-private delivery path ---

private fun pipelineNextSeq(): Method? = holder.nextSeq
private fun coreEnqueue(): Method? = holder.enqueue
private fun spanCalleeField(): Field? = holder.calleeField

private val holder by lazy {
    object {
        val nextSeq: Method? = runCatching {
            Class.forName("dev.huginnlabs.dataflow.Pipeline")
                .getDeclaredMethod("nextSeq")
                .apply { isAccessible = true }
        }.getOrNull()

        val enqueue: Method? = runCatching {
            Class.forName("dev.huginnlabs.dataflow.Dataflow")
                .getDeclaredMethod("enqueue", DataflowProto.TraceEvent::class.java)
                .apply { isAccessible = true }
        }.getOrNull()

        val calleeField: Field? = runCatching {
            Span::class.java.getDeclaredField("calleePackage")
                .apply { isAccessible = true }
        }.getOrNull()
    }
}

// ---------------------------------------------------------------------------
// SQL: java.sql.Connection proxy.
// ---------------------------------------------------------------------------

/**
 * Returns a tracing proxy over [conn] emitting one `DB_QUERY` span per
 * executed statement (see [DbSpan] for the wire shape). Tracing is decided
 * per query — connections may be wrapped before [configure] and start
 * tracing as soon as the SDK is configured, mirroring the Go SDK. All other
 * connection and statement calls pass through to the driver untouched; SQL
 * exceptions surface to the caller exactly as the driver raised them.
 *
 * `system` is the database dialect reported in `db.system` and as the
 * span's callee ("postgres", "mysql", "h2", ...).
 *
 * ```
 * dataSource.connection.use { raw ->
 *     wrapConnection(raw, "postgres").use { conn ->
 *         conn.prepareStatement("SELECT total FROM orders WHERE id = ?").use { st ->
 *             st.setLong(1, orderId)
 *             st.executeQuery().use { rs -> ... }
 *         }
 *     }
 * }
 * ```
 */
fun wrapConnection(conn: Connection, system: String): Connection =
    proxy(
        conn,
        Connection::class.java,
        ConnectionHandler(conn, system),
    ) as Connection

/** Statement lifecycle methods whose first argument is the SQL text. */
private val PREPARE_METHODS = setOf("prepareStatement", "prepareCall")

/** Statement methods that execute SQL and therefore open a DB span. */
private val EXECUTE_METHODS = setOf(
    "execute", "executeQuery", "executeUpdate", "executeLargeUpdate",
)

/** Batch execution methods (traced only when the statement text is known). */
private val BATCH_METHODS = setOf("executeBatch", "executeLargeBatch")

/** Handler over a proxied [Connection]: wraps produced statements. */
private class ConnectionHandler(
    private val conn: Connection,
    private val system: String,
) : InvocationHandler {
    override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
        identityMethod(method, proxy, args)?.let { return it }
        val result = invokeUnderlying(conn, method, args)
        if (result is Statement && (method.name == "createStatement" || method.name in PREPARE_METHODS)) {
            return proxy(
                result,
                method.returnType,
                StatementHandler(result, system, preparedSql(method, args)),
            )
        }
        return result
    }

    /** SQL text known up front for prepared/callable statements. */
    private fun preparedSql(method: Method, args: Array<out Any?>?): String? =
        if (method.name in PREPARE_METHODS) args?.firstOrNull() as? String else null
}

/** Handler over a proxied [Statement]: spans the executing methods. */
private class StatementHandler(
    private val stmt: Statement,
    private val system: String,
    preparedSql: String?,
) : InvocationHandler {
    // Known up front for prepared/callable statements; null on plain
    // statements, where the SQL arrives as the execute* argument.
    private val summary: String? = preparedSql?.let { stmtSummary(it) }
    private val statement: String? = preparedSql?.let { clipStatement(it) }

    override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
        identityMethod(method, proxy, args)?.let { return it }
        val sql = if (method.name in EXECUTE_METHODS) {
            (args?.firstOrNull() as? String) ?: statement
        } else if (method.name in BATCH_METHODS) {
            statement // only prepared statements carry known batch text
        } else {
            null
        }
        if (sql == null) return invokeUnderlying(stmt, method, args)

        val span = DbSpan(system, stmtSummary(sql), clipStatement(sql))
        try {
            val result = invokeUnderlying(stmt, method, args)
            span.status(200)
            return result
        } catch (t: Throwable) {
            span.recordError(t)
            span.status(500)
            throw t
        } finally {
            span.end()
        }
    }
}

/**
 * Identity semantics for the Object methods every proxy inherits: equals /
 * hashCode / toString answer on the proxy itself instead of descending into
 * the driver object (equals against the proxy would otherwise always be
 * false, and hashCode would differ on every driver object). Returns null for
 * every other method.
 */
private fun identityMethod(method: Method, proxy: Any, args: Array<out Any?>?): Any? {
    if (method.declaringClass != Any::class.java) return null
    return when (method.name) {
        "equals" -> proxy === args?.firstOrNull()
        "hashCode" -> System.identityHashCode(proxy)
        "toString" ->
            "dataflow-proxy@" + Integer.toHexString(System.identityHashCode(proxy))
        else -> null
    }
}

/** Invokes [method] on [target], rethrowing the driver's real exception. */
private fun invokeUnderlying(target: Any, method: Method, args: Array<out Any?>?): Any? =
    try {
        method.invoke(target, *args.orEmpty())
    } catch (e: InvocationTargetException) {
        throw e.targetException
    }

/** Creates a proxy over [type] delegating to [handler] via [target]'s loader. */
private fun proxy(target: Any, type: Class<*>, handler: InvocationHandler): Any =
    Proxy.newProxyInstance(
        target.javaClass.classLoader ?: type.classLoader,
        arrayOf(type),
        handler,
    )
