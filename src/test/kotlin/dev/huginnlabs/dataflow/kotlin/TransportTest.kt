package dev.huginnlabs.dataflow.kotlin

import dev.huginnlabs.dataflow.Dataflow
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Interceptor
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import okio.Timeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.TimeUnit

/**
 * Transport tests: statement summarizing (mirrors the Go SDK's sqltrace
 * cases), the OkHttp interceptor over a fake chain, and the JDBC connection
 * proxy over handler-backed fake connections. Span emission itself needs the
 * gRPC pipeline and is only smoke-checked (no throw) here.
 */
class TransportTest {

    // ------------------------------------------------------------------
    // stmtSummary / clipStatement
    // ------------------------------------------------------------------

    @Test
    fun `stmtSummary derives verb and table`() {
        val cases = mapOf(
            // Same table as the Go SDK's TestStmtSummary.
            "SELECT id, total FROM orders WHERE id = \$1" to "SELECT orders",
            "  insert into users (email) values (\$1)" to "INSERT users",
            "UPDATE public.items SET total = total - 1" to "UPDATE items",
            "DELETE FROM sessions WHERE expires < now()" to "DELETE sessions",
            "CREATE TABLE IF NOT EXISTS migrations (id int)" to "CREATE migrations",
            "select u.id\nfrom users u\njoin orders o on o.user_id = u.id" to "SELECT users",
            "PRAGMA journal_mode=WAL" to "PRAGMA",
            // Kotlin-suite extras.
            "CREATE TABLE \"logs\" (id int)" to "CREATE logs",
            "INSERT INTO public.orders (id) VALUES (1)" to "INSERT orders",
            "EXPLAIN ANALYZE SELECT * FROM t" to "EXPLAIN t",
            "WITH recent AS (SELECT 1 FROM orders) SELECT * FROM recent" to "WITH orders",
            "BEGIN" to "BEGIN",
            "SET statement_timeout = 5000" to "SET",
            "MERGE INTO inventory USING cart ON ..." to "MERGE",
            "" to "QUERY",
            "   " to "QUERY",
        )
        for ((query, want) in cases) {
            assertEquals(want, stmtSummary(query), "stmtSummary($query)")
        }
    }

    @Test
    fun `clipStatement collapses whitespace and caps at 200`() {
        assertEquals(
            "SELECT a b c",
            clipStatement("SELECT a\n   b\t\tc"),
        )
        val long = "SELECT " + "x, ".repeat(100) + "1"
        val clipped = clipStatement(long)
        assertEquals(200, clipped.length)
        assertTrue(clipped.startsWith("SELECT "))
        assertEquals("short query", clipStatement("short query"))
        assertEquals("", clipStatement(null))
        assertEquals("", clipStatement("   "))
    }

    // ------------------------------------------------------------------
    // OkHttp interceptor (fake chain — no network, no client)
    // ------------------------------------------------------------------

    private fun request(url: String = "http://api.example.com/orders?limit=1") =
        Request.Builder().url(url).get().build()

    private fun response(req: Request, code: Int): Response =
        Response.Builder()
            .request(req)
            .protocol(Protocol.HTTP_1_1)
            .code(code)
            .message("OK")
            .build()

    private fun enableSdk() {
        Dataflow.configure(
            Dataflow.Builder()
                .endpoint("127.0.0.1:1")
                .apiKey("test-key")
                .serviceName("transport-test"),
        )
    }

    private fun disableSdk() {
        Dataflow.configure(Dataflow.Builder().endpoint("127.0.0.1:1").apiKey("k").disabled(true))
    }

    @Test
    fun `interceptor injects trace header and reports status`() {
        enableSdk()
        var seen: Request? = null
        val chain = FakeChain(request()) { r ->
            seen = r
            response(r, 200)
        }

        val resp = okhttpInterceptor().intercept(chain)

        assertEquals(200, resp.code)
        val traceId = seen?.header(TRACE_HEADER)
        assertNotNull(traceId, "trace header must be injected")
        assertEquals(32, traceId!!.length, "trace id is a 32-char uuid")
        // Original request handed to the chain stays untouched.
        assertNull(chain.request().header(TRACE_HEADER))
    }

    @Test
    fun `interceptor is inert when sdk disabled`() {
        disableSdk()
        var seen: Request? = null
        val chain = FakeChain(request()) { r ->
            seen = r
            response(r, 200)
        }

        val resp = okhttpInterceptor().intercept(chain)

        assertEquals(200, resp.code)
        assertNull(seen?.header(TRACE_HEADER), "no header when disabled")
    }

    @Test
    fun `interceptor records and rethrows transport failure`() {
        enableSdk()
        val boom = IOException("connection reset")
        val chain = FakeChain(request()) { _ -> throw boom }

        val err = assertThrows<IOException> { okhttpInterceptor().intercept(chain) }
        assertSame(boom, err, "original exception must propagate untouched")
    }

    // ------------------------------------------------------------------
    // JDBC proxy (handler-backed fakes — no driver on the classpath)
    // ------------------------------------------------------------------

    /** Records executed SQL; optionally fails execute calls with a SQLException. */
    private class FakeStatements(val fail: Boolean = false) {
        val executed = ArrayList<String>()

        /**
         * Proxy over the statement interfaces. Prepared/callable statements
         * must advertise PreparedStatement: the proxy stub casts handler
         * returns to the interface method's declared return type, exactly
         * like a real driver's object satisfies it.
         */
        fun statementProxy(preparedSql: String?, interfaces: Array<Class<*>>): Statement =
            Proxy.newProxyInstance(
                Statement::class.java.classLoader,
                interfaces,
                InvocationHandler { _, method, args ->
                    when (method.name) {
                        "execute" -> {
                            if (fail) throw SQLException("boom")
                            executed += args?.firstOrNull() as? String ?: preparedSql!!
                            true
                        }
                        "executeQuery" -> {
                            if (fail) throw SQLException("boom")
                            executed += (args?.firstOrNull() as? String) ?: preparedSql!!
                            null
                        }
                        "executeUpdate" -> {
                            if (fail) throw SQLException("boom")
                            executed += args?.firstOrNull() as? String ?: preparedSql!!
                            1
                        }
                        "close", "isClosed" -> if (method.name == "isClosed") false else null
                        else -> null
                    }
                },
            ) as Statement
    }

    private fun fakeConnection(statements: FakeStatements): Connection =
        Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java),
            InvocationHandler { _, method, args ->
                when (method.name) {
                    "createStatement" -> statements.statementProxy(null, arrayOf(Statement::class.java))
                    "prepareStatement", "prepareCall" ->
                        statements.statementProxy(
                            args?.firstOrNull() as? String,
                            arrayOf(java.sql.PreparedStatement::class.java),
                        )
                    else -> null
                }
            },
        ) as Connection

    @Test
    fun `wrapConnection passes statements through and preserves SQLException`() {
        val fakes = FakeStatements(fail = true)
        val conn = wrapConnection(fakeConnection(fakes), "h2")

        // The driver's SQLException must reach the caller unwrapped (no
        // InvocationTargetException / UndeclaredThrowableException).
        val e = assertThrows<SQLException> {
            conn.createStatement().execute("SELECT 1")
        }
        assertEquals("boom", e.message)
        assertTrue(fakes.executed.isEmpty(), "failed execute must not be recorded as done")

        conn.close() // pass-through, must not throw
    }

    @Test
    fun `wrapConnection wraps plain and prepared statements`() {
        val fakes = FakeStatements()
        val conn = wrapConnection(fakeConnection(fakes), "h2")

        assertTrue(conn.createStatement().execute("DELETE FROM sessions WHERE id = 7"))
        assertEquals("DELETE FROM sessions WHERE id = 7", fakes.executed.last())

        val ps = conn.prepareStatement("SELECT id FROM public.orders WHERE x = ?")
        assertNull(ps.executeQuery())
        assertEquals("SELECT id FROM public.orders WHERE x = ?", fakes.executed.last())

        assertEquals(1, ps.executeUpdate("UPDATE t SET a = 1"))
        assertEquals("UPDATE t SET a = 1", fakes.executed.last())

        // End-to-end smoke: DbSpan.end() must not throw with the pipeline off.
        val span = DbSpan("h2", stmtSummary("SELECT 1 FROM t"), clipStatement("SELECT 1 FROM t"))
        span.status(200)
        span.end()
        span.end() // idempotent
    }

    @Test
    fun `DbSpan smoke - never throws when pipeline is down`() {
        // No configure() at all: end() must be a silent no-op.
        val span = DbSpan("postgres", "SELECT t", "SELECT 1 FROM t")
        span.recordError(RuntimeException("x"))
        span.status(500)
        span.end()
    }
}

/** Minimal Interceptor.Chain over a canned proceed lambda. */
private class FakeChain(
    private val req: Request,
    private val proceedFn: (Request) -> Response,
) : Interceptor.Chain {
    override fun request(): Request = req
    override fun proceed(request: Request): Response = proceedFn(request)
    override fun connection(): okhttp3.Connection? = null
    override fun call(): Call = FakeCall(req)
    override fun connectTimeoutMillis(): Int = 10_000
    override fun withConnectTimeout(timeout: Int, unit: TimeUnit): Interceptor.Chain = this
    override fun readTimeoutMillis(): Int = 10_000
    override fun withReadTimeout(timeout: Int, unit: TimeUnit): Interceptor.Chain = this
    override fun writeTimeoutMillis(): Int = 10_000
    override fun withWriteTimeout(timeout: Int, unit: TimeUnit): Interceptor.Chain = this
}

/** Inert okhttp3.Call standing behind the fake chain. */
private class FakeCall(private val req: Request) : Call {
    override fun request(): Request = req
    override fun execute(): Response = throw UnsupportedOperationException()
    override fun enqueue(responseCallback: Callback) = throw UnsupportedOperationException()
    override fun cancel() {}
    override fun isExecuted(): Boolean = false
    override fun isCanceled(): Boolean = false
    override fun timeout(): Timeout = Timeout()
    public override fun clone(): Call = this
}
