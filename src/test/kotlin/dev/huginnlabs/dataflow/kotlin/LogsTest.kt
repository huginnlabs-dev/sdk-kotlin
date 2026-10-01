package dev.huginnlabs.dataflow.kotlin

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import dev.huginnlabs.dataflow.Dataflow
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger

/**
 * Log shipping tests: trace/span correlation on an active span, wire shape
 * and caps (50 fields, 8192-char messages, 1000-per-batch), the 1024-line
 * drop-oldest buffer, the background flusher's 50-line threshold, the JUL
 * bridge (forwarding, level mapping, idempotency) and disabled-SDK no-ops.
 *
 * Every test ships against its own local HttpServer and uses a unique
 * message prefix, so assertions are immune to leftover buffer traffic from
 * the process-wide flusher.
 */
class LogsTest {

    private var port = 0
    private lateinit var server: HttpServer

    /** Every request body that hit this test's server, in arrival order. */
    private val bodies = CopyOnWriteArrayList<String>()

    @BeforeEach
    fun startServer() {
        server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newSingleThreadExecutor()
        server.createContext("/api/v1/logs") { exchange: HttpExchange ->
            val body = exchange.requestBody.readAllBytes().toString(StandardCharsets.UTF_8)
            bodies.add(body)
            val resp = "{\"accepted\":1}".toByteArray(StandardCharsets.UTF_8)
            exchange.sendResponseHeaders(200, resp.size.toLong())
            exchange.responseBody.use { it.write(resp) }
        }
        server.start()
        port = server.address.port
    }

    @AfterEach
    fun stopServer() {
        suspendLogFlusher = false
        runCatching { flushLogs() } // drain leftovers; they may hit the next test's server
        server.stop(0)
    }

    private fun enableSdk() {
        Dataflow.configure(
            Dataflow.Builder()
                .endpoint("http://127.0.0.1:$port")
                .apiKey("test-key")
                .serviceName("logs-test"),
        )
    }

    private fun disableSdk() {
        Dataflow.configure(Dataflow.Builder().endpoint("http://127.0.0.1:$port").apiKey("k").disabled(true))
    }

    // ------------------------------------------------------------------
    // Local JSON shapes
    // ------------------------------------------------------------------

    /** All log lines received by this test's server, across requests. */
    private fun allLogs(): List<Map<String, Any?>> =
        bodies.map { body -> (TinyJson.parse(body) as Map<*, *>)["logs"] }
            .filterIsInstance<List<*>>()
            .flatten()
            .filterIsInstance<Map<*, *>>()
            .map { m -> m.entries.associate { (k, v) -> k as String to v } }

    /** Log lines whose message starts with [prefix]. */
    private fun withPrefix(prefix: String): List<Map<String, Any?>> =
        allLogs().filter { (it["message"] as? String)?.startsWith(prefix) == true }

    /** Per-request log counts (for batch-size assertions). */
    private fun requestLogCounts(): List<Int> =
        bodies.map { body -> ((TinyJson.parse(body) as Map<*, *>)["logs"] as List<*>).size }

    /** Waits until [expected] lines with [prefix] arrived, or times out. */
    private fun awaitLogs(prefix: String, expected: Int, timeoutMs: Long = 5_000): List<Map<String, Any?>> {
        val deadline = System.currentTimeMillis() + timeoutMs
        var got = withPrefix(prefix)
        while (got.size < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(25)
            got = withPrefix(prefix)
        }
        return got
    }

    // ------------------------------------------------------------------
    // Recording: levels, trace correlation, wire shape
    // ------------------------------------------------------------------

    @Test
    fun `shortcuts ship levels with trace and span ids on an active span`() {
        enableSdk()
        val before = System.currentTimeMillis()
        trace("app.Work") { span ->
            debug("tc-debug")
            info("tc-info")
            warn("tc-warn")
            error("tc-error")
            log("warning", "tc-warning")
            flushLogs()
            assertEquals(32, span.traceId().length)
            assertEquals(32, span.spanId().length)
        }

        val lines = awaitLogs("tc-", 5)
        assertEquals(5, lines.size, "each shortcut ships exactly one line")
        val byMessage = lines.associateBy { it["message"] }
        val traceId = lines.first()["trace_id"]
        val spanId = lines.first()["span_id"]
        for ((msg, level) in mapOf(
            "tc-debug" to "debug",
            "tc-info" to "info",
            "tc-warn" to "warn",
            "tc-error" to "error",
            "tc-warning" to "warn", // "warning" normalizes to the wire level
        )) {
            val line = byMessage[msg]
            assertTrue(line != null, "missing line $msg")
            assertEquals(level, line!!["level"], msg)
            assertEquals(traceId, line["trace_id"], "trace id must match the active span")
            assertEquals(spanId, line["span_id"], "span id must match the active span")
            assertEquals("logs-test", line["service_name"])
            assertEquals(emptyMap<String, Any>(), line["fields"], "no fields given — empty map")
            assertTrue((line["timestamp"] as Number).toLong() >= before, "unix ms timestamp")
        }
    }

    @Test
    fun `log outside a trace ships empty ids`() {
        enableSdk()
        info("nt-line")
        flushLogs()

        val lines = awaitLogs("nt-", 1)
        assertEquals(1, lines.size)
        assertEquals("", lines[0]["trace_id"], "no active span — empty trace id")
        assertEquals("", lines[0]["span_id"], "no active span — empty span id")
    }

    @Test
    fun `log normalizes unknown levels and skips empty messages`() {
        enableSdk()
        log("CRITICAL", "lv-critical")
        log("Info", "lv-ok")
        log("info", "   ") // trimmed empty — not shipped
        log("info", "") // empty — not shipped
        flushLogs()

        val lines = awaitLogs("lv-", 2)
        assertEquals(2, lines.size, "empty messages are dropped client-side")
        assertEquals(listOf("info", "info"), lines.map { it["level"] }, "unknown level falls back to info")
    }

    @Test
    fun `fields are stringified and capped at 50`() {
        enableSdk()
        val fields = LinkedHashMap<String, Any?>()
        for (i in 1..50) fields["k$i"] = i // insertion order survives the cap
        fields["k51"] = "overflow"
        fields["nul"] = null
        log("info", "fd-line", fields)
        log("info", "fd-null", mapOf<String, Any?>("only" to null))
        flushLogs()

        val lines = awaitLogs("fd-", 2)
        val byMessage = lines.associateBy { it["message"] }
        @Suppress("UNCHECKED_CAST")
        val wire = byMessage["fd-line"]!!["fields"] as Map<String, Any?>
        assertEquals(50, wire.size, "hard cap of 50 fields")
        assertEquals("1", wire["k1"], "values are stringified with toString()")
        assertEquals("50", wire["k50"])
        assertTrue(!wire.containsKey("k51"), "entries beyond the cap are dropped")
        assertTrue(!wire.containsKey("nul"))

        @Suppress("UNCHECKED_CAST")
        val nulls = byMessage["fd-null"]!!["fields"] as Map<String, Any?>
        assertEquals("", nulls["only"], "null value becomes an empty string")
    }

    @Test
    fun `messages are capped at 8192 chars`() {
        enableSdk()
        info("cap-" + "x".repeat(10_000))
        flushLogs()

        val lines = awaitLogs("cap-", 1)
        assertEquals(1, lines.size)
        val msg = lines[0]["message"] as String
        assertEquals(LOG_MESSAGE_CAP, msg.length)
        assertTrue(msg.startsWith("cap-"))
    }

    // ------------------------------------------------------------------
    // Buffer: drop-oldest at 1024, batches of at most 1000
    // ------------------------------------------------------------------

    @Test
    fun `buffer drops oldest lines beyond 1024 and batches at most 1000`() {
        enableSdk()
        suspendLogFlusher = true // keep the background flusher out of the fill
        try {
            for (i in 0 until 1100) log("info", "dp-$i")
            // Drain while the flusher is still suspended — flushLogs is not
            // gated by the flag, so this drain is synchronous and exclusive.
            flushLogs()
            suspendLogFlusher = false

            val got = withPrefix("dp-")
            assertEquals(1024, got.size, "76 oldest lines were dropped")
            assertEquals(
                (76 until 1100).map { "dp-$it" }.toSet(),
                got.map { it["message"] }.toSet(),
                "the newest 1024 lines survive, in any request split",
            )
            val counts = requestLogCounts()
            assertTrue(counts.isNotEmpty())
            counts.forEach { c -> assertTrue(c <= MAX_LOGS_PER_BATCH, "no request exceeds the 1000-log cap") }
            assertEquals(1024, counts.sum(), "all buffered lines arrived")
        } finally {
            suspendLogFlusher = false
        }
    }

    @Test
    fun `background flusher posts when 50 lines buffer`() {
        enableSdk()
        for (i in 0 until FLUSH_LINES) info("bg-$i")
        // No flushLogs() here — only the daemon flusher (threshold signal) ships.

        val got = awaitLogs("bg-", FLUSH_LINES)
        assertEquals(FLUSH_LINES, got.size, "the flusher delivered every buffered line")
        assertEquals(
            (0 until FLUSH_LINES).map { "bg-$it" }.toSet(),
            got.map { it["message"] }.toSet(),
        )
    }

    // ------------------------------------------------------------------
    // JUL bridge
    // ------------------------------------------------------------------

    @Test
    fun `installLogHandler forwards records and is idempotent`() {
        enableSdk()
        val logger = Logger.getLogger("df-logs-jul-test")
        logger.level = Level.ALL

        installLogHandler(logger)
        installLogHandler(logger) // idempotent: no double wrap
        assertEquals(1, logger.handlers.size, "a second install must not add another handler")

        logger.info("jl-info")
        logger.warning("jl-warn")
        logger.severe("jl-error")
        logger.log(Level.FINE, "jl-debug")
        logger.log(Level.INFO, "jl-hello {0} {1}", arrayOf("brave", "world"))

        // Publish one record with a throwable straight through the handler:
        // the exception must ride as a field, message text stays intact.
        val handler = logger.handlers.single()
        val thrownRec = LogRecord(Level.SEVERE, "jl-thrown boom")
        thrownRec.loggerName = "df-logs-jul-test" // Logger.log() sets this; set it manually here
        thrownRec.thrown = IOException("disk full")
        handler.publish(thrownRec)

        flushLogs()

        val lines = awaitLogs("jl-", 6)
        assertEquals(6, lines.size)
        val byMessage = lines.associateBy { it["message"] }
        assertEquals("info", byMessage["jl-info"]!!["level"])
        assertEquals("warn", byMessage["jl-warn"]!!["level"])
        assertEquals("error", byMessage["jl-error"]!!["level"])
        assertEquals("debug", byMessage["jl-debug"]!!["level"])
        assertTrue(byMessage.containsKey("jl-hello brave world"), "parameters are substituted")
        assertEquals("error", byMessage["jl-thrown boom"]!!["level"])
        @Suppress("UNCHECKED_CAST")
        val thrownFields = byMessage["jl-thrown boom"]!!["fields"] as Map<String, Any?>
        assertEquals("java.io.IOException: disk full", thrownFields["exception"])
        lines.forEach { line ->
            @Suppress("UNCHECKED_CAST")
            val fields = line["fields"] as Map<String, Any?>
            assertEquals("df-logs-jul-test", fields["logger"], "logger name rides as a field")
        }

        removeLogHandler(logger)
        removeLogHandler(logger) // idempotent removal
        assertEquals(0, logger.handlers.size, "remove restores the bare logger")

        logger.info("jl-after-remove")
        flushLogs()
        assertTrue(withPrefix("jl-after-remove").isEmpty(), "nothing forwarded after removal")
    }

    @Test
    fun `jul level mapping and message formatting`() {
        assertEquals("error", julLevelName(Level.SEVERE))
        assertEquals("warn", julLevelName(Level.WARNING))
        assertEquals("info", julLevelName(Level.INFO))
        assertEquals("debug", julLevelName(Level.CONFIG))
        assertEquals("debug", julLevelName(Level.FINE))
        assertEquals("debug", julLevelName(Level.FINEST))
        // Level's constructor is protected — anonymous subclasses are the idiom.
        assertEquals("error", julLevelName(object : Level("CUSTOM-HIGH", 1100) {}))
        assertEquals("debug", julLevelName(object : Level("CUSTOM-LOW", 150) {}))

        val withParams = LogRecord(Level.INFO, "paying {0} for order {1}")
        withParams.parameters = arrayOf<Any>("42", "A-1")
        assertEquals("paying 42 for order A-1", julMessage(withParams))

        val noParams = LogRecord(Level.INFO, "plain {0} text")
        noParams.parameters = arrayOf<Any>()
        assertEquals("plain {0} text", julMessage(noParams), "no parameters — raw message")

        val broken = LogRecord(Level.INFO, "broken pattern {0")
        broken.parameters = arrayOf<Any>("x")
        assertEquals("broken pattern {0", julMessage(broken), "format failure falls back to raw")
    }

    // ------------------------------------------------------------------
    // Disabled SDK: pure no-ops
    // ------------------------------------------------------------------

    @Test
    fun `disabled sdk - logging and the JUL handler ship nothing`() {
        disableSdk()
        info("off-1")
        log("warn", "off-2", mapOf("a" to 1))
        error("off-3")

        val logger = Logger.getLogger("df-logs-jul-off")
        installLogHandler(logger)
        logger.severe("off-4")
        removeLogHandler(logger)

        flushLogs() // must not throw and must not ship the buffered nothing
        assertTrue(withPrefix("off-").isEmpty(), "disabled SDK records nothing")
    }
}

/** Minimal JSON parser for the flat shapes this suite asserts on. */
private object TinyJson {
    fun parse(s: String): Any? = P(s).value()

    private class P(private val s: String) {
        private var i = 0

        fun value(): Any? {
            ws()
            return when (s.getOrNull(i) ?: fail("eof")) {
                '{' -> obj()
                '[' -> arr()
                '"' -> str()
                't' -> { lit("true"); true }
                'f' -> { lit("false"); false }
                'n' -> { lit("null"); null }
                else -> num()
            }
        }

        private fun ws() {
            while (i < s.length && s[i].isWhitespace()) i++
        }

        private fun lit(word: String) {
            if (!s.startsWith(word, i)) fail(word)
            i += word.length
        }

        private fun obj(): LinkedHashMap<String, Any?> {
            expect('{')
            val m = LinkedHashMap<String, Any?>()
            if (takeIfNext('}')) return m
            while (true) {
                ws()
                val k = str()
                expect(':')
                m[k] = value()
                when {
                    takeIfNext(',') -> {}
                    takeIfNext('}') -> return m
                    else -> fail("',' or '}'")
                }
            }
        }

        private fun arr(): MutableList<Any?> {
            expect('[')
            val list = ArrayList<Any?>()
            if (takeIfNext(']')) return list
            while (true) {
                list.add(value())
                when {
                    takeIfNext(',') -> {}
                    takeIfNext(']') -> return list
                    else -> fail("',' or ']'")
                }
            }
        }

        private fun takeIfNext(c: Char): Boolean {
            ws()
            if (i < s.length && s[i] == c) {
                i++
                return true
            }
            return false
        }

        private fun expect(c: Char) {
            if (!takeIfNext(c)) fail(c.toString())
        }

        private fun str(): String {
            expect('"')
            val out = StringBuilder()
            while (true) {
                if (i >= s.length) fail("string end")
                when (val c = s[i++]) {
                    '"' -> return out.toString()
                    '\\' -> {
                        if (i >= s.length) fail("escape")
                        when (val e = s[i++]) {
                            '"' -> out.append('"')
                            '\\' -> out.append('\\')
                            '/' -> out.append('/')
                            'n' -> out.append('\n')
                            't' -> out.append('\t')
                            'r' -> out.append('\r')
                            'b' -> out.append('\b')
                            'u' -> {
                                if (i + 4 > s.length) fail("unicode escape")
                                out.append(Integer.parseInt(s.substring(i, i + 4), 16).toChar())
                                i += 4
                            }
                            else -> fail("escape char")
                        }
                    }
                    else -> out.append(c)
                }
            }
        }

        private fun num(): Double {
            val start = i
            while (i < s.length && s[i] in "-+.eE0123456789") i++
            return s.substring(start, i).toDoubleOrNull() ?: fail("number")
        }

        private fun fail(what: String): Nothing = throw IllegalStateException("bad json near $i: expected $what")
    }
}
