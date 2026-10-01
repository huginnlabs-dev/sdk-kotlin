package dev.huginnlabs.dataflow.kotlin

import dev.huginnlabs.dataflow.Dataflow
import dev.huginnlabs.dataflow.Json
import dev.huginnlabs.dataflow.Span
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.text.MessageFormat
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger

/**
 * Application log shipping with trace correlation.
 *
 * [log] (and the [info]/[warn]/[error]/[debug] shortcuts) buffers one line
 * at a time — stamped with the current span's `trace_id` / `span_id` from
 * the java core's public `Span.current()` (empty when no trace is active).
 * A daemon flusher posts buffered lines in batches to
 * `POST {base}/api/v1/logs` (same manifest-style base resolution: a bare
 * `host:port` endpoint means logging stays off). [installLogHandler]
 * forwards `java.util.logging` records through the same path.
 *
 * Best-effort by contract, like everything else in this SDK: logging never
 * blocks or throws, batches are capped at 1000 lines (server limit), the
 * buffer is bounded at 1024 lines with drop-oldest, and any delivery
 * failure silently drops the batch.
 */

/** Buffer bound: at most this many lines are held between flushes. */
internal const val MAX_LOG_LINES = 1024

/** Lines buffered before an immediate flush (else flush every interval). */
internal const val FLUSH_LINES = 50

/** Milliseconds between background flush attempts. */
internal const val FLUSH_INTERVAL_MS = 500L

/** Server wire limit: at most 1000 logs per POST. */
internal const val MAX_LOGS_PER_BATCH = 1000

/** Server wire limit: at most 50 fields per log line. */
internal const val MAX_LOG_FIELDS = 50

/** Client-side message cap keeping the bounded buffer's memory bounded. */
internal const val LOG_MESSAGE_CAP = 8192

/** Wire caps mirrored from the server (fields key/value, ids, service). */
internal const val LOG_FIELD_KEY_CAP = 128
internal const val LOG_FIELD_VALUE_CAP = 512
internal const val LOG_ID_CAP = 128
internal const val LOG_SERVICE_CAP = 256

/** Guards [logBuffer] (a JVM monitor — Kotlin's Any hides wait/notify). */
private val logLock = Object()

/** Bounded, ordered line buffer; entries are wire-shaped JSON maps. */
private val logBuffer = ArrayDeque<LinkedHashMap<String, Any>>()

/** The single background flusher thread; started with the first line. */
private val flusherStarted = AtomicBoolean(false)

/** Test seam: when true the flusher stops draining (buffer tests only). */
@Volatile
internal var suspendLogFlusher = false

/** Shared HTTP client for log posts (immutable, thread-safe). */
private val logHttpClient: HttpClient? by lazy {
    runCatching {
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()
    }.getOrNull()
}

// ---------------------------------------------------------------------------
// Recording: log() + level shortcuts
// ---------------------------------------------------------------------------

/**
 * Buffers one application log line for shipping. [level] is normalized to
 * `debug|info|warn|error` (`"warning"` maps to `warn`, anything else to
 * `info`, matching the server); [fields] values are stringified with
 * `toString()` (at most 50 fields). When a span is active on this thread
 * its trace/span ids ride along, correlating the line with the trace.
 *
 * Never blocks or throws: with the SDK disabled, before [configure], or on
 * a bare `host:port` endpoint (no HTTP base — logging off) this is a no-op;
 * on a full buffer the oldest line is dropped.
 */
fun log(level: String, message: String, fields: Map<String, Any?> = emptyMap()) {
    runCatching { recordLog(level, message, fields) }
}

/** Buffers an `info` line (see [log]). */
fun info(message: String) = log("info", message)

/** Buffers a `warn` line (see [log]). */
fun warn(message: String) = log("warn", message)

/**
 * Buffers an `error` line (see [log]). Note: for files importing this
 * package this shadows `kotlin.error` — here `error("...")` logs instead
 * of throwing.
 */
fun error(message: String) = log("error", message)

/** Buffers a `debug` line (see [log]). */
fun debug(message: String) = log("debug", message)

/** Normalizes a caller-supplied level to the wire vocabulary. */
internal fun normalizeLogLevel(level: String): String {
    val l = level.trim().lowercase()
    return when (l) {
        "debug", "info", "warn", "error" -> l
        "warning" -> "warn"
        else -> "info"
    }
}

/**
 * Builds the wire map and enqueues it. The caller's thread provides the
 * span context (ThreadLocal in the core), so correlation is exact here.
 */
private fun recordLog(level: String, message: String, fields: Map<String, Any?>) {
    if (!sdkEnabled()) return
    if (httpBaseURL(endpoint()) == null) return // bare host:port → logging off
    val msg = message.trim()
    if (msg.isEmpty()) return
    val current = runCatching { Span.current() }.getOrNull()
    val line = LinkedHashMap<String, Any>()
    line["timestamp"] = System.currentTimeMillis()
    line["level"] = normalizeLogLevel(level)
    line["message"] = clipTo(msg, LOG_MESSAGE_CAP)
    line["trace_id"] = clipTo(current?.traceId() ?: "", LOG_ID_CAP)
    line["span_id"] = clipTo(current?.spanId() ?: "", LOG_ID_CAP)
    line["service_name"] = clipTo(serviceName(), LOG_SERVICE_CAP)
    val wire = LinkedHashMap<String, String>()
    for ((k, v) in fields) {
        if (wire.size >= MAX_LOG_FIELDS) break
        wire[clipTo(k ?: "", LOG_FIELD_KEY_CAP)] = clipTo(v?.toString() ?: "", LOG_FIELD_VALUE_CAP)
    }
    line["fields"] = wire
    synchronized(logLock) {
        while (logBuffer.size >= MAX_LOG_LINES) logBuffer.removeFirst() // drop-oldest
        logBuffer.addLast(line)
        if (logBuffer.size >= FLUSH_LINES) logLock.notifyAll()
    }
    startFlusher()
}

/** Current settings' endpoint; "" when the core is unusable. */
private fun endpoint(): String =
    runCatching { Dataflow.settings().endpoint }.getOrDefault("")

/** Current service name; "" when the core is unusable. */
private fun serviceName(): String =
    runCatching { Dataflow.serviceName() }.getOrDefault("")

/** Truncates to [max] characters. */
private fun clipTo(s: String, max: Int): String =
    if (s.length <= max) s else s.substring(0, max)

// ---------------------------------------------------------------------------
// Shipping: background flusher + explicit flush
// ---------------------------------------------------------------------------

/**
 * Starts the daemon flusher once per process. Wakeups: every
 * [FLUSH_INTERVAL_MS], or immediately when [FLUSH_LINES] have buffered
 * (the enqueue signals). Drains are atomic under [logLock], so the
 * flusher and [flushLogs] can interleave without losing or duplicating
 * lines.
 */
private fun startFlusher() {
    if (!flusherStarted.compareAndSet(false, true)) return
    Thread {
        while (true) {
            val batch: List<LinkedHashMap<String, Any>> = synchronized(logLock) {
                // Park while suspended or below the threshold — never spin,
                // never drain a buffer the caller asked us to leave alone.
                if (suspendLogFlusher || logBuffer.size < FLUSH_LINES) {
                    runCatching { logLock.wait(FLUSH_INTERVAL_MS) }
                }
                if (suspendLogFlusher) emptyList() else drainLocked()
            }
            for (chunk in batch.chunked(MAX_LOGS_PER_BATCH)) postLogs(chunk)
        }
    }.apply {
        name = "dataflow-logs"
        isDaemon = true
        start()
    }
}

/** Drains the whole buffer under [logLock]; caller synchronizes. */
private fun drainLocked(): MutableList<LinkedHashMap<String, Any>> {
    val out = ArrayList<LinkedHashMap<String, Any>>(logBuffer.size)
    while (logBuffer.isNotEmpty()) out.add(logBuffer.removeFirst())
    return out
}

/**
 * Synchronously drains and posts every buffered line, split into
 * [MAX_LOGS_PER_BATCH]-sized requests. Useful before shutdown; safe from
 * any thread; a no-op when the buffer is empty, the SDK is disabled or
 * logging is off. Never throws.
 */
fun flushLogs() {
    runCatching {
        while (true) {
            val batch = synchronized(logLock) { drainLocked() }
            if (batch.isEmpty()) return
            for (chunk in batch.chunked(MAX_LOGS_PER_BATCH)) postLogs(chunk)
        }
    }
}

/**
 * Posts one batch to `POST {base}/api/v1/logs` with the `X-Api-Key` header
 * (same shape as the manifest report). Best-effort: no key, no HTTP base,
 * a non-2xx response or any transport failure silently drops the batch.
 */
private fun postLogs(lines: List<LinkedHashMap<String, Any>>) {
    if (lines.isEmpty()) return
    val client = logHttpClient ?: return
    val s = runCatching { Dataflow.settings() }.getOrNull() ?: return
    if (s.apiKey.isEmpty()) return
    val base = runCatching { httpBaseURL(s.endpoint) }.getOrNull() ?: return
    runCatching {
        val body = Json.write(linkedMapOf<String, Any>("logs" to lines))
        val req = HttpRequest.newBuilder(URI.create("$base/api/v1/logs"))
            .timeout(Duration.ofSeconds(5))
            .header("Content-Type", "application/json")
            .header("X-Api-Key", s.apiKey)
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build()
        // Response body is discarded; non-2xx is fine to ignore.
        client.send(req, HttpResponse.BodyHandlers.discarding())
    }
}

// ---------------------------------------------------------------------------
// java.util.logging bridge
// ---------------------------------------------------------------------------

/** Guards [installedHandlers]. */
private val handlerLock = Any()

/** The handler this SDK installed per logger (idempotency key: the logger). */
private val installedHandlers = HashMap<Logger, Handler>()

/**
 * Adds a JUL handler on [logger] (default: the global logger) that forwards
 * every published record through the same buffered shipping path as [log].
 * Idempotent: a second call for the same logger is a no-op. With the SDK
 * disabled the handler stays installed but forwards nothing.
 *
 * ```
 * installLogHandler()                       // forward the global logger
 * installLogHandler(myLogger)               // forward a specific one
 * ```
 */
fun installLogHandler(logger: Logger = Logger.getGlobal()) {
    synchronized(handlerLock) {
        if (installedHandlers.containsKey(logger)) return
        val handler = DataflowLogHandler()
        runCatching { logger.addHandler(handler) }
        installedHandlers[logger] = handler
    }
}

/**
 * Removes the handler installed by [installLogHandler] on [logger].
 * Idempotent: a no-op when nothing is installed.
 */
fun removeLogHandler(logger: Logger = Logger.getGlobal()) {
    synchronized(handlerLock) {
        val handler = installedHandlers.remove(logger) ?: return
        runCatching { logger.removeHandler(handler) }
    }
}

/** JUL level → wire level by severity order; anything verbose is debug. */
internal fun julLevelName(level: Level): String = when {
    level.intValue() >= Level.SEVERE.intValue() -> "error"
    level.intValue() >= Level.WARNING.intValue() -> "warn"
    level.intValue() >= Level.INFO.intValue() -> "info"
    else -> "debug"
}

/**
 * The record's message with `{0}`-style parameters substituted; falls back
 * to the raw message when formatting fails (JUL treats messages as
 * MessageFormat patterns only when parameters are present).
 */
internal fun julMessage(record: LogRecord): String {
    val raw = record.message ?: ""
    val params = record.parameters ?: return raw
    if (params.isEmpty() || !raw.contains("{")) return raw
    return runCatching { MessageFormat.format(raw, *params) }.getOrDefault(raw)
}

/**
 * The JUL handler itself: maps the level, substitutes parameters, and adds
 * the logger name plus a thrown throwable's `toString()` as fields —
 * message text stays intact. Publishing never throws and never blocks
 * (it only buffers).
 */
private class DataflowLogHandler : Handler() {
    init {
        level = Level.ALL // the logger's own level already gates publish
    }

    override fun publish(record: LogRecord?) {
        if (record == null) return
        runCatching {
            val fields = LinkedHashMap<String, Any?>()
            val name = record.loggerName
            if (!name.isNullOrEmpty()) fields["logger"] = name
            record.thrown?.let { fields["exception"] = it.toString() }
            log(julLevelName(record.level), julMessage(record), fields)
        }
    }

    override fun flush() {}

    override fun close() {}
}
