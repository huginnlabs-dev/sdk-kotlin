package dev.huginnlabs.dataflow.kotlin

import dev.huginnlabs.dataflow.Dataflow
import dev.huginnlabs.dataflow.Span

/**
 * Crash capture: [capture] and the default uncaught-exception hook turn
 * crashes into recorded spans without ever changing control flow.
 *
 * Wire convention: the failing span — the current one when a trace is
 * active, otherwise a synthetic `exception` span — gets status 500,
 * `error_message` = `throwable.toString()` (capped at 500 chars) and an
 * `error.stack` attribute carrying the stack trace (whole lines from the
 * top, capped at 8192 chars). The exception is always rethrown; the
 * uncaught hook always chains to the handler installed before it.
 *
 * The stack rides the public `Span.attr` API (metadata), so no raw-field
 * fallback is needed — unlike the DB_QUERY type in [Transport.kt].
 */

/** `error_message` cap, applied to `throwable.toString()`. */
internal const val CRASH_MESSAGE_CAP = 500

/** `error.stack` cap, applied to the top stack lines. */
internal const val CRASH_STACK_CAP = 8192

/** Metadata key carrying the stack trace. */
internal const val ERROR_STACK_ATTR = "error.stack"

/** Synthetic span names for crashes outside any active trace. */
@PublishedApi
internal const val CRASH_SPAN_NAME = "exception"
internal const val UNCAUGHT_SPAN_NAME = "uncaught exception"

/**
 * Test seam: the span the last crash was recorded on (null when recording
 * was skipped or the SDK is disabled). Reset it between assertions.
 */
@Volatile
internal var lastCrashSpan: Span? = null

/**
 * Runs [block] and records a thrown [Throwable] on the current span — or a
 * synthetic `exception` span when no trace is active — before rethrowing it
 * unchanged:
 *
 * ```
 * capture {
 *     chargeCard(order)   // a crash lands on the span, then propagates
 * }
 * ```
 *
 * Best-effort: recording failures never mask the original exception, and
 * with the SDK disabled this is a pure passthrough.
 */
inline fun <T> capture(block: () -> T): T {
    if (!sdkEnabled()) return block()
    try {
        return block()
    } catch (t: Throwable) {
        runCatching { recordCrash(CRASH_SPAN_NAME, t) }
        throw t
    }
}

// ---------------------------------------------------------------------------
// Recording: current-or-synthetic span, status 500 + message + error.stack.
// ---------------------------------------------------------------------------

/**
 * True when the core is configured and shipping spans; never throws (an
 * unusable core must not break the application's error path).
 */
@PublishedApi
internal fun sdkEnabled(): Boolean =
    runCatching { Dataflow.enabled() }.getOrDefault(false)

/**
 * Records a crash on the current span, or on a fresh synthetic span named
 * [syntheticName] when no trace is active (which is ended immediately —
 * it has no enclosing scope). Returns the span recorded on, or null when
 * the SDK is disabled or the core is unavailable.
 */
@PublishedApi
internal fun recordCrash(syntheticName: String, t: Throwable): Span? {
    if (!sdkEnabled()) return null
    val current = runCatching { Span.current() }.getOrNull()
    val span = current
        ?: runCatching { Dataflow.startSpan(syntheticName, "FUNCTION_CALL") }.getOrNull()
        ?: return null
    span.status(500)
    span.recordError(crashMessage(t))
    span.attr(ERROR_STACK_ATTR, capStack(t.stackTraceToString(), CRASH_STACK_CAP))
    if (current == null) runCatching { span.end() }
    lastCrashSpan = span
    return span
}

/** `throwable.toString()` capped at [CRASH_MESSAGE_CAP] chars. */
internal fun crashMessage(t: Throwable): String = clipTo(t.toString(), CRASH_MESSAGE_CAP)

/**
 * Stack trace capped at [cap] characters, keeping whole lines from the top
 * — the header and innermost frames land first, which is where the cause
 * lives; deep `Caused by` tails are the first to go.
 */
internal fun capStack(stack: String, cap: Int): String {
    if (stack.length <= cap) return stack
    val out = StringBuilder(cap)
    for (line in stack.lineSequence()) {
        val need = line.length + if (out.isEmpty()) 0 else 1
        if (out.length + need > cap) break
        if (out.isNotEmpty()) out.append('\n')
        out.append(line)
    }
    return out.toString()
}

/** Truncates to [max] characters. */
private fun clipTo(s: String, max: Int): String =
    if (s.length <= max) s else s.substring(0, max)

// ---------------------------------------------------------------------------
// Uncaught exceptions: default-handler wrapper that records, then chains to
// the handler installed before it. Idempotent in both directions.
// ---------------------------------------------------------------------------

/** Guards [installedHandler] / [previousHandler]. */
private val uncaughtLock = Any()

/** The handler this SDK installed; null while captureUncaught is not active. */
private var installedHandler: Thread.UncaughtExceptionHandler? = null

/** The default handler captured at install time; chained after recording. */
private var previousHandler: Thread.UncaughtExceptionHandler? = null

/**
 * Installs a default uncaught-exception handler that records the crash on a
 * synthetic `uncaught exception` span and then chains to the handler that
 * was installed before (the JVM default or a framework's hook) — nothing is
 * swallowed. Idempotent: a second call is a no-op.
 */
fun captureUncaught() {
    synchronized(uncaughtLock) {
        if (installedHandler != null) return
        val ours = Thread.UncaughtExceptionHandler { thread, error -> uncaughtHook(thread, error) }
        previousHandler = runCatching { Thread.getDefaultUncaughtExceptionHandler() }.getOrNull()
        runCatching { Thread.setDefaultUncaughtExceptionHandler(ours) }
        installedHandler = ours
    }
}

/**
 * Removes the handler installed by [captureUncaught], restoring the default
 * that was active before it — unless a third party replaced it in the
 * meantime, in which case that handler is left untouched. Idempotent: a
 * no-op when nothing is installed.
 */
fun ignoreUncaught() {
    synchronized(uncaughtLock) {
        val ours = installedHandler ?: return
        installedHandler = null
        val prev = previousHandler
        previousHandler = null
        val current = runCatching { Thread.getDefaultUncaughtExceptionHandler() }.getOrNull()
        if (current === ours) runCatching { Thread.setDefaultUncaughtExceptionHandler(prev) }
    }
}

/**
 * The uncaught-exception pipeline: record first, then chain to the handler
 * that was installed before [captureUncaught]. Recording is best-effort and
 * never prevents the chain; with the SDK disabled this is pure passthrough.
 */
internal fun uncaughtHook(thread: Thread, error: Throwable) {
    runCatching { recordCrash(UNCAUGHT_SPAN_NAME, error) }
    val prev = synchronized(uncaughtLock) { previousHandler }
    if (prev != null) runCatching { prev.uncaughtException(thread, error) }
}
