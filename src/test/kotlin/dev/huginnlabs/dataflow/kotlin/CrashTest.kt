package dev.huginnlabs.dataflow.kotlin

import dev.huginnlabs.dataflow.Dataflow
import dev.huginnlabs.dataflow.Span
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Crash capture tests: recording + rethrow on the current span, synthetic
 * spans outside a trace, wire caps (message 500 / stack 8192 from the top),
 * uncaught-hook chaining and idempotency, and disabled-SDK passthrough.
 * Span internals are read reflectively (they are private in the java
 * core); emission itself is only smoke-checked — the pipeline is down in
 * every test and recording must never mind.
 */
class CrashTest {

    private fun enableSdk() {
        Dataflow.configure(
            Dataflow.Builder()
                .endpoint("127.0.0.1:1")
                .apiKey("test-key")
                .serviceName("crash-test"),
        )
    }

    private fun disableSdk() {
        Dataflow.configure(Dataflow.Builder().endpoint("127.0.0.1:1").apiKey("k").disabled(true))
    }

    // Span fields are private in the java core — read them reflectively.
    private fun field(span: Span, name: String): Any? = runCatching {
        Span::class.java.getDeclaredField(name).apply { isAccessible = true }.get(span)
    }.getOrNull()

    private fun meta(span: Span, key: String): String? =
        (field(span, "metadata") as? Map<*, *>)?.get(key) as? String

    // ------------------------------------------------------------------
    // capture: return value, record + rethrow, synthetic spans
    // ------------------------------------------------------------------

    @Test
    fun `capture returns the block value`() {
        enableSdk()
        assertEquals(7, capture { 7 })
        disableSdk()
        assertEquals(3, capture { 3 })
    }

    @Test
    fun `capture records on the current span and rethrows unchanged`() {
        enableSdk()
        lastCrashSpan = null
        val boom = RuntimeException("boom")
        trace("app.Work") { span ->
            val err = assertThrows<RuntimeException> { capture { throw boom } }
            assertSame(boom, err, "original exception must propagate untouched")
            assertEquals(500, field(span, "statusCode"))
            assertEquals("java.lang.RuntimeException: boom", field(span, "errorMessage"))
            val stack = meta(span, "error.stack")
            assertNotNull(stack, "error.stack attribute must be set")
            assertTrue(stack!!.startsWith("java.lang.RuntimeException: boom"))
            assertTrue(stack.contains("\tat "), "stack must carry frame lines")
            assertSame(span, lastCrashSpan, "recording went to the current span")
        }
    }

    @Test
    fun `capture opens a synthetic span when no trace is active`() {
        enableSdk()
        lastCrashSpan = null
        val boom = IllegalStateException("no current")

        val err = assertThrows<IllegalStateException> { capture { throw boom } }
        assertSame(boom, err)

        val span = lastCrashSpan
        assertNotNull(span, "synthetic exception span must be created")
        assertEquals("", field(span!!, "parentSpanId"), "synthetic span roots a fresh trace")
        assertEquals(500, field(span, "statusCode"))
        assertEquals("java.lang.IllegalStateException: no current", field(span, "errorMessage"))
        assertNotNull(meta(span, "error.stack"))
    }

    @Test
    fun `capture survives a broken pipeline and long stacks`() {
        enableSdk()
        lastCrashSpan = null
        val deep = RuntimeException("deep")
        // A deep stack exercises the cap against a live Throwable.
        val err = assertThrows<RuntimeException> {
            capture {
                try {
                    throw deep
                } catch (t: Throwable) {
                    throw RuntimeException(t)
                }
            }
        }
        assertSame(deep, err.cause, "cause chain must be preserved")
        assertNotNull(lastCrashSpan)
    }

    // ------------------------------------------------------------------
    // Wire caps: error_message 500, error.stack 8192 from the top
    // ------------------------------------------------------------------

    @Test
    fun `crashMessage caps throwable toString at 500`() {
        val big = RuntimeException("m".repeat(1000))
        val msg = crashMessage(big)
        assertEquals(500, msg.length)
        assertTrue(msg.startsWith("java.lang.RuntimeException: "))
        assertTrue(msg.startsWith("java.lang.RuntimeException: " + "m".repeat(400)))
        assertEquals("java.lang.RuntimeException: short", crashMessage(RuntimeException("short")))
    }

    @Test
    fun `capStack keeps top lines under the cap`() {
        val line = "x".repeat(1000)
        val stack = (1..40).joinToString("\n") { "\tat Frame$it($line)" }

        val capped = capStack(stack, CRASH_STACK_CAP)
        assertTrue(capped.length <= CRASH_STACK_CAP, "cap is hard")
        assertTrue(capped.startsWith("\tat Frame1("), "top lines are kept")
        assertFalse(capped.contains("Frame40"), "tail lines are dropped first")
        // Whole lines only: every output line is an intact input line.
        val original = stack.lines().toSet()
        capped.lines().forEach { l -> assertTrue(l in original, "whole lines only: $l") }

        assertEquals(stack, capStack(stack, stack.length))
        assertEquals("short", capStack("short", CRASH_STACK_CAP))
    }

    @Test
    fun `recorded crash respects both caps`() {
        enableSdk()
        lastCrashSpan = null
        val big = RuntimeException("e".repeat(2000))

        assertThrows<RuntimeException> { capture { throw big } }

        val span = lastCrashSpan
        assertNotNull(span)
        assertEquals(500, (field(span!!, "errorMessage") as String).length)
        val stack = meta(span, "error.stack")!!
        assertTrue(stack.length <= CRASH_STACK_CAP)
        assertTrue(stack.startsWith("java.lang.RuntimeException: "))
    }

    // ------------------------------------------------------------------
    // Uncaught exceptions: record, then chain; idempotent install/remove
    // ------------------------------------------------------------------

    @Test
    fun `captureUncaught records then chains to previous handler`() {
        val seen = ArrayList<String>()
        val prev = Thread.UncaughtExceptionHandler { _, _ ->
            seen.add("prev:" + (lastCrashSpan != null))
        }
        Thread.setDefaultUncaughtExceptionHandler(prev)
        try {
            captureUncaught()
            val ours = Thread.getDefaultUncaughtExceptionHandler()
            assertNotNull(ours)
            assertNotSame(prev, ours, "a wrapping handler must be installed")
            captureUncaught() // idempotent: no double wrap
            assertSame(ours, Thread.getDefaultUncaughtExceptionHandler())

            enableSdk()
            lastCrashSpan = null
            val boom = RuntimeException("uncaught")
            ours!!.uncaughtException(Thread.currentThread(), boom)

            // Recording happened BEFORE the chain, and the chain ran.
            assertEquals(listOf("prev:true"), seen)
            val span = lastCrashSpan
            assertNotNull(span, "uncaught crash must be recorded")
            assertEquals("", field(span!!, "parentSpanId"), "uncaught span is synthetic")
            assertEquals(500, field(span, "statusCode"))
            assertEquals("java.lang.RuntimeException: uncaught", field(span, "errorMessage"))
            assertNotNull(meta(span, "error.stack"))
        } finally {
            ignoreUncaught()
            Thread.setDefaultUncaughtExceptionHandler(null)
        }
    }

    @Test
    fun `ignoreUncaught restores previous handler and is idempotent`() {
        val prev = Thread.UncaughtExceptionHandler { _, _ -> }
        Thread.setDefaultUncaughtExceptionHandler(prev)
        try {
            captureUncaught()
            assertNotSame(prev, Thread.getDefaultUncaughtExceptionHandler())

            ignoreUncaught()
            assertSame(prev, Thread.getDefaultUncaughtExceptionHandler())
            ignoreUncaught() // second call is a no-op
            assertSame(prev, Thread.getDefaultUncaughtExceptionHandler())
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(null)
        }
    }

    // ------------------------------------------------------------------
    // Disabled SDK: pure passthrough everywhere
    // ------------------------------------------------------------------

    @Test
    fun `disabled sdk - capture and uncaught hook record nothing`() {
        disableSdk()
        lastCrashSpan = null
        val boom = IllegalStateException("down")

        val err = assertThrows<IllegalStateException> { capture { throw boom } }
        assertSame(boom, err)
        assertNull(lastCrashSpan, "no recording when disabled")

        val seen = ArrayList<String>()
        val prev = Thread.UncaughtExceptionHandler { _, _ -> seen.add("prev") }
        Thread.setDefaultUncaughtExceptionHandler(prev)
        try {
            captureUncaught()
            Thread.getDefaultUncaughtExceptionHandler()!!.uncaughtException(
                Thread.currentThread(),
                boom,
            )
            assertEquals(listOf("prev"), seen, "chain still runs when disabled")
            assertNull(lastCrashSpan, "no recording when disabled")
        } finally {
            ignoreUncaught()
            Thread.setDefaultUncaughtExceptionHandler(null)
        }
    }

    @Test
    fun `recordCrash is a no-op when disabled`() {
        disableSdk()
        lastCrashSpan = null
        assertNull(recordCrash(CRASH_SPAN_NAME, RuntimeException("x")))
        assertNull(lastCrashSpan)
    }
}
