package dev.huginnlabs.dataflow.kotlin

import dev.huginnlabs.dataflow.Dataflow
import dev.huginnlabs.dataflow.Span

/**
 * Idiomatic Kotlin layer over the Java Dataflow SDK.
 *
 * ```
 * configure()
 *
 * trace("market.Checkout") { root ->
 *     root.data("cart", cart.id)
 *     trace("cart.Total") { it.data("items", items.size) }
 * }
 * ```
 *
 * Span members (`data`, `attr`, `status`, `callee`, `recordError`) are
 * used directly. Span scoping follows the calling thread; inside
 * coroutines pass spans explicitly across dispatcher hops — the Java core
 * keeps context in a ThreadLocal by design.
 */

/** Configures the SDK from DATAFLOW_* environment variables. */
fun configure() = Dataflow.configure()

/**
 * Runs [block] inside a named span, joining the current trace. Errors
 * thrown from [block] are recorded on the span before it ends.
 */
inline fun <T> trace(name: String, block: (Span) -> T): T {
    val t = Dataflow.trace(name)
    try {
        return block(t.span())
    } catch (e: Throwable) {
        t.span().recordError(e)
        throw e
    } finally {
        t.close()
    }
}

/** Alias of [trace] for nested measurement points. */
inline fun <T> span(name: String, block: (Span) -> T): T = trace(name, block)
