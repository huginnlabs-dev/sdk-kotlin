package dev.huginnlabs.dataflow.kotlin

import dev.huginnlabs.dataflow.Dataflow
import dev.huginnlabs.dataflow.Span
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.ApplicationPlugin
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.application.hooks.CallFailed
import io.ktor.server.application.hooks.ResponseSent
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import io.ktor.server.request.queryString
import io.ktor.server.response.header
import io.ktor.server.routing.HttpMethodRouteSelector
import io.ktor.server.routing.PathSegmentConstantRouteSelector
import io.ktor.server.routing.PathSegmentOptionalParameterRouteSelector
import io.ktor.server.routing.PathSegmentParameterRouteSelector
import io.ktor.server.routing.PathSegmentTailcardRouteSelector
import io.ktor.server.routing.PathSegmentWildcardRouteSelector
import io.ktor.server.routing.RootRouteSelector
import io.ktor.server.routing.Route
import io.ktor.server.routing.Routing
import io.ktor.server.routing.TrailingSlashRouteSelector
import io.ktor.util.AttributeKey
import java.lang.reflect.Field
import java.lang.reflect.Method

/**
 * Ktor server middleware: one `HTTP_SERVER` span per request, layered over
 * the Java core's delivery pipeline (Ktor 2.x, `createApplicationPlugin`).
 *
 * - `onCall` — opens the span, named `"METHOD path"` (`"GET /orders"`),
 *   stamps `http.*` metadata, injects `X-Dataflow-Trace-Id` into the
 *   response headers, adopts an incoming `X-Dataflow-Trace-Id` request
 *   header (same propagation as the Java core's filter) and activates the
 *   span so `trace { }` inside handlers joins it.
 * - `Routing.RoutingCallStarted` — records the matched pattern; after
 *   routing resolves, the span name is upgraded from path to route
 *   (`"GET /orders/7/items"` → `"GET /orders/{id}/items"`).
 * - `ResponseSent` — records the response status (`http.status_code`,
 *   5xx also recorded as an error, like the Java filter) and ends the span.
 * - `CallFailed` — records status 500, the exception and an `error.stack`
 *   attribute (same wire shape as [capture]) before the exception
 *   propagates to the engine unchanged.
 *
 * Best-effort by contract, like everything else in this SDK: when the SDK
 * is disabled the middleware is inert, and no instrumentation failure ever
 * breaks the application's call.
 *
 * ```
 * install(ktorMiddleware)
 * routing { get("/orders/{id}") { ... } }
 * ```
 *
 * Note: span activation rides the JVM core's ThreadLocal — handler code
 * that hops to another dispatcher opens traces of its own unless the span
 * is passed explicitly (see the Coroutines section in the README).
 */

/** Installable Ktor plugin: `install(ktorMiddleware)` (Ktor 2.x API). */
val ktorMiddleware: ApplicationPlugin<Unit> = createApplicationPlugin("DataflowKtor") {
    application.environment.monitor.subscribe(Routing.RoutingCallStarted) { call ->
        // Route resolved; remember the pattern for the name upgrade. The
        // routing call shares the engine call's attribute map, so both the
        // ResponseSent and CallFailed hooks can read it back.
        runCatching { routePathOf(call.route) }
            .getOrNull()
            ?.let { call.attributes.put(ROUTE_KEY, it) }
    }
    onCall { call -> openServerSpan(call) }
    on(ResponseSent) { call -> finishServerSpan(call) }
    on(CallFailed) { call, cause -> failServerSpan(call, cause) }
}

/** Attribute key carrying the open server span across the pipeline phases. */
private val SPAN_KEY = AttributeKey<Span>("dataflow.kotlin.span")

/** Attribute key carrying the matched route pattern (set after routing). */
private val ROUTE_KEY = AttributeKey<String>("dataflow.kotlin.route")

/**
 * Test seam: the span the middleware last opened (null when opening was
 * skipped or the SDK is disabled). Reset it between assertions.
 */
@Volatile
internal var lastServerSpan: Span? = null

// ---------------------------------------------------------------------------
// Span lifecycle: open on call, finish on response, fail on exception
// ---------------------------------------------------------------------------

/**
 * Opens the `HTTP_SERVER` span for [call] (`"METHOD path"`, upgraded to the
 * route pattern once routing resolves), joins the trace from an incoming
 * `X-Dataflow-Trace-Id` header, injects the same header into the response,
 * and activates the span. A no-op when the SDK is disabled or anything goes
 * wrong — instrumentation must never fail the request.
 */
private fun openServerSpan(call: ApplicationCall) {
    if (!sdkEnabled()) return
    val method = runCatching { call.request.httpMethod.value }.getOrDefault("")
    val path = runCatching { call.request.path() }.getOrDefault("")
    if (method.isEmpty()) return
    val span = runCatching { Dataflow.startServerSpan(method + " " + path) }.getOrNull() ?: return
    lastServerSpan = span
    runCatching {
        call.request.headers[TRACE_HEADER]?.takeIf { it.isNotEmpty() }?.let { joinTrace(span, it) }
        span.attr("http.method", method)
        span.attr("http.path", path)
        val query = runCatching { call.request.queryString() }.getOrDefault("")
        if (query.isNotEmpty()) span.attr("http.query", query)
        span.attr("agent.sdk", "kotlin-sdk/$SDK_VERSION")
        call.response.header(TRACE_HEADER, span.traceId())
        call.attributes.put(SPAN_KEY, span)
        activate(span)
    }
}

/**
 * Ends [call]'s span after the response is sent: upgrades the name to the
 * matched route pattern when routing resolved one, records the response
 * status (5xx also as an error, mirroring the Java core's filter) and
 * deactivates. Missing state or failures are silently ignored.
 */
private fun finishServerSpan(call: ApplicationCall) {
    val span = runCatching { call.attributes.getOrNull(SPAN_KEY) }.getOrNull() ?: return
    runCatching {
        upgradeName(span, call)
        val status = runCatching { call.response.status()?.value }.getOrNull() ?: 0
        span.status(status)
        span.attr("http.status_code", status.toString())
        if (status >= 500) span.recordError("http " + status)
    }
    runCatching { span.end() }
    deactivate(span)
}

/**
 * Records a failed call on [call]'s span: status 500, the exception and an
 * `error.stack` attribute (same wire shape as [capture]), then ends the
 * span. Never swallows anything — the exception keeps propagating to the
 * engine untouched; when the response was already sent this only records.
 */
private fun failServerSpan(call: ApplicationCall, cause: Throwable) {
    val span = runCatching { call.attributes.getOrNull(SPAN_KEY) }.getOrNull() ?: return
    runCatching {
        upgradeName(span, call)
        span.status(500)
        span.recordError(crashMessage(cause))
        span.attr(ERROR_STACK_ATTR, capStack(cause.stackTraceToString(), CRASH_STACK_CAP))
    }
    runCatching { span.end() }
    deactivate(span)
}

/**
 * Replaces the span's path-qualified name with the route pattern one when a
 * matched route is known (`"GET /orders/7/items"` → `"GET /orders/{id}/items"`).
 * `Span.name` is private in the java core — set reflectively (same bridge
 * pattern as the delivery path in [Transport.kt]).
 */
private fun upgradeName(span: Span, call: ApplicationCall) {
    val route = runCatching { call.attributes.getOrNull(ROUTE_KEY) }.getOrNull() ?: return
    val current = spanNameOf(span) ?: return
    val space = current.indexOf(' ')
    if (space <= 0) return
    val nameField = coreBridge.nameField ?: return
    runCatching { nameField.set(span, current.substring(0, space) + " " + route) }
}

/**
 * The route pattern for [route]: path selectors from the matched node up to
 * the root, parameters kept in braces (`"/orders/{id}/items"`). Method and
 * trailing-slash selectors are skipped; any unexpected selector shape (or an
 * empty walk) yields null — the caller keeps the request-path name.
 */
internal fun routePathOf(route: Route): String? {
    val segments = ArrayList<String>(8)
    var node: Route? = route
    var hops = 0
    while (node != null && hops++ < 64) {
        when (val selector = node.selector) {
            is RootRouteSelector -> break
            is HttpMethodRouteSelector, is TrailingSlashRouteSelector -> {}
            is PathSegmentConstantRouteSelector -> segments.add(selector.value)
            is PathSegmentParameterRouteSelector -> segments.add("{" + selector.name + "}")
            is PathSegmentOptionalParameterRouteSelector -> segments.add("{" + selector.name + "?}")
            is PathSegmentWildcardRouteSelector -> segments.add("{*}")
            is PathSegmentTailcardRouteSelector -> segments.add("{...}")
            else -> return null
        }
        node = node.parent
    }
    if (segments.isEmpty()) return null
    return "/" + segments.reversed().joinToString("/")
}

/** A span's private name field read back for the route upgrade. */
private fun spanNameOf(span: Span): String? =
    runCatching { coreBridge.nameField?.get(span) as? String }.getOrNull()

// --- memoized reflective bridges into the java core's package-private span scope ---

private fun joinTrace(span: Span, id: String) {
    runCatching { coreBridge.joinTrace?.invoke(span, id) }
}

private fun activate(span: Span) {
    runCatching { coreBridge.activate?.invoke(span) }
}

private fun deactivate(span: Span) {
    runCatching { coreBridge.deactivate?.invoke(span) }
}

private val coreBridge by lazy {
    object {
        val joinTrace: Method? = runCatching {
            Span::class.java.getDeclaredMethod("joinTrace", String::class.java)
                .apply { isAccessible = true }
        }.getOrNull()

        val activate: Method? = runCatching {
            Span::class.java.getDeclaredMethod("activate")
                .apply { isAccessible = true }
        }.getOrNull()

        val deactivate: Method? = runCatching {
            Span::class.java.getDeclaredMethod("deactivate")
                .apply { isAccessible = true }
        }.getOrNull()

        val nameField: Field? = runCatching {
            Span::class.java.getDeclaredField("name")
                .apply { isAccessible = true }
        }.getOrNull()
    }
}
