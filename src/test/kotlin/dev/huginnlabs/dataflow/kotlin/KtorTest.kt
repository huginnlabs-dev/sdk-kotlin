package dev.huginnlabs.dataflow.kotlin

import dev.huginnlabs.dataflow.Dataflow
import dev.huginnlabs.dataflow.Span
import io.ktor.client.request.get
import io.ktor.client.request.headers
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Ktor middleware tests over the real test host (`testApplication`, CIO):
 * one HTTP_SERVER span per call, path → route name upgrade, status recording
 * (404 and handler-crash 500 + error.stack), trace-id propagation in both
 * directions, inner `trace { }` joining the server span, and the disabled
 * no-op. Span internals are read reflectively (private in the java core);
 * emission itself is only smoke-checked — the pipeline is down in every
 * test and recording must never mind.
 */
class KtorTest {

    private fun enableSdk() {
        Dataflow.configure(
            Dataflow.Builder()
                .endpoint("127.0.0.1:1")
                .apiKey("test-key")
                .serviceName("ktor-test"),
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

    @Test
    fun `middleware opens one server span per call with path name and trace header`() {
        enableSdk()
        lastServerSpan = null
        testApplication {
            application {
                install(ktorMiddleware)
                routing { get("/hello") { call.respondText("ok") } }
            }

            val response = client.get("/hello")

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("ok", response.bodyAsText())
            val span = lastServerSpan
            assertNotNull(span, "a server span must open per call")
            assertEquals("GET /hello", field(span!!, "name"))
            assertEquals(200, field(span, "statusCode"))
            assertEquals("HTTP_SERVER", field(span, "type"))
            assertEquals(32, span.traceId().length, "trace id is a 32-char uuid")
            assertEquals(span.traceId(), response.headers[TRACE_HEADER], "trace id rides the response")
            assertEquals("GET", meta(span, "http.method"))
            assertEquals("/hello", meta(span, "http.path"))
            assertEquals("200", meta(span, "http.status_code"))
        }
    }

    @Test
    fun `span name upgrades to the matched route pattern`() {
        enableSdk()
        lastServerSpan = null
        testApplication {
            application {
                install(ktorMiddleware)
                routing { get("/orders/{id}/items") { call.respondText("[]") } }
            }

            val response = client.get("/orders/7/items")

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("GET /orders/{id}/items", field(lastServerSpan!!, "name"))
        }
    }

    @Test
    fun `nested route pattern composes base and sub path`() {
        enableSdk()
        lastServerSpan = null
        testApplication {
            application {
                install(ktorMiddleware)
                routing {
                    route("/api/v1") {
                        post("/orders") { call.respondText("created") }
                    }
                }
            }

            val response = client.post("/api/v1/orders")

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("POST /api/v1/orders", field(lastServerSpan!!, "name"))
        }
    }

    @Test
    fun `unmatched request keeps the path name and records the 404`() {
        enableSdk()
        lastServerSpan = null
        testApplication {
            application {
                install(ktorMiddleware)
                routing { get("/hello") { call.respondText("ok") } }
            }

            val response = client.get("/missing")

            assertEquals(HttpStatusCode.NotFound, response.status)
            assertEquals("GET /missing", field(lastServerSpan!!, "name"))
            assertEquals(404, field(lastServerSpan!!, "statusCode"))
        }
    }

    @Test
    fun `query string rides as http query metadata`() {
        enableSdk()
        lastServerSpan = null
        testApplication {
            application {
                install(ktorMiddleware)
                routing { get("/search") { call.respondText("hits") } }
            }

            client.get("/search?q=kotlin&page=2")

            assertEquals("q=kotlin&page=2", meta(lastServerSpan!!, "http.query"))
        }
    }

    @Test
    fun `handler exception records 500 and error stack, then propagates unchanged`() {
        enableSdk()
        lastServerSpan = null
        val boom = IllegalStateException("boom")
        testApplication {
            application {
                install(ktorMiddleware)
                routing { get("/boom") { throw boom } }
            }

            // The test host surfaces the unhandled route exception to the
            // client (re-created across the test transport) — the middleware
            // must not swallow it; a production engine answers 500 from the
            // same unhandled path.
            val err = assertThrows<IllegalStateException> { client.get("/boom") }
            assertTrue(err.message!!.contains("boom"), "exception must propagate unchanged")

            val span = lastServerSpan
            assertNotNull(span, "the failed call still records its span")
            assertEquals(500, field(span!!, "statusCode"))
            assertEquals("java.lang.IllegalStateException: boom", field(span, "errorMessage"))
            val stack = meta(span, "error.stack")
            assertNotNull(stack, "error.stack attribute must be set")
            assertTrue(stack!!.startsWith("java.lang.IllegalStateException: boom"))
            assertTrue(stack.contains("\tat "), "stack must carry frame lines")
        }
    }

    @Test
    fun `incoming trace header joins the server trace`() {
        enableSdk()
        lastServerSpan = null
        testApplication {
            application {
                install(ktorMiddleware)
                routing { get("/join") { call.respondText("ok") } }
            }

            client.get("/join") {
                headers { append(TRACE_HEADER, "trace-abc-123") }
            }

            assertEquals("trace-abc-123", lastServerSpan!!.traceId(), "span must adopt the incoming trace id")
        }
    }

    @Test
    fun `inner trace inside a handler joins the server span`() {
        enableSdk()
        lastServerSpan = null
        var inner: Span? = null
        testApplication {
            application {
                install(ktorMiddleware)
                routing {
                    get("/work") {
                        trace("app.Work") { inner = it }
                        call.respondText("ok")
                    }
                }
            }

            client.get("/work")

            val server = lastServerSpan
            val innerSpan = inner
            assertNotNull(server)
            assertNotNull(innerSpan, "the inner trace must have opened")
            assertEquals(
                server!!.spanId(),
                field(innerSpan!!, "parentSpanId"),
                "inner span is a child of the server span",
            )
            assertEquals(server.traceId(), innerSpan.traceId(), "inner span shares the trace")
        }
    }

    @Test
    fun `disabled sdk - middleware is a no-op`() {
        disableSdk()
        lastServerSpan = null
        testApplication {
            application {
                install(ktorMiddleware)
                routing { get("/hello") { call.respondText("ok") } }
            }

            val response = client.get("/hello")

            assertEquals(HttpStatusCode.OK, response.status, "the call must be served normally")
            assertEquals("ok", response.bodyAsText())
            assertNull(lastServerSpan, "no span when the SDK is disabled")
            assertNull(response.headers[TRACE_HEADER], "no trace header when disabled")
        }
    }
}
