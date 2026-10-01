package dev.huginnlabs.dataflow.kotlin

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path

/**
 * Route scanner tests: fixture Kotlin sources in a temp dir cover Ktor flat
 * and nested routes, Spring annotations (with class-level prefixes), and
 * Micronaut controllers, plus the wire JSON, skip rules, cap, URL
 * resolution and the --print CLI path. No network: posting is never reached.
 */
class ScanCliTest {

    @field:TempDir
    lateinit var root: Path

    private fun write(rel: String, content: String): File {
        val f = root.resolve(rel).toFile()
        f.parentFile.mkdirs()
        f.writeText(content.trimIndent() + "\n")
        return f
    }

    private fun scan(): List<ScannedRoute> = scanRoutes(root.toFile()).routes

    private fun find(routes: List<ScannedRoute>, method: String, path: String): ScannedRoute =
        routes.firstOrNull { it.method == method && it.path == path }
            // kotlin.error qualified: the SDK's log-shipping error() shadows it in this package
            ?: kotlin.error("no $method $path in ${routes.map { it.method + " " + it.path }}")

    // ------------------------------------------------------------------
    // Ktor
    // ------------------------------------------------------------------

    @Test
    fun `ktor flat routes with all verbs and handlers`() {
        write(
            "src/App.kt",
            """
            package demo

            import io.ktor.server.routing.*
            import io.ktor.server.response.*

            // get("/commented-out") { }   — comments are never reported
            /* post("/block-commented") { } */

            fun Route.orderRoutes() {
                get("/orders") { call.respond(emptyList<Int>()) }
                post("/orders") { call.respondText("created") }
                put("/orders/{id}") { call.respondText("updated") }
                delete("/orders/{id}") { call.respondText("deleted") }
                patch("/orders/{id}") { call.respondText("patched") }
                head("/orders") { }
                options("/orders") { }
            }
            """,
        )

        val routes = scan()
        assertEquals(7, routes.size)
        assertEquals("GET", find(routes, "GET", "/orders").method)
        assertEquals("orderRoutes", find(routes, "GET", "/orders").handler)
        assertEquals("src/App.kt", find(routes, "GET", "/orders").sourceFile)
        assertEquals("orderRoutes", find(routes, "POST", "/orders").handler)
        // Path params keep the {id} syntax as written.
        assertEquals("/orders/{id}", find(routes, "PUT", "/orders/{id}").path)
        assertEquals("/orders/{id}", find(routes, "DELETE", "/orders/{id}").path)
        assertEquals("/orders/{id}", find(routes, "PATCH", "/orders/{id}").path)
        assertEquals("orderRoutes", find(routes, "HEAD", "/orders").handler)
        assertEquals("orderRoutes", find(routes, "OPTIONS", "/orders").handler)
        assertTrue(routes.none { it.path.contains("commented") }, "commented routes must be skipped")
    }

    @Test
    fun `nested ktor route prefixes apply and close with braces`() {
        write(
            "src/Routes.kt",
            """
            fun api() = routing {
                route("/api") {
                    get("/users") { }
                    post("/users") { }
                }
                get("/health") { }
            }

            fun main() {
                routing {
                    route("/v1") {
                        route("/orders") {
                            get("/") { }
                        }
                    }
                }
            }
            """,
        )

        val routes = scan()
        assertEquals(4, routes.size)
        assertEquals("/api/users", find(routes, "GET", "/api/users").path)
        assertEquals("api", find(routes, "GET", "/api/users").handler)
        assertEquals("/api/users", find(routes, "POST", "/api/users").path)
        // Outside the route("/api") block the prefix no longer applies.
        assertEquals("/health", find(routes, "GET", "/health").path)
        // Stacked prefixes and a root path under them.
        assertEquals("/v1/orders", find(routes, "GET", "/v1/orders").path)
    }

    @Test
    fun `routes in strings and member calls are ignored`() {
        write(
            "src/Tricky.kt",
            """
            val raw = ""${'"'}get("/fake") { }""${'"'}
            val template = "post(\"/also-fake\")"
            val cache = mapOf<String, Int>().get("/still-fake") ?: 0

            fun real() = routing {
                get("/real") { }
            }
            """,
        )

        val routes = scan()
        assertEquals(listOf("GET /real"), routes.map { it.method + " " + it.path })
        assertEquals("real", routes[0].handler)
    }

    // ------------------------------------------------------------------
    // Spring
    // ------------------------------------------------------------------

    @Test
    fun `spring mappings with class-level prefix`() {
        write(
            "src/OrderRoutes.kt",
            """
            package demo

            import org.springframework.web.bind.annotation.*

            @RestController
            @RequestMapping("/api/orders")
            class OrderRoutes {
                @GetMapping("/{id}")
                fun one(@PathVariable id: Long): String = "one"

                @PostMapping
                fun create(): String = "created"

                @PutMapping("/{id}")
                fun update(): String = "updated"

                @DeleteMapping("/{id}")
                fun remove(): String = "deleted"

                @PatchMapping("/{id}")
                fun patch(): String = "patched"

                @RequestMapping(value = ["/search"], method = [RequestMethod.GET])
                fun search(): String = "results"

                @RequestMapping("/meta")
                fun meta(): String = "meta"
            }
            """,
        )

        val routes = scan()
        assertEquals(7, routes.size)
        assertEquals("OrderRoutes.one", find(routes, "GET", "/api/orders/{id}").handler)
        assertEquals("OrderRoutes.create", find(routes, "POST", "/api/orders").handler)
        assertEquals("OrderRoutes.update", find(routes, "PUT", "/api/orders/{id}").handler)
        assertEquals("OrderRoutes.remove", find(routes, "DELETE", "/api/orders/{id}").handler)
        assertEquals("OrderRoutes.patch", find(routes, "PATCH", "/api/orders/{id}").handler)
        assertEquals("OrderRoutes.search", find(routes, "GET", "/api/orders/search").handler)
        // Method-less @RequestMapping reports ANY (matches every verb), like the Java scanner.
        assertEquals("ANY", find(routes, "ANY", "/api/orders/meta").method)
        assertEquals("OrderRoutes.meta", find(routes, "ANY", "/api/orders/meta").handler)
        assertEquals("src/OrderRoutes.kt", find(routes, "GET", "/api/orders/{id}").sourceFile)
    }

    @Test
    fun `multi-line spring annotation arguments are joined`() {
        write(
            "src/Items.kt",
            """
            @RestController
            @RequestMapping(
                value = ["/api/items"],
                produces = ["application/json"]
            )
            class ItemRoutes {
                @GetMapping(
                    "/{id}"
                )
                fun one(): String = "item"
            }
            """,
        )

        val routes = scan()
        assertEquals(1, routes.size)
        assertEquals("GET", find(routes, "GET", "/api/items/{id}").method)
        assertEquals("ItemRoutes.one", find(routes, "GET", "/api/items/{id}").handler)
    }

    // ------------------------------------------------------------------
    // Micronaut
    // ------------------------------------------------------------------

    @Test
    fun `micronaut controller with verb annotations`() {
        write(
            "src/UserRoutes.kt",
            """
            import io.micronaut.http.annotation.*

            @Controller("/users")
            class UserRoutes {
                @Get
                fun list(): List<String> = emptyList()

                @Post("/{id}")
                fun update(id: String): String = id

                @Delete
                fun remove(): String = "deleted"
            }
            """,
        )

        val routes = scan()
        assertEquals(3, routes.size)
        assertEquals("UserRoutes.list", find(routes, "GET", "/users").handler)
        assertEquals("UserRoutes.update", find(routes, "POST", "/users/{id}").handler)
        assertEquals("UserRoutes.remove", find(routes, "DELETE", "/users").handler)
    }

    // ------------------------------------------------------------------
    // Walk rules, cap, wire format
    // ------------------------------------------------------------------

    @Test
    fun `skips build dirs and test files`() {
        write(
            "src/main/kotlin/Real.kt",
            """
            fun real() = routing {
                get("/real") { }
            }
            """,
        )
        write(
            "build/generated/Gen.kt",
            """
            fun gen() = routing {
                get("/generated") { }
            }
            """,
        )
        write(
            "target/site/Site.kt",
            """
            fun site() = routing {
                get("/target") { }
            }
            """,
        )
        write(
            "src/test/kotlin/RealTest.kt",
            """
            fun testRoutes() = routing {
                get("/from-test") { }
            }
            """,
        )

        val result = scanRoutes(root.toFile())
        assertEquals(listOf("GET /real"), result.routes.map { it.method + " " + it.path })
        // Only Real.kt counts as scanned: Gen.kt and Site.kt live in skipped
        // dirs, RealTest.kt is a test file.
        assertEquals(1, result.files)
    }

    @Test
    fun `source files are relative with forward slashes`() {
        write(
            "src/a/b/Nested.kt",
            """
            fun n() = routing {
                get("/nested") { }
            }
            """,
        )
        assertEquals("src/a/b/Nested.kt", scan().single().sourceFile)
    }

    @Test
    fun `routes are capped at the server limit`() {
        val sb = StringBuilder("fun many() = routing {\n")
        repeat(MAX_ROUTES + 5) { sb.append("    get(\"/p$it\") { }\n") }
        sb.append("}\n")
        root.resolve("src").toFile().mkdirs()
        root.resolve("src/Many.kt").toFile().writeText(sb.toString())

        val result = scanRoutes(root.toFile())
        assertEquals(MAX_ROUTES, result.routes.size)
    }

    @Test
    fun `catalog JSON matches the server contract`() {
        val body = buildCatalogJSON(
            "order-service",
            listOf(
                ScannedRoute("GET", "/api/orders/{id}", "OrderRoutes.one", "src/OrderRoutes.kt"),
                ScannedRoute("ANY", "/api/orders/meta", "OrderRoutes.meta", "src/OrderRoutes.kt"),
            ),
        )
        assertEquals(
            "{\"service_name\":\"order-service\",\"routes\":[" +
                "{\"method\":\"GET\",\"path\":\"/api/orders/{id}\",\"handler\":\"OrderRoutes.one\",\"source_file\":\"src/OrderRoutes.kt\"}," +
                "{\"method\":\"ANY\",\"path\":\"/api/orders/meta\",\"handler\":\"OrderRoutes.meta\",\"source_file\":\"src/OrderRoutes.kt\"}" +
                "]}",
            body,
        )
    }

    // ------------------------------------------------------------------
    // CLI plumbing
    // ------------------------------------------------------------------

    @Test
    fun `url resolution follows flag then env then URL-form endpoint`() {
        assertEquals("http://a", resolveCatalogURL("http://a/", null, null))
        assertEquals("http://flag", resolveCatalogURL("http://flag", "http://env", "http://ep"))
        assertEquals("http://env", resolveCatalogURL(null, "http://env", "http://ep"))
        assertEquals("http://ep", resolveCatalogURL(null, null, "http://ep"))
        assertEquals("https://ep", resolveCatalogURL(null, "", "https://ep"))
        // A bare host:port gRPC endpoint has no derivable HTTP base.
        assertNull(resolveCatalogURL(null, null, "127.0.0.1:4317"))
        assertNull(resolveCatalogURL(null, null, null))
        assertNull(resolveCatalogURL("  ", "", "host:7"))
    }

    @Test
    fun `print prints the catalog JSON and skips posting`() {
        write(
            "src/App.kt",
            """
            fun routes() = routing {
                get("/orders") { }
            }
            """,
        )
        val out = mutableListOf<String>()
        val err = mutableListOf<String>()

        val code = runScan(
            arrayOf("--dir", root.toString(), "--service", "print-svc", "--print"),
            env = { null },
            out = out::add,
            err = err::add,
        )

        assertEquals(0, code)
        assertEquals(1, out.size)
        assertEquals(
            "{\"service_name\":\"print-svc\",\"routes\":[" +
                "{\"method\":\"GET\",\"path\":\"/orders\",\"handler\":\"routes\",\"source_file\":\"src/App.kt\"}]}",
            out[0],
        )
        assertTrue(err.isEmpty(), "diagnostics must not pollute stdout JSON: $err")
    }

    @Test
    fun `usage errors return exit code two`() {
        val out = mutableListOf<String>()
        val err = mutableListOf<String>()

        assertEquals(2, runScan(arrayOf("--dir", root.toString()), env = { null }, out = out::add, err = err::add))
        assertTrue(err.first().contains("--service is required"))

        assertEquals(2, runScan(arrayOf("--bogus"), env = { null }, out = out::add, err = err::add))
        assertTrue(err.any { it.contains("unknown argument") })

        assertEquals(
            2,
            runScan(
                arrayOf("--dir", root.resolve("missing").toString(), "--service", "s", "--print"),
                env = { null },
                out = out::add,
                err = err::add,
            ),
        )
        assertTrue(err.any { it.contains("not a directory") })

        assertThrows<IllegalArgumentException> { parseArgs(arrayOf("--service")) }
        assertNotNull(parseArgs(arrayOf("--dir", "/tmp", "--print")))
    }

    @Test
    fun `missing http base is skipped with the bare-endpoint message`() {
        val out = mutableListOf<String>()
        val err = mutableListOf<String>()

        // Bare host:port endpoint: recognized but unusable for HTTP.
        val code = runScan(
            arrayOf("--dir", root.toString(), "--service", "s"),
            env = { k -> if (k == "DATAFLOW_ENDPOINT") "127.0.0.1:4317" else null },
            out = out::add,
            err = err::add,
        )

        assertEquals(1, code)
        assertTrue(out.isEmpty())
        assertTrue(err.single().contains("bare host:port"))
        assertTrue(err.single().contains("gRPC-only"))
    }
}
