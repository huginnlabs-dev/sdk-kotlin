package dev.huginnlabs.dataflow.kotlin

import dev.huginnlabs.dataflow.Dataflow
import dev.huginnlabs.dataflow.Json
import dev.huginnlabs.dataflow.Span
import java.io.File
import java.net.URI
import java.net.URL
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.jar.JarFile

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

/** SDK version, stamped into the service manifest. Kept in sync with pom.xml. */
const val SDK_VERSION = "0.4.0"

/** Upper bound on dependencies reported in the manifest (server caps at 500). */
private const val MAX_MANIFEST_DEPS = 500

/** Wire-contract string caps (service_name/others 128, dep name 256, version 64). */
private const val CAP_DEFAULT = 128
private const val CAP_DEP_NAME = 256
private const val CAP_DEP_VERSION = 64

/** Framework markers scanned in order; the first dependency name containing a marker wins. */
private val KNOWN_FRAMEWORKS = listOf(
    "spring-boot" to "spring-boot",
    "spring-web" to "spring",
    "micronaut" to "micronaut",
    "quarkus" to "quarkus",
    "vertx" to "vertx",
    "ktor" to "ktor",
    "jersey" to "jersey",
)

/** Guards the once-per-process manifest report. */
private val manifestSent = AtomicBoolean(false)

/**
 * Configures the SDK from DATAFLOW_* environment variables and reports the
 * service manifest once (best-effort, on a daemon thread — see
 * [sendManifest]).
 */
fun configure() {
    // The Kotlin wrapper owns the manifest: keep the JVM core silent so the
    // catalog reports one kotlin-language entry instead of a two-writer race.
    Dataflow.skipManifest()
    Dataflow.configure()
    sendManifest()
}

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

// ---------------------------------------------------------------------------
// Service manifest: one best-effort HTTP POST at startup describing this
// service (framework, runtime, dependency inventory from the classpath). The
// server turns it into the project's service catalog. Failures are silent —
// tracing never depends on the manifest reaching the server.
// ---------------------------------------------------------------------------

/**
 * Builds the manifest JSON body for POST `{http_base}/api/v1/manifest`:
 * service identity, runtime/framework profile and the dependency inventory
 * discovered from the running fat jar (`BOOT-INF/lib/`) or, failing that,
 * the `.jar` entries on `java.class.path`. Never throws.
 */
internal fun buildManifest(serviceName: String, sdkVersion: String): String {
    val deps = discoverDependencies()
    val m = LinkedHashMap<String, Any>()
    m["service_name"] = clip(serviceName, CAP_DEFAULT)
    m["language"] = "kotlin"
    m["sdk_version"] = clip(sdkVersion, CAP_DEFAULT)
    m["runtime_version"] = clip(
        (System.getProperty("java.version") ?: "unknown") + " / kotlin " + KotlinVersion.CURRENT,
        CAP_DEFAULT,
    )
    m["framework"] = clip(detectFramework(deps), CAP_DEFAULT)
    m["os_arch"] = clip(osArch(), CAP_DEFAULT)
    m["app_version"] = clip(System.getenv("DATAFLOW_APP_VERSION") ?: "", CAP_DEFAULT)
    m["dependencies"] = deps.map { (n, v) ->
        linkedMapOf<String, Any>("name" to clip(n, CAP_DEP_NAME), "version" to clip(v, CAP_DEP_VERSION))
    }
    // Reuse the Java core's JSON writer (stdlib-only, escapes strings safely).
    return Json.write(m)
}

/**
 * Dependency inventory as (name, version) pairs parsed from jar file names
 * (`artifact-1.2.3.jar` splits at the LAST '-'). The running fat jar wins;
 * plain classpath jars are the fallback; capped at [MAX_MANIFEST_DEPS].
 */
internal fun discoverDependencies(): List<Pair<String, String>> {
    val fat = runCatching { fatJarDependencies() }.getOrNull()
    if (!fat.isNullOrEmpty()) return fat.take(MAX_MANIFEST_DEPS)
    val fromClasspath = runCatching {
        val cp = System.getProperty("java.class.path") ?: return@runCatching emptyList<Pair<String, String>>()
        cp.split(File.pathSeparator)
            .filter { it.endsWith(".jar") }
            .mapNotNull { entry ->
                val f = File(entry)
                if (f.isFile) depFromJarFileName(f.name) else null
            }
    }.getOrNull().orEmpty()
    return fromClasspath.take(MAX_MANIFEST_DEPS)
}

/**
 * Lists BOOT-INF/lib dependencies of the fat jar this class was loaded from;
 * null when there is no readable jar or it is not a Spring Boot-style fat
 * jar (caller falls back to the classpath).
 */
private fun fatJarDependencies(): List<Pair<String, String>>? {
    val location = Dataflow::class.java.protectionDomain?.codeSource?.location ?: return null
    val path = jarPathFromUrl(location) ?: return null
    val file = File(path)
    if (!file.isFile) return null
    JarFile(file, false).use { jar ->
        val deps = ArrayList<Pair<String, String>>(64)
        val entries = jar.entries()
        while (entries.hasMoreElements()) {
            val name = entries.nextElement().name
            if (name.startsWith("BOOT-INF/lib/") && name.endsWith(".jar")) {
                deps.add(depFromJarFileName(name.substringAfterLast('/')))
            }
        }
        return deps.ifEmpty { null }
    }
}

/** Extracts a filesystem path from a code-source URL, unwrapping jar:/nested URLs. */
private fun jarPathFromUrl(u: URL): String? {
    var p = u.toString()
    if (p.startsWith("jar:")) p = p.removePrefix("jar:")
    val bang = p.indexOf("!/")
    if (bang >= 0) p = p.substring(0, bang)
    if (p.startsWith("file:")) p = p.removePrefix("file:")
    return p.ifEmpty { null }
}

/**
 * Splits `artifact-1.2.3` into (name, version): strips directories and the
 * `.jar` suffix, then splits at the last '-'; a name with no '-' reports an
 * empty version.
 */
private fun depFromJarFileName(fileName: String): Pair<String, String> {
    var s = fileName
    if (s.endsWith(".jar")) s = s.substring(0, s.length - 4)
    val slash = maxOf(s.lastIndexOf('/'), s.lastIndexOf('\\'))
    if (slash >= 0) s = s.substring(slash + 1)
    val dash = s.lastIndexOf('-')
    return if (dash > 0) s.substring(0, dash) to s.substring(dash + 1) else s to ""
}

/** First dependency whose name contains a known framework marker, else "". */
internal fun detectFramework(deps: List<Pair<String, String>>): String {
    for ((name, _) in deps) {
        val n = name.lowercase()
        for ((marker, framework) in KNOWN_FRAMEWORKS) {
            if (marker in n) return framework
        }
    }
    return ""
}

/** Normalized os.name: linux / windows / mac. */
private fun normalizedOs(): String {
    val name = (System.getProperty("os.name") ?: "").lowercase()
    return when {
        name.contains("linux") || name.contains("nix") || name.contains("nux") -> "linux"
        name.contains("win") -> "windows"
        name.contains("mac") -> "mac"
        else -> name.replace(Regex("\\s+"), "-")
    }
}

/** "linux/amd64"-style descriptor: normalized OS name + raw os.arch. */
private fun osArch(): String =
    normalizedOs() + "/" + (System.getProperty("os.arch") ?: "")

/** Truncates to the server-side wire cap. */
private fun clip(s: String, max: Int): String =
    if (s.length <= max) s else s.substring(0, max)

/**
 * Resolves the HTTP API base for manifest reporting: an explicit
 * DATAFLOW_HTTP_URL wins (needed when the gRPC DATAFLOW_ENDPOINT is a bare
 * host:port); URL-form endpoints map directly; a bare gRPC endpoint with no
 * override has no derivable HTTP base and reporting is skipped (null).
 */
internal fun httpBaseURL(endpoint: String): String? {
    val override = System.getenv("DATAFLOW_HTTP_URL")
    if (!override.isNullOrBlank()) return override.trim().trimEnd('/')
    val e = endpoint.trim()
    return if (e.startsWith("http://") || e.startsWith("https://")) e.trimEnd('/') else null
}

/**
 * Reports the manifest once per process. Best-effort: daemon thread, 5s
 * timeout, silent failures — startup and tracing are never delayed.
 */
internal fun sendManifest() {
    if (!manifestSent.compareAndSet(false, true)) return
    try {
        val s = Dataflow.settings()
        if (s.disabled || s.apiKey.isEmpty()) return
        val base = httpBaseURL(s.endpoint) ?: return
        val body = buildManifest(Dataflow.serviceName(), SDK_VERSION)
        Thread {
            try {
                val client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(3))
                    .build()
                val req = HttpRequest.newBuilder(URI.create("$base/api/v1/manifest"))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .header("X-Api-Key", s.apiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build()
                // Response body is discarded; non-2xx is fine to ignore.
                client.send(req, HttpResponse.BodyHandlers.discarding())
            } catch (_: Throwable) {
                // Any failure (bad URL, DNS, timeout, 4xx/5xx) is silently ignored.
            }
        }.apply {
            name = "dataflow-manifest"
            isDaemon = true
            start()
        }
    } catch (_: Throwable) {
        manifestSent.set(false)
    }
}
