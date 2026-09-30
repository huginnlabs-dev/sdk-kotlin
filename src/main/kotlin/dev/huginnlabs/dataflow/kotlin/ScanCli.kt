package dev.huginnlabs.dataflow.kotlin

import dev.huginnlabs.dataflow.Json
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import kotlin.system.exitProcess

/**
 * Static route scanner for Kotlin sources (`ScanCli`): regex extraction over
 * source lines — no Kotlin compiler dependency. Declared HTTP endpoints are
 * posted to the server catalog (POST `/api/v1/catalog`) so declared routes
 * can be correlated with observed traffic. Mirrors the Java SDK's ScanCli
 * conventions (handler `Class.method`, `ANY` for method-less mappings,
 * `--print` prints instead of posting).
 *
 * Frameworks:
 * - Ktor: `routing { get("/path") { } }`, `post/put/delete/patch/head/options`,
 *   and nested `route("/base") { get("/x") { } }` prefixes (one-level-deep
 *   prefix stack with brace counting).
 * - Spring: `@GetMapping("/x")` & co, plus `@RequestMapping` (class-level
 *   prefix, or method-level with `method = [RequestMethod.GET]`).
 * - Micronaut: `@Controller("/base")` + `@Get/@Post/@Put/@Delete/@Patch`.
 * - http4k and other DSLs: not scanned.
 *
 * Handler attribution: the enclosing class plus the `fun name()` the
 * annotations or route belong to, else empty. Path parameters keep the
 * `{id}` syntax as written.
 *
 * Usage:
 * ```
 * java -cp ... dev.huginnlabs.dataflow.kotlin.ScanCli \
 *     --dir . --service my-service --url https://dataflow.example.com \
 *     --api-key $DATAFLOW_API_KEY [--print]
 * ```
 */

/** Server-side cap: at most 1000 routes are accepted per catalog post. */
internal const val MAX_ROUTES = 1000

/** How many following lines a multi-line annotation's arguments may span. */
private const val JOIN_WINDOW = 5

/** Wire-contract string caps (service 128; route fields 256; method 16). */
private const val CAP_SERVICE = 128
private const val CAP_ROUTE_FIELD = 256
private const val CAP_METHOD = 16

/** One declared HTTP endpoint extracted from a Kotlin source file. */
internal data class ScannedRoute(
    val method: String,
    val path: String,
    val handler: String,
    val sourceFile: String,
)

/** Scan outcome: routes plus how many files were inspected. */
internal data class ScanResult(val routes: List<ScannedRoute>, val files: Int)

// ---------------------------------------------------------------------------
// Line classification: a per-file state machine marking every character as
// code / string / char literal / comment, so brace counting and route regexes
// ignore string contents (paths like "/users/{id}" must not move the brace
// depth) and commented-out routes are never reported.
// ---------------------------------------------------------------------------

/** Char classes of a classified line. */
internal const val CL_CODE: Byte = 0
internal const val CL_STRING: Byte = 1
internal const val CL_CHAR: Byte = 2
internal const val CL_COMMENT: Byte = 3

/**
 * Sequential classifier: feed the file's lines in order; each call returns
 * the per-character class array. Carries block-comment (nestable) and
 * `"""`-raw-string state across lines.
 */
internal class LineClassifier {
    private var blockCommentDepth = 0
    private var inRawString = false

    fun classify(line: String): ByteArray {
        val out = ByteArray(line.length)
        var i = 0
        val n = line.length
        while (i < n) {
            if (blockCommentDepth > 0) {
                when {
                    line.startsWith("/*", i) -> { blockCommentDepth++; out[i] = CL_COMMENT; out[i + 1] = CL_COMMENT; i += 2 }
                    line.startsWith("*/", i) -> { blockCommentDepth--; out[i] = CL_COMMENT; out[i + 1] = CL_COMMENT; i += 2 }
                    else -> { out[i] = CL_COMMENT; i++ }
                }
                continue
            }
            if (inRawString) {
                if (line.startsWith("\"\"\"", i)) {
                    inRawString = false
                    out[i] = CL_STRING; out[i + 1] = CL_STRING; out[i + 2] = CL_STRING; i += 3
                } else {
                    out[i] = CL_STRING; i++
                }
                continue
            }
            val c = line[i]
            when {
                c == '/' && i + 1 < n && line[i + 1] == '/' -> {
                    while (i < n) { out[i] = CL_COMMENT; i++ }
                }
                c == '/' && i + 1 < n && line[i + 1] == '*' -> {
                    blockCommentDepth = 1; out[i] = CL_COMMENT; out[i + 1] = CL_COMMENT; i += 2
                }
                c == '"' && line.startsWith("\"\"\"", i) -> {
                    inRawString = true
                    out[i] = CL_STRING; out[i + 1] = CL_STRING; out[i + 2] = CL_STRING; i += 3
                }
                c == '"' -> i = scanQuoted(line, out, i, CL_STRING, '"')
                c == '\'' -> i = scanQuoted(line, out, i, CL_CHAR, '\'')
                else -> { out[i] = CL_CODE; i++ }
            }
        }
        return out
    }

    /** Marks a quoted literal (escape-aware) starting at [start]; returns the index after the closing quote. */
    private fun scanQuoted(line: String, out: ByteArray, start: Int, cls: Byte, quote: Char): Int {
        var i = start
        val n = line.length
        out[i] = cls
        i++
        while (i < n) {
            out[i] = cls
            if (line[i] == '\\' && i + 1 < n) {
                out[i + 1] = cls
                i += 2
                continue
            }
            val c = line[i]
            i++
            if (c == quote) break
        }
        return i
    }
}

private fun isCode(cls: ByteArray, idx: Int): Boolean = idx in cls.indices && cls[idx] == CL_CODE

/** Net brace delta of the code (non-string, non-comment) region of a line segment. */
private fun braceDelta(line: String, cls: ByteArray, from: Int, to: Int): Int {
    var d = 0
    for (i in from until minOf(to, line.length)) {
        if (cls[i] != CL_CODE) continue
        when (line[i]) {
            '{' -> d++
            '}' -> d--
        }
    }
    return d
}

/** True when the segment contains any code-region brace. */
private fun hasBrace(line: String, cls: ByteArray, from: Int, to: Int): Boolean {
    for (i in from until minOf(to, line.length)) {
        if (cls[i] == CL_CODE && (line[i] == '{' || line[i] == '}')) return true
    }
    return false
}

// ---------------------------------------------------------------------------
// Extraction.
// ---------------------------------------------------------------------------

private val KTOR_METHOD = Regex("""\b(get|post|put|delete|patch|head|options)\b\s*\(\s*"((?:[^"\\]|\\.)*)"""")
private val KTOR_ROUTE = Regex("""\broute\b\s*\(\s*"((?:[^"\\]|\\.)*)"""")
private val SPRING_MAPPING = Regex("""@(Get|Post|Put|Delete|Patch)Mapping\b""")
private val REQUEST_MAPPING = Regex("""@RequestMapping\b""")
private val CONTROLLER_ANNOTATION = Regex("""@(Controller|RestController)\b""")
private val MICRONAUT_METHOD = Regex("""@(Get|Post|Put|Delete|Patch|Head|Options)\b""")
private val FUN_DECL = Regex("""\bfun\b\s*(?:<[^()]*?>\s*)?(?:[\w?.]+(?:<[^()]*>)?\.)?([A-Za-z_]\w*)\s*\(""")
private val CLASS_DECL = Regex("""\b(?:class|object)\s+([A-Za-z_]\w*)""")
private val REQUEST_METHOD_SPEC = Regex("""RequestMethod\.([A-Za-z]+)""")
private val STRING_LITERAL = Regex("\"((?:[^\"\\\\]|\\\\.)*)\"")

/** Uppercase wire method for a source-level keyword/annotation name; null when unknown. */
private fun wireMethod(name: String): String = when (name.uppercase()) {
    "GET" -> "GET"
    "POST" -> "POST"
    "PUT" -> "PUT"
    "DELETE" -> "DELETE"
    "PATCH" -> "PATCH"
    "HEAD" -> "HEAD"
    "OPTIONS" -> "OPTIONS"
    else -> name.uppercase()
}

/** Unescapes the two escapes a route path can realistically contain. */
private fun unescapePath(raw: String): String =
    raw.replace("\\\\", "\u0000").replace("\\\"", "\"").replace("\u0000", "\\")

/**
 * Joins path segments (`/`-trimmed) into one template path; each segment is
 * a literal as written, `{id}` params preserved. Empty parts collapse, so
 * `["api", "users"]` → `/api/users` and the root alone stays `/`.
 */
internal fun joinPath(parts: List<String>): String =
    "/" + parts.map { it.trim('/') }.filter { it.isNotEmpty() }.joinToString("/")

/** All string-literal contents in [args], in order. */
private fun stringLiteralsIn(args: String): List<String> =
    STRING_LITERAL.findAll(args).map { unescapePath(it.groupValues[1]) }.toList()

/**
 * First path-looking literal in annotation args ("" when the annotation
 * carries none): a `"/..."` literal wins, skipping produces/consumes media
 * types.
 */
private fun annotationPath(args: String?): String {
    if (args.isNullOrEmpty()) return ""
    val literals = stringLiteralsIn(args)
    return literals.firstOrNull { it.startsWith("/") } ?: literals.firstOrNull() ?: ""
}

/** One annotation awaiting the declaration it belongs to. */
private data class PendingAnno(val name: String, val methods: List<String>, val path: String)

/**
 * Annotation argument text (string-aware, paren-balanced) starting after the
 * annotation name; multi-line arguments pull in up to [JOIN_WINDOW] further
 * lines. Returns the last line index consumed plus the args (null when the
 * annotation carries no parenthesized arguments).
 */
private fun annotationArgsSpan(lines: List<String>, classes: List<ByteArray>, i: Int, from: Int): Pair<Int, String?> {
    val n = lines.size
    // Locate the open paren on the first line; anything else non-blank → no args.
    var open = -1
    run {
        val line = lines[i]
        val cls = classes[i]
        var p = from
        while (p < line.length) {
            if (cls[p] == CL_CODE && line[p] == '(') { open = p; break }
            if (cls[p] != CL_CODE || !line[p].isWhitespace()) return i to null
            p++
        }
        if (open == -1) return i to null
    }
    val sb = StringBuilder()
    var depth = 0
    var started = false
    var li = i
    while (li < n) {
        val line = lines[li]
        val cls = classes[li]
        var p = if (li == i) open else 0
        while (p < line.length) {
            val c = line[p]
            if (cls[p] != CL_CODE) {
                if (started) sb.append(c)
                p++
                continue
            }
            when (c) {
                '(' -> { depth++; started = true; if (depth > 1) sb.append(c) }
                ')' -> {
                    depth--
                    if (depth == 0) return li to sb.toString()
                    sb.append(c)
                }
                else -> if (started) sb.append(c)
            }
            p++
        }
        if (started) sb.append(' ')
        li++
        if (li - i > JOIN_WINDOW) break
    }
    return (li - 1).coerceAtLeast(i) to (if (started) sb.toString() else null)
}

/** Parses one matched annotation into a [PendingAnno]; `methodWord` is the verb-carrying group when present. */
private fun pendingAnno(name: String, methodWord: String?, args: String?): PendingAnno = when (name) {
    "RequestMapping" -> PendingAnno(name, REQUEST_METHOD_SPEC.findAll(args ?: "").map { wireMethod(it.groupValues[1]) }.toList(), annotationPath(args))
    "Controller", "RestController" -> PendingAnno(name, emptyList(), annotationPath(args))
    else -> PendingAnno(name, listOf(wireMethod(methodWord ?: name)), annotationPath(args)) // XMapping / micronaut verbs
}

/**
 * Scans one Kotlin file's lines (already classified) for declared routes.
 * Single positional walk per line: brace deltas are applied between matched
 * events so `route()` prefixes and class/function scopes open and close with
 * the real brace depth (one-liners work). Annotations are collected as
 * pending and consumed by the next class declaration (class-level prefix) or
 * function declaration (method-level route), mirroring the Java scanner.
 */
internal fun scanLines(lines: List<String>, classes: List<ByteArray>, sourceFile: String): List<ScannedRoute> {
    val routes = ArrayList<ScannedRoute>()

    /** A `route()` or class-level path prefix awaiting its closing brace. */
    class PrefixEntry(val raw: String, val pushDepth: Int)

    val prefixes = ArrayDeque<PrefixEntry>()
    val pending = ArrayList<PendingAnno>()
    var depth = 0
    var currentFun: String? = null
    var funDepth = 0
    var sawBraceSinceFun = false
    var currentClass: String? = null
    var classDepth = 0
    var sawBraceSinceClass = false
    var skipTo = -1 // continuation lines of a multi-line annotation

    fun applyDelta(from: Int, to: Int, line: String, cls: ByteArray, braces: () -> Unit) {
        depth += braceDelta(line, cls, from, to)
        if (hasBrace(line, cls, from, to)) braces()
        while (prefixes.isNotEmpty() && prefixes.last().pushDepth >= depth) prefixes.removeLast()
    }

    fun effectiveParts(): List<String> = prefixes.map { it.raw }

    fun addRoute(method: String, rawPath: String, handler: String) {
        if (routes.size >= MAX_ROUTES) return
        routes.add(ScannedRoute(method.uppercase().take(CAP_METHOD), joinPath(effectiveParts() + rawPath), handler, sourceFile))
    }

    /** Turns the annotations collected before a function declaration into routes. */
    fun emitPending(funName: String) {
        if (pending.isEmpty()) return
        val annos = pending.toList()
        pending.clear()
        var path = ""
        val methods = LinkedHashSet<String>()
        for (a in annos) {
            if (a.methods.isNotEmpty() || a.name == "RequestMapping") {
                if (path.isEmpty()) path = a.path
                methods.addAll(a.methods)
            }
        }
        if (methods.isEmpty()) {
            // @RequestMapping without a method spec matches every verb → ANY;
            // only class-scoping annotations (@Controller) above a fun are not endpoints.
            if (annos.none { it.name == "RequestMapping" }) return
            methods.add("ANY")
        }
        val handler = (currentClass?.let { "$it." } ?: "") + funName
        for (m in methods) addRoute(m, path, handler)
    }

    /** Class-level path prefix from the pending annotations, or null to keep the current one. */
    fun classPrefixFrom(): String? {
        for (a in pending) {
            if (a.name == "RequestMapping" && a.path.isNotEmpty()) return a.path
        }
        for (a in pending) {
            if ((a.name == "Controller" || a.name == "RestController") && a.path.isNotEmpty()) return a.path
        }
        return null
    }

    class Event(val start: Int, val end: Int, val kind: String, val m: MatchResult)

    for (i in lines.indices) {
        if (i <= skipTo) continue // inside a multi-line annotation consumed earlier
        val line = lines[i]
        val cls = classes[i]
        var bracesSeen = false
        val markBraces = { bracesSeen = true }
        var hadNonAnnoEvent = false // a declaration or route call happened on this line

        val events = ArrayList<Event>()
        for (m in KTOR_METHOD.findAll(line)) events.add(Event(m.range.first, m.range.last + 1, "ktorMethod", m))
        for (m in KTOR_ROUTE.findAll(line)) events.add(Event(m.range.first, m.range.last + 1, "ktorRoute", m))
        for (m in SPRING_MAPPING.findAll(line)) events.add(Event(m.range.first, m.range.last + 1, "anno:spring", m))
        for (m in REQUEST_MAPPING.findAll(line)) events.add(Event(m.range.first, m.range.last + 1, "anno:RequestMapping", m))
        for (m in CONTROLLER_ANNOTATION.findAll(line)) events.add(Event(m.range.first, m.range.last + 1, "anno:" + m.groupValues[1], m))
        for (m in MICRONAUT_METHOD.findAll(line)) {
            // @GetMapping must not also match the micronaut @Get pattern.
            val overlaps = events.any { it.kind.startsWith("anno:") && it.start < m.range.last + 1 && m.range.first < it.end }
            if (!overlaps) events.add(Event(m.range.first, m.range.last + 1, "anno:micronaut", m))
        }
        for (m in FUN_DECL.findAll(line)) events.add(Event(m.range.first, m.range.last + 1, "fun", m))
        for (m in CLASS_DECL.findAll(line)) events.add(Event(m.range.first, m.range.last + 1, "class", m))
        events.sortBy { it.start }

        // Ktor verbs are bare calls (get("/x")); reject member receivers
        // (map.get("k")) and anything living in a string or comment.
        events.removeAll { ev ->
            when {
                ev.kind == "ktorMethod" -> !isCode(cls, ev.start) || (ev.start > 0 && line[ev.start - 1] == '.')
                else -> !isCode(cls, ev.start)
            }
        }

        var cursor = 0
        for (ev in events) {
            applyDelta(cursor, ev.start, line, cls, markBraces)
            cursor = ev.end
            when {
                ev.kind == "fun" -> {
                    currentFun = ev.m.groupValues[1]
                    funDepth = depth
                    sawBraceSinceFun = false
                    emitPending(currentFun)
                    hadNonAnnoEvent = true
                }
                ev.kind == "class" -> {
                    classPrefixFrom()?.let { prefixes.addLast(PrefixEntry(it, depth)) }
                    currentClass = ev.m.groupValues[1]
                    classDepth = depth
                    sawBraceSinceClass = false
                    pending.clear()
                    hadNonAnnoEvent = true
                }
                ev.kind == "ktorRoute" -> {
                    pending.clear()
                    hadNonAnnoEvent = true
                    val raw = unescapePath(ev.m.groupValues[1])
                    if (raw.trim('/').isNotEmpty()) prefixes.addLast(PrefixEntry(raw, depth))
                }
                ev.kind == "ktorMethod" -> {
                    pending.clear()
                    hadNonAnnoEvent = true
                    addRoute(ev.m.groupValues[1], unescapePath(ev.m.groupValues[2]), currentFun ?: "")
                }
                else -> { // annotation: join multi-line args, hold pending for the next decl
                    val annoName = when (ev.kind) {
                        "anno:spring" -> ev.m.groupValues[1] + "Mapping"
                        "anno:RequestMapping" -> "RequestMapping"
                        "anno:micronaut" -> ev.m.groupValues[1]
                        else -> ev.kind.removePrefix("anno:")
                    }
                    val methodWord = if (ev.kind == "anno:spring" || ev.kind == "anno:micronaut") ev.m.groupValues[1] else null
                    val (lastLine, args) = annotationArgsSpan(lines, classes, i, ev.end)
                    if (lastLine > skipTo) skipTo = lastLine
                    pending.add(pendingAnno(annoName, methodWord, args))
                }
            }
        }
        applyDelta(cursor, line.length, line, cls, markBraces)

        // A function scope ends when its body's braces have closed again; a
        // class scope additionally always owns a block (bodyless declarations
        // do not enclose routes).
        if (bracesSeen) {
            sawBraceSinceFun = true
            sawBraceSinceClass = true
        }
        if (currentFun != null && sawBraceSinceFun && depth <= funDepth) currentFun = null
        if (currentClass != null && sawBraceSinceClass && depth <= classDepth) currentClass = null

        // Any real code line that is not a declaration breaks the
        // annotation -> declaration chain (mirrors the Java scanner).
        val t = line.trim()
        val annotationOrCommentOnly = t.isEmpty() || t.startsWith("//") || t.startsWith("*") ||
            t.startsWith("/*") || (t.startsWith("@") && !hadNonAnnoEvent)
        if (!annotationOrCommentOnly) pending.clear()
    }
    return routes
}

/**
 * Walks [root] for `*.kt` sources, skipping `build/`, `target/`, `.git/`
 * directories and `*Test.kt` files, and extracts declared routes. Files are
 * scanned in sorted path order for deterministic output; the result is
 * capped at [MAX_ROUTES].
 */
internal fun scanRoutes(root: File): ScanResult {
    if (!root.isDirectory) return ScanResult(emptyList(), 0)
    val skipDirs = setOf("build", "target", ".git")
    val files = root.walkTopDown()
        .onEnter { it.isDirectory && it.name !in skipDirs }
        .filter { it.isFile && it.name.endsWith(".kt") && !it.name.endsWith("Test.kt") }
        .toList()
        .sortedBy { it.path }
    val out = ArrayList<ScannedRoute>()
    var scanned = 0
    for (f in files) {
        if (out.size >= MAX_ROUTES) break
        scanned++
        val rel = f.toRelativeString(root).replace('\\', '/')
        val routes = runCatching {
            val lines = f.readLines(StandardCharsets.UTF_8)
            val classifier = LineClassifier()
            scanLines(lines, lines.map { classifier.classify(it) }, rel)
        }.getOrDefault(emptyList())
        out.addAll(routes)
    }
    return ScanResult(out.take(MAX_ROUTES), scanned)
}

/**
 * Builds the catalog request body for POST `/api/v1/catalog` (server
 * contract: service_name + routes with method/path/handler/source_file).
 * Reuses the Java core's stdlib JSON writer.
 */
internal fun buildCatalogJSON(serviceName: String, routes: List<ScannedRoute>): String {
    val arr = ArrayList<Any>(routes.size)
    for (r in routes) {
        arr.add(
            linkedMapOf<String, Any>(
                "method" to clip(r.method, CAP_METHOD),
                "path" to clip(r.path, CAP_ROUTE_FIELD),
                "handler" to clip(r.handler, CAP_ROUTE_FIELD),
                "source_file" to clip(r.sourceFile, CAP_ROUTE_FIELD),
            ),
        )
    }
    val body = LinkedHashMap<String, Any>()
    body["service_name"] = clip(serviceName, CAP_SERVICE)
    body["routes"] = arr
    return Json.write(body)
}

private fun clip(s: String, max: Int): String = if (s.length <= max) s else s.substring(0, max)

/**
 * Resolves the catalog API base: `--url` wins, then `DATAFLOW_HTTP_URL`,
 * then a URL-form `DATAFLOW_ENDPOINT`. A bare `host:port` gRPC endpoint has
 * no derivable HTTP base — null (the CLI skips the upload with a message).
 * Same precedence as the startup manifest.
 */
internal fun resolveCatalogURL(urlFlag: String?, httpUrlEnv: String?, endpointEnv: String?): String? {
    urlFlag?.trim()?.takeIf { it.isNotEmpty() }?.let { return it.trimEnd('/') }
    httpUrlEnv?.trim()?.takeIf { it.isNotEmpty() }?.let { return it.trimEnd('/') }
    val e = endpointEnv?.trim()?.takeIf { it.isNotEmpty() } ?: return null
    return if (e.startsWith("http://") || e.startsWith("https://")) e.trimEnd('/') else null
}

/** POSTs the catalog body; returns the HTTP status (0 on transport failure). Errors go to [err]. */
internal fun postCatalog(url: String, apiKey: String, body: String, err: (String) -> Unit): Int =
    try {
        val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
        val req = HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(15))
            .header("Content-Type", "application/json")
            .header("X-Api-Key", apiKey)
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build()
        val resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
        val status = resp.statusCode()
        if (status < 200 || status >= 300) {
            val excerpt = resp.body()?.trim().orEmpty()
            err(msg("server said: " + if (excerpt.length > 200) excerpt.substring(0, 200) else excerpt))
        }
        status
    } catch (e: Exception) {
        err(msg("post failed: $e"))
        0
    }

/** Parsed CLI flags. */
internal data class ScanOptions(
    val dir: String,
    val service: String?,
    val url: String?,
    val apiKey: String?,
    val print: Boolean,
)

private fun msg(text: String): String = "dataflow-scan: $text"

private const val USAGE = """usage: ScanCli --dir <source root> --service <name> [--url <http base>] [--api-key <key>] [--print]"""

/** Parses flags; throws IllegalArgumentException on unknown flags or missing values. */
internal fun parseArgs(args: Array<String>): ScanOptions {
    var dir: String? = null
    var service: String? = null
    var url: String? = null
    var apiKey: String? = null
    var print = false
    var i = 0
    fun value(flag: String): String {
        if (i + 1 >= args.size) throw IllegalArgumentException("missing value for $flag")
        i++
        return args[i]
    }
    while (i < args.size) {
        when (args[i]) {
            "--dir" -> dir = value("--dir")
            "--service" -> service = value("--service")
            "--url" -> url = value("--url")
            "--api-key" -> apiKey = value("--api-key")
            "--print" -> print = true
            "--help", "-h" -> throw IllegalArgumentException("__usage__")
            else -> throw IllegalArgumentException("unknown argument: ${args[i]}")
        }
        i++
    }
    if (dir.isNullOrEmpty()) throw IllegalArgumentException("--dir is required")
    return ScanOptions(dir, service, url, apiKey, print)
}

/**
 * Runs the scanner: extract routes from `--dir`, print the request JSON
 * (`--print`, then stop) and otherwise POST it to the catalog. Returns the
 * process exit code: 0 success, 1 upload failure or unusable URL, 2 usage
 * error. Diagnostics go to stderr; `--print` JSON goes to stdout.
 */
internal fun runScan(args: Array<String>, env: (String) -> String? = System::getenv, out: (String) -> Unit, err: (String) -> Unit): Int {
    val opts = try {
        parseArgs(args)
    } catch (e: IllegalArgumentException) {
        if (e.message == "__usage__") {
            out(USAGE)
            return 0
        }
        err(msg(e.message ?: "bad arguments"))
        err(USAGE)
        return 2
    }

    val dir = File(opts.dir)
    if (!dir.isDirectory) {
        err(msg("not a directory: ${opts.dir}"))
        return 2
    }
    val service = opts.service ?: env("DATAFLOW_SERVICE_NAME")?.trim()?.takeIf { it.isNotEmpty() }
    if (service == null) {
        err(msg("--service is required (or set DATAFLOW_SERVICE_NAME)"))
        return 2
    }

    val result = scanRoutes(dir)
    val body = buildCatalogJSON(service, result.routes)

    if (opts.print) {
        out(body)
        return 0
    }

    val base = resolveCatalogURL(opts.url, env("DATAFLOW_HTTP_URL"), env("DATAFLOW_ENDPOINT"))
    if (base == null) {
        err(msg("no HTTP base URL: pass --url, or set DATAFLOW_HTTP_URL; a bare host:port DATAFLOW_ENDPOINT is gRPC-only and cannot be used"))
        return 1
    }
    val apiKey = opts.apiKey ?: env("DATAFLOW_API_KEY")?.trim()?.takeIf { it.isNotEmpty() }
    if (apiKey == null) {
        err(msg("no API key: pass --api-key or set DATAFLOW_API_KEY"))
        return 2
    }

    err(msg("${result.routes.size} route(s) from ${result.files} file(s) -> $base/api/v1/catalog"))
    val status = postCatalog("$base/api/v1/catalog", apiKey, body, err)
    if (status < 200 || status >= 300) {
        err(msg("catalog post failed with HTTP $status"))
        return 1
    }
    err(msg("accepted (HTTP $status)"))
    return 0
}

/**
 * CLI entry point:
 * `ScanCli --dir <root> --service <name> [--url <base>] [--api-key <key>] [--print]`.
 * See [runScan] for exit codes.
 */
object ScanCli {
    @JvmStatic
    fun main(args: Array<String>) {
        exitProcess(runScan(args, out = ::println, err = { System.err.println(it) }))
    }
}
