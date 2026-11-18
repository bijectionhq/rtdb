package xtdb

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.jupiter.api.extension.ExtensionContext
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.indexer.LogProcessor
import xtdb.indexer.LogProcessorSimTest
import xtdb.indexer.SimLog
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE_NEW
import java.nio.file.StandardOpenOption.WRITE
import java.security.MessageDigest
import java.time.Instant
import java.util.UUID

internal class LogProcessorTrace(private val seed: Int) {
    private data class Event(val kind: String, val fields: List<Any?>)

    private val events = ArrayList<Event>()
    private var started = false
    private var finished = false
    private var overflow = false
    private var nextTail = 0

    fun start() {
        started = true
        event("case_started")
    }

    fun assertionsFinished() {
        event("assertions_finished")
        finished = true
    }

    fun event(kind: String, vararg values: Any?) {
        if (!started || finished || overflow) return
        if (events.size == 65_536) {
            overflow = true
            return
        }
        events.add(Event(kind, values.toList()))
    }

    fun openedTail(stream: String, actor: Int?, start: Int): Int {
        val tail = nextTail++
        event("tail_open", stream, tail, actor, start)
        return tail
    }

    fun enqueue(stream: String, actor: Int?, epoch: Int, offset: Long, time: Instant, message: Any?) {
        val fields: List<Any?> = when (message) {
            is SourceMessage.FlushBlock -> listOf("flush", message.expectedBlockIdx)
            is ReplicaMessage.NoOp -> listOf("noop", message.termId, message.termSeq, message.srcMsgId)
            is ReplicaMessage.ResolvedTx -> listOf("transaction", message.termId, message.termSeq, message.txId,
                message.systemTime, message.committed, message.externalSourceToken?.copyOf(), message.srcMsgId)
            is ReplicaMessage.BlockBoundary -> listOf("boundary", message.termId, message.termSeq,
                message.blockIndex, message.latestProcessedMsgId, message.externalSourceToken?.copyOf())
            is ReplicaMessage.BlockUploaded -> listOf("upload", message.termId, message.termSeq,
                message.blockIndex, message.latestProcessedMsgId, message.externalSourceToken?.copyOf())
            is ReplicaMessage -> listOf("other_replica", message.javaClass.name, message.termId, message.termSeq)
            else -> listOf("other_source", message?.javaClass?.name)
        }
        event("enqueue", stream, actor, epoch, offset, time, fields)
    }

    fun write(context: ExtensionContext, lane: Path) {
        val directory = lane.parent.parent.resolve("causal")
        Files.createDirectories(directory)
        val name = "${digest(context.uniqueId.toByteArray(Charsets.UTF_8))}.json"
        val path = directory.resolve(name)
        val worker = Path.of(ProcessHandle.current().info().command().orElseThrow()).toAbsolutePath()
        val javaHome = Path.of(System.getProperty("java.home")).toAbsolutePath()
        val classes = listOf(LogProcessorSimTest::class.java, DeterministicDispatcher::class.java, SimLog::class.java,
            LogProcessorTrace::class.java, LogProcessor::class.java,
            Class.forName("rtdb.conformance.JunitRecorder", false, LogProcessorSimTest::class.java.classLoader))
        val header = buildJsonObject {
            put("schema_version", 2)
            put("unique_id", context.uniqueId)
            put("seed", seed)
            put("assertions_finished", finished)
            put("overflow", overflow)
            put("java_home", javaHome.toRealPath().toString())
            put("java_home_request", javaHome.toString())
            put("java_modules", javaHome.resolve("lib/modules").toRealPath().toString())
            put("java_executable", worker.toRealPath().toString())
            put("java_executable_request", worker.toString())
            put("jvm_args", json(ManagementFactory.getRuntimeMXBean().inputArguments))
            put("classes", buildJsonObject {
                for (type in classes) {
                    put(type.name, Path.of(type.protectionDomain.codeSource.location.toURI()).toRealPath().toString())
                }
            })
            put("events", JsonArray(events.mapIndexed { index, event ->
                buildJsonObject {
                    put("sequence", index)
                    put("kind", event.kind)
                    put("fields", json(event.fields))
                }
            }))
        }
        val bytes = (header.toString() + "\n").toByteArray(Charsets.UTF_8)
        Files.write(path, bytes, CREATE_NEW, WRITE)
        context.publishReportEntry(mapOf("rtdb.simulation.trace" to "causal/$name", "rtdb.simulation.sha256" to digest(bytes)))
    }

    private fun digest(bytes: ByteArray): String =
        java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))

    private fun json(value: Any?): JsonElement = when (value) {
        null -> JsonNull
        is Boolean -> JsonPrimitive(value)
        is Number -> JsonPrimitive(value)
        is String -> JsonPrimitive(value)
        is UUID -> JsonPrimitive(value.toString())
        is Instant -> JsonArray(listOf(JsonPrimitive(value.epochSecond), JsonPrimitive(value.nano)))
        is ByteArray -> JsonPrimitive(java.util.HexFormat.of().formatHex(value))
        is List<*> -> JsonArray(value.map(::json))
        else -> error("Unsupported simulation trace value: ${value.javaClass.name}")
    }

    companion object {
        private const val SELECTED_CLASS = "xtdb.indexer.LogProcessorSimTest"
        private const val SELECTED_METHOD = "a node whose claims the log refuses goes on following and indexing"

        fun select(context: ExtensionContext, seed: Int): LogProcessorTrace? =
            if (context.requiredTestClass.name == SELECTED_CLASS && context.requiredTestMethod.name == SELECTED_METHOD
                && System.getProperty("rtdb.inventory.output") != null) LogProcessorTrace(seed) else null
    }
}
