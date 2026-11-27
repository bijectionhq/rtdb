#!/usr/bin/env python3
"""Run one pinned JVM processor simulation and retain its observed causal inputs."""

import argparse
import hashlib
import json
import re
import shutil
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID
from zipfile import BadZipFile, ZipFile

from run_controls import HERE, REFERENCE as C84_REFERENCE, REPOSITORY, digest, git, run_process, write_json
from runtime_inventory import inspect_plan
from source_gate import require_phase_two


UPSTREAM_COMMIT = (HERE / "upstream-commit.txt").read_text(encoding="ascii").strip()
SUPPORTED_REFERENCES = (UPSTREAM_COMMIT, C84_REFERENCE)
PROJECT = ":xtdb-core"
TASK = PROJECT + ":property-test"
LANE = TASK.replace(":", "_")
TEST_CLASS = "xtdb.indexer.LogProcessorSimTest"
METHOD = "a node whose claims the log refuses goes on following and indexing"
FIXTURE = "core/src/test/kotlin/xtdb/indexer/LogProcessorSimTest.kt"
HELPER = "core/src/test/kotlin/xtdb/LogProcessorTrace.kt"
LIVE_INDEX_TAPE_SCHEMA = 2
LIVE_INDEX_ASSERTION = "live_index_caught_up"
LIVE_INDEX_ASSERTION_LINE = 461
CLOSURE_CLASSES = {
    TEST_CLASS, "xtdb.DeterministicDispatcher", "xtdb.LogProcessorTrace", "xtdb.indexer.SimLog",
    "xtdb.indexer.LogProcessor", "rtdb.conformance.JunitRecorder",
}
MAX_EVENTS = 65_536
MAX_JSON = 32 * 1024 * 1024
OVERLAY_HASHES = {
    "core/src/test/kotlin/xtdb/DeterministicDispatcher.kt":
        "444f127568d1f5cc5f2215a09098d3d363746bdef56531f69fb43c7fb2897e9e",
    "core/src/test/kotlin/xtdb/SimulationTestBase.kt":
        "d6caafa75e6a37fcc01a099ad9beae5d8be99a3074d0de903d283eea3e5a6893",
    "core/src/test/kotlin/xtdb/indexer/SimLog.kt":
        "be39b5a3158a7f188d63ca7f7f1cd8a29cf5517f1eb31efc290189ef0488a352",
    FIXTURE: "ea66a722d649eb5ade400c939af02f2cb39da141e63bb51b58b0924fe5a8f047",
}
OVERLAYS = {
    "core/src/test/kotlin/xtdb/DeterministicDispatcher.kt": (
        ("    private var runningThread: Thread? = null",
         "    internal var simulationTrace: LogProcessorTrace? = null\n\n    private var runningThread: Thread? = null"),
        ("""                val idx = rand.nextInt(jobs.size)
                val job = jobs.removeAt(idx)
                job.block.run()""",
         """                val queued = jobs.size
                val idx = rand.nextInt(jobs.size)
                val job = jobs.removeAt(idx)
                simulationTrace?.event("pick", queued, idx)
                job.block.run()"""),
    ),
    "core/src/test/kotlin/xtdb/SimulationTestBase.kt": (
        ("import org.junit.jupiter.api.extension.BeforeEachCallback",
         "import org.junit.jupiter.api.extension.AfterEachCallback\nimport org.junit.jupiter.api.extension.BeforeEachCallback\nimport java.nio.file.Path"),
        ("class SeedExtension : BeforeEachCallback, TestExecutionExceptionHandler {",
         "class SeedExtension : BeforeEachCallback, AfterEachCallback, TestExecutionExceptionHandler {"),
        ("""                it.dispatcher = DeterministicDispatcher(seed)
            }
    }

    override fun handleTestExecutionException""",
         """                it.dispatcher = DeterministicDispatcher(seed)
                it.simulationTrace = LogProcessorTrace.select(ctx, seed)
                it.dispatcher.simulationTrace = it.simulationTrace
            }
    }

    override fun afterEach(ctx: ExtensionContext) {
        val test = ctx.requiredTestInstance as SimulationTestBase
        test.simulationTrace?.write(ctx, Path.of(System.getProperty("rtdb.inventory.output")))
    }

    override fun handleTestExecutionException"""),
        ("abstract class SimulationTestBase {", "abstract class SimulationTestBase {\n    internal var simulationTrace: LogProcessorTrace? = null"),
    ),
    "core/src/test/kotlin/xtdb/indexer/SimLog.kt": (
        ("import xtdb.api.log.Log\n", "import xtdb.LogProcessorTrace\nimport xtdb.api.log.Log\n"),
        ("internal class SimLog<M>(private val name: String, private val rand: Random) : Log<M> {",
         """internal class SimLog<M>(
    private val name: String, private val rand: Random, private val trace: LogProcessorTrace? = null
) : Log<M> {"""),
        ("    class Consumer<M>(var nextOffset: Int) {",
         """    class Consumer<M>(var nextOffset: Int, val traceTail: Int?) {
        var emptyTipCount = 0L"""),
        ("""    override suspend fun enqueueMessage(message: M, partition: Int): Deferred<MessageMetadata> {""",
         """    override suspend fun enqueueMessage(message: M, partition: Int): Deferred<MessageMetadata> =
        enqueueObserved(message, null)

    suspend fun enqueueObserved(message: M, actor: Int?): Deferred<MessageMetadata> {"""),
        ("        inFlight += Record(epoch, offset, ts, message) to handle",
         "        inFlight += Record(epoch, offset, ts, message) to handle\n        trace?.enqueue(name, actor, epoch, offset, ts, message)"),
        ("""        latestSubmittedOffset0 = record.logOffset
        consumers.forEach<Consumer<M>> { it.wake.trySend(Unit) }
        handle.complete(MessageMetadata(epoch, record.logOffset, record.logTimestamp))""",
         """        latestSubmittedOffset0 = record.logOffset
        trace?.event("durable", name, record.logOffset)
        consumers.forEach<Consumer<M>> { it.wake.trySend(Unit) }
        trace?.event("receipt", name, record.logOffset)
        handle.complete(MessageMetadata(epoch, record.logOffset, record.logTimestamp))"""),
        ("""        action: suspend (Log.Tail<M>) -> R,
    ): R {
        val startOffset""",
         """        action: suspend (Log.Tail<M>) -> R,
    ): R = withObservedTail(afterMsgId, null, action)

    suspend fun <R> withObservedTail(afterMsgId: MessageId, actor: Int?, action: suspend (Log.Tail<M>) -> R): R {
        val startOffset"""),
        ("        val consumer = Consumer<M>(startOffset)",
         "        val consumer = Consumer<M>(startOffset, trace?.openedTail(name, actor, startOffset))"),
        ("""                            consumer.reportTip = false
                            return emptyList()""",
         """                            consumer.reportTip = false
                            if (trace != null) {
                                consumer.emptyTipCount++
                                trace.event("tip", name, consumer.traceTail, nextOffset)
                            }
                            return emptyList()"""),
        ("                        consumer.nextOffset += messageCount",
         """                        consumer.nextOffset += messageCount
                        trace?.event("delivery", name, consumer.traceTail, nextOffset, consumer.nextOffset)"""),
        ("            consumers -= consumer",
         '            consumers -= consumer\n            trace?.event("tail_closed", name, consumer.traceTail, consumer.nextOffset, consumer.reportTip, consumer.emptyTipCount)'),
        ("    fun reportTip() = consumers.forEach { it.reportTip = true; it.wake.trySend(Unit) }",
         """    fun reportTip() = consumers.forEach {
        it.reportTip = true
        trace?.event("tip_requested", name, it.traceTail)
        it.wake.trySend(Unit)
    }"""),
        ("""    suspend fun awaitAllDelivered() {
        while (inFlight.isNotEmpty() || consumers.any { it.nextOffset < topic.size }) {
            consumers.forEach { it.wake.trySend(Unit) }
            yield()
        }
    }""",
         """    suspend fun awaitAllDelivered() {
        while (inFlight.isNotEmpty() || consumers.any { it.nextOffset < topic.size }) {
            consumers.forEach { it.wake.trySend(Unit) }
            yield()
        }
        trace?.event("replica_delivered", latestEnqueuedOffset + 1, latestSubmittedOffset0 + 1,
            consumers.map { listOf(it.traceTail, it.nextOffset) })
    }

    fun observeIdle() {
        trace?.event("idle", consumers.map { listOf(it.traceTail, it.reportTip, it.emptyTipCount) })
    }"""),
    ),
    "core/src/test/kotlin/xtdb/indexer/LogProcessorSimTest.kt": (
        ("import xtdb.NodeBase\n", "import xtdb.LogProcessorTrace\nimport xtdb.NodeBase\n"),
        ("private class NodeReplicaLog(private val log: Log<ReplicaMessage>) : Log<ReplicaMessage> by log {",
         """private class NodeReplicaLog(
    private val log: SimLog<ReplicaMessage>, private val trace: LogProcessorTrace?, private val actor: Int?
) : Log<ReplicaMessage> by log {"""),
        ("""        log.enqueueMessage(message, partition).also { enqueued ->
            enqueued.invokeOnCompletion { if (it == null) appendedOffsets += enqueued.getCompleted().logOffset }
        }

    override suspend fun appendMessage""",
         """        log.enqueueObserved(message, actor).also { enqueued ->
            enqueued.invokeOnCompletion {
                if (it == null) {
                    val offset = enqueued.getCompleted().logOffset
                    appendedOffsets += offset
                    trace?.event("replica_ack", actor, offset)
                }
            }
        }

    override suspend fun <R> withTail(
        partition: Int, afterMsgId: MessageId, action: suspend (Log.Tail<ReplicaMessage>) -> R
    ): R = log.withObservedTail(afterMsgId, actor, action)

    override suspend fun appendMessage"""),
        ('        srcLog = SimLog("src", rand)\n        replicaLog = SimLog("replica", rand)',
         '        srcLog = SimLog("src", rand, simulationTrace)\n        replicaLog = SimLog("replica", rand, simulationTrace)'),
        ("            nextActionIdxState.value = actionIdx",
         '            simulationTrace?.event("assignment", afterToken?.copyOf(), actionIdx)\n            nextActionIdxState.value = actionIdx'),
        ("""                txIndexer.executeTx(externalSourceToken = externalSourceToken) { openTx ->
                    when (action) {""",
         """                simulationTrace?.event("external_start", actionIdx, externalSourceToken.copyOf())
                val result = txIndexer.executeTx(externalSourceToken = externalSourceToken) { openTx ->
                    simulationTrace?.event("external_writer", actionIdx, openTx.txKey.txId, openTx.txKey.systemTime)
                    when (action) {"""),
        ("                nextActionIdxState.value = ++actionIdx",
         """                simulationTrace?.event("external_applied", actionIdx, result.txKey.txId, result.txKey.systemTime,
                    result is xtdb.api.TransactionResult.Committed)
                nextActionIdxState.value = ++actionIdx"""),
        ("        private val readOnly: Boolean = false,\n    ) : AutoCloseable {",
         "        private val readOnly: Boolean = false,\n        val traceActor: Int? = null,\n    ) : AutoCloseable {"),
        ("        val nodeReplicaLog = NodeReplicaLog(replicaLog)",
         "        val nodeReplicaLog = NodeReplicaLog(replicaLog, simulationTrace, traceActor)"),
        ("        val isLeader get() = logProcessor?.isLeader ?: false",
         """        val isLeader get() = logProcessor?.isLeader ?: false

        fun sampledLeader() = isLeader.also { simulationTrace?.event("role_sample", traceActor, it) }"""),
        ("""    private class ClaimRefusingDriver(private val inner: LogProcessor.LogsDriver) : LogProcessor.LogsDriver by inner {
        override suspend fun enqueueToReplica(msg: ReplicaMessage): Deferred<Log.MessageMetadata> =
            if (msg is NoOp && msg.srcMsgId == null)
                throw IOException("[sim] the replica log will not accept a claim")
            else inner.enqueueToReplica(msg)
    }""",
         """    private class ClaimRefusingDriver(
        private val inner: LogProcessor.LogsDriver, private val trace: LogProcessorTrace?, private val actor: Int?
    ) : LogProcessor.LogsDriver by inner {
        override suspend fun enqueueToReplica(msg: ReplicaMessage): Deferred<Log.MessageMetadata> =
            if (msg is NoOp && msg.srcMsgId == null) {
                trace?.event("claim_refused", actor, msg.termId, msg.termSeq, msg.srcMsgId)
                throw IOException("[sim] the replica log will not accept a claim")
            } else inner.enqueueToReplica(msg)
    }"""),
    ),
}
CASE_EDITS = (
    ("""        runTest(timeout = 5.seconds) {
            val indexerConfig""",
     """        runTest(timeout = 5.seconds) {
            val trace = simulationTrace
            trace?.start()
            val indexerConfig"""),
    ("            val simExtSource = SimExtSource(buildActions(rand, totalActions))",
     "            val actions = buildActions(rand, totalActions)\n            val simExtSource = SimExtSource(actions)"),
    ("            val srcLogEventCount = rand.nextInt(20, 40)",
     """            val srcLogEventCount = rand.nextInt(20, 40)
            trace?.event("scenario", indexerConfig.rowsPerBlock, totalActions, srcLogEventCount,
                actions.map { if (it is SimAction.Commit) it.rows.toList() else null })"""),
    ('SimNode("test-db", bp, indexerConfig, simExtSource).use { incumbent',
     'SimNode("test-db", bp, indexerConfig, simExtSource, traceActor = 0).use { incumbent'),
    ('SimNode("test-db", bp, indexerConfig, simExtSource).use { refusedA',
     'SimNode("test-db", bp, indexerConfig, simExtSource, traceActor = 1).use { refusedA'),
    ('SimNode("test-db", bp, indexerConfig, simExtSource).use { refusedB',
     'SimNode("test-db", bp, indexerConfig, simExtSource, traceActor = 2).use { refusedB'),
    ("                                refused.forEach { it.openLogProcessor(this) { inner -> ClaimRefusingDriver(inner) } }",
     "                                refused.forEach { it.openLogProcessor(this) { inner -> ClaimRefusingDriver(inner, trace, it.traceActor) } }"),
    ("""                                    repeat(srcLogEventCount) {
                                        yield()
                                        // Sampled every step""",
     """                                    repeat(srcLogEventCount) { step ->
                                        yield()
                                        trace?.event("step", step)
                                        // Sampled every step"""),
    ("""                                        refusedLed = refusedLed || refused.any { it.isLeader }
                                        if (rand.nextInt(100) < 50) replicaLog.reportTip()
                                        else srcLog.appendMessage(SourceMessage.FlushBlock(null))""",
     """                                        refusedLed = refusedLed || refused.any { it.sampledLeader() }
                                        if (rand.nextInt(100) < 50) {
                                            trace?.event("choice", step, "tip")
                                            replicaLog.reportTip()
                                            trace?.event("tip_round_finished", step)
                                        } else {
                                            trace?.event("choice", step, "flush")
                                            srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                        }"""),
    ("""                                    simExtSource.awaitQuiescence()
                                    replicaLog.awaitAllDelivered()
                                    nodes.awaitTx(lastAppliedTxId())
                                    awaitIdle()

                                    refusedLed = refusedLed || refused.any { it.isLeader }
                                    leadersAtQuiescence = nodes.leaderCount()""",
     """                                    simExtSource.awaitQuiescence()
                                    trace?.event("external_settled")
                                    replicaLog.awaitAllDelivered()
                                    nodes.awaitTx(lastAppliedTxId().also { trace?.event("await_tx", it) })
                                    trace?.event("nodes_caught_up")
                                    awaitIdle()
                                    replicaLog.observeIdle()
                                    refusedLed = refusedLed || refused.any { it.sampledLeader() }
                                    leadersAtQuiescence = nodes.count { it.sampledLeader() }
                                    trace?.event("quiescence", refusedLed, leadersAtQuiescence)"""),
    ("""                            }.join()

                            // `assertNoNodeFailed`""",
     """                            }.join()
                            trace?.event("joined")

                            // `assertNoNodeFailed`"""),
    ("""                                "a refused claim costs the database no transactions (seed=$currentSeed)"
                            )""",
     """                                "a refused claim costs the database no transactions (seed=$currentSeed)"
                            )
                            trace?.assertionsFinished()"""),
)


LIVE_INDEX_OVERLAY = (
    """    private fun assertLiveIndexesCaughtUp(nodes: List<SimNode>) {
        for ((idx, node) in nodes.withIndex())
            assertEquals(
                lastAppliedTxId(), node.liveIndex.state.latestCompletedTx?.txId,
                "node $idx's live index has not applied the log to its end (seed=$currentSeed)"
            )
    }""",
    """    private fun assertLiveIndexesCaughtUp(nodes: List<SimNode>) {
        for ((idx, node) in nodes.withIndex()) {
            val expected = lastAppliedTxId()
            val actual = node.liveIndex.state.latestCompletedTx?.txId
            assertEquals(
                expected, actual,
                "node $idx's live index has not applied the log to its end (seed=$currentSeed)"
            )
            simulationTrace?.event("live_index_caught_up", idx, expected, actual)
        }
    }""",
)


def replace_once(source, before, after):
    if source.count(before) != 1:
        raise ValueError(f"reference overlay anchor is missing or repeated: {before.splitlines()[0]}")
    return source.replace(before, after, 1)


def derived_source(path, raw, seed, tape_schema=LIVE_INDEX_TAPE_SCHEMA):
    require(type(tape_schema) is int and tape_schema in (1, LIVE_INDEX_TAPE_SCHEMA), "unknown simulation tape version")
    if hashlib.sha256(raw).hexdigest() != OVERLAY_HASHES[path]:
        raise ValueError(f"reference test source differs from the pinned overlay input: {path}")
    source = raw.decode("utf-8")
    for before, after in OVERLAYS[path]:
        source = replace_once(source, before, after)
    if path == FIXTURE:
        start = f"    fun `{METHOD}`() =\n"
        end = '    @RepeatableSimulationTest\n    fun `a node joining a live database catches up without unseating its leader`'
        offset = source.index(start)
        boundary = source.index(end, offset)
        selected = source[offset:boundary]
        for before, after in CASE_EDITS:
            selected = replace_once(selected, before, after)
        if seed is not None:
            selected = f"    @xtdb.WithSeed({seed})\n" + selected
        source = source[:offset] + selected + source[boundary:]
        if tape_schema == LIVE_INDEX_TAPE_SCHEMA:
            source = replace_once(source, *LIVE_INDEX_OVERLAY)
    return source.encode("utf-8")


def require(condition, failure):
    if not condition:
        raise ValueError(failure)


def verify_overlay(tree, expected, reference_revision):
    require(git(tree, "rev-parse", "HEAD") == reference_revision, "derived reference revision changed")
    dirty = set(git(tree, "diff", "--name-only", "HEAD").splitlines())
    dirty.update(git(tree, "ls-files", "--others", "--exclude-standard").splitlines())
    require(dirty == set(expected), "derived reference changed outside its declared test overlay")
    for name, sha256 in expected.items():
        path = tree / name
        require(not path.is_symlink() and digest(path) == sha256, f"derived reference file changed: {name}")


def owned_overlay(reference, run, seed, reference_revision):
    tree = run / "reference"
    subprocess.run(["git", "init", "--quiet", "--template=", str(tree)], check=True)
    subprocess.run(["git", "-C", str(tree), "fetch", "--quiet", "--depth=1", "--no-tags", "--",
                    str(reference), reference_revision], check=True)
    subprocess.run(["git", "-C", str(tree), "checkout", "--quiet", "--detach", reference_revision], check=True)
    require(git(tree, "rev-parse", "HEAD") == reference_revision, "local fetch did not produce the selected upstream revision")
    require(not git(tree, "status", "--porcelain", "--untracked-files=all"), "derived checkout is dirty")
    require(not (tree / ".git/objects/info/alternates").exists(), "derived checkout borrows Git objects")
    expected = {}
    for name, sha256 in OVERLAY_HASHES.items():
        path = tree / name
        raw = path.read_bytes()
        require(not path.is_symlink() and digest(reference / name) == sha256, f"original overlay input changed: {name}")
        origin = run / "original" / name
        origin.parent.mkdir(parents=True, exist_ok=True)
        origin.write_bytes(raw)
        derived = derived_source(name, raw, seed)
        path.write_bytes(derived)
        expected[name] = hashlib.sha256(derived).hexdigest()
    helper = tree / HELPER
    recorder = run / "adapter/simulation/LogProcessorTrace.kt"
    with helper.open("xb") as target:
        target.write(recorder.read_bytes())
    expected[HELPER] = digest(recorder)
    verify_overlay(tree, expected, reference_revision)
    return tree, expected


def gradle_command(tree, run):
    adapter = run / "adapter"
    return [tree / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk", "--init-script",
            adapter / "inventory.init.gradle", f"-Prtdb.adapterRoot={adapter}",
            f"-Prtdb.inventoryOutput={run / 'inventory'}", "-PsimulationIterations=1",
            TASK, "--tests", f"{TEST_CLASS}.{METHOD}"]


def regular(path):
    require(path.is_file() and path.resolve() == path, f"missing or redirected evidence file: {path}")
    return path


def bounded_bytes(path):
    with regular(path).open("rb") as source:
        data = source.read(MAX_JSON + 1)
    require(len(data) <= MAX_JSON, f"evidence file exceeds its size bound: {path}")
    return data


def bounded_json(path):
    return json.loads(bounded_bytes(path))


@dataclass(frozen=True)
class RecordedFile:
    path: Path
    data: bytes
    sha256: str

    @classmethod
    def read(cls, path):
        data = bounded_bytes(path)
        return cls(path, data, hashlib.sha256(data).hexdigest())

    def receipt(self, root):
        return {"path": self.path.relative_to(root).as_posix(), "sha256": self.sha256, "bytes": len(self.data)}


def selected_invocation(run):
    inventory = run / "inventory"
    lanes_file = RecordedFile.read(inventory / "requested-lanes.json")
    lanes = json.loads(lanes_file.data)
    require(lanes == {"schema_version": 1, "lanes": [{"project": PROJECT, "task": TASK, "enabled": True}]},
            "Gradle did not select exactly the requested property task")
    directory = inventory / LANE
    selection_file = RecordedFile.read(directory / "selection.json")
    selection = json.loads(selection_file.data)
    require(selection["schema_version"] == 1 and selection["project"] == PROJECT and selection["task"] == TASK
            and selection["include_tags"] == ["property"] and str(selection["simulation_iterations"]) == "1",
            "Gradle selected different task options")
    streams = list(directory.glob("*.jsonl"))
    require(len(streams) == 1, "expected one selected JUnit worker stream")
    journal = RecordedFile.read(streams[0])
    frames = [json.loads(line) for line in journal.data.decode("utf-8").splitlines() if line.strip()]
    plan = inspect_plan(journal.path, frames, journal.sha256)
    require(not plan["errors"] and plan["project"] == PROJECT and plan["task"] == TASK and len(plan["cases"]) == 1,
            "the JUnit worker did not record exactly one concrete selected case")
    case, = plan["cases"]
    unique = case["descriptor"]["unique_id"]
    require(case["outcome"].get("status") == "SUCCESSFUL", "original assertion invocation did not succeed")
    descriptors = {frame["descriptor"]["unique_id"]: frame["descriptor"] for frame in frames
                   if frame["event"] in {"discovered", "dynamic_registered"}}
    descriptor = case["descriptor"]
    parents = set()
    while not descriptor.get("source", {}).get("method_name"):
        parent = descriptor.get("parent_id")
        require(parent in descriptors and parent not in parents, "selected case has no original method identity")
        parents.add(parent)
        descriptor = descriptors[parent]
    require(descriptor["source"]["class_name"] == TEST_CLASS and descriptor["source"]["method_name"] == METHOD
            and case["descriptor"]["display_name"] == f"{METHOD} [iteration 1]",
            "JUnit selected a different original case")
    trace_markers = [frame for frame in frames if frame["event"] == "report_entry"
                     and "rtdb.simulation.trace" in frame["entries"]]
    require(len(trace_markers) == 1, "JUnit worker omitted or repeated its simulation tape link")
    marker, = trace_markers
    start, = [frame["sequence"] for frame in frames if frame["event"] == "execution_started"
              and frame["descriptor"]["unique_id"] == unique]
    require(marker["descriptor"]["unique_id"] == unique and start < marker["sequence"] < case["outcome"]["sequence"],
            "simulation tape marker belongs to a different invocation")
    name = f"causal/{hashlib.sha256(unique.encode('utf-8')).hexdigest()}.json"
    recorded = RecordedFile.read(run / name)
    require(marker["entries"]["rtdb.simulation.trace"] == name and
            marker["entries"].get("rtdb.simulation.sha256") == recorded.sha256,
            "JUnit simulation tape link differs from the retained bytes")
    return unique, selection, (lanes_file, selection_file, journal), recorded


def int_value(value, lower, upper=2**63 - 1):
    return type(value) is int and lower <= value <= upper


def timestamp(value):
    return isinstance(value, list) and len(value) == 2 and int_value(value[0], -(2**63)) and int_value(value[1], 0, 999_999_999)


def hex_token(value):
    return isinstance(value, str) and re.fullmatch("[0-9a-f]{8}", value) is not None


def action_token(index):
    return index.to_bytes(4, "big").hex()


def optional_int(value, lower):
    return value is None or int_value(value, lower)


def checked_message(stream, message):
    require(isinstance(message, list) and len(message) > 0 and isinstance(message[0], str),
            "log enqueue has no observed message")
    kind, *fields = message
    if stream == "src":
        require((kind == "flush" and len(fields) == 1 and optional_int(fields[0], -1))
                or (kind == "other_source" and len(fields) == 1 and isinstance(fields[0], str)),
                "invalid source message fields")
        return
    if kind == "other_replica":
        require(len(fields) == 3 and isinstance(fields[0], str)
                and int_value(fields[1], 0) and optional_int(fields[2], 0), "invalid observed replica message")
        return
    require(kind in {"noop", "transaction", "boundary", "upload"}, "unknown observed replica message")
    event_fields(kind, fields, {"noop": 3, "transaction": 7, "boundary": 5, "upload": 5}[kind])
    require(int_value(fields[0], 1) and optional_int(fields[1], 0), "invalid replica term fields")
    if kind == "noop":
        require(optional_int(fields[2], -1), "invalid NoOp watermark")
    elif kind == "transaction":
        require(int_value(fields[2], 0) and timestamp(fields[3]) and type(fields[4]) is bool
                and hex_token(fields[5]) and optional_int(fields[6], -1), "invalid resolved transaction fields")
    else:
        require(int_value(fields[2], 0) and int_value(fields[3], -1)
                and (fields[4] is None or hex_token(fields[4])), "invalid block message fields")


def event_fields(kind, fields, size):
    require(isinstance(fields, list) and len(fields) == size, f"wrong simulation fields for {kind}")


def check_tape(tape, unique, seed):
    schema = tape["schema_version"]
    require(type(schema) is int and schema in (1, LIVE_INDEX_TAPE_SCHEMA) and tape["unique_id"] == unique
            and int_value(tape["seed"], -(2**31), 2**31 - 1) and (seed is None or tape["seed"] == seed),
            "simulation tape has a different invocation or seed")
    require(tape["assertions_finished"] is True and tape["overflow"] is False, "simulation observer is incomplete")
    frames = tape["events"]
    require(isinstance(frames, list) and 2 <= len(frames) <= MAX_EVENTS
            and frames[0].get("kind") == "case_started" and frames[-1].get("kind") == "assertions_finished",
            "simulation tape is missing its original entry or assertion completion")
    for index, frame in enumerate(frames):
        require(set(frame) == {"sequence", "kind", "fields"} and type(frame["sequence"]) is int and frame["sequence"] == index,
                f"out-of-order simulation tape event {index}")
    kind = frames[1]["kind"]
    fields = frames[1]["fields"]
    require(kind == "scenario", "simulation tape has no authored scenario")
    event_fields(kind, fields, 4)
    rows, actions, steps, documents = fields
    require(int_value(rows, 15, 24) and int_value(actions, 30, 59) and int_value(steps, 20, 39)
            and isinstance(documents, list) and len(documents) == actions, "simulation generator differs from the original case")
    for group in documents:
        if group is not None:
            require(isinstance(group, list) and 1 <= len(group) <= 5
                    and all(isinstance(row, str) and str(UUID(row)) == row for row in group), "invalid UUID group")

    enqueued = {"src": [], "replica": []}
    durable = {"src": 0, "replica": 0}
    received = {"src": 0, "replica": 0}
    writers, tails, opened, refused = set(), {}, set(), set()
    replica_actors = set()
    next_step = next_choice = next_action = 0
    external_attempt = external_write = None
    applied_tx = -1
    transactions = {}
    flush_choices = 0
    sampled = []
    refused_led = False
    tip_round = None
    assigned = False
    picked = False
    leaders = None
    checkpoints = []
    live_indexes = []
    checkpoint_order = ["external_settled", "replica_delivered", "await_tx", "nodes_caught_up", "idle", "quiescence", "joined"]

    for frame in frames[2:-1]:
        kind, fields = frame["kind"], frame["fields"]
        require(tip_round is None or kind in {"tip_requested", "tip_round_finished"},
                "tip-choice notification was interrupted or omitted")
        if kind == "pick":
            event_fields(kind, fields, 2)
            queued, choice = fields
            require(int_value(queued, 1) and int_value(choice, 0, queued - 1), "invalid dispatcher pick")
            picked = True
        elif kind == "step":
            event_fields(kind, fields, 1)
            require(fields[0] == next_step == next_choice, "original driver steps are out of order")
            next_step += 1
        elif kind == "choice":
            event_fields(kind, fields, 2)
            require(fields[0] == next_choice == next_step - 1 and fields[1] in {"tip", "flush"}
                    and [actor for actor, _ in sampled] == [1, 2], "unmatched driver choice or original role samples")
            refused_led = refused_led or any(leading for _, leading in sampled)
            if fields[1] == "tip":
                tip_round = {tail for stream, tail in tails if stream == "replica"}
            else:
                flush_choices += 1
            next_choice += 1
            sampled.clear()
        elif kind == "tip_round_finished":
            event_fields(kind, fields, 1)
            require(tip_round == set() and fields == [next_choice - 1], "tip choice omitted a replica tail notification")
            tip_round = None
        elif kind == "role_sample":
            event_fields(kind, fields, 2)
            require(int_value(fields[0], 0, 2) and type(fields[1]) is bool, "invalid original role sample")
            sampled.append(tuple(fields))
        elif kind == "claim_refused":
            event_fields(kind, fields, 4)
            actor, term, sequence, source = fields
            require(int_value(actor, 1, 2) and int_value(term, 1) and sequence == 0 and source is None
                    and any(state[0] == actor for state in tails.values()), "invalid pre-enqueue claim refusal")
            refused.add(actor)
        elif kind == "enqueue":
            event_fields(kind, fields, 6)
            stream, actor, epoch, offset, time, message = fields
            require(stream in enqueued and int_value(epoch, 0, 0) and type(offset) is int
                    and offset == len(enqueued[stream]) and timestamp(time), "log enqueue is out of order")
            require((stream == "src" and actor is None) or (stream == "replica" and int_value(actor, 0, 0)),
                    "log enqueue has no valid producer")
            checked_message(stream, message)
            enqueued[stream].append((actor, message))
        elif kind in {"durable", "receipt"}:
            event_fields(kind, fields, 2)
            stream, offset = fields
            counter, preceding = (durable, len(enqueued[stream])) if kind == "durable" else (received, durable[stream])
            require(type(offset) is int and offset == counter[stream] < preceding, f"log {kind} is out of order")
            counter[stream] += 1
            if kind == "durable" and stream == "replica":
                _, message = enqueued[stream][offset]
                if message[0] == "transaction":
                    _, _, _, tx, time, committed, token, _ = message
                    require(token not in transactions, "duplicate durable external transaction token")
                    transactions[token] = [tx, time, committed]
        elif kind == "replica_ack":
            event_fields(kind, fields, 2)
            actor, offset = fields
            require(int_value(offset, 0, received["replica"] - 1) and offset not in writers
                    and actor == enqueued["replica"][offset][0], "replica writer acknowledgement lacks durable authorship")
            writers.add(offset)
        elif kind == "tail_open":
            event_fields(kind, fields, 4)
            stream, tail, actor, start = fields
            require(stream in durable and tail == len(opened) and int_value(start, 0, durable[stream])
                    and ((stream == "src" and actor is None) or (stream == "replica" and int_value(actor, 0, 2))),
                    "invalid tail registration")
            opened.add(tail)
            tails[stream, tail] = (actor, start, False, 0)
            if stream == "replica":
                replica_actors.add(actor)
        elif kind in {"delivery", "tip_requested", "tip", "tail_closed"}:
            event_fields(kind, fields, {"delivery": 4, "tip_requested": 2, "tip": 3, "tail_closed": 5}[kind])
            stream, tail, *offsets = fields
            require((stream, tail) in tails, "log event belongs to a tail outside its lifetime")
            actor, position, requested, empty_tips = tails[stream, tail]
            if kind == "delivery":
                require(offsets[0] == position and int_value(offsets[1], position + 1, durable[stream]),
                        "log delivered outside its durable prefix")
                position = offsets[1]
            elif kind == "tip_requested":
                require(stream == "replica" and tip_round is not None and tail in tip_round,
                        "tip request is outside the original choice or duplicated")
                tip_round.remove(tail)
                requested = True
            elif kind == "tip":
                require(requested and offsets == [position] and position == durable[stream], "poll did not observe its requested tip")
                requested = False
                empty_tips += 1
            else:
                require(tuple(type(value) for value in offsets) == (int, bool, int)
                        and offsets == [position, requested, empty_tips],
                        "tail closed at a different position, pending tip state or empty-tip count")
                del tails[stream, tail]
                continue
            tails[stream, tail] = (actor, position, requested, empty_tips)
        elif kind == "assignment":
            event_fields(kind, fields, 2)
            token, resume = fields
            require(resume == next_action and token == (None if resume == 0 else action_token(resume - 1)),
                    "external assignment did not resume from its applied zero-based token")
            external_attempt = external_write = None
            assigned = True
        elif kind == "external_start":
            event_fields(kind, fields, 2)
            require(assigned and external_attempt is None and next_action < actions and fields == [next_action, action_token(next_action)],
                    "external source submitted a different action or token")
            external_attempt = next_action
        elif kind == "external_writer":
            event_fields(kind, fields, 3)
            index, tx, time = fields
            require(index == external_attempt and external_write is None and int_value(tx, 0) and timestamp(time),
                    "external writer ran outside its observed action")
            external_write = [tx, time]
        elif kind == "external_applied":
            event_fields(kind, fields, 4)
            index, tx, time, committed = fields
            require(index == external_attempt == next_action and external_write == [tx, time]
                    and type(committed) is bool and committed == (documents[index] is not None)
                    and transactions.get(action_token(index)) == [tx, time, committed] and tx > applied_tx,
                    "applied external outcome differs from its original writer or durable record")
            applied_tx = tx
            next_action += 1
            external_attempt = external_write = None
        elif kind in checkpoint_order:
            event_fields(kind, fields, {"replica_delivered": 3, "await_tx": 1, "idle": 1, "quiescence": 2}.get(kind, 0))
            require(len(checkpoints) < len(checkpoint_order) and kind == checkpoint_order[len(checkpoints)],
                    "original quiescence checkpoints are out of order")
            checkpoints.append(kind)
            if kind == "external_settled":
                require(next_action == actions and next_choice == steps, "external source settled before original work finished")
            if kind in {"replica_delivered", "idle"}:
                replicas = {tail: state for (stream, tail), state in tails.items() if stream == "replica"}
                require(sorted(state[0] for state in replicas.values()) == [0, 1, 2], "checkpoint lacks its three replica tails")
                tail_samples = fields[-1]
                sample_types = (int, int) if kind == "replica_delivered" else (int, bool, int)
                require(isinstance(tail_samples, list) and all(isinstance(sample, list)
                        and tuple(type(value) for value in sample) == sample_types for sample in tail_samples),
                        "invalid replica checkpoint tail samples")
                if kind == "replica_delivered":
                    enqueued_count = len(enqueued["replica"])
                    require(int_value(fields[0], 1) and int_value(fields[1], 1) and fields[0] == fields[1] == enqueued_count
                            == durable["replica"] == received["replica"]
                            and fields[2] == [[tail, state[1]] for tail, state in replicas.items()]
                            and all(state[1] == enqueued_count for state in replicas.values()),
                            "replica drain checkpoint differs from observed enqueue, durability, receipt or tail delivery")
                else:
                    require(fields == [[[tail, state[2], state[3]] for tail, state in replicas.items()]],
                            "idle checkpoint differs from the observed replica tip requests or empty-tip counts")
            if kind == "await_tx":
                require(fields == [applied_tx], "observation omitted the awaited transaction")
            if kind == "quiescence":
                require([actor for actor, _ in sampled] == [1, 2, 0, 1, 2], "quiescence omitted the original role samples")
                refused_led = refused_led or any(leading for _, leading in sampled[:2])
                require(type(fields[0]) is bool and type(fields[1]) is int
                        and fields == [refused_led, sum(leading for _, leading in sampled[2:])],
                        "quiescence role aggregate contradicts the recorded samples")
                require(sampled[:2] == sampled[3:], "refused peer role changed between consecutive quiescence reads")
                leaders = fields
        elif kind == LIVE_INDEX_ASSERTION:
            event_fields(kind, fields, 3)
            actor, expected, actual = fields
            require(schema == LIVE_INDEX_TAPE_SCHEMA and checkpoints == checkpoint_order
                    and int_value(actor, 0, 2) and actor == len(live_indexes)
                    and int_value(expected, 0) and int_value(actual, 0) and expected == actual == applied_tx,
                    "post-join live-index assertion differs from the original actor or awaited transaction")
            live_indexes.append(fields)
        else:
            raise ValueError(f"unexpected simulation event: {kind}")

    event_fields("case_started", frames[0]["fields"], 0)
    event_fields("assertions_finished", frames[-1]["fields"], 0)
    require(not tails and picked and assigned and next_action == actions and next_choice == steps
            and refused == {1, 2} and replica_actors == {0, 1, 2} and leaders == [False, 1]
            and checkpoints == checkpoint_order and tip_round is None and writers == set(range(durable["replica"]))
            and len(transactions) == actions and flush_choices == sum(message == ["flush", None] for _, message in enqueued["src"])
            and all(durable[stream] == received[stream] == len(enqueued[stream]) for stream in enqueued)
            and enqueued["replica"] and enqueued["replica"][0][1] == ["noop", 1, 0, None],
            "simulation did not observe the original refusal, stream drain and convergence boundaries")
    observed = {"seed": tape["seed"], "events": len(frames), "actions": actions, "steps": steps}
    if schema == LIVE_INDEX_TAPE_SCHEMA:
        require(len(live_indexes) == 3 and all(frame["kind"] == LIVE_INDEX_ASSERTION for frame in frames[-4:-1]),
                "original invocation omitted post-join live-index assertions")
        observed[LIVE_INDEX_ASSERTION] = live_indexes
    return observed


def runtime_requests(selection, tape):
    home = Path(tape["java_home_request"])
    jvm_args = tape["jvm_args"]
    require(home.is_absolute() and isinstance(jvm_args, list) and all(isinstance(value, str) for value in jvm_args),
            "malformed JVM home or arguments")
    agents = [value.removeprefix("-javaagent:").split("=", 1)[0]
              for value in jvm_args if value.startswith("-javaagent:")]
    require(agents, "JUnit worker omitted the original watchdog agent")
    requests = {
        "classpath": selection["classpath"],
        "test_class_directories": selection["test_class_directories"],
        "java": [tape["java_executable_request"], str(home / "bin/java")],
        "modules": [str(home / "lib/modules")],
        "java_agents": agents,
    }
    require(requests["classpath"] and all(isinstance(paths, list) and
            all(isinstance(path, str) and Path(path).is_absolute() for path in paths) for paths in requests.values()),
            "Gradle runtime paths are missing or relative")
    return requests


def resolve_runtime_requests(requests):
    def resolve(name, optional):
        try:
            return str(Path(name).resolve(strict=True))
        except FileNotFoundError:
            require(optional, f"required JVM runtime path is missing: {name}")
            return None

    return {kind: [{"requested": name, "canonical": resolve(name, kind in {"classpath", "test_class_directories"})}
                   for name in paths] for kind, paths in requests.items()}


def require_consistent_runtime_aliases(resolved):
    canonical_by_request = {}
    for entries in resolved.values():
        for entry in entries:
            canonical = entry["canonical"]
            require(canonical_by_request.setdefault(entry["requested"], canonical) == canonical,
                    "one JVM runtime request has inconsistent resolutions")


def require_disjoint_runtime_root(run, location):
    artifacts = run / "artifacts"
    require(not location.is_relative_to(artifacts) and not artifacts.is_relative_to(location),
            f"runtime root overlaps its own artifact directory: {location}")


def runtime_tree(directory):
    pending = [directory]
    while pending:
        current = pending.pop()
        require(stat.S_ISDIR(current.lstat().st_mode), f"invalid runtime directory: {current}")
        for member in current.iterdir():
            mode = member.lstat().st_mode
            is_directory = stat.S_ISDIR(mode)
            require(is_directory or stat.S_ISREG(mode), f"non-regular runtime member: {member}")
            if is_directory:
                pending.append(member)
            yield member, is_directory


def runtime_locations(entries):
    return {Path(entry["canonical"]) for entry in entries if entry["canonical"] is not None}


def retained_runtime_path(original_run, path):
    if path.is_relative_to(original_run):
        return path.relative_to(original_run)
    return Path("artifacts/external").joinpath(*path.parts[1:])


def retain_runtime(run, tree, selection, tape):
    requests = runtime_requests(selection, tape)
    resolved = resolve_runtime_requests(requests)
    require_consistent_runtime_aliases(resolved)
    canonical = runtime_locations(resolved["classpath"])
    require({run / "adapter/jvm-services", tree / "build/rtdb-conformance/recorder"} <= canonical,
            "Gradle did not use the owned adapter services and compiled listener")
    test_directories = runtime_locations(resolved["test_class_directories"])
    locations = {name: Path(path) for name, path in tape["classes"].items()}
    require(set(locations) == CLOSURE_CLASSES and set(locations.values()) <= canonical,
            "observed JVM classes are outside the selected test classpath")
    processor = "xtdb.indexer.LogProcessor"
    listener = "rtdb.conformance.JunitRecorder"
    for name in CLOSURE_CLASSES - {processor, listener}:
        require(locations[name] in test_directories and locations[name].is_relative_to(tree / "core/build"),
                f"recorded test class is outside the derived build: {name}")
    require(locations[processor].is_relative_to(tree / "core/build") and locations[processor] not in test_directories
            and locations[listener] == tree / "build/rtdb-conformance/recorder",
            "recorded JVM loaded a different production processor or inventory listener")
    for name, location in locations.items():
        entry = name.replace(".", "/") + ".class"
        if location.is_dir():
            regular(location / entry)
        else:
            with ZipFile(regular(location)) as archive:
                require(entry in archive.namelist(), f"observed class is missing from its artifact: {name}")

    java = runtime_locations(resolved["java"])
    require(Path(tape["java_home_request"]).resolve(strict=True) == Path(tape["java_home"]),
            "JUnit worker JDK home differs from its request")
    require(java == {Path(tape["java_executable"])}, "JUnit worker executable differs from its JDK")
    modules = runtime_locations(resolved["modules"])
    require(modules == {Path(tape["java_modules"])}, "JDK modules differ from the worker observation")
    fixed_files = java | modules | runtime_locations(resolved["java_agents"])
    require(all(path.is_file() for path in fixed_files), "worker executable, modules and agents must be regular files")
    require(all(path.is_dir() for path in test_directories), "Gradle test class roots must be directories")
    runtime = canonical | test_directories | fixed_files
    for location in runtime:
        require_disjoint_runtime_root(run, location)
    roots = []
    retained = {}

    def capture(path):
        if path in retained:
            return
        sha256 = digest(regular(path))
        size = path.stat().st_size
        relative = retained_runtime_path(run, path)
        owned = run / relative
        if owned != path:
            owned.parent.mkdir(parents=True, exist_ok=True)
            with path.open("rb") as source, owned.open("xb") as target:
                shutil.copyfileobj(source, target)
        require(regular(owned).stat().st_size == size and digest(owned) == sha256 and digest(path) == sha256,
                f"runtime artifact changed during capture: {path}")
        retained[path] = {"path": str(path), "retained": owned.relative_to(run).as_posix(),
                          "sha256": sha256, "bytes": size}

    for location in sorted(runtime):
        if location.is_dir():
            destination = run / retained_runtime_path(run, location)
            destination.mkdir(parents=True, exist_ok=True)
            members = []
            for member, is_directory in sorted(runtime_tree(location)):
                if not is_directory:
                    capture(member)
                    members.append(member.relative_to(location).as_posix())
            roots.append({"path": str(location), "kind": "directory", "members": sorted(members)})
        else:
            capture(location)
            roots.append({"path": str(location), "kind": "file", "members": []})
    require(retained, "JVM runtime closure is empty")
    require(resolve_runtime_requests(requests) == resolved, "a selected JVM runtime path changed during capture")
    write_json(run / "runtime-paths.json", resolved)
    return {"schema_version": 2, "classpath": selection["classpath"], "requested_roots": resolved, "roots": roots,
            "java_home": tape["java_home"], "java_executable": tape["java_executable"],
            "java_modules": tape["java_modules"], "jvm_args": tape["jvm_args"],
            "class_sources": tape["classes"], "artifacts": list(retained.values())}


def clean_checkout(repository):
    require(not git(repository, "status", "--porcelain", "--untracked-files=all"),
            f"expected a clean Git checkout: {repository}")
    return git(repository, "rev-parse", "HEAD")


def capture(args):
    revision = clean_checkout(REPOSITORY)
    require_phase_two(REPOSITORY, args.phase_two)
    reference = args.reference.resolve(strict=True)
    reference_revision = clean_checkout(reference)
    require(reference_revision in SUPPORTED_REFERENCES, "expected a pinned upstream reference checkout")
    out = args.output.absolute()
    run = out.parent.resolve(strict=True) / out.name
    require(not any(run.is_relative_to(path) for path in (REPOSITORY, reference)), "run directory must be outside both checkouts")
    run.mkdir()
    (run / "artifacts").mkdir()
    inputs = {name: digest(HERE / name) for name in (
        "run_log_processor_sim.py", "simulation/LogProcessorTrace.kt", "inventory.init.gradle",
        "jvm-java/rtdb/conformance/DiscoveryRecorder.java", "jvm-java/rtdb/conformance/JunitRecorder.java",
        "jvm-services/META-INF/services/org.junit.platform.launcher.TestExecutionListener",
        "source_gate.py", "run_controls.py", "runtime_inventory.py",
    )}
    for name, sha256 in inputs.items():
        target = run / "adapter" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(HERE / name, target)
        require(digest(target) == sha256, f"adapter input changed during capture: {name}")
    tree, overlay = owned_overlay(reference, run, args.seed, reference_revision)
    write_json(run / "inputs.json", {"reference_revision": reference_revision, "implementation_revision": revision,
                                    "upstream_sources": OVERLAY_HASHES, "derived_sources": overlay, "adapter": inputs,
                                    "requested_seed": args.seed})
    logs = tree / "build"
    logs.mkdir()
    process = run_process(gradle_command(tree, run), tree, logs / "test-run.log", run / "gradle.stderr.log", args.timeout)
    write_json(run / "process.json", process)
    verify_overlay(tree, overlay, reference_revision)
    require(clean_checkout(reference) == reference_revision and clean_checkout(REPOSITORY) == revision,
            "input checkout changed during reference recording")
    require(all(digest(HERE / name) == digest(regular(run / "adapter" / name)) == sha256
                for name, sha256 in inputs.items()), "adapter source changed during recording")
    require(process.get("completed") is True and process.get("exit_code") == 0, "the selected Gradle property task failed")

    unique, selection, worker_files, tape_file = selected_invocation(run)
    tape = json.loads(tape_file.data)
    require(tape["schema_version"] == LIVE_INDEX_TAPE_SCHEMA, "current observer omitted the live-index assertion tape")
    observed = check_tape(tape, unique, args.seed)
    xml = regular(tree / "core/build/test-results/property-test" / f"TEST-{TEST_CLASS}.xml")
    runtime = retain_runtime(run, tree, selection, tape)
    write_json(run / "runtime-artifacts.json", runtime)
    closure = [run / name for name in ("inputs.json", "process.json", "runtime-artifacts.json", "runtime-paths.json", "gradle.stderr.log")]
    closure.extend([logs / "test-run.log", xml])
    closure.extend(run / "original" / name for name in OVERLAY_HASHES)
    closure.extend(tree / name for name in overlay)
    closure.extend(run / "adapter" / name for name in inputs)
    write_json(run / "reference-capture.json", {
        "schema_version": 1, "scope": "jvm-log-processor-refused-claim-pilot",
        "reference_revision": reference_revision, "source": FIXTURE, "qualified_name": f"{TEST_CLASS}/{METHOD}",
        "task": TASK, "unique_id": unique, "selected_iterations": 1, "original_assertion_status": "SUCCESSFUL",
        "observation": observed,
        "artifacts": [{"path": path.relative_to(run).as_posix(), "sha256": digest(path), "bytes": path.stat().st_size}
                      for path in closure] + [file.receipt(run) for file in (*worker_files, tape_file)],
    })
    return run / "reference-capture.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--timeout", type=int, default=3600)
    args = parser.parse_args()
    if args.seed is not None and not int_value(args.seed, -(2**31), 2**31 - 1):
        parser.error("seed must be a signed 32-bit integer")
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    try:
        result = capture(args)
    except (OSError, ValueError, KeyError, TypeError, IndexError, AttributeError, BadZipFile, subprocess.CalledProcessError) as error:
        parser.exit(1, f"simulation reference capture failed: {error}\n")
    print(result)


if __name__ == "__main__":
    main()
