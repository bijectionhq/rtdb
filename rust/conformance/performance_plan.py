import datetime
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "release"))
from evidence import canonical, digest_value, index, integer, number, text
from filesystem_observer import validate_selection


WORKLOADS = {"id_lookup", "bitemporal_range", "mixed_type_join", "bulk_ingest", "replicated_commit", "compaction_under_load", "restore"}
PROTOCOLS = {"pgwire", "flight_sql"}
OPERATIONS = {"query", "transaction", "bulk", "flush", "await_query", "capture_basis", "replicated_transaction"}


def validate_operation(operation):
    text(operation.get("id"), "operation ID")
    kind = operation.get("kind")
    if kind not in OPERATIONS:
        raise ValueError("unknown benchmark operation")
    if kind in {"query", "await_query"}:
        text(operation.get("sql"), "query SQL")
        if not isinstance(operation.get("expected_rows"), list):
            raise ValueError("every measured query needs immutable typed expected rows")
        if kind == "await_query":
            number(operation.get("timeout_seconds"), "query barrier timeout", 0.001)
    if kind in {"transaction", "replicated_transaction"}:
        if not isinstance(operation.get("statements"), list) or not operation["statements"]:
            raise ValueError("transaction has no statements")
        for statement in operation["statements"]:
            text(statement.get("sql"), "transaction SQL")
        if kind == "replicated_transaction":
            text(operation.get("read_node"), "replica read node")
            text(operation.get("verification_sql"), "replicated verification query")
            if not isinstance(operation.get("expected_rows"), list):
                raise ValueError("replicated commit requires independent read-back expectations")
    if kind == "bulk":
        text(operation.get("sql"), "bulk SQL")
        integer(len(operation.get("rows", [])), "bulk row count", 1)
    if kind == "flush":
        text(operation.get("database"), "flush database")
        barrier = operation.get("barrier", {})
        if barrier.get("kind") != "await_query":
            raise ValueError("flush requires an actual adoption query barrier")
        validate_operation(barrier)
    if kind == "capture_basis":
        text(operation.get("expected_system_time"), "expected applied system time")
    if "parameters" in operation and not isinstance(operation["parameters"], list):
        raise ValueError("parameters must be an ordered array")
    if "system_time_sequence" in operation:
        sequence = operation["system_time_sequence"]
        if kind not in {"transaction", "replicated_transaction", "bulk"} or datetime.datetime.fromisoformat(sequence["start"].replace("Z", "+00:00")).utcoffset() is None:
            raise ValueError("explicit transaction time sequence requires a zoned instant")
        integer(sequence["step_microseconds"], "transaction time sequence step", 1)
    canonical(operation)


def validate_plan(plan):
    if plan.get("schema_version") != 1:
        raise ValueError("unsupported performance plan schema")
    text(plan.get("id"), "performance plan ID")
    capabilities = plan.get("package_capabilities")
    if not isinstance(capabilities, list) or not capabilities or capabilities != sorted(set(capabilities)):
        raise ValueError("package capabilities must be an explicit sorted unique selection")
    digest_value(plan.get("fixture_sha256"), "fixture digest")
    resource = plan.get("resources", {})
    integer(resource.get("memory_bytes"), "container memory limit", 1)
    integer(resource.get("cpu_count"), "container CPU count", 1)
    text(resource.get("cpu_set"), "CPU placement")
    text(resource.get("storage_placement"), "storage placement")
    text(resource.get("architecture"), "execution architecture")
    if resource.get("host_page_cache") not in {"uncontrolled", "isolated_cold", "warm"}:
        raise ValueError("host page-cache state must be explicit")
    integer(plan.get("jvm_repetitions"), "JVM baseline repetitions", 2)
    integer(plan.get("native_repetitions"), "native repetitions", 2)
    if set(plan.get("engines", {})) != {"jvm", "rust"}:
        raise ValueError("performance plan requires exactly the pinned JVM and native engines")
    for engine in ("jvm", "rust"):
        selected = plan.get("engines", {}).get(engine, {})
        digest_value(selected.get("package", {}).get("sha256"), "server package digest")
        nodes = index(selected.get("nodes"), lambda node: text(node.get("name"), "node name"), "server node")
        if set(nodes) != {node["name"] for node in plan["engines"]["jvm"]["nodes"]}:
            raise ValueError("paired server node names differ")
        for node in nodes.values():
            if not isinstance(node.get("command"), list) or not node["command"]:
                raise ValueError("server command argv is required")
            if any(not isinstance(arg, str) or not arg for arg in node["command"]):
                raise ValueError("invalid server command argument")
            if "/configuration.yaml" not in node["command"]:
                raise ValueError("server command must consume its retained configuration")
            digest_value(node.get("configuration", {}).get("sha256"), "server configuration digest")
            integer(node.get("pg_port"), "pgwire port", 1)
            integer(node.get("flight_port"), "Flight port", 1)
            integer(node.get("health_port"), "health port", 1)
        text(selected.get("engine_memory_configuration"), "engine-specific memory accounting")
        measurement = selected.get("provider_measurement")
        if measurement is not None and measurement.get("kind") not in {"owned_http_observer", "owned_filesystem_observer"}:
            raise ValueError("performance requires the owned provider observation path")
        if measurement is not None and measurement.get("kind") == "owned_filesystem_observer":
            validate_selection(measurement)
            digest_value(measurement.get("observer_artifact", {}).get("sha256"), "filesystem observer artifact")
            if capabilities != ["local"]:
                raise ValueError("filesystem provider observation requires an explicitly local-only package capability selection")
    first = plan["engines"]["jvm"].get("provider_measurement")
    second = plan["engines"]["rust"].get("provider_measurement")
    if any(selection and selection.get("kind") == "owned_filesystem_observer" for selection in (first, second)) and second != first:
        raise ValueError("paired filesystem observers must select identical binary, limits, and backing placement")
    cases = index(plan.get("cases"), lambda case: text(case.get("id"), "case ID"), "performance case")
    seen = set()
    node_names = {node["name"] for node in plan["engines"]["jvm"]["nodes"]}
    for case in cases.values():
        kind, protocol = case.get("workload"), case.get("protocol")
        if kind not in WORKLOADS or protocol not in PROTOCOLS:
            raise ValueError("unknown workload or client protocol")
        if case.get("setup_protocol", protocol) not in PROTOCOLS:
            raise ValueError("unknown setup client protocol")
        seen.add(kind)
        if case.get("node") not in node_names:
            raise ValueError("benchmark case names an absent server node")
        text(case.get("applied_prefix"), "logical applied prefix")
        text(case.get("layout"), "compaction layout")
        if case.get("cache_mode") not in {"process_cold", "warm"}:
            raise ValueError("cache mode must distinguish process restart from warm execution")
        integer(case.get("samples"), "sample rounds", 1)
        workers = case.get("workers")
        integer(len(workers or []), "concurrency", 1)
        if len(workers) > 64 or case["samples"] > 1_000_000:
            raise ValueError("performance invocation exceeds its bounded worker/sample selection")
        if any(op.get("system_time_sequence") for worker in workers for op in worker) and (protocol != "pgwire" or len(workers) != 1):
            raise ValueError("explicit measured time sequences require one ordered pgwire worker")
        operation_ids = set()
        for operations in [case.get("setup", []), case.get("warmup", []), case.get("checkpoint", []), case.get("checks", []), *workers]:
            for operation in operations:
                validate_operation(operation)
                if operation["id"] in operation_ids:
                    raise ValueError("operation identities repeat within a case")
                operation_ids.add(operation["id"])
                if operation["kind"] == "replicated_transaction" and (operation["read_node"] not in node_names or operation["read_node"] == case["node"]):
                    raise ValueError("replicated commit requires another actual server")
        if not case.get("checks") or not all(worker for worker in workers):
            raise ValueError("benchmark case requires nonempty measured work and postconditions")
        if case["cache_mode"] == "warm" and not case.get("warmup"):
            raise ValueError("warm measurements require explicit unmeasured warmup operations")
        checkpoint = case.get("checkpoint", [])
        if not any(op["kind"] == "capture_basis" for op in checkpoint) or not {"logical_prefix", "physical_layout"}.issubset({op.get("role") for op in checkpoint}):
            raise ValueError("measurement requires actual applied-basis, logical-prefix and catalog-layout checkpoints")
        if case["cache_mode"] == "process_cold" and any(op["kind"] not in {"query"} for worker in workers for op in worker):
            raise ValueError("repeated cold-process samples must preserve their restored logical prefix")
        if any(op["kind"] == "capture_basis" for worker in workers for op in worker):
            raise ValueError("opaque basis capture belongs in setup or checks, not matched latency samples")
        if kind == "replicated_commit" and not any(op["kind"] == "replicated_transaction" for worker in workers for op in worker):
            raise ValueError("replicated commit workload omitted its second-node receipt")
        if kind == "bulk_ingest" and not any(op["kind"] == "bulk" for worker in workers for op in worker):
            raise ValueError("bulk-ingest workload omitted actual batch submission")
        if kind == "compaction_under_load":
            observer = case.get("overlap_observer", {})
            text(observer.get("id"), "adoption observer ID")
            text(observer.get("sql"), "adoption catalog SQL")
            number(observer.get("timeout_seconds"), "adoption deadline", 0.001)
            minimum = datetime.datetime.fromisoformat(observer["minimum_system_time"].replace("Z", "+00:00"))
            if observer.get("kind") != "catalog_adoption" or minimum.utcoffset() is None or observer["id"] in operation_ids or case["cache_mode"] != "warm":
                raise ValueError("compaction workload requires a distinct measured adoption observer")
            mutations = [op for worker in workers for op in worker if op["kind"] in {"transaction", "bulk", "replicated_transaction"}]
            if not mutations or case["samples"] < 2 or any("system_time_sequence" not in op for op in mutations):
                raise ValueError("compaction overlap requires ingestion before and after observation")
            if minimum != min(datetime.datetime.fromisoformat(op["system_time_sequence"]["start"].replace("Z", "+00:00")) for op in mutations):
                raise ValueError("adoption temporal boundary differs from measured writes")
        elif case.get("overlap_observer"):
            raise ValueError("overlap observer belongs to the compaction workload")
        if kind == "restore" and case.get("restart_before_measurement") is not True:
            raise ValueError("restore workload requires a real server restart")
    if seen != WORKLOADS:
        raise ValueError(f"performance plan omits workload classes: {sorted(WORKLOADS - seen)}")
    if {case["cache_mode"] for case in cases.values() if case["workload"] == "id_lookup"} != {"process_cold", "warm"}:
        raise ValueError("ID lookups require separate cold-process and warm measurements")
    if {case["protocol"] for case in cases.values()} != PROTOCOLS:
        raise ValueError("packaged performance requires pgwire and Flight client paths")
    return plan


def operation_trace_digest(case):
    return hashlib.sha256(canonical({field: case.get(field, []) for field in ("setup", "checkpoint", "warmup", "workers", "checks", "overlap_observer")}).encode()).hexdigest()
