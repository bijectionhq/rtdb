import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "release"))
from evidence import canonical, digest_value, index, integer, number, text
from performance_plan import operation_trace_digest, validate_plan
from clients.performance import adoption_catalog


PROVIDER_BOUNDARY = "successful_object_http_payload_and_requests"


def distribution(samples):
    values = sorted(integer(value, "latency sample") for value in samples)
    if not values:
        raise ValueError("performance result has no latency samples")
    return {"count": len(values), "p50_ns": values[(len(values) - 1) * 50 // 100],
            "p95_ns": values[(len(values) - 1) * 95 // 100], "p99_ns": values[(len(values) - 1) * 99 // 100],
            "total_ns": sum(values), "minimum_ns": values[0], "maximum_ns": values[-1]}


def logical_result(sample):
    value = sample["value"]
    if "schema" in value:
        return value
    if "second_node" in value:
        return {"second_node": value["second_node"], "committed": True, "system_time": acknowledgement_time(value)}
    if "acknowledgement" in value:
        return {"committed": True, "system_time": acknowledgement_time(value)}
    if "logical_completed" in value:
        return {"logical_completed": value["logical_completed"]}
    return value


def acknowledgement_time(value):
    result = value["acknowledgement"]
    names = [field["name"] for field in result["schema"]]
    return result["rows"][0][names.index("system_time")]


def inspect_checkpoint(case, receipt):
    checkpoint = receipt.get("checkpoint", {})
    if checkpoint.get("execution_complete") is not True or checkpoint.get("client_closed") is not True:
        raise ValueError("actual premeasurement checkpoint is missing or incomplete")
    samples = checkpoint.get("samples", [])
    expected = {operation["id"]: operation for operation in case.get("checkpoint", [])}
    observed = index(samples, lambda value: value["operation"], "actual prefix checkpoint")
    if not expected or set(observed) != set(expected):
        raise ValueError("actual prefix checkpoint differs from declared operations")
    for identity, sample in observed.items():
        operation, value = expected[identity], sample["value"]
        if sample.get("worker") != 0 or sample.get("round") != 0:
            raise ValueError("actual prefix checkpoint has an unexpected worker or iteration")
        if operation["kind"] in {"query", "await_query"} and canonical(value.get("rows")) != canonical(operation["expected_rows"]):
            raise ValueError("actual prefix or layout differs from immutable expected rows")
        if operation["kind"] == "capture_basis":
            completed = value.get("logical_completed")
            if not completed or any(item["system_time"].get("value") != operation["expected_system_time"] for item in completed):
                raise ValueError("actual applied system-time prefix differs")
    return {identity: logical_result(sample) for identity, sample in observed.items()}


def inspect_case(case, receipt, resources):
    errors = []
    inspect_checkpoint(case, receipt)
    if receipt.get("case") != case["id"] or receipt.get("trace_sha256") != operation_trace_digest(case):
        raise ValueError("performance receipt names another operation trace")
    if receipt.get("resources") != resources or receipt.get("execution_complete") is not True or receipt.get("drained") is not True:
        errors.append("execution, resource equality or owner drain")
    rounds = receipt.get("rounds", [])
    expected_rounds = case["samples"] if case["cache_mode"] == "process_cold" else 1
    if len(rounds) != expected_rounds:
        errors.append("missing cold-process or warm round")
    observed = {}
    elapsed = 0
    peaks = []
    storage = []
    for outer, round_ in enumerate(rounds):
        result = round_.get("client", {})
        if result.get("execution_complete") is not True or result.get("client_closed") is not True:
            errors.append("client did not close successfully")
        if case["workload"] == "compaction_under_load":
            inspect_overlap(case, result)
        elapsed += integer(result.get("elapsed_ns"), "client elapsed", 1)
        for sample in result.get("samples", []):
            identity = sample.get("worker"), outer if case["cache_mode"] == "process_cold" else sample.get("round"), sample.get("operation")
            if identity in observed:
                raise ValueError("duplicate measured operation identity")
            observed[identity] = sample
        process = round_.get("processes", [])
        if not process:
            errors.append("missing actual engine process resource counters")
        for value in process:
            peaks.append(integer(value.get("peak_rss_bytes"), "engine peak RSS", 1))
            if value.get("peak_boundary") != "engine_process_lifetime_through_sample":
                errors.append("unknown process peak measurement boundary")
        measurement = round_.get("provider")
        if not isinstance(measurement, dict) or measurement.get("boundary") != PROVIDER_BOUNDARY:
            errors.append("missing comparable successful provider measurement")
        else:
            digest_value(measurement.get("observer_artifact_sha256"), "provider observer artifact")
            text(measurement.get("storage_placement"), "provider storage placement")
            if measurement.get("rejected_requests", 0) or measurement.get("rejected_connections", 0):
                errors.append("provider observer rejected work within the measured interval")
            for name in ("successful_read_bytes", "successful_read_requests", "successful_write_bytes", "successful_write_requests"):
                integer(measurement.get(name), name)
            storage.append(measurement)
    expected = {(worker, iteration, operation["id"]) for worker, operations in enumerate(case["workers"])
                for iteration in range(case["samples"]) for operation in operations}
    if set(observed) != expected:
        errors.append("measured operation denominator differs from trace")
    for identity, sample in observed.items():
        worker, _, operation_id = identity
        if type(worker) is not int or not 0 <= worker < len(case["workers"]):
            raise ValueError("sample names an absent worker")
        operations = {operation["id"]: operation for operation in case["workers"][worker]}
        operation = operations.get(operation_id)
        if operation is None:
            raise ValueError("sample names an absent original operation")
        value = sample.get("value", {})
        if operation["kind"] in {"query", "await_query"} and canonical(value.get("rows")) != canonical(operation["expected_rows"]):
            errors.append("query differs from immutable expected rows")
        if operation["kind"] == "replicated_transaction" and canonical(value.get("second_node", {}).get("rows")) != canonical(operation["expected_rows"]):
            errors.append("second-node read differs from immutable expected rows")
        if operation["kind"] in {"transaction", "replicated_transaction", "bulk"}:
            acknowledgement = value.get("acknowledgement", {})
            names = [column.get("name") for column in acknowledgement.get("schema", [])]
            if "committed" not in names or len(acknowledgement.get("rows", [])) != 1 or acknowledgement["rows"][0][names.index("committed")] != {"kind": "bool", "value": True}:
                errors.append("missing committed transaction read-back")
            if "system_time_sequence" in operation:
                sequence = operation["system_time_sequence"]
                expected_time = datetime.datetime.fromisoformat(sequence["start"].replace("Z", "+00:00")) + datetime.timedelta(microseconds=identity[1] * sequence["step_microseconds"])
                if acknowledgement_time(value).get("value") != expected_time.isoformat():
                    errors.append("acknowledged transaction differs from explicit measured system-time sequence")
    if not observed:
        raise ValueError("empty benchmark case")
    statistics = distribution([sample["elapsed_ns"] for sample in observed.values()])
    statistics.update(throughput_operations_per_second=len(observed) * 1_000_000_000 / elapsed,
                      peak_engine_rss_bytes=max(peaks) if peaks else None,
                      provider=storage, elapsed_ns=elapsed)
    if case["workload"] == "restore":
        recovery = receipt.get("recovery_ns")
        integer(recovery, "restore readiness elapsed", 1)
        statistics["recovery_ns"] = recovery
    return errors, observed, statistics


def inspect_overlap(case, result):
    observation = result.get("overlap")
    if not isinstance(observation, dict):
        raise ValueError("compaction lacks a measured overlap observation")
    operation = case["overlap_observer"]
    observed = integer(observation.get("observed_ns"), "compaction observation time", 1)
    start, end = integer(result.get("started_ns"), "measurement start", 1), integer(result.get("finished_ns"), "measurement end", 1)
    if not start < observed < end or end - start != result["elapsed_ns"]:
        raise ValueError("compaction observation is outside the measured invocation")
    baseline = observation.get("baseline", {})
    baseline_time = integer(baseline.get("captured_ns"), "compaction baseline time", 1)
    initial = adoption_catalog(baseline.get("value", {}), operation)
    actual = adoption_catalog(observation.get("value", {}), operation)
    new_keys = sorted(set(actual) - set(initial))
    if observation.get("operation") != operation["id"] or not new_keys or observation.get("new_trie_keys") != new_keys or not start <= baseline_time < observed:
        raise ValueError("compaction observation lacks newly adopted measured-write trie identities")
    mutations = {(worker, operation["id"]) for worker, operations in enumerate(case["workers"]) for operation in operations
                 if operation["kind"] in {"transaction", "bulk", "replicated_transaction"}}
    samples = [value for value in result["samples"] if (value["worker"], value["operation"]) in mutations]
    for sample in samples:
        beginning = integer(sample.get("started_ns"), "mutation start", 1)
        completion = integer(sample.get("completed_ns"), "mutation completion", 1)
        if not start <= beginning <= completion <= end or completion - beginning != sample["elapsed_ns"]:
            raise ValueError("mutation timestamp differs from measured sample")
        if beginning <= baseline_time:
            raise ValueError("ingestion began before the adoption baseline was captured")
    before = sum(value["completed_ns"] <= observed for value in samples)
    registered = integer(observation.get("commits_before"), "registered commits before observation", 1)
    if before < registered or not any(value["started_ns"] > observed for value in samples):
        raise ValueError("compaction lacks committed ingestion before and after observation")


def compare(plan, runs):
    validate_plan(plan)
    identities = index(runs, lambda run: (run.get("engine"), run.get("repetition")), "performance run")
    expected = {(engine, repetition) for engine, count in (("jvm", plan["jvm_repetitions"]), ("rust", plan["native_repetitions"])) for repetition in range(count)}
    if set(identities) != expected:
        raise ValueError("paired benchmark omitted or added baseline/native repetitions")
    selected = {case["id"] for case in plan["cases"]}
    for run in runs:
        if {case["case"] for case in run.get("cases", [])} != selected or len(run["cases"]) != len(selected):
            raise ValueError("benchmark repetition omitted or duplicated selected cases")
        if run.get("fixture_sha256") != plan["fixture_sha256"] or run.get("packaged_server") is not True:
            raise ValueError("benchmark did not execute the selected fixture through a packaged server")
    results = []
    for case in plan["cases"]:
        errors, measured = [], {"jvm": [], "rust": []}
        baseline = None
        prefix = None
        provider_identity = None
        for engine, repetition in sorted(expected):
            receipt = next(value for value in identities[engine, repetition]["cases"] if value["case"] == case["id"])
            failures, samples, statistics = inspect_case(case, receipt, plan["resources"])
            actual_prefix = inspect_checkpoint(case, receipt)
            if prefix is None:
                prefix = actual_prefix
            elif canonical(prefix) != canonical(actual_prefix):
                errors.append(f"{engine}/{repetition}: applied logical prefix or physical catalog layout differs")
            errors.extend(f"{engine}/{repetition}: {failure}" for failure in failures)
            logical = {str(key): logical_result(value) for key, value in samples.items()}
            if baseline is None:
                baseline = logical
            elif canonical(logical) != canonical(baseline):
                errors.append(f"{engine}/{repetition}: semantic or schema divergence")
            for provider in statistics["provider"]:
                identity = {key: provider.get(key) for key in ("boundary", "observer_artifact_sha256", "storage_placement")}
                if provider_identity is None:
                    provider_identity = identity
                elif identity != provider_identity:
                    errors.append("provider measurement identities differ")
            measured[engine].append(statistics)
        p95 = {engine: [sample["p95_ns"] for sample in values] for engine, values in measured.items()}
        throughput = {engine: [sample["throughput_operations_per_second"] for sample in values] for engine, values in measured.items()}
        investigation = []
        if min(p95["rust"]) > max(p95["jvm"]) * 1.10:
            investigation.append("repeatable p95 regression above 10 percent")
        if max(throughput["rust"]) < min(throughput["jvm"]) * 0.90:
            investigation.append("repeatable throughput loss above 10 percent")
        budgets = case.get("acceptance_budgets")
        if not isinstance(budgets, dict) or set(budgets) != {"p95_ns", "minimum_throughput", "peak_rss_bytes"}:
            errors.append("explicit pilot acceptance budgets are absent")
        else:
            for values in measured["rust"]:
                if values["p95_ns"] > number(budgets["p95_ns"], "p95 budget", 1) or values["throughput_operations_per_second"] < number(budgets["minimum_throughput"], "throughput budget", 0.001):
                    errors.append("native latency or throughput budget")
                if values["peak_engine_rss_bytes"] is None or values["peak_engine_rss_bytes"] > integer(budgets["peak_rss_bytes"], "RSS budget", 1):
                    errors.append("native peak RSS budget")
        if investigation:
            errors.append("performance investigation remains open")
        results.append({"case": case["id"], "measurements": measured, "jvm_p95_range_ns": [min(p95["jvm"]), max(p95["jvm"])],
                        "investigation": investigation, "missing_or_failed": errors, "qualified": not errors})
    return {"schema_version": 1, "packaged_performance_qualified": all(case["qualified"] for case in results),
            "complete_parity": False, "cases": results, "percentile_rule": "pinned xtdb.bench.stats floor(p*(n-1))"}
