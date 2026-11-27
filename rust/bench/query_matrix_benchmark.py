#!/usr/bin/env python3
"""Replay the SQL query matrix against two pinned engines under the matched 512 MiB budget."""

import datetime as dt
import math
import os
import struct
import sys
from itertools import chain
from pathlib import Path

from budget_benchmark import (
    ENGINES, READY_SQL, REPETITIONS, analyze_peak, benchmark_cli, connect, deadline, expected_limits,
    invocation, matched_inputs, observe, observe_spill, row_digest,
)
from budget_fixture import BATCH_ROWS, QUERY_BYTES
from clients.performance import rows
from query_matrix_fixture import (
    ACCOUNT_COLUMNS, BEFORE_SYSTEM_TIME, CUSTOMER_COLUMNS, ERROR_SQL, ERROR_SQLSTATE, FLOAT_SUM_TOLERANCE,
    LISTS_SQL, LISTS_SYSTEM_TIME, READ_BASIS, SCALES, SEED_START, UPDATE_SQL, UPDATE_SYSTEM_TIME, account_records,
    attempt_probe, cases_for_scale, updated_records,
)
from run_controls import digest, write_json


def seed(connection, process, scale):
    customers, accounts = account_records(scale)

    def transaction(system_time, write):
        with deadline(connection, process, 120) as expired, connection.cursor() as cursor:
            cursor.execute(f"BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP '{system_time}')", prepare=False)
            try:
                write(cursor)
                cursor.execute("COMMIT", prepare=False)
            except Exception:
                if not connection.broken:
                    cursor.execute("ROLLBACK", prepare=False)
                raise
        if expired.is_set():
            raise TimeoutError(f"fixture transaction timed out at {system_time}")

    transaction_number = 0
    for table, columns, records in (("customers", CUSTOMER_COLUMNS, customers),
                                    ("accounts", ACCOUNT_COLUMNS, accounts)):
        parameters = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({parameters})"
        for offset in range(0, len(records), BATCH_ROWS):
            instant = SEED_START + dt.timedelta(microseconds=transaction_number)
            batch = records[offset:offset + BATCH_ROWS]
            transaction(instant.isoformat(), lambda cursor: cursor.executemany(sql, batch))
            transaction_number += 1

    transaction(LISTS_SYSTEM_TIME, lambda cursor: cursor.execute(LISTS_SQL, prepare=False))
    transaction(UPDATE_SYSTEM_TIME, lambda cursor: cursor.execute(UPDATE_SQL, (scale // 10,), prepare=False))
    checks = {}
    with deadline(connection, process, 300) as expired, connection.cursor() as cursor:
        for key, table, columns, records in (
            ("history", f"accounts FOR SYSTEM_TIME AS OF TIMESTAMP '{BEFORE_SYSTEM_TIME}'", ACCOUNT_COLUMNS, accounts),
            ("accounts", "accounts", ACCOUNT_COLUMNS, updated_records(accounts)),
            ("customers", "customers", CUSTOMER_COLUMNS, customers),
        ):
            cursor.execute(f"{READ_BASIS}SELECT {', '.join(columns)} FROM {table} ORDER BY _id", prepare=False)
            checks[key] = {"actual": row_digest(iter(lambda: cursor.fetchmany(BATCH_ROWS), [])),
                           "expected": row_digest([records])}
        cursor.execute(READ_BASIS + "SELECT COUNT(*) FROM replay_lists", prepare=False)
        checks["replay_lists"] = {"actual": cursor.fetchone()[0], "expected": scale}
    if expired.is_set():
        raise TimeoutError("matrix readback verification timed out")
    return checks


def live(observation):
    return (bool(observation) and not observation.get("timeout") and not observation.get("connection_broken")
            and observation.get("server_exit") is None)


def matching_rows(case, left, right):
    width = len(case["columns"])
    if len(left) != len(right) or any(len(row) != width for row in chain(left, right)):
        return False
    reductions = case["float_reduction_columns"]
    if not case["ordered"]:
        keys = [index for index in range(width) if index not in reductions]

        def sort_key(row):
            return tuple(tuple(sorted(row[index].items())) for index in keys)

        left, right = sorted(left, key=sort_key), sorted(right, key=sort_key)
    for left_row, right_row in zip(left, right):
        for index, (actual, expected) in enumerate(zip(left_row, right_row)):
            if actual == expected:
                continue
            if index not in reductions or actual["kind"] != "float64" or expected["kind"] != "float64":
                return False
            actual_float, = struct.unpack(">d", bytes.fromhex(actual["bits"]))
            expected_float, = struct.unpack(">d", bytes.fromhex(expected["bits"]))
            if not math.isclose(actual_float, expected_float, **FLOAT_SUM_TOLERANCE):
                return False
    return True


def matches(case, observation, probe):
    return (live(observation) and "error" not in observation and observation.get("columns") == case["columns"]
            and observation.get("parameters") == probe["parameters"] and observation.get("prepare") == case["prepare"]
            and "rows" in observation
            and matching_rows(case, observation["rows"], rows(probe["rows"])))


def read_cpu(pid):
    stat = Path(f"/proc/{pid}/stat").read_text()
    fields = stat[stat.rfind(")") + 2:].split()
    return {"start_ticks": int(fields[19]), "user_ticks": int(fields[11]), "system_ticks": int(fields[12])}


def cpu_ticks(boundary):
    first, last = boundary.get("before"), boundary.get("after")
    if first is None or last is None or first["start_ticks"] != last["start_ticks"]:
        return None
    deltas = [last[key] - first[key] for key in ("user_ticks", "system_ticks")]
    return sum(deltas) if min(deltas) >= 0 else None


def observe_case(connection, process, case, probe, *, prefix=""):
    parameters = probe["parameters"]
    prepare = case["prepare"] and not prefix
    outcome = observe(connection, process, prefix + case["sql"], case["timeout_seconds"],
                      parameters=parameters, prepare=prepare)
    return {**outcome, "parameters": parameters, "prepare": prepare}


def execute_cases(connection, process, engine, cases, spill, report):
    for case in cases:
        if process.poll() is not None:
            break
        result = report["cases"][case["id"]]
        try:
            with connect() as case_connection:
                for index in range(case["warmups"]):
                    probe = attempt_probe(case, index)
                    outcome = observe_case(case_connection, process, case, probe)
                    result["warmups"].append(outcome)
                    if not matches(case, outcome, probe):
                        break
                else:
                    cpu = {"before": read_cpu(process.pid)}
                    result["sample_process_cpu"] = cpu
                    for index in range(case["warmups"], case["warmups"] + case["samples"]):
                        probe = attempt_probe(case, index)
                        outcome = observe_case(case_connection, process, case, probe)
                        result["samples"].append(outcome)
                        if not matches(case, outcome, probe):
                            break
                    cpu["after"] = read_cpu(process.pid)
        except (OSError, ValueError) as error:
            result["measurement_error"] = str(error)
    for case in cases:
        if connection.broken or process.poll() is not None:
            break
        result = report["cases"][case["id"]]
        probe = attempt_probe(case, case["warmups"])
        result["plan"] = observe_case(connection, process, case, probe, prefix="EXPLAIN ")
        with observe_spill(spill, engine) as observed:
            result["analyze"] = observe_case(connection, process, case, probe, prefix="EXPLAIN ANALYZE ")
        result["spill"] = observed
        result["query_peak_bytes"] = analyze_peak(result["analyze"])

    if not connection.broken and process.poll() is None:
        report["sqlstate_probe"] = [
            {"parameters": [arg], **observe(connection, process, ERROR_SQL, 5, parameters=(arg,), prepare=True)}
            for arg in (1, 0, 1)
        ]
        report["follow_up"] = observe(connection, process, READY_SQL, 5)
        status = Path(f"/proc/{process.pid}/status").read_text()
        peak_line = next(line for line in status.splitlines() if line.startswith("VmHWM:"))
        _, value, unit = peak_line.split()
        if unit != "kB":
            raise ValueError("unexpected kernel RSS unit")
        report["process_lifetime_peak_rss_bytes"] = int(value) * 1024


def sqlstate_probe_matches(observations):
    if [item.get("parameters") for item in observations] != [[1], [0], [1]] or not all(
        live(item) for item in observations
    ):
        return False
    first, failure, recovered = observations
    error = failure.get("error", {})
    return (error.get("sqlstate") == ERROR_SQLSTATE
            and "division by zero" in error.get("fields", {}).get("message_primary", "")
            and all(item.get("rows") == rows([[1]]) and item.get("columns") == [{"name": "quotient", "oid": 20}]
                    and "error" not in item for item in (first, recovered)))


def summarize(case, report, ticks_per_second):
    result = report["cases"][case["id"]]
    failures = []
    for phase, offset in (("warmups", 0), ("samples", case["warmups"])):
        if len(result[phase]) != case[phase] or any(
            not matches(case, observation, attempt_probe(case, offset + index))
            for index, observation in enumerate(result[phase])
        ):
            failures.append(phase)
    if report.get("failure") or report.get("cleanup_error") or not report.get("clean_shutdown"):
        failures.append("invocation")
    if any(report.get(phase) != expected_limits(report["engine"]) for phase in ("allocator_before", "allocator_after")):
        failures.append("effective_budget")
    if not sqlstate_probe_matches(report.get("sqlstate_probe", [])):
        failures.append("sqlstate")
    follow_up = report.get("follow_up", {})
    if not live(follow_up) or follow_up.get("rows") != rows([[1]]) or "error" in follow_up:
        failures.append("server_liveness")
    for phase in ("plan", "analyze"):
        outcome = result.get(phase, {})
        if not live(outcome) or not outcome.get("rows") or "error" in outcome:
            failures.append(phase)
    peak = result.get("query_peak_bytes")
    if type(peak) is not int or not 0 <= peak <= QUERY_BYTES:
        failures.append("query_peak")
    rss = report.get("process_lifetime_peak_rss_bytes")
    if type(rss) is not int or rss <= 0:
        failures.append("process_peak_rss")
    if "spill" not in result or result["spill"].get("error"):
        failures.append("spill_observer")
    ticks = cpu_ticks(result.get("sample_process_cpu", {}))
    if ticks is None or result.get("measurement_error"):
        failures.append("process_cpu")
    summary = {"qualifies": not failures, "failures": failures, "samples_expected": case["samples"],
               "samples_returned": len(result["samples"]), "query_peak_bytes": peak,
               "process_lifetime_peak_rss_bytes": rss,
               "spill_observed": bool(result.get("spill", {}).get("nonempty_files")),
               "sqlstates": sorted({state for item in chain(result["warmups"], result["samples"])
                                    if (state := item.get("error", {}).get("sqlstate")) is not None})}
    if not failures:
        summary.update(timing_metrics([sample["elapsed_ns"] for sample in result["samples"]], ticks, ticks_per_second))
    return summary


def timing_metrics(latencies, ticks, ticks_per_second):
    ordered = sorted(latencies)
    count = len(ordered)
    return {"p50_ms": ordered[(50 * count + 99) // 100 - 1] / 1e6,
            "p99_ms": ordered[(99 * count + 99) // 100 - 1] / 1e6,
            "server_cpu_ms_per_op": ticks * 1000 / (ticks_per_second * count)}


def paired_rows(case, reports):
    for phase in ("warmups", "samples"):
        left, right = (reports[engine]["cases"][case["id"]][phase] for engine in ENGINES)
        if len(left) != case[phase] or len(right) != case[phase]:
            return False
        for a, b in zip(left, right):
            if ("rows" not in a or "rows" not in b or a.get("columns") != b.get("columns")
                    or a.get("parameters") != b.get("parameters") or not matching_rows(case, a["rows"], b["rows"])):
                return False
    return True


def compare_case(repetitions):
    qualifies = len(repetitions) == REPETITIONS and all(
        repetition["rows_and_types_match"] and all(repetition[engine]["qualifies"] for engine in ENGINES)
        for repetition in repetitions
    )
    if qualifies:
        repetitions = [
            {**repetition, "rtdb_to_xtdb": {
                metric: repetition["rtdb"][metric] / repetition["xtdb"][metric]
                for metric in ("p50_ms", "p99_ms", "server_cpu_ms_per_op") if repetition["xtdb"][metric] > 0
            }}
            for repetition in repetitions
        ]
    return {"qualifies": qualifies, "repetitions": repetitions}


def run(args):
    cohorts = {scale: cases_for_scale(scale) for scale in SCALES}
    ticks_per_second = os.sysconf("SC_CLK_TCK")
    plan = {
        "benchmark": "post-fix-sql-query-matrix",
        "schema_version": 1,
        "cases_by_scale": cohorts,
        "seed": {"batch_rows": BATCH_ROWS, "first_system_time": SEED_START.isoformat(),
                 "step_microseconds": 1, "list_insert": LISTS_SQL, "list_system_time": LISTS_SYSTEM_TIME,
                 "pre_update_cut": BEFORE_SYSTEM_TIME, "update": UPDATE_SQL, "update_system_time": UPDATE_SYSTEM_TIME,
                 "update_parameter": "scale // 10", "query_basis": READ_BASIS},
        "sqlstate_probe": {"sql": ERROR_SQL, "parameters": [1, 0, 1], "prepare": True, "sqlstate": ERROR_SQLSTATE},
        "float_reduction_tolerance": FLOAT_SUM_TOLERANCE,
        "server_cpu_clock_ticks_per_second": ticks_per_second,
    }
    sources = (Path(__file__), Path(__file__).with_name("query_matrix_fixture.py"))
    comparisons = {scale: {case["id"]: [] for case in cases} for scale, cases in cohorts.items()}
    with matched_inputs(args, plan, sources) as (configurations, plan_sha256):
        for repetition in range(REPETITIONS):
            for scale, cases in cohorts.items():
                reports, receipts = {}, {}
                for engine in ENGINES if repetition % 2 == 0 else reversed(ENGINES):
                    directory = args.output / f"{repetition}-{engine}-{scale}"
                    reports[engine] = invocation(args, engine, scale, cases, directory, configurations[engine],
                                                 seed_data=seed, execute=execute_cases, plan_sha256=plan_sha256)
                    receipt = directory / "execution.json"
                    receipts[engine] = {"path": str(receipt.relative_to(args.output)), "sha256": digest(receipt)}
                for case in cases:
                    comparisons[scale][case["id"]].append({
                        "index": repetition, "rows_and_types_match": paired_rows(case, reports),
                        **{engine: {**summarize(case, reports[engine], ticks_per_second), "execution": receipts[engine]}
                           for engine in ENGINES},
                    })
    results = {scale: {identifier: compare_case(repetitions) for identifier, repetitions in cases.items()}
               for scale, cases in comparisons.items()}
    write_json(args.output / "comparison.json",
               {"schema_version": 1, "plan_sha256": plan_sha256, "cases_by_scale": results})
    return 0 if all(result["qualifies"] for cases in results.values() for result in cases.values()) else 1


if __name__ == "__main__":
    sys.exit(benchmark_cli(__doc__, run))
