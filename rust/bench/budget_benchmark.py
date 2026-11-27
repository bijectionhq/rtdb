#!/usr/bin/env python3
"""Replay the differential budget probes against two pinned native-host servers."""

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from statistics import median
from zipfile import ZipFile

from budget_fixture import (
    ACCOUNT_COLUMNS, BATCH_ROWS, CASES, COHORTS, ENGINE_BYTES, JVM_DIRECT_BYTES, NATIVE_CACHE_BYTES,
    QUERY_BYTES, REFERENCE, REGIONS, REGIONS_SQL, SAMPLES, SMALL_SQL, WARMUPS, dataset, server_configuration,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "conformance"))
from clients.performance import rows  # noqa: E402
from run_controls import ProcessCancelled, REPOSITORY, digest, git, owned_cancellation, terminate, write_json  # noqa: E402
from run_history import input_receipt, verify_inputs  # noqa: E402
from source_gate import require_phase_two  # noqa: E402

ENGINES = ("xtdb", "rtdb")
REPETITIONS = 2
PORTS = (35432, 33000, 38080)
READY_SQL = "SELECT 1"
EXCLUSIONS = {
    "MEM-3": "Historical TPC-H and expression-property corpus failures require the original corpus runs.",
    "VE-9": "TSBS, EDGAR, food-prices and other upstream benchmark suites require their own workloads.",
}


def connect():
    import psycopg
    from psycopg.types.numeric import Int8Dumper
    from psycopg.types.string import StrDumper, TextLoader

    connection = psycopg.connect(host="127.0.0.1", port=PORTS[0], user="xtdb", dbname="xtdb",
                                autocommit=True, connect_timeout=2, sslmode="disable", gssencmode="disable",
                                options="", client_encoding="UTF8")
    connection.adapters.register_dumper(int, Int8Dumper)
    connection.adapters.register_dumper(str, StrDumper)
    connection.adapters.register_loader("interval", TextLoader)
    return connection


@contextmanager
def deadline(connection, process, seconds):
    finished, expired = threading.Event(), threading.Event()

    def cancel():
        expired.set()
        try:
            connection.cancel_safe(timeout=5)
        except Exception:
            pass
        if not finished.wait(5) and process.poll() is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass

    timer = threading.Timer(seconds, cancel)
    timer.start()
    try:
        yield expired
    finally:
        finished.set()
        timer.cancel()
        timer.join()


def diagnostics(error):
    diag = getattr(error, "diag", None)
    fields = ("severity", "severity_nonlocalized", "message_primary", "message_detail", "message_hint",
              "statement_position", "internal_position", "internal_query", "context", "schema_name",
              "table_name", "column_name", "datatype_name", "constraint_name", "source_file",
              "source_line", "source_function")
    return {"type": type(error).__name__, "sqlstate": getattr(error, "sqlstate", None), "message": str(error),
            "fields": {key: value for key in fields if (value := getattr(diag, key, None)) is not None}}


def observe(connection, process, sql, timeout, *, parameters=None, prepare=False):
    outcome = {"sql": sql}
    with connection.cursor() as cursor, deadline(connection, process, timeout) as expired:
        start = time.perf_counter_ns()
        try:
            cursor.execute(sql, parameters, prepare=prepare)
            actual = cursor.fetchall()
        except Exception as error:
            outcome["error"] = diagnostics(error)
        elapsed = time.perf_counter_ns() - start
        if "error" not in outcome:
            try:
                outcome.update(rows=rows(actual), row_count=len(actual), command_tag=cursor.statusmessage,
                               columns=[{"name": col.name, "oid": col.type_code} for col in cursor.description])
            except Exception as error:
                outcome["error"] = diagnostics(error)
    outcome.update(elapsed_ns=elapsed, timeout=expired.is_set(), connection_broken=connection.broken,
                   server_exit=process.poll())
    return outcome


def matches(case, observation):
    return (not observation.get("timeout") and not observation.get("connection_broken")
            and observation.get("server_exit") is None and "error" not in observation
            and observation.get("rows") == rows(case["rows"])
            and [column["oid"] for column in observation.get("columns", [])] == case["oids"])


def row_digest(batches):
    content = hashlib.sha256()
    count = 0
    for batch in batches:
        for values in batch:
            normalized = [value.astimezone(dt.timezone.utc) if isinstance(value, dt.datetime) and value.tzinfo else value
                          for value in values]
            row, = rows([normalized])
            content.update(json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))
            content.update(b"\n")
            count += 1
    return {"rows": count, "sha256": content.hexdigest()}


def seed(connection, process, scale):
    customers, accounts = dataset(scale)
    table = f"acc{scale}"

    def insert(name, columns, records):
        values = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {name} ({', '.join(columns)}) VALUES ({values})"
        for offset in range(0, len(records), BATCH_ROWS):
            with deadline(connection, process, 120) as timed_out, connection.transaction(), connection.cursor() as cursor:
                cursor.executemany(sql, records[offset:offset + BATCH_ROWS])
            if timed_out.is_set():
                raise TimeoutError(f"setup timed out for {name} at offset {offset}")

    with deadline(connection, process, 600) as timed_out:
        for sql in (SMALL_SQL, REGIONS_SQL):
            connection.execute(sql)
    if timed_out.is_set():
        raise TimeoutError("small/regions setup timed out")
    insert(f"cust{scale}", ("_id", "name", "tier"), customers)
    insert(table, ACCOUNT_COLUMNS, accounts)
    with deadline(connection, process, 600) as timed_out, connection.cursor() as cursor:
        cursor.execute(f"INSERT INTO nt{scale} (_id, obj, grp) "
                       f"SELECT _id, {{k: _id % 1000, t: ts}} AS obj, _id % 1000 AS grp FROM {table}")
    if timed_out.is_set():
        raise TimeoutError("nested timestamp setup timed out")
    checks = {}
    with deadline(connection, process, 300) as timed_out, connection.cursor() as cursor:
        for name, columns, records in (
            (table, ACCOUNT_COLUMNS, accounts),
            (f"cust{scale}", ("_id", "name", "tier"), customers),
            ("small", ("_id", "label"), [(i, f"L{i % 10}") for i in range(100)]),
            ("regions", ("_id", "rzone"), sorted((region, region.split("-")[0]) for region in REGIONS)),
        ):
            cursor.execute(f"SELECT {', '.join(columns)} FROM {name} ORDER BY _id", prepare=False)
            checks[name] = {"actual": row_digest(iter(lambda: cursor.fetchmany(BATCH_ROWS), [])),
                            "expected": row_digest([records])}
        cursor.execute(f"SELECT COUNT(*) FROM nt{scale}", prepare=False)
        checks[f"nt{scale}"] = {"actual": cursor.fetchone()[0], "expected": scale}
    if timed_out.is_set():
        raise TimeoutError("readback verification timed out")
    return checks


def allocator_limits(engine, receipt):
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
        f"http://127.0.0.1:{PORTS[2]}/metrics", timeout=5
    ) as response:
        data = response.read(4 * 1024 * 1024 + 1)
    if len(data) > 4 * 1024 * 1024:
        raise ValueError("metrics response exceeds receipt bound")
    receipt.write_bytes(data)
    return parse_allocator_limits(engine, data.decode())


def parse_allocator_limits(engine, exposition):
    selected = {}
    for line in exposition.splitlines():
        name = line.split("{", 1)[0].split(" ", 1)[0]
        if engine == "xtdb" and name == "xtdb_allocator_root_memory_limit_bytes":
            key = "query"
        elif engine == "rtdb" and name == "xtdb_allocator_memory_limit_bytes":
            label = re.search(r'[{,]allocator="(query|memory-cache)"', line)
            if not label:
                continue
            key = label[1]
        else:
            continue
        if key in selected:
            raise ValueError(f"multiple allocator limit metrics for {key}")
        value = float(line.rsplit(None, 1)[1])
        if not value.is_integer() or value <= 0:
            raise ValueError(f"invalid allocator limit for {key}")
        selected[key] = int(value)
    return selected


def expected_limits(engine):
    return {"query": QUERY_BYTES, **({"memory-cache": NATIVE_CACHE_BYTES} if engine == "rtdb" else {})}


@contextmanager
def observe_spill(root, engine):
    stopped = threading.Event()
    pattern = "external-sort*/temp-sort-*.arrow" if engine == "xtdb" else ".tmp*/.tmp*"
    preexisting = set(root.glob(pattern))
    receipt = {"nonempty_files": {}, "error": None,
               "preexisting_files": sorted(str(path.relative_to(root)) for path in preexisting)}

    def sample():
        try:
            while not stopped.is_set():
                for path in root.glob(pattern):
                    if path in preexisting:
                        continue
                    try:
                        size = path.stat().st_size
                    except FileNotFoundError:
                        continue
                    if size > 0:
                        name = str(path.relative_to(root))
                        receipt["nonempty_files"][name] = max(size, receipt["nonempty_files"].get(name, 0))
                stopped.wait(0.02)
        except OSError as error:
            receipt["error"] = str(error)

    observer = threading.Thread(target=sample)
    observer.start()
    try:
        yield receipt
    finally:
        stopped.set()
        observer.join()


def analysis_column(observation, name):
    columns = [column["name"] for column in observation.get("columns", [])]
    if name not in columns:
        return []
    index = columns.index(name)
    return [row[index] for row in observation.get("rows", [])]


def analyze_peak(observation):
    values = analysis_column(observation, "query_peak_memory_bytes")
    if not values or any(cell["kind"] != "integer" for cell in values):
        return None
    return max(int(cell["value"]) for cell in values)


def start_ready(process):
    import psycopg

    end = time.monotonic() + 180
    while time.monotonic() < end:
        if process.poll() is not None:
            raise RuntimeError(f"server exited before readiness ({process.returncode})")
        try:
            connection = connect()
        except psycopg.OperationalError:
            time.sleep(0.1)
            continue
        try:
            outcome = observe(connection, process, READY_SQL, 5)
            if outcome.get("rows") != rows([[1]]) or outcome["timeout"]:
                raise RuntimeError(f"readiness query failed: {outcome}")
            return connection
        except BaseException:
            connection.close()
            raise
    raise TimeoutError("server did not become ready")


def config_command(engine, args, config, spill):
    environment = {"PATH": os.defpath, "LANG": "C.UTF-8", "TZ": "UTC", "TMPDIR": str(spill)}
    if engine == "rtdb":
        return [str(args.native), "node", "--file", str(config), "--memory-limit", str(ENGINE_BYTES)], environment
    return [str(args.java), f"-Xmx{JVM_DIRECT_BYTES}", f"-XX:MaxDirectMemorySize={JVM_DIRECT_BYTES}",
            f"-Djava.io.tmpdir={spill}", "-Duser.timezone=UTC", "-Dclojure.main.report=stderr",
            "--add-opens=java.base/java.nio=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED",
            "-Dio.netty.tryReflectionSetAccessible=true", "-Dio.netty.noUnsafe=false",
            "--sun-misc-unsafe-memory-access=allow",
            "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector",
            "-cp", str(args.jar), "clojure.main", "-m", "xtdb.main", "-f", str(config)], environment


def execute_cases(connection, process, engine, cases, spill, report):
    for phase, repetitions in (("warmups", WARMUPS), ("samples", SAMPLES)):
        for case in cases:
            observations = report["cases"][case["id"]][phase]
            for _ in range(repetitions):
                observations.append(observe(connection, process, case["sql"], case["timeout"]))
    for case in cases:
        result = report["cases"][case["id"]]
        result["plan"] = observe(connection, process, "EXPLAIN " + case["sql"], case["timeout"])
        if case["id"] == "E3":
            with observe_spill(spill, engine) as observed:
                result["analyze"] = observe(connection, process, "EXPLAIN ANALYZE " + case["sql"], case["timeout"])
            result["spill"] = observed
        else:
            result["analyze"] = observe(connection, process, "EXPLAIN ANALYZE " + case["sql"], case["timeout"])
        result["query_peak_bytes"] = analyze_peak(result["analyze"])
    report["follow_up"] = observe(connection, process, READY_SQL, 5)


def invocation(args, engine, scale, cases, directory, config, *, seed_data=seed, execute=execute_cases,
               plan_sha256=None):
    directory.mkdir()
    report = {"engine": engine, "scale": scale, "cases": {p["id"]: {"warmups": [], "samples": []} for p in cases}}
    if plan_sha256 is not None:
        report["plan_sha256"] = plan_sha256
    process, stage = None, "start"
    with owned_cancellation(deferred=True) as owner:
        data = Path(tempfile.mkdtemp(dir=directory, prefix="data-"))
        try:
            for port in PORTS:
                with socket.socket() as probe:
                    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    probe.bind(("127.0.0.1", port))
            spill = data / "tmp"
            spill.mkdir()
            command, environment = config_command(engine, args, config, spill)
            report.update(argv=command, environment=environment, configuration_sha256=digest(config))
            with (directory / "server.log").open("xb") as log:
                process = subprocess.Popen(command, env=environment, cwd=data, stdin=subprocess.DEVNULL,
                                           stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            owner.arm()
            with start_ready(process) as connection:
                stage = "allocator_before"
                report[stage] = allocator_limits(engine, directory / "allocator-before.prom")
                if report[stage] != expected_limits(engine):
                    raise ValueError(f"effective allocator limit does not match: {report[stage]}")
                stage = "setup"
                with deadline(connection, process, 5) as expired:
                    connection.execute("SET TIME ZONE 'UTC'", prepare=False)
                if expired.is_set():
                    raise TimeoutError("time zone setup timed out")
                report["version"] = observe(connection, process, "SELECT version()", 5)
                report[stage] = seed_data(connection, process, scale)
                if any(check["actual"] != check["expected"] for check in report[stage].values()):
                    raise ValueError("seeded rows differ from the reference fixture")
                stage = "queries"
                execute(connection, process, engine, cases, spill, report)
                stage = "allocator_after"
                report[stage] = allocator_limits(engine, directory / "allocator-after.prom")
                status = Path(f"/proc/{process.pid}/status").read_text()
                (directory / "server-proc-status.txt").write_text(status)
        except (ProcessCancelled, KeyboardInterrupt):
            report["failure"] = {"stage": stage, "type": "interrupted"}
            raise
        except Exception as error:
            report["failure"] = {"stage": stage, **diagnostics(error)}
        finally:
            owner.begin_cleanup()
            try:
                if process is not None:
                    report["exit_before_stop"] = process.poll()
                    terminate(process, grace=60)
                    report["exit_after_stop"] = process.returncode
                    log_path = directory / "server.log"
                    with log_path.open("rb") as log:
                        log.seek(max(0, log_path.stat().st_size - 65536))
                        tail = log.read()
                    report["clean_shutdown"] = report["exit_before_stop"] is None and (
                        process.returncode == 0 if engine == "rtdb"
                        else process.returncode in (-15, 0, 143) and b"Node stopped." in tail and b"forcing exit" not in tail
                    )
                shutil.rmtree(data)
            except Exception as error:
                report["cleanup_error"] = diagnostics(error)
                raise
            finally:
                write_json(directory / "execution.json", report)
    return report


def result_summary(case, report):
    result = report["cases"][case["id"]]
    failures = []
    for phase, required in (("warmups", WARMUPS), ("samples", SAMPLES)):
        attempts = result[phase]
        if len(attempts) != required or not all(matches(case, attempt) for attempt in attempts):
            failures.append(phase)
    for phase in ("plan", "analyze"):
        observation = result.get(phase, {})
        if (observation.get("timeout") or observation.get("connection_broken")
                or observation.get("server_exit") is not None or not observation.get("rows") or observation.get("error")):
            failures.append(phase)
    peak = result.get("query_peak_bytes")
    if type(peak) is not int or not 0 <= peak <= QUERY_BYTES:
        failures.append("query_peak")
    spill = result.get("spill", {})
    if case["id"] == "E3":
        operators = {cell["value"].lstrip(":") for cell in analysis_column(result.get("analyze", {}), "op")
                     if cell["kind"] == "text"}
        if not operators.intersection({"order-by", "sort", "spilled-sort"}):
            failures.append("external_sort_operator_unobserved")
        if spill.get("error") or not spill.get("nonempty_files"):
            failures.append("external_sort_spill_unobserved")
    follow_up = report.get("follow_up", {})
    if follow_up.get("timeout") or follow_up.get("error") or follow_up.get("rows") != rows([[1]]):
        failures.append("server_liveness")
    if report.get("failure") or report.get("cleanup_error") or not report.get("clean_shutdown"):
        failures.append("invocation")
    if any(report.get(phase) != expected_limits(report["engine"]) for phase in ("allocator_before", "allocator_after")):
        failures.append("effective_budget")
    if failures:
        return {"qualifies": False, "excluded_from_throughput": failures, "samples_expected": SAMPLES,
                "samples_returned": len(result["samples"])}
    timings = [item["elapsed_ns"] for item in result["samples"]]
    elapsed = sum(timings) / 1e9
    return {"qualifies": True, "samples_expected": SAMPLES, "samples_returned": SAMPLES,
            "median_ms": median(timings) / 1e6, "queries_per_second": SAMPLES / elapsed,
            "logical_input_rows_per_second": SAMPLES * case["input_rows"] / elapsed,
            "returned_rows_per_second": SAMPLES * len(case["rows"]) / elapsed}


def compare_repetitions(comparison):
    cases = {}
    for identifier, runs in comparison.items():
        qualified = all(len(runs[engine]) == REPETITIONS and all(r["qualifies"] for r in runs[engine])
                        for engine in ENGINES)
        result = {"qualifies": qualified, "runs": runs}
        if qualified:
            latency = {engine: median(r["median_ms"] for r in runs[engine]) for engine in ENGINES}
            result.update(median_ms=latency, rtdb_to_xtdb_latency_ratio=latency["rtdb"] / latency["xtdb"])
        cases[identifier] = result
    return cases


def clean_checkout(path, expected):
    if git(path, "rev-parse", "HEAD") != expected or git(path, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError(f"expected clean checkout at {expected}: {path}")


@contextmanager
def matched_inputs(args, plan, sources):
    require_phase_two(REPOSITORY, args.phase_two)
    clean_checkout(REPOSITORY, args.revision)
    clean_checkout(args.reference, REFERENCE)
    if not sys.platform.startswith("linux"):
        raise ValueError("the shared host must be Linux for peak RSS and CPU-affinity evidence")

    import psycopg
    if not psycopg.capabilities.has_cancel_safe():
        raise ValueError("benchmark cancellation requires libpq 17 or newer")

    args.jar = (args.reference / "docker/standalone/build/libs/xtdb-standalone.jar").resolve(strict=True)
    if args.output.is_relative_to(REPOSITORY) or args.output.is_relative_to(args.reference):
        raise ValueError("the evidence directory must be outside both source checkouts")
    if not args.jar.is_relative_to(args.reference):
        raise ValueError("standalone JAR escapes the pinned checkout")
    modules = args.java.parent.parent / "lib/modules"
    artifact_inputs = input_receipt([args.native, args.jar, args.java, modules])
    for record, expected in zip(artifact_inputs[:2], (args.native_sha256, args.jvm_sha256)):
        if record["sha256"] != expected:
            raise ValueError(f"artifact does not match selected SHA256: {record['path']}")
    with ZipFile(args.jar) as jar:
        manifest = jar.read("META-INF/MANIFEST.MF").decode()
    if manifest.splitlines().count(f"Implementation-Version: {REFERENCE}") != 1:
        raise ValueError("XTDB standalone JAR does not carry the required build-time inventory revision")
    args.output.mkdir(parents=True, mode=0o700)
    args.output_created = True
    with owned_cancellation():
        retained = [Path(__file__), Path(__file__).with_name("budget_fixture.py"), *sources]
        configurations = {}
        for engine in ENGINES:
            path = args.output / f"{engine}.yaml"
            path.write_text(server_configuration(engine), encoding="utf-8")
            configurations[engine] = path
            retained.append(path)
        inputs = artifact_inputs + input_receipt(retained)
        native_build = subprocess.run([args.native, "--build-info"], env={"PATH": os.defpath, "TZ": "UTC"},
                                      check=True, capture_output=True, timeout=15)
        build_info = json.loads(native_build.stdout)
        if build_info.get("engine") != "rust" or build_info.get("revision") != args.revision:
            raise ValueError("RTDB binary was compiled for a different revision")
        plan_path = args.output / "plan.json"
        write_json(plan_path, {
            "implementation_revision": args.revision, "reference_revision": REFERENCE, "inputs": inputs,
            "native_build_info": build_info, "jvm_implementation_version": REFERENCE,
            "host": platform.platform(), "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "python": sys.version, "psycopg": psycopg.__version__, "libpq": psycopg.pq.version(),
            "engine_budget_bytes": ENGINE_BYTES, "jvm_heap_bytes": JVM_DIRECT_BYTES,
            "jvm_direct_bytes": JVM_DIRECT_BYTES, "engine_allocation_ceilings": {e: expected_limits(e) for e in ENGINES},
            "repetitions_per_engine": REPETITIONS, **plan,
        })
        plan_sha256 = digest(plan_path)
        yield configurations, plan_sha256
        verify_inputs(inputs)
        if digest(plan_path) != plan_sha256:
            raise ValueError("the retained benchmark plan changed during execution")
        clean_checkout(REPOSITORY, args.revision)
        clean_checkout(args.reference, REFERENCE)


def run(args):
    plan = {"samples_per_invocation": SAMPLES, "warmups_per_invocation": WARMUPS, "cases": CASES,
            "scope_exclusions": EXCLUSIONS}
    with matched_inputs(args, plan, ()) as (configurations, plan_sha256):
        comparison = {case["id"]: {engine: [] for engine in ENGINES} for case in CASES}
        for repetition in range(REPETITIONS):
            for engine in ENGINES if repetition % 2 == 0 else reversed(ENGINES):
                for scale, cases in COHORTS:
                    name = f"{repetition}-{engine}-{cases[0]['id']}"
                    report = invocation(args, engine, scale, cases, args.output / name, configurations[engine])
                    for case in cases:
                        comparison[case["id"]][engine].append({"invocation": name, **result_summary(case, report)})
    cases = compare_repetitions(comparison)
    write_json(args.output / "comparison.json", {"cases": cases, "scope_exclusions": EXCLUSIONS,
                                                 "plan_sha256": plan_sha256})
    return 0 if all(case["qualifies"] for case in cases.values()) else 1


def benchmark_cli(description, run_benchmark):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--revision", required=True, help="Full clean RTDB revision used to compile the binary")
    parser.add_argument("--reference", type=Path, required=True, help="Clean XTDB inventory checkout containing the built standalone JAR")
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--native-sha256", required=True)
    parser.add_argument("--jvm-sha256", required=True)
    parser.add_argument("--java", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="New directory outside both source checkouts")
    args = parser.parse_args()
    for name in ("reference", "native", "java", "output"):
        setattr(args, name, getattr(args, name).resolve())
    args.output_created = False
    try:
        return run_benchmark(args)
    except BaseException as error:
        if args.output_created:
            write_json(args.output / "failure.json", diagnostics(error))
        raise


def main():
    return benchmark_cli(__doc__, run)


if __name__ == "__main__":
    sys.exit(main())
