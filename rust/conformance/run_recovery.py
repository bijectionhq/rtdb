#!/usr/bin/env python3

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from history_results import inspect_execution, verify_artifacts, verify_runtime_artifacts
from recovery_results import clone_closed, compare_recovery, manifest, verify_files, verify_partial
from run_controls import (HERE, REFERENCE, REPOSITORY, ProcessCancelled, digest, enable_child_reaping, git, group_alive,
                          owned_cancellation, run_process, signal_group, terminate, validate_native_revision,
                          wait_group, write_json)
from run_history import input_receipt, verify_inputs


CANARY = {"id": "C", "price": 991, "system_time": "2026-10-01T00:00:00Z", "valid_time": "2026-10-01T00:00:00Z"}


def read_json(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("receipt is missing, redirected, or too large")
    return json.loads(path.read_bytes())


def provenance(args, execution, engine, directory, request):
    if execution.get("engine") != engine or execution.get("request_sha256") != digest(request) or execution.get("fixture_sha256") != digest(args.fixture):
        raise ValueError("recovery execution identity differs from selected request")
    if engine == "rust":
        validate_native_revision(execution, args.revision)
        if execution.get("binary_sha256") != digest(args.native):
            raise ValueError("recovery execution does not identify the invoked binary")
    else:
        artifact_path = directory / "receipt/runtime-artifacts.json"
        if digest(artifact_path) != execution.get("binary_sha256") or read_json(artifact_path) != execution.get("artifacts"):
            raise ValueError("JVM recovery runtime closure differs from its receipt")
        verify_runtime_artifacts(execution["artifacts"])


def kill_at_boundary(command, cwd, directory, request, binary, timeout):
    if os.name != "posix":
        raise ValueError("recovery fault ownership requires POSIX process groups")
    enable_child_reaping()
    started = time.monotonic()
    status = {"command": [str(value) for value in command], "cwd": str(cwd), "completed": False, "closure": "not_started"}
    marker = directory / "receipt/boundary.json"
    ready = directory / "receipt/boundary-ready.json"
    with owned_cancellation(deferred=True) as cancellation, (directory / "stdout.log").open("xb") as stdout, (directory / "stderr.log").open("xb") as stderr:
        process = subprocess.Popen(status["command"], cwd=cwd, stdout=stdout, stderr=stderr, start_new_session=True, env={**os.environ, "TZ": "UTC"})
        try:
            cancellation.arm()
            while not ready.exists():
                if process.poll() is not None:
                    raise RuntimeError("crash adapter exited before publishing its fault boundary")
                if time.monotonic() - started >= timeout:
                    raise RuntimeError("crash adapter did not reach its selected boundary")
                time.sleep(0.01)
            boundary = read_json(marker)
            if read_json(ready).get("boundary_sha256") != digest(marker):
                raise ValueError("fault readiness does not identify its durable boundary receipt")
            if boundary.get("request_sha256") != digest(request) or boundary.get("binary_sha256") != digest(binary):
                raise ValueError("fault marker does not identify the running request and binary")
            selected = read_json(request)["action"]
            expected = {"canary": "after_canary_acknowledged", "partial_compaction": "after_compaction_data_upload"}[selected]
            if boundary.get("boundary") != expected or process.poll() is not None:
                raise ValueError("fault boundary is wrong or its writer already exited")
            status["boundary_sha256"] = digest(marker)
            cancellation.begin_cleanup()
            signal_group(process, signal.SIGKILL)
            if not wait_group(process, 10):
                raise RuntimeError("crashed writer process group did not join")
            status.update(completed=True, closure="forced_kill", exit_code=process.returncode)
            if process.returncode != -signal.SIGKILL:
                raise ValueError("fault writer did not terminate through the selected SIGKILL")
            return boundary
        except BaseException as error:
            status["error"] = str(error)
            raise
        finally:
            cancellation.begin_cleanup()
            if group_alive(process):
                terminate(process)
            process.wait()
            status["elapsed_seconds"] = time.monotonic() - started
            status["exit_code"] = process.returncode
            write_json(directory / "process.json", status)


def invoke(args, engine, directory, database, protected, action="recover", retry=False):
    directory.mkdir()
    manifest_path = directory / "manifest.json"
    write_json(manifest_path, protected)
    request_path = directory / "request.json"
    request = {"schema_version": 1, "fixture": str(args.fixture), "fixture_sha256": digest(args.fixture),
               "directory": str(database), "manifest": str(manifest_path), "manifest_sha256": digest(manifest_path),
               "prefix": protected["prefix"], "action": action, "canary": CANARY, "retry_compaction": retry}
    write_json(request_path, request)
    if engine == "rust":
        command, cwd = [args.native, "recovery", request_path, directory / "receipt"], REPOSITORY
    else:
        command = [args.reference / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk", "--init-script", HERE / "recovery.init.gradle",
                   "rtdbConformanceRecovery", f"-Prtdb.adapterRoot={HERE / 'jvm'}", f"-Prtdb.request={request_path}", f"-Prtdb.output={directory / 'receipt'}"]
        cwd = args.reference
    if action in ("canary", "partial_compaction"):
        boundary = kill_at_boundary(command, cwd, directory, request_path, args.native, args.timeout)
        provenance(args, boundary, engine, directory, request_path)
        acknowledged = read_json(directory / "receipt/acknowledged.json")
        provenance(args, acknowledged, engine, directory, request_path)
        if acknowledged.get("canary") != CANARY or acknowledged.get("transaction", {}).get("committed") is not True:
            raise ValueError("crash boundary lacks the identifiable committed canary acknowledgement")
        return boundary, acknowledged
    process = run_process(command, cwd, directory / "stdout.log", directory / "stderr.log", args.timeout)
    write_json(directory / "process.json", process)
    execution = read_json(directory / "receipt/execution.json")
    provenance(args, execution, engine, directory, request_path)
    if not process["completed"] or process["exit_code"] != 0 or execution.get("execution_complete") is not True or execution.get("closure") != "graceful":
        raise ValueError("recovery process did not complete and close; retained failure receipt is authoritative")
    if engine == "rust" and read_json(directory / "stdout.log") != execution:
        raise ValueError("native stdout differs from retained recovery receipt")
    write_json(directory / "trace.json", {"request_sha256": digest(request_path), "manifest_sha256": digest(manifest_path),
        "process_sha256": digest(directory / "process.json"), "stdout_sha256": digest(directory / "stdout.log"),
        "stderr_sha256": digest(directory / "stderr.log"), "execution_sha256": digest(directory / "receipt/execution.json")})
    return execution


def capture(args, engine, source, baseline, directory, origin, canary=None):
    directory.mkdir()
    protected = manifest(source, digest(args.fixture), baseline["prefix"], baseline["checkpoint"], origin)
    database = clone_closed(source, directory / "database", protected["files"])
    execution = invoke(args, engine, directory / "capture", database, protected, action="capture")
    verify_files(database, protected["files"])
    protected["checkpoint"] = execution["result"]["checkpoint"]
    protected["origin_capture_sha256"] = digest(directory / "capture/receipt/execution.json")
    write_json(directory / "protected.json", protected)
    report = compare_recovery(args.fixture_value, baseline, execution, protected, canary)
    write_json(directory / "capture-comparison.json", report)
    if not report["recovery_selection_passed"]:
        raise ValueError("origin engine did not restore the protected current and historical state")
    return database, protected


def check(args, engine, source, protected, baseline, directory, canary=None, partial=None):
    directory.mkdir()
    database = clone_closed(source, directory / "database", protected["files"])
    execution = invoke(args, engine, directory / "read", database, protected, retry=partial is not None)
    report = compare_recovery(args.fixture_value, baseline, execution, protected, canary)
    if partial is not None:
        if "after_retry" not in execution.get("result", {}):
            raise ValueError("fallback omitted the requested compaction retry")
        verify_partial(database, partial, retried=True, original_files=protected["files"])
    else:
        verify_files(database, protected["files"])
    write_json(directory / "comparison.json", report)
    if not report["recovery_selection_passed"]:
        raise ValueError("current or historical recovery observations differ")
    return {"engine": engine, "comparison_sha256": digest(directory / "comparison.json"), "execution_sha256": digest(directory / "read/receipt/execution.json")}


def history_input(args, engine):
    directory = args.history / ("reference" if engine == "jvm" else "native") / "history"
    execution = read_json(directory / "execution.json")
    process, trace = read_json(directory.parent / "process.json"), read_json(directory.parent / "trace.json")
    if process.get("completed") is not True or process.get("exit_code") != 0 or process.get("unexpected_live_descendants"):
        raise ValueError("history checkpoint writer has no joined successful process receipt")
    for field, path in (("process_sha256", directory.parent / "process.json"), ("stdout_sha256", directory.parent / "stdout.log"),
                        ("stderr_sha256", directory.parent / "stderr.log"), ("execution_sha256", directory / "execution.json")):
        if trace.get(field) != digest(path):
            raise ValueError("history process trace differs from retained bytes")
    _, failures = inspect_execution(args.fixture_value, execution, engine, digest(args.fixture))
    if failures or execution.get("engine") != engine:
        raise ValueError("selected history artifacts lack valid original observations")
    verify_artifacts(directory, execution)
    if engine == "rust":
        validate_native_revision(execution, args.revision)
        if execution["binary_sha256"] != digest(args.native):
            raise ValueError("native recovery binary differs from retained history writer")
    else:
        if digest(directory / "runtime-artifacts.json") != execution["binary_sha256"] or read_json(directory / "runtime-artifacts.json") != execution["artifacts"]:
            raise ValueError("history JVM runtime closure is inconsistent")
        verify_runtime_artifacts(execution["artifacts"])
    return directory, execution


def run(args):
    state = read_json(REPOSITORY / "rust/implementation-status.json")
    if not args.phase_two or state.get("phase_2_authorized_by_completeness_gate") is not True:
        raise ValueError("recovery execution requires the Phase 2 completeness gate")
    if args.timeout <= 0:
        raise ValueError("recovery timeout must be positive")
    args.revision = git(REPOSITORY, "rev-parse", "HEAD")
    for directory, revision in ((REPOSITORY, args.revision), (args.reference, REFERENCE)):
        if git(directory, "rev-parse", "HEAD") != revision or git(directory, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("recovery sources must be clean and the JVM checkout pinned")
    args.fixture_value = read_json(args.fixture)
    sources = {engine: history_input(args, engine) for engine in ("jvm", "rust")}
    inputs = input_receipt([Path(__file__).resolve(), HERE / "recovery_results.py", HERE / "history_results.py", HERE / "run_history.py",
        HERE / "run_controls.py", HERE / "recovery.init.gradle", HERE / "jvm/rtdb/conformance/recovery.clj", HERE / "jvm/rtdb/conformance/history.clj", args.fixture, args.native,
        *[path for directory, _ in sources.values() for path in (directory / "execution.json", directory.parent / "process.json", directory.parent / "trace.json")]])
    args.output.mkdir(parents=True, exist_ok=False)
    args.created_output = True
    write_json(args.output / "inputs.json", {"reference_revision": REFERENCE, "implementation_revision": args.revision, "inputs": inputs,
        "qualification_scope": "local sequential single-partition handoff; original artifacts are immutable"})
    cases, captured = [], {}
    for engine, (directory, execution) in sources.items():
        for baseline in execution["layouts"]:
            name = f"{baseline['prefix']}-{baseline['layout']}"
            source = directory / name
            work = args.output / f"{engine}-{name}"
            try:
                database, protected = capture(args, engine, source, baseline, work, {"engine": engine, "execution_sha256": digest(directory / "execution.json")})
                captured[engine, baseline["prefix"], baseline["layout"]] = database, protected, baseline
                target = "rust" if engine == "jvm" else "jvm"
                read = check(args, target, database, protected, baseline, work / "handoff")
                cases.append({"id": f"{engine}-to-{target}/{name}", "passed": True, "read": read})
            except ProcessCancelled:
                raise
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                work.mkdir(exist_ok=True)
                write_json(work / "failure.json", {"error": str(error), "ingress": "paused", "retained_artifacts": True})
                cases.append({"id": f"{engine}/{name}", "passed": False, "error": str(error)})
    for prefix in args.fixture_value["prefixes"]:
        for action in ("canary", "partial_compaction"):
            name = f"cutover-{prefix['id']}-{action}"
            work = args.output / name
            work.mkdir()
            try:
                source, protected, baseline = captured["jvm", prefix["id"], "flushed"]
                database = clone_closed(source, work / "writer-database", protected["files"])
                boundary, acknowledged = invoke(args, "rust", work / "writer", database, protected, action=action)
                if action == "partial_compaction":
                    verify_partial(database, boundary)
                    checkpoint = read_json(work / "writer/receipt/before-compaction.json")["checkpoint"]
                else:
                    checkpoint = acknowledged["checkpoint"]
                if checkpoint["tx_key"] != {key: acknowledged["transaction"][key] for key in ("tx_id", "system_time_micros")}:
                    raise ValueError("post-crash checkpoint does not name the acknowledged canary")
                changed = {**baseline, "checkpoint": checkpoint}
                source, protected = capture(args, "rust", database, changed, work / "closed-crash", {"engine": "rust", "boundary_sha256": digest(work / "writer/receipt/boundary.json"), "acknowledged_sha256": digest(work / "writer/receipt/acknowledged.json")}, CANARY)
                second_node = check(args, "rust", source, protected, baseline, work / "second-node", CANARY)
                fallback = check(args, "jvm", source, protected, baseline, work / "fallback", CANARY, boundary if action == "partial_compaction" else None)
                cases.append({"id": name, "passed": True, "second_node": second_node, "fallback": fallback, "acknowledged_sha256": digest(work / "writer/receipt/acknowledged.json")})
            except ProcessCancelled:
                raise
            except (KeyError, OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                write_json(work / "failure.json", {"error": str(error), "ingress": "paused", "retained_artifacts": True})
                cases.append({"id": name, "passed": False, "error": str(error)})
    verify_inputs(inputs)
    for engine, (directory, execution) in sources.items():
        verify_artifacts(directory, execution)
    for directory, revision in ((REPOSITORY, args.revision), (args.reference, REFERENCE)):
        if git(directory, "rev-parse", "HEAD") != revision or git(directory, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("recovery source checkout changed during execution")
    passed = bool(cases) and all(case["passed"] for case in cases)
    write_json(args.output / "result.json", {"schema_version": 1, "recovery_selection_passed": passed, "complete_parity": False,
        "production_cutover_qualified": False, "mixed_writers_qualified": False, "external_sources_qualified": False,
        "fallback_revision": REFERENCE, "inputs_sha256": digest(args.output / "inputs.json"), "cases": cases})
    if not passed:
        raise ValueError("recovery qualification failed; retain artifacts and keep ingress paused")


def main():
    parser = argparse.ArgumentParser(description="Qualify sequential local cross-engine recovery on copies of retained history artifacts.")
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, default=HERE / "fixtures/historical-prices.json")
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    args.created_output = False
    for field in ("reference", "native", "history", "output", "fixture"):
        setattr(args, field, getattr(args, field).resolve())
    try:
        with owned_cancellation():
            run(args)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        if args.created_output:
            write_json(args.output / "failure.json", {"recovery_selection_passed": False, "error": str(error), "ingress": "paused", "retained_artifacts": True})
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
