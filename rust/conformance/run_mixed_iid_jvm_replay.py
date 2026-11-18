#!/usr/bin/env python3

import argparse
import hashlib
import json
import math
import os
import re
import stat
import subprocess
import sys
from pathlib import Path, PurePosixPath
from zipfile import BadZipFile, ZipFile

from history_results import canonical, integer
from mixed_iid_checkpoint import DIRECTORY_FLAGS, LAST_PREFIX_TIME, copy_file
from recovery_results import local_log_head
from run_controls import HERE, REFERENCE, REPOSITORY, git, run_process, validate_native_revision, write_json
from run_embedded_migration import verify_runtime as verify_classpath
from run_history import input_receipt, verify_inputs
from run_mixed_iid_native import (
    OBSERVATIONS, read_reference, receipt_digest, verify_data_path, verify_file_shape,
)
from run_mixed_iid_reference import (
    ASSERTIONS, CASE_DIRECTORY, CHECKPOINT_MAX_BYTES, CHECKPOINT_MAX_DEPTH, CHECKPOINT_MAX_ENTRIES,
    CHECKPOINT_SIDES, FIELD_TYPE, READINGS_DATA, SOURCE, SOURCE_SHA256, UNIQUE_ID, check_file, check_rows,
    checkpoint_files, is_block_catalog, is_iid, receipt_bytes, require, require_revision, verify_checkpoint,
    verify_checkpoint_file, write_receipt,
)
from run_parser_wire import validate_runtime_artifact_shapes
from source_gate import require_phase_two


TASK = "rtdbConformanceMixedIidReplay"
MODE = "native-to-jvm"
STAGE = "after-rewrite-before-reopen"
NATIVE_ROOT = PurePosixPath("storage/v06")
LOGS = {PurePosixPath("log/LOG"), PurePosixPath("log/REPLICA_LOG")}
HELPERS = {"ingest!": 79, "queries": 101, "patch!": 119, "flight-sql-iids": 124, "pgwire-iids": 143}
CLASSES = {
    "clojure.lang.RT", "xtdb.api.Xtdb", "xtdb.database.Database",
    "org.apache.arrow.flight.sql.FlightSqlClient", "org.apache.arrow.vector.FixedSizeBinaryVector",
}
NATIVE_FIELDS = {
    "schema_version", "engine", "mode", "stage", "closed", "case", "source_sha256",
    "execution_complete", "complete_parity", "binary_sha256", "artifact_revision", "source_dirty",
    "storage_version", "storage_epoch", "partitions", "control", "mixed", "rewritten_files",
}
EXECUTION_FIELDS = {
    "schema_version", "engine", "mode", "case", "source", "source_sha256", "original_helpers",
    "continuation_ticks", "request_sha256", "runtime_sha256", "execution_complete",
    "complete_parity", "recovered", "live_files", "live_keys", "control", "mixed",
}


def sha256(payload):
    return hashlib.sha256(payload).hexdigest()


def strict_json(payload):
    def fields(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate receipt field: {key}")
            result[key] = value
        return result

    def invalid_constant(token):
        raise ValueError(f"non-finite receipt value: {token}")

    return json.loads(payload, object_pairs_hook=fields, parse_constant=invalid_constant)


def checked_prefix(ticks):
    require(isinstance(ticks, list) and len(ticks) == 40, "native checkpoint has a different tick prefix")
    last_id = -1
    for tick, transaction in enumerate(ticks):
        stamp = 1_577_836_800_000_000 + tick * 6 * 3_600_000_000
        tx_id = transaction["tx_id"]
        delete = {"ids": [0, 1, 2], "valid_from_micros": stamp + 3_600_000_000} if tick % 10 == 0 else None
        expected = {"tick": tick, "tx_id": tx_id, "system_time_micros": stamp,
                    "put": {"ids": list(range(30)), "value": tick, "valid_from_micros": stamp}, "delete": delete}
        require(integer(tx_id) and last_id < tx_id and canonical(transaction) == canonical(expected),
                f"native checkpoint transaction differs from C84 at tick {tick}")
        last_id = tx_id
    return {"tx_id": last_id, "system_time_micros": LAST_PREFIX_TIME}


def native_path(value):
    path = PurePosixPath(value)
    require(not path.is_absolute() and path.as_posix() == value and ".." not in path.parts
            and "\\" not in value and "\0" not in value and len(path.parts) <= CHECKPOINT_MAX_DEPTH - 1
            and (path in LOGS or NATIVE_ROOT in path.parents), f"invalid native checkpoint path: {value}")
    return path


def jvm_path(side, native):
    return PurePosixPath(side, "objects", *native.parts[1:]) if NATIVE_ROOT in native.parents else PurePosixPath(side, native)


def verify_native_receipt(execution_bytes, stdout_bytes, revision, binary_sha):
    require(execution_bytes == stdout_bytes, "native checkpoint stdout differs from its exact execution receipt")
    execution = strict_json(execution_bytes)
    require(set(execution) == NATIVE_FIELDS
            and type(execution["schema_version"]) is int and execution["schema_version"] == 1
            and execution["engine"] == "rust" and execution["mode"] == "closed-checkpoint"
            and execution["stage"] == STAGE and execution["case"] == UNIQUE_ID
            and execution["source_sha256"] == SOURCE_SHA256 and execution["binary_sha256"] == binary_sha
            and execution["closed"] is True and execution["execution_complete"] is True
            and execution["complete_parity"] is False
            and canonical([execution["storage_version"], execution["storage_epoch"], execution["partitions"]]) == "[6,0,1]",
            "invalid native mixed IID checkpoint identity")
    validate_native_revision(execution, revision)
    return execution


def validate_archive(execution):
    inventory, basis, copies = {}, {}, []
    for side in CHECKPOINT_SIDES:
        record = execution[side]
        directory = f"mixed-iid-{side}"
        require(set(record) == {"directory", "files", "log_heads", "first_ticks"} and record["directory"] == directory,
                "invalid native checkpoint side")
        tx = checked_prefix(record["first_ticks"])
        heads = record["log_heads"]
        require(set(heads) == {"source", "replica"} and all(integer(value) and 0 <= value < 1 << 48 for value in heads.values())
                and tx["tx_id"] <= heads["source"], "invalid native checkpoint log heads")
        side_files = {}
        for file in record["files"]:
            path = native_path(file["path"])
            require(path not in side_files, f"duplicate native checkpoint file: {side}/{path}")
            side_files[path] = (file["bytes"], file["sha256"])
            copies.append({**file, "path": jvm_path(side, path).as_posix()})
        catalog = [int(path.stem[2:], 16) for path, (size, _) in side_files.items()
                   if path.parent == NATIVE_ROOT / "blocks" and integer(size) and size > 0 and is_block_catalog(path.name)]
        require(catalog, f"native checkpoint has no block catalog: {side}")
        inventory[side] = side_files
        basis[side] = {"source_position": heads["source"], "replica_position": heads["replica"],
                       "tx": tx, "adopted_block": max(catalog)}
    checkpoint = {"schema_version": 1, "stage": STAGE, "files": sorted(copies, key=lambda file: file["path"])}
    checkpoint_files(checkpoint)
    rewrites = {}
    for record in execution["rewritten_files"]:
        path = PurePosixPath(verify_data_path(record))
        before, after = record["before"], record["after"]
        require(path in inventory["mixed"] and path not in rewrites, "native checkpoint lacks a unique rewrite file")
        verify_file_shape(before, "plain")
        verify_file_shape(after, "run-end-i32")
        require(before["sha256"] != after["sha256"] and [page["rows"] for page in before["pages"]] ==
                [page["rows"] for page in after["pages"]]
                and inventory["mixed"][path] == (after["bytes"], after["sha256"]),
                "native checkpoint rewrite differs from its archived file")
        rewrites[path] = {**after, "path": (CASE_DIRECTORY / jvm_path("mixed", path)).as_posix()}
    require(any(any(page["runs"] < page["rows"] for page in record["pages"]) for record in rewrites.values()),
            "native checkpoint lacks a multi-row IID run")
    return inventory, checkpoint, basis, rewrites


def verify_native_files(root, inventory, basis):
    require(root.is_absolute() and root.resolve(strict=True) == root and root.is_dir(),
            "native checkpoint root is redirected or relative")
    entries = 0

    def walk(directory, relative, expected, seen):
        nonlocal entries
        with os.scandir(directory) as children:
            for child in children:
                entries += 1
                path = relative / child.name
                require(entries <= CHECKPOINT_MAX_ENTRIES and len(path.parts) <= CHECKPOINT_MAX_DEPTH,
                        "native checkpoint tree exceeds its retained limits")
                mode = child.stat(follow_symlinks=False).st_mode
                if stat.S_ISDIR(mode):
                    fd = os.open(child.name, DIRECTORY_FLAGS, dir_fd=directory)
                    try:
                        walk(fd, path, expected, seen)
                    finally:
                        os.close(fd)
                else:
                    require(stat.S_ISREG(mode) and path in expected, f"unexpected native checkpoint entry: {path}")
                    verify_checkpoint_file(directory, child.name, expected[path], path)
                    seen.add(path)

    for side, expected in inventory.items():
        directory = root / f"mixed-iid-{side}"
        seen = set()
        for relative in (PurePosixPath("log"), NATIVE_ROOT):
            subtree = directory / relative
            require(subtree.resolve(strict=True) == subtree, f"native checkpoint subtree is redirected: {relative}")
            fd = os.open(subtree, DIRECTORY_FLAGS)
            try:
                walk(fd, relative, expected, seen)
            finally:
                os.close(fd)
        require(seen == expected.keys(), f"native checkpoint omitted retained files: {side}")
        for field, name in (("source_position", "LOG"), ("replica_position", "REPLICA_LOG")):
            require(local_log_head(directory / "log" / name) == basis[side][field],
                    f"native checkpoint {side} {name} differs from its retained log head")


def stage_jvm_copies(native, workspace, inventory, checkpoint, rewrites):
    require(not (native.is_relative_to(workspace) or workspace.is_relative_to(native)),
            "native checkpoint and writable JVM copies must use disjoint roots")
    case_root = workspace / CASE_DIRECTORY
    case_root.mkdir(parents=True)
    for side, files in inventory.items():
        for source, (size, checksum) in files.items():
            target = case_root / jvm_path(side, source)
            copy_file(native, PurePosixPath(f"mixed-iid-{side}") / source, target, size, checksum)
    verify_checkpoint(case_root, checkpoint, {PurePosixPath(file["path"]): file for file in rewrites.values()})


def verify_runtime(runtime):
    require(isinstance(runtime, dict)
            and set(runtime) == {"classpath", "absent_classpath", "jdk", "library", "class_locations", "artifacts",
                                 "java_home", "java_executable", "java_modules"},
            "invalid JVM replay runtime closure")
    for key in ("classpath", "absent_classpath", "jdk"):
        require(isinstance(runtime[key], list) and all(isinstance(path, str) and path for path in runtime[key]),
                "invalid JVM replay path inventory")
    require(all(isinstance(runtime[key], str) and runtime[key] for key in ("java_home", "java_executable", "java_modules")),
            "invalid JVM replay JDK paths")
    locations = runtime["class_locations"]
    require(isinstance(locations, dict) and all(isinstance(path, str) and path for path in locations.values()),
            "invalid JVM replay class locations")
    validate_runtime_artifact_shapes(runtime["artifacts"])
    home = Path(runtime["java_home"])
    require(home.is_absolute() and home.resolve(strict=True) == home and home.is_dir(),
            "JVM replay JDK home is not canonical")
    java, modules = ((home / relative).resolve(strict=True) for relative in ("bin/java", "lib/modules"))
    require(java.is_file() and modules.is_file() and java != modules
            and runtime["java_executable"] == str(java) and runtime["java_modules"] == str(modules)
            and runtime["jdk"] == [str(java), str(modules)], "JVM replay JDK does not identify its launcher and modules")
    absent = set(runtime["absent_classpath"])
    require(len(absent) == len(runtime["absent_classpath"]), "duplicate absent JVM replay classpath entry")
    paths = set(runtime["classpath"]) | {artifact["path"] for artifact in runtime["artifacts"]}
    for name in paths:
        path = Path(name)
        require(path.is_absolute() and path.resolve(strict=name not in absent) == path,
                "JVM replay runtime path is absent or redirected")
    verify_classpath(runtime, "reference", None, classes=CLASSES)
    for name, location in locations.items():
        path = Path(location)
        entry = name.replace(".", "/") + ".class"
        if path.is_dir():
            require((path / entry).is_file(), f"JVM replay class is absent from its retained directory: {name}")
        else:
            try:
                with ZipFile(path) as archive:
                    require(entry in archive.namelist(), f"JVM replay class is absent from its retained JAR: {name}")
            except BadZipFile as error:
                raise ValueError(f"JVM replay class artifact is not a valid JAR: {name}") from error


def verify_recovered(actual, expected):
    fields = {"source_tip", "source_position", "replica_position", "public_tx", "live_tx",
              "adopted_tx", "adopted_block", "open_block"}
    require(set(actual) == fields
            and all(integer(actual[field]) for field in fields - {"public_tx", "live_tx", "adopted_tx"})
            and actual["source_tip"] == actual["source_position"] == expected["source_position"]
            and actual["replica_position"] >= expected["replica_position"]
            and actual["adopted_block"] == expected["adopted_block"]
            and actual["open_block"] == actual["adopted_block"] + 1
            and all(canonical(actual[field]) == canonical(expected["tx"]) for field in ("public_tx", "live_tx", "adopted_tx")),
            "JVM replay did not recover the protected tick-39 basis")


def verify_live_files(execution, workspace, rewrites):
    live = {}
    checked = {file["path"]: file for file in rewrites.values()}
    for record in execution["live_files"]:
        physical = {key: value for key, value in record.items() if key != "trie_key"}
        path = check_file(physical)
        absolute = workspace / path
        require(path.parent == READINGS_DATA and record["trie_key"] == path.stem and path not in live
                and absolute.resolve(strict=True) == absolute and absolute.is_file()
                and absolute.stat().st_size == physical["bytes"] and receipt_digest(absolute) == physical["sha256"],
                "JVM replay observed a different live readings data file")
        if physical["encoding"] == "run-end-i32":
            require(checked.get(path.as_posix()) == physical, "live JVM run-end data differs from the native rewrite")
        else:
            require(path.as_posix() not in checked, "live plain JVM data is an archived native rewrite")
        live[path] = physical["encoding"]
    require(execution["live_keys"] == sorted(path.stem for path in live),
            "JVM replay omitted live files reached by the original trie query")
    return set(live.values())


def checked_flight(flight):
    require(set(flight) == {"declared", "batch_fields", "batch_count", "iids"}
            and integer(flight["batch_count"]) and flight["batch_count"] > 0
            and isinstance(flight["batch_fields"], list) and flight["batch_fields"]
            and isinstance(flight["iids"], list) and flight["iids"] and all(is_iid(iid) for iid in flight["iids"])
            and flight["iids"] == sorted(flight["iids"], key=lambda iid: iid["signed_bytes"]),
            "JVM replay is missing actual Flight batch fields or signed IID values")
    for field in [flight["declared"], *flight["batch_fields"]]:
        require(set(field) == {"name", "type", "nullable", "metadata"} and field["name"] == "_iid"
                and type(field["nullable"]) is bool and isinstance(field["metadata"], dict),
                "invalid retained JVM Flight IID field")
    return flight


def compare(execution, reference, live_encodings):
    left, right = (execution[side] for side in CHECKPOINT_SIDES)
    reports = []

    def record(identity, actual, expected, rule="exact"):
        equal = canonical(actual) == canonical(expected)
        report = {"id": identity, "source_line": ASSERTIONS[identity], "rule": rule, "equal": equal}
        if not equal:
            report.update(actual=actual, expected=expected)
        reports.append(report)

    record("run-encoded-live", "run-end-i32" in live_encodings, True, "live-native-rewrite")
    record("plain-live", "plain" in live_encodings, True, "live-plain-file")
    for side in (left, right):
        require(set(side) == {"observations", "pgwire", "flight"} and set(side["observations"]) == set(OBSERVATIONS),
                "JVM replay is missing original query helpers")
    for identity in OBSERVATIONS:
        actual = [side["observations"][identity] for side in (left, right)]
        for rows in actual:
            check_rows(rows, iid_rows=identity == "sorted-iids")
        record(identity, actual, reference[identity])
    pgwire = [side["pgwire"] for side in (left, right)]
    for rows in pgwire:
        check_rows(rows, iid_rows=True)
    record("pgwire-nonempty", pgwire[:1], reference["pgwire-nonempty"])
    record("pgwire", pgwire, reference["pgwire"])
    flights = [checked_flight(side["flight"]) for side in (left, right)]
    record("flight-declared", [FIELD_TYPE, flights[0]["declared"]["type"]], reference["flight-declared"])
    record("flight-batch-fields", [[flights[0]["declared"]], *[flight["batch_fields"] for flight in flights]],
           reference["flight-batch-fields"])
    record("flight-multiple-batches", flights[1]["batch_count"] > 1, reference["flight-multiple-batches"][1]["value"] > 1,
           "mixed-batch-count-greater-than-one")
    record("flight-iids", [flight["iids"] for flight in flights], reference["flight-iids"])
    return reports


def successful(process, command, cwd):
    require(process.get("completed") is True and integer(process.get("exit_code")) and process["exit_code"] == 0
            and process["command"] == [str(part) for part in command] and process["cwd"] == str(cwd),
            "mixed IID process did not complete with its selected arguments")


def verify_jvm_execution(execution, request_bytes, runtime_bytes, basis):
    require(set(execution) == EXECUTION_FIELDS and type(execution["schema_version"]) is int
            and execution["schema_version"] == 1 and execution["engine"] == "jvm" and execution["mode"] == MODE
            and execution["case"] == UNIQUE_ID and execution["source"] == SOURCE and execution["source_sha256"] == SOURCE_SHA256
            and canonical(execution["original_helpers"]) == canonical(HELPERS)
            and canonical(execution["continuation_ticks"]) == canonical(list(range(40, 70)))
            and execution["request_sha256"] == sha256(request_bytes) and execution["runtime_sha256"] == sha256(runtime_bytes)
            and execution["execution_complete"] is True and execution["complete_parity"] is False
            and set(execution["recovered"]) == set(CHECKPOINT_SIDES), "JVM checkpoint replay returned a different execution identity")
    for side in CHECKPOINT_SIDES:
        verify_recovered(execution["recovered"][side], basis[side])


def run(args):
    require_phase_two(REPOSITORY, args.phase_two)
    for name in ("reference", "reference_run", "native", "output"):
        setattr(args, name, getattr(args, name).resolve())
    require(math.isfinite(args.timeout) and args.timeout > 0, "mixed IID replay needs a finite positive timeout")
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    require_revision(REPOSITORY, revision)
    require_revision(args.reference, REFERENCE)
    source = args.reference / SOURCE
    require(receipt_digest(source) == SOURCE_SHA256, "original mixed IID source differs from C84")
    require(not any(args.output.is_relative_to(path) or path.is_relative_to(args.output)
                    for path in (REPOSITORY, args.reference, args.reference_run, args.native)),
            "new replay output must be disjoint from both source checkouts and its selected inputs")
    reference, original_receipts, _ = read_reference(args.reference_run)
    for path, digest in original_receipts.items():
        if path.suffix == ".json":
            payload = receipt_bytes(path)
            require(sha256(payload) == digest, f"original mixed IID receipt changed: {path}")
            strict_json(payload)

    runner_inputs = input_receipt([Path(__file__).resolve(), HERE / "reference.init.gradle",
                                  HERE / "jvm/rtdb/conformance/mixed_iid_replay.clj",
                                  HERE / "jvm/rtdb/conformance/mixed_iid_reference.clj",
                                  HERE / "jvm/rtdb/conformance/history.clj", HERE / "jvm/rtdb/conformance/embedded_parser.clj",
                                  *[HERE / filename for filename in ("source_gate.py", "run_controls.py", "run_history.py",
                                                                     "history_results.py", "recovery_results.py",
                                                                     "run_mixed_iid_reference.py", "run_mixed_iid_native.py",
                                                                     "mixed_iid_checkpoint.py", "run_parser_wire.py",
                                                                     "run_embedded_migration.py", "run_clients.py", "client_results.py")], source, args.native])
    binary_sha = runner_inputs[-1]["sha256"]
    args.output.mkdir(parents=True)
    args.created_output = True
    native_receipts = args.output / "native"
    native_receipts.mkdir()
    native = native_receipts / "closed"
    workspace = args.output / "workspace"
    workspace.mkdir()
    jvm_receipts = args.output / "jvm"
    jvm_receipts.mkdir()
    inputs_path = args.output / "inputs.json"
    inputs_sha = write_receipt(inputs_path, {"implementation_revision": revision, "reference_revision": REFERENCE, "case": UNIQUE_ID,
                                             "inputs": runner_inputs + [{"path": str(path), "sha256": digest}
                                                                        for path, digest in original_receipts.items()]})

    capture_command = [args.native, "mixed-iid-capture", native]
    native_stdout = native_receipts / "stdout.log"
    native_stderr = native_receipts / "stderr.log"
    process = run_process(capture_command, REPOSITORY, native_stdout, native_stderr, args.timeout)
    native_process_sha = write_receipt(native_receipts / "process.json", process)
    successful(process, capture_command, REPOSITORY)
    native_bytes = receipt_bytes(native / "execution.json")
    execution = verify_native_receipt(native_bytes, receipt_bytes(native_stdout), revision, binary_sha)
    rewrite_path = native / "closed-rewrite.json"
    rewrite_bytes = receipt_bytes(rewrite_path)
    require(canonical(strict_json(rewrite_bytes)) == canonical(execution["rewritten_files"]),
            "native rewrite receipt differs from its closed capture")
    inventory, checkpoint, basis, rewrites = validate_archive(execution)
    verify_native_files(native, inventory, basis)
    stage_jvm_copies(native, workspace, inventory, checkpoint, rewrites)
    reference_result_sha = original_receipts[args.reference_run / "result.json"]
    request_path = args.output / "request.json"
    request_sha = write_receipt(request_path, {
        "schema_version": 1, "mode": MODE, "case": UNIQUE_ID, "source": SOURCE, "source_sha256": SOURCE_SHA256,
        "reference_revision": REFERENCE, "reference_result_sha256": reference_result_sha,
        "native_execution_sha256": sha256(native_bytes), "native_binary_sha256": binary_sha,
        "checkpoint": checkpoint, "basis": basis,
    })
    request_bytes = receipt_bytes(request_path)
    require(sha256(request_bytes) == request_sha, "native-to-JVM request changed before launch")
    command = [args.reference / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk",
               "--init-script", HERE / "reference.init.gradle", TASK, f"-Prtdb.adapterRoot={HERE / 'jvm'}",
               f"-Prtdb.output={jvm_receipts}", f"-Prtdb.mixedIidWorkspace={workspace}", f"-Prtdb.mixedIidRequest={request_path}"]
    jvm_stdout, jvm_stderr = jvm_receipts / "stdout.log", jvm_receipts / "stderr.log"
    process = run_process(command, args.reference, jvm_stdout, jvm_stderr, args.timeout)
    jvm_process_sha = write_receipt(jvm_receipts / "process.json", process)
    successful(process, command, args.reference)
    jvm_pathname = jvm_receipts / "execution.json"
    runtime_path = jvm_receipts / "runtime-artifacts.json"
    jvm_bytes, runtime_bytes = receipt_bytes(jvm_pathname), receipt_bytes(runtime_path)
    replay = strict_json(jvm_bytes)
    verify_jvm_execution(replay, request_bytes, runtime_bytes, basis)
    verify_runtime(strict_json(runtime_bytes))
    comparisons = compare(replay, reference, verify_live_files(replay, workspace, rewrites))

    evidence = {
        "inputs.json": inputs_sha, "request.json": request_sha,
        "native/closed/execution.json": sha256(native_bytes), "native/closed/closed-rewrite.json": sha256(rewrite_bytes),
        "native/stdout.log": sha256(native_bytes), "native/stderr.log": receipt_digest(native_stderr),
        "native/process.json": native_process_sha, "jvm/execution.json": sha256(jvm_bytes),
        "jvm/runtime-artifacts.json": sha256(runtime_bytes), "jvm/stdout.log": receipt_digest(jvm_stdout),
        "jvm/stderr.log": receipt_digest(jvm_stderr), "jvm/process.json": jvm_process_sha,
    }
    report = {"schema_version": 1, "mode": MODE, "case": UNIQUE_ID, "source": SOURCE,
              "source_sha256": SOURCE_SHA256, "reference_result_sha256": reference_result_sha,
              "evidence": evidence, "comparisons": comparisons,
              "matched": all(report["equal"] for report in comparisons), "complete_parity": False}
    report_sha = write_receipt(args.output / "comparison.json", report)
    require(all(receipt_digest(args.output / path) == digest for path, digest in evidence.items())
            and receipt_digest(args.output / "comparison.json") == report_sha,
            "mixed IID replay evidence changed during verification")
    _, current_reference_receipts, _ = read_reference(args.reference_run)
    require(current_reference_receipts == original_receipts, "original mixed IID evidence changed during replay")
    verify_native_files(native, inventory, basis)
    verify_inputs(runner_inputs)
    require_revision(args.reference, REFERENCE)
    require_revision(REPOSITORY, revision)
    write_json(args.output / "result.json", {"case": UNIQUE_ID, "mode": MODE, "matched": report["matched"],
                                            "comparisons": len(comparisons), "comparison_sha256": report_sha,
                                            "complete_parity": False})
    return report["matched"]


def main():
    parser = argparse.ArgumentParser(description="Continue the original mixed IID workload on JVM copies of the closed native checkpoint.")
    parser.add_argument("--phase-two", action="store_true")
    for name in ("reference", "reference-run", "native", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    args.created_output = False
    try:
        return 0 if run(args) else 1
    except (OSError, ValueError, RuntimeError, KeyError, TypeError, subprocess.SubprocessError) as error:
        if args.created_output:
            write_json(args.output / "failure.json", {"mode": MODE, "matched": False, "error": str(error)})
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
