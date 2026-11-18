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
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from history_results import integer, verify_runtime_artifacts
from run_controls import HERE, REFERENCE, REPOSITORY, digest, git, run_process, write_json
from run_history import input_receipt, verify_inputs
from source_gate import require_phase_two


SOURCE = "src/test/clojure/xtdb/mixed_iid_encoding_test.clj"
SOURCE_SHA256 = "7cdd0bf680e9311f9d814c4c0b07fd6db2282995528d80510ebc5b7f80fe9d39"
CASE_NAME = "a-scan-over-plain-and-run-end-encoded-iid-pages-answers-as-over-plain-ones-6258"
UNIQUE_ID = f"[engine:dev.clojurephant.jovial.clojure-test]/[namespace:xtdb.mixed-iid-encoding-test]/[name:{CASE_NAME}]"
JUNIT_SUCCESS = {"unique_id": UNIQUE_ID, "found": 1, "started": 1, "succeeded": 1,
                 "failed": 0, "aborted": 0, "skipped": 0, "failures": 0}
ASSERTIONS = {"rewritten-runs": 165, "run-encoded-live": 174, "plain-live": 175, "sorted-iids": 180,
              "history": 180, "iid-join": 180, "iid-intersect": 180, "pgwire-nonempty": 183,
              "pgwire": 184, "patch": 186, "flight-declared": 190, "flight-batch-fields": 191,
              "flight-multiple-batches": 192, "flight-iids": 193}
FIELD_TYPE = {"kind": "fixed-size-binary", "byte_width": 16}
CASE_DIRECTORY = PurePosixPath("target/mixed-iid-encoding")
MIXED_OBJECTS = CASE_DIRECTORY / "mixed/objects/v06/tables"
READINGS_DATA = MIXED_OBJECTS / "public$readings/data"
CHECKPOINT_SIDES = ("control", "mixed")
CHECKPOINT_LOGS = {PurePosixPath(side, "log", log) for side in CHECKPOINT_SIDES for log in ("LOG", "REPLICA_LOG")}
CHECKPOINT_OBJECTS = tuple(PurePosixPath(side, "objects", "v06") for side in CHECKPOINT_SIDES)
CHECKPOINT_MAX_ENTRIES = 4096
CHECKPOINT_MAX_BYTES = 64 * 1024 * 1024
CHECKPOINT_MAX_DEPTH = 16
MAX_RECEIPT_BYTES = 64 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def nonnegative(value):
    return integer(value) and value >= 0


@contextmanager
def open_receipt(path):
    with open(path, "rb", opener=lambda name, flags: os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK)) as stream:
        require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), f"non-regular receipt file: {path}")
        yield stream


def receipt_bytes(path):
    with open_receipt(path) as stream:
        payload = stream.read(MAX_RECEIPT_BYTES + 1)
    require(len(payload) <= MAX_RECEIPT_BYTES, f"receipt file exceeds its size bound: {path}")
    return payload


def read_receipt(path):
    payload = receipt_bytes(path)
    return json.loads(payload), hashlib.sha256(payload).hexdigest()


def write_receipt(path, value):
    payload = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    require(len(payload) <= MAX_RECEIPT_BYTES, f"receipt file exceeds its size bound: {path}")
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    return hashlib.sha256(payload).hexdigest()


def is_iid(cell):
    return (isinstance(cell, dict) and set(cell) == {"kind", "signed_bytes"} and cell["kind"] == "fixed-size-binary"
            and isinstance(cell["signed_bytes"], list) and len(cell["signed_bytes"]) == 16
            and all(type(byte) is int and -128 <= byte <= 127 for byte in cell["signed_bytes"]))


def is_cell(cell):
    if not isinstance(cell, dict):
        return False
    match cell.get("kind"):
        case "i64":
            return set(cell) == {"kind", "value"} and integer(cell["value"])
        case "null":
            return cell == {"kind": "null"}
        case "timestamp":
            return (set(cell) == {"kind", "epoch_second", "nanosecond", "zone"} and integer(cell["epoch_second"])
                    and nonnegative(cell["nanosecond"]) and cell["nanosecond"] < 1_000_000_000
                    and (cell["zone"] is None or isinstance(cell["zone"], str)))
        case _:
            return False


def check_rows(rows, iid_rows=False):
    require(isinstance(rows, list) and rows, "missing result rows")
    for row in rows:
        require(isinstance(row, dict) and row, "missing result columns")
        for name, cell in row.items():
            require(is_iid(cell) if name == "xt/iid" else is_cell(cell), f"invalid typed result cell {name}")
        if iid_rows:
            require(set(row) == {"xt/id", "xt/iid"} and row["xt/id"]["kind"] == "i64", "missing SQL IID projection")


def scalar_count(cell):
    require(is_cell(cell) and cell["kind"] == "i64" and cell["value"] > 0, "invalid positive count")
    return cell["value"]


def check_flight(observations):
    require(observations["flight-declared"] == [FIELD_TYPE, FIELD_TYPE], "Flight field is not 16-byte fixed binary")
    bags = observations["flight-batch-fields"]
    require(len(bags) == 3 and all(isinstance(bag, list) and len(bag) == 1 for bag in bags),
            "Flight assertion requires one field in each observed field set")
    field = bags[0][0]
    require(bags[0] == bags[1] == bags[2] and set(field) == {"name", "type", "nullable", "metadata"}
            and field["name"] == "_iid" and field["type"] == FIELD_TYPE and type(field["nullable"]) is bool
            and isinstance(field["metadata"], dict) and all(isinstance(value, str) for value in field["metadata"].values()),
            "Flight batches differ from the declared IID field")
    lower, batches = map(scalar_count, observations["flight-multiple-batches"])
    require(lower == 1 and batches > lower, "Flight IID stream did not span multiple batches")
    control, mixed = observations["flight-iids"]
    require(isinstance(control, list) and control and control == mixed and all(is_iid(iid) for iid in control),
            "Flight signed IID values are missing or differ")


def check_file(record):
    path = PurePosixPath(record["path"])
    require(str(path) == record["path"] and ".." not in path.parts and path.is_relative_to(MIXED_OBJECTS)
            and len(path.relative_to(MIXED_OBJECTS).parts) == 3 and path.parent.name == "data"
            and path.suffix == ".arrow" and not path.name.startswith("l00-"), "invalid compacted data path")
    require(type(record["bytes"]) is int and record["bytes"] > 0
            and re.fullmatch(r"[0-9a-f]{64}", record["sha256"]), "invalid compacted data digest")
    encoding = record["encoding"]
    require(encoding in {"plain", "run-end-i32"}, "missing physical IID encoding")
    pages = record["pages"]
    require(isinstance(pages, list) and pages, "missing compacted file pages")
    for page in pages:
        require(nonnegative(page["rows"]), "invalid compacted page row count")
        if encoding == "run-end-i32":
            require(set(page) == {"rows", "runs"} and nonnegative(page["runs"])
                    and (0 < page["runs"] <= page["rows"] or page["runs"] == page["rows"] == 0),
                    "invalid compacted page run count")
        else:
            require(set(page) == {"rows"}, "plain compacted page has a run count")
    return path


def check_layout(execution, observations, workspace):
    rewritten = {}
    by_path = {}
    for rewrite in execution["rewritten_files"]:
        before, after, result = rewrite["before"], rewrite["after"], rewrite["result"]
        path = check_file(after)
        require(check_file(before) == path and path not in by_path and before["sha256"] != after["sha256"]
                and before["encoding"] == "plain" and after["encoding"] == "run-end-i32",
                "rewritten file lacks distinct plain and run-end evidence")
        old_rows = [page["rows"] for page in before["pages"]]
        new_rows = [page["rows"] for page in after["pages"]]
        require(old_rows == new_rows and result == {
            "rows": sum(new_rows), "runs": sum(page["runs"] for page in after["pages"]),
        }, "rewrite row/run evidence differs from the original callback result")
        by_path[path] = after
        rewritten[path.stem] = result

    runs, rows = map(scalar_count, observations["rewritten-runs"])
    require(runs < rows and runs == sum(item["runs"] for item in rewritten.values())
            and rows == sum(item["rows"] for item in rewritten.values()),
            "rewrite does not substantiate the original multi-row-run assertion")
    rewrite_keys, live_keys = observations["run-encoded-live"]
    require(rewrite_keys == sorted(rewritten) and live_keys == observations["plain-live"][0]
            and live_keys == sorted(set(live_keys)), "live/rewrite keys differ across original assertions")

    live = {}
    for record in execution["live_files"]:
        file_record = {key: value for key, value in record.items() if key != "trie_key"}
        path = check_file(file_record)
        key = record["trie_key"]
        artifact = workspace / path
        require(path.parent == READINGS_DATA and path.stem == key and key not in live
                and artifact.resolve(strict=True) == artifact and artifact.is_file(), "invalid live readings artifact")
        require(artifact.stat().st_size == record["bytes"] and digest(artifact) == record["sha256"],
                "retained live artifact differs from its observed bytes")
        if record["encoding"] == "run-end-i32":
            require(by_path.get(path) == file_record, "live run-end file is absent from observed rewrites")
        live[key] = record["encoding"]
    require(sorted(live) == live_keys and any(live[key] == "run-end-i32" for key in set(live) & set(rewritten))
            and any(live[key] == "plain" for key in set(live) - set(rewritten)),
            "live readings inventory does not establish both original encodings")
    return by_path


def is_block_catalog(name):
    match = re.fullmatch(r"b([0-9a-f]{2,17})\.binpb", name)
    if match is None:
        return False
    encoded = match[1]
    index = int(encoded[1:], 16)
    body = f"{index:x}"
    return index < 1 << 63 and encoded == f"{len(body) - 1:x}{body}"


def checkpoint_files(checkpoint):
    require(isinstance(checkpoint, dict) and set(checkpoint) == {"schema_version", "stage", "files"}
            and type(checkpoint["schema_version"]) is int and checkpoint["schema_version"] == 1
            and checkpoint["stage"] == "after-rewrite-before-reopen", "invalid mixed IID checkpoint identity")
    files = checkpoint["files"]
    require(isinstance(files, list) and 0 < len(files) <= CHECKPOINT_MAX_ENTRIES, "invalid checkpoint file count")
    inventory = {}
    total = 0
    for entry in files:
        require(isinstance(entry, dict) and set(entry) == {"path", "bytes", "sha256"}
                and isinstance(entry["path"], str) and nonnegative(entry["bytes"])
                and isinstance(entry["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]),
                "invalid checkpoint file record")
        name = entry["path"]
        path = PurePosixPath(name)
        require(not path.is_absolute() and path.as_posix() == name and ".." not in path.parts
                and "\\" not in name and "\0" not in name and len(path.parts) <= CHECKPOINT_MAX_DEPTH
                and path not in inventory and (path in CHECKPOINT_LOGS or any(root in path.parents for root in CHECKPOINT_OBJECTS)),
                f"invalid or repeated checkpoint path: {name}")
        total += entry["bytes"]
        require(total <= CHECKPOINT_MAX_BYTES, "checkpoint byte limit exceeded")
        inventory[path] = (entry["bytes"], entry["sha256"])
    require(all(path in inventory and inventory[path][0] > 0 for path in CHECKPOINT_LOGS)
            and all(any(root in path.parents for path in inventory) for root in CHECKPOINT_OBJECTS),
            "checkpoint omitted a local log or object root")
    for root in CHECKPOINT_OBJECTS:
        require(any(path.parent == root / "blocks" and size > 0 and is_block_catalog(path.name)
                    for path, (size, _) in inventory.items()),
                f"checkpoint omitted a nonempty block catalog: {root}")
    return inventory


def verify_checkpoint_file(directory, name, expected, path):
    size, checksum = expected
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open(name, flags, dir_fd=directory), "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size == size, f"checkpoint file size or kind differs: {path}")
        digest = hashlib.sha256()
        remaining = size
        while remaining:
            chunk = stream.read(min(remaining, 65536))
            require(chunk, f"checkpoint file shrank during verification: {path}")
            digest.update(chunk)
            remaining -= len(chunk)
        require(not stream.read(1) and digest.hexdigest() == checksum, f"checkpoint file bytes differ: {path}")


def verify_checkpoint(root, checkpoint, rewritten):
    inventory = checkpoint_files(checkpoint)
    for path, record in rewritten.items():
        relative = path.relative_to(CASE_DIRECTORY)
        require(inventory.get(relative) == (record["bytes"], record["sha256"]),
                f"checkpoint omitted or changed the mixed rewrite postimage: {relative}")

    directories = {parent for path in inventory for parent in path.parents if parent.parts}
    seen = set()
    entries = 0
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK

    def verify_tree(directory, relative):
        nonlocal entries
        with os.scandir(directory) as children:
            for entry in children:
                entries += 1
                require(entries <= CHECKPOINT_MAX_ENTRIES, "checkpoint entry limit exceeded")
                path = relative / entry.name
                mode = entry.stat(follow_symlinks=False).st_mode
                if stat.S_ISDIR(mode):
                    require(path in directories, f"unexpected checkpoint directory: {path}")
                    child = os.open(entry.name, flags, dir_fd=directory)
                    try:
                        verify_tree(child, path)
                    finally:
                        os.close(child)
                else:
                    require(stat.S_ISREG(mode) and path in inventory, f"unexpected or non-regular checkpoint file: {path}")
                    verify_checkpoint_file(directory, entry.name, inventory[path], path)
                    seen.add(path)

    require(root.is_absolute() and root.resolve(strict=True) == root, "checkpoint root is redirected or relative")
    directory = os.open(root, flags)
    try:
        verify_tree(directory, PurePosixPath())
    finally:
        os.close(directory)
    require(seen == inventory.keys(), "checkpoint omitted retained files")


def verify_execution(execution, workspace, *, require_checkpoint=False):
    version = execution.get("schema_version")
    require(type(version) is int and version in (1, 2) and (version == 2 or not require_checkpoint)
            and execution.get("engine") == "jvm"
            and execution.get("source") == SOURCE and execution.get("runtime_unchanged") is True
            and execution.get("junit") == JUNIT_SUCCESS, "Jovial did not complete the exact original case")
    require(execution["assertion_failures"] == [] and execution["capture_errors"] == [],
            "the original case reported an assertion or capture failure")
    observations = {}
    for item in execution["assertions"]:
        identity = item["id"]
        require(identity in ASSERTIONS and item["line"] == ASSERTIONS[identity]
                and identity not in observations and item["status"] == "passed", "unexpected original assertion")
        observations[identity] = item["operands"]
    require(observations.keys() == ASSERTIONS.keys(), "missing original assertion")
    for identity in ("sorted-iids", "history", "iid-join", "iid-intersect", "pgwire", "patch"):
        left, right = observations[identity]
        check_rows(left, identity in {"sorted-iids", "pgwire"})
        require(left == right, f"{identity} control and mixed results differ")
        if identity in {"iid-join", "iid-intersect"}:
            require(len(left) == 1 and set(left[0]) == {"n"}, f"{identity} aggregate is missing")
            scalar_count(left[0]["n"])
    require(observations["pgwire-nonempty"] == observations["pgwire"][:1], "pgwire nonempty assertion differs")
    check_flight(observations)
    rewritten = check_layout(execution, observations, workspace)
    if version == 2:
        verify_checkpoint(workspace.parent / "checkpoint", execution.get("checkpoint"), rewritten)
    return len(observations)


def require_revision(repository, expected):
    require(git(repository, "rev-parse", "HEAD") == expected
            and not git(repository, "status", "--porcelain", "--untracked-files=all"),
            f"checkout must be clean and pinned at {expected}: {repository}")


def run(args):
    require_phase_two(REPOSITORY, args.phase_two)
    require(math.isfinite(args.timeout) and args.timeout > 0, "mixed IID timeout must be positive and finite")
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    require_revision(REPOSITORY, revision)
    require_revision(args.reference, REFERENCE)
    source = args.reference / SOURCE
    require(digest(source) == SOURCE_SHA256, "original mixed IID source digest differs from C84")
    require(not any(args.output.is_relative_to(root) for root in (REPOSITORY, args.reference)),
            "mixed IID workspace must be outside both source checkouts")
    inputs = input_receipt([Path(__file__).resolve(), HERE / "run_controls.py", HERE / "run_history.py",
                            HERE / "history_results.py", HERE / "source_gate.py", HERE / "reference.init.gradle",
                            HERE / "jvm/rtdb/conformance/history.clj",
                            HERE / "jvm/rtdb/conformance/mixed_iid_reference.clj", source])
    args.output.mkdir(parents=True, exist_ok=False)
    args.created_output = True
    workspace = args.output / "workspace"
    workspace.mkdir()
    receipt_hashes = {"inputs.json": write_receipt(args.output / "inputs.json", {
        "reference_revision": REFERENCE, "implementation_revision": revision, "original_case": UNIQUE_ID, "inputs": inputs,
    })}
    command = [args.reference / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk",
               "--init-script", HERE / "reference.init.gradle", "rtdbConformanceMixedIidReference",
               f"-Prtdb.adapterRoot={HERE / 'jvm'}", f"-Prtdb.output={args.output}", f"-Prtdb.mixedIidWorkspace={workspace}"]
    process = run_process(command, args.reference, args.output / "stdout.log", args.output / "stderr.log", args.timeout)
    receipt_hashes["process.json"] = write_receipt(args.output / "process.json", process)
    require(process["completed"] and process["exit_code"] == 0, "mixed IID reference process did not complete")
    runtime, receipt_hashes["runtime-artifacts.json"] = read_receipt(args.output / "runtime-artifacts.json")
    execution, receipt_hashes["execution.json"] = read_receipt(args.output / "execution.json")
    verify_runtime_artifacts(runtime)
    count = verify_execution(execution, workspace, require_checkpoint=True)
    verify_inputs(inputs)
    require_revision(args.reference, REFERENCE)
    require_revision(REPOSITORY, revision)
    logs = {}
    for name in ("stdout.log", "stderr.log"):
        with open_receipt(args.output / name) as stream:
            logs[name] = hashlib.file_digest(stream, "sha256").hexdigest()
    for name, checksum in receipt_hashes.items():
        require(hashlib.sha256(receipt_bytes(args.output / name)).hexdigest() == checksum,
                f"mixed IID receipt changed during verification: {name}")
    write_json(args.output / "result.json", {
        "reference_observed": True, "complete_parity": False, "assertions": count, "case": UNIQUE_ID,
        "sha256": {**receipt_hashes, **logs},
    })


def main():
    parser = argparse.ArgumentParser(description="Observe the original C84 mixed IID encoding case through Jovial.")
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    args.reference = args.reference.resolve()
    args.output = args.output.resolve()
    args.created_output = False
    try:
        run(args)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError, subprocess.SubprocessError) as error:
        if args.created_output:
            write_json(args.output / "failure.json", {"reference_observed": False, "error": str(error)})
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
