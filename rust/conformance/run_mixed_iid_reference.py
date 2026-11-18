#!/usr/bin/env python3

import argparse
import json
import math
import re
import subprocess
import sys
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
MIXED_OBJECTS = PurePosixPath("target/mixed-iid-encoding/mixed/objects/v06/tables")
READINGS_DATA = MIXED_OBJECTS / "public$readings/data"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def nonnegative(value):
    return integer(value) and value >= 0


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


def verify_execution(execution, workspace):
    require(execution.get("schema_version") == 1 and execution.get("engine") == "jvm"
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
    check_layout(execution, observations, workspace)
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
    write_json(args.output / "inputs.json", {"reference_revision": REFERENCE, "implementation_revision": revision,
                                            "original_case": UNIQUE_ID, "inputs": inputs})
    command = [args.reference / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk",
               "--init-script", HERE / "reference.init.gradle", "rtdbConformanceMixedIidReference",
               f"-Prtdb.adapterRoot={HERE / 'jvm'}", f"-Prtdb.output={args.output}", f"-Prtdb.mixedIidWorkspace={workspace}"]
    process = run_process(command, args.reference, args.output / "stdout.log", args.output / "stderr.log", args.timeout)
    write_json(args.output / "process.json", process)
    require(process["completed"] and process["exit_code"] == 0, "mixed IID reference process did not complete")
    execution = args.output / "execution.json"
    runtime = args.output / "runtime-artifacts.json"
    verify_runtime_artifacts(json.loads(runtime.read_bytes()))
    count = verify_execution(json.loads(execution.read_bytes()), workspace)
    verify_inputs(inputs)
    require_revision(args.reference, REFERENCE)
    require_revision(REPOSITORY, revision)
    write_json(args.output / "result.json", {
        "reference_observed": True, "complete_parity": False, "assertions": count, "case": UNIQUE_ID,
        "sha256": {path.name: digest(path) for path in
                   (execution, runtime, args.output / "inputs.json", args.output / "process.json",
                    args.output / "stdout.log", args.output / "stderr.log")},
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
