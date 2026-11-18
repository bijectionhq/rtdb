#!/usr/bin/env python3

import argparse
import json
import math
import subprocess
import sys
from pathlib import Path, PurePosixPath

from history_results import integer, verify_artifacts, verify_runtime_artifacts
from run_controls import HERE, REFERENCE, REPOSITORY, digest, git, run_process, validate_native_revision, write_json
from run_history import input_receipt, verify_inputs
from run_mixed_iid_reference import ASSERTIONS, SOURCE, SOURCE_SHA256, UNIQUE_ID, check_rows, require, require_revision, verify_execution
from source_gate import require_phase_two


OBSERVATIONS = ("sorted-iids", "history", "iid-join", "iid-intersect", "patch")
REFERENCE_FILES = ("execution.json", "runtime-artifacts.json", "inputs.json", "process.json", "stdout.log", "stderr.log")
SETTINGS = {"timezone": "Europe/London", "rows_per_block": 200, "file_size_target": 16 * 1024,
            "automatic_compaction": False, "automatic_gc": False, "plain_compaction": True}
COMPACTED_ROOT = PurePosixPath("storage/v06/tables")
PATCH_SQL = "PATCH INTO readings FOR VALID_TIME FROM TIMESTAMP '2020-01-03T00:00:00Z' RECORDS {_id: 7, extra: 1}"


def read_json(path):
    require(path.is_file() and not path.is_symlink(), f"missing or redirected receipt file: {path}")
    return json.loads(path.read_bytes())


def read_reference(directory):
    result = read_json(directory / "result.json")
    require(result.get("reference_observed") is True and result.get("complete_parity") is False
            and result.get("case") == UNIQUE_ID and result.get("assertions") == len(ASSERTIONS),
            "missing successful D201 reference result")
    hashes = result["sha256"]
    require(set(hashes) == set(REFERENCE_FILES), "incomplete D201 result provenance")
    for name, expected in hashes.items():
        path = directory / name
        require(path.is_file() and not path.is_symlink() and digest(path) == expected,
                f"D201 reference artifact changed: {name}")
    inputs = read_json(directory / "inputs.json")
    require(inputs["reference_revision"] == REFERENCE and inputs["original_case"] == UNIQUE_ID
            and any(item["path"].endswith("/" + SOURCE) and item["sha256"] == SOURCE_SHA256 for item in inputs["inputs"]),
            "D201 reference provenance differs from C84")
    process = read_json(directory / "process.json")
    require(process["completed"] is True and process["exit_code"] == 0, "D201 reference process did not complete")
    verify_runtime_artifacts(read_json(directory / "runtime-artifacts.json"))
    execution = read_json(directory / "execution.json")
    verify_execution(execution, directory / "workspace")
    return {entry["id"]: entry["operands"] for entry in execution["assertions"]}


def verify_tape(record):
    require(len(record["first_ticks"]) == 40 and len(record["ticks"]) == 30, "invalid native tick partitions")
    previous_tx = -1
    for tick, transaction in enumerate(record["first_ticks"] + record["ticks"]):
        system_time = 1_577_836_800_000_000 + tick * 6 * 3_600_000_000
        delete = {"ids": [0, 1, 2], "valid_from_micros": system_time + 3_600_000_000} if tick % 10 == 0 else None
        tx_id = transaction["tx_id"]
        require(integer(tx_id) and tx_id > previous_tx and transaction == {
            "tick": tick, "tx_id": tx_id, "system_time_micros": system_time,
            "put": {"ids": list(range(30)), "value": tick, "valid_from_micros": system_time}, "delete": delete,
        }, f"native transaction differs from C84 action at tick {tick}")
        previous_tx = tx_id
    patch = record["patch"]
    require(integer(patch["tx_id"]) and patch["tx_id"] > previous_tx
            and patch["system_time_micros"] == 1_609_459_200_000_000 and patch["sql"] == PATCH_SQL,
            "native PATCH differs from C84 action")


def verify_data_path(record):
    path = PurePosixPath(record["path"])
    require(str(path) == record["path"] and ".." not in path.parts and path.is_relative_to(COMPACTED_ROOT)
            and len(path.relative_to(COMPACTED_ROOT).parts) == 3 and path.parent.name == "data"
            and path.suffix == ".arrow" and not path.name.startswith("l00-") and path.stem == record["trie_key"],
            "invalid native compacted file path")
    return str(path)


def verify_file_shape(file, encoding):
    require(encoding in {"plain", "run-end-i32"} and file["encoding"] == encoding and integer(file["bytes"])
            and file["bytes"] > 0 and len(file["sha256"]) == 64
            and all(char in "0123456789abcdef" for char in file["sha256"]), "invalid native physical file evidence")
    for page in file["pages"]:
        require(integer(page["rows"]) and page["rows"] >= 0, "invalid compacted row count")
        if encoding == "run-end-i32":
            require(set(page) == {"rows", "runs"} and integer(page["runs"])
                    and (0 < page["runs"] <= page["rows"] or page["runs"] == page["rows"] == 0),
                    "invalid native run count")
        else:
            require(set(page) == {"rows"}, "plain data page has a run count")


def verify_layout(execution):
    rewrites = {}
    has_runs = False
    for file in execution["rewritten_files"]:
        path = verify_data_path(file)
        require(path not in rewrites, "repeated native rewrite path")
        before, after = file["before"], file["after"]
        verify_file_shape(before, "plain")
        verify_file_shape(after, "run-end-i32")
        require(before["sha256"] != after["sha256"]
                and [page["rows"] for page in before["pages"]] == [page["rows"] for page in after["pages"]],
                "native rewrite did not preserve page row counts")
        has_runs |= any(page["runs"] < page["rows"] for page in after["pages"])
        rewrites[path] = file
    require(has_runs, "native rewrite did not witness a multi-row run")

    for side in ("control", "mixed"):
        layout = execution["live_layout"][side]
        artifacts = {file["path"]: file for file in execution[side]["artifacts"]}
        encodings = set()
        keys = set()
        for entry in layout["files"]:
            path = verify_data_path(entry)
            key = entry["trie_key"]
            require(key not in keys, "repeated live native trie key")
            keys.add(key)
            file = entry["file"]
            rewritten = side == "mixed" and path in rewrites
            encoding = "run-end-i32" if rewritten else "plain"
            verify_file_shape(file, encoding)
            require(entry["rewritten"] is rewritten
                    and all(file[name] == artifacts[path][name] for name in ("bytes", "sha256")),
                    "live native file disagrees with its closed artifact")
            recorded_size = file["bytes"]
            if rewritten:
                require(file == rewrites[path]["after"], "live native file differs from its run-end rewrite")
                recorded_size = rewrites[path]["before"]["bytes"]
            require(entry["catalog_bytes"] == recorded_size, "native catalog changed its recorded data file size")
            encodings.add(encoding)
        wanted = {"plain", "run-end-i32"} if side == "mixed" else {"plain"}
        require(encodings == wanted, f"native {side} layout does not prove its expected live encodings")


def normalize_rows(rows):
    return [
        {name: ({key: value for key, value in cell.items() if key != "zone"} if cell["kind"] == "timestamp" else cell)
         for name, cell in row.items() if cell["kind"] != "null"}
        for row in rows
    ]


def observations(execution, reference):
    reports = []
    normalized = {}
    for index, side in enumerate(("control", "mixed")):
        results = execution[side]["observations"]
        require(set(results) == set(OBSERVATIONS), "missing native internal observations")
        for identity in OBSERVATIONS:
            actual, expected = results[identity], reference[identity][index]
            check_rows(actual, iid_rows=identity == "sorted-iids")
            check_rows(expected, iid_rows=identity == "sorted-iids")
            actual = normalize_rows(actual)
            expected = normalize_rows(expected)
            normalized[side, identity] = actual
            result = {"layout": side, "id": identity, "source_line": ASSERTIONS[identity],
                      "native_rows": len(actual), "reference_rows": len(expected), "equal": actual == expected}
            if not result["equal"]:
                result["first_different_row"] = next((index for index, (left, right) in enumerate(zip(actual, expected))
                                                      if left != right), min(len(actual), len(expected)))
            reports.append(result)
    for identity in OBSERVATIONS:
        control, mixed = normalized["control", identity], normalized["mixed", identity]
        if control != mixed:
            reports.append({"layout": "control-vs-mixed", "id": identity, "source_line": ASSERTIONS[identity],
                            "equal": False})
    return reports


def verify_native(execution, output, revision, binary_sha256):
    validate_native_revision(execution, revision)
    require(execution["schema_version"] == 1 and execution["engine"] == "rust"
            and execution["source_sha256"] == SOURCE_SHA256 and execution["binary_sha256"] == binary_sha256
            and execution["execution_complete"] is True and execution["complete_parity"] is False
            and execution["wire_pending"] == ["pgwire", "flight-sql"] and execution["settings"] == SETTINGS,
            "invalid native mixed IID execution identity or physical settings")
    for side in ("control", "mixed"):
        verify_tape(execution[side])
    verify_artifacts(output, {"layouts": [
        {"prefix": "mixed-iid", "layout": side, "artifacts": execution[side]["artifacts"]}
        for side in ("control", "mixed")
    ]})
    verify_layout(execution)


def run(args):
    require_phase_two(REPOSITORY, args.phase_two)
    for name in ("reference", "reference_run", "native", "output"):
        setattr(args, name, getattr(args, name).resolve())
    require(math.isfinite(args.timeout) and args.timeout > 0, "native mixed IID timeout must be positive and finite")
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    require_revision(REPOSITORY, revision)
    require_revision(args.reference, REFERENCE)
    source = args.reference / SOURCE
    require(digest(source) == SOURCE_SHA256, "original mixed IID source digest differs from C84")
    require(not any(args.output.is_relative_to(root) for root in (REPOSITORY, args.reference, args.reference_run)),
            "new native output must be outside the source checkouts and reference evidence")
    reference = read_reference(args.reference_run)
    inputs = input_receipt([Path(__file__).resolve(), HERE / "run_mixed_iid_reference.py", HERE / "run_history.py",
                            HERE / "run_controls.py", HERE / "source_gate.py", HERE / "history_results.py", source,
                            args.native, args.reference_run / "result.json",
                            *(args.reference_run / name for name in REFERENCE_FILES)])
    args.output.mkdir(parents=True, exist_ok=False)
    args.created_output = True
    write_json(args.output / "inputs.json", {"reference_revision": REFERENCE, "implementation_revision": revision,
                                            "original_case": UNIQUE_ID, "inputs": inputs})
    native = args.output / "native"
    process = run_process([args.native, "mixed-iid", native], REPOSITORY, args.output / "stdout.log",
                          args.output / "stderr.log", args.timeout)
    write_json(args.output / "process.json", process)
    require(process["completed"] and process["exit_code"] == 0, "native mixed IID process did not complete")
    execution_path = native / "execution.json"
    execution = read_json(execution_path)
    require(read_json(args.output / "stdout.log") == execution, "native mixed IID stdout differs from its receipt")
    verify_native(execution, native, revision, digest(args.native))
    report = observations(execution, reference)
    write_json(args.output / "comparison.json", {"observations": report})
    verify_inputs(inputs)
    require_revision(REPOSITORY, revision)
    require_revision(args.reference, REFERENCE)
    require(all(result["equal"] for result in report), "native internal mixed IID results differ from D201")
    write_json(args.output / "result.json", {
        "native_internal_match": True, "complete_parity": False, "wire_pending": execution["wire_pending"],
        "case": UNIQUE_ID, "observations": len(report), "execution_sha256": digest(execution_path),
        "reference_result_sha256": digest(args.reference_run / "result.json"),
        "comparison_sha256": digest(args.output / "comparison.json"),
    })


def main():
    parser = argparse.ArgumentParser(description="Compare native mixed IID internal observations against the original C84 case.")
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--reference-run", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    args.created_output = False
    try:
        run(args)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError, subprocess.SubprocessError) as error:
        if args.created_output:
            write_json(args.output / "failure.json", {"native_internal_match": False, "error": str(error)})
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
