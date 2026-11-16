#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
from pathlib import Path

from source_gate import require_phase_two
from history_results import compare_history, verify_artifacts, verify_runtime_artifacts
from run_controls import HERE, REFERENCE, REPOSITORY, digest, git, run_process, validate_native_revision, write_json


def input_receipt(paths):
    return [{"path": str(path), "sha256": digest(path)} for path in paths]


def verify_inputs(inputs):
    if any(digest(item["path"]) != item["sha256"] for item in inputs):
        raise ValueError("history runner input changed during execution")


def execute(args, engine, directory, revision):
    directory.mkdir()
    history = directory / "history"
    if engine == "rust":
        command = [args.native, "history", args.fixture, history]
        cwd = REPOSITORY
    else:
        command = [args.reference / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk",
                   "--init-script", HERE / "reference.init.gradle", "rtdbConformanceHistory",
                   f"-Prtdb.adapterRoot={HERE / 'jvm'}", f"-Prtdb.fixture={args.fixture}", f"-Prtdb.output={history}"]
        cwd = args.reference
    process = run_process(command, cwd, directory / "stdout.log", directory / "stderr.log", args.timeout)
    write_json(directory / "process.json", process)
    if not process["completed"] or process["exit_code"] != 0:
        raise RuntimeError(f"{engine} historical fixture failed; inspect {directory}")
    execution = json.loads((history / "execution.json").read_bytes())
    if engine == "rust":
        validate_native_revision(execution, revision)
        if execution["binary_sha256"] != digest(args.native):
            raise ValueError("history receipt does not identify the executed native binary")
        if json.loads((directory / "stdout.log").read_bytes()) != execution:
            raise ValueError("native history stdout differs from its retained execution receipt")
    else:
        artifacts = history / "runtime-artifacts.json"
        if digest(artifacts) != execution.get("binary_sha256") or json.loads(artifacts.read_bytes()) != execution.get("artifacts"):
            raise ValueError("JVM history receipt differs from its actual runtime artifact closure")
        verify_runtime_artifacts(execution["artifacts"])
    if execution.get("engine") != engine or execution.get("fixture_sha256") != digest(args.fixture):
        raise ValueError("history execution has a different engine or fixture identity")
    binary = execution.get("binary_sha256", "")
    if len(binary) != 64 or any(char not in "0123456789abcdef" for char in binary):
        raise ValueError("history execution omitted its actual artifact digest")
    verify_artifacts(history, execution)
    write_json(directory / "trace.json", {"process_sha256": digest(directory / "process.json"),
        "stdout_sha256": digest(directory / "stdout.log"), "stderr_sha256": digest(directory / "stderr.log"),
        "execution_sha256": digest(history / "execution.json"), "artifact_revision": revision})
    return execution


def run(args):
    require_phase_two(REPOSITORY, args.phase_two)
    if args.timeout <= 0:
        raise ValueError("history runner timeout must be positive")
    if git(args.reference, "rev-parse", "HEAD") != REFERENCE or git(args.reference, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("reference checkout must be clean and pinned")
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    if git(REPOSITORY, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("commit the implementation and history fixture before collecting receipts")
    inputs = input_receipt([Path(__file__).resolve(), HERE / "history_results.py", HERE / "run_controls.py",
                            HERE / "reference.init.gradle", HERE / "jvm/rtdb/conformance/history.clj", args.fixture, args.native])
    fixture = json.loads(args.fixture.read_bytes())
    args.output.mkdir(parents=True, exist_ok=False)
    args.created_output = True
    write_json(args.output / "inputs.json", {"reference_revision": REFERENCE, "implementation_revision": revision, "inputs": inputs})
    reference = execute(args, "jvm", args.output / "reference", REFERENCE)
    repeated = execute(args, "jvm", args.output / "reference-repeat", REFERENCE)
    native = execute(args, "rust", args.output / "native", revision)
    repeat = compare_history(fixture, reference, repeated, digest(args.fixture))
    comparison = compare_history(fixture, reference, native, digest(args.fixture))
    write_json(args.output / "repeat-comparison.json", repeat)
    write_json(args.output / "comparison.json", comparison)
    verify_inputs(inputs)
    for repository, expected in ((REPOSITORY, revision), (args.reference, REFERENCE)):
        if git(repository, "rev-parse", "HEAD") != expected or git(repository, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("a history source checkout changed during execution")
    if not repeat["history_selection_passed"] or not comparison["history_selection_passed"]:
        raise ValueError("historical observations differ; inspect the retained comparison reports")
    write_json(args.output / "result.json", {"history_selection_passed": True, "complete_parity": False,
        "assertion_ledger_reconciled": False, "observations": comparison["required_observations"],
        "comparison_sha256": digest(args.output / "comparison.json"), "repeat_sha256": digest(args.output / "repeat-comparison.json"),
        "inputs_sha256": digest(args.output / "inputs.json"), "read_measurements_comparable": False})


def main():
    parser = argparse.ArgumentParser(description="Retain native and pinned JVM historical-price observations without changing expected values.")
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, default=HERE / "fixtures/historical-prices.json")
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    args.created_output = False
    for field in ("reference", "native", "output", "fixture"):
        setattr(args, field, getattr(args, field).resolve())
    try:
        run(args)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        if args.created_output:
            write_json(args.output / "failure.json", {"history_selection_passed": False, "error": str(error)})
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
