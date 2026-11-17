#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
from pathlib import Path

from source_gate import require_phase_two
from history_results import verify_runtime_artifacts
from run_controls import HERE, REFERENCE, REPOSITORY, digest, git, owned_cancellation, run_process, validate_native_revision, write_json
from sql_logic_records import write_records
from sql_logic_results import compare, verify_execution
from sql_logic_selections import NON_DIRECT, SELECTION_SOURCES, selections, validate_pinned_selections


def execute(args, engine, directory, plan_path, plan, revision):
    directory.mkdir()
    observations = directory / "observations"
    if engine == "rust":
        command = [args.native, "sql-logic", plan_path, observations]
        cwd = REPOSITORY
    else:
        command = [args.reference / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk",
                   "--init-script", HERE / "reference.init.gradle", "rtdbConformanceSqlLogic",
                   f"-Prtdb.adapterRoot={HERE / 'jvm'}", f"-Prtdb.fixture={plan_path}", f"-Prtdb.output={observations}"]
        cwd = args.reference
    process = run_process(command, cwd, directory / "stdout.log", directory / "stderr.log", args.timeout)
    write_json(directory / "process.json", process)
    if not process["completed"] or process["exit_code"] != 0:
        raise RuntimeError(f"{engine} SQL logic owner did not complete; inspect {directory}")
    execution = json.loads((observations / "execution.json").read_bytes())
    if execution.get("engine") != engine:
        raise ValueError("SQL logic execution engine differs")
    if engine == "rust":
        validate_native_revision(execution, revision)
        if execution.get("binary_sha256") != digest(args.native) or json.loads((directory / "stdout.log").read_bytes()) != execution:
            raise ValueError("native SQL logic execution does not identify its artifact and retained receipt")
    else:
        artifact_path = observations / "runtime-artifacts.json"
        if digest(artifact_path) != execution.get("binary_sha256") or json.loads(artifact_path.read_bytes()) != execution.get("artifacts"):
            raise ValueError("JVM SQL logic execution omits actual runtime artifact closure")
        verify_runtime_artifacts(execution["artifacts"])
        if execution.get("artifact_revision") != REFERENCE:
            raise ValueError("JVM SQL logic execution is not pinned")
    files = verify_execution(plan, digest(plan_path), execution, observations)
    write_json(directory / "trace.json", {"process_sha256": digest(directory / "process.json"),
        "stdout_sha256": digest(directory / "stdout.log"), "stderr_sha256": digest(directory / "stderr.log"),
        "execution_sha256": digest(observations / "execution.json"), "artifact_revision": revision})
    return files


def run(args):
    require_phase_two(REPOSITORY, args.phase_two)
    if args.timeout <= 0 or not 0 < args.max_rows <= 2**63 - 1 or not 0 < args.max_result_bytes <= 512 * 1024 * 1024 or args.query_limit is not None and not 0 <= args.query_limit <= 2**63 - 1:
        raise ValueError("invalid SQL logic execution limit")
    if git(args.reference, "rev-parse", "HEAD") != REFERENCE or git(args.reference, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("reference checkout must be clean and pinned")
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    if git(REPOSITORY, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("commit SQL logic machinery before collecting execution evidence")
    validate_pinned_selections(args.reference)
    selected = selections(args.selection, (args.reference, args.directory) if args.directory else None)
    args.output.mkdir(parents=True, exist_ok=False)
    args.created_output = True
    plan_directory = args.output / "plan"
    plan_directory.mkdir()
    scripts = []
    for index, (selection, source, direct, decorrelate) in enumerate(selected):
        options = {"direct_sql": direct, "decorrelate": decorrelate, "query_limit": args.query_limit,
                   "max_rows": args.max_rows, "max_result_bytes": args.max_result_bytes}
        scripts.append(write_records(args.reference, source, REFERENCE, selection, options, plan_directory / f"records-{index:05}.jsonl"))
    plan = {"schema_version": 1, "reference": REFERENCE, "selection": "+".join(args.selection), "scripts": scripts}
    plan_path = plan_directory / "plan.json"
    write_json(plan_path, plan)
    selection_paths = {source for _, source, _, _ in selected}
    corpus = [path for path in git(args.reference, "ls-tree", "-r", "--name-only", "HEAD").splitlines()
              if path.endswith(".test") and (path.startswith("sqlite-test/") or path.startswith("src/test/resources/xtdb/sql/logic_test/"))]
    write_json(args.output / "selection.json", {"reference": REFERENCE, "names": args.selection, "query_limit": args.query_limit,
        "original_task_allowances": NON_DIRECT, "allowances_grant_passing_records": False,
        "sources": [{"path": source, "sha256": digest(args.reference / source), "selected": source in selection_paths,
                     "reason": "selected" if source in selection_paths else "outside_explicit_original_selection"} for source in corpus]})
    paths = [Path(__file__).resolve(), HERE / "sql_logic_records.py", HERE / "sql_logic_selections.py", HERE / "sql_logic_results.py",
             HERE / "run_controls.py", HERE / "history_results.py", HERE / "reference.init.gradle",
             HERE / "jvm/rtdb/conformance/sql_logic.clj", args.native, plan_path,
             *(plan_directory / script["records_file"] for script in scripts), *(args.reference / source for source in SELECTION_SOURCES),
             *(args.reference / source for source in selection_paths), args.output / "selection.json"]
    inputs = [{"path": str(path), "sha256": digest(path)} for path in paths]
    write_json(args.output / "inputs.json", {"reference_revision": REFERENCE, "implementation_revision": revision, "inputs": inputs})
    reference = execute(args, "jvm", args.output / "reference", plan_path, plan, REFERENCE)
    repeated = execute(args, "jvm", args.output / "reference-repeat", plan_path, plan, REFERENCE)
    native = execute(args, "rust", args.output / "native", plan_path, plan, revision)
    repeat = compare(plan, plan_directory, reference, repeated, args.output / "repeat-records.jsonl")
    comparison = compare(plan, plan_directory, reference, native, args.output / "comparison-records.jsonl")
    write_json(args.output / "repeat.json", repeat)
    write_json(args.output / "comparison.json", comparison)
    if any(digest(item["path"]) != item["sha256"] for item in inputs):
        raise ValueError("SQL logic artifact or source changed during execution")
    for repository, expected in ((REPOSITORY, revision), (args.reference, REFERENCE)):
        if git(repository, "rev-parse", "HEAD") != expected or git(repository, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("SQL logic source checkout changed during execution")
    write_json(args.output / "result.json", {"selection_passed": repeat["selection_passed"] and comparison["selection_passed"],
        "complete_parity": False, "assertion_ledger_reconciled": False, "inputs_sha256": digest(args.output / "inputs.json"),
        "comparison_sha256": digest(args.output / "comparison.json"), "repeat_sha256": digest(args.output / "repeat.json")})
    if not repeat["selection_passed"] or not comparison["selection_passed"]:
        raise ValueError("SQL logic selection retains failed, skipped, unavailable, suppressed, or mismatching assertions")


def main():
    parser = argparse.ArgumentParser(description="Execute original SQL logic records through native and pinned JVM production nodes.")
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--selection", action="append", choices=["slt-test", "slt-test-2", "direct-sql-test", "slt-test-dir"])
    parser.add_argument("--directory")
    parser.add_argument("--query-limit", type=int)
    parser.add_argument("--max-rows", type=int, default=1_000_000)
    parser.add_argument("--max-result-bytes", type=int, default=128 * 1024 * 1024)
    parser.add_argument("--timeout", type=float, default=7200)
    args = parser.parse_args()
    args.selection = args.selection or ["slt-test", "slt-test-2", "direct-sql-test"]
    args.created_output = False
    for field in ("reference", "native", "output"):
        setattr(args, field, getattr(args, field).resolve())
    try:
        with owned_cancellation():
            run(args)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        if args.created_output:
            write_json(args.output / "failure.json", {"selection_passed": False, "error": str(error)})
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
