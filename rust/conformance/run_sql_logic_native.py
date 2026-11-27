#!/usr/bin/env python3
"""Run upstream XTDB's SQL Logic Test corpus against a cargo-built rtdb-conformance binary (no Gradle, no JVM)."""

import argparse
import concurrent.futures
import json
import subprocess
import sys
import tarfile
import time
from io import BytesIO
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import sql_logic_records  # noqa: E402
import sql_logic_results  # noqa: E402
import sql_logic_selections  # noqa: E402

UPSTREAM_COMMIT = (HERE / "upstream-commit.txt").read_text(encoding="ascii").strip()
C84_REFERENCE = "c84f9334eb060ceb92d28ecdd4a06e6b625932fd"
SLT_PATHS = (
    "build.gradle.kts",
    "sqlite-test",
    "src/test/clojure/xtdb/sql/logic_test",
    "src/test/resources/xtdb/sql/logic_test",
)
PASS_DISPOSITIONS = sql_logic_results.PASSING
EXCLUDED_DISPOSITIONS = {
    "engine_condition",
    "query_limit",
    "suppressed_postgresql_error",
    "suppressed_duplicate_projection",
}


def assertion_results(script, records_path, observations_path):
    """Join the observed prefix to the immutable plan, retaining every source assertion."""
    counts = {"passed": 0, "failed": 0, "excluded": 0, "unobserved": 0}
    dispositions = {}
    failures = []
    problems = []
    queries = 0
    observed_records = 0
    maximum_bytes = 64 * 1024 * 1024 + 6 * script["options"]["max_result_bytes"]
    observations = sql_logic_results.json_lines(observations_path, maximum_bytes)
    with_observations = observations_path.is_file()
    records = sql_logic_results.json_lines(records_path)
    for original in records:
        observation = None
        if with_observations:
            try:
                observation = next(observations, None)
            except ValueError as error:
                problems.append(str(error))
            if observation is not None:
                problem = (sql_logic_results.check_observation(original, observation)
                           or sql_logic_results.check_selection(original, observation, script, queries))
                if problem:
                    problems.append(f'line {original["line"]}: {problem}')
                    observation = None
                else:
                    observed_records += 1
            with_observations = observation is not None
        record = original.get("record") or {}
        limit = script["options"]["query_limit"]
        if not script["parse_errors"] and queries != limit and record.get("type") == "query":
            queries += 1
        if original["assertion_ordinal"] is None:
            continue
        disposition = observation["disposition"] if observation is not None else "unobserved"
        dispositions[disposition] = dispositions.get(disposition, 0) + 1
        if disposition in PASS_DISPOSITIONS:
            counts["passed"] += 1
        elif disposition in EXCLUDED_DISPOSITIONS:
            counts["excluded"] += 1
        else:
            counts["failed"] += 1
            counts["unobserved"] += disposition == "unobserved"
            if len(failures) < 20:
                failures.append({
                    "line": original["line"],
                    "disposition": disposition,
                    "error": (observation or {}).get("error"),
                    "expected": record.get("expected"),
                    "result": (observation or {}).get("result"),
                })
    if with_observations:
        try:
            if next(observations, None) is not None:
                problems.append("extra observations after the final source record")
        except ValueError as error:
            problems.append(str(error))
    observations.close()
    if counts["passed"] + counts["failed"] + counts["excluded"] != script["assertions"]:
        raise ValueError("SQL logic assertion denominator differs from retained plan")
    if observed_records != script["records"]:
        problems.append(f'observed {observed_records} of {script["records"]} records')
    return {**counts, "dispositions": dispositions, "sample_failures": failures, "evidence_errors": problems}


def extract_upstream(repo: Path, commit: str, dest: Path) -> Path:
    marker = dest / ".commit"
    resolved = subprocess.check_output(["git", "-C", str(repo), "rev-parse", f"{commit}^{{commit}}"], text=True).strip()
    if marker.is_file() and marker.read_text(encoding="utf-8").strip() == resolved:
        return dest
    dest.mkdir(parents=True, exist_ok=True)
    archive = subprocess.check_output(["git", "-C", str(repo), "archive", "--format=tar", resolved, "--", *SLT_PATHS])
    with tarfile.open(fileobj=BytesIO(archive), mode="r:") as tar:
        tar.extractall(dest, filter="data")
    marker.write_text(resolved + "\n", encoding="utf-8")
    return dest


def run_script(native: Path, root: Path, reference: str, out_root: Path, index: int, entry: tuple, options_base: dict, binary_sha256: str, keep_jsonl: bool) -> dict:
    selection_id, source, direct, decorrelate = entry
    job_dir = out_root / f"job-{index:05d}"
    plan_dir = job_dir / "plan"
    plan_dir.mkdir(parents=True)
    options = {**options_base, "direct_sql": direct, "decorrelate": decorrelate}
    script = sql_logic_records.write_records(root, source, reference, selection_id, options, plan_dir / "records-00000.jsonl")
    plan_path = plan_dir / "plan.json"
    plan_path.write_text(json.dumps({"schema_version": 1, "reference": reference, "selection": selection_id, "scripts": [script]}), encoding="utf-8")
    obs_dir = job_dir / "observations"
    started = time.monotonic()
    proc = subprocess.run([str(native), "sql-logic", str(plan_path), str(obs_dir)], capture_output=True, text=True)
    elapsed = round(time.monotonic() - started, 2)
    exec_path = obs_dir / "execution.json"
    obs_file = obs_dir / "script-00000.jsonl"
    results = assertion_results(script, plan_dir / script["records_file"], obs_file)
    exec_data, script_receipt = {}, {}
    if exec_path.is_file():
        try:
            exec_data = json.loads(exec_path.read_text(encoding="utf-8"))
            script_receipt, = exec_data["scripts"]
            if exec_data["binary_sha256"] != binary_sha256:
                raise ValueError("SQL logic execution used another binary")
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            exec_data, script_receipt = {}, {}
            results["evidence_errors"].append(f"invalid execution receipt: {error}")
    if proc.returncode == 0:
        try:
            sql_logic_results.verify_execution(
                {"scripts": [script]}, sql_logic_records.digest(plan_path), exec_data or {}, obs_dir,
            )
        except (KeyError, TypeError, ValueError) as error:
            results["evidence_errors"].append(str(error))
    if proc.stderr:
        (job_dir / "stderr.log").write_text(proc.stderr, encoding="utf-8")
    if not keep_jsonl:
        obs_file.unlink(missing_ok=True)
        (plan_dir / script["records_file"]).unlink()
    return {
        "namespace": selection_id,
        "source": source,
        "direct_sql": direct,
        "decorrelate": decorrelate,
        "assertions": script["assertions"],
        **results,
        "exit_code": proc.returncode,
        "elapsed_s": elapsed,
        "artifact_revision": (exec_data or {}).get("artifact_revision"),
        "source_dirty": (exec_data or {}).get("source_dirty"),
        "error": script_receipt.get("error") or (proc.stderr.strip()[-400:] if proc.returncode != 0 else None),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", type=Path, required=True, help="Path to cargo-built rtdb-conformance binary")
    parser.add_argument("--output", type=Path, required=True, help="Output directory (must not exist)")
    parser.add_argument("--repo", type=Path, default=HERE.parents[1], help="Git repository containing the upstream commit")
    parser.add_argument("--commit", choices=(UPSTREAM_COMMIT, C84_REFERENCE), default=UPSTREAM_COMMIT,
                        help="Upstream commit SHA to extract SLT corpus from")
    parser.add_argument("--selection", action="append", choices=["direct-sql-test", "slt-test", "slt-test-2"])
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--query-limit", type=int)
    parser.add_argument("--max-rows", type=int, default=1_000_000)
    parser.add_argument("--max-result-bytes", type=int, default=128 * 1024 * 1024)
    parser.add_argument("--keep-jsonl", action="store_true")
    args = parser.parse_args(argv)

    args.native = args.native.resolve()
    args.repo = args.repo.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)

    selections = args.selection or ["direct-sql-test", "slt-test", "slt-test-2"]
    upstream_root = extract_upstream(args.repo, args.commit, args.output / ".upstream-slt")
    resolved_commit = (upstream_root / ".commit").read_text(encoding="utf-8").strip()
    sql_logic_selections.validate_pinned_selections(upstream_root)
    selected = sql_logic_selections.selections(selections)
    options_base = {"query_limit": args.query_limit, "max_rows": args.max_rows, "max_result_bytes": args.max_result_bytes}

    binary_sha256 = sql_logic_records.digest(args.native)
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        futures = [
            pool.submit(run_script, args.native, upstream_root, resolved_commit, args.output, idx, entry, options_base, binary_sha256, args.keep_jsonl)
            for idx, entry in enumerate(selected)
        ]
        for fut in futures:
            item = fut.result()
            results.append(item)
            status = "ERROR" if item["exit_code"] != 0 or item["evidence_errors"] else ("FAIL" if item["failed"] else "OK")
            print(f"[{status:5s}] {item['namespace']} ({item['source']}): pass={item['passed']} fail={item['failed']} excl={item['excluded']} ({item['elapsed_s']}s)", flush=True)

    summary = {
        "upstream_commit": resolved_commit,
        "selections": selections,
        "binary_sha256": binary_sha256,
        "totals": {
            "scripts": len(results),
            "assertions": sum(r["assertions"] for r in results),
            "passed": sum(r["passed"] for r in results),
            "failed": sum(r["failed"] for r in results),
            "excluded": sum(r["excluded"] for r in results),
            "unobserved": sum(r["unobserved"] for r in results),
            "crashed_scripts": sum(r["exit_code"] < 0 for r in results),
            "incomplete_scripts": sum(r["exit_code"] != 0 or bool(r["evidence_errors"]) for r in results),
        },
        "scripts": results,
    }
    (args.output / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    print(json.dumps(summary["totals"], indent=1))
    return 0 if summary["totals"]["failed"] == 0 and summary["totals"]["incomplete_scripts"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
