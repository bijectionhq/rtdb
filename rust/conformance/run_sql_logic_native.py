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
import sql_logic_selections  # noqa: E402

UPSTREAM_COMMIT = "0b9fa440f23d2165250d215209f7fa9bd7c26a6f"
SLT_PATHS = (
    "build.gradle.kts",
    "sqlite-test",
    "src/test/clojure/xtdb/sql/logic_test",
    "src/test/resources/xtdb/sql/logic_test",
)
PASS_DISPOSITIONS = {"statement_ok", "statement_expected_error", "query_ok"}
FAIL_DISPOSITIONS = {"query_mismatch", "error", "harness_gap", "statement_expected_error_missing", "record_parse_error"}
EXCLUDED_DISPOSITIONS = {
    "engine_condition",
    "query_limit",
    "script_parse_error",
    "optimizer_mode_unavailable",
    "suppressed_postgresql_error",
    "suppressed_duplicate_projection",
}


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


def run_script(native: Path, root: Path, reference: str, out_root: Path, index: int, entry: tuple, options_base: dict, keep_jsonl: bool) -> dict:
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
    exec_data = json.loads(exec_path.read_text(encoding="utf-8")) if exec_path.is_file() else None
    script_receipt = (exec_data.get("scripts") or [{}])[0] if isinstance(exec_data, dict) else {}
    dispositions = script_receipt.get("dispositions") or {}
    failures = []
    obs_file = obs_dir / "script-00000.jsonl"
    if obs_file.is_file():
        with obs_file.open(encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                row = json.loads(line)
                if row.get("disposition") in FAIL_DISPOSITIONS:
                    failures.append({
                        "line": row["identity"]["line"],
                        "disposition": row["disposition"],
                        "error": row.get("error"),
                        "expected": row.get("expected"),
                        "result": row.get("result"),
                    })
        if not keep_jsonl:
            obs_file.unlink()
    if not keep_jsonl:
        (plan_dir / "records-00000.jsonl").unlink(missing_ok=True)
    passed = sum(v for k, v in dispositions.items() if k in PASS_DISPOSITIONS)
    failed = sum(v for k, v in dispositions.items() if k in FAIL_DISPOSITIONS)
    excluded = sum(v for k, v in dispositions.items() if k in EXCLUDED_DISPOSITIONS)
    return {
        "namespace": selection_id,
        "source": source,
        "direct_sql": direct,
        "decorrelate": decorrelate,
        "assertions": script["assertions"],
        "passed": passed,
        "failed": failed,
        "excluded": excluded,
        "dispositions": dispositions,
        "exit_code": proc.returncode,
        "elapsed_s": elapsed,
        "error": script_receipt.get("error") or (proc.stderr.strip()[-400:] if proc.returncode != 0 else None),
        "sample_failures": failures[:20],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", type=Path, required=True, help="Path to cargo-built rtdb-conformance binary")
    parser.add_argument("--output", type=Path, required=True, help="Output directory (must not exist)")
    parser.add_argument("--repo", type=Path, default=HERE.parents[1], help="Git repository containing the upstream commit")
    parser.add_argument("--commit", default=UPSTREAM_COMMIT, help="Upstream commit SHA to extract SLT corpus from")
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

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        futures = [
            pool.submit(run_script, args.native, upstream_root, resolved_commit, args.output, idx, entry, options_base, args.keep_jsonl)
            for idx, entry in enumerate(selected)
        ]
        for fut in futures:
            item = fut.result()
            results.append(item)
            status = "CRASH" if item["exit_code"] != 0 else ("FAIL" if item["failed"] else "OK")
            print(f"[{status:5s}] {item['namespace']} ({item['source']}): pass={item['passed']} fail={item['failed']} excl={item['excluded']} ({item['elapsed_s']}s)", flush=True)

    summary = {
        "upstream_commit": resolved_commit,
        "selections": selections,
        "totals": {
            "scripts": len(results),
            "assertions": sum(r["assertions"] for r in results),
            "passed": sum(r["passed"] for r in results),
            "failed": sum(r["failed"] for r in results),
            "excluded": sum(r["excluded"] for r in results),
            "crashed_scripts": sum(1 for r in results if r["exit_code"] != 0),
        },
        "scripts": results,
    }
    (args.output / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    print(json.dumps(summary["totals"], indent=1))
    return 0 if summary["totals"]["failed"] == 0 and summary["totals"]["crashed_scripts"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
