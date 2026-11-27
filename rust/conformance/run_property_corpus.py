#!/usr/bin/env python3
"""Run or replay independently counted native properties from the pinned XTDB corpus."""

import argparse
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

from run_controls import HERE, REPOSITORY, digest, git, run_process, write_json


MANIFEST = HERE / "property-corpus.json"
COUNTS = ("planned_trials", "passed_trials", "failed_trials", "unobserved_trials")


def plan_cases(manifest, family, case_name, seed, trials, max_size):
    selected = []
    reports = []
    for group in manifest["families"]:
        if family and group["id"] != family:
            continue
        report = {"family": group["id"], "pinned_vars": len(group["cases"]),
                  "implemented_vars": 0, "selected_vars": 0, "unported_vars": []}
        for item in group["cases"]:
            case = dict(item, family=group["id"], namespace=group["namespace"],
                        kind=group["kind"], source=group["source"])
            if "unported" in case:
                report["unported_vars"].append(case)
                continue
            report["implemented_vars"] += 1
            full_name = f'{case["namespace"]}/{case["name"]}'
            if case_name and case_name != full_name:
                continue
            report["selected_vars"] += 1
            selected.append({"case": case, "settings": {"seed": seed, "max_size": max_size,
                                                        "trials": trials or group["default_trials"]}})
        reports.append(report)
    if not selected:
        raise ValueError("no implemented properties match the selection; see property-corpus.json")
    return selected, reports


def case_key(case):
    return case["namespace"], case["name"]


def summarize(plan, rows, completed):
    start = rows[0] if rows else {}
    started = (start.get("event") == "start" and
               start.get("identity") == {key: plan[key] for key in ("upstream_commit", "manifest_sha256")} and
               start.get("runtime", {}).get("native_library_sha256") == plan["native_library_sha256"])
    stream_complete = bool(completed and started and rows[-1].get("event") == "complete")
    body = rows[1:-1] if stream_complete else rows[1:]
    if plan["mode"] == "replay":
        replay = body[0] if len(body) == 1 and body[0].get("event") == "replay_complete" else None
        return {"mode": "replay", "execution_complete": bool(stream_complete and replay),
                "replay": replay, "runtime": start.get("runtime")}

    by_case = defaultdict(list)
    for row in body:
        if "case" in row:
            by_case[case_key(row["case"])].append(row)
    families = {family["family"]: dict(family, completed_vars=0, **dict.fromkeys(COUNTS, 0))
                for family in plan["families"]}
    all_terminal = (all("case" in row for row in body)
                    and set(by_case) <= {case_key(item["case"]) for item in plan["selected"]})
    for item in plan["selected"]:
        case, planned = item["case"], item["settings"]["trials"]
        family = families[case["family"]]
        receipts = by_case[case_key(case)]
        terminals = [row for row in receipts if row["event"] == "case_complete"]
        trials = [row for row in receipts if row["event"] == "trial"]
        observed = [row["evidence"]["outcome"] for row in trials]
        passed = observed.count("passed")
        failed = len(observed) - passed
        counts = dict(zip(COUNTS, (planned, passed, failed, planned - len(trials))))
        ordered = [row["trial"] for row in trials] == list(range(1, len(trials) + 1))
        valid_trials = (ordered and all(row["case"] == case for row in receipts)
                        and len(trials) <= planned and failed <= 1
                        and all(outcome == "passed" for outcome in observed[:-1])
                        and all(outcome in {"passed", "invariant_failed", "error", "unobserved"} for outcome in observed)
                        and all(any(event.get("kind") in {"sql_query", "logical_query"}
                                    for event in row["evidence"]["observations"])
                                for row in trials if row["evidence"]["outcome"] in {"passed", "invariant_failed"}))
        terminal = terminals[0] if len(terminals) == 1 else {}
        accounted = (started and valid_trials and len(receipts) == len(trials) + 1
                     and receipts[-1] == terminal
                     and all(terminal.get(key) == count for key, count in counts.items())
                     and (len(trials) == planned or failed == 1))
        if not (started and valid_trials):
            counts = dict(zip(COUNTS, (planned, 0, 0, planned)))
        for key, count in counts.items():
            family[key] += count
        family["completed_vars"] += accounted
        all_terminal &= accounted
    complete = stream_complete and all_terminal
    return {"mode": "sweep", "execution_complete": complete,
            "all_selected_passed": complete and all(not family["failed_trials"] for family in families.values()),
            "runtime": start.get("runtime"), "families": list(families.values())}


def read_stream(path):
    rows = []
    if path.is_file():
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    break
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("native_library", type=Path)
    parser.add_argument("output", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--family")
    mode.add_argument("--replay", type=Path)
    parser.add_argument("--case", help="exact upstream namespace/Var")
    parser.add_argument("--seed", type=int, default=6269)
    parser.add_argument("--trials", type=int)
    parser.add_argument("--max-size", type=int, default=32)
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args(argv)
    if (not -(1 << 63) <= args.seed < 1 << 63 or not 1 <= args.max_size <= 200
            or (args.trials is not None and not 1 <= args.trials <= 1000) or args.timeout <= 0):
        parser.error("seed must fit i64, max size must be 1–200, trials must be 1–1000, and timeout must be positive")
    if args.replay and args.case:
        parser.error("--case selects generated trials")
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if manifest["upstream_commit"] != (HERE / "upstream-commit.txt").read_text(encoding="ascii").strip():
        parser.error("property manifest needs review against the updated upstream inventory baseline")
    library = args.native_library.resolve(strict=True)
    if not library.is_file():
        parser.error("native library must be a regular file")
    plan = {"schema_version": 1, "mode": "replay" if args.replay else "sweep",
            "upstream_commit": manifest["upstream_commit"], "manifest_sha256": digest(MANIFEST),
            "native_library_sha256": digest(library), "runner_checkout": git(REPOSITORY, "rev-parse", "HEAD"),
            "runner_checkout_dirty": bool(git(REPOSITORY, "status", "--porcelain"))}
    output = args.output.resolve()
    if args.replay:
        with args.replay.open("rb") as stream:
            token = stream.read(4 * 1024 * 1024 + 1)
        if not token or len(token) > 4 * 1024 * 1024:
            parser.error("replay input must be between 1 byte and 4 MiB")
        output.mkdir(parents=True)
        (output / "replay.edn").write_bytes(token)
        plan["replay_sha256"] = digest(output / "replay.edn")
    else:
        try:
            plan["selected"], plan["families"] = plan_cases(manifest, args.family, args.case, args.seed,
                                                           args.trials, args.max_size)
        except ValueError as exc:
            parser.error(str(exc))
        output.mkdir(parents=True)
    plan_path = output / "selection.json"
    write_json(plan_path, plan)
    source = output / ".upstream-tests"
    env = {**os.environ, "XTDB_UPSTREAM_COMMIT": manifest["upstream_commit"]}
    subprocess.run([HERE / "upstream_tests.sh", source], cwd=REPOSITORY, check=True, env=env)
    process = run_process(
        [REPOSITORY / "gradlew", "--no-daemon", "--console=plain", "-I", HERE / "property-corpus.init.gradle",
         ":bindings:xtdb-rust-bindings:propertyCorpus", f"-PcorpusSources={source}",
         f"-Dxtdb.native.library={library}", f"-Prtdb.propertyPlan={plan_path}",
         f"-Prtdb.propertyManifest={MANIFEST}", f"-Prtdb.propertyOutput={output}"],
        REPOSITORY, output / "stdout.log", output / "stderr.log", args.timeout,
    )
    write_json(output / "process.json", process)
    rows = read_stream(output / "properties.jsonl")
    result = summarize(plan, rows, process["completed"] and process.get("exit_code") == 0)
    write_json(output / "summary.json", result)
    print(json.dumps(result, indent=2))
    return 0 if result["execution_complete"] and result.get("all_selected_passed", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
