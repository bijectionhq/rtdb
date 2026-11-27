"""Summarises native corpus results: totals per outcome, the explicit exclusions applied, and the largest failure clusters.

A Var that failed counts as a failure unless `corpus-exclusions.json` lists it and every failure the corpus recorded for it
satisfies the match of the entry's decision (a Var with more failures than the runner records is never excused); each decision names the RTDB feature or decision that justifies it.
There is no pattern that excuses a Var the list does not name, and one unexcused failure is enough for the Var to count.
A Var that passed without reaching a native entry point is `pass-jvm-only`: it is neither a failure nor native evidence.
The summary also names the test sources the run loaded (the pinned upstream commit `upstream_tests.sh` extracted), entries that
no longer apply (stale) and entries a recorded failure fell outside (uncovered), so the list is re-reviewed after each run.
"""

import argparse
import collections
import json
import pathlib
import re

HERE = pathlib.Path(__file__).resolve().parent
EXCLUSIONS_PATH = HERE / "corpus-exclusions.json"
EXCLUDED_OUTCOMES = {"jvm-api-boundary", "jvm-internal", "environment-unavailable", "jvm-layout"}
FAILED_RESULTS = {"error", "fail"}
NATIVE_PASSES = {"pass", "pass-wire"}
PASSED_RESULTS = NATIVE_PASSES | {"pass-jvm-only"}
EXCLUDED_VAR_RESULTS = EXCLUDED_OUTCOMES | {"pass-jvm-only", "skipped-integration"}
VAR_RESULTS = PASSED_RESULTS | FAILED_RESULTS | {"timeout", "skipped-integration", "no-assertions"}
UNSUCCESSFUL = {"error", "fail", "timeout", "process-crash", "load-error", "no-assertions", "unobserved"}
NOISE = re.compile(r"\d+|0x[0-9a-f]+")

# Gauges of the JVM runtime and of Arrow Java's child-allocator tree.
JVM_ONLY_METRICS = {"jvm.memory.used", "xtdb.allocator.memory.allocated"}
MISSING_METRICS = re.compile(r'actual: \(not \(empty\? #\{([^}]*)\}\)\)')
DATA_FILE_SIZE = re.compile(r":data-file-size \d+")


def only_data_file_sizes_differ(detail):
    """Whether a failed block-catalog comparison's two maps agree once data file sizes are set aside."""
    if ".binpb.edn]" not in detail: return False
    start = detail.find("(not (= ")
    if start < 0: return False
    body = detail[start + len("(not (= "):]
    depth = 0
    for index, char in enumerate(body):
        if char == "{": depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                expected, actual = body[:index + 1], body[index + 1:].strip()
                actual = actual[:actual.rfind("}") + 1]
                return expected != actual and DATA_FILE_SIZE.sub(":data-file-size _", expected) == DATA_FILE_SIZE.sub(":data-file-size _", actual)
    return False


def only_jvm_runtime_metrics_missing(detail):
    """Whether a metrics assertion failed only because JVM-runtime or child-allocator gauges are absent."""
    missing = MISSING_METRICS.findall(detail)
    return bool(missing) and all(re.fullmatch(r'(?:\s*"[^"]+")+\s*', names)
                                 and set(re.findall(r'"([^"]+)"', names)) <= JVM_ONLY_METRICS
                                 for names in missing)


CHECKS = {
    "block-catalog-differs-only-in-data-file-size": only_data_file_sizes_differ,
    "missing-only-jvm-runtime-metrics": only_jvm_runtime_metrics_missing,
    "any-failure": lambda detail: True,
}


class Exclusions:
    """The reviewed exclusion list: decisions, and the Vars each one excuses."""

    def __init__(self, document):
        problems = []
        self.decisions = {}
        for name, decision in document.get("decisions", {}).items():
            unknown = set(decision) - {"outcome", "decision", "match", "check"}
            if unknown: problems.append(f"decision {name} has unknown keys {sorted(unknown)}")
            if decision.get("outcome") not in EXCLUDED_OUTCOMES: problems.append(f"decision {name} has outcome {decision.get('outcome')!r}")
            if not (decision.get("decision") or "").strip(): problems.append(f"decision {name} does not name its justification")
            if ("match" in decision) == ("check" in decision): problems.append(f"decision {name} needs exactly one of match or check")
            if "check" in decision and decision["check"] not in CHECKS: problems.append(f"decision {name} has unknown check {decision['check']!r}")
            if "match" in decision and not decision["match"]: problems.append(f"decision {name} has an empty match")
            matchers = [re.compile(pattern) for pattern in decision.get("match", [])]
            self.decisions[name] = (decision, matchers)
        self.vars = {}
        for entry in document.get("vars", []):
            var = entry.get("var")
            unknown = set(entry) - {"var", "decision", "note"}
            if unknown: problems.append(f"entry {var} has unknown keys {sorted(unknown)}")
            if not var or "/" not in var: problems.append(f"entry {entry!r} does not name a Var as ns/var")
            if entry.get("decision") not in self.decisions: problems.append(f"entry {var} names unknown decision {entry.get('decision')!r}")
            if var in self.vars: problems.append(f"entry {var} is listed twice")
            self.vars[var] = entry
        if problems:
            raise ValueError("invalid corpus exclusions:\n  " + "\n  ".join(problems))

    def covers(self, var, failure):
        decision, matchers = self.decisions[self.vars[var]["decision"]]
        if "check" in decision:
            return CHECKS[decision["check"]](failure)
        return any(matcher.search(failure) for matcher in matchers)

    def outcome_of(self, var):
        return self.decisions[self.vars[var]["decision"]][0]["outcome"]


def load_exclusions(path=EXCLUSIONS_PATH):
    return Exclusions(json.loads(pathlib.Path(path).read_text(encoding="utf-8")))


EXCLUSIONS = load_exclusions()


def failures(row):
    """Every failure the corpus recorded for a Var; rows from a runner that kept only the first carry it as `detail`."""
    if "failures" in row:
        return list(row["failures"])
    return [row["detail"]] if row.get("detail") else []


def uncovered_failures(row, exclusions=None):
    """The recorded failures of a listed Var that its exclusion does not cover."""
    exclusions = exclusions or EXCLUSIONS
    return [failure for failure in failures(row) if not exclusions.covers(row["var"], failure)]


def failure_evidence_complete(row):
    texts = row.get("failures")
    if not texts or row.get("failures-truncated"):
        return False
    if "fail" in row or "error" in row:
        return len(texts) == row.get("fail", 0) + row.get("error", 0)
    return True


def outcome(row, exclusions=None):
    exclusions = exclusions or EXCLUSIONS
    result = row["result"]
    if result == "jvm-internal":
        return "pass-jvm-only"  # a native-free pass, as the runner named it before pass-jvm-only
    if (result in FAILED_RESULTS and row["var"] in exclusions.vars and failure_evidence_complete(row)
            and not uncovered_failures(row, exclusions)):
        return exclusions.outcome_of(row["var"])
    return result


def load(directory: pathlib.Path):
    if (directory / "selection.json").is_file():
        yield from read_run(directory)["rows"]
        return
    for path in sorted(directory.glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                yield json.loads(line)


def upstream_commit_of(directory: pathlib.Path):
    """The upstream commit whose test sources the run loaded, as upstream_tests.sh recorded it, or None."""
    marker = directory / ".upstream-tests" / ".commit"
    return marker.read_text(encoding="utf-8").strip() if marker.is_file() else None


def namespace_results(directory, namespace):
    rows, problems = [], []
    vars_ = None
    finished = False
    path = directory / f"{namespace}.jsonl"
    if path.is_file():
        with path.open(encoding="utf-8") as stream:
            try:
                for line in stream:
                    if not line.endswith("\n"):
                        raise ValueError("incomplete corpus result line")
                    row = json.loads(line)
                    if not isinstance(row, dict) or row.get("ns") != namespace or finished:
                        raise ValueError("unexpected namespace result")
                    event = row.get("event")
                    if event == "load-error" and vars_ is None:
                        raise ValueError(f'namespace load failed: {row.get("detail")}')
                    if event == "namespace-start" and vars_ is None:
                        found = row.get("vars")
                        if (not isinstance(found, list) or not all(
                                isinstance(var, str) and var.startswith(namespace + "/") for var in found)
                                or len(found) != len(set(found))):
                            raise ValueError("invalid discovered Var list")
                        vars_ = found
                    elif event == "namespace-complete" and vars_ is not None and len(rows) == len(vars_):
                        finished = True
                    elif (event is None and vars_ is not None and len(rows) < len(vars_)
                          and row.get("var") == vars_[len(rows)] and row.get("result") in VAR_RESULTS):
                        rows.append(row)
                    else:
                        raise ValueError("unexpected or reordered corpus result")
            except ValueError as error:
                problems.append(str(error))
    exit_path = directory / f"{namespace}.exit"
    exit_code = exit_path.read_text(encoding="ascii").strip() if exit_path.is_file() else None
    if exit_code != "0":
        problems.append(f"JVM exit status {exit_code if exit_code is not None else 'unknown'}")
    if not finished:
        problems.append("namespace did not complete")
    if vars_ is not None:
        rows.extend({"ns": namespace, "var": var, "result": "unobserved"}
                    for var in vars_[len(rows):])
    return rows, vars_ is not None, problems


def read_run(directory):
    selection = json.loads((directory / "selection.json").read_text(encoding="utf-8"))
    namespaces = selection["namespaces"]
    if (selection.get("schema_version") != 1 or not isinstance(namespaces, list) or not namespaces
            or any(not isinstance(ns, str) or not re.fullmatch(r"[\w.-]+", ns, re.ASCII) for ns in namespaces)
            or len(namespaces) != len(set(namespaces))):
        raise ValueError("invalid corpus selection")
    if selection["upstream_commit"] != upstream_commit_of(directory):
        raise ValueError("corpus selection and extracted upstream SHA differ")
    rows, discovered, problems = [], set(), {}
    for namespace in namespaces:
        batch, known, errors = namespace_results(directory, namespace)
        rows.extend(batch)
        if known:
            discovered.add(namespace)
        if errors:
            problems[namespace] = errors
    return {"selection": selection, "rows": rows, "discovered": discovered, "problems": problems,
            "complete": not problems and len(discovered) == len(namespaces)}


def var_counts(totals):
    passed = sum(totals[name] for name in NATIVE_PASSES)
    excluded = sum(totals[name] for name in EXCLUDED_VAR_RESULTS)
    discovered = sum(totals.values())
    return {"discovered_vars": discovered, "passed": passed, "failed": discovered - passed - excluded,
            "excluded": excluded, "unobserved": totals["unobserved"]}


def run_summary(run, exclusions=None):
    grouped = collections.defaultdict(collections.Counter)
    for row in run["rows"]:
        grouped[row["ns"]][outcome(row, exclusions)] += 1
    namespaces = {}
    for ns in run["selection"]["namespaces"]:
        counts = var_counts(grouped[ns])
        namespaces[ns] = {**counts, "var_denominator": counts["discovered_vars"] if ns in run["discovered"] else None,
                          "outcomes": dict(grouped[ns]), "evidence_errors": run["problems"].get(ns, [])}
    totals = var_counts(sum(grouped.values(), collections.Counter()))
    unknown = sorted(set(namespaces) - run["discovered"])
    return {"schema_version": 1, **run["selection"], "execution_complete": run["complete"],
            "totals": {**totals, "var_denominator": totals["discovered_vars"] if not unknown else None},
            "unknown_namespaces": unknown, "namespaces": namespaces}


def summarise(rows, exclusions=None):
    exclusions = exclusions or EXCLUSIONS
    totals = collections.Counter()
    excluded = collections.Counter()
    clusters = collections.Counter()
    examples = {}
    stale, uncovered, seen = [], [], set()
    failure_count = legacy = 0
    for row in rows:
        var = row["var"]
        seen.add(var)
        kind = outcome(row, exclusions)
        totals[kind] += 1
        if row["result"] in FAILED_RESULTS and "failures" not in row:
            legacy += 1
        if kind in EXCLUDED_OUTCOMES and var in exclusions.vars:
            excluded[exclusions.vars[var]["decision"]] += 1
        elif var in exclusions.vars:
            if row["result"] in PASSED_RESULTS or row["result"] == "jvm-internal":
                stale.append(var)
            elif row["result"] in FAILED_RESULTS:
                missed = uncovered_failures(row, exclusions)
                uncovered.append((var, len(missed), (missed or ["(no failure text recorded)"])[0][:300]))
        if kind in UNSUCCESSFUL:
            texts = failures(row) or [row.get("detail") or ""]
            failure_count += len(failures(row))
            for text in texts:
                signature = NOISE.sub("#", text[:160])
                clusters[(kind, signature)] += 1
                examples.setdefault((kind, signature), var)
    ran = {var.split("/")[0] for var in seen}
    absent = sorted(var for var in exclusions.vars if var not in seen and var.split("/")[0] in ran)
    return {"totals": totals, "excluded": excluded, "clusters": clusters, "examples": examples, "stale": sorted(stale),
            "uncovered": sorted(uncovered), "absent": absent, "failure_count": failure_count, "legacy_rows": legacy}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=pathlib.Path)
    parser.add_argument("--clusters", type=int, default=40)
    parser.add_argument("--exclusions", type=pathlib.Path, default=EXCLUSIONS_PATH)
    args = parser.parse_args(argv)
    exclusions = load_exclusions(args.exclusions)
    run = read_run(args.results) if (args.results / "selection.json").is_file() else None
    summary = summarise(run["rows"] if run else load(args.results), exclusions)
    receipt = run_summary(run, exclusions) if run else None
    if receipt:
        (args.results / "summary.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(receipt["totals"], indent=2))
    else:
        print("historical results without discovery and process receipts")
    commit = upstream_commit_of(args.results)
    print(f"test sources: upstream XTDB {commit}" if commit else
          "test sources: UNKNOWN - this run did not load upstream_tests.sh's extraction, so it does not measure upstream conformance")
    if summary["legacy_rows"]:
        print(f"warning: {summary['legacy_rows']} failed Vars recorded only their first failure (results from the older runner)")
    print(json.dumps(dict(summary["totals"].most_common()), indent=1))
    print(f"failures recorded across unsuccessful Vars: {summary['failure_count']}")
    print("excluded by decision (corpus-exclusions.json):")
    print(json.dumps(dict(summary["excluded"].most_common()), indent=1))
    for var, count, first in summary["uncovered"]:
        print(f"uncovered exclusion: {var} has {count} failure(s) outside its decision or incomplete diagnostics\n     {first}")
    for var in summary["stale"]:
        print(f"stale exclusion: {var} no longer fails")
    for var in summary["absent"]:
        print(f"absent exclusion: {var} is not in its namespace's results")
    for (kind, signature), count in summary["clusters"].most_common(args.clusters):
        print(f"{count:4d} {kind:13s} {summary['examples'][(kind, signature)]}\n     {signature}")
    return 0 if receipt and receipt["execution_complete"] and receipt["totals"]["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
