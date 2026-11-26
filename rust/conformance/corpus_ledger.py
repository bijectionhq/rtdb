#!/usr/bin/env python3
"""Generates a corpus-backed assertion ledger, its runtime invocation bindings and the native inventory they bind to.

A reference Clojure test's native counterpart is the same Var run through the native corpus (`corpus:<ns>/<var>`).
A test the corpus cannot run natively, or one that exercises JVM implementation classes, carries an explicit
classification instead of a counterpart; classified cases are reconciled but never count as passed native assertions.
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import corpus_summary  # noqa: E402
import runtime_inventory  # noqa: E402

CLASSIFIED_OUTCOMES = {"jvm-api-boundary": "jvm-api-boundary", "jvm-internal": "jvm-internal",
                       "environment-unavailable": "environment-unavailable", "pass-jvm-only": "jvm-internal"}
PASS_JVM_ONLY_REASON = "the Var passes without reaching a native entry point, so it is no evidence about the native engine"
PARAMETER_FIXTURE = "none"
REFERENCE_CONFIGURATION = "reference-pinned"
CLASS_DECLARATION = re.compile(r"\bclass\s+(\w+)\b[^{;]*?(?::\s*([A-Za-z_][\w.]*)\s*\(|\bextends\s+([A-Za-z_][\w.]*))", re.S)


def namespace_of(source):
    """The Clojure namespace a test file declares by convention, from its path under a `clojure` source root."""
    path = source.split("/clojure/", 1)[-1]
    return path.removesuffix(".clj").replace("/", ".").replace("_", "-")


def canonical_var(candidate):
    """A candidate's Var; namespace metadata (`(ns ^:kafka xtdb.kafka-test)`) made the source inventory record `^:kafka/...`."""
    name = candidate["identity"]["name"]
    if "/" not in name:
        return name
    namespace, var = name.split("/", 1)
    if namespace.startswith("^"):
        return namespace_of(candidate["identity"]["source"]) + "/" + var
    return name


def qualified_name(candidate):
    name = candidate["identity"]["name"]
    return name.replace("/", ".", 1) if candidate["kind"] == "junit_template_candidate" else name


def identity_of(candidate, reference):
    return {"reference_revision": reference, "source": candidate["identity"]["source"], "qualified_name": qualified_name(candidate),
            "parameter_fixture": PARAMETER_FIXTURE, "configuration": REFERENCE_CONFIGURATION}


def engine_internal_reason(candidate):
    source = candidate["identity"]["source"]
    if candidate["kind"] == "clojure_var_candidate":
        if source.startswith("api/"):
            return "a Clojure test of the JVM xtdb-api library outside the corpus root"
        return f"a Clojure test of a JVM module outside the corpus root ({source.split('/src/', 1)[0]})"
    if source.startswith("core/"):
        return "a JUnit test of xtdb-core JVM implementation classes"
    if source.startswith("modules/"):
        return f"a JUnit test of the JVM {source.split('/')[1]} module"
    if source.startswith("api/"):
        return "a JUnit test of the JVM xtdb-api library"
    return "a JUnit test of JVM implementation classes in the root test source set"


def superclasses(reference_root):
    """Simple class name to simple superclass name, from every Kotlin and Java test source at the reference revision.
    A subclass that only inherits its tests declares none, so its file has no candidate of its own."""
    parents = {}
    for path in sorted(reference_root.rglob("*")):
        if path.suffix not in (".kt", ".java") or "/src/test/" not in path.as_posix() or not path.is_file():
            continue
        for match in CLASS_DECLARATION.finditer(path.read_text(encoding="utf-8", errors="replace")):
            parent = match.group(2) or match.group(3)
            parents.setdefault(match.group(1), parent.rsplit(".", 1)[-1])
    return parents


def alias_for(name, candidates_by_name, junit_by_method, parents):
    """Maps a runtime source name onto the candidate it executes: a Var behind namespace metadata, or an inherited JUnit method."""
    if name in candidates_by_name:
        return None
    if "/" not in name:
        return None
    owner, method = name.split("/", 1)
    simple = owner.rsplit(".", 1)[-1]
    seen = set()
    while simple in parents and simple not in seen:
        seen.add(simple)
        simple = parents[simple]
        matches = junit_by_method.get((simple, method), [])
        if len(matches) == 1:
            return matches[0]["identity"]["name"]
    return None


def corpus_reason(row, outcome, exclusions):
    """Why a classified corpus Var is not a counterpart: its exclusion's decision, or that it passed without native code."""
    if outcome == "pass-jvm-only":
        return PASS_JVM_ONLY_REASON
    return exclusions.decisions[exclusions.vars[row["var"]]["decision"]][0]["decision"]


def build(source, runtime_cases, corpus_rows, native_configuration, reference_root, corpus_source_revision, exclusions=None):
    reference = source["reference"]
    exclusions = exclusions or corpus_summary.EXCLUSIONS
    corpus = {}
    for row in corpus_rows:
        outcome = corpus_summary.outcome(row, exclusions)
        corpus[row["var"]] = {"outcome": outcome, "detail": (row.get("detail") or "")[:500]}
        if outcome in CLASSIFIED_OUTCOMES:
            corpus[row["var"]]["reason"] = corpus_reason(row, outcome, exclusions)
    candidates_by_name = defaultdict(list)
    junit_by_method = defaultdict(list)
    meta_named = {}
    for candidate in source["candidates"]:
        candidates_by_name[candidate["identity"]["name"]].append(candidate)
        if candidate["kind"] == "junit_template_candidate" and "/" in candidate["identity"]["name"]:
            owner, method = candidate["identity"]["name"].split("/", 1)
            junit_by_method[(owner.rsplit(".", 1)[-1], method)].append(candidate)
        if candidate["kind"] == "clojure_var_candidate" and canonical_var(candidate) != candidate["identity"]["name"]:
            meta_named[canonical_var(candidate)] = candidate
    parents = superclasses(reference_root)

    cases, counts, emitted = [], defaultdict(int), set()
    for candidate in source["candidates"]:
        if (candidate["identity"]["source"], candidate["identity"]["name"]) in emitted:
            counts["redefined_vars"] += 1
            continue
        emitted.add((candidate["identity"]["source"], candidate["identity"]["name"]))
        if candidate["kind"] == "client_source_candidate":
            counts["unmapped_client_candidates"] += 1
            continue
        identity = identity_of(candidate, reference)
        if candidate["kind"] == "clojure_var_candidate" and candidate["identity"]["source"].startswith("src/test/clojure/"):
            var = canonical_var(candidate)
            native = corpus.get(var)
            if native is None:
                counts["absent_from_corpus"] += 1
                continue
            kind = CLASSIFIED_OUTCOMES.get(native["outcome"])
            if kind:
                cases.append({"identity": identity, "counterparts": [], "classification": {"kind": kind, "reason": native["reason"]}})
            else:
                cases.append({"identity": identity, "counterparts": ["corpus:" + var]})
            continue
        cases.append({"identity": identity, "counterparts": [],
                      "classification": {"kind": "engine-internal", "reason": engine_internal_reason(candidate)}})
    identities = {(case["identity"]["source"], case["identity"]["qualified_name"]): case["identity"] for case in cases}

    aliases, invocations = {}, []
    for case in runtime_cases:
        descriptor = case["descriptor"]
        name = runtime_inventory.source_name(descriptor)
        if name is None:
            counts["runtime_without_source_name"] += 1
            continue
        target = name if name in candidates_by_name else None
        if target is None and name in meta_named:
            target = meta_named[name]["identity"]["name"]
        if target is None:
            target = alias_for(name, candidates_by_name, junit_by_method, parents)
        if target is None or len({c["identity"]["source"] for c in candidates_by_name.get(target, [])}) != 1:
            counts["runtime_without_candidate"] += 1
            continue
        if target != name:
            aliases[descriptor["unique_id"]] = target
        candidate = candidates_by_name[target][0]
        identity = identities.get((candidate["identity"]["source"], qualified_name(candidate)))
        if identity is None:
            counts["runtime_of_unmapped_candidate"] += 1
            continue
        invocations.append({"project": case["project"], "task": case["task"], "unique_id": descriptor["unique_id"],
                            "identity": identity, "native_configuration": native_configuration})

    native = {"schema_version": 1, "complete": True, "kind": "corpus",
              "corpus_source_revision": corpus_source_revision,
              "tests": [{"counterpart": "corpus:" + var, "configuration": native_configuration, **value}
                        for var, value in sorted(corpus.items())]}
    ledger = {"schema_version": 1, "inventory_revision": reference, "inventory_complete": False, "runtime_reconciled": False,
              "mapping": "corpus-backed", "cases": cases}
    return ledger, invocations, aliases, native, dict(counts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--reference-checkout", type=Path, required=True)
    parser.add_argument("--native-configuration", required=True)
    parser.add_argument("--corpus-source-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = json.loads(args.source.read_bytes())
    runtime_cases = [case for path in sorted(args.runtime.rglob("*.jsonl")) for case in runtime_inventory.read_plan(path)["cases"]]
    ledger, invocations, aliases, native, counts = build(source, runtime_cases, list(corpus_summary.load(args.corpus)),
                                                         args.native_configuration, args.reference_checkout, args.corpus_source_revision)
    args.output.mkdir(parents=True, exist_ok=False)
    for name, value in [("ledger.json", ledger), ("invocations.json", invocations), ("aliases.json", aliases),
                        ("native.json", native), ("generation.json", counts)]:
        (args.output / name).write_text(json.dumps(value, indent=1) + "\n")
    print(json.dumps(counts, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
