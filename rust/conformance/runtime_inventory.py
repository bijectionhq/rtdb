#!/usr/bin/env python3
"""Reconcile recorded runtime descriptors with the pinned source inventory without executing tests."""

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path


def file_digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_plan(path):
    data = path.read_bytes()
    records = [json.loads(line) for line in data.decode("utf-8").splitlines() if line.strip()]
    return inspect_plan(path, records, hashlib.sha256(data).hexdigest())


def inspect_plan(path, records, trace_digest):
    errors = []
    if not records or records[0].get("event") != "plan_started" or records[-1].get("event") != "plan_finished":
        errors.append("plan is missing its start or successful recording completion")
    if [record.get("sequence") for record in records] != list(range(len(records))):
        errors.append("event sequence is missing, duplicated, or out of order")
    descriptors, outcomes, started = {}, {}, set()
    configuration = records[0] if records else {}
    for record in records:
        event = record.get("event")
        descriptor = record.get("descriptor")
        if descriptor is None:
            continue
        unique_id = descriptor["unique_id"]
        previous = descriptors.get(unique_id)
        if previous is not None and previous != descriptor:
            errors.append(f"descriptor changed: {unique_id}")
        if event in ("discovered", "dynamic_registered"):
            if unique_id in descriptors:
                errors.append(f"duplicate descriptor registration: {unique_id}")
            descriptors[unique_id] = descriptor
        elif unique_id not in descriptors:
            errors.append(f"execution event has no descriptor registration: {unique_id}")
            descriptors[unique_id] = descriptor
        if event == "execution_started":
            if unique_id in started:
                errors.append(f"execution started twice: {unique_id}")
            started.add(unique_id)
        elif event in ("execution_skipped", "execution_finished"):
            if unique_id in outcomes:
                errors.append(f"execution has multiple outcomes: {unique_id}")
            if event == "execution_finished" and unique_id not in started:
                errors.append(f"execution finished without starting: {unique_id}")
            outcomes[unique_id] = {key: value for key, value in record.items() if key != "descriptor"}
    cases = []
    for unique_id, descriptor in sorted(descriptors.items()):
        if not descriptor["is_test"]:
            continue
        ancestor = descriptor
        inherited = None
        seen = set()
        while ancestor.get("parent_id") and ancestor["parent_id"] not in seen:
            parent = ancestor["parent_id"]
            seen.add(parent)
            outcome = outcomes.get(parent)
            if outcome and outcome["event"] == "execution_skipped":
                inherited = {"event": "ancestor_skipped", "ancestor": parent, "reason": outcome["reason"]}
                break
            ancestor = descriptors.get(parent, {})
        outcome = outcomes.get(unique_id, inherited)
        if outcome is None:
            errors.append(f"discovered test has no execution outcome: {unique_id}")
            outcome = {"event": "missing_outcome"}
        cases.append({"project": configuration.get("project"), "task": configuration.get("task"),
                      "descriptor": descriptor, "outcome": outcome, "trace": str(path), "trace_sha256": trace_digest})
    return {"path": str(path), "sha256": trace_digest, "errors": errors, "cases": cases,
            "project": configuration.get("project"), "task": configuration.get("task")}


def source_name(descriptor):
    source = descriptor.get("source", {})
    if source.get("class_name") and source.get("method_name"):
        return source["class_name"] + "/" + source["method_name"]
    return descriptor.get("clojure_var")


CLASSIFICATION_KINDS = ("engine-internal", "jvm-api-boundary", "jvm-internal", "environment-unavailable")
PASSING_NATIVE_OUTCOMES = ("pass", "pass-wire", "SUCCESSFUL")


def case_key(identity):
    return tuple(identity.get(key) for key in ("reference_revision", "source", "qualified_name", "parameter_fixture", "configuration"))


def invocation_key(value):
    return value.get("project"), value.get("task"), value.get("unique_id")


def reconcile(source, ledger, runtime, native, aliases, invocations=(), external=None, external_root=None):
    errors = []
    external_result = {"groups": set(), "cases": [], "resolved_files": set(), "all_passed": True}
    if external is not None:
        from external_inventory import reconcile_external
        try:
            external_result = reconcile_external(external, external_root, source, ledger)
        except (ValueError, KeyError, TypeError, OSError, StopIteration) as error:
            errors.append("external evidence: " + str(error))
    if ledger["inventory_revision"] != source["reference"]:
        errors.append("ledger and source inventory belong to different reference revisions")
    selections = []
    plans = [read_plan(path) for path in sorted(runtime.rglob("*.jsonl"))]
    for path in sorted(runtime.rglob("selection.json")):
        selection = json.loads(path.read_bytes())
        selection["evidence_sha256"] = file_digest(path)
        selections.append(selection)
        matching = [plan for plan in plans if plan["task"] == selection["task"]]
        if not matching or not any(plan["cases"] for plan in matching):
            errors.append(f"selected lane has no recorded cases: {selection['task']}")
    if not selections:
        errors.append("no Gradle task selections were recorded")
    requested_path = runtime / "requested-lanes.json"
    if requested_path.is_file():
        requested = json.loads(requested_path.read_bytes())["lanes"]
        recorded_tasks = {selection["task"] for selection in selections}
        for lane in requested:
            if lane["task"] not in recorded_tasks:
                errors.append(f"requested lane did not start recording: {lane['task']}")
        if not requested:
            errors.append("requested-lane manifest contains no test tasks")
    else:
        errors.append("requested-lane manifest is missing")
    for plan in plans:
        errors.extend(f"{plan['path']}: {error}" for error in plan["errors"])
    discovered, discovery_projects = [], set()
    for path in sorted(runtime.glob("*-discovery.json")):
        value = json.loads(path.read_bytes())
        if value.get("kind") != "discovery_only":
            errors.append(f"unexpected discovery artifact: {path}")
            continue
        discovery_projects.add(value["project"])
        trace_digest = file_digest(path)
        discovered.extend({"project": value["project"], "descriptor": item, "trace": str(path),
                           "trace_sha256": trace_digest} for item in value["descriptors"])
    for project in {selection["project"] for selection in selections} - discovery_projects:
        errors.append(f"project has no unfiltered discovery: {project}")
    runtime_cases = [case for plan in plans for case in plan["cases"]]
    runtime_by_name, discovered_by_name = defaultdict(list), defaultdict(list)
    for case in runtime_cases:
        name = source_name(case["descriptor"])
        name = aliases.get(case["descriptor"]["unique_id"], name)
        if name:
            runtime_by_name[name].append(case)
    for case in discovered:
        name = source_name(case["descriptor"])
        name = aliases.get(case["descriptor"]["unique_id"], name)
        if name:
            discovered_by_name[name].append(case)
    mappings = defaultdict(list)
    mappings_by_identity = {}
    classified = {}
    for mapping in ledger["cases"]:
        identity = mapping["identity"]
        classification = mapping.get("classification")
        if classification is not None:
            if classification.get("kind") not in CLASSIFICATION_KINDS or not str(classification.get("reason") or "").strip():
                errors.append(f"ledger case classification lacks a known kind or a reason: {identity}")
            elif mapping.get("counterparts"):
                errors.append(f"classified ledger case also names native counterparts: {identity}")
            else:
                classified[case_key(identity)] = classification["kind"]
        elif not mapping.get("counterparts"):
            errors.append(f"ledger case has neither native counterparts nor a classification: {identity}")
        mappings[(identity["source"], identity["qualified_name"])].append(mapping)
        key = case_key(identity)
        if key in mappings_by_identity:
            errors.append(f"duplicate ledger case identity: {identity}")
        mappings_by_identity[key] = mapping
    native_tests = native.get("tests", [])
    native_ids = {(case["counterpart"], case.get("configuration")) for case in native_tests}
    native_outcomes = {(case["counterpart"], case.get("configuration")): case.get("outcome") for case in native_tests}
    counterpart_results = {}
    if len(native_ids) != len(native_tests):
        errors.append("native inventory contains duplicate counterpart/configuration identities")
    if not native.get("complete", False) or not native_ids:
        errors.append("native test executable discovery is incomplete")
    bindings = {}
    for binding in invocations:
        key = invocation_key(binding)
        if any(not isinstance(part, str) or not part.strip() for part in key):
            errors.append(f"invocation binding lacks a project, task, or unique ID: {binding}")
            continue
        if key in bindings:
            errors.append(f"duplicate invocation binding: {key}")
        bindings[key] = binding
    observed_invocations, bound_groups = set(), set(external_result["groups"])
    missing_native_by_group = defaultdict(set)
    for case in runtime_cases:
        key = invocation_key({**case, "unique_id": case["descriptor"]["unique_id"]})
        if key in observed_invocations:
            errors.append(f"runtime invocation recorded more than once: {key}")
        observed_invocations.add(key)
        binding = bindings.get(key)
        if binding is None:
            errors.append(f"runtime invocation has no explicit fixture/configuration binding: {key}")
            continue
        identity = binding.get("identity", {})
        group = case_key(identity)
        mapping = mappings_by_identity.get(group)
        if mapping is None or mapping["identity"] != identity:
            errors.append(f"invocation binding has no exact ledger case: {key}")
            continue
        name = aliases.get(case["descriptor"]["unique_id"], source_name(case["descriptor"]))
        candidates = [candidate for candidate in source["candidates"]
                      if candidate["identity"]["source"] == identity["source"]
                      and candidate["identity"]["name"] == name
                      and (name.replace("/", ".", 1) if candidate["kind"] == "junit_template_candidate" else name) == identity["qualified_name"]]
        if len({(candidate["identity"]["source"], candidate["identity"]["name"]) for candidate in candidates}) != 1:
            errors.append(f"invocation binding does not identify one source candidate: {key}")
            continue
        configuration = binding.get("native_configuration")
        if not isinstance(configuration, str) or not configuration.strip():
            errors.append(f"invocation binding has no native configuration: {key}")
            continue
        missing = [counterpart for counterpart in mapping["counterparts"] if (counterpart, configuration) not in native_ids]
        if missing:
            errors.append(f"invocation native counterparts missing in configuration {configuration}: {key}: {missing}")
            missing_native_by_group[group].update(missing)
        for counterpart in mapping["counterparts"]:
            outcome = native_outcomes.get((counterpart, configuration))
            if (counterpart, configuration) in native_ids and outcome is not None:
                counterpart_results[(counterpart, configuration)] = outcome
        bound_groups.add(group)
        case["binding"] = binding
    for key in bindings.keys() - observed_invocations:
        errors.append(f"invocation binding has no recorded runtime case: {key}")
    for group in mappings_by_identity.keys() - bound_groups - classified.keys():
        errors.append(f"ledger case has no bound runtime invocation: {group}")
    failing_counterparts = sorted(f"{counterpart} ({outcome})" for (counterpart, _), outcome in counterpart_results.items()
                                  if outcome not in PASSING_NATIVE_OUTCOMES)
    errors.extend(f"native counterpart did not pass: {failure}" for failure in failing_counterparts)
    candidate_results, matched_names = [], set()
    for candidate in source["candidates"]:
        identity = candidate["identity"]
        name = identity["name"]
        qualified_name = name.replace("/", ".", 1) if candidate["kind"] == "junit_template_candidate" else name
        matches = mappings.get((identity["source"], qualified_name), [])
        external_cases = [case for case in external_result["cases"] if case["source_candidate"] == candidate["id"]]
        if external_cases:
            candidate_results.append({"source_candidate": candidate["id"], "identity": identity,
                "mapping": "mapped", "mapped_groups": [case["binding"]["identity"] for case in external_cases],
                "missing_counterparts": [], "runtime_cases": external_cases, "discovered_templates": [], "runtime_status": "observed"})
            continue
        execution = runtime_by_name.get(name, [])
        discovery = discovered_by_name.get(name, [])
        missing_counterparts = sorted({counterpart for mapping in matches for counterpart in missing_native_by_group[case_key(mapping["identity"])]})
        if matches and all(case_key(mapping["identity"]) in classified for mapping in matches):
            status = "classified"
        else:
            status = "mapped" if matches and not missing_counterparts else "unmapped"
        if execution or discovery:
            matched_names.add(name)
        candidate_results.append({"source_candidate": candidate["id"], "identity": identity,
                                  "mapping": status, "mapped_groups": [mapping["identity"] for mapping in matches],
                                  "missing_counterparts": missing_counterparts,
                                  "runtime_cases": execution, "discovered_templates": discovery,
                                  "runtime_status": "observed" if execution else "discovered_not_executed" if discovery else "not_discovered"})
    unmapped_runtime = [case for name, cases in runtime_by_name.items() if name not in matched_names for case in cases]
    unmapped_runtime.extend(case for case in runtime_cases
                            if not source_name(case["descriptor"]) and case["descriptor"]["unique_id"] not in aliases)
    mapped_source_names = {candidate["identity"]["name"] for candidate in source["candidates"]
                           if mappings.get((candidate["identity"]["source"], candidate["identity"]["name"].replace("/", ".", 1)
                               if candidate["kind"] == "junit_template_candidate" else candidate["identity"]["name"]))}
    unmapped_discovery = [case for case in discovered
                         if (case["descriptor"]["is_test"] or source_name(case["descriptor"]) is not None)
                         and aliases.get(case["descriptor"]["unique_id"], source_name(case["descriptor"])) not in mapped_source_names]
    external = [file for file in source["files"] if (file["lane"] in ("sql_logic", "language_client")
                or file["path"].startswith("src/test/resources/xtdb/sql/logic_test/") and file["path"].endswith(".test"))
                and file["path"] not in external_result["resolved_files"]]
    unreconciled = [value for value in candidate_results
                    if value["mapping"] != "classified" and (value["mapping"] != "mapped" or value["runtime_status"] != "observed")]
    complete = not errors and not unreconciled and not unmapped_runtime and not unmapped_discovery and not external and bool(candidate_results)
    assertions_passed = complete and external_result["all_passed"] and not failing_counterparts and all(case["outcome"].get("event") == "execution_finished"
        and case["outcome"].get("status") == "SUCCESSFUL" for case in runtime_cases)
    classified_by_kind = defaultdict(int)
    for kind in classified.values():
        classified_by_kind[kind] += 1
    return {"schema_version": 1, "reference_revision": source["reference"], "complete": complete,
            "all_assertions_passed": assertions_passed, "external_cases": external_result["cases"],
            "assertion_denominator": len(ledger["cases"]) - len(classified) if complete else None,
            "classified_case_count": len(classified), "classified_by_kind": dict(sorted(classified_by_kind.items())),
            "native_counterparts_passing": sum(1 for outcome in counterpart_results.values() if outcome in PASSING_NATIVE_OUTCOMES),
            "native_counterparts_failing": failing_counterparts,
            "errors": errors, "selections": selections, "candidates": candidate_results,
            "unmapped_runtime": unmapped_runtime,
            "unmapped_discovery": unmapped_discovery,
            "external_lanes_requiring_receipts": external,
            "source_only_candidate_count": len(source["candidates"]), "recorded_runtime_case_count": len(runtime_cases)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--aliases", type=Path)
    parser.add_argument("--invocations", type=Path)
    parser.add_argument("--external", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    read = lambda path: json.loads(path.read_bytes())
    report = reconcile(read(args.source), read(args.ledger), args.runtime, read(args.native), read(args.aliases) if args.aliases else {},
                       read(args.invocations) if args.invocations else [], read(args.external) if args.external else None,
                       args.external.resolve().parent if args.external else None)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
