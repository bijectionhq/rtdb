"""Decode original client-framework reports without treating matching failures as parity."""

import collections
import json
import re
import xml.etree.ElementTree as ET


def tag(element):
    return element.tag.rsplit("}", 1)[-1]


def xml_document(raw):
    if len(raw) > 64 * 1024 * 1024 or b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
        raise ValueError("client XML report exceeds its bound or declares entities")
    return ET.fromstring(raw)


def unique_cases(cases):
    identities = set()
    for case in cases:
        if not case["name"]:
            raise ValueError("client report contains an unnamed test")
        identity = case_identity(case)
        if identity in identities:
            raise ValueError("client report contains ambiguous duplicate invocation identities")
        identities.add(identity)
    if not cases:
        raise ValueError("client report contains no test cases")
    return cases


def case_identity(case):
    return json.dumps({key: case[key] for key in ("framework", "suite", "name", "source")},
                      sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def parse_junit(raw):
    root = xml_document(raw)
    if tag(root) not in {"testsuites", "testsuite"}:
        raise ValueError("client report has no JUnit suite root")
    cases = []
    for suite in root.iter():
        if tag(suite) not in {"testsuite", "testsuites"}:
            continue
        descendants = [item for item in suite.iter() if tag(item) == "testcase"]
        observed = collections.Counter()
        for case in descendants:
            states = [tag(child) for child in case if tag(child) in {"failure", "error", "skipped"}]
            if len(set(states)) > 1:
                raise ValueError("JUnit case reports conflicting outcomes")
            observed[states[0] if states else "passed"] += 1
        for attribute, count in {"tests": len(descendants), "failures": observed["failure"],
                                 "errors": observed["error"], "skipped": observed["skipped"]}.items():
            if attribute in suite.attrib and int(suite.attrib[attribute]) != count:
                raise ValueError(f"JUnit {attribute} summary disagrees with recorded cases")
        for case in suite:
            if tag(case) != "testcase":
                continue
            children = [child for child in case if tag(child) in {"failure", "error", "skipped"}]
            state = tag(children[0]) if children else "passed"
            cases.append({"framework": "junit", "suite": case.get("classname", suite.get("name", "")), "name": case.get("name", ""),
                          "outcome": {"failure": "failed", "error": "error", "skipped": "skipped"}.get(state, state),
                          "source": {key: case.get(key, suite.get(key)) for key in ("file", "line") if case.get(key, suite.get(key)) is not None},
                          "details": [{"kind": tag(child), "attributes": child.attrib,
                                       "text": "".join(child.itertext())} for child in children]})
    return unique_cases(cases)


def parse_trx(raw):
    root = xml_document(raw)
    if tag(root) != "TestRun":
        raise ValueError("client report has no TRX test-run root")
    definitions = {}
    for definition in root.iter():
        if tag(definition) != "UnitTest":
            continue
        methods = [item for item in definition if tag(item) == "TestMethod"]
        if len(methods) != 1 or definition.get("id") in definitions:
            raise ValueError("TRX has duplicate or incomplete test definitions")
        definitions[definition.get("id")] = methods[0].attrib
    cases = []
    outcomes = collections.Counter()
    for result in root.iter():
        if tag(result) != "UnitTestResult":
            continue
        if result.get("testId") not in definitions:
            raise ValueError("TRX result lacks its source definition")
        method = definitions[result.get("testId")]
        outcome = result.get("outcome", "Unknown")
        outcomes[outcome] += 1
        cases.append({"framework": "trx", "suite": method.get("className", ""), "name": result.get("testName", ""),
                      "outcome": {"Passed": "passed", "Failed": "failed", "NotExecuted": "skipped"}.get(outcome, "error"),
                      "source": {**method, "test_id": result.get("testId")}, "details": {"outcome": outcome,
                      "execution_id": result.get("executionId"), "text": "".join(result.itertext())}})
    counters = [item for item in root.iter() if tag(item) == "Counters"]
    summaries = [item for item in root.iter() if tag(item) == "ResultSummary"]
    if len(counters) != 1 or len(summaries) != 1:
        raise ValueError("TRX has no unique completion summary")
    counter = counters[0]
    if int(counter.get("total", "-1")) != len(cases):
        raise ValueError("TRX total disagrees with recorded cases")
    for field, outcome in [("passed", "Passed"), ("failed", "Failed"), ("notExecuted", "NotExecuted")]:
        if field in counter.attrib and int(counter.attrib[field]) != outcomes[outcome]:
            raise ValueError(f"TRX {field} summary disagrees with recorded cases")
    if summaries[0].get("outcome") not in {"Completed", "Passed", "Failed"}:
        raise ValueError("TRX execution did not complete")
    return unique_cases(cases)


def parse_exunit(raw):
    events = [json.loads(line) for line in raw.splitlines() if line.strip()]
    if not events or events[0].get("kind") != "suite_started" or events[-1].get("kind") != "suite_finished":
        raise ValueError("ExUnit report lacks a complete event interval")
    if [event.get("sequence") for event in events] != list(range(len(events))):
        raise ValueError("ExUnit report has a missing or duplicate event sequence")
    if sum(event["kind"] == "suite_started" for event in events) != 1 or sum(event["kind"] == "suite_finished" for event in events) != 1:
        raise ValueError("ExUnit report contains multiple suite intervals")
    cases, active = [], set()
    for event in events[1:-1]:
        kind = event["kind"]
        if kind == "test_started":
            identity = event["suite"], event["name"]
            if identity in active:
                raise ValueError("ExUnit test started twice without completion")
            active.add(identity)
        elif kind == "test_finished":
            identity = event["suite"], event["name"]
            if event["outcome"] not in {"passed", "failed", "skipped", "excluded", "invalid"}:
                raise ValueError("ExUnit test has an unknown outcome")
            if event["outcome"] in {"passed", "failed"} and identity not in active:
                raise ValueError("ExUnit completed an unstarted executable test")
            active.discard(identity)
            cases.append({"framework": "exunit", **{key: event[key] for key in ("suite", "name", "outcome", "source", "details")}})
        elif kind == "module_failed":
            raise ValueError("ExUnit module fixture failed; inspect the event trace")
        else:
            raise ValueError(f"ExUnit execution was incomplete: {kind}")
    if active or events[-1].get("test_count") != len(cases):
        raise ValueError("ExUnit terminal count differs from completed tests")
    return unique_cases(cases)


RUST_CASE = re.compile(r"^test (.+) \.\.\. (ok|FAILED|ignored)(?:, .*)?$")
RUST_SUMMARY = re.compile(r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in .+$")


def parse_libtest(raw, target):
    cases, summaries, announced = [], [], []
    for line in raw.decode("utf-8", errors="strict").splitlines():
        if match := RUST_CASE.fullmatch(line):
            name, state = match.groups()
            cases.append({"framework": "libtest", "suite": target, "name": name,
                          "outcome": {"ok": "passed", "FAILED": "failed", "ignored": "skipped"}[state],
                          "source": {"file": f"lang/rust/tests/{target}.rs"}, "details": {"line": line}})
        elif match := RUST_SUMMARY.fullmatch(line):
            summaries.append(match.groups())
        elif match := re.fullmatch(r"running (\d+) tests?", line):
            announced.append(int(match.group(1)))
    if len(summaries) != 1 or announced != [len(cases)]:
        raise ValueError("libtest report has no single complete test interval")
    state, passed, failed, skipped, measured, filtered = summaries[0]
    actual = collections.Counter(case["outcome"] for case in cases)
    if [int(passed), int(failed), int(skipped)] != [actual["passed"], actual["failed"], actual["skipped"]]:
        raise ValueError("libtest summary disagrees with recorded cases")
    if int(measured) or int(filtered) or (state == "ok") != (int(failed) == 0):
        raise ValueError("libtest selection was filtered, measured, or has an inconsistent outcome")
    return unique_cases(cases)


def compare_clients(reference, native, required_lanes):
    def indexed(lanes):
        result = {}
        for lane in lanes:
            for case in lane["cases"]:
                identity = (lane["lane"], case_identity(case))
                if identity in result:
                    raise ValueError("client inventory contains a duplicate invocation identity")
                result[identity] = case
        return result
    left, right = indexed(reference), indexed(native)
    missing, unverified, passed = [], [], []
    for identity in sorted(left.keys() | right.keys()):
        if identity not in left or identity not in right:
            missing.append({"identity": identity, "missing": "reference" if identity not in left else "native"})
        elif left[identity]["outcome"] == right[identity]["outcome"] == "passed":
            passed.append(identity)
        else:
            unverified.append({"identity": identity, "reference": left[identity]["outcome"], "native": right[identity]["outcome"]})
    selection = set(required_lanes)
    selection_complete = bool(selection) and all(len(lanes) == len(selection) and {lane["lane"] for lane in lanes} == selection
                                               for lanes in (reference, native))
    valid_processes = selection_complete and all(lane["cases"] and lane["process"]["completed"] and lane["process"].get("exit_code") == 0
                            and not lane.get("report_error") for lane in reference + native)
    return {"schema_version": 1, "passing_original_client_cases": passed, "missing": missing, "unverified": unverified,
            "selection_complete": selection_complete,
            "client_selection_passed": bool(passed) and valid_processes and not missing and not unverified,
            "assertion_ledger_reconciled": False, "complete_parity": False}
