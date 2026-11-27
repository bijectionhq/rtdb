#!/usr/bin/env python3
"""Run two pinned JVM tests and record the independent native retention and cache contracts."""

import argparse
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from run_controls import HERE, REPOSITORY, digest, git, run_process, write_json
from source_gate import require_phase_two


PIN = "0b9fa440f23d2165250d215209f7fa9bd7c26a6f"
CASES = {
    "offloaded-retention": {
        "task": ":test",
        "source": "src/test/kotlin/xtdb/catalog/BlockGarbageCollectorTest.kt",
        "blob": "9a574697a2df10c013fe13bcd8eb3c49d0afcafd",
        "class": "xtdb.catalog.BlockGarbageCollectorTest",
        "method": "garbageCollectBlocks collects offloaded payloads once their block ages out",
        "assertion_lines": [135, 136],
    },
    "pinning-cache": {
        "task": ":xtdb-core:test",
        "source": "core/src/test/kotlin/xtdb/cache/PinningCacheTest.kt",
        "blob": "1ee5dda844cbf50be4858e75b900b4a10c280f54",
        "class": "xtdb.cache.PinningCacheTest",
        "method": "releasing the last pin evicts an over-size cache without the common pool",
        "assertion_lines": [64, 65],
    },
}


def clean_revision(checkout):
    if git(checkout, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError(f"expected a clean checkout at {checkout}")
    return git(checkout, "rev-parse", "HEAD")


def verify_reference(checkout):
    if (HERE / "upstream-commit.txt").read_text().strip() != PIN or clean_revision(checkout) != PIN:
        raise ValueError(f"the original JVM tests require the pinned XTDB checkout {PIN}")
    for case in CASES.values():
        source = case["source"]
        if (git(checkout, "rev-parse", f"{PIN}:{source}") != case["blob"]
                or git(checkout, "hash-object", "--no-filters", source) != case["blob"]):
            raise ValueError(f"reference source differs from its pinned blob: {source}")


def junit_outcome(directory, case):
    reports = list(directory.glob("TEST-*.xml"))
    if len(reports) != 1:
        raise ValueError("expected one fresh Gradle JUnit XML report")
    suite = ET.parse(reports[0]).getroot()
    tests = suite.findall("testcase")
    if suite.tag != "testsuite" or suite.get("tests") != "1" or len(tests) != 1:
        raise ValueError("expected one executed JUnit test case")
    test = tests[0]
    name = test.get("name", "")
    method = case["method"]
    if test.get("classname") != case["class"] or not (
        name == method or (name.startswith(method + "(") and name.endswith(")"))
    ):
        raise ValueError(f"JUnit reported a different case: {test.get('classname')}/{name}")
    if test.find("skipped") is not None or suite.get("skipped") != "0":
        raise ValueError(f"JUnit skipped {name}")
    failed = any(test.find(tag) is not None for tag in ("failure", "error"))
    if int(suite.get("failures", "0")) + int(suite.get("errors", "0")) != int(failed):
        raise ValueError("JUnit counters disagree with the test case outcome")
    return {
        "status": "failed" if failed else "passed",
        "name": name,
        "report": reports[0].name,
        "report_sha256": digest(reports[0]),
    }


def run_reference(checkout, output, case, timeout):
    output.mkdir()
    xml = output / "junit"
    command = [
        checkout / "gradlew", "--no-daemon", "--no-build-cache",
        "--project-cache-dir", output / "gradle-cache",
        "-I", HERE / "reference-test-contracts.init.gradle",
        f"-Prtdb.referenceContractsTask={case['task']}",
        f"-Prtdb.referenceContractsReport={xml}",
        case["task"], "--tests", f"{case['class']}.{case['method']}",
    ]
    process = run_process(command, checkout, output / "stdout.log", output / "stderr.log", timeout)
    result = {"selection": case, "process": process}
    try:
        outcome = junit_outcome(xml, case)
        if not process["completed"] or (process["exit_code"] == 0) != (outcome["status"] == "passed"):
            raise ValueError("Gradle did not complete with the reported JUnit outcome")
        return {**result, **outcome}
    except (ET.ParseError, OSError, ValueError) as error:
        return {**result, "status": "unavailable", "detail": str(error)}


def native_observations(stdout, revision):
    observations = {}
    for line in stdout.read_text(encoding="utf-8").splitlines():
        if not line.startswith("VE-11 "):
            continue
        record = json.loads(line.removeprefix("VE-11 "))
        case = record["case"]
        if case in observations or case not in {"offloaded-retention", "disk-cache", "memory-cache"}:
            raise ValueError(f"unexpected or repeated native case {case}")
        if record["revision"] != revision or record["source_dirty"] is not False:
            raise ValueError("native observations came from another revision or a dirty build")
        observations[case] = record["observation"]
    return observations


def compare(reference, native):
    comparisons = {}
    if reference["offloaded-retention"]["status"] == "passed" and "offloaded-retention" in native:
        retention = native["offloaded-retention"]
        keys = retention["published_payloads"]
        remaining = retention["remaining_payloads"]
        comparisons["offloaded-retention"] = {
            "only_young_payload_remains": {
                "upstream": True, "native": remaining == [keys["9"]],
            },
            "aged_payload_absent": {"upstream": True, "native": keys["6"] not in remaining},
        }
    if reference["pinning-cache"]["status"] == "passed":
        if "disk-cache" in native:
            disk = native["disk-cache"]
            comparisons["disk-cache-final-keys"] = {
                "retained": {"upstream": ["b", "c"], "native": disk["retained"]},
                "evicted": {"upstream": ["a"], "native": disk["evicted"]},
            }
        if "memory-cache" in native:
            comparisons["memory-cache-admission"] = {
                "admitted_three_while_pinned": {
                    "upstream": True, "native": native["memory-cache"]["admitted_three_while_pinned"],
                },
            }
    return {
        case: {
            name: {**observed, "matches": observed["upstream"] == observed["native"]}
            for name, observed in fields.items()
        }
        for case, fields in comparisons.items()
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    args = parser.parse_args()
    try:
        require_phase_two(REPOSITORY, args.phase_two)
        reference = args.reference.resolve(strict=True)
        output = args.output.resolve()
        if args.timeout_seconds <= 0 or any(output.is_relative_to(tree) for tree in (reference, REPOSITORY)):
            raise ValueError("use a positive timeout and an output directory outside both checkouts")
        verify_reference(reference)
        revision = clean_revision(REPOSITORY)
        output.mkdir()

        results = {
            name: run_reference(reference, output / name, case, args.timeout_seconds)
            for name, case in CASES.items()
        }
        command = ["cargo", "test", "--locked", "-p", "rtdb-conformance",
                   "--test", "reference_test_contracts", "--", "--nocapture", "--test-threads=1"]
        stdout = output / "native.stdout.log"
        process = run_process(command, REPOSITORY / "rust", stdout, output / "native.stderr.log",
                              args.timeout_seconds)
        write_json(output / "process.json", {"reference": results, "native": process})
        observations = native_observations(stdout, revision)
        verify_reference(reference)
        if clean_revision(REPOSITORY) != revision:
            raise ValueError("implementation checkout changed during the run")

        native_passed = process["completed"] and process["exit_code"] == 0
        native_complete = set(observations) == {"offloaded-retention", "disk-cache", "memory-cache"}
        compared = compare(results, observations)
        strict_match = (
            all(item["matches"] for fields in compared.values() for item in fields.values())
            if native_passed and native_complete and len(compared) == 3 else None
        )
        write_json(output / "result.json", {
            "schema_version": 1,
            "reference_revision": PIN,
            "native_revision": revision,
            "reference": results,
            "native_assertions_passed": native_passed and native_complete,
            "native_observations": observations,
            "comparisons": compared,
            "strict_match": strict_match,
        })
        if not process["completed"] or not native_complete or any(
            item["status"] == "unavailable" for item in results.values()
        ):
            return 2
        return int(not native_passed or any(item["status"] == "failed" for item in results.values()))
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
