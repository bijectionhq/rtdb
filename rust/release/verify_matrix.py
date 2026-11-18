#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "conformance"))
from client_results import parse_junit, parse_libtest
from runtime_inventory import read_plan
from evidence import artifact, canonical, read_json, sha256
from matrix_plan import expand, summarize
from lane_artifacts import ASSERTION_COMPARISON_INPUTS, package_matches, verify_bindings, verify_inventory_ledger


def verify_lane_artifacts(directory, receipt):
    runner = receipt["runner"]
    if runner == "cargo-production":
        build = read_json(directory / "production-artifacts.json")
        if build.get("selection") != receipt["selection"] or build.get("configuration") != receipt["id"] or not build.get("binaries"):
            raise ValueError("production build artifact selection differs")
        messages = artifact(directory, build["messages"])
        records = [json.loads(line) for line in messages.read_text().splitlines() if line.strip()]
        process = read_json(artifact(directory, build["process"]))
        if process.get("command") != build["command"] or process.get("exit_code") != 0 or not records or records[-1].get("reason") != "build-finished" or records[-1].get("success") is not True:
            raise ValueError("production artifact has no matching successful build owner")
        for binary in build["binaries"]:
            path = artifact(directory, binary["retained_artifact"])
            if sha256(path) != binary["sha256"] or binary["compiler_artifact"] not in records or binary["compiler_artifact"]["profile"]["test"]:
                raise ValueError("retained production executable differs from its compiler artifact")
    elif runner == "native-tests":
        result = read_json(directory / "native-results.json")
        if not result.get("cases") or any(case.get("outcome") != "passed" for case in result["cases"]):
            raise ValueError("native case receipts are not passing")
        inventory_path = directory / "run/native-inventory.json"
        if result.get("inventory_sha256") != sha256(inventory_path):
            raise ValueError("native case receipts name another target inventory")
        inventory = read_json(inventory_path)
        binaries = result.get("retained_binaries", [])
        if len(binaries) != len(inventory["binaries"]) or not binaries:
            raise ValueError("native executable retention denominator differs")
        observed_cases = []
        for position, (retained_binary, original) in enumerate(zip(binaries, inventory["binaries"])):
            if {key: value for key, value in retained_binary.items() if key != "retained_executable"} != original:
                raise ValueError("native retained executable changed its discovery identity")
            if sha256(artifact(directory, retained_binary["retained_executable"])) != original["binary_sha256"]:
                raise ValueError("native executable bytes differ from discovery")
            selected = [case for case in inventory["tests"] if case["executable"] == original["artifact"]["executable"]]
            if not selected:
                continue
            process = read_json(directory / f"tests-{position:04}.process.json")
            command = process.get("command", [])
            executed_path = str(Path(result["execution_root"]) / retained_binary["retained_executable"]["path"])
            if process.get("exit_code") != 0 or len(command) != 4 or command[0] != executed_path or command[1:] != ["--include-ignored", "--format=pretty", "--color=never"]:
                raise ValueError("native execution does not bind the retained executable and unfiltered selection")
            parsed = parse_libtest((directory / f"tests-{position:04}.stdout.log").read_bytes(), original["artifact"]["target"]["name"])
            if {case["name"] for case in parsed} != {case["name"] for case in selected} or len(parsed) != len(selected):
                raise ValueError("native raw execution differs from discovered target denominator")
            for case in parsed:
                counterpart = next(value for value in selected if value["name"] == case["name"])
                case.update(source={"file": original["artifact"]["target"]["src_path"]}, counterpart=counterpart["counterpart"], configuration=inventory["configuration"])
            observed_cases.extend(parsed)
        if observed_cases != result["cases"]:
            raise ValueError("native summary differs from retained raw assertion execution")
    elif runner == "embedded":
        result = read_json(directory / "embedded-results.json")
        artifact(directory, result["library"])
        cases = [case for reference in result["reports"] for case in parse_junit(artifact(directory, reference).read_bytes())]
        if not cases or cases != result["cases"] or any(case["outcome"] != "passed" for case in cases):
            raise ValueError("embedded summary differs from retained raw assertions")
    elif runner == "gradle":
        result = read_json(directory / "reference-results.json")
        if result.get("selection") != receipt["selection"] or not result.get("cases") or any(case["outcome"].get("status") != "SUCCESSFUL" for case in result["cases"]):
            raise ValueError("original framework results do not satisfy the selected assertions")
        plans = [read_plan(path) for path in (directory / "run").rglob("*.jsonl")]
        if not plans or any(plan["errors"] for plan in plans):
            raise ValueError("original framework event traces are incomplete")
        actual = [case for plan in plans for case in plan["cases"]]
        def assertion(case):
            return {key: value for key, value in case.items() if key != "trace"}
        if [assertion(case) for case in actual] != [assertion(case) for case in result["cases"]]:
            raise ValueError("original framework summary differs from its retained event stream")
    elif runner == "package":
        result = read_json(directory / "run/package.json")
        package_matches(result, receipt)
        if result.get("qualification") != "built_unqualified" or result.get("variant") != receipt["selection"]["variant"] or result.get("platform") != receipt["platform"]:
            raise ValueError("package build receipt differs from selected artifact")
        for reference in result["artifacts"]:
            artifact(directory / "run", reference)
    elif runner == "inventory":
        verify_inventory_ledger(directory)
        result = read_json(directory / "inventory.json")
        if result.get("complete") is not True or result.get("all_assertions_passed") is not True:
            raise ValueError("inventory has missing or failing original assertions")
    elif runner == "assertion-comparison":
        result = read_json(directory / "assertion-comparison.stdout.log")
        if result.get("complete_parity") is not True:
            raise ValueError("full assertion comparison lacks strict parity")
    else:
        fields = {"controls": ("result.json", "controls_verified"), "history": ("result.json", "history_selection_passed"),
            "recovery": ("result.json", "recovery_selection_passed"), "sql-logic": ("result.json", "selection_passed"),
            "clients": ("comparison.json", "client_selection_passed"), "performance": ("comparison.json", "packaged_performance_qualified"),
            "packaged-recovery": ("comparison.json", "packaged_recovery_qualified")}
        if runner not in fields:
            raise ValueError("successful receipt names an unimplemented verification adapter")
        path, field = fields[runner]
        if read_json(directory / "run" / path).get(field) is not True:
            raise ValueError("lane result contradicts its successful receipt")


def verify(matrix, roots, plan_sha256, implementation_revision, reference_revision, matrix_sha256):
    lanes = expand(matrix)
    attempts = {}
    for root in roots:
        root = Path(root).resolve()
        result = read_json(root / "result.json")
        if result.get("plan_sha256") != plan_sha256 or result.get("implementation_revision") != implementation_revision or result.get("reference_revision") != reference_revision:
            raise ValueError("worker evidence uses another plan or source revision")
        if result.get("matrix_sha256") != matrix_sha256:
            raise ValueError("worker evidence names different matrix input bytes")
        if result.get("selection_sha256") != sha256(root / "selection.json") or canonical(read_json(root / "selection.json")) != canonical(lanes):
            raise ValueError("worker selection differs from the full committed matrix")
        inputs_path = root / "inputs.json"
        if result.get("inputs_sha256") != sha256(inputs_path):
            raise ValueError("worker input provenance differs")
        inputs = read_json(inputs_path)
        if inputs.get("implementation_revision") != implementation_revision or inputs.get("reference_revision") != reference_revision:
            raise ValueError("worker input revisions differ from the selected revisions")
        worker = result.get("worker_platform")
        recorded = {}
        for reference in result.get("receipt_artifacts", []):
            path = artifact(root, reference)
            value = read_json(path)
            if value["id"] in recorded:
                raise ValueError("worker duplicated a lane receipt")
            recorded[value["id"]] = (value, path.parent)
        selections = {value["id"]: value for value in lanes}
        def dependency(identity):
            value, directory = recorded[identity]
            selected = selections[identity]
            if value.get("verified") is not True or any(value.get(key) != selected[key] for key in ("id", "runner", "platform", "selection")):
                raise ValueError("selected producer lacks a matching verified receipt")
            return selected, directory
        for reference in result.get("receipt_artifacts", []):
            receipt = read_json(artifact(root, reference))
            if receipt.get("verified") is True:
                if receipt.get("platform") != worker:
                    raise ValueError("worker claims native execution on a different architecture")
                for item in receipt.get("artifacts", []):
                    artifact(root, item)
                closure_file = artifact(root, receipt["closure"])
                files = read_json(closure_file)
                if not files:
                    raise ValueError("successful lane lacks its retained artifact closure")
                if receipt["runner"] in {"inventory", "assertion-comparison"}:
                    required = {"artifact-bindings.json", "input-artifacts.json", "ledger.json"}
                    if receipt["runner"] == "inventory":
                        required.update(("inventory.json", "inventory.process.json"))
                    else:
                        required.update(filename for _, filename in ASSERTION_COMPARISON_INPUTS)
                        required.update(("assertion-comparison.process.json", "assertion-comparison.stdout.log",
                                         "assertion-comparison.stderr.log"))
                    if not required <= {item["path"] for item in files}:
                        raise ValueError("assertion evidence is absent from the retained lane closure")
                process_count = 0
                for item in files:
                    path = artifact(closure_file.parent, item)
                    if path.name.endswith(".process.json") or path.name == "process.json":
                        process = read_json(path)
                        process_count += 1
                        if process.get("completed") is not True or process.get("unexpected_live_descendants"):
                            raise ValueError("successful lane retained an unjoined process")
                if process_count == 0:
                    raise ValueError("successful lane omitted invocation owner receipts")
                lane = selections[receipt["id"]]
                if receipt["runner"] == "sql-logic":
                    verify_bindings(lane, closure_file.parent, root, dependency, retained_paths={item["path"] for item in files})
                else:
                    verify_lane_artifacts(closure_file.parent, receipt)
                    verify_bindings(lane, closure_file.parent, root, dependency)
                attempts.setdefault(receipt["id"], []).append(receipt)
    if any(len(values) != 1 for values in attempts.values()):
        raise ValueError("multiple successful receipts for a lane require an explicit retained selection")
    return summarize(lanes, [values[0] for values in attempts.values()])


def main():
    parser = argparse.ArgumentParser(description="Verify exact native-architecture worker receipts against the complete release matrix.")
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--worker", type=Path, action="append", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--implementation-revision", required=True)
    parser.add_argument("--reference-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = verify(read_json(args.matrix), args.worker, args.plan_sha256, args.implementation_revision, args.reference_revision, sha256(args.matrix))
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    return 0 if report["release_matrix_verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
