#!/usr/bin/env python3
"""Discover cases in already-built Cargo test executables and preserve their artifact digests."""

import argparse
import json
import tomllib
from pathlib import Path

from run_controls import REPOSITORY, digest, git, run_process, write_json


def artifacts(messages):
    records = [json.loads(line) for line in messages.splitlines() if line.strip()]
    if not records or records[-1].get("reason") != "build-finished" or records[-1].get("success") is not True:
        raise ValueError("Cargo messages have no terminal successful build-finished record")
    if sum(record.get("reason") == "build-finished" for record in records) != 1:
        raise ValueError("Cargo messages contain multiple build completions")
    seen, result = {}, []
    for value in records:
        if value.get("reason") != "compiler-artifact" or not value.get("executable") or not value["profile"]["test"]:
            continue
        identity = (value["package_id"], value["target"]["name"], value["executable"])
        if identity in seen:
            if artifact_key(seen[identity]) != artifact_key(value):
                raise ValueError("repeated test artifact changed its target or feature selection")
            continue
        seen[identity] = value
        result.append(value)
    if not result:
        raise ValueError("Cargo build produced no selected test executables")
    return result


def artifact_key(value):
    return value["package_id"], value["target"]["name"], tuple(sorted(value["target"]["kind"])), tuple(sorted(value["features"]))


def validate_selection(selection, observed, workspace, revision, configuration):
    if selection.get("schema_version") != 1:
        raise ValueError("unsupported native build-selection schema")
    if Path(selection.get("workspace", "")).resolve() != workspace.resolve():
        raise ValueError("native build selection names a different workspace")
    if selection.get("implementation_revision") != revision or selection.get("configuration") != configuration:
        raise ValueError("native build selection revision or configuration differs from discovery")
    command = selection.get("command")
    if not isinstance(command, list) or not command or any(not isinstance(value, str) or not value for value in command):
        raise ValueError("native build selection must record the build command argv")
    targets = selection.get("targets", [])
    if not targets:
        raise ValueError("native build selection has no expected test targets")
    expected = {artifact_key(value) for value in targets}
    actual = {artifact_key(value) for value in observed}
    if len(expected) != len(targets) or len(actual) != len(observed):
        raise ValueError("native build selection contains duplicate package/target/feature identities")
    if actual != expected:
        raise ValueError(f"native build artifacts differ from the declared selection; missing={expected - actual}, extra={actual - expected}")


def parse_listing(text, package, target, kind):
    tests = []
    for line in text.splitlines():
        if not line:
            continue
        name, separator, case_kind = line.rpartition(": ")
        if separator and case_kind in ("test", "benchmark"):
            prefix = package + ("::" + target if not set(kind).intersection({"lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"}) else "")
            tests.append({"counterpart": prefix + "::" + name, "name": name, "kind": case_kind})
        elif line.endswith(" tests, 0 benchmarks") or (" tests, " in line and line.endswith(" benchmarks")):
            continue
        else:
            raise ValueError(f"unrecognized native test listing line: {line!r}")
    return tests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--cargo-messages", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--configuration", required=True)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    state = json.loads((REPOSITORY / "rust/implementation-status.json").read_bytes())
    if not args.phase_two or not state["phase_2_authorized_by_completeness_gate"]:
        raise ValueError("native test discovery belongs to Phase 2 after the source completeness gate")
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    if git(REPOSITORY, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("native test discovery requires a clean implementation checkout")
    selection_digest, messages_digest = digest(args.selection), digest(args.cargo_messages)
    selection = json.loads(args.selection.read_bytes())
    observed = artifacts(args.cargo_messages.read_text())
    workspace = REPOSITORY / "rust"
    validate_selection(selection, observed, workspace, revision, args.configuration)
    args.output.mkdir(parents=True, exist_ok=False)
    tests, binaries, failures = [], [], []
    for index, artifact in enumerate(observed):
        manifest = Path(artifact["manifest_path"]).resolve()
        if not manifest.is_relative_to(workspace.resolve()):
            raise ValueError("selected native test package is outside the declared workspace")
        package = tomllib.loads(manifest.read_text())["package"]["name"]
        executable = Path(artifact["executable"])
        output = args.output / f"{index:04d}.stdout.log"
        log = args.output / f"{index:04d}.stderr.log"
        binary_digest = digest(executable)
        process = run_process([executable, "--list", "--format", "terse"], REPOSITORY, output, log, args.timeout)
        write_json(args.output / f"{index:04d}.process.json", process)
        record = {"artifact": artifact, "binary_sha256": binary_digest, "process": process,
                  "stdout_sha256": digest(output), "stderr_sha256": digest(log)}
        binaries.append(record)
        if not process["completed"] or process["exit_code"] != 0 or digest(executable) != binary_digest:
            failures.append({"executable": str(executable), "reason": "listing failed or binary changed"})
            continue
        target = artifact["target"]
        for case in parse_listing(output.read_text(), package, target["name"], target["kind"]):
            case.update(binary_sha256=binary_digest, configuration=args.configuration,
                        executable=str(executable), discovery_only=True)
            tests.append(case)
    if digest(args.selection) != selection_digest or digest(args.cargo_messages) != messages_digest:
        failures.append({"reason": "build selection or Cargo messages changed during native discovery"})
    if git(REPOSITORY, "rev-parse", "HEAD") != revision or git(REPOSITORY, "status", "--porcelain", "--untracked-files=all"):
        failures.append({"reason": "implementation checkout changed during native discovery"})
    report = {"schema_version": 1, "complete": bool(binaries) and bool(tests) and not failures,
              "configuration": args.configuration, "cargo_messages_sha256": messages_digest,
              "selection": selection, "selection_sha256": selection_digest, "implementation_revision": revision,
              "tests": tests, "binaries": binaries, "failures": failures}
    write_json(args.output / "native-inventory.json", report)
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
