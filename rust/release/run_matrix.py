#!/usr/bin/env python3

import argparse
import json
import os
import platform
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFORMANCE = HERE.parent / "conformance"
sys.path.insert(0, str(CONFORMANCE))
from source_gate import require_phase_two
from run_controls import REFERENCE, REPOSITORY, ProcessCancelled, digest, git, owned_cancellation, run_process, write_json
from client_results import parse_libtest
from runtime_inventory import read_plan as read_junit_plan
from evidence import artifact, closure, read_json, retained
from matrix_plan import expand, summarize, validate_bindings, validate_workflows
from runtime_bundle import merge_runtime
from lane_artifacts import bind_inputs, package_matches


def host_platform():
    architecture = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine())
    return "linux/" + architecture if platform.system() == "Linux" and architecture else "unsupported"


class Run:
    def __init__(self, args, plan, lanes, bindings):
        self.args, self.plan, self.lanes, self.bindings = args, plan, lanes, bindings
        self.completed = {}
        self.directories = {lane["id"]: args.output / f"lane-{index:05}" for index, lane in enumerate(lanes)}

    def input(self, lane, reference):
        if "from_lane" in reference:
            source = reference["from_lane"]
            if source not in lane["dependencies"] or self.completed.get(source, {}).get("verified") is not True:
                raise ValueError("input requires a verified declared predecessor")
            relative = Path(reference["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("predecessor artifact escapes its owner")
            path = self.directories[source] / relative
            cursor = self.directories[source]
            for part in relative.parts:
                cursor /= part
                if cursor.is_symlink():
                    raise ValueError("predecessor input traverses a symbolic link")
            if path.is_symlink() or not path.exists():
                raise ValueError("predecessor artifact is missing or redirected")
            return path
        return artifact(self.args.plan.parent, reference)

    def command(self, directory, name, argv, cwd=REPOSITORY, expected=(0,)):
        output, log = directory / (name + ".stdout.log"), directory / (name + ".stderr.log")
        process = run_process(argv, cwd, output, log, self.args.timeout)
        write_json(directory / (name + ".process.json"), process)
        if process.get("completed") is not True or process.get("unexpected_live_descendants") or process.get("exit_code") not in expected:
            raise ValueError("lane command failed or did not join: " + name)
        return output

    def invoke(self, lane, binding, directory):
        runner, selection, inputs = lane["runner"], lane["selection"], binding["inputs"]
        source = lambda key: self.input(lane, inputs[key])
        def dependency(identity):
            if self.completed.get(identity, {}).get("verified") is not True:
                raise ValueError("artifact producer has no verified receipt")
            return next(value for value in self.lanes if value["id"] == identity), self.directories[identity]
        write_json(directory / "artifact-bindings.json", bind_inputs(lane, directory, self.args.output, source, dependency))
        protected = []
        for key, reference in inputs.items():
            if isinstance(reference, dict) and "path" in reference:
                path = self.input(lane, reference)
                if path.is_dir():
                    protected.extend({"input": key, "root": str(path), **item} for item in closure(path))
                else:
                    protected.append({"input": key, "root": str(path.parent), **retained(path.parent, path)})
        write_json(directory / "input-artifacts.json", protected)
        output = directory / "run"
        common = ["--phase-two", "--reference", str(self.args.reference), "--output", str(output)]
        if runner == "package":
            argv = [sys.executable, HERE / "build.py", *common, "--engine", selection["engine"], "--variant", selection["variant"],
                    "--platform", lane["platform"], "--builder-image", inputs["builder_image"], "--runtime-image", inputs["runtime_image"]]
            self.command(directory, "package", argv)
            record = read_json(output / "package.json")
            package_matches(record, lane)
            if record.get("qualification") != "built_unqualified" or record.get("platform") != lane["platform"] or record.get("variant") != selection["variant"]:
                raise ValueError("package receipt differs from selected build")
            return [output / "package.json"]
        if runner in {"cargo-production", "native-tests"}:
            features = selection["features"]
            command = ["cargo", "build" if runner == "cargo-production" else "test", "--manifest-path", str(REPOSITORY / "rust/Cargo.toml"),
                       "--locked", "--workspace", "--all-targets", "--no-default-features", "--message-format=json"]
            if runner == "native-tests":
                command.append("--no-run")
            if features:
                command += ["--features", ",".join(features)]
            messages = self.command(directory, "cargo", command)
            lines = [json.loads(line) for line in messages.read_text().splitlines() if line.strip()]
            if not lines or lines[-1].get("reason") != "build-finished" or lines[-1].get("success") is not True or sum(line.get("reason") == "build-finished" for line in lines) != 1:
                raise ValueError("Cargo lacks one successful terminal build receipt")
            if runner == "cargo-production":
                binaries = []
                binaries_directory = directory / "binaries"
                binaries_directory.mkdir()
                for record in lines:
                    if record.get("reason") == "compiler-artifact" and record.get("executable") and not record["profile"]["test"]:
                        path = Path(record["executable"])
                        target = binaries_directory / (str(len(binaries)) + "-" + path.name)
                        before = digest(path)
                        shutil.copy2(path, target)
                        if digest(target) != before or digest(path) != before:
                            raise ValueError("production executable changed while retaining its artifact")
                        binaries.append({"compiler_artifact": record, "path": str(target), "sha256": before, "bytes": target.stat().st_size,
                                         "retained_artifact": retained(directory, target)})
                if not binaries:
                    raise ValueError("production build emitted no executable artifacts")
                write_json(directory / "production-artifacts.json", {"configuration": lane["id"], "selection": selection,
                    "implementation_revision": git(REPOSITORY, "rev-parse", "HEAD"), "command": command,
                    "process": retained(directory, directory / "cargo.process.json"), "messages": retained(directory, messages), "binaries": binaries})
                return [directory / "production-artifacts.json", messages]
            selected = source("native_build_selection")
            expected = read_json(selected)
            if expected.get("command") != command:
                raise ValueError("native target selection does not describe the executed build command")
            configuration = lane["id"]
            self.command(directory, "list-native", [sys.executable, CONFORMANCE / "list_native.py", "--phase-two", "--cargo-messages", messages,
                "--selection", selected, "--configuration", configuration, "--output", output])
            inventory = read_json(output / "native-inventory.json")
            if inventory.get("complete") is not True:
                raise ValueError("native executable inventory is incomplete")
            cases = []
            binaries_directory = directory / "binaries"
            binaries_directory.mkdir()
            for index, binary in enumerate(inventory["binaries"]):
                executable = Path(binary["artifact"]["executable"])
                retained_executable = binaries_directory / (str(index) + "-" + executable.name)
                shutil.copy2(executable, retained_executable)
                if digest(retained_executable) != binary["binary_sha256"]:
                    raise ValueError("test executable changed while retaining its artifact")
                binary["retained_executable"] = retained(directory, retained_executable)
                listed = [test for test in inventory["tests"] if test["executable"] == str(executable)]
                if not listed:
                    continue
                if digest(executable) != binary["binary_sha256"]:
                    raise ValueError("compiled test binary changed before execution")
                stdout = self.command(directory, f"tests-{index:04}", [retained_executable, "--include-ignored", "--format=pretty", "--color=never"], expected=(0, 101))
                parsed = parse_libtest(stdout.read_bytes(), binary["artifact"]["target"]["name"])
                for case in parsed:
                    original = next((item for item in listed if item["name"] == case["name"]), None)
                    case["source"] = {"file": binary["artifact"]["target"]["src_path"]}
                    case["counterpart"] = original["counterpart"] if original else None
                    case["configuration"] = configuration
                if {item["name"] for item in parsed} != {item["name"] for item in listed} or len(parsed) != len(listed):
                    raise ValueError("native test execution differs from its exact discovery selection")
                if any(item["outcome"] != "passed" for item in parsed) or digest(retained_executable) != binary["binary_sha256"]:
                    raise ValueError("native case failed, skipped, or changed artifact")
                cases.extend(parsed)
            if not cases:
                raise ValueError("native lane executed zero cases")
            write_json(directory / "native-results.json", {"configuration": configuration, "cases": cases,
                       "execution_root": str(directory),
                       "retained_binaries": inventory["binaries"],
                       "inventory_sha256": digest(output / "native-inventory.json")})
            return [output / "native-inventory.json", directory / "native-results.json"]
        if runner == "gradle":
            argv = [self.args.reference / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk", "--init-script", HERE / "matrix.init.gradle",
                "--init-script", CONFORMANCE / "inventory.init.gradle", f"-Prtdb.adapterRoot={CONFORMANCE}", f"-Prtdb.inventoryOutput={output}",
                "rtdbDiscoverTests", selection["task"]]
            if selection["task"] == "property-test":
                argv += ["-Piterations=500", "-PsimulationIterations=1000"]
            self.command(directory, "gradle", argv, self.args.reference)
            plans = [read_junit_plan(path) for path in output.rglob("*.jsonl")]
            cases = [case for plan in plans for case in plan["cases"]]
            if not plans or not cases or any(plan["errors"] for plan in plans):
                raise ValueError("original lane has no complete nonempty runtime recording")
            if any(case["outcome"].get("status") != "SUCCESSFUL" for case in cases):
                raise ValueError("original lane contains failed, skipped or unexecuted invocations")
            write_json(directory / "reference-results.json", {"selection": selection, "cases": cases})
            return [directory / "reference-results.json", output / "requested-lanes.json"]
        if runner == "sql-logic":
            argv = [sys.executable, CONFORMANCE / "run_sql_logic.py", *common, "--native", source("native"), "--selection", selection["selection"]]
            if "directory" in selection:
                argv += ["--directory", selection["directory"]]
            result, field = "result.json", "selection_passed"
        elif runner in {"controls", "history", "recovery"}:
            argv = [sys.executable, CONFORMANCE / ("run_" + runner + ".py"), *common, "--native", source("native")]
            if runner == "recovery":
                argv += ["--history", source("history")]
            if runner == "controls":
                argv += ["--ledger", source("ledger"), "--configuration", source("configuration")]
            result, field = "result.json", {"controls": "controls_verified", "history": "history_selection_passed", "recovery": "recovery_selection_passed"}[runner]
        elif runner == "clients":
            native, reference = source("native_package"), source("jvm_package")
            argv = [sys.executable, CONFORMANCE / "run_clients.py", *common, "--native-image", read_json(native)["image_id"], "--native-provenance", native,
                    "--reference-image", read_json(reference)["image_id"], "--reference-provenance", reference]
            result, field = "comparison.json", "client_selection_passed"
        elif runner == "performance":
            argv = [sys.executable, CONFORMANCE / "run_performance.py", *common, "--plan", source("performance_plan")]
            result, field = "comparison.json", "packaged_performance_qualified"
        elif runner == "packaged-recovery":
            argv = [sys.executable, CONFORMANCE / "run_packaged_recovery.py", *common, "--plan", source("recovery_plan")]
            result, field = "comparison.json", "packaged_recovery_qualified"
        elif runner == "inventory":
            runtime = merge_runtime(directory / "runtime", [self.directories[item["id"]] / "run" for item in self.lanes
                if item["runner"] == "gradle" and item["id"] in lane["dependencies"]])
            argv = [sys.executable, CONFORMANCE / "runtime_inventory.py", "--source", source("source_inventory"), "--ledger", source("ledger"),
                    "--runtime", runtime, "--native", source("native_inventory"), "--aliases", source("aliases"), "--invocations", source("invocations"),
                    "--output", directory / "inventory.json"]
            if "external" in inputs:
                argv += ["--external", source("external")]
            self.command(directory, runner, argv, expected=(0, 1))
            record = read_json(directory / "inventory.json")
            if record.get("complete") is not True or record.get("all_assertions_passed") is not True:
                raise ValueError("full source/runtime/assertion inventory is unreconciled")
            return [directory / "inventory.json"]
        elif runner == "embedded":
            native = source("native_library")
            library = directory / native.name
            library_digest = digest(native)
            shutil.copy2(native, library)
            if digest(library) != library_digest or digest(native) != library_digest:
                raise ValueError("embedded library changed while retaining evidence")
            self.command(directory, "embedded-binding", [REPOSITORY / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk", ":bindings:xtdb-rust-bindings:test",
                "-Dxtdb.native.library=" + str(library), "--rerun-tasks"])
            reports = list((REPOSITORY / "bindings/jvm/build/test-results/test").glob("TEST-*.xml"))
            from client_results import parse_junit
            report_directory = directory / "reports"
            report_directory.mkdir()
            captured = []
            for path in reports:
                target = report_directory / path.name
                report_digest = digest(path)
                shutil.copy2(path, target)
                if digest(target) != report_digest or digest(path) != report_digest:
                    raise ValueError("embedded assertion report changed while retaining evidence")
                captured.append(target)
            cases = [case for path in captured for case in parse_junit(path.read_bytes())]
            if not cases or any(case["outcome"] != "passed" for case in cases):
                raise ValueError("embedded binding lane lacks passing original lifetime assertions")
            if digest(library) != library_digest:
                raise ValueError("embedded library changed during test execution")
            write_json(directory / "embedded-results.json", {"library": retained(directory, library), "cases": cases,
                       "reports": [retained(directory, path) for path in captured]})
            return [directory / "embedded-results.json"]
        else:
            raise ValueError("release lane has no executable runner")
        self.command(directory, runner, argv)
        record = read_json(output / result)
        if record.get(field) is not True:
            raise ValueError("lane did not satisfy its selected assertions or measurements")
        return [output / result]

    def run(self):
        for lane in self.lanes:
            directory = self.directories[lane["id"]]
            directory.mkdir()
            receipt = {key: lane[key] for key in ("id", "runner", "platform", "selection")}
            receipt.update(verified=False, process_joined=False, artifacts=[])
            binding = self.bindings[lane["id"]]
            try:
                if lane["platform"] != host_platform():
                    raise ValueError("lane requires a worker running the selected native architecture")
                if any(self.completed.get(dependency, {}).get("verified") is not True for dependency in lane["dependencies"]):
                    raise ValueError("declared prerequisite lane has no successful receipt")
                missing = [name for name in binding.get("required_environment", []) if not os.environ.get(name)]
                if missing:
                    raise ValueError("missing explicitly required environment names: " + ", ".join(missing))
                paths = self.invoke(lane, binding, directory)
                for item in read_json(directory / "input-artifacts.json"):
                    artifact(Path(item["root"]), item)
                receipt.update(verified=True, process_joined=True, artifacts=[retained(self.args.output, path) for path in paths])
            except ProcessCancelled:
                raise
            except Exception as error:
                receipt["reason"] = str(error)
            captured = closure(directory)
            write_json(directory / "closure.json", captured)
            receipt["closure"] = retained(self.args.output, directory / "closure.json")
            write_json(directory / "receipt.json", receipt)
            self.completed[lane["id"]] = receipt
        return list(self.completed.values())


def main():
    parser = argparse.ArgumentParser(description="Execute every declared release lane and retain unmet prerequisites without granting partial parity.")
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=7200)
    args = parser.parse_args()
    args.plan, args.reference, args.output = args.plan.resolve(), args.reference.resolve(), args.output.resolve()
    require_phase_two(REPOSITORY, args.phase_two)
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    for repository, expected in ((REPOSITORY, revision), (args.reference, REFERENCE)):
        if git(repository, "rev-parse", "HEAD") != expected or git(repository, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("release orchestration requires clean pinned sources")
    plan = read_json(args.plan)
    if plan.get("schema_version") != 1:
        raise ValueError("unsupported release execution plan schema")
    matrix_path = artifact(args.plan.parent, plan["matrix"])
    if digest(matrix_path) != digest(HERE / "matrix.json"):
        raise ValueError("execution plan does not use the committed release matrix")
    lanes = expand(read_json(matrix_path))
    bindings = validate_bindings(lanes, plan["bindings"])
    validate_workflows(args.reference)
    protected_paths = [args.plan, *HERE.glob("*.py"), HERE / "matrix.init.gradle", matrix_path,
        *CONFORMANCE.glob("*.py"), *CONFORMANCE.glob("*.gradle"), *CONFORMANCE.glob("clients/*"),
        args.reference / ".github/workflows/slt.yml", args.reference / ".github/workflows/nightly-property-test.yml",
        args.reference / ".github/workflows/nightly-test.yml"]
    protected = [{"path": str(path), "sha256": digest(path)} for path in protected_paths if path.is_file()]
    args.output.mkdir(parents=True, exist_ok=False)
    write_json(args.output / "selection.json", lanes)
    write_json(args.output / "inputs.json", {"implementation_revision": revision, "reference_revision": REFERENCE, "files": protected})
    try:
        with owned_cancellation():
            receipts = Run(args, plan, lanes, bindings).run()
    except BaseException as error:
        write_json(args.output / "failure.json", {"release_matrix_verified": False, "error": str(error), "production_cutover_authorized": False})
        raise
    if any(digest(item["path"]) != item["sha256"] for item in protected):
        raise ValueError("release selection or orchestration source changed during execution")
    for repository, expected in ((REPOSITORY, revision), (args.reference, REFERENCE)):
        if git(repository, "rev-parse", "HEAD") != expected or git(repository, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("release checkout changed during execution")
    report = summarize(lanes, receipts)
    report.update(implementation_revision=revision, reference_revision=REFERENCE, plan_sha256=digest(args.plan), matrix_sha256=digest(matrix_path),
                  worker_platform=host_platform(), selection_sha256=digest(args.output / "selection.json"),
                  inputs_sha256=digest(args.output / "inputs.json"),
                  receipt_artifacts=[retained(args.output, path) for path in sorted(args.output.glob("lane-*/receipt.json"))])
    write_json(args.output / "result.json", report)
    return 0 if report["release_matrix_verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
