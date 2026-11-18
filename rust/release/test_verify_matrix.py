import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from evidence import closure, retained, sha256
from lane_artifacts import ASSERTION_COMPARISON_INPUTS, assertion_comparison_command, bind_inputs, verify_bindings
from matrix_plan import expand
from verify_matrix import verify, verify_lane_artifacts


def write_record(path, value):
    path.write_text(json.dumps(value))
    return path


def assertion_comparison_fixture(root):
    fixture_directory = Path(__file__).resolve().parent.parent / "conformance"
    committed_ledger = json.loads((fixture_directory / "assertion-ledger.json").read_bytes())
    case = next(case for case in committed_ledger["cases"]
                if case["identity"]["parameter_fixture"] == "rust/conformance/fixtures/temporal-ceilings.json")
    reference_revision = committed_ledger["inventory_revision"]
    implementation_revision = "b" * 40
    fixture_path = fixture_directory / "fixtures/temporal-ceilings.json"
    configuration_path = fixture_directory / "configurations/controls.json"
    original_fixture = json.loads(fixture_path.read_bytes())
    configuration = json.loads(configuration_path.read_bytes())
    del configuration["schema_version"]
    observations = [{"id": item["id"], "executed": True, "outcome": {
        "kind": "returned", "typed_value": original_fixture["expected_observations"][item["id"]]}}
        for item in case["observations"]]
    write_record(root / "inputs.json", {"reference_revision": reference_revision, "implementation_revision": implementation_revision})

    production = root / "production"
    inventory = root / "inventory"
    directory = root / "comparison"
    for child in (production, inventory, directory):
        child.mkdir()
    native = production / "rtdb-conformance"
    native.write_bytes(b"retained comparator artifact")
    prefix = "linux-arm64/rust/local"
    write_record(production / "production-artifacts.json", {"configuration": prefix + "/production", "selection": {},
                 "implementation_revision": implementation_revision, "binaries": [{
                     "compiler_artifact": {"target": {"name": "rtdb-conformance"}, "profile": {"test": False}},
                     "retained_artifact": retained(production, native)}]})
    ledger = write_record(inventory / "ledger.json", {**committed_ledger, "inventory_complete": True,
                          "runtime_reconciled": True, "cases": [case]})
    write_record(inventory / "input-artifacts.json", [{"input": "ledger", **retained(inventory, ledger)}])
    inventory_report = write_record(inventory / "inventory.json", {"schema_version": 1, "reference_revision": reference_revision,
                                    "assertion_denominator": 1, "complete": True, "all_assertions_passed": True})
    original_worker = Path("/original/arm64-worker")
    original_inventory = original_worker / inventory.name
    write_record(inventory / "inventory.process.json", {
        "command": ["python", "runtime_inventory.py", "--ledger", str(original_inventory / ledger.name),
                    "--output", str(original_inventory / inventory_report.name)],
        "cwd": str(original_inventory), "completed": True, "exit_code": 0})

    paths = {"native": native, "inventory": inventory_report, "ledger": ledger}
    for side, revision in (("reference", reference_revision), ("native", implementation_revision)):
        role = side + "_receipts"
        context = {**configuration, "artifact_revision": revision, "binary_sha256": sha256(native),
                   "fixture_sha256": sha256(fixture_path), "configuration_sha256": sha256(configuration_path)}
        paths[role] = write_record(root / (role + ".json"), [{"identity": case["identity"],
                                   "runner": f"rtdb-controls/{side}/1", "trace_sha256": sha256(fixture_path),
                                   "context": context, "observations": observations}])
    for role, filename in ASSERTION_COMPARISON_INPUTS:
        (directory / filename).write_bytes(paths[role].read_bytes())
    write_record(directory / "input-artifacts.json", [{"input": role, **retained(root, paths[role])}
                                                       for role, _ in ASSERTION_COMPARISON_INPUTS])

    selected = {"platform": "linux/arm64", "selection": {}}
    predecessors = {identity: ({**selected, "id": identity}, producer)
                    for identity, producer in ((prefix + "/production", production), (prefix + "/inventory", inventory))}
    lane = {**selected, "id": prefix + "/assertion-comparison", "runner": "assertion-comparison",
            "selection": {"variant": "local", "features": []}, "dependencies": list(predecessors)}
    bindings = bind_inputs(lane, directory, root, paths.__getitem__, predecessors.__getitem__)
    native_binding = next(binding for binding in bindings if binding["role"] == "native")
    original_native = original_worker / native.relative_to(root)
    native_binding["consumed_path"] = str(original_native)
    write_record(directory / "artifact-bindings.json", bindings)
    original_directory = original_worker / directory.name
    process = {"command": assertion_comparison_command(original_native, original_directory),
               "cwd": str(original_directory), "completed": True, "exit_code": 0}
    report = {"complete_parity": True, "comparator_version": "1", "required_groups": 1, "passing_both": 1,
              "matching_expected_failure": 0, "missing_evidence": 0, "mismatches": 0, "strict_agreement": 1.0,
              "manifest_errors": [], "cases": [{"identity": case["identity"], "disposition": "passing_both",
                                               "comparisons": len(observations), "differences": []}]}
    write_record(directory / "assertion-comparison.process.json", process)
    write_record(directory / "assertion-comparison.stdout.log", report)
    (directory / "assertion-comparison.stderr.log").write_text("")
    return lane, directory, paths, predecessors, process, report


class RetainedLaneArtifacts(unittest.TestCase):
    def test_gradle_summary_cannot_promote_a_failed_recorded_assertion(self):
        from runtime_inventory import read_plan
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / "run"
            run.mkdir()
            descriptor = {"unique_id": "case", "is_test": True, "parent_id": None}
            events = [{"event": "plan_started", "project": ":", "task": ":test"},
                      {"event": "discovered", "descriptor": descriptor}, {"event": "execution_started", "descriptor": descriptor},
                      {"event": "execution_finished", "descriptor": descriptor, "status": "FAILED"}, {"event": "plan_finished"}]
            path = run / "events.jsonl"
            path.write_text("".join(json.dumps({**event, "sequence": position}) + "\n" for position, event in enumerate(events)))
            cases = read_plan(path)["cases"]
            cases[0]["outcome"]["status"] = "SUCCESSFUL"
            (root / "reference-results.json").write_text(json.dumps({"selection": {}, "cases": cases}))
            with self.assertRaises(ValueError):
                verify_lane_artifacts(root, {"runner": "gradle", "selection": {}})

    def test_native_summary_cannot_promote_a_failed_raw_framework_result(self):
        from client_results import parse_libtest
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "run").mkdir()
            binary = root / "test-binary"
            binary.write_bytes(b"test-executable")
            binary_reference = retained(root, binary)
            original = {"artifact": {"executable": "/compiled/test-binary", "target": {"name": "suite", "src_path": "test.rs"}},
                        "binary_sha256": binary_reference["sha256"]}
            inventory = {"configuration": "local", "binaries": [original], "tests": [{"executable": "/compiled/test-binary", "name": "case", "counterpart": "native::case"}]}
            inventory_path = root / "run/native-inventory.json"
            inventory_path.write_text(json.dumps(inventory))
            raw = b"running 1 test\ntest case ... FAILED\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n"
            (root / "tests-0000.stdout.log").write_bytes(raw)
            process = {"exit_code": 0, "command": [str(binary), "--include-ignored", "--format=pretty", "--color=never"]}
            (root / "tests-0000.process.json").write_text(json.dumps(process))
            cases = parse_libtest(raw, "suite")
            cases[0].update(outcome="passed", source={"file": "test.rs"}, counterpart="native::case", configuration="local")
            result = {"execution_root": str(root), "cases": cases, "inventory_sha256": retained(root, inventory_path)["sha256"],
                      "retained_binaries": [{**original, "retained_executable": binary_reference}]}
            (root / "native-results.json").write_text(json.dumps(result))
            with self.assertRaises(ValueError):
                verify_lane_artifacts(root, {"runner": "native-tests"})

    def test_embedded_invocation_keeps_library_and_report_digests_distinct(self):
        from run_matrix import Run
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            library = root / "library.so"
            library.write_bytes(b"actual-native-library")
            reports = root / "bindings/jvm/build/test-results/test"
            reports.mkdir(parents=True)
            (reports / "TEST-lifetime.xml").write_text('<testsuite tests="1"><testcase classname="Bindings" name="lifetime"/></testsuite>')
            lane = {"id": "embedded", "runner": "embedded", "selection": {}, "dependencies": []}
            args = types.SimpleNamespace(plan=root / "plan.json", output=root, reference=root, timeout=10)
            run = Run(args, {}, [lane], {})
            output = root / "retained"
            output.mkdir()
            binding = {"inputs": {"native_library": retained(root, library)}}
            with patch("run_matrix.REPOSITORY", root), patch("run_matrix.bind_inputs", return_value=[]), patch.object(run, "command"):
                run.invoke(lane, binding, output)
            verify_lane_artifacts(output, {"runner": "embedded"})
            result = json.loads((output / "embedded-results.json").read_text())
            self.assertNotEqual(result["library"]["sha256"], result["reports"][0]["sha256"])

    def test_embedded_summary_must_retain_and_match_actual_framework_xml(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            library = root / "library.so"
            library.write_bytes(b"native-library")
            report = root / "report.xml"
            report.write_text('<testsuite tests="1"><testcase classname="Bindings" name="lifetime"/></testsuite>')
            from client_results import parse_junit
            result = {"library": retained(root, library), "reports": [retained(root, report)], "cases": parse_junit(report.read_bytes())}
            summary = root / "embedded-results.json"
            summary.write_text(json.dumps(result))
            verify_lane_artifacts(root, {"runner": "embedded"})
            report.unlink()
            with self.assertRaises(ValueError):
                verify_lane_artifacts(root, {"runner": "embedded"})

    def test_missing_production_binary_bytes_cannot_be_replaced_by_digest_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            messages = root / "cargo.log"
            compiler = {"reason": "compiler-artifact", "profile": {"test": False}}
            messages.write_text(json.dumps(compiler) + '\n{"reason":"build-finished","success":true}\n')
            process = root / "process.json"
            process.write_text('{"command":["cargo","build"],"exit_code":0}')
            result = {"selection": {}, "configuration": "production", "command": ["cargo", "build"],
                "messages": retained(root, messages), "process": retained(root, process), "binaries": [{"compiler_artifact": compiler,
                "sha256": "a" * 64, "retained_artifact": {"path": "missing-binary", "sha256": "a" * 64}}]}
            (root / "production-artifacts.json").write_text(json.dumps(result))
            with self.assertRaises(ValueError):
                verify_lane_artifacts(root, {"runner": "cargo-production", "id": "production", "selection": {}})

    def test_inventory_success_cannot_bypass_the_exact_retained_ledger(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            _, _, paths, _, _, _ = assertion_comparison_fixture(root)
            directory = paths["inventory"].parent
            verify_lane_artifacts(directory, {"runner": "inventory"})
            process_path = directory / "inventory.process.json"
            process = json.loads(process_path.read_bytes())
            process["command"][3] = "/original/arm64-worker/inventory/other-ledger.json"
            write_record(process_path, process)
            with self.assertRaisesRegex(ValueError, "retained assertion ledger"):
                verify_lane_artifacts(directory, {"runner": "inventory"})

    def test_offline_comparison_requires_the_original_ordered_operands_and_complete_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            lane, directory, _, predecessors, process, report = assertion_comparison_fixture(root)
            verify_lane_artifacts(directory, lane)
            verify_bindings(lane, directory, root, predecessors.__getitem__)

            command = process["command"]
            process_path = directory / "assertion-comparison.process.json"
            write_record(process_path, {**process, "command": [*command[:3], command[4], command[3]]})
            with self.assertRaisesRegex(ValueError, "selected executable and operands"):
                verify_bindings(lane, directory, root, predecessors.__getitem__)
            write_record(process_path, process)

            report_path = directory / "assertion-comparison.stdout.log"
            write_record(report_path, {**report, "required_groups": 2})
            with self.assertRaisesRegex(ValueError, "summary differs"):
                verify_bindings(lane, directory, root, predecessors.__getitem__)
            write_record(report_path, report)

            native_receipts = directory / "native-receipts.json"
            original_receipts = native_receipts.read_bytes()
            native_receipts.write_bytes(original_receipts + b" ")
            with self.assertRaisesRegex(ValueError, "differs from its retained digest"):
                verify_bindings(lane, directory, root, predecessors.__getitem__)

            receipts = json.loads(original_receipts)
            receipts[0]["context"]["artifact_revision"] = "c" * 40
            write_record(native_receipts, receipts)
            manifest = directory / "input-artifacts.json"
            inputs = [item for item in json.loads(manifest.read_bytes()) if item["input"] != "native_receipts"]
            write_record(manifest, [*inputs, {"input": "native_receipts", **retained(directory, native_receipts)}])
            with self.assertRaisesRegex(ValueError, "artifact revision"):
                verify_bindings(lane, directory, root, predecessors.__getitem__)

            receipts = json.loads(original_receipts)
            receipts[0]["context"]["binary_sha256"] = "c" * 64
            write_record(native_receipts, receipts)
            write_record(manifest, [*inputs, {"input": "native_receipts", **retained(directory, native_receipts)}])
            with self.assertRaisesRegex(ValueError, "another production executable"):
                verify_bindings(lane, directory, root, predecessors.__getitem__)

    def test_comparison_exit_one_retains_its_report_and_leaves_the_lane_unmet(self):
        from run_matrix import Run
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            lane, _, paths, predecessors, _, report = assertion_comparison_fixture(root)
            args = types.SimpleNamespace(output=root, plan=root / "plan.json", reference=root, timeout=10)
            prefix = "linux-arm64/rust/local"
            dependency_lanes = [producer for producer, _ in predecessors.values()]
            run = Run(args, {}, [*dependency_lanes, lane], {})
            for identity, (_, directory) in predecessors.items():
                run.completed[identity] = {"verified": True}
                run.directories[identity] = directory
            directory = run.directories[lane["id"]]
            directory.mkdir()
            comparison = {**report, "complete_parity": False, "strict_agreement": 0.0, "passing_both": 0,
                          "matched_declared_migration": 1, "cases": [
                              {**report["cases"][0], "disposition": "matched_declared_migration"}]}

            def comparator(command, cwd, output, log, timeout):
                self.assertEqual(command, assertion_comparison_command(paths["native"], directory))
                self.assertEqual(cwd, directory)
                write_record(output, comparison)
                log.write_text("")
                return {"command": [str(argument) for argument in command], "cwd": str(cwd),
                        "completed": True, "exit_code": 1}

            binding = {"inputs": {role: retained(root, path) for role, path in paths.items()}}
            with patch("run_matrix.run_process", side_effect=comparator):
                with self.assertRaisesRegex(ValueError, "strict parity"):
                    run.invoke(lane, binding, directory)
            self.assertEqual(json.loads((directory / "assertion-comparison.stdout.log").read_bytes()), comparison)
            process = json.loads((directory / "assertion-comparison.process.json").read_bytes())
            self.assertEqual(process["exit_code"], 1)
            self.assertEqual(set(run.completed), {prefix + "/production", prefix + "/inventory"})

    def test_comparison_receipts_must_be_in_the_verified_worker_closure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            lane, directory, _, _, _, _ = assertion_comparison_fixture(root)
            matrix_path = Path(__file__).with_name("matrix.json")
            matrix = json.loads(matrix_path.read_bytes())
            selection = write_record(root / "selection.json", expand(matrix))
            revisions = json.loads((root / "inputs.json").read_bytes())

            retained_files = [item for item in closure(directory) if item["path"] != "native-receipts.json"]
            closure_file = write_record(directory / "closure.json", retained_files)
            receipt = write_record(directory / "receipt.json", {**lane, "verified": True, "process_joined": True,
                                   "artifacts": [retained(root, directory / "assertion-comparison.stdout.log")],
                                   "closure": retained(root, closure_file)})
            matrix_sha256, plan_sha256 = sha256(matrix_path), "a" * 64
            write_record(root / "result.json", {**revisions, "plan_sha256": plan_sha256, "matrix_sha256": matrix_sha256,
                         "selection_sha256": sha256(selection), "inputs_sha256": sha256(root / "inputs.json"),
                         "worker_platform": lane["platform"], "receipt_artifacts": [retained(root, receipt)]})
            with self.assertRaisesRegex(ValueError, "absent from the retained lane closure"):
                verify(matrix, [root], plan_sha256, revisions["implementation_revision"],
                       revisions["reference_revision"], matrix_sha256)


if __name__ == "__main__":
    unittest.main()
