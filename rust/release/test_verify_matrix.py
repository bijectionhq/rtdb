import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from evidence import retained
from verify_matrix import verify_lane_artifacts


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


if __name__ == "__main__":
    unittest.main()
