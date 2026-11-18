import copy
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
from pathlib import Path
from types import SimpleNamespace

import run_controls as runner


def mapping(name):
    return {"identity": {"reference_revision": runner.REFERENCE, "source": "source.clj", "qualified_name": name,
                         "parameter_fixture": "fixture.json", "configuration": "unit/default"}}


def comparison(cases, complete=False):
    counts = {kind: sum(case["disposition"] == kind for case in cases) for kind in
              ("passing_both", "missing_evidence", "mismatch", "matching_expected_failure")}
    return {"required_groups": len(cases), "passing_both": counts["passing_both"],
            "missing_evidence": counts["missing_evidence"], "mismatches": counts["mismatch"],
            "matching_expected_failure": counts["matching_expected_failure"],
            "complete_parity": complete and counts["missing_evidence"] == 0 and counts["mismatch"] == 0,
            "strict_agreement": (counts["passing_both"] + counts["matching_expected_failure"]) / len(cases) if complete and cases else None,
            "manifest_errors": [] if complete else ["source inventory is incomplete", "runtime discovery has not been reconciled"], "cases": cases}


def case_report(name, disposition="passing_both", differences=()):
    return {"identity": mapping(name)["identity"], "disposition": disposition, "differences": list(differences)}


def control_scope(ledger, selection):
    return runner.ControlSelection(json.dumps(ledger).encode(), json.dumps(selection).encode())


def scope_for(mappings):
    ledger = {"schema_version": 1, "inventory_revision": runner.REFERENCE, "inventory_complete": False,
              "runtime_reconciled": False, "cases": mappings}
    selection = {"schema_version": 1, "inventory_revision": runner.REFERENCE, "cases": [case["identity"] for case in mappings]}
    return control_scope(ledger, selection)


def comparison_writer(report):
    def compare(args, ledger, reference, native, directory):
        directory.mkdir()
        runner.write_json(directory / "report.json", report)
        return report
    return compare


class EvidenceTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "requires POSIX process-group ownership")
    def test_sigterm_to_runner_cancels_and_joins_its_separate_child_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready = root / "ready"
            program = """import os, signal, sys
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0, sys.argv[1])
from run_controls import ProcessCancelled, run_process
root = Path(sys.argv[2])
previous = signal.getsignal(signal.SIGTERM)
child = "import os, sys, time; from pathlib import Path; p = Path(sys.argv[1]); p.with_suffix('.tmp').write_text(str(os.getpid())); p.with_suffix('.tmp').replace(p); time.sleep(60)"
try:
    run_process([sys.executable, '-c', child, str(root / 'ready')], root, root / 'stdout', root / 'stderr', 60)
except ProcessCancelled:
    assert signal.getsignal(signal.SIGTERM) == previous
    sys.exit(23)
sys.exit(24)
"""
            process = subprocess.Popen([sys.executable, "-c", program, str(Path(runner.__file__).parent), str(root)], start_new_session=True)
            try:
                deadline = time.monotonic() + 5
                while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(ready.exists())
                child = int(ready.read_text())
                os.kill(process.pid, signal.SIGTERM)
                self.assertEqual(process.wait(timeout=5), 23)
                with self.assertRaises(ProcessLookupError):
                    os.kill(child, 0)
            finally:
                runner.terminate(process, grace=0)
                if ready.exists():
                    try:
                        os.killpg(int(ready.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    @unittest.skipUnless(os.name == "posix", "requires POSIX signals")
    def test_cleanup_suppresses_repeated_signals_in_nested_owners_and_restores_handlers(self):
        previous = {number: signal.getsignal(number) for number in (signal.SIGTERM, signal.SIGINT)}
        with runner.owned_cancellation() as owner:
            with self.assertRaises(runner.ProcessCancelled):
                signal.raise_signal(signal.SIGTERM)
            owner.begin_cleanup()
            with runner.owned_cancellation(deferred=True) as nested:
                nested.arm()
                signal.raise_signal(signal.SIGTERM)
                signal.raise_signal(signal.SIGINT)
            signal.raise_signal(signal.SIGTERM)
        for number, handler in previous.items():
            self.assertEqual(signal.getsignal(number), handler)

    @unittest.skipUnless(os.name == "posix", "requires POSIX process-group ownership")
    def test_sigterm_during_launch_is_deferred_until_the_child_handle_is_owned(self):
        original = subprocess.Popen
        launched = []
        def interrupted_launch(*args, **kwargs):
            process = original(*args, **kwargs)
            launched.append(process)
            signal.raise_signal(signal.SIGTERM)
            return process
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            try:
                with mock.patch.object(runner.subprocess, "Popen", side_effect=interrupted_launch), self.assertRaises(runner.ProcessCancelled):
                    runner.run_process([sys.executable, "-c", "import time; time.sleep(60)"], root, root / "stdout", root / "stderr", 60)
                self.assertEqual(len(launched), 1)
                self.assertFalse(runner.group_alive(launched[0]))
            finally:
                for process in launched:
                    runner.terminate(process, grace=0)

    @unittest.skipUnless(os.name == "posix", "requires POSIX process-group ownership")
    def test_shutdown_drains_a_child_that_outlives_its_parent_and_ignores_termination(self):
        runner.enable_child_reaping()
        with tempfile.TemporaryDirectory() as directory:
            ready = Path(directory) / "child-ready"
            program = """import os, signal, sys, time
pid = os.fork()
if pid == 0:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    open(sys.argv[1], 'w').write(str(os.getpid()))
    while True: time.sleep(1)
while not os.path.exists(sys.argv[1]): time.sleep(0.01)
os._exit(0)
"""
            process = subprocess.Popen([sys.executable, "-c", program, str(ready)], start_new_session=True)
            try:
                process.wait(timeout=5)
                self.assertTrue(runner.group_alive(process))
                runner.terminate(process, grace=0.05)
                self.assertFalse(runner.group_alive(process))
            finally:
                runner.terminate(process, grace=0)

    def test_identically_wrong_adapter_results_still_fail_the_original_assertions(self):
        fixture = {"kind": "temporal_ceilings", "expected_observations": {"ceiling": [2]}}
        execution = {"observations": [{"id": "ceiling", "outcome": {"kind": "returned", "typed_value": [3]}}]}
        with self.assertRaises(ValueError):
            runner.validate_assertions(execution, fixture)
        execution["observations"][0]["outcome"]["typed_value"] = [2]
        runner.validate_assertions(execution, fixture)
        execution["observations"][0]["outcome"]["typed_value"] = [2.0]
        with self.assertRaises(ValueError):
            runner.validate_assertions(execution, fixture)

    def test_stale_or_dirty_native_artifacts_cannot_receive_the_current_revision(self):
        for execution in [{"artifact_revision": "old", "source_dirty": False},
                          {"artifact_revision": "current", "source_dirty": True}, {}]:
            with self.assertRaises(ValueError):
                runner.validate_native_revision(execution, "current")
        runner.validate_native_revision({"artifact_revision": "current", "source_dirty": False}, "current")

    def test_reference_receipt_binds_the_retained_manifest_and_actual_artifact_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            runtime = root / "runtime"
            runtime.mkdir()
            jar = runtime / "adapter.jar"
            jar.write_bytes(b"original")
            adapter = {"path": jar.name, "bytes": jar.stat().st_size, "sha256": runner.digest(jar)}
            artifacts = [{"kind": "directory", "path": str(runtime), "files": [adapter]}]
            fixture = root / "fixture.json"
            runner.write_json(fixture, {"kind": "temporal_ceilings", "expected_observations": {"ceiling": [2]}})
            configuration = root / "configuration.json"
            config = dict(applied_basis=None, valid_time=None, system_time=None, clock="fixed", timezone="UTC",
                          storage_version=6, features=[])
            runner.write_json(configuration, config)
            args = SimpleNamespace(reference=root, timeout=1, configuration=configuration)
            selected = {**mapping("first"), "observations": [{"id": "ceiling"}]}

            def observe(command, cwd, stdout, stderr, timeout):
                stdout.write_bytes(b"")
                stderr.write_bytes(b"")
                manifest = stdout.parent / "runtime-artifacts.json"
                runner.write_json(manifest, artifacts)
                execution = {"schema_version": 1, "fixture_sha256": runner.digest(fixture), "artifacts": artifacts,
                             "binary_sha256": runner.digest(manifest), "observations": [{"id": "ceiling", "executed": True,
                                 "outcome": {"kind": "returned", "typed_value": [2]}}]}
                runner.write_json(stdout.parent / "execution.json", execution)
                if stdout.parent.name == "altered-manifest":
                    manifest.write_bytes(manifest.read_bytes() + b"\n")
                return {"completed": True, "exit_code": 0}

            with mock.patch.object(runner, "REPOSITORY", root), mock.patch.object(runner, "run_process", side_effect=observe):
                passed = root / "verified"
                receipt = runner.execute_case(selected, "reference", args, passed, config, runner.REFERENCE)
                trace = json.loads((passed / "trace.json").read_bytes())
                self.assertEqual(trace["runtime_artifacts_sha256"], runner.digest(passed / "runtime-artifacts.json"))
                self.assertEqual(trace["runtime_artifact_verifier_sha256"], runner.digest(runner.HERE / "history_results.py"))
                self.assertEqual(receipt["context"]["binary_sha256"], trace["runtime_artifacts_sha256"])
                self.assertEqual(receipt["trace_sha256"], runner.digest(passed / "trace.json"))

                changed = root / "altered-manifest"
                with self.assertRaisesRegex(ValueError, "manifest differs"):
                    runner.execute_case(selected, "reference", args, changed, config, runner.REFERENCE)
                self.assertFalse((changed / "receipt.json").exists())

                jar.write_bytes(b"modified")
                stale = root / "altered-artifact"
                with self.assertRaisesRegex(ValueError, "artifact bytes changed"):
                    runner.execute_case(selected, "reference", args, stale, config, runner.REFERENCE)
                self.assertFalse((stale / "receipt.json").exists())

    def test_column_table_assertions_check_original_names_types_cells_and_negative_control(self):
        fixture = json.loads((runner.HERE / "fixtures/table-column.json").read_bytes())
        execution = {"observations": [{"id": case["id"], "outcome": {"kind": "returned",
                      "typed_value": {case["column"]: copy.deepcopy(case["expected"])}}} for case in fixture["cases"]]}
        runner.validate_assertions(execution, fixture)
        for field in ("name", "type", "cell"):
            changed = copy.deepcopy(execution)
            value = changed["observations"][0]["outcome"]["typed_value"]
            if field == "name":
                value["wrong"] = value.pop("x5")
            elif field == "type":
                value["x5"]["data_type"] = {"Scalar": "F64"}
            else:
                value["x5"]["rows"][0][0] = {"I64": 1.0}
            with self.subTest(field=field), self.assertRaises(ValueError):
                runner.validate_assertions(changed, fixture)
        control = fixture["negative_control"]
        target = next(item for item in execution["observations"] if item["id"] == control["observation"])
        runner.replace_pointer(target["outcome"]["typed_value"], control["path"], control["replacement"])
        with self.assertRaises(ValueError):
            runner.validate_assertions(execution, fixture)

    def test_manifest_errors_other_than_incomplete_inventory_block_control_verification(self):
        mappings = [mapping("first"), mapping("second")]
        report = comparison([case_report("first"), case_report("second")])
        scope_for(mappings).require_agreement(report)
        report["manifest_errors"].append("unsupported ledger schema version")
        with self.assertRaises(ValueError):
            scope_for(mappings).require_agreement(report)

    def test_negative_control_changes_only_the_selected_copy(self):
        source = {"ceilings": [9223372036854775807, 2, 9223372036854775807], "other": [0, 100]}
        mutated = copy.deepcopy(source)
        runner.replace_pointer(mutated, "/ceilings/1", 3)
        self.assertEqual(source["ceilings"][1], 2)
        self.assertEqual(mutated["ceilings"][1], 3)
        self.assertEqual(mutated["other"], source["other"])

    def test_negative_control_handles_null_and_escaped_object_keys(self):
        value = {"a/b": {"~seq": None}}
        runner.replace_pointer(value, "/a~1b/~0seq", 0)
        self.assertEqual(value, {"a/b": {"~seq": 0}})
        with self.assertRaises(ValueError):
            runner.replace_pointer(value, "/a~1b/~0seq", 0)

    def test_missing_extra_duplicate_and_unexecuted_observations_cannot_be_receipts(self):
        mapping = {"observations": [{"id": "required"}]}
        receipt = {"schema_version": 1, "fixture_sha256": "fixture", "binary_sha256": "a" * 64,
                   "observations": [{"id": "required", "executed": True}]}
        for observations in [[], [{"id": "required", "executed": False}],
                             [{"id": "required", "executed": True}] * 2,
                             [{"id": "required", "executed": True}, {"id": "extra", "executed": True}]]:
            with self.subTest(observations=observations), self.assertRaises(ValueError):
                runner.validate_execution({**receipt, "observations": observations}, "fixture", mapping)
        runner.validate_execution(receipt, "fixture", mapping)

    def test_a_failed_comparison_or_empty_lane_cannot_verify_controls(self):
        mappings = [mapping("first"), mapping("second")]
        reports = [comparison([]), comparison([case_report("first"), case_report("second", "missing_evidence")]),
                   comparison([case_report("first"), case_report("second", "mismatch")])]
        for report in reports:
            with self.subTest(report=report), self.assertRaises(ValueError):
                scope_for(mappings).require_agreement(report)

    def test_repeated_receipt_snapshots_replace_atomically_and_leave_no_temporary_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            runner.write_json(path, [1])
            runner.write_json(path, [1, 2])
            self.assertEqual(json.loads(path.read_text()), [1, 2])
            self.assertEqual(list(Path(directory).iterdir()), [path])


class GroupDrainTests(unittest.TestCase):
    def test_permission_denial_retains_the_group_until_absence_is_proved(self):
        process = mock.Mock(pid=12345, returncode=None)
        process.poll.return_value = None
        calls = [PermissionError(1, "denied"), PermissionError(1, "denied"), None, None, ProcessLookupError(3, "gone")]
        with mock.patch.object(runner.os, "killpg", side_effect=calls) as killpg, mock.patch.object(runner.time, "sleep"):
            self.assertTrue(runner.drain_group(process, signal.SIGTERM, 1))
        self.assertEqual(killpg.call_args_list, [mock.call(12345, signal.SIGTERM), mock.call(12345, 0),
                                               mock.call(12345, signal.SIGTERM), mock.call(12345, 0), mock.call(12345, 0)])
        process.wait.assert_called_once_with()

    def test_permanent_signal_denial_cannot_report_a_joined_group(self):
        process = mock.Mock(pid=12345, returncode=None)
        process.poll.return_value = None
        with mock.patch.object(runner.os, "killpg", side_effect=PermissionError(1, "denied")), \
             mock.patch.object(runner.time, "monotonic", side_effect=[0, 0.5, 1]), mock.patch.object(runner.time, "sleep"):
            self.assertFalse(runner.drain_group(process, signal.SIGTERM, 1))
        process.wait.assert_not_called()


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.ledger = {"schema_version": 1, "inventory_revision": runner.REFERENCE,
                       "inventory_complete": False, "runtime_reconciled": False,
                       "cases": [mapping("first"), mapping("second"), mapping("sql-table")]}
        self.selection = {"schema_version": 1, "inventory_revision": runner.REFERENCE,
                          "cases": [mapping("second")["identity"], mapping("first")["identity"]]}

    def test_default_selection_identifies_both_mandatory_global_mappings(self):
        ledger = json.loads((runner.HERE / "assertion-ledger.json").read_bytes())
        selection = json.loads((runner.HERE / "configurations/control-selection.json").read_bytes())
        self.assertEqual(len(control_scope(ledger, selection).mappings()), 2)
        self.assertEqual({identity["parameter_fixture"] for identity in selection["cases"]},
                         {"rust/conformance/fixtures/temporal-ceilings.json", "rust/conformance/fixtures/term-sequence.json"})

    def test_selection_preserves_order_without_shrinking_or_mutating_the_global_ledger(self):
        before = copy.deepcopy(self.ledger)
        selected = control_scope(self.ledger, self.selection).mappings()
        self.assertEqual([case["identity"]["qualified_name"] for case in selected], ["second", "first"])
        self.assertEqual(self.ledger, before)
        self.assertEqual(selected[0], self.ledger["cases"][1])
        selected[0]["identity"]["configuration"] = "changed"
        self.assertEqual(control_scope(self.ledger, self.selection).mappings()[0], before["cases"][1])

    def test_empty_duplicate_partial_unmapped_and_foreign_selection_cases_are_rejected(self):
        identity = mapping("first")["identity"]
        for cases in [[], [identity, identity], [{"qualified_name": "first"}], [mapping("unknown")["identity"]],
                      [{**identity, "reference_revision": "f" * 40}], [{**identity, "configuration": ""}],
                      [{**identity, "extra": "not-an-identity-field"}]]:
            with self.subTest(cases=cases), self.assertRaises(ValueError):
                control_scope(self.ledger, {**self.selection, "cases": cases})
        for key, value in [("schema_version", 2), ("inventory_revision", "f" * 40)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                control_scope(self.ledger, {**self.selection, key: value})

    def test_duplicate_or_foreign_global_mappings_cannot_hide_outside_the_selection(self):
        for case in [mapping("sql-table"), {"identity": {**mapping("other")["identity"], "reference_revision": "f" * 40}}]:
            with self.subTest(case=case), self.assertRaises(ValueError):
                control_scope({**self.ledger, "cases": self.ledger["cases"] + [case]}, self.selection)

    def test_unselected_assertions_remain_missing_in_the_full_comparison(self):
        report = comparison([case_report("first"), case_report("second"), case_report("sql-table", "missing_evidence")])
        before = copy.deepcopy(report)
        control_scope(self.ledger, self.selection).require_agreement(report)
        self.assertEqual(report, before)
        self.assertEqual(report["missing_evidence"], 1)
        self.assertFalse(report["complete_parity"])
        for disposition in ["missing_evidence", "mismatch", "matching_expected_failure"]:
            changed = copy.deepcopy(report)
            changed["cases"][0]["disposition"] = disposition
            with self.subTest(disposition=disposition), self.assertRaises(ValueError):
                control_scope(self.ledger, self.selection).require_agreement(comparison(changed["cases"]))

    def test_unselected_evidence_and_missing_or_duplicate_control_reports_are_rejected(self):
        mappings = control_scope(self.ledger, self.selection).mappings()
        for cases in [[case_report("first")], [case_report("first"), case_report("first"), case_report("second")],
                      [case_report("first"), case_report("second"), case_report("sql-table")],
                      [case_report("first"), case_report("second"), case_report("sql-table", "mismatch")]]:
            with self.subTest(cases=cases), self.assertRaises(ValueError):
                control_scope(self.ledger, self.selection).require_agreement(comparison(cases))

    def test_negative_control_keeps_unselected_missing_assertions_visible(self):
        first, second = mapping("first"), mapping("second")
        difference = {"observation": "value", "path": "/count"}
        report = comparison([case_report("first", "mismatch", [difference]), case_report("second"), case_report("sql-table", "missing_evidence")])
        reference = [{"identity": first["identity"], "runner": "test", "trace_sha256": "trace",
                      "observations": [{"id": "value", "outcome": {"typed_value": {"count": 1}}}]}]
        before = copy.deepcopy(reference)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runner.write_json(root / "fixture.json", {"negative_control": {"observation": "value", "path": "/count", "replacement": 2}})
            with mock.patch.object(runner, "REPOSITORY", root), mock.patch.object(runner, "compare_files", side_effect=comparison_writer(report)):
                runner.negative_control(None, control_scope(self.ledger, self.selection), reference, root / "native.json", first, root / "negative")
                self.assertTrue(json.loads((root / "negative/control-result.json").read_bytes())["detected"])
                for disposition in ["missing_evidence", "mismatch"]:
                    report["cases"][1]["disposition"] = disposition
                    report.update(comparison(report["cases"]))
                    with self.subTest(disposition=disposition), self.assertRaises(ValueError):
                        runner.negative_control(None, control_scope(self.ledger, self.selection), reference, root / "native.json", first, root / disposition)
        self.assertEqual(reference, before)

    def test_omitting_only_an_unselected_case_cannot_shrink_the_report_denominator(self):
        selected_only = comparison([case_report("first"), case_report("second")])
        scope = control_scope(self.ledger, self.selection)
        with self.assertRaises(ValueError):
            scope.require_agreement(selected_only)
        selected_only["complete_parity"] = True
        with self.assertRaises(ValueError):
            scope.require_agreement(selected_only)

    def test_summary_counts_and_parity_are_derived_from_the_full_inventory(self):
        report = comparison([case_report("first"), case_report("second"), case_report("sql-table", "missing_evidence")])
        scope = control_scope(self.ledger, self.selection)
        for field, value in [("required_groups", 2), ("passing_both", 3), ("missing_evidence", 0),
                             ("mismatches", 1), ("matching_expected_failure", 1), ("strict_agreement", 1.0),
                             ("complete_parity", True), ("manifest_errors", [])]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                scope.require_agreement({**report, field: value})
        ledger = {**self.ledger, "inventory_complete": True, "runtime_reconciled": True}
        complete_inventory = control_scope(ledger, self.selection)
        report = comparison(report["cases"], complete=True)
        complete_inventory.require_agreement(report)
        self.assertFalse(report["complete_parity"])
        self.assertEqual(report["strict_agreement"], 2 / 3)

    def test_declared_migration_cannot_be_selected_as_an_equality_control(self):
        ledger = copy.deepcopy(self.ledger)
        selected = next(case for case in ledger["cases"] if case["identity"] == self.selection["cases"][0])
        selected["observations"] = [{"id": "parser-cause", "expected": {"kind": "declared_migration"}}]
        with self.assertRaisesRegex(ValueError, "control selection cannot include a declared migration"):
            control_scope(ledger, self.selection)

    def test_malformed_selected_observations_report_control_input_errors(self):
        ledger = copy.deepcopy(self.ledger)
        selected = next(case for case in ledger["cases"] if case["identity"] == self.selection["cases"][0])
        for observations in (None, 17, [None], [{"expected": None}], [{"expected": []}]):
            selected["observations"] = observations
            with self.subTest(observations=observations), self.assertRaisesRegex(ValueError, "control case observation"):
                control_scope(ledger, self.selection)

    def test_unselected_migration_remains_missing_with_legacy_control_reports(self):
        ledger = copy.deepcopy(self.ledger)
        migration = next(case for case in ledger["cases"] if case["identity"]["qualified_name"] == "sql-table")
        migration["observations"] = [{"id": "parser-cause", "expected": {"kind": "declared_migration"}}]
        scope = control_scope(ledger, self.selection)
        report = comparison([case_report("first"), case_report("second"), case_report("sql-table", "missing_evidence")])
        scope.require_agreement(report)
        scope.require_agreement({**report, "matched_declared_migration": 0})
        for invalid in (1, False, "0"):
            with self.subTest(count=invalid), self.assertRaisesRegex(ValueError, "summary differs"):
                scope.require_agreement({**report, "matched_declared_migration": invalid})

    def test_migration_counts_cannot_claim_parity_or_supply_unselected_control_evidence(self):
        ledger = {**self.ledger, "inventory_complete": True, "runtime_reconciled": True}
        scope = control_scope(ledger, self.selection)
        report = comparison([case_report("first"), case_report("second"), case_report("sql-table", "matched_declared_migration")], complete=True)
        report["matched_declared_migration"] = 1
        with self.assertRaisesRegex(ValueError, "completeness differs"):
            scope.require_agreement(report)
        report["complete_parity"] = False
        with self.assertRaisesRegex(ValueError, "unexpected evidence outside the control selection"):
            scope.require_agreement(report)

    def test_changed_global_ledger_snapshot_cannot_reach_the_comparator(self):
        scope = control_scope(self.ledger, self.selection)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "ledger.json").write_bytes(scope.ledger_bytes + b" ")
            args = SimpleNamespace(output=root, native=root / "native", timeout=1)
            with mock.patch.object(runner, "run_process") as process, self.assertRaisesRegex(ValueError, "changed before comparison"):
                runner.compare_files(args, scope, root / "reference", root / "native", root / "comparison")
            process.assert_not_called()
            (root / "ledger.json").write_bytes(scope.ledger_bytes)
            def changed_during(command, cwd, output, log, timeout):
                self.assertEqual(command[2], root / "ledger.json")
                (root / "ledger.json").write_bytes(scope.ledger_bytes + b" ")
                runner.write_json(output, comparison([case_report("first"), case_report("second"), case_report("sql-table", "missing_evidence")]))
                return {"completed": True, "exit_code": 0}
            with mock.patch.object(runner, "run_process", side_effect=changed_during), self.assertRaisesRegex(ValueError, "changed during comparison"):
                runner.compare_files(args, scope, root / "reference", root / "native", root / "comparison")

    def test_runner_retains_global_ledger_and_selection_digests_for_each_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger, selection, config, native = [root / name for name in ["ledger.json", "selection.json", "config.json", "native"]]
            runner.write_json(ledger, self.ledger)
            runner.write_json(selection, self.selection)
            runner.write_json(config, {})
            native.write_bytes(b"native-artifact")
            args = SimpleNamespace(phase_two=True, reference=root / "reference", native=native, ledger=ledger,
                                   selection=selection, configuration=config, output=root / "output", created_output=False)
            report = comparison([case_report("first"), case_report("second"), case_report("sql-table", "missing_evidence")])
            def git(repository, *arguments):
                return "" if arguments[0] == "status" else (runner.REFERENCE if repository == args.reference else "a" * 40)
            with mock.patch.object(runner, "require_phase_two") as gate, mock.patch.object(runner, "git", side_effect=git), \
                 mock.patch.object(runner, "execute_case", side_effect=lambda mapping, *args: {"identity": mapping["identity"]}) as execute, \
                 mock.patch.object(runner, "compare_files", side_effect=comparison_writer(report)) as compare, mock.patch.object(runner, "negative_control") as negative:
                runner.run(args)
                gate.assert_called_once_with(runner.REPOSITORY, True)
                self.assertEqual(execute.call_count, 6)
                self.assertEqual(negative.call_count, 2)
                self.assertEqual([json.loads(call.args[1].ledger_bytes) for call in compare.call_args_list], [self.ledger, self.ledger])
                self.assertEqual([call.args[0]["identity"]["qualified_name"] for call in execute.call_args_list], ["second"] * 3 + ["first"] * 3)
            manifest = json.loads((args.output / "run.json").read_bytes())
            self.assertEqual(manifest["ledger_sha256"], runner.digest(ledger))
            self.assertEqual(manifest["selection_sha256"], runner.digest(selection))
            self.assertEqual((args.output / "ledger.json").read_bytes(), ledger.read_bytes())
            self.assertEqual((args.output / "selection.json").read_bytes(), selection.read_bytes())
            self.assertEqual((args.output / "configuration.json").read_bytes(), config.read_bytes())
            result = json.loads((args.output / "result.json").read_bytes())
            self.assertTrue(result["controls_verified"])
            self.assertFalse(result["full_inventory_parity"])
            self.assertEqual((result["paired_groups"], result["inventory_groups"]), (2, 3))


if __name__ == "__main__":
    unittest.main()
