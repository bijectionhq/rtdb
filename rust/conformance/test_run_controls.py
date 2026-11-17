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

import run_controls as runner


class EvidenceTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "requires POSIX process-group ownership")
    def test_sigterm_to_runner_cancels_and_joins_its_separate_child_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready = root / "ready"
            program = """import os, signal, sys
from pathlib import Path
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

    def test_manifest_errors_other_than_incomplete_inventory_block_control_verification(self):
        report = {"required_groups": 2, "missing_evidence": 0, "mismatches": 0, "passing_both": 2,
                  "manifest_errors": ["source inventory is incomplete", "runtime discovery has not been reconciled"]}
        runner.require_agreement(report)
        report["manifest_errors"].append("unsupported ledger schema version")
        with self.assertRaises(ValueError):
            runner.require_agreement(report)

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
        for report in [{"required_groups": 0, "missing_evidence": 0, "mismatches": 0, "passing_both": 0},
                       {"required_groups": 2, "missing_evidence": 1, "mismatches": 0, "passing_both": 1},
                       {"required_groups": 2, "missing_evidence": 0, "mismatches": 1, "passing_both": 1}]:
            with self.subTest(report=report), self.assertRaises(ValueError):
                runner.require_agreement(report)

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


if __name__ == "__main__":
    unittest.main()
