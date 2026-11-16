import json
import os
import signal
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from run_controls import ProcessCancelled, digest, write_json
from run_recovery import kill_at_boundary


@unittest.skipUnless(os.name == "posix", "owned process-group fault injection requires POSIX")
class RecoveryProcessTests(unittest.TestCase):
    def inputs(self, root):
        request = root / "request.json"
        write_json(request, {"action": "canary"})
        (root / "receipt").mkdir()
        marker = {"request_sha256": digest(request), "binary_sha256": digest(sys.executable), "boundary": "after_canary_acknowledged"}
        return request, marker

    def test_exact_boundary_kills_and_joins_writer_and_descendants(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            request, marker = self.inputs(root)
            program = """import hashlib, json, os, pathlib, subprocess, sys, time
root = pathlib.Path(sys.argv[1])
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
(root / 'child.pid').write_text(str(child.pid))
with (root / 'receipt/boundary.pending').open('w') as output:
    json.dump(json.loads(sys.argv[2]), output)
    output.flush()
    os.fsync(output.fileno())
(root / 'receipt/boundary.pending').replace(root / 'receipt/boundary.json')
directory = os.open(root / 'receipt', os.O_RDONLY)
os.fsync(directory)
os.close(directory)
with (root / 'receipt/ready.pending').open('w') as output:
    json.dump({'boundary_sha256': hashlib.sha256((root / 'receipt/boundary.json').read_bytes()).hexdigest()}, output)
    output.flush()
    os.fsync(output.fileno())
(root / 'receipt/ready.pending').replace(root / 'receipt/boundary-ready.json')
time.sleep(60)
"""
            actual = kill_at_boundary([sys.executable, "-c", program, root, json.dumps(marker)], root, root, request, Path(sys.executable), 10)
            self.assertEqual(actual, marker)
            receipt = json.loads((root / "process.json").read_bytes())
            self.assertEqual(receipt["closure"], "forced_kill")
            self.assertEqual(receipt["exit_code"], -signal.SIGKILL)
            self.assertTrue(receipt["completed"])
            with self.assertRaises(ProcessLookupError):
                os.kill(int((root / "child.pid").read_text()), 0)

    def test_timeout_keeps_failure_receipt_and_joins_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            request, _ = self.inputs(root)
            with self.assertRaises(RuntimeError):
                kill_at_boundary([sys.executable, "-c", "import time; time.sleep(60)"], root, root, request, Path(sys.executable), 0.01)
            receipt = json.loads((root / "process.json").read_bytes())
            self.assertFalse(receipt["completed"])
            self.assertIn("selected boundary", receipt["error"])
            self.assertIsInstance(receipt["exit_code"], int)

    def test_cancelled_owner_joins_before_propagating(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            request, _ = self.inputs(root)
            real_sleep = __import__("time").sleep
            called = False
            def cancel_once(seconds):
                nonlocal called
                if not called:
                    called = True
                    raise ProcessCancelled("owner cancelled")
                real_sleep(seconds)
            with patch("run_recovery.time.sleep", side_effect=cancel_once):
                with self.assertRaises(ProcessCancelled):
                    kill_at_boundary([sys.executable, "-c", "import time; time.sleep(60)"], root, root, request, Path(sys.executable), 10)
            self.assertIsInstance(json.loads((root / "process.json").read_bytes())["exit_code"], int)


if __name__ == "__main__":
    unittest.main()
