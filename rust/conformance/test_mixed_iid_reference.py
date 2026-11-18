import argparse
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from run_controls import digest
from run_mixed_iid_reference import (
    ASSERTIONS, FIELD_TYPE, JUNIT_SUCCESS, READINGS_DATA, SOURCE, run, verify_execution,
)


def i64(value):
    return {"kind": "i64", "value": value}


def file_record(path, contents, encoding, pages, workspace):
    file = workspace / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_bytes(contents)
    return {"path": str(path), "bytes": len(contents), "sha256": digest(file), "encoding": encoding, "pages": pages}


def sample_execution(workspace):
    rewritten_key, plain_key = "l01-rc-b00", "l01-rc-b01"
    rewritten_path = READINGS_DATA / f"{rewritten_key}.arrow"
    plain_path = READINGS_DATA / f"{plain_key}.arrow"
    before = file_record(rewritten_path, b"plain before rewrite", "plain", [{"rows": 2}], workspace)
    after = file_record(rewritten_path, b"run encoded after rewrite", "run-end-i32", [{"rows": 2, "runs": 1}], workspace)
    plain = file_record(plain_path, b"plain new file", "plain", [{"rows": 3}], workspace)
    iid = {"kind": "fixed-size-binary", "signed_bytes": [0] * 15 + [-128]}
    iid_rows = [{"xt/id": i64(7), "xt/iid": iid}]
    history = [{"xt/id": i64(7), "xt/valid-from": {
        "kind": "timestamp", "epoch_second": 0, "nanosecond": 0, "zone": "UTC",
    }}]
    aggregate = [{"n": i64(4)}]
    field = {"name": "_iid", "type": FIELD_TYPE, "nullable": False, "metadata": {}}
    operands = {
        "rewritten-runs": [i64(1), i64(2)],
        "run-encoded-live": [[rewritten_key], [rewritten_key, plain_key]],
        "plain-live": [[rewritten_key, plain_key]],
        "sorted-iids": [iid_rows, iid_rows],
        "history": [history, history],
        "iid-join": [aggregate, aggregate],
        "iid-intersect": [aggregate, aggregate],
        "pgwire-nonempty": [iid_rows],
        "pgwire": [iid_rows, iid_rows],
        "patch": [history, history],
        "flight-declared": [FIELD_TYPE, FIELD_TYPE],
        "flight-batch-fields": [[field], [field], [field]],
        "flight-multiple-batches": [i64(1), i64(2)],
        "flight-iids": [[iid], [iid]],
    }
    return {"schema_version": 1, "engine": "jvm", "source": SOURCE, "runtime_unchanged": True,
            "junit": dict(JUNIT_SUCCESS), "assertion_failures": [], "capture_errors": [],
            "assertions": [{"id": identity, "line": line, "status": "passed", "operands": operands[identity]}
                           for identity, line in ASSERTIONS.items()],
            "rewritten_files": [{"before": before, "after": after, "result": {"rows": 2, "runs": 1}}],
            "live_files": [{**after, "trie_key": rewritten_key}, {**plain, "trie_key": plain_key}]}


class MixedIidReferenceTests(unittest.TestCase):
    def test_publishes_only_complete_exact_assertions_and_junit_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory).resolve()
            execution = sample_execution(workspace)
            self.assertEqual(14, verify_execution(execution, workspace))
            execution["junit"]["unique_id"] += "-different"
            with self.assertRaisesRegex(ValueError, "exact original case"):
                verify_execution(execution, workspace)
            execution["junit"] = dict(JUNIT_SUCCESS)
            execution["assertions"].pop()
            with self.assertRaisesRegex(ValueError, "missing original assertion"):
                verify_execution(execution, workspace)

    def test_rejects_unsigned_iid_and_changed_live_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory).resolve()
            execution = sample_execution(workspace)
            pgwire = next(item for item in execution["assertions"] if item["id"] == "pgwire")
            pgwire["operands"][0][0]["xt/iid"]["signed_bytes"][-1] = 128
            with self.assertRaisesRegex(ValueError, "invalid typed result cell"):
                verify_execution(execution, workspace)
            execution = sample_execution(workspace)
            (workspace / execution["live_files"][0]["path"]).write_bytes(b"changed after observation")
            with self.assertRaisesRegex(ValueError, "retained live artifact differs"):
                verify_execution(execution, workspace)

    def test_gate_precedes_the_workspace_and_project_process(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "workspace"
            args = argparse.Namespace(phase_two=False, output=output)
            with patch("run_mixed_iid_reference.run_process") as process:
                with self.assertRaisesRegex(ValueError, "requires --phase-two"):
                    run(args)
                process.assert_not_called()
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
