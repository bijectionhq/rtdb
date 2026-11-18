import argparse
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from run_controls import digest
from run_mixed_iid_reference import (
    ASSERTIONS, CASE_DIRECTORY, CHECKPOINT_LOGS, FIELD_TYPE, JUNIT_SUCCESS, READINGS_DATA, SOURCE, is_block_catalog, run,
    verify_execution,
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


def sample_checkpoint(workspace, execution):
    archive = workspace.parent / "checkpoint"
    postimage = execution["rewritten_files"][0]["after"]
    rewrite = (workspace / postimage["path"]).relative_to(workspace / CASE_DIRECTORY)
    files = {path: str(path).encode() for path in CHECKPOINT_LOGS}
    files.update({
        "control/objects/v06/blocks/b00.binpb": b"control block",
        "control/objects/v06/tables/public$readings/data/l01-rc-b00.arrow": b"control data",
        "mixed/objects/v06/blocks/b00.binpb": b"mixed block",
        rewrite: (workspace / postimage["path"]).read_bytes(),
    })
    inventory = []
    for path, contents in files.items():
        target = archive / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents)
        inventory.append({"path": str(path), "bytes": len(contents), "sha256": digest(target)})
    execution.update(schema_version=2, checkpoint={
        "schema_version": 1, "stage": "after-rewrite-before-reopen", "files": inventory,
    })
    return archive, rewrite


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

    def test_current_reference_requires_the_pre_reopen_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory).resolve() / "workspace"
            execution = sample_execution(workspace)
            self.assertEqual(14, verify_execution(execution, workspace))
            with self.assertRaisesRegex(ValueError, "exact original case"):
                verify_execution(execution, workspace, require_checkpoint=True)
            sample_checkpoint(workspace, execution)

            live_log = workspace / CASE_DIRECTORY / "mixed/log/LOG"
            live_log.parent.mkdir(parents=True)
            live_log.write_bytes(b"the original invocation continued after the checkpoint")
            self.assertEqual(14, verify_execution(execution, workspace, require_checkpoint=True))
            execution["checkpoint"]["stage"] = "after-reopen"
            with self.assertRaisesRegex(ValueError, "checkpoint identity"):
                verify_execution(execution, workspace)

    def test_checkpoint_requires_every_retained_file_with_exact_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory).resolve() / "workspace"
            execution = sample_execution(workspace)
            archive, _ = sample_checkpoint(workspace, execution)
            object_file = archive / "control/objects/v06/blocks/b00.binpb"
            original = object_file.read_bytes()
            object_file.write_bytes(b"x" * len(original))
            with self.assertRaisesRegex(ValueError, "checkpoint file bytes differ"):
                verify_execution(execution, workspace)
            object_file.unlink()
            with self.assertRaisesRegex(ValueError, "omitted retained files"):
                verify_execution(execution, workspace)
            object_file.write_bytes(original)
            addition = object_file.with_name("b01.binpb")
            addition.write_bytes(b"unrecorded block")
            with self.assertRaisesRegex(ValueError, "unexpected or non-regular checkpoint file"):
                verify_execution(execution, workspace)

    def test_checkpoint_rejects_a_self_consistent_replacement_of_the_rewrite(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory).resolve() / "workspace"
            execution = sample_execution(workspace)
            archive, rewrite = sample_checkpoint(workspace, execution)
            target = archive / rewrite
            target.write_bytes(b"different run-end file")
            record = next(entry for entry in execution["checkpoint"]["files"] if entry["path"] == str(rewrite))
            record.update(bytes=target.stat().st_size, sha256=digest(target))
            with self.assertRaisesRegex(ValueError, "mixed rewrite postimage"):
                verify_execution(execution, workspace)

    def test_checkpoint_requires_a_block_catalog_even_when_other_control_objects_remain(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory).resolve() / "workspace"
            execution = sample_execution(workspace)
            archive, _ = sample_checkpoint(workspace, execution)
            for catalog_path in ("control/objects/v06/blocks/b00.binpb", "mixed/objects/v06/blocks/b00.binpb"):
                with self.subTest(catalog=catalog_path):
                    target = archive / catalog_path
                    contents = target.read_bytes()
                    target.unlink()
                    target.parent.rmdir()
                    inventory = execution["checkpoint"]["files"]
                    record = next(entry for entry in inventory if entry["path"] == catalog_path)
                    inventory.remove(record)
                    with self.assertRaisesRegex(ValueError, "checkpoint omitted a nonempty block catalog"):
                        verify_execution(execution, workspace)
                    target.parent.mkdir()
                    target.write_bytes(contents)
                    inventory.append(record)

    def test_block_catalog_names_require_canonical_signed_lex_hex_positions(self):
        for name, valid in (
            ("b00.binpb", True), ("b010.binpb", False), ("b110.binpb", True), ("b100.binpb", False),
            ("bf7fffffffffffffff.binpb", True), ("bf8000000000000000.binpb", False),
            ("b0A.binpb", False), ("b110.arrow", False),
        ):
            with self.subTest(name=name):
                self.assertIs(is_block_catalog(name), valid)

    def test_checkpoint_rejects_traversal_symlinks_and_nonregular_files(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory).resolve() / "workspace"
            execution = sample_execution(workspace)
            archive, _ = sample_checkpoint(workspace, execution)
            record = execution["checkpoint"]["files"][0]
            path = record["path"]
            record["path"] = "mixed/objects/v06/../../../outside"
            with self.assertRaisesRegex(ValueError, "invalid or repeated checkpoint path"):
                verify_execution(execution, workspace)
            record["path"] = path
            log = archive / path
            outside = workspace.parent / "outside"
            outside.write_bytes(log.read_bytes())
            log.unlink()
            log.symlink_to(outside)
            with self.assertRaisesRegex(ValueError, "unexpected or non-regular checkpoint file"):
                verify_execution(execution, workspace)
            log.unlink()
            os.mkfifo(log)
            with self.assertRaisesRegex(ValueError, "unexpected or non-regular checkpoint file"):
                verify_execution(execution, workspace)

    def test_checkpoint_rejects_an_unreadable_directory_and_an_oversize_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory).resolve() / "workspace"
            execution = sample_execution(workspace)
            sample_checkpoint(workspace, execution)
            with patch("run_mixed_iid_reference.os.scandir", side_effect=PermissionError("unreadable checkpoint")):
                with self.assertRaisesRegex(PermissionError, "unreadable checkpoint"):
                    verify_execution(execution, workspace)
            with patch("run_mixed_iid_reference.CHECKPOINT_MAX_BYTES", 1):
                with self.assertRaisesRegex(ValueError, "checkpoint byte limit exceeded"):
                    verify_execution(execution, workspace)

    def test_reference_result_binds_the_four_validated_receipts_and_two_process_logs(self):
        changed_names = ("inputs.json", "process.json", "runtime-artifacts.json", "execution.json")
        for changed in (None, *changed_names):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                reference, output = root / "reference", root / "output"
                source = reference / SOURCE
                source.parent.mkdir(parents=True)
                source.write_bytes(b"original fixture")
                args = argparse.Namespace(phase_two=True, timeout=1, reference=reference, output=output)

                def complete(_command, _reference, stdout, stderr, _timeout):
                    workspace = output / "workspace"
                    execution = sample_execution(workspace)
                    sample_checkpoint(workspace, execution)
                    (output / "execution.json").write_text(json.dumps(execution), encoding="utf-8")
                    (output / "runtime-artifacts.json").write_bytes(b"[]")
                    stdout.write_bytes(b"worker stdout")
                    stderr.write_bytes(b"worker stderr")
                    return {"completed": True, "exit_code": 0}

                def replace_receipt(_inputs):
                    if changed is not None:
                        receipt = output / changed
                        receipt.write_bytes(receipt.read_bytes() + b"\n")

                result_path = output / "result.json"
                with (patch("run_mixed_iid_reference.require_phase_two"),
                      patch("run_mixed_iid_reference.git", return_value="revision"),
                      patch("run_mixed_iid_reference.require_revision"),
                      patch("run_mixed_iid_reference.SOURCE_SHA256", digest(source)),
                      patch("run_mixed_iid_reference.input_receipt", return_value=[]),
                      patch("run_mixed_iid_reference.run_process", side_effect=complete),
                      patch("run_mixed_iid_reference.verify_runtime_artifacts"),
                      patch("run_mixed_iid_reference.verify_inputs", side_effect=replace_receipt)):
                    if changed is None:
                        run(args)
                        result = json.loads(result_path.read_bytes())
                        self.assertEqual(result["sha256"], {
                            name: digest(output / name) for name in (*changed_names, "stderr.log", "stdout.log")
                        })
                    else:
                        with self.assertRaisesRegex(ValueError, f"mixed IID receipt changed during verification: {changed}"):
                            run(args)
                        self.assertFalse(result_path.exists())

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
