import copy
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile

import run_mixed_iid_jvm_replay as replay


def framed(payload, offset):
    return struct.pack(">Biq", 0x1E, len(payload), 0) + payload + struct.pack(">q", offset)


def prefix():
    result = []
    for tick in range(40):
        time = 1_577_836_800_000_000 + tick * 6 * 3_600_000_000
        result.append({"tick": tick, "tx_id": tick, "system_time_micros": time,
                       "put": {"ids": list(range(30)), "value": tick, "valid_from_micros": time},
                       "delete": {"ids": [0, 1, 2], "valid_from_micros": time + 3_600_000_000} if tick % 10 == 0 else None})
    return result


def file_record(name, payload):
    return {"path": name, "bytes": len(payload), "sha256": replay.sha256(payload)}


def archive(root):
    result = {"schema_version": 1, "engine": "rust", "mode": "closed-checkpoint", "stage": replay.STAGE, "closed": True,
              "case": replay.UNIQUE_ID, "source_sha256": replay.SOURCE_SHA256, "execution_complete": True,
              "complete_parity": False, "binary_sha256": "b" * 64, "artifact_revision": "a" * 40,
              "source_dirty": False, "storage_version": 6, "storage_epoch": 0, "partitions": 1}
    path = "storage/v06/tables/public$readings/data/l01-example.arrow"
    native_after = b"run-end"
    for index, side in enumerate(replay.CHECKPOINT_SIDES):
        side_root = root / f"mixed-iid-{side}"
        first = framed(bytes([index] * (64 + index)), 0)
        files = {"log/LOG": first + framed(b"flush", len(first)), "log/REPLICA_LOG": framed(b"replica", 0),
                 "storage/v06/blocks/b01.binpb": b"adopted", path: b"plain" if index == 0 else native_after}
        for name, content in files.items():
            file = side_root / name
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes(content)
        result[side] = {"directory": side_root.name, "files": [file_record(name, data) for name, data in files.items()],
                        "first_ticks": prefix(), "log_heads": {"source": len(first), "replica": 0}}
    result["rewritten_files"] = [
        {"path": path, "trie_key": Path(path).stem,
         "before": {"bytes": 5, "sha256": replay.sha256(b"plain"), "encoding": "plain", "pages": [{"rows": 4}]},
         "after": {"bytes": len(native_after), "sha256": replay.sha256(native_after),
                   "encoding": "run-end-i32", "pages": [{"rows": 4, "runs": 1}]}},
    ]
    return result


def recovered(basis):
    return {"source_tip": basis["source_position"], "source_position": basis["source_position"],
            "replica_position": basis["replica_position"] + 1, "public_tx": basis["tx"],
            "live_tx": basis["tx"], "adopted_tx": basis["tx"], "adopted_block": basis["adopted_block"],
            "open_block": basis["adopted_block"] + 1}


def operands():
    iid = {"kind": "fixed-size-binary", "signed_bytes": [-128] + [0] * 15}
    row = [{"xt/id": {"kind": "i64", "value": 1}, "xt/iid": iid}]
    field = {"name": "_iid", "type": replay.FIELD_TYPE, "nullable": False, "metadata": {}}
    flight = {"declared": field, "batch_fields": [field], "batch_count": 2, "iids": [iid]}
    queries = {name: row if name == "sorted-iids" else [{"n": {"kind": "i64", "value": 1}}] for name in replay.OBSERVATIONS}
    result = {"observations": queries, "pgwire": row, "flight": flight}
    reference = {key: [value, value] for key, value in queries.items()}
    reference.update({"pgwire": [row, row], "pgwire-nonempty": [row], "flight-declared": [replay.FIELD_TYPE, replay.FIELD_TYPE],
                      "flight-batch-fields": [[field], [field], [field]], "flight-multiple-batches": [{"value": 1}, {"value": 9}],
                      "flight-iids": [[iid], [iid]]})
    return {"control": copy.deepcopy(result), "mixed": copy.deepcopy(result)}, reference


class MixedIidJvmReplayTests(unittest.TestCase):
    def test_copies_both_native_log_heads_and_rewritten_storage_without_moving_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory).resolve()
            native, workspace = output / "native", output / "workspace"
            execution = archive(native)
            inventory, checkpoint, basis, rewrites = replay.validate_archive(execution)
            replay.verify_native_files(native, inventory, basis)
            replay.stage_jvm_copies(native, workspace, inventory, checkpoint, rewrites)

            case = workspace / replay.CASE_DIRECTORY
            for side in replay.CHECKPOINT_SIDES:
                for source in inventory[side]:
                    original = native / f"mixed-iid-{side}" / source
                    target = case / replay.jvm_path(side, source)
                    self.assertEqual(original.read_bytes(), target.read_bytes())
                self.assertEqual(basis[side]["adopted_block"], 1)
                self.assertEqual(basis[side]["tx"]["tx_id"], 39)
                self.assertGreater(basis[side]["source_position"], 39)
            target = case / "mixed/log/LOG"
            target.write_bytes(b"continued")
            replay.verify_native_files(native, inventory, basis)

    def test_same_sized_rewrite_replacement_and_cross_side_log_tip_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            native = Path(directory).resolve()
            execution = archive(native)
            inventory, _, basis, _ = replay.validate_archive(execution)
            wrong = copy.deepcopy(basis)
            wrong["control"]["source_position"] = basis["mixed"]["source_position"]
            with self.assertRaisesRegex(ValueError, "control LOG differs"):
                replay.verify_native_files(native, inventory, wrong)
            rewrite = native / "mixed-iid-mixed" / execution["rewritten_files"][0]["path"]
            rewrite.write_bytes(b"run-enD")
            with self.assertRaisesRegex(ValueError, "checkpoint file bytes differ"):
                replay.verify_native_files(native, inventory, basis)

    def test_checkpoint_inventory_rejects_missing_catalog_and_wrong_rewrite_path(self):
        with tempfile.TemporaryDirectory() as directory:
            execution = archive(Path(directory).resolve())
            missing = copy.deepcopy(execution)
            missing["control"]["files"] = [
                item for item in missing["control"]["files"] if item["path"] != "storage/v06/blocks/b01.binpb"
            ]
            with self.assertRaisesRegex(ValueError, "has no block catalog"):
                replay.validate_archive(missing)
            execution["rewritten_files"][0]["path"] = execution["rewritten_files"][0]["path"].replace("public$readings", "xt$txs")
            with self.assertRaisesRegex(ValueError, "lacks a unique rewrite file"):
                replay.validate_archive(execution)

    def test_native_stdout_receipt_is_exact_and_json_keys_are_unambiguous(self):
        with tempfile.TemporaryDirectory() as directory:
            execution = archive(Path(directory).resolve())
            data = json.dumps(execution).encode()
            self.assertEqual(replay.verify_native_receipt(data, data, "a" * 40, "b" * 64), execution)
            different_number_type = data.replace(b'"storage_version": 6', b'"storage_version": 6.0')
            with self.assertRaisesRegex(ValueError, "stdout differs"):
                replay.verify_native_receipt(data, different_number_type, "a" * 40, "b" * 64)
            with self.assertRaisesRegex(ValueError, "invalid native mixed IID"):
                replay.verify_native_receipt(different_number_type, different_number_type, "a" * 40, "b" * 64)
            ambiguous = b'{"schema_version":1,' + data[1:]
            with self.assertRaisesRegex(ValueError, "duplicate receipt field"):
                replay.verify_native_receipt(ambiguous, ambiguous, "a" * 40, "b" * 64)

    def test_before_ingest_basis_requires_actual_tick39_and_both_log_watermarks(self):
        basis = {"source_position": 83, "replica_position": 17, "adopted_block": 1,
                 "tx": {"tx_id": 39, "system_time_micros": replay.LAST_PREFIX_TIME}}
        actual = recovered(basis)
        replay.verify_recovered(actual, basis)
        for name, value in (("source_position", 82), ("replica_position", 16), ("public_tx", {"tx_id": 39.0}),
                            ("live_tx", {"tx_id": 38}), ("adopted_tx", {"tx_id": 37}), ("open_block", True)):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "protected tick-39 basis"):
                replay.verify_recovered({**actual, name: value}, basis)

    def test_runtime_checks_nested_class_map_and_the_executing_java(self):
        runtime = {"classpath": [], "absent_classpath": [], "jdk": [], "library": None,
                   "class_locations": sorted(replay.CLASSES), "artifacts": [],
                   "java_home": "/jdk", "java_executable": "/jdk/bin/java", "java_modules": "/jdk/lib/modules"}
        with self.assertRaisesRegex(ValueError, "invalid JVM replay class locations"):
            replay.verify_runtime(runtime)

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            java, modules, jar = home / "bin/java", home / "lib/modules", home / "classes.jar"
            for path in (java, modules, jar):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(path.name.encode())
            adapter = home / "adapter"
            adapter.mkdir()
            runtime.update({"java_home": str(home), "java_executable": str(java), "java_modules": str(modules),
                            "classpath": [str(adapter), str(jar)], "jdk": [str(java), str(modules)],
                            "class_locations": {name: str(jar) for name in replay.CLASSES},
                            "artifacts": [{"kind": "directory", "path": str(adapter), "files": []},
                                          *[dict(kind="file", **file_record(str(path), path.read_bytes()))
                                            for path in (java, modules, jar)]]})
            with patch("run_embedded_migration.JVM", adapter):
                with self.assertRaisesRegex(ValueError, "not a valid JAR"):
                    replay.verify_runtime(runtime)
                with ZipFile(jar, "w") as archive:
                    for name in replay.CLASSES:
                        archive.writestr(name.replace(".", "/") + ".class", b"retained class")
                runtime["artifacts"][-1] = {"kind": "file", **file_record(str(jar), jar.read_bytes())}
                replay.verify_runtime(runtime)
                runtime["java_executable"] = str(jar)
                with self.assertRaisesRegex(ValueError, "does not identify its launcher"):
                    replay.verify_runtime(runtime)

    def test_continuation_compares_each_reached_typed_observation_and_rejects_original_junit_receipt(self):
        execution, reference = operands()
        layout = {"plain", "run-end-i32"}
        checks = replay.compare(execution, reference, layout)
        self.assertTrue(all(check["equal"] for check in checks))
        self.assertEqual({check["id"] for check in checks}, set(replay.ASSERTIONS) - {"rewritten-runs"})
        execution["mixed"]["observations"]["history"] = [{"n": {"kind": "i64", "value": 2}}]
        changed = [check for check in replay.compare(execution, reference, layout) if not check["equal"]]
        self.assertEqual([(check["id"], check["source_line"]) for check in changed], [("history", 180)])
        self.assertIn("actual", changed[0])
        declared = execution["control"]["flight"]["declared"]
        bad_type = {"kind": "fixed-size-binary", "byte_width": 8}
        execution["control"]["flight"]["declared"] = {**declared, "type": bad_type}
        declaration = next(check for check in replay.compare(execution, reference, layout) if check["id"] == "flight-declared")
        self.assertEqual(declaration["actual"], [replay.FIELD_TYPE, bad_type])
        with self.assertRaisesRegex(ValueError, "different execution identity"):
            replay.verify_jvm_execution({"junit": {}, "schema_version": 1}, b"request", b"runtime", {})


if __name__ == "__main__":
    unittest.main()
