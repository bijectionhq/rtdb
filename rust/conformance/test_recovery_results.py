import copy
import hashlib
import struct
import tempfile
import unittest
from pathlib import Path

from history_results import micros
from recovery_results import clone_closed, compare_recovery, file_manifest, local_log_head, replica_boundary, verify_files, verify_partial
from test_history_results import HistoryComparisonTests


class RecoveryEvidenceTests(unittest.TestCase):
    def receipt(self):
        helper = HistoryComparisonTests()
        fixture = helper.fixture()
        fixture["transactions"][0].update(operation="put", prices={"A": 12, "B": 7}, valid_from="2026-04-01T00:00:00Z")
        baseline = helper.execution()["layouts"][0]
        observations = copy.deepcopy(baseline["observations"])
        for value in observations:
            value["id"] = value["id"].replace("/live/", "/recovery/")
        for product in ("A", "B", "C"):
            value = copy.deepcopy(observations[0 if product == "A" else 1])
            value.update(id=f"april/recovery/normal/current/{product}", query="current", product=product,
                         valid_time="2026-04-01T00:00:00Z", system_time="2026-04-01T00:00:00Z")
            if product == "C":
                value["rows"] = []
            observations.append(value)
        protected = {"prefix": "april", "checkpoint": baseline["checkpoint"], "log_heads": {"replica": 10}}
        execution = {"execution_complete": True, "closure": "graceful", "result": {"checkpoint": baseline["checkpoint"], "observations": observations}}
        return fixture, baseline, execution, protected

    def test_closed_exact_history_and_current_values_are_required(self):
        fixture, baseline, execution, protected = self.receipt()
        self.assertTrue(compare_recovery(fixture, baseline, execution, protected)["recovery_selection_passed"])
        for mutation in (lambda value: value.update(closure="forced_kill"),
                         lambda value: value["result"]["observations"].pop(),
                         lambda value: value["result"]["observations"].append(copy.deepcopy(value["result"]["observations"][0])),
                         lambda value: value["result"]["checkpoint"].update(replica_position=11),
                         lambda value: value["result"]["observations"][0]["rows"][0][1].update(value=0),
                         lambda value: value["result"]["observations"][-2]["rows"][0][0].update(value=99)):
            changed = copy.deepcopy(execution)
            mutation(changed)
            self.assertFalse(compare_recovery(fixture, baseline, changed, protected)["recovery_selection_passed"])

    def test_canary_presence_requires_its_price_and_system_identity(self):
        fixture, baseline, execution, protected = self.receipt()
        canary = {"id": "C", "price": 991, "system_time": "2026-10-01T00:00:00Z", "valid_time": "2026-10-01T00:00:00Z"}
        protected["checkpoint"]["tx_key"]["system_time_micros"] = micros(canary["system_time"])
        for value in execution["result"]["observations"]:
            if value["query"] == "current":
                value["valid_time"] = value["system_time"] = canary["system_time"]
                if value["product"] == "C":
                    value["rows"] = copy.deepcopy(execution["result"]["observations"][0]["rows"])
                    value["rows"][0][0]["value"] = 991
                    value["rows"][0][1]["value"] = value["rows"][0][3]["value"] = micros(canary["system_time"])
        self.assertTrue(compare_recovery(fixture, baseline, execution, protected, canary)["recovery_selection_passed"])
        execution["result"]["observations"][-1]["rows"][0][3]["value"] -= 1
        self.assertFalse(compare_recovery(fixture, baseline, execution, protected, canary)["recovery_selection_passed"])

    def test_independent_transfer_has_exact_bytes_and_no_shared_inode(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            (source / "bytes").write_bytes(b"protected")
            files = file_manifest(source)
            clone = clone_closed(source, root / "clone", files)
            self.assertNotEqual((source / "bytes").stat().st_ino, (clone / "bytes").stat().st_ino)
            (clone / "bytes").write_bytes(b"mutated")
            verify_files(source, files)
            with self.assertRaises(ValueError):
                verify_files(clone, files)
            (source / "link").symlink_to(source / "bytes")
            with self.assertRaises(ValueError):
                file_manifest(source)
            alias = root / "source-alias"
            alias.symlink_to(source, target_is_directory=True)
            with self.assertRaises(ValueError):
                clone_closed(alias, root / "redirected-clone", files)

    def test_local_log_head_validates_every_frame_including_an_incomplete_tail(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "LOG"
            first = b"\x1e" + struct.pack(">iq", 3, 4) + b"abc" + struct.pack(">q", 0)
            second = b"\x1e" + struct.pack(">iq", 0, 5) + struct.pack(">q", len(first))
            path.write_bytes(first + second)
            self.assertEqual(local_log_head(path), len(first))
            for invalid in (first + second[:-1], first + b"\x1e", first[:4] + b"\xff" + first[5:] + second):
                path.write_bytes(invalid)
                with self.assertRaises(ValueError):
                    local_log_head(path)

    def test_partial_retry_must_keep_uploaded_data_and_complete_its_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "storage/v06/tables/products/data/l1.arrow"
            metadata = root / "storage/v06/tables/products/meta/l1.arrow"
            data.parent.mkdir(parents=True)
            data.write_bytes(b"immutable")
            boundary = {"data_key": "tables/products/data/l1.arrow", "metadata_key": "tables/products/meta/l1.arrow", "data_bytes": 9}
            files = file_manifest(root)
            verify_partial(root, boundary)
            metadata.parent.mkdir()
            metadata.write_bytes(b"complete")
            verify_partial(root, boundary, True, files)
            data.write_bytes(b"different")
            with self.assertRaises(ValueError):
                verify_partial(root, boundary, True, files)

    def test_replica_manifest_keeps_term_sequence_and_exact_payload_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "REPLICA_LOG"
            payload = b"\x03\x38\x02\x48\x07\x22\x00"
            path.write_bytes(b"\x1e" + struct.pack(">iq", len(payload), 0) + payload + struct.pack(">q", 0))
            self.assertEqual(replica_boundary(path, local_log_head(path)), {"position": 0, "term": 2, "term_sequence": 7,
                "payload_sha256": hashlib.sha256(payload).hexdigest()})


if __name__ == "__main__":
    unittest.main()
