import copy
import hashlib
import tempfile
import unittest
from pathlib import Path

from history_results import FIELDS, LAYOUTS, compare_history, verify_artifacts, verify_runtime_artifacts


class HistoryComparisonTests(unittest.TestCase):
    def fixture(self):
        return {"transactions": [{"system_time": "2026-04-01T00:00:00Z"}],
                "prefixes": [{"id": "april", "transactions": 1}],
                "queries": [{"id": "read", "prefix": "april", "valid_time": "2026-05-15T12:00:00Z",
                             "system_time": "2026-05-15T12:00:00Z", "expected_prices": {"A": 12, "B": 7}}]}

    def execution(self):
        time_type = {"kind": "timestamp", "unit": "microsecond", "timezone": "UTC"}
        timestamp = {**time_type, "value": 1775001600000000}
        result = {"schema_version": 1, "execution_complete": True, "fixture_sha256": "a" * 64, "layouts": []}
        query = self.fixture()["queries"][0]
        for layout in LAYOUTS:
            observations = []
            for pruning in ("normal", "bypassed"):
                for product, price in (("A", 12), ("B", 7)):
                    observations.append({"id": f"april/{layout}/{pruning}/read/{product}", "executed": True,
                        "query": "read", "product": product, "pruning": pruning, "valid_time": query["valid_time"],
                        "system_time": query["system_time"], "read_boundary": "object_store_payload", "read_bytes": 0, "read_calls": 0,
                        "schema": [{"name": name, "types": [{"kind": "i64"}] if index == 0 else [time_type, {"kind": "null"}]} for index, name in enumerate(FIELDS)],
                        "rows": [[{"kind": "i64", "value": price}, timestamp, {"kind": "null"}, timestamp, {"kind": "null"}]]})
            result["layouts"].append({"prefix": "april", "layout": layout, "status": "observed", "observations": observations,
                "checkpoint": {"source_position": 5, "replica_position": 10, "open_block": 1,
                    "tx_key": {"system_time_micros": 1775001600000000}, "adopted_block": None if layout == "live" else {"index": 0},
                    "live_product_segments": 1 if layout == "live" else 0,
                    "live_product_tries": [] if layout == "live" else [{"key": "physical-key", "level": 0 if layout == "flushed" else 1}]}})
        return result

    def test_matching_typed_history_is_selection_only(self):
        report = compare_history(self.fixture(), self.execution(), self.execution(), "a" * 64)
        self.assertTrue(report["history_selection_passed"], report)
        self.assertEqual(report["required_observations"], 16)
        self.assertFalse(report["complete_parity"])
        self.assertFalse(report["assertion_ledger_reconciled"])

    def test_equal_wrong_prices_cannot_pass(self):
        bad = self.execution()
        for layout in bad["layouts"]:
            for observation in layout["observations"]:
                observation["rows"] = [[{"kind": "i64", "value": 99}, *observation["rows"][0][1:]]]
        report = compare_history(self.fixture(), bad, bad)
        self.assertFalse(report["history_selection_passed"])
        self.assertTrue(any(failure["field"] == "price" for failure in report["failures"]))

    def test_precise_temporal_bound_difference_is_retained(self):
        original = self.execution()
        changed = copy.deepcopy(original)
        row = changed["layouts"][2]["observations"][1]["rows"][0]
        row[1] = {**row[1], "value": row[1]["value"] + 1}
        report = compare_history(self.fixture(), original, changed)
        self.assertFalse(report["history_selection_passed"])
        self.assertTrue(any(failure["side"] == "comparison" and failure["id"] == "april/compacted/normal/read/B" and failure["field"] == "rows" for failure in report["failures"]))

    def test_missing_duplicate_unexecuted_and_uncompacted_receipts_fail(self):
        mutations = [lambda value: value["layouts"].pop(),
                     lambda value: value["layouts"].append(copy.deepcopy(value["layouts"][0])),
                     lambda value: value["layouts"][0]["observations"][0].update(executed=False),
                     lambda value: value["layouts"][2]["checkpoint"]["live_product_tries"][0].update(level=0),
                     lambda value: value["layouts"][1]["checkpoint"].update(live_product_segments=1),
                     lambda value: value["layouts"][0]["observations"][0].update(rows=[]),
                     lambda value: value.update(execution_complete=False),
                     lambda value: value.update(fixture_sha256="b" * 64)]
        for mutation in mutations:
            changed = self.execution()
            mutation(changed)
            with self.subTest(mutation=mutation):
                self.assertFalse(compare_history(self.fixture(), self.execution(), changed, "a" * 64)["history_selection_passed"])

    def test_unbounded_upper_is_null_but_lower_cannot_be_null(self):
        changed = self.execution()
        changed["layouts"][0]["observations"][0]["rows"][0][1] = {"kind": "null"}
        report = compare_history(self.fixture(), changed, changed)
        self.assertFalse(report["history_selection_passed"])
        self.assertTrue(any(failure["field"] == "temporal_bounds" for failure in report["failures"]))

    def test_equal_wrong_schema_is_rejected(self):
        changed = self.execution()
        for layout in changed["layouts"]:
            for observation in layout["observations"]:
                observation["schema"][0]["types"] = [{"kind": "utf8"}]
        self.assertFalse(compare_history(self.fixture(), changed, changed)["history_selection_passed"])

    def test_retained_checkpoint_verification_requires_exact_file_set_and_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            checkpoint = root / "april-live"
            checkpoint.mkdir()
            (checkpoint / "LOG").write_bytes(b"durable-log")
            execution = {"layouts": [{"prefix": "april", "layout": "live", "artifacts": [{"path": "LOG", "bytes": 11, "sha256": hashlib.sha256(b"durable-log").hexdigest()}]}]}
            verify_artifacts(root, execution)
            (checkpoint / "LOG").write_bytes(b"changed-log")
            with self.assertRaises(ValueError):
                verify_artifacts(root, execution)
            (checkpoint / "LOG").write_bytes(b"durable-log")
            (checkpoint / "extra").write_bytes(b"unexpected")
            with self.assertRaises(ValueError):
                verify_artifacts(root, execution)

    def test_runtime_closure_hashes_actual_classpath_and_rejects_extra_classes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "Example.class").write_bytes(b"class")
            artifacts = [{"kind": "directory", "path": str(root), "files": [
                {"path": "Example.class", "bytes": 5, "sha256": hashlib.sha256(b"class").hexdigest()}]}]
            verify_runtime_artifacts(artifacts)
            (root / "Extra.class").write_bytes(b"different")
            with self.assertRaises(ValueError):
                verify_runtime_artifacts(artifacts)


if __name__ == "__main__":
    unittest.main()
