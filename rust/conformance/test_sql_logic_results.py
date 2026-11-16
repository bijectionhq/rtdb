import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from run_controls import validate_native_revision

from sql_logic_records import records
from sql_logic_results import check_observation, compare, json_lines, verify_execution
from sql_logic_selections import selections
import io


def query_record():
    original = list(records(io.StringIO("query I nosort\nSELECT 1\n----\n1\n")))[0]
    original["identity"] = {"source": "original.test", "ordinal": 1, "line": 1, "options": {"direct_sql": False}}
    return original


def observation(original, text="1\n", disposition="query_ok"):
    return {"identity": original["identity"], "executed": True, "engine_executed": True, "disposition": disposition,
            "result": {"text": text, "md5": hashlib.md5(text.encode()).hexdigest()}, "query_count_before": 0, "query_count_after": 1}


class SqlLogicEvidence(unittest.TestCase):
    def test_native_receipt_requires_matching_clean_embedded_revision(self):
        validate_native_revision({"artifact_revision": "revision", "source_dirty": False}, "revision")
        for receipt in ({"artifact_revision": "revision", "source_dirty": True},
                        {"artifact_revision": "other", "source_dirty": False},
                        {"artifact_revision": "revision", "artifact_dirty": False}):
            with self.assertRaises(ValueError):
                validate_native_revision(receipt, "revision")

    def test_json_lines_rejects_oversized_and_incomplete_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "observations.jsonl"
            for contents in (b'{"value":"oversized"}\n', b'{}'):
                path.write_bytes(contents)
                with self.assertRaises(ValueError):
                    list(json_lines(path, maximum_bytes=8))
            path.write_bytes(b'{}\n')
            self.assertEqual([{}], list(json_lines(path, maximum_bytes=3)))

    def test_comparison_rejects_missing_or_extra_script_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for reference, native in (([], [root / "native"]), ([root / "jvm"], []),
                                      ([root / "jvm", root / "extra"], [root / "native"])):
                with self.assertRaises(ValueError):
                    compare({"scripts": [{}]}, root, reference, native, root / "report.jsonl")

    def test_missing_identity_execution_and_changed_expected_results_fail_closed(self):
        original = query_record()
        self.assertIsNone(check_observation(original, observation(original)))
        for field, value in (("identity", {}), ("executed", False), ("engine_executed", False), ("result", None)):
            actual = observation(original)
            actual[field] = value
            self.assertIsNotNone(check_observation(original, actual))
        self.assertIsNotNone(check_observation(original, observation(original, "2\n")))

    def test_actual_text_and_md5_must_agree(self):
        original = query_record()
        actual = observation(original)
        actual["result"]["md5"] = "0" * 32
        self.assertIsNotNone(check_observation(original, actual))

    def compare_one(self, reference, native):
        original = query_record()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, value in (("records.jsonl", original), ("jvm.jsonl", reference), ("native.jsonl", native)):
                (root / name).write_text(json.dumps(value) + "\n")
            plan = {"scripts": [{"records_file": "records.jsonl", "records": 1}]}
            return compare(plan, root, [root / "jvm.jsonl"], [root / "native.jsonl"], root / "report.jsonl")

    def test_paired_matching_failure_is_not_passing_coverage(self):
        original = query_record()
        actual = observation(original, "2\n", "query_mismatch")
        report = self.compare_one(actual, actual)
        self.assertEqual(1, report["required_assertions"])
        self.assertEqual(1, report["matching_reference_failure"])
        self.assertEqual(0, report["passing_both"])
        self.assertFalse(report["selection_passed"])

    def test_skips_and_suppression_remain_in_assertion_denominator(self):
        original = query_record()
        for disposition in ("engine_condition", "query_limit", "suppressed_postgresql_error", "harness_gap", "optimizer_mode_unavailable"):
            actual = {"identity": original["identity"], "executed": False, "engine_executed": False, "disposition": disposition}
            report = self.compare_one(actual, actual)
            self.assertEqual((1, 1, 0), (report["required_assertions"], report["missing_evidence"], report["passing_both"]))

    def test_query_limit_counter_and_different_actual_failures_are_mismatches(self):
        original = query_record()
        left, right = observation(original), observation(original)
        right["query_count_after"] = 0
        self.assertEqual(1, self.compare_one(left, right)["missing_evidence"])
        self.assertEqual(1, self.compare_one(observation(original, "2\n", "query_mismatch"), observation(original, "3\n", "query_mismatch"))["mismatch"])

    def test_original_selections_keep_correlated_variant_and_excluded_select5(self):
        default = selections(["slt-test", "slt-test-2", "direct-sql-test"])
        self.assertFalse(any(source == "sqlite-test/select5.test" for _, source, _, _ in default))
        joins = [entry for entry in default if entry[1].endswith("qualified_joins.test")]
        self.assertEqual(2, len(joins))
        self.assertEqual({True, False}, {entry[3] for entry in joins})


if __name__ == "__main__":
    unittest.main()
