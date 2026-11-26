import pathlib
import tempfile
import unittest

from corpus_summary import EXCLUSIONS, Exclusions, load_exclusions, outcome, summarise, upstream_commit_of


def detail(expected_size, actual_size, actual_rows=6):
    block = lambda size, rows: "{:row-count %d, :partitions [{:tries [{:trie-key \"l00-rc-b00\", :data-file-size %d}]}]}" % (rows, size)
    return ("fail [objects/v06/tables/public$xt_docs/blocks/b00.binpb.edn]\n  expected: (= ...)\n  actual: (not (= "
            + block(expected_size, 6) + " " + block(actual_size, actual_rows) + "))")


def metrics(*names):
    return "fail\n  expected: (empty? missing)\n  actual: (not (empty? #{" + " ".join(f'"{name}"' for name in names) + "}))"


EXCLUDED = Exclusions({
    "decisions": {
        "not-a-jvm-node": {"outcome": "jvm-api-boundary", "decision": "the binding's node is not an xtdb.api.Xtdb",
                           "match": [r"cannot be cast to class xtdb\.api\.Xtdb\b"]},
        "layout": {"outcome": "jvm-layout", "decision": "data file sizes differ", "check": "block-catalog-differs-only-in-data-file-size"},
        "metrics": {"outcome": "jvm-internal", "decision": "no JVM gauges", "check": "missing-only-jvm-runtime-metrics"},
    },
    "vars": [{"var": "xtdb.a-test/casts", "decision": "not-a-jvm-node"},
             {"var": "xtdb.indexer-test/test-multi-block-metadata", "decision": "layout"},
             {"var": "xtdb.information-schema-test/test-metrics", "decision": "metrics"},
             {"var": "xtdb.a-test/now-passes", "decision": "not-a-jvm-node"}]})
CAST = "error java.lang.ClassCastException: class xtdb.rust.NativeDatabase cannot be cast to class xtdb.api.Xtdb"


class DataFileLayoutTest(unittest.TestCase):
    def test_a_block_catalog_differing_only_in_data_file_size_is_a_layout_difference(self):
        self.assertEqual(outcome({"var": "xtdb.indexer-test/test-multi-block-metadata", "result": "error", "detail": detail(5638, 5598)}, EXCLUDED), "jvm-layout")

    def test_any_other_block_catalog_difference_still_fails(self):
        self.assertEqual(outcome({"var": "xtdb.indexer-test/test-multi-block-metadata", "result": "fail", "detail": detail(5638, 5598, actual_rows=7)}, EXCLUDED), "fail")


class ExplicitExclusionTest(unittest.TestCase):
    def test_a_var_the_list_does_not_name_fails_whatever_its_failure_says(self):
        self.assertEqual(outcome({"var": "xtdb.a-test/unlisted", "result": "error", "failures": [CAST]}, EXCLUDED), "error")

    def test_a_listed_var_is_excluded_only_when_every_recorded_failure_is_covered(self):
        self.assertEqual(outcome({"var": "xtdb.a-test/casts", "result": "error", "failures": [CAST, CAST]}, EXCLUDED), "jvm-api-boundary")
        self.assertEqual(outcome({"var": "xtdb.a-test/casts", "result": "error", "failures": [CAST, "fail expected 1 actual 2"]}, EXCLUDED), "error")
        self.assertEqual(outcome({"var": "xtdb.a-test/casts", "result": "error", "failures": [CAST], "failures-truncated": True}, EXCLUDED), "error")

    def test_a_metrics_test_missing_a_native_metric_besides_jvm_gauges_fails(self):
        row = {"var": "xtdb.information-schema-test/test-metrics", "result": "fail",
               "failures": [metrics("jvm.memory.used"), metrics("query.warning")]}
        self.assertEqual(outcome(row, EXCLUDED), "fail")
        row["failures"] = [metrics("jvm.memory.used", "xtdb.allocator.memory.allocated")]
        self.assertEqual(outcome(row, EXCLUDED), "jvm-internal")

    def test_a_pass_without_native_code_is_reported_apart_from_exclusions(self):
        self.assertEqual(outcome({"var": "xtdb.a-test/jvm", "result": "jvm-internal"}, EXCLUDED), "pass-jvm-only")
        self.assertEqual(outcome({"var": "xtdb.a-test/jvm", "result": "pass-jvm-only"}, EXCLUDED), "pass-jvm-only")

    def test_the_summary_reports_every_failure_and_entries_needing_review(self):
        rows = [{"var": "xtdb.a-test/casts", "result": "error", "failures": [CAST, "fail expected 1 actual 2"]},
                {"var": "xtdb.a-test/now-passes", "result": "pass"},
                {"var": "xtdb.a-test/fails", "result": "fail", "failures": ["fail a", "fail b", "fail c"]},
                {"var": "xtdb.b-test/legacy", "result": "fail", "detail": "fail only the first"}]
        summary = summarise(rows, EXCLUDED)
        self.assertEqual(summary["totals"]["error"], 1)
        self.assertEqual(summary["totals"]["fail"], 2)
        self.assertEqual(summary["failure_count"], 6)
        self.assertEqual(summary["stale"], ["xtdb.a-test/now-passes"])
        self.assertEqual([(var, count) for var, count, _ in summary["uncovered"]], [("xtdb.a-test/casts", 1)])
        self.assertEqual(summary["legacy_rows"], 1)
        self.assertNotIn("xtdb.indexer-test/test-multi-block-metadata", summary["absent"])

    def test_an_entry_must_name_the_decision_that_justifies_it(self):
        with self.assertRaises(ValueError):
            Exclusions({"decisions": {"vague": {"outcome": "jvm-internal", "decision": " ", "match": ["x"]}}, "vars": []})
        with self.assertRaises(ValueError):
            Exclusions({"decisions": {}, "vars": [{"var": "xtdb.a-test/x", "decision": "missing"}]})
        with self.assertRaises(ValueError):
            Exclusions({"decisions": {"pass": {"outcome": "pass", "decision": "not an exclusion", "match": ["x"]}}, "vars": []})

    def test_the_reviewed_list_is_valid_and_every_entry_is_justified(self):
        shipped = load_exclusions()
        self.assertTrue(shipped.vars)
        for var, entry in shipped.vars.items():
            self.assertTrue(shipped.decisions[entry["decision"]][0]["decision"].strip(), var)
        self.assertEqual(set(shipped.vars), set(EXCLUSIONS.vars))


class UpstreamCommitTest(unittest.TestCase):
    def test_a_run_names_the_upstream_commit_it_loaded_or_none(self):
        with tempfile.TemporaryDirectory() as directory:
            results = pathlib.Path(directory)
            self.assertIsNone(upstream_commit_of(results))
            (results / ".upstream-tests").mkdir()
            (results / ".upstream-tests" / ".commit").write_text("0b9fa440f23d2165250d215209f7fa9bd7c26a6f\n")
            self.assertEqual(upstream_commit_of(results), "0b9fa440f23d2165250d215209f7fa9bd7c26a6f")


if __name__ == "__main__":
    unittest.main()
