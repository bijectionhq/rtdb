import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from run_sql_logic_native import UPSTREAM_COMMIT, assertion_results
from sql_logic_records import write_records


class NativeSqlLogicSummaryTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.records_path = self.root / "records.jsonl"
        self.observations = self.root / "observations.jsonl"

    def plan(self, source, query_limit=None):
        (self.root / "fixture.test").write_text(source)
        options = {"direct_sql": True, "decorrelate": True, "query_limit": query_limit, "max_result_bytes": 1024}
        return write_records(self.root, "fixture.test", UPSTREAM_COMMIT, "fixture", options, self.records_path)

    def observe(self, *fields):
        originals = [json.loads(line) for line in self.records_path.read_text().splitlines()]
        with self.observations.open("w") as stream:
            for original, values in zip(originals, fields):
                stream.write(json.dumps({"identity": original["identity"], **values}) + "\n")

    def counts(self, totals):
        return tuple(totals[key] for key in ("passed", "failed", "excluded", "unobserved"))

    def test_query_limit_counts_only_assertions_and_counts_the_missing_tail(self):
        plan = self.plan("query I\nSELECT 1\n----\n1\n\nhash-threshold 2\n\nquery I\nSELECT 2\n----\n2\n\n"
                         "query I\nSELECT 3\n----\n3\n", query_limit=1)
        self.observe({"disposition": "query_ok", "executed": True, "engine_executed": True,
                      "query_count_before": 0, "query_count_after": 1,
                      "result": {"text": "1\n", "md5": hashlib.md5(b"1\n").hexdigest()}},
                     {"disposition": "query_limit", "engine_executed": False},
                     {"disposition": "query_limit", "engine_executed": False})
        with self.observations.open("a") as stream:
            stream.write('{"identity":')
        result = assertion_results(plan, self.records_path, self.observations)
        self.assertEqual(self.counts(result), (1, 1, 1, 1))
        self.assertEqual(result["dispositions"], {"query_ok": 1, "query_limit": 1, "unobserved": 1})
        self.assertIn("incomplete SQL logic observation line", result["evidence_errors"])

    def test_eager_parse_failure_cannot_exclude_assertions(self):
        plan = self.plan("statement maybe\nSELECT 1\n\nhash-threshold 2\n")
        unavailable = {"disposition": "script_parse_error", "engine_executed": False}
        self.observe(unavailable, unavailable)
        result = assertion_results(plan, self.records_path, self.observations)
        self.assertEqual(self.counts(result), (0, 1, 0, 0))
        self.assertEqual(result["dispositions"], {"script_parse_error": 1})

    def test_wrong_identity_cannot_be_counted_as_an_exclusion(self):
        plan = self.plan("query I\nSELECT 1\n----\n1\n")
        self.observe({"identity": {}, "disposition": "query_limit", "engine_executed": False})
        result = assertion_results(plan, self.records_path, self.observations)
        self.assertEqual(self.counts(result), (0, 1, 0, 1))
        self.assertIn("identity", result["evidence_errors"][0])
