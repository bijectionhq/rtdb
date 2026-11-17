import copy
import unittest

from performance_plan import operation_trace_digest
from performance_results import distribution, inspect_case, inspect_overlap


def measured_case():
    operation = {"id": "lookup", "kind": "query", "sql": "SELECT 1", "expected_rows": [[{"kind": "integer", "value": "1"}]]}
    case = {"id": "lookup", "workload": "id_lookup", "cache_mode": "warm", "samples": 1,
            "setup": [], "checkpoint": [{**operation, "id": "prefix"}], "workers": [[operation]], "checks": [operation]}
    client = {"execution_complete": True, "client_closed": True, "elapsed_ns": 1000,
              "samples": [{"worker": 0, "round": 0, "operation": "lookup", "elapsed_ns": 100,
                           "value": {"schema": [{"name": "one", "oid": 20}], "rows": copy.deepcopy(operation["expected_rows"])}}]}
    provider = {"boundary": "successful_object_http_payload_and_requests", "observer_artifact_sha256": "a" * 64,
                "storage_placement": "same-zone", "successful_read_bytes": 0, "successful_read_requests": 0,
                "successful_write_bytes": 0, "successful_write_requests": 0}
    process = {"peak_rss_bytes": 10000, "peak_boundary": "engine_process_lifetime_through_sample"}
    receipt = {"case": "lookup", "trace_sha256": operation_trace_digest(case), "resources": {}, "execution_complete": True,
               "drained": True, "checkpoint": copy.deepcopy({**client, "samples": [{**client["samples"][0], "operation": "prefix"}]}),
               "rounds": [{"client": client, "processes": [process], "provider": provider}]}
    return case, receipt


class PerformanceEvidence(unittest.TestCase):
    def test_compaction_after_the_load_or_without_later_ingestion_cannot_qualify(self):
        observer = {"id": "adopted", "minimum_system_time": "2100-01-03T00:00:00Z"}
        empty = {"schema": [{"name": "trie_key"}, {"name": "max_system_from"}], "rows": []}
        catalog = {**empty, "rows": [[{"kind": "text", "value": "new-trie"}, {"kind": "timestamp", "value": "2100-01-03T00:00:00+00:00"}]]}
        case = {"overlap_observer": observer, "workers": [[{"id": "write", "kind": "transaction"}]]}
        result = {"started_ns": 10, "finished_ns": 100, "elapsed_ns": 90,
                  "overlap": {"operation": "adopted", "observed_ns": 50, "commits_before": 1, "value": catalog,
                              "baseline": {"captured_ns": 15, "value": empty}, "new_trie_keys": ["new-trie"]},
                  "samples": [{"worker": 0, "operation": "write", "started_ns": 20, "completed_ns": 30, "elapsed_ns": 10},
                              {"worker": 0, "operation": "write", "started_ns": 60, "completed_ns": 70, "elapsed_ns": 10}]}
        inspect_overlap(case, result)
        preexisting = copy.deepcopy(result)
        preexisting["overlap"]["baseline"]["value"] = catalog
        with self.assertRaisesRegex(ValueError, "newly adopted"):
            inspect_overlap(case, preexisting)
        seed_only = copy.deepcopy(result)
        seed_only["overlap"]["value"]["rows"][0][1]["value"] = "2100-01-01T00:00:00+00:00"
        with self.assertRaisesRegex(ValueError, "predates measured ingestion"):
            inspect_overlap(case, seed_only)
        for time in (5, 75, 110):
            wrong = copy.deepcopy(result)
            wrong["overlap"]["observed_ns"] = time
            with self.subTest(time=time), self.assertRaises(ValueError):
                inspect_overlap(case, wrong)
        wrong = copy.deepcopy(result)
        wrong.pop("overlap")
        with self.assertRaises(ValueError):
            inspect_overlap(case, wrong)

    def test_pinned_percentiles_do_not_substitute_another_nearest_rank_definition(self):
        result = distribution([10, 20])
        self.assertEqual((10, 10, 10), (result["p50_ns"], result["p95_ns"], result["p99_ns"]))
        with self.assertRaises(ValueError):
            distribution([])
        with self.assertRaises(ValueError):
            distribution([True])

    def test_missing_provider_boundary_and_unjoined_owner_cannot_qualify(self):
        case, receipt = measured_case()
        self.assertEqual([], inspect_case(case, receipt, {})[0])
        receipt["rounds"][0]["provider"] = None
        self.assertIn("missing comparable successful provider measurement", inspect_case(case, receipt, {})[0])
        receipt["drained"] = False
        self.assertIn("execution, resource equality or owner drain", inspect_case(case, receipt, {})[0])

    def test_equal_wrong_results_fail_against_immutable_expectations(self):
        case, receipt = measured_case()
        receipt["rounds"][0]["client"]["samples"][0]["value"]["rows"][0][0]["value"] = "2"
        self.assertEqual(receipt["checkpoint"]["samples"][0]["value"]["rows"], [[{"kind": "integer", "value": "1"}]])
        self.assertEqual(case["workers"][0][0]["expected_rows"], [[{"kind": "integer", "value": "1"}]])
        self.assertIn("query differs from immutable expected rows", inspect_case(case, receipt, {})[0])

    def test_duplicate_samples_and_missing_cold_restarts_preserve_denominator(self):
        case, receipt = measured_case()
        receipt["rounds"][0]["client"]["samples"].append(copy.deepcopy(receipt["rounds"][0]["client"]["samples"][0]))
        with self.assertRaises(ValueError):
            inspect_case(case, receipt, {})
        case, receipt = measured_case()
        case.update(cache_mode="process_cold", samples=2)
        receipt["trace_sha256"] = operation_trace_digest(case)
        self.assertIn("missing cold-process or warm round", inspect_case(case, receipt, {})[0])

    def test_labels_without_actual_prefix_checkpoint_cannot_qualify(self):
        case, receipt = measured_case()
        del receipt["checkpoint"]
        with self.assertRaises(ValueError):
            inspect_case(case, receipt, {})


if __name__ == "__main__":
    unittest.main()
