import unittest

from performance_fixture import fixture
from performance_plan import WORKLOADS, validate_operation


class SharedPackagedFixture(unittest.TestCase):
    def test_all_seven_workloads_and_both_clients_have_actual_operation_traces(self):
        value = fixture(10, 2)
        cases = value["cases"]
        self.assertEqual(WORKLOADS, {case["workload"] for case in cases})
        self.assertEqual({"pgwire", "flight_sql"}, {case["protocol"] for case in cases})
        self.assertEqual({"process_cold", "warm"}, {case["cache_mode"] for case in cases if case["workload"] == "id_lookup"})
        for case in cases:
            for operations in [case["setup"], case["checkpoint"], case["warmup"], case["checks"], *case["workers"]]:
                for operation in operations:
                    validate_operation(operation)
            if case["cache_mode"] == "warm":
                self.assertTrue(case["warmup"])
            self.assertEqual({field: case.get(field, []) for field in ("setup", "checkpoint", "warmup", "workers", "checks", "overlap_observer")}, value["traces"][case["id"]])
            for worker in case["workers"]:
                for operation in worker:
                    if operation["kind"] in {"transaction", "replicated_transaction", "bulk"}:
                        self.assertEqual("2100-01-03T00:00:00Z", operation["system_time_sequence"]["start"])

    def test_http_flush_submission_requires_a_common_adoption_boundary(self):
        with self.assertRaises(ValueError):
            validate_operation({"id": "cut", "kind": "flush", "database": "xtdb"})


if __name__ == "__main__":
    unittest.main()
