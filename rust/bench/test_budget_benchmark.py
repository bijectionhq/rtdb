import unittest

from budget_benchmark import (
    ENGINES, REPETITIONS, compare_repetitions, expected_limits, matches, parse_allocator_limits,
    result_summary, row_digest, rows,
)
from budget_fixture import CASES, COHORTS, ENGINE_BYTES, JVM_DIRECT_BYTES, NATIVE_CACHE_BYTES, QUERY_BYTES, SAMPLES, WARMUPS, dataset


class BudgetBenchmarkTest(unittest.TestCase):
    def test_effective_query_limits_match_inside_the_same_total_budget(self):
        self.assertEqual(ENGINE_BYTES, 536_870_912)
        self.assertEqual(ENGINE_BYTES, QUERY_BYTES + NATIVE_CACHE_BYTES)
        self.assertEqual(ENGINE_BYTES, 2 * JVM_DIRECT_BYTES)
        root = 'xtdb_allocator_root_memory_limit_bytes{node_id="jvm"} 2.41591910E8\n'
        native = (f'xtdb_allocator_memory_limit_bytes{{allocator="query",database=""}} {QUERY_BYTES}\n'
                  f'xtdb_allocator_memory_limit_bytes{{allocator="memory-cache",database=""}} {NATIVE_CACHE_BYTES}\n'
                  'xtdb_allocator_memory_limit_bytes{allocator="buffer-pool",database="xtdb"} 123\n')
        self.assertEqual(parse_allocator_limits("xtdb", root), expected_limits("xtdb"))
        self.assertEqual(parse_allocator_limits("rtdb", native), expected_limits("rtdb"))
        with self.assertRaisesRegex(ValueError, "multiple allocator limit metrics"):
            parse_allocator_limits("xtdb", root + root)

    def test_both_window_scales_and_all_pressure_inputs_keep_the_pinned_cardinalities(self):
        self.assertEqual([(scale, [case["id"] for case in cases]) for scale, cases in COHORTS],
                         [(100_000, ["E2", "E3", "E1", "E4"]), (10_000, ["L12@10000"]), (100_000, ["L12@100000"])])
        for case in CASES[:4]:
            self.assertEqual(case["input_rows"], 10_000_000)
        self.assertEqual(CASES[1]["rows"], [[9_999_990]])
        self.assertEqual(CASES[3]["rows"], [[1_000_000]])
        for case in CASES[4:]:
            self.assertEqual(case["rows"], [[80]])
            self.assertIn(f"FROM acc{case['scale']}", case["sql"])

        customers, accounts = dataset(100_000)
        self.assertEqual((len(customers), len(accounts)), (1_000, 100_000))
        self.assertEqual(accounts[0][:6], (0, 654, "eu-west", 1113.31, "open", "name-0000000-éß"))
        self.assertEqual(accounts[-1][5], "name-0099999-ab")
        self.assertEqual(len({record[5] for record in accounts}), 100_000)

    def test_setup_digest_is_order_and_value_sensitive_across_batch_boundaries(self):
        accounts = dataset(10_000)[1][:2]
        self.assertEqual(row_digest([accounts]), row_digest([[accounts[0]], [accounts[1]]]))
        self.assertNotEqual(row_digest([accounts]), row_digest([accounts[::-1]]))
        self.assertNotEqual(row_digest([[(1,)]]), row_digest([[(True,)]]))

    @staticmethod
    def result(case):
        sample = {"rows": rows(case["rows"]), "columns": [{"oid": oid} for oid in case["oids"]], "elapsed_ns": 1_000_000}
        return {
            "engine": "rtdb", "allocator_before": expected_limits("rtdb"), "allocator_after": expected_limits("rtdb"),
            "clean_shutdown": True, "follow_up": {"rows": rows([[1]])},
            "cases": {case["id"]: {
                "warmups": [dict(sample) for _ in range(WARMUPS)],
                "samples": [dict(sample) for _ in range(SAMPLES)],
                "plan": {"rows": rows([["order-by"]])},
                "analyze": {"columns": [{"name": "op"}], "rows": rows([["order-by"]])},
                "query_peak_bytes": QUERY_BYTES, "spill": {"nonempty_files": {".tmp1/.tmp2": 4096}},
            }},
        }

    def test_wrong_rows_oids_and_cancellations_cannot_count_toward_throughput(self):
        case = CASES[0]
        for changes in ({"rows": rows(case["rows"][:-1])}, {"columns": [{"oid": 25}, {"oid": 20}]},
                        {"timeout": True}, {"error": {"sqlstate": "XX000"}}, {"server_exit": -9}):
            with self.subTest(changes=changes):
                report = self.result(case)
                sample = report["cases"][case["id"]]["samples"][0]
                sample.update(changes)
                self.assertFalse(matches(case, sample))
                summary = result_summary(case, report)
                self.assertEqual(summary["excluded_from_throughput"], ["samples"])
                self.assertEqual(summary["samples_returned"], SAMPLES)
                self.assertNotIn("queries_per_second", summary)

    def test_external_sort_requires_positive_spill_and_observed_effective_ceilings(self):
        case = CASES[1]
        report = self.result(case)
        self.assertTrue(result_summary(case, report)["qualifies"])
        report["cases"]["E3"]["spill"]["nonempty_files"].clear()
        report["allocator_after"]["query"] += 1
        self.assertEqual(result_summary(case, report)["excluded_from_throughput"],
                         ["external_sort_spill_unobserved", "effective_budget"])

    def test_missing_repetition_excludes_the_entire_pair_from_latency_comparison(self):
        result = {"qualifies": True, "median_ms": 1.0}
        comparison = {"E1": {engine: [result] * REPETITIONS for engine in ENGINES}}
        self.assertEqual(compare_repetitions(comparison)["E1"]["rtdb_to_xtdb_latency_ratio"], 1.0)
        comparison["E1"]["rtdb"].pop()
        self.assertFalse(compare_repetitions(comparison)["E1"]["qualifies"])
        self.assertNotIn("rtdb_to_xtdb_latency_ratio", compare_repetitions(comparison)["E1"])


if __name__ == "__main__":
    unittest.main()
