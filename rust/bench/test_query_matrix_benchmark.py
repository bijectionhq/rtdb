import unittest

from budget_benchmark import ENGINES, REPETITIONS, expected_limits
from clients.performance import rows
from query_matrix_benchmark import (
    compare_case, cpu_ticks, matches, paired_rows, sqlstate_probe_matches, summarize, timing_metrics,
)
from query_matrix_fixture import (
    BEFORE_SYSTEM_TIME, ERROR_SQLSTATE, READ_BASIS, SCALES, account_records, attempt_probe, cases_for_scale,
    updated_records,
)


def observation(case, probe):
    return {"rows": rows(probe["rows"]), "columns": case["columns"], "parameters": probe["parameters"],
            "prepare": case["prepare"]}


class QueryMatrixBenchmarkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cases = {scale: {case["id"]: case for case in cases_for_scale(scale)} for scale in SCALES}

    def test_same_workloads_at_both_scales_pin_temporal_lateral_and_window_results(self):
        self.assertEqual(SCALES, (10_000, 100_000))
        self.assertEqual(tuple(self.cases[10_000]), tuple(self.cases[100_000]))
        for scale, cases in self.cases.items():
            with self.subTest(scale=scale):
                self.assertTrue(all(case["sql"].startswith(READ_BASIS) for case in cases.values()))
                temporal = attempt_probe(cases["sql_temporal_diff_join"], 0)
                self.assertEqual(temporal, {"parameters": [BEFORE_SYSTEM_TIME], "rows": [(scale // 10,)]})
                _, original = account_records(scale)
                current = updated_records(original)
                self.assertEqual([row[0] for before, row in zip(original, current) if before[3] != row[3]],
                                 list(range(scale // 10)))

                lateral = attempt_probe(cases["left_lateral_filtered_unnest"], 0)["rows"]
                n = scale // 4
                self.assertEqual(lateral, [(0, 2 * n, n, 2 * n, 7 * n, 5 * n),
                                           (1, n, 0, 0, None, None), (2, n, 0, 0, None, None),
                                           (3, n, 0, 0, None, None)])
                self.assertEqual(attempt_probe(cases["left_lateral_correlated_aggregate"], 0)["rows"][0][0],
                                 scale // 100)
                window = attempt_probe(cases["window_row_number"], 0)["rows"]
                self.assertEqual(window, attempt_probe(cases["window_row_number_ordered_input"], 0)["rows"])
                self.assertEqual(len(window), 80)
                self.assertEqual(len({row[1] for row in window}), 80)
                self.assertEqual([row[2] for row in window], list(range(1, 11)) * 8)

    def test_named_and_unnamed_point_reads_share_every_argument_and_range_tail_is_truncated(self):
        for scale, cases in self.cases.items():
            named, unnamed = (cases["point_by_id_" + mode] for mode in ("prepared", "unnamed"))
            self.assertEqual((named["sql"], named["probes"]), (unnamed["sql"], unnamed["probes"]))
            self.assertTrue(named["prepare"])
            self.assertFalse(unnamed["prepare"])
            self.assertEqual(len(named["probes"]), named["warmups"] + named["samples"])
            ranges = cases["range_by_id_100"]
            self.assertEqual(attempt_probe(ranges, 0)["parameters"], [scale - 1, scale + 99])
            self.assertEqual(len(attempt_probe(ranges, 0)["rows"]), 1)
            self.assertEqual(len(attempt_probe(ranges, 1)["rows"]), 100)

    def test_rows_require_exact_types_binds_and_order_except_declared_float_reductions(self):
        grouped = self.cases[10_000]["sql_join_group_by"]
        probe = attempt_probe(grouped, 0)
        actual = observation(grouped, probe)
        actual["rows"].reverse()
        tier, count, balance = probe["rows"][-1]
        actual["rows"][0] = rows([(tier, count, balance + 1e-8)])[0]
        self.assertTrue(matches(grouped, actual, probe))
        actual["rows"][0] = rows([(tier, count, balance + 1)])[0]
        self.assertFalse(matches(grouped, actual, probe))

        case = self.cases[10_000]["point_by_id_prepared"]
        probe = attempt_probe(case, 0)
        actual = observation(case, probe)
        for changed in ({"parameters": [0]}, {"prepare": False}, {"error": {"sqlstate": "57014"}},
                        {"columns": [{**column, "oid": 23} for column in case["columns"]]}):
            self.assertFalse(matches(case, {**actual, **changed}, probe))
        window = self.cases[10_000]["window_row_number"]
        probe = attempt_probe(window, 0)
        actual = observation(window, probe)
        actual["rows"].reverse()
        self.assertFalse(matches(window, actual, probe))

    def test_sqlstate_probe_requires_the_named_read_to_recover_after_division_by_zero(self):
        good = {"parameters": [1], "rows": rows([[1]]), "columns": [{"name": "quotient", "oid": 20}]}
        div_zero = {"parameters": [0],
                    "error": {"sqlstate": ERROR_SQLSTATE, "fields": {"message_primary": "data exception - division by zero"}}}
        self.assertTrue(sqlstate_probe_matches([good, div_zero, good]))
        self.assertFalse(sqlstate_probe_matches([good, div_zero]))
        self.assertFalse(sqlstate_probe_matches([good, div_zero, {**good, "connection_broken": True}]))
        self.assertFalse(sqlstate_probe_matches([good, {**div_zero, "error": {"sqlstate": ERROR_SQLSTATE}}, good]))

    def test_cpu_counters_and_nearest_rank_keep_measured_denominator(self):
        first = {"start_ticks": 7, "user_ticks": 100, "system_ticks": 50}
        last = {"start_ticks": 7, "user_ticks": 119, "system_ticks": 51}
        self.assertEqual(cpu_ticks({"before": first, "after": last}), 20)
        self.assertIsNone(cpu_ticks({"before": first, "after": {**last, "start_ticks": 8}}))
        self.assertIsNone(cpu_ticks({"before": last, "after": first}))
        self.assertEqual(timing_metrics([i * 1_000_000 for i in range(1, 101)], 20, 100),
                         {"p50_ms": 50, "p99_ms": 99, "server_cpu_ms_per_op": 2})

    def test_summary_requires_resource_observers_and_all_attempts(self):
        case = {**self.cases[10_000]["sql_temporal_diff_join"], "warmups": 1, "samples": 1}
        sample = {**observation(case, attempt_probe(case, 0)), "elapsed_ns": 1_000_000}
        before = {"start_ticks": 7, "user_ticks": 100, "system_ticks": 50}
        good = {"parameters": [1], "rows": rows([[1]]), "columns": [{"name": "quotient", "oid": 20}]}
        error = {"parameters": [0],
                 "error": {"sqlstate": ERROR_SQLSTATE, "fields": {"message_primary": "division by zero"}}}
        result = {"warmups": [sample], "samples": [sample], "plan": {"rows": rows([["join"]])},
                  "analyze": {"rows": rows([["join"]])}, "query_peak_bytes": 1024,
                  "spill": {"error": None, "nonempty_files": {}},
                  "sample_process_cpu": {"before": before, "after": {**before, "system_ticks": 51}}}
        report = {"engine": "rtdb", "cases": {case["id"]: result}, "clean_shutdown": True,
                  "allocator_before": expected_limits("rtdb"), "allocator_after": expected_limits("rtdb"),
                  "sqlstate_probe": [good, error, good], "process_lifetime_peak_rss_bytes": 4096,
                  "follow_up": {"rows": rows([[1]])}}
        summary = summarize(case, report, 100)
        self.assertTrue(summary["qualifies"])
        self.assertFalse(summary["spill_observed"])
        self.assertEqual(summary["server_cpu_ms_per_op"], 10)
        result["spill"]["error"] = "read failed"
        result["samples"].clear()
        summary = summarize(case, report, 100)
        self.assertFalse(summary["qualifies"])
        self.assertEqual(summary["failures"], ["samples", "spill_observer"])
        self.assertEqual(summary["samples_expected"], 1)
        self.assertEqual(summary["samples_returned"], 0)
        self.assertNotIn("p99_ms", summary)

    def test_a_failed_or_partial_pair_cannot_publish_ratios(self):
        case = {**self.cases[10_000]["sql_temporal_diff_join"], "warmups": 1, "samples": 1}
        good = observation(case, attempt_probe(case, 0))
        reports = {engine: {"cases": {case["id"]: {"warmups": [good], "samples": [good]}}} for engine in ENGINES}
        self.assertTrue(paired_rows(case, reports))
        reports["rtdb"]["cases"][case["id"]]["samples"][0] = {"error": {"sqlstate": "08P01"}}
        self.assertFalse(paired_rows(case, reports))
        summary = {"qualifies": True, "p50_ms": 10, "p99_ms": 25, "server_cpu_ms_per_op": 0}
        repeat = {engine: dict(summary) for engine in ENGINES}
        repeat["rows_and_types_match"] = True
        complete = compare_case([dict(repeat) for _ in range(REPETITIONS)])
        self.assertTrue(complete["qualifies"])
        self.assertEqual(complete["repetitions"][0]["rtdb_to_xtdb"], {"p50_ms": 1, "p99_ms": 1})
        for repetitions in ([dict(repeat)], [dict(repeat), {**repeat, "rows_and_types_match": False}]):
            result = compare_case(repetitions)
            self.assertFalse(result["qualifies"])
            self.assertTrue(all("rtdb_to_xtdb" not in item for item in result["repetitions"]))


if __name__ == "__main__":
    unittest.main()
