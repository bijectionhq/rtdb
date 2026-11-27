import copy
import json
import struct
import unittest
from pathlib import Path

from numeric_results import compare, joined, scalar


FIXTURE = json.loads((Path(__file__).parent / "fixtures/numeric-differential.json").read_bytes())


def scalar_observation(case, bits):
    return {"kind": "rows", "schema": [["v", 701, 1]], "row_count": 1,
            "sql": f"SELECT {case['expression']} AS v", "result_order": "singleton", "raw_rows": [[bits]]}


def join_observation(case, rows):
    return {"kind": "rows", "schema": [[name, oid, 1] for name, oid in FIXTURE["join"]["schemas"]["aggregate"]],
            "sql": case["sql"], "result_order": "unspecified", "row_count": len(rows), "raw_rows": rows}


def expected_trace():
    fixture = FIXTURE["join"]
    projections = {}
    for table, expected in fixture["input_projections"].items():
        projections[table] = {"kind": "rows", "result_order": "_id_ascending",
                              "schema": [[name, oid, 1] for name, oid in fixture["schemas"][table]],
                              "row_count": expected["rows"], "sha256": expected["sha256"]}
    rows = [[tier.encode().hex(), struct.pack("!q", group["count"]).hex(),
             struct.pack("!d", group["sum_cents"] / 100).hex()] for tier, group in fixture["groups"].items()]
    scalars = {}
    for case in FIXTURE["scalars"]:
        policy = case["policy"]
        if policy["kind"] == "sql_error":
            result = {"kind": "sql_error", "sql": f"SELECT {case['expression']} AS v", "result_order": "singleton",
                      "sqlstate": policy["sqlstate"], "message": policy["message"],
                      "detail": json.dumps({"category": policy["category"], "code": policy["code"]})}
        else:
            result = scalar_observation(case, policy.get("bits", "7ff8000000000000"))
        scalars[case["id"]] = result
    return {"load": {"kind": "committed"}, "projections": projections, "scalars": scalars,
            "joins": {case["id"]: join_observation(case, copy.deepcopy(rows)) for case in fixture["queries"]}}


class NumericResults(unittest.TestCase):
    def test_scalar_variation_is_bounded_against_the_oracle_on_both_engines(self):
        cases = {case["id"]: case for case in FIXTURE["scalars"]}
        case = cases["sin"]
        self.assertTrue(scalar(case, scalar_observation(case, "3feaed548f090ced"))["qualified"])
        self.assertFalse(scalar(case, scalar_observation(case, "3feaed548f090cec"))["qualified"])
        negative_zero = cases["sin_negative_zero"]
        self.assertFalse(scalar(negative_zero, scalar_observation(negative_zero, "0000000000000000"))["qualified"])
        nan = cases["ln_negative"]
        self.assertTrue(scalar(nan, scalar_observation(nan, "fff8000000000005"))["qualified"])
        self.assertFalse(scalar(nan, scalar_observation(nan, "fff0000000000000"))["qualified"])
        trace = expected_trace()
        trace["scalars"][case["id"]] = scalar_observation(case, "3feaed548f090cec")
        report = compare(FIXTURE, {"jvm": trace, "rust": trace})
        self.assertFalse(report["SQL-13"])
        self.assertTrue(report["scalars"]["sin"]["raw_bits_equal"])

    def test_expected_sql_error_checks_semantics_and_unexpected_error_fails(self):
        jvm, rust = expected_trace(), expected_trace()
        self.assertTrue(compare(FIXTURE, {"jvm": jvm, "rust": rust})["SQL-13"])
        rust["scalars"]["division_by_zero"]["detail"] = '{"category":"incorrect","code":"xtdb/sql-error"}'
        error = {**rust["scalars"]["division_by_zero"], "sql": "SELECT SIN(1.0E0) AS v"}
        rust["scalars"]["sin"] = error
        report = compare(FIXTURE, {"jvm": jvm, "rust": rust})
        self.assertFalse(report["SQL-13"])
        for case in ("division_by_zero", "sin"):
            self.assertFalse(report["scalars"][case]["engines"]["rust"]["qualified"])

    def test_historical_join_variations_keep_position_counts_and_raw_bits(self):
        jvm, rust = expected_trace(), expected_trace()
        case, reverse_case = FIXTURE["join"]["queries"]
        for bits_source, trace in [(125452825.10999979, jvm), (125452825.11000012, rust)]:
            trace["joins"][case["id"]]["raw_rows"][0][2] = struct.pack("!d", bits_source).hex()
        rust["joins"][case["id"]]["raw_rows"].reverse()
        report = compare(FIXTURE, {"jvm": jvm, "rust": rust})
        self.assertTrue(report["T11"])
        result = report["joins"][case["id"]]
        self.assertEqual(result["engines"]["rust"]["delivered_order"], list(reversed(FIXTURE["join"]["groups"])))
        self.assertFalse(result["raw_bits_equal_by_tier"]["bronze"])
        self.assertFalse(report["raw_bits_equal_between_from_orders"]["rust"]["bronze"])
        rust["joins"][reverse_case["id"]]["raw_rows"][0][1] = struct.pack("!q", 25184).hex()
        self.assertFalse(compare(FIXTURE, {"jvm": jvm, "rust": rust})["T11"])

    def test_join_tolerance_preserves_bad_values_duplicate_groups_and_input_failures(self):
        fixture = FIXTURE["join"]
        case = fixture["queries"][0]
        jvm, rust = expected_trace(), expected_trace()
        observed = rust["joins"][case["id"]]
        for value in (125452826.11, float("inf"), float("nan")):
            changed = copy.deepcopy(observed)
            changed["raw_rows"][0][2] = struct.pack("!d", value).hex()
            self.assertFalse(joined(case, fixture, changed)["qualified"])
        changed = copy.deepcopy(observed)
        changed["raw_rows"][0] = changed["raw_rows"][1]
        self.assertFalse(joined(case, fixture, changed)["qualified"])
        changed = copy.deepcopy(observed)
        changed["schema"][2][2] = 0
        self.assertFalse(joined(case, fixture, changed)["qualified"])
        rust["projections"]["accounts"]["sha256"] = "0" * 64
        self.assertFalse(compare(FIXTURE, {"jvm": jvm, "rust": rust})["T11"])


if __name__ == "__main__":
    unittest.main()
