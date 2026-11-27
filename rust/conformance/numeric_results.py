import json
import math
import re
import struct
from fractions import Fraction


def float64(bits):
    if not isinstance(bits, str) or not re.fullmatch("[0-9a-f]{16}", bits):
        raise ValueError("expected eight raw bytes for binary64")
    return struct.unpack("!d", bytes.fromhex(bits))[0]


def rows_match(observed, schema, count):
    return (observed.get("kind") == "rows"
            and observed.get("schema") == [[name, oid, 1] for name, oid in schema]
            and observed.get("row_count") == count)


def scalar(case, observed):
    policy = case["policy"]
    kind = policy["kind"]
    if (observed.get("sql") != f"SELECT {case['expression']} AS v"
            or observed.get("result_order") != "singleton"):
        return {"qualified": False, "reason": "wrong scalar query or order metadata"}
    if kind == "sql_error":
        try:
            detail = json.loads(observed.get("detail") or "null")
        except ValueError:
            detail = None
        fields = {"sqlstate": observed.get("sqlstate"), "message": observed.get("message"),
                  "category": detail.get("category") if isinstance(detail, dict) else None,
                  "code": detail.get("code") if isinstance(detail, dict) else None}
        return {"qualified": observed.get("kind") == "sql_error" and
                all(value == policy[key] for key, value in fields.items()), **fields}
    rows = observed.get("raw_rows")
    if (not rows_match(observed, [["v", 701]], 1) or not isinstance(rows, list) or len(rows) != 1
            or not isinstance(rows[0], list) or len(rows[0]) != 1):
        return {"qualified": False, "reason": "expected one binary64 cell"}
    bits = rows[0][0]
    try:
        value = float64(bits)
    except ValueError:
        return {"qualified": False, "reason": "invalid binary64 cell"}
    if kind == "exact_bits":
        return {"qualified": bits == policy["bits"], "bits": bits}
    if kind == "nan":
        return {"qualified": math.isnan(value), "bits": bits}
    if kind != "platform_ulp":
        raise ValueError(f"unknown scalar policy {kind}")
    if not math.isfinite(value):
        return {"qualified": False, "bits": bits, "reason": "expected a finite result"}
    ulps = abs(int(bits, 16) - int(policy["bits"], 16))
    return {"qualified": ulps <= policy["max_ulps"], "bits": bits, "ulps_from_rounded_oracle": ulps}


def sum_group(group, count, bits):
    value = float64(bits)
    if not math.isfinite(value):
        return {"qualified": False, "count": count, "bits": bits, "reason": "expected a finite sum"}
    ideal = Fraction(group["sum_cents"], 100)
    terms = group["count"]
    bound = ideal * Fraction(terms, 2**53 - terms)
    error = abs(Fraction(value) - ideal)
    return {"qualified": count == terms and error <= bound, "count": count, "bits": bits,
            "absolute_error": str(error), "max_absolute_error": str(bound)}


def joined(case, fixture, observed):
    want = fixture["groups"]
    rows = observed.get("raw_rows")
    if (observed.get("sql") != case["sql"] or observed.get("result_order") != "unspecified"
            or not rows_match(observed, fixture["schemas"]["aggregate"], len(want))
            or not isinstance(rows, list) or len(rows) != len(want)):
        return {"qualified": False, "reason": "expected one result row per tier with binary text, int8 and float8"}
    groups = {}
    try:
        for tier_hex, count_hex, bits in rows:
            tier = bytes.fromhex(tier_hex).decode("utf-8")
            count, = struct.unpack("!q", bytes.fromhex(count_hex))
            if tier not in want or tier in groups:
                raise ValueError("unexpected or duplicate tier")
            groups[tier] = sum_group(want[tier], count, bits)
    except (TypeError, ValueError, struct.error) as error:
        return {"qualified": False, "reason": str(error)}
    return {"qualified": all(group["qualified"] for group in groups.values()),
            "delivered_order": list(groups), "groups": groups}


def equal_group_bits(left, right):
    if left is None or right is None:
        return None
    return {tier: left[tier]["bits"] == right[tier]["bits"] for tier in left}


def projection_matches(fixture, table, observed):
    expected = fixture["input_projections"][table]
    return (rows_match(observed, fixture["schemas"][table], expected["rows"])
            and observed.get("result_order") == "_id_ascending" and observed.get("sha256") == expected["sha256"])


def compare(fixture, traces):
    engines = ("jvm", "rust")
    projections = {engine: {
        "load_committed": traces[engine]["load"] == {"kind": "committed"},
        **{table: projection_matches(fixture["join"], table, traces[engine]["projections"].get(table, {}))
           for table in fixture["join"]["input_projections"]},
    } for engine in engines}
    scalars = {}
    for case in fixture["scalars"]:
        results = {engine: scalar(case, traces[engine]["scalars"].get(case["id"], {})) for engine in engines}
        bits = [results[engine].get("bits") for engine in engines]
        scalars[case["id"]] = {"policy": case["policy"], "engines": results,
                               "raw_bits_equal": bits[0] == bits[1] if None not in bits else None}
    joins = {}
    for case in fixture["join"]["queries"]:
        results = {engine: joined(case, fixture["join"], traces[engine]["joins"].get(case["id"], {}))
                   for engine in engines}
        joins[case["id"]] = {"engines": results,
                            "raw_bits_equal_by_tier": equal_group_bits(results["jvm"].get("groups"),
                                                                      results["rust"].get("groups"))}
    first, second = joins.values()
    equal_between_orders = {
        engine: equal_group_bits(first["engines"][engine].get("groups"), second["engines"][engine].get("groups"))
        for engine in engines
    }
    return {
        "schema_version": 1, "complete_parity": False,
        "input_checks": projections, "scalars": scalars, "joins": joins,
        "raw_bits_equal_between_from_orders": equal_between_orders,
        "join_policy": {"kind": "binary64_positive_sum_roundoff",
                        "bound": "exact decimal sum * count / (2^53 - count)",
                        "groups_compared_by": "tier", "result_order": "unspecified",
                        "expected_groups": fixture["join"]["groups"]},
        "SQL-13": all(result["qualified"] for case in scalars.values() for result in case["engines"].values()),
        "T11": all(all(checks.values()) for checks in projections.values())
        and all(result["qualified"] for case in joins.values() for result in case["engines"].values()),
    }
