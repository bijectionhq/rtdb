import argparse
import json
from pathlib import Path


def integer(value):
    return {"kind": "integer", "value": str(value)}


def query(identity, sql, expected):
    return {"id": identity, "kind": "query", "sql": sql, "expected_rows": expected}


def transaction(identity, time, sql):
    return {"id": identity, "kind": "transaction", "begin_sql": "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP '" + time + "+00:00')",
            "statements": [{"sql": sql}]}


def seed(table, count):
    return transaction("seed", "2100-01-01 00:00:00", "INSERT INTO " + table + " (_id, value, _valid_from) VALUES " +
                       ", ".join(f"({number}, {number * 2}, TIMESTAMP '2020-01-01T00:00:00Z')" for number in range(count)))


def adopted(table, identity="adopted"):
    return {"id": identity, "kind": "await_query", "timeout_seconds": 300,
        "sql": "SELECT count(*) AS remaining FROM xt.live_tables WHERE table_name = '" + table + "' AND row_count > 0",
        "expected_rows": [[integer(0)]]}


def flush(table, identity="flush"):
    return {"id": identity, "kind": "flush", "database": "xtdb", "barrier": adopted(table, identity + "-adoption")}


def fixture(rows=1000, samples=100):
    if type(rows) is not int or not 10 <= rows <= 100_000 or type(samples) is not int or not 1 <= samples <= 100_000:
        raise ValueError("fixture size exceeds explicit source generator limits")
    cases = []
    def add(identity, workload, setup, operations, checks, protocol="pgwire", cache="warm", sample_count=samples, **options):
        warmup = [query("warmup", "SELECT count(*) AS n FROM perf_docs", [[integer(rows)]])] if cache == "warm" else []
        cases.append({"id": identity, "workload": workload, "protocol": protocol, "setup_protocol": "pgwire", "node": "leader",
            "applied_prefix": identity + "/seed-2100-01-01", "layout": "adopted-block", "cache_mode": cache,
            "samples": sample_count, "setup": setup, "checkpoint": [
                {"id": "actual-basis", "kind": "capture_basis", "expected_system_time": "2100-01-02T00:00:00+00:00" if identity in {"bitemporal", "mixed-join"} else "2100-01-01T00:00:00+00:00"},
                {**query("prefix-values", "SELECT count(*) AS n, sum(value) AS total FROM perf_docs", [[integer(rows), integer(rows * (rows - 1))]]), "role": "logical_prefix"},
                {**query("layout-values", "SELECT level, sum(row_count) AS rows FROM xt.trie_stats WHERE table_name = 'perf_docs' AND trie_state = 'live' GROUP BY level ORDER BY level", [[integer(0), integer(rows)]]), "role": "physical_layout"}],
            "warmup": warmup, "workers": [operations], "checks": checks, **options})
    base = [seed("perf_docs", rows), flush("perf_docs"), adopted("perf_docs")]
    lookup = query("lookup", "SELECT value FROM perf_docs WHERE _id = 7", [[integer(14)]])
    check = query("check-count", "SELECT count(*) AS n FROM perf_docs", [[integer(rows)]])
    add("id-cold", "id_lookup", base, [lookup], [check], cache="process_cold")
    add("id-warm", "id_lookup", base, [lookup], [check])
    temporal = [*base, transaction("temporal-seed", "2100-01-02 00:00:00",
        "INSERT INTO perf_temporal (_id, value, _valid_from, _valid_to) VALUES (1, 10, TIMESTAMP '2020-01-01T00:00:00Z', TIMESTAMP '2020-02-01T00:00:00Z'), (2, 20, TIMESTAMP '2020-02-01T00:00:00Z', TIMESTAMP '2020-03-01T00:00:00Z')"),
        flush("perf_temporal", "temporal-flush"), adopted("perf_temporal", "temporal-adopted")]
    period_query = query("range", "SELECT value FROM perf_temporal FOR ALL VALID_TIME FOR SYSTEM_TIME AS OF TIMESTAMP '2100-01-03T00:00:00Z' WHERE _valid_from < TIMESTAMP '2020-03-01T00:00:00Z' AND _valid_to > TIMESTAMP '2020-01-01T00:00:00Z' ORDER BY value", [[integer(10)], [integer(20)]])
    add("bitemporal", "bitemporal_range", temporal, [period_query], [check], protocol="flight_sql")
    mixed = [*base, transaction("mixed-seed", "2100-01-02 00:00:00", "INSERT INTO perf_mixed (_id, join_key, _valid_from) VALUES (1, 7, TIMESTAMP '2020-01-01T00:00:00Z'), (2, 'text', TIMESTAMP '2020-01-01T00:00:00Z'), (3, NULL, TIMESTAMP '2020-01-01T00:00:00Z'), (4, 7.0, TIMESTAMP '2020-01-01T00:00:00Z')"),
        flush("perf_mixed", "mixed-flush"), adopted("perf_mixed", "mixed-adopted")]
    join = query("mixed-join", "SELECT d.value FROM perf_docs d JOIN perf_mixed m ON d._id = m.join_key ORDER BY m._id", [[integer(14)], [integer(14)]])
    add("mixed-join", "mixed_type_join", mixed, [join], [check])
    batch = {"id": "bulk", "kind": "bulk", "sql": "INSERT INTO perf_bulk (_id, value, _valid_from) VALUES (%s, %s, TIMESTAMP '2020-01-01T00:00:00Z')",
             "system_time_sequence": {"start": "2100-01-03T00:00:00Z", "step_microseconds": 1},
             "rows": [[number, number * 3] for number in range(rows)]}
    add("bulk", "bulk_ingest", base, [batch], [query("bulk-count", "SELECT count(*) AS n FROM perf_bulk", [[integer(rows)]])], sample_count=1)
    replicated = {"id": "commit", "kind": "replicated_transaction", "statements": [{"sql": "INSERT INTO perf_replicated (_id, value, _valid_from) VALUES (1, 42, TIMESTAMP '2020-01-01T00:00:00Z')"}],
                  "system_time_sequence": {"start": "2100-01-03T00:00:00Z", "step_microseconds": 1},
                  "read_node": "follower", "verification_sql": "SELECT value FROM perf_replicated WHERE _id = 1", "expected_rows": [[integer(42)]]}
    add("replicated", "replicated_commit", base, [replicated], [check])
    compaction = {"id": "compaction-adopted", "kind": "catalog_adoption", "timeout_seconds": 600,
        "minimum_system_time": "2100-01-03T00:00:00Z",
        "sql": "SELECT t.trie_key, t.temporal_metadata.max_system_from AS max_system_from FROM xt.trie_stats t WHERE t.table_name = 'perf_docs' AND t.level > 0 AND t.trie_state = 'live' AND t.temporal_metadata.max_system_from >= TIMESTAMP '2100-01-03T00:00:00Z' ORDER BY t.trie_key"}
    load = [{"id": "write", "kind": "transaction", "system_time_sequence": {"start": "2100-01-03T00:00:00Z", "step_microseconds": 1}, "statements": [{"sql": "INSERT INTO perf_docs (_id, value, _valid_from) VALUES (7, 14, TIMESTAMP '2020-01-01T00:00:00Z')"}]},
            flush("perf_docs", "cut"), lookup]
    add("compaction", "compaction_under_load", base, load, [check], sample_count=max(16, samples),
        overlap_observer=compaction, layout="compaction-under-concurrent-ingestion")
    add("restore", "restore", base, [lookup], [check], cache="process_cold", sample_count=1, restart_before_measurement=True)
    return {"schema_version": 1, "fixture_kind": "shared-packaged-workloads", "cases": cases,
            "traces": {case["id"]: {field: case.get(field, []) for field in ("setup", "checkpoint", "warmup", "workers", "checks", "overlap_observer")} for case in cases},
            "qualification": "unexecuted_fixture_requires_backend_configs_and_explicit_pilot_budgets"}


def main():
    parser = argparse.ArgumentParser(description="Write deterministic seven-workload traces without opening an engine or supplying credentials.")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(fixture(args.rows, args.samples), stream, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    main()
