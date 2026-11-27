import datetime as dt
import math
import random
from collections import defaultdict

from budget_fixture import dataset


SCALES = (10_000, 100_000)
WARMUPS = 10
SAMPLES = 100
TIMEOUT = 120
CPU_POINT_SAMPLES = 500
SEED_START = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
LISTS_SYSTEM_TIME = "2023-01-02T00:00:00Z"
BEFORE_SYSTEM_TIME = "2024-01-01T00:00:00Z"
UPDATE_SYSTEM_TIME = "2024-01-02T00:00:00Z"
READ_SYSTEM_TIME = "2024-01-03T00:00:00Z"
UPDATE_VALID_TIME = "2022-01-01T00:00:00Z"
READ_VALID_TIME = "2024-01-03T00:00:00Z"
READ_BASIS = (f"SETTING DEFAULT VALID_TIME AS OF TIMESTAMP '{READ_VALID_TIME}', "
              f"DEFAULT SYSTEM_TIME AS OF TIMESTAMP '{READ_SYSTEM_TIME}' ")
ACCOUNT_COLUMNS = ("_id", "cust", "region", "balance", "status", "_valid_from")
CUSTOMER_COLUMNS = ("_id", "name", "tier")
LISTS_SQL = (
    "INSERT INTO replay_lists (_id, items) SELECT _id, CASE _id % 4 "
    "WHEN 0 THEN ARRAY[CAST(3 AS BIGINT), NULL, 7, 9] WHEN 1 THEN ARRAY[1, 2] "
    "WHEN 2 THEN ARRAY[] ELSE NULL END FROM accounts"
)
UPDATE_SQL = (
    f"UPDATE accounts FOR PORTION OF VALID_TIME FROM TIMESTAMP '{UPDATE_VALID_TIME}' TO NULL "
    "SET balance = balance + 1, status = 'closed' WHERE _id < %s"
)
ERROR_SQL = "SELECT CAST(1 AS BIGINT) / CAST(%s AS BIGINT) AS quotient"
ERROR_SQLSTATE = "08P01"
FLOAT_SUM_TOLERANCE = {"rel_tol": 1e-11, "abs_tol": 1e-9}


def account_records(scale):
    customers, accounts = dataset(scale)
    return customers, [(*row[:5], row[-1]) for row in accounts]


def updated_records(accounts):
    cutoff = len(accounts) // 10
    valid_from = dt.datetime.fromisoformat(UPDATE_VALID_TIME)
    return [(key, customer, region, balance + 1, "closed", valid_from)
            if key < cutoff else (key, customer, region, balance, status, original_valid_from)
            for key, customer, region, balance, status, original_valid_from in accounts]


def probe(expected_rows, parameters=None):
    return {"parameters": None if parameters is None else list(parameters), "rows": expected_rows}


def case(identifier, sql, columns, probes, *, prepare=False, ordered=False, reductions=(), samples=SAMPLES):
    return {"id": identifier, "sql": READ_BASIS + sql, "columns": [{"name": name, "oid": oid} for name, oid in columns],
            "probes": probes, "prepare": prepare, "ordered": ordered, "float_reduction_columns": reductions,
            "warmups": WARMUPS, "samples": samples, "timeout_seconds": TIMEOUT}


def cases_for_scale(scale):
    customers, original = account_records(scale)
    accounts = updated_records(original)
    tiers = defaultdict(list)
    regions = defaultdict(list)
    groups = defaultdict(list)
    for key, customer, region, balance, status, _ in accounts:
        tiers[customers[customer][2]].append(balance)
        groups[region, status].append(balance)
        regions[region].append((balance, key))

    rng = random.Random(43)
    keys = [scale - 1, 0] + [rng.randrange(scale) for _ in range(CPU_POINT_SAMPLES + WARMUPS - 2)]
    id_ranges, balance_ranges, points = [], [], []
    for index, key in enumerate(keys):
        points.append(probe([accounts[key][:5]], (key,)))
        if index < SAMPLES + WARMUPS:
            end = key + 100
            id_ranges.append(probe([(row[0], row[3]) for row in accounts[key:end]], (key, end)))
            lower = float((index * 97) % 9900)
            upper = lower + 100.0
            selected = [row[3] for row in accounts if lower <= row[3] <= upper]
            balance_ranges.append(probe([(len(selected), math.fsum(selected) if selected else None)], (lower, upper)))

    point_sql = "SELECT _id, cust, region, balance, status FROM accounts WHERE _id = %s"
    point_columns = (("_id", 20), ("cust", 20), ("region", 25), ("balance", 701), ("status", 25))
    window_columns = (("region", 25), ("_id", 20), ("rn", 20))
    window_rows = [(region, key, rank)
                   for region, entries in sorted(regions.items())
                   for rank, (_, key) in enumerate(sorted(entries)[:10], 1)]
    window_sql = ("SELECT region, _id, rn FROM (SELECT region, _id, "
                  "ROW_NUMBER() OVER (PARTITION BY region ORDER BY balance, _id) AS rn "
                  "FROM {source}) w WHERE rn <= 10 ORDER BY region, rn")
    parent_limit = scale // 100
    matched_ids = [row[1] for row in accounts[:parent_limit] if customers[row[1]][2] == "gold"]
    per_shape = scale // 4

    return (
        case("sql_temporal_diff_join",
             "SELECT count(*) AS n FROM accounts a "
             "JOIN accounts FOR SYSTEM_TIME AS OF CAST(%s AS TIMESTAMP WITH TIME ZONE) b "
             "ON a._id = b._id WHERE a.balance <> b.balance",
             (("n", 20),), [probe([(scale // 10,)], (BEFORE_SYSTEM_TIME,))], prepare=True),
        case("sql_join_group_by", "SELECT c.tier, count(*) AS n, sum(a.balance) AS total "
             "FROM accounts a JOIN customers c ON a.cust = c._id GROUP BY c.tier",
             (("tier", 25), ("n", 20), ("total", 701)),
             [probe([(tier, len(balances), math.fsum(balances)) for tier, balances in sorted(tiers.items())])],
             reductions=(2,)),
        case("sql_group_by", "SELECT region, status, count(*) AS n, sum(balance) AS total, avg(balance) AS mean "
             "FROM accounts GROUP BY region, status",
             (("region", 25), ("status", 25), ("n", 20), ("total", 701), ("mean", 701)),
             [probe([(*key, len(balances), math.fsum(balances), math.fsum(balances) / len(balances))
                     for key, balances in sorted(groups.items())])], reductions=(3, 4)),
        case("range_by_id_100", "SELECT _id, balance FROM accounts WHERE _id >= %s AND _id < %s",
             (("_id", 20), ("balance", 701)), id_ranges, prepare=True),
        case("range_scan_balance_1pct", "SELECT count(*) AS n, sum(balance) AS total FROM accounts "
             "WHERE balance BETWEEN %s AND %s", (("n", 20), ("total", 701)), balance_ranges,
             prepare=True, reductions=(1,)),
        case("point_by_id_unnamed", point_sql, point_columns, points, samples=CPU_POINT_SAMPLES),
        case("point_by_id_prepared", point_sql, point_columns, points, prepare=True, samples=CPU_POINT_SAMPLES),
        case("sql_top_100", "SELECT _id, balance FROM accounts ORDER BY balance DESC, _id LIMIT 100",
             (("_id", 20), ("balance", 701)),
             [probe([(row[0], row[3]) for row in sorted(accounts, key=lambda row: (-row[3], row[0]))[:100]])],
             ordered=True),
        case("left_lateral_filtered_unnest",
             "SELECT p._id % 4 AS shape, count(*) AS n, count(u.item) AS items, count(u.ord) AS ordinals, "
             "sum(u.item) AS item_total, sum(CAST(u.ord AS BIGINT)) AS ordinal_total FROM replay_lists p "
             "LEFT JOIN UNNEST(p.items) WITH ORDINALITY AS u(item, ord) "
             "ON u.ord <= 3 AND (u.item >= 7 OR u.item IS NULL) GROUP BY p._id % 4",
             tuple((name, 20) for name in ("shape", "n", "items", "ordinals", "item_total", "ordinal_total")),
             [probe([(0, 2 * per_shape, per_shape, 2 * per_shape, 7 * per_shape, 5 * per_shape),
                     *((shape, per_shape, 0, 0, None, None) for shape in (1, 2, 3))])]),
        case("left_lateral_correlated_aggregate",
             "SELECT count(*) AS n, count(c.customer_id) AS matched, sum(c.customer_id) AS id_total "
             "FROM (SELECT cust FROM accounts WHERE _id < %s) a LEFT JOIN LATERAL "
             "(SELECT max(c._id) AS customer_id FROM customers c WHERE c._id = a.cust AND c.tier = 'gold') c ON TRUE",
             (("n", 20), ("matched", 20), ("id_total", 20)),
             [probe([(parent_limit, len(matched_ids), sum(matched_ids) if matched_ids else None)], (parent_limit,))],
             prepare=True),
        case("window_row_number", window_sql.format(source="accounts"), window_columns, [probe(window_rows)],
             ordered=True),
        case("window_row_number_ordered_input",
             window_sql.format(source="(SELECT region, _id, balance FROM accounts ORDER BY region, balance, _id) s"),
             window_columns, [probe(window_rows)], ordered=True),
    )


def attempt_probe(case, index):
    return case["probes"][index % len(case["probes"])]
