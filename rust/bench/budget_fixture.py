"""Inputs retained from the pgwire differential runner's load, mem and lat probes."""

import datetime as dt
import random


REFERENCE = "0b9fa440f23d2165250d215209f7fa9bd7c26a6f"
ENGINE_BYTES = 512 << 20
JVM_DIRECT_BYTES = ENGINE_BYTES // 2
QUERY_BYTES = JVM_DIRECT_BYTES * 9 // 10
NATIVE_CACHE_BYTES = ENGINE_BYTES - QUERY_BYTES
BATCH_ROWS = 1000
WARMUPS = 1
SAMPLES = 3
REGIONS = ("eu-west", "eu-north", "us-east", "us-west", "apac", "latam", "mena", "africa")
ACCOUNT_COLUMNS = ("_id", "cust", "region", "balance", "status", "name", "ts", "_valid_from")
SMALL_SQL = (
    "INSERT INTO small (_id, label) SELECT x AS _id, 'L' || CAST(x % 10 AS VARCHAR) AS label "
    "FROM generate_series(0, 99) AS g(x)"
)
REGIONS_SQL = "INSERT INTO regions (_id, rzone) VALUES " + ", ".join(
    f"('{region}', '{region.split('-')[0]}')" for region in REGIONS
)
CASES = (
    {"id": "E2", "scale": 100_000, "input_rows": 10_000_000, "timeout": 300,
     "sql": "SELECT a._id, a.name || b.label AS nn FROM acc100000 a JOIN small b ON true ORDER BY nn DESC LIMIT 5",
     "rows": [[99999, "name-0099999-abL9"] for _ in range(5)], "oids": [20, 25]},
    {"id": "E3", "scale": 100_000, "input_rows": 10_000_000, "timeout": 300,
     "sql": "SELECT COUNT(*) FROM (SELECT a.name || b.label AS nn FROM acc100000 a JOIN small b ON true ORDER BY nn DESC OFFSET 10) x",
     "rows": [[9_999_990]], "oids": [20]},
    {"id": "E1", "scale": 100_000, "input_rows": 10_000_000, "timeout": 300,
     "sql": "SELECT a.name || b.label AS k, COUNT(*) FROM acc100000 a JOIN small b ON true "
            "GROUP BY a.name || b.label ORDER BY 2 DESC, 1 LIMIT 5",
     "rows": [[f"name-0000000-éßL{label}", 10] for label in range(5)], "oids": [25, 20]},
    {"id": "E4", "scale": 100_000, "input_rows": 10_000_000, "timeout": 300,
     "sql": "SELECT COUNT(*) FROM (SELECT DISTINCT a.name, b.label FROM acc100000 a JOIN small b ON true) x",
     "rows": [[1_000_000]], "oids": [20]},
    *({"id": f"L12@{scale}", "scale": scale, "input_rows": scale, "timeout": 120,
       "sql": f"SELECT COUNT(*) FROM (SELECT _id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY balance) "
              f"rn FROM acc{scale}) x WHERE rn <= 10",
       "rows": [[80]], "oids": [20]} for scale in (10_000, 100_000)),
)
COHORTS = (
    (100_000, CASES[:4]),
    (10_000, CASES[4:5]),
    (100_000, CASES[5:]),
)


def dataset(scale):
    rng = random.Random(42)
    epoch = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    customer_count = max(100, scale // 100)
    customers = [(i, f"customer-{i}", ("bronze", "silver", "gold", "platinum")[i % 4])
                 for i in range(customer_count)]
    accounts = [
        (i, rng.randrange(customer_count), REGIONS[i % 8], round(rng.uniform(0, 10000), 2),
         ("open", "closed", "frozen")[i % 3], f"name-{i:07d}-{'éß' if i % 7 == 0 else 'ab'}",
         epoch + dt.timedelta(minutes=i * 7), epoch + dt.timedelta(days=i % 730))
        for i in range(scale)
    ]
    return customers, accounts


def server_configuration(engine):
    return """server: {host: 127.0.0.1, port: 35432}
flightSql: {host: 127.0.0.1, port: 33000}
healthz: {host: 127.0.0.1, port: 38080}
defaultTz: UTC
log: !Local {path: log}
storage: !Local {path: storage}
""" + (f"memoryCache: {{maxSizeBytes: {NATIVE_CACHE_BYTES}}}\n" if engine == "rtdb" else "")
