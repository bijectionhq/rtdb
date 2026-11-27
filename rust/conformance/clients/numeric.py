import datetime as dt
import hashlib
import json
import platform
import random
import sys
from pathlib import Path

import psycopg
from psycopg.types.numeric import Int8Dumper
from psycopg.types.string import StrDumper


REGIONS = ("eu-west", "eu-north", "us-east", "us-west", "apac", "latam", "mena", "africa")
STATUSES = ("open", "closed", "frozen")
PROJECTIONS = {
    "accounts": "SELECT _id, cust, balance FROM acc100000 ORDER BY _id",
    "customers": "SELECT _id, tier FROM cust100000 ORDER BY _id",
}


def load_join(connection, fixture):
    customers = fixture["customer_rows"]
    tiers = fixture["tiers"]
    rng = random.Random(fixture["seed"])
    epoch = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    customer_rows = [(i, f"customer-{i}", tiers[i % len(tiers)]) for i in range(customers)]
    account_rows = [
        (i, rng.randrange(customers), REGIONS[i % 8], round(rng.uniform(0, 10000), 2),
         STATUSES[i % 3], f"name-{i:07d}-{'éß' if i % 7 == 0 else 'ab'}",
         epoch + dt.timedelta(minutes=i * 7), epoch + dt.timedelta(days=i % 730))
        for i in range(fixture["account_rows"])
    ]
    for sql, rows in [
        ("INSERT INTO cust100000 (_id, name, tier) VALUES (%s, %s, %s)", customer_rows),
        ("INSERT INTO acc100000 (_id, cust, region, balance, status, name, ts, _valid_from)"
         " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", account_rows),
    ]:
        batch = fixture["transaction_rows"]
        for start in range(0, len(rows), batch):
            with connection.transaction(), connection.cursor() as cursor:
                cursor.executemany(sql, rows[start:start + batch])


def describe(result):
    return [
        [result.fname(col).decode("utf-8"), result.ftype(col), result.fformat(col)]
        for col in range(result.nfields)
    ]


def error_record(error):
    if error.sqlstate is None:
        raise error
    return {"kind": "sql_error", "sqlstate": error.sqlstate, "message": error.diag.message_primary,
            "detail": error.diag.message_detail, "hint": error.diag.message_hint}


def query(connection, sql, order, *, projection=None):
    try:
        with connection.cursor(binary=True) as cursor:
            cursor.execute(sql, prepare=False)
            result = cursor.pgresult
            schema = describe(result)
            observation = {"kind": "rows", "sql": sql, "result_order": order, "schema": schema,
                           "row_count": result.ntuples, "status": cursor.statusmessage}
            if projection:
                digest = hashlib.sha256()
                for row in range(result.ntuples):
                    for col in range(result.nfields):
                        value = result.get_value(row, col)
                        if value is None:
                            return {**observation, "invalid_projection": {"null_at": [row, col]}}
                        digest.update(value)
                    if projection == "customers":
                        digest.update(b"\n")
                return {**observation, "sha256": digest.hexdigest()}
            if result.ntuples > 32:
                return {**observation, "invalid_projection": {"retention_limit": 32}}
            rows = []
            for row in range(result.ntuples):
                values = [result.get_value(row, col) for col in range(result.nfields)]
                rows.append([value.hex() if value is not None else None for value in values])
            return {**observation, "raw_rows": rows}
    except psycopg.Error as error:
        return {**error_record(error), "sql": sql, "result_order": order}


def capture(request, fixture, trace):
    node = request["endpoint"]
    with psycopg.connect(host=request["node"], port=node["pg_port"], dbname=node["database"], user=node["user"],
                         autocommit=True, connect_timeout=10) as connection:
        connection.adapters.register_dumper(str, StrDumper)
        connection.adapters.register_dumper(int, Int8Dumper)
        for case in fixture["scalars"]:
            trace["scalars"][case["id"]] = query(connection, f"SELECT {case['expression']} AS v", "singleton")
        try:
            load_join(connection, fixture["join"])
        except psycopg.Error as error:
            trace["load"] = error_record(error)
        else:
            trace["load"] = {"kind": "committed"}
            for table, sql in PROJECTIONS.items():
                trace["projections"][table] = query(connection, sql, "_id_ascending", projection=table)
            for case in fixture["join"]["queries"]:
                trace["joins"][case["id"]] = query(connection, case["sql"], "unspecified")
    trace["client_closed"] = True


def main():
    request_path, fixture_path, output = map(Path, sys.argv[1:])
    payload = request_path.read_bytes()
    request = json.loads(payload)
    fixture = fixture_path.read_bytes()
    actual = hashlib.sha256(fixture).hexdigest()
    if request["fixture_sha256"] != actual:
        raise ValueError("numeric fixture differs from selected digest")
    trace = {"schema_version": 1, "request_sha256": hashlib.sha256(payload).hexdigest(),
             "fixture_sha256": actual, "engine": request["engine"],
             "client": {"python": sys.version, "machine": platform.machine(), "psycopg": psycopg.__version__,
                        "libpq": psycopg.pq.version()},
             "scalars": {}, "load": None, "projections": {}, "joins": {}, "client_closed": False}
    try:
        capture(request, json.loads(fixture), trace)
    finally:
        with output.open("x") as handle:
            json.dump(trace, handle, indent=2, allow_nan=False)


if __name__ == "__main__":
    main()
