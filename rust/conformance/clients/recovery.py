import datetime
import hashlib
import json
import os
import signal
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

if __package__:
    from .performance import Client, cell
else:
    from performance import Client, cell


def timestamp(value):
    return "TIMESTAMP '" + datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat() + "'"


def history_statement(transaction):
    operation = transaction["operation"]
    if operation == "put":
        columns = "_id, price, _valid_from" + (", _valid_to" if "valid_to" in transaction else "")
        values = []
        for product, price in transaction["prices"].items():
            if product not in {"A", "B"} or type(price) is not int:
                raise ValueError("historical recovery fixture contains an unknown product or price")
            fields = ["'" + product + "'", str(price), timestamp(transaction["valid_from"])]
            if "valid_to" in transaction:
                fields.append(timestamp(transaction["valid_to"]))
            values.append("(" + ", ".join(fields) + ")")
        return "INSERT INTO prices (" + columns + ") VALUES " + ", ".join(values)
    if transaction["id"] != "A":
        raise ValueError("historical recovery mutation has an unknown product")
    if operation == "delete":
        return "DELETE FROM prices FOR PORTION OF VALID_TIME FROM " + timestamp(transaction["valid_from"]) + " TO NULL WHERE _id = 'A'"
    if operation == "erase":
        return "ERASE FROM prices WHERE _id = 'A'"
    raise ValueError("unknown historical recovery mutation")


def await_result(client, sql, expected, timeout):
    deadline = time.monotonic() + timeout
    while True:
        value = client.query(sql)
        if value["rows"] == expected:
            return value
        if time.monotonic() >= deadline:
            raise ValueError("recovery query did not reach its required result")
        time.sleep(0.05)


def observe(client, fixture, prefix, canary):
    observed = []
    for query in fixture["queries"]:
        if query["prefix"] != prefix:
            continue
        for product in ("A", "B"):
            sql = ("SELECT price, _valid_from AS valid_from, _valid_to AS valid_to, _system_from AS system_from, _system_to AS system_to "
                   "FROM prices FOR VALID_TIME AS OF " + timestamp(query["valid_time"]) + " FOR SYSTEM_TIME AS OF " + timestamp(query["system_time"]) + " WHERE _id = '" + product + "'")
            value = client.query(sql)
            expected = query["expected_prices"][product]
            if [row[0] for row in value["rows"]] != ([] if expected is None else [cell(expected)]):
                raise ValueError("historical recovery price differs: " + query["id"] + "/" + product)
            for row in value["rows"]:
                for lower, upper, requested in ((row[1], row[2], query["valid_time"]), (row[3], row[4], query["system_time"])):
                    point = datetime.datetime.fromisoformat(requested.replace("Z", "+00:00"))
                    if lower.get("kind") != "timestamp" or not datetime.datetime.fromisoformat(lower["value"]) <= point:
                        raise ValueError("historical lower bound does not contain requested time")
                    if upper.get("kind") != "null" and (upper.get("kind") != "timestamp" or point >= datetime.datetime.fromisoformat(upper["value"])):
                        raise ValueError("historical exclusive upper bound does not contain requested time")
            observed.append({"id": query["id"] + "/" + product, "sql": sql, **value})
    if not observed:
        raise ValueError("recovery prefix has no historical observations")
    value = client.query("SELECT price FROM prices FOR VALID_TIME AS OF TIMESTAMP '2026-10-02T00:00:00Z' WHERE _id = 'C'")
    if value["rows"] != ([[cell(991)]] if canary else []):
        raise ValueError("acknowledged recovery canary differs")
    return {"history": observed, "canary": value, "snapshot": client.query("SHOW SNAPSHOT_TOKEN"),
            "completed": client.query("SHOW LATEST_COMPLETED_TXS"), "catalog": client.query("SELECT trie_key, level, trie_state FROM xt.trie_stats WHERE table_name = 'prices' ORDER BY trie_key")}


def perform(request, client):
    action = request["action"]
    if action == "ready":
        return client.query("SELECT 1 AS ready")
    if action == "attach":
        configuration = Path(request["database_configuration"]).read_text()
        if "$$" in configuration or len(configuration.encode()) > 1024 * 1024:
            raise ValueError("attached database configuration cannot be quoted safely")
        client.execute("ATTACH DATABASE cdc WITH $$" + configuration + "$$")
        return client.query("SHOW LATEST_SUBMITTED_TX")
    if action == "history_step":
        transaction = request["fixture"]["transactions"][request["step"]]
        return client.transaction({"kind": "transaction", "begin_sql": "BEGIN READ WRITE WITH (SYSTEM_TIME = " + timestamp(transaction["system_time"]) + ")",
                                   "statements": [{"sql": history_statement(transaction)}]})
    if action == "canary":
        return client.transaction({"kind": "transaction", "begin_sql": "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP '2026-10-01T00:00:00Z')",
            "statements": [{"sql": "INSERT INTO prices (_id, price, _valid_from) VALUES ('C', 991, TIMESTAMP '2026-10-01T00:00:00Z')"}]})
    if action == "observe":
        return observe(client, request["fixture"], request["prefix"], request["canary"])
    if action == "flush":
        endpoint = request["endpoints"][request["node"]]
        blocks = "SELECT max(block_idx) AS block_idx FROM xt.block_files WHERE block_idx BETWEEN '00' AND '0f'"
        before = client.query(blocks)
        url = f"http://{request['node']}:{endpoint['health_port']}/system/finish-block?db=" + urllib.parse.quote(endpoint["database"], safe="")
        with urllib.request.urlopen(urllib.request.Request(url, method="POST"), timeout=30) as response:
            if not 200 <= response.status < 300 or len(response.read(65537)) > 65536:
                raise ValueError("flush was not admitted")
        deadline = time.monotonic() + request["timeout_seconds"]
        while True:
            after = client.query(blocks)
            live = client.query("SELECT count(*) AS n FROM xt.live_tables WHERE row_count > 0")
            if after["rows"] != before["rows"] and live["rows"] == [[cell(0)]]:
                return {"before": before, "after": after, "live": live}
            if time.monotonic() >= deadline:
                raise ValueError("flush did not adopt a new block and empty live tables")
            time.sleep(0.05)
    if action == "compaction_adopted":
        key = request["trie_key"]
        if not key or not all(char.isalnum() or char in "-_" for char in key):
            raise ValueError("invalid compaction trie key")
        return await_result(client, "SELECT count(*) AS n FROM xt.trie_stats WHERE trie_key = '" + key + "' AND level > 0 AND trie_state = 'live'", [[cell(1)]], request["timeout_seconds"])
    if action == "external_rows":
        schema = request["namespace"].replace("-", "_")
        if not schema.startswith("rtdb_recovery_") or not schema.replace("_", "").isalnum():
            raise ValueError("external source schema is not invocation-scoped")
        table = schema + ".pg_recover_in" if request["external_kind"] == "postgres" else "public.events"
        expected = [[cell(key), cell(name)] for key, name in request["expected"]]
        current = await_result(client, "SELECT _id, name FROM " + table + " ORDER BY _id", expected, request["timeout_seconds"])
        history = client.query("SELECT _id, name, _valid_from, _valid_to, _system_from, _system_to FROM " + table + " FOR ALL VALID_TIME FOR ALL SYSTEM_TIME ORDER BY _id, _system_from, _valid_from")
        return {"current": current, "history": history}
    raise ValueError("unknown packaged recovery client action")


def upstream(request):
    import psycopg
    name = request["namespace"].replace("-", "_")
    if not name.startswith("rtdb_recovery_") or not name.replace("_", "").isalnum():
        raise ValueError("upstream namespace is invalid")
    config = request["postgres"]
    password = os.environ[config["password_environment"]]
    with psycopg.connect(host=config["hostname"], port=config["port"], dbname=config["database"], user=config["username"], password=password,
                         autocommit=True, connect_timeout=10, options="-c statement_timeout=30000") as connection:
        with connection.cursor() as cursor:
            action = request["action"]
            if action == "postgres_absent":
                cursor.execute("SELECT count(*) FROM pg_namespace WHERE nspname=%s", (name,))
                schemas = cursor.fetchone()[0]
                cursor.execute("SELECT count(*) FROM pg_publication WHERE pubname=%s", (name,))
                publications = cursor.fetchone()[0]
                cursor.execute("SELECT count(*) FROM pg_replication_slots WHERE slot_name=%s", (name,))
                slots = cursor.fetchone()[0]
                if schemas or publications or slots:
                    raise ValueError("PostgreSQL recovery namespace is already occupied")
                return {"absent_schema": name, "absent_publication": name, "absent_slot": name}
            if action == "postgres_initialize":
                cursor.execute("CREATE SCHEMA " + name)
                cursor.execute("CREATE TABLE " + name + ".pg_recover_in (_id INT PRIMARY KEY, name TEXT)")
                cursor.execute("CREATE TABLE " + name + ".pg_recover_out (_id INT PRIMARY KEY, name TEXT)")
                cursor.execute("INSERT INTO " + name + ".pg_recover_in VALUES (1, 'snapshot-row')")
                cursor.execute("CREATE PUBLICATION " + name + " FOR TABLE " + name + ".pg_recover_in")
            elif action in {"postgres_insert", "postgres_noise"}:
                table = ".pg_recover_in" if action == "postgres_insert" else ".pg_recover_out"
                cursor.execute("INSERT INTO " + name + table + " VALUES (%s, %s)", (request["id"], request["name"]))
            elif action == "postgres_cleanup":
                cursor.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name=%s", (name,))
                if cursor.fetchone():
                    cursor.execute("SELECT pg_drop_replication_slot(%s)", (name,))
                cursor.execute("DROP PUBLICATION IF EXISTS " + name)
                cursor.execute("DROP SCHEMA IF EXISTS " + name + " CASCADE")
                return {"slot": name, "schema": name, "cleaned": True}
            elif action != "postgres_position":
                raise ValueError("unknown PostgreSQL upstream action")
            cursor.execute("SELECT pg_current_wal_lsn()::text")
            wal = cursor.fetchone()[0]
            cursor.execute("SELECT confirmed_flush_lsn::text, restart_lsn::text, active FROM pg_replication_slots WHERE slot_name=%s", (name,))
            row = cursor.fetchone()
            return {"slot": name, "wal_lsn": wal, "confirmed_flush_lsn": row[0] if row else None, "restart_lsn": row[1] if row else None, "active": row[2] if row else None}


def main():
    input_path, output_path = map(Path, sys.argv[1:])
    raw = input_path.read_bytes()
    if len(raw) > 4 * 1024 * 1024:
        raise ValueError("recovery client request exceeds four MiB")
    request = json.loads(raw)
    if request["schema_version"] != 1 or not 1 <= request["timeout_seconds"] <= 600:
        raise ValueError("invalid recovery request version or timeout")
    started = time.time_ns()
    client = None
    def cancelled(signum, frame):
        if client is not None:
            client.cancel()
        raise InterruptedError("recovery client cancelled")
    signal.signal(signal.SIGTERM, cancelled)
    try:
        if request["action"].startswith("postgres_"):
            value = upstream(request)
        else:
            client = Client({**request, "max_result_rows": 10000, "max_result_bytes": 4 * 1024 * 1024}, request["node"], "pgwire")
            value = perform(request, client)
    finally:
        if client is not None:
            client.close()
    result = {"schema_version": 1, "action": request["action"], "request_sha256": hashlib.sha256(raw).hexdigest(),
              "started_at_ns": started, "completed_at_ns": time.time_ns(), "execution_complete": True, "client_closed": True, "result": value}
    with output_path.open("x") as output:
        json.dump(result, output, allow_nan=False)
        output.write("\n")


if __name__ == "__main__":
    main()
