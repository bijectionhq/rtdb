import concurrent.futures
import datetime
import decimal
import hashlib
import json
import signal
import struct
import sys
import threading
import time
import urllib.request
from pathlib import Path


def cell(value):
    if value is None:
        return {"kind": "null"}
    if type(value) is bool:
        return {"kind": "bool", "value": value}
    if type(value) is int:
        return {"kind": "integer", "value": str(value)}
    if type(value) is float:
        return {"kind": "float64", "bits": struct.pack(">d", value).hex()}
    if isinstance(value, decimal.Decimal):
        sign, digits, exponent = value.as_tuple()
        return {"kind": "decimal", "sign": sign, "digits": list(digits), "exponent": exponent}
    if isinstance(value, datetime.datetime):
        return {"kind": "timestamp", "value": value.isoformat(), "timezone": str(value.tzinfo) if value.tzinfo else None}
    if isinstance(value, (datetime.date, datetime.time)):
        return {"kind": type(value).__name__, "value": value.isoformat()}
    if isinstance(value, str):
        return {"kind": "text", "value": value}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"kind": "binary", "hex": bytes(value).hex()}
    if isinstance(value, (list, tuple)):
        return {"kind": "list", "value": [cell(item) for item in value]}
    if isinstance(value, dict):
        return {"kind": "map", "entries": [[cell(key), cell(item)] for key, item in value.items()]}
    raise ValueError("unqualified benchmark result type: " + type(value).__name__)


def rows(values):
    return [[cell(value) for value in row] for row in values]


def adoption_catalog(value, operation):
    names = [field["name"] for field in value["schema"]]
    if names != ["trie_key", "max_system_from"]:
        raise ValueError("compaction observation lacks exact trie identity and temporal columns")
    minimum = datetime.datetime.fromisoformat(operation["minimum_system_time"].replace("Z", "+00:00"))
    found = {}
    for row in value["rows"]:
        if len(row) != 2 or row[0].get("kind") != "text" or not row[0].get("value") or row[1].get("kind") != "timestamp":
            raise ValueError("compaction observation has invalid trie identity or temporal evidence")
        time_ = datetime.datetime.fromisoformat(row[1]["value"].replace("Z", "+00:00"))
        if time_.utcoffset() is None or time_ < minimum or row[0]["value"] in found:
            raise ValueError("compaction observation predates measured ingestion or duplicates a trie")
        found[row[0]["value"]] = row[1]
    return found


class Client:
    def __init__(self, request, node, protocol):
        self.protocol = protocol
        self.request = request
        self.node = node
        endpoint = request["endpoints"][node]
        password = Path(endpoint["password_file"]).read_text().rstrip("\n") if endpoint.get("password_file") else None
        if protocol == "pgwire":
            import psycopg
            if not psycopg.capabilities.has_cancel_safe():
                raise ValueError("benchmark cancellation requires libpq 17 or newer")
            self.connection = psycopg.connect(host=node, port=endpoint["pg_port"], dbname=endpoint["database"],
                user=endpoint["user"], password=password, autocommit=True, connect_timeout=10)
        else:
            import adbc_driver_flightsql.dbapi as flightsql
            options = {"adbc.flight.sql.rpc.call_header.x-xtdb-database": endpoint["database"],
                       "adbc.flight.sql.rpc.with_cookie_middleware": "true"}
            if endpoint.get("user"):
                options["username"] = endpoint["user"]
            if password is not None:
                options["password"] = password
            self.connection = flightsql.connect(f"grpc://{node}:{endpoint['flight_port']}", db_kwargs=options, autocommit=True)

    def close(self):
        self.connection.close()

    def cancel(self):
        if self.protocol == "pgwire":
            self.connection.cancel_safe(timeout=5)
        else:
            self.connection.adbc_cancel()

    def query(self, sql, parameters=()):
        result, retained_bytes = [], 0
        def append(batch):
            nonlocal retained_bytes
            if len(result) + len(batch) > self.request["max_result_rows"]:
                raise ValueError("benchmark result exceeds retained row bound")
            converted = rows(batch)
            retained_bytes += len(json.dumps(converted, allow_nan=False).encode())
            if retained_bytes > self.request["max_result_bytes"]:
                raise ValueError("benchmark result exceeds retained byte bound")
            result.extend(converted)
        with self.connection.cursor() as cursor:
            if self.protocol == "pgwire":
                cursor.execute(sql, parameters or None, prepare=False)
                schema = [{"name": column.name, "oid": column.type_code} for column in cursor.description]
                while batch := cursor.fetchmany(1024):
                    append(batch)
            else:
                cursor.execute(sql, parameters or None)
                reader = cursor.fetch_record_batch()
                schema = [{"name": field.name, "type": str(field.type), "nullable": field.nullable,
                           "metadata": sorted((key.hex(), value.hex()) for key, value in (field.metadata or {}).items())} for field in reader.schema]
                with reader:
                    for batch in reader:
                        if len(result) + batch.num_rows > self.request["max_result_rows"]:
                            raise ValueError("benchmark result exceeds retained row bound")
                        columns = [column.to_pylist() for column in batch.columns]
                        append(list(zip(*columns)) if columns else [() for _ in range(batch.num_rows)])
            return {"schema": schema, "rows": result}

    def execute(self, sql, parameters=()):
        with self.connection.cursor() as cursor:
            if self.protocol == "pgwire":
                cursor.execute(sql, parameters or None, prepare=False)
            else:
                cursor.execute(sql, parameters or None)

    def transaction(self, operation):
        begin = operation.get("begin_sql")
        if begin:
            if self.protocol != "pgwire":
                raise ValueError("Flight transactions use ADBC transaction APIs, not SQL BEGIN")
            self.execute(begin)
        else:
            self.connection.autocommit = False
        try:
            if operation["kind"] == "bulk":
                with self.connection.cursor() as cursor:
                    cursor.executemany(operation["sql"], operation["rows"])
            else:
                for statement in operation["statements"]:
                    self.execute(statement["sql"], statement.get("parameters", []))
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        finally:
            self.connection.autocommit = True
        acknowledgement = self.query("SHOW LATEST_SUBMITTED_TX")
        names = [column["name"] for column in acknowledgement["schema"]]
        if "committed" not in names or len(acknowledgement["rows"]) != 1 or acknowledgement["rows"][0][names.index("committed")] != {"kind": "bool", "value": True}:
            raise ValueError("transaction lacks a committed read-back acknowledgement")
        return acknowledgement


class Execution:
    def __init__(self, request):
        self.request = request
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.clients = set()
        self.retained_bytes = 0
        self.active_workers = 0
        self.commits = 0

    def cancelled(self):
        if self.stop.is_set():
            raise InterruptedError("benchmark invocation cancelled")

    def cancel(self):
        self.stop.set()
        with self.lock:
            for client in self.clients:
                try:
                    client.cancel()
                except Exception:
                    pass

    def operation(self, client, operation):
        self.cancelled()
        kind = operation["kind"]
        if kind in {"query", "await_query"}:
            deadline = time.monotonic() + operation.get("timeout_seconds", 0)
            while True:
                result = client.query(operation["sql"], operation.get("parameters", []))
                if result["rows"] == operation["expected_rows"]:
                    return result
                if kind != "await_query" or time.monotonic() >= deadline:
                    raise ValueError("query differs from immutable expected rows: " + operation["id"])
                if self.stop.wait(0.05):
                    self.cancelled()
        if kind == "capture_basis":
            completed = client.query("SHOW LATEST_COMPLETED_TXS")
            columns = [field["name"] for field in completed["schema"]]
            logical = [{name: row[columns.index(name)] for name in ("db_name", "part", "system_time")} for row in completed["rows"]]
            database = self.request["endpoints"][client.node]["database"]
            selected = [value for value in logical if value["db_name"] == {"kind": "text", "value": database}]
            if not selected or any(value["system_time"].get("value") != operation["expected_system_time"] for value in selected):
                raise ValueError("actual applied basis differs from the immutable seeded prefix")
            return {"snapshot": client.query("SHOW SNAPSHOT_TOKEN"), "completed": completed, "logical_completed": selected}
        if kind == "flush":
            from urllib.parse import quote
            endpoint = self.request["endpoints"][client.node]
            url = f"http://{client.node}:{endpoint['health_port']}/system/finish-block?db=" + quote(operation["database"], safe="")
            with urllib.request.urlopen(urllib.request.Request(url, method="POST"), timeout=30) as response:
                if len(response.read(65537)) > 65536 or not 200 <= response.status < 300:
                    raise ValueError("flush submission response is invalid")
            return {"adopted": self.operation(client, operation["barrier"])}
        acknowledgement = client.transaction(operation)
        if kind != "replicated_transaction":
            return {"acknowledgement": acknowledgement}
        token = client.query("SHOW AWAIT_TOKEN")["rows"][0][0]
        if token.get("kind") != "text":
            raise ValueError("replicated transaction omitted its await token")
        replica = Client(self.request, operation["read_node"], client.protocol)
        with self.lock:
            self.clients.add(replica)
        try:
            replica.execute("SET AWAIT_TOKEN = '" + token["value"].replace("'", "''") + "'")
            result = replica.query(operation["verification_sql"])
            if result["rows"] != operation["expected_rows"]:
                raise ValueError("second node did not observe the acknowledged write")
            return {"acknowledgement": acknowledgement, "second_node": result, "await_token": token}
        finally:
            with self.lock:
                self.clients.remove(replica)
            replica.close()

    def worker(self, number, operations, barrier):
        selected = self.request["case"]
        protocol = selected.get("setup_protocol", selected["protocol"]) if self.request["phase"] == "setup" else selected["protocol"]
        client = Client(self.request, selected["node"], protocol)
        with self.lock:
            self.clients.add(client)
        try:
            if barrier is not None:
                barrier.wait(timeout=30)
            result = []
            rounds = self.request["case"]["samples"] if self.request["phase"] == "measure" else 1
            for iteration in range(rounds):
                for operation in operations:
                    self.cancelled()
                    operation = dict(operation)
                    if operation.get("system_time_sequence"):
                        sequence = operation["system_time_sequence"]
                        instant = datetime.datetime.fromisoformat(sequence["start"].replace("Z", "+00:00")) + datetime.timedelta(microseconds=iteration * sequence["step_microseconds"])
                        operation["begin_sql"] = "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP '" + instant.isoformat() + "')"
                    started = time.perf_counter_ns()
                    value = self.operation(client, operation)
                    completed = time.perf_counter_ns()
                    elapsed = completed - started
                    size = len(json.dumps(value, allow_nan=False).encode()) + 256
                    with self.lock:
                        self.retained_bytes += size
                        if self.retained_bytes > self.request["max_receipt_bytes"]:
                            raise ValueError("benchmark sample receipts exceed retained byte bound")
                        if operation["kind"] in {"transaction", "bulk", "replicated_transaction"}:
                            self.commits += 1
                    result.append({"worker": number, "round": iteration, "operation": operation["id"], "elapsed_ns": elapsed,
                                   "started_ns": started, "completed_ns": completed, "value": value})
            return result
        except BaseException:
            self.stop.set()
            if barrier is not None:
                barrier.abort()
            raise
        finally:
            with self.lock:
                self.clients.remove(client)
                self.active_workers -= 1
            client.close()

    def observe_overlap(self, operation, barrier):
        selected = self.request["case"]
        client = Client(self.request, selected["node"], selected["protocol"])
        with self.lock:
            self.clients.add(client)
        try:
            baseline = client.query(operation["sql"], operation.get("parameters", []))
            initial = adoption_catalog(baseline, operation)
            baseline_time = time.perf_counter_ns()
            barrier.wait(timeout=30)
            deadline = time.monotonic() + operation["timeout_seconds"]
            while True:
                self.cancelled()
                with self.lock:
                    if self.active_workers == 0:
                        raise ValueError("compaction was not observed during measured ingestion")
                result = client.query(operation["sql"], operation.get("parameters", []))
                new_keys = sorted(set(adoption_catalog(result, operation)) - set(initial))
                with self.lock:
                    if new_keys and self.commits > 0 and self.active_workers > 0:
                        size = len(json.dumps([baseline, result], allow_nan=False).encode()) + 512
                        self.retained_bytes += size
                        if self.retained_bytes > self.request["max_receipt_bytes"]:
                            raise ValueError("compaction observation exceeds retained byte bound")
                        return {"operation": operation["id"], "observed_ns": time.perf_counter_ns(),
                                "baseline": {"captured_ns": baseline_time, "value": baseline}, "new_trie_keys": new_keys,
                                "commits_before": self.commits, "value": result}
                if time.monotonic() >= deadline:
                    raise ValueError("compaction did not overlap the bounded measured load")
                if self.stop.wait(0.01):
                    self.cancelled()
        except BaseException:
            self.stop.set()
            barrier.abort()
            raise
        finally:
            with self.lock:
                self.clients.remove(client)
            client.close()

    def run(self):
        case, phase = self.request["case"], self.request["phase"]
        if phase == "ready":
            deadline = time.monotonic() + 90
            while True:
                self.cancelled()
                try:
                    client = Client(self.request, case["node"], "pgwire")
                    try:
                        result = client.query("SELECT 1")
                        if result["rows"] != [[{"kind": "integer", "value": "1"}]]:
                            raise ValueError("invalid readiness query result")
                        return {"samples": [], "elapsed_ns": 0}
                    finally:
                        client.close()
                except InterruptedError:
                    raise
                except Exception:
                    if time.monotonic() >= deadline:
                        raise
                    self.stop.wait(0.1)
        workers = case["workers"] if phase == "measure" else [case[phase]]
        if not workers[0]:
            return {"samples": [], "elapsed_ns": 0}
        overlap = case.get("overlap_observer") if phase == "measure" else None
        count = len(workers) + bool(overlap)
        barrier = threading.Barrier(count) if count > 1 else None
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=count)
        futures = []
        started = time.perf_counter_ns()
        try:
            self.active_workers = len(workers)
            futures = [pool.submit(self.worker, index, operations, barrier) for index, operations in enumerate(workers)]
            observer = pool.submit(self.observe_overlap, overlap, barrier) if overlap else None
            if observer is not None:
                futures.append(observer)
            samples = []
            observation = None
            for future in concurrent.futures.as_completed(futures):
                if future is observer:
                    observation = future.result()
                else:
                    samples.extend(future.result())
            if observation is not None:
                mutations = {operation["id"] for operations in workers for operation in operations if operation["kind"] in {"transaction", "bulk", "replicated_transaction"}}
                if not any(value["operation"] in mutations and value["started_ns"] > observation["observed_ns"] for value in samples):
                    raise ValueError("no measured ingestion began after observed compaction")
            finished = time.perf_counter_ns()
            return {"samples": sorted(samples, key=lambda item: (item["worker"], item["round"], item["operation"])),
                    "elapsed_ns": finished - started, "started_ns": started, "finished_ns": finished, "overlap": observation}
        except BaseException:
            self.cancel()
            for future in futures:
                future.cancel()
            raise
        finally:
            pool.shutdown(wait=True, cancel_futures=True)


def main():
    request_path, output = map(Path, sys.argv[1:])
    with request_path.open("rb") as stream:
        data = stream.read(64 * 1024 * 1024 + 1)
    if len(data) > 64 * 1024 * 1024:
        raise ValueError("benchmark request exceeds limit")
    request = json.loads(data)
    execution = Execution(request)
    previous = {kind: signal.getsignal(kind) for kind in (signal.SIGTERM, signal.SIGINT)}
    def interrupted(*_):
        for kind in previous:
            signal.signal(kind, signal.SIG_IGN)
        raise InterruptedError("benchmark process received cancellation")
    for kind in previous:
        signal.signal(kind, interrupted)
    try:
        value = execution.run()
        value.update(schema_version=1, request_sha256=hashlib.sha256(data).hexdigest(), phase=request["phase"],
                     case=request["case"]["id"], execution_complete=True, client_closed=True)
        with output.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, allow_nan=False)
            stream.write("\n")
    finally:
        for kind, handler in previous.items():
            signal.signal(kind, handler)


if __name__ == "__main__":
    main()
