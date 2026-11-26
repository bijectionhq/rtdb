#!/usr/bin/env python3
"""Measure owned Flight commits on one holder with three fsyncing Logkeepers.

Requires PyArrow and psycopg 3. Run with a release rtdb-multi binary and an
output directory on the filesystem to measure. Use --shared-session to drive
the same bearer concurrently, as the Bijection client does.
"""

import argparse
import concurrent.futures
import contextlib
import datetime
import hashlib
import json
import pathlib
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid

import psycopg
import pyarrow as pa
from pyarrow import flight

from owned_commits import action, free_port, ipc


NAME = "commit-bench"
RELATION = b"fact_layout_readiness"
PASSWORD = b"commit-bench"
INSTALLATION = uuid.UUID("74270000-0000-0000-0000-000000000001")
WRITER = bytes.fromhex("11" * 16)
DEADLINE = struct.pack(">q", (1 << 63) - 1)


def sized(value, size="H"):
    return struct.pack(f">{size}", len(value)) + value


def group(relation, batch):
    return sized(relation) + sized(batch, "I")


def mutation(key):
    return group(RELATION, ipc(
        _id=pa.array([key], type=pa.int64()),
        _op=pa.array(["create"]),
        value=pa.array([key ^ 0x5A5A], type=pa.int64()),
        bucket=pa.array([b"b"]),
    ))


def counted_commit(key):
    bound = sized(RELATION) + b"\0" + sized(b"bucket") + sized(b"b") + DEADLINE
    return b"\0\1" + bound + b"\0\0" + DEADLINE + b"\0\1" + mutation(key)


def hosted_commit(claim, key, staged, fact=None):
    proof = struct.pack(">q", claim) + INSTALLATION.bytes + WRITER
    authority = group(b"bj_installation", ipc(
        _id=pa.array(["authority"]),
        _expected_micros=pa.array([claim], type=pa.int64()),
        _check_only=pa.array([True]),
    ))
    receipt = group(b"bj_commit_bench", ipc(
        _id=pa.array([str(key)]),
        _expected_micros=pa.array([None], type=pa.int64()),
        value=pa.array([key ^ 0x5A5A], type=pa.int64()),
    ))
    facts = sized(b"\0\1" + (mutation(key) if fact is None else fact), "I")
    if staged:
        deadline = time.time_ns() // 1000 + 5_000_000
        cutoff = struct.pack(">qq", deadline, deadline + 100_000)
        certificate = hashlib.sha256(b"local benchmark receiver clock").digest()
        timing = b"\x08\0\0" + cutoff + certificate
    else:
        timing = b"\0\0\0" + DEADLINE
    return proof + timing + facts + b"\0\2" + authority + receipt


def connect(port, authorization=None):
    selector = (b"x-xtdb-database", f"{NAME}@{INSTALLATION.hex}".encode())
    client = flight.FlightClient(f"grpc://127.0.0.1:{port}")
    if authorization is None:
        authorization = client.authenticate_basic_token(
            b"xtdb", PASSWORD, flight.FlightCallOptions(timeout=30, headers=[selector])
        )
    return client, flight.FlightCallOptions(timeout=30, headers=[selector, authorization]), authorization


def wait_for(port, process, log):
    deadline = time.monotonic() + 30
    while process.poll() is None and time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"Port {port} failed to listen. See {log}")


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def sql_connection(port):
    return psycopg.connect(host="127.0.0.1", port=port, dbname=f"{NAME}@{INSTALLATION.hex}",
                           user="xtdb", password=PASSWORD.decode(), sslmode="disable",
                           connect_timeout=30, autocommit=True)


def epoch_micros(timestamp):
    elapsed = timestamp - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return elapsed // datetime.timedelta(microseconds=1)


@contextlib.contextmanager
def cell(binary, data, logs):
    ports = set()
    while len(ports) < 9:
        ports.add(free_port())
    directory, pg, flight_port, node_pg, node_flight, health, *journals = sorted(ports)

    def address(port):
        return f"127.0.0.1:{port}"

    remote = ["--directory", address(directory)]
    state_dir = data / "directory"
    state_dir.mkdir()
    state = ["--state", str(state_dir)]
    secret = data / "secret"
    secret.write_bytes(bytes(32))
    config = data / "node.yaml"
    config.write_text(f"""authn: !SingleRootUser
  password: {PASSWORD.decode()}
server:
  port: {node_pg}
  numThreads: 4
healthz:
  host: 127.0.0.1
  port: {health}
flightSql:
  port: {node_flight}
  ownedNativeActions: true
  hostedControls:
    owner: xtdb
    relations: [bj_installation, bj_commit_bench]
""")
    logs.mkdir()
    subprocess.run([binary, "initialize-directory", *state], check=True, timeout=30)
    with contextlib.ExitStack() as stack:
        def start(name, port, *args):
            log = logs / f"{name}.log"
            output = stack.enter_context(log.open("w"))
            process = subprocess.Popen([binary, *args], stdout=output, stderr=subprocess.STDOUT)
            stack.callback(stop, process)
            wait_for(port, process, log)
            return process.pid

        start("directory", directory, "directory", *state, "--listen", address(directory))
        for index, port in enumerate(journals):
            start(f"journal-{index}", port, "logkeeper", *remote, "--listen", address(port),
                  "--data", data / f"journal-{index}", "--zone", f"zone-{index}")
        holder = start("holder", node_flight, "node", *remote, "--config", config, "--zone", "holder",
                       "--storage-root", data / "objects", "--keys", data / "keys",
                       "--cache", data / "cache", "--cell-secret", secret)
        start("gateway", flight_port, "gateway", *remote, "--listen", address(pg),
              "--flight-listen", address(flight_port), "--cell-secret", secret)
        with (logs / "install.json").open("w") as output:
            subprocess.run([binary, "install", *remote, "--id", str(INSTALLATION), "--name", NAME,
                            "--org", "benchmark", "--replicas", "3"], stdout=output, check=True, timeout=30)
        yield pg, flight_port, health, holder


def prepare(pg, port, workload):
    with sql_connection(pg) as conn:
        conn.execute(f"""INSERT INTO bj_installation RECORDS {{_id: 'authority', kind: 'hosted',
            incarnation: X('{INSTALLATION.hex}'), writer: '{WRITER.hex()}',
            _valid_from: TIMESTAMP '1970-01-01Z'}}""")
        conn.execute("""INSERT INTO bj_commit_bench RECORDS {_id: 'setup', value: 0,
            _valid_from: TIMESTAMP '1970-01-01Z'}""")
        (revision,) = conn.execute("SELECT _system_from FROM bj_installation WHERE _id = 'authority'").fetchone()
    claim = epoch_micros(revision)
    if workload == "commit-v7":
        declaration = group(RELATION, ipc(
            _id=pa.array(["bucket"]), _op=pa.array(["declare-capacity"]), column=pa.array(["bucket"]),
            prefix_bytes=pa.array([1]), cap=pa.array([(1 << 63) - 1]), when=pa.array([b""]),
        ))
        client, options, _ = connect(port)
        try:
            action(client, options, "rtdb.owned.DecideHosted.v4", hosted_commit(claim, 0, False, declaration))
        finally:
            client.close()
        return "rtdb.owned.Commit.v7", counted_commit
    return "rtdb.owned.DecideHosted.v4", lambda key: hosted_commit(claim, key, workload == "hosted-staged")


def verify(port, acknowledged, receipt_floor):
    client, options, _ = connect(port)
    try:
        floor = INSTALLATION.bytes + b"\1" + struct.pack(">qqQ", *receipt_floor, 0)
        handle = action(client, options, "rtdb.owned.OpenAppliedRead.v5", floor)[:16].to_pybytes()
        valid = pa.array([max(receipt_floor[1], time.time_ns() // 1000)], type=pa.int64())
        try:
            for first in range(0, len(acknowledged), 64):
                keys = acknowledged[first:first + 64]
                requests = [handle + sized(RELATION) + ipc(_id=pa.array([key]), valid_micros=valid) for key in keys]
                if len(requests) == 1:
                    replies = [action(client, options, "rtdb.owned.ReadHeldPoint.v2", requests[0])]
                else:
                    frame = struct.pack(">H", len(requests)) + b"".join(b"\0" + sized(request, "I") for request in requests)
                    replies = (result.body for result in client.do_action(flight.Action("rtdb.owned.ReadPoints.v1", frame), options))
                for key, reply in zip(keys, replies, strict=True):
                    rows = pa.ipc.open_stream(reply).read_all().to_pylist()
                    if len(rows) != 1 or rows[0]["_id"] != key or rows[0]["value"] != key ^ 0x5A5A:
                        raise AssertionError(f"Incorrect readback for {key}: {rows}")
        finally:
            list(client.do_action(flight.Action("rtdb.owned.CloseRead.v1", handle), options))
    finally:
        client.close()


def verify_hosted_receipts(port, results):
    expected = {str(key): (key ^ 0x5A5A, revision)
                for keys, receipts, _ in results for key, (_, revision) in zip(keys, receipts, strict=True)}
    with sql_connection(port) as conn:
        rows = conn.execute("SELECT _id, value, _system_from FROM bj_commit_bench WHERE _id <> 'setup'")
        for key, value, revision in rows:
            if expected.pop(key, None) != (value, epoch_micros(revision)):
                raise AssertionError(f"Incorrect hosted receipt for {key}")
    if expected:
        raise AssertionError(f"{len(expected)} hosted receipts are missing")


def measure(args, count, logs, pg, port, health, holder):
    name, commit = prepare(pg, port, args.workload)
    shared, authorization = None, None
    if args.shared_session:
        shared, _, authorization = connect(port)
    start = 0.0

    def begin():
        nonlocal start
        start = time.perf_counter()

    gate = threading.Barrier(count, action=begin)

    def worker(client_id):
        try:
            client, options, _ = connect(port, authorization)
            keys, receipts, latencies = [], [], []
            try:
                gate.wait(timeout=45)
                sequence = 0
                while True:
                    if args.rate:
                        due = start + (sequence * count + client_id) / args.rate
                        delay = due - time.perf_counter()
                        if delay > 0:
                            time.sleep(delay)
                    if time.perf_counter() >= start + args.warmup + args.seconds:
                        break
                    key = (client_id << 40) | sequence
                    body = commit(key)
                    submitted = time.perf_counter()
                    response = action(client, options, name, body)
                    finished = time.perf_counter()
                    (receipt,) = pa.ipc.open_stream(response).read_all().to_pylist()
                    receipts.append((receipt["tx_id"], receipt["system_time_micros"]))
                    keys.append(key)
                    if start + args.warmup <= submitted < start + args.warmup + args.seconds:
                        latencies.append(finished - submitted)
                    sequence += 1
                return keys, receipts, latencies
            finally:
                client.close()
        except BaseException:
            gate.abort()
            raise

    print(f"{name}, {count} clients, holder pid {holder}", flush=True, file=sys.stderr)
    with contextlib.ExitStack() as stack:
        if shared is not None:
            stack.callback(shared.close)
        if args.sample_seconds:
            process = subprocess.Popen(["sample", str(holder), str(args.sample_seconds), "1",
                                        "-file", str(logs / "holder.sample.txt")], stdout=sys.stderr)
            stack.callback(stop, process)
        pool = stack.enter_context(concurrent.futures.ThreadPoolExecutor(max_workers=count))
        futures = [pool.submit(worker, index) for index in range(count)]
        results = [pending.result() for pending in futures]
        if args.sample_seconds and process.wait() != 0:
            raise RuntimeError("Sampling the holder failed")

    with urllib.request.urlopen(f"http://127.0.0.1:{health}/metrics", timeout=10) as metrics:
        (logs / "holder.prom").write_bytes(metrics.read())
    keys = [key for batch, _, _ in results for key in batch]
    receipts = sorted(receipt for _, batch, _ in results for receipt in batch)
    if any(a[0] >= b[0] or a[1] >= b[1] for a, b in zip(receipts, receipts[1:])):
        raise AssertionError("Commit receipts do not identify distinct increasing transaction keys")
    latencies = sorted(latency for _, _, batch in results for latency in batch)
    if not latencies:
        raise RuntimeError("No measured commits")
    verify(port, keys, receipts[-1])
    if args.workload != "commit-v7":
        verify_hosted_receipts(pg, results)
    return dict(action=name, workload=args.workload, clients=count, seconds=args.seconds, offered_rate=args.rate,
                shared_session=args.shared_session, commits=len(latencies),
                commits_per_second=len(latencies) / args.seconds,
                p50_ms=latencies[(len(latencies) - 1) // 2] * 1000,
                p99_ms=latencies[(99 * len(latencies) + 99) // 100 - 1] * 1000,
                acknowledged=len(keys), verified=len(keys))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--workload", choices=["commit-v7", "hosted", "hosted-staged"], default="hosted")
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 16, 32, 64])
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--rate", type=float, help="Pace requests at this total rate per second")
    parser.add_argument("--shared-session", action="store_true")
    parser.add_argument("--sample-seconds", type=int, help="Run the macOS sample profiler")
    args = parser.parse_args()
    if args.seconds <= 0 or args.warmup < 0 or min(args.clients) <= 0 or args.rate is not None and args.rate <= 0:
        parser.error("clients, duration and optional rate must be positive, and warmup must be nonnegative")
    args.binary = args.binary.resolve(strict=True)
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    for clients in args.clients:
        with tempfile.TemporaryDirectory(prefix="cell-", dir=args.output) as data:
            logs = args.output / f"{clients}-clients"
            with cell(args.binary, pathlib.Path(data), logs) as runtime:
                print(json.dumps(measure(args, clients, logs, *runtime)), flush=True)


if __name__ == "__main__":
    main()
