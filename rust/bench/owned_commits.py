#!/usr/bin/env python3
"""Measure distinct-key owned Flight commits against a standalone local RTDB."""

import argparse
import concurrent.futures
import contextlib
import json
import pathlib
import socket
import struct
import sys
import subprocess
import tempfile
import threading
import time
import urllib.request

import pyarrow as pa
from pyarrow import flight


RELATION = b"commit_bench"
PREFIX = struct.pack(">H", len(RELATION)) + RELATION
COMMIT = "rtdb.owned.Commit.v1"
READ = "rtdb.owned.ReadHeldPoint.v2"


def ipc(**columns):
    batch = pa.record_batch(columns)
    buffer = pa.BufferOutputStream()
    with pa.ipc.new_stream(buffer, batch.schema) as stream:
        stream.write_batch(batch)
    return buffer.getvalue().to_pybytes()


def action(client, options, name, body):
    (result,) = client.do_action(flight.Action(name, body), options)
    return result.body


def connect(port):
    client = flight.FlightClient(f"grpc://127.0.0.1:{port}")
    authorization = client.authenticate_basic_token(b"xtdb", b"commit-bench")
    return client, flight.FlightCallOptions(timeout=30, headers=[authorization])


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@contextlib.contextmanager
def server(binary, directory, stderr):
    port, health = free_port(), free_port()
    while health == port:
        health = free_port()
    config = directory / "node.yaml"
    config.write_text(f"""server:
  port: -1
healthz:
  host: 127.0.0.1
  port: {health}
flightSql:
  host: 127.0.0.1
  port: {port}
  ownedNativeActions: true
log: !Local
  path: {json.dumps(str(directory / "log"))}
storage: !Local
  path: {json.dumps(str(directory / "storage"))}
authn: !SingleRootUser
  password: commit-bench
""")
    with (
        stderr.open("w") as output,
        subprocess.Popen(
            [str(binary), "node", "--file", str(config)],
            stdout=output,
            stderr=subprocess.STDOUT,
        ) as process,
    ):
        try:
            deadline = time.monotonic() + 45
            while process.poll() is None and time.monotonic() < deadline:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.05)
            else:
                raise RuntimeError(f"RTDB failed to listen. See {stderr}")
            yield port, health, process.pid
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def worker(port, client_id, start, end, warmup, gate):
    client, options = connect(port)
    try:
        acknowledged = []
        timings = []
        gate.wait(timeout=45)
        sequence = 0
        while time.perf_counter() < end[0]:
            key = (client_id << 40) | sequence
            body = PREFIX + struct.pack(">q", (1 << 63) - 1) + ipc(
                _id=pa.array([key]),
                _op=pa.array(["create"]),
                value=pa.array([key ^ 0x5A5A]),
            )
            began = time.perf_counter()
            action(client, options, COMMIT, body)
            elapsed = time.perf_counter() - began
            acknowledged.append(key)
            if began >= start[0] + warmup:
                timings.append(elapsed)
            sequence += 1
        return acknowledged, timings
    except BaseException:
        gate.abort()
        raise
    finally:
        client.close()


def verify(port, keys):
    client, options = connect(port)
    try:
        handle = action(client, options, "rtdb.owned.OpenRead.v3", b"")[:16].to_pybytes()
        valid = pa.array([None], type=pa.int64())
        try:
            for first in range(0, len(keys), 64):
                group = keys[first:first + 64]
                requests = [handle + PREFIX + ipc(_id=pa.array([key]), valid_micros=valid) for key in group]
                if len(requests) == 1:
                    replies = [action(client, options, READ, requests[0])]
                else:
                    frame = struct.pack(">H", len(requests)) + b"".join(
                        struct.pack(">BI", 0, len(request)) + request for request in requests
                    )
                    replies = (result.body for result in client.do_action(
                        flight.Action("rtdb.owned.ReadPoints.v1", frame), options
                    ))
                for key, reply in zip(group, replies, strict=True):
                    rows = pa.ipc.open_stream(reply).read_all().to_pylist()
                    if len(rows) != 1 or rows[0]["_id"] != key or rows[0]["value"] != key ^ 0x5A5A:
                        raise AssertionError(f"Incorrect readback for {key}: {rows}")
        finally:
            for _ in client.do_action(flight.Action("rtdb.owned.CloseRead.v1", handle), options):
                pass
    finally:
        client.close()
    return len(keys)


def measure(args, clients):
    log = args.output / f"rtdb-{clients}.log"
    with tempfile.TemporaryDirectory(prefix=f"run-{clients}-", dir=args.output) as temporary:
        with server(args.binary, pathlib.Path(temporary), log) as (port, health, pid):
            print(f"{clients} clients, RTDB pid {pid}", flush=True, file=sys.stderr)
            start, end = [0.0], [0.0]
            sample = None

            def begin():
                nonlocal sample
                start[0] = time.perf_counter()
                end[0] = start[0] + args.warmup + args.seconds
                if args.sample_seconds:
                    sample = subprocess.Popen([
                        "sample", str(pid), str(args.sample_seconds), "1",
                        "-file", str(args.output / f"sample-{clients}.txt"),
                    ], stdout=sys.stderr)

            gate = threading.Barrier(clients, action=begin)
            with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as pool:
                workers = [
                    pool.submit(worker, port, index, start, end, args.warmup, gate)
                    for index in range(clients)
                ]
                results = [worker.result() for worker in workers]
            if sample is not None and sample.wait() != 0:
                raise RuntimeError("Sampling RTDB failed")
            with urllib.request.urlopen(f"http://127.0.0.1:{health}/metrics", timeout=10) as response:
                (args.output / f"metrics-{clients}.prom").write_bytes(response.read())
            timings = sorted(t for _, times in results for t in times)
            if not timings:
                raise RuntimeError("No measured commits")
            keys = [key for acknowledged, _ in results for key in acknowledged]
            verified = verify(port, keys)
            return {
                "clients": clients,
                "seconds": args.seconds,
                "commits": len(timings),
                "commits_per_second": len(timings) / args.seconds,
                "p50_ms": timings[(len(timings) - 1) // 2] * 1_000,
                "p99_ms": timings[(99 * len(timings) + 99) // 100 - 1] * 1_000,
                "acknowledged": len(keys),
                "verified": verified,
            }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 16, 32, 64])
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--sample-seconds", type=int, help="Run the macOS sample profiler")
    args = parser.parse_args()
    if args.seconds <= 0 or args.warmup < 0 or any(count <= 0 for count in args.clients):
        parser.error("clients and measurement seconds must be positive, and warmup must be nonnegative")
    args.binary = args.binary.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    for count in args.clients:
        print(json.dumps(measure(args, count)), flush=True)


if __name__ == "__main__":
    main()
