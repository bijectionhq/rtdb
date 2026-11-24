#!/usr/bin/env python3
"""Measure rtdb.v1 Transact commits: the write path's throughput baseline (W14, P6).

The rtdb.v1 port of placed_commits.py. Each client commits distinct-key single-row
`Put`s through the `rtdb.v1.Transact` DoAction and waits for its receipt. The
benchmark reports commits per second and latency percentiles per client count, then
checks every acknowledged key with `rtdb.v1.TxStatus`: the receipt must replay with
the same basis, and bases must be distinct and increasing.

    --mode multi    one holder, a gateway and three fsyncing Logkeepers (as placed_commits.py)
    --mode default  one standalone node with its local log (as owned_commits.py)

Requires PyArrow. Request bodies are hand-encoded protobuf for rust/proto/rtdb.v1.proto,
so no generated stubs are needed.

Wire assumptions to confirm against the Transact handler once it is wired (W7):
a row's id travels as its `_id` cell, and `Put.table` names the table as `schema.table`.
"""

import argparse
import concurrent.futures
import contextlib
import json
import pathlib
import struct
import sys
import tempfile
import threading
import time

import pyarrow as pa
from pyarrow import flight

from owned_commits import ipc, server as standalone_server
from placed_commits import cell as multi_cell, connect as multi_connect

TABLE = "public.v1_bench"
BRANCH = "main"


# --- protobuf encoding (proto3, only what TransactRequest and TxStatusRequest need) ---

def varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def tag(field, wire):
    return varint(field << 3 | wire)


def length_delimited(field, payload):
    return tag(field, 2) + varint(len(payload)) + payload


def string(field, text):
    return length_delimited(field, text.encode())


def boolean(field, value):
    return tag(field, 0) + varint(1) if value else b""


def arrow_cell(array):
    """Cell{arrow_ipc}: an Arrow IPC stream with exactly one scalar cell."""
    return length_delimited(1, ipc(value=array))


def put_op(identity, value):
    cells = {"_id": arrow_cell(pa.array([identity], type=pa.int64())),
             "v": arrow_cell(pa.array([value], type=pa.int64()))}
    # map<string, Cell> cells = 1: repeated entries {key = 1; value = 2}.
    row = b"".join(length_delimited(1, string(1, name) + length_delimited(2, cell))
                   for name, cell in cells.items())
    put = string(1, BRANCH) + string(2, TABLE) + length_delimited(3, row)
    return length_delimited(1, put)  # Op.put = 1


def transact_request(key, identity):
    return length_delimited(1, key) + length_delimited(5, put_op(identity, identity ^ 0x5A5A))


def tx_status_request(key):
    return length_delimited(1, key)


# --- protobuf decoding (Receipt) ---

def fields(body):
    position = 0
    while position < len(body):
        key, position = read_varint(body, position)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, position = read_varint(body, position)
        elif wire == 2:
            size, position = read_varint(body, position)
            value, position = body[position:position + size], position + size
        else:
            raise ValueError(f"unexpected wire type {wire}")
        yield field, value


def read_varint(body, position):
    shift, value = 0, 0
    while True:
        byte = body[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, position
        shift += 7


def zigzag(value):
    return (value >> 1) ^ -(value & 1)


def receipt(body):
    parsed = dict(key=None, basis=None, system_time=None, outcome=None, replayed=False)
    for field, value in fields(bytes(body)):
        if field == 1:
            parsed["key"] = bytes(value)
        elif field == 2:
            parsed["basis"] = dict(fields(bytes(value))).get(1, b"")
        elif field == 3:
            parsed["system_time"] = zigzag(value)
        elif field in (4, 5, 6):
            parsed["outcome"] = {4: "then", 5: "otherwise", 6: "refused"}[field]
        elif field == 7:
            parsed["replayed"] = bool(value)
    return parsed


# --- the run ---

def key_for(client_id, sequence):
    return struct.pack(">QQ", client_id, sequence)


def action(client, options, name, body):
    (result,) = client.do_action(flight.Action(name, body), options)
    return result.body.to_pybytes()


def require_transact(client, options):
    probe = transact_request(b"\0" * 16, 0)
    try:
        action(client, options, "rtdb.v1.Transact", probe)
    except flight.FlightError as error:
        if "unsupported" in str(error).lower() or "feature-required" in str(error):
            raise SystemExit(f"This server does not serve rtdb.v1.Transact yet: {error}")
        raise


def measure(connect, count, seconds, warmup, rate):
    start = 0.0

    def begin():
        nonlocal start
        start = time.perf_counter()

    gate = threading.Barrier(count, action=begin)

    def worker(client_id):
        try:
            client, options = connect()
            acknowledged, latencies = [], []
            try:
                gate.wait(timeout=45)
                sequence = 0
                while True:
                    if rate:
                        due = start + (sequence * count + client_id) / rate
                        if (delay := due - time.perf_counter()) > 0:
                            time.sleep(delay)
                    if time.perf_counter() >= start + warmup + seconds:
                        break
                    key = key_for(client_id + 1, sequence)
                    submitted = time.perf_counter()
                    answer = receipt(action(client, options, "rtdb.v1.Transact",
                                            transact_request(key, (client_id << 40) | sequence)))
                    finished = time.perf_counter()
                    if answer["key"] != key or answer["outcome"] != "then" or answer["replayed"]:
                        raise AssertionError(f"Unexpected receipt {answer}")
                    acknowledged.append(answer)
                    if start + warmup <= submitted < start + warmup + seconds:
                        latencies.append(finished - submitted)
                    sequence += 1
                return acknowledged, latencies
            finally:
                client.close()
        except BaseException:
            gate.abort()
            raise

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        results = [future.result() for future in [pool.submit(worker, i) for i in range(count)]]
    acknowledged = [answer for batch, _ in results for answer in batch]
    latencies = sorted(latency for _, batch in results for latency in batch)
    if not latencies:
        raise RuntimeError("No measured commits")
    bases = sorted(answer["basis"] for answer in acknowledged)
    if len(set(bases)) != len(bases):
        raise AssertionError("Two acknowledged commits share a basis")
    verify(connect, acknowledged)
    return dict(clients=count, seconds=seconds, offered_rate=rate, commits=len(latencies),
                commits_per_second=len(latencies) / seconds,
                p50_ms=latencies[(len(latencies) - 1) // 2] * 1000,
                p99_ms=latencies[(99 * len(latencies) + 99) // 100 - 1] * 1000,
                acknowledged=len(acknowledged), verified=len(acknowledged))


def verify(connect, acknowledged):
    """Every acknowledged key replays its exact receipt through TxStatus."""
    client, options = connect()
    try:
        for answer in acknowledged:
            replay = receipt(action(client, options, "rtdb.v1.TxStatus", tx_status_request(answer["key"])))
            if not replay["replayed"] or replay["basis"] != answer["basis"] or replay["system_time"] != answer["system_time"]:
                raise AssertionError(f"TxStatus differs for {answer['key'].hex()}: {replay} != {answer}")
    finally:
        client.close()


@contextlib.contextmanager
def deployment(mode, binary, data, logs):
    data.mkdir(parents=True)
    if mode == "multi":
        with multi_cell(binary, data, logs) as (_, flight_port, _, holder):
            print(f"holder pid {holder}", file=sys.stderr, flush=True)

            def connect():
                client, options, _ = multi_connect(flight_port)
                return client, options
            yield connect
    else:
        logs.mkdir(parents=True, exist_ok=True)
        with (logs / "node.log").open("w") as stderr, standalone_server(binary, data, stderr) as (port, _, _):
            def connect():
                client = flight.FlightClient(f"grpc://127.0.0.1:{port}")
                authorization = client.authenticate_basic_token(b"xtdb", b"commit-bench")
                return client, flight.FlightCallOptions(timeout=30, headers=[authorization])
            yield connect


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("binary", type=pathlib.Path, help="rtdb-multi for --mode multi, rtdb for --mode default")
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--mode", choices=["multi", "default"], default="multi")
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 16, 32, 64])
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--rate", type=float, help="Pace requests at this total rate per second")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / "results.jsonl").open("a") as results:
        for count in args.clients:
            with tempfile.TemporaryDirectory(dir=args.output) as scratch:
                scratch = pathlib.Path(scratch)
                with deployment(args.mode, args.binary, scratch / "data", args.output / f"logs-{count}") as connect:
                    client, options = connect()
                    try:
                        require_transact(client, options)
                    finally:
                        client.close()
                    row = dict(mode=args.mode, **measure(connect, count, args.seconds, args.warmup, args.rate))
            print(json.dumps(row), flush=True)
            results.write(json.dumps(row) + "\n")


if __name__ == "__main__":
    main()
