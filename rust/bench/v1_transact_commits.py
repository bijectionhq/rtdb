#!/usr/bin/env python3
"""Measure rtdb.v1 Transact commits: the write path's throughput baseline (W14, P6).

The rtdb.v1 port of placed_commits.py. Each client commits distinct-key single-row
`Put`s through the `rtdb.v1.Transact` DoAction and waits for its receipt. The
benchmark reports commits per second and latency percentiles per client count, then
checks every acknowledged key with `rtdb.v1.TxStatus` and verifies distinct
opaque bases. By default it starts the official `rtdb dev` binary in the
selected mode. Use `--endpoint` for a separately prepared local topology,
including a Multi writer backed by three durable Logkeepers.

Requires PyArrow. Request bodies are hand-encoded protobuf for rust/proto/rtdb.v1.proto,
so no generated stubs are needed.

"""

import argparse
import concurrent.futures
import contextlib
import ipaddress
import json
import pathlib
import re
import struct
import subprocess
import tempfile
import threading
import time
import urllib.parse
import uuid

import pyarrow as pa
from pyarrow import flight

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
    batch = pa.record_batch({"value": array})
    buffer = pa.BufferOutputStream()
    with pa.ipc.new_stream(buffer, batch.schema) as stream:
        stream.write_batch(batch)
    return length_delimited(1, buffer.getvalue().to_pybytes())


def put_op(table, identity, value):
    cells = {"_id": arrow_cell(pa.array([identity], type=pa.int64())),
             "v": arrow_cell(pa.array([value], type=pa.int64()))}
    # map<string, Cell> cells = 1: repeated entries {key = 1; value = 2}.
    row = b"".join(length_delimited(1, string(1, name) + length_delimited(2, cell))
                   for name, cell in cells.items())
    put = string(1, BRANCH) + string(2, table) + length_delimited(3, row)
    return length_delimited(1, put)  # Op.put = 1


def transact_request(key, table, identity):
    return length_delimited(1, key) + length_delimited(5, put_op(table, identity, identity ^ 0x5A5A))


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

def key_for(trial, client_id, sequence):
    return trial.bytes + struct.pack(">QQ", client_id, sequence)


def action(client, options, name, body):
    (result,) = client.do_action(flight.Action(name, body), options)
    return result.body.to_pybytes()


def require_transact(client, options, trial, table):
    key = key_for(trial, 0, 0)
    probe = transact_request(key, table, -1)
    deadline = time.monotonic() + 45
    while True:
        try:
            reply = receipt(action(client, options, "rtdb.v1.Transact", probe))
            break
        except flight.FlightUnavailableError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)
        except flight.FlightError as error:
            if "unsupported" in str(error).lower() or "feature-required" in str(error):
                raise SystemExit(f"This server does not serve rtdb.v1.Transact: {error}") from error
            raise
    if reply["key"] != key or reply["outcome"] != "then" or not reply["basis"]:
        raise RuntimeError(f"RTDB did not accept the v1 startup probe: {reply}")


def measure(connect, trial, table, count, seconds, warmup, rate):
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
                    key = key_for(trial, client_id + 1, sequence)
                    submitted = time.perf_counter()
                    answer = receipt(action(client, options, "rtdb.v1.Transact",
                                            transact_request(key, table, (client_id << 40) | sequence)))
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
    if len({answer["basis"] for answer in acknowledged if answer["basis"]}) != len(acknowledged):
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
            if replay != {**answer, "replayed": True}:
                raise AssertionError(f"TxStatus differs for {answer['key'].hex()}: {replay} != {answer}")
    finally:
        client.close()


@contextlib.contextmanager
def native_deployment(mode, binary, data, logs):
    command = [str(binary), "dev", "--data", str(data), "--flight-sql-port", "0"]
    command += ["--multi"] if mode == "multi" else ["--port", "0"]
    logs.mkdir(parents=True, exist_ok=True)
    logfile = logs / "node.log"
    with logfile.open("w") as log, subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT) as process:
        try:
            deadline = time.monotonic() + 45
            while process.poll() is None and time.monotonic() < deadline:
                status = logfile.read_text(errors="replace")
                bound = re.search(r"^RTDB listeners bound: .*?\bflight_sql: Some\((127\.0\.0\.1:[0-9]+)\)",
                                  status, re.MULTILINE)
                if bound:
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError(f"RTDB failed to listen. See {logfile}")
            yield f"grpc://{bound.group(1)}"
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def endpoint_is_local(endpoint):
    try:
        url = urllib.parse.urlsplit(endpoint)
        host = url.hostname
        if url.scheme not in ("grpc", "grpc+tls") or not host or url.username or url.password or url.path or url.query or url.fragment:
            return False
        return url.port is not None and url.port > 0 and (host == "localhost" or ipaddress.ip_address(host).is_loopback)
    except ValueError:
        return False


def connector(endpoint, database, user, password, ca):
    def connect():
        client = flight.FlightClient(endpoint, tls_root_certs=ca)
        headers = [(b"x-xtdb-database", database.encode())] if database else []
        try:
            if user is not None:
                options = flight.FlightCallOptions(timeout=30, headers=headers)
                headers.append(client.authenticate_basic_token(user.encode(), password, options))
            return client, flight.FlightCallOptions(timeout=30, headers=headers)
        except BaseException:
            client.close()
            raise
    return connect


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("binary", type=pathlib.Path, nargs="?", help="official rtdb executable")
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--endpoint", help="connect to an existing local native Flight listener")
    parser.add_argument("--database", help="exact database selector on --endpoint")
    parser.add_argument("--user", help="Basic principal on --endpoint")
    parser.add_argument("--password-file", type=pathlib.Path)
    parser.add_argument("--ca-file", type=pathlib.Path)
    parser.add_argument("--mode", choices=["multi", "default"], default="multi")
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 16, 32, 64])
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--rate", type=float, help="Pace requests at this total rate per second")
    args = parser.parse_args()
    if (args.binary is None) == (args.endpoint is None):
        parser.error("provide the official binary or --endpoint")
    if args.endpoint is not None and not endpoint_is_local(args.endpoint):
        parser.error("--endpoint must name a local grpc:// or grpc+tls:// listener")
    if args.endpoint is None and any((args.database, args.user, args.password_file, args.ca_file)):
        parser.error("database, credentials and CA options require --endpoint")
    if (args.user is None) != (args.password_file is None):
        parser.error("provide --user and --password-file together")
    password = args.password_file.read_bytes().rstrip(b"\r\n") if args.password_file else None
    ca = args.ca_file.read_bytes() if args.ca_file else None
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / "results.jsonl").open("a") as results:
        for count in args.clients:
            with tempfile.TemporaryDirectory(dir=args.output) as scratch:
                scratch = pathlib.Path(scratch)
                deployment = (contextlib.nullcontext(args.endpoint) if args.endpoint else native_deployment(
                    args.mode, args.binary.resolve(), scratch / "data", args.output / f"logs-{count}"))
                with deployment as endpoint:
                    connect = connector(endpoint, args.database, args.user, password, ca)
                    trial = uuid.uuid4()
                    table = f"public.v1_bench_{trial.hex}"
                    client, options = connect()
                    try:
                        require_transact(client, options, trial, table)
                    finally:
                        client.close()
                    row = dict(mode=args.mode, topology="existing" if args.endpoint else "native-dev",
                               **measure(connect, trial, table, count, args.seconds, args.warmup, args.rate))
            print(json.dumps(row), flush=True)
            results.write(json.dumps(row) + "\n")


if __name__ == "__main__":
    main()
