#!/usr/bin/env python3
"""Qualify RTDB Multi on separate keeper-hosted consensus processes.

Starts three voter keepers in distinct failure domains and a catalog keeper,
each as its own `logkeeper keeper` process with enrolled media and mutual TLS.
Starts `rtdb dev --multi` with a `logkeeper-consensus://` journal URL and
drives rtdb.v1 Transact commits over Flight. Every phase checks each
acknowledged key with rtdb.v1.TxStatus, and later phases recheck every key
acknowledged before them.

Phases: steady commits; loss of one voter during commits and its restart;
RTDB process kill and reopen on the same data; every keeper stopped and
restarted while RTDB keeps running; a final replay of every acknowledged key.

Requires OpenSSL and PyArrow. Results go to OUTPUT/results.json, with the
process logs beside it.
"""

import argparse
import hashlib
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import v1_transact_commits as driver  # noqa: E402

PRINCIPAL = "urn:logkeeper:client:rtdb-consensus-qualification"


def run(*command, cwd=None):
    subprocess.run([str(part) for part in command], cwd=cwd, check=True, capture_output=True)


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def write_private(path, data):
    path.write_bytes(data if isinstance(data, bytes) else data.encode())
    path.chmod(0o600)


def wait_for_port(host, port, process, seconds=30):
    deadline = time.monotonic() + seconds
    while True:
        if process.poll() is not None:
            raise RuntimeError("process exited before listening; see its log")
        try:
            socket.create_connection((host, port), timeout=0.2).close()
            return
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)


class Pki:
    def __init__(self, directory, name):
        self.directory = directory
        self.ca = directory / f"{name}-ca.pem"
        self.key = directory / f"{name}-ca.key"
        run("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
            "-keyout", self.key, "-out", self.ca, "-subj", f"/CN={name} qualification CA", "-days", "2",
            "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")

    def issue(self, name, san, usage):
        key, csr, ext, cert = (self.directory / f"{name}{suffix}" for suffix in ("-key.pem", ".csr", ".ext", ".pem"))
        run("openssl", "req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
            "-keyout", key, "-out", csr, "-subj", f"/CN={name}")
        ext.write_text(f"subjectAltName={san}\nextendedKeyUsage={usage}\nbasicConstraints=critical,CA:FALSE\n"
                       "keyUsage=critical,digitalSignature\nauthorityKeyIdentifier=keyid,issuer\n")
        run("openssl", "x509", "-req", "-in", csr, "-CA", self.ca, "-CAkey", self.key, "-CAcreateserial",
            "-out", cert, "-days", "2", "-extfile", ext)
        key.chmod(0o600)
        return cert, key


class Keepers:
    def __init__(self, binary, work, logs):
        self.binary, self.logs, self.processes = binary, logs, {}
        self.pki = Pki(work, "journal")
        self.voters = [str(uuid.uuid4()) for _ in range(3)]
        self.catalog = str(uuid.uuid4())
        roster = self.voters + [self.catalog]
        domains = dict(zip(roster, ["zone-a", "zone-b", "zone-c", "directory"]))
        lanes = {lane: {keeper: f"127.0.0.1:{free_port()}" for keeper in roster} for lane in ("keepers", "peers", "bulk")}
        self.addresses = lanes
        registry = work / "registry"
        registry.mkdir()
        (registry / "placement.json").write_text(json.dumps(dict(catalog=self.catalog, domains=domains)))
        self.configs = {}
        for keeper in roster:
            cert, key = self.pki.issue(f"keeper-{keeper}", f"URI:urn:logkeeper:keeper:{keeper}", "serverAuth,clientAuth")
            data = work / f"data-{keeper}"
            (registry / f"{keeper}.provision.json").write_text(json.dumps(dict(
                keeper=keeper, data=str(data), domain=domains[keeper],
                certificate_sha256=hashlib.sha256(cert.read_bytes()).hexdigest())))
            config = work / f"keeper-{keeper}.json"
            config.write_text(json.dumps(dict(
                keeper=keeper, listen=lanes["keepers"][keeper], data=str(data), enrollment=str(registry),
                tls=dict(certificate=str(cert), private_key=str(key), ca=str(self.pki.ca)),
                **lanes, domains=domains, catalog=self.catalog,
                readers=[PRINCIPAL], writers=[PRINCIPAL], administrators=[PRINCIPAL])))
            run(binary, "keeper", "initialize", "--config", config)
            self.configs[keeper] = config
        self.client = self.pki.issue("rtdb-writer", f"URI:{PRINCIPAL}", "clientAuth")
        self.journal = work / "journal.json"
        self.journal.write_text(json.dumps(dict(
            keepers=lanes["keepers"], catalog=self.catalog, voters=self.voters,
            quota_bytes=8 << 30, max_record_bytes=4 << 20)))
        self.url = f"logkeeper-consensus://{self.journal}"

    def start(self, keeper):
        log = (self.logs / f"keeper-{keeper}.log").open("a")
        self.processes[keeper] = subprocess.Popen(
            [str(self.binary), "keeper", "--config", str(self.configs[keeper])], stdout=log, stderr=log)
        deadline = time.monotonic() + 30
        for lane in ("keepers", "peers", "bulk"):
            host, port = self.addresses[lane][keeper].split(":")
            while True:
                if self.processes[keeper].poll() is not None:
                    raise RuntimeError(f"keeper {keeper} exited; see its log")
                try:
                    socket.create_connection((host, int(port)), timeout=0.2).close()
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.05)

    def stop(self, keeper, kill=False):
        process = self.processes.pop(keeper)
        process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
        process.wait(timeout=30)

    def all(self):
        return self.voters + [self.catalog]


class Rtdb:
    """rtdb dev --multi on one journal. `keepers` supplies the journal CA,
    client credentials and URL. Without it RTDB uses its local FileJournal."""

    def __init__(self, binary, work, keepers, logs, checkpoints="deferred"):
        self.binary, self.logs, self.process = binary, logs, None
        self.env = checkpoint_environment(checkpoints)
        self.data = work / "rtdb"
        self.incarnation = str(uuid.uuid4())
        self.tls = work / "flight-tls"
        self.tls.mkdir()
        flight = Pki(work, "flight")
        cert, key = flight.issue("flight-server", "IP:127.0.0.1", "serverAuth")
        write_private(self.tls / "certificate.pem", cert.read_bytes())
        write_private(self.tls / "key.pem", key.read_bytes())
        self.password = os.urandom(32).hex()
        write_private(self.tls / "password", self.password)
        self.ca = flight.ca.read_bytes()
        self.url = None
        if keepers is not None:
            client_cert, client_key = keepers.client
            write_private(self.tls / "journal-certificate.pem", client_cert.read_bytes())
            write_private(self.tls / "journal-key.pem", client_key.read_bytes())
            write_private(self.tls / "journal-ca.pem", keepers.pki.ca.read_bytes())
            self.url = keepers.url

    def start(self):
        ready = self.logs / "rtdb-ready.json"
        ready.write_text("")
        log = (self.logs / "rtdb.log").open("a")
        journal = ["--multi-journal-url", self.url] if self.url else []
        self.process = subprocess.Popen([
            str(self.binary), "dev", "--multi", "--data", str(self.data), "--multi-tls-dir", str(self.tls),
            "--incarnation", self.incarnation, *journal, "--flight-sql-port", "0",
        ], stdout=ready.open("w"), stderr=log, env=self.env)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("rtdb exited before readiness; see rtdb.log")
            for line in ready.read_text().splitlines():
                try:
                    status = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if status.get("incarnation") == self.incarnation and status.get("writer_epoch", 0) > 0:
                    self.endpoint = "grpc+tls://" + status["native_flight_url"].removeprefix("https://")
                    return status
            time.sleep(0.1)
        raise RuntimeError("rtdb did not report writer readiness")

    def stop(self, kill=False):
        self.process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
        self.process.wait(timeout=60)

    def connect(self):
        return driver.connector(self.endpoint, "xtdb", "xtdb", self.password.encode(), self.ca)


CHECKPOINT_MODES = ("classic", "binary", "deferred")


def checkpoint_environment(mode):
    """The RTDB environment for a checkpoint mode: `classic` writes JSON
    checkpoints under the database lock, `binary` writes CBOR ones, and
    `deferred` also builds them off the commit path. Phase timings of each
    checkpoint go to rtdb.log."""
    if mode not in CHECKPOINT_MODES:
        raise ValueError(f"unknown checkpoint mode {mode}")
    env = dict(os.environ, RTDB_CHECKPOINT_TIMING="1")
    env["RTDB_MULTI_BINARY_CHECKPOINTS"] = "0" if mode == "classic" else "1"
    env["RTDB_MULTI_DEFERRED_CHECKPOINTS"] = "1" if mode == "deferred" else "0"
    return env


def commit(rtdb, clients, seconds, phase, results, acknowledged):
    connect = rtdb.connect()
    trial = uuid.uuid4()
    table = f"public.consensus_{trial.hex}"
    client, options = connect()
    try:
        driver.require_transact(client, options, trial, table)
    finally:
        client.close()
    keys = []
    original = driver.verify

    def remember(connect, answers):
        keys.extend(answers)
        original(connect, answers)

    driver.verify = remember
    try:
        row = driver.measure(connect, trial, table, clients, seconds, 0.5, None)
    finally:
        driver.verify = original
    acknowledged.extend(keys)
    row.update(phase=phase)
    results.append(row)
    print(json.dumps(row), flush=True)
    return row


def recheck(rtdb, phase, results, acknowledged):
    started = time.monotonic()
    deadline = started + 60
    while True:
        try:
            driver.verify(rtdb.connect(), acknowledged)
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)
    row = dict(phase=phase, rechecked=len(acknowledged), seconds=round(time.monotonic() - started, 2))
    results.append(row)
    print(json.dumps(row), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rtdb", type=pathlib.Path, required=True)
    parser.add_argument("--logkeeper", type=pathlib.Path, required=True)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--checkpoints", choices=CHECKPOINT_MODES, default="deferred")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    work, logs = args.output / "work", args.output / "logs"
    work.mkdir()
    logs.mkdir()
    keepers = Keepers(args.logkeeper.resolve(), work, logs)
    for keeper in keepers.all():
        keepers.start(keeper)
    rtdb = Rtdb(args.rtdb.resolve(), work, keepers, logs, args.checkpoints)
    results, acknowledged = [], []
    try:
        results.append(dict(phase="readiness", **rtdb.start()))
        for clients in (1, 4, 16):
            commit(rtdb, clients, args.seconds, "steady", results, acknowledged)

        # Lose one voter during commits, then restart it from its media.
        victim = keepers.voters[1]
        timer = threading.Timer(args.seconds, keepers.stop, args=(victim,), kwargs=dict(kill=True))
        timer.start()
        commit(rtdb, 4, 3 * args.seconds, "voter-killed", results, acknowledged)
        timer.join()
        keepers.start(victim)
        commit(rtdb, 4, args.seconds, "voter-restarted", results, acknowledged)
        recheck(rtdb, "recheck-after-voter-restart", results, acknowledged)

        # Kill RTDB and reopen it on the same data and journal.
        rtdb.stop(kill=True)
        results.append(dict(phase="rtdb-reopened", **rtdb.start()))
        recheck(rtdb, "recheck-after-rtdb-reopen", results, acknowledged)
        commit(rtdb, 4, args.seconds, "after-rtdb-reopen", results, acknowledged)

        # Stop and restart every keeper while RTDB keeps running.
        for keeper in keepers.all():
            keepers.stop(keeper)
        for keeper in keepers.all():
            keepers.start(keeper)
        recheck(rtdb, "recheck-after-keeper-restart", results, acknowledged)
        commit(rtdb, 4, args.seconds, "after-keeper-restart", results, acknowledged)
        recheck(rtdb, "final-recheck", results, acknowledged)
    finally:
        (args.output / "results.json").write_text(json.dumps(dict(
            rtdb=str(args.rtdb), logkeeper=str(args.logkeeper), results=results,
            acknowledged=len(acknowledged)), indent=2) + "\n")
        if rtdb.process and rtdb.process.poll() is None:
            rtdb.stop()
        for keeper in list(keepers.processes):
            keepers.stop(keeper)


if __name__ == "__main__":
    main()
