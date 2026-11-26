#!/usr/bin/env python3
"""Compare RTDB Multi commit throughput across journal back ends.

Runs the same rtdb.v1 Transact workload as v1_consensus_keepers.py, with
every acknowledged key checked through TxStatus, against one release `rtdb
dev --multi` per back end on the same host:

- `file`: the local FileJournal in the RTDB process, a single-node lower bound.
- `stream`: three stream keepers behind the stream Writer (`logkeeper+tls://`).
- `consensus`: three consensus voters and a catalog keeper
  (`logkeeper-consensus://`).

Results go to OUTPUT/comparison.json.
"""

import argparse
import json
import pathlib
import subprocess
import sys
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import v1_consensus_keepers as harness  # noqa: E402

STREAM_PRINCIPAL = "urn:logkeeper:client:rtdb-journal-comparison"


class StreamKeepers:
    """Three stream keepers, the journal Multi used before consensus streams."""

    def __init__(self, binary, work, logs):
        self.binary, self.logs, self.processes = binary, logs, {}
        self.pki = harness.Pki(work, "stream")
        self.configs = {}
        addresses = []
        for index in range(3):
            keeper = str(uuid.uuid4())
            cert, key = self.pki.issue(f"stream-{keeper}", f"URI:urn:logkeeper:keeper:{keeper}", "serverAuth,clientAuth")
            listen = f"127.0.0.1:{harness.free_port()}"
            addresses.append(listen)
            config = work / f"stream-{keeper}.json"
            config.write_text(json.dumps(dict(
                keeper=keeper, listen=listen, data=str(work / f"stream-data-{keeper}"), zone=f"local-{index}",
                tls=dict(certificate=str(cert), private_key=str(key), ca=str(self.pki.ca)),
                clients=[STREAM_PRINCIPAL], writers=[STREAM_PRINCIPAL],
                limits=dict(stream_quota_bytes=8 << 30, max_record_bytes=32 << 20,
                            round_bytes_per_stream=4 << 20, round_requests=64))))
            self.configs[keeper] = (config, listen)
        self.client = self.pki.issue("stream-writer", f"URI:{STREAM_PRINCIPAL}", "clientAuth")
        self.url = "logkeeper+tls://" + ",".join(addresses)

    def start_all(self):
        for keeper, (config, listen) in self.configs.items():
            log = (self.logs / f"stream-{keeper}.log").open("a")
            self.processes[keeper] = subprocess.Popen(
                [str(self.binary), "keeper", "--config", str(config)], stdout=log, stderr=log)
            host, port = listen.split(":")
            harness.wait_for_port(host, int(port), self.processes[keeper])

    def stop_all(self):
        for process in self.processes.values():
            process.terminate()
            process.wait(timeout=30)
        self.processes.clear()


def run_backend(name, args, results):
    work, logs = args.output / name / "work", args.output / name / "logs"
    work.mkdir(parents=True)
    logs.mkdir(parents=True)
    keepers = None
    if name == "stream":
        keepers = StreamKeepers(args.logkeeper.resolve(), work, logs)
        keepers.start_all()
    elif name == "consensus":
        keepers = harness.Keepers(args.logkeeper.resolve(), work, logs)
        for keeper in keepers.all():
            keepers.start(keeper)
    rtdb = harness.Rtdb(args.rtdb.resolve(), work, keepers, logs, args.checkpoints)
    acknowledged = []
    try:
        rtdb.start()
        for clients in args.clients:
            row = harness.commit(rtdb, clients, args.seconds, name, [], acknowledged)
            results.append(dict(backend=name, **row))
    finally:
        if rtdb.process and rtdb.process.poll() is None:
            rtdb.stop()
        if name == "stream":
            keepers.stop_all()
        elif name == "consensus":
            for keeper in list(keepers.processes):
                keepers.stop(keeper)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rtdb", type=pathlib.Path, required=True)
    parser.add_argument("--logkeeper", type=pathlib.Path, required=True)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--backends", nargs="+", default=["file", "stream", "consensus"])
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 16, 64])
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--checkpoints", choices=harness.CHECKPOINT_MODES, default="deferred")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    try:
        for name in args.backends:
            run_backend(name, args, results)
    finally:
        (args.output / "comparison.json").write_text(json.dumps(results, indent=2) + "\n")
    print(f"{'backend':<10} {'clients':>7} {'commits/s':>10} {'p50 ms':>8} {'p99 ms':>8} {'verified':>9}")
    for row in results:
        print(f"{row['backend']:<10} {row['clients']:>7} {row['commits_per_second']:>10.0f} "
              f"{row['p50_ms']:>8.2f} {row['p99_ms']:>8.2f} {row['verified']:>9}")


if __name__ == "__main__":
    main()
