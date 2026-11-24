#!/usr/bin/env python3
"""Check one acknowledged commit and its readback for each placed workload."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

import placed_commits as bench


def smoke(binary, output, workload):
    key = 42
    with TemporaryDirectory(prefix="cell-", dir=output) as temporary:
        with bench.cell(binary, Path(temporary), output / workload) as (pg, port, _, _):
            name, build = bench.prepare(pg, port, workload)
            client, options, _ = bench.connect(port)
            try:
                response = bench.action(client, options, name, build(key))
                (receipt,) = pa.ipc.open_stream(response).read_all().to_pylist()
            finally:
                client.close()

            position = (receipt["tx_id"], receipt["system_time_micros"])
            bench.verify(port, [key], position)
            checks_receipt = workload != "commit-v7"
            if checks_receipt:
                bench.verify_hosted_receipts(pg, [([key], [position], [])])

    return {
        "workload": workload,
        "action": name,
        "key": key,
        "tx_id": position[0],
        "system_time_micros": position[1],
        "fact_verified": True,
        "hosted_receipt_verified": checks_receipt,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    for workload in ("hosted", "commit-v7", "hosted-staged"):
        print(json.dumps(smoke(binary, output, workload)), flush=True)


if __name__ == "__main__":
    main()
