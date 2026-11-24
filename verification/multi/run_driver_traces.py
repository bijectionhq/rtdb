#!/usr/bin/env python3
"""Record the replication driver fixtures and replay their events against Lean."""

import argparse
import os
from pathlib import Path
import subprocess
import tempfile


MULTI = Path(__file__).resolve().parent
ROOT = MULTI.parents[1]
TRACES = {
    "apply-before-provider-return.trace": (1, 1, 1, False),
    "apply-fault.trace": (0, 1, 0, True),
    "readback-mismatch.trace": (0, 1, 0, True),
    "replay-after-restart.trace": (0, 1, 1, False),
    "writer-replacement.trace": (0, 0, 0, True),
}


def lean_check(lake, trace, status, expected):
    result = subprocess.run(
        [lake, "env", "lean", "--run", "check_driver_trace.lean", str(trace)],
        cwd=MULTI,
        capture_output=True,
        text=True,
    )
    actual = (result.stdout if status == 0 else result.stderr).strip()
    if result.returncode != status or actual != expected:
        raise RuntimeError(
            f"{trace.name}: expected exit {status} and {expected!r}, "
            f"got {result.returncode}\n{result.stdout}{result.stderr}"
        )
    print(f"{trace.name}: {actual}", flush=True)


def reject(lake, directory, name, events, index, event, law):
    trace = directory / f"{name}.reject"
    trace.write_text("\n".join([*events[:index], event, *events[index:]]) + "\n")
    lean_check(lake, trace, 1, f"line {index + 1}: {law}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake", default=str(Path.home() / ".elan/bin/lake"))
    parser.add_argument(
        "--cargo-wrapper", default=str(ROOT.parent / "codesign-agents" / "guarded-cargo.py")
    )
    parser.add_argument("--trace-dir", type=Path, help="check an existing native capture")
    args = parser.parse_args()
    subprocess.run([args.lake, "build", "ReplicationDriverTraces"], cwd=MULTI, check=True)
    directory = args.trace_dir
    if directory is None:
        results = ROOT / ".results"
        results.mkdir(exist_ok=True)
        directory = Path(tempfile.mkdtemp(prefix="driver-traces-", dir=results))
        print(f"Evidence directory {directory}", flush=True)
        subprocess.run(
            [args.cargo_wrapper, "rtdb", "test", "--locked", "-p", "rtdb-replication",
             "driver_trace_refinement"],
            cwd=ROOT / "rust",
            env={**os.environ, "RTDB_DRIVER_TRACE_DIR": str(directory)},
            check=True,
        )
    directory = directory.resolve()

    if {trace.name for trace in directory.glob("*.trace")} != set(TRACES):
        raise RuntimeError(f"{directory}: expected {', '.join(TRACES)}")
    for name, (routes, head, cursor, halted) in TRACES.items():
        trace = directory / name
        events = trace.read_text().splitlines()
        expected = (f"{len(events)} actions, {routes} acknowledgements, "
                    f"head {head}, cursor {cursor}, halted {str(halted).lower()}")
        lean_check(args.lake, trace, 0, expected)

    success = (directory / "apply-before-provider-return.trace").read_text().splitlines()
    ack_index, ack = next((index, event) for index, event in enumerate(success) if event.startswith("ack "))
    reject(args.lake, directory, "acknowledge-before-apply", success,
           success.index("applyBegin"), ack, "acknowledge_after_apply")
    reject(args.lake, directory, "route-acknowledgement-twice", success,
           ack_index + 1, ack, "route_once")
    replacement = (directory / "writer-replacement.trace").read_text().splitlines()
    choice = next(event for event in success if event.startswith("choose "))
    reject(args.lake, directory, "reuse-superseded-term", replacement,
           replacement.index("providerFault 0"), choice, "current_writer_at_choice")


if __name__ == "__main__":
    main()
