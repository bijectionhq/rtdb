#!/usr/bin/env python3
"""Record the replication driver and Multi journal fixtures and replay their events against Lean.

The replication driver traces come from `rtdb-replication` and are checked by
`check_driver_trace.lean` through Lake. The Multi journal traces come from the
deferred-checkpoint tests of `rtdb-db` and are checked by
`check_journal_trace.lean` against the `ReplayClosure.DeferredBase` model,
built with plain `lean`.
"""

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


# Multi journal traces: commits between a checkpoint's cut and its announcement.
JOURNAL_TRACES = {
    "deferred-checkpoint.trace": "15 events, floor 5, base 10, head 11, commits 9, replays 2",
    "classic-then-two-deferred.trace": "22 events, floor 12, base 14, head 14, commits 10, replays 2",
}
JOURNAL_TESTS = [
    "multi::tests::deferred_checkpoint_keeps_the_commits_between_its_cut_and_its_announcement",
    "multi::tests::replay_installs_a_classic_floor_then_two_deferred_floors_in_a_row",
]


def journal_check(lean, olean, trace, status, expected):
    result = subprocess.run(
        [lean, "--run", "check_journal_trace.lean", str(trace)],
        cwd=MULTI,
        env={**os.environ, "LEAN_PATH": str(olean)},
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
    return actual


def journal_reject(lean, olean, directory, name, events, index, replacement, law, line=None):
    trace = directory / f"{name}.reject"
    trace.write_text("\n".join([*events[:index], *replacement, *events[index + 1:]]) + "\n")
    journal_check(lean, olean, trace, 1, f"line {line or index + 1}: {law}")


def journal_traces(args):
    olean = Path(tempfile.mkdtemp(prefix="journal-model-"))
    subprocess.run(
        [args.lean, "-DwarningAsError=true", "--root=lean", "-o", str(olean / "ReplayClosure.olean"),
         "lean/ReplayClosure.lean"],
        cwd=MULTI, check=True, stdout=subprocess.DEVNULL,
    )
    directory = args.journal_trace_dir
    if directory is None:
        results = ROOT / ".results"
        results.mkdir(exist_ok=True)
        directory = Path(tempfile.mkdtemp(prefix="journal-traces-", dir=results))
        print(f"Journal evidence directory {directory}", flush=True)
        for test in JOURNAL_TESTS:
            subprocess.run(
                [args.cargo, "test", "--locked", "-p", "rtdb-db", "--lib", test, "--", "--exact"],
                cwd=ROOT / "rust",
                env={**os.environ, "RTDB_JOURNAL_TRACE_DIR": str(directory),
                     "RUST_MIN_STACK": os.environ.get("RUST_MIN_STACK", "16777216")},
                check=True,
            )
    directory = directory.resolve()
    if {trace.name for trace in directory.glob("*.trace")} != set(JOURNAL_TRACES):
        raise RuntimeError(f"{directory}: expected {', '.join(JOURNAL_TRACES)}")
    for name, expected in JOURNAL_TRACES.items():
        journal_check(args.lean, olean, directory / name, 0, expected)

    events = (directory / "deferred-checkpoint.trace").read_text().splitlines()
    trim = next(index for index, event in enumerate(events) if event.startswith("trim "))
    cut = int(events[trim].split()[1])
    announce = next(index for index, event in enumerate(events) if event.startswith("announce "))
    # Trimming through the announcement drops the commits after the cut.
    journal_reject(args.lean, olean, directory, "trim-through-announcement", events, trim,
                   [f"trim {int(events[announce].split()[1]) - 1}"], "trim_through_announced_cut")
    # Trimming before the announcement commits leaves the floor without a base.
    journal_reject(args.lean, olean, directory, "trim-before-announcement", events, announce,
                   [f"trim {cut}", events[announce]], "trim_through_announced_cut")
    replay = next(index for index, event in enumerate(events) if event.startswith("replay "))
    floor, _, head = events[replay].split()[1:]
    # A reader that takes the floor record as its base selects a different base.
    journal_reject(args.lean, olean, directory, "floor-as-base", events, replay,
                   [f"replay {floor} {floor} {head}"], "replay_base")
    capture = next(index for index, event in enumerate(events) if event.startswith("capture "))
    # An announcement must name a captured cut.
    journal_reject(args.lean, olean, directory, "uncaptured-cut", events, capture,
                   [], "announce_captured_cut", line=announce)


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
    parser.add_argument("--lean", default=str(Path.home() / ".elan/bin/lean"))
    parser.add_argument("--cargo", default="cargo")
    parser.add_argument("--journal-trace-dir", type=Path, help="check existing Multi journal traces")
    parser.add_argument("--journal-only", action="store_true", help="check only the Multi journal traces")
    args = parser.parse_args()
    journal_traces(args)
    if args.journal_only:
        return
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
