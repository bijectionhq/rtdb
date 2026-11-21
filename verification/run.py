"""Run RTDB's local proof studies in disposable copies and retain evidence."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import subprocess
import tarfile
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def replace_once(path, find, replacement):
    source = path.read_text()
    if source.count(find) != 1:
        raise ValueError(f"{path.name}: mutation must identify exactly one occurrence")
    path.write_text(source.replace(find, replacement, 1))


def clone_file(relative, directory):
    destination = directory / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes((ROOT / relative).read_bytes())
    return destination


def run_case(command, directory, evidence, case, env=None):
    result = subprocess.run(command, cwd=directory, text=True, capture_output=True, timeout=120, env=env)
    (evidence / f"{case}.stdout").write_text(result.stdout)
    (evidence / f"{case}.stderr").write_text(result.stderr)
    return result


def rust_function_at(source, line):
    signature = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?fn (\w+)\(")
    function = None
    contract = False
    for index, text in enumerate(source.read_text().splitlines(), 1):
        if text.startswith(("#[verus_spec(", "#[cfg_attr(verus_keep_ghost, verus_spec(")):
            contract = True
        match = signature.match(text)
        if match:
            function = match[1]
            contract = False
        if index >= line and not contract:
            return function
    raise ValueError(f"No function found at {source}:{line}")


def verus_errors(output, directory):
    diagnostics = re.findall(r"^error: (.+)\n\s*--> (.+?):(\d+):\d+", output, re.M)
    return sorted(
        (str((directory / file).resolve().relative_to(directory.resolve())),
         rust_function_at(directory / file, int(line)), category)
        for category, file, line in diagnostics
    )


def verify_verus(args, scratch):
    for manifest in sorted((ROOT / "verification/verus").glob("*/area.json")):
        area = manifest.parent.relative_to(ROOT)
        config = json.loads(manifest.read_text())
        harness = area / "proof.rs"
        paths = {path.relative_to(ROOT) for path in manifest.parent.glob("*.rs")}
        for path in list(paths):
            for included in re.findall(r'#\[path\s*=\s*"([^"]+)"\]', (ROOT / path).read_text()):
                paths.add((ROOT / path.parent / included).resolve().relative_to(ROOT))
        for name, control in {"baseline": None, **config["controls"]}.items():
            case = f"{area.name}-{name}"
            directory = scratch / case
            for path in sorted(paths):
                clone_file(path, directory)
            if control:
                for mutation in control["mutate"]:
                    replace_once(directory / mutation["file"], mutation["find"], mutation["replace"])
            output = run_case(
                [args.verus, "--output-json", "--multiple-errors", "5", *config["flags"], str(harness)],
                directory, args.evidence, case,
            )
            result = json.loads(output.stdout)["verification-results"]
            expected = sorted(
                (item["file"], item["function"], item["category"])
                for item in (control or {}).get("expect", [])
            )
            observed = verus_errors(output.stderr, directory)
            floor = (control or config)["min_verified_functions"]
            if (
                bool(output.returncode) != bool(control)
                or result["encountered-vir-error"]
                or result["errors"] != len(expected)
                or result["verified"] < floor
                or observed != expected
            ):
                raise ValueError(f"{case}: {result}, observed {observed}, expected {expected}")
            print(f"{case}: {result['verified']} verified, {len(expected)} expected errors")


def verify_rank_bound(args, scratch):
    path = Path("verification/smt/rank-bound.smt2")
    baseline = {
        "term-roundoff": "unsat",
        "padding": "unsat",
        "frequency-and-length": "unsat",
        "rounding-monotone": "unsat",
        "sum-step": "unsat",
    }
    controls = {
        "baseline": None,
        "no-padding": ("(+ 1 (* 32 epsilon))", "1", "padding"),
        "bad-minimum-length": (
            "(<= minimum-penalty penalty)", "(> minimum-penalty penalty)", "frequency-and-length",
        ),
    }
    for name, control in controls.items():
        case = f"rank-arithmetic-{name}"
        directory = scratch / case
        source = clone_file(path, directory)
        expected = baseline.copy()
        if control:
            replace_once(source, control[0], control[1])
            expected[control[2]] = "sat"
        checked = run_case([args.z3, "-T:20", str(source)], directory, args.evidence, case)
        if checked.returncode or checked.stderr or checked.stdout.splitlines() != [
            line for item in expected.items() for line in item
        ]:
            raise ValueError(f"{case}: expected {expected}, observed {checked.stdout.strip()}")
        print(f"{case}: {'negative control has a counterexample' if control else 'five obligations checked'}")
    witness = clone_file(Path("verification/smt/rank-witness.smt2"), scratch)
    checked = run_case([args.z3, "-T:20", str(witness)], scratch, args.evidence, "rank-ieee-witness")
    if checked.returncode or checked.stderr or checked.stdout.splitlines() != [
        "exact-unpadded-reversal", "unsat", "exact-padded-bound", "unsat"
    ]:
        raise ValueError("binary64 witness did not match")
    print("rank-ieee-witness: checked exact reversal and padded bound")


def verify_lean(args, scratch):
    path = Path("verification/lean/History.lean")
    controls = {
        "baseline": None,
        "closed-upper": (
            "valid < span.upper", "valid ≤ span.upper",
            ["contains_between_endpoints", "half_open"],
        ),
        "erase-at-query-time": (
            "if key = other then none else scan system key valid tail",
            "if key = other ∧ event.commit ≤ system then none else scan system key valid tail",
            ["erased_history_cannot_change_fresh_answer", "key_partition_preserves_answer", "scan_refines_point"],
        ),
        "allow-tied-cutoff": (
            "    ¬ frontier < kth := by omega",
            "    ¬ frontier ≤ kth := by omega",
            ["cutoff_keeps_possible_ties", "unverified_row_prevents_cutoff"],
        ),
        "ignore-unexpanded-windows": (
            "  (pending ++ windows).max?\n",
            "  pending.max?\n",
            ["frontier_covers_supported_row"],
        ),
    }
    for name, mutation in controls.items():
        case = f"lean-{name}"
        directory = scratch / case
        source = clone_file(path, directory)
        if mutation:
            replace_once(source, mutation[0], mutation[1])
        command = [args.lean, str(path)] if mutation else [args.lean, "-o", "History.olean", str(path)]
        result = run_case(command, directory, args.evidence, case)
        unsupported = re.findall(r"depends on axioms: \[([^\]]*)\]", result.stdout)
        allowed = {"propext", "Classical.choice", "Quot.sound"}
        if mutation:
            source_lines = source.read_text().splitlines()
            errors = re.findall(r"^verification/lean/History\.lean:(\d+):\d+: error", result.stdout, re.M)
            observed = sorted(
                re.findall(r"^theorem (\w+)", "\n".join(source_lines[:int(line)]), re.M)[-1]
                for line in errors
            )
            if not result.returncode or observed != mutation[2]:
                raise ValueError(f"{case}: expected {mutation[2]}, observed {observed}")
        elif result.returncode or not unsupported or any(
            set(axioms.split(", ")) - allowed for axioms in unsupported
        ):
            raise ValueError(f"{case}: proof or axiom audit failed")
        print(f"{case}: {'negative control rejected' if mutation else 'proved'}")
        if not mutation:
            env = os.environ | {"LEAN_PATH": str(directory)}
            independence = Path("verification/lean/Minimality.lean")
            clone_file(independence, directory)
            checked = run_case(
                [args.lean, str(independence)], directory, args.evidence, "lean-minimality", env,
            )
            audited = re.findall(r"depends on axioms: \[([^\]]*)\]", checked.stdout)
            if checked.returncode or not audited or any(
                set(axioms.split(", ")) - allowed for axioms in audited
            ):
                raise ValueError("Lean independence proofs or axiom audit failed")
            print("lean-minimality: independence witnesses checked")
            trace = Path("verification/traces/temporal.tsv")
            replay = Path("verification/lean/Trace.lean")
            clone_file(trace, directory)
            clone_file(replay, directory)
            checked = run_case(
                [args.lean, "--run", str(replay), str(trace)], directory, args.evidence,
                "lean-shared-trace", env,
            )
            if checked.returncode or "13 trace observations match" not in checked.stdout:
                raise ValueError("Lean shared trace failed")
            print(f"lean-shared-trace: {checked.stdout.strip()}")
            generated = args.evidence / "temporal-seed-52544442.tsv"
            count = write_generated_temporal_trace(generated)
            checked = run_case(
                [args.lean, "--run", str(replay), str(generated)], directory, args.evidence,
                "lean-generated-trace", env,
            )
            if checked.returncode or f"{count} trace observations match" not in checked.stdout:
                raise ValueError("Lean rejected the generated temporal trace")
            print(f"lean-generated-trace: {checked.stdout.strip()}")


def write_generated_temporal_trace(path):
    rng = random.Random(0x52544442)
    lines = []
    observations = 0
    for case in range(128):
        lines.append(("case", f"generated_{case}"))
        facts = []
        for commit in range(1, 6):
            for _ in range(rng.randrange(1, 6)):
                key = rng.randrange(4)
                lower = rng.randrange(-10, 31)
                upper = lower + rng.randrange(0, 12)
                operation = rng.choice(("put", "put", "put", "delete", "erase"))
                value = rng.randrange(1_000)
                line = (operation, commit, key)
                if operation != "erase":
                    line += (lower, upper)
                if operation == "put":
                    line += (value,)
                lines.append(line)
                facts.append((commit, key, operation, lower, upper, value))
        for _ in range(32):
            horizon = rng.randrange(1, 6)
            system = rng.randrange(1, horizon + 1)
            key = rng.randrange(4)
            candidates = [bound for _, _, op, lower, upper, _ in facts
                          if op != "erase" for bound in (lower, upper)]
            valid = rng.choice(candidates) if candidates else 0
            expected = "none"
            for commit, subject, operation, lower, upper, value in reversed(facts):
                if commit > horizon or subject != key:
                    continue
                if operation == "erase":
                    break
                if commit <= system and lower <= valid < upper:
                    if operation == "put":
                        expected = value
                    break
            lines.append(("expect", horizon, system, key, valid, expected))
            observations += 1
    path.write_text("".join("\t".join(map(str, line)) + "\n" for line in lines))
    return observations


TLA_CASES = (
    ("Kernel", "pinned-reader", None, None),
    ("Kernel", "copied-reader", ("CopyOnRead = FALSE", "CopyOnRead = TRUE"), None),
    ("Kernel", "early-ack", ('Mutation = "none"', 'Mutation = "early_ack"'), "AckHasDurableBatch"),
    ("Kernel", "stale-term", ('Mutation = "none"', 'Mutation = "stale_term"'), "Fenced"),
    ("Kernel", "missing-root", ('Mutation = "none"', 'Mutation = "undurable_publish"'), "RootDurable"),
    ("Kernel", "collect-pinned", ('Mutation = "none"', 'Mutation = "collect_pinned"'), "PinReachable"),
    ("Kernel", "torn-batch", ('Mutation = "none"', 'Mutation = "torn_batch"'), "WholeBatches"),
    ("Kernel", "duplicate-retry", ('Mutation = "none"', 'Mutation = "duplicate_retry"'), "UniqueRequest"),
    ("Kernel", "moving-pin", ('Mutation = "none"', 'Mutation = "move_pin"'), "PinStable"),
    ("Resolution", "validated", None, None),
    ("Resolution", "serialized", ('Mode = "validated"', 'Mode = "serialized"'), None),
    ("Resolution", "blind", ('Mode = "validated"', 'Mode = "blind"'), "NoLostUpdates"),
    ("Kernel", "eventual-ack", None, None),
    ("Kernel", "unfair-apply", ("SPECIFICATION FairSpec", "SPECIFICATION Spec"), "temporal"),
    ("CacheGeneration", "cache-current-publisher", None, None),
    ("CacheGeneration", "cache-late-publisher", ("LatePublish = FALSE", "LatePublish = TRUE"), "FreshLeaseReachable"),
    ("ReadBack", "readback", None, None),
    ("ReadBack", "apply-before-readback", ('Mutation = "none"', 'Mutation = "skip_readback"'), "AppliedAfterReadBack"),
    ("ReadBack", "ack-after-append", ('Mutation = "none"', 'Mutation = "ack_on_append"'), "AckAfterApply"),
    ("LeaseAdmission", "validated-reader-lease", None, None),
    ("LeaseAdmission", "coordinated-reader-pin", ('Mode = "validated_lease"', 'Mode = "pin"'), None),
    ("LeaseAdmission", "age-only-collection", ('Mode = "validated_lease"', 'Mode = "age_only"'), "LiveReaderHasObject"),
    ("LeaseAdmission", "stale-follower-admission", ('Mode = "validated_lease"', 'Mode = "stale_follower"'), "LiveReaderHasObject"),
)


def verify_tla(args, scratch):
    for module, name, mutation, invariant in TLA_CASES:
        case = f"tlc-{name}"
        directory = scratch / case
        clone_file(Path(f"verification/tla/{module}.tla"), directory)
        config_name = "KernelProgress" if name in {"eventual-ack", "unfair-apply"} else module
        config = clone_file(Path(f"verification/tla/{config_name}.cfg"), directory)
        if mutation:
            replace_once(config, *mutation)
        log = run_case(
            [args.java, "-XX:+UseParallelGC", "-Xmx1g", "-cp", args.tlc, "tlc2.TLC",
             "-workers", "2", "-metadir", str(directory / "states"), "-noGenerateSpecTE",
             "-config", config.name, f"{module}.tla"],
            config.parent, args.evidence, case,
        )
        if invariant:
            message = (
                "Temporal property EventuallyAcknowledged was violated" if invariant == "temporal"
                else f"Invariant {invariant} is violated"
            )
            if not log.returncode or message not in log.stdout:
                raise ValueError(f"{case}: expected a counterexample to {invariant}")
            print(f"{case}: {invariant} rejected the negative control")
        else:
            states = re.search(r"([\d,]+) distinct states found, 0 states left on queue", log.stdout)
            if log.returncode or not states or "No error has been found." not in log.stdout:
                raise ValueError(f"{case}: incomplete model check")
            print(f"{case}: {states[1]} distinct states, no violation")


def verify_alloy(args, scratch):
    path = Path("verification/alloy/Reclamation.als")
    source = clone_file(path, scratch)
    out = args.evidence / "alloy"
    log = run_case(
        [args.java, "-jar", args.alloy, "exec", "-o", str(out), "-c", "*", "-t", "json", source.name],
        source.parent, args.evidence, "alloy",
    )
    if log.returncode:
        raise ValueError("Alloy failed to run")
    commands = json.loads((out / "receipt.json").read_text())["commands"]
    witnessed = {name: bool(command.get("solution")) for name, command in commands.items()}
    if witnessed != {"SurvivingReferences": False, "FirstRemovalIsEnough": True, "SharedFileSurvives": True}:
        raise ValueError(f"Unexpected Alloy search outcomes: {witnessed}")
    print("alloy: last-reference invariant holds in scope 4, control and shared-file witness found")


def verify_apalache(args, scratch):
    for mode in ("validated", "serialized", "blind"):
        case = f"apalache-{mode}"
        directory = scratch / case
        module = clone_file(Path("verification/tla/Resolution.tla"), directory)
        config = clone_file(Path("verification/tla/Resolution.cfg"), directory)
        if mode != "validated":
            replace_once(config, 'Mode = "validated"', f'Mode = "{mode}"')
        env = os.environ | {"JAVA_HOME": str(Path(args.java).resolve().parent.parent)}
        log = run_case(
            [args.apalache, f"--out-dir={args.evidence / case}", "check", f"--config={config.name}",
             "--length=8", "--inv=NoLostUpdates", module.name],
            module.parent, args.evidence, case, env,
        )
        if mode == "blind":
            if log.returncode != 12 or "state invariant 0 violated" not in log.stdout:
                raise ValueError(f"{case}: expected the lost-update counterexample")
            print(f"{case}: lost-update counterexample found")
        else:
            if log.returncode or "no error up to computation length 8" not in log.stdout:
                raise ValueError(f"{case}: symbolic check failed")
            print(f"{case}: holds up to 8 transitions")


def verify_maude(args, scratch):
    model = clone_file(Path("verification/maude/Resolution.maude"), scratch)
    log = run_case(
        [args.maude, "-no-banner", "-no-advise", str(model)], model.parent, args.evidence, "maude",
    )
    outcomes = re.findall(r"^(No solution\.|Solution 1 \(state \d+\))", log.stdout, re.M)
    if (
        log.returncode or "Warning:" in log.stderr
        or outcomes[:2] != ["No solution.", "No solution."]
        or len(outcomes) != 3 or not outcomes[-1].startswith("Solution 1")
    ):
        raise ValueError(f"Unexpected Maude search outcomes: {outcomes}")
    print("maude: serialized and validated models hold, blind model has a counterexample")


def verify_replica_boundary(args, scratch):
    source = scratch / "replica-apply"
    source.mkdir()
    subprocess.run(["git", "diff", "--exit-code", "HEAD", "--", "rust"], cwd=ROOT, check=True)
    with subprocess.Popen(["git", "archive", "--format=tar", "HEAD"], cwd=ROOT, stdout=subprocess.PIPE) as git:
        with tarfile.open(fileobj=git.stdout, mode="r|") as archive:
            archive.extractall(source, filter="data")
        if git.wait():
            raise ValueError("Could not capture the production source at HEAD")

    driver = source / "rust/crates/replication/src/driver.rs"
    original = driver.read_bytes()
    anchor = "        let observed_term = record.envelope.term;"
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    controls = {
        "baseline": None,
        "forge": ("        let _forged = AdmittedReplica(record.clone());", "E0423"),
        "apply-raw": ("        if false { self.apply_admitted(record.clone()).await?; }", "E0308"),
    }
    for name, control in controls.items():
        driver.write_bytes(original)
        if control:
            replace_once(driver, anchor, f"{anchor}\n{control[0]}")
        log = run_case(
            ["cargo", "check", "--locked", "-p", "rtdb-replication", "--message-format=json"],
            source / "rust", args.evidence, f"replica-boundary-{name}", env,
        )
        messages = [json.loads(line) for line in log.stdout.splitlines() if line.startswith("{")]
        errors = [item["message"] for item in messages
                  if item.get("reason") == "compiler-message" and item["message"]["level"] == "error"]
        expected = [control[1]] if control else []
        observed = [item["code"]["code"] if item["code"] else None for item in errors]
        if bool(log.returncode) != bool(control) or observed != expected:
            raise ValueError(f"replica-boundary-{name}: expected {expected}, observed {observed}")
        if control and not any(
            span["is_primary"] and span["file_name"].endswith("replication/src/driver.rs")
            for span in errors[0]["spans"]
        ):
            raise ValueError(f"replica-boundary-{name}: wrong failure site")
        print(f"replica-boundary-{name}: {'rejected with ' + control[1] if control else 'compiled'}")


def replay_rust_trace(args):
    env = os.environ | {
        "RTDB_TEMPORAL_TRACE": str(args.evidence / "temporal-seed-52544442.tsv"),
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    log = run_case(
        ["cargo", "test", "--locked", "-p", "rtdb-temporal", "--test", "visibility",
         "the_production_resolver_agrees_with_the_reference_trace", "--", "--exact"],
        ROOT / "rust", args.evidence, "rust-generated-trace", env,
    )
    if log.returncode or "1 passed; 0 failed" not in log.stdout:
        raise ValueError("Production resolver rejected the generated temporal trace")
    print("rust-generated-trace: production resolver agrees on all 4096 observations")


def replay_rank_witness(args, scratch):
    test = "tests::a_rounded_frequency_reversal_still_has_a_conservative_window_bound"
    path = Path("rust/crates/text/src/lib.rs")
    for mutated in (False, True):
        case = "rust-rank-unpadded" if mutated else "rust-rank-baseline"
        directory = scratch / case
        source = clone_file(path, directory)
        if mutated:
            replace_once(source, " * (1.0 + 16.0 * f64::EPSILON)", "")
        # rtdb-text has no dependencies, so the unchanged source can compile on its own.
        built = run_case(
            ["rustc", "--edition=2024", "--test", str(source), "-o", "rtdb-text-tests"],
            directory, args.evidence, f"{case}-build",
        )
        if built.returncode:
            raise ValueError(f"{case}: Rust witness failed to compile")
        checked = run_case(
            ["./rtdb-text-tests", "--exact", test],
            directory, args.evidence, case,
        )
        if mutated:
            if (
                checked.returncode != 101
                or f"{test} ... FAILED" not in checked.stdout
                or "rounded score exceeds its window bound" not in checked.stdout
                or "0 passed; 1 failed" not in checked.stdout
            ):
                raise ValueError(f"{case}: expected one failed window bound assertion")
        elif checked.returncode or "1 passed; 0 failed" not in checked.stdout:
            raise ValueError(f"{case}: expected one passing window bound assertion")
        print(f"{case}: {'negative control rejected' if mutated else 'production witness passes'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lean", required=True)
    parser.add_argument("--verus", required=True)
    parser.add_argument("--z3", default=os.environ.get("VERUS_Z3_PATH"), required="VERUS_Z3_PATH" not in os.environ)
    parser.add_argument("--java", required=True)
    parser.add_argument("--tlc", required=True)
    parser.add_argument("--alloy", required=True)
    parser.add_argument("--apalache", required=True)
    parser.add_argument("--maude", required=True)
    parser.add_argument("--rust-boundary", action="store_true")
    parser.add_argument("--native-replay", action="store_true")
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    if os.environ.get("BIJECTION_LOCAL_EXECUTION") != "owner-authorized":
        parser.error("local proof execution requires BIJECTION_LOCAL_EXECUTION=owner-authorized")
    args.evidence = args.evidence.resolve()
    args.evidence.mkdir(parents=True, exist_ok=False)
    source_paths = [
        path for path in (ROOT / "verification").rglob("*")
        if path.is_file() and path.suffix in {".lean", ".tla", ".cfg", ".als", ".maude", ".py", ".rs", ".json", ".tsv", ".smt2"}
    ]
    source_paths.extend(ROOT / path for path in (
        "rust/crates/text/src/lib.rs",
        "rust/crates/search/src/file.rs",
        "rust/crates/search/src/accelerate.rs",
        "rust/crates/expr/src/search.rs",
        "rust/crates/expr/src/evaluate.rs",
        "rust/crates/query/src/accelerate.rs",
    ))
    for path in list(source_paths):
        if path.name == "proof.rs":
            for included in re.findall(r'#\[path\s*=\s*"([^"]+)"\]', path.read_text()):
                source_paths.append((path.parent / included).resolve())
    inputs = {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(source_paths)
    }
    (args.evidence / "inputs.json").write_text(json.dumps(inputs, indent=2) + "\n")
    with tempfile.TemporaryDirectory(prefix="rtdb-proof-") as scratch:
        directory = Path(scratch)
        verify_lean(args, directory)
        verify_tla(args, directory)
        verify_verus(args, directory)
        verify_rank_bound(args, directory)
        verify_alloy(args, directory)
        verify_apalache(args, directory)
        verify_maude(args, directory)
        if args.rust_boundary:
            verify_replica_boundary(args, directory)
        if args.native_replay:
            replay_rank_witness(args, directory)
            replay_rust_trace(args)


if __name__ == "__main__":
    main()
