"""Run RTDB's local proof studies in disposable copies and retain evidence."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
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
    if command[:2] == ["cargo", "test"] and "--no-run" not in command:
        # A fresh source copy rebuilds the workspace; the build gets its own limit so the case limit bounds only the run.
        split = command.index("--") if "--" in command else len(command)
        exact = command[split + 1:] == ["--exact"]
        build_command = [*command[:split], "--no-run"]
        if exact:
            build_command.append("--message-format=json")
        build = run_case_with_timeout(build_command, directory, evidence, f"{case}-build", env, 3600)
        if build.returncode:
            return build
        if exact:
            artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith("{")]
            binaries = [artifact for artifact in artifacts if artifact.get("reason") == "compiler-artifact"
                        and artifact["profile"]["test"] and artifact.get("executable")]
            if len(binaries) == 1:
                # Use Cargo's package working directory. A second cargo invocation can wait
                # on another build's lock instead of running the already-built test.
                binary = binaries[0]
                test_command = [binary["executable"], command[split - 1], "--exact"]
                package = Path(binary["manifest_path"]).parent
                return run_case_with_timeout(test_command, package, evidence, case, env, 300)
    return run_case_with_timeout(command, directory, evidence, case, env, 300 if command[0] == "cargo" else 120)


def run_case_with_timeout(command, directory, evidence, case, env, timeout):
    (evidence / f"{case}.command.json").write_text(json.dumps({
        "command": command,
        "directory": str(directory),
        "timeout_seconds": timeout,
    }, indent=2) + "\n")
    try:
        result = subprocess.run(
            command, cwd=directory, text=True, capture_output=True,
            timeout=timeout, env=env,
        )
    except subprocess.TimeoutExpired as error:
        (evidence / f"{case}.stdout").write_bytes(error.stdout or b"")
        (evidence / f"{case}.stderr").write_bytes(error.stderr or b"")
        raise TimeoutError(f"{case} timed out after {error.timeout} seconds") from error
    (evidence / f"{case}.stdout").write_text(result.stdout)
    (evidence / f"{case}.stderr").write_text(result.stderr)
    return result


def rust_function_at(source, line):
    signature = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:proof\s+)?fn (\w+)\s*[<(]")
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
            expected_failed_functions = {(file, function) for file, function, _ in expected}
            if (
                bool(output.returncode) != bool(control)
                or result["encountered-vir-error"]
                or result["errors"] != len(expected_failed_functions)
                or result["verified"] < floor
                or observed != expected
            ):
                raise ValueError(f"{case}: {result}, observed {observed}, expected {expected}")
            print(f"{case}: {result['verified']} verified, {len(expected)} expected diagnostics")


def verify_jvm_leaf_merge(args, scratch):
    path = Path("core/src/main/java/xtdb/trie/LeafMerge.java")
    capacity = "      @ requires left.length + right.length <= Integer.MAX_VALUE;"
    cases = {
        "baseline": None,
        "larger-head": (
            "order.compare(left[leftIndex], right[rightIndex]) < 0",
            "order.compare(left[leftIndex], right[rightIndex]) > 0",
            "left.length == 1 && right.length == 1 && order.rank(left[0]) == 0 && order.rank(right[0]) == 1",
            "Assert",
        ),
        "drop-suffix": (
            "while (next < merged.length)",
            "while (next < merged.length && leftIndex < left.length && rightIndex < right.length)",
            "left.length == 1 && right.length == 0",
            "Assert",
        ),
        "change-right-value": (
            "merged[next] = right[rightIndex];",
            "merged[next] = 0;",
            "left.length == 0 && right.length == 1 && right[0] != 0",
            "LoopInvariant",
        ),
    }
    version = run_case([args.openjml, "--version"], ROOT, args.evidence, "openjml-version")
    if version.returncode or "openjml " not in version.stdout:
        raise ValueError("OpenJML did not report its version")
    for name, mutation in cases.items():
        case = f"openjml-leaf-merge-{name}"
        directory = scratch / case
        source = clone_file(path, directory)
        flags = []
        expected_diagnostic = None
        if mutation:
            before, after, witness, expected_diagnostic = mutation
            replace_once(source, before, after)
            replace_once(source, capacity, f"      @ requires {witness};\n{capacity}")
        if name == "larger-head":
            # The reversed comparator reaches the right branch for this witness.
            # End this control after the order assertion, before loop preservation.
            assertion = "//@ assert leftIndex == left.length || order.rank(right[rightIndex]) <= order.rank(left[leftIndex]);"
            replace_once(source, assertion, assertion + "\n                //@ halt;")
            flags = ["--method=merge", "--split=AB"]
        checked = run_case(
            [args.openjml, "--esc", "--timeout=40", "--timeout-method=65",
             "--esc-max-warnings=1", "--progress", *flags, str(source)],
            directory, args.evidence, case,
        )
        output = checked.stdout + "\n" + checked.stderr
        observed = {label: int(count) for label, count in re.findall(
            r"(?m)^\s*(Valid|Invalid|Infeasible|Timeout|Error|Skipped):\s+(\d+)\s*$", output,
        )}
        expected = {
            "Valid": 2,
            "Invalid": 1 if mutation else 0,
            "Infeasible": 0,
            "Timeout": 0,
            "Error": 0,
            "Skipped": 2,
        }
        if mutation:
            expected["Valid"] = 1
        if flags:
            expected.update(Valid=0, Skipped=3)
        diagnostics = re.findall(r"LeafMerge\.java:\d+: verify:.*\((Assert|LoopInvariant)\) in method merge", output)
        wanted = [expected_diagnostic] if mutation else []
        if checked.returncode != (6 if mutation else 0) or observed != expected or diagnostics != wanted:
            raise ValueError(f"{case}: expected {expected} and {wanted}, observed {observed} and {diagnostics}")
        print(f"{case}: {'negative control rejected' if mutation else 'proved'}")


def verify_residence_clock_margin(args, scratch):
    path = Path("verification/smt/residence-clock-margin.smt2")
    directory = scratch / "smt-residence-clock-margin"
    clone_file(path, directory)
    checked = run_case([args.z3, str(path)], directory, args.evidence, "smt-residence-clock-margin")
    if checked.returncode or checked.stdout.split() != ["unsat", "sat", "sat"]:
        raise ValueError("Clock and I/O margin must exclude overlap, and omitting either bound must admit it")
    print("smt-residence-clock-margin: sufficient margin and two counterexamples checked")


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


def verify_rank_cutoff_order(args, scratch):
    path = Path("verification/smt/rank-cutoff-order.smt2")
    for name, expected in (("baseline", "unsat"), ("distinct-zeroes", "sat")):
        case = f"rank-cutoff-order-{name}"
        directory = scratch / case
        source = clone_file(path, directory)
        if name == "distinct-zeroes":
            replace_once(source, "(ite (= bits #x8000000000000000) #x0000000000000000 bits)", "bits")
        checked = run_case([args.z3, "-T:20", str(source)], directory, args.evidence, case)
        if checked.returncode or checked.stderr or checked.stdout.splitlines() != [expected]:
            raise ValueError(f"{case}: expected {expected}, observed {checked.stdout.strip()}")
        print(f"{case}: {expected}")


def lean_error_theorems(source, relative, output):
    source_lines = source.read_text().splitlines()
    errors = re.findall(rf"^{re.escape(str(relative))}:(\d+):\d+: error", output, re.M)
    return sorted(
        re.findall(r"^theorem (\w+)", "\n".join(source_lines[:int(line)]), re.M)[-1]
        for line in errors
    )


def verify_ceiling(args, scratch, history):
    path = Path("verification/lean/Ceiling.lean")
    controls = {
        "baseline": None,
        "closed-upper": (
            "  | some upper => decide (system < upper)", "  | some upper => decide (system ≤ upper)",
            ["emitted_below_closed_ceiling"],
        ),
        "unchanged-ceiling": (
            "let later := emit system (some head.1) tail", "let later := emit system ceiling tail",
            ["emission_refines_latest", "emission_refines_latest", "emitted_below_closed_ceiling"],
        ),
    }
    env = os.environ | {"LEAN_PATH": str(history)}
    allowed = {"propext", "Classical.choice", "Quot.sound"}
    for name, mutation in controls.items():
        case = f"lean-ceiling-{name}"
        directory = scratch / case
        source = clone_file(path, directory)
        if mutation:
            replace_once(source, mutation[0], mutation[1])
        result = run_case([args.lean, str(path)], directory, args.evidence, case, env)
        if mutation:
            observed = lean_error_theorems(source, path, result.stdout)
            if not result.returncode or observed != mutation[2]:
                raise ValueError(f"{case}: expected {mutation[2]}, observed {observed}")
        else:
            audited = re.findall(r"depends on axioms: \[([^\]]*)\]", result.stdout)
            if result.returncode or len(audited) != 2 or any(set(axioms.split(", ")) - allowed for axioms in audited):
                raise ValueError("Lean ceiling proof or axiom audit failed")
        print(f"{case}: {'negative control rejected' if mutation else 'proved'}")


def verify_compaction_projection(args, scratch, history):
    path = Path("verification/lean/Compaction.lean")
    controls = {
        "baseline": None,
        "write-to-other-bucket": (
            "bucketOf event.fact.key = bucket", "bucketOf event.fact.key + 1 = bucket",
            ["key_projection_through_its_bucket"],
        ),
        "read-from-other-bucket": (
            "point (forBucket view bucketOf (bucketOf key)) system key valid",
            "point (forBucket view bucketOf (bucketOf key + 1)) system key valid",
            ["partitioned_point_refines_the_reference"],
        ),
    }
    allowed = {"propext", "Classical.choice", "Quot.sound"}
    env = os.environ | {"LEAN_PATH": str(history)}
    for name, mutation in controls.items():
        case = f"lean-compaction-{name}"
        directory = scratch / case
        source = clone_file(path, directory)
        if mutation:
            replace_once(source, mutation[0], mutation[1])
        checked = run_case([args.lean, str(path)], directory, args.evidence, case, env)
        if mutation:
            observed = lean_error_theorems(source, path, checked.stdout)
            if not checked.returncode or observed != mutation[2]:
                raise ValueError(f"{case}: expected {mutation[2]}, observed {observed}")
        else:
            audited = re.findall(r"depends on axioms: \[([^\]]*)\]", checked.stdout)
            if checked.returncode or len(audited) != 2 or any(set(axioms.split(", ")) - allowed for axioms in audited):
                raise ValueError("Lean compaction projection proof or axiom audit failed")
        print(f"{case}: {'negative control rejected' if mutation else 'proved'}")


def verify_lean_model(args, scratch, path, study, controls, theorems, env=None):
    allowed = {"propext", "Classical.choice", "Quot.sound"}
    for name, mutation in {"baseline": None, **controls}.items():
        case = f"lean-{study}-{name}"
        directory = scratch / case
        source = clone_file(path, directory)
        if mutation:
            replace_once(source, mutation[0], mutation[1])
        checked = run_case([args.lean, str(path)], directory, args.evidence, case, env)
        if mutation:
            observed = lean_error_theorems(source, path, checked.stdout)
            expected = sorted(mutation[2])
            if not checked.returncode or observed != expected:
                raise ValueError(f"{case}: expected {expected}, observed {observed}")
        else:
            audited = re.findall(r"depends on axioms: \[([^\]]*)\]|does not depend on any axioms", checked.stdout)
            if checked.returncode or len(audited) != theorems or any(
                set(re.findall(r"[\w.]+", axioms)) - allowed for axioms in audited
            ):
                raise ValueError(f"{case}: proof or axiom audit failed")
        print(f"{case}: {'negative control rejected' if mutation else 'proved'}")



def verify_search_member_bounds(args, scratch):
    path = Path("verification/lean/SearchMemberBounds.lean")
    assignment = "prefixCount doc (startsFrom 0 counts) - 1"
    laws = ["directory_owner_is_the_reference_owner", "empty_members_on_either_side_do_not_own_the_document"]
    controls = {
        "strict-start": (assignment, "(List.takeWhile (· < doc) (startsFrom 0 counts)).length - 1", laws),
        "collapse-empty": (assignment, "prefixCount doc (startsFrom 0 (counts.filter (· != 0))) - 1", laws),
    }
    verify_lean_model(args, scratch, path, "search-member-bounds", controls, 4)


def verify_search_windows(args, scratch):
    path = Path("verification/lean/SearchWindows.lean")
    controls = {
        "longest-document": (
            "min posting.length impact.length", "max posting.length impact.length",
            ["summary_bounds_every_posting", "summary_bounds_every_posting",
             "summary_combines_extrema_from_different_postings"],
        ),
        "intersect-next-window": (
            "(windows term).contains window", "(windows term).contains (window + 1)",
            ["intersection_keeps_a_shared_window", "intersection_keeps_every_matching_document"],
        ),
        "shift-lower-bound": (
            "min (window * 128) maxWord", "min (window * 128 + 1) maxWord",
            ["a_window_reads_its_first_document", "scanning_interval_matches_assignment",
             "scanning_interval_matches_assignment"],
        ),
    }
    verify_lean_model(args, scratch, path, "search-windows", controls, 7)


def verify_partitioned_top_k(args, scratch):
    path = Path("verification/lean/PartitionedTopK.lean")
    controls = {
        "drop-ties": (
            "row.score < candidate.score",
            "(row.score < candidate.score ∨ row.score = candidate.score ∧ candidate.id < row.id)",
            ["prefix_row_survives_partition_pruning", "tied_rows_remain_eligible"],
        ),
        "ignore-offset": (
            "rows.filter (keeps rows (offset + fetch))", "rows.filter (keeps rows fetch)",
            ["an_offset_keeps_the_following_row", "ordered_page_survives_partition_pruning"],
        ),
    }
    verify_lean_model(args, scratch, path, "partitioned-top-k", controls, 5)


def verify_catalog_recovery(args, scratch):
    path = Path("verification/lean/CatalogRecovery.lean")
    controls = {
        "forget-pending": (
            "catalog.filterMap pendingEntry", "[]",
            ["restored_pending_is_exact", "an_exact_block_promotes_both_recencies"],
        ),
        "query-next-block": (
            "if (atBlock, trie.recency) ∈ pending", "if (atBlock + 1, trie.recency) ∈ pending",
            ["current_after_restore_matches_catalog_scan", "current_after_restore_matches_catalog_scan",
             "an_exact_block_promotes_both_recencies"],
        ),
    }
    verify_lean_model(args, scratch, path, "catalog-recovery", controls, 4)


def verify_checkpoint_discovery(args, scratch):
    path = Path("verification/lean/CheckpointDiscovery.lean")
    controls = {
        "allow-same": (
            "if attempted < index", "if attempted ≤ index",
            ["attempts_strictly_increase", "attempts_strictly_increase", "attempts_strictly_increase",
             "retries_fit_in_the_remaining_indices", "retries_fit_in_the_remaining_indices",
             "same_index_cannot_be_repeated"],
        ),
    }
    verify_lean_model(args, scratch, path, "checkpoint-discovery", controls, 3)


def verify_compaction_merge(args, scratch):
    path = Path("verification/lean/CompactionMerge.lean")
    controls = {
        "admit-larger-head": (
            "key row ≤ key candidate\n", "key row ≤ key candidate + 1\n",
            ["any_eligible_head_lower_bounds_the_input"],
        ),
        "stop-before-last": (
            "pages.flatten.length ≤ 0", "pages.flatten.length ≤ 1",
            ["drain_preserves_all_occurrences"],
        ),
    }
    verify_lean_model(args, scratch, path, "compaction-merge", controls, 5)


def verify_compaction_planner(args, scratch):
    path = Path("verification/lean/CompactionPlanner.lean")
    controls = {
        "drop-coarse-leaf": (
            "| .leaf rows => .leaf rows", "| .leaf _ => .empty",
            ["expose_preserves_route"],
        ),
        "route-bucket-one-to-zero": (
            "then readTask iid (depth + 1) remaining one", "then readTask iid (depth + 1) remaining zero",
            ["read_task_agrees_with_per_segment_route"],
        ),
        "ignore-job-partition": (
            "if admitsPrefix iid 0 part then", "if true || admitsPrefix iid 0 part then",
            ["read_part_preserves_matching_occurrences"],
        ),
    }
    verify_lean_model(args, scratch, path, "compaction-planner", controls, 3)


def verify_compaction_intervals(args, scratch, history):
    path = Path("verification/lean/CompactionIntervals.lean")
    controls = {
        "drop-first-uncovered-point": (
            "max span.lower blocker.upper", "max span.lower (blocker.upper + 1)",
            ["subtraction_preserves_exactly_uncovered_points"] * 2,
        ),
        "forget-new-blocker": (
            "normalize (command.span :: blockers) older", "normalize blockers older",
            ["normalization_preserves_visible_winners", "normalized_commands_respect_prior_blockers",
             "normalization_makes_all_output_fragments_disjoint", "normalization_makes_all_output_fragments_disjoint"],
        ),
        "lose-payload": (
            "map fun piece => { command with span := piece }", "map fun piece => { command with span := piece, value := none }",
            ["clipped_command_preserves_payload_when_visible"] * 2,
        ),
        "drop-passthrough-event": (
            "| .cohort key commit commands => (normalize [] commands).map (encode key commit)\n  | .event passthrough => [passthrough]",
            "| .cohort key commit commands => (normalize [] commands).map (encode key commit)\n  | .event _ => []",
            ["normalization_preserves_arbitrary_surrounding_history"],
        ),
        "reverse-command-priority": (
            "| .cohort key commit commands => (normalize [] commands).map (encode key commit)",
            "| .cohort key commit commands => (normalize [] commands.reverse).map (encode key commit)",
            ["normalization_preserves_arbitrary_surrounding_history"],
        ),
    }
    env = os.environ | {"LEAN_PATH": str(history)}
    verify_lean_model(args, scratch, path, "compaction-intervals", controls, 7, env)


def verify_system_time(args, scratch):
    path = Path("verification/lean/SystemTime.lean")
    controls = {
        "raw-catalog-clock": (
            "| .catalog clock => automatic maximum previous clock",
            "| .catalog clock => some clock",
            ["chosen_time_advances", "catalog_and_explicit_refusals_use_the_same_clock"],
        ),
    }
    verify_lean_model(args, scratch, path, "system-time", controls, 5)


def verify_message_position(args, scratch):
    path = Path("verification/lean/MessagePosition.lean")
    controls = {
        "allow-sign-bit": (
            "def epochLimit : Int := 32768",
            "def epochLimit : Int := 32769",
            ["an_epoch_fits_exactly_within_the_supported_range", "supported_positions_fit_signed_order"],
        ),
        "short-epoch-spacing": (
            "epoch * offsetLimit + offset",
            "epoch * (offsetLimit - 1) + offset",
            ["an_epoch_fits_exactly_within_the_supported_range", "order_is_lexicographic", "packed_round_trip"],
        ),
    }
    verify_lean_model(args, scratch, path, "message-position", controls, 5)


def verify_type_join(args, scratch):
    path = Path("verification/lean/TypeJoin.lean")
    controls = {
        "intersect-scalar-legs": (
            "first.accepts kind ∨ second.accepts kind, first.allowsNull",
            "first.accepts kind ∧ second.accepts kind, first.allowsNull",
            ["bottom_contributes_no_scalar_legs", "field_join_assoc", "null_contributes_only_null",
             "scalar_join_assoc", "scalar_join_comm", "scalar_join_preserves_membership"],
        ),
        "forget-missing-field": (
            "some (joinScalars null value)",
            "some value",
            ["field_absence_contributes_null_if_the_key_exists"],
        ),
    }
    verify_lean_model(args, scratch, path, "type-join", controls, 10)


def verify_floor_preservation(args, scratch):
    path = Path("verification/lean/CatalogAdmissionFloor.lean")
    controls = {
        "admit-without-floor-check": (
            "(allowed : permits state.floor cut)",
            "(allowed : True)",
            ["a_step_preserves_held_readers"],
        ),
        "advance-across-a-reader": (
            "(allAllow : ∀ cut ∈ state.readers, permits (some floor) cut)",
            "(allAllow : True)",
            ["a_step_preserves_held_readers"],
        ),
        "rewind-the-floor": (
            "(covered : coversPreviousFloor state.floor (some floor))",
            "(covered : True)",
            ["a_step_never_lowers_the_floor"],
        ),
    }
    verify_lean_model(args, scratch, path, "catalog-floor-preservation", controls, 4)


def verify_retirement_identity(args, scratch):
    path = Path("verification/lean/RetirementIdentity.lean")
    controls = {
        "select-by-logical-key": (
            "  selected.identity = observed.identity\n",
            "  selected.logicalKey = observed.logicalKey\n",
            ["accepted_observation_identifies_selected", "a_replacement_can_share_the_key_and_digest",
             "another_namespace_can_share_the_key_and_version"],
        ),
        "erase-by-physical-key": (
            "decide (publication.identity ≠ identity)",
            "decide (publication.identity.physicalKey ≠ identity.physicalKey)",
            ["exact_delete_retains_any_other_publication"],
        ),
        "reuse-name-on-reset": (
            "  ⟨logicalKey, epoch, inputDigest⟩",
            "  ⟨logicalKey, 0, inputDigest⟩",
            ["reset_disjoins_qualified_names"],
        ),
        "omit-input-digest-from-name": (
            "  ⟨logicalKey, epoch, inputDigest⟩",
            "  ⟨logicalKey, epoch, 0⟩",
            ["different_inputs_disjoin_qualified_names"],
        ),
    }
    verify_lean_model(args, scratch, path, "retirement-identity", controls, 7)



def verify_retirement_certificate(args, scratch):
    dependency = Path("verification/lean/RetirementIdentity.lean")
    directory = scratch / "lean-retirement-certificate-dependency"
    clone_file(dependency, directory)
    compiled = run_case([args.lean, "-o", "RetirementIdentity.olean", str(dependency)],
                        directory, args.evidence, "lean-retirement-certificate-dependency")
    if compiled.returncode:
        raise ValueError("Lean retirement certificate dependency failed")
    controls = {
        "ignore-holds": (
            "retirement ≤ state.head ∧ allHoldsAfter retirement state",
            "retirement ≤ state.head ∧ True",
            ["certificate_preserved_by_step"],
        ),
        "accept-stale-observation": (
            "(fresh : state.proposal = some state.revision)",
            "(fresh : state.proposal.isSome = true)",
            ["certificate_preserved_by_step"],
        ),
        "match-by-key": (
            "(samePublication : selected = observed)",
            "(samePublication : selected.physicalKey = observed.physicalKey)",
            ["certificate_preserved_by_step"],
        ),
        "admit-below-floor": (
            "(aboveFloor : state.floor ≤ hold.generation)",
            "(aboveFloor : 0 ≤ hold.generation)",
            ["certificate_preserved_by_step"],
        ),
    }
    verify_lean_model(args, scratch, Path("verification/lean/RetirementCertificate.lean"),
                      "retirement-certificate", controls, 6, os.environ | {"LEAN_PATH": str(directory)})


def verify_reader_reclamation_minimality(args, scratch):
    path = Path("verification/lean/ReaderReclamationMinimality.lean")
    controls = {
        "reap-too-early": (
            "| [time] => decide (limit ≤ time)", "| [time] => decide (limit ≤ time + 1)",
            ["a_valid_bound_makes_timed_reclamation_safe"],
        ),
        "assume-drained": (
            "| [result] => result.drained", "| [_] => true",
            ["confirmed_drain_makes_reclamation_safe"],
        ),
    }
    verify_lean_model(args, scratch, path, "reader-reclamation-minimality", controls, 5)



def verify_group_publication(args, scratch):
    path = Path("verification/lean/GroupPublication.lean")
    controls = {
        "accept-unwitnessed-group": (
            "!records.isEmpty && records.all", "records.all",
            ["a_binding_covers_each_held_record", "an_unwitnessed_group_cannot_bind"],
        ),
        "trust-any-member": (
            "records.all (fun publication", "records.any (fun publication",
            ["a_binding_covers_each_held_record", "a_legacy_member_cannot_bind_the_group",
             "several_versions_cannot_bind_the_same_group"],
        ),
    }
    verify_lean_model(args, scratch, path, "group-publication", controls, 5)


def verify_basis_tokens(args, scratch):
    path = Path("verification/lean/BasisTokens.lean")
    controls = {
        "take-the-smaller-required-position": (
            "List.zipWith max left right",
            "List.zipWith min left right",
            ["merged_positions_cover_both_tokens", "a_common_upper_bound_covers_the_merge"],
        ),
        "treat-the-entire-first-second-as-absent": (
            "if wire = zeroTime then none else some wire",
            "if wire.seconds = 0 then none else some wire",
            ["every_non_default_time_round_trips_without_losing_precision",
             "maps_preserve_positions_and_normalize_only_the_default_time",
             "maps_preserve_positions_and_normalize_only_the_default_time"],
        ),
    }
    verify_lean_model(args, scratch, path, "basis-tokens", controls, 6)


def verify_run_encoding(args, scratch):
    path = Path("verification/lean/RunEncoding.lean")
    controls = {
        "inclusive-end": (
            "if logicalIndex < stop",
            "if logicalIndex ≤ stop",
            ["a_returned_value_has_a_covering_run", "a_returned_value_has_a_covering_run",
             "end_lookup_matches_expansion", "end_lookup_matches_expansion",
             "first_end_after_at_position", "first_end_after_at_position"],
        ),
        "lose-slice-origin": (
            "firstEndAfter (start + row) runs",
            "firstEndAfter (0 + row) runs",
            ["arrow_search_for_a_slice_matches_plain", "encoded_slice_read_matches_plain"],
        ),
        "stay-at-exact-end": (
            "| .found j => j + 1", "| .found j => j",
            ["arrow_physical_index_is_first_exclusive_end", "arrow_physical_index_is_first_exclusive_end"],
        ),
        "read-before-insertion": (
            "| .absent j => j", "| .absent j => j - 1",
            ["arrow_physical_index_is_first_exclusive_end", "arrow_physical_index_is_first_exclusive_end"],
        ),
        "drop-slice-offset-in-cast": (
            "(BitVec.ofNat width (start + row)).toInt = Int.ofNat (start + row)",
            "(BitVec.ofNat width row).toInt = Int.ofNat (start + row)",
            ["signed_index_cast_preserves_sliced_coordinate"],
        ),
    }
    verify_lean_model(args, scratch, path, "run-encoding", controls, 9)


def verify_memory_admission(args, scratch):
    path = Path("verification/lean/MemoryAdmission.lean")
    controls = {
        "omit-rollback": (
            "else (false, rollBack provisional request)",
            "else (false, provisional)",
            ["a_refused_admission_restores_both_counters"],
        ),
        "omit-parent-limit": (
            "nextGrant ≤ limits.wordMaximum ∧ nextParent ≤ limits.parent",
            "nextGrant ≤ limits.wordMaximum",
            ["a_refused_admission_restores_both_counters", "admitted_charge_stays_within_its_limits"],
        ),
    }
    verify_lean_model(args, scratch, path, "memory-admission", controls, 6)


def verify_memory_trie(args, scratch):
    path = Path("verification/lean/MemoryTrie.lean")
    controls = {
        "drop-at-max-depth": (
            "| 0 => .leaf rows",
            "| 0 => .leaf []",
            ["lookup_rebuild", "rebuild_preserves_rows"],
        ),
        "look-in-wrong-bucket": (
            "| .three => lookup iidOf route target (depth + 1) three",
            "| .three => lookup iidOf route target (depth + 1) two",
            ["lookup_addAll", "lookup_rebuild"],
        ),
    }
    verify_lean_model(args, scratch, path, "memory-trie", controls, 5)


def verify_live_row_address(args, scratch):
    path = Path("verification/lean/LiveRowAddress.lean")
    controls = {
        "include-own-chunk-in-start": (
            "(chunks.take index).flatten.length",
            "(chunks.take (index + 1)).flatten.length",
            ["chunk_read_matches_flat_history", "last_start_locates_a_row",
             "next_chunk_starts_at_exclusive_end"],
        ),
        "reverse-joined-chunks": (
            "prior ++ (left ++ right) :: suffix",
            "prior ++ (right ++ left) :: suffix",
            ["edit_preserves_existing_rows"],
        ),
    }
    verify_lean_model(args, scratch, path, "live-row-address", controls, 5)


def verify_iid_byte_order(args, scratch):
    path = Path("verification/lean/IidByteOrder.lean")
    controls = {
        "compare-signed-words": (
            "(left ^^^ 0x8000000000000000#64).slt (right ^^^ 0x8000000000000000#64) = true",
            "left.slt right = true",
            ["arrow_word_comparison_matches_eight_bytes"],
        ),
        "reverse-head-order": (
            "arrowWordBefore leftHead rightHead ∨",
            "arrowWordBefore rightHead leftHead ∨",
            ["arrow_leaf_comparison_matches_iid_then_descending_row"],
        ),
        "older-row-first": (
            "(leftTail = rightTail ∧ rightRow < leftRow)))",
            "(leftTail = rightTail ∧ leftRow < rightRow)))",
            ["arrow_leaf_comparison_matches_iid_then_descending_row"],
        ),
        "rank-orders-oldest-first": (
            "unsignedWord iid * 4294967296 + (4294967295 - row)",
            "unsignedWord iid * 4294967296 + row",
            ["iid_row_rank_agrees_with_the_key"],
        ),
    }
    verify_lean_model(args, scratch, path, "iid-byte-order", controls, 7)


def verify_leaf_runs(args, scratch):
    path = Path("verification/lean/LeafRuns.lean")
    controls = {
        "reuse-stale-cache": (
            "⟨leaf.data, leaf.pending ++ added, none⟩",
            "⟨leaf.data, leaf.pending ++ added, leaf.cached⟩",
            ["pending_append_preserves_order"],
        ),
        "omit-pending-on-overflow": (
            "settled (mergeRuns rank leaf.data (mergeRuns rank (canonical rank leaf.pending) (canonical rank batch)))",
            "settled (mergeRuns rank leaf.data (canonical rank batch))",
            ["bulk_represents_all_old_and_new_occurrences"],
        ),
        "skip-bucket-filter": (
            "settled ((rowsOf rank leaf).filter select)",
            "settled (rowsOf rank leaf)",
            ["child_represents_the_matching_occurrences"],
        ),
        "skip-adjacent-comparison": (
            "∀ second, tail.head? = some second → rowLE rank first second = true",
            "∀ second, tail.head? = some second → rowLE rank second second = true",
            ["adjacent_validation_implies_leaf_order", "adjacent_validation_implies_leaf_order"],
        ),
    }
    rank = Path("verification/lean/IidByteOrder.lean")
    dependency = scratch / "lean-leaf-runs-rank"
    clone_file(rank, dependency)
    built = run_case([args.lean, "-DwarningAsError=true", "-o", "IidByteOrder.olean", str(rank)],
                     dependency, args.evidence, "lean-leaf-runs-rank")
    if built.returncode:
        raise ValueError("Lean leaf rank dependency did not compile")
    verify_lean_model(args, scratch, path, "leaf-runs", controls, 7,
                      os.environ | {"LEAN_PATH": str(dependency)})
    return dependency


def verify_trie_sibling_order(args, scratch, dependency):
    path = Path("verification/lean/TrieSiblingOrder.lean")
    controls = {
        "wrong-byte-at-depth": (
            "(bytes[depth / 4]?).bind",
            "(bytes[depth / 2]?).bind",
            ["bucket_at_depth_uses_the_corresponding_byte_lane"],
        ),
        "reverse-siblings": (
            "(bucketOrder : buckets.Pairwise fun first second => first.val < second.val)",
            "(bucketOrder : buckets.Pairwise fun first second => second.val < first.val)",
            ["sibling_order_recovers_the_source_run"],
        ),
        "drop-first-visited-row": (
            "    buckets.flatMap rows = source := by",
            "    (buckets.flatMap rows).tail = source := by",
            ["sibling_order_recovers_the_source_run"],
        ),
    }
    leaf_runs = Path("verification/lean/LeafRuns.lean")
    clone_file(leaf_runs, dependency)
    env = os.environ | {"LEAN_PATH": str(dependency)}
    built = run_case([args.lean, "-DwarningAsError=true", "-o", "LeafRuns.olean", str(leaf_runs)],
                     dependency, args.evidence, "lean-trie-sibling-order-leaf-runs", env)
    if built.returncode:
        raise ValueError("Lean leaf-run dependency did not compile")
    verify_lean_model(args, scratch, path, "trie-sibling-order", controls, 7, env)


def verify_snapshot_ownership(args, scratch):
    path = Path("verification/lean/SnapshotOwnership.lean")
    controls = {
        "retain-retired-owner": (
            "published := false\n    references := s.references - 1",
            "published := true\n    references := s.references - 1",
            ["any_step_preserves_reference_ownership"],
        ),
        "release-with-another-reader": (
            "readers := s.readers - 1\n    references := s.references - 1\n"
            "    released := s.released || s.references == 1",
            "readers := s.readers - 1\n    references := s.references - 1\n"
            "    released := s.released || s.references == 2",
            ["after_retirement_last_reader_releases_resources", "any_step_preserves_reference_ownership",
             "any_step_preserves_reference_ownership"],
        ),
        "acquire-after-retirement": (
            "s.published = true → Step s (openReader s)",
            "s.published = s.published → Step s (openReader s)",
            ["any_step_preserves_reference_ownership", "released_resources_admit_no_further_operations"],
        ),
    }
    verify_lean_model(args, scratch, path, "snapshot-ownership", controls, 7)


def verify_jvm_trie_pending_logs(args, scratch):
    path = Path("verification/lean/JvmTriePendingLogs.lean")
    controls = {
        "overwrite-shared-buffer": (
            "(buffers ++ [log], ⟨buffers.length, log.length⟩)",
            "(buffers.set parent.buffer log, ⟨parent.buffer, log.length⟩)",
            ["child_reads_its_parent_prefix_and_added_rows",
             "fork_history_extends_the_allocated_buffers",
             "fork_preserves_every_retained_log"],
        ),
        "discard-parent-prefix": (
            "let log := read buffers parent ++ added\n  (buffers",
            "let log := added\n  (buffers",
            ["child_reads_its_parent_prefix_and_added_rows"],
        ),
        "copy-only-one-new-row": (
            "destination.take start ++ rows ++ destination.drop (start + rows.length)",
            "destination.take start ++ rows.take 1 ++ destination.drop (start + rows.length)",
            ["bounded_batch_matches_visible_append"],
        ),
    }
    verify_lean_model(args, scratch, path, "jvm-trie-pending-logs", controls, 6)


def verify_trie_wire_paths(args, scratch):
    path = Path("verification/lean/TrieWirePaths.lean")
    controls = {
        "drop-nonempty-root": (
            "if rootPath.isEmpty then none else some rootPath, encode root",
            "none, encode root",
            ["restored_wire_recovers_the_root_and_every_bucket"],
        ),
        "derive-wrong-child-path": (
            "(decode (path ++ [.one]) one)",
            "(decode (path ++ [.two]) one)",
            ["decoded_nodes_recover_every_path_and_row", "decoding_reconstructs_a_well_placed_tree"],
        ),
    }
    verify_lean_model(args, scratch, path, "trie-wire-paths", controls, 6)


def verify_outer_join(args, scratch):
    path = Path("verification/lean/OuterJoin.lean")
    controls = {
        "lose-null-extension": (
            "| [] => [none]",
            "| [] => []",
            ["moving_nullable_side_where_can_create_a_row"],
        ),
        "accept-unknown": (
            "| .yes => true",
            "| .yes | .unknown => true",
            ["an_unknown_predicate_does_not_accept"],
        ),
    }
    verify_lean_model(args, scratch, path, "outer-join", controls, 5)


def verify_table_slugs(args, scratch):
    path = Path("verification/lean/TableSlugs.lean")
    controls = {
        "select-claimed-slug": (
            "candidates.find? (fun slug => decide (slug ∉ used))",
            "candidates.find? (fun slug => decide (slug ∈ used))",
            ["a_selected_slug_is_unclaimed", "enough_distinct_candidates_supply_a_slug"],
        ),
        "omit-old-records": (
            "item ∈ (name, slug) :: records := by",
            "item ∈ [(name, slug)] := by",
            ["registration_preserves_recorded_slugs"],
        ),
        "scope-too-few-candidates": (
            "List.range' 1 (used.length + 1)",
            "List.range' 1 used.length",
            ["first_sufficient_suffixes_always_supply_a_slug"],
        ),
    }
    verify_lean_model(args, scratch, path, "table-slugs", controls, 6)


def verify_staged_prefix(args, scratch, history):
    path = Path("verification/lean/StagedPrefix.lean")
    controls = {
        "ignore-block": (
            "if stage.predecessor = snapshot.head ∧ stage.block = snapshot.block then",
            "if stage.predecessor = snapshot.head then",
            ["success_admits_exactly_the_linked_stage_chain"],
        ),
        "append-as-oldest": (
            "stage.rows :: snapshot.history",
            "snapshot.history ++ [stage.rows]",
            ["empty_visible_batch_still_advances_the_head",
             "resolver_reads_the_newest_queued_batches_then_durable"],
        ),
        "publish-on-stage": (
            "| some _ => some ⟨pipeline.applied, pipeline.pending ++ [next]⟩",
            "| some resolved => some ⟨resolved, pipeline.pending ++ [next]⟩",
            ["staging_preserves_the_external_snapshot"],
        ),
        "discard-applied-rows": (
            "⟨pipeline.applied, []⟩",
            "⟨{ pipeline.applied with history := [] }, []⟩",
            ["discarding_the_queue_preserves_the_applied_prefix"],
        ),
    }
    verify_lean_model(args, scratch, path, "staged-prefix", controls, 8,
                      os.environ | {"LEAN_PATH": str(history)})


def verify_read_back_identity(args, scratch):
    path = Path("verification/lean/ReadBackIdentity.lean")
    controls = {
        "accept-missing-sequence": (
            "delivery.sequence = some pending.sequence ∧ delivery.resolved = true ∧",
            "(delivery.sequence = some pending.sequence ∨ delivery.sequence = none) ∧ delivery.resolved = true ∧",
            ["confirmation_proves_equal_frames_given_no_collision", "missing_sequence_refuses"],
        ),
        "accept-control-as-transaction": (
            " ∧ delivery.resolved = true", "",
            ["control_at_pending_sequence_refuses"],
        ),
        "ignore-different-fingerprint": (
            "hash delivery.frame = hash pending.frame)", "hash delivery.frame = hash delivery.frame)",
            ["confirmation_proves_equal_frames_given_no_collision"],
        ),
        "ignore-queue-head": (
            " ∧ head = some pending.staged then", " then",
            ["publication_uses_the_confirmed_queue_head"],
        ),
    }
    verify_lean_model(args, scratch, path, "read-back-identity", controls, 5)


def verify_queued_read_back(args, scratch):
    path = Path("verification/lean/QueuedReadBack.lean")
    controls = {
        "reverse-durable-order": (
            "unread := flow.unread ++ [entry]", "unread := entry :: flow.unread",
            ["step_preserves_order"],
        ),
        "reverse-held-order": (
            "held := flow.held ++ [entry]", "held := entry :: flow.held",
            ["step_preserves_order"],
        ),
        "replace-applied-prefix": (
            "published := flow.published ++ [entry]", "published := [entry]",
            ["successful_publication_consumes_the_first_held_entry"],
        ),
        "discard-held-on-term-end": (
            "replayCandidates := flow.held ++ flow.unread", "replayCandidates := flow.unread",
            ["resignation_preserves_accepted_and_unread_durable_entries"],
        ),
    }
    verify_lean_model(args, scratch, path, "queued-read-back", controls, 5)


def verify_read_back_interleaving(args, scratch):
    path = Path("verification/lean/ReadBackInterleaving.lean")
    controls = {
        "expose-before-stamp": (
            "if flow.stamped then { flow with exposed := true } else flow",
            "{ flow with exposed := true }",
            ["transition_preserves_safe"],
        ),
        "hold-before-confirmation": (
            "if frame = .original then",
            "if frame = .original ∨ resolverBorrowed = true then",
            ["transition_preserves_safe"],
        ),
        "apply-after-term-loss": (
            "if flow.active && flow.exposed then", "if flow.exposed then",
            ["an_ended_term_step_preserves_import"],
        ),
        "wait-for-append-receipt": (
            "if flow.active && flow.exposed then", "if flow.active && flow.exposed && flow.receiptSeen then",
            ["a_visible_frame_can_apply_before_its_receipt"],
        ),
    }
    verify_lean_model(args, scratch, path, "read-back-interleaving", controls, 5)


def verify_replica_prefix(args, scratch):
    path = Path("verification/lean/ReplicaPrefix.lean")
    controls = {
        "ignore-predecessor": (
            "staged.predecessor = state.head ∧ staged.block = state.block",
            "staged.block = state.block",
            ["a_successful_stage_uses_the_decoded_predecessor"],
        ),
        "allow-source-regression": (
            "advances staged.body.key state.head ∧ state.source ≤ staged.body.source",
            "advances staged.body.key state.head",
            ["a_successful_stage_advances_the_transaction_and_source"],
        ),
        "replace-applied-history": (
            "history := state.history ++ [staged.body]", "history := [staged.body]",
            ["successful_local_apply_appends_one_result"],
        ),
        "drop-replica-tail": (
            "(applyReplica state body).bind (fun next => replicaPrefix decode next rest)",
            "applyReplica state body",
            ["a_linked_prefix_replays_to_the_same_published_state"],
        ),
    }
    verify_lean_model(args, scratch, path, "replica-prefix", controls, 4)


def verify_progress_wait(args, scratch):
    path = Path("verification/lean/ProgressWait.lean")
    native_ready = "if engine = .native ∧ ready snapshot request = true then reached snapshot request"
    controls = {
        "native-fault-before-ready": (
            native_ready, "if False then reached snapshot request",
            ["native_wait_can_observe_an_applied_prefix_after_failure"],
        ),
        "kotlin-ready-before-fault": (
            native_ready, "if ready snapshot request = true then reached snapshot request",
            ["kotlin_wait_reports_the_observed_terminal_state"],
        ),
        "return-newer-result": (
            "(snapshot.latestResult.filter (fun receipt => receipt.1 == id)).map Prod.snd",
            "snapshot.latestResult.map Prod.snd",
            ["an_older_wait_never_reuses_a_newer_transaction_result"],
        ),
        "cancel-the-other-wait": (
            "{ subscriptions with first := .cancelled }",
            "{ subscriptions with first := .cancelled, second := .cancelled }",
            ["cancelling_one_wait_preserves_the_other_and_the_snapshot"],
        ),
    }
    verify_lean_model(args, scratch, path, "progress-wait", controls, 5)


def verify_limit_open(args, scratch):
    path = Path("verification/lean/LimitOpen.lean")
    controls = {
        "offset-before-input-failure": (
            "| .error issue => ([.input], .error issue)",
            "| .error issue => match bound .offset offset with\n"
            "    | .error offsetIssue => ([.offset], .error offsetIssue)\n"
            "    | .ok _ => ([.input], .error issue)",
            ["input_failure_precedes_every_bound"],
        ),
        "fetch-before-offset": (
            "match bound .offset offset with",
            "match bound .fetch fetch with",
            ["direct_bounds_can_skip_evaluating_the_counts", "fetch_refusal_follows_valid_offset",
             "offset_refusal_precedes_fetch"],
        ),
        "coerce-direct-negative-to-zero": (
            "if 0 ≤ integer ∧ integer ≤ 9223372036854775807 then some (some integer.toNat) else none",
            "if integer ≤ 9223372036854775807 then some (some integer.toNat) else none",
            ["direct_bound_has_the_same_success"],
        ),
        "overflow-positive-acceptance": (
            "if 0 ≤ integer ∧ integer ≤ 9223372036854775807 then .ok (some integer.toNat)",
            "if 0 ≤ integer ∧ integer ≤ 9223372036854775808 then .ok (some integer.toNat)",
            ["direct_bound_has_the_same_success", "minus_one_and_overflow_are_refused",
             "nonnegative_representable_integer_is_preserved"],
        ),
    }
    verify_lean_model(args, scratch, path, "limit-open", controls, 10)


def verify_private_replay(args, scratch):
    path = Path("verification/lean/PrivateReplay.lean")
    controls = {
        "inclusive-cutoff": (
            "decide (sample < cutoff)",
            "decide (sample ≤ cutoff)",
            ["admits_overlap_every_retained_marker", "exact_cutoff_refuses"],
        ),
        "shorten-marker": (
            "max (sample + 3600000000) cutoff",
            "sample + 3600000000",
            ["installed_marker_covers_cutoff"],
        ),
        "point-lookup": (
            "decide (sample < marker.expiresAt)",
            "decide (marker.startsAt ≤ sample ∧ sample < marker.expiresAt)",
            ["admits_overlap_every_retained_marker", "overlap_realizes_valid_time_range"],
        ),
        "duplicate-side-effect": (
            "if overlaps marker attempt.submittedAt then\n            (state, .committed marker.original)",
            "if overlaps marker attempt.submittedAt then\n            ({ state with effects := state.effects + 1 }, .committed marker.original)",
            ["duplicates_preserve_the_original_for_any_admitted_sample"],
        ),
        "execute-after-lookup-failure": (
            "if attempt.lookupFailed then (state, .aborted)",
            "if attempt.lookupFailed then perform cutoff state attempt",
            ["lookup_failure_preserves_marker_and_effects"],
        ),
        "install-aborted-marker": (
            "else (state, .aborted)",
            "else ({ state with marker := some ⟨attempt.submittedAt, markerEnd cutoff attempt.submittedAt, attempt.outcome⟩ }, .aborted)",
            ["step_preserves_atomic_effect"],
        ),
    }
    verify_lean_model(args, scratch, path, "private-replay", controls, 9)


def verify_explicit_session(args, scratch):
    path = Path("verification/lean/ExplicitSession.lean")
    controls = {
        "use-fresh-basis-instead-of-pin": (
            "| .active (.reading pinned) false, .query _ => (.active (.reading pinned) false, .read pinned)",
            "| .active (.reading pinned) false, .query fresh => (.active (.reading pinned) false, .read fresh)",
            ["a_later_query_observes_the_pinned_basis"] * 2,
        ),
        "allow-dml-in-pending-read-only": (
            "| .active (.pending false) false, .dml op",
            "| .active (.pending _) false, .dml op",
            ["first_dml_resolves_only_an_unspecified_pending_transaction"],
        ),
        "submit-on-rollback": (
            "  | _, .rollback => (.idle, .done)",
            "  | .active (.writing buffered) _, .rollback => (.idle, .submit buffered)\n"
            "  | _, .rollback => (.idle, .done)",
            ["rollback_and_close_discard_the_buffer_without_submission"],
        ),
        "reverse-buffer-order": (
            "(.writing (buffered ++ [op]))",
            "(.writing (op :: buffered))",
            ["admitted_dml_preserves_complete_operation_order"],
        ),
        "clear-closed-state": (
            "| .closed, _ => (.closed, .refused)",
            "| .closed, _ => (.idle, .refused)",
            ["a_closed_session_remains_closed"] * 6,
        ),
    }
    verify_lean_model(args, scratch, path, "explicit-session", controls, 9)


def verify_delegated_scan(args, scratch, history):
    path = Path("verification/lean/DelegatedScan.lean")
    controls = {
        "allow-later-head": (
            "if worker = coordinator ∧ privateEvents = [] then",
            "if coordinator ≤ worker ∧ privateEvents = [] then",
            ["successful_fragment_agrees_with_capture"],
        ),
        "accept-private-overlay": (
            "if worker = coordinator ∧ privateEvents = [] then",
            "if worker = coordinator then",
            ["successful_fragment_agrees_with_capture"],
        ),
        "erase-witness-without-advance": (
            "point (admit [correction, original] 2) 1 0 3",
            "point (admit [correction, original] 1) 1 0 3",
            ["advancing_only_worker_can_change_a_historical_answer"],
        ),
    }
    env = os.environ | {"LEAN_PATH": str(history)}
    verify_lean_model(args, scratch, path, "delegated-scan", controls, 3, env)


def verify_residence_reader_retention(args, scratch):
    path = Path("verification/lean/ResidenceReaderRetention.lean")
    controls = {
        "floor-omit-holds": (
            "(mayDelete : FloorAllows retirement sweep)\n    (generation : Nat)",
            "(mayDelete : retirement ≤ sweep.head ∧ True)\n    (generation : Nat)",
            ["floor_preserves_dependencies"],
        ),
        "exact-omit-release-order": (
            "(mayDelete : ExactAllows used retirement sweep)",
            "(mayDelete : True ∧ ∀ cut, sweep.held cut → ¬ used cut)",
            ["exact_preserves_dependencies"],
        ),
        "exact-omit-holds": (
            "(mayDelete : ExactAllows used retirement sweep)",
            "(mayDelete : retirement ≤ sweep.head ∧ True)",
            ["exact_preserves_dependencies"],
        ),
        "compare-without-certificate": (
            "theorem exact_allows_every_floor_deletion (certified : Certified used retirement)",
            "theorem exact_allows_every_floor_deletion (_certified : True)",
            ["exact_allows_every_floor_deletion"],
        ),
        "witness-overlaps-held-generation": (
            "refine ⟨(· = 1), 2, ⟨2, (· = 0)⟩, ?_, ⟨1, rfl⟩, ?_, ?_⟩",
            "refine ⟨(· = 0), 2, ⟨2, (· = 0)⟩, ?_, ⟨0, rfl⟩, ?_, ?_⟩",
            ["exact_can_collect_with_an_unrelated_earlier_hold"],
        ),
    }
    verify_lean_model(args, scratch, path, "residence-reader-retention", controls, 4)


def verify_checkpoint_metadata_retention(args, scratch):
    path = Path("verification/lean/CheckpointMetadataRetention.lean")
    controls = {
        "allow-stale-admission": (
            "(floor : Nat) : Prop := state.published ≤ floor",
            "(floor : Nat) : Prop := floor = floor",
            ["plans_survive_interleavings"],
        ),
        "ignore-existing-holds": (
            "(block : Nat) : MayAuthorize state block →",
            "(block : Nat) : block < state.published →",
            ["plans_survive_interleavings"],
        ),
        "publish-backwards": (
            "state.published ≤ head → Step state { state with published := head }",
            "True → Step state { state with published := head }",
            ["plans_survive_interleavings"],
        ),
    }
    verify_lean_model(args, scratch, path, "checkpoint-metadata-retention", controls, 3)


def verify_replica_trim_admission(args, scratch):
    path = Path("verification/lean/ReplicaTrimAdmission.lean")
    controls = {
        "trust-physical-floor": (
            "  state.authorized ≤ first\n",
            "  state.floor ≤ first\n",
            ["step_preserves_admission"],
        ),
        "ignore-recovery": (
            "  ∀ reader lowest, state.held reader = some lowest → first ≤ lowest\n",
            "  True\n",
            ["step_preserves_admission"],
        ),
    }
    verify_lean_model(args, scratch, path, "replica-trim-admission", controls, 3)


def verify_replica_hold_close(args, scratch):
    path = Path("verification/lean/ReplicaHoldClose.lean")
    controls = {
        "leave-revision-unchanged": (
            "revision := registry.revision + 1",
            "revision := registry.revision",
            ["close_fences_prior_admissions"],
        ),
        "drop-every-hold": (
            "else registry.holds current",
            "else none",
            ["close_consumes_only_its_hold"],
        ),
    }
    verify_lean_model(args, scratch, path, "replica-hold-close", controls, 3)


def verify_logkeeper(args, scratch):
    models = Path("verification/multi/lean")
    dependency = scratch / "lean-logkeeper-quorum"
    quorum = models / "Quorum.lean"
    clone_file(quorum, dependency)
    built = run_case([args.lean, "-DwarningAsError=true", "-o", "Quorum.olean", str(quorum)],
                     dependency, args.evidence, "lean-logkeeper-quorum")
    if built.returncode:
        raise ValueError("Lean Logkeeper quorum dependency failed")

    promise_controls = {
        "ignore-promised-epoch": (
            "dropped = false ∧ promised = writer ∧ synced = some writer",
            "dropped = false ∧ synced = some writer",
            ["a_stale_writer_cannot_append_after_a_seal"],
        ),
    }
    verify_lean_model(args, scratch, models / "ReplicaPromise.lean", "logkeeper-promise",
                      promise_controls, 3, os.environ | {"LEAN_PATH": str(dependency)})

    winner_controls = {
        "prefer-smaller-rank": (
            "if Dominates candidate current then candidate else current",
            "if Dominates candidate current then current else candidate",
            ["choose_dominates_both"] * 5,
        ),
    }
    verify_lean_model(args, scratch, models / "RecoveryWinner.lean", "logkeeper-winner", winner_controls, 3)

    audit_controls = {
        "restart-chain-on-every-page": (
            "pages.foldl (chain link) seed",
            "pages.foldl (fun _ page => chain link seed page) seed",
            ["online_pages_preserve_the_flat_committed_chain"],
        ),
        "accept-without-quorum": (
            "quorum ≤ covered.length",
            "0 ≤ covered.length",
            ["strict_agreement_requires_quorum_coverage", "strict_agreement_requires_equal_returned_digests",
             "strict_agreement_reports_insufficient_when_agreeing_proofs_fall_short"],
        ),
    }
    verify_lean_model(args, scratch, models / "RetainedAudit.lean", "logkeeper-audit", audit_controls, 5)

    retirement_controls = {
        "erase-tombstone-on-snapshot": (
            "⟨state.promised, state.retired⟩",
            "⟨state.promised, false⟩",
            ["compacting_and_replaying_preserves_the_control_state"],
        ),
        "retire-on-any-reply": (
            "addressed.all settled",
            "addressed.any settled",
            ["retirement_completion_covers_every_addressed_replica", "a_majority_does_not_establish_retirement_of_all_replicas"],
        ),
    }
    verify_lean_model(args, scratch, models / "TerminalJournal.lean", "logkeeper-retirement", retirement_controls, 5)


def verify_generation_retention(args, scratch):
    path = Path("verification/lean/GenerationRetention.lean")
    controls = {
        "publish-pending-object": (
            "dependencies generation object → state.present object ∧ ¬ state.pendingDeletion object) →",
            "dependencies generation object → state.present object) →",
            ["step_preserves_entitlement"],
        ),
        "close-drops-inflight": (
            "state with readers := state.readers.erase generation }",
            "state with readers := state.readers.erase generation, inflight := [] }",
            ["closing_a_reader_preserves_its_inflight_operations"],
        ),
        "authorize-current-generation-only": (
            "(∀ generation, entitled state generation → ¬ dependencies generation object) →",
            "(¬ dependencies state.current object) →",
            ["step_preserves_entitlement", "an_unreferenced_object_can_be_collected"],
        ),
        "finish-all-of-generation": (
            "inflight := state.inflight.erase generation", "inflight := state.inflight.filter (· != generation)",
            ["completing_one_read_preserves_all_other_references"] * 2,
        ),
    }
    verify_lean_model(args, scratch, path, "generation-retention", controls, 5)


def verify_reader_retention(args, scratch):
    path = Path("verification/lean/ReaderRetention.lean")
    controls = {
        "baseline": None,
        "include-expiry": (
            "readerClock + skewBound + ioBound < expiry",
            "readerClock + skewBound + ioBound ≤ expiry",
            ["lease_read_finishes_before_reclamation"],
        ),
        "drop-on-close": (
            "opened := false, pinned := decide (0 < s.active)",
            "opened := false, pinned := false",
            ["pin_step_preserves_entitlement"],
        ),
    }
    allowed = {"propext", "Classical.choice", "Quot.sound"}
    for name, mutation in controls.items():
        case = f"lean-reader-retention-{name}"
        directory = scratch / case
        source = clone_file(path, directory)
        if mutation:
            replace_once(source, mutation[0], mutation[1])
        result = run_case([args.lean, str(path)], directory, args.evidence, case)
        if mutation:
            observed = lean_error_theorems(source, path, result.stdout)
            if not result.returncode or observed != mutation[2]:
                raise ValueError(f"{case}: expected {mutation[2]}, observed {observed}")
        else:
            audited = re.findall(r"depends on axioms: \[([^\]]*)\]", result.stdout)
            if result.returncode or len(audited) != 3 or any(set(axioms.split(", ")) - allowed for axioms in audited):
                raise ValueError("Lean reader-retention proof or axiom audit failed")
        print(f"{case}: {'negative control rejected' if mutation else 'proved'}")


def verify_gateway_replay_lean(args, scratch):
    path = Path("verification/multi/lean/GatewayReplay.lean")
    controls = {
        "reset-ordinal": (
            "{ backend with sqlSettings := [] }",
            "{ backend with sqlSettings := [], ordinal := backend.ordinal + 1 }",
            ["reset_preserves_effective_key", "reset_preserves_effective_key"],
        ),
        "malformed-skips-key": (
            '| _ => backend.commandKey ++ "/" ++ toString backend.ordinal',
            '| .absent => backend.commandKey ++ "/" ++ toString backend.ordinal\n  | .malformed => ""',
            ["malformed_metadata_uses_command_key", "malformed_metadata_uses_command_key"],
        ),
        "renew-deadline": (
            "deadline.getD (cutoff window now)", "cutoff window now",
            ["replacements_preserve_cutoff"],
        ),
    }
    verify_lean_model(args, scratch, path, "gateway-replay", controls, 4)


def verify_lean(args, scratch):
    verify_gateway_replay_lean(args, scratch)
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
            observed = lean_error_theorems(source, path, result.stdout)
            if not result.returncode or observed != mutation[2]:
                raise ValueError(f"{case}: expected {mutation[2]}, observed {observed}")
        elif result.returncode or not unsupported or any(
            set(axioms.split(", ")) - allowed for axioms in unsupported
        ):
            raise ValueError(f"{case}: proof or axiom audit failed")
        print(f"{case}: {'negative control rejected' if mutation else 'proved'}")
        if not mutation:
            env = os.environ | {"LEAN_PATH": str(directory)}
            verify_ceiling(args, scratch, directory)
            verify_system_time(args, scratch)
            verify_message_position(args, scratch)
            verify_type_join(args, scratch)
            verify_floor_preservation(args, scratch)
            verify_retirement_identity(args, scratch)
            verify_retirement_certificate(args, scratch)
            verify_group_publication(args, scratch)
            verify_reader_reclamation_minimality(args, scratch)
            verify_basis_tokens(args, scratch)
            verify_run_encoding(args, scratch)
            verify_memory_admission(args, scratch)
            verify_memory_trie(args, scratch)
            verify_iid_byte_order(args, scratch)
            leaf_models = verify_leaf_runs(args, scratch)
            verify_trie_sibling_order(args, scratch, leaf_models)
            verify_live_row_address(args, scratch)
            verify_snapshot_ownership(args, scratch)
            verify_jvm_trie_pending_logs(args, scratch)
            verify_trie_wire_paths(args, scratch)
            verify_outer_join(args, scratch)
            verify_table_slugs(args, scratch)
            verify_explicit_session(args, scratch)
            verify_private_replay(args, scratch)
            verify_limit_open(args, scratch)
            verify_staged_prefix(args, scratch, directory)
            verify_read_back_identity(args, scratch)
            verify_queued_read_back(args, scratch)
            verify_read_back_interleaving(args, scratch)
            verify_replica_prefix(args, scratch)
            verify_progress_wait(args, scratch)
            verify_logkeeper(args, scratch)
            verify_reader_retention(args, scratch)
            verify_generation_retention(args, scratch)
            verify_replica_trim_admission(args, scratch)
            verify_replica_hold_close(args, scratch)
            verify_checkpoint_metadata_retention(args, scratch)
            verify_residence_reader_retention(args, scratch)
            verify_delegated_scan(args, scratch, directory)
            verify_compaction_projection(args, scratch, directory)
            verify_compaction_planner(args, scratch)
            verify_compaction_merge(args, scratch)
            verify_compaction_intervals(args, scratch, directory)
            verify_search_windows(args, scratch)
            verify_search_member_bounds(args, scratch)
            verify_partitioned_top_k(args, scratch)
            verify_catalog_recovery(args, scratch)
            verify_checkpoint_discovery(args, scratch)
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
    ("Kernel", "unfair-apply", ("SPECIFICATION FairSpec", "SPECIFICATION Spec"), "temporal:EventuallyAcknowledged"),
    ("CacheGeneration", "cache-current-publisher", None, None),
    ("CacheGeneration", "cache-late-publisher", ("LatePublish = FALSE", "LatePublish = TRUE"), "FreshLeaseReachable"),
    ("ReadBack", "readback", None, None),
    ("ReadBack", "apply-before-readback", ('Mutation = "none"', 'Mutation = "skip_readback"'), "AppliedAfterReadBack"),
    ("ReadBack", "ack-after-append", ('Mutation = "none"', 'Mutation = "ack_on_append"'), "AckAfterApply"),
    ("SystemClock", "system-clock", None, None),
    ("SystemClock", "system-clock-raw-catalog", ('Mutation = "none"', 'Mutation = "raw_catalog"'), "StrictSystemTime"),
    ("SystemClock", "system-clock-durable-head", ('Mutation = "none"', 'Mutation = "durable_head"'), "StrictSystemTime"),
    ("SystemClock", "system-clock-empty-restart", ('Mutation = "none"', 'Mutation = "empty_restart"'), "StrictSystemTime"),
    ("SystemClock", "system-clock-recovery-witness",
     ("INVARIANT StrictSystemTime", "INVARIANT NoCatalogAfterRestart"), "NoCatalogAfterRestart"),
    ("LeaseAdmission", "validated-reader-lease", None, None),
    ("LeaseAdmission", "coordinated-reader-pin", ('Mode = "validated_lease"', 'Mode = "pin"'), None),
    ("LeaseAdmission", "age-only-collection", ('Mode = "validated_lease"', 'Mode = "age_only"'), "LiveReaderHasObject"),
    ("LeaseAdmission", "stale-follower-admission", ('Mode = "validated_lease"', 'Mode = "stale_follower"'), "LiveReaderHasObject"),
    ("ReaderIO", "bounded-reader-io", None, None),
    ("ReaderIO", "pinned-reader-io", ('Mode = "lease"', 'Mode = "pin"'), None),
    ("ReaderIO", "reader-io-raw-clock", ('Mode = "lease"', 'Mode = "raw_clock"'), "InflightHasObject"),
    ("ReaderIO", "reader-io-inclusive-deadline", ('Mode = "lease"', 'Mode = "inclusive_deadline"'), "InflightHasObject"),
    ("ReaderIO", "reader-io-overruns", ('Mode = "lease"', 'Mode = "unbounded_io"'), "InflightHasObject"),
    ("ReaderIO", "reader-io-release-on-close", ('Mode = "lease"', 'Mode = "release_during_io"'), "InflightHasObject"),
    ("ReaderIO", "reader-io-failover-loses-pin", ('Mode = "lease"', 'Mode = "forget_pin_on_failover"'), "InflightHasObject"),
    ("CompactionGroup", "complete-compaction-group", None, None),
    ("CompactionGroup", "compaction-group-early-child", ('Mutation = "none"', 'Mutation = "early_child"'), "ExactCoverage"),
    ("CompactionGroup", "compaction-group-early-parent", ('Mutation = "none"', 'Mutation = "early_parent"'), "ExactCoverage"),
    ("CompactionGroup", "compaction-group-pending-write", ('Mutation = "none"', 'Mutation = "pending_write"'), "PublishedOutputReadable"),
    ("CompactionGroup", "compaction-group-reachable", ("INVARIANTS TypeOK ExactCoverage PublishedOutputReadable", "INVARIANT OnlyParentsCaptured"), "OnlyParentsCaptured"),
    ("CompactionOverlap", "compaction-overlap-complete", None, None),
    ("CompactionOverlap", "compaction-overlap-watermarks", ('Mode = "complete"', 'Mode = "watermark"'), "CompleteCoverage"),
    ("CompactionOverlap", "compaction-overlap-reachable", ("INVARIANTS TypeOK CompleteCoverage", "INVARIANT NoReplacementActivated"), "NoReplacementActivated"),
    ("ReclamationAuthority", "reclamation-authority", None, None),
    ("ReclamationAuthority", "reclamation-one-reader", ('Mode = "normal"', 'Mode = "one_reader"'), "ReaderDependenciesAvailable"),
    ("ReclamationAuthority", "reclamation-stale-after-authorization", ('Mode = "normal"', 'Mode = "allow_stale"'), "ReaderDependenciesAvailable"),
    ("ReclamationAuthority", "reclamation-reuse-path", ('Mode = "normal"', 'Mode = "reuse_path"'), "CurrentGenerationAvailable"),
    ("ReclamationAuthority", "reclamation-reachable", ("INVARIANTS TypeOK ReaderDependenciesAvailable CurrentGenerationAvailable", "INVARIANT OldGenerationNeverDeleted"), "OldGenerationNeverDeleted"),
    ("Checkpoint", "checkpoint-root-last", None, None),
    ("Checkpoint", "checkpoint-root-after-data", ('Mutation = "none"', 'Mutation = "root_after_data"'), "CompletePublishedRoot"),
    ("Checkpoint", "checkpoint-lost-dependency", ('Mutation = "none"', 'Mutation = "durability_lie"'), "CompletePublishedRoot"),
    ("Checkpoint", "checkpoint-trim-with-stale-listing", ('Mutation = "none"', 'Mutation = "trim_with_unvalidated_listing"'), "RecoveryHasRepresentation"),
    ("Checkpoint", "checkpoint-check-copy-completion", ('Mutation = "none"', 'Mutation = "check_copy_completion"'), None),
    ("Checkpoint", "checkpoint-trust-copy-acceptance", ('Mutation = "none"', 'Mutation = "trust_copy_acceptance"'), "CompletePublishedRoot"),
    ("RecoveryCollection", "recovery-collection", None, None),
    ("RecoveryCollection", "recovery-collection-incomplete-root", ('Mutation = "none"', 'Mutation = "publish_incomplete"'), "AdvertisedRootsReadable"),
    ("RecoveryCollection", "recovery-collection-ignores-selection", ('Mutation = "none"', 'Mutation = "ignore_selected_root"'), "RecoveryReadSetAvailable"),
    ("RecoveryCollection", "recovery-collection-shared-data", ('Mutation = "none"', 'Mutation = "forget_shared_dependency"'), "AdvertisedRootsReadable"),
    ("RecoveryCollection", "recovery-collection-trim-for-head", ('Mutation = "none"', 'Mutation = "trim_for_head_only"'), "RecoverySuffixAvailable"),
    ("RecoveryCollection", "recovery-collection-select-below-floor", ('Mutation = "none"', 'Mutation = "select_below_floor"'), "RecoverySuffixAvailable"),
    ("RecoveryCollection", "recovery-collection-reachable",
     ("INVARIANTS TypeOK AdvertisedRootsReadable RecoveryReadSetAvailable RecoverySuffixAvailable UsableHead",
      "INVARIANT NoReplacementOpenAfterCollection"),
     "NoReplacementOpenAfterCollection"),
    ("RecoveryDiscovery", "checkpoint-discovery", None, None),
    ("RecoveryDiscovery", "checkpoint-discovery-fail-fast", ('Mutation = "none"', 'Mutation = "fail_fast"'), "MissingFailureHasRefreshed"),
    ("RecoveryDiscovery", "checkpoint-discovery-allow-same", ('Mutation = "none"', 'Mutation = "allow_same"'), "AttemptsAdvance"),
    ("RecoveryDiscovery", "checkpoint-discovery-ignore-newer", ('Mutation = "none"', 'Mutation = "ignore_newer"'), "UsesNewerListing"),
    ("RecoveryDiscovery", "checkpoint-discovery-empty-after-missing", ('Mutation = "none"', 'Mutation = "empty_after_missing"'), "NoEmptyAfterMissing"),
    ("RecoveryDiscovery", "checkpoint-discovery-retry-corrupt", ('Mutation = "none"', 'Mutation = "retry_corrupt"'), "CorruptionIsTerminal"),
    ("RecoveryDiscovery", "checkpoint-discovery-reachable",
     ("INVARIANTS TypeOK AttemptsAdvance NoEmptyAfterMissing UsesNewerListing CorruptionIsTerminal MissingFailureHasRefreshed",
      "INVARIANT NeverRecoveredAfterRefresh"),
     "NeverRecoveredAfterRefresh"),
    ("CatalogCutPublication", "catalog-cut-read-status",
     ('Mutation = "none"', 'Mutation = "remote_status"'), "Readable"),
    ("CatalogCutPublication", "catalog-cut-sample-late",
     ('Mutation = "none"', 'Mutation = "sample_late"'), "Readable"),
    ("CatalogCutPublication", "catalog-cut-transaction-basis",
     ('Mutation = "none"', 'Mutation = "transaction_basis"'), "Readable"),
    ("CatalogCutPublication", "catalog-cut", None, None),
    ("CatalogCutPublication", "catalog-cut-refusal-witness",
     ("INVARIANT Readable", "INVARIANT NoRefusedStale"), "NoRefusedStale"),
    ("CatalogCutPublication", "catalog-cut-post-collection-witness",
     ("INVARIANT Readable", "INVARIANT NoNewReaderAfterDeletion"), "NoNewReaderAfterDeletion"),
    ("FragmentResidence", "fragment-residence-rebind",
     ('Mutation = "none"', 'Mutation = "rebind_to_route"'), "SameHistory"),
    ("FragmentResidence", "fragment-residence-trust-plan",
     ('Mutation = "none"', 'Mutation = "trust_plan"'), "SameHistory"),
    ("FragmentResidence", "fragment-residence-ignore-epoch",
     ('Mutation = "none"', 'Mutation = "ignore_epoch"'), "SameHistory"),
    ("FragmentResidence", "fragment-residence", None, None),
    ("FragmentResidence", "fragment-residence-matching-witness",
     ("INVARIANT SameHistory", "INVARIANT NoMatchingResponse"), "NoMatchingResponse"),
    ("FragmentResidence", "fragment-residence-receiver-refusal-witness",
     ("INVARIANT SameHistory", "INVARIANT NoReceiverFallback"), "NoReceiverFallback"),
    ("DelegatedScan", "delegated-scan-ahead",
     ('Mutation = "none"', 'Mutation = "admit_ahead"'), "ExactAnswer"),
    ("DelegatedScan", "delegated-scan-trust-probe",
     ('Mutation = "none"', 'Mutation = "trust_probe"'), "ExactAnswer"),
    ("DelegatedScan", "delegated-scan-forget-drain",
     ('Mutation = "none"', 'Mutation = "forget_drain"'), "InFlightReadable"),
    ("DelegatedScan", "delegated-scan", None, None),
    ("DelegatedScan", "delegated-scan-matching-reply",
     ("INVARIANTS ExactAnswer InFlightReadable", "INVARIANT NoMatchingReply"), "NoMatchingReply"),
    ("DelegatedScan", "delegated-scan-fallback-witness",
     ("INVARIANTS ExactAnswer InFlightReadable", "INVARIANT NoFallbackAfterErase"), "NoFallbackAfterErase"),
    ("DelegatedScan", "delegated-scan-collection-witness",
     ("INVARIANTS ExactAnswer InFlightReadable", "INVARIANT NoCollectionAfterDrain"), "NoCollectionAfterDrain"),
    ("InstalledCheckpoint", "installed-checkpoint", None, None),
    ("InstalledCheckpoint", "installed-checkpoint-follows-current",
     ('Mutation = "none"', 'Mutation = "follow_current"'), "InstalledReadable"),
    ("InstalledCheckpoint", "installed-checkpoint-ignores-nascent",
     ('Mutation = "none"', 'Mutation = "ignore_nascent"'), "InstalledReadable"),
    ("InstalledCheckpoint", "installed-checkpoint-collection-witness",
     ("INVARIANTS TypeOK InstalledReadable ReaderReadable", "INVARIANT OldFilesNeverCollected"), "OldFilesNeverCollected"),
    ("RetirementPublication", "retirement-premature-certificate",
     ('Mutation = "none"', 'Mutation = "certify_before_install"'), "CertificateFollowsInstall"),
    ("RetirementPublication", "retirement-missing-admission-floor",
     ('Mutation = "none"', 'Mutation = "no_admission_floor"'), "AdmittedReaderReadable"),
    ("RetirementPublication", "retirement-erase-by-key",
     ('Mutation = "none"', 'Mutation = "erase_by_key"'), "CurrentCatalogReadable"),
    ("RetirementPublication", "retirement-publication", None, None),
    ("RetirementPublication", "retirement-collection-and-reuse",
     ("INVARIANTS CertificateFollowsInstall CurrentCatalogReadable AdmittedReaderReadable",
      "INVARIANT NeverCollectsAndReuses"), "NeverCollectsAndReuses"),
    ("PublicationVersion", "publication-version", None, None),
    ("PublicationVersion", "publication-version-head-after-write", (
        'Mutation = "none"', 'Mutation = "head_after_write"'), "BindsCheckedVersion"),
    ("PublicationVersion", "publication-version-head-after-read", (
        'Mutation = "none"', 'Mutation = "head_after_read"'), "BindsCheckedVersion"),
    ("PublicationVersion", "publication-version-ignore-attempt", (
        'Mutation = "none"', 'Mutation = "ignore_attempt"'), "BindsCheckedVersion"),
    ("PublicationVersion", "publication-version-ignore-publication", (
        'Mutation = "none"', 'Mutation = "ignore_publication"'), "RetirementMatchesBinding"),
    ("PublicationVersion", "publication-version-lost-reply", (
        "INVARIANTS BindsCheckedVersion RetirementMatchesBinding",
        "INVARIANT LostReplyWitness"), "LostReplyWitness"),
    ("PublicationVersion", "publication-version-verified-retry", (
        "INVARIANTS BindsCheckedVersion RetirementMatchesBinding",
        "INVARIANT VerifiedRetryWitness"), "VerifiedRetryWitness"),
    ("CatalogAdmissionFloor", "catalog-admission-floor", None, None),
    ("CatalogAdmissionFloor", "catalog-floor-admit-older", (
        'Mutation = "none"', 'Mutation = "ignore_floor"'), "HeldViewAdmissible"),
    ("CatalogAdmissionFloor", "catalog-floor-ignore-opaque", (
        'Mutation = "none"', 'Mutation = "ignore_opaque"'), "HeldViewAdmissible"),
    ("CatalogAdmissionFloor", "catalog-read-without-cas", (
        'Mutation = "none"', 'Mutation = "read_without_cas"'), "HeldViewAdmissible"),
    ("CatalogAdmissionFloor", "catalog-floor-without-cas", (
        'Mutation = "none"', 'Mutation = "floor_without_cas"'), "HeldViewAdmissible"),
    ("CatalogAdmissionFloor", "catalog-floor-failover-witness", (
        "INVARIANTS CoveredByReceipt HeldViewAdmissible",
        "INVARIANT FreshReadAfterFailoverWitness"), "FreshReadAfterFailoverWitness"),
    ("ResidenceRetirement", "residence-retirement", None, None),
    ("ResidenceRetirement", "residence-early-reply",
     ('Mutation = "none"', 'Mutation = "reply_before_install"'), "ReceiptCoversInstalled"),
    ("ResidenceRetirement", "residence-forget-intent",
     ('Mutation = "none"', 'Mutation = "forget_intent"'), "CoveredIntent"),
    ("ResidenceRetirement", "residence-forget-installed-intent",
     ('Mutation = "none"', 'Mutation = "forget_installed_intent"'), "CoveredIntent"),
    ("ResidenceRetirement", "residence-ignore-admission-floor",
     ('Mutation = "none"', 'Mutation = "ignore_floor"'), "RetainedRootsReadable"),
    ("ResidenceRetirement", "residence-forget-provider-io",
     ('Mutation = "none"', 'Mutation = "forget_provider_io"'), "RetainedRootsReadable"),
    ("ResidenceRetirement", "residence-forget-recovery",
     ('Mutation = "none"', 'Mutation = "forget_recovery"'), "RetainedRootsReadable"),
    ("ResidenceRetirement", "residence-forget-checkpoint",
     ('Mutation = "none"', 'Mutation = "forget_checkpoint"'), "RetainedRootsReadable"),
    ("ResidenceRetirement", "residence-forget-fork",
     ('Mutation = "none"', 'Mutation = "forget_fork"'), "RetainedRootsReadable"),
    ("ResidenceRetirement", "residence-erase-by-key",
     ('Mutation = "none"', 'Mutation = "erase_by_key"'), "HeadReadable"),
    ("ResidenceRetirement", "residence-early-shared-retirement",
     ('Mutation = "none"', 'Mutation = "retire_shared_early"'), "HeadReadable"),
    ("ResidenceRetirement", "residence-retirement-witness",
     ("INVARIANTS CoveredIntent ReceiptCoversInstalled HeadReadable RetainedRootsReadable",
      "INVARIANT RetirementWitness"), "RetirementWitness"),
    ("S3KeyCustody", "s3-acknowledge-conflict",
     ('Mutation = "none"', 'Mutation = "ack_conflict"'), "WithdrawalIsTerminal"),
    ("S3KeyCustody", "s3-reinstall-key",
     ('Mutation = "none"', 'Mutation = "reinstall"'), "WithdrawalIsTerminal"),
    ("S3KeyCustody", "s3-ignore-versioning",
     ('Mutation = "none"', 'Mutation = "ignore_versioning"'), "NoProviderKeyAfterWithdrawal"),
    ("S3KeyCustody", "s3-key-custody", None, None),
    ("S3KeyCustody", "s3-withdraw-installed-key",
     ("INVARIANTS WithdrawalIsTerminal NoProviderKeyAfterWithdrawal",
      "INVARIANT NeverWithdrawsAnInstalledKey"), "NeverWithdrawsAnInstalledKey"),
    ("ReplicaHoldLifecycle", "replica-hold-lifecycle", None, None),
    ("ReplicaHoldLifecycle", "replica-hold-release-on-cancel",
     ('Mutation = "none"', 'Mutation = "close_on_cancel"'), "action:ReleaseAfterDrainOrStop"),
    ("ReplicaHoldLifecycle", "replica-hold-timeout-as-stop",
     ('Mutation = "none"', 'Mutation = "use_timeout_as_stop"'), "action:ReleaseAfterDrainOrStop"),
    ("ReplicaHoldLifecycle", "replica-hold-skip-empty-close-cas",
     ('Mutation = "none"', 'Mutation = "skip_empty_close_cas"'), "ClosedNeverRegainsHold"),
    ("ReplicaHoldLifecycle", "replica-hold-trim-after-stop-witness",
     ("INVARIANTS ActiveHoldCovered ClosedNeverRegainsHold", "INVARIANT NeverTrimsAfterConfirmedDeath"),
     "NeverTrimsAfterConfirmedDeath"),
    ("ForkPublication", "fork-stale-inventory",
     ('Mutation = "none"', 'Mutation = "ignore_new_fork"'), "PublishedChildReadable"),
    ("ForkPublication", "fork-admit-during-delete",
     ('Mutation = "none"', 'Mutation = "admit_during_delete"'), "PublishedChildReadable"),
    ("ForkPublication", "fork-delete-before-copy",
     ('Mutation = "none"', 'Mutation = "delete_before_copy"'), "PublishedChildReadable"),
    ("ForkPublication", "fork-publish-partial-copy",
     ('Policy = "reference"\n  Mutation = "none"', 'Policy = "copy"\n  Mutation = "publish_partial"'), "PublishedChildReadable"),
    ("ForkPublication", "fork-reference", None, None),
    ("ForkPublication", "fork-copy",
     ('Policy = "reference"', 'Policy = "copy"'), None),
    ("ForkPublication", "fork-reference-survives-witness",
     ("INVARIANTS TypeOK PublishedChildReadable", "INVARIANT NeverServesAfterSourceCollection"),
     "NeverServesAfterSourceCollection"),
    ("ForkPublication", "fork-copy-survives-witness",
     ('Policy = "reference"\n  Mutation = "none"\nINVARIANTS TypeOK PublishedChildReadable',
      'Policy = "copy"\n  Mutation = "none"\nINVARIANT NeverServesAfterSourceCollection'),
     "NeverServesAfterSourceCollection"),
    ("ForkPublication", "fork-reference-refuses-witness",
     ("INVARIANTS TypeOK PublishedChildReadable", "INVARIANT NeverRefusesAfterSourceCollection"),
     "NeverRefusesAfterSourceCollection"),
    ("ForkPublication", "fork-copy-refuses-witness",
     ('Policy = "reference"\n  Mutation = "none"\nINVARIANTS TypeOK PublishedChildReadable',
      'Policy = "copy"\n  Mutation = "none"\nINVARIANT NeverRefusesAfterSourceCollection'),
     "NeverRefusesAfterSourceCollection"),
    ("RenewedResidenceHold", "renewed-residence-hold", None, None),
    ("RenewedResidenceHold", "renewed-residence-hold-stale-observation",
     ('Mutation = "none"', 'Mutation = "reuse_observation"'), "EntitlementPreservesBytes"),
    ("RenewedResidenceHold", "renewed-residence-hold-early-expiry",
     ('Mutation = "none"', 'Mutation = "expire_on_first_observation"'), "EntitlementPreservesBytes"),
    ("RenewedResidenceHold", "renewed-residence-hold-reply-clock",
     ('Mutation = "none"', 'Mutation = "deadline_at_reply"'), "EntitlementPreservesBytes"),
    ("RenewedResidenceHold", "renewed-residence-hold-no-io-fence",
     ('Mutation = "none"', 'Mutation = "omit_io_fence"'), "EntitlementPreservesBytes"),
    ("RenewedResidenceHold", "renewed-residence-hold-unbounded-io",
     ('Mutation = "none"', 'Mutation = "ignore_io_bound"'), "EntitlementPreservesBytes"),
    ("RenewedResidenceHold", "renewed-residence-hold-unfair-expiry",
     ('Mutation = "none"', 'Mutation = "unfair_expiry"'), "temporal:AbandonEventuallyCollects"),
    ("RenewedResidenceHold", "renewed-residence-hold-collection-witness",
     ("INVARIANT EntitlementPreservesBytes", "INVARIANT NeverCollectsAfterRenewal"), "NeverCollectsAfterRenewal"),
    ("ResidenceReaderRetention", "residence-reader-floor-stale-admission",
     ('Mutation = "none"', 'Mutation = "admit_stale"'), "ReaderReadable"),
    ("ResidenceReaderRetention", "residence-reader-floor-ignore-holds",
     ('Mutation = "none"', 'Mutation = "ignore_holds"'), "ReaderReadable"),
    ("ResidenceReaderRetention", "residence-reader-floor-late-release",
     ('Mutation = "none"', 'Mutation = "trust_late_release"'), "ReaderReadable"),
    ("ResidenceReaderRetention", "residence-reader-floor-ignore-drain",
     ('Mutation = "none"', 'Mutation = "ignore_drain"'), "ReaderReadable"),
    ("ResidenceReaderRetention", "residence-reader-exact-ignore-drain",
     ('Policy = "floor"\n    Mutation = "none"', 'Policy = "exact"\n    Mutation = "ignore_drain"'), "ReaderReadable"),
    ("ResidenceReaderRetention", "residence-reader-exact-late-release",
     ('Policy = "floor"\n    Mutation = "none"', 'Policy = "exact"\n    Mutation = "trust_late_release"'), "ReaderReadable"),
    ("ResidenceReaderRetention", "residence-reader-floor", None, None),
    ("ResidenceReaderRetention", "residence-reader-floor-conservative",
     ("INVARIANTS TypeOK ServingReadable ReaderReadable", "INVARIANTS TypeOK ServingReadable ReaderReadable LaterObjectRetainedWithEarlierReader"), None),
    ("ResidenceReaderRetention", "residence-reader-exact-selective-progress",
     ('Policy = "floor"\n    Mutation = "none"\nINVARIANTS TypeOK ServingReadable ReaderReadable',
      'Policy = "exact"\n    Mutation = "none"\nINVARIANT LaterObjectRetainedWithEarlierReader'),
     "LaterObjectRetainedWithEarlierReader"),
    ("ResidenceReaderRetention", "residence-reader-exact",
     ('Policy = "floor"', 'Policy = "exact"'), None),
    ("ResidenceReaderRetention", "residence-reader-exact-stale-admission",
     ('Policy = "floor"\n    Mutation = "none"', 'Policy = "exact"\n    Mutation = "admit_stale"'), "ReaderReadable"),
    ("ResidenceReaderRetention", "residence-reader-exact-ignore-holds",
     ('Policy = "floor"\n    Mutation = "none"', 'Policy = "exact"\n    Mutation = "ignore_holds"'), "ReaderReadable"),
    ("ResidenceReaderRetention", "residence-reader-floor-collection-witness",
     ("INVARIANTS TypeOK ServingReadable ReaderReadable", "INVARIANT ReleasedWhileLatestReaderNeverObserved"),
     "ReleasedWhileLatestReaderNeverObserved"),
    ("ResidenceReaderRetention", "residence-reader-exact-collection-witness",
     ('Policy = "floor"\n    Mutation = "none"\nINVARIANTS TypeOK ServingReadable ReaderReadable',
      'Policy = "exact"\n    Mutation = "none"\nINVARIANT ReleasedWhileLatestReaderNeverObserved'),
     "ReleasedWhileLatestReaderNeverObserved"),
    ("CheckpointMetadataRetention", "checkpoint-metadata-retention", None, None),
    ("CheckpointMetadataRetention", "checkpoint-metadata-stale-admission",
     ('Mutation = "none"', 'Mutation = "allow_stale"'), "AdmittedMetadataPresent"),
    ("CheckpointMetadataRetention", "checkpoint-metadata-ignore-existing-holds",
     ('Mutation = "none"', 'Mutation = "ignore_existing_holds"'), "AdmittedMetadataPresent"),
    ("CheckpointMetadataRetention", "checkpoint-metadata-publish-backwards",
     ('Mutation = "none"', 'Mutation = "publish_backwards"'), "PublishedMetadataPresent"),
    ("CheckpointMetadataRetention", "checkpoint-metadata-collection-witness",
     ("INVARIANTS PublishedMetadataPresent AdmittedMetadataPresent", "INVARIANT NeverCollectsSupersededMetadata"),
     "NeverCollectsSupersededMetadata"),
    ("ReplicaTrimAdmission", "replica-trim-admission", None, None),
    ("ReplicaTrimAdmission", "replica-trim-trust-physical-floor",
     ('Mutation = "none"', 'Mutation = "trust_physical_floor"'), "ActiveRecoveryCovered"),
    ("ReplicaTrimAdmission", "replica-trim-ignore-recovery",
     ('Mutation = "none"', 'Mutation = "ignore_recovery"'), "ActiveRecoveryCovered"),
    ("ReplicaTrimAdmission", "replica-trim-replacement-witness",
     ("INVARIANTS PublishedCheckpointCovered ActiveRecoveryCovered",
      "INVARIANT NeverRecoveredAfterTrim"), "NeverRecoveredAfterTrim"),
    ("PreparedReplay", "prepared-replay", None, None),
    ("PreparedReplay", "prepared-replay-skip-admission",
     ('Mutation = "none"', 'Mutation = "install_without_admission"'), "InstallHasAdmission"),
    ("PreparedReplay", "prepared-replay-reopen",
     ('Mutation = "none"', 'Mutation = "reopen"'), "DriverUsesPrepared"),
    ("PreparedReplay", "prepared-replay-early-refusal",
     ('Mutation = "none"', 'Mutation = "refuse_without_refresh"'), "RefreshBeforeRefusing"),
    ("PreparedReplay", "prepared-replay-replacement-witness",
     ("INVARIANTS InstallHasAdmission DriverUsesPrepared OnlyNewerCheckpointsRetried RefreshBeforeRefusing",
      "INVARIANT NeverReplaysReplacement"), "NeverReplaysReplacement"),
    ("PreparedReplay", "prepared-replay-later-trim-witness",
     ("INVARIANTS InstallHasAdmission DriverUsesPrepared OnlyNewerCheckpointsRetried RefreshBeforeRefusing",
      "INVARIANT NeverTrimmedAfterAdmission"), "NeverTrimmedAfterAdmission"),
    ("RecoveryReplay", "checkpoint-replay-prefix", None, None),
    ("RecoveryReplay", "checkpoint-replay-skip-probe", ('Mutation = "none"', 'Mutation = "skip_probe"'), "ObservedAdmission"),
    ("RecoveryReplay", "checkpoint-replay-fail-fast", ('Mutation = "none"', 'Mutation = "fail_fast"'), "RefreshBeforeRefusing"),
    ("RecoveryReplay", "checkpoint-replay-retry-same", ('Mutation = "none"', 'Mutation = "retry_same"'), "OnlyNewerCheckpointsRetried"),
    ("RecoveryReplay", "checkpoint-replay-trust-probe", ('Mutation = "none"', 'Mutation = "trust_probe"'), "TailStartCovered"),
    ("RecoveryReplay", "checkpoint-replay-replacement-witness",
     ("INVARIANTS ObservedAdmission OnlyNewerCheckpointsRetried RefreshBeforeRefusing TailStartCovered",
      "INVARIANT NeverOpenedReplacement"), "NeverOpenedReplacement"),
    ("RecoveryReplay", "checkpoint-replay-post-probe-loss-witness",
     ("INVARIANTS ObservedAdmission OnlyNewerCheckpointsRetried RefreshBeforeRefusing TailStartCovered",
      "INVARIANT NeverLostAfterProbe"), "NeverLostAfterProbe"),
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
            if invariant.startswith("temporal:"):
                message = f"Temporal property {invariant.removeprefix('temporal:')} was violated"
            elif invariant.startswith("action:"):
                message = f"Action property {invariant.removeprefix('action:')} is violated"
            else:
                message = f"Invariant {invariant} is violated"
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


def check_apalache(args, scratch, module, name, invariant, length, mutation=None, counterexample=None):
    case = f"apalache-{name}"
    directory = scratch / case
    source = clone_file(Path(f"verification/tla/{module}.tla"), directory)
    config = clone_file(Path(f"verification/tla/{module}.cfg"), directory)
    if mutation:
        replace_once(config, *mutation)
    env = os.environ | {"JAVA_HOME": str(Path(args.java).resolve().parent.parent)}
    log = run_case(
        [args.apalache, f"--out-dir={args.evidence / case}", "check", f"--config={config.name}",
         f"--length={length}", f"--inv={invariant}", source.name],
        source.parent, args.evidence, case, env,
    )
    if counterexample:
        if log.returncode != 12 or "state invariant 0 violated" not in log.stdout:
            raise ValueError(f"{case}: expected the {counterexample} counterexample")
        print(f"{case}: {counterexample} counterexample found")
    else:
        if log.returncode or f"no error up to computation length {length}" not in log.stdout:
            raise ValueError(f"{case}: symbolic check failed")
        print(f"{case}: holds up to {length} transitions")


def verify_apalache(args, scratch):
    for mode in ("validated", "serialized", "blind"):
        mutation = None if mode == "validated" else ('Mode = "validated"', f'Mode = "{mode}"')
        counterexample = "lost-update" if mode == "blind" else None
        check_apalache(args, scratch, "Resolution", mode, "NoLostUpdates", 8, mutation, counterexample)
    check_apalache(args, scratch, "SystemClock", "system-clock", "StrictSystemTime", 8)
    check_apalache(
        args, scratch, "SystemClock", "system-clock-empty-restart", "StrictSystemTime", 8,
        ('Mutation = "none"', 'Mutation = "empty_restart"'), "regressing restart clock",
    )
    check_apalache(
        args, scratch, "ForkPublication", "fork-stale-inventory", "PublishedChildReadable", 8,
        ('Mutation = "none"', 'Mutation = "ignore_new_fork"'), "stale reference inventory",
    )
    check_apalache(args, scratch, "ForkPublication", "fork-reference", "PublishedChildReadable", 8)
    check_apalache(
        args, scratch, "ForkPublication", "fork-copy", "PublishedChildReadable", 8,
        ('Policy = "reference"', 'Policy = "copy"'),
    )
    check_apalache(args, scratch, "CatalogCutPublication", "catalog-cut", "Readable", 8)
    check_apalache(
        args, scratch, "CatalogCutPublication", "catalog-cut-read-status", "Readable", 8,
        ('Mutation = "none"', 'Mutation = "remote_status"'), "replica receipt ahead of catalog",
    )
    check_apalache(args, scratch, "RecoveryCollection", "recovery-collection", "RecoverySafe", 10)
    check_apalache(args, scratch, "PublicationVersion", "publication-version", "Safety", 6)
    check_apalache(
        args, scratch, "PublicationVersion", "publication-version-head-after-read", "BindsCheckedVersion", 6,
        ('Mutation = "none"', 'Mutation = "head_after_read"'), "a later version substituted for the verified read",
    )
    check_apalache(args, scratch, "CatalogAdmissionFloor", "catalog-admission-floor", "Safety", 8)
    check_apalache(
        args, scratch, "CatalogAdmissionFloor", "catalog-floor-without-cas", "HeldViewAdmissible", 8,
        ('Mutation = "none"', 'Mutation = "floor_without_cas"'), "a stale floor write crosses a reader admission",
    )
    check_apalache(args, scratch, "ResidenceRetirement", "residence-retirement", "Safety", 10)
    check_apalache(
        args, scratch, "ResidenceRetirement", "residence-retirement-forget-checkpoint", "RetainedRootsReadable", 10,
        ('Mutation = "none"', 'Mutation = "forget_checkpoint"'), "collection across a retained checkpoint",
    )
    check_apalache(
        args, scratch, "RecoveryCollection", "recovery-collection-trim-for-head", "RecoverySuffixAvailable", 10,
        ('Mutation = "none"', 'Mutation = "trim_for_head_only"'), "missing recovery suffix",
    )

    check_apalache(args, scratch, "RecoveryDiscovery", "checkpoint-discovery", "DiscoverySafe", 10)
    check_apalache(
        args, scratch, "RecoveryDiscovery", "checkpoint-discovery-fail-fast", "MissingFailureHasRefreshed", 10,
        ('Mutation = "none"', 'Mutation = "fail_fast"'), "missing checkpoint without refreshed discovery",
    )
    check_apalache(args, scratch, "InstalledCheckpoint", "installed-checkpoint", "RetentionSafe", 10)
    check_apalache(
        args, scratch, "InstalledCheckpoint", "installed-checkpoint-ignores-nascent", "InstalledReadable", 10,
        ('Mutation = "none"', 'Mutation = "ignore_nascent"'), "lost nascent dependency",
    )
    check_apalache(args, scratch, "RecoveryReplay", "checkpoint-replay-prefix", "ReplaySafe", 10)
    check_apalache(
        args, scratch, "RecoveryReplay", "checkpoint-replay-trust-probe", "TailStartCovered", 10,
        ('Mutation = "none"', 'Mutation = "trust_probe"'), "log no longer retained when the tail opens",
    )


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


def verify_mcrl2(args, scratch):
    model = Path("verification/mcrl2/CheckpointDiscovery.mcrl2")
    impl = "init hide({select_candidate, read_root, read_table}, Discover(no_block));"
    usable = "advertised(offer) != no_block && position(advertised(offer)) > position(previous)"
    stays = "previous != no_block && position(advertised(offer)) <= position(previous)"
    variants = {
        "contract": [(impl, "init Contract(no_block);")],
        "baseline": [],
        "fail-fast": [
            ("Refresh(previous: Candidate) = Discover(previous);",
             "Refresh(previous: Candidate) = unavailable . delta;"),
        ],
        "retry-same": [
            (usable, usable.replace(" > ", " >= ")),
            (stays, "previous != no_block && "
             "(advertised(offer) == no_block || position(advertised(offer)) < position(previous))"),
        ],
        "reset-on-empty": [
            ("previous == no_block && advertised(offer) == no_block", "advertised(offer) == no_block"),
            (stays, "previous != no_block && advertised(offer) != no_block && "
             "position(advertised(offer)) <= position(previous)"),
        ],
    }
    systems = {}
    for name, mutations in variants.items():
        case = f"mcrl2-checkpoint-{name}"
        directory = scratch / case
        source = clone_file(model, directory)
        for find, replacement in mutations:
            replace_once(source, find, replacement)
        linear = run_case([args.mcrl22lps, str(source), "checkpoint.lps"],
                          directory, args.evidence, f"{case}-linear")
        if linear.returncode:
            raise ValueError(f"{case}: linearization failed")
        automaton = args.evidence / f"{case}.aut"
        generated = run_case([args.lps2lts, "checkpoint.lps", str(automaton)],
                              directory, args.evidence, f"{case}-states")
        if generated.returncode or not automaton.is_file():
            raise ValueError(f"{case}: state-space generation failed")
        systems[name] = automaton
        if name == "contract":
            continue
        comparison = [args.ltscompare]
        counterexample = args.evidence / f"{case}.mcf"
        if name == "baseline":
            comparison.append("--equivalence=dpbranching-bisim")
        else:
            comparison.extend(["--equivalence=branching-bisim", "--counter-example",
                               f"--counter-example-file={counterexample}"])
        checked = run_case(
            [*comparison, str(systems["contract"]), str(automaton)],
            directory, args.evidence, f"{case}-compare",
        )
        expected = "true" if name == "baseline" else "false"
        if checked.returncode or checked.stdout.strip() != expected:
            raise ValueError(f"{case}: expected {expected}, observed {checked.stdout.strip()}")
        if name != "baseline" and (not counterexample.is_file() or "observed(" not in counterexample.read_text()):
            raise ValueError(f"{case}: expected a distinguishing formula over observations")
        print(f"{case}: {'equivalent to the checkpoint contract' if name == 'baseline' else 'negative control distinguished'}")


def verify_hyperltl(args, scratch):
    model = Path("verification/hyperltl/CheckpointRecovery.smv")
    property_path = Path("verification/hyperltl/CheckpointRecovery.hltl")
    reachable = Path("verification/hyperltl/CheckpointRecoveryReachable.hltl")
    cases = (
        ("baseline", property_path, None, "SAT", False),
        ("reachable", reachable, None, "SAT", True),
        ("fail-fast", property_path, (
            "phase = refresh                                               : read_root;",
            "phase = refresh                                               : failed;",
        ), "UNSAT", True),
        ("stale-refresh", property_path, (
            "phase = refresh               : stable_head;",
            "phase = refresh               : candidate;",
        ), "UNSAT", True),
    )
    for name, law, mutation, expected, needs_witness in cases:
        case = f"hyperltl-checkpoint-{name}"
        directory = scratch / case
        source = clone_file(model, directory)
        formula = clone_file(law, directory)
        if mutation:
            replace_once(source, *mutation)
        checked = run_case([args.autohyper, "--nusmv", "--witness", str(source), str(formula)],
                           directory, args.evidence, case)
        verdicts = [line for line in checked.stdout.splitlines() if line in {"SAT", "UNSAT"}]
        if checked.returncode or verdicts != [expected]:
            raise ValueError(f"{case}: expected {expected}, observed {verdicts}")
        if needs_witness and not all(marker in checked.stdout for marker in ("Witnesses", "\nA: ", "\nB: ")):
            raise ValueError(f"{case}: no two-run witness")
        print(f"{case}: {expected}")


def capture_production(source):
    source.mkdir()
    subprocess.run(["git", "diff", "--exit-code", "HEAD", "--", "rust"], cwd=ROOT, check=True)
    paths = ["rust", "core", "api", "modules", "src", "verification"]
    with subprocess.Popen(["git", "archive", "--format=tar", "HEAD", *paths], cwd=ROOT, stdout=subprocess.PIPE) as git:
        with tarfile.open(fileobj=git.stdout, mode="r|") as archive:
            archive.extractall(source, filter="data")
        if git.wait():
            raise ValueError("Could not capture the production source at HEAD")


def verify_replica_boundary(args, source):
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
    driver.write_bytes(original)


def verify_checkpoint_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    command = ["cargo", "test", "--locked", "-p", "rtdb-conformance", "--test", "node_faults"]
    baseline = run_case(
        [*command, "--test", "recovery"], source / "rust", args.evidence, "rust-checkpoint-baseline", env,
    )
    overlapping_collection = (
        "recovery_advances_after_collection_of_the_listed_checkpoint",
        "recovery_advances_after_collection_of_a_selected_table_block",
    )
    follower_leases = (
        "a_follower_cursor_keeps_its_files_while_its_lease_lasts",
        "a_follower_cursor_past_its_lease_is_refused_before_reading_collected_files",
    )
    if (
        baseline.returncode
        or not all(f"{count} passed; 0 failed" in baseline.stdout for count in (1, 8))
        or not all(f"{test} ... ok" in baseline.stdout for test in
                   ("recovery_preserves_queries_through_tiered_publication", *overlapping_collection, *follower_leases))
    ):
        raise ValueError("checkpoint replay baseline failed")
    blocks = source / "rust/crates/replication/src/blocks.rs"
    original = blocks.read_bytes()
    table_writes = """        for entry in table_catalog.entries() {
            check_cancelled(cancellation)?;
            let block = table_blocks.get(&entry.table).ok_or_else(|| invalid("Recorded table has no block metadata"))?;
            self.store.put(&ObjectKey::new(entry.slug.block_key(index)?)?, Bytes::from(block.encode())).await?;
        }
"""
    root_write = "        self.store.put(&key, Bytes::from(catalog.encode())).await?;\n"
    replace_once(blocks, table_writes, "")
    replace_once(blocks, root_write, root_write + table_writes)
    test = "recovery_reads_the_complete_checkpoint_if_interrupted_after_putting_the_root"
    checked = run_case(
        [*command, test, "--", "--exact"], source / "rust", args.evidence, "rust-checkpoint-root-first", env,
    )
    if (
        checked.returncode != 101
        or f"{test} ... FAILED" not in checked.stdout
        or "checkpoint could not open after After interruption: rtdb.storage/missing-object" not in checked.stdout
        or "0 passed; 1 failed" not in checked.stdout
    ):
        raise ValueError("root-first mutation did not fail at checkpoint startup")
    blocks.write_bytes(original)

    missing = 'Err(error) if refresh && error.code == "rtdb.storage/missing-object" => {'
    try:
        replace_once(blocks, missing, missing.replace("if ", "if false && ", 1))
        collected = run_case(
            [*command, "recovery_advances_after_collection"], source / "rust",
            args.evidence, "rust-checkpoint-no-discovery-refresh", env,
        )
        if (
            collected.returncode != 101
            or not all(f"{test} ... FAILED" in collected.stdout for test in overlapping_collection)
            or collected.stdout.count("recovery could not use the newer checkpoint: rtdb.storage/missing-object") != 2
            or "0 passed; 2 failed" not in collected.stdout
        ):
            raise ValueError("disabling refresh did not fail both collection replay cases")
    finally:
        blocks.write_bytes(original)
    print("rust-checkpoint-replay: nine tests passed, root-first and fail-fast mutations rejected")


def verify_checkpoint_trie_retention(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    test = "collection_preserves_live_and_pending_tries_of_the_installed_checkpoint_until_replaced"
    command = ["cargo", "test", "--locked", "-p", "rtdb-maintenance", "--test", "gc", test, "--", "--exact"]
    baseline = run_case(command, source / "rust", args.evidence, "rust-checkpoint-trie-retention-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("native retention of the installed checkpoint's tries failed")

    partition = source / "rust/crates/index/src/partition.rs"
    original = partition.read_bytes()
    try:
        replace_once(partition, "any_reference_requires(checkpoint_requires, self.readers.len(),",
                     "any_reference_requires(false, self.readers.len(),")
        checked = run_case(command, source / "rust", args.evidence, "rust-checkpoint-trie-retention-ignore-checkpoint", env)
        if (
            checked.returncode != 101
            or f"{test} ... FAILED" not in checked.stdout
            or "an installed checkpoint still needs both tries" not in checked.stdout
            or "0 passed; 1 failed" not in checked.stdout
        ):
            raise ValueError("omitting the installed checkpoint did not fail the collection test")
    finally:
        partition.write_bytes(original)
    print("rust-checkpoint-trie-retention: native dependency test passed, ignoring the checkpoint failed")


def verify_trie_removal_readback(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target")),
        "CARGO_PROFILE_TEST_DEBUG": "0",
    }
    test = "driver::tests::trie_removal_receipt_waits_for_the_installed_catalog_cut"
    command = ["cargo", "test", "--locked", "-p", "rtdb-replication", "--lib", test, "--", "--exact"]
    baseline = run_case(command, source / "rust", args.evidence, "rust-trie-removal-readback-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("native acknowledgement did not wait for the installed prefix")

    driver = source / "rust/crates/replication/src/driver.rs"
    original = driver.read_bytes()
    try:
        replace_once(driver,
                     "leader.trie_removals.push(AppliedTrieRemoval { sequence, position: record.metadata.position, deletions, _record: record });",
                     "complete_record(&mut leader.pending, Some(sequence), Ok(Applied::TrieRemoval(record.metadata.position)));")
        checked = run_case(command, source / "rust", args.evidence, "rust-trie-removal-readback-early", env)
        if (checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
            or "trie removal replied before the installed prefix" not in checked.stdout
            or "0 passed; 1 failed" not in checked.stdout):
            raise ValueError("an early receipt did not fail at the installed-prefix assertion")
    finally:
        driver.write_bytes(original)
    print("rust-trie-removal-readback: installed receipt passed, early receipt failed")


def verify_durable_trie_retirement(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target")),
        "CARGO_PROFILE_TEST_DEBUG": "0",
    }
    test = "durable_retirement_intents_survive_writer_stop_on_both_sides_of_installation"
    command = ["cargo", "test", "--locked", "-p", "rtdb-maintenance", "--test", "gc", test, "--", "--exact"]
    baseline = run_case(command, source / "rust", args.evidence, "rust-trie-retirement-intent-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("native restart failed to recover the staged retirement")

    journal = source / "rust/crates/maintenance/src/retirement_intent.rs"
    original = journal.read_bytes()
    controls = {
        "skip-stage": ("store.put(&pending.intent, pending.encoded.clone()).await?;", "", "retirement intent was not durable before publication"),
        "skip-replay": ("if keys.iter().all(|key| !view.retains_trie(&intent.table, key)) {",
                        "if false && keys.iter().all(|key| !view.retains_trie(&intent.table, key)) {",
                        "the successor must confirm the staged removal"),
        "omit-files": ("tries: keys.iter().map(TrieKey::to_string).collect(), files };",
                       "tries: keys.iter().map(TrieKey::to_string).collect(), files: Vec::new() };",
                       "the staged intent must include the file plan"),
        "drop-before-floor": ("if store.advance_catalog_floor(cut).await? {",
                              "if { store.advance_catalog_floor(cut).await?; true } {",
                              "a hold blocking the floor keeps the intent durable"),
    }
    for name, (before, after, failure) in controls.items():
        try:
            replace_once(journal, before, after)
            checked = run_case(command, source / "rust", args.evidence, f"rust-trie-retirement-intent-{name}", env)
            if (checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
                or failure not in checked.stdout or "0 passed; 1 failed" not in checked.stdout):
                raise ValueError(f"trie retirement control {name} did not fail at its declared assertion")
        finally:
            journal.write_bytes(original)
    print("rust-trie-retirement-intent: restart passed, omitting stage, replay, the file plan or the floor gate failed")


def verify_catalog_admission_floor(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target")),
        "CARGO_PROFILE_TEST_DEBUG": "0",
    }
    test = "catalog_readers::tests::floor_installation_and_reader_admission_share_one_cas"
    command = ["cargo", "test", "--locked", "-p", "rtdb-multi", "--features", "dynamodb", "--lib", test, "--", "--exact"]
    baseline = run_case(command, source / "rust", args.evidence, "rust-catalog-floor-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("the residence did not coordinate reader admission with the floor")

    readers = source / "rust/crates/multi/src/catalog_readers.rs"
    original = readers.read_bytes()
    controls = {
        "skip-reader-check": ("if all_readers_allow {", "if true {", "an older held catalog must block the floor"),
        "ignore-floor": (
            "if !admits_after_floor(cut.map(|cut| cut.through.0), self.floor(record).map(|floor| floor.0)) {",
            "if false && !admits_after_floor(cut.map(|cut| cut.through.0), self.floor(record).map(|floor| floor.0)) {",
            "a capture older than the installed floor must refuse",
        ),
    }
    for name, (before, after, failure) in controls.items():
        try:
            replace_once(readers, before, after)
            checked = run_case(command, source / "rust", args.evidence, f"rust-catalog-floor-{name}", env)
            if (checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
                or failure not in checked.stdout or "0 passed; 1 failed" not in checked.stdout):
                raise ValueError(f"catalog floor control {name} did not fail at its declared assertion")
        finally:
            readers.write_bytes(original)
    print("rust-catalog-floor: directory replay passed, both missing-admission controls failed")


def verify_trie_naming_epoch(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target")),
        "CARGO_PROFILE_TEST_DEBUG": "0",
    }
    test = "a_delayed_notice_removes_or_adopts_only_its_named_incarnation"
    command = ["cargo", "test", "--locked", "-p", "rtdb-index", "--test", "catalogs", test, "--", "--exact"]
    baseline = run_case(command, source / "rust", args.evidence, "rust-naming-epoch-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("the trie catalog did not preserve its naming epoch under delayed notices")

    controls = {
        "old-publication": ("partition.rs",
                            "tries.iter().filter(|trie| trie.key.level() == 0 || trie.key.epoch() == epoch)",
                            "tries.iter()",
                            "a delayed publication from before reset must not be adopted"),
        "old-removal": ("trie_catalog.rs", "trie.key == *key && matches!(trie.state, TrieState::Garbage { .. })",
                        "matches!(trie.state, TrieState::Garbage { .. })",
                        "a delayed retirement must preserve the replacement"),
    }
    for name, (file, before, after, failure) in controls.items():
        path = source / "rust/crates/index/src" / file
        original = path.read_bytes()
        try:
            replace_once(path, before, after)
            checked = run_case(command, source / "rust", args.evidence, f"rust-naming-epoch-{name}", env)
            if (checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
                or failure not in checked.stdout or "0 passed; 1 failed" not in checked.stdout):
                raise ValueError(f"naming epoch control {name} did not fail at its declared assertion")
        finally:
            path.write_bytes(original)
    print("rust-naming-epoch: delayed notices passed, both controls failed")


def verify_compaction_publication_versions(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target")),
        "CARGO_PROFILE_TEST_DEBUG": "0",
    }
    cases = (
        ("publication_versions_come_from_the_acknowledgement_or_verified_existing_bytes",
         ["-p", "rtdb-storage", "--features", "remote", "--test", "remote_lifetime"], "publication-read",
         "rust/crates/storage/src/remote.rs", "        Ok(version)\n    }\n\n    async fn publication_confirmed",
         "        Ok(self.client.head(&self.path(key)).await.map_err(|error| provider_error(key, error))?.version)\n    }\n\n    async fn publication_confirmed",
         "the same read must supply the body and version"),
        ("retirement_binds_only_the_same_version_the_catalog_published",
         ["-p", "rtdb-maintenance", "--test", "gc"], "retirement-version-match",
         "rust/crates/maintenance/src/retirement_intent.rs",
         "published.as_ref().is_some_and(|expected| *expected == PublishedObjectVersion::from(observed.clone()))",
         "published.is_some()", "an observed replacement cannot stand in for the catalog's publication"),
        ("a_groups_artefacts_go_only_with_the_last_member_the_catalog_holds",
         ["-p", "rtdb-maintenance", "--test", "gc"], "group-publication-match",
         "rust/crates/maintenance/src/gc.rs",
         "versions.all(|version| version == Some(first)).then_some(first)", "Some(first)",
         "differing member publications cannot bind the group file"),
    )
    for test, selection, name, file, before, after, failure in cases:
        command = ["cargo", "test", "--locked", *selection, test, "--", "--exact"]
        baseline = run_case(command, source / "rust", args.evidence, f"rust-{name}-baseline", env)
        if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
            raise ValueError(f"{name}: native baseline did not pass")
        path = source / file
        original = path.read_bytes()
        try:
            replace_once(path, before, after)
            checked = run_case(command, source / "rust", args.evidence, f"rust-{name}-control", env)
            if (checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
                or failure not in checked.stdout or "0 passed; 1 failed" not in checked.stdout):
                raise ValueError(f"{name}: mutation did not fail at its declared assertion")
        finally:
            path.write_bytes(original)
        print(f"rust-{name}: publication baseline passed, weakened binding failed")


def verify_s3_object_version(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target")),
        "CARGO_PROFILE_TEST_DEBUG": "0",
    }
    test = "tests::retirement_observation_accepts_only_exact_version_ids"
    command = ["cargo", "test", "--locked", "-p", "rtdb-s3", "--lib", test, "--", "--exact"]
    baseline = run_case(command, source / "rust", args.evidence, "rust-s3-object-version-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("the S3 adapter did not observe the expected version IDs")

    adapter = source / "rust/crates/storage/src/remote.rs"
    original = adapter.read_bytes()
    try:
        replace_once(adapter, ".filter(|version| valid_object_version(version.as_bytes()))", "")
        checked = run_case(command, source / "rust", args.evidence, "rust-s3-object-version-bypass", env)
        if (checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
            or "retirement observation returned an unusable provider version" not in checked.stdout
            or "0 passed; 1 failed" not in checked.stdout):
            raise ValueError("bypassing version validation did not fail at the expected assertion")
    finally:
        adapter.write_bytes(original)
    print("rust-s3-object-version: loopback HEAD passed, bypassing validation failed")


def verify_replay_prefix(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    test = "startup_selects_a_newer_checkpoint_when_its_initial_replica_prefix_was_trimmed"
    command = ["cargo", "test", "--locked", "-p", "rtdb-node", "--test", "native", test, "--", "--exact"]
    baseline = run_case(command, source / "rust", args.evidence, "rust-replay-prefix-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("native checkpoint selection after replica prefix trimming failed")

    database = source / "rust/crates/node/src/database.rs"
    original = database.read_bytes()
    gate = "let recovered = if self.options.replication.enabled {"
    try:
        replace_once(database, gate, gate.replace("if self", "if false && self"))
        checked = run_case(command, source / "rust", args.evidence, "rust-replay-prefix-skip-admission", env)
        if (
            checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
            or "startup completed before admitting its replica cursor" not in checked.stdout
            or "0 passed; 1 failed" not in checked.stdout
        ):
            raise ValueError("skipping cursor admission did not fail during replay startup")
    finally:
        database.write_bytes(original)
    driver = source / "rust/crates/replication/src/driver.rs"
    original = driver.read_bytes()
    try:
        replace_once(driver, "let replica_tail = recovered.take_replay_tail(&logs, read_position)?;",
                     "let replica_tail = None;")
        checked = run_case(command, source / "rust", args.evidence, "rust-replay-prefix-reopen-accepted-cursor", env)
        if (
            checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
            or "the driver must reuse startup's admitted cursor" not in checked.stdout
            or "0 passed; 1 failed" not in checked.stdout
        ):
            raise ValueError("reopening an admitted cursor did not fail the handoff assertion")
    finally:
        driver.write_bytes(original)
    print("rust-replay-prefix: checkpoint selection and same-cursor replay passed, both controls rejected")


def verify_multi_trim_admission(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    test = "logs::tests::recovery_refuses_a_pending_trim_before_the_logkeeper_applies_it"
    command = ["cargo", "test", "--locked", "-p", "rtdb-multi", "--lib", test, "--", "--exact"]
    label = "rust-multi-trim-admission"
    baseline = run_case(command, source / "rust", args.evidence, f"{label}-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("native Multi recovery admission did not enforce the authorized trim")

    holds = source / "rust/crates/multi/src/recovery_holds.rs"
    original = holds.read_bytes()
    try:
        replace_once(holds, "if let Some(anchor) = &record.replica_anchor", "if false && let Some(anchor) = &record.replica_anchor")
        control = run_case(command, source / "rust", args.evidence, f"{label}-ignore-authorization", env)
        if (
            control.returncode != 101 or f"{test} ... FAILED" not in control.stdout
            or "authorized trim must exclude the required record before it is removed" not in control.stdout
            or "0 passed; 1 failed" not in control.stdout
        ):
            raise ValueError("ignoring Multi trim authorization did not fail at the native admission assertion")
    finally:
        holds.write_bytes(original)
    print(f"{label}: directory floor enforced before physical trim, missing check rejected")


def verify_fragment_residence_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    test = "fragment_requires_the_opened_residence_and_replica_epoch"
    command = ["cargo", "test", "--locked", "-p", "rtdb-query", "-p", "rtdb-multi",
               "-p", "rtdb-node", "-p", "rtdb-replication", "--test", "cell", test, "--", "--exact"]
    label = "rust-fragment-residence"
    node = source / "rust/crates/node/src/node.rs"
    original = node.read_bytes()
    baseline = run_case(command, source / "rust", args.evidence, f"{label}-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("fragment residence baseline did not pass")
    anchor = "fragment.residence.as_ref().is_some_and(|expected| database.fragment_residence() == Some(expected))"
    controls = {
        "ignore-storage": (
            "fragment.residence.as_ref().is_some_and(|expected| database.fragment_residence().is_some_and(|actual| actual.replica_epoch == expected.replica_epoch))",
            "the worker must refuse another storage residence",
        ),
        "ignore-epoch": (
            "fragment.residence.as_ref().is_some_and(|expected| database.fragment_residence().is_some_and(|actual| actual.storage == expected.storage))",
            "the worker must refuse another replica epoch",
        ),
    }
    try:
        for name, (replacement, assertion) in controls.items():
            node.write_bytes(original)
            replace_once(node, anchor, replacement)
            control = run_case(command, source / "rust", args.evidence, f"{label}-{name}", env)
            if (control.returncode != 101 or f"{test} ... FAILED" not in control.stdout
                or assertion not in control.stdout or "0 passed; 1 failed" not in control.stdout):
                raise ValueError(f"{label}-{name}: expected the named residence assertion")
    finally:
        node.write_bytes(original)
    print(f"{label}: exact opened scope matched, both narrowed comparisons failed their assertions")


def verify_delegated_scan_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    command = ["cargo", "test", "--locked", "-p", "rtdb-query", "-p", "rtdb-multi",
               "-p", "rtdb-node", "-p", "rtdb-replication", "--test", "scan"]
    label = "rust-delegated-scan"
    fragment = source / "rust/crates/query/src/fragment.rs"
    original = fragment.read_bytes()
    controls = {
        "allow-later-head": (
            "snapshot.tx_basis() != Some(fragment.basis)",
            "snapshot.tx_basis().is_none_or(|head| head.tx_id < fragment.basis.tx_id)",
            "delegated_scan_refuses_a_later_erase_horizon_and_falls_back",
            "local fallback must preserve the coordinator's captured row",
        ),
        "release-on-cancel": (
            "IoRegion::retaining(context, work).await.flatten().ok_or_else(cancelled)?",
            "work.await.flatten().ok_or_else(cancelled)?",
            "delegated_scan_keeps_its_snapshot_until_cancelled_provider_reads_drain",
            "the admitted provider read must retain the fragment snapshot",
        ),
    }
    baseline = run_case([*command, "delegated_scan_"], source / "rust", args.evidence, f"{label}-baseline", env)
    if (baseline.returncode or "2 passed; 0 failed" not in baseline.stdout
        or any(f"{test} ... ok" not in baseline.stdout for _, _, test, _ in controls.values())):
        raise ValueError("delegated scan baselines did not both pass")
    try:
        for name, (find, replacement, test, assertion) in controls.items():
            fragment.write_bytes(original)
            replace_once(fragment, find, replacement)
            control = run_case([*command, test, "--", "--exact"], source / "rust", args.evidence, f"{label}-{name}", env)
            if (control.returncode != 101 or f"{test} ... FAILED" not in control.stdout
                or assertion not in control.stdout or "0 passed; 1 failed" not in control.stdout):
                raise ValueError(f"{label}-{name}: expected the named fragment assertion")
    finally:
        fragment.write_bytes(original)
    print(f"{label}: both baseline cases passed and both source controls failed at their expected assertions")


def verify_checkpoint_metadata_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    test = "store::tests::an_admitted_checkpoint_keeps_packed_and_loose_metadata_until_provider_reads_drain"
    command = ["cargo", "test", "--locked", "-p", "rtdb-multi", "--lib", test, "--", "--exact"]
    label = "rust-multi-checkpoint-metadata"
    baseline = run_case(command, source / "rust", args.evidence, f"{label}-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("Multi reaper failed the checkpoint metadata lifetime contract")

    store = source / "rust/crates/multi/src/store.rs"
    original = store.read_bytes()
    try:
        replace_once(store, "let oldest = record.storage_regions.values().filter_map(StorageRegion::checkpoint_metadata_from).min();",
                     "let oldest = None;")
        control = run_case(command, source / "rust", args.evidence, f"{label}-ignore-holds", env)
        if (
            control.returncode != 101 or f"{test} ... FAILED" not in control.stdout
            or "an admitted checkpoint root must survive collection" not in control.stdout
            or "0 passed; 1 failed" not in control.stdout
        ):
            raise ValueError("ignoring checkpoint holds did not fail at the packed root assertion")
    finally:
        store.write_bytes(original)
    print(f"{label}: startup metadata survived collection until provider drain, ignoring the hold failed")


def verify_recovery_hold_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    test = "a_recovering_subscriber_keeps_its_suffix_through_a_concurrent_trim"
    command = ["cargo", "test", "--locked", "-p", "rtdb-multi", "--test", "cell", test, "--", "--exact"]
    label = "rust-multi-recovery-hold"
    baseline = run_case(command, source / "rust", args.evidence, f"{label}-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("Multi subscriber did not complete recovery and allow trim progress")

    agent = source / "rust/crates/multi/src/agent.rs"
    original = agent.read_bytes()
    try:
        replace_once(agent, "StorageRegion::ReplicaReplay { first_required, .. } => Some(*first_required),",
                     "StorageRegion::ReplicaReplay { .. } => None,")
        control = run_case(command, source / "rust", args.evidence, f"{label}-ignore-holds", env)
        if (
            control.returncode != 101 or f"{test} ... FAILED" not in control.stdout
            or "trim must retain the first required replica record" not in control.stdout
            or "0 passed; 1 failed" not in control.stdout
        ):
            raise ValueError("ignoring replay holds did not fail at the physical retention assertion")
    finally:
        agent.write_bytes(original)
    print(f"{label}: recovery and later trim progress passed, ignoring the hold failed")


def verify_catalog_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    command = ["cargo", "test", "--locked", "-p", "rtdb-index", "--test", "catalogs"]
    historical = "a_checkpoint_preserves_pending_historical_promotion_for_each_block"
    complete = "four_iid_parts_promote_together_and_garbage_deletion_keeps_watermark_4946"
    overlap = "checkpoint_replay_preserves_overlapping_group_publication"
    baseline = run_case(command, source / "rust", args.evidence, "rust-catalog-baseline", env)
    if baseline.returncode or not all(f"{test} ... ok" in baseline.stdout for test in (historical, complete, overlap)):
        raise ValueError("catalog promotion or checkpoint replay baseline failed")

    restore = """            for (shard, value) in &state.shards {
                let (1, Some(recency), []) = (shard.level, shard.recency, shard.part.as_slice()) else { continue; };
                for (block, trie) in &value.tries {
                    if trie.state == TrieState::Nascent {
                        state.l1h_recencies.entry(*block).or_default().insert(recency);
                    }
                }
            }
"""
    controls = (
        (
            "forget-pending", "trie_catalog.rs", restore, historical,
            'left: {"l00-rc-b01", "l01-rc-b00"}',
        ),
        (
            "skip-last-bucket", "tiered_publication.rs", "\n        && is_usable(members[3], block)", complete,
            'left: {"l02-rc-p0-b00", "l02-rc-p1-b00", "l02-rc-p2-b00"}',
        ),
    )
    for name, file, find, test, expected in controls:
        path = source / "rust/crates/index/src" / file
        original = path.read_bytes()
        try:
            replace_once(path, find, "")
            checked = run_case(
                [*command, test, "--", "--exact"], source / "rust", args.evidence, f"rust-catalog-{name}", env,
            )
            if (
                checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
                or expected not in checked.stdout or "0 passed; 1 failed" not in checked.stdout
            ):
                raise ValueError(f"{name}: expected a failure at {test}")
        finally:
            path.write_bytes(original)
    print("rust-catalog-replay: checkpoint and promotion tests passed, both production mutations rejected")


def verify_system_time_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    test = "catalog_transactions_advance_system_time_and_replay_in_order"
    command = ["cargo", "test", "--locked", "-p", "rtdb-node", "--test", "native", test, "--", "--exact"]
    baseline = run_case(command, source / "rust", args.evidence, "rust-system-time-baseline", env)
    if baseline.returncode or f"{test} ... ok" not in baseline.stdout or "1 passed; 0 failed" not in baseline.stdout:
        raise ValueError("native catalog clock and replay baseline failed")

    resolver = source / "rust/crates/tx/src/resolver.rs"
    original = resolver.read_bytes()
    decision = "system_time: self.smooth(log_time)? };\n        let (result, operation) = match verdict {"
    try:
        replace_once(resolver, decision, decision.replace("self.smooth(log_time)?", "log_time"))
        checked = run_case(command, source / "rust", args.evidence, "rust-system-time-raw-catalog-clock", env)
        if (
            checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
            or "catalog attach must advance past the previous transaction's time" not in checked.stdout
            or "Transaction key does not advance its predecessor" not in checked.stdout
            or "0 passed; 1 failed" not in checked.stdout
        ):
            raise ValueError("raw catalog clock mutation did not fail at the native attach")
    finally:
        resolver.write_bytes(original)
    print("rust-system-time-replay: catalog outcomes survive restart and the raw clock mutation is rejected")


def verify_merge_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    test = "merge::tests::row_heap_matches_flatten_and_sort_without_losing_occurrences"
    command = ["cargo", "test", "--locked", "-p", "rtdb-maintenance", "--lib", test, "--", "--exact"]
    checked = run_case(command, source / "rust", args.evidence, "rust-merge-baseline", env)
    if checked.returncode or f"{test} ... ok" not in checked.stdout:
        raise ValueError("production merge did not agree with flatten-and-sort")

    merge_dir = source / "rust/crates/maintenance/src/merge"
    controls = (
        ("choose-later-child", "row_heap.rs", "precedes(right_key, child_key)", "precedes(child_key, right_key)",
         "merge order differs from sorting the input"),
        ("drop-final-row", "row_heap.rs", "if row == page_length {\n        let last", "if row.saturating_add(1) >= page_length {\n        let last",
         "merge did not consume every input occurrence"),
        ("return-first-coordinate", "row_heap.rs", "\n    result\n}", "\n    (result.0, 0)\n}",
         "merge skipped or repeated a row in a stream"),
        ("reverse-iid-bytes", "planner.rs", "u128::from_be_bytes", "u128::from_le_bytes",
         "Compaction page is not IID ascending and system-time descending"),
    )
    for name, file, find, replacement, expected in controls:
        source_file = merge_dir / file
        original = source_file.read_bytes()
        try:
            replace_once(source_file, find, replacement)
            checked = run_case(command, source / "rust", args.evidence, f"rust-merge-{name}", env)
            if (
                checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
                or expected not in checked.stdout or "0 passed; 1 failed" not in checked.stdout
            ):
                raise ValueError(f"{name}: expected a single failure at {test}")
        finally:
            source_file.write_bytes(original)
    print("rust-merge-replay: production heap agrees with flatten-and-sort, all four mutations rejected")


def verify_planner_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    test = "mixed_trie_depths_preserve_every_row_in_the_requested_partition"
    command = ["cargo", "test", "--locked", "-p", "rtdb-maintenance", "--test", "merge", test, "--", "--exact"]
    checked = run_case(command, source / "rust", args.evidence, "rust-planner-baseline", env)
    if checked.returncode or f"{test} ... ok" not in checked.stdout:
        raise ValueError("native compaction did not preserve the requested row occurrences")

    maintenance = source / "rust/crates/maintenance/src"
    controls = (
        ("drop-coarse-page", "merge/planner.rs", "_ => children.push((segment, index)),", "_ => (),", "[]"),
        ("skip-recency-branch", "merge/planner.rs",
         "MetadataNode::RecencyBranch(children) => for (_, child) in children { expand(segment, *child, inputs, output); },",
         "MetadataNode::RecencyBranch(_) => (),", "[]"),
        ("skip-job-clip", "merge.rs", "if job.part.len() > task.path.len() { &job.part } else { &task.path }",
         "&task.path", "[1, 0]"),
    )
    for name, file, find, replacement, partition in controls:
        source_file = maintenance / file
        original = source_file.read_bytes()
        try:
            replace_once(source_file, find, replacement)
            checked = run_case(command, source / "rust", args.evidence, f"rust-planner-{name}", env)
            if (
                checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
                or f"compaction changed row occurrences for partition {partition}" not in checked.stdout
                or "0 passed; 1 failed" not in checked.stdout
            ):
                raise ValueError(f"{name}: expected the row oracle to reject partition {partition}")
        finally:
            source_file.write_bytes(original)
    print("rust-planner-replay: six partitions matched the row oracle, all three mutations rejected")


def verify_same_time_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    test = "merge::tests::same_time_compaction_preserves_the_winning_put_or_delete_at_each_valid_time"
    command = ["cargo", "test", "--locked", "-p", "rtdb-maintenance", "--lib", test, "--", "--exact"]
    checked = run_case(command, source / "rust", args.evidence, "rust-same-time-baseline", env)
    if checked.returncode or f"{test} ... ok" not in checked.stdout:
        raise ValueError("native same-time resolution did not preserve the winning commands")

    merge = source / "rust/crates/maintenance/src/merge.rs"
    original = merge.read_bytes()
    controls = (
        ("keep-shadowed-fragment", "system_to >= event.system_from", "resolved same-time fragments overlap"),
        ("drop-visible-start", "system_to > event.system_from && event.valid_time.lower() != valid_from",
         "same-time resolution changed the winning command"),
    )
    for name, replacement, expected in controls:
        try:
            replace_once(merge, "system_to > event.system_from", replacement)
            checked = run_case(command, source / "rust", args.evidence, f"rust-same-time-{name}", env)
            if (
                checked.returncode != 101 or f"{test} ... FAILED" not in checked.stdout
                or expected not in checked.stdout or "0 passed; 1 failed" not in checked.stdout
            ):
                raise ValueError(f"{name}: expected the same-time oracle to reject the mutation")
        finally:
            merge.write_bytes(original)
    print("rust-same-time-replay: winners and non-overlap checked, both mutations rejected")

    history_test = "same_time_clipping_and_erase_preserve_the_surrounding_history"
    history_command = ["cargo", "test", "--locked", "-p", "rtdb-maintenance", "--test", "merge", history_test, "--", "--exact"]
    checked = run_case(history_command, source / "rust", args.evidence, "rust-compaction-history-baseline", env)
    if checked.returncode or f"{history_test} ... ok" not in checked.stdout:
        raise ValueError("native compaction changed historical query points")
    try:
        replace_once(merge, "buckets.entry(target).or_default().push(event);",
                     "if event.op != EventOp::Erase { buckets.entry(target).or_default().push(event); }")
        checked = run_case(history_command, source / "rust", args.evidence, "rust-compaction-history-drop-erase", env)
        if (
            checked.returncode != 101 or f"{history_test} ... FAILED" not in checked.stdout
            or "compaction changed the historical point" not in checked.stdout
            or "0 passed; 1 failed" not in checked.stdout
        ):
            raise ValueError("expected the historical oracle to reject dropping the erase barrier")
    finally:
        merge.write_bytes(original)
    print("rust-compaction-history: historical points matched, erase mutation rejected")


def verify_azure_replay(args, source):
    env = os.environ | {
        "CARGO_TARGET_DIR": str(ROOT / "target"),
        "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0",
    }
    command = ["cargo", "test", "--locked", "-p", "rtdb-azure", "--test", "copy_completion"]
    checked = run_case(command, source / "rust", args.evidence, "rust-azure-copy-baseline", env)
    if checked.returncode or "2 passed; 0 failed" not in checked.stdout:
        raise ValueError("Azure copy replay baseline failed")
    controls = (
        ("ack-on-accept", "rust/crates/azure/src/copy.rs",
         'Some(b"success") => Ok(response),', 'Some(b"success" | b"pending") => Ok(response),',
         "a_pending_copy_finishes_before_publication_and_staging_cleanup", "publication or cleanup preceded copy completion"),
        ("delete-on-unknown", "rust/crates/storage/src/remote.rs",
         "            Err(error) if copy_outcome_unknown(&error) => return Err(provider_error(key, error)),\n", "",
         "an_unconfirmed_copy_preserves_its_staging_source", "an uncertain copy did not keep its staging source"),
    )
    for name, file, find, replacement, test, expected in controls:
        path = source / file
        original = path.read_bytes()
        replace_once(path, find, replacement)
        checked = run_case(
            [*command, test, "--", "--exact"], source / "rust", args.evidence, f"rust-azure-{name}", env,
        )
        if (
            checked.returncode != 101
            or f"{test} ... FAILED" not in checked.stdout
            or expected not in checked.stdout
            or "0 passed; 1 failed" not in checked.stdout
        ):
            raise ValueError(f"{name}: expected a failure at {test}")
        path.write_bytes(original)
    print("rust-azure-copy: completion and unknown-outcome tests passed, both production mutations rejected")


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
    print("rust-generated-trace: both production resolvers and visibility agree on all 4096 observations")


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
    parser.add_argument("--mcrl2-bin", type=Path)
    parser.add_argument("--openjml")
    parser.add_argument("--autohyper")
    parser.add_argument("--rust-boundary", action="store_true")
    parser.add_argument("--native-replay", action="store_true")
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    if os.environ.get("BIJECTION_LOCAL_EXECUTION") != "owner-authorized":
        parser.error("local proof execution requires BIJECTION_LOCAL_EXECUTION=owner-authorized")
    args.evidence = args.evidence.resolve()
    args.evidence.mkdir(parents=True, exist_ok=False)
    supplied_tools = {name: getattr(args, name) for name in
                      ("lean", "verus", "z3", "java", "tlc", "alloy", "apalache", "maude")}
    if args.mcrl2_bin:
        supplied_tools.update({name: args.mcrl2_bin / name for name in ("mcrl22lps", "lps2lts", "ltscompare")})
    if args.openjml:
        supplied_tools["openjml"] = args.openjml
    if args.autohyper:
        app = Path(shutil.which(args.autohyper) or args.autohyper).resolve()
        paths = app.with_name("paths.json")
        paths_config = json.loads(paths.read_text())
        supplied_tools.update({
            "autohyper": app,
            "autohyper_config": paths,
            "autohyper_dll": app.with_name("AutoHyper.dll"),
            "autohyper_omega_dll": app.with_name("FsOmegaLib.dll"),
            "autohyper_system_dll": app.with_name("TransitionSystemLib.dll"),
            "autfilt": paths_config["autfilt"],
            "ltl2tgba": paths_config["ltl2tgba"],
        })
        if dotnet_root := os.environ.get("DOTNET_ROOT"):
            supplied_tools["dotnet"] = Path(dotnet_root) / "dotnet"
    tools = {}
    for name, supplied in supplied_tools.items():
        executable = Path(shutil.which(supplied) or supplied).resolve()
        setattr(args, name, str(executable))
        with executable.open("rb") as handle:
            tools[name] = {"path": str(executable), "sha256": hashlib.file_digest(handle, "sha256").hexdigest()}
    (args.evidence / "run.json").write_text(json.dumps({
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "rust_boundary": args.rust_boundary,
        "native_replay": args.native_replay,
        "tools": tools,
    }, indent=2) + "\n")
    source_paths = [
        path for path in (ROOT / "verification").rglob("*")
        if path.is_file() and path.suffix in {".lean", ".tla", ".cfg", ".als", ".maude", ".mcrl2", ".hltl", ".smv", ".py", ".rs", ".json", ".tsv", ".smt2"}
    ]
    source_paths.extend(ROOT / path for path in (
        "rust/crates/text/src/lib.rs",
        "rust/crates/search/src/file.rs",
        "rust/crates/search/src/lib.rs",
        "rust/crates/query/src/optimize.rs",
        "rust/crates/search/src/accelerate.rs",
        "rust/crates/expr/src/search.rs",
        "rust/crates/expr/src/evaluate.rs",
        "rust/crates/query/src/accelerate.rs",
        "rust/crates/replication/src/blocks.rs",
        "rust/crates/replication/src/appender.rs",
        "rust/crates/replication/src/driver.rs",
        "rust/crates/replication/src/logs.rs",
        "rust/crates/replication/src/tests.rs",
        "rust/crates/format/src/keys.rs",
        "rust/crates/index/src/trie_catalog.rs",
        "rust/crates/index/tests/catalogs.rs",
        "rust/crates/maintenance/src/gc.rs",
        "rust/crates/maintenance/src/retirement_intent.rs",
        "rust/crates/maintenance/tests/gc.rs",
        "rust/crates/index/src/partition.rs",
        "rust/crates/query/src/cursor.rs",
        "rust/crates/query/src/fragment.rs",
        "rust/crates/query/src/scan.rs",
        "rust/crates/query/tests/scan.rs",
        "rust/crates/node/src/node.rs",
        "rust/crates/storage/src/io_region.rs",
        "rust/crates/maintenance/src/merge.rs",
        "rust/crates/maintenance/src/merge/planner.rs",
        "rust/crates/maintenance/tests/merge.rs",
        "rust/crates/trie/src/bucketer.rs",
        "rust/crates/conformance/src/faults.rs",
        "rust/crates/conformance/tests/node_faults.rs",
        "rust/crates/conformance/tests/recovery.rs",
        "rust/crates/node/tests/native.rs",
        "rust/crates/node/src/database.rs",
        "rust/crates/tx/src/resolver.rs",
        "rust/crates/azure/src/copy.rs",
        "rust/crates/azure/src/lib.rs",
        "rust/crates/azure/tests/copy_completion.rs",
        "rust/crates/storage/src/remote.rs",
        "rust/crates/storage/tests/remote_lifetime.rs",
        "rust/crates/search/src/file.rs",
        "rust/crates/search/src/lane.rs",
        "rust/crates/format/src/lib.rs",
        "core/src/main/proto/xtdb/log/proto/log.proto",
        "rust/crates/storage/src/object_store.rs",
        "rust/crates/storage/src/disk_cache.rs",
        "rust/crates/s3/src/lib.rs",
        "rust/crates/s3/src/versions.rs",
        "rust/crates/multi/src/reference.rs",
        "rust/crates/multi/src/archive.rs",
        "rust/crates/types/src/position.rs",
        "rust/crates/storage/src/log/mod.rs",
        "rust/crates/storage/src/log/memory.rs",
        "rust/crates/kafka/src/lib.rs",
        "rust/crates/logkeeper/src/client/mod.rs",
        "rust/crates/multi/src/logs.rs",
        "rust/crates/multi/src/recovery_holds.rs",
        "rust/crates/multi/src/store.rs",
        "rust/crates/multi/src/catalog_readers.rs",
        "rust/crates/multi/src/packing.rs",
        "rust/crates/directory/src/lib.rs",
        "rust/crates/directory/src/dynamo/parked.rs",
        "rust/crates/node/src/resources.rs",
        "rust/crates/multi/src/agent.rs",
        "rust/crates/multi/src/control.rs",
        "rust/crates/multi/src/fragments.rs",
        "rust/crates/multi/src/process.rs",
        "rust/crates/query/src/context.rs",
        "rust/crates/multi/tests/cell.rs",
    ))
    if args.openjml:
        source_paths.extend(ROOT / path for path in (
            "core/src/main/java/xtdb/trie/LeafMerge.java",
            "core/src/main/kotlin/xtdb/trie/MemoryHashTrie.kt",
            "core/src/test/kotlin/xtdb/trie/MemoryHashTrieTest.kt",
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
        if args.openjml:
            verify_jvm_leaf_merge(args, directory)
        verify_rank_bound(args, directory)
        verify_residence_clock_margin(args, directory)
        verify_rank_cutoff_order(args, directory)
        verify_alloy(args, directory)
        verify_apalache(args, directory)
        verify_maude(args, directory)
        if args.mcrl2_bin:
            verify_mcrl2(args, directory)
        if args.autohyper:
            verify_hyperltl(args, directory)
        if args.rust_boundary or args.native_replay:
            source = directory / "production"
            capture_production(source)
        if args.rust_boundary:
            verify_replica_boundary(args, source)
        if args.native_replay:
            replay_rank_witness(args, directory)
            replay_rust_trace(args)
            verify_checkpoint_replay(args, source)
            verify_checkpoint_trie_retention(args, source)
            verify_trie_removal_readback(args, source)
            verify_durable_trie_retirement(args, source)
            verify_catalog_admission_floor(args, source)
            verify_trie_naming_epoch(args, source)
            verify_s3_object_version(args, source)
            verify_compaction_publication_versions(args, source)
            verify_replay_prefix(args, source)
            verify_multi_trim_admission(args, source)
            verify_recovery_hold_replay(args, source)
            verify_checkpoint_metadata_replay(args, source)
            verify_delegated_scan_replay(args, source)
            verify_fragment_residence_replay(args, source)
            verify_catalog_replay(args, source)
            verify_system_time_replay(args, source)
            verify_merge_replay(args, source)
            verify_planner_replay(args, source)
            verify_same_time_replay(args, source)
            verify_azure_replay(args, source)


if __name__ == "__main__":
    main()
