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
    timeout = 300 if command[0] == "cargo" else 120
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
            ["end_lookup_matches_expansion", "end_lookup_matches_expansion"],
        ),
        "lose-slice-origin": (
            "firstEndAfter (start + row) runs",
            "firstEndAfter (0 + row) runs",
            ["encoded_slice_read_matches_plain"],
        ),
    }
    verify_lean_model(args, scratch, path, "run-encoding", controls, 5)


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
            verify_basis_tokens(args, scratch)
            verify_run_encoding(args, scratch)
            verify_memory_admission(args, scratch)
            verify_memory_trie(args, scratch)
            verify_live_row_address(args, scratch)
            verify_snapshot_ownership(args, scratch)
            verify_jvm_trie_pending_logs(args, scratch)
            verify_trie_wire_paths(args, scratch)
            verify_outer_join(args, scratch)
            verify_table_slugs(args, scratch)
            verify_explicit_session(args, scratch)
            verify_logkeeper(args, scratch)
            verify_reader_retention(args, scratch)
            verify_generation_retention(args, scratch)
            verify_replica_trim_admission(args, scratch)
            verify_replica_hold_close(args, scratch)
            verify_compaction_projection(args, scratch, directory)
            verify_compaction_planner(args, scratch)
            verify_compaction_merge(args, scratch)
            verify_compaction_intervals(args, scratch, directory)
            verify_search_windows(args, scratch)
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
    ("Kernel", "unfair-apply", ("SPECIFICATION FairSpec", "SPECIFICATION Spec"), "temporal"),
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
    ("InstalledCheckpoint", "installed-checkpoint", None, None),
    ("InstalledCheckpoint", "installed-checkpoint-follows-current",
     ('Mutation = "none"', 'Mutation = "follow_current"'), "InstalledReadable"),
    ("InstalledCheckpoint", "installed-checkpoint-ignores-nascent",
     ('Mutation = "none"', 'Mutation = "ignore_nascent"'), "InstalledReadable"),
    ("InstalledCheckpoint", "installed-checkpoint-collection-witness",
     ("INVARIANTS TypeOK InstalledReadable ReaderReadable", "INVARIANT OldFilesNeverCollected"), "OldFilesNeverCollected"),
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
            if invariant == "temporal":
                message = "Temporal property EventuallyAcknowledged was violated"
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
    check_apalache(args, scratch, "RecoveryCollection", "recovery-collection", "RecoverySafe", 10)
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
    if (
        baseline.returncode
        or not all(f"{count} passed; 0 failed" in baseline.stdout for count in (1, 6))
        or not all(f"{test} ... ok" in baseline.stdout for test in
                   ("recovery_preserves_queries_through_tiered_publication", *overlapping_collection))
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
    print("rust-checkpoint-replay: seven tests passed, root-first and fail-fast mutations rejected")


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
        "rust/crates/replication/src/driver.rs",
        "rust/crates/replication/src/logs.rs",
        "rust/crates/format/src/keys.rs",
        "rust/crates/index/src/trie_catalog.rs",
        "rust/crates/index/tests/catalogs.rs",
        "rust/crates/maintenance/src/gc.rs",
        "rust/crates/maintenance/tests/gc.rs",
        "rust/crates/index/src/partition.rs",
        "rust/crates/query/src/cursor.rs",
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
        "rust/crates/storage/src/log/mod.rs",
        "rust/crates/storage/src/log/memory.rs",
        "rust/crates/kafka/src/lib.rs",
        "rust/crates/logkeeper/src/client/mod.rs",
        "rust/crates/multi/src/logs.rs",
        "rust/crates/multi/src/recovery_holds.rs",
        "rust/crates/multi/src/agent.rs",
        "rust/crates/multi/tests/cell.rs",
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
            verify_replay_prefix(args, source)
            verify_multi_trim_admission(args, source)
            verify_recovery_hold_replay(args, source)
            verify_catalog_replay(args, source)
            verify_system_time_replay(args, source)
            verify_merge_replay(args, source)
            verify_planner_replay(args, source)
            verify_same_time_replay(args, source)
            verify_azure_replay(args, source)


if __name__ == "__main__":
    main()
