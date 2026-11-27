#!/usr/bin/env python3
"""Drive a native processor trial from one closed upstream refused-claim capture."""

import argparse
import json
import subprocess
from pathlib import Path, PurePosixPath
from zipfile import BadZipFile, ZipFile

import run_log_processor_sim as reference
from run_controls import REPOSITORY, digest, run_process, validate_native_revision, write_json
from source_gate import require_phase_two


SCOPE = "jvm-refused-claim-boot-and-authored-inputs"
ADAPTER_FILES = {
    "run_log_processor_sim.py", "simulation/LogProcessorTrace.kt", "inventory.init.gradle",
    "jvm-java/rtdb/conformance/DiscoveryRecorder.java", "jvm-java/rtdb/conformance/JunitRecorder.java",
    "jvm-services/META-INF/services/org.junit.platform.launcher.TestExecutionListener",
    "source_gate.py", "run_controls.py", "runtime_inventory.py",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def relative_path(name):
    path = PurePosixPath(name)
    require(not path.is_absolute() and path.parts and path.as_posix() == name
            and all(part != ".." for part in path.parts), "invalid archived path")
    return path


def owned_file(root, name):
    return reference.regular(root / relative_path(name))


def verify_file(root, item, path_field):
    path = owned_file(root, item[path_field])
    require(path.stat().st_size == item["bytes"] and digest(path) == item["sha256"],
            f"archived file differs from its receipt: {item[path_field]}")
    return path


def validate_recorded_file(root, item, recorded):
    require(item == recorded.receipt(root), f"archived receipt differs from the validated bytes: {item['path']}")


def retain_validated(recorded, output):
    with output.open("xb") as target:
        target.write(recorded.data)


def canonical_path(name):
    path = Path(name)
    require(path.anchor == "/" and str(path) == name and ".." not in path.parts, "invalid canonical JVM path")
    return path


def runtime_aliases(selection, tape, runtime):
    requests = reference.runtime_requests(selection, tape)
    resolved = runtime["requested_roots"]
    require(set(resolved) == set(requests), "JVM runtime alias roles differ from the recorded requests")
    aliases = {}
    for kind, requested in requests.items():
        entries = resolved[kind]
        require([entry["requested"] for entry in entries] == requested
                and all(set(entry) == {"requested", "canonical"} for entry in entries),
                f"JVM runtime aliases differ from the recorded {kind} paths")
        optional = kind in {"classpath", "test_class_directories"}
        paths = set()
        for entry in entries:
            canonical = entry["canonical"]
            if canonical is None:
                require(optional, f"JVM runtime omitted a required {kind} path")
            else:
                paths.add(canonical_path(canonical))
        aliases[kind] = paths
    reference.require_consistent_runtime_aliases(resolved)
    return aliases


def runtime_members(root, original, runtime, aliases):
    expected_roots = set().union(*aliases.values())
    file_roots = aliases["java"] | aliases["modules"] | aliases["java_agents"]
    require(len(runtime["roots"]) == len(expected_roots), "JVM runtime root count differs from its aliases")
    roots = set()
    members = set()
    outside_directories = set()
    for entry in runtime["roots"]:
        path = canonical_path(entry["path"])
        kind = entry["kind"]
        require(set(entry) == {"path", "kind", "members"} and path in expected_roots and path not in roots
                and kind in {"file", "directory"}, "invalid canonical JVM runtime root")
        reference.require_disjoint_runtime_root(original, path)
        roots.add(path)
        if path in file_roots:
            require(kind == "file", "JVM executable, module or agent root must be a file")
        if path in aliases["test_class_directories"]:
            require(kind == "directory", "JVM test-class root must be a directory")
        inventory = entry["members"]
        if kind == "file":
            require(inventory == [], "JVM runtime file lists directory members")
            members.add(path)
            continue
        require(isinstance(inventory, list) and inventory == sorted(set(inventory)),
                "JVM runtime member inventory is not unique and ordered")
        listed = {relative_path(name) for name in inventory}
        members.update(path / name for name in listed)
        relative = reference.retained_runtime_path(original, path)
        directory = root / relative
        require(directory.is_dir() and directory.resolve() == directory, f"missing selected runtime directory: {relative}")
        local = {member.relative_to(directory) for member, is_directory in reference.runtime_tree(directory)
                 if not is_directory}
        require(local == listed, f"JVM runtime directory closure differs: {relative}")
        if not path.is_relative_to(original):
            outside_directories.add(relative)
    require(roots == expected_roots, "JVM runtime roots differ from the requested paths")
    outside_files = {reference.retained_runtime_path(original, path) for path in members if not path.is_relative_to(original)}
    verify_external_tree(root, outside_files, outside_directories)
    return members


def verify_external_tree(root, files, directories):
    expected_directories = {Path("artifacts")} | directories
    for member in files | directories:
        expected_directories.update(parent for parent in member.parents if parent != Path("."))
    artifacts = root / "artifacts"
    require(artifacts.is_dir() and artifacts.resolve() == artifacts, "missing physical JVM runtime archive")
    actual_files = set()
    actual_directories = {Path("artifacts")}
    for path, is_directory in reference.runtime_tree(artifacts):
        if is_directory:
            actual_directories.add(path.relative_to(root))
        else:
            actual_files.add(path.relative_to(root))
    require(actual_files == files and actual_directories == expected_directories,
            "physical JVM runtime archive contains missing or orphaned entries")


def verify_runtime(root, original, selection, tape, runtime, captured_requests):
    require(runtime["schema_version"] == 2 and runtime["classpath"] == selection["classpath"]
            and runtime["class_sources"] == tape["classes"]
            and all(runtime[key] == tape[key] for key in ("java_home", "java_executable", "java_modules", "jvm_args")),
            "archived JVM runtime differs from its worker and task selection")
    require(runtime["requested_roots"] == captured_requests, "JVM runtime aliases differ from their capture-time receipt")
    aliases = runtime_aliases(selection, tape, runtime)
    selected = aliases["classpath"]
    recorder = original / "reference/build/rtdb-conformance/recorder"
    service = original / "adapter/jvm-services"
    require({recorder, service} <= selected, "selected JVM runtime omitted the owned recorder")
    classes = {name: canonical_path(path) for name, path in tape["classes"].items()}
    require(set(classes) == reference.CLOSURE_CLASSES and set(classes.values()) <= selected,
            "invalid observed JVM code sources")
    require(aliases["java"] == {canonical_path(tape["java_executable"])},
            "worker executable differs from its recorded JDK launcher")
    require(aliases["modules"] == {canonical_path(tape["java_modules"])},
            "JDK modules differ from the archived worker observation")
    require(len(runtime["artifacts"]) <= 200_000, "invalid JVM runtime artifact count")
    members = runtime_members(root, original, runtime, aliases)
    require(0 < len(members) == len(runtime["artifacts"]), "JVM runtime omitted or added retained members")
    artifacts = {}
    retained_paths = set()
    for item in runtime["artifacts"]:
        origin = canonical_path(item["path"])
        require(origin in members and origin not in artifacts, "invalid or repeated JVM artifact origin")
        retained = reference.retained_runtime_path(original, origin).as_posix()
        require(item["retained"] == retained and retained not in retained_paths,
                "JVM artifact is outside its unique retained location")
        retained_paths.add(retained)
        artifacts[origin] = verify_file(root, item, "retained")

    test_roots = aliases["test_class_directories"]
    for name, location in classes.items():
        if name == "rtdb.conformance.JunitRecorder":
            require(location == recorder, "worker used a different recorder location")
        elif name == "xtdb.indexer.LogProcessor":
            require(location.is_relative_to(original / "reference/core/build") and location not in test_roots,
                    "worker used a different production processor")
        else:
            require(location in test_roots and location.is_relative_to(original / "reference/core/build"),
                    f"worker used a different test class: {name}")
        entry = name.replace(".", "/") + ".class"
        if location in artifacts:
            with ZipFile(artifacts[location]) as archive:
                require(entry in archive.namelist(), f"archived JVM omitted class: {name}")
        else:
            require(location / entry in artifacts, f"archived JVM omitted class: {name}")


def read_capture(root):
    capture_file = reference.RecordedFile.read(owned_file(root, "reference-capture.json"))
    capture = json.loads(capture_file.data)
    reference_revision = capture["reference_revision"]
    require(capture["schema_version"] == 1 and capture["scope"] == "jvm-log-processor-refused-claim-pilot"
            and capture["source"] == reference.FIXTURE and reference_revision in reference.SUPPORTED_REFERENCES
            and capture["qualified_name"] == f"{reference.TEST_CLASS}/{reference.METHOD}"
            and capture["task"] == reference.TASK and capture["selected_iterations"] == 1
            and capture["original_assertion_status"] == "SUCCESSFUL", "archive is not a complete original refused-claim capture")
    inputs_file = reference.RecordedFile.read(root / "inputs.json")
    inputs = json.loads(inputs_file.data)
    require(inputs["reference_revision"] == reference_revision and inputs["upstream_sources"] == reference.OVERLAY_HASHES
            and set(inputs["adapter"]) == ADAPTER_FILES
            and set(inputs["derived_sources"]) == set(reference.OVERLAY_HASHES) | {reference.HELPER},
            "archive has different source inputs")
    process_file = reference.RecordedFile.read(root / "process.json")
    process = json.loads(process_file.data)
    tree = Path(process["cwd"])
    require(tree.is_absolute() and tree.name == "reference" and process["completed"] is True and process["exit_code"] == 0
            and process["command"] == [str(part) for part in reference.gradle_command(tree, tree.parent)],
            "archive has a different original Gradle invocation")
    unique, selection, worker_files, tape_file = reference.selected_invocation(root)
    tape = json.loads(tape_file.data)
    require(capture["unique_id"] == unique, "capture marker identifies a different worker invocation")
    runtime = reference.RecordedFile.read(owned_file(root, "runtime-artifacts.json"))
    resolved = reference.RecordedFile.read(owned_file(root, "runtime-paths.json"))
    validated = {file.path.relative_to(root).as_posix(): file
                 for file in (*worker_files, tape_file, runtime, resolved, inputs_file, process_file)}
    for path, checksum in inputs["adapter"].items():
        name = f"adapter/{path}"
        file = reference.RecordedFile.read(owned_file(root, name))
        require(file.sha256 == checksum, f"wrong archived adapter source: {path}")
        validated[name] = file
    for path in reference.OVERLAY_HASHES:
        original_name, derived_name = f"original/{path}", f"reference/{path}"
        original = reference.RecordedFile.read(owned_file(root, original_name))
        derived = reference.RecordedFile.read(owned_file(root, derived_name))
        expected_source = reference.derived_source(path, original.data, inputs["requested_seed"], tape["schema_version"])
        require(derived.data == expected_source and derived.sha256 == inputs["derived_sources"][path],
                f"wrong archived derived source: {path}")
        validated[original_name], validated[derived_name] = original, derived
    helper_name = f"reference/{reference.HELPER}"
    helper = reference.RecordedFile.read(owned_file(root, helper_name))
    require(helper.sha256 == inputs["derived_sources"][reference.HELPER] == inputs["adapter"]["simulation/LogProcessorTrace.kt"],
            "wrong archived observer helper")
    validated[helper_name] = helper
    expected = {"gradle.stderr.log", "reference/build/test-run.log",
                f"reference/core/build/test-results/property-test/TEST-{reference.TEST_CLASS}.xml"} | set(validated)
    paths = [item["path"] for item in capture["artifacts"]]
    require(len(paths) == len(expected) and set(paths) == expected, "capture marker omitted or repeated archived paths")
    for item in capture["artifacts"]:
        if item["path"] in validated:
            validate_recorded_file(root, item, validated[item["path"]])
        else:
            verify_file(root, item, "path")

    require(reference.check_tape(tape, unique, inputs["requested_seed"]) == capture["observation"],
            "tape does not match its original capture summary")
    verify_runtime(root, tree.parent, selection, tape, json.loads(runtime.data), json.loads(resolved.data))
    return tape, tape_file, capture_file


def project(tape, tape_sha, capture_sha, reference_revision):
    require(reference_revision in reference.SUPPORTED_REFERENCES, "unsupported upstream simulation reference")
    schema = tape["schema_version"]
    require(type(schema) is int and schema in (1, reference.LIVE_INDEX_TAPE_SCHEMA), "unknown simulation tape version")
    rows_per_block, _, _, documents = tape["events"][1]["fields"]
    boot = []
    inputs = []
    tails = {}
    opened = set()
    refused = set()
    delivered = set()
    writers = {}
    starts = {}
    assignments = []
    for frame in tape["events"]:
        sequence = frame["sequence"]
        kind, fields = frame["kind"], frame["fields"]

        def event(variant=kind, **fields):
            return {"sequence": sequence, "event": {"kind": variant, **fields}}

        if kind == "tail_open" and fields[0] == "replica":
            _, tail, actor, start = fields
            tails[tail] = actor
            if actor not in opened:
                require(start == 0, f"initial replica tail did not start at zero at {sequence}")
                boot.append(event(actor=actor, tail=tail, start=start))
                opened.add(actor)
        elif kind == "claim_refused" and fields[0] not in refused:
            actor, term, term_sequence, _ = fields
            require(term == 1 and term_sequence == 0, f"missing first refused claim at {sequence}")
            boot.append(event("refusal", actor=actor, term=term, term_sequence=term_sequence))
            refused.add(actor)
        elif kind == "enqueue":
            stream, actor, _, offset, time, message = fields
            if stream == "src":
                require(message == ["flush", None], f"unsupported source stimulus at {sequence}")
                inputs.append(event("flush", offset=offset, time=time))
            elif offset == 0:
                require(actor == 0 and message == ["noop", 1, 0, None], "invalid initial reference claim")
                boot.append(event("claim", actor=actor, term=1, term_sequence=0, time=time))
        elif kind in {"durable", "receipt"} and fields == ["replica", 0]:
            boot.append(event(offset=0))
        elif kind == "delivery" and fields[0] == "replica":
            _, tail, start, end = fields
            actor = tails[tail]
            if actor not in delivered:
                require(start == 0 and end > start, f"initial replica delivery did not contain zero at {sequence}")
                boot.append(event(actor=actor, tail=tail, **{"from": start, "to": end}))
                delivered.add(actor)
        elif kind == "assignment":
            assignments.append(fields)
        elif kind == "external_start":
            action, token = fields
            require(action not in starts, f"external action started twice at {sequence}")
            start = event(action=action, token=token, documents=documents[action])
            starts[action] = start["event"]
            inputs.append(start)
        elif kind == "external_writer":
            action, tx_id, time = fields
            require(action in starts and action not in writers, f"duplicate or unsubmitted external writer at {sequence}")
            starts[action].update(tx_id=tx_id, time=time)
            writers[action] = [tx_id, time]
        elif kind == "external_applied":
            action, tx_id, time, committed = fields
            require(writers.get(action) == [tx_id, time], f"external receipt differs from writer at {sequence}")
            inputs.append(event(action=action, tx_id=tx_id, time=time, committed=committed))

    require(assignments == [[None, 0]] and opened == {0, 1, 2} and refused == {1, 2} and delivered == opened
            and len(boot) == 11 and len(starts) == len(writers) == len(documents),
            "capture lacks the boot and single-assignment stimulus required by this trial")
    return {"schema_version": schema, "reference_revision": reference_revision, "unique_id": tape["unique_id"],
            "seed": tape["seed"], "reference_capture_sha256": capture_sha, "tape_sha256": tape_sha,
            "rows_per_block": rows_per_block, "boot": boot, "inputs": inputs}


def match_live_index_assertion(tape, execution):
    if tape["schema_version"] != reference.LIVE_INDEX_TAPE_SCHEMA:
        return []
    frames = [frame for frame in tape["events"] if frame["kind"] == reference.LIVE_INDEX_ASSERTION]
    require(len(frames) == 3, "reference live-index assertion lacks the three actor operands")
    summary = execution["summary"]
    joined = summary["joined_live_index"]
    prefix = summary["native_replica"]
    suffix = joined["replica_suffix"]
    selected_tip, tip = summary["captured_replica_tip"], joined["replica_tip"]
    require(isinstance(prefix, list) and isinstance(suffix, list) and reference.int_value(tip, 0, 4095)
            and reference.int_value(selected_tip, 0, tip) and len(prefix) == selected_tip + 1
            and len(prefix) + len(suffix) == tip + 1,
            "native joined replica evidence does not cover its complete prefix")
    records = prefix + suffix
    admitted = []
    for offset, record in enumerate(records):
        require(type(record["offset"]) is int and record["offset"] == offset and type(record["term_admitted"]) is bool,
                "invalid native replica offset or term admission")
        message = record["message"]
        if record["term_admitted"] and message["kind"] == "transaction":
            require(reference.int_value(message["tx_id"], 0), "invalid admitted native replica transaction ID")
            admitted.append(message["tx_id"])
    require(admitted and type(joined["last_applied_tx_id"]) is int and joined["last_applied_tx_id"] == max(admitted),
            "native live-index expectation differs from the admitted replica transactions")
    expected_tx = max(admitted)
    actors = joined["actors"]
    require(isinstance(actors, list) and len(actors) == 3, "native live-index assertion lacks the three actor operands")
    jvm = []
    native = []
    for actor, frame, observed in zip(range(3), frames, actors):
        observed_actor, reference_expected, reference_actual = frame["fields"]
        require(type(observed_actor) is int and observed_actor == actor
                and reference.int_value(reference_expected, 0) and reference.int_value(reference_actual, 0)
                and reference_expected == reference_actual, "invalid captured reference live-index operands")
        require(set(observed) == {"actor", "tx_id"} and type(observed["actor"]) is int and observed["actor"] == actor
                and reference.int_value(observed["tx_id"], 0) and observed["tx_id"] == expected_tx,
                "native joined live index differs from its admitted replica transactions")
        require((expected_tx, observed["tx_id"]) == (reference_expected, reference_actual),
                "native joined live-index operands differ from the original JVM assertion")
        jvm.append({"actor": actor, "expected_tx_id": reference_expected, "actual_tx_id": reference_actual,
                    "sequence": frame["sequence"]})
        native.append({"actor": actor, "expected_tx_id": expected_tx, "actual_tx_id": observed["tx_id"]})
    return [{"source": reference.FIXTURE, "line": reference.LIVE_INDEX_ASSERTION_LINE,
             "method": "assertLiveIndexesCaughtUp", "unique_id": tape["unique_id"],
             "reference": jvm, "native": native, "native_replica_tip": tip}]


def replay(args):
    require_phase_two(REPOSITORY, args.phase_two)
    revision = reference.clean_checkout(REPOSITORY)
    archive = args.capture.resolve(strict=True)
    native = args.native.resolve(strict=True)
    destination = args.output.absolute()
    run = destination.parent.resolve(strict=True) / destination.name
    require(not any(run.is_relative_to(path) for path in (REPOSITORY, archive)), "replay output must be outside both input trees")
    require(args.timeout > 0 and native.is_file(), "timeout and native binary must be available")
    run.mkdir()
    tape, tape_file, capture_file = read_capture(archive)
    owned_tape = run / "reference-tape.json"
    owned_capture = run / "reference-capture.json"
    retain_validated(tape_file, owned_tape)
    retain_validated(capture_file, owned_capture)
    tape_sha = tape_file.sha256
    capture_sha = capture_file.sha256
    projected_path = run / "projected.json"
    reference_revision = json.loads(capture_file.data)["reference_revision"]
    projected = project(tape, tape_sha, capture_sha, reference_revision)
    write_json(projected_path, projected)
    projected_file = reference.RecordedFile.read(projected_path)
    require(json.loads(projected_file.data) == projected, "recorded projection differs from the authored replay inputs")
    binary_sha = digest(native)
    output = run / "native"
    process = run_process([native, "simulation-refused-claim", projected_path, output],
                          REPOSITORY, run / "stdout.log", run / "stderr.log", args.timeout)
    process_path = run / "process.json"
    write_json(process_path, process)
    process_file = reference.RecordedFile.read(process_path)
    require(json.loads(process_file.data) == process and process["completed"] is True and process["exit_code"] == 0,
            "native refused-claim trial failed or its process receipt differs")
    execution_file = reference.RecordedFile.read(output / "execution.json")
    stdout = reference.RecordedFile.read(run / "stdout.log")
    require(execution_file.data == stdout.data, "native execution receipt bytes differ from stdout")
    execution = json.loads(execution_file.data)
    validate_native_revision(execution, revision)
    require(type(execution["schema_version"]) is int and execution["schema_version"] == projected["schema_version"]
            and execution["scope"] == SCOPE
            and execution["execution_complete"] is True and execution["binary_sha256"] == digest(native) == binary_sha
            and execution["input_sha256"] == projected_file.sha256
            and execution["reference_capture_sha256"] == capture_sha and execution["tape_sha256"] == tape_sha
            and execution["reference_revision"] == reference_revision
            and execution["unique_id"] == tape["unique_id"] and execution["seed"] == tape["seed"],
            "native replay receipt differs from its observed inputs")
    assertions = match_live_index_assertion(tape, execution)
    retained = (reference.RecordedFile(owned_capture, capture_file.data, capture_sha),
                reference.RecordedFile(owned_tape, tape_file.data, tape_sha), projected_file, process_file, stdout, execution_file)
    stderr = reference.regular(run / "stderr.log")
    artifacts = [file.receipt(run) for file in retained]
    artifacts.append({"path": "stderr.log", "sha256": digest(stderr), "bytes": stderr.stat().st_size})
    require(reference.clean_checkout(REPOSITORY) == revision
            and all(digest(reference.regular(file.path)) == file.sha256 for file in retained),
            "validated simulation evidence changed before publication")
    terminal = run / "result.json"
    result = {"schema_version": 1, "scope": SCOPE, "reference_revision": reference_revision,
              "reference_capture_sha256": capture_sha, "artifacts": artifacts}
    if assertions:
        result["matched_assertions"] = assertions
    write_json(terminal, result)
    return terminal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--capture", required=True, type=Path)
    parser.add_argument("--native", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--timeout", type=int, default=120)
    try:
        result = replay(parser.parse_args())
    except (OSError, ValueError, RuntimeError, KeyError, TypeError, IndexError, AttributeError, BadZipFile, subprocess.SubprocessError) as error:
        parser.exit(1, f"simulation replay failed: {error}\n")
    print(result)


if __name__ == "__main__":
    main()
