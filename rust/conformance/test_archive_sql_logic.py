import hashlib
import json
import shutil
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from archive_sql_logic import archive_sql_logic
from emit_sql_logic_case import CONFIGURATION, SIDES, receipt_bundle, verify_production_build
from evidence import canonical, closure, read_json, retained, sha256
from external_inventory import Evidence
from sql_logic_records import write_records
from sql_logic_results import compare, json_lines
from sql_logic_selections import NON_DIRECT, SELECTION_SOURCES


ADAPTER_INPUTS = ("run_sql_logic.py", "sql_logic_records.py", "sql_logic_selections.py", "sql_logic_results.py",
                  "run_controls.py", "history_results.py", "reference.init.gradle", "jvm/rtdb/conformance/sql_logic.clj")


def write(path, contents):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(contents)
    return path


def json_bytes(value):
    return (canonical(value) + "\n").encode()


def write_json(path, value):
    return write(path, json_bytes(value))


def absolute_file(path):
    return {**retained(path.parent, path), "path": str(path)}


def production(worker, revision):
    directory = worker / "production-lane"
    checkout = worker / "build-checkout"
    cargo_target = worker / "cargo-target/debug"
    command = ["cargo", "build", "--manifest-path", str(checkout / "rust/Cargo.toml"), "--locked",
               "--workspace", "--all-targets", "--no-default-features", "--message-format=json"]

    def compiler(name, crate, executable, test=False):
        source = checkout / "rust/crates" / crate
        return {"reason": "compiler-artifact", "manifest_path": str(source / "Cargo.toml"),
                "target": {"name": name, "kind": ["bin"], "src_path": str(source / "src/main.rs")},
                "executable": str(executable), "profile": {"test": test}}

    server = compiler("rtdb", "server", cargo_target / "rtdb")
    native = compiler("rtdb-conformance", "conformance", cargo_target / "rtdb-conformance")
    test = compiler("rtdb-conformance", "conformance", cargo_target / "deps/rtdb_conformance-1234567890abcdef", test=True)
    process = write_json(directory / "cargo.process.json", {"command": command, "cwd": str(checkout), "completed": True, "exit_code": 0})
    messages = write(directory / "cargo.stdout.log", b"".join(map(json_bytes, (server, test, native, {"reason": "build-finished", "success": True}))))
    write(directory / "cargo.stderr.log", b"Finished dev profile\n")
    binaries = []
    for ordinal, event in enumerate((server, native)):
        copy = write(directory / "binaries" / f"{ordinal}-{Path(event['executable']).name}", f"retained executable {ordinal}\n".encode())
        binaries.append({**absolute_file(copy), "compiler_artifact": event, "retained_artifact": retained(directory, copy)})
    build = write_json(directory / "production-artifacts.json", {"configuration": "linux-amd64/rust/local/production",
                       "selection": {"variant": "local", "features": []}, "implementation_revision": revision,
                       "command": command, "process": retained(directory, process), "messages": retained(directory, messages), "binaries": binaries})
    return build, Path(binaries[-1]["path"])


def runtime_closures(worker):
    runtime = worker / "runtime"
    java = write(runtime / "jdk/bin/java", b"java executable fixture\n")
    modules = write(runtime / "jdk/lib/modules", b"java modules fixture\n")
    shared = write(runtime / "dependencies/shared.jar", b"shared library fixture\n")
    closures = {}
    for side in SIDES[:2]:
        classes = runtime / side / "classes"
        write(classes / "xtdb/Adapter.class", f"private {side} class fixture\n".encode())
        write(classes / "META-INF/services/adapter", b"xtdb.Adapter\n")
        library = write(runtime / side / "libraries/owner.jar", f"private {side} jar fixture\n".encode())
        closures[side] = [{"path": str(classes), "kind": "directory", "files": closure(classes)},
                          *[{**absolute_file(path), "kind": "file"} for path in (library, shared, java, modules)]]
    return closures


def observation_rows(records, side):
    queries = 0
    for entry in json_lines(records):
        record = entry["record"]
        kind = record["type"]
        row = {"identity": entry["identity"], "executed": True, "engine_executed": kind in ("query", "statement"),
               "query_count_before": queries, "error": None, "result": None}
        if kind == "query":
            queries += 1
            result = record["expected"]
            mismatch = side == "native" and entry["ordinal"] == 5
            text = "fixture-mismatch\n" if mismatch else result["text"]
            values = result["count"]
            row.update(disposition="query_mismatch" if mismatch else "query_ok",
                       result={"text": text, "values": values, "rows": values // len(record["type_string"]),
                               "md5": hashlib.md5(text.encode()).hexdigest()})
            if side == "native":
                row["result"]["basis"] = {"timezone": "UTC", "current_time_micros": 1_577_998_800_000_000, "snapshot_token": "native-fixture"}
        else:
            row["disposition"] = "statement_ok" if kind == "statement" else "directive"
        row["query_count_after"] = queries
        yield row


def preserve_execution(run, side, execution):
    owner = run / side
    receipt = write_json(owner / "observations/execution.json", execution)
    stdout = owner / "stdout.log"
    if side == "native":
        stdout.write_bytes(receipt.read_bytes())
    trace = {"artifact_revision": execution["artifact_revision"], "execution_sha256": sha256(receipt),
             "process_sha256": sha256(owner / "process.json"), "stdout_sha256": sha256(stdout), "stderr_sha256": sha256(owner / "stderr.log")}
    write_json(owner / "trace.json", trace)


def live_capture(root):
    worker = root / "worker"
    run = worker / "sql-logic-lane/run"
    reference = worker / "reference-checkout"
    native = worker / "native-checkout"
    equivalent_reference = root / "pinned-source-copy"
    identity = read_json(CONFIGURATION)["runtime_identity"]
    revision, implementation = identity["reference"], "a" * 40
    original = identity["source"]
    pinned_bytes = (Path(__file__).resolve().parents[2] / original).read_bytes()
    write(reference / original, pinned_bytes)
    write(equivalent_reference / original, pinned_bytes)
    for relative in SELECTION_SOURCES:
        contents = f"retained selection input {relative}\n".encode()
        write(reference / relative, contents)
        write(equivalent_reference / relative, contents)
    adapter = native / "rust/conformance"
    for relative in ADAPTER_INPUTS:
        write(adapter / relative, f"retained implementation input {relative}\n".encode())
    build, binary = production(worker, implementation)

    plan_dir = run / "plan"
    plan_dir.mkdir(parents=True)
    records = plan_dir / "records-00000.jsonl"
    script = write_records(reference, original, revision, identity["selection"], identity["options"], records)
    plan = {"schema_version": 1, "reference": revision, "selection": "slt-test", "scripts": [script]}
    plan_path = write_json(plan_dir / "plan.json", plan)
    selection = write_json(run / "selection.json", {"reference": revision, "names": ["slt-test"], "query_limit": None,
                           "original_task_allowances": NON_DIRECT, "allowances_grant_passing_records": False,
                           "sources": [{"path": original, "sha256": sha256(reference / original), "selected": True, "reason": "selected"}]})
    input_files = [*(adapter / relative for relative in ADAPTER_INPUTS), binary, plan_path, records,
                   *(reference / relative for relative in SELECTION_SOURCES), reference / original, selection]
    inputs = write_json(run / "inputs.json", {"reference_revision": revision, "implementation_revision": implementation,
                        "inputs": [{"path": str(path), "sha256": sha256(path)} for path in input_files]})

    runtime = runtime_closures(worker)
    streams = {}
    for side in SIDES:
        owner = run / side
        observations = owner / "observations"
        engine, artifact_revision = ("rust", implementation) if side == "native" else ("jvm", revision)
        execution = {"schema_version": 1, "engine": engine, "artifact_revision": artifact_revision, "selection": "slt-test",
                     "plan_sha256": sha256(plan_path), "execution_complete": True, "complete_parity": False,
                     "environment": {"storage_version": 6, "timezone": "UTC", "locale": "en_US",
                                     "source_clock": "2020-01-01T00:00:00Z plus one day per submitted transaction",
                                     "query_clock": "wall_clock_or_CURRENT_TIMESTAMP_variable"}}
        if side == "native":
            command = [str(binary), "sql-logic", str(plan_path), str(observations)]
            cwd = native
            execution.update(binary_sha256=sha256(binary), source_dirty=False)
        else:
            command = [str(reference / "gradlew"), "--no-daemon", "--console=plain", "-PfullJdk", "--init-script",
                       str(adapter / "reference.init.gradle"), "rtdbConformanceSqlLogic", f"-Prtdb.adapterRoot={adapter / 'jvm'}",
                       f"-Prtdb.fixture={plan_path}", f"-Prtdb.output={observations}"]
            cwd = reference
            runtime_path = write_json(observations / "runtime-artifacts.json", runtime[side])
            execution.update(binary_sha256=sha256(runtime_path), artifacts=runtime[side])
        rows = list(observation_rows(records, side))
        raw = write(observations / "script-00000.jsonl", b"".join(map(json_bytes, rows)))
        execution["scripts"] = [{"source": original, "selection": "slt-test", "records": len(rows), "execution_complete": True,
                                 "queries_run": rows[-1]["query_count_after"], "dispositions": dict(Counter(row["disposition"] for row in rows)),
                                 "observations": retained(observations, raw)}]
        write_json(owner / "process.json", {"command": command, "cwd": str(cwd), "completed": True, "exit_code": 0})
        write(owner / "stdout.log", f"{side} SQL logic fixture\n".encode())
        write(owner / "stderr.log", b"")
        preserve_execution(run, side, execution)
        streams[side] = raw

    repeat = compare(plan, plan_dir, [streams["reference"]], [streams["reference-repeat"]], run / "repeat-records.jsonl")
    comparison = compare(plan, plan_dir, [streams["reference"]], [streams["native"]], run / "comparison-records.jsonl")
    repeat_path = write_json(run / "repeat.json", repeat)
    comparison_path = write_json(run / "comparison.json", comparison)
    write_json(run / "result.json", {"selection_passed": repeat["selection_passed"] and comparison["selection_passed"],
               "complete_parity": False, "assertion_ledger_reconciled": False, "inputs_sha256": sha256(inputs),
               "comparison_sha256": sha256(comparison_path), "repeat_sha256": sha256(repeat_path)})
    write_json(run / "failure.json", {"selection_passed": False, "error": "SQL logic selection retains failed, skipped, unavailable, suppressed, or mismatching assertions"})
    return run, build, equivalent_reference


class SqlLogicArchive(unittest.TestCase):
    def test_partial_capture_is_portable_after_original_runtime_sources_and_production_are_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            run, build, reference = live_capture(root)
            original_run = closure(run)
            declared_inputs = read_json(run / "inputs.json")["inputs"]
            self.assertFalse(read_json(run / "result.json")["selection_passed"])
            self.assertTrue(read_json(run / "repeat.json")["selection_passed"])
            source = read_json(CONFIGURATION)["runtime_identity"]["source"]
            source_key = str(Path(read_json(run / "reference/process.json")["cwd"]) / source)
            binary_key = read_json(run / "native/process.json")["command"][0]
            runtime_files = {}
            for side in SIDES[:2]:
                for entry in read_json(run / side / "observations/runtime-artifacts.json"):
                    files = [entry] if entry["kind"] == "file" else [
                        {**file, "path": str(Path(entry["path"]) / file["path"])} for file in entry["files"]]
                    for file in files:
                        runtime_files[file["path"]] = file
            external = archive_sql_logic(run, build, reference, root / "archive", run_id="captured-slt")
            duplicate = archive_sql_logic(run, build, reference, root / "second-archive", run_id="captured-slt")
            self.assertEqual(external.read_bytes(), duplicate.read_bytes())
            self.assertEqual(original_run, closure(run))
            moved = root / "relocated"
            shutil.move(external.parent, moved)
            shutil.rmtree(root / "worker")
            shutil.rmtree(reference)

            external = moved / external.name
            document = read_json(external)
            capture, = document["runs"]
            evidence = Evidence(moved, capture)
            self.assertEqual(str(run), capture["recorded_root"])
            self.assertEqual([], capture["bindings"])
            self.assertEqual([], document["files"])
            self.assertEqual(source_key, capture["sources"][0]["artifact"]["path"])
            self.assertEqual(str(build), capture["native_build"]["path"])
            self.assertEqual(binary_key, capture["native_binary"]["path"])
            for reference_ in original_run:
                self.assertEqual(reference_["sha256"], sha256(evidence.path(reference_["path"])))
            for original in declared_inputs:
                self.assertEqual(original["sha256"], sha256(evidence.recorded(original)))
            for original in runtime_files.values():
                self.assertEqual(original["sha256"], sha256(evidence.recorded(original)))
            shared = str(root / "worker/runtime/dependencies/shared.jar")
            self.assertEqual(1, sum(item["original"] == shared for item in capture["relocations"]))
            self.assertEqual(capture["native_build"]["sha256"], sha256(evidence.recorded(capture["native_build"])))
            self.assertEqual(capture["native_binary"]["sha256"], sha256(evidence.recorded(capture["native_binary"])))
            receipts = receipt_bundle(external, "captured-slt")
            self.assertEqual("NULL\n", json.loads(receipts["native.json"])[0]["observations"][0]["outcome"]["typed_value"]["text"])
            self.assertEqual(sha256(evidence.path("reference-repeat/trace.json")),
                             json.loads(receipts["inputs.json"])["executions"]["reference-repeat"]["trace_sha256"])

    def test_inconsistent_input_or_missing_evidence_leaves_no_archive(self):
        for difference in ("source", "adapter", "repeat-trace", "runtime-member", "runtime-conflict", "native-copy", "comparison-records", "late-record"):
            with self.subTest(difference=difference), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                run, build, reference = live_capture(root)
                worker = root / "worker"
                if difference == "source":
                    source = read_json(CONFIGURATION)["runtime_identity"]["source"]
                    with (reference / source).open("ab") as file:
                        file.write(b"# altered pinned input\r\n")
                elif difference == "adapter":
                    (worker / "native-checkout/rust/conformance/run_sql_logic.py").write_bytes(b"changed adapter\n")
                elif difference == "repeat-trace":
                    (run / "reference-repeat/trace.json").unlink()
                elif difference == "runtime-member":
                    (worker / "runtime/reference-repeat/classes/xtdb/Adapter.class").unlink()
                elif difference == "runtime-conflict":
                    owner = run / "reference-repeat/observations"
                    execution = read_json(owner / "execution.json")
                    runtime = execution["artifacts"]
                    shared = next(entry for entry in runtime if entry["path"].endswith("/shared.jar"))
                    shared["sha256"] = "0" * 64
                    manifest = write_json(owner / "runtime-artifacts.json", runtime)
                    execution["binary_sha256"] = sha256(manifest)
                    preserve_execution(run, "reference-repeat", execution)
                elif difference == "native-copy":
                    Path(read_json(run / "native/process.json")["command"][0]).write_bytes(b"different binary\n")
                elif difference == "comparison-records":
                    with (run / "comparison-records.jsonl").open("ab") as file:
                        file.write(b"{}\n")
                elif difference == "late-record":
                    owner = run / "native/observations"
                    raw = owner / "script-00000.jsonl"
                    raw.write_bytes(b"".join(raw.read_bytes().splitlines(keepends=True)[:-1]))
                    execution = read_json(owner / "execution.json")
                    execution["scripts"][0]["observations"] = retained(owner, raw)
                    preserve_execution(run, "native", execution)
                originals = closure(run)
                children = set(root.iterdir())
                with self.assertRaises((OSError, ValueError)):
                    archive_sql_logic(run, build, reference, root / "archive")
                self.assertEqual(children, set(root.iterdir()))
                self.assertEqual(originals, closure(run))

    def test_existing_destination_is_not_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            run, build, reference = live_capture(root)
            output = root / "archive"
            sentinel = write(output / "existing", b"previous archive\n")
            with self.assertRaises(FileExistsError):
                archive_sql_logic(run, build, reference, output)
            self.assertEqual([sentinel], list(output.iterdir()))
            self.assertEqual(b"previous archive\n", sentinel.read_bytes())

    def test_destination_created_during_validation_is_not_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            run, build, reference = live_capture(root)
            output = root / "archive"
            before = set(root.iterdir())

            def concurrent_destination(*args):
                verify_production_build(*args)
                output.mkdir()

            with patch("archive_sql_logic.verify_production_build", side_effect=concurrent_destination):
                with self.assertRaises(FileExistsError):
                    archive_sql_logic(run, build, reference, output)
            self.assertEqual([], list(output.iterdir()))
            self.assertEqual(before | {output}, set(root.iterdir()))

    def test_failed_manifest_publication_removes_the_owned_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            run, build, reference = live_capture(root)
            output = root / "archive"
            before = set(root.iterdir())
            with patch.object(Path, "hardlink_to", side_effect=OSError("cannot publish manifest")):
                with self.assertRaisesRegex(OSError, "cannot publish manifest"):
                    archive_sql_logic(run, build, reference, output)
            self.assertEqual(before, set(root.iterdir()))


if __name__ == "__main__":
    unittest.main()
