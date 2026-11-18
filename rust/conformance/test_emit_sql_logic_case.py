import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path, PurePosixPath
from unittest.mock import patch

from emit_sql_logic_case import CONFIGURATION, SIDES, encode, main, receipt_bundle
from evidence import closure, read_json, retained, sha256
from sql_logic_records import write_records
from sql_logic_results import json_lines


def write_json(path, value):
    path.write_bytes(encode(value))
    return path


def capture(archive, change=None, source_suffix=b"", build_change=None):
    identity = read_json(CONFIGURATION)["runtime_identity"]
    reference = identity["reference"]
    implementation = "a" * 40
    original = PurePosixPath("/original/worker")
    recorded_root = original / "run"
    root = archive / "capture"
    source = root / identity["source"]
    source.parent.mkdir(parents=True)
    pinned = Path(__file__).resolve().parents[2] / identity["source"]
    source.write_bytes(pinned.read_bytes() + source_suffix)
    relocations = []

    def relocate(path, original_path):
        record = retained(root, path)
        relocations.append({"original": str(original_path), "artifact": record["path"]})
        return {**record, "path": str(original_path)}

    source_artifact = relocate(source, original / "reference" / identity["source"])
    plan = root / "plan"
    plan.mkdir()
    records = plan / "records-00000.jsonl"
    script = write_records(root, identity["source"], reference, identity["selection"], identity["options"], records)
    plan_path = write_json(plan / "plan.json", {"schema_version": 1, "reference": reference, "selection": "slt-test", "scripts": [script]})
    write_json(root / "inputs.json", {"reference_revision": reference, "implementation_revision": implementation})
    write_json(root / "result.json", {"selection_passed": False, "complete_parity": False})

    production = root / "production"
    binaries = production / "binaries"
    binaries.mkdir(parents=True)
    original_build = original / "production"
    repository = original / "build-checkout"
    executable = binaries / "1-rtdb-conformance"
    executable.write_bytes(b"selected native production fixture")
    binary = relocate(executable, original_build / executable.relative_to(production).as_posix())
    other = binaries / "0-rtdb"
    other.write_bytes(b"other native production fixture")
    other_binary = relocate(other, original_build / other.relative_to(production).as_posix())

    def compiled(name, crate, test=False):
        source_root = repository / f"rust/crates/{crate}"
        compiler_path = f"deps/{name.replace('-', '_')}-1234567890abcdef" if test else name
        return {"reason": "compiler-artifact", "target": {"name": name, "kind": ["bin"], "src_path": str(source_root / "src/main.rs")},
                "manifest_path": str(source_root / "Cargo.toml"), "profile": {"test": test},
                "executable": str(original / "cargo-target/debug" / compiler_path)}

    compiler = compiled("rtdb-conformance", "conformance")
    other_compiler = compiled("rtdb", "server")
    selected_artifact = {**binary, "compiler_artifact": compiler, "retained_artifact": retained(production, executable)}
    cargo = ["cargo", "build", "--manifest-path", str(repository / "rust/Cargo.toml"), "--locked",
             "--workspace", "--all-targets", "--no-default-features", "--message-format=json"]
    build_status = {"completed": True, "exit_code": 0, "command": cargo, "cwd": str(repository)}
    if build_change is not None:
        build_change(build_status, selected_artifact)
    build_messages = production / "cargo.stdout.log"
    compiled_events = (other_compiler, compiled("rtdb-conformance", "conformance", test=True), compiler,
                       {"reason": "build-finished", "success": True})
    build_messages.write_bytes(b"".join(encode(event) for event in compiled_events))
    relocate(build_messages, original_build / build_messages.name)
    build_process = write_json(production / "cargo.process.json", build_status)
    relocate(build_process, original_build / build_process.name)
    build = write_json(production / "production-artifacts.json", {"implementation_revision": implementation,
                       "configuration": "linux-amd64/rust/local/production", "selection": {"variant": "local", "features": []},
                       "command": cargo, "process": retained(production, build_process), "messages": retained(production, build_messages),
                       "binaries": [{**other_binary, "compiler_artifact": other_compiler, "retained_artifact": retained(production, other)},
                                    selected_artifact]})
    native_build = relocate(build, original_build / build.name)
    library = root / "reference.jar"
    library.write_bytes(b"retained reference runtime fixture")
    artifacts = [{**relocate(library, original / "jdk/reference.jar"), "kind": "file"}]

    for side in SIDES:
        engine, revision = ("rust", implementation) if side == "native" else ("jvm", reference)
        directory = root / side
        observations = directory / "observations"
        observations.mkdir(parents=True)
        output = recorded_root / side / "observations"
        original_plan = recorded_root / "plan/plan.json"
        execution = {"schema_version": 1, "engine": engine, "artifact_revision": revision, "selection": "slt-test",
                     "plan_sha256": sha256(plan_path), "execution_complete": True,
                     "environment": {"storage_version": 6, "timezone": "UTC", "query_clock": "wall_clock_or_CURRENT_TIMESTAMP_variable"}}
        process = {"completed": True, "exit_code": 0}
        if side == "native":
            process.update(command=[binary["path"], "sql-logic", str(original_plan), str(output)], cwd=str(original / "implementation"))
            execution.update(binary_sha256=sha256(executable), source_dirty=False)
        else:
            reference_root = original / "reference"
            adapter = original / "implementation/rust/conformance"
            process.update(command=[str(reference_root / "gradlew"), "--no-daemon", "--console=plain", "-PfullJdk", "--init-script",
                                    str(adapter / "reference.init.gradle"), "rtdbConformanceSqlLogic", f"-Prtdb.adapterRoot={adapter / 'jvm'}",
                                    f"-Prtdb.fixture={original_plan}", f"-Prtdb.output={output}"], cwd=str(reference_root))
            manifest = write_json(observations / "runtime-artifacts.json", artifacts)
            execution.update(binary_sha256=sha256(manifest), artifacts=artifacts)

        rows = []
        queries = 0
        for entry in json_lines(records):
            kind = entry["record"]["type"]
            observation = {"identity": entry["identity"], "executed": True, "engine_executed": kind in ("query", "statement"),
                           "query_count_before": queries, "error": None, "result": None}
            if kind == "query":
                queries += 1
                observation.update(disposition="error", error={"message": "unrelated assertion fixture"})
                if entry["ordinal"] == 4:
                    value = {"text": "NULL\n", "rows": 1, "values": 1, "md5": hashlib.md5(b"NULL\n").hexdigest()}
                    if side == "native":
                        value.update(schema=[{"name": "_column_1", "type": "Null"}],
                                     basis={"timezone": "UTC", "current_time_micros": 1_577_998_800_000_000, "snapshot_token": "native-token"})
                    observation.update(disposition="query_ok", error=None, result=value)
            else:
                observation["disposition"] = "statement_ok" if kind == "statement" else "directive"
            observation["query_count_after"] = queries
            rows.append(observation)
        if change is not None:
            change(side, process, execution, rows)
        raw = observations / "script-00000.jsonl"
        raw.write_bytes(b"".join(encode(row) for row in rows))
        execution["scripts"] = [{"source": script["source"], "selection": script["selection"], "execution_complete": True,
                                 "observations": retained(observations, raw)}]
        process_path = write_json(directory / "process.json", process)
        execution_path = write_json(observations / "execution.json", execution)
        stdout = directory / "stdout.log"
        stdout.write_bytes(execution_path.read_bytes() if side == "native" else side.encode())
        stderr = directory / "stderr.log"
        stderr.touch()
        write_json(directory / "trace.json", {"artifact_revision": revision, "process_sha256": sha256(process_path),
                   "execution_sha256": sha256(execution_path), "stdout_sha256": sha256(stdout), "stderr_sha256": sha256(stderr)})

    run = {"id": "captured-slt", "kind": "sql_logic", "root": root.name, "recorded_root": str(recorded_root),
           "artifacts": closure(root), "relocations": relocations, "native_build": native_build, "native_binary": binary,
           "sources": [{"source": identity["source"], "artifact": source_artifact}]}
    return write_json(archive / "EXTERNAL.json", {"schema_version": 1, "reference_revision": reference,
                      "implementation_revision": implementation, "runs": [run]})


class SqlLogicCaseReceipts(unittest.TestCase):
    def test_scoped_receipts_rederive_after_archive_relocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            external = capture(root / "archive")
            output = root / "receipts"
            with patch("sys.argv", ["emit_sql_logic_case.py", "--external", str(external), "--run", "captured-slt", "--output", str(output)]):
                main()
            moved = root / "relocated"
            shutil.move(external.parent, moved)
            verify = ["emit_sql_logic_case.py", "--external", str(moved / external.name), "--run", "captured-slt", "--verify", str(output)]
            with patch("sys.argv", verify):
                main()
            ledger = read_json(output / "ledger.json")
            case, = ledger["cases"]
            self.assertFalse(ledger["inventory_complete"])
            self.assertFalse(ledger["runtime_reconciled"])
            script, = read_json(moved / "capture/plan/plan.json")["scripts"]
            self.assertEqual((44, 43), (script["records"], script["assertions"]))
            self.assertEqual([9, 10, 11, 12], case["source_assertion_lines"])
            self.assertEqual("record:4", case["identity"]["qualified_name"])
            self.assertEqual("assertion:3/line:9", case["identity"]["parameter_fixture"])
            lineage = read_json(output / "inputs.json")
            self.assertEqual(sha256(moved / "capture/reference-repeat/trace.json"), lineage["executions"]["reference-repeat"]["trace_sha256"])
            for side in ("reference", "native"):
                path = output / f"{side}.json"
                receipt, = read_json(path)
                self.assertEqual(case["identity"], receipt["identity"])
                self.assertEqual(sha256(moved / f"capture/{side}/trace.json"), receipt["trace_sha256"])
                self.assertEqual(sha256(path), lineage["outputs"][path.name])
                context = receipt["context"]
                artifact = (moved / "capture/production/binaries/1-rtdb-conformance" if side == "native"
                            else moved / "capture/reference/observations/runtime-artifacts.json")
                self.assertEqual(sha256(artifact), context["binary_sha256"])
                self.assertEqual(6, context["storage_version"])
                self.assertEqual("UTC", context["timezone"])
                self.assertEqual([None, None, None], [context[field] for field in ("applied_basis", "valid_time", "system_time")])
                self.assertEqual([{"id": "formatted-result", "executed": True, "outcome": {"kind": "returned", "typed_value": {
                    "type_string": "T", "sort": "rowsort", "text": "NULL\n"}}}], receipt["observations"])
            native = output / "native.json"
            native.write_bytes(native.read_bytes() + b"\n")
            with patch("sys.argv", verify), self.assertRaisesRegex(ValueError, "receipt differs from its archived proof: native.json"):
                main()

    def test_equal_reported_versions_are_projected_from_execution_evidence(self):
        def change(_side, _process, execution, _rows):
            execution["environment"]["storage_version"] = 9
        with tempfile.TemporaryDirectory() as directory:
            bundle = receipt_bundle(capture(Path(directory), change), "captured-slt")
        self.assertEqual(9, json.loads(bundle["native.json"])[0]["context"]["storage_version"])

    def test_selected_receipt_rejects_incoherent_execution_proof(self):
        for kind, diagnostic in (("missing-version", "storage version"), ("other-version", "different storage versions"),
                                 ("clock", "different query environment"), ("timezone", "different native query timezone"),
                                 ("repeat", "different reference-repeat invocation"), ("binary", "native SQL logic binary differs"),
                                 ("text", "did not pass on all three"), ("late-loss", "lost or added original records")):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                def change(side, process, execution, rows):
                    if kind == "repeat" and side == "reference-repeat":
                        process["command"][-1] = process["command"][-1].replace("/reference-repeat/", "/reference/")
                    if side != "native":
                        return
                    if kind == "missing-version":
                        del execution["environment"]["storage_version"]
                    elif kind == "other-version":
                        execution["environment"]["storage_version"] = 7
                    elif kind == "clock":
                        execution["environment"]["query_clock"] = "fixed"
                    elif kind == "timezone":
                        rows[3]["result"]["basis"]["timezone"] = "Europe/London"
                    elif kind == "binary":
                        execution["binary_sha256"] = "9" * 64
                    elif kind == "text":
                        rows[3]["result"].update(text="false\n", md5=hashlib.md5(b"false\n").hexdigest())
                    elif kind == "late-loss":
                        rows.pop()
                with self.assertRaisesRegex(ValueError, diagnostic):
                    receipt_bundle(capture(Path(directory), change), "captured-slt")

    def test_selected_receipt_requires_the_original_source_and_retained_repeat(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            external = capture(root / "changed", source_suffix=b"\r\n# changed original\r\n")
            with self.assertRaisesRegex(ValueError, "different original source bytes"):
                receipt_bundle(external, "captured-slt")
            external = capture(root / "missing")
            document = read_json(external)
            run, = document["runs"]
            run["artifacts"] = [item for item in run["artifacts"] if item["path"] != "reference-repeat/trace.json"]
            write_json(external, document)
            with self.assertRaisesRegex(ValueError, "unretained artifact: reference-repeat/trace.json"):
                receipt_bundle(external, "captured-slt")

    def test_selected_receipt_requires_the_cargo_invocation_and_ordinal_copy(self):
        for kind, diagnostic in (("target", "different Cargo target"), ("kind", "different Cargo target"),
                                 ("missing-executable", "production assembly differs"),
                                 ("manifest", "different source manifest"), ("entry", "different source manifest or entry point"),
                                 ("invocation", "different Cargo invocation"), ("cwd", "absolute normalized Cargo working directory"),
                                 ("copy", "ordinal production copy"), ("size", "retained executable bytes differ")):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                def change(process, artifact):
                    compiler = artifact["compiler_artifact"]
                    if kind == "target":
                        compiler["target"]["name"] = "rtdb"
                    elif kind == "kind":
                        compiler["target"]["kind"] = ["example"]
                    elif kind == "missing-executable":
                        del compiler["executable"]
                    elif kind == "manifest":
                        compiler["manifest_path"] = compiler["manifest_path"].replace("/conformance/", "/server/")
                    elif kind == "entry":
                        compiler["target"]["src_path"] = compiler["target"]["src_path"].replace("/main.rs", "/lib.rs")
                    elif kind == "invocation":
                        process["command"][3] = process["command"][3].replace("rust/Cargo.toml", "rust/crates/conformance/Cargo.toml")
                    elif kind == "cwd":
                        process["cwd"] += "/../build-checkout"
                    elif kind == "copy":
                        artifact["retained_artifact"]["path"] = "binaries/0-rtdb"
                    elif kind == "size":
                        artifact["bytes"] += 1
                with self.assertRaisesRegex(ValueError, diagnostic):
                    receipt_bundle(capture(Path(directory), build_change=change), "captured-slt")


if __name__ == "__main__":
    unittest.main()
