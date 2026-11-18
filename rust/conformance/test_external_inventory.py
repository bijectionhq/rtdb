import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from external_inventory import Evidence, canonical, joined, reconcile_external, sql_cases
from evidence import closure, retained, sha256
from sql_logic_records import write_records
from sql_logic_results import json_lines


def fixture(directory):
    path = Path(directory) / "original.test"
    path.write_text("statement error\nSELECT invalid;\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    reference = {"path": path.name, "sha256": digest}
    source_name = "sqlite-test/case.test"
    runtime = {"reference": "reference", "source": source_name, "source_sha256": digest,
        "selection": "slt-test", "options": {"direct_sql": False}, "ordinal": 1, "assertion_ordinal": 1, "line": 1}
    configuration = canonical({"adapter": "sql_logic", "selection": runtime["selection"], "options": runtime["options"]})
    identity = {"reference_revision": "reference", "source": source_name, "qualified_name": "record:1",
        "parameter_fixture": "original-line:1", "configuration": configuration}
    build_selection = {"variant": "local", "features": []}
    native_configuration = canonical({"build": "local-production", "selection": build_selection, "execution": json.loads(configuration)})
    binding = {"runtime_identity": runtime, "identity": identity, "counterparts": ["sql-logic/record:1"], "native_configuration": native_configuration}
    source = {"reference": "reference", "files": [{"path": source_name, "sha256": digest, "lane": "sql_logic", "record_count": 1}], "candidates": []}
    ledger = {"cases": [{"identity": identity, "counterparts": binding["counterparts"]}]}
    run = {"id": "selected", "kind": "sql_logic", "root": ".", "recorded_root": str(directory), "artifacts": [reference],
        "sources": [{"source": source_name, "artifact": reference}], "bindings": [binding]}
    document = {"schema_version": 1, "reference_revision": "reference", "implementation_revision": "native", "runs": [run],
        "files": [{"source": source_name, "sha256": digest, "required_invocations": [{"run": "selected", "identity": runtime}]}]}
    observed = [{"runtime_identity": runtime, "source": source_name, "passed": True, "outcome": "expected_error_passed", "observations": [], "errors": [],
                 "native_build_configuration": "local-production", "native_build_selection": build_selection}]
    return document, source, ledger, observed


NO_PROJECTION_SOURCE = "src/test/resources/xtdb/sql/logic_test/direct-sql/no-projected-cols.test"
NO_PROJECTION_SCRIPT = (
    "hash-threshold 100\n\n"
    "statement ok\nINSERT INTO foo (_id) VALUES (1), (2)\n\n"
    "statement ok\nINSERT INTO bar (_id) VALUES (3)\n\n"
    "query I\nSELECT 4 FROM foo\n----\n4\n4\n\n"
    "query I\nSELECT (SELECT foo._id FROM bar) FROM foo\n----\n2\n1\n"
)
NO_PROJECTION_SELECTION = "xtdb.sql.logic-test.direct-sql-test/direct-sql--no-projected-cols"
NATIVE_SCHEMA = [{"name": "_column_1", "type": {"Scalar": "I64"}}]
NATIVE_BASIS = {"current_time_micros": 1_577_998_800_000_000, "timezone": "UTC", "snapshot_token": "native-basis"}


def write_json(path, value):
    path.write_text(json.dumps(value) + "\n", encoding="utf-8", newline="\n")
    return path


def formatted_result(text):
    return {"text": text, "md5": hashlib.md5(text.encode("utf-8")).hexdigest(),
            "rows": len(text.splitlines()), "values": len(text.splitlines())}


def recorded_sql_cases(root, contents=NO_PROJECTION_SCRIPT, query_texts=("4\n4\n", "2\n1\n"), change=None):
    revision = "c84f9334eb060ceb92d28ecdd4a06e6b625932fd"
    implementation = "a" * 40
    original_root = Path("/original/sql-logic-worker")
    source_path = root / NO_PROJECTION_SOURCE
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes(contents.encode("utf-8"))
    options = {"direct_sql": True, "decorrelate": True, "query_limit": None, "max_rows": 1_000_000,
               "max_result_bytes": 128 * 1024 * 1024}
    plan = root / "plan"
    plan.mkdir()
    records_path = plan / "records-00000.jsonl"
    script = write_records(root, NO_PROJECTION_SOURCE, revision, NO_PROJECTION_SELECTION, options, records_path)
    plan_path = write_json(plan / "plan.json", {"schema_version": 1, "reference": revision,
                           "selection": "direct-sql-test", "scripts": [script]})
    write_json(root / "inputs.json", {"reference_revision": revision, "implementation_revision": implementation})

    def recorded(path):
        return {**retained(root, path), "path": str(original_root / path.relative_to(root))}

    production = root / "production"
    production.mkdir()
    binary = production / "rtdb-conformance"
    binary.write_bytes(b"retained production executable fixture")
    runtime = root / "reference.jar"
    runtime.write_bytes(b"retained reference JVM library fixture")
    compiler = {"reason": "compiler-artifact", "executable": str(original_root / "production/rtdb-conformance"),
                "target": {"name": "rtdb-conformance"}, "profile": {"test": False}}
    messages = production / "cargo.stdout.log"
    messages.write_text(json.dumps(compiler) + "\n" + json.dumps({"reason": "build-finished", "success": True}) + "\n")
    command = ["cargo", "build", "--locked"]
    build_process = write_json(production / "cargo.process.json", {"command": command, "completed": True, "exit_code": 0})
    build = write_json(production / "production-artifacts.json", {"configuration": "linux-amd64/rust/local/production",
                       "selection": {"variant": "local", "features": []}, "implementation_revision": implementation,
                       "command": command, "process": retained(production, build_process), "messages": retained(production, messages),
                       "binaries": [{**recorded(binary), "compiler_artifact": compiler, "retained_artifact": retained(production, binary)}]})
    originals = list(json_lines(records_path))
    for side, engine, artifact_revision in (("reference", "jvm", revision), ("reference-repeat", "jvm", revision),
                                           ("native", "rust", implementation)):
        directory = root / side
        observations = directory / "observations"
        observations.mkdir(parents=True)
        queries = 0
        actual = []
        for original in originals:
            source = original["record"]
            kind = source["type"]
            observation = {"identity": original["identity"], "executed": True, "engine_executed": kind in ("query", "statement"),
                           "kind": kind, "result": None, "error": None, "query_count_before": queries}
            if kind == "query":
                value = formatted_result(query_texts[queries])
                if engine == "rust":
                    value.update(schema=NATIVE_SCHEMA, basis=NATIVE_BASIS)
                observation.update(disposition="query_ok", result=value)
                queries += 1
            elif kind == "statement" and source["mode"] == "error":
                error = ({"category": "incorrect", "code": "xtdb.sql/parse-error", "message": "syntax error"} if engine == "rust"
                         else {"class": "xtdb.api.error.Incorrect", "message": "syntax error", "suppressed": []})
                observation.update(disposition="statement_expected_error", error=error)
            else:
                observation["disposition"] = "statement_ok" if kind == "statement" else "directive"
            observation["query_count_after"] = queries
            if change is not None:
                change(side, observation)
            actual.append(observation)
        rows = observations / "script-00000.jsonl"
        rows.write_text("".join(json.dumps(observation) + "\n" for observation in actual), encoding="utf-8", newline="\n")
        execution = {"schema_version": 1, "engine": engine, "selection": "direct-sql-test", "plan_sha256": sha256(plan_path),
                     "execution_complete": True, "artifact_revision": artifact_revision,
                     "scripts": [{"source": NO_PROJECTION_SOURCE, "selection": NO_PROJECTION_SELECTION, "execution_complete": True,
                                  "observations": retained(observations, rows)}]}
        output = original_root / side / "observations"
        plan_arg = original_root / "plan/plan.json"
        stdout = directory / "stdout.log"
        if engine == "rust":
            execution.update(binary_sha256=sha256(binary), source_dirty=False)
            command = [recorded(binary)["path"], "sql-logic", str(plan_arg), str(output)]
            write_json(stdout, execution)
        else:
            artifacts = [{**recorded(runtime), "kind": "file"}]
            manifest = write_json(observations / "runtime-artifacts.json", artifacts)
            execution.update(artifacts=artifacts, binary_sha256=sha256(manifest))
            command = ["/reference/gradlew", "--no-daemon", "--console=plain", "-PfullJdk", "--init-script",
                       "/impl/rust/conformance/reference.init.gradle", "rtdbConformanceSqlLogic", "-Prtdb.adapterRoot=/impl/rust/conformance/jvm",
                       f"-Prtdb.fixture={plan_arg}", f"-Prtdb.output={output}"]
            stdout.write_text(side, encoding="utf-8")
        stderr = directory / "stderr.log"
        stderr.touch()
        process = write_json(directory / "process.json", {"command": command, "cwd": "/impl" if engine == "rust" else "/reference",
                             "completed": True, "exit_code": 0})
        receipt = write_json(observations / "execution.json", execution)
        write_json(directory / "trace.json", {"artifact_revision": artifact_revision, "process_sha256": sha256(process),
                   "stdout_sha256": sha256(stdout), "stderr_sha256": sha256(stderr), "execution_sha256": sha256(receipt)})
    run = {"id": "selected", "kind": "sql_logic", "root": ".", "recorded_root": str(original_root), "artifacts": closure(root),
           "sources": [{"source": NO_PROJECTION_SOURCE, "artifact": recorded(source_path)}],
           "native_build": recorded(build), "native_binary": recorded(binary)}
    return sql_cases(Evidence(root, run), run, revision, implementation)


class ExternalEvidenceIdentity(unittest.TestCase):
    def test_source_query_agrees_on_formatted_text_with_native_schema_and_basis_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            cases = recorded_sql_cases(Path(directory))
        self.assertEqual([3, 6, 9, 15], [case["runtime_identity"]["line"] for case in cases])
        self.assertTrue(all(case["passed"] for case in cases))
        query = cases[2]
        self.assertEqual(4, query["runtime_identity"]["ordinal"])
        self.assertEqual(3, query["runtime_identity"]["assertion_ordinal"])
        self.assertEqual("e9f70ff1bebb08063d971647c26c31dfbe98b07a77be93377c866c392969bdec", query["runtime_identity"]["source_sha256"])
        reference, repeat, native = (item["result"] for item in query["observations"])
        expected = formatted_result("4\n4\n")
        self.assertEqual([expected, expected, {**expected, "schema": NATIVE_SCHEMA, "basis": NATIVE_BASIS}], [reference, repeat, native])

    def test_formatted_agreement_still_requires_the_original_oracle_and_all_three_executions(self):
        for difference in ("text", "md5", "missing-text", "execution", "counter", "both-wrong", "repeat"):
            with self.subTest(difference=difference), tempfile.TemporaryDirectory() as directory:
                def change(side, observation):
                    if observation["identity"]["line"] != 9:
                        return
                    if difference == "both-wrong":
                        observation.update(disposition="query_mismatch", result=formatted_result("5\n4\n"))
                    elif difference == "repeat" and side == "reference-repeat":
                        observation.update(disposition="query_mismatch", result=formatted_result("4\n5\n"))
                    elif side == "native":
                        if difference == "text":
                            observation["result"].update(formatted_result("4\n5\n"))
                        elif difference == "md5":
                            observation["result"]["md5"] = "0" * 32
                        elif difference == "missing-text":
                            del observation["result"]["text"]
                        elif difference == "execution":
                            observation["engine_executed"] = False
                        elif difference == "counter":
                            observation["query_count_after"] = 2

                cases = recorded_sql_cases(Path(directory), change=change)
                self.assertEqual([True, True, False, True], [case["passed"] for case in cases])
                self.assertEqual("unverified", cases[2]["outcome"])

    def test_original_rowsort_and_hashed_valuesort_evidence_keep_their_formatted_bytes(self):
        text = "10\n2\n"
        expected_hash = hashlib.md5(text.encode("utf-8")).hexdigest()
        script = ("query I rowsort\nVALUES (2), (10)\n----\n10\n2\n\n"
                  f"query I valuesort\nVALUES (2), (10)\n----\n2 values hashing to {expected_hash}\n")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            passing = recorded_sql_cases(root / "passing", contents=script, query_texts=(text, text))
            changed_order = recorded_sql_cases(root / "wrong", contents=script, query_texts=(text, "2\n10\n"))
        self.assertEqual([True, True], [case["passed"] for case in passing])
        self.assertEqual([True, False], [case["passed"] for case in changed_order])

    def test_expected_error_preserves_observed_side_errors_and_requires_each_one(self):
        def omit_native_error(side, observation):
            if side == "native":
                observation["error"] = None

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = "statement error\nSELECT invalid;\n"
            passing, = recorded_sql_cases(root / "passing", contents=script, query_texts=())
            missing, = recorded_sql_cases(root / "missing", contents=script, query_texts=(), change=omit_native_error)
        self.assertTrue(passing["passed"])
        self.assertEqual("expected_error_passed", passing["outcome"])
        self.assertEqual("xtdb.api.error.Incorrect", passing["observations"][0]["error"]["class"])
        self.assertEqual("xtdb.sql/parse-error", passing["observations"][2]["error"]["code"])
        self.assertFalse(missing["passed"])
        self.assertEqual("unverified", missing["outcome"])

    def test_expected_error_pass_is_retained_distinctly_with_exact_original_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            document, source, ledger, observed = fixture(directory)
            with patch("external_inventory.sql_cases", return_value=observed):
                result = reconcile_external(document, Path(directory), source, ledger)
            self.assertTrue(result["all_passed"])
            self.assertEqual("expected_error_passed", result["cases"][0]["outcome"])
            self.assertEqual({"sqlite-test/case.test"}, result["resolved_files"])

    def test_binding_cannot_change_options_or_omit_an_actual_invocation(self):
        for change in ("configuration", "invocation"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                document, source, ledger, observed = fixture(directory)
                if change == "configuration":
                    document["runs"][0]["bindings"][0]["native_configuration"] = "different"
                else:
                    additional = copy.deepcopy(observed[0])
                    additional["runtime_identity"]["ordinal"] = 2
                    observed.append(additional)
                with patch("external_inventory.sql_cases", return_value=observed), self.assertRaises(ValueError):
                    reconcile_external(document, Path(directory), source, ledger)

    def test_matching_reference_failure_does_not_resolve_source(self):
        with tempfile.TemporaryDirectory() as directory:
            document, source, ledger, observed = fixture(directory)
            observed[0].update(passed=False, outcome="unverified")
            with patch("external_inventory.sql_cases", return_value=observed):
                result = reconcile_external(document, Path(directory), source, ledger)
            self.assertFalse(result["all_passed"])
            self.assertEqual(set(), result["resolved_files"])

    def test_mutated_or_unretained_artifact_cannot_establish_external_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            document, _, _, _ = fixture(directory)
            evidence = Evidence(Path(directory), document["runs"][0])
            with self.assertRaises(ValueError):
                evidence.path("unretained.json")
            (Path(directory) / "original.test").write_text("different original")
            with self.assertRaises(ValueError):
                Evidence(Path(directory), document["runs"][0])

    def test_exit_success_without_join_never_establishes_execution(self):
        for process in ({"exit_code": 0}, {"completed": True, "exit_code": 0, "unexpected_live_descendants": [12]}):
            with self.assertRaises(ValueError):
                joined(process)

    def test_unknown_candidate_free_source_cannot_self_declare_support(self):
        with tempfile.TemporaryDirectory() as directory:
            document, source, ledger, observed = fixture(directory)
            original = source["files"][0]
            unknown = "lang/python/undetected_tests.py"
            source["files"].append({**original, "path": unknown, "lane": "language_client", "record_count": 0})
            document["files"].append({"source": unknown, "sha256": original["sha256"], "classification": "support_input",
                                      "required_invocations": [], "artifact": document["runs"][0]["artifacts"][0]})
            with patch("external_inventory.sql_cases", return_value=observed):
                result = reconcile_external(document, Path(directory), source, ledger)
            self.assertNotIn(unknown, result["resolved_files"])


if __name__ == "__main__":
    unittest.main()
