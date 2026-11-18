import hashlib
import json
import tempfile
import types
import unittest
from pathlib import Path, PurePosixPath
from unittest.mock import patch

from evidence import closure, retained, sha256
from lane_artifacts import ASSERTION_COMPARISON_INPUTS, assertion_comparison_command, bind_inputs, sql_logic_command, verify_bindings
from matrix_plan import expand
from verify_matrix import verify, verify_lane_artifacts


def write_record(path, value):
    path.write_text(json.dumps(value))
    return path


def write_json_line(path, value):
    path.write_text(json.dumps(value) + "\n")
    return path


def write_sql_logic_trace(directory, revision):
    write_record(directory / "trace.json", {"artifact_revision": revision, **{
        f"{name}_sha256": sha256(directory / relative) for name, relative in (
            ("process", "process.json"), ("stdout", "stdout.log"), ("stderr", "stderr.log"),
            ("execution", "observations/execution.json"))}})


def sql_logic_fixture_record(source):
    if source == "sqlite-test/xtdb.test":
        return 9, 4, 3, ["query T rowsort", "SELECT ( 4 ) IN ( NULL )", "----", "NULL"]
    if source == "sqlite-test/select4.test":
        return 1, 1, 1, ["statement ok", "CREATE TABLE t1(", "  a1 INTEGER,", "  b1 INTEGER,", "  c1 INTEGER,",
                         "  d1 INTEGER,", "  e1 INTEGER,", "  x1 VARCHAR(30)", ")"]
    if source.startswith("sqlite-test/select"):
        line, ordinal = (1, 1) if source == "sqlite-test/select1.test" else (3, 2)
        return line, ordinal, 1, ["statement ok", "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)"]
    return 3, 2, 1, ["statement ok", "CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)"]


def write_sql_logic_capture(directory, command, repository, native_sha, revisions, script_count=None):
    from sql_logic_records import parse_record
    from sql_logic_results import compare
    from sql_logic_selections import NON_DIRECT, selections

    reference, original_output, native = (PurePosixPath(command[position]) for position in (4, 6, 8))
    selection = command[10]
    pinned = revisions["reference_revision"]
    run = directory / "run"
    plan = run / "plan"
    plan.mkdir(parents=True)
    inventory = Path(__file__).resolve().parent.parent / "conformance/source-inventory.json"
    corpus = {item["path"]: item["sha256"] for item in json.loads(inventory.read_bytes())["files"]
              if item["path"].startswith(("sqlite-test/", "src/test/resources/xtdb/sql/logic_test/")) and item["path"].endswith(".test")}
    roster = selections([selection])
    selected_paths = {source for _, source, _, _ in roster}
    records_paths, scripts, raw_observations = [], [], []
    for position, (name, source, direct, decorrelate) in enumerate(roster[:script_count]):
        options = {"direct_sql": direct, "decorrelate": decorrelate, "query_limit": None,
                   "max_rows": 1_000_000, "max_result_bytes": 128 * 1024 * 1024}
        line, ordinal, assertion_ordinal, lines = sql_logic_fixture_record(source)
        original = {"ordinal": ordinal, "assertion_ordinal": assertion_ordinal, "line": line}
        identity = {**original, "reference": pinned, "source": source, "source_sha256": corpus[source],
                    "selection": name, "options": options}
        record = parse_record(lines)
        records = write_json_line(plan / f"records-{position:05}.jsonl", {**original, "lines": lines,
                                  "identity": identity, "parse_error": None, "record": record})
        records_paths.append(records)
        scripts.append({"source": source, "source_sha256": corpus[source], "records": 1, "assertions": 1, "parse_errors": 0,
                        "records_file": records.name, "records_sha256": sha256(records), "selection": name, "options": options})
        query = record["type"] == "query"
        expected = record.get("expected")
        raw_observations.append({"identity": identity, "executed": True, "engine_executed": True,
                                 "result": {**expected, "md5": hashlib.md5(expected["text"].encode()).hexdigest()} if query else None,
                                 "disposition": "query_ok" if query else "statement_ok", "query_count_before": 0,
                                 "query_count_after": int(query), "error": None})

    plan_data = {"schema_version": 1, "reference": pinned, "selection": selection, "scripts": scripts}
    plan_path = write_record(plan / "plan.json", plan_data)
    selected = write_record(run / "selection.json", {"reference": pinned, "names": [selection], "query_limit": None,
                            "original_task_allowances": NON_DIRECT, "allowances_grant_passing_records": False,
                            "sources": [{"path": name, "sha256": digest, "selected": name in selected_paths,
                                         "reason": "selected" if name in selected_paths else "outside_explicit_original_selection"}
                                        for name, digest in corpus.items()]})
    inputs = write_record(run / "inputs.json", {**revisions, "inputs": [{"path": str(native), "sha256": native_sha}, *(
        {"path": str(original_output / path.relative_to(run).as_posix()), "sha256": sha256(path)}
        for path in (plan_path, selected, *records_paths))]})

    adapter = repository / "rust/conformance"
    observations = {}
    for side, engine in (("reference", "jvm"), ("reference-repeat", "jvm"), ("native", "rust")):
        observed = run / side / "observations"
        observed.mkdir(parents=True)
        output = original_output / side / "observations"
        if engine == "rust":
            argv = [native, "sql-logic", original_output / "plan/plan.json", output]
            cwd, revision = repository, revisions["implementation_revision"]
        else:
            argv = [reference / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk", "--init-script",
                    adapter / "reference.init.gradle", "rtdbConformanceSqlLogic", f"-Prtdb.adapterRoot={adapter / 'jvm'}",
                    f"-Prtdb.fixture={original_output / 'plan/plan.json'}", f"-Prtdb.output={output}"]
            cwd, revision = reference, pinned
        write_record(observed.parent / "process.json", {"command": [str(value) for value in argv], "cwd": str(cwd),
                                                       "completed": True, "exit_code": 0})
        rows = [write_json_line(observed / f"script-{position:05}.jsonl", raw)
                for position, raw in enumerate(raw_observations)]
        observations[side] = rows
        execution = {"schema_version": 1, "engine": engine, "selection": selection, "plan_sha256": sha256(plan_path),
                     "artifact_revision": revision, "execution_complete": True, "complete_parity": False,
                     "scripts": [{"source": script["source"], "selection": script["selection"], "execution_complete": True,
                                  "observations": {"path": observation.name, "sha256": sha256(observation)}}
                                 for script, observation in zip(scripts, rows)]}
        if engine == "rust":
            execution.update(source_dirty=False, binary_sha256=native_sha)
            write_record(observed.parent / "stdout.log", execution)
        else:
            runtime = [{"path": str(reference / "jdk/lib/modules"), "kind": "file", "sha256": "c" * 64, "bytes": 32}]
            runtime_path = write_record(observed / "runtime-artifacts.json", runtime)
            execution.update(artifacts=runtime, binary_sha256=sha256(runtime_path))
            (observed.parent / "stdout.log").write_text(side)
        write_record(observed / "execution.json", execution)
        (observed.parent / "stderr.log").write_text("")
        write_sql_logic_trace(observed.parent, revision)

    result = {"selection_passed": True, "inputs_sha256": sha256(inputs),
              "complete_parity": False, "assertion_ledger_reconciled": False}
    for name, side in (("comparison", "native"), ("repeat", "reference-repeat")):
        report = compare(plan_data, plan, observations["reference"], observations[side], run / f"{name}-records.jsonl")
        summary = write_record(run / f"{name}.json", report)
        result[f"{name}_sha256"] = sha256(summary)
    write_record(run / "result.json", result)


def sql_logic_fixture(root):
    from run_controls import REFERENCE

    matrix_path = Path(__file__).with_name("matrix.json")
    matrix = json.loads(matrix_path.read_bytes())
    lanes = expand(matrix)
    prefix = "linux-arm64/rust/local"
    production, lane = (next(item for item in lanes if item["id"] == f"{prefix}/{name}") for name in ("production", "slt-test"))
    revisions = {"reference_revision": REFERENCE, "implementation_revision": "b" * 40}
    write_record(root / "inputs.json", revisions)
    directory = root / "production"
    directory.mkdir()
    write_record(directory / "artifact-bindings.json", [])
    native = directory / "rtdb-conformance"
    native.write_bytes(b"selected production executable bytes")
    compiler_artifact = {"reason": "compiler-artifact", "target": {"name": "rtdb-conformance"}, "profile": {"test": False}}
    cargo = ["cargo", "build"]
    process = write_record(directory / "cargo.process.json", {"command": cargo, "completed": True, "exit_code": 0})
    messages = directory / "cargo.stdout.log"
    messages.write_text("\n".join(map(json.dumps, (compiler_artifact, {"reason": "build-finished", "success": True}))) + "\n")
    build = write_record(directory / "production-artifacts.json", {
        "configuration": production["id"], "selection": production["selection"], "implementation_revision": revisions["implementation_revision"],
        "command": cargo, "process": retained(directory, process), "messages": retained(directory, messages),
        "binaries": [{"compiler_artifact": compiler_artifact, "retained_artifact": retained(directory, native), "sha256": sha256(native)}]})
    production_closure = write_record(directory / "closure.json", closure(directory))
    production_receipt = write_record(directory / "receipt.json", {**production, "verified": True, "process_joined": True,
                                      "artifacts": [retained(root, build)], "closure": retained(root, production_closure)})
    return matrix, matrix_path, lanes, production, lane, revisions, native, production_receipt


def offline_sql_logic_fixture(root, script_count=None):
    matrix, matrix_path, lanes, production, lane, revisions, native, production_receipt = sql_logic_fixture(root)
    directory = root / "sql-logic"
    directory.mkdir()
    bindings = bind_inputs(lane, directory, root, {"native": native}.__getitem__, lambda _: (production, native.parent))
    original = PurePosixPath("/original/sql-logic-worker")
    repository = original / "implementation"
    original_native = original / native.relative_to(root).as_posix()
    bindings[0]["consumed_path"] = str(original_native)
    write_record(directory / "artifact-bindings.json", bindings)
    command = sql_logic_command("python3", repository, original / "reference", original / "sql-logic/run", original_native, lane["selection"])
    write_record(directory / "sql-logic.process.json", {"command": command, "cwd": str(repository), "completed": True, "exit_code": 0})
    for name in ("stdout", "stderr"):
        (directory / f"sql-logic.{name}.log").touch()
    write_sql_logic_capture(directory, command, repository, sha256(native), revisions, script_count)
    selection = write_record(root / "selection.json", lanes)
    matrix_sha, plan_sha = sha256(matrix_path), "a" * 64

    def verify_worker(exclude=()):
        files = [item for item in closure(directory) if item["path"] not in {"closure.json", "receipt.json", *exclude}]
        lane_closure = write_record(directory / "closure.json", files)
        receipt = write_record(directory / "receipt.json", {**lane, "verified": True, "process_joined": True,
                               "artifacts": [retained(root, directory / "run/result.json")], "closure": retained(root, lane_closure)})
        write_record(root / "result.json", {**revisions, "plan_sha256": plan_sha, "matrix_sha256": matrix_sha,
                     "selection_sha256": sha256(selection), "inputs_sha256": sha256(root / "inputs.json"), "worker_platform": lane["platform"],
                     "receipt_artifacts": [retained(root, path) for path in (production_receipt, receipt)]})
        return verify(matrix, [root], plan_sha, revisions["implementation_revision"], revisions["reference_revision"], matrix_sha)

    return lane, directory / "run", revisions, verify_worker


def assertion_comparison_fixture(root):
    fixture_directory = Path(__file__).resolve().parent.parent / "conformance"
    committed_ledger = json.loads((fixture_directory / "assertion-ledger.json").read_bytes())
    case = next(case for case in committed_ledger["cases"]
                if case["identity"]["parameter_fixture"] == "rust/conformance/fixtures/temporal-ceilings.json")
    reference_revision = committed_ledger["inventory_revision"]
    implementation_revision = "b" * 40
    fixture_path = fixture_directory / "fixtures/temporal-ceilings.json"
    configuration_path = fixture_directory / "configurations/controls.json"
    original_fixture = json.loads(fixture_path.read_bytes())
    configuration = json.loads(configuration_path.read_bytes())
    del configuration["schema_version"]
    observations = [{"id": item["id"], "executed": True, "outcome": {
        "kind": "returned", "typed_value": original_fixture["expected_observations"][item["id"]]}}
        for item in case["observations"]]
    write_record(root / "inputs.json", {"reference_revision": reference_revision, "implementation_revision": implementation_revision})

    production = root / "production"
    inventory = root / "inventory"
    directory = root / "comparison"
    for child in (production, inventory, directory):
        child.mkdir()
    native = production / "rtdb-conformance"
    native.write_bytes(b"retained comparator artifact")
    prefix = "linux-arm64/rust/local"
    write_record(production / "production-artifacts.json", {"configuration": prefix + "/production", "selection": {},
                 "implementation_revision": implementation_revision, "binaries": [{
                     "compiler_artifact": {"target": {"name": "rtdb-conformance"}, "profile": {"test": False}},
                     "retained_artifact": retained(production, native)}]})
    ledger = write_record(inventory / "ledger.json", {**committed_ledger, "inventory_complete": True,
                          "runtime_reconciled": True, "cases": [case]})
    write_record(inventory / "input-artifacts.json", [{"input": "ledger", **retained(inventory, ledger)}])
    inventory_report = write_record(inventory / "inventory.json", {"schema_version": 1, "reference_revision": reference_revision,
                                    "assertion_denominator": 1, "complete": True, "all_assertions_passed": True})
    original_worker = Path("/original/arm64-worker")
    original_inventory = original_worker / inventory.name
    write_record(inventory / "inventory.process.json", {
        "command": ["python", "runtime_inventory.py", "--ledger", str(original_inventory / ledger.name),
                    "--output", str(original_inventory / inventory_report.name)],
        "cwd": str(original_inventory), "completed": True, "exit_code": 0})

    paths = {"native": native, "inventory": inventory_report, "ledger": ledger}
    for side, revision in (("reference", reference_revision), ("native", implementation_revision)):
        role = side + "_receipts"
        context = {**configuration, "artifact_revision": revision, "binary_sha256": sha256(native),
                   "fixture_sha256": sha256(fixture_path), "configuration_sha256": sha256(configuration_path)}
        paths[role] = write_record(root / (role + ".json"), [{"identity": case["identity"],
                                   "runner": f"rtdb-controls/{side}/1", "trace_sha256": sha256(fixture_path),
                                   "context": context, "observations": observations}])
    for role, filename in ASSERTION_COMPARISON_INPUTS:
        (directory / filename).write_bytes(paths[role].read_bytes())
    write_record(directory / "input-artifacts.json", [{"input": role, **retained(root, paths[role])}
                                                       for role, _ in ASSERTION_COMPARISON_INPUTS])

    selected = {"platform": "linux/arm64", "selection": {}}
    predecessors = {identity: ({**selected, "id": identity}, producer)
                    for identity, producer in ((prefix + "/production", production), (prefix + "/inventory", inventory))}
    lane = {**selected, "id": prefix + "/assertion-comparison", "runner": "assertion-comparison",
            "selection": {"variant": "local", "features": []}, "dependencies": list(predecessors)}
    bindings = bind_inputs(lane, directory, root, paths.__getitem__, predecessors.__getitem__)
    native_binding = next(binding for binding in bindings if binding["role"] == "native")
    original_native = original_worker / native.relative_to(root)
    native_binding["consumed_path"] = str(original_native)
    write_record(directory / "artifact-bindings.json", bindings)
    original_directory = original_worker / directory.name
    process = {"command": assertion_comparison_command(original_native, original_directory),
               "cwd": str(original_directory), "completed": True, "exit_code": 0}
    report = {"complete_parity": True, "comparator_version": "1", "required_groups": 1, "passing_both": 1,
              "matching_expected_failure": 0, "missing_evidence": 0, "mismatches": 0, "strict_agreement": 1.0,
              "manifest_errors": [], "cases": [{"identity": case["identity"], "disposition": "passing_both",
                                               "comparisons": len(observations), "differences": []}]}
    write_record(directory / "assertion-comparison.process.json", process)
    write_record(directory / "assertion-comparison.stdout.log", report)
    (directory / "assertion-comparison.stderr.log").write_text("")
    return lane, directory, paths, predecessors, process, report


class RetainedLaneArtifacts(unittest.TestCase):
    def test_gradle_summary_cannot_promote_a_failed_recorded_assertion(self):
        from runtime_inventory import read_plan
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / "run"
            run.mkdir()
            descriptor = {"unique_id": "case", "is_test": True, "parent_id": None}
            events = [{"event": "plan_started", "project": ":", "task": ":test"},
                      {"event": "discovered", "descriptor": descriptor}, {"event": "execution_started", "descriptor": descriptor},
                      {"event": "execution_finished", "descriptor": descriptor, "status": "FAILED"}, {"event": "plan_finished"}]
            path = run / "events.jsonl"
            path.write_text("".join(json.dumps({**event, "sequence": position}) + "\n" for position, event in enumerate(events)))
            cases = read_plan(path)["cases"]
            cases[0]["outcome"]["status"] = "SUCCESSFUL"
            (root / "reference-results.json").write_text(json.dumps({"selection": {}, "cases": cases}))
            with self.assertRaises(ValueError):
                verify_lane_artifacts(root, {"runner": "gradle", "selection": {}})

    def test_native_summary_cannot_promote_a_failed_raw_framework_result(self):
        from client_results import parse_libtest
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "run").mkdir()
            binary = root / "test-binary"
            binary.write_bytes(b"test-executable")
            binary_reference = retained(root, binary)
            original = {"artifact": {"executable": "/compiled/test-binary", "target": {"name": "suite", "src_path": "test.rs"}},
                        "binary_sha256": binary_reference["sha256"]}
            inventory = {"configuration": "local", "binaries": [original], "tests": [{"executable": "/compiled/test-binary", "name": "case", "counterpart": "native::case"}]}
            inventory_path = root / "run/native-inventory.json"
            inventory_path.write_text(json.dumps(inventory))
            raw = b"running 1 test\ntest case ... FAILED\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n"
            (root / "tests-0000.stdout.log").write_bytes(raw)
            process = {"exit_code": 0, "command": [str(binary), "--include-ignored", "--format=pretty", "--color=never"]}
            (root / "tests-0000.process.json").write_text(json.dumps(process))
            cases = parse_libtest(raw, "suite")
            cases[0].update(outcome="passed", source={"file": "test.rs"}, counterpart="native::case", configuration="local")
            result = {"execution_root": str(root), "cases": cases, "inventory_sha256": retained(root, inventory_path)["sha256"],
                      "retained_binaries": [{**original, "retained_executable": binary_reference}]}
            (root / "native-results.json").write_text(json.dumps(result))
            with self.assertRaises(ValueError):
                verify_lane_artifacts(root, {"runner": "native-tests"})

    def test_embedded_invocation_keeps_library_and_report_digests_distinct(self):
        from run_matrix import Run
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            library = root / "library.so"
            library.write_bytes(b"actual-native-library")
            reports = root / "bindings/jvm/build/test-results/test"
            reports.mkdir(parents=True)
            (reports / "TEST-lifetime.xml").write_text('<testsuite tests="1"><testcase classname="Bindings" name="lifetime"/></testsuite>')
            lane = {"id": "embedded", "runner": "embedded", "selection": {}, "dependencies": []}
            args = types.SimpleNamespace(plan=root / "plan.json", output=root, reference=root, timeout=10)
            run = Run(args, {}, [lane], {})
            output = root / "retained"
            output.mkdir()
            binding = {"inputs": {"native_library": retained(root, library)}}
            with patch("run_matrix.REPOSITORY", root), patch("run_matrix.bind_inputs", return_value=[]), patch.object(run, "command"):
                run.invoke(lane, binding, output)
            verify_lane_artifacts(output, {"runner": "embedded"})
            result = json.loads((output / "embedded-results.json").read_text())
            self.assertNotEqual(result["library"]["sha256"], result["reports"][0]["sha256"])

    def test_embedded_summary_must_retain_and_match_actual_framework_xml(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            library = root / "library.so"
            library.write_bytes(b"native-library")
            report = root / "report.xml"
            report.write_text('<testsuite tests="1"><testcase classname="Bindings" name="lifetime"/></testsuite>')
            from client_results import parse_junit
            result = {"library": retained(root, library), "reports": [retained(root, report)], "cases": parse_junit(report.read_bytes())}
            summary = root / "embedded-results.json"
            summary.write_text(json.dumps(result))
            verify_lane_artifacts(root, {"runner": "embedded"})
            report.unlink()
            with self.assertRaises(ValueError):
                verify_lane_artifacts(root, {"runner": "embedded"})

    def test_missing_production_binary_bytes_cannot_be_replaced_by_digest_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            messages = root / "cargo.log"
            compiler = {"reason": "compiler-artifact", "profile": {"test": False}}
            messages.write_text(json.dumps(compiler) + '\n{"reason":"build-finished","success":true}\n')
            process = root / "process.json"
            process.write_text('{"command":["cargo","build"],"exit_code":0}')
            result = {"selection": {}, "configuration": "production", "command": ["cargo", "build"],
                "messages": retained(root, messages), "process": retained(root, process), "binaries": [{"compiler_artifact": compiler,
                "sha256": "a" * 64, "retained_artifact": {"path": "missing-binary", "sha256": "a" * 64}}]}
            (root / "production-artifacts.json").write_text(json.dumps(result))
            with self.assertRaises(ValueError):
                verify_lane_artifacts(root, {"runner": "cargo-production", "id": "production", "selection": {}})

    def test_inventory_success_cannot_bypass_the_exact_retained_ledger(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            _, _, paths, _, _, _ = assertion_comparison_fixture(root)
            directory = paths["inventory"].parent
            verify_lane_artifacts(directory, {"runner": "inventory"})
            process_path = directory / "inventory.process.json"
            process = json.loads(process_path.read_bytes())
            process["command"][3] = "/original/arm64-worker/inventory/other-ledger.json"
            write_record(process_path, process)
            with self.assertRaisesRegex(ValueError, "retained assertion ledger"):
                verify_lane_artifacts(directory, {"runner": "inventory"})

    def test_offline_comparison_requires_the_original_ordered_operands_and_complete_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            lane, directory, _, predecessors, process, report = assertion_comparison_fixture(root)
            verify_lane_artifacts(directory, lane)
            verify_bindings(lane, directory, root, predecessors.__getitem__)

            command = process["command"]
            process_path = directory / "assertion-comparison.process.json"
            write_record(process_path, {**process, "command": [*command[:3], command[4], command[3]]})
            with self.assertRaisesRegex(ValueError, "selected executable and operands"):
                verify_bindings(lane, directory, root, predecessors.__getitem__)
            write_record(process_path, process)

            report_path = directory / "assertion-comparison.stdout.log"
            write_record(report_path, {**report, "required_groups": 2})
            with self.assertRaisesRegex(ValueError, "summary differs"):
                verify_bindings(lane, directory, root, predecessors.__getitem__)
            write_record(report_path, report)

            native_receipts = directory / "native-receipts.json"
            original_receipts = native_receipts.read_bytes()
            native_receipts.write_bytes(original_receipts + b" ")
            with self.assertRaisesRegex(ValueError, "differs from its retained digest"):
                verify_bindings(lane, directory, root, predecessors.__getitem__)

            receipts = json.loads(original_receipts)
            receipts[0]["context"]["artifact_revision"] = "c" * 40
            write_record(native_receipts, receipts)
            manifest = directory / "input-artifacts.json"
            inputs = [item for item in json.loads(manifest.read_bytes()) if item["input"] != "native_receipts"]
            write_record(manifest, [*inputs, {"input": "native_receipts", **retained(directory, native_receipts)}])
            with self.assertRaisesRegex(ValueError, "artifact revision"):
                verify_bindings(lane, directory, root, predecessors.__getitem__)

            receipts = json.loads(original_receipts)
            receipts[0]["context"]["binary_sha256"] = "c" * 64
            write_record(native_receipts, receipts)
            write_record(manifest, [*inputs, {"input": "native_receipts", **retained(directory, native_receipts)}])
            with self.assertRaisesRegex(ValueError, "another production executable"):
                verify_bindings(lane, directory, root, predecessors.__getitem__)

    def test_comparison_exit_one_retains_its_report_and_leaves_the_lane_unmet(self):
        from run_matrix import Run
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            lane, _, paths, predecessors, _, report = assertion_comparison_fixture(root)
            args = types.SimpleNamespace(output=root, plan=root / "plan.json", reference=root, timeout=10)
            prefix = "linux-arm64/rust/local"
            dependency_lanes = [producer for producer, _ in predecessors.values()]
            run = Run(args, {}, [*dependency_lanes, lane], {})
            for identity, (_, directory) in predecessors.items():
                run.completed[identity] = {"verified": True}
                run.directories[identity] = directory
            directory = run.directories[lane["id"]]
            directory.mkdir()
            comparison = {**report, "complete_parity": False, "strict_agreement": 0.0, "passing_both": 0,
                          "matched_declared_migration": 1, "cases": [
                              {**report["cases"][0], "disposition": "matched_declared_migration"}]}

            def comparator(command, cwd, output, log, timeout):
                self.assertEqual(command, assertion_comparison_command(paths["native"], directory))
                self.assertEqual(cwd, directory)
                write_record(output, comparison)
                log.write_text("")
                return {"command": [str(argument) for argument in command], "cwd": str(cwd),
                        "completed": True, "exit_code": 1}

            binding = {"inputs": {role: retained(root, path) for role, path in paths.items()}}
            with patch("run_matrix.run_process", side_effect=comparator):
                with self.assertRaisesRegex(ValueError, "strict parity"):
                    run.invoke(lane, binding, directory)
            self.assertEqual(json.loads((directory / "assertion-comparison.stdout.log").read_bytes()), comparison)
            process = json.loads((directory / "assertion-comparison.process.json").read_bytes())
            self.assertEqual(process["exit_code"], 1)
            self.assertEqual(set(run.completed), {prefix + "/production", prefix + "/inventory"})

    def test_comparison_receipts_must_be_in_the_verified_worker_closure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            lane, directory, _, _, _, _ = assertion_comparison_fixture(root)
            matrix_path = Path(__file__).with_name("matrix.json")
            matrix = json.loads(matrix_path.read_bytes())
            selection = write_record(root / "selection.json", expand(matrix))
            revisions = json.loads((root / "inputs.json").read_bytes())

            retained_files = [item for item in closure(directory) if item["path"] != "native-receipts.json"]
            closure_file = write_record(directory / "closure.json", retained_files)
            receipt = write_record(directory / "receipt.json", {**lane, "verified": True, "process_joined": True,
                                   "artifacts": [retained(root, directory / "assertion-comparison.stdout.log")],
                                   "closure": retained(root, closure_file)})
            matrix_sha256, plan_sha256 = sha256(matrix_path), "a" * 64
            write_record(root / "result.json", {**revisions, "plan_sha256": plan_sha256, "matrix_sha256": matrix_sha256,
                         "selection_sha256": sha256(selection), "inputs_sha256": sha256(root / "inputs.json"),
                         "worker_platform": lane["platform"], "receipt_artifacts": [retained(root, receipt)]})
            with self.assertRaisesRegex(ValueError, "absent from the retained lane closure"):
                verify(matrix, [root], plan_sha256, revisions["implementation_revision"],
                       revisions["reference_revision"], matrix_sha256)


    def test_sql_logic_provenance_survives_worker_relocation(self):
        with tempfile.TemporaryDirectory() as temporary:
            lane, run, _, verify_worker = offline_sql_logic_fixture(Path(temporary).resolve())
            plan = json.loads((run / "plan/plan.json").read_bytes())
            self.assertEqual(len(plan["scripts"]), 9)
            original_comparisons = closure(run)
            with patch.dict("os.environ", {"TMPDIR": str(run.parent.parent)}):
                report = verify_worker()
            self.assertEqual(closure(run), original_comparisons)
            self.assertIn(lane["id"], report["verified_lanes"])
            self.assertFalse(report["release_matrix_verified"])

    def test_sql_logic_rejects_an_incomplete_script_roster_with_a_passing_subset(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, run, _, verify_worker = offline_sql_logic_fixture(Path(temporary).resolve(), script_count=1)
            self.assertTrue(json.loads((run / "comparison.json").read_bytes())["selection_passed"])
            with self.assertRaisesRegex(ValueError, "pinned ordered script selection"):
                verify_worker()

    def test_sql_logic_reruns_both_comparisons_on_digest_consistent_raw_evidence(self):
        for side, comparison in (("native", "comparison"), ("reference-repeat", "repeat")):
            with self.subTest(side=side), tempfile.TemporaryDirectory() as temporary:
                _, run, revisions, verify_worker = offline_sql_logic_fixture(Path(temporary).resolve())
                executed = run / side
                raw = executed / "observations/script-00000.jsonl"
                if side == "native":
                    raw.write_bytes(b"")
                    revision = revisions["implementation_revision"]
                else:
                    observation = json.loads(raw.read_bytes())
                    different_text = "different\n"
                    different = {"text": different_text, "count": 1, "md5": hashlib.md5(different_text.encode()).hexdigest()}
                    write_json_line(raw, {**observation, "disposition": "query_mismatch", "result": different})
                    revision = revisions["reference_revision"]
                execution_path = executed / "observations/execution.json"
                execution = json.loads(execution_path.read_bytes())
                execution["scripts"][0]["observations"]["sha256"] = sha256(raw)
                write_record(execution_path, execution)
                if side == "native":
                    write_record(executed / "stdout.log", execution)
                write_sql_logic_trace(executed, revision)
                with self.assertRaisesRegex(ValueError, f"{comparison} differs from the comparison of retained observations"):
                    verify_worker()

    def test_sql_logic_rejects_a_same_revision_execution_from_different_binary_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, run, revisions, verify_worker = offline_sql_logic_fixture(Path(temporary).resolve())
            side = run / "native"
            path = side / "observations/execution.json"
            execution = json.loads(path.read_bytes())
            execution["binary_sha256"] = hashlib.sha256(b"another executable from the same revision").hexdigest()
            write_record(path, execution)
            write_record(side / "stdout.log", execution)
            write_sql_logic_trace(side, revisions["implementation_revision"])
            with self.assertRaisesRegex(ValueError, "differs from the selected production executable"):
                verify_worker()

    def test_sql_logic_rejects_an_inner_command_using_a_different_executable(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, run, revisions, verify_worker = offline_sql_logic_fixture(Path(temporary).resolve())
            side = run / "native"
            path = side / "process.json"
            process = json.loads(path.read_bytes())
            process["command"][0] += ".other"
            write_record(path, process)
            write_sql_logic_trace(side, revisions["implementation_revision"])
            with self.assertRaisesRegex(ValueError, "native invocation differs from the selected execution"):
                verify_worker()

    def test_sql_logic_reference_trace_cannot_be_replaced_with_its_repeat(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, run, _, verify_worker = offline_sql_logic_fixture(Path(temporary).resolve())
            repeat = run / "reference-repeat"
            for path in repeat.rglob("*"):
                if path.is_file():
                    (run / "reference" / path.relative_to(repeat)).write_bytes(path.read_bytes())
            with self.assertRaisesRegex(ValueError, "reference invocation differs from the selected execution"):
                verify_worker()

    def test_sql_logic_checks_raw_files_and_trace_digests_after_worker_closure_validation(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, run, _, verify_worker = offline_sql_logic_fixture(Path(temporary).resolve())
            side = run / "reference"
            stderr = side / "stderr.log"
            original_stderr = stderr.read_bytes()
            stderr.write_bytes(b"different reference diagnostics")
            with self.assertRaisesRegex(ValueError, "reference trace differs from retained execution files"):
                verify_worker()
            stderr.write_bytes(original_stderr)

            observations = side / "observations/script-00000.jsonl"
            original_observations = observations.read_bytes()
            observations.write_bytes(original_observations + b"\n")
            with self.assertRaisesRegex(ValueError, "observations differ from retained digest"):
                verify_worker()
            observations.write_bytes(original_observations)
            with self.assertRaisesRegex(ValueError, "absent from the retained lane closure"):
                verify_worker(exclude=("run/reference/observations/script-00000.jsonl",))

    def test_sql_logic_rejects_another_plan_selection_and_a_failed_inner_owner(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, run, revisions, verify_worker = offline_sql_logic_fixture(Path(temporary).resolve())
            path = run / "plan/plan.json"
            selected_plan = path.read_bytes()
            plan = json.loads(selected_plan)
            write_record(path, {**plan, "selection": "slt-test-2"})
            with self.assertRaisesRegex(ValueError, "plan differs from its pinned lane selection"):
                verify_worker()
            path.write_bytes(selected_plan)

            side = run / "native"
            path = side / "process.json"
            process = json.loads(path.read_bytes())
            write_record(path, {**process, "exit_code": 1})
            write_sql_logic_trace(side, revisions["implementation_revision"])
            with self.assertRaisesRegex(ValueError, "execution owner did not join successfully"):
                verify_worker()

    def test_sql_logic_worker_checks_inner_provenance_before_accepting_the_lane(self):
        from run_matrix import Run

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            _, _, _, production, lane, revisions, native, _ = sql_logic_fixture(root)
            args = types.SimpleNamespace(output=root, plan=root / "plan.json", reference=root / "reference", timeout=10)
            worker = Run(args, {}, [production, lane], {})
            worker.completed[production["id"]] = {"verified": True}
            worker.directories[production["id"]] = native.parent
            binding = {"inputs": {"native": {"from_lane": production["id"], "path": native.name}}}

            def capture(command, cwd, output, log, timeout):
                write_sql_logic_capture(output.parent, command, cwd, sha256(native), revisions)
                if output.parent.name == "wrong-executable":
                    side = output.parent / "run/native"
                    path = side / "observations/execution.json"
                    execution = json.loads(path.read_bytes())
                    execution["binary_sha256"] = "c" * 64
                    write_record(path, execution)
                    write_record(side / "stdout.log", execution)
                    write_sql_logic_trace(side, revisions["implementation_revision"])
                output.touch()
                log.touch()
                return {"command": command, "cwd": str(cwd), "completed": True, "exit_code": 0}

            with patch("run_matrix.run_process", side_effect=capture):
                valid, wrong = root / "valid-executable", root / "wrong-executable"
                valid.mkdir()
                wrong.mkdir()
                self.assertEqual(worker.invoke(lane, binding, valid), [valid / "run/result.json"])
                with self.assertRaisesRegex(ValueError, "differs from the selected production executable"):
                    worker.invoke(lane, binding, wrong)


if __name__ == "__main__":
    unittest.main()
