import itertools
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "release"))
from evidence import artifact, canonical, index, read_json, sha256, text
from client_results import case_identity, parse_exunit, parse_junit, parse_libtest, parse_trx
from sql_logic_records import records
from sql_logic_results import PASSING, check_observation, check_selection, json_lines, verify_execution


SUPPORT_REFERENCE = "c84f9334eb060ceb92d28ecdd4a06e6b625932fd"
SUPPORT_SOURCES = {
    "lang/README.adoc", "lang/docker-compose.yml", "lang/yarn.lock",
    "lang/dotnet/.dockerignore", "lang/dotnet/.gitignore", "lang/dotnet/Dockerfile", "lang/dotnet/README.md",
    "lang/dotnet/XtdbTest.csproj", "lang/dotnet/packages.lock.json",
    "lang/elixr/.formatter.exs", "lang/elixr/.gitignore", "lang/elixr/Dockerfile", "lang/elixr/README.md",
    "lang/elixr/mix.exs", "lang/elixr/mix.lock", "lang/elixr/test/test_helper.exs",
    "lang/js/.gitignore", "lang/js/Dockerfile", "lang/js/README.md", "lang/js/package.json", "lang/js/tsconfig.json", "lang/js/yarn.lock",
    "lang/python/.gitignore", "lang/python/Dockerfile", "lang/python/README.md", "lang/python/poetry.lock", "lang/python/pyproject.toml",
    "lang/rust/.gitignore", "lang/rust/Cargo.toml", "lang/rust/Dockerfile", "lang/rust/src/lib.rs",
}


def identity_key(identity):
    return tuple(identity.get(key) for key in ("reference_revision", "source", "qualified_name", "parameter_fixture", "configuration"))


class Evidence:
    def __init__(self, parent, run):
        relative = Path(text(run.get("root"), "run root"))
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("external evidence root escapes its manifest")
        self.root = parent / relative
        cursor = parent
        for part in relative.parts:
            cursor /= part
            if cursor.is_symlink():
                raise ValueError("external evidence root traverses a symbolic link")
        self.paths = {entry["path"]: artifact(self.root, entry) for entry in run["artifacts"]}
        if len(self.paths) != len(run["artifacts"]):
            raise ValueError("external artifact closure contains duplicate paths")
        self.recorded_root = Path(text(run.get("recorded_root"), "original receipt root"))
        self.relocations = {value["original"]: value["artifact"] for value in run.get("relocations", [])}
        if len(self.relocations) != len(run.get("relocations", [])):
            raise ValueError("external artifact relocation repeats an original identity")

    def path(self, name):
        if name not in self.paths:
            raise ValueError("external receipt references an unretained artifact: " + name)
        return self.paths[name]

    def read(self, name):
        return read_json(self.path(name))

    def recorded(self, reference):
        path = Path(reference["path"])
        if str(path) in self.relocations:
            path = Path(self.relocations[str(path)])
        elif path.is_absolute():
            path = path.relative_to(self.recorded_root)
        name = str(path)
        selected = self.path(name)
        if sha256(selected) != reference["sha256"]:
            raise ValueError("recorded artifact digest differs from retained closure")
        if "bytes" in reference and selected.stat().st_size != reference["bytes"]:
            raise ValueError("recorded artifact size differs from retained closure")
        return selected


def joined(process):
    if process.get("completed") is not True or process.get("unexpected_live_descendants") or type(process.get("exit_code")) is not int:
        raise ValueError("external execution owner did not join successfully")


def native_build(evidence, run, implementation):
    reference = run["native_build"]
    receipt = evidence.recorded(reference)
    build = read_json(receipt)
    if build.get("implementation_revision") != implementation:
        raise ValueError("native external build belongs to another implementation revision")
    root = Path(reference["path"]).parent
    process = read_json(evidence.recorded({**build["process"], "path": str(root / build["process"]["path"])}))
    joined(process)
    if process["exit_code"] != 0 or process.get("command") != build.get("command"):
        raise ValueError("native build selection differs from successful owner command")
    messages = evidence.recorded({**build["messages"], "path": str(root / build["messages"]["path"])})
    records_ = list(json_lines(messages))
    if not records_ or records_[-1].get("reason") != "build-finished" or records_[-1].get("success") is not True or sum(value.get("reason") == "build-finished" for value in records_) != 1:
        raise ValueError("native external build lacks a unique successful terminal Cargo receipt")
    binary = evidence.recorded(run["native_binary"])
    selected = [value for value in build["binaries"] if value["path"] == run["native_binary"]["path"] and value["sha256"] == sha256(binary)]
    if len(selected) != 1 or selected[0]["compiler_artifact"] not in records_ or selected[0]["compiler_artifact"]["profile"]["test"]:
        raise ValueError("native external executable lacks its exact production compilation identity")
    text(build.get("configuration"), "native build configuration")
    return build


def sql_cases(evidence, run, revision, implementation):
    return list(iter_sql_cases(evidence, run, revision, implementation))


def iter_sql_cases(evidence, run, revision, implementation):
    """Yield original assertions with observations in reference, reference-repeat and native order.

    Consume the iterator through exhaustion to validate trailing records and assertion counts.
    Invalid or incomplete evidence raises an error during iteration.
    """
    build = native_build(evidence, run, implementation)
    plan = evidence.read("plan/plan.json")
    if plan.get("reference") != revision:
        raise ValueError("SQL logic plan uses another pinned revision")
    inputs = evidence.read("inputs.json")
    if inputs.get("reference_revision") != revision or inputs.get("implementation_revision") != implementation:
        raise ValueError("SQL logic source provenance differs")
    observations = {}
    for side, engine, expected_revision in (("reference", "jvm", revision), ("reference-repeat", "jvm", revision), ("native", "rust", implementation)):
        prefix = side + "/"
        trace = evidence.read(prefix + "trace.json")
        for name in ("process", "execution", "stdout", "stderr"):
            path = prefix + ("observations/execution.json" if name == "execution" else name + (".json" if name == "process" else ".log"))
            if trace.get(name + "_sha256") != sha256(evidence.path(path)):
                raise ValueError("SQL logic owner trace digest differs")
        process = evidence.read(prefix + "process.json")
        joined(process)
        if process["exit_code"] != 0:
            raise ValueError("SQL logic execution owner failed")
        execution = evidence.read(prefix + "observations/execution.json")
        if execution.get("engine") != engine or execution.get("artifact_revision") != expected_revision:
            raise ValueError("SQL logic executed artifact identity differs")
        if engine == "rust":
            if execution.get("source_dirty") is not False:
                raise ValueError("native SQL logic artifact came from dirty sources")
            if sha256(evidence.recorded(run["native_binary"])) != execution.get("binary_sha256"):
                raise ValueError("native SQL logic binary differs from execution")
            if evidence.read(prefix + "stdout.log") != execution:
                raise ValueError("native SQL logic stdout differs from its execution receipt")
        else:
            closure = evidence.path(prefix + "observations/runtime-artifacts.json")
            if sha256(closure) != execution.get("binary_sha256") or read_json(closure) != execution.get("artifacts"):
                raise ValueError("JVM SQL logic runtime artifact closure differs")
            if not execution["artifacts"]:
                raise ValueError("JVM SQL logic runtime artifact closure is empty")
            for entry in execution["artifacts"]:
                if entry["kind"] == "file":
                    evidence.recorded(entry)
                elif entry["kind"] == "directory" and isinstance(entry.get("files"), list):
                    if len({file["path"] for file in entry["files"]}) != len(entry["files"]):
                        raise ValueError("JVM runtime directory repeats a file identity")
                    for file in entry["files"]:
                        evidence.recorded({**file, "path": str(Path(entry["path"]) / file["path"])})
                else:
                    raise ValueError("JVM SQL logic runtime artifact has no retained bytes")
        files = verify_execution(plan, sha256(evidence.path("plan/plan.json")), execution, evidence.root / side / "observations")
        for path in files:
            evidence.path(str(path.relative_to(evidence.root)))
        observations[side] = files
    sources = index(run["sources"], lambda value: value["source"], "SQL logic original source")
    for position, script in enumerate(plan["scripts"]):
        source = sources[script["source"]]
        original_path = evidence.recorded(source["artifact"])
        if sha256(original_path) != script["source_sha256"]:
            raise ValueError("original SQL logic source differs from selected plan")
        selected_path = evidence.path("plan/" + script["records_file"])
        if sha256(selected_path) != script["records_sha256"]:
            raise ValueError("original SQL logic record stream digest differs")
        result_limit = script.get("options", {}).get("max_result_bytes", 128 * 1024 * 1024)
        if type(result_limit) is not int or not 0 < result_limit <= 512 * 1024 * 1024:
            raise ValueError("SQL logic result byte limit differs from the bounded runner contract")
        streams = [json_lines(observations[side][position], 64 * 1024 * 1024 + 6 * result_limit) for side in observations]
        count = assertions = queries = 0
        with original_path.open(encoding="utf-8", newline="\n") as original:
            for raw, selected, *actual in itertools.zip_longest(records(original), json_lines(selected_path), *streams):
                if raw is None or selected is None or any(value is None for value in actual):
                    raise ValueError("external SQL logic evidence lost or added original records")
                expected_identity = {"reference": revision, "source": script["source"], "source_sha256": script["source_sha256"],
                    "selection": script["selection"], "options": script["options"], "ordinal": raw["ordinal"],
                    "assertion_ordinal": raw["assertion_ordinal"], "line": raw["line"]}
                if selected != {**raw, "identity": expected_identity}:
                    raise ValueError("SQL logic retained records differ from original source bytes")
                count += 1
                errors = [error for value in actual for error in (check_observation(selected, value), check_selection(selected, value, script, queries)) if error]
                dispositions = [value.get("disposition") for value in actual]
                passed = not errors and len(set(dispositions)) == 1 and dispositions[0] in PASSING
                if raw.get("record", {}) and raw["record"]["type"] == "query":
                    passed = passed and len({value["result"]["text"] for value in actual}) == 1
                if raw["assertion_ordinal"] is not None:
                    assertions += 1
                    yield {"runtime_identity": expected_identity, "source": script["source"], "passed": passed,
                           "native_build_configuration": build["configuration"], "native_build_selection": build["selection"],
                           "outcome": "expected_error_passed" if passed and dispositions[0] == "statement_expected_error" else "passed" if passed else "unverified",
                           "observations": actual, "errors": errors}
                limit = script.get("options", {}).get("query_limit")
                if not script.get("parse_errors", 0) and (limit is None or queries != limit) and (raw.get("record") or {}).get("type") == "query":
                    queries += 1
        if count != script["records"] or assertions != script["assertions"]:
            raise ValueError("SQL logic record/assertion denominator differs")


def client_cases(evidence, run, revision, implementation):
    from run_clients import LANES, validate_native_package_artifacts, validate_package
    comparison = evidence.read("comparison.json")
    if comparison.get("reference_revision") != revision or comparison.get("implementation_revision") != implementation or comparison.get("selection") != LANES:
        raise ValueError("client provenance or original selection differs")
    if comparison.get("source_manifest_sha256") != sha256(evidence.path("sources.json")):
        raise ValueError("original client source manifest differs")
    for source in evidence.read("sources.json"):
        if source["path"].startswith("lang/") and sha256(evidence.path("client-sources/" + source["path"])) != source["sha256"]:
            raise ValueError("original client source bytes differ")
    clients = evidence.read("client-images.json")
    all_cases = {}
    for side, engine, expected_revision in (("reference", "jvm", revision), ("native", "rust", implementation)):
        package = evidence.read(side + "-package.json")
        validate_package(package["image"], package["manifest"], expected_revision, engine)
        package_reference = run["packages"][side]
        package_path = evidence.recorded(package_reference)
        if sha256(package_path) != package["manifest_sha256"] or read_json(package_path) != package["manifest"]:
            raise ValueError("client package manifest differs from retained build evidence")
        package_root = Path(package_reference["path"]).parent
        evidence.recorded({"path": str(package_root / "source-manifest.json"), "sha256": package["manifest"]["source_manifest_sha256"]})
        retained = {item["path"]: evidence.recorded({**item, "path": str(package_root / item["path"])}) for item in package["manifest"]["artifacts"]}
        if engine == "rust":
            validate_native_package_artifacts(retained, package["manifest"])
        running = side + ("/running-build.json" if engine == "rust" else "/running-xtdb.jar")
        artifact_name = "artifacts/build.json" if engine == "rust" else "artifacts/xtdb.jar"
        claimed = next(entry for entry in package["manifest"]["artifacts"] if entry["path"] == artifact_name)
        if sha256(evidence.path(running)) != claimed["sha256"]:
            raise ValueError("running packaged server differs from selected artifact")
        lifecycle = evidence.read(side + "/lifecycle.json")
        if lifecycle.get("drained") is not True or lifecycle.get("cleanup_errors") or lifecycle.get("primary_error"):
            raise ValueError("client container lifecycle did not drain")
        lanes = index(evidence.read(side + "/results.json"), lambda value: value["lane"], "client lane")
        if set(lanes) != {lane["lane"] for lane in LANES}:
            raise ValueError("external client evidence omits an original lane")
        collected = {}
        for selected in LANES:
            lane = lanes[selected["lane"]]
            joined(lane["process"])
            if lane.get("report_error"):
                raise ValueError("client framework report was not decoded")
            inspection = read_json(evidence.recorded(lane["container"]))
            if len(inspection) != 1 or inspection[0].get("Image") != clients[selected["service"]]["Id"] or lane.get("client_image_id") != inspection[0]["Image"]:
                raise ValueError("client invocation does not identify its actual immutable image")
            if inspection[0].get("Config", {}).get("Labels", {}).get("com.docker.compose.project") != lifecycle["project"]:
                raise ValueError("client invocation belongs to another lifecycle owner")
            raw = evidence.recorded(lane["report"]).read_bytes()
            cases = parse_libtest(raw, selected["target"]) if selected["parser"] == "libtest" else {"junit": parse_junit, "trx": parse_trx, "exunit": parse_exunit}[selected["parser"]](raw)
            if cases != lane["cases"]:
                raise ValueError("stored client cases differ from actual framework report")
            for case in cases:
                identity = canonical({"lane": selected["lane"], "case": json.loads(case_identity(case))})
                if identity in collected:
                    raise ValueError("external client invocation identity repeats")
                collected[identity] = {"case": case, "configuration": lane["configuration"], "owner_exit_code": lane["process"]["exit_code"]}
        all_cases[side] = collected
    if set(all_cases["reference"]) != set(all_cases["native"]):
        raise ValueError("client engines did not execute the same original invocation identities")
    return [{"runtime_identity": json.loads(identity), "source": None,
             "passed": left["case"]["outcome"] == right["case"]["outcome"] == "passed" and left["owner_exit_code"] == right["owner_exit_code"] == 0,
             "outcome": "passed" if left["case"]["outcome"] == right["case"]["outcome"] == "passed" and left["owner_exit_code"] == right["owner_exit_code"] == 0 else "unverified",
             "observations": [left, right]}
            for identity, left in all_cases["reference"].items() for right in [all_cases["native"][identity]]]


def reconcile_external(document, parent, source, ledger):
    """Reconcile version-1 adapter evidence against exact source and ledger identities.

    - Inputs: document follows external-inventory.adoc; parent is the root for its retained artifact paths.
      Source is the source inventory object and ledger is the counterpart binding object consumed by runtime_inventory.py.
      Inputs are read only and no test is executed.
    - Result: return an object with groups, cases, resolved_files and all_passed.
      Groups is a set of ledger identity tuples and cases is an ordered list retaining original parameter/configuration identities.
      Resolved_files is a set of source paths with complete recorded denominators.
      All_passed is false for any nonpassing original assertion.
    - Failure: evidence validation raises ValueError; malformed schema objects propagate decoding, type or lookup errors, including KeyError and TypeError.
      Filesystem failures propagate as OSError.
    """
    if document.get("schema_version") != 1 or document.get("reference_revision") != source["reference"]:
        raise ValueError("external inventory belongs to another schema or reference")
    implementation = text(document.get("implementation_revision"), "native implementation revision")
    mappings = {identity_key(case["identity"]): case for case in ledger["cases"]}
    originals = {entry["path"]: entry for entry in source["files"]}
    candidates = {entry["id"]: entry for entry in source["candidates"]}
    groups, observed, file_runs = set(), [], {}
    runs = index(document.get("runs", []), lambda value: text(value.get("id"), "external run ID"), "external run") if document.get("runs") else {}
    for run in runs.values():
        evidence = Evidence(parent, run)
        if run.get("kind") not in {"sql_logic", "language_client"}:
            raise ValueError("unknown external evidence adapter")
        cases = (sql_cases if run["kind"] == "sql_logic" else client_cases)(evidence, run, source["reference"], implementation)
        bindings = index(run["bindings"], lambda value: canonical(value["runtime_identity"]), "external invocation binding")
        if set(bindings) != {canonical(case["runtime_identity"]) for case in cases}:
            raise ValueError("external bindings must name every actual assertion invocation exactly once")
        retained_sources = index(run["sources"], lambda value: value["source"], "external original source")
        for name, entry in retained_sources.items():
            if name not in originals or sha256(evidence.recorded(entry["artifact"])) != originals[name]["sha256"]:
                raise ValueError("external original source does not match pinned source inventory")
        for case in cases:
            binding = bindings[canonical(case["runtime_identity"])]
            identity = binding["identity"]
            key = identity_key(identity)
            for field, value in zip(("reference_revision", "source", "qualified_name", "parameter_fixture", "configuration"), key):
                text(value, "external ledger " + field)
            mapping = mappings.get(key)
            if mapping is None or mapping["identity"] != identity or key in groups:
                raise ValueError("external invocation lacks one distinct exact ledger identity")
            if identity["source"] not in retained_sources or case["source"] is not None and case["source"] != identity["source"]:
                raise ValueError("external invocation source is absent or differs")
            if not isinstance(binding.get("native_configuration"), str) or not binding["native_configuration"].strip() or binding.get("counterparts") != mapping["counterparts"]:
                raise ValueError("external counterpart/configuration binding is incomplete")
            candidate_id = binding.get("source_candidate")
            if run["kind"] == "sql_logic":
                runtime = case["runtime_identity"]
                configuration = canonical({"adapter": "sql_logic", "selection": runtime["selection"], "options": runtime["options"]})
                native_configuration = canonical({"build": case["native_build_configuration"], "selection": case["native_build_selection"], "execution": json.loads(configuration)})
                if binding["native_configuration"] != native_configuration or identity["configuration"] != configuration:
                    raise ValueError("SQL logic binding differs from actual selection/options configuration")
            if run["kind"] == "language_client":
                candidate = candidates.get(candidate_id)
                if candidate is None or candidate["identity"]["source"] != identity["source"] or candidate["identity"]["name"] != identity["qualified_name"]:
                    raise ValueError("client invocation does not bind an exact original declaration")
                expected_configuration = case["observations"][1]["configuration"]
                if binding["native_configuration"] != expected_configuration or identity["configuration"] != case["observations"][0]["configuration"]:
                    raise ValueError("client invocation binding differs from recorded engine configurations")
            groups.add(key)
            observed.append({**case, "binding": binding, "run": run["id"], "source_candidate": candidate_id})
            file_runs.setdefault(identity["source"], []).append(observed[-1])
    files = index(document.get("files", []), lambda value: value["source"], "external file binding") if document.get("files") else {}
    resolved = set()
    for name, binding in files.items():
        if name not in originals or binding.get("sha256") != originals[name]["sha256"]:
            raise ValueError("external file binding differs from original source identity")
        actual = file_runs.get(name, [])
        expected = {canonical(value) for value in binding.get("required_invocations", [])}
        keys = {canonical({"run": value["run"], "identity": value["runtime_identity"]}) for value in actual}
        if expected != keys or len(expected) != len(binding.get("required_invocations", [])):
            raise ValueError("external file configuration/parameter denominator differs")
        candidate_ids = {value["id"] for value in candidates.values() if value["identity"]["source"] == name}
        observed_ids = {value["source_candidate"] for value in actual if value["source_candidate"] is not None}
        if candidate_ids != observed_ids:
            continue
        if actual and all(value["passed"] for value in actual):
            resolved.add(name)
        elif binding.get("classification") == "support_input" and not candidate_ids and source["reference"] == SUPPORT_REFERENCE and name in SUPPORT_SOURCES:
            proof = artifact(parent, binding["artifact"])
            if sha256(proof) != originals[name]["sha256"]:
                raise ValueError("external support input differs from original bytes")
            resolved.add(name)
    return {"groups": groups, "cases": observed, "resolved_files": resolved,
            "all_passed": bool(observed) and all(case["passed"] for case in observed)}
