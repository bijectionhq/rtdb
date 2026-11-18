#!/usr/bin/env python3

import argparse
import hashlib
from pathlib import Path, PurePosixPath

from external_inventory import Evidence, iter_sql_cases
from evidence import MAX_JSON_BYTES, canonical, index, integer, read_json, sha256, text
from sql_logic_records import records
from sql_logic_results import json_lines


CONFIGURATION = Path(__file__).resolve().parent / "configurations/sql-logic-scalar-case.json"
SIDES = ("reference", "reference-repeat", "native")


def verify_original(evidence, run, selection):
    identity = selection["runtime_identity"]
    sources = index(run["sources"], lambda source: source["source"], "original SQL logic source")
    source = evidence.recorded(sources[identity["source"]]["artifact"])
    if sha256(source) != identity["source_sha256"]:
        raise ValueError("selected SQL assertion uses different original source bytes")
    with source.open(encoding="utf-8", newline="\n") as stream:
        original = next((record for record in records(stream) if record["ordinal"] == identity["ordinal"]), None)
    formatted = selection["formatted_result"]
    expected = {"type": "query", "sql": selection["sql"], "type_string": formatted["type_string"], "sort": formatted["sort"],
                "label": None, "expected": {"count": 1, "text": formatted["text"]}}
    if (original is None or original["parse_error"] is not None or original["record"] != expected
            or original["line"] != identity["line"] or original["assertion_ordinal"] != identity["assertion_ordinal"]):
        raise ValueError("selected SQL assertion differs from the pinned original record")


def verify_production_build(evidence, run, binary):
    build = read_json(evidence.recorded(run["native_build"]))
    original = PurePosixPath(run["native_build"]["path"])
    if not original.is_absolute() or original.name != "production-artifacts.json":
        raise ValueError("selected SQL assertion requires the original production manifest path")
    directory = original.parent
    process_ref, messages_ref = build["process"], build["messages"]
    if process_ref["path"] != "cargo.process.json" or messages_ref["path"] != "cargo.stdout.log":
        raise ValueError("selected SQL assertion production build has different receipt paths")
    process = read_json(evidence.recorded({**process_ref, "path": str(directory / process_ref["path"])}))
    cwd = text(process.get("cwd"), "production Cargo working directory")
    repository = PurePosixPath(cwd)
    if repository.root != "/" or str(repository) != cwd or ".." in repository.parts:
        raise ValueError("selected SQL assertion requires an absolute normalized Cargo working directory")
    expected_command = ["cargo", "build", "--manifest-path", str(repository / "rust/Cargo.toml"), "--locked",
                        "--workspace", "--all-targets", "--no-default-features", "--message-format=json"]
    if build["command"] != expected_command or process["command"] != expected_command:
        raise ValueError("selected SQL assertion production build has a different Cargo invocation")

    messages = evidence.recorded({**messages_ref, "path": str(directory / messages_ref["path"])})
    emitted = [record for record in json_lines(messages)
               if record.get("reason") == "compiler-artifact" and record.get("executable") and not record["profile"]["test"]]
    if emitted != [artifact["compiler_artifact"] for artifact in build["binaries"]]:
        raise ValueError("selected SQL assertion production assembly differs from recorded Cargo artifacts")
    selected = run["native_binary"]
    (ordinal, artifact), = ((position, value) for position, value in enumerate(build["binaries"])
                            if value["path"] == selected["path"] and value["sha256"] == selected["sha256"])
    compiler = artifact["compiler_artifact"]
    target = compiler["target"]
    if (target["name"] != "rtdb-conformance" or target["kind"] != ["bin"]
            or compiler["profile"]["test"] is not False):
        raise ValueError("selected SQL assertion uses a different Cargo target")
    source = repository / "rust/crates/conformance"
    if compiler.get("manifest_path") != str(source / "Cargo.toml") or target.get("src_path") != str(source / "src/main.rs"):
        raise ValueError("selected SQL assertion Cargo target has a different source manifest or entry point")
    executable = PurePosixPath(compiler["executable"])
    if not executable.is_absolute() or executable.name != "rtdb-conformance":
        raise ValueError("selected SQL assertion uses a different original Cargo executable")
    copy = f"binaries/{ordinal}-{executable.name}"
    retained = artifact["retained_artifact"]
    if binary != directory / copy or retained["path"] != copy:
        raise ValueError("selected SQL assertion executable differs from the ordinal production copy")
    original_copy = evidence.recorded({**retained, "path": str(directory / copy)})
    if (original_copy != evidence.recorded(selected) or retained["sha256"] != selected["sha256"]
            or integer(artifact.get("bytes"), "production executable size") != original_copy.stat().st_size
            or integer(selected.get("bytes"), "selected executable size") != original_copy.stat().st_size
            or integer(retained.get("bytes"), "retained executable size") != original_copy.stat().st_size):
        raise ValueError("selected SQL assertion retained executable bytes differ from the production artifact")


def execution_proof(evidence, run, selection):
    processes = {side: evidence.read(f"{side}/process.json") for side in SIDES}
    output = PurePosixPath(run["recorded_root"])
    binary = PurePosixPath(run["native_binary"]["path"])
    repository = PurePosixPath(processes["native"]["cwd"])
    reference = PurePosixPath(processes["reference"]["cwd"])
    if not all(path.is_absolute() for path in (output, binary, repository, reference)):
        raise ValueError("selected SQL assertion requires absolute recorded execution paths")
    verify_production_build(evidence, run, binary)
    plan = output / "plan/plan.json"
    adapter = repository / "rust/conformance"
    lineage = {}
    versions = set()
    for side in SIDES:
        destination = output / side / "observations"
        if side == "native":
            command = [str(binary), "sql-logic", str(plan), str(destination)]
            cwd = repository
        else:
            command = [str(reference / "gradlew"), "--no-daemon", "--console=plain", "-PfullJdk", "--init-script",
                       str(adapter / "reference.init.gradle"), "rtdbConformanceSqlLogic", f"-Prtdb.adapterRoot={adapter / 'jvm'}",
                       f"-Prtdb.fixture={plan}", f"-Prtdb.output={destination}"]
            cwd = reference
        if processes[side]["command"] != command or processes[side]["cwd"] != str(cwd):
            raise ValueError(f"selected SQL assertion has a different {side} invocation")
        execution = evidence.read(f"{side}/observations/execution.json")
        trace_path = f"{side}/trace.json"
        trace = evidence.read(trace_path)
        if trace.get("artifact_revision") != execution["artifact_revision"]:
            raise ValueError(f"selected SQL assertion {side} trace revision differs from its execution")
        environment = execution.get("environment")
        if (not isinstance(environment, dict) or environment.get("timezone") != selection["timezone"]
                or environment.get("query_clock") != selection["clock"]):
            raise ValueError(f"selected SQL assertion {side} has a different query environment")
        version = integer(environment.get("storage_version"), f"{side} SQL logic storage version")
        if version > 2**31 - 1:
            raise ValueError(f"selected SQL assertion {side} storage version exceeds the comparator range")
        versions.add(version)
        lineage[side] = {"artifact_revision": execution["artifact_revision"], "binary_sha256": execution["binary_sha256"],
                         "trace": trace_path, "trace_sha256": sha256(evidence.path(trace_path))}
    if len(versions) != 1:
        raise ValueError("selected SQL assertion executions have different storage versions")
    return lineage, versions.pop()


def selected_observations(evidence, run, identity, implementation):
    selected = None
    for case in iter_sql_cases(evidence, run, identity["reference"], implementation):
        observed = case["runtime_identity"]
        if observed["source"] != identity["source"] or observed["ordinal"] != identity["ordinal"]:
            continue
        if selected is not None or canonical(observed) != canonical(identity):
            raise ValueError("selected SQL assertion has a duplicate or different runtime identity")
        selected = case
    if selected is None or not selected["passed"]:
        raise ValueError("selected SQL assertion did not pass on all three executions")
    if (selected["native_build_selection"] != {"variant": "local", "features": []}
            or selected["native_build_configuration"] not in {"linux-amd64/rust/local/production", "linux-arm64/rust/local/production"}):
        raise ValueError("selected SQL assertion requires a local production build")
    return dict(zip(SIDES, selected["observations"], strict=True))


def encode(value):
    return (canonical(value) + "\n").encode("utf-8")


def receipt_bundle(external, run_id):
    if external.is_symlink() or not external.is_file() or external.stat().st_size > MAX_JSON_BYTES:
        raise ValueError("selected SQL assertion requires a regular external evidence manifest")
    external_digest = sha256(external)
    configuration_digest = sha256(CONFIGURATION)
    document = read_json(external)
    configuration = read_json(CONFIGURATION)
    if integer(configuration.get("schema_version"), "SQL logic assertion configuration schema", 1) != 1:
        raise ValueError("unsupported SQL logic assertion configuration")
    runtime = configuration["runtime_identity"]
    if (integer(document.get("schema_version"), "external evidence schema", 1) != 1
            or document.get("reference_revision") != runtime["reference"]):
        raise ValueError("external evidence uses another schema or reference")
    implementation = text(document.get("implementation_revision"), "native implementation revision")
    run = index(document["runs"], lambda value: value["id"], "external run")[run_id]
    if run["kind"] != "sql_logic":
        raise ValueError("selected external run is not SQL logic")
    evidence = Evidence(external.parent, run)
    verify_original(evidence, run, configuration)
    observations = selected_observations(evidence, run, runtime, implementation)
    lineage, storage_version = execution_proof(evidence, run, configuration)
    basis = observations["native"]["result"].get("basis")
    if not isinstance(basis, dict) or basis.get("timezone") != configuration["timezone"]:
        raise ValueError("selected SQL assertion has a different native query timezone")

    identity = {"reference_revision": runtime["reference"], "source": runtime["source"],
                "qualified_name": f"record:{runtime['ordinal']}",
                "parameter_fixture": f"assertion:{runtime['assertion_ordinal']}/line:{runtime['line']}",
                "configuration": canonical({"adapter": "sql_logic", "selection": runtime["selection"], "options": runtime["options"]})}
    observation_id = "formatted-result"
    mapping = {"identity": identity, "source_assertion_lines": configuration["source_assertion_lines"],
               "counterparts": [f"rtdb-conformance/sql-logic/{runtime['source']}#record:{runtime['ordinal']}"],
               "observations": [{"id": observation_id, "comparison": "exact", "expected": {"kind": "returned"}}]}
    ledger = {"schema_version": 1, "inventory_revision": runtime["reference"], "inventory_complete": False,
              "runtime_reconciled": False, "cases": [mapping]}
    context = {"fixture_sha256": runtime["source_sha256"], "configuration_sha256": configuration_digest,
               "applied_basis": None, "valid_time": None, "system_time": None, "clock": configuration["clock"],
               "timezone": configuration["timezone"], "storage_version": storage_version, "features": []}
    bundle = {"ledger.json": encode(ledger)}
    for side in ("reference", "native"):
        artifact = lineage[side]
        receipt = {"identity": identity, "runner": f"rtdb-sql-logic/{side}/1", "trace_sha256": artifact["trace_sha256"],
                   "context": {**context, "artifact_revision": artifact["artifact_revision"], "binary_sha256": artifact["binary_sha256"]},
                   "observations": [{"id": observation_id, "executed": True, "outcome": {"kind": "returned", "typed_value": {
                       **configuration["formatted_result"], "text": observations[side]["result"]["text"]}}}]}
        bundle[f"{side}.json"] = encode([receipt])
    bundle["inputs.json"] = encode({"schema_version": 1, "external_manifest_sha256": external_digest, "run_id": run_id,
                                    "configuration_sha256": configuration_digest, "executions": lineage,
                                    "outputs": {name: hashlib.sha256(data).hexdigest() for name, data in bundle.items()}})
    Evidence(external.parent, run)
    if sha256(external) != external_digest or sha256(CONFIGURATION) != configuration_digest:
        raise ValueError("selected SQL assertion inputs changed during verification")
    return bundle


def main():
    parser = argparse.ArgumentParser(description="Bind one pinned SQL logic assertion to archived production observations.")
    parser.add_argument("--external", type=Path, required=True)
    parser.add_argument("--run", required=True)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--output", type=Path)
    target.add_argument("--verify", type=Path)
    args = parser.parse_args()
    bundle = receipt_bundle(args.external, args.run)
    if args.output is not None:
        if args.output.resolve().is_relative_to(args.external.resolve().parent):
            raise ValueError("receipt output must stay outside the external evidence archive")
        args.output.mkdir(parents=True)
        for name, payload in bundle.items():
            (args.output / name).write_bytes(payload)
    else:
        for name, payload in bundle.items():
            path = args.verify / name
            if path.is_symlink() or not path.is_file() or path.stat().st_size != len(payload) or path.read_bytes() != payload:
                raise ValueError(f"selected SQL assertion receipt differs from its archived proof: {name}")


if __name__ == "__main__":
    main()
