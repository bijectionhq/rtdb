#!/usr/bin/env python3

import argparse
import json
import platform
import shutil
from pathlib import Path

from history_results import verify_runtime_artifacts
from run_clients import validate_package, validate_package_files
from run_controls import (
    HERE, REFERENCE, REPOSITORY, case_key, digest, git, owned_cancellation,
    run_process, validate_execution, write_json,
)
from source_gate import require_phase_two

CONFIGURATION = HERE / "configurations/embedded-parser-migration.json"
FIXTURE = HERE / "fixtures/embedded-parser-cause.json"
ADAPTER = HERE / "embedded-parser.init.gradle"
JVM = HERE / "jvm"
AUTHORED_REVISION = "891454fe1ea3406d3a4cfa33a31477436408eb23"
REFERENCE_SOURCE = "core/src/test/kotlin/xtdb/query/SqlParserCauseTest.kt"
NATIVE_SOURCE = "bindings/jvm/src/test/java/xtdb/rust/NativeAdbcTest.java"
LIBRARY = "artifacts/librtdb_ffi.so"
TASKS = {
    "reference": ":rtdbConformanceEmbeddedParser",
    "native": ":bindings:xtdb-rust-bindings:rtdbConformanceEmbeddedParser",
}
CLASSES = {
    "reference": {"xtdb.query.SqlParser", "xtdb.antlr.Sql", "org.antlr.v4.runtime.InputMismatchException"},
    "native": {"xtdb.rust.NativeDatabase", "xtdb.rust.NativeConnection", "xtdb.rust.NativeStatement",
               "xtdb.rust.NativeException", "xtdb.api.error.Incorrect"},
}
UNRECONCILED_INVENTORY = [
    "source inventory is incomplete",
    "runtime discovery has not been reconciled",
]


def clean_revision(repository, revision):
    if git(repository, "rev-parse", "HEAD") != revision or git(repository, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError(f"embedded observation needs a clean checkout at {revision}: {repository}")


def selection(configuration, fixture_sha256):
    expected_fields = {"schema_version", "authored_revision", "authored_sources", "fixture_sha256", "context", "ledger"}
    if set(configuration) != expected_fields or configuration["schema_version"] != 1:
        raise ValueError("embedded migration configuration has an unknown schema")
    if configuration["authored_revision"] != AUTHORED_REVISION or set(configuration["authored_sources"]) != {REFERENCE_SOURCE, NATIVE_SOURCE}:
        raise ValueError("embedded migration does not bind the authored source assertions")
    if configuration["fixture_sha256"] != fixture_sha256:
        raise ValueError("embedded migration fixture differs from its selection")
    ledger = configuration["ledger"]
    if (ledger["schema_version"] != 1 or ledger["inventory_revision"] != REFERENCE
            or ledger["inventory_complete"] is not False or ledger["runtime_reconciled"] is not False
            or len(ledger["cases"]) != 1):
        raise ValueError("embedded migration requires one scoped case and the unreconciled inventory flags")
    mapping, = ledger["cases"]
    identity = mapping["identity"]
    case_key(identity)
    if (identity["reference_revision"], identity["source"], identity["parameter_fixture"], identity["configuration"]) != (
            REFERENCE, REFERENCE_SOURCE, str(FIXTURE.relative_to(REPOSITORY)), "embedded-sql-parser-cause"):
        raise ValueError("embedded migration identity differs from the selected source and fixture")
    observations = mapping["observations"]
    if (len(observations) != 1 or observations[0]["id"] != "statement_mismatch"
            or observations[0]["comparison"] != "exact"
            or observations[0]["expected"]["kind"] != "declared_migration"
            or observations[0]["expected"]["migration"] != "embedded_sql_parser_cause"):
        raise ValueError("embedded migration must select the exact statement mismatch observation")
    return mapping


def native_library(package, revision):
    manifest = json.loads(package.read_bytes())
    validate_package(manifest["image"], manifest, revision, "rust")
    validate_package_files(package, manifest)
    host = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine())
    if platform.system() != "Linux" or host is None or manifest["platform"] != f"linux/{host}":
        raise ValueError("embedded migration requires a host matching the selected native package")
    record, = [value for value in manifest["artifacts"] if value["path"] == LIBRARY]
    library = package.parent / LIBRARY
    if library.is_symlink() or digest(library) != record["sha256"]:
        raise ValueError("embedded native library differs from the packaged artifact")
    return library, record


def gradle_command(side, repository, fixture, directory, library=None):
    command = [repository / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk",
               "--init-script", ADAPTER, TASKS[side], f"-Prtdb.embeddedSide={side}",
               f"-Prtdb.adapterRoot={JVM}", f"-Prtdb.fixture={fixture}", f"-Prtdb.output={directory}"]
    if side == "native":
        command += [f"-Dxtdb.native.library={library}"]
    return command


def verify_runtime(runtime, side, library):
    artifacts = runtime["artifacts"]
    paths = {value["path"] for value in artifacts}
    classpath = set(runtime["classpath"])
    absent = set(runtime["absent_classpath"])
    jdk = set(runtime["jdk"])
    loaded = runtime["library"]
    native_paths = {str(library)} if side == "native" else set()
    if loaded != (str(library) if side == "native" else None):
        raise ValueError("embedded observation used a different native library")
    if (len(jdk) != 2 or not absent <= classpath
            or any(not Path(path).is_absolute() or Path(path).exists() or Path(path).is_symlink() for path in absent)
            or len(paths) != len(artifacts) or paths != (classpath - absent) | jdk | native_paths):
        raise ValueError("embedded JVM runtime closure differs from the observed classpath")
    locations = runtime["class_locations"]
    if set(locations) != CLASSES[side] or not set(locations.values()) <= classpath - absent:
        raise ValueError("embedded JVM classes lack their observed artifact identities")
    if str(JVM.resolve()) not in classpath - absent:
        raise ValueError("embedded JVM did not load the selected adapter classpath")
    verify_runtime_artifacts(artifacts)


def check_supplemental(reference, native, fixture):
    reference_unwanted = reference["unwanted_token"]["outcome"]
    unwanted = native["unwanted_token"]["outcome"]
    if reference_unwanted != {"kind": "expected_failure", "stage": "parse_statement_ast", "category": "incorrect",
                             "code": "xtdb/sql-error", "signature": {"cause": None}}:
        raise ValueError("reference unwanted-token observation differs from the authored null-cause assertion")
    if unwanted != {"kind": "expected_failure", "stage": "statement_prepare", "category": "incorrect",
                    "code": "xtdb/sql-error", "signature": {"cause": None}}:
        raise ValueError("native unwanted-token diagnostic differs from the authored assertion")
    for direct, wrapped, sql_id in (("statement_mismatch", "wrapped_mismatch", "statement_mismatch"),
                                    ("unwanted_token", "wrapped_unwanted", "unwanted_token")):
        diagnostic = native[direct]["diagnostics"]
        wrapper = native[wrapped]
        if (diagnostic["status"] != "INVALID_ARGUMENT" or not diagnostic["message"] or diagnostic["transit_bytes"] <= 0
                or wrapper["message"] != diagnostic["message"] or wrapper["sql"] != fixture["sql"][sql_id]
                or wrapper["outcome"] != {"kind": "expected_failure", "stage": "clojure_query", "category": "incorrect",
                                          "code": "xtdb/sql-error", "signature": {
                                              "cause": {"class": "xtdb.api.error.Incorrect", "cause": None}}}):
            raise ValueError(f"native embedded diagnostic evidence differs at {sql_id}")
    if reference["follow_up"]["outcome"] != {"kind": "returned", "typed_value": "xtdb.antlr.Sql$QueryExprContext"}:
        raise ValueError("reference follow-up did not return a statement context")
    if native["follow_up"]["outcome"] != {"kind": "returned", "typed_value": {
            "first_batch": True, "rows": 1, "answer": 42, "additional_batch": False}}:
        raise ValueError("native follow-up did not return the expected row on the same connection")


def observe(side, repository, revision, directory, fixture, fixture_sha256,
            configuration_sha256, context, mapping, timeout, library=None):
    directory.mkdir()
    execution_path = directory / "execution.json"
    command = gradle_command(side, repository, fixture, directory, library)
    process = run_process(command, repository, directory / "stdout.log", directory / "stderr.log", timeout)
    write_json(directory / "process.json", process)
    if not process["completed"] or process["exit_code"] != 0:
        raise ValueError(f"{side} embedded observation did not complete; inspect {directory}")
    execution = json.loads(execution_path.read_bytes())
    validate_execution(execution, fixture_sha256, mapping)
    runtime_path = directory / "runtime-artifacts.json"
    runtime = json.loads(runtime_path.read_bytes())
    if execution["side"] != side or execution["binary_sha256"] != digest(runtime_path):
        raise ValueError("embedded execution does not bind its side and runtime artifacts")
    verify_runtime(runtime, side, library)
    trace = {
        "task": TASKS[side],
        "observer": f"rtdb.conformance.embedded-parser-{side}/-main",
        "artifact_revision": revision,
        "configuration_sha256": configuration_sha256,
        "fixture_sha256": fixture_sha256,
        "inputs_sha256": digest(directory.parent / "inputs.json"),
        "artifacts": {name: digest(directory / name) for name in (
            "process.json", "stdout.log", "stderr.log", "execution.json", "runtime-artifacts.json")},
    }
    trace_path = directory / "trace.json"
    write_json(trace_path, trace)
    return {
        "identity": mapping["identity"],
        "runner": f"rtdb-embedded-parser/{side}/1",
        "trace_sha256": digest(trace_path),
        "context": {**context, "artifact_revision": revision, "binary_sha256": execution["binary_sha256"],
                    "configuration_sha256": configuration_sha256, "fixture_sha256": fixture_sha256},
        "observations": execution["observations"],
    }, execution["supplemental"]


def require_migration_report(report, mapping):
    cases = [{"identity": mapping["identity"], "disposition": "matched_declared_migration",
              "comparisons": 1, "differences": []}]
    counts = {"required_groups": 1, "passing_both": 0, "matching_expected_failure": 0,
              "matched_declared_migration": 1, "missing_evidence": 0, "mismatches": 0}
    if (report["cases"] != cases or type(report["cases"][0]["comparisons"]) is not int
            or any(type(report[key]) is not int or report[key] != value for key, value in counts.items())
            or report["manifest_errors"] != UNRECONCILED_INVENTORY
            or report["strict_agreement"] is not None or report["complete_parity"] is not False):
        raise ValueError("embedded comparator did not confirm the scoped declared migration")


def retain(path, destination, expected):
    shutil.copyfile(path, destination)
    if digest(destination) != expected or digest(path) != expected:
        raise ValueError(f"embedded input changed while retaining bytes: {path}")


def run(args):
    require_phase_two(REPOSITORY, args.phase_two)
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    clean_revision(REPOSITORY, revision)
    clean_revision(args.reference, REFERENCE)
    if args.timeout <= 0:
        raise ValueError("embedded observation timeout must be positive")
    if any(args.output.is_relative_to(repository) for repository in (REPOSITORY, args.reference)):
        raise ValueError("embedded evidence output must be outside both source checkouts")
    fixture_sha256 = digest(FIXTURE)
    configuration_sha256 = digest(CONFIGURATION)
    configuration = json.loads(CONFIGURATION.read_bytes())
    mapping = selection(configuration, fixture_sha256)
    for name, expected in configuration["authored_sources"].items():
        if (digest(REPOSITORY / name) != expected
                or git(REPOSITORY, "hash-object", "--", name) != git(REPOSITORY, "rev-parse", f"{AUTHORED_REVISION}:{name}")):
            raise ValueError(f"authored embedded source bytes changed: {name}")
    package_sha256 = digest(args.native_package)
    library, library_record = native_library(args.native_package, revision)
    input_paths = [CONFIGURATION, FIXTURE, ADAPTER, Path(__file__), args.comparator, args.native_package, library,
                   HERE / "run_controls.py", HERE / "run_clients.py", HERE / "history_results.py", HERE / "source_gate.py",
                   REPOSITORY / "bindings/jvm/build.gradle.kts",
                   *(REPOSITORY / name for name in configuration["authored_sources"]),
                   *(JVM / "rtdb/conformance" / name for name in (
                       "embedded_parser.clj", "embedded_parser_reference.clj", "embedded_parser_native.clj"))]
    inputs = {str(path): digest(path) for path in input_paths}
    if (inputs[str(CONFIGURATION)] != configuration_sha256 or inputs[str(FIXTURE)] != fixture_sha256
            or inputs[str(args.native_package)] != package_sha256 or inputs[str(library)] != library_record["sha256"]):
        raise ValueError("embedded selection or package changed while loading inputs")

    output = args.output
    output.mkdir(parents=True, exist_ok=False)
    fixture = output / "fixture.json"
    captured_configuration = output / "configuration.json"
    captured_library = output / LIBRARY
    captured_library.parent.mkdir()
    retain(FIXTURE, fixture, fixture_sha256)
    retain(CONFIGURATION, captured_configuration, configuration_sha256)
    retain(library, captured_library, library_record["sha256"])
    captured_package = output / "package.json"
    retain(args.native_package, captured_package, package_sha256)
    ledger_path = output / "ledger.json"
    write_json(ledger_path, configuration["ledger"])
    ledger_sha256 = digest(ledger_path)
    write_json(output / "inputs.json", {"reference_revision": REFERENCE, "implementation_revision": revision,
                                         "sha256": inputs, "retained_library_sha256": library_record["sha256"],
                                         "ledger_sha256": ledger_sha256})

    receipts = {}
    supplemental = {}
    for side, repository, selected_revision in (("reference", args.reference, REFERENCE), ("native", REPOSITORY, revision)):
        receipts[side], supplemental[side] = observe(
            side, repository, selected_revision, output / side, fixture, fixture_sha256,
            configuration_sha256, configuration["context"], mapping, args.timeout,
            captured_library if side == "native" else None)
        write_json(output / f"{side}.json", [receipts[side]])

    report_path = output / "report.json"
    process = run_process([args.comparator, "compare", ledger_path, output / "reference.json", output / "native.json"],
                          REPOSITORY, report_path, output / "compare.stderr.log", args.timeout)
    write_json(output / "compare.process.json", process)
    if not process["completed"] or process["exit_code"] != 1:
        raise ValueError("scoped migration comparison did not return the strict non-parity status")
    report = json.loads(report_path.read_bytes())
    require_migration_report(report, mapping)
    check_supplemental(supplemental["reference"], supplemental["native"], json.loads(fixture.read_bytes()))
    clean_revision(REPOSITORY, revision)
    clean_revision(args.reference, REFERENCE)
    if (any(digest(path) != expected for path, expected in inputs.items())
            or digest(fixture) != fixture_sha256 or digest(captured_configuration) != configuration_sha256
            or digest(captured_library) != library_record["sha256"] or digest(captured_package) != package_sha256
            or digest(ledger_path) != ledger_sha256):
        raise ValueError("embedded inputs changed during observation")
    write_json(output / "result.json", {"selected_migration_observed": True, "complete_parity": False,
                                        "report_sha256": digest(report_path)})


def main():
    parser = argparse.ArgumentParser(description="Observe the scoped embedded SQL parser cause migration.")
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--native-package", type=Path, required=True)
    parser.add_argument("--comparator", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=3600)
    args = parser.parse_args()
    for field in ("reference", "native_package", "comparator", "output"):
        setattr(args, field, getattr(args, field).resolve())
    with owned_cancellation():
        run(args)


if __name__ == "__main__":
    main()
