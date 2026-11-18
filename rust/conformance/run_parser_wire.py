#!/usr/bin/env python3

import argparse
import hashlib
import json
import math
import re
import struct
import subprocess
import sys
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from run_controls import HERE, REFERENCE, REPOSITORY, digest, git, run_process, validate_native_revision, write_json
from run_embedded_migration import verify_runtime
from run_history import input_receipt, verify_inputs
from run_mixed_iid_reference import receipt_bytes, require, require_revision
from source_gate import require_phase_two


FIXTURE = HERE / "fixtures/parser-wire-diagnostics.json"
SQL = {"statement_mismatch": "SELEC 1", "unwanted_token": "extra SELECT 1"}
FOLLOW_UP = "SELECT 42::bigint AS answer"
FLIGHT_CALLS = ["statement_mismatch", "unwanted_token", "follow_up_info", "follow_up_stream"]
JVM_CLASSES = {
    "xtdb.api.Xtdb", "xtdb.antlr.Sql", "xtdb.query.SqlParser", "xtdb.pgwire.PgType", "xtdb.flight_sql.XtdbProducer",
    "org.apache.arrow.flight.FlightClient", "org.apache.arrow.flight.sql.FlightSqlClient",
    "org.apache.arrow.flight.FlightStream", "org.apache.arrow.vector.BigIntVector",
}
REFERENCE_SOURCE = {
    "revision": REFERENCE, "path": "src/test/clojure/xtdb/api_test.clj", "line": 415,
    "qualified_name": "xtdb.api-test/an-error-keeps-its-category-and-code-on-every-server",
    "sha256": "24634a2a75925cfe2c39f2e44e537e08a7031f18479a03c9166f314858a764f4",
}
SUPPLEMENTAL_SOURCE = {
    "revision": "891454fe1ea3406d3a4cfa33a31477436408eb23",
    "path": "core/src/test/kotlin/xtdb/query/SqlParserCauseTest.kt", "line": 44,
    "qualified_name": "SqlParserCauseTest.statement unwanted token is reported with a null cause",
    "sha256": "7c82b6d4aee31bc645835f42ef375fe623145082a08f90a22b42ad826b06af00",
}


def wire_bytes(value, limit):
    require(isinstance(value, list) and len(value) <= limit
            and all(type(byte) is int and 0 <= byte <= 255 for byte in value), "invalid retained wire bytes")
    return bytes(value)


def fields_exact(left, right):
    canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return canonical(left) == canonical(right)


def strict_json(payload):
    def record(pairs):
        fields = dict(pairs)
        require(len(fields) == len(pairs), "duplicate JSON field")
        return fields

    result = json.loads(payload.decode("utf-8"), object_pairs_hook=record)
    require(isinstance(result, dict), "expected a JSON object")
    json.dumps(result, allow_nan=False)
    return result


def read_receipt(path):
    payload = receipt_bytes(path)
    return strict_json(payload), hashlib.sha256(payload).hexdigest()


def validate_execution_header(execution, engine, fixture_sha):
    require(type(execution.get("schema_version")) is int and execution["schema_version"] == 1
            and execution.get("engine") == engine and execution.get("fixture_sha256") == fixture_sha,
            "parser wire execution used a different fixture or engine")


def require_digest(record):
    require(isinstance(record, dict) and type(record.get("bytes")) is int and record["bytes"] >= 0
            and isinstance(record.get("sha256"), str) and re.fullmatch("[0-9a-f]{64}", record["sha256"]),
            "invalid reference runtime file digest")


def validate_runtime_artifact_shapes(artifacts):
    require(isinstance(artifacts, list) and artifacts, "invalid reference runtime artifact list")
    for artifact in artifacts:
        require(isinstance(artifact, dict) and isinstance(artifact.get("path"), str) and artifact["path"],
                "invalid reference runtime artifact record")
        if artifact.get("kind") == "file":
            require(set(artifact) == {"kind", "path", "bytes", "sha256"}, "invalid reference runtime file record")
            require_digest(artifact)
        else:
            require(artifact.get("kind") == "directory" and set(artifact) == {"kind", "path", "files"}
                    and isinstance(artifact["files"], list), "invalid reference runtime directory record")
            for member in artifact["files"]:
                require(isinstance(member, dict) and set(member) == {"path", "bytes", "sha256"}
                        and isinstance(member["path"], str) and member["path"], "invalid reference runtime directory member")
                require_digest(member)


def verify_jvm_runtime(runtime):
    require(isinstance(runtime, dict)
            and set(runtime) == {"classpath", "jdk", "absent_classpath", "class_locations", "library", "artifacts",
                                 "java_home", "java_executable", "java_modules"},
            "invalid parser wire JVM runtime closure")
    for key in ("classpath", "absent_classpath", "jdk"):
        require(isinstance(runtime[key], list) and all(isinstance(path, str) and path for path in runtime[key]),
                "invalid parser wire JVM path inventory")
    require(all(isinstance(runtime[key], str) and runtime[key] for key in ("java_home", "java_executable", "java_modules")),
            "invalid parser wire JVM JDK paths")
    locations = runtime["class_locations"]
    require(isinstance(locations, dict) and all(isinstance(path, str) and path for path in locations.values()),
            "invalid parser wire JVM class locations")
    validate_runtime_artifact_shapes(runtime["artifacts"])
    home = Path(runtime["java_home"])
    require(home.is_absolute() and home.resolve(strict=True) == home and home.is_dir(), "parser wire JDK home is not canonical")
    java, modules = ((home / relative).resolve(strict=True) for relative in ("bin/java", "lib/modules"))
    require(java.is_file() and modules.is_file() and java != modules
            and runtime["java_executable"] == str(java) and runtime["java_modules"] == str(modules)
            and runtime["jdk"] == [str(java), str(modules)], "parser wire JVM executable or modules differs from its JDK")
    absent = set(runtime["absent_classpath"])
    require(len(absent) == len(runtime["absent_classpath"]), "duplicate absent JVM classpath entry")
    for name in runtime["classpath"]:
        path = Path(name)
        require(path.is_absolute() and path.resolve(strict=name not in absent) == path,
                "parser wire JVM classpath is not canonical")
    for artifact in runtime["artifacts"]:
        path = Path(artifact["path"])
        require(path.is_absolute() and path.resolve(strict=True) == path, "parser wire JVM artifact is not canonical")
    verify_runtime(runtime, "reference", None, classes=JVM_CLASSES)
    for name, location in locations.items():
        location = Path(location)
        entry = f"{name.replace('.', '/')}.class"
        if location.is_dir():
            require((location / entry).is_file(), f"JVM class is missing from its observed artifact: {name}")
        else:
            with ZipFile(location) as artifact:
                require(entry in artifact.namelist(), f"JVM class is missing from its observed artifact: {name}")


def frame_bytes(query, sql, tags):
    require(set(query) == {"sql", "frames"} and query["sql"] == sql
            and [frame["tag"] for frame in query["frames"]] == tags
            and all(set(frame) == {"tag", "bytes"} for frame in query["frames"]),
            "unexpected PostgreSQL frame sequence")
    bodies = [wire_bytes(frame["bytes"], 65532) for frame in query["frames"]]
    require(bodies[-1] == b"I", "PostgreSQL connection did not return idle")
    return bodies


def pg_error(query, sql):
    body, ready = frame_bytes(query, sql, ["E", "Z"])
    parts = body.split(b"\0")
    require(parts[-2:] == [b"", b""] and all(len(part) >= 2 for part in parts[:-2]),
            "malformed PostgreSQL ErrorResponse")
    fields = {part[:1].decode("ascii"): part[1:].decode("utf-8") for part in parts[:-2]}
    require(len(fields) == len(parts) - 2 and {"S", "V", "C", "M", "D"} <= fields.keys()
            and len(fields["C"]) == 5, "PostgreSQL ErrorResponse omitted required diagnostics")
    detail = strict_json(fields["D"].encode("utf-8"))
    require(detail.get("message") == fields["M"], "PostgreSQL error message and detail disagree")
    return {"fields": {key: value for key, value in fields.items() if key != "D"}, "detail": detail,
            "payload": list(body), "ready": ready.decode("ascii")}


def pg_follow_up(query):
    description, row, tag, _ = frame_bytes(query, FOLLOW_UP, ["T", "D", "C", "Z"])
    require(description.startswith(b"\0\1answer\0") and len(description) == 27,
            "PostgreSQL follow-up omitted its answer column")
    _, _, oid, size, _, fmt = struct.unpack(">IhIhih", description[9:])
    require((oid, size, fmt) == (20, 8, 0) and row == b"\0\1\0\0\0\x0242" and tag == b"SELECT 1\0",
            "PostgreSQL follow-up did not return bigint answer 42")


def flight_error(query, sql, engine):
    require(set(query) == {"sql", "status", "message", "metadata_bytes"} and query["sql"] == sql
            and isinstance(query["message"], str) and query["message"],
            "Flight parser rejection omitted its SQL or description")
    status = query["status"]
    require(isinstance(status, str) and status, "Flight parser rejection omitted its status")
    if engine == "rust" and status == "InvalidArgument":
        status = "INVALID_ARGUMENT"
    wire = wire_bytes(query["metadata_bytes"], 4096)
    detail = strict_json(wire)
    require(detail.get("message") == query["message"], "Flight error description and metadata disagree")
    return {"status": status, "message": query["message"], "detail": detail, "payload": list(wire)}


def schema_metadata(value):
    return isinstance(value, dict) and all(isinstance(key, str) and isinstance(item, str) for key, item in value.items())


def validate_schema(schema):
    require(set(schema) == {"fields", "metadata"} and schema_metadata(schema["metadata"])
            and isinstance(schema["fields"], list) and len(schema["fields"]) == 1,
            "invalid Flight follow-up schema")
    field, = schema["fields"]
    require(set(field) == {"name", "type", "nullable", "metadata"}
            and field["name"] == "answer" and field["type"] == "i64" and type(field["nullable"]) is bool
            and schema_metadata(field["metadata"]), "invalid Flight follow-up bigint field")


def flight_follow_up(flight):
    require(fields_exact(flight["session"], {"options_applied": True, "cookie_established": True, "calls": FLIGHT_CALLS}),
            "Flight receipt did not establish and retain one session")
    follow_up = flight["follow_up"]
    require(set(follow_up) == {"sql", "declared_schema", "stream_schema", "batches"} and follow_up["sql"] == FOLLOW_UP
            and isinstance(follow_up["batches"], list) and len(follow_up["batches"]) == 1,
            "invalid Flight follow-up evidence")
    batch, = follow_up["batches"]
    require(set(batch) == {"schema", "rows"}, "invalid Flight follow-up batch")
    declared = follow_up["declared_schema"]
    for schema in (declared, follow_up["stream_schema"], batch["schema"]):
        validate_schema(schema)
        require(fields_exact(declared, schema), "Flight follow-up stream or batch schema differs from FlightInfo")
    require(fields_exact(batch["rows"], [[{"kind": "i64", "value": 42}]]),
            "Flight follow-up did not return the ordered bigint answer 42")
    return follow_up


def diagnostics(execution, engine):
    observations = execution["observations"]
    require(set(observations) == {"pgwire", "flight_sql"}, "wire receipt omitted a protocol")
    pg, flight = observations["pgwire"], observations["flight_sql"]
    require(set(pg) == {"errors", "follow_up"} and set(flight) == {"errors", "follow_up", "session"}
            and set(pg["errors"]) == set(flight["errors"]) == set(SQL), "wire receipt omitted a malformed statement")
    pg_follow_up(pg["follow_up"])
    return {
        "pgwire": {key: pg_error(pg["errors"][key], sql) for key, sql in SQL.items()},
        "flight_sql": {key: flight_error(flight["errors"][key], sql, engine) for key, sql in SQL.items()},
    }, flight_follow_up(flight)


def require_parser_branch(diagnostic, identity, engine):
    detail = diagnostic["detail"]
    require(detail.get("category") == "incorrect" and detail.get("code") == "xtdb/sql-error",
            "wire diagnostic did not come from a parser rejection")
    message = detail["message"]
    if identity == "unwanted_token":
        jvm_branch = message.startswith("Errors parsing SQL statement:\n  - line 1:0 extraneous input 'extra' expecting ")
        native_branch = engine == "rust" and re.match(r"^sql parser error: Expected: .+, found: extra(?:\s|$)", message)
        require(jvm_branch or native_branch, "wire diagnostic did not reach the supplemental extra-token branch")
    elif engine == "jvm":
        require(message.startswith("Errors parsing SQL statement"), "pinned JVM parser message differs from its source assertion")
    else:
        require(message.startswith("Errors parsing SQL statement:\n  - line 1:0 mismatched input 'SELEC' expecting ")
                or re.match(r"^sql parser error: Expected: .+, found: SELEC(?:\s|$)", message),
                "native wire diagnostic did not reject the pinned misspelled statement")


def compare(reference, native):
    jvm, jvm_follow_up = diagnostics(reference, "jvm")
    rust, rust_follow_up = diagnostics(native, "rust")
    for protocol, status in (("pgwire", "08P01"), ("flight_sql", "INVALID_ARGUMENT")):
        diagnostic = jvm[protocol]["statement_mismatch"]
        observed = diagnostic["fields"]["C"] if protocol == "pgwire" else diagnostic["status"]
        require(observed == status, "pinned JVM statement mismatch omitted its wire status")

    rows = []
    for protocol in jvm:
        for identity in SQL:
            left, right = jvm[protocol][identity], rust[protocol][identity]
            require_parser_branch(left, identity, "jvm")
            require_parser_branch(right, identity, "rust")
            different_fields = [name for name in left if not fields_exact(left[name], right[name])]
            rows.append({"protocol": protocol, "id": identity, "sql": SQL[identity],
                         "source": REFERENCE_SOURCE if identity == "statement_mismatch" else SUPPLEMENTAL_SOURCE,
                         "reference": left, "native": right, "different_fields": different_fields,
                         "exact_match": not different_fields})
    follow_up_match = fields_exact(jvm_follow_up, rust_follow_up)
    return {"comparisons": rows, "diagnostic_exact_match": all(row["exact_match"] for row in rows),
            "flight_follow_up": {"reference": jvm_follow_up, "native": rust_follow_up, "exact_match": follow_up_match}}


def observe(args, side, fixture, fixture_sha, revision, hashes):
    directory = args.output / side
    directory.mkdir()
    artifacts = directory / "artifacts"
    if side == "jvm":
        artifacts.mkdir()
        command = [args.reference / "gradlew", "--no-daemon", "--console=plain", "-PfullJdk",
                   "--init-script", HERE / "reference.init.gradle", "rtdbConformanceParserWire",
                   f"-Prtdb.adapterRoot={HERE / 'jvm'}", f"-Prtdb.fixture={fixture}", f"-Prtdb.output={artifacts}"]
        cwd = args.reference
    else:
        command = [args.native, "parser-wire", fixture, artifacts]
        cwd = REPOSITORY
    stdout, stderr = directory / "stdout.log", directory / "stderr.log"
    process = run_process(command, cwd, stdout, stderr, args.timeout)
    process_path = directory / "process.json"
    write_json(process_path, process)
    saved_process, hashes[process_path] = read_receipt(process_path)
    require(fields_exact(saved_process, process) and process["completed"] is True
            and type(process["exit_code"]) is int and process["exit_code"] == 0,
            f"{side} parser wire worker did not complete")
    execution_path = artifacts / "execution.json"
    execution, hashes[execution_path] = read_receipt(execution_path)
    validate_execution_header(execution, side, fixture_sha)
    if side == "jvm":
        path = artifacts / "runtime-artifacts.json"
        runtime, hashes[path] = read_receipt(path)
        require(execution["binary_sha256"] == hashes[path] and execution["runtime_unchanged"] is True,
                "reference parser wire receipt omitted the observed runtime closure")
        verify_jvm_runtime(runtime)
        hashes[stdout] = digest(stdout)
    else:
        validate_native_revision(execution, revision)
        saved_execution, hashes[stdout] = read_receipt(stdout)
        require(fields_exact(saved_execution, execution) and execution["binary_sha256"] == digest(args.native),
                "native parser wire execution differs from its binary or stdout")
    hashes[stderr] = digest(stderr)
    return execution


def run(args):
    require_phase_two(REPOSITORY, args.phase_two)
    require(math.isfinite(args.timeout) and args.timeout > 0, "parser wire timeout must be positive and finite")
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    require_revision(REPOSITORY, revision)
    require_revision(args.reference, REFERENCE)
    require(not any(args.output.is_relative_to(root) for root in (REPOSITORY, args.reference)),
            "parser wire output must be outside both checkouts")
    reference_source = args.reference / REFERENCE_SOURCE["path"]
    supplemental_source = REPOSITORY / SUPPLEMENTAL_SOURCE["path"]
    require(digest(reference_source) == REFERENCE_SOURCE["sha256"] and digest(supplemental_source) == SUPPLEMENTAL_SOURCE["sha256"]
            and git(REPOSITORY, "hash-object", "--", str(supplemental_source))
            == git(REPOSITORY, "rev-parse", f"{SUPPLEMENTAL_SOURCE['revision']}:{SUPPLEMENTAL_SOURCE['path']}"),
            "parser wire source assertion changed")

    fixture_input, fixture_sha = read_receipt(FIXTURE)
    require(fields_exact(fixture_input, {"schema_version": 1, **SQL, "follow_up": FOLLOW_UP}), "parser wire fixture changed")
    inputs = input_receipt([FIXTURE, reference_source, supplemental_source, args.native, Path(__file__).resolve(),
                            HERE / "history_results.py", HERE / "run_history.py", HERE / "run_controls.py", HERE / "run_clients.py",
                            HERE / "run_mixed_iid_reference.py", HERE / "run_embedded_migration.py",
                            HERE / "source_gate.py", HERE / "reference.init.gradle",
                            HERE / "jvm/rtdb/conformance/embedded_parser.clj", HERE / "jvm/rtdb/conformance/parser_wire.clj"])
    captured = {item["path"]: item["sha256"] for item in inputs}
    require(all(captured[str(path)] == checksum for path, checksum in (
        (FIXTURE, fixture_sha), (reference_source, REFERENCE_SOURCE["sha256"]), (supplemental_source, SUPPLEMENTAL_SOURCE["sha256"])
    )), "parser wire inputs changed while capturing provenance")
    args.output.mkdir(parents=True, exist_ok=False)
    args.created_output = True
    fixture = args.output / "fixture.json"
    fixture.write_bytes(FIXTURE.read_bytes())
    require(digest(fixture) == fixture_sha, "retained parser wire fixture changed")
    input_path = args.output / "inputs.json"
    metadata = {"reference_revision": REFERENCE, "implementation_revision": revision, "inputs": inputs,
                "reference_source": REFERENCE_SOURCE, "supplemental_source": SUPPLEMENTAL_SOURCE}
    write_json(input_path, metadata)
    retained, input_sha = read_receipt(input_path)
    require(fields_exact(retained, metadata), "retained parser wire inputs changed")
    hashes = {fixture: fixture_sha, input_path: input_sha}
    reference = observe(args, "jvm", fixture, fixture_sha, REFERENCE, hashes)
    native = observe(args, "rust", fixture, fixture_sha, revision, hashes)
    report = compare(reference, native)
    report.update(artifact_sha256={str(path.relative_to(args.output)): checksum for path, checksum in hashes.items()},
                  complete_parity=False)
    comparison = args.output / "comparison.json"
    write_json(comparison, report)
    retained, hashes[comparison] = read_receipt(comparison)
    require(fields_exact(retained, report), "retained wire comparison changed")
    verify_inputs(inputs)
    require_revision(args.reference, REFERENCE)
    require_revision(REPOSITORY, revision)
    require(all(digest(path) == checksum for path, checksum in hashes.items()), "wire receipt changed during verification")
    exact_match = report["diagnostic_exact_match"] and report["flight_follow_up"]["exact_match"]
    write_json(args.output / "result.json", {"comparison_completed": True, "complete_parity": False,
                                             "diagnostic_exact_match": report["diagnostic_exact_match"],
                                             "flight_follow_up_exact_match": report["flight_follow_up"]["exact_match"],
                                             "comparison_sha256": hashes[comparison]})
    return exact_match


def main():
    parser = argparse.ArgumentParser(description="Compare pinned JVM and native serialized parser diagnostics.")
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    args.reference, args.native, args.output = (path.resolve() for path in (args.reference, args.native, args.output))
    args.created_output = False
    try:
        return 0 if run(args) else 1
    except (OSError, ValueError, TypeError, KeyError, RuntimeError, BadZipFile, subprocess.SubprocessError) as error:
        if args.created_output:
            write_json(args.output / "failure.json", {"comparison_completed": False, "error": str(error)})
        print(str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
