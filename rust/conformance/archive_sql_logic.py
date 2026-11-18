#!/usr/bin/env python3

import argparse
import shutil
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory

from emit_sql_logic_case import SIDES, encode, verify_production_build
from external_inventory import Evidence, SUPPORT_REFERENCE, iter_sql_cases
from evidence import MAX_JSON_BYTES, artifact, closure, digest_value, index, integer, read_json, retained, sha256, text
from history_results import verify_runtime_artifacts
from sql_logic_selections import SELECTION_SOURCES


ADAPTER_INPUTS = ("run_sql_logic.py", "sql_logic_records.py", "sql_logic_selections.py", "sql_logic_results.py",
                  "run_controls.py", "history_results.py", "reference.init.gradle", "jvm/rtdb/conformance/sql_logic.clj")


def original_path(value):
    value = text(value, "original SQL logic path")
    path = PurePosixPath(value)
    if path.root != "/" or str(path) != value or ".." in path.parts:
        raise ValueError("original SQL logic path must be absolute and normalized")
    return path


def relative_path(value):
    value = text(value, "relative SQL logic path")
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or str(path) != value or ".." in path.parts:
        raise ValueError("relative SQL logic path escapes its recorded root")
    return path


def copy_checked(source, destination, expected):
    source = Path(source)
    digest = digest_value(expected.get("sha256"), "captured artifact digest")
    if source.resolve() != source or not source.is_file():
        raise ValueError(f"captured artifact is missing or redirected: {source}")
    size = integer(expected.get("bytes", source.stat().st_size), "captured artifact size")
    if source.stat().st_size != size:
        raise ValueError(f"captured artifact size differs: {source}")
    if destination.exists():
        if sha256(source) != digest:
            raise ValueError(f"captured artifact digest differs: {source}")
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    if destination.stat().st_size != size or sha256(destination) != digest:
        raise ValueError(f"captured artifact digest or size differs: {source}")
    return {"sha256": digest, "bytes": size}


class CapturedFiles:
    def __init__(self, root, recorded_root, run_files):
        self.root = root
        self.entries = {str(recorded_root / entry["path"]): entry for entry in run_files}

    def add(self, original, source, expected):
        original = str(original)
        digest = digest_value(expected.get("sha256"), "captured artifact digest")
        previous = self.entries.get(original)
        if previous is not None and (previous["sha256"] != digest or expected.get("bytes", previous["bytes"]) != previous["bytes"]):
            raise ValueError(f"conflicting retained bytes for original path: {original}")
        location = previous["path"] if previous is not None else f"payload/{digest}"
        verified = copy_checked(source, self.root / location, expected)
        self.entries[original] = {"path": location, **verified}
        return {"path": original, **verified}


def invocations(root):
    processes = {side: read_json(root / side / "process.json") for side in SIDES}
    native = processes["native"]["command"]
    if not isinstance(native, list) or len(native) != 4:
        raise ValueError("native SQL logic invocation has different operands")
    binary = original_path(native[0])
    plan = original_path(native[2])
    run = plan.parent.parent
    repository = original_path(processes["native"]["cwd"])
    reference = original_path(processes["reference"]["cwd"])
    if native != [str(binary), "sql-logic", str(run / "plan/plan.json"), str(run / "native/observations")]:
        raise ValueError("native SQL logic invocation has different operands")
    adapter = repository / "rust/conformance"
    for side in ("reference", "reference-repeat"):
        expected = [str(reference / "gradlew"), "--no-daemon", "--console=plain", "-PfullJdk", "--init-script",
                    str(adapter / "reference.init.gradle"), "rtdbConformanceSqlLogic", f"-Prtdb.adapterRoot={adapter / 'jvm'}",
                    f"-Prtdb.fixture={plan}", f"-Prtdb.output={run / side / 'observations'}"]
        if processes[side]["command"] != expected or processes[side]["cwd"] != str(reference):
            raise ValueError(f"{side} SQL logic invocation has different operands")
    return run, repository, reference, binary


def check_result(root):
    result = read_json(root / "result.json")
    if (result.get("complete_parity") is not False or result.get("assertion_ledger_reconciled") is not False
            or result.get("inputs_sha256") != sha256(root / "inputs.json")):
        raise ValueError("SQL logic result differs from the retained input receipt")
    selections = []
    for name in ("repeat", "comparison"):
        path = root / f"{name}.json"
        summary = read_json(path)
        if (type(summary.get("selection_passed")) is not bool or result.get(f"{name}_sha256") != sha256(path)
                or summary.get("record_comparison_sha256") != sha256(root / f"{name}-records.jsonl")):
            raise ValueError(f"SQL logic {name} summary differs from its retained records")
        selections.append(summary["selection_passed"])
    if result.get("selection_passed") is not all(selections):
        raise ValueError("SQL logic result disagrees with its retained summaries")


def capture_production(files, build_path, binary):
    build = read_json(build_path)
    if (build.get("selection") != {"variant": "local", "features": []}
            or build.get("configuration") not in {"linux-amd64/rust/local/production", "linux-arm64/rust/local/production"}):
        raise ValueError("SQL logic archive requires a local production build")
    selected = index(build["binaries"], lambda value: value["path"], "production executable").get(str(binary))
    if selected is None:
        raise ValueError("SQL logic execution lacks its selected production executable")
    selected_file = selected["retained_artifact"]
    location = relative_path(selected_file["path"])
    if len(location.parts) != 2 or location.parts[0] != "binaries" or binary.name != location.name or binary.parent.name != "binaries":
        raise ValueError("SQL logic execution has a different ordinal production copy")
    original = binary.parent.parent
    manifest = files.add(original / "production-artifacts.json", build_path, retained(build_path.parent, build_path))
    for key, filename in (("process", "cargo.process.json"), ("messages", "cargo.stdout.log")):
        entry = build[key]
        if entry["path"] != filename:
            raise ValueError("production build has different Cargo receipt paths")
        files.add(original / filename, build_path.parent / filename, entry)
    executable = files.add(binary, build_path.parent / location, selected_file)
    if executable["sha256"] != selected["sha256"] or executable["bytes"] != selected["bytes"]:
        raise ValueError("SQL logic execution has different retained production executable bytes")
    return manifest, executable


def capture_inputs(files, plan, inputs, selection, roots, binary):
    run, repository, reference = roots
    adapter = repository / "rust/conformance"
    if (plan.get("schema_version") != 1 or plan.get("reference") != SUPPORT_REFERENCE
            or inputs.get("reference_revision") != SUPPORT_REFERENCE or selection.get("reference") != SUPPORT_REFERENCE
            or plan.get("selection") != "+".join(selection["names"])):
        raise ValueError("SQL logic inputs differ from the pinned reference or plan selection")
    corpus = index(selection["sources"], lambda value: value["path"], "SQL logic corpus source")
    sources = {}
    records = {}
    for script in plan["scripts"]:
        source = relative_path(script["source"])
        if not source.is_relative_to("sqlite-test") and not source.is_relative_to("src/test/resources/xtdb/sql/logic_test"):
            raise ValueError("SQL logic script lies outside the pinned source directories")
        name = str(source)
        digest = script["source_sha256"]
        if sources.setdefault(name, digest) != digest:
            raise ValueError("SQL logic source has conflicting recorded digests")
        if source.suffix == ".test" and (name not in corpus or corpus[name].get("selected") is not True or corpus[name]["sha256"] != digest):
            raise ValueError("SQL logic source differs from the recorded selection")
        record = relative_path(script["records_file"])
        if len(record.parts) != 1 or record in records:
            raise ValueError("SQL logic plan repeats or redirects a record stream")
        records[record] = script["records_sha256"]
    if not sources or {name for name, entry in corpus.items() if entry.get("selected") is True} != {name for name in sources if PurePosixPath(name).suffix == ".test"}:
        raise ValueError("SQL logic plan and selection name different original sources")

    expected = {str(adapter / path) for path in ADAPTER_INPUTS}
    expected.update(str(reference / path) for path in [*SELECTION_SOURCES, *sources])
    expected.update({str(run / "plan/plan.json"), str(run / "selection.json"), binary["path"]})
    expected.update(str(run / "plan" / name) for name in records)
    declared = index(inputs["inputs"], lambda value: value["path"], "SQL logic input")
    if set(declared) != expected:
        raise ValueError("SQL logic input receipt names a different set of runner inputs")
    for name, digest in records.items():
        if declared[str(run / "plan" / name)]["sha256"] != digest:
            raise ValueError("SQL logic record stream differs from the input receipt")
    for name, digest in sources.items():
        if declared[str(reference / name)]["sha256"] != digest:
            raise ValueError("SQL logic original source differs from the input receipt")
    if declared[binary["path"]]["sha256"] != binary["sha256"]:
        raise ValueError("SQL logic input receipt identifies a different native executable")
    return sources, declared


def capture_runtime(files):
    for side in ("reference", "reference-repeat"):
        observations = files.root / side / "observations"
        execution = read_json(observations / "execution.json")
        path = observations / "runtime-artifacts.json"
        runtime = read_json(path)
        if execution.get("binary_sha256") != sha256(path) or execution.get("artifacts") != runtime:
            raise ValueError(f"SQL logic {side} has a different JVM runtime closure")
        for entry in runtime:
            original_path(entry["path"])
            if entry["kind"] == "directory":
                for member in entry["files"]:
                    relative_path(member["path"])
        verify_runtime_artifacts(runtime)
        for entry in runtime:
            origin = Path(entry["path"])
            if entry["kind"] == "file":
                files.add(origin, origin, entry)
            else:
                for member in entry["files"]:
                    path = origin / member["path"]
                    files.add(path, path, member)


def archive_sql_logic(run_dir, production_build, reference, output, run_id="sql-logic"):
    """Publish a completed worker run and its live inputs, returning the portable EXTERNAL.json path.

    The output directory must be outside the run, production manifest and reference directories, under an existing parent.
    An existing output raises FileExistsError.
    Missing or inconsistent evidence prevents publication.
    """
    run_dir = Path(run_dir).resolve()
    production_build = Path(production_build).resolve()
    reference = Path(reference).resolve()
    output = Path(output)
    destination = output.parent.resolve() / output.name
    text(run_id, "SQL logic archive run ID")
    if production_build.name != "production-artifacts.json" or not run_dir.is_dir() or not reference.is_dir():
        raise ValueError("SQL logic archive requires a run directory, production-artifacts.json and a pinned source directory")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    if any(destination.is_relative_to(root) for root in (run_dir, production_build.parent, reference)):
        raise ValueError("SQL logic archive output must stay outside the captured directories")

    with TemporaryDirectory(dir=destination.parent, prefix=".sql-logic-archive-") as temporary:
        staged = Path(temporary) / "archive"
        root = staged / "capture"
        root.mkdir(parents=True)
        run_files = closure(run_dir)
        for entry in run_files:
            copy_checked(run_dir / entry["path"], root / entry["path"], entry)
        recorded_root, repository, recorded_reference, binary = invocations(root)
        files = CapturedFiles(root, recorded_root, run_files)
        check_result(root)
        plan = read_json(root / "plan/plan.json")
        inputs = read_json(root / "inputs.json")
        selection = read_json(root / "selection.json")
        implementation = text(inputs.get("implementation_revision"), "native implementation revision")
        manifest, executable = capture_production(files, production_build, binary)
        sources, declared = capture_inputs(files, plan, inputs, selection, (recorded_root, repository, recorded_reference), executable)
        reference_inputs = {recorded_reference / path for path in (*SELECTION_SOURCES, *sources)}
        for name, entry in declared.items():
            original = original_path(name)
            if original == binary or original.is_relative_to(recorded_root):
                continue
            if original in reference_inputs:
                source = reference / original.relative_to(recorded_reference)
            else:
                source = Path(original)
            files.add(original, source, entry)
        capture_runtime(files)

        run = {"id": run_id, "kind": "sql_logic", "root": "capture", "recorded_root": str(recorded_root),
               "native_build": manifest, "native_binary": executable, "artifacts": closure(root), "bindings": [],
               "relocations": [{"original": original, "artifact": entry["path"]} for original, entry in sorted(files.entries.items())],
               "sources": [{"source": name, "artifact": {**files.entries[str(recorded_reference / name)], "path": str(recorded_reference / name)}}
                           for name in sorted(sources)]}
        evidence = Evidence(staged, run)
        for entry in declared.values():
            evidence.recorded(entry)
        for side in SIDES:
            if evidence.read(f"{side}/observations/execution.json").get("selection") != plan["selection"]:
                raise ValueError(f"SQL logic {side} execution uses a different selection")
        verify_production_build(evidence, run, binary)
        for _ in iter_sql_cases(evidence, run, SUPPORT_REFERENCE, implementation):
            pass

        document = {"schema_version": 1, "reference_revision": SUPPORT_REFERENCE, "implementation_revision": implementation,
                    "runs": [run], "files": []}
        payload = encode(document)
        if len(payload) > MAX_JSON_BYTES:
            raise ValueError("SQL logic archive manifest exceeds the evidence reader limit")
        manifest_path = staged / "EXTERNAL.json"
        manifest_path.write_bytes(payload)
        destination.mkdir()
        try:
            capture = destination / "capture"
            root.rename(capture)
            try:
                (destination / "EXTERNAL.json").hardlink_to(manifest_path)
            except BaseException:
                capture.rename(root)
                raise
        except BaseException:
            destination.rmdir()
            raise
    return destination / "EXTERNAL.json"


def main():
    parser = argparse.ArgumentParser(description="Capture a completed SQL logic worker run and its live input bytes.")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--production-build", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--id", default="sql-logic")
    args = parser.parse_args()
    archive_sql_logic(args.run_dir, args.production_build, args.reference, args.output, args.id)


if __name__ == "__main__":
    main()
