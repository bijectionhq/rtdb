#!/usr/bin/env python3
"""Run unchanged language clients against independently packaged JVM and Rust servers."""

import argparse
import json
import re
import tarfile
import uuid
from pathlib import Path

from source_gate import require_phase_two
from client_results import compare_clients, parse_exunit, parse_junit, parse_libtest, parse_trx
from run_controls import HERE, REFERENCE, REPOSITORY, digest, git, owned_cancellation, run_process, write_json


CLIENTS = {"js": "js", "python": "python", "elixir": "elixr", "rust": "rust", "dotnet": "dotnet"}
LANES = [
    {"lane": "js", "service": "js", "entrypoint": "yarn", "argv": ["run", "test", "--reporter", "mocha-junit-reporter",
     "--reporter-options", "mochaFile=/evidence/js/results.xml,includePending=true"], "report": "results.xml", "parser": "junit"},
    {"lane": "python", "service": "python", "entrypoint": "poetry", "argv": ["run", "pytest", "--junitxml=/evidence/python/results.xml"],
     "report": "results.xml", "parser": "junit"},
    {"lane": "elixir", "service": "elixir", "entrypoint": "elixir", "argv": ["-r", "/adapters/formatter.ex", "-S", "mix", "test",
     "--formatter", "RTDB.ConformanceFormatter"], "report": "results.jsonl", "parser": "exunit"},
    {"lane": "rust-pg", "service": "rust", "entrypoint": "cargo", "argv": ["test", "--test", "integration_test", "--", "--test-threads=1", "--format=pretty", "--color=never"],
     "target": "integration_test", "parser": "libtest"},
    {"lane": "rust-flight", "service": "rust", "entrypoint": "cargo", "argv": ["test", "--test", "flight_sql_test", "--", "--test-threads=1", "--format=pretty", "--color=never"],
     "target": "flight_sql_test", "parser": "libtest"},
    {"lane": "dotnet", "service": "dotnet", "entrypoint": "dotnet", "argv": ["test", "--no-restore", "--logger", "trx;LogFileName=results.trx", "--results-directory", "/evidence/dotnet"],
     "report": "results.trx", "parser": "trx"},
]


class Commands:
    def __init__(self, directory, timeout):
        self.directory = directory
        self.timeout = timeout
        self.sequence = 0
        directory.mkdir()

    def run(self, name, argv, required=True, timeout=None):
        directory = self.directory / f"{self.sequence:04}-{name}"
        self.sequence += 1
        directory.mkdir()
        output, log = directory / "stdout.log", directory / "stderr.log"
        status = run_process(argv, REPOSITORY, output, log, timeout or self.timeout)
        write_json(directory / "process.json", status)
        if required and (not status["completed"] or status.get("exit_code") != 0):
            raise RuntimeError(f"{name} failed; inspect {directory}")
        return status, output


def image_record(commands, reference, name):
    _, output = commands.run("inspect-" + name, ["docker", "image", "inspect", reference])
    values = json.loads(output.read_bytes())
    if len(values) != 1 or not re.fullmatch(r"sha256:[0-9a-f]{64}", values[0].get("Id", "")):
        raise ValueError("Docker did not identify one immutable image")
    return values[0]


def validate_package(record, manifest, revision, engine):
    if manifest.get("schema_version") != 1 or manifest.get("artifact_kind") != "server_image":
        raise ValueError("client lane requires a server-image build provenance manifest")
    if manifest.get("revision") != revision or manifest.get("source_dirty") is not False:
        raise ValueError("server image provenance does not name the clean required revision")
    if manifest.get("image_id") != record["Id"] or manifest.get("engine") != engine:
        raise ValueError("server image does not match its build provenance")
    labels = record.get("Config", {}).get("Labels", {})
    source = {"rust": "https://github.com/bijectionhq/rtdb", "jvm": "https://github.com/xtdb/xtdb"}[engine]
    if labels.get("org.opencontainers.image.revision") != revision or labels.get("org.opencontainers.image.source") != source:
        raise ValueError("server image labels disagree with its source provenance")
    if engine == "rust" and (labels.get("io.rtdb.source-dirty") != "false" or labels.get("io.rtdb.test-hooks") != "false"):
        raise ValueError("native image is missing its production build labels")
    if manifest.get("test_hooks") is not False:
        raise ValueError("black-box client lane requires a package without test hooks")
    command = manifest.get("build_command")
    if not isinstance(command, list) or not command or any(not isinstance(value, str) or not value for value in command):
        raise ValueError("server image provenance lacks its build command argv")
    if not re.fullmatch(r"[0-9a-f]{64}", manifest.get("source_manifest_sha256", "")):
        raise ValueError("server image provenance lacks its build selection or source manifest digest")


def verify_native_build_info(artifacts, metadata, executables=("rtdb", "rtdb-config")):
    selected = metadata.get("tzdb")
    if not isinstance(selected, dict) or not isinstance(selected.get("sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", selected["sha256"]) or type(selected.get("bytes")) is not int or not 1 <= selected["bytes"] <= 1024 * 1024:
        raise ValueError("native build metadata lacks its bounded TZDB input digest")
    if digest(artifacts["artifacts/tzdb.dat"]) != selected["sha256"] or artifacts["artifacts/tzdb.dat"].stat().st_size != selected["bytes"]:
        raise ValueError("packaged TZDB bytes differ from the selected build input")
    reports = []
    for executable in executables:
        report = json.loads(artifacts["artifacts/" + executable + "-build-info.json"].read_bytes())
        expected = {"schema_version": 1, "engine": "rust", "revision": metadata["revision"], "version": metadata["version"], "tzdb_sha256": metadata["tzdb"]["sha256"]}
        if not isinstance(report, dict) or any(report.get(key) != value for key, value in expected.items()):
            raise ValueError(f"{executable} compiled metadata differs from the selected build input")
        if not isinstance(report.get("tzdb_version"), str) or not 1 <= len(report["tzdb_version"]) <= 128:
            raise ValueError(f"{executable} does not identify its compiled TZDB version")
        reports.append(report)
    if any(report != reports[0] for report in reports[1:]):
        raise ValueError("packaged executable build information differs")
    return reports[0]


def validate_native_package_artifacts(artifacts, manifest):
    required = {"artifacts/" + name for name in ["build.json", "rtdb", "rtdb-config", "tzdb.dat", "rtdb-build-info.json", "rtdb-config-build-info.json"]}
    if not required <= artifacts.keys():
        raise ValueError("native package lacks its executables or compiled provenance artifacts")
    metadata = json.loads(artifacts["artifacts/build.json"].read_bytes())
    if not isinstance(metadata, dict):
        raise ValueError("native package build metadata is not an object")
    for field in ("revision", "engine", "version", "source_dirty", "source_manifest_sha256", "features", "platform", "test_hooks", "tzdb"):
        if field not in metadata or metadata[field] != manifest.get(field):
            raise ValueError(f"native package build metadata differs at {field}")
    compiled = verify_native_build_info(artifacts, metadata)
    if compiled != manifest.get("compiled_build_info"):
        raise ValueError("native package manifest differs from its compiled executable reports")


def validate_package_files(path, manifest):
    directory = path.resolve().parent
    sources = directory / "source-manifest.json"
    if digest(sources) != manifest["source_manifest_sha256"]:
        raise ValueError("package source manifest differs from the build record")
    source = json.loads(sources.read_bytes())
    if source.get("revision") != manifest["revision"]:
        raise ValueError("package source manifest names a different revision")
    artifacts = {}
    for artifact in manifest.get("artifacts", []):
        selected = (directory / artifact["path"]).resolve()
        if not selected.is_relative_to(directory) or artifact["path"] in artifacts:
            raise ValueError("package artifact paths escape the evidence directory or repeat")
        if digest(selected) != artifact["sha256"] or selected.stat().st_size != artifact["bytes"]:
            raise ValueError("package artifact differs from the build record")
        artifacts[artifact["path"]] = artifact
    required = "artifacts/build.json" if manifest["engine"] == "rust" else "artifacts/xtdb.jar"
    if required not in artifacts:
        raise ValueError("package lacks its embedded provenance artifact")
    if manifest["engine"] == "rust":
        validate_native_package_artifacts({name: directory / name for name in artifacts}, manifest)
    return artifacts[required]


def verify_running_artifact(commands, container_id, package, directory):
    native = package["engine"] == "rust"
    source = "/usr/share/rtdb/build.json" if native else "/usr/local/lib/xtdb/xtdb.jar"
    output = directory / ("running-build.json" if native else "running-xtdb.jar")
    commands.run("running-artifact", ["docker", "cp", container_id + ":" + source, str(output)])
    artifact = package["provenance_artifact"]
    if digest(output) != artifact["sha256"] or output.stat().st_size != artifact["bytes"]:
        raise ValueError("running server artifact differs from its recorded package")
    if native:
        metadata = json.loads(output.read_bytes())
        for field in ("revision", "engine", "source_dirty", "source_manifest_sha256", "features", "platform", "test_hooks", "tzdb"):
            if field not in metadata or metadata[field] != package[field]:
                raise ValueError(f"running native build metadata differs at {field}")


def compose_manifest(reference, project, server_image, directory, client_images=None, adapters=None):
    mounts = [{"type": "bind", "source": str(directory / "reports"), "target": "/evidence"},
              {"type": "bind", "source": str(adapters or HERE / "clients"), "target": "/adapters", "read_only": True}]
    services = {"xtdb": {"image": server_image, "pull_policy": "never",
                "command": ["playground", "--port", "5439", "--flight-sql-port", "3000"],
                "environment": {"TZ": "UTC"}, "stop_grace_period": "30s"}}
    for service, context in CLIENTS.items():
        value = {"image": client_images[service]["Id"] if client_images else f"{project}-{service}:evidence",
                 "environment": {"PG_HOST": "xtdb", "PG_PORT": "5439", "FLIGHT_SQL_HOST": "xtdb", "FLIGHT_SQL_PORT": "3000",
                                 "TZ": "UTC", "RTDB_CLIENT_REPORT": "/evidence/elixir/results.jsonl"},
                 "volumes": mounts}
        if client_images:
            value["pull_policy"] = "never"
        else:
            value["build"] = {"context": str(reference / "lang" / context)}
        services[service] = value
    return {"services": services}


def report_cases(lane, output, directory):
    path = directory / "reports" / lane["lane"] / lane["report"] if "report" in lane else output
    raw = path.read_bytes()
    if lane["parser"] == "libtest":
        cases = parse_libtest(raw, lane["target"])
    else:
        cases = {"junit": parse_junit, "trx": parse_trx, "exunit": parse_exunit}[lane["parser"]](raw)
    return cases, {"path": str(path), "sha256": digest(path)}


def inspect_run(commands, project, lane, client_images):
    name = project + "-" + lane["lane"]
    _, output = commands.run("container-" + lane["lane"], ["docker", "inspect", name])
    records = json.loads(output.read_bytes())
    if len(records) != 1 or records[0]["Image"] != client_images[lane["service"]]["Id"]:
        raise ValueError("executed client container differs from the selected artifact")
    if records[0].get("Config", {}).get("Labels", {}).get("com.docker.compose.project") != project:
        raise ValueError("client container does not belong to this evidence run")
    return {"path": str(output), "sha256": digest(output)}


def run_engine(args, side, project, image, package, clients, directory):
    directory.mkdir()
    for lane in LANES:
        (directory / "reports" / lane["lane"]).mkdir(parents=True)
    manifest = directory / "compose.json"
    write_json(manifest, compose_manifest(args.reference, project, image["Id"], directory, clients, args.output / "client-adapters"))
    compose = ["docker", "compose", "--project-name", project, "--file", str(manifest)]
    commands = Commands(directory / "commands", args.timeout)
    results, cleanup_errors = [], []
    primary = None
    with owned_cancellation() as owner:
        try:
            commands.run("start-server", compose + ["up", "--detach", "--no-build", "xtdb"])
            commands.run("readiness", compose + ["run", "--no-deps", "--rm", "--entrypoint", "poetry", "python",
                                                "run", "python", "/adapters/ready.py"], timeout=120)
            _, server_id = commands.run("server-id", compose + ["ps", "--quiet", "xtdb"])
            identifier = server_id.read_text().strip()
            if not re.fullmatch(r"[0-9a-f]{12,64}", identifier):
                raise ValueError("server project has no unique running container")
            _, server_inspection = commands.run("server-container", ["docker", "inspect", identifier])
            containers = json.loads(server_inspection.read_bytes())
            if len(containers) != 1 or containers[0]["Image"] != image["Id"] or containers[0]["State"]["Running"] is not True:
                raise ValueError("running server differs from the selected packaged artifact")
            verify_running_artifact(commands, identifier, package, directory)
            for lane in LANES:
                process, output = commands.run(lane["lane"], compose + ["run", "--no-deps", "--name", project + "-" + lane["lane"],
                    "--entrypoint", lane["entrypoint"], lane["service"], *lane["argv"]], required=False)
                result = {"lane": lane["lane"], "service": lane["service"], "process": process, "cases": [],
                          "configuration": side + "/packaged-playground", "client_image_id": clients[lane["service"]]["Id"]}
                try:
                    result["container"] = inspect_run(commands, project, lane, clients)
                    result["cases"], result["report"] = report_cases(lane, output, directory)
                except (ValueError, OSError, RuntimeError) as error:
                    result["report_error"] = str(error)
                results.append(result)
                write_json(directory / "results.json", results)
                if not process["completed"]:
                    raise RuntimeError("client process ownership failed; remaining lanes were not started")
        except BaseException as error:
            primary = error
        finally:
            owner.begin_cleanup()
            for name, command in [("server-logs", compose + ["logs", "--no-color"]),
                                  ("shutdown", compose + ["down", "--volumes", "--remove-orphans", "--timeout", "30"])]:
                try:
                    commands.run(name, command, timeout=120)
                except BaseException as error:
                    cleanup_errors.append({"action": name, "error": str(error)})
            for resource, command in [("containers", ["docker", "ps", "--all", "--quiet"]),
                                      ("networks", ["docker", "network", "ls", "--quiet"]),
                                      ("volumes", ["docker", "volume", "ls", "--quiet"])]:
                try:
                    _, remaining = commands.run("remaining-" + resource, command + ["--filter", "label=com.docker.compose.project=" + project], timeout=30)
                    if remaining.read_text().strip():
                        cleanup_errors.append({"action": "drain-" + resource, "error": remaining.read_text()})
                except BaseException as error:
                    cleanup_errors.append({"action": "inspect-" + resource, "error": str(error)})
            write_json(directory / "lifecycle.json", {"project": project, "primary_error": str(primary) if primary else None,
                       "cleanup_errors": cleanup_errors, "drained": not cleanup_errors})
    if primary:
        raise primary
    if cleanup_errors:
        raise RuntimeError(f"{side} container resources did not drain; inspect {directory / 'lifecycle.json'}")
    return results


def source_manifest(reference):
    paths = git(reference, "ls-files", "lang", ".github/workflows/build.yml").splitlines()
    return [{"path": path, "sha256": digest(reference / path)} for path in paths]


def materialize_clients(commands, reference, directory):
    _, archive = commands.run("client-source-archive", ["git", "-C", str(reference), "archive", "--format=tar", REFERENCE, "--", "lang"])
    directory.mkdir()
    with tarfile.open(archive, "r:") as source:
        source.extractall(directory, filter="data")
    return {"archive_sha256": digest(archive), "revision": REFERENCE}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--reference-image", required=True)
    parser.add_argument("--native-image", required=True)
    parser.add_argument("--reference-provenance", type=Path, required=True)
    parser.add_argument("--native-provenance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1200)
    args = parser.parse_args()
    if args.timeout <= 0:
        raise ValueError("client process timeout must be positive")
    require_phase_two(REPOSITORY, args.phase_two)
    args.reference, args.output = args.reference.resolve(), args.output.resolve()
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    if git(args.reference, "rev-parse", "HEAD") != REFERENCE:
        raise ValueError("client source checkout is not the pinned reference")
    for repository in (args.reference, REPOSITORY):
        if git(repository, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("client qualification requires clean reference and implementation checkouts")
    args.output.mkdir(parents=True, exist_ok=False)
    try:
        with owned_cancellation():
            commands = Commands(args.output / "commands", args.timeout)
            commands.run("docker-version", ["docker", "version"])
            commands.run("compose-version", ["docker", "compose", "version"])
            images, packages = {}, {}
            for side, selected, provenance, expected in [("reference", args.reference_image, args.reference_provenance, REFERENCE),
                                                        ("native", args.native_image, args.native_provenance, revision)]:
                record = image_record(commands, selected, side)
                package = json.loads(provenance.read_bytes())
                validate_package(record, package, expected, "jvm" if side == "reference" else "rust")
                artifact = validate_package_files(provenance, package)
                images[side] = record
                write_json(args.output / (side + "-package.json"), {"manifest": package, "manifest_sha256": digest(provenance), "image": record})
                packages[side] = {**package, "provenance_artifact": artifact}
            if images["reference"]["Id"] == images["native"]["Id"]:
                raise ValueError("reference and native qualification require distinct server artifacts")
            sources = source_manifest(args.reference)
            write_json(args.output / "sources.json", sources)
            source_directory = args.output / "client-sources"
            write_json(args.output / "source-archive.json", materialize_clients(commands, args.reference, source_directory))
            adapters = args.output / "client-adapters"
            adapters.mkdir()
            adapter_sources = []
            for filename in ("formatter.ex", "ready.py"):
                source, destination = HERE / "clients" / filename, adapters / filename
                destination.write_bytes(source.read_bytes())
                adapter_sources.append({"path": str(source.relative_to(REPOSITORY)), "sha256": digest(destination)})
            write_json(args.output / "adapters.json", adapter_sources)
            project = "rtdb-clients-" + uuid.uuid4().hex[:16]
            build_manifest = args.output / "client-build.json"
            write_json(build_manifest, compose_manifest(source_directory, project, images["reference"]["Id"], args.output, adapters=adapters))
            commands.run("client-build", ["docker", "compose", "--project-name", project, "--file", str(build_manifest), "build", *CLIENTS])
            clients = {service: image_record(commands, f"{project}-{service}:evidence", service) for service in CLIENTS}
            write_json(args.output / "client-images.json", clients)
            reference = run_engine(args, "reference", project + "-jvm", images["reference"], packages["reference"], clients, args.output / "reference")
            native = run_engine(args, "native", project + "-rust", images["native"], packages["native"], clients, args.output / "native")
            if sources != source_manifest(args.reference) or git(REPOSITORY, "rev-parse", "HEAD") != revision:
                raise ValueError("source selection changed during client qualification")
            for repository in (args.reference, REPOSITORY):
                if git(repository, "status", "--porcelain", "--untracked-files=all"):
                    raise ValueError("source checkout changed during client qualification")
            report = compare_clients(reference, native, [lane["lane"] for lane in LANES])
            report.update(reference_revision=REFERENCE, implementation_revision=revision, selection=LANES,
                          source_manifest_sha256=digest(args.output / "sources.json"),
                          runner_sources=[{"path": str(path.relative_to(REPOSITORY)), "sha256": digest(path)} for path in
                                          [Path(__file__), HERE / "client_results.py", HERE / "run_controls.py", HERE / "clients/formatter.ex", HERE / "clients/ready.py"]])
            write_json(args.output / "comparison.json", report)
            return 0 if report["client_selection_passed"] else 1
    except BaseException as error:
        write_json(args.output / "failure.json", {"schema_version": 1, "complete_parity": False, "error": str(error)})
        raise


if __name__ == "__main__":
    raise SystemExit(main())
