#!/usr/bin/env python3

"""Capture SQL-13 and T11 through binary pgwire on the pinned packaged runtimes."""

import argparse
import math
import sys
import uuid
from pathlib import Path

from numeric_results import compare
from run_clients import Commands, image_record, inspect_run, materialize_clients, validate_package, validate_package_files
from run_controls import HERE, REFERENCE, REPOSITORY, digest, git, owned_cancellation, write_json
from run_history import input_receipt, verify_inputs
from run_mixed_iid_reference import require_revision
from run_performance import clean_stop, compose_plan, inspect_server
from source_gate import require_phase_two

sys.path.insert(0, str(HERE.parent / "release"))
from evidence import read_json


FIXTURE = HERE / "fixtures/numeric-differential.json"
CONFIG = HERE / "configurations/numeric-local.yaml"
DATA = "/var/lib/xtdb"
SERVER = "xtdb"


def observe(args, engine, package, client, resources, adapters, fixture_sha):
    directory = args.output / engine
    directory.mkdir()
    project = "rtdb-numeric-" + uuid.uuid4().hex[:20]
    environment = ({"JDK_JAVA_OPTIONS": "-Xmx4g -XX:MaxDirectMemorySize=4g"} if engine == "jvm"
                   else {"RTDB_MEMORY_LIMIT_BYTES": str(8 * 1024**3)})
    node = {"name": SERVER, "command": ["-f", "/configuration.yaml"], "data_path": DATA,
            "configuration": {"path": str(CONFIG.relative_to(HERE)), "sha256": digest(CONFIG)},
            "pg_port": 5432, "flight_port": 3000, "health_port": 8080, "database": SERVER, "user": SERVER,
            "environment": environment}
    manifest, endpoints = compose_plan({"resources": resources}, {"nodes": [node]}, package, client["Id"],
                                       directory, adapters, project, HERE)
    manifest["services"]["measure"]["environment"].update(PG_HOST=SERVER, PG_PORT="5432")
    manifest_path = directory / "compose.json"
    write_json(manifest_path, manifest)
    compose = ["docker", "compose", "--project-name", project, "--file", str(manifest_path)]
    commands = Commands(directory / "commands", args.timeout)
    request_path = directory / "request.json"
    write_json(request_path, {"engine": engine, "node": SERVER, "endpoint": endpoints[SERVER], "fixture_sha256": fixture_sha})
    request_sha = digest(request_path)
    run_client = compose + ["run", "--no-deps"]
    ready_name = project + "-ready"
    client_name = project + "-numeric"
    owned_containers = ["docker", "ps", "--all", "--quiet", "--no-trunc",
                        "--filter", "label=com.docker.compose.project=" + project]
    primary, cleanup = None, []
    with owned_cancellation() as owner:
        try:
            commands.run("start-server", compose + ["up", "--detach", "--no-build", SERVER])
            commands.run("ready", run_client + ["--rm", "--name", ready_name, "--entrypoint", "poetry", "measure", "run", "python",
                                                "/adapters/ready.py"], timeout=120)
            identifier = inspect_server(commands, compose, SERVER, package, resources, directory, "before")
            commands.run("engine-platform", ["docker", "exec", identifier, "uname", "-a"])
            commands.run("engine-os", ["docker", "exec", identifier, "cat", "/etc/os-release"])
            if engine == "jvm":
                commands.run("engine-jvm", ["docker", "exec", identifier, "java", "-version"])
            commands.run("numeric", run_client + ["--name", client_name, "--entrypoint", "poetry", "measure",
                                                  "run", "python", "/adapters/numeric.py", "/evidence/request.json",
                                                  "/adapters/fixture.json", "/evidence/trace.json"])
            inspect_run(commands, project, {"lane": "numeric", "service": "measure"}, {"measure": client})
            if inspect_server(commands, compose, SERVER, package, resources, directory, "after") != identifier:
                raise ValueError("numeric trace's server container changed")
        except BaseException as error:
            primary = error
        finally:
            owner.begin_cleanup()
            for name in (ready_name, client_name):
                try:
                    _, matches = commands.run("owned-client", owned_containers + ["--filter", f"name=^/{name}$"], timeout=30)
                    identifiers = matches.read_text().split()
                    if identifiers:
                        commands.run("remove-owned-client", ["docker", "rm", "--force", *identifiers], timeout=60)
                except BaseException as error:
                    cleanup.append(error)
            try:
                clean_stop(commands, compose, [SERVER], engine)
            except BaseException as error:
                cleanup.append(error)
            for action, command in [
                ("server-logs", compose + ["logs", "--no-color"]),
                ("remove-project", compose + ["down", "--volumes", "--remove-orphans", "--timeout", "60"]),
            ]:
                try:
                    commands.run(action, command, timeout=180)
                except BaseException as error:
                    cleanup.append(error)
            for resource, command in [
                ("containers", ["docker", "ps", "--all", "--quiet"]),
                ("networks", ["docker", "network", "ls", "--quiet"]),
                ("volumes", ["docker", "volume", "ls", "--quiet"]),
            ]:
                try:
                    _, remaining = commands.run("drain-" + resource, command + ["--filter", "label=com.docker.compose.project=" + project])
                    if remaining.read_text().strip():
                        raise ValueError(f"numeric project retained {resource}")
                except BaseException as error:
                    cleanup.append(error)
            write_json(directory / "lifecycle.json", {"project": project, "drained": not cleanup,
                       "primary_error": str(primary) if primary else None, "cleanup_errors": list(map(str, cleanup))})
    if primary is not None or cleanup:
        raise BaseExceptionGroup("numeric execution or cleanup failed", ([primary] if primary is not None else []) + cleanup)
    trace = read_json(directory / "trace.json")
    if (digest(request_path) != request_sha or trace.get("schema_version") != 1 or trace.get("engine") != engine
            or trace.get("request_sha256") != request_sha or trace.get("fixture_sha256") != fixture_sha
            or trace.get("client_closed") is not True):
        raise ValueError(f"{engine} numeric receipt omitted request or client boundary evidence")
    return trace, digest(directory / "trace.json")


def run(args):
    require_phase_two(REPOSITORY, args.phase_two)
    if not math.isfinite(args.timeout) or args.timeout <= 0 or args.cpus <= 0:
        raise ValueError("numeric timeout and CPU count must be positive and finite")
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    require_revision(REPOSITORY, revision)
    require_revision(args.reference, REFERENCE)
    if any(args.output.is_relative_to(root) for root in (REPOSITORY, args.reference)):
        raise ValueError("numeric workspace must be outside both checkouts")
    args.output.mkdir(parents=True, exist_ok=False)
    try:
        with owned_cancellation():
            return execute(args, revision)
    except BaseException as error:
        write_json(args.output / "failure.json", {"schema_version": 1, "complete_parity": False, "error": str(error)})
        raise


def execute(args, revision):
    commands = Commands(args.output / "commands", args.timeout)
    _, host = commands.run("actual-docker-architecture", ["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"])
    architecture = host.read_text().strip().replace("x86_64", "amd64").replace("aarch64", "arm64")
    if architecture not in {"linux/amd64", "linux/arm64"}:
        raise ValueError("numeric qualification requires a native Linux Docker worker")
    resources = {"architecture": architecture, "memory_bytes": 12 * 1024**3, "cpu_count": args.cpus, "cpu_set": ""}
    inputs = [Path(__file__).resolve(), FIXTURE, CONFIG, HERE / "upstream-commit.txt", HERE / "numeric_results.py",
              HERE / "run_clients.py", HERE / "run_controls.py", HERE / "run_performance.py",
              HERE / "run_history.py", HERE / "run_mixed_iid_reference.py", HERE / "packaged_lifecycle.py",
              HERE / "source_gate.py", HERE.parent / "release/evidence.py"]
    packages = {}
    for engine, package_path, expected in (("jvm", args.reference_package, REFERENCE),
                                           ("rust", args.native_package, revision)):
        package = read_json(package_path)
        image = image_record(commands, package["image_id"], engine)
        validate_package(image, package, expected, engine)
        artifact = validate_package_files(package_path, package)
        if package["platform"] != architecture or image["Os"] + "/" + image["Architecture"] != architecture:
            raise ValueError("server image architecture differs from the Docker worker")
        packages[engine] = {**package, "provenance_artifact": artifact}
        write_json(args.output / f"{engine}-package.json", packages[engine])
        inputs.extend([package_path, package_path.parent / "source-manifest.json",
                       *(package_path.parent / item["path"] for item in package["artifacts"])])
    sources = args.output / "client-sources"
    write_json(args.output / "client-source.json", materialize_clients(commands, args.reference, sources))
    iid = args.output / "client-image.id"
    commands.run("build-shared-client", ["docker", "build", "--platform", architecture, "--iidfile", str(iid), str(sources / "lang/python")])
    client = image_record(commands, iid.read_text().strip(), "numeric-client")
    if client["Os"] + "/" + client["Architecture"] != architecture:
        raise ValueError("numeric client architecture differs from the Docker worker")
    adapters = args.output / "adapters"
    adapters.mkdir()
    for name, source in [("ready.py", HERE / "clients/ready.py"), ("numeric.py", HERE / "clients/numeric.py"), ("fixture.json", FIXTURE)]:
        target = adapters / name
        target.write_bytes(source.read_bytes())
        if digest(target) != digest(source):
            raise ValueError("numeric adapter differs from its checked-in source")
        inputs.extend([source, target])
    captured = input_receipt(dict.fromkeys(inputs))
    write_json(args.output / "inputs.json", captured)
    fixture_sha = digest(adapters / "fixture.json")
    fixture = read_json(adapters / "fixture.json")
    if fixture["schema_version"] != 1 or fixture["inventory_revision"] != (HERE / "upstream-commit.txt").read_text().strip():
        raise ValueError("numeric fixture differs from the inventory pin")
    observations = {engine: observe(args, engine, package, client, resources, adapters, fixture_sha)
                    for engine, package in packages.items()}
    verify_inputs(captured)
    require_revision(args.reference, REFERENCE)
    require_revision(REPOSITORY, revision)
    report = compare(fixture, {engine: trace for engine, (trace, _) in observations.items()})
    report.update(reference_revision=REFERENCE, inventory_revision=fixture["inventory_revision"],
                  implementation_revision=revision, client_image_id=client["Id"], fixture_sha256=fixture_sha,
                  retained_sha256={str(path.relative_to(args.output)): digest(path) for path in [
                      args.output / "inputs.json", args.output / "client-source.json",
                      *(args.output / engine / filename for engine in packages
                        for filename in ("trace.json", "request.json", "compose.json", "lifecycle.json")),
                      *(args.output / f"{engine}-package.json" for engine in packages)]})
    if any(report["retained_sha256"][f"{engine}/trace.json"] != sha for engine, (_, sha) in observations.items()):
        raise ValueError("numeric trace changed after comparison input was read")
    write_json(args.output / "comparison.json", report)
    return 0 if report["SQL-13"] and report["T11"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase-two", action="store_true")
    for name in ("reference", "reference-package", "native-package", "output"):
        parser.add_argument("--" + name, type=lambda path: Path(path).resolve(), required=True)
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--cpus", type=int, default=4)
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
