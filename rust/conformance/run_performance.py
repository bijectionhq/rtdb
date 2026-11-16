#!/usr/bin/env python3

import argparse
import json
import re
import sys
import time
import uuid
from pathlib import Path

from run_controls import HERE, REFERENCE, REPOSITORY, digest, git, owned_cancellation, write_json
from run_clients import Commands, image_record, materialize_clients, validate_package, validate_package_files, verify_running_artifact
from performance_plan import operation_trace_digest, validate_plan
from performance_results import PROVIDER_BOUNDARY, compare
from packaged_lifecycle import validate_stopped
from filesystem_observer import FILESYSTEM_BOUNDARY, FilesystemObserver, nonoverlapping_target, verify_evidence
sys.path.insert(0, str(HERE.parent / "release"))
from evidence import artifact, read_json, retained


def compose_plan(plan, selected, package, client_image, directory, adapters, project, root, filesystem_mount=None):
    services = {}
    volumes = {}
    endpoints = {}
    resources = plan["resources"]
    for node in selected["nodes"]:
        name = node["name"]
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,40}", name):
            raise ValueError("node names must be Docker DNS names")
        configuration = artifact(root, node["configuration"])
        volume = node.get("data_volume_group", name + "-data")
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,50}", volume):
            raise ValueError("invalid benchmark volume identity")
        mounts = [{"type": "bind", "source": str(configuration), "target": "/configuration.yaml", "read_only": True},
                  {"type": "volume", "source": volume, "target": node["data_path"]}]
        for credential in node.get("credential_files", []):
            mounts.append({"type": "bind", "source": str(artifact(root, credential)), "target": credential["target"], "read_only": True})
        if filesystem_mount is not None:
            for mount in mounts:
                nonoverlapping_target(mount["target"])
            mounts.append({"type": "bind", "source": str(filesystem_mount), "target": "/measured-objects"})
        environment = {"TZ": "UTC", "RTDB_RUN_ID": project, **node.get("environment", {})}
        if environment["RTDB_RUN_ID"] != project:
            raise ValueError("benchmark namespace cannot override its invocation identity")
        services[name] = {"image": package["image_id"], "platform": resources["architecture"], "pull_policy": "never", "command": node["command"],
            "environment": environment, "volumes": mounts, "mem_limit": resources["memory_bytes"],
            "memswap_limit": resources["memory_bytes"], "cpus": resources["cpu_count"], "cpuset": resources["cpu_set"],
            "stop_grace_period": "60s", "healthcheck": {"disable": True}}
        volumes[volume] = {}
        endpoints[name] = {field: node[field] for field in ("pg_port", "flight_port", "health_port", "database", "user")}
    services["measure"] = {"image": client_image, "platform": resources["architecture"], "pull_policy": "never", "environment": {"TZ": "UTC"},
        "mem_limit": resources["memory_bytes"], "memswap_limit": resources["memory_bytes"], "cpus": resources["cpu_count"],
        "volumes": [{"type": "bind", "source": str(directory), "target": "/evidence"},
                    {"type": "bind", "source": str(adapters), "target": "/adapters", "read_only": True}]}
    for node in selected["nodes"]:
        if node.get("client_password"):
            reference = node["client_password"]
            target = "/secrets/" + node["name"]
            services["measure"]["volumes"].append({"type": "bind", "source": str(artifact(root, reference)), "target": target, "read_only": True})
            endpoints[node["name"]]["password_file"] = target
    measurement = selected.get("provider_measurement")
    if measurement and measurement.get("kind") == "owned_http_observer":
        observer = artifact(root, measurement["observer_artifact"])
        if digest(observer) != digest(HERE / "clients/provider_observer.py"):
            raise ValueError("owned observer must identify the committed measurement implementation")
        configuration = {**measurement["configuration"], "namespace": project, "storage_placement": resources["storage_placement"]}
        services["object-observer"] = {"image": client_image, "platform": resources["architecture"], "pull_policy": "never",
            "entrypoint": "poetry", "command": ["run", "python", "/adapters/provider_observer.py"],
            "environment": {"RTDB_OBSERVER_CONFIGURATION": json.dumps(configuration)}, "stop_grace_period": "330s",
            "mem_limit": resources["memory_bytes"], "memswap_limit": resources["memory_bytes"],
            "volumes": [{"type": "bind", "source": str(adapters), "target": "/adapters", "read_only": True}]}
        if measurement.get("ca_file"):
            ca = artifact(root, measurement["ca_file"])
            services["object-observer"]["volumes"].append({"type": "bind", "source": str(ca), "target": "/observer-ca.pem", "read_only": True})
            configuration["ca_file"] = "/observer-ca.pem"
            services["object-observer"]["environment"]["RTDB_OBSERVER_CONFIGURATION"] = json.dumps(configuration)
    return {"services": services, "volumes": volumes}, endpoints


def call_client(commands, compose, directory, case, endpoints, phase, sequence):
    request = directory / f"request-{sequence:05d}.json"
    output = directory / f"client-{sequence:05d}.json"
    write_json(request, {"schema_version": 1, "case": case, "endpoints": endpoints, "phase": phase,
                        "max_result_rows": 1_000_000, "max_result_bytes": 16 * 1024 * 1024, "max_receipt_bytes": 256 * 1024 * 1024})
    process, _ = commands.run(f"client-{phase}", compose + ["run", "--no-deps", "--rm", "--entrypoint", "poetry", "measure", "run", "python", "/adapters/performance.py",
        "/evidence/" + request.name, "/evidence/" + output.name])
    value = read_json(output)
    if value.get("request_sha256") != digest(request) or value.get("execution_complete") is not True or value.get("client_closed") is not True or not process["completed"]:
        raise ValueError("client receipt does not identify completed, closed execution of this request")
    return value


def inspect_server(commands, compose, name, package, resources, directory, sequence):
    _, ids = commands.run("server-id-" + name, compose + ["ps", "--quiet", name])
    identifier = ids.read_text().strip()
    if not re.fullmatch(r"[0-9a-f]{12,64}", identifier):
        raise ValueError("benchmark server has no unique running container")
    expression = '{"image":{{json .Image}},"state":{{json .State}},"memory":{{.HostConfig.Memory}},"swap":{{.HostConfig.MemorySwap}},"cpus":{{.HostConfig.NanoCpus}},"cpuset":{{json .HostConfig.CpusetCpus}}}'
    _, output = commands.run("inspect-" + name, ["docker", "inspect", "--format", expression, identifier])
    value = read_json(output)
    if value["image"] != package["image_id"] or value["state"]["Running"] is not True:
        raise ValueError("performance server differs from selected running artifact")
    if value["memory"] != resources["memory_bytes"] or value["swap"] != resources["memory_bytes"] or value["cpus"] != resources["cpu_count"] * 1_000_000_000 or value["cpuset"] != resources["cpu_set"]:
        raise ValueError("actual container resource settings differ from matched plan")
    identity_dir = directory / f"artifact-{name}-{sequence}"
    identity_dir.mkdir()
    verify_running_artifact(commands, identifier, package, identity_dir)
    return identifier


def resources(commands, client_image, adapters, identifier, project):
    _, output = commands.run("engine-process-resources", ["docker", "run", "--rm", "--pid=container:" + identifier,
        "--label", "com.docker.compose.project=" + project, "--label", "com.docker.compose.service=resource-observer",
        "--user", "20000:20000", "--mount", f"type=bind,source={adapters},target=/adapters,readonly", "--entrypoint", "poetry",
        client_image, "run", "python", "/adapters/resources.py"])
    return read_json(output)


def provider_sample(commands, compose, measurement, project, filesystem=None):
    if measurement is None:
        return None
    if measurement["kind"] == "owned_filesystem_observer":
        if filesystem is None:
            raise ValueError("filesystem measurement has no owning mounted observer")
        return filesystem.sample()
    url = (f"http://object-observer:{measurement['configuration']['port']}/__rtdb_observer__" if measurement.get("kind") == "owned_http_observer"
           else measurement["url"].replace("{namespace}", project))
    if not url.startswith(("http://", "https://")) or "@" in url:
        raise ValueError("provider observer URL is invalid or contains credentials")
    code = "import json,sys,urllib.request; r=urllib.request.urlopen(sys.argv[1],timeout=30); b=r.read(1048577); assert len(b)<=1048576; json.dump(json.loads(b),sys.stdout)"
    deadline = time.monotonic() + 330
    while True:
        _, output = commands.run("provider-counters", compose + ["run", "--no-deps", "--rm", "--entrypoint", "poetry", "measure", "run", "python", "-c", code, url])
        value = read_json(output)
        if value.get("namespace") != project or value.get("boundary") != PROVIDER_BOUNDARY or value.get("observer_artifact_sha256") != measurement["observer_artifact"]["sha256"]:
            raise ValueError("provider counters do not identify this isolated namespace and observer")
        if type(value.get("in_flight_requests")) is not int or value["in_flight_requests"] < 0:
            raise ValueError("provider observer omitted its admitted request boundary")
        if value["in_flight_requests"] == 0:
            return value
        if time.monotonic() >= deadline:
            raise ValueError("provider requests did not reach a quiescent measurement boundary")


def provider_delta(before, after, plan):
    if before is None or after is None:
        return None
    if before.get("storage_placement") != plan["resources"]["storage_placement"] or after.get("storage_placement") != before["storage_placement"]:
        raise ValueError("provider observer placement differs from matched resource contract")
    identity_fields = ("boundary", "observer_artifact_sha256", "storage_placement", "measurement_configuration_sha256", "physical_placement")
    if any(before.get(key) != after.get(key) for key in identity_fields) or before["boundary"] not in {PROVIDER_BOUNDARY, FILESYSTEM_BOUNDARY}:
        raise ValueError("provider identity changed across measurement")
    result = {key: before.get(key) for key in identity_fields}
    for field in ("successful_read_bytes", "successful_read_requests", "successful_write_bytes", "successful_write_requests",
                  "failed_requests", "failed_request_bytes", "failed_response_bytes"):
        if type(before.get(field)) is not int or type(after.get(field)) is not int or not 0 <= before[field] <= after[field]:
            raise ValueError("provider counter regressed or is not a nonnegative integer")
        result[field] = after[field] - before[field]
    for field in ("rejected_requests", "rejected_connections"):
        if type(before.get(field)) is not int or type(after.get(field)) is not int or not 0 <= before[field] <= after[field]:
            raise ValueError("provider rejection counter regressed or is missing")
        result[field] = after[field] - before[field]
    if before["boundary"] == FILESYSTEM_BOUNDARY:
        if before.get("overflowed") is not False or after.get("overflowed") is not False:
            raise ValueError("filesystem payload counters overflowed")
        field = "successful_metadata_requests"
        if type(before.get(field)) is not int or type(after.get(field)) is not int or not 0 <= before[field] <= after[field]:
            raise ValueError("filesystem metadata counter regressed")
        result[field] = after[field] - before[field]
    return result


def clean_stop(commands, compose, names, engine):
    commands.run("stop-server-owners", compose + ["stop", "--timeout", "60", *names], timeout=180)
    for name in names:
        _, ids = commands.run("stopped-server-id", compose + ["ps", "--all", "--quiet", name])
        identifier = ids.read_text().strip()
        _, output = commands.run("stopped-server-state", ["docker", "inspect", "--format", "{{json .State}}", identifier])
        state = read_json(output)
        _, logs = commands.run("stopped-server-logs", ["docker", "logs", identifier])
        combined = logs.read_text() + logs.with_name("stderr.log").read_text()
        validate_stopped(engine, state, combined)


def run_case(args, plan, engine, package, client_image, adapters, case, directory):
    directory.mkdir()
    project = "rtdb-perf-" + uuid.uuid4().hex[:20]
    selected = plan["engines"][engine]
    measurement = selected.get("provider_measurement")
    filesystem = None
    if measurement and measurement["kind"] == "owned_filesystem_observer":
        filesystem = FilesystemObserver(measurement, artifact(args.plan.parent, measurement["observer_artifact"]), directory / "filesystem-observer",
                                        project, plan["resources"]["storage_placement"], git(REPOSITORY, "rev-parse", "HEAD"), plan["resources"]["architecture"], selected["nodes"], case)
    manifest, endpoints = compose_plan(plan, selected, package, client_image, directory, adapters, project, args.plan.parent,
                                      filesystem.mount if filesystem else None)
    write_json(directory / "compose.json", manifest)
    compose = ["docker", "compose", "--project-name", project, "--file", str(directory / "compose.json")]
    commands = Commands(directory / "commands", args.timeout)
    names = [node["name"] for node in selected["nodes"]]
    owners = names + (["object-observer"] if "object-observer" in manifest["services"] else [])
    sequence, rounds, primary, cleanup = 0, [], None, []
    node_containers_removed = False
    recovery_ns = None
    with owned_cancellation() as owner:
        try:
            if filesystem:
                filesystem.start(commands, args.plan.parent, artifact)
            commands.run("start-server-owners", compose + ["up", "--detach", "--no-build", *owners])
            for name in names:
                sequence += 1
                call_client(commands, compose, directory, {**case, "node": name}, endpoints, "ready", sequence)
                identifier = inspect_server(commands, compose, name, package, plan["resources"], directory, sequence)
                if filesystem:
                    filesystem.route(commands, client_image, adapters, identifier, name, project)
            sequence += 1
            setup = call_client(commands, compose, directory, case, endpoints, "setup", sequence)
            write_json(directory / "setup.json", setup)
            sequence += 1
            checkpoint = call_client(commands, compose, directory, case, endpoints, "checkpoint", sequence)
            write_json(directory / "checkpoint.json", checkpoint)
            if case["cache_mode"] == "warm":
                sequence += 1
                write_json(directory / "warmup.json", call_client(commands, compose, directory, case, endpoints, "warmup", sequence))
            outer_count = case["samples"] if case["cache_mode"] == "process_cold" else 1
            for outer in range(outer_count):
                if case["cache_mode"] == "process_cold" or case.get("restart_before_measurement"):
                    clean_stop(commands, compose, names, engine)
                    started = time.perf_counter_ns()
                    commands.run("restore-server-owners", compose + ["start", *names])
                    for name in names:
                        sequence += 1
                        call_client(commands, compose, directory, {**case, "node": name}, endpoints, "ready", sequence)
                    if case["workload"] == "restore":
                        sequence += 1
                        call_client(commands, compose, directory, case, endpoints, "checks", sequence)
                        recovery_ns = time.perf_counter_ns() - started
                ids = [inspect_server(commands, compose, name, package, plan["resources"], directory, sequence + index) for index, name in enumerate(names)]
                if filesystem:
                    for name, identifier in zip(names, ids):
                        filesystem.route(commands, client_image, adapters, identifier, name, project)
                measurement = selected.get("provider_measurement")
                before = provider_sample(commands, compose, measurement, project, filesystem)
                sequence += 1
                measured = {**case, "samples": 1} if case["cache_mode"] == "process_cold" else case
                client = call_client(commands, compose, directory, measured, endpoints, "measure", sequence)
                after = provider_sample(commands, compose, measurement, project, filesystem)
                rounds.append({"client": client, "processes": [resources(commands, client_image, adapters, identifier, project) for identifier in ids],
                               "provider": provider_delta(before, after, plan), "provider_snapshots": {"before": before, "after": after}})
            sequence += 1
            checks = call_client(commands, compose, directory, case, endpoints, "checks", sequence)
            write_json(directory / "checks.json", checks)
            provider_final = provider_sample(commands, compose, selected.get("provider_measurement"), project, filesystem)
            if provider_final is not None and provider_final["successful_read_requests"] + provider_final["successful_write_requests"] == 0:
                raise ValueError("selected workload never exercised its claimed measured provider path")
            write_json(directory / "provider-final.json", provider_final)
        except BaseException as error:
            primary = error
        finally:
            owner.begin_cleanup()
            try:
                clean_stop(commands, compose, names, engine)
                if "object-observer" in owners:
                    commands.run("stop-provider-observer", compose + ["stop", "--timeout", "330", "object-observer"], timeout=360)
                    clean_stop(commands, compose, ["object-observer"], "provider_observer")
            except BaseException as error:
                cleanup.append(error)
            for action, command in [("server-logs", compose + ["logs", "--no-color"]),
                                    ("remove-project", compose + ["down", "--volumes", "--remove-orphans", "--timeout", "60"])]:
                try:
                    commands.run(action, command, timeout=180)
                except BaseException as error:
                    cleanup.append(error)
            for resource, command in (("containers", ["docker", "ps", "--all", "--quiet"]), ("networks", ["docker", "network", "ls", "--quiet"]), ("volumes", ["docker", "volume", "ls", "--quiet"])):
                try:
                    _, output = commands.run("drain-" + resource, command + ["--filter", "label=com.docker.compose.project=" + project], timeout=30)
                    if output.read_text().strip():
                        if filesystem and resource == "containers":
                            identifiers = output.read_text().split()
                            if all(re.fullmatch(r"[0-9a-f]{12,64}", identifier) for identifier in identifiers):
                                commands.run("force-remove-node-mounts", ["docker", "rm", "--force", *identifiers], timeout=180)
                                _, remaining = commands.run("verify-removed-node-mounts", command + ["--filter", "label=com.docker.compose.project=" + project], timeout=30)
                                if remaining.read_text().strip():
                                    raise ValueError("node containers still own filesystem bind mounts")
                                node_containers_removed = True
                        raise ValueError("benchmark project retained " + resource)
                    if resource == "containers":
                        node_containers_removed = True
                except BaseException as error:
                    cleanup.append(error)
            if filesystem:
                try:
                    filesystem_evidence = filesystem.close(commands, node_containers_removed)
                except BaseException as error:
                    cleanup.append(error)
            write_json(directory / "lifecycle.json", {"project": project, "drained": not cleanup, "primary_error": str(primary) if primary else None, "cleanup_errors": list(map(str, cleanup))})
    if primary is not None or cleanup:
        raise BaseExceptionGroup("benchmark execution or cleanup failed", ([primary] if primary else []) + cleanup)
    receipt = {"case": case["id"], "trace_sha256": operation_trace_digest(case), "resources": plan["resources"],
               "execution_complete": True, "drained": True, "rounds": rounds, "recovery_ns": recovery_ns, "checkpoint": checkpoint,
               "setup_sha256": digest(directory / "setup.json"), "checks_sha256": digest(directory / "checks.json"),
               "lifecycle_sha256": digest(directory / "lifecycle.json")}
    if filesystem:
        verify_evidence(filesystem_evidence, measurement, selected["nodes"], filesystem.workload_request, project)
        receipt["filesystem_observer"] = filesystem_evidence
    write_json(directory / "receipt.json", receipt)
    return receipt


def run(args):
    state = read_json(REPOSITORY / "rust/implementation-status.json")
    if not args.phase_two or state.get("phase_2_authorized_by_completeness_gate") is not True:
        raise ValueError("performance execution requires the Phase 2 completeness gate")
    plan = validate_plan(read_json(args.plan))
    fixture_path = artifact(args.plan.parent, plan["fixture"])
    if digest(fixture_path) != plan["fixture_sha256"]:
        raise ValueError("performance fixture digest differs from selected trace")
    fixture = read_json(fixture_path)
    for case in plan["cases"]:
        trace = {field: case.get(field, []) for field in ("setup", "checkpoint", "warmup", "workers", "checks", "overlap_observer")}
        if fixture.get("traces", {}).get(case["id"]) != trace:
            raise ValueError("performance operations differ from immutable fixture")
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    for repository, expected in ((REPOSITORY, revision), (args.reference, REFERENCE)):
        if git(repository, "rev-parse", "HEAD") != expected or git(repository, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("benchmark inputs require clean pinned source checkouts")
    args.output.mkdir(parents=True, exist_ok=False)
    with owned_cancellation():
        commands = Commands(args.output / "commands", args.timeout)
        _, actual_host = commands.run("actual-docker-architecture", ["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"])
        host = actual_host.read_text().strip().replace("x86_64", "amd64").replace("aarch64", "arm64")
        if host != plan["resources"]["architecture"]:
            raise ValueError("matched performance requires a native worker architecture, without CPU emulation")
        inputs = [args.plan, fixture_path, Path(__file__), HERE / "performance_plan.py", HERE / "performance_results.py", HERE / "run_clients.py", HERE / "run_controls.py",
                  HERE / "client_results.py", HERE / "packaged_lifecycle.py", HERE / "filesystem_observer.py", HERE.parent / "release/evidence.py"]
        packages = {}
        for engine, selected in plan["engines"].items():
            path = artifact(args.plan.parent, selected["package"])
            package = read_json(path)
            image = image_record(commands, package["image_id"], engine)
            validate_package(image, package, revision if engine == "rust" else REFERENCE, engine)
            provenance = validate_package_files(path, package)
            if package["platform"] != plan["resources"]["architecture"]:
                raise ValueError("server package architecture differs from resource contract")
            packages[engine] = {**package, "provenance_artifact": provenance}
            inputs.append(path)
            inputs.append(path.parent / "source-manifest.json")
            inputs.extend(artifact(path.parent, item) for item in package["artifacts"])
            for node in selected["nodes"]:
                inputs.append(artifact(args.plan.parent, node["configuration"]))
                inputs.extend(artifact(args.plan.parent, item) for item in node.get("credential_files", []))
                if node.get("client_password"):
                    inputs.append(artifact(args.plan.parent, node["client_password"]))
            if selected.get("provider_measurement"):
                inputs.append(artifact(args.plan.parent, selected["provider_measurement"]["observer_artifact"]))
                if selected["provider_measurement"].get("ca_file"):
                    inputs.append(artifact(args.plan.parent, selected["provider_measurement"]["ca_file"]))
        sources = args.output / "client-sources"
        write_json(args.output / "client-source.json", materialize_clients(commands, args.reference, sources))
        iid = args.output / "client-image.id"
        commands.run("build-shared-client", ["docker", "build", "--platform", plan["resources"]["architecture"], "--iidfile", str(iid), str(sources / "lang/python")])
        client = image_record(commands, iid.read_text().strip(), "performance-client")
        if client["Os"] + "/" + client["Architecture"] != plan["resources"]["architecture"]:
            raise ValueError("client image architecture differs from the selected servers")
        write_json(args.output / "client-image.json", client)
        adapters = args.output / "adapters"
        adapters.mkdir()
        for filename in ("performance.py", "resources.py", "provider_observer.py", "filesystem_route.py"):
            source = HERE / "clients" / filename
            (adapters / filename).write_bytes(source.read_bytes())
            inputs.append(source)
        captured = [{"path": str(path), "sha256": digest(path)} for path in inputs]
        write_json(args.output / "inputs.json", captured)
        runs = []
        for engine, count in (("jvm", plan["jvm_repetitions"]), ("rust", plan["native_repetitions"])):
            for repetition in range(count):
                directory = args.output / f"{engine}-{repetition:03}"
                directory.mkdir()
                cases = [run_case(args, plan, engine, packages[engine], client["Id"], adapters, case, directory / f"case-{index:04}") for index, case in enumerate(plan["cases"])]
                receipt = {"engine": engine, "repetition": repetition, "cases": cases, "fixture_sha256": plan["fixture_sha256"], "packaged_server": True,
                           "server_image": packages[engine]["image_id"], "client_image": client["Id"]}
                write_json(directory / "execution.json", receipt)
                runs.append(receipt)
        if any(digest(item["path"]) != item["sha256"] for item in captured):
            raise ValueError("benchmark source/configuration/artifact input changed during execution")
        for repository, expected in ((REPOSITORY, revision), (args.reference, REFERENCE)):
            if git(repository, "rev-parse", "HEAD") != expected or git(repository, "status", "--porcelain", "--untracked-files=all"):
                raise ValueError("benchmark source checkout changed during execution")
        report = compare(plan, runs)
        report.update(plan_sha256=digest(args.plan), inputs_sha256=digest(args.output / "inputs.json"),
                      client_image_sha256=digest(args.output / "client-image.json"))
        write_json(args.output / "comparison.json", report)
        return report


def main():
    parser = argparse.ArgumentParser(description="Measure immutable packaged JVM/native servers with the same captured pgwire/Flight client.")
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    args.plan, args.reference, args.output = args.plan.resolve(), args.reference.resolve(), args.output.resolve()
    try:
        report = run(args)
    except BaseException as error:
        if args.output.is_dir():
            write_json(args.output / "failure.json", {"packaged_performance_qualified": False, "error": str(error)})
        raise
    return 0 if report["packaged_performance_qualified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
