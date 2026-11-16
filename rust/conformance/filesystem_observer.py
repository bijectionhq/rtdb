
import json
import hashlib
import copy
import os
import re
import shutil
import signal
import subprocess
import time
import urllib.request
from pathlib import Path, PurePosixPath

from run_controls import digest, owned_cancellation, signal_group, wait_group, write_json
from clients.performance import sequenced_begin, await_token_sql

FILESYSTEM_BOUNDARY = "successful_backing_filesystem_payload_and_requests"
MOUNT_TARGET = "/measured-objects"


def _read_text(path, maximum=1048576):
    with Path(path).open("rb") as stream:
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise ValueError("filesystem observation exceeds its retained byte bound")
    return data.decode()


def _read_json(path, maximum=1048576):
    return json.loads(_read_text(path, maximum))


def _canonical_absolute(value):
    return isinstance(value, str) and value.startswith("/") and "//" not in value and "\0" not in value and "\\" not in value and not any(part in {".", ".."} for part in value.split("/")) and str(PurePosixPath(value)) == value


def configuration_digest(configuration):
    """Hash the versioned measurement settings shared by the binary and offline verifier."""
    value = "v1:{max_inodes}:{max_handles}:{max_io_bytes}:normal_page_cache:no_passthrough".format(**configuration)
    return hashlib.sha256(value.encode()).hexdigest()


def environment_digest(node, namespace):
    """Hash the exact explicit container environment using serde BTreeMap-compatible JSON bytes."""
    environment = {"TZ": "UTC", "RTDB_RUN_ID": namespace, **node.get("environment", {})}
    if environment["RTDB_RUN_ID"] != namespace or any(not isinstance(key, str) or not isinstance(value, str) for key, value in environment.items()):
        raise ValueError("node environment is not the explicit immutable string map")
    return hashlib.sha256(json.dumps(environment, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def postgres_sql(source):
    """Normalize the positional psycopg 3.3.6 placeholder subset used by retained workloads.
    Like the driver, conversion spans quoted/comment text and collapses %% when parameters are supplied.
    Named mappings and invalid placeholder forms are ineligible for this measurement selection.
    """
    output, index, parameter = [], 0, 0
    while index < len(source):
        current = source[index]
        index += 1
        if current != "%" or index == len(source) or source[index] == "\n":
            output.append(current)
        elif source[index] == "%":
            output.append("%")
            index += 1
        elif source[index] in "sbt":
            parameter += 1
            output.append("$" + str(parameter))
            index += 1
        else:
            raise ValueError("filesystem workload requires valid positional psycopg placeholders")
    return "".join(output)


def workload_request(case, nodes):
    """Collect the ordered SQL selection the client can execute for this case, including generated controls.
    Endpoint and flush databases must be the configured primary database.
    The returned request is bounded to 10,000 statements and 16 MiB; SQL classification belongs to the native production parser.
    """
    if any(node.get("database") != "xtdb" for node in nodes):
        raise ValueError("filesystem workloads can select only the configured primary database")
    statements = []
    def add(location, kind, sql, psycopg_parameters=False):
        if not isinstance(sql, str) or not sql or len(sql.encode()) > 65536 or len(statements) == 10000:
            raise ValueError("filesystem workload SQL selection exceeds its bound")
        if psycopg_parameters:
            postgres_sql(sql)
        statements.append({"location": location, "kind": kind, "sql": sql, "psycopg_parameters": psycopg_parameters})
    fixed = ["SELECT 1", "BEGIN", "COMMIT", "ROLLBACK", "SHOW LATEST_SUBMITTED_TX", "SHOW LATEST_COMPLETED_TXS", "SHOW SNAPSHOT_TOKEN", "SHOW AWAIT_TOKEN", await_token_sql("observer-proof")]
    for index, sql in enumerate(fixed):
        add(f"generated/{index}", "generated", sql)
    def operation(value, location, iterations, protocol):
        kind = value["kind"]
        if "begin_sql" in value:
            add(location + "/begin_sql", "begin", value["begin_sql"])
        if value.get("system_time_sequence"):
            for iteration in range(iterations):
                add(location + f"/system_time_sequence/{iteration}", "begin", sequenced_begin(value["system_time_sequence"], iteration))
        if kind in {"query", "await_query"}:
            add(location + "/sql", "read", value["sql"], protocol == "pgwire" and bool(value.get("parameters")))
        elif kind == "bulk":
            if any(not isinstance(row, list) for row in value["rows"]):
                raise ValueError("filesystem bulk workload requires positional parameter rows")
            add(location + "/sql", "mutation", value["sql"], protocol == "pgwire")
        elif kind in {"transaction", "replicated_transaction"}:
            for index, statement in enumerate(value["statements"]):
                add(location + f"/statements/{index}", "transaction_statement", statement["sql"], protocol == "pgwire" and bool(statement.get("parameters")))
            if kind == "replicated_transaction":
                add(location + "/verification_sql", "read", value["verification_sql"])
        elif kind == "flush":
            if value["database"] != "xtdb":
                raise ValueError("filesystem flush cannot select an attached database")
            operation(value["barrier"], location + "/barrier", 1, protocol)
        elif kind != "capture_basis":
            raise ValueError("unknown filesystem workload operation")
    for phase in ("setup", "checkpoint", "warmup", "checks"):
        for index, value in enumerate(case.get(phase, [])):
            operation(value, f"{phase}/{index}", 1, case.get("setup_protocol", case["protocol"]) if phase == "setup" else case["protocol"])
    for worker, operations in enumerate(case.get("workers", [])):
        for index, value in enumerate(operations):
            operation(value, f"workers/{worker}/{index}", 1 if case.get("cache_mode") == "process_cold" else case["samples"], case["protocol"])
    if case.get("overlap_observer"):
        add("overlap/sql", "read", case["overlap_observer"]["sql"], case["protocol"] == "pgwire" and bool(case["overlap_observer"].get("parameters")))
    result = {"schema_version": 1, "statements": statements}
    workload_bytes(result)
    return result


def workload_bytes(request):
    """Encode the exact bounded ordered workload proof input as compact UTF-8 JSON."""
    data = json.dumps(request, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    if len(data) > 16 * 1024 * 1024:
        raise ValueError("filesystem workload proof exceeds input byte bound")
    return data


def verify_workload(proof, expected):
    """Bind a successful production-parser policy receipt to every selected SQL source and its order."""
    records = [{"location": item["location"], "kind": item["kind"], "psycopg_parameters": item["psycopg_parameters"],
                "sql_sha256": hashlib.sha256(item["sql"].encode()).hexdigest(),
                "wire_sql_sha256": hashlib.sha256((postgres_sql(item["sql"]) if item["psycopg_parameters"] else item["sql"]).encode()).hexdigest()} for item in expected["statements"]]
    if not isinstance(proof, dict) or proof.get("schema_version") != 1 or proof.get("policy") != "local-storage-workload-v1" or proof.get("request_sha256") != hashlib.sha256(workload_bytes(expected)).hexdigest() or proof.get("statements") != records:
        raise ValueError("filesystem workload proof differs from actual selected SQL")
    if proof.get("attached_databases_allowed") is not False or proof.get("runtime_configuration_changes_allowed") is not False:
        raise ValueError("filesystem workload proof permits unmeasured runtime configuration changes")


def validate_selection(measurement):
    """Validate the passive observer selection before acquiring directories or processes.
    Raises ValueError for unknown fields, invalid limits, or an unresolved backing placement.
    """
    if measurement.get("kind") != "owned_filesystem_observer" or set(measurement) != {"kind", "observer_artifact", "backing_parent", "configuration"}:
        raise ValueError("filesystem observer selection has unknown or missing fields")
    parent = measurement["backing_parent"]
    if not _canonical_absolute(parent):
        raise ValueError("backing placement requires an absolute canonical host path")
    configuration = measurement["configuration"]
    bounds = {"max_inodes": (16, 1_000_000), "max_handles": (16, 65536), "max_io_bytes": (4096, 1048576)}
    if not isinstance(configuration, dict) or set(configuration) != set(bounds):
        raise ValueError("filesystem observer limit selection is incomplete")
    for key, (lower, upper) in bounds.items():
        if type(configuration[key]) is not int or not lower <= configuration[key] <= upper:
            raise ValueError("filesystem observer limit exceeds bounded implementation")


def nonoverlapping_target(value):
    """Reject container binds that could hide or include the measured object mount."""
    path = PurePosixPath(value)
    target = PurePosixPath(MOUNT_TARGET)
    if not _canonical_absolute(value) or path == target or path in target.parents or target in path.parents:
        raise ValueError("runtime/log/credential mount overlaps the measured object root")


def mount_record(text, target):
    """Select exactly one real FUSE mountinfo record for an unescaped owned path.
    Raises ValueError for missing, stacked, escaped, or nonobserver mounts.
    """
    records = []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 10 or fields[4] != target:
            continue
        if "-" not in fields or fields.index("-") + 3 >= len(fields):
            raise ValueError("malformed observer mountinfo")
        separator = fields.index("-")
        if fields[separator + 1] != "fuse.rtdb-observer" or fields[3] != "/":
            raise ValueError("measured object root is not the complete observer mount")
        records.append({"device": fields[2], "source": fields[separator + 2], "line": line})
    if len(records) != 1:
        raise ValueError("exactly one measured FUSE mount is required")
    return records[0]


def verify_route(ready, route, namespace):
    """Bind container routing to the observer's host mount and invocation.
    Raises ValueError for a different device, namespace, mount root, or hidden nested mount.
    """
    host = mount_record(ready["mount"]["host_mountinfo"], ready["mount"]["mountpoint"])
    actual = mount_record(route["mountinfo"], MOUNT_TARGET)
    if host["device"] != actual["device"] or host["source"] != namespace or actual["source"] != namespace:
        raise ValueError("node object root routes to another filesystem observer")
    for line in route["mountinfo"].splitlines():
        fields = line.split()
        if len(fields) > 4 and fields[4].startswith(MOUNT_TARGET + "/"):
            raise ValueError("nested node mount bypasses object observation")
    if route.get("root_device") != ready["mount"]["device"]:
        raise ValueError("node root stat differs from mounted observer device")


def verify_evidence(value, measurement, nodes, workload, namespace=None):
    """Check completed filesystem observation, routing, activity, and immutable configuration evidence.
    This validates retained observations and does not infer execution from process exit alone.
    Raises ValueError when any required receipt or successful provider operation is absent.
    """
    ready, final = value.get("ready"), value.get("final")
    if not isinstance(ready, dict) or not isinstance(final, dict):
        raise ValueError("filesystem observer lacks readiness or joined final receipt")
    namespace = namespace or ready["namespace"]
    verify_workload(value.get("workload"), workload)
    for sample in (ready, final):
        if sample.get("boundary") != FILESYSTEM_BOUNDARY or sample.get("namespace") != namespace or sample.get("observer_artifact_sha256") != measurement["observer_artifact"]["sha256"]:
            raise ValueError("filesystem observation identifies another boundary or binary")
        if sample.get("source_dirty") is not False or sample.get("passthrough") is not False or sample.get("overflowed") is not False:
            raise ValueError("filesystem observer provenance or counter bound is unqualified")
        if sample.get("measurement_configuration_sha256") != configuration_digest(measurement["configuration"]):
            raise ValueError("filesystem observer limits differ from selection")
        if not re.fullmatch(r"[0-9a-f]{40}", sample.get("source_revision", "")) or sample.get("source_revision") != ready.get("source_revision"):
            raise ValueError("filesystem observer lacks immutable source provenance")
        placement = sample.get("physical_placement", {})
        if placement.get("backing_parent") != measurement["backing_parent"] or type(placement.get("backing_device")) is not int or not re.fullmatch(r"[0-9a-f-]{36}", placement.get("host_boot_id", "")) or placement != ready.get("physical_placement"):
            raise ValueError("filesystem provider physical placement is missing or changed")
    for field in ("successful_read_requests", "successful_read_bytes", "successful_write_requests", "successful_write_bytes", "successful_metadata_requests"):
        if type(ready.get(field)) is not int or type(final.get(field)) is not int or not 0 <= ready[field] <= final[field]:
            raise ValueError("filesystem provider counter regressed or is absent")
    if final.get("joined") is not True or final.get("unmounted") is not True or final.get("errors") or value.get("process_exit") != 0 or value.get("forced_termination") is not False or value.get("node_containers_removed") is not True:
        raise ValueError("filesystem observer did not unmount and join cleanly")
    if final.get("in_flight_requests") != 0 or final["successful_read_requests"] + final["successful_write_requests"] <= 0:
        raise ValueError("filesystem observer lacks successful provider activity or quiescence")
    configurations = value.get("configurations", [])
    names = [config.get("node") for config in configurations]
    if any(not isinstance(name, str) or not name for name in names) or len(names) != len(set(names)):
        raise ValueError("filesystem decoded configuration identities are absent or duplicated")
    if set(names) != {node["name"] for node in nodes}:
        raise ValueError("filesystem configuration proof omits or adds a selected node")
    if not value.get("routes") or {route.get("node") for route in value["routes"]} != set(names):
        raise ValueError("filesystem observation lacks all node routing/configuration evidence")
    for route in value["routes"]:
        verify_route(ready, route, namespace)
    for config in value["configurations"]:
        if config.get("mount_target") != MOUNT_TARGET or not config.get("databases") or not re.fullmatch(r"[0-9a-f]{64}", config.get("configuration_sha256", "")):
            raise ValueError("filesystem observation lacks decoded configuration proof")
        if config.get("attached_databases_allowed") is not False or config.get("runtime_configuration_changes_allowed") is not False:
            raise ValueError("decoded configuration proof permits unmeasured runtime changes")
        selected = next((node for node in nodes if node["name"] == config["node"]), None)
        if selected is None or config["configuration_sha256"] != selected["configuration"]["sha256"] or config.get("environment_sha256") != environment_digest(selected, namespace):
            raise ValueError("decoded configuration or environment differs from selected node inputs")
        for database in config["databases"]:
            if not all(_canonical_absolute(database.get(key)) for key in ("storage_path", "log_path")):
                raise ValueError("decoded storage/log paths are not canonical absolute paths")
            storage, log = PurePosixPath(database["storage_path"]), PurePosixPath(database["log_path"])
            if not storage.is_relative_to(MOUNT_TARGET) or not log.is_absolute() or log.is_relative_to(MOUNT_TARGET) or PurePosixPath(MOUNT_TARGET).is_relative_to(log):
                raise ValueError("decoded storage/log paths violate the measured boundary")


def verify_interval(round_, evidence):
    """Recompute payload deltas from retained quiescent snapshots within the joined observer lifetime.
    Raises ValueError for fabricated deltas, changed routing identities, or counters outside that lifetime.
    """
    before, after = round_["provider_snapshots"]["before"], round_["provider_snapshots"]["after"]
    ready, final, delta = evidence["ready"], evidence["final"], round_["provider"]
    for sample in (before, after):
        for key in ("boundary", "namespace", "observer_artifact_sha256", "measurement_configuration_sha256", "physical_placement", "mount"):
            if sample.get(key) != ready.get(key):
                raise ValueError("filesystem interval names another observer lifetime")
        if sample.get("in_flight_requests") != 0 or sample.get("overflowed") is not False:
            raise ValueError("filesystem interval lacks a quiescent bounded snapshot")
    for key in ("successful_read_bytes", "successful_read_requests", "successful_write_bytes", "successful_write_requests", "successful_metadata_requests",
                "failed_requests", "failed_request_bytes", "failed_response_bytes", "rejected_requests", "rejected_connections"):
        values = [sample.get(key) for sample in (ready, before, after, final)]
        if any(type(value) is not int or value < 0 for value in values) or values != sorted(values) or delta.get(key) != after[key] - before[key]:
            raise ValueError("filesystem interval delta differs from actual cumulative snapshots")


class FilesystemObserver:
    """Own one observer process, its empty backing namespace, and its mounted directory.
    Call close on every outcome with the verified container-removal result, including partial startup failures.
    Normal unmount requires all node containers to be removed; unresolved mounts select the failed-owner emergency path.
    A forced shutdown remains failed evidence even if the kernel mount is subsequently reclaimed.
    """

    def __init__(self, measurement, binary, directory, namespace, placement, revision, architecture, nodes, case):
        validate_selection(measurement)
        self.measurement, self.binary, self.directory = measurement, Path(binary), directory
        self.namespace, self.placement, self.revision = namespace, placement, revision
        self.architecture = architecture
        self.nodes = copy.deepcopy(nodes)
        self.workload_request = workload_request(case, self.nodes)
        self.workload_proof = None
        self.mount = directory / "object-mount"
        self.backing = Path(measurement["backing_parent"]) / namespace
        self.process, self.ready = None, None
        self.configurations, self.routes = [], []
        self.streams, self.created = [], []
        self.forced = False

    def start(self, commands, root, artifact):
        """Create the isolated namespace, validate real configurations, and await the actual FUSE mount.
        The owner retains partial startup state for close on every exception.
        Raises errors from filesystem, subprocess, decoder, HTTP readiness, or evidence validation.
        """
        self.directory.mkdir()
        if os.uname().sysname != "Linux" or not Path("/dev/fuse").exists():
            raise ValueError("local provider observation requires Linux /dev/fuse and mount privileges")
        parent = self.backing.parent
        if parent.resolve() != parent or not parent.is_dir():
            raise ValueError("filesystem backing placement is not an existing canonical directory")
        observer_copy = self.directory / "observer"
        shutil.copyfile(self.binary, observer_copy)
        observer_copy.chmod(0o755)
        if digest(observer_copy) != self.measurement["observer_artifact"]["sha256"]:
            raise ValueError("retained observer binary changed")
        self.binary = observer_copy
        _, identity_output = commands.run("filesystem-observer-identity", [str(self.binary), "identity"])
        identity = _read_json(identity_output)
        if identity.get("source_revision") != self.revision or identity.get("source_dirty") is not False or identity.get("observer_artifact_sha256") != digest(self.binary):
            raise ValueError("observer artifact is not a clean build of the selected native source")
        targets = {"linux/amd64": "x86_64-", "linux/arm64": "aarch64-"}
        target = identity.get("target", "")
        if self.architecture not in targets or not target.startswith(targets[self.architecture]) or "-linux-" not in target or identity.get("observer_features") != []:
            raise ValueError("observer artifact target or observer feature selection differs from the worker")
        workload_file = self.directory / "workload-request.json"
        with workload_file.open("xb") as stream:
            stream.write(workload_bytes(self.workload_request))
        _, workload_output = commands.run("filesystem-workload-proof", [str(self.binary), "validate-workload", str(workload_file)])
        self.workload_proof = _read_json(workload_output, 16 * 1024 * 1024)
        verify_workload(self.workload_proof, self.workload_request)
        for index, node in enumerate(self.nodes):
            nonoverlapping_target(node["data_path"])
            for credential in node.get("credential_files", []):
                nonoverlapping_target(credential["target"])
            request = self.directory / f"configuration-{index}.json"
            environment = {"TZ": "UTC", "RTDB_RUN_ID": self.namespace, **node.get("environment", {})}
            write_json(request, {"configuration": str(artifact(root, node["configuration"])), "environment": environment})
            _, output = commands.run("filesystem-config-proof", [str(self.binary), "validate-config", str(request)])
            proof = _read_json(output)
            if proof.get("configuration_sha256") != node["configuration"]["sha256"] or proof.get("environment_sha256") != environment_digest(node, self.namespace):
                raise ValueError("filesystem configuration proof identifies another config or environment")
            if any(not PurePosixPath(database["log_path"]).is_relative_to(node["data_path"]) for database in proof["databases"]):
                raise ValueError("decoded local log is outside the retained runtime volume")
            self.configurations.append({**proof, "node": node["name"]})
        with owned_cancellation(deferred=True) as owner:
            self.mount.mkdir()
            self.created.append(self.mount)
            self.backing.mkdir(mode=0o777)
            self.created.append(self.backing)
            self.backing.chmod(0o777)
            owner.arm()
        config = {**self.measurement["configuration"], "namespace": self.namespace, "storage_placement": self.placement,
                  "backing": str(self.backing), "mount": str(self.mount), "ready": str(self.directory / "ready.json"),
                  "final_receipt": str(self.directory / "final.json"), "expected_binary_sha256": digest(self.binary)}
        write_json(self.directory / "configuration.json", config)
        with owned_cancellation(deferred=True) as owner:
            for name in ("stdout.log", "stderr.log"):
                self.streams.append((self.directory / name).open("xb"))
            self.process = subprocess.Popen([str(self.binary), "run", str(self.directory / "configuration.json")],
                                            stdout=self.streams[0], stderr=self.streams[1], start_new_session=True)
            owner.arm()
        deadline = time.monotonic() + 60
        while not (self.directory / "ready.json").is_file():
            if self.process.poll() is not None or time.monotonic() >= deadline:
                raise ValueError("filesystem observer failed to become ready")
            time.sleep(0.02)
        self.ready = _read_json(self.directory / "ready.json")
        host = mount_record(_read_text("/proc/self/mountinfo"), str(self.mount))
        reported = mount_record(self.ready["mount"]["host_mountinfo"], str(self.mount))
        if host != reported or host["source"] != self.namespace:
            raise ValueError("observer readiness does not identify the actual host mount")
        self.sample()

    def sample(self):
        """Return one bounded quiescent cumulative snapshot from the owned loopback listener.
        Raises errors for missing ownership, changed identity, overflow, or a 60-second drain timeout.
        """
        if self.ready is None or self.process.poll() is not None:
            raise ValueError("filesystem observer is not running")
        deadline = time.monotonic() + 60
        while True:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.ready['control_port']}/__rtdb_observer__", timeout=5) as response:
                body = response.read(1048577)
            if len(body) > 1048576:
                raise ValueError("filesystem counter response exceeds byte limit")
            value = json.loads(body)
            if any(value.get(key) != self.ready.get(key) for key in ("namespace", "boundary", "observer_artifact_sha256", "measurement_configuration_sha256", "mount")) or value.get("overflowed") is not False:
                raise ValueError("filesystem counter identity changed or overflowed")
            if value.get("in_flight_requests") == 0:
                return value
            if time.monotonic() >= deadline:
                raise ValueError("filesystem operations did not reach a quiescent boundary")
            time.sleep(0.01)

    def route(self, commands, client_image, adapters, identifier, node, project):
        """Retain node-namespace mountinfo through the shared client image's PID view."""
        _, output = commands.run("filesystem-node-route", ["docker", "run", "--rm", "--pid=container:" + identifier,
            "--label", "com.docker.compose.project=" + project, "--user", "20000:20000", "--mount", f"type=bind,source={adapters},target=/adapters,readonly",
            "--entrypoint", "poetry", client_image, "run", "python", "/adapters/filesystem_route.py"])
        value = _read_json(output)
        verify_route(self.ready, value, self.namespace)
        self.routes.append({**value, "node": node})

    def close(self, commands, node_containers_removed):
        """Stop, join, and reclaim the observer after callers have removed all container mounts.
        Retains lifecycle evidence when its output directory remains writable.
        Forced process death or failed unmount raises an error.
        Normal unmount is attempted only after verified removal of all node containers.
        Otherwise it kills and waits the owned process group, retaining the unresolved mount/backing namespace as failed evidence.
        """
        errors = []
        if not node_containers_removed:
            errors.append("node container removal is unresolved; normal unmount is forbidden")
        if self.process is not None:
            try:
                if not node_containers_removed:
                    self.forced = True
                signal_group(self.process, signal.SIGTERM if node_containers_removed else signal.SIGKILL)
                if not wait_group(self.process, 60):
                    self.forced = True
                    signal_group(self.process, signal.SIGKILL)
                    if not wait_group(self.process, 10):
                        errors.append("filesystem observer process group did not drain")
            except BaseException as error:
                errors.append(str(error))
            if self.process.poll() != 0:
                errors.append("filesystem observer exited unsuccessfully")
        for stream in self.streams:
            try:
                stream.close()
            except BaseException as error:
                errors.append(str(error))
        unmounted = False
        try:
            host_mounts = _read_text("/proc/self/mountinfo") if os.uname().sysname == "Linux" else ""
            active = lambda text: any(len(line.split()) > 4 and line.split()[4] == str(self.mount) for line in text.splitlines())
            if active(host_mounts):
                errors.append("observer left its mount active")
                if node_containers_removed:
                    commands.run("reclaim-failed-filesystem-mount", ["umount", "--", str(self.mount)], timeout=30)
                    host_mounts = _read_text("/proc/self/mountinfo")
            unmounted = node_containers_removed and not active(host_mounts)
        except BaseException as error:
            errors.append(str(error))
        value = {"ready": self.ready, "final": None, "configurations": self.configurations, "routes": self.routes, "workload": self.workload_proof,
                 "forced_termination": self.forced, "node_containers_removed": node_containers_removed,
                 "process_exit": self.process.poll() if self.process else None, "cleanup_errors": errors,
                 "unresolved_mount": None if unmounted else str(self.mount), "unresolved_backing": None if unmounted else str(self.backing)}
        if (self.directory / "final.json").is_file():
            try:
                value["final"] = _read_json(self.directory / "final.json")
            except BaseException as error:
                errors.append(str(error))
        if self.ready is not None:
            try:
                verify_evidence(value, self.measurement, self.nodes, self.workload_request, self.namespace)
            except (ValueError, KeyError, TypeError) as error:
                errors.append(str(error))
        for path in reversed(self.created):
            try:
                if not unmounted or self.process is not None and self.process.poll() is None:
                    raise ValueError("retaining backing namespace because its I/O owner or mount remains active")
                if path == self.mount:
                    if os.path.ismount(path):
                        raise ValueError("refusing to remove a still-mounted observer root")
                    path.rmdir()
                else:
                    shutil.rmtree(path)
            except BaseException as error:
                errors.append(str(error))
        write_json(self.directory / "evidence.json", value)
        if errors:
            raise ValueError("filesystem observer cleanup failed: " + "; ".join(errors))
        return value
