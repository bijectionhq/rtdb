#!/usr/bin/env python3

import argparse
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from run_controls import HERE, REFERENCE, REPOSITORY, ProcessCancelled, digest, git, owned_cancellation, write_json
from run_clients import Commands, image_record, materialize_clients, validate_package, validate_package_files, verify_running_artifact
from packaged_lifecycle import validate_stopped
from packaged_recovery_plan import validate_plan, scenarios, node_configuration, attached_configuration
from packaged_recovery_results import comparison, token_position, lsn, worker_failure, gcs_denial, cloudtrail_denial, cloudtrail_entries

sys.path.insert(0, str(HERE.parent / "release"))
from evidence import artifact, retained, read_json


CONTAINER_INSPECTION = ('[{"Id":{{json .Id}},"Image":{{json .Image}},'
    '"Config":{"Labels":{{json .Config.Labels}},"Cmd":{{json .Config.Cmd}}},"State":{{json .State}},"Mounts":{{json .Mounts}}}]')


class RecoveryRun:
    def __init__(self, args, plan, case, packages, client_image, directory, adapters, native):
        self.args, self.plan, self.case, self.packages = args, plan, case, packages
        self.directory, self.adapters, self.native = directory, adapters, native
        self.namespace = plan["namespace"] + "-" + case["id"]
        self.commands = Commands(directory / "commands", args.timeout)
        self.sequence = 0
        self.started = {}
        self.captures, self.calls, self.events, self.lifecycle_events = {}, {}, [], []
        self.external = case["kind"] == "external"
        self.epoch = 0
        self.client_image = client_image
        self.compose_path = directory / "compose.json"
        self.compose = ["docker", "compose", "--project-name", self.namespace, "--file", str(self.compose_path)]
        self.definition = {"services": {}, "volumes": {"data": {}}}
        self.credential_bindings = []
        self.owned_topics = []
        self.upstream_created = False
        self.namespace_owned = False
        self.volume_ids = set()
        self.record = {"id": case["id"], "selection": case, "namespace": self.namespace, "helper_sha256": digest(native),
                       "execution_complete": False, "captures": self.captures, "calls": self.calls}
        directory.chmod(0o777)
        self.write_services()

    def credential_options(self, fault=False):
        values = self.plan["credentials"]
        if fault:
            override = self.plan["faults"][self.case["id"]]["credentials"]
            values = {"environment": {**values.get("environment", {}), **override.get("environment", {})},
                      "files": list({item["target"]: item for item in [*values.get("files", []), *override.get("files", [])]}.values())}
        environment = {"TZ": "UTC"}
        files = []
        for target, source in values.get("environment", {}).items():
            if source not in os.environ:
                raise ValueError("missing explicit recovery credential environment: " + source)
            environment[target] = "${" + source + ":?required recovery credential}"
        for item in values.get("files", []):
            path = artifact(self.args.plan.parent, item)
            files.append({"type": "bind", "source": str(path), "target": item["target"], "read_only": True})
        return environment, files

    def service(self, image, fault=False):
        environment, files = self.credential_options(fault)
        value = {"image": image, "platform": self.plan["platform"], "pull_policy": "never", "environment": environment,
            "mem_limit": self.plan["memory_bytes"], "memswap_limit": self.plan["memory_bytes"], "cpus": self.plan["cpu_count"],
            "stop_grace_period": "60s", "healthcheck": {"disable": True},
            "volumes": [{"type": "volume", "source": "data", "target": "/var/lib/rtdb"},
                        {"type": "bind", "source": str(self.directory), "target": "/evidence"},
                        {"type": "bind", "source": str(self.adapters), "target": "/adapters", "read_only": True}, *files]}
        if fault:
            profile = self.plan["faults"][self.case["id"]]["apparmor_profile"]
            if profile:
                value["security_opt"] = ["apparmor=" + profile]
        return value

    def write_services(self):
        helper = self.service(self.packages["rust"]["image_id"])
        helper["entrypoint"] = "/adapters/rtdb-conformance"
        helper["volumes"].append({"type": "bind", "source": str(self.native), "target": "/adapters/rtdb-conformance", "read_only": True})
        client = self.service(self.client_image)
        client["entrypoint"] = "poetry"
        password = artifact(self.args.plan.parent, self.plan["client_password"])
        client["volumes"].append({"type": "bind", "source": str(password), "target": "/secrets/client-password", "read_only": True})
        kafka = self.service(self.packages["jvm"]["image_id"])
        kafka["entrypoint"] = "java"
        self.definition["services"].update(helper=helper, client=client, kafka=kafka)
        self.save_compose()

    def save_compose(self):
        self.compose_path.write_text(json.dumps(self.definition, indent=2) + "\n")

    def new_request(self, kind, value):
        self.sequence += 1
        name = f"{self.sequence:05}-{kind}"
        path = self.directory / (name + ".request.json")
        write_json(path, value)
        return name, path

    def client(self, name, action, role="origin", database=None, **options):
        request = {"schema_version": 1, "action": action, "namespace": self.namespace, "timeout_seconds": self.plan["timeout_seconds"],
            "node": role, "endpoints": {role: {"pg_port": 5432, "flight_port": 3000, "health_port": 8080,
                "database": database or ("cdc" if self.external else "xtdb"), "user": "xtdb", "password_file": "/secrets/client-password"}}, **options}
        identity, path = self.new_request(name, request)
        target = self.directory / (identity + ".json")
        process, stdout = self.commands.run(name, self.compose + ["run", "--no-deps", "--rm", "client", "run", "python", "/adapters/recovery.py",
            "/evidence/" + path.name, "/evidence/" + target.name])
        result = read_json(target)
        if result.get("request_sha256") != digest(path) or result.get("execution_complete") is not True or result.get("client_closed") is not True:
            raise ValueError("protocol client receipt does not bind this completed invocation")
        reference = retained(self.args.output, target)
        self.calls[name] = reference
        self.events.append({"kind": "client", "request": retained(self.args.output, path), "receipt": reference,
            "process": retained(self.args.output, stdout.with_name("process.json")), "role": role})
        return result["result"]

    def kafka(self, action, topics, records=None):
        request = {"schema_version": 1, "namespace": self.namespace, "kafka": self.plan["kafka"], "action": action, "topics": topics}
        if records is not None:
            request["records"] = records
        identity, path = self.new_request("kafka-" + action, request)
        output = self.directory / (identity + ".json")
        _, stdout = self.commands.run("kafka-" + action, self.compose + ["run", "--no-deps", "--rm", "kafka", "-cp", "/usr/local/lib/xtdb/xtdb.jar", "clojure.main",
            "/adapters/kafka_recovery.clj", "/evidence/" + path.name, "/evidence/" + output.name])
        value = read_json(output)
        if value.get("request_sha256") != digest(path) or value.get("execution_complete") is not True or value.get("clients_closed") is not True:
            raise ValueError("Kafka client did not retain a completed real provider invocation")
        self.events.append({"kind": "kafka", "request": retained(self.args.output, path), "receipt": retained(self.args.output, output),
            "process": retained(self.args.output, stdout.with_name("process.json")), "image_id": self.packages["jvm"]["image_id"]})
        return value["result"]

    def backend(self, name, action=None, fault=False):
        config = self.directory / f"inspection-{self.epoch}.yaml"
        text = node_configuration(self.plan, self.namespace, "inspection", self.epoch)
        if config.exists() and config.read_text() != text:
            raise ValueError("inspection configuration changed within a log epoch")
        if not config.exists():
            config.write_text(text)
        request = {"schema_version": 1, "namespace": self.namespace, "configuration": "/evidence/" + config.name,
            "configuration_sha256": digest(config), "capability": self.plan["capability"],
            "token_kind": ("postgres" if self.plan["capability"] == "pg-source" else "kafka_connect") if self.external else "none",
            "action": action or {"kind": "capture"}, "maximum_bytes": self.plan["maximum_capture_bytes"], "timeout_seconds": self.plan["timeout_seconds"]}
        identity, path = self.new_request(name, request)
        helper = "helper"
        if fault:
            helper = "fault-helper"
            value = self.service(self.packages["rust"]["image_id"], True)
            value["entrypoint"] = "/adapters/rtdb-conformance"
            value["volumes"].append({"type": "bind", "source": str(self.native), "target": "/adapters/rtdb-conformance", "read_only": True})
            self.definition["services"][helper] = value
            self.save_compose()
        _, stdout = self.commands.run(name, self.compose + ["run", "--no-deps", "--rm", helper, "backend", "/evidence/" + path.name, "/evidence/" + identity])
        output = self.directory / identity / "execution.json"
        value = read_json(output)
        if value != read_json(stdout) or value.get("request_sha256") != digest(path) or value.get("binary_sha256") != digest(self.native) or value.get("source_dirty") is not False or value.get("execution_complete") is not True or value.get("resources_closed") is not True:
            raise ValueError("backend receipt differs from its actual selected helper invocation")
        reference = retained(self.args.output, output)
        self.events.append({"kind": "backend", "request": retained(self.args.output, path), "receipt": reference,
            "process": retained(self.args.output, stdout.with_name("process.json")), "stdout": retained(self.args.output, stdout)})
        if action is None:
            self.captures[name] = reference
        return value, reference

    def start(self, role, engine, readonly=False, fault=False, compactor=False):
        if role in self.started:
            raise ValueError("server role already has an admitted owner")
        configuration = self.directory / (role + ".yaml")
        configuration.write_text(node_configuration(self.plan, self.namespace, role, self.epoch, self.external, readonly, fault or compactor))
        value = self.service(self.packages[engine]["image_id"], fault)
        value["command"] = ["node", "--file", "/evidence/" + configuration.name]
        self.definition["services"][role] = value
        self.save_compose()
        self.commands.run("start-" + role, self.compose + ["up", "--detach", "--no-build", "--force-recreate", role])
        _, output = self.commands.run("identity-" + role, self.compose + ["ps", "--all", "--quiet", role])
        container = output.read_text().strip()
        if not re.fullmatch(r"[0-9a-f]{12,64}", container):
            raise ValueError("packaged server owner has no unique container identity")
        self.started[role] = (engine, container)
        _, inspection = self.commands.run("inspect-" + role, ["docker", "inspect", "--format", CONTAINER_INSPECTION, container])
        inspected = read_json(inspection)
        if len(inspected) != 1 or inspected[0]["Image"] != self.packages[engine]["image_id"]:
            raise ValueError("running server uses a different packaged image")
        for mount in inspected[0].get("Mounts", []):
            if mount.get("Type") == "volume":
                self.volume_ids.add(mount["Name"])
        artifacts = self.directory / (role + "-artifact")
        artifacts.mkdir(exist_ok=True)
        verify_running_artifact(self.commands, container, self.packages[engine], artifacts)
        self.lifecycle_events.append({"event": "started", "role": role, "engine": engine, "container": container,
            "image_id": self.packages[engine]["image_id"], "configuration": retained(self.args.output, configuration),
            "inspection": retained(self.args.output, inspection)})
        if not fault:
            self.await_ready(role)
            if self.external:
                attached = self.directory / (role + "-database.yaml")
                attached.write_text(attached_configuration(self.plan, self.namespace, self.epoch))
                self.client("attach-" + role, "attach", role, "xtdb", database_configuration="/evidence/" + attached.name)

    def await_ready(self, role):
        deadline = time.monotonic() + self.plan["timeout_seconds"]
        while True:
            try:
                self.client("ready-" + role, "ready", role, "xtdb")
                return
            except ProcessCancelled:
                raise
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)

    def logs(self, role):
        _, output = self.commands.run("logs-" + role, ["docker", "logs", self.started[role][1]])
        path = output.parent / "combined.log"
        path.write_bytes(output.read_bytes() + output.with_name("stderr.log").read_bytes())
        return path

    def stop(self, role, kill=False):
        engine, container = self.started[role]
        if kill:
            self.commands.run("kill-" + role, ["docker", "kill", "--signal", "KILL", container])
        else:
            self.commands.run("stop-" + role, ["docker", "stop", "--signal", "SIGTERM", "--time", "60", container], timeout=180)
        _, waited = self.commands.run("join-" + role, ["docker", "wait", container], timeout=180)
        _, inspection = self.commands.run("stopped-" + role, ["docker", "inspect", "--format", "{{json .State}}", container])
        state = read_json(inspection)
        log = self.logs(role)
        if kill:
            if state.get("Running") is not False or state.get("OOMKilled") is not False or state.get("ExitCode") != 137:
                raise ValueError("fault owner did not join the requested SIGKILL")
        else:
            validate_stopped(engine, state, log.read_text())
        self.lifecycle_events.append({"event": "killed" if kill else "stopped", "role": role, "engine": engine, "container": container,
            "state": retained(self.args.output, inspection), "wait": retained(self.args.output, waited), "logs": retained(self.args.output, log)})
        del self.started[role]
        return log

    def initialize(self):
        code = "import os,sys; p='/var/lib/rtdb/'+sys.argv[1]; os.mkdir(p); os.chown(p,20000,20000)"
        self.commands.run("initialize-private-volume", self.compose + ["run", "--no-deps", "--rm", "client", "run", "python", "-c", code, self.namespace])
        if "kafka" in self.plan:
            topics = [self.namespace + suffix for suffix in ("-source-0", "-source-0-replica", "-source-1", "-source-1-replica")]
            if self.external:
                topics.append(self.namespace + "-upstream")
            self.kafka("assert_absent", topics)
            self.owned_topics = topics
            self.kafka("create", topics)
        initial, _ = self.backend("initial")
        if initial["result"]["objects"] or initial["result"]["source"]["tip"] != -1 or initial["result"]["replica"]["tip"] != -1:
            raise ValueError("recovery namespace was not empty before its first writer")
        self.namespace_owned = True

    def historical(self, fixture):
        case = self.case
        count = next(value["transactions"] for value in fixture["prefixes"] if value["id"] == case["prefix"])
        self.start("origin", case["origin"])
        for index in range(count):
            self.client("seed-" + str(index), "history_step", fixture=fixture, step=index)
            if index == count - 2:
                self.client("seed-flush", "flush")
        if case["kind"] == "partial_compaction":
            self.client("canary-before-fault", "canary")
            self.client("fault-flush", "flush")
        self.client("baseline", "observe", fixture=fixture, prefix=case["prefix"], canary=case["kind"] == "partial_compaction")
        self.stop("origin")
        handoff, _ = self.backend("handoff")
        if case["kind"] == "partial_compaction":
            return self.partial(fixture, handoff)
        self.start("reader", case["target"], readonly=True)
        self.client("reader", "observe", "reader", fixture=fixture, prefix=case["prefix"], canary=False)
        self.stop("reader")
        self.backend("reader")
        self.start("canary", case["target"])
        self.client("canary-acknowledgement", "canary", "canary")
        self.start("second", case["target"], readonly=True)
        self.client("second", "observe", "second", fixture=fixture, prefix=case["prefix"], canary=True)
        self.stop("second")
        self.stop("canary")
        self.backend("acknowledged")
        for role, engine in (("restarted", case["target"]), ("fallback", case["origin"])):
            self.start(role, engine, readonly=True)
            self.client(role, "observe", role, fixture=fixture, prefix=case["prefix"], canary=True)
            self.stop(role)
            self.backend(role)

    def partial(self, fixture, handoff):
        fault = self.plan.get("faults", {}).get(self.case["id"])
        if fault is None:
            raise ValueError("missing exact metadata-denial credentials or installed policy; ordinary recovery scenarios execute separately")
        policy = artifact(self.args.plan.parent, fault["policy"])
        copied_policy = self.directory / "fault-policy.json"
        shutil.copyfile(policy, copied_policy)
        jobs = [job for job in handoff["result"]["catalog"]["available_compactions"] if job["table"] == "public/prices" and job["metadata_key"] == fault["metadata_key"]]
        if len(jobs) != 1:
            raise ValueError("fault policy does not target the exact available prices compaction")
        job = jobs[0]
        _, probe = self.backend("denial-probe", {"kind": "denial_probe", "key": job["metadata_key"], "policy_sha256": digest(policy)}, True)
        self.record.update(job=job, probe=probe, policy=retained(self.args.output, copied_policy))
        self.start("fault", self.case["origin"], fault=True)
        requires_audit = self.plan["capability"] == "gcs" and self.case["origin"] == "jvm"
        cloudtrail = fault.get("cloudtrail") if self.case["origin"] == "jvm" else None
        deadline = time.monotonic() + self.plan["timeout_seconds"]
        while True:
            log = self.logs("fault")
            try:
                worker = worker_failure(self.case["origin"], log.read_text(), job, requires_audit, bool(cloudtrail))
                break
            except ValueError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        if requires_audit:
            self.audit_gcs_denial(job, datetime.now(timezone.utc).isoformat())
        elif worker["object_key"] is None:
            self.audit_s3_denial(job, worker["request_id"], datetime.now(timezone.utc).isoformat())
        witnessed, _ = self.backend("before-crash")
        witnessed_keys = {item["key"] for item in witnessed["result"]["objects"]}
        if job["data_key"] not in witnessed_keys or job["metadata_key"] in witnessed_keys:
            raise ValueError("provider did not expose data-only output before the selected crash")
        self.record["worker_log"] = retained(self.args.output, self.stop("fault", kill=True))
        partial, _ = self.backend("partial")
        keys = {item["key"] for item in partial["result"]["objects"]}
        if job["data_key"] not in keys or job["metadata_key"] in keys:
            raise ValueError("stopped fault did not preserve an actual data-only compaction output")
        self.start("retry", self.case["target"], compactor=True)
        self.client("adopted-retry", "compaction_adopted", "retry", trie_key=job["output"])
        self.client("retry-flush", "flush", "retry")
        self.client("retry", "observe", "retry", fixture=fixture, prefix=self.case["prefix"], canary=True)
        self.stop("retry")
        self.backend("retry")
        self.start("fallback", self.case["origin"], readonly=True)
        self.client("fallback", "observe", "fallback", fixture=fixture, prefix=self.case["prefix"], canary=True)
        self.stop("fallback")
        self.backend("fallback")

    def audit_gcs_denial(self, job, ended_at):
        fault = self.plan["faults"][self.case["id"]]
        inspected = read_json(self.args.output / self.lifecycle_events[-1]["inspection"]["path"])[0]
        store = self.plan["storage"]
        request = {"schema_version": 1, "namespace": self.namespace, "project": store["project"], "bucket": store["bucket"],
            "object": store["prefix"] + "/" + self.namespace + "/v06/" + job["metadata_key"],
            "principal_email": fault["gcs_audit"]["principal_email"], "started_at": inspected["State"]["StartedAt"], "ended_at": ended_at}
        deadline = time.monotonic() + self.plan["timeout_seconds"]
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError("GCS audit evidence did not arrive before its deadline")
            identity, path = self.new_request("gcs-audit", request)
            output = self.directory / (identity + ".json")
            _, stdout = self.commands.run("gcs-audit", self.compose + ["run", "--no-deps", "--rm", "kafka", "-cp", "/usr/local/lib/xtdb/xtdb.jar", "clojure.main",
                "/adapters/gcs_recovery_audit.clj", "/evidence/" + path.name, "/evidence/" + output.name], timeout=min(self.args.timeout, remaining))
            value = read_json(output)
            if value.get("request_sha256") != digest(path) or value.get("execution_complete") is not True or value.get("client_closed") is not True:
                raise ValueError("GCS audit receipt does not bind its completed provider invocation")
            event = {"kind": "gcs_audit", "request": retained(self.args.output, path), "receipt": retained(self.args.output, output),
                "process": retained(self.args.output, stdout.with_name("process.json")), "image_id": self.packages["jvm"]["image_id"]}
            self.events.append(event)
            entries = []
            for page in value["result"]["pages"]:
                file = artifact(output.parent, {"path": page["file"], "sha256": page["sha256"]})
                entries.extend(read_json(file).get("entries", []))
            try:
                gcs_denial(entries, request)
                self.record["gcs_audit"] = event
                return
            except ValueError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)

    def audit_s3_denial(self, job, request_id, ended_at):
        selected = self.plan["faults"][self.case["id"]]["cloudtrail"]
        inspected = read_json(self.args.output / self.lifecycle_events[-1]["inspection"]["path"])[0]
        store = self.plan["storage"]
        request = {"schema_version":1, "namespace":self.namespace, **selected, "request_id":request_id,
            "target_bucket":store["bucket"], "target_object":store["prefix"] + "/" + self.namespace + "/v06/" + job["metadata_key"],
            "started_at":inspected["State"]["StartedAt"], "ended_at":ended_at}
        deadline = time.monotonic() + self.plan["timeout_seconds"]
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError("CloudTrail evidence did not arrive before its deadline")
            identity, path = self.new_request("cloudtrail", request)
            output = self.directory / (identity + ".json")
            _, stdout = self.commands.run("cloudtrail", self.compose + ["run", "--no-deps", "--rm", "kafka", "-cp", "/usr/local/lib/xtdb/xtdb.jar", "clojure.main",
                "/adapters/s3_recovery_audit.clj", "/evidence/" + path.name, "/evidence/" + output.name], timeout=min(self.args.timeout, remaining))
            value = read_json(output)
            if value.get("request_sha256") != digest(path) or value.get("execution_complete") is not True or value.get("clients_closed") is not True:
                raise ValueError("CloudTrail receipt does not bind its completed provider invocation")
            event = {"kind":"cloudtrail", "request":retained(self.args.output, path), "receipt":retained(self.args.output, output),
                "process":retained(self.args.output, stdout.with_name("process.json")), "image_id":self.packages["jvm"]["image_id"]}
            self.events.append(event)
            try:
                cloudtrail_denial(cloudtrail_entries(self.args.output, event), request)
                self.record["cloudtrail"] = event
                return
            except ValueError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)

    def external_source(self):
        postgres = self.plan["capability"] == "pg-source"
        kind = "postgres" if postgres else "kafka_connect"
        if postgres:
            self.client("upstream-absent", "postgres_absent", postgres=self.plan["postgres"])
            self.upstream_created = True
            self.client("upstream-initialized", "postgres_initialize", postgres=self.plan["postgres"])
        else:
            self.kafka("produce", [self.namespace + "-upstream"], [{"key": "1", "value": '{"name":"snapshot-row"}'}])
        key = lambda value: value if postgres else str(value)
        self.start("origin", self.case["origin"])
        self.client("snapshot", "external_rows", expected=[[key(1), "snapshot-row"]], external_kind=kind)
        if postgres:
            self.client("upstream-durable", "postgres_insert", postgres=self.plan["postgres"], id=2, name="durable-row")
        else:
            self.kafka("produce", [self.namespace + "-upstream"], [{"key": "2", "value": '{"name":"durable-row"}'}])
        self.client("durable-rows", "external_rows", expected=[[key(1), "snapshot-row"], [key(2), "durable-row"]], external_kind=kind)
        self.client("external-flush", "flush")
        durable_capture, _ = self.backend("durable")
        durable = token_position(durable_capture["result"]["catalog"]["external_token"])
        if postgres:
            deadline = time.monotonic() + self.plan["timeout_seconds"]
            index = 0
            while True:
                value = self.client("gap-" + str(index), "postgres_noise", postgres=self.plan["postgres"], id=index, name="unpublished")
                if value["confirmed_flush_lsn"] and lsn(value["confirmed_flush_lsn"]) > durable:
                    self.record["gap_confirmation"] = self.calls["gap-" + str(index)]
                    break
                if time.monotonic() >= deadline:
                    raise ValueError("upstream did not confirm unrelated WAL beyond the durable source token")
                index += 1
                time.sleep(0.1)
            self.client("upstream-held", "postgres_insert", postgres=self.plan["postgres"], id=3, name="unflushed-row")
        else:
            produced = self.kafka("produce", [self.namespace + "-upstream"], [{"key": "3", "value": '{"name":"unflushed-row"}'}])
            self.record["held_upstream_offset"] = produced["records"][0]["offset"]
        expected = [[key(1), "snapshot-row"], [key(2), "durable-row"], [key(3), "unflushed-row"]]
        self.client("unflushed-rows", "external_rows", expected=expected, external_kind=kind)
        live, _ = self.backend("live")
        tokens = [item["decoded"]["body"]["external_token"] for item in live["result"]["replica"]["records"]
                  if item.get("decoded") and item["decoded"].get("body") and item["decoded"]["body"].get("kind") == "resolved_tx" and item["decoded"]["body"].get("external_token")]
        if not tokens:
            raise ValueError("external live head has no actual replica resume token")
        applied = token_position(tokens[-1])
        if applied <= durable or token_position(live["result"]["catalog"]["external_token"]) != durable:
            raise ValueError("external held transaction was not strictly above the durable block token")
        self.record["applied_token"] = tokens[-1]
        if postgres:
            self.record["confirmation_samples"] = []
            for index in range(10):
                value = self.client("held-confirmation-" + str(index), "postgres_position", postgres=self.plan["postgres"])
                if not value["confirmed_flush_lsn"] or lsn(value["confirmed_flush_lsn"]) >= applied:
                    raise ValueError("upstream confirmed a transaction absent from every durable block")
                self.record["confirmation_samples"].append(self.calls["held-confirmation-" + str(index)])
                time.sleep(0.5)
        elif applied != self.record["held_upstream_offset"]:
            raise ValueError("KafkaConnect held token differs from the acknowledged upstream record offset")
        self.stop("origin")
        handoff, _ = self.backend("handoff")
        if token_position(handoff["result"]["catalog"]["external_token"]) != durable:
            raise ValueError("shutdown made the deliberately unflushed transaction durable")
        if postgres:
            self.client("upstream-after-stop", "postgres_insert", postgres=self.plan["postgres"], id=4, name="after-stop")
        else:
            self.kafka("produce", [self.namespace + "-upstream"], [{"key": "4", "value": '{"name":"after-stop"}'}, {"key": "2", "value": None}])
            expected = [row for row in expected if row[0] != "2"]
        expected.append([key(4), "after-stop"])
        if self.case["mode"] == "storage-only":
            self.epoch = 1
        self.start("target", self.case["target"])
        self.client("resumed-rows", "external_rows", "target", expected=expected, external_kind=kind)
        self.client("resumed-flush", "flush", "target")
        if postgres:
            deadline = time.monotonic() + self.plan["timeout_seconds"]
            index = 0
            while True:
                value = self.client("durable-confirmation-" + str(index), "postgres_position", postgres=self.plan["postgres"])
                if value["confirmed_flush_lsn"] and lsn(value["confirmed_flush_lsn"]) >= applied:
                    self.record["durable_confirmation"] = self.calls["durable-confirmation-" + str(index)]
                    break
                if time.monotonic() >= deadline:
                    raise ValueError("upstream did not confirm after adopted durable recovery block")
                index += 1
                time.sleep(0.1)
        self.stop("target")
        self.backend("recovered")
        for role, engine in (("restarted", self.case["target"]), ("fallback", self.case["origin"])):
            self.start(role, engine)
            self.client(role, "external_rows", role, expected=expected, external_kind=kind)
            self.stop(role)
            self.backend(role)

    def stop_admitted_containers(self):
        _, admitted = self.commands.run("all-admitted-containers", ["docker", "ps", "--all", "--quiet", "--filter", "label=com.docker.compose.project=" + self.namespace])
        for container in admitted.read_text().splitlines():
            inspected, raw = self.commands.run("admitted-container-mounts", ["docker", "inspect", "--format", CONTAINER_INSPECTION, container], required=False)
            if inspected.get("completed") and inspected.get("exit_code") == 0:
                for value in read_json(raw):
                    self.volume_ids.update(mount["Name"] for mount in value.get("Mounts", []) if mount.get("Type") == "volume")
            self.commands.run("stop-admitted-container", ["docker", "stop", "--time", "60", container], required=False, timeout=180)
        _, active = self.commands.run("active-owner-check", ["docker", "ps", "--quiet", "--filter", "label=com.docker.compose.project=" + self.namespace])
        if active.read_text().strip():
            raise ValueError("resource cleanup cannot run with live server owners")

    def execute(self, fixture):
        primary, failures = None, []
        with owned_cancellation() as owner:
            try:
                self.initialize()
                if self.external:
                    self.external_source()
                else:
                    self.historical(fixture)
            except BaseException as error:
                primary = error
            finally:
                owner.begin_cleanup()
                for role in list(self.started):
                    try:
                        self.stop(role)
                    except BaseException as error:
                        failures.append(error)
                try:
                    self.stop_admitted_containers()
                    if self.namespace_owned and self.plan["storage"]["kind"] != "local":
                        _, captured = self.backend("cleanup-manifest")
                        local_path = (self.args.output / captured["path"]).relative_to(self.directory)
                        self.backend("purge", {"kind": "purge", "manifest": "/evidence/" + str(local_path), "manifest_sha256": captured["sha256"]})
                    if self.owned_topics:
                        self.kafka("delete", self.owned_topics)
                    if self.upstream_created:
                        self.client("upstream-cleanup", "postgres_cleanup", postgres=self.plan["postgres"])
                except BaseException as error:
                    failures.append(error)
                try:
                    self.commands.run("remove-project", self.compose + ["down", "--volumes", "--remove-orphans", "--timeout", "60"], timeout=180)
                except BaseException as error:
                    failures.append(error)
                for resource, command in (("containers", ["docker", "ps", "--all", "--quiet"]), ("networks", ["docker", "network", "ls", "--quiet"]), ("volumes", ["docker", "volume", "ls", "--quiet"])):
                    try:
                        _, output = self.commands.run("drain-" + resource, command + ["--filter", "label=com.docker.compose.project=" + self.namespace])
                        if output.read_text().strip():
                            raise ValueError("recovery invocation retained " + resource)
                    except BaseException as error:
                        failures.append(error)
                for volume in sorted(self.volume_ids):
                    try:
                        _, output = self.commands.run("drain-observed-volume", ["docker", "volume", "ls", "--quiet", "--filter", "name=" + volume])
                        if volume in output.read_text().splitlines():
                            raise ValueError("recovery retained an admitted anonymous or named volume")
                    except BaseException as error:
                        failures.append(error)
                lifecycle = self.directory / "lifecycle.json"
                write_json(lifecycle, {"project": self.namespace, "events": self.lifecycle_events, "drained": not failures,
                    "primary_error": type(primary).__name__ if primary else None, "cleanup_errors": [type(error).__name__ for error in failures]})
                self.record["lifecycle"] = retained(self.args.output, lifecycle)
                self.record["events"] = self.events
                self.record["compose"] = retained(self.args.output, self.compose_path)
                self.record["execution_complete"] = primary is None and not failures
                if primary or failures:
                    self.record["reason"] = "scenario invocation or resource cleanup failed; inspect retained commands and lifecycle"
                write_json(self.directory / "receipt.json", self.record)
        if isinstance(primary, (ProcessCancelled, KeyboardInterrupt, SystemExit)):
            raise primary
        return self.record


def run(args):
    state = read_json(REPOSITORY / "rust/implementation-status.json")
    if not args.phase_two or state.get("phase_2_authorized_by_completeness_gate") is not True:
        raise ValueError("packaged recovery execution requires the Phase 2 completeness gate")
    plan = validate_plan(read_json(args.plan))
    revision = git(REPOSITORY, "rev-parse", "HEAD")
    for path, expected in ((REPOSITORY, revision), (args.reference, REFERENCE)):
        if git(path, "rev-parse", "HEAD") != expected or git(path, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("packaged recovery requires clean pinned source trees")
    fixture_path = artifact(args.plan.parent, plan["fixture"])
    if digest(fixture_path) != digest(HERE / "fixtures/historical-prices.json"):
        raise ValueError("recovery must use the committed historical fixture")
    fixture = read_json(fixture_path)
    args.output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(fixture_path, args.output / "fixture.json")
    commands = Commands(args.output / "commands", args.timeout)
    packages = {}
    inputs = [args.plan, fixture_path, *HERE.glob("packaged_*.py"), Path(__file__), HERE / "run_clients.py", HERE / "run_controls.py"]
    native = artifact(args.plan.parent, plan["native"])
    inputs.append(native)
    inputs.append(artifact(args.plan.parent, plan["client_password"]))
    bindings = [plan["credentials"], *(value["credentials"] for value in plan.get("faults", {}).values())]
    for binding in bindings:
        inputs.extend(artifact(args.plan.parent, item) for item in binding.get("files", []))
    inputs.extend(artifact(args.plan.parent, value["policy"]) for value in plan.get("faults", {}).values())
    retained_binary = args.output / "rtdb-conformance"
    shutil.copy2(native, retained_binary)
    if digest(retained_binary) != digest(native):
        raise ValueError("retained backend helper differs from the selected artifact")
    with owned_cancellation():
        _, actual = commands.run("docker-platform", ["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"])
        if actual.read_text().strip().replace("x86_64", "amd64").replace("aarch64", "arm64") != plan["platform"]:
            raise ValueError("packaged recovery requires the selected native worker platform")
        for engine in ("jvm", "rust"):
            path = artifact(args.plan.parent, plan["engines"][engine]["package"])
            package = read_json(path)
            validate_package(image_record(commands, package["image_id"], engine), package, revision if engine == "rust" else REFERENCE, engine)
            provenance = validate_package_files(path, package)
            if package["platform"] != plan["platform"]:
                raise ValueError("package platform differs from recovery selection")
            packages[engine] = {**package, "provenance_artifact": provenance}
            write_json(args.output / (engine + "-package.json"), {"manifest_sha256": digest(path), "manifest": package})
            inputs.extend([path, path.parent / "source-manifest.json", *[artifact(path.parent, value) for value in package["artifacts"]]])
        source = args.output / "client-sources"
        write_json(args.output / "client-source.json", materialize_clients(commands, args.reference, source))
        image_id = args.output / "client-image.id"
        commands.run("build-recovery-client", ["docker", "build", "--platform", plan["platform"], "--iidfile", str(image_id), str(source / "lang/python")])
        image = image_record(commands, image_id.read_text().strip(), "recovery-client")
        if image["Os"] + "/" + image["Architecture"] != plan["platform"]:
            raise ValueError("client image differs from the selected native platform")
        write_json(args.output / "client-image.json", image)
        adapters = args.output / "adapters"
        adapters.mkdir()
        for name in ("performance.py", "recovery.py", "kafka_recovery.clj", "gcs_recovery_audit.clj", "s3_recovery_audit.clj"):
            path = HERE / "clients" / name
            shutil.copyfile(path, adapters / name)
            inputs.append(path)
        protected_inputs = [{"path": str(path), "sha256": digest(path)} for path in inputs]
        write_json(args.output / "inputs.json", protected_inputs)
        runs = []
        for case in scenarios(plan["capability"]):
            directory = args.output / case["id"]
            directory.mkdir()
            runs.append(RecoveryRun(args, plan, case, packages, image["Id"], directory, adapters, retained_binary).execute(fixture))
        if any(digest(item["path"]) != item["sha256"] for item in protected_inputs):
            raise ValueError("recovery source, fixture, configuration or artifact changed during execution")
        report = comparison(plan, runs, args.output)
        report.update(plan_sha256=digest(args.plan), implementation_revision=revision, reference_revision=REFERENCE)
        write_json(args.output / "comparison.json", report)
        return report


def main():
    parser = argparse.ArgumentParser(description="Exercise real packaged backend recovery, exact output faults and upstream resume boundaries.")
    parser.add_argument("--phase-two", action="store_true")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=900)
    args = parser.parse_args()
    args.plan, args.reference, args.output = args.plan.resolve(), args.reference.resolve(), args.output.resolve()
    result = run(args)
    raise SystemExit(0 if result["packaged_recovery_qualified"] else 1)


if __name__ == "__main__":
    main()
