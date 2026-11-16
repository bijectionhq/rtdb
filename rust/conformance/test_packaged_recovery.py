import copy
import gzip
import json
import re
import tempfile
import unittest
from unittest.mock import Mock, patch
from pathlib import Path

from packaged_recovery_plan import attached_configuration, backend_configuration, node_configuration, scenarios, validate_plan
from packaged_recovery_results import cloudtrail_denial, cloudtrail_entries, comparison, external_expectations, gcs_denial, history_rows, load, lsn, protected, required_requests, token_position, validate_external_observation, validate_server_configuration, worker_failure
from run_controls import ProcessCancelled, digest
from run_packaged_recovery import RecoveryRun


def plan(capability="local"):
    reference = {"path": "artifact", "sha256": "a" * 64}
    value = {"schema_version": 1, "namespace": "rtdb-recovery-0123456789abcdef", "platform": "linux/amd64", "capability": capability,
        "engines": {engine: {"package": reference} for engine in ("jvm", "rust")}, "native": reference, "fixture": reference,
        "storage": {"kind": "local"}, "credentials": {"environment": {"RTDB_RECOVERY_PASSWORD": "PASSWORD"}, "files": []},
        "client_password": reference, "faults": {}, "memory_bytes": 1024 ** 3, "cpu_count": 1, "timeout_seconds": 60, "maximum_capture_bytes": 1024 ** 3}
    if capability in {"kafka", "kafka-connect"}:
        value["kafka"] = {"bootstrap_servers": "broker:9092", "properties_environment": {}}
    if capability == "pg-source":
        value["postgres"] = {"hostname": "postgres", "port": 5432, "database": "test", "username": "tester", "password_environment": "PGPASSWORD"}
    return value


class PackagedRecoveryPlanTest(unittest.TestCase):
    def test_every_direction_keeps_all_history_prefixes_and_real_fault_checkpoint(self):
        values = scenarios("local")
        self.assertEqual(10, len(values))
        self.assertEqual(8, len([value for value in values if value["kind"] == "history"]))
        for capability in ("pg-source", "kafka-connect"):
            values = scenarios(capability)
            self.assertEqual(14, len(values))
            self.assertEqual({("jvm", "rust", "retained-log"), ("jvm", "rust", "storage-only"), ("rust", "jvm", "retained-log"), ("rust", "jvm", "storage-only")},
                {(value["origin"], value["target"], value["mode"]) for value in values if value["kind"] == "external"})

    def test_missing_fault_inputs_are_unmet_without_suppressing_regular_recovery_selection(self):
        value = validate_plan(plan())
        report = comparison(value, [], Path("unused"))
        self.assertFalse(report["packaged_recovery_qualified"])
        self.assertEqual(10, len(report["unmet_scenarios"]))
        self.assertFalse(report["production_cutover_authorized"])

    def test_backend_selection_and_reserved_namespace_are_not_labels(self):
        value = plan()
        for changed in ({"capability": "s3"}, {"namespace": "production"}, {"platform": "linux/riscv64"}, {"scenario_overrides": []}):
            with self.assertRaises(ValueError):
                validate_plan({**value, **changed})
        for credentials in ({"environment": {}}, {"environment": {"RTDB_RECOVERY_PASSWORD": "INVALID-NAME"}}):
            with self.assertRaises(ValueError):
                validate_plan({**value, "credentials": credentials})

    def test_external_restart_is_writable_and_only_explicit_reader_is_readonly(self):
        value = plan("pg-source")
        namespace = value["namespace"] + "-jvm-to-rust-storage-only"
        text = node_configuration(value, namespace, "restarted", 1, external=True)
        self.assertIn("readOnlyDatabases: false", text)
        self.assertIn("primary-restarted/log", text)
        self.assertIn("readOnlyDatabases: true", node_configuration(value, namespace, "second", readonly=True))
        attached = attached_configuration(value, namespace, 1)
        self.assertIn("epoch: 1", attached)
        self.assertIn("slotName: \"" + namespace.replace("-", "_") + "\"", attached)
        self.assertNotIn("primary-restarted", attached)

    def test_storage_only_changes_log_identity_without_changing_storage_root(self):
        value = plan("kafka-connect")
        before = backend_configuration(value, value["namespace"], 0)
        after = backend_configuration(value, value["namespace"], 1)
        self.assertEqual(before.split("storage: ")[1], after.split("storage: ")[1])
        self.assertIn("-source-0", before)
        self.assertIn("-source-1", after)

    def test_gcs_jvm_fault_requires_explicit_audited_writer_principal(self):
        value = plan("gcs")
        value["storage"] = {"kind": "gcs", "project": "recovery-project", "bucket": "recovery-bucket", "prefix": "qualified"}
        fault = {"policy": value["native"], "metadata_key": "tables/prices/meta/l01-rc-b00.arrow", "credentials": {}, "apparmor_profile": None}
        value["faults"] = {"jvm-to-rust-partial": fault}
        with self.assertRaises(ValueError):
            validate_plan(value)
        fault["gcs_audit"] = {"principal_email": "writer@recovery-project.iam.gserviceaccount.com"}
        validate_plan(value)
        fault["gcs_audit"]["principal_email"] = ""
        with self.assertRaises(ValueError):
            validate_plan(value)

    def test_cloudtrail_principal_selection_is_exact_and_jvm_s3_scoped(self):
        value = plan("s3")
        value["storage"] = {"kind":"s3", "bucket":"data", "prefix":"isolated", "region":"us-east-1", "endpoint":None, "path_style":False}
        selected = {"bucket":"audit", "prefix":"AWSLogs/123456789012/CloudTrail/us-east-1", "region":"us-east-1", "principal":{"arn":"arn:aws:sts::123456789012:assumed-role/recovery/session"}}
        fault = {"policy":value["native"], "metadata_key":"tables/prices/meta/l01-rc-b00.arrow", "credentials":{}, "apparmor_profile":None, "cloudtrail":selected}
        value["faults"] = {"jvm-to-rust-partial":fault}
        validate_plan(value)
        for principal in ({"arn":"arn:aws:iam::123456789012:role/recovery"}, {"arn":"*"}, {"arn":selected["principal"]["arn"], "principal_id":"AROA123:session"}):
            with self.assertRaises(ValueError):
                validate_plan({**value, "faults":{"jvm-to-rust-partial":{**fault, "cloudtrail":{**selected, "principal":principal}}}})


class RecoveryEvidenceTest(unittest.TestCase):
    def test_external_verification_requires_held_row_post_stop_row_and_kafka_tombstone(self):
        def observed(rows):
            return {"current":{"schema":[{"name":"_id"}, {"name":"name"}], "rows":[
                [{"kind":"integer", "value":str(key)} if type(key) is int else {"kind":"text", "value":key},
                 {"kind":"text", "value":name}] for key, name in rows]}}
        for capability in ("pg-source", "kafka-connect"):
            with self.subTest(capability=capability):
                expected = external_expectations(capability)
                key = (lambda value:value) if capability == "pg-source" else str
                resumed = expected["resumed-rows"]
                validate_external_observation(observed(resumed), resumed)
                for absent in (key(3), key(4)):
                    with self.assertRaisesRegex(ValueError, "fixed upstream transactions"):
                        validate_external_observation(observed([row for row in resumed if row[0] != absent]), resumed)
                requests = required_requests({"selection":{"kind":"external"}}, {}, capability)
                self.assertEqual(resumed, requests["resumed-rows"]["expected"])
                self.assertEqual("postgres" if capability == "pg-source" else "kafka_connect", requests["resumed-rows"]["external_kind"])
        kafka = [["1", "snapshot-row"], ["2", "durable-row"], ["3", "unflushed-row"], ["4", "after-stop"]]
        with self.assertRaisesRegex(ValueError, "fixed upstream transactions"):
            validate_external_observation(observed(kafka), external_expectations("kafka-connect")["resumed-rows"])

    def test_server_evidence_rejects_idle_compactor_and_readonly_external_recovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            selected = plan("pg-source")
            run = {"selection":{"kind":"external", "mode":"storage-only"}, "namespace":selected["namespace"] + "-jvm-to-rust-storage-only"}
            configuration = root / "target.yaml"
            configuration.write_text(node_configuration(selected, run["namespace"], "target", 1, external=True))
            event = {"role":"target", "configuration":{"path":configuration.name, "sha256":digest(configuration)}}
            inspected = {"Config":{"Cmd":["node", "--file", "/evidence/target.yaml"]}}
            validate_server_configuration(selected, run, root, event, inspected)
            inspected["Config"]["Cmd"][0] = "compactor"
            with self.assertRaisesRegex(ValueError, "command or configuration"):
                validate_server_configuration(selected, run, root, event, inspected)
            inspected["Config"]["Cmd"][0] = "node"
            configuration.write_text(node_configuration(selected, run["namespace"], "target", 1, external=True, readonly=True))
            event["configuration"]["sha256"] = digest(configuration)
            with self.assertRaisesRegex(ValueError, "command or configuration"):
                validate_server_configuration(selected, run, root, event, inspected)

    def test_fault_starts_normal_node_compaction_and_inspects_only_lifecycle_fields(self):
        for engine in ("jvm", "rust"):
            with self.subTest(engine=engine), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                calls = []
                def invoke(name, argv, **options):
                    calls.append((name, argv))
                    path = root / (str(len(calls)) + ".json")
                    if name == "identity-fault":
                        path.write_text("a" * 64)
                    elif name == "inspect-fault":
                        path.write_text(json.dumps([{"Id":"a" * 64, "Image":"sha256:selected", "Mounts":[]}]))
                    else:
                        path.write_text("")
                    return {"completed":True, "exit_code":0}, path
                owner = object.__new__(RecoveryRun)
                owner.directory, owner.plan, owner.namespace = root, plan(), "rtdb-recovery-0123456789abcdef-jvm-to-rust-partial"
                owner.epoch, owner.external = 0, False
                owner.args = Mock(output=root)
                owner.started, owner.volume_ids, owner.lifecycle_events = {}, set(), []
                owner.packages = {engine:{"image_id":"sha256:selected"}}
                owner.definition = {"services":{}}
                owner.compose = ["docker", "compose", "--project-name", owner.namespace]
                owner.service, owner.save_compose, owner.await_ready = Mock(return_value={}), Mock(), Mock()
                owner.commands = Mock()
                owner.commands.run.side_effect = invoke
                with patch("run_packaged_recovery.verify_running_artifact"):
                    owner.start("fault", engine, fault=True)
                self.assertEqual(["node", "--file", "/evidence/fault.yaml"], owner.definition["services"]["fault"]["command"])
                self.assertIn("compactor: {threads: 1}", (root / "fault.yaml").read_text())
                self.assertIn("readOnlyDatabases: false", (root / "fault.yaml").read_text())
                inspection = next(argv for name, argv in calls if name == "inspect-fault")
                self.assertEqual("--format", inspection[2])
                self.assertEqual({".Id", ".Image", ".Config.Labels", ".Config.Cmd", ".State", ".Mounts"}, set(re.findall(r"{{json ([^}]+)}}", inspection[3])))
                owner.await_ready.assert_not_called()

    def test_cleanup_stops_project_one_off_containers_and_retains_their_volume_owners(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calls = []
            def invoke(name, argv, **options):
                calls.append((name, argv, options))
                path = root / (str(len(calls)) + ".json")
                if name == "all-admitted-containers":
                    path.write_text("server\ninterrupted-provider-helper\n")
                elif name == "admitted-container-mounts":
                    path.write_text(json.dumps([{"Mounts":[{"Type":"volume", "Name":argv[-1] + "-anonymous"}]}]))
                else:
                    path.write_text("")
                return {"completed":True, "exit_code":0}, path
            owner = object.__new__(RecoveryRun)
            owner.namespace, owner.volume_ids = "rtdb-recovery-owned", set()
            owner.commands = Mock()
            owner.commands.run.side_effect = invoke
            owner.stop_admitted_containers()
            self.assertEqual({"server-anonymous", "interrupted-provider-helper-anonymous"}, owner.volume_ids)
            self.assertEqual(["server", "interrupted-provider-helper"], [argv[-1] for name, argv, _ in calls if name == "stop-admitted-container"])
            self.assertIn("label=com.docker.compose.project=rtdb-recovery-owned", calls[-1][1])

    def test_cloudtrail_rejects_decompression_beyond_its_expanded_byte_bound(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            object_path = root / "object.json.gz"
            object_path.write_bytes(gzip.compress(b" " * (8388608 + 1)))
            request = root / "request.json"
            request.write_text(json.dumps({"bucket":"audit", "prefix":"logs", "started_at":"2026-01-01T00:00:00Z", "ended_at":"2026-01-01T00:01:00Z"}))
            receipt = root / "receipt.json"
            size = object_path.stat().st_size
            receipt.write_text(json.dumps({"result":{"prefixes":["logs/2026/01/01/"], "compressed_bytes":size,
                "objects":[{"bucket":"audit", "key":"logs/2026/01/01/object.json.gz", "file":object_path.name, "bytes":size, "sha256":digest(object_path)}]}}))
            event = {"request":{"path":request.name, "sha256":digest(request)}, "receipt":{"path":receipt.name, "sha256":digest(receipt)}}
            with self.assertRaisesRegex(ValueError, "expanded"):
                cloudtrail_entries(root, event)

    def test_keyless_s3_worker_failure_requires_actual_request_id(self):
        job = {"table":"public/prices", "output":"l01-rc-b00", "metadata_key":"tables/prices/meta/l01-rc-b00.arrow"}
        trace = "error running compaction job: public/prices/l01-rc-b00\nS3Exception: AccessDenied (Status Code: 403, Request ID: ABC123, Extended Request ID: XYZ)"
        with self.assertRaises(ValueError):
            worker_failure("jvm", trace, job)
        result = worker_failure("jvm", trace, job, s3_audit=True)
        self.assertEqual("ABC123", result["request_id"])
        self.assertIsNone(result["object_key"])
        with self.assertRaises(ValueError):
            worker_failure("jvm", trace.replace("Request ID: ABC123, ", ""), job, s3_audit=True)

    def test_cloudtrail_matches_final_destination_request_id_and_session_identity(self):
        request = {"target_bucket":"data", "target_object":"prefix/owned/v06/tables/prices/meta/l01.arrow", "principal":{"arn":"arn:aws:sts::123456789012:assumed-role/role/session"},
                   "request_id":"ABC123", "started_at":"2026-01-01T00:01:00.123456789Z", "ended_at":"2026-01-01T00:02:00Z"}
        entry = {"eventSource":"s3.amazonaws.com", "eventName":"CopyObject", "eventType":"AwsApiCall", "errorCode":"AccessDenied", "eventTime":"2026-01-01T00:01:00Z",
                 "requestID":"ABC123", "userIdentity":{"arn":request["principal"]["arn"], "principalId":"AROA123:session"},
                 "requestParameters":{"bucketName":"data", "key":request["target_object"], "x-amz-copy-source":"data/staging/upload"}}
        self.assertEqual(entry, cloudtrail_denial([entry], request))
        self.assertEqual(entry, cloudtrail_denial([entry], {**request, "principal":{"principal_id":"AROA123:session"}}))
        for changed in ({"requestID":"OTHER"}, {"eventTime":"2026-01-01T00:00:59Z"}, {"eventName":"GetObject"}, {"errorCode":None},
                        {"userIdentity":{"arn":"arn:aws:sts::123456789012:assumed-role/role/other"}}, {"requestParameters":{"bucketName":"data", "key":"staging/upload"}}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                cloudtrail_denial([{**entry, **changed}], request)

    def test_readiness_cancellation_returns_to_the_admitted_owner_without_retry(self):
        owner = object.__new__(RecoveryRun)
        owner.plan = {"timeout_seconds": 600}
        owner.client = Mock(side_effect=ProcessCancelled("cancelled"))
        with self.assertRaises(ProcessCancelled):
            owner.await_ready("origin")
        owner.client.assert_called_once_with("ready-origin", "ready", "origin", "xtdb")

    def test_receipt_reference_rejects_symlink_and_parent_traversal(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            path = root / "receipt.json"
            path.write_text("{}")
            (root / "alias.json").symlink_to(path)
            self.assertEqual({}, load(root, {"path": path.name, "sha256": digest(path)}))
            for reference in ("alias.json", "../" + root.name + "/receipt.json"):
                with self.assertRaises(ValueError):
                    load(root, {"path": reference, "sha256": digest(path)})

    def test_gcs_denial_requires_exact_principal_object_method_status_and_worker_time(self):
        request = {"project": "recovery-project", "bucket": "recovery-bucket", "object": "prefix/owned/v06/tables/prices/meta/l01.arrow",
                   "principal_email": "writer@example.test", "started_at": "2026-01-01T00:01:00Z", "ended_at": "2026-01-01T00:02:00Z"}
        payload = {"@type": "type.googleapis.com/google.cloud.audit.AuditLog", "serviceName": "storage.googleapis.com", "methodName": "storage.objects.create", "status": {"code": 7},
                   "resourceName": "projects/_/buckets/" + request["bucket"] + "/objects/" + request["object"],
                   "authenticationInfo": {"principalEmail": request["principal_email"]}}
        entry = {"logName": "projects/recovery-project/logs/cloudaudit.googleapis.com%2Fdata_access", "timestamp": "2026-01-01T00:01:30Z",
                 "resource": {"type": "gcs_bucket", "labels": {"bucket_name": "recovery-bucket"}}, "protoPayload": payload}
        self.assertEqual(entry, gcs_denial([entry], request))
        for changed in ({"status": {"code": 0}}, {"methodName": "storage.objects.get"}, {"resourceName": payload["resourceName"] + "-neighbor"},
                        {"authenticationInfo": {"principalEmail": "probe@example.test"}}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                gcs_denial([{**entry, "protoPayload": {**payload, **changed}}], request)
        for timestamp in ("2026-01-01T00:00:59Z", "2026-01-01T00:02:01Z"):
            with self.assertRaises(ValueError):
                gcs_denial([{**entry, "timestamp": timestamp}], request)
        with self.assertRaises(ValueError):
            gcs_denial([], request)
        with self.assertRaises(ValueError):
            gcs_denial([{**entry, "timestamp": "2026-01-01T00:01:00.000000001Z"}], {**request, "started_at": "2026-01-01T00:01:00.000000002Z"})

    def test_gcs_reference_lost_cause_needs_separate_audit_evidence(self):
        job = {"table": "public/prices", "output": "l01-rc-b00", "metadata_key": "tables/prices/meta/l01-rc-b00.arrow"}
        trace = "error running compaction job: public/prices/l01-rc-b00\nError when writing object prefix/" + job["metadata_key"] + " to bucket bucket"
        with self.assertRaises(ValueError):
            worker_failure("jvm", trace, job)
        self.assertFalse(worker_failure("jvm", trace, job, gcs_audit=True)["permission_denied"])
        with self.assertRaises(ValueError):
            worker_failure("jvm", trace.replace("meta/", "data/"), job, gcs_audit=True)

    def test_azure_provider_key_warning_must_share_the_worker_request_identity(self):
        job = {"table": "public/prices", "output": "l01-rc-b00", "metadata_key": "tables/prices/meta/l01-rc-b00.arrow"}
        exception = "com.azure.storage.blob.models.BlobStorageException: Status code 403 RequestId:actual-request"
        provider = "WARNING: Exception thrown when putting object " + job["metadata_key"] + "\n" + exception + "\nstack\n"
        worker = "error running compaction job: public/prices/l01-rc-b00\n" + exception
        self.assertTrue(worker_failure("jvm", provider + worker, job)["permission_denied"])
        for changed in (provider.replace("actual-request", "other-request"), provider.replace("meta/", "data/")):
            with self.assertRaises(ValueError):
                worker_failure("jvm", changed + worker, job)

    def test_history_summary_cannot_omit_prices_or_temporal_containment(self):
        fixture = {"queries": [{"id": "q", "prefix": "june", "valid_time": "2026-01-01T00:00:00Z", "system_time": "2026-01-01T00:00:00Z", "expected_prices": {"A": 12, "B": None}}]}
        schema = [{"name": name} for name in ("price", "valid_from", "valid_to", "system_from", "system_to")]
        lower = {"kind": "timestamp", "value": "2025-01-01T00:00:00+00:00"}
        rows = [{"id": "q/A", "schema": schema, "rows": [[{"kind": "integer", "value": "12"}, lower, {"kind":"null"}, lower, {"kind":"null"}]]},
                {"id": "q/B", "schema": schema, "rows": []}]
        history_rows(rows, fixture, "june")
        with self.assertRaises(ValueError):
            history_rows(rows[:1], fixture, "june")
        changed = copy.deepcopy(rows)
        changed[0]["rows"][0][2] = {"kind":"timestamp", "value":"2026-01-01T00:00:00+00:00"}
        with self.assertRaises(ValueError):
            history_rows(changed, fixture, "june")

    def test_immutable_objects_and_retained_log_records_survive_every_handoff(self):
        value = {"namespace": "owned", "identity": {"storage": {"prefix": "owned"}, "log": {"topic": "owned-source"}},
            "result": {"objects": [{"key": "blocks/000.bin", "bytes": 3, "sha256": "a" * 64}],
                "source": {"records": [{"offset": 0, "bytes": 1, "sha256": "b" * 64, "timestamp_micros": 7}]}, "replica": {"records": []}}}
        protected(value, copy.deepcopy(value))
        changed = copy.deepcopy(value)
        changed["result"]["objects"][0]["sha256"] = "c" * 64
        with self.assertRaises(ValueError):
            protected(value, changed)
        changed = copy.deepcopy(value)
        changed["result"]["source"]["records"] = []
        with self.assertRaises(ValueError):
            protected(value, changed)
        changed["identity"]["log"] = {"topic": "owned-source-epoch1"}
        protected(value, changed, preserve_logs=False)

    def test_probe_or_generic_worker_error_cannot_prove_metadata_permission_failure(self):
        job = {"table": "public/prices", "output": "l01-rc-b00", "metadata_key": "tables/prices/meta/l01-rc-b00.arrow"}
        event = {"event": "compaction_failed", "table": "public/prices", "output_trie_key": job["output"], "object_key": job["metadata_key"],
            "operation": "put_file", "permission_denied": True, "anomaly_code": "rtdb.storage/permission-denied", "anomaly_category": "forbidden", "message_sha256": "a" * 64}
        self.assertEqual(event, worker_failure("rust", json.dumps(event), job))
        for changed in ({"event": "denial_probe"}, {"object_key": "tables/prices/data/l01-rc-b00.arrow"}, {"permission_denied": False}, {"output_trie_key": "l01-rc-b01"}):
            with self.assertRaises(ValueError):
                worker_failure("rust", json.dumps({**event, **changed}), job)
        with self.assertRaises(ValueError):
            worker_failure("jvm", "AccessDeniedException " + job["metadata_key"], job)
        original = "error running compaction job: public/prices/" + job["output"] + "\nAccessDeniedException " + job["metadata_key"]
        self.assertTrue(worker_failure("jvm", original, job)["permission_denied"])

    def test_external_position_requires_decoded_record_and_exact_lsn(self):
        self.assertEqual(0x100000002, lsn("1/2"))
        self.assertEqual(7, token_position({"sha256": "a" * 64, "decoded": {"kind": "postgres", "lsn": 7}}))
        for value in (None, {"sha256": "a" * 64, "decoded": {"kind": "kafka_connect", "offset": -1}}):
            with self.assertRaises(ValueError):
                token_position(value)
        with self.assertRaises(ValueError):
            lsn("1/2 ignored")
