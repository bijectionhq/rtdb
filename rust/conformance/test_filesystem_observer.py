import copy
import hashlib
import json
import tempfile
import signal
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from filesystem_observer import FILESYSTEM_BOUNDARY, FilesystemObserver, configuration_digest, environment_digest, mount_record, nonoverlapping_target, validate_selection, verify_evidence as check_evidence, verify_interval, verify_route, workload_request, workload_bytes, verify_workload, postgres_sql


def node_selection():
    return {"name": "one", "environment": {}, "configuration": {"sha256": "c" * 64}, "data_path": "/runtime", "database": "xtdb"}


def case_selection():
    return {"protocol": "pgwire", "cache_mode": "warm", "samples": 1, "workers": [[{"kind": "query", "sql": "SELECT 1"}]]}


def workload_proof(request):
    return {"schema_version": 1, "policy": "local-storage-workload-v1", "request_sha256": hashlib.sha256(workload_bytes(request)).hexdigest(),
            "attached_databases_allowed": False, "runtime_configuration_changes_allowed": False,
            "statements": [{"location": row["location"], "kind": row["kind"], "psycopg_parameters": row["psycopg_parameters"],
                "sql_sha256": hashlib.sha256(row["sql"].encode()).hexdigest(),
                "wire_sql_sha256": hashlib.sha256((postgres_sql(row["sql"]) if row["psycopg_parameters"] else row["sql"]).encode()).hexdigest()} for row in request["statements"]]}


def verify_evidence(value, selection):
    return check_evidence(value, selection, [node_selection()], workload_request(case_selection(), [node_selection()]))


def fixture():
    selection = {"kind": "owned_filesystem_observer", "observer_artifact": {"path": "observer", "sha256": "a" * 64}, "backing_parent": "/disk",
                 "configuration": {"max_inodes": 4096, "max_handles": 128, "max_io_bytes": 65536}}
    host = "42 1 0:99 / /case/object-mount rw - fuse.rtdb-observer case rw"
    route = {"node": "one", "mountinfo": "51 1 0:99 / /measured-objects rw - fuse.rtdb-observer case rw", "root_device": 99}
    ready = {"boundary": FILESYSTEM_BOUNDARY, "namespace": "case", "observer_artifact_sha256": "a" * 64, "source_revision": "b" * 40,
             "source_dirty": False, "passthrough": False, "overflowed": False, "in_flight_requests": 0,
             "measurement_configuration_sha256": configuration_digest(selection["configuration"]),
             "physical_placement": {"backing_parent": "/disk", "backing_device": 3, "host_boot_id": "11111111-2222-3333-4444-555555555555"},
             "mount": {"host_mountinfo": host, "mountpoint": "/case/object-mount", "device": 99}}
    for key in ("successful_read_bytes", "successful_read_requests", "successful_write_bytes", "successful_write_requests", "successful_metadata_requests",
                "failed_requests", "failed_request_bytes", "failed_response_bytes", "rejected_requests", "rejected_connections"):
        ready[key] = 0
    final = {**copy.deepcopy(ready), "successful_read_bytes": 4096, "successful_read_requests": 1, "joined": True, "unmounted": True, "errors": []}
    proof = {"node": "one", "mount_target": "/measured-objects", "configuration_sha256": "c" * 64,
             "environment_sha256": environment_digest(node_selection(), "case"),
             "attached_databases_allowed": False, "runtime_configuration_changes_allowed": False,
             "databases": [{"storage_path": "/measured-objects", "log_path": "/runtime/log"}]}
    value = {"ready": ready, "final": final, "routes": [route], "configurations": [proof], "forced_termination": False, "process_exit": 0, "node_containers_removed": True, "process_group_drained": True,
             "workload": workload_proof(workload_request(case_selection(), [node_selection()]))}
    return selection, value


class FilesystemEvidence(unittest.TestCase):
    def test_actual_mount_device_and_full_root_are_required(self):
        selection, value = fixture()
        verify_evidence(value, selection)
        for replacement in ("0:98", "0:99 /subtree", "0:99"):
            bad = copy.deepcopy(value["routes"][0])
            bad["mountinfo"] = bad["mountinfo"].replace("0:99 /", replacement + " /", 1) if replacement != "0:99" else bad["mountinfo"].replace("case rw", "neighbor rw")
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                verify_route(value["ready"], bad, "case")
        bad = copy.deepcopy(value["routes"][0])
        bad["mountinfo"] += "\n60 51 8:1 / /measured-objects/tables rw - ext4 /dev/sda rw"
        with self.assertRaisesRegex(ValueError, "nested"):
            verify_route(value["ready"], bad, "case")

    def test_no_activity_forced_death_and_label_only_receipts_fail(self):
        selection, value = fixture()
        for field, replacement in (("joined", False), ("unmounted", False), ("successful_read_requests", 0), ("source_dirty", True), ("passthrough", True), ("overflowed", True)):
            bad = copy.deepcopy(value)
            bad["final"][field] = replacement
            with self.subTest(field=field), self.assertRaises(ValueError):
                verify_evidence(bad, selection)
        for field, replacement in (("forced_termination", True), ("routes", []), ("configurations", []), ("process_group_drained", False), ("process_group_drained", None)):
            bad = copy.deepcopy(value)
            bad[field] = replacement
            with self.assertRaises(ValueError):
                verify_evidence(bad, selection)

    def test_counter_delta_is_rederived_and_bound_to_actual_lifetime(self):
        selection, value = fixture()
        keys = [key for key in value["ready"] if key.startswith(("successful_", "failed_", "rejected_"))]
        round_ = {"provider_snapshots": {"before": value["ready"], "after": value["final"]}, "provider": {key: value["final"][key] for key in keys}}
        verify_interval(round_, value)
        bad = copy.deepcopy(round_)
        bad["provider"]["successful_read_bytes"] += 1
        with self.assertRaisesRegex(ValueError, "delta"):
            verify_interval(bad, value)
        bad = copy.deepcopy(round_)
        bad["provider_snapshots"]["after"]["in_flight_requests"] = 1
        with self.assertRaisesRegex(ValueError, "quiescent"):
            verify_interval(bad, value)
        bad = copy.deepcopy(value)
        bad["final"]["physical_placement"]["backing_device"] = 4
        with self.assertRaisesRegex(ValueError, "placement"):
            verify_evidence(bad, selection)

    def test_limits_and_mount_scope_are_exact(self):
        selection, value = fixture()
        validate_selection(selection)
        for path in ("/", "/measured-objects", "/measured-objects/cache", "relative", "/runtime/../measured-objects"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                nonoverlapping_target(path)
        nonoverlapping_target("/runtime")
        bad = copy.deepcopy(selection)
        bad["configuration"]["max_handles"] = 0
        with self.assertRaises(ValueError):
            validate_selection(bad)
        bad = copy.deepcopy(selection)
        bad["configuration"]["max_io_bytes"] *= 2
        with self.assertRaisesRegex(ValueError, "limits"):
            verify_evidence(value, bad)

    def test_failed_unmount_retains_backing_namespace_instead_of_deleting_active_files(self):
        selection, _ = fixture()
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "evidence"
            directory.mkdir()
            selection["backing_parent"] = root
            owner = FilesystemObserver(selection, Path(root) / "binary", directory, "case", "disk", "b" * 40, "linux/amd64", [node_selection()], case_selection())
            owner.mount.mkdir()
            owner.backing.mkdir()
            (owner.backing / "retained.arrow").write_bytes(b"owned payload")
            owner.created = [owner.mount, owner.backing]
            class Commands:
                def run(self, *args, **kwargs):
                    raise OSError("unmount denied")
            line = f"42 1 0:99 / {owner.mount} rw - fuse.rtdb-observer case rw"
            with patch("filesystem_observer.os.uname", return_value=SimpleNamespace(sysname="Linux")), patch("filesystem_observer._read_text", return_value=line):
                with self.assertRaisesRegex(ValueError, "cleanup failed"):
                    owner.close(Commands(), True)
            self.assertEqual(b"owned payload", (owner.backing / "retained.arrow").read_bytes())
            self.assertTrue((directory / "evidence.json").is_file())

    def test_unresolved_node_mounts_force_only_process_drain_and_preserve_namespace(self):
        selection, _ = fixture()
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "evidence"
            directory.mkdir()
            selection["backing_parent"] = root
            owner = FilesystemObserver(selection, Path(root) / "binary", directory, "case", "disk", "b" * 40, "linux/amd64", [node_selection()], case_selection())
            owner.mount.mkdir(); owner.backing.mkdir()
            owner.created = [owner.mount, owner.backing]
            owner.process = SimpleNamespace(poll=lambda: -signal.SIGKILL)
            class Commands:
                def run(self, *args, **kwargs):
                    raise AssertionError("unmount must not run before container removal")
            line = f"42 1 0:99 / {owner.mount} rw - fuse.rtdb-observer case rw"
            with patch("filesystem_observer.os.uname", return_value=SimpleNamespace(sysname="Linux")), patch("filesystem_observer._read_text", return_value=line), patch("filesystem_observer.drain_group", return_value=True) as drained:
                with self.assertRaisesRegex(ValueError, "container removal is unresolved"):
                    owner.close(Commands(), False)
                drained.assert_called_once_with(owner.process, signal.SIGKILL, 60)
            self.assertTrue(owner.backing.is_dir()); self.assertTrue(owner.mount.is_dir())

    def test_exited_leader_and_absent_mount_cannot_reclaim_an_undrained_group_namespace(self):
        selection, _ = fixture()
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "evidence"
            directory.mkdir()
            selection["backing_parent"] = root
            owner = FilesystemObserver(selection, Path(root) / "binary", directory, "case", "disk", "b" * 40, "linux/amd64", [node_selection()], case_selection())
            owner.mount.mkdir(); owner.backing.mkdir()
            payload = owner.backing / "retained.arrow"
            payload.write_bytes(b"owned payload")
            owner.created = [owner.mount, owner.backing]
            owner.process = SimpleNamespace(poll=lambda: 0)
            with patch("filesystem_observer.os.uname", return_value=SimpleNamespace(sysname="Linux")), \
                 patch("filesystem_observer._read_text", return_value=""), patch("filesystem_observer.drain_group", return_value=False) as drained:
                with self.assertRaisesRegex(ValueError, "process group did not drain"):
                    owner.close(None, True)
                self.assertEqual(drained.call_count, 2)
            self.assertEqual(payload.read_bytes(), b"owned payload")
            self.assertTrue(owner.mount.is_dir())
            evidence = json.loads((directory / "evidence.json").read_bytes())
            self.assertFalse(evidence["process_group_drained"])
            self.assertEqual(evidence["unresolved_backing"], str(owner.backing))

    def test_offline_path_proof_rejects_dot_segments_and_neighboring_namespaces(self):
        selection, value = fixture()
        for storage in ("/measured-objects/../outside", "/measured-objects/./tables", "/measured-objects//tables", "/measured-objects-neighbor"):
            bad = copy.deepcopy(value)
            bad["configurations"][0]["databases"][0]["storage_path"] = storage
            with self.subTest(storage=storage), self.assertRaises(ValueError):
                verify_evidence(bad, selection)

    def test_expanded_environment_proof_cannot_be_reused_for_other_node_inputs(self):
        selection, value = fixture()
        bad = copy.deepcopy(value)
        bad["configurations"][0]["environment_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "environment"):
            verify_evidence(bad, selection)
        changed = node_selection()
        changed["environment"]["STORE"] = "/outside"
        with self.assertRaisesRegex(ValueError, "environment"):
            check_evidence(value, selection, [changed], workload_request(case_selection(), [changed]))
        self.assertEqual(environment_digest({"environment": {"Z": "é", "A": "a"}}, "case"), environment_digest({"environment": {"A": "a", "Z": "é"}}, "case"))

    def test_decoded_configuration_policy_and_every_selected_node_are_required(self):
        selection, value = fixture()
        for flag in ("attached_databases_allowed", "runtime_configuration_changes_allowed"):
            for replacement in (True, None):
                bad = copy.deepcopy(value)
                bad["configurations"][0][flag] = replacement
                with self.subTest(flag=flag, replacement=replacement), self.assertRaisesRegex(ValueError, "runtime changes"):
                    verify_evidence(bad, selection)
        nodes = [node_selection(), {**node_selection(), "name": "two"}]
        with self.assertRaisesRegex(ValueError, "selected node"):
            check_evidence(value, selection, nodes, workload_request(case_selection(), nodes))

    def test_every_executed_sql_field_and_generated_begin_is_in_ordered_proof(self):
        case = case_selection()
        case.update(samples=2, workers=[[{"kind": "replicated_transaction", "begin_sql": "BEGIN READ WRITE", "statements": [{"sql": "INSERT INTO t (_id) VALUES (1)"}],
            "verification_sql": "SELECT _id FROM t", "system_time_sequence": {"start": "2100-01-03T00:00:00Z", "step_microseconds": 1}}]],
            setup=[{"kind": "flush", "database": "xtdb", "barrier": {"kind": "await_query", "sql": "SELECT 1"}}],
            overlap_observer={"sql": "SELECT trie_key FROM xt.trie_stats"})
        request = workload_request(case, [node_selection()])
        selected = {row["location"]: row["sql"] for row in request["statements"]}
        self.assertEqual("BEGIN READ WRITE", selected["workers/0/0/begin_sql"])
        self.assertIn("2100-01-03T00:00:00.000001+00:00", selected["workers/0/0/system_time_sequence/1"])
        self.assertEqual("SELECT _id FROM t", selected["workers/0/0/verification_sql"])
        self.assertEqual("SELECT 1", selected["setup/0/barrier/sql"])
        self.assertEqual("SELECT trie_key FROM xt.trie_stats", selected["overlap/sql"])
        proof = workload_proof(request)
        verify_workload(proof, request)
        changed = copy.deepcopy(request)
        changed["statements"][-1]["sql"] = "ATTACH DATABASE escape"
        with self.assertRaisesRegex(ValueError, "selected SQL"):
            verify_workload(proof, changed)
        changed = copy.deepcopy(proof)
        changed["attached_databases_allowed"] = True
        with self.assertRaisesRegex(ValueError, "configuration changes"):
            verify_workload(changed, request)
        with self.assertRaisesRegex(ValueError, "primary database"):
            workload_request(case, [{**node_selection(), "database": "attached"}])

    def test_positional_driver_conversion_is_bound_separately_from_source_sql(self):
        self.assertEqual("SELECT $1, '$2', 7 % 2 -- $3\n", postgres_sql("SELECT %s, '%b', 7 %% 2 -- %t\n"))
        with self.assertRaises(ValueError):
            postgres_sql("SELECT %(named)s")
        case = case_selection()
        case["workers"] = [[{"kind": "bulk", "sql": "INSERT INTO t (_id) VALUES (%s)", "rows": [[1]]}]]
        request = workload_request(case, [node_selection()])
        proof = workload_proof(request)
        verify_workload(proof, request)
        self.assertTrue(request["statements"][-1]["psycopg_parameters"])
        self.assertNotEqual(proof["statements"][-1]["sql_sha256"], proof["statements"][-1]["wire_sql_sha256"])


if __name__ == "__main__":
    unittest.main()
