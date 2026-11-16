import copy
import json
import tempfile
import unittest
from pathlib import Path

from cutover_evidence import STAGES, assess, boundary, external_progress, nonregressing, same_boundaries
from evidence import artifact, retained


def checkpoint():
    return {"database": "xtdb", "partition": 0, "storage_version": 6, "storage_epoch": 0, "finalized_block": 0,
            "source_position": 3, "replica_position": 8, "transaction": {"tx_id": 3, "system_time_micros": 100},
            "term": 0, "term_sequence": None, "external_token": None}


def minimal_plan(root):
    artifacts = {}
    for name in ("deployed_jvm", "native", "fallback_jvm", "migrator"):
        path = root / name
        path.write_bytes(name.encode())
        artifacts[name] = {**retained(root, path), "revision": name + "-revision"}
    configuration = {"configuration_sha256": "c" * 64, "configuration_compatible": True, "configuration_gaps": [], "databases": [{"name": "xtdb", "partitions": 1, "external_source": None}],
                     "provenance": {"server_artifact_sha256": artifacts["native"]["sha256"], "migrator_artifact_sha256": artifacts["migrator"]["sha256"]}}
    config = root / "configuration.json"
    config.write_text(json.dumps(configuration))
    return {"schema_version": 1, "deployment": "pilot", "evidence_scope": "isolated_preproduction_rehearsal", "control_plane_owner": "operator", "artifacts": artifacts,
            "configuration_report": retained(root, config), "publishers": [{"id": "writer", "kind": "writer", "database": "xtdb"}],
            "partitions": [checkpoint()], "write_pause_budget_seconds": 30, "recovery_target_seconds": 60,
            "rollback_deadline_unix_seconds": 1000, "assessment_time_unix_seconds": 500, "simultaneous_writers": False, "receipts": []}


def receipt(root, plan, stage, **fields):
    proof = root / (stage + ".proof")
    proof.write_text("retained operator observation")
    value = {"stage": stage, "deployment": plan["deployment"], "control_plane_owner": plan["control_plane_owner"],
             "artifact_digests": {name: artifact["sha256"] for name, artifact in plan["artifacts"].items()},
             "observed_at_unix_seconds": 100, "verified": True, "evidence": [retained(root, proof)], **fields}
    path = root / (stage + ".json")
    path.write_text(json.dumps(value))
    plan["receipts"].append(retained(root, path))


class CutoverEvidence(unittest.TestCase):
    def test_external_token_order_requires_bound_durable_progress_not_hash_order(self):
        prior = {**checkpoint(), "external_token": "f" * 64}
        current = {**prior, "external_token": "a" * 64, "finalized_block": 1,
                   "transaction": {"tx_id": 4, "system_time_micros": 101}}
        source = {"kind": "postgres", "slot": "owned"}
        progress = {"source": source, "previous_token": prior["external_token"], "durable_token": current["external_token"],
                    "previous_position": 10, "durable_position": 11, "confirmed_position": 11}
        self.assertTrue(external_progress(progress, source, prior, current))
        for field, value in (("source", {"kind": "postgres", "slot": "other"}), ("durable_position", 9),
                             ("confirmed_position", 12), ("previous_token", "b" * 64)):
            with self.subTest(field=field):
                self.assertFalse(external_progress({**progress, field: value}, source, prior, current))
        self.assertFalse(external_progress(None, source, prior, current))
        self.assertFalse(external_progress(progress, source, prior, {**current, "transaction": prior["transaction"]}))

    def test_known_sequence_and_transaction_bounds_cannot_disappear_on_other_partitions(self):
        prior = {**checkpoint(), "term_sequence": 9}
        for field, value in (("term_sequence", None), ("term_sequence", 8), ("transaction", None),
                             ("storage_epoch", 1), ("transaction", {"tx_id": 3, "system_time_micros": 101})):
            with self.subTest(field=field, value=value):
                self.assertFalse(nonregressing(prior, {**prior, field: value}))
        self.assertTrue(nonregressing(prior, {**prior, "term": 1, "term_sequence": 0}))

    def test_missing_or_malformed_configuration_hashes_never_match_by_absence(self):
        for config_hash, runtime_hash in ((None, None), ("bad", "bad"), ("c" * 64, None)):
            with self.subTest(config_hash=config_hash, runtime_hash=runtime_hash), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                plan = minimal_plan(root)
                path = root / "configuration.json"
                config = json.loads(path.read_text())
                config["configuration_sha256"] = config_hash
                path.write_text(json.dumps(config))
                plan["configuration_report"] = retained(root, path)
                runtime = root / "publishers.json"
                runtime.write_text(json.dumps({"configuration_sha256": runtime_hash}))
                plan["runtime_publisher_inventory"] = retained(root, runtime)
                with self.assertRaises(ValueError):
                    assess(plan, root)

    def test_canary_does_not_hide_another_partitions_regression(self):
        for field, value in (("source_position", 2), ("replica_position", 7), ("finalized_block", -1),
                             ("term", -1), ("transaction", {"tx_id": 2, "system_time_micros": 99})):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                plan = minimal_plan(root)
                other = {**checkpoint(), "partition": 1}
                plan["partitions"].append(other)
                current = {**checkpoint(), "source_position": 4, "replica_position": 9,
                           "transaction": {"tx_id": 4, "system_time_micros": 101}}
                receipt(root, plan, "canary", origin="client", acknowledged_transaction={"database": "xtdb", "partition": 0, **current["transaction"]},
                        acknowledged_partitions=[current, {**other, field: value}], second_node_read=True, read_back_committed=True,
                        semantic_agreement=True, history_checked=True)
                self.assertIn("canary: partition regressed from handoff: ('xtdb', 1)", assess(plan, root)["missing_requirements"])

    def test_external_canary_advances_durable_token_without_source_log_movement(self):
        for kind in ("postgres", "kafka-connect"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                plan = minimal_plan(root)
                source = {"kind": kind, "remote": "upstream"}
                path = root / "configuration.json"
                config = json.loads(path.read_text())
                config["databases"][0]["external_source"] = source
                path.write_text(json.dumps(config))
                plan["configuration_report"] = retained(root, path)
                plan["partitions"][0]["external_token"] = "a" * 64
                current = {**checkpoint(), "replica_position": 9, "finalized_block": 1, "external_token": "b" * 64,
                           "transaction": {"tx_id": 4, "system_time_micros": 101}}
                receipt(root, plan, "canary", origin="external", acknowledged_transaction={"database": "xtdb", "partition": 0, **current["transaction"]},
                        acknowledged_partitions=[current], external_progress=[{"database": "xtdb", "partition": 0, "source": source,
                            "previous_token": "a" * 64, "durable_token": "b" * 64, "previous_position": 10,
                            "durable_position": 11, "confirmed_position": 11 if kind == "postgres" else None}],
                        second_node_read=True, read_back_committed=True, semantic_agreement=True, history_checked=True)
                self.assertFalse(any(item.startswith("canary:") for item in assess(plan, root)["missing_requirements"]))
                changed = json.loads((root / "canary.json").read_text())
                changed["acknowledged_partitions"][0]["external_token"] = "a" * 64
                (root / "canary.json").write_text(json.dumps(changed))
                plan["receipts"] = [retained(root, root / "canary.json")]
                self.assertIn("canary: external write lacks forward durable upstream extent", assess(plan, root)["missing_requirements"])

    def test_compatible_configuration_does_not_authorize_cutover_without_operational_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = assess(minimal_plan(root), root)
            self.assertFalse(report["prerequisites_satisfied"])
            self.assertFalse(report["production_actions_authorized"])
            self.assertFalse(report["cutover_executed"])
            self.assertEqual(len(STAGES) + 1, len(report["missing_requirements"]))

    def test_expired_historical_rehearsal_does_not_report_an_open_rollback_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = minimal_plan(root)
            plan["assessment_time_unix_seconds"] = 1000
            report = assess(plan, root)
            self.assertEqual("expired", report["rollback_window"])
            self.assertIn("rollback retention window expired at assessment", report["missing_requirements"])

    def test_initial_inventory_matches_partition_identities_without_freezing_future_log_positions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = minimal_plan(root)
            receipt(root, plan, "inventory", publishers=plan["publishers"], client_versions=["client"], restore_procedure="procedure",
                    partitions=[{"database": "xtdb", "partition": 0}])
            report = assess(plan, root)
            self.assertNotIn("inventory: all attached database partitions", report["missing_requirements"])

    def test_equal_fallback_arrays_must_still_match_acknowledged_restart_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = minimal_plan(root)
            receipt(root, plan, "canary", acknowledged_transaction={"database": "xtdb", "partition": 0, "tx_id": 3, "system_time_micros": 100},
                    acknowledged_partitions=[checkpoint()], second_node_read=True, read_back_committed=True, semantic_agreement=True, history_checked=True)
            receipt(root, plan, "restart", acknowledged_partitions=[checkpoint()], semantic_agreement=True, history_checked=True)
            wrong = {**checkpoint(), "source_position": 99}
            receipt(root, plan, "restore_fallback", last_acknowledged_partitions=[wrong], recovered_partitions=[wrong], elapsed_seconds=1,
                    semantic_agreement=True, history_checked=True)
            self.assertIn("fallback: boundary is not the retained latest acknowledged restart evidence", assess(plan, root)["missing_requirements"])

    def test_reusing_the_handoff_transaction_is_not_a_native_canary_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = minimal_plan(root)
            receipt(root, plan, "canary", acknowledged_transaction={"database": "xtdb", "partition": 0, "tx_id": 3, "system_time_micros": 100},
                    acknowledged_partitions=[checkpoint()], second_node_read=True, read_back_committed=True, semantic_agreement=True, history_checked=True)
            missing = assess(plan, root)["missing_requirements"]
            self.assertIn("canary: no forward admitted native write from handoff", missing)
            self.assertIn("canary: acknowledgement reuses or precedes the handoff transaction", missing)

    def test_none_and_zero_term_sequence_and_external_token_changes_remain_distinct(self):
        absent = checkpoint()
        zero = {**absent, "term_sequence": 0}
        self.assertEqual(absent, boundary(absent))
        self.assertEqual(zero, boundary(zero))
        self.assertFalse(same_boundaries({("xtdb", 0): absent}, {("xtdb", 0): zero}))
        self.assertFalse(same_boundaries({("xtdb", 0): absent}, {("xtdb", 0): {**absent, "external_token": "a" * 64}}))
        with self.assertRaises(ValueError):
            boundary({**absent, "source_position": True})

    def test_artifact_mutation_and_redirected_receipts_fail_before_qualification(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = minimal_plan(root)
            (root / "native").write_bytes(b"different native")
            with self.assertRaises(ValueError):
                assess(plan, root)
            (root / "alias").symlink_to(root / "fallback_jvm")
            with self.assertRaises(ValueError):
                artifact(root, {**plan["artifacts"]["fallback_jvm"], "path": "alias"})


if __name__ == "__main__":
    unittest.main()
