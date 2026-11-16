#!/usr/bin/env python3

import argparse
import json
from pathlib import Path

from evidence import artifact, canonical, digest_value, index, integer, number, read_json, text


STAGES = ("inventory", "retention", "shadow", "pause_and_drain", "fence_jvm", "handoff",
          "restore_rust", "canary", "restart", "reverse_rehearsal", "fence_rust", "restore_fallback")
BOUNDARY_FIELDS = {"database", "partition", "storage_version", "storage_epoch", "finalized_block", "source_position",
                   "replica_position", "transaction", "term", "term_sequence", "external_token"}


def partition_key(value):
    return text(value.get("database"), "database"), integer(value.get("partition"), "partition")


def boundary(value):
    if set(value) != BOUNDARY_FIELDS:
        raise ValueError("partition boundary lacks an exact versioned field set")
    partition_key(value)
    for field in ("storage_version", "storage_epoch"):
        integer(value[field], field)
        if value[field] > 2**31 - 1:
            raise ValueError("storage identity exceeds its signed 32-bit representation")
    for field in ("finalized_block", "source_position", "replica_position", "term"):
        integer(value[field], field, -1)
        if value[field] > 2**63 - 1:
            raise ValueError("partition position exceeds its signed 64-bit representation")
    if value["term_sequence"] is not None:
        integer(value["term_sequence"], "term sequence")
        if value["term_sequence"] > 2**63 - 1:
            raise ValueError("term sequence exceeds its signed 64-bit representation")
    transaction = value["transaction"]
    if transaction is not None:
        if set(transaction) != {"tx_id", "system_time_micros"}:
            raise ValueError("boundary transaction identity is incomplete")
        integer(transaction["tx_id"], "transaction ID", -1)
        integer(transaction["system_time_micros"], "transaction system time", -(2**63))
        if any(transaction[field] > 2**63 - 1 for field in transaction):
            raise ValueError("transaction identity exceeds its signed 64-bit representation")
    if value["external_token"] is not None:
        digest_value(value["external_token"], "opaque external token digest")
    return value


def boundaries(values):
    result = index(values, partition_key, "partition boundary")
    for value in result.values():
        boundary(value)
    return result


def same_boundaries(left, right):
    return set(left) == set(right) and all(canonical(left[key]) == canonical(right[key]) for key in left)


def nonregressing(prior, current):
    if any(current[key] != prior[key] for key in ("storage_version", "storage_epoch")):
        return False
    if any(current[key] < prior[key] for key in ("finalized_block", "source_position", "replica_position", "term")):
        return False
    if current["term"] == prior["term"] and prior["term_sequence"] is not None:
        if current["term_sequence"] is None or current["term_sequence"] < prior["term_sequence"]:
            return False
    previous, following = prior["transaction"], current["transaction"]
    if previous is not None:
        if following is None or any(following[key] < previous[key] for key in previous):
            return False
        if (following["tx_id"] == previous["tx_id"]) != (following["system_time_micros"] == previous["system_time_micros"]):
            return False
    return True


def external_progress(value, source, prior, current):
    if value is None or value.get("source") != source:
        return False
    if value.get("previous_token") != prior["external_token"] or value.get("durable_token") != current["external_token"] or current["external_token"] is None:
        return False
    previous = integer(value.get("previous_position"), "previous upstream position", -1)
    durable = integer(value.get("durable_position"), "durable upstream position")
    if durable <= previous or current["finalized_block"] <= prior["finalized_block"]:
        return False
    previous_tx, current_tx = prior["transaction"], current["transaction"]
    if current_tx is None or previous_tx is not None and any(current_tx[key] <= previous_tx[key] for key in previous_tx):
        return False
    if source.get("kind") == "postgres":
        confirmed = integer(value.get("confirmed_position"), "confirmed upstream position")
        return confirmed <= durable <= 2**64 - 1
    if source.get("kind") == "kafka-connect":
        return durable <= 2**63 - 1 and "confirmed_position" in value and value["confirmed_position"] is None
    return False


def assess(plan, root):
    if plan.get("schema_version") != 1:
        raise ValueError("unsupported cutover evidence schema")
    deployment = text(plan.get("deployment"), "deployment")
    if plan.get("evidence_scope") != "isolated_preproduction_rehearsal":
        raise ValueError("cutover prerequisite assessment requires an explicit isolated rehearsal scope")
    owner = text(plan.get("control_plane_owner"), "control-plane owner")
    publishers = index(plan.get("publishers"), lambda item: text(item.get("id"), "publisher ID"), "publisher")
    for publisher in publishers.values():
        if publisher.get("kind") not in {"writer", "compactor", "trie_gc", "block_gc", "cdc_acknowledger"}:
            raise ValueError("unknown publisher kind")
        text(publisher.get("database"), "publisher database")
    expected = boundaries(plan.get("partitions"))
    artifacts = plan.get("artifacts", {})
    if set(artifacts) != {"deployed_jvm", "native", "fallback_jvm", "migrator"}:
        raise ValueError("cutover requires independently identified deployed, native, fallback and migrator artifacts")
    for name, value in artifacts.items():
        artifact(root, value)
        text(value.get("revision"), name + " revision")
    pause_budget = number(plan.get("write_pause_budget_seconds"), "write pause budget", 0.001)
    recovery_budget = number(plan.get("recovery_target_seconds"), "recovery target", 0.001)
    rollback_deadline = integer(plan.get("rollback_deadline_unix_seconds"), "rollback deadline", 1)
    assessment_time = integer(plan.get("assessment_time_unix_seconds"), "assessment time", 1)
    configuration = read_json(artifact(root, plan["configuration_report"]))
    configuration_digest = digest_value(configuration.get("configuration_sha256"), "configuration SHA256")
    missing = []
    if assessment_time >= rollback_deadline:
        missing.append("rollback retention window expired at assessment")
    if configuration.get("configuration_compatible") is not True or configuration.get("configuration_gaps"):
        missing.append("configuration compatibility")
    provenance = configuration.get("provenance") or {}
    if provenance.get("server_artifact_sha256") != artifacts["native"]["sha256"] or provenance.get("migrator_artifact_sha256") != artifacts["migrator"]["sha256"]:
        missing.append("configuration artifact provenance")
    databases = index(configuration.get("databases"), lambda item: text(item.get("name"), "configured database"), "configured database")
    if any("external_source" not in database for database in databases.values()):
        raise ValueError("configuration report omits a database source identity")
    partition_counts = {item["name"]: integer(item.get("partitions"), "configured partitions", 1) for item in databases.values()}
    if sum(partition_counts.values()) != len(expected) or any(database not in partition_counts or partition >= partition_counts[database] for database, partition in expected):
        missing.append("complete configured partition inventory")
    runtime_inventory = plan.get("runtime_publisher_inventory")
    if runtime_inventory is None:
        missing.append("grounded runtime publisher inventory")
    else:
        runtime = read_json(artifact(root, runtime_inventory))
        runtime_digest = digest_value(runtime.get("configuration_sha256"), "runtime inventory configuration SHA256")
        if runtime.get("deployment") != deployment or runtime.get("control_plane_owner") != owner or runtime.get("complete") is not True:
            missing.append("runtime publisher inventory authority and completeness")
        if runtime_digest != configuration_digest or runtime.get("publishers") != list(publishers.values()) or set(runtime.get("database_names", [])) != {key[0] for key in expected}:
            missing.append("runtime publisher inventory configuration and scope")
    receipts = {}
    for reference in plan.get("receipts", []):
        receipt = read_json(artifact(root, reference))
        stage = receipt.get("stage")
        if stage not in STAGES or stage in receipts:
            raise ValueError("unknown or duplicated cutover stage receipt")
        if receipt.get("deployment") != deployment or receipt.get("control_plane_owner") != owner:
            raise ValueError("cutover receipt belongs to another deployment or authority")
        if receipt.get("artifact_digests") != {name: value["sha256"] for name, value in artifacts.items()}:
            raise ValueError("cutover receipt names different writer/fallback artifacts")
        integer(receipt.get("observed_at_unix_seconds"), "receipt observation time", 1)
        if receipt["observed_at_unix_seconds"] > assessment_time:
            missing.append(stage + ": observation is after assessment time")
        if receipt.get("verified") is not True or not receipt.get("evidence"):
            missing.append(stage + ": verified evidence")
        for evidence in receipt.get("evidence", []):
            artifact(root, evidence)
        receipts[stage] = receipt
    for stage in STAGES:
        if stage not in receipts:
            missing.append(stage + ": absent receipt")
    if "inventory" in receipts:
        inventory = receipts["inventory"]
        if inventory.get("publishers") != list(publishers.values()) or not inventory.get("client_versions") or not inventory.get("restore_procedure"):
            missing.append("inventory: all publishers, clients and restore procedure")
        if set(index(inventory.get("partitions"), partition_key, "inventory partition")) != set(expected):
            missing.append("inventory: all attached database partitions")
    for stage in ("fence_jvm", "fence_rust"):
        if stage in receipts:
            value = receipts[stage]
            if set(value.get("fenced_publishers", [])) != set(publishers) or value.get("publication_denied") is not True or not value.get("writer_lease_id"):
                missing.append(stage + ": complete publication fence and lease")
    for stage in ("handoff", "restore_rust"):
        if stage in receipts and not same_boundaries(boundaries(receipts[stage].get("partitions")), expected):
            missing.append(stage + ": exact handoff boundary")
    for stage in ("shadow", "canary", "restart", "reverse_rehearsal", "restore_fallback"):
        if stage in receipts:
            value = receipts[stage]
            if value.get("semantic_agreement") is not True or value.get("history_checked") is not True:
                missing.append(stage + ": semantic and historical observations")
    if "shadow" in receipts:
        value = receipts["shadow"]
        if value.get("read_only_credentials") is not True or value.get("publishers_disabled") is not True or value.get("matched_durable_basis") is not True:
            missing.append("shadow: exclusive reader capabilities and equal basis")
    if "pause_and_drain" in receipts:
        value = receipts["pause_and_drain"]
        if value.get("ingress_paused") is not True or value.get("upstream_paused") is not True or value.get("ambiguous_outcomes_reconciled") is not True or value.get("outstanding_blocks_confirmed") is not True:
            missing.append("pause_and_drain: admitted work and upstream intake")
        if number(value.get("elapsed_seconds"), "pause elapsed") > pause_budget:
            missing.append("write-pause budget")
    if "retention" in receipts:
        value = receipts["retention"]
        if integer(value.get("protected_until_unix_seconds"), "retention deadline") < rollback_deadline or value.get("logs_and_object_versions_protected") is not True:
            missing.append("retention through rollback deadline")
    if "canary" in receipts:
        value = receipts["canary"]
        acknowledged = boundaries(value.get("acknowledged_partitions"))
        tx = value.get("acknowledged_transaction", {})
        if set(tx) != {"database", "partition", "tx_id", "system_time_micros"}:
            raise ValueError("canary acknowledgement lacks exact partition and transaction identity")
        selected = partition_key(tx)
        progress = index(value["external_progress"], partition_key, "external progress") if value.get("external_progress") else {}
        if not set(progress).issubset(expected):
            raise ValueError("external progress names an unconfigured partition")
        for field in ("tx_id", "system_time_micros"):
            integer(tx[field], "canary " + field, 0 if field == "tx_id" else -(2**63))
            if tx[field] > 2**63 - 1:
                raise ValueError("canary acknowledgement exceeds signed 64-bit representation")
        if set(acknowledged) != set(expected) or selected not in acknowledged or acknowledged[selected]["transaction"] != {key: tx[key] for key in ("tx_id", "system_time_micros")}:
            missing.append("canary: acknowledged transaction and complete partition boundary")
        elif selected in expected:
            prior, current = expected[selected], acknowledged[selected]
            previous_tx = prior["transaction"]
            source = databases.get(selected[0], {}).get("external_source")
            origin = "external" if source is not None else "client"
            if value.get("origin") != origin:
                missing.append("canary: origin does not match configured database source")
            if current["replica_position"] <= prior["replica_position"] or not nonregressing(prior, current):
                missing.append("canary: no forward admitted native write from handoff")
            if origin == "client" and current["source_position"] <= prior["source_position"]:
                missing.append("canary: no forward admitted native write from handoff")
            if origin == "external" and (current["external_token"] == prior["external_token"] or not external_progress(progress.get(selected), source, prior, current)):
                missing.append("canary: external write lacks forward durable upstream extent")
            if previous_tx is not None and (tx["tx_id"] <= previous_tx["tx_id"] or tx["system_time_micros"] <= previous_tx["system_time_micros"]):
                missing.append("canary: acknowledgement reuses or precedes the handoff transaction")
        for key in set(expected) & set(acknowledged):
            prior, current = expected[key], acknowledged[key]
            if not nonregressing(prior, current):
                missing.append("canary: partition regressed from handoff: " + repr(key))
            source = databases.get(key[0], {}).get("external_source")
            if source is None and (prior["external_token"] is not None or current["external_token"] is not None):
                missing.append("canary: client partition carries an external token: " + repr(key))
            if source is not None and prior["external_token"] != current["external_token"]:
                if not external_progress(progress.get(key), source, prior, current):
                    missing.append("canary: changed upstream token lacks durable progression: " + repr(key))
        if value.get("second_node_read") is not True or value.get("read_back_committed") is not True:
            missing.append("canary: acknowledged read-back and independent node")
        if "restart" in receipts and not same_boundaries(boundaries(receipts["restart"].get("acknowledged_partitions")), acknowledged):
            missing.append("restart: exact acknowledged canary boundary")
    if "reverse_rehearsal" in receipts:
        value = receipts["reverse_rehearsal"]
        required = {"after_acknowledged_canary_crash", "after_partial_compaction_upload"}
        if set(value.get("fault_boundaries", [])) != required or value.get("performed_on_clone_before_production_mutation") is not True or value.get("shared_key_bytes_unchanged") is not True:
            missing.append("reverse rehearsal: both durable crash boundaries and immutable retry")
    if "restore_fallback" in receipts:
        value = receipts["restore_fallback"]
        acknowledged = boundaries(value.get("last_acknowledged_partitions"))
        recovered = boundaries(value.get("recovered_partitions"))
        if set(acknowledged) != set(expected) or not same_boundaries(acknowledged, recovered):
            missing.append("fallback: exact latest acknowledged boundary including external tokens")
        latest = receipts.get("restart", {}).get("acknowledged_partitions")
        if latest is None or not same_boundaries(acknowledged, boundaries(latest)):
            missing.append("fallback: boundary is not the retained latest acknowledged restart evidence")
        if number(value.get("elapsed_seconds"), "fallback recovery elapsed") > recovery_budget:
            missing.append("fallback recovery target")
    times = [receipts[stage]["observed_at_unix_seconds"] for stage in STAGES if stage in receipts]
    if times != sorted(times):
        missing.append("handoff stage ordering")
    if plan.get("simultaneous_writers") is not False:
        missing.append("separately qualified simultaneous writers")
    return {"schema_version": 1, "deployment": deployment, "prerequisites_satisfied": not missing,
            "missing_requirements": missing, "production_actions_authorized": False, "cutover_executed": False,
            "authority_boundary": "operator/control-plane supplied observations; artifact checks do not prove publication revocation",
            "evidence_scope": "isolated_preproduction_rehearsal", "live_handoff_still_required": True,
            "assessment_time_unix_seconds": assessment_time,
            "rollback_window": "open" if assessment_time < rollback_deadline else "expired",
            "on_failure": "keep ingress paused and retain acknowledged Rust logs and files"}


def main():
    parser = argparse.ArgumentParser(description="Validate retained cutover prerequisites without opening or changing a deployment.")
    parser.add_argument("plan", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    report = assess(read_json(args.plan), args.plan.resolve().parent)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    return 0 if report["prerequisites_satisfied"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
