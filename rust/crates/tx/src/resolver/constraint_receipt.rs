use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Result, StructFields, Value};

use crate::{HostedControlWrite, TxOp, source::Operation};

const BUSINESS: &str = "bj_fact_root_receipts";
const RECEIPT_BUDGET: &str = "receipt_budget:";
const PROPOSAL_RECEIPT: &str = "root_proposal_receipt:";
const EXECUTION: &str = "bj_private_execution_receipts";
const ADMISSION: &str = "bj_fact_root_admission";
const ATTEMPTS: &str = "fact_attempts";
const PUBLICATIONS: &str = "fact_publications";
const ROOT_HEAD: &str = "head";
const RECEIPT_HEAD: &str = "sync_receipt_head";
const DECISION_PREFIX: &str = "root_decision:";
const IMPACT_PREFIX: &str = "root_decision_impact:";
const CLOCK_TASK: &str = "clock-task:";
const ACTION_PERFORMANCE: &str = "action-performance:";
const LANDING_RECEIPT: &str = "root_proposal_landing:";
const LANDING_PROOF: &str = "root_proposal_landing_proof:";
const MAX_DECISION_BYTES: usize = 1 << 20;

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.hosted/constraint-receipt",
        "A constraint receipt must settle the exact original registered dispatch",
    )
}

pub(super) fn split(ops: &[Operation], placed: bool) -> Result<(&[Operation], &[Operation])> {
    let Some(index) = ops
        .iter()
        .position(|op| matches!(op, Operation::Public(TxOp::HostedConstraintRefusal)))
    else {
        return Ok((ops, &[]));
    };
    let primary = &ops[..index];
    let refusal = &ops[index + 1..];
    if refusal.iter().filter_map(hosted).any(|control| landing_slot(control).is_some()) {
        validate_landing_choice(primary, refusal, placed)?;
        return Ok((primary, refusal));
    }
    if refusal.iter().filter_map(hosted).any(|control| {
        control.expected().is_some() && continuation_control(control)
    }) {
        validate_continuation_choice(primary, refusal, placed)?;
        return Ok((primary, refusal));
    }
    validate_registered_choice(primary, refusal, placed)?;
    Ok((primary, refusal))
}

fn validate_registered_choice(primary: &[Operation], refusal: &[Operation], placed: bool) -> Result<()> {
    use crate::hosted_control::{
        ProposalLandingEffect, hosted_proposal_land, hosted_proposal_overlay, retained_release_world,
        root_partition_frontiers_match,
    };

    let proposal = hosted_proposal_overlay(primary).is_some();
    let world = proposal
        || retained_release_world(primary).is_some()
        || hosted_proposal_land(primary).is_some_and(|landing| landing.effect == ProposalLandingEffect::Close);
    let embedded = primary.iter().find_map(|op| match op {
        Operation::Public(TxOp::OwnedForkWorld { attempt, fingerprint, .. }) => Some((attempt, fingerprint)),
        Operation::Public(TxOp::OwnedPublishWorld { attempt, fingerprint, .. }) if world => Some((attempt, fingerprint)),
        _ => None,
    });
    if !placed || !root_partition_frontiers_match(primary)
        || !(world || primary.iter().all(primary_op)
            || crate::hosted_control::hosted_root_world_reads(primary))
    {
        return Err(invalid());
    }
    let offered_controls = primary.iter().filter_map(hosted)
        .filter(|control| !control.is_check()).collect::<Vec<_>>();
    let mut control_addresses = std::collections::BTreeSet::new();
    if offered_controls.iter().any(|control| !control_addresses.insert((control.name(), control.id()))) {
        return Err(invalid());
    }
    let attempts = primary.iter().filter(|op| write_relation(op)
        .is_some_and(|relation| relation.name() == ATTEMPTS)).collect::<Vec<_>>();
    if (attempts.is_empty() && embedded.is_none()) || (!attempts.is_empty() && embedded.is_some()) {
        return Err(invalid());
    }
    let mut copied_attempts = std::collections::BTreeSet::new();
    let writes_data = primary.iter().filter_map(write_relation)
        .any(|relation| !matches!(relation.name(), ATTEMPTS | PUBLICATIONS));
    let has_count = primary.iter().any(|op| matches!(op,
        Operation::Public(TxOp::OwnedCheckCapacity { .. })
    ));
    let offers_root_head = primary.iter().any(|op| matches!(op,
        Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, .. })
            if relation.name() == PUBLICATIONS && marker(documents.rows(), ROOT_HEAD)
    ));
    let primary_head = if writes_data || (has_count && offers_root_head) {
        ROOT_HEAD
    } else if embedded.is_some() || has_count {
        RECEIPT_HEAD
    } else {
        return Err(invalid());
    };
    let mut expected_types = 1 | 2 | 32;
    for control in &offered_controls {
        if let EntityId::String(id) = control.id() {
            if control.name() == BUSINESS && id.starts_with(RECEIPT_BUDGET) {
                expected_types |= 16;
            }
            if control.name() == ADMISSION {
                if proposal && id.starts_with(PROPOSAL_RECEIPT) { expected_types |= 128; }
                if id.starts_with("sync_dispatch:") { expected_types |= 4; }
                if id == "sync_public_pending" { expected_types |= 8; }
                if id.starts_with(DECISION_PREFIX) { expected_types |= 64; }
            }
        }
    }
    let impact_count = offered_controls.iter().filter(|control| decision_impact(control)).count();
    let mut types = 0;
    let mut impact_ids = std::collections::BTreeSet::new();
    let mut receipts = None;
    let mut charges = None;
    for alternative in refusal {
        let bit = match alternative {
            Operation::Public(TxOp::HostedControl(control)) => {
                let original = offered_controls
                    .iter().copied()
                    .find(|original| {
                        original.name() == control.name() && original.id() == control.id()
                    })
                    .ok_or_else(invalid)?;
                if decision_impact(control) {
                    let replacement = control.replacement().ok_or_else(invalid)?;
                    if control.scope() != original.scope() || control.expected() != original.expected()
                        || original.replacement() != Some(replacement)
                        || !matches!(replacement.get("admission"), Some(Value::Binary(body)) if !body.is_empty() && body.len() <= 4096)
                        || !impact_ids.insert(control.id())
                    {
                        return Err(invalid());
                    }
                    continue;
                }
                let choice = control_choice(original, control)?;
                match choice {
                    1 => receipts = original.replacement().zip(control.replacement()),
                    16 => charges = original.replacement().zip(control.replacement()),
                    _ => (),
                }
                choice
            }
            Operation::Public(TxOp::OwnedInsertDocs {
                relation,
                documents,
                valid_from,
                valid_to,
            }) if relation.name() == ATTEMPTS => {
                let index = if let Some((attempt, fingerprint)) = embedded {
                    if !attempt_marker(documents, *valid_from, *valid_to, attempt, fingerprint) {
                        return Err(invalid());
                    }
                    0
                } else {
                    attempts.iter().position(|op| matches!(op, Operation::Public(TxOp::OwnedInsertDocs {
                        relation: old, documents: old_rows, valid_from: start, valid_to: end,
                    }) if old == relation && old_rows.rows() == documents.rows()
                        && start == valid_from && end == valid_to)).ok_or_else(invalid)?
                };
                if !copied_attempts.insert(index) { return Err(invalid()); }
                continue;
            }
            Operation::Public(TxOp::OwnedReplaceDocs {
                relation,
                documents,
                valid_from,
                valid_to,
            }) if relation.name() == PUBLICATIONS && marker(documents.rows(), RECEIPT_HEAD) => {
                if !(embedded.is_some() && *valid_from == Some(rtdb_types::Timestamp::MIN)
                    && *valid_to == Some(rtdb_types::Timestamp::MAX))
                    && !primary.iter().any(|op| matches!(op, Operation::Public(TxOp::OwnedReplaceDocs {
                        relation: old, documents: old_rows, valid_from: start, valid_to: end,
                    }) if old == relation && marker(old_rows.rows(), primary_head) && start == valid_from && end == valid_to))
                {
                    return Err(invalid());
                }
                32
            }
            _ => return Err(invalid()),
        };
        if types & bit != 0 {
            return Err(invalid());
        }
        types |= bit;
    }
    if types != expected_types
        || copied_attempts.len() != attempts.len() + usize::from(embedded.is_some())
        || impact_ids.len() != impact_count
        || !receipts.is_some_and(|receipts| receipt_charge_matches(receipts, charges))
    {
        return Err(invalid());
    }
    Ok(())
}

fn attempt_marker(
    documents: &crate::Documents,
    from: Option<rtdb_types::Timestamp>,
    to: Option<rtdb_types::Timestamp>,
    attempt: &str,
    fingerprint: &[u8; 32],
) -> bool {
    let [row] = documents.rows() else { return false; };
    from == Some(rtdb_types::Timestamp::MIN)
        && to == Some(rtdb_types::Timestamp::MAX)
        && row.len() == 2
        && matches!(row.get("_id"), Some(Value::Utf8(id)) if id == attempt)
        && matches!(row.get("fingerprint"), Some(Value::Binary(bytes)) if bytes.as_slice() == fingerprint)
}

fn landing_slot(control: &HostedControlWrite) -> Option<usize> {
    if control.name() != ADMISSION || control.expected().is_some() || control.is_check() {
        return None;
    }
    let EntityId::String(id) = control.id() else { return None; };
    if id.starts_with(LANDING_RECEIPT) { Some(0) }
    else if id.starts_with(LANDING_PROOF) { Some(1) }
    else { None }
}

fn validate_landing_choice(primary: &[Operation], refusal: &[Operation], placed: bool) -> Result<()> {
    use crate::hosted_control::{ProposalLandingEffect, hosted_proposal_land};
    use rtdb_types::Timestamp;

    if !placed || refusal.len() != 3 || hosted_proposal_land(primary)
        .is_none_or(|landing| landing.effect == ProposalLandingEffect::Close)
    {
        return Err(invalid());
    }
    let Some((attempt, fingerprint)) = primary.iter().find_map(|op| match op {
        Operation::Public(TxOp::OwnedPublishWorld { attempt, fingerprint, land_root: Some(_), .. }) => Some((attempt, fingerprint)),
        _ => None,
    }) else { return Err(invalid()); };
    let mut found_attempt = false;
    let mut offered = [None, None];
    let mut selected = [None, None];
    for op in refusal {
        match op {
            Operation::Public(TxOp::HostedControl(control)) => {
                let slot = landing_slot(control).ok_or_else(invalid)?;
                let original = primary.iter().filter_map(hosted).find(|prior| {
                    prior.id() == control.id() && landing_slot(prior) == Some(slot)
                }).ok_or_else(invalid)?;
                let before = original.replacement().ok_or_else(invalid)?;
                let after = control.replacement().ok_or_else(invalid)?;
                if control.scope() != original.scope() || !same_binding(before, after, &["admission"])
                    || offered[slot].replace(admission_bytes(before).ok_or_else(invalid)?).is_some()
                {
                    return Err(invalid());
                }
                selected[slot] = Some(admission_bytes(after).ok_or_else(invalid)?);
            }
            Operation::Public(TxOp::OwnedInsertDocs { relation, documents, valid_from, valid_to })
                if relation.name() == ATTEMPTS && *valid_from == Some(Timestamp::MIN)
                    && *valid_to == Some(Timestamp::MAX) =>
            {
                if found_attempt || !attempt_marker(documents, *valid_from, *valid_to, attempt, fingerprint) {
                    return Err(invalid());
                }
                found_attempt = true;
            }
            _ => return Err(invalid()),
        }
    }
    let ([Some(before), Some(proof)], [Some(after), Some(refusal_proof)]) = (offered, selected)
        else { return Err(invalid()); };
    if !found_attempt || before != proof || after != refusal_proof || !landing_verdict(before, after) {
        return Err(invalid());
    }
    Ok(())
}

fn admission_bytes(document: &StructFields<Value>) -> Option<&[u8]> {
    let Some(Value::Binary(body)) = document.get("admission") else { return None; };
    Some(body)
}

fn landing_verdict(before: &[u8], after: &[u8]) -> bool {
    if before.len() > MAX_DECISION_BYTES || after.len() > MAX_DECISION_BYTES {
        return false;
    }
    let (Ok(mut before), Ok(after)) = (serde_json::from_slice::<serde_json::Value>(before),
        serde_json::from_slice::<serde_json::Value>(after)) else { return false; };
    let Some(record) = before.as_object_mut() else { return false; };
    if record.get("outcome").and_then(serde_json::Value::as_str) != Some("applied")
        || record.get("roots").and_then(serde_json::Value::as_array).is_none_or(Vec::is_empty)
    {
        return false;
    }
    record.insert("outcome".into(), "constraint_refused".into());
    record.insert("roots".into(), serde_json::Value::Array(Vec::new()));
    before == after
}

fn continuation_control(control: &HostedControlWrite) -> bool {
    control.name() == ADMISSION && matches!(control.id(), EntityId::String(id)
        if id.starts_with(CLOCK_TASK) || id.starts_with(ACTION_PERFORMANCE))
}

fn validate_continuation_choice(primary: &[Operation], refusal: &[Operation], placed: bool) -> Result<()> {
    if !placed || !(primary.iter().all(primary_op) || crate::hosted_control::hosted_root_world_reads(primary)) {
        return Err(invalid());
    }
    let original_controls = primary.iter().filter_map(hosted).collect::<Vec<_>>();
    let attempts = primary.iter().filter(|op| write_relation(op).is_some_and(|relation| relation.name() == ATTEMPTS))
        .collect::<Vec<_>>();
    let Some((start, end)) = primary.iter().find_map(|op| match op {
        Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to })
            if relation.name() == PUBLICATIONS && marker(documents.rows(), ROOT_HEAD) => Some((valid_from, valid_to)),
        _ => None,
    }) else { return Err(invalid()); };
    if attempts.is_empty() || original_controls.iter().any(|control| !control.is_check() && !continuation_control(control)) {
        return Err(invalid());
    }
    let mut copied_attempts = std::collections::BTreeSet::new();
    let mut head = None;
    let mut completion = None;
    let mut followup = false;
    for op in refusal {
        match op {
            Operation::Public(TxOp::HostedControl(control)) if continuation_control(control) => {
                let original = original_controls.iter().find(|prior| prior.id() == control.id() && prior.name() == control.name())
                    .ok_or_else(invalid)?;
                let EntityId::String(id) = control.id() else { return Err(invalid()); };
                let replacement = control.replacement().ok_or_else(invalid)?;
                if control.scope() != original.scope() || control.expected() != original.expected() {
                    return Err(invalid());
                }
                if control.expected().is_some() {
                    let clock = id.starts_with(CLOCK_TASK);
                    if completion.replace(clock).is_some() {
                        return Err(invalid());
                    }
                    let offered = original.replacement().ok_or_else(invalid)?;
                    if !same_binding(offered, replacement, &["admission"]) || !continuation_verdict(offered, replacement, clock) {
                        return Err(invalid());
                    }
                } else {
                    if followup || !id.starts_with(CLOCK_TASK) || !refused_run_followup(replacement) {
                        return Err(invalid());
                    }
                    followup = true;
                }
            }
            Operation::Public(TxOp::OwnedInsertDocs { relation, documents, valid_from, valid_to })
                if relation.name() == ATTEMPTS =>
            {
                let original = attempts.iter().position(|op| matches!(op,
                    Operation::Public(TxOp::OwnedInsertDocs { relation: prior, documents: rows, valid_from: start, valid_to: end })
                    if prior == relation && rows.rows() == documents.rows() && start == valid_from && end == valid_to
                )).ok_or_else(invalid)?;
                if !copied_attempts.insert(original) { return Err(invalid()); }
            }
            Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to })
                if relation.name() == PUBLICATIONS && valid_from == start && valid_to == end =>
            {
                if head.is_some() { return Err(invalid()); }
                head = Some(marker_id(documents.rows()).ok_or_else(invalid)?);
            }
            _ => return Err(invalid()),
        }
    }
    let clock = completion.ok_or_else(invalid)?;
    if copied_attempts.len() != attempts.len() || followup && !clock
        || head != Some(if clock { ROOT_HEAD } else { RECEIPT_HEAD })
    {
        return Err(invalid());
    }
    Ok(())
}

fn admission_json(document: &StructFields<Value>) -> Option<serde_json::Value> {
    let body = admission_bytes(document)?;
    if body.len() > 64 * 1024 { return None; }
    serde_json::from_slice(body).ok()
}

fn clock_command_matches(record: &serde_json::Value) -> bool {
    use sha2::{Digest, Sha256};

    match record.pointer("/task/command") {
        Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::String(command)) => {
            let digest = format!("{:x}", Sha256::digest(command.as_bytes()));
            command.starts_with("[\"bijection.task.emit.v2\",")
                && record.pointer("/task/digest").and_then(serde_json::Value::as_str)
                    == Some(digest.as_str())
        }
        Some(_) | None => false,
    }
}

fn continuation_verdict(original: &StructFields<Value>, refusal: &StructFields<Value>, clock: bool) -> bool {
    let (Some(mut original), Some(refusal)) = (admission_json(original), admission_json(refusal)) else { return false; };
    if clock && !clock_command_matches(&original) {
        return false;
    }
    let path = if !clock {
        "/continuation/completion"
    } else if original.get("previous_delivery").is_some() && original.get("delivered").is_none() {
        "/previous_delivery/completion"
    } else {
        "/delivered"
    };
    let Some(verdict) = original.pointer_mut(path) else { return false; };
    if verdict.as_str() != Some("delivered") { return false; }
    *verdict = "constraint_refused".into();
    original == refusal
}

fn refused_run_followup(document: &StructFields<Value>) -> bool {
    admission_json(document).is_some_and(|body| {
        clock_command_matches(&body) && body.pointer("/task/continuation/run_fate/outcome").is_some_and(|outcome| {
            outcome.get("status").and_then(serde_json::Value::as_str) == Some("refused")
                && outcome.get("reason").and_then(serde_json::Value::as_str) == Some("ConstraintRefused")
        })
    })
}

fn marker(rows: &[StructFields<Value>], name: &str) -> bool {
    marker_id(rows) == Some(name)
}

fn marker_id(rows: &[StructFields<Value>]) -> Option<&str> {
    let [row] = rows else {
        return None;
    };
    match row.get("_id") {
        Some(Value::Utf8(id)) if row.len() == 1 => Some(id),
        _ => None,
    }
}

fn write_relation(op: &Operation) -> Option<&rtdb_types::OwnedRelation> {
    match op {
        Operation::Public(
            TxOp::OwnedInsertDocs { relation, .. }
            | TxOp::OwnedReplaceDocs { relation, .. }
            | TxOp::OwnedArrowDocs { relation, .. }
            | TxOp::OwnedDelete { relation, .. },
        )
        | Operation::OwnedArrowIids { relation, .. }
        | Operation::OwnedWriteIids { relation, .. } => Some(relation),
        _ => None,
    }
}

fn hosted(op: &Operation) -> Option<&HostedControlWrite> {
    match op {
        Operation::Public(TxOp::HostedControl(control)) => Some(control),
        _ => None,
    }
}

fn decision_impact(control: &HostedControlWrite) -> bool {
    if control.name() != ADMISSION { return false; }
    let EntityId::String(id) = control.id() else { return false; };
    let Some((digest, page)) = id.strip_prefix(IMPACT_PREFIX).and_then(|suffix| suffix.split_once(':')) else {
        return false;
    };
    digest.len() == 64
        && digest.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && (page == "head" || page.parse::<u64>().is_ok_and(|number| number.to_string() == page))
}

fn control_choice(original: &HostedControlWrite, refusal: &HostedControlWrite) -> Result<u8> {
    let before = original.replacement().ok_or_else(invalid)?;
    let after = refusal.replacement().ok_or_else(invalid)?;
    if original.scope() != refusal.scope() || original.expected() != refusal.expected() {
        return Err(invalid());
    }
    let EntityId::String(id) = refusal.id() else {
        return Err(invalid());
    };
    match refusal.name() {
        BUSINESS if id.starts_with(RECEIPT_BUDGET) && same_binding(before, after, &["outcome"]) => Ok(16),
        BUSINESS
            if !id.starts_with(RECEIPT_BUDGET) && refusal.expected().is_none()
                && matches!(after.get("outcome"), Some(Value::Binary(body)) if !body.is_empty())
                && same_binding(before, after, &["outcome"]) =>
        {
            Ok(1)
        }
        EXECUTION
            if refusal.expected().is_none()
                && matches!(after.get("marker"), Some(Value::Binary(marker)) if !marker.is_empty())
                && same_binding(before, after, &["marker", "capsule"]) =>
        {
            Ok(2)
        }
        ADMISSION
            if refusal.expected().is_some()
                && before == after
                && id.starts_with("sync_dispatch:") =>
        {
            Ok(4)
        }
        ADMISSION
            if refusal.expected().is_some() && before == after && id == "sync_public_pending" =>
        {
            Ok(8)
        }
        ADMISSION
            if refusal.expected().is_none()
                && id.strip_prefix(DECISION_PREFIX).is_some_and(|digest| {
                    digest.len() == 64
                        && digest.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
                })
                && [before, after].iter().all(|document| {
                    matches!(document.get("admission"), Some(Value::Binary(body))
                        if !body.is_empty() && body.len() <= MAX_DECISION_BYTES)
                })
                && same_binding(before, after, &["admission"]) =>
        {
            Ok(64)
        }
        ADMISSION
            if refusal.expected().is_none() && id.starts_with(PROPOSAL_RECEIPT)
                && same_binding(before, after, &["admission"])
                && proposal_refusal(before, after) =>
        {
            Ok(128)
        }
        _ => Err(invalid()),
    }
}

fn proposal_refusal(before: &StructFields<Value>, after: &StructFields<Value>) -> bool {
    let Some(bytes) = admission_bytes(before).filter(|body| body.len() <= MAX_DECISION_BYTES) else {
        return false;
    };
    let (Ok(mut before), Some(after)) = (serde_json::from_slice::<serde_json::Value>(bytes), admission_json(after))
    else {
        return false;
    };
    if before.get("writes_root").and_then(serde_json::Value::as_bool) != Some(false) {
        return false;
    }
    let Some(root) = before.get_mut("root") else { return false; };
    *root = serde_json::Value::Null;
    let Some(approval) = before.get_mut("approval") else { return false; };
    *approval = serde_json::Value::Null;
    before == after
}

fn receipt_charge_matches(
    (before, after): (&StructFields<Value>, &StructFields<Value>),
    charges: Option<(&StructFields<Value>, &StructFields<Value>)>,
) -> bool {
    let Some((reserved, refused)) = charges else {
        return before.get("budget_label").is_none();
    };
    let Some(Value::Utf8(label)) = before.get("budget_label") else { return false; };
    let bytes = |document: &StructFields<Value>| match document.get("outcome") {
        Some(Value::Binary(body)) => Some(body.len() as i128),
        _ => None,
    };
    let total = |charge: &StructFields<Value>| match charge.get("outcome") {
        Some(Value::Binary(body)) if matches!(charge.get("fingerprint"),
            Some(Value::Binary(fingerprint)) if fingerprint.as_slice() == [0; 32])
            && matches!(charge.get("until_nanos"), Some(Value::I64(0)))
            && matches!(charge.get("budget_label"), Some(Value::Utf8(bound)) if bound == label) =>
        {
            Some(i128::from(u64::from_be_bytes(body.as_slice().try_into().ok()?)))
        }
        _ => None,
    };
    match (bytes(before), bytes(after), total(reserved), total(refused)) {
        (Some(before), Some(after), Some(reserved), Some(refused)) =>
            reserved >= before && refused >= after && refused - reserved == after - before,
        _ => false,
    }
}

fn same_binding(
    original: &StructFields<Value>,
    refusal: &StructFields<Value>,
    choice: &[&str],
) -> bool {
    let fixed = |field: &str| !choice.contains(&field);
    original
        .iter()
        .all(|(field, value)| !fixed(field) || refusal.get(field.as_str()) == Some(value))
        && refusal
            .keys()
            .all(|field| !fixed(field) || original.contains_key(field.as_str()))
}

fn primary_op(op: &Operation) -> bool {
    match op {
        Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedClockSlot { .. }
            | TxOp::OwnedForkWorld { .. } | TxOp::HostedControl(_)) => true,
        Operation::Public(
            TxOp::OwnedCheck { relation, .. }
            | TxOp::OwnedCheckRange { relation, .. }
            | TxOp::OwnedCheckCapacity { relation, .. }
            | TxOp::OwnedRelativePeriod { relation, .. }
            | TxOp::OwnedInsertDocs { relation, .. }
            | TxOp::OwnedReplaceDocs { relation, .. }
            | TxOp::OwnedArrowDocs { relation, .. }
            | TxOp::OwnedDelete { relation, .. },
        )
        | Operation::OwnedArrowIids { relation, .. }
        | Operation::OwnedWriteIids { relation, .. } => {
            relation.is_root_data() || matches!(relation.name(), ATTEMPTS | PUBLICATIONS)
        }
        _ => false,
    }
}

pub(super) fn shared_check(op: &&Operation) -> bool {
    match op {
        Operation::Public(
            TxOp::OwnedDeadline { .. }
            | TxOp::OwnedCheck { .. }
            | TxOp::OwnedCheckRange { .. }
            | TxOp::OwnedCheckWorldScope { .. }
            | TxOp::OwnedCheckWorldPurge { .. },
        ) => true,
        Operation::Public(TxOp::HostedControl(control)) => control.is_check(),
        _ => false,
    }
}

pub(super) fn may_settle(error: &Anomaly) -> bool {
    error.category == AnomalyCategory::Conflict
        && matches!(
            error.code.as_str(),
            "rtdb.owned/unique-taken"
                | "rtdb.owned/capacity-exceeded"
                | "rtdb.owned/count-bound-exceeded"
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn landing_refusal_keeps_the_request_and_withdraws_candidate_assertions() {
        let before = br#"{"request":{"branch":"one"},"outcome":"applied","roots":[{"position":1}],"environment":{}}"#;
        let after = br#"{"request":{"branch":"one"},"outcome":"constraint_refused","roots":[],"environment":{}}"#;
        let foreign = br#"{"request":{"branch":"two"},"outcome":"constraint_refused","roots":[],"environment":{}}"#;
        assert!(landing_verdict(before, after));
        assert!(!landing_verdict(before, foreign));
        assert!(!landing_verdict(after, before));
    }

    fn publications(head: &str) -> Vec<Operation> {
        use rtdb_columnar::MemoryBudget;
        use rtdb_types::{OwnedRelation, PlacedAttemptScope, Timestamp};

        let memory = MemoryBudget::new(1 << 20);
        let scope = PlacedAttemptScope::new("installed-replica", 1).unwrap();
        let doc = |id: &str, fields: Vec<(&str, Value)>| {
            let mut row = StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
            for (name, value) in fields {
                row.insert(name.into(), value);
            }
            row
        };
        let control = |name, id, fields, expected| {
            Operation::Public(TxOp::HostedControl(
                HostedControlWrite::new(
                    OwnedRelation::new(name).unwrap(),
                    scope,
                    expected,
                    doc(id, fields),
                )
                .unwrap(),
            ))
        };
        let payload = if head == "head" { 1 } else { 2 };
        vec![
            Operation::Public(TxOp::OwnedInsertDocs {
                relation: OwnedRelation::new(ATTEMPTS).unwrap(),
                documents: crate::Documents::new(
                    vec![doc(
                        "issued",
                        vec![("fingerprint", Value::Binary(vec![1; 32]))],
                    )],
                    &memory,
                )
                .unwrap(),
                valid_from: Some(Timestamp(i64::MIN)),
                valid_to: None,
            }),
            Operation::Public(TxOp::OwnedReplaceDocs {
                relation: OwnedRelation::new(PUBLICATIONS).unwrap(),
                documents: crate::Documents::new(vec![doc(head, Vec::new())], &memory).unwrap(),
                valid_from: Some(Timestamp(i64::MIN)),
                valid_to: None,
            }),
            control(
                BUSINESS,
                "receipt",
                vec![
                    ("fingerprint", Value::Binary(vec![2; 32])),
                    ("outcome", Value::Binary(vec![payload])),
                    ("until_nanos", Value::I64(5_000)),
                ],
                None,
            ),
            control(
                EXECUTION,
                "execution",
                vec![
                    ("world", Value::I64(0)),
                    ("marker", Value::Binary(vec![payload])),
                ],
                None,
            ),
            control(
                ADMISSION,
                "sync_dispatch:request",
                vec![("admission", Value::Binary(vec![7]))],
                Some(Timestamp(1)),
            ),
            control(
                ADMISSION,
                "sync_public_pending",
                vec![("admission", Value::Binary(vec![8]))],
                Some(Timestamp(2)),
            ),
        ]
    }

    fn delete_row(relation: &str) -> Operation {
        Operation::Public(TxOp::OwnedDelete {
            relation: rtdb_types::OwnedRelation::new(relation).unwrap(),
            id: EntityId::String("row".into()),
            valid_from: None,
            valid_to: None,
        })
    }

    #[test]
    fn refusal_pairs_every_control_and_exact_attempt_marker() {
        let mut ops = publications(ROOT_HEAD);
        ops.push(delete_row("fact_first"));
        ops.push(Operation::Public(TxOp::HostedConstraintRefusal));
        ops.extend(publications(RECEIPT_HEAD));
        let (primary, refusal) = split(&ops, true).unwrap();
        assert_eq!((primary.len(), refusal.len()), (7, 6));
        assert!(split(&ops, false).is_err());

        let partition_head = publications("fact_partition_head:fact_row").remove(1);
        ops.insert(6, partition_head);
        assert!(split(&ops, true).is_err());

        ops.insert(7, delete_row("fact_row"));
        assert!(split(&ops, true).is_ok());
        ops.drain(6..8);

        let unrelated = ops[2].clone();
        ops.insert(6, unrelated);
        assert!(split(&ops, true).is_err());
        ops.remove(6);

        let slot = ops.len() - 1;
        ops[slot] = ops[slot - 1].clone();
        assert!(split(&ops, true).is_err());
    }

    #[test]
    fn kept_constraint_refusal_preserves_decision_impact_pages() -> Result<()> {
        use rtdb_types::{OwnedRelation, PlacedAttemptScope, Timestamp};

        let digest = "a".repeat(64);
        let decision = format!("{DECISION_PREFIX}{digest}");
        let impact = format!("{IMPACT_PREFIX}{digest}:head");
        let scope = PlacedAttemptScope::new("installed-replica", 1)?;
        let control = |id: &str, expected, payload| -> Result<Operation> {
            let fields = StructFields::from([
                ("_id".into(), Value::Utf8(id.into())),
                ("admission".into(), Value::Binary(vec![payload])),
            ]);
            Ok(Operation::Public(TxOp::HostedControl(HostedControlWrite::new(
                OwnedRelation::new(ADMISSION)?, scope, expected, fields,
            )?)))
        };
        let mut ops = publications(ROOT_HEAD);
        ops.push(delete_row("fact_first"));
        ops.push(control(&decision, None, 1)?);
        ops.push(control(&impact, Some(Timestamp(1)), 3)?);
        ops.push(Operation::Public(TxOp::HostedConstraintRefusal));
        ops.extend(publications(RECEIPT_HEAD));
        ops.push(control(&decision, None, 2)?);
        ops.push(control(&impact, Some(Timestamp(1)), 3)?);
        assert!(split(&ops, true).is_ok());

        ops.pop();
        assert!(split(&ops, true).is_err());
        ops.push(control(&impact, Some(Timestamp(1)), 4)?);
        assert!(split(&ops, true).is_err());
        Ok(())
    }

    #[test]
    fn selection_keeps_the_entire_retained_world_guard() -> Result<()> {
        use rtdb_types::{Iid, OwnedRelation, PlacedAttemptScope, Timestamp};

        let world = Iid([1; 16]);
        let relation = OwnedRelation::new("fact_row")?.in_world(world)?;
        let scope = PlacedAttemptScope::new("installed-replica", 1)?;
        let mut ops = publications(ROOT_HEAD);
        ops.push(delete_row("fact_first"));
        ops.extend(
            [
                TxOp::owned_deadline(Timestamp(8)),
                TxOp::OwnedCheckWorldScope { scope, clock: None },
                TxOp::OwnedCheckWorldPurge {
                    relation: relation.clone(),
                    pin: Iid([2; 16]),
                    frontier: 0,
                    local_frontier: None,
                },
                TxOp::OwnedCheck {
                    relation: relation.clone(),
                    id: EntityId::String("row".into()),
                    valid_at: Timestamp(4),
                    expected: None,
                },
            ]
            .into_iter()
            .map(Operation::Public),
        );
        ops.push(Operation::Public(TxOp::HostedConstraintRefusal));
        ops.extend(publications(RECEIPT_HEAD));
        let (primary, refusal) = split(&ops, true)?;
        let selected = primary
            .iter()
            .filter(shared_check)
            .chain(refusal)
            .cloned()
            .collect::<Vec<_>>();
        assert!(crate::hosted_control::hosted_root_world_reads(&selected));
        assert_eq!(selected.len(), 4 + refusal.len());

        ops.swap(9, 10);
        assert!(split(&ops, true).is_err());
        ops.swap(9, 10);
        ops.insert(
            10,
            Operation::Public(TxOp::OwnedDelete {
                relation,
                id: EntityId::String("row".into()),
                valid_from: None,
                valid_to: None,
            }),
        );
        assert!(split(&ops, true).is_err());
        Ok(())
    }

    #[test]
    fn receipt_only_decision_requires_a_final_count_and_owns_only_the_receipt_head() {
        let mut ops = publications(RECEIPT_HEAD);
        ops.push(Operation::Public(TxOp::HostedConstraintRefusal));
        ops.extend(publications(RECEIPT_HEAD));
        assert!(split(&ops, true).is_err());

        ops.insert(
            0,
            Operation::Public(TxOp::OwnedCheckCapacity {
                relation: rtdb_types::OwnedRelation::new("fact_partition").unwrap(),
                column: "partition".into(),
                prefix: Vec::new(),
                at_most: 1,
            }),
        );
        assert!(split(&ops, true).is_ok());

        ops.insert(0, delete_row("fact_partition"));
        assert!(split(&ops, true).is_err());
    }

    #[test]
    fn receipt_charge_follows_the_selected_body_and_label() {
        let receipt = |len| StructFields::from([
            ("budget_label".into(), Value::Utf8("public".into())),
            ("outcome".into(), Value::Binary(vec![1; len])),
        ]);
        let charge = |total: u64, label: &str| StructFields::from([
            ("budget_label".into(), Value::Utf8(label.into())),
            ("fingerprint".into(), Value::Binary(vec![0; 32])),
            ("until_nanos".into(), Value::I64(0)),
            ("outcome".into(), Value::Binary(total.to_be_bytes().to_vec())),
        ]);
        let offered = receipt(10);
        let refused = receipt(4);
        let reserved = charge(100, "public");
        assert!(receipt_charge_matches((&offered, &refused), Some((&reserved, &charge(94, "public")))));
        assert!(!receipt_charge_matches((&offered, &refused), Some((&reserved, &charge(93, "public")))));
        assert!(!receipt_charge_matches((&offered, &refused), Some((&reserved, &charge(94, "other")))));
        assert!(!receipt_charge_matches((&offered, &refused), None));
    }

    #[test]
    fn proposal_refusal_keeps_world_and_request_without_any_review_effect() {
        let admission = |body: serde_json::Value| StructFields::from([
            ("admission".into(), Value::Binary(serde_json::to_vec(&body).unwrap())),
        ]);
        let mut body = serde_json::json!({
            "world": [1],
            "fingerprint": [2],
            "writes_root": false,
            "root": {"position": 0},
            "approval": null,
        });
        let offered = admission(body.clone());
        assert!(!proposal_refusal(&offered, &offered));
        body["root"] = serde_json::Value::Null;
        assert!(proposal_refusal(&offered, &admission(body.clone())));
        body["world"] = serde_json::json!([3]);
        assert!(!proposal_refusal(&offered, &admission(body)));
    }

    #[test]
    fn choice_cannot_change_the_receipt_fingerprint_or_deadline() {
        let receipt = |fingerprint, until, outcome| {
            StructFields::from([
                ("fingerprint".into(), Value::Binary(vec![fingerprint; 32])),
                ("until_nanos".into(), Value::I64(until)),
                ("outcome".into(), Value::Binary(vec![outcome])),
            ])
        };
        let original = receipt(7, 10, 1);
        assert!(same_binding(&original, &receipt(7, 10, 2), &["outcome"]));
        assert!(!same_binding(&original, &receipt(8, 10, 2), &["outcome"]));
        assert!(!same_binding(&original, &receipt(7, 11, 2), &["outcome"]));
    }

    #[test]
    fn expired_conflicted_and_invalid_attempts_never_select_a_refusal_receipt() {
        for code in [
            "rtdb.owned/expired",
            "rtdb.hosted/stale-control",
            "rtdb.owned/stale-range",
        ] {
            assert!(!may_settle(&Anomaly::new(
                AnomalyCategory::Conflict,
                code,
                "refused"
            )));
        }
        assert!(!may_settle(&Anomaly::incorrect(
            "rtdb.owned/unique-taken",
            "malformed"
        )));
        assert!(may_settle(&Anomaly::new(
            AnomalyCategory::Conflict,
            "rtdb.owned/unique-taken",
            "taken"
        )));
    }
}
