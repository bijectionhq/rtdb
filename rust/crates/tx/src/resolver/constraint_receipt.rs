use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Result, StructFields, Value};

use crate::{HostedControlWrite, TxOp, source::Operation};

const BUSINESS: &str = "bj_fact_root_receipts";
const EXECUTION: &str = "bj_private_execution_receipts";
const ADMISSION: &str = "bj_fact_root_admission";
const ATTEMPTS: &str = "fact_attempts";
const PUBLICATIONS: &str = "fact_publications";

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
    if !placed || !primary.iter().all(primary_op) || refusal.len() != 6 {
        return Err(invalid());
    }

    let mut types = 0;
    for alternative in refusal {
        let bit = match alternative {
            Operation::Public(TxOp::HostedControl(control)) => {
                let original = primary
                    .iter()
                    .filter_map(hosted)
                    .find(|original| {
                        original.name() == control.name() && original.id() == control.id()
                    })
                    .ok_or_else(invalid)?;
                control_choice(original, control)?
            }
            Operation::Public(TxOp::OwnedInsertDocs {
                relation,
                documents,
                valid_from,
                valid_to,
            }) if relation.name() == ATTEMPTS && documents.rows().len() == 1 => {
                if !primary.iter().any(|op| {
                    matches!(op, Operation::Public(TxOp::OwnedInsertDocs {
                    relation: old, documents: old_rows, valid_from: start, valid_to: end,
                }) if old == relation && old_rows.rows() == documents.rows()
                    && start == valid_from && end == valid_to)
                }) {
                    return Err(invalid());
                }
                16
            }
            Operation::Public(TxOp::OwnedReplaceDocs {
                relation,
                documents,
                valid_from,
                valid_to,
            }) if relation.name() == PUBLICATIONS
                && marker(documents.rows(), "sync_receipt_head") =>
            {
                if !primary.iter().any(|op| matches!(op, Operation::Public(TxOp::OwnedReplaceDocs {
                    relation: old, documents: old_rows, valid_from: start, valid_to: end,
                }) if old == relation && marker(old_rows.rows(), "head") && start == valid_from && end == valid_to))
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
    if types != 63 {
        return Err(invalid());
    }
    Ok((primary, refusal))
}

fn marker(rows: &[StructFields<Value>], name: &str) -> bool {
    matches!(rows, [row] if row.len() == 1
        && matches!(row.get("_id"), Some(Value::Utf8(id)) if id == name))
}

fn hosted(op: &Operation) -> Option<&HostedControlWrite> {
    match op {
        Operation::Public(TxOp::HostedControl(control)) => Some(control),
        _ => None,
    }
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
        BUSINESS
            if refusal.expected().is_none()
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
        _ => Err(invalid()),
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
        Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::HostedControl(_)) => true,
        Operation::Public(
            TxOp::OwnedCheck { relation, .. }
            | TxOp::OwnedCheckRange { relation, .. }
            | TxOp::OwnedCheckCapacity { relation, .. }
            | TxOp::OwnedInsertDocs { relation, .. }
            | TxOp::OwnedReplaceDocs { relation, .. }
            | TxOp::OwnedDelete { relation, .. },
        )
        | Operation::OwnedWriteIids { relation, .. } => {
            relation.is_root_data() || matches!(relation.name(), ATTEMPTS | PUBLICATIONS)
        }
        _ => false,
    }
}

pub(super) fn shared_check(op: &&Operation) -> bool {
    match op {
        Operation::Public(
            TxOp::OwnedDeadline { .. } | TxOp::OwnedCheck { .. } | TxOp::OwnedCheckRange { .. },
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
