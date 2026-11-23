use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Result, StructFields, Value};

use crate::{HostedControlWrite, TxOp, source::Operation};

const BUSINESS: &str = "bj_fact_root_receipts";
const EXECUTION: &str = "bj_private_execution_receipts";
const ADMISSION: &str = "bj_fact_root_admission";
const ATTEMPTS: &str = "fact_attempts";
const PUBLICATIONS: &str = "fact_publications";
const ROOT_HEAD: &str = "head";
const RECEIPT_HEAD: &str = "sync_receipt_head";

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

    let control_writes = primary
        .iter()
        .filter_map(hosted)
        .filter(|control| !control.is_check())
        .count();
    let written_relations = primary
        .iter()
        .filter_map(write_relation)
        .map(|relation| relation.name())
        .collect::<std::collections::BTreeSet<_>>();
    let primary_head = if written_relations
        .iter()
        .any(|relation| !matches!(*relation, ATTEMPTS | PUBLICATIONS))
    {
        ROOT_HEAD
    } else if primary
        .iter()
        .any(|op| matches!(op, Operation::Public(TxOp::OwnedCheckCapacity { .. })))
    {
        RECEIPT_HEAD
    } else {
        return Err(invalid());
    };
    let marker_writes = primary
        .iter()
        .filter(|op| match op {
            Operation::Public(TxOp::OwnedReplaceDocs {
                relation,
                documents,
                ..
            }) if relation.name() == PUBLICATIONS
                && marker_id(documents.rows())
                    .and_then(|id| id.strip_prefix("fact_partition_head:"))
                    .is_some_and(|relation| {
                        relation.starts_with("fact_")
                            && written_relations.contains(relation)
                            && !matches!(relation, ATTEMPTS | PUBLICATIONS)
                    }) =>
            {
                false
            }
            _ => write_relation(op)
                .is_some_and(|relation| matches!(relation.name(), ATTEMPTS | PUBLICATIONS)),
        })
        .count();
    // Each alternative replacement is matched below. Counting both sets also
    // rules out silently dropping an unrelated control or attempt on refusal.
    if control_writes != 4 || marker_writes != 2 {
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
            }) if relation.name() == PUBLICATIONS && marker(documents.rows(), RECEIPT_HEAD) => {
                if !primary.iter().any(|op| matches!(op, Operation::Public(TxOp::OwnedReplaceDocs {
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
    if types != 63 {
        return Err(invalid());
    }
    Ok((primary, refusal))
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
            | TxOp::OwnedDelete { relation, .. },
        )
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
