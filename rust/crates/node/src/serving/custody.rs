use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use rtdb_columnar::MemoryBudget;
use rtdb_crypt::CustodyVault;
use rtdb_index::{Snapshot, branches::{BranchIndex, BranchView}};
use rtdb_tx::transact::{Cell, CustodyGroupId, CustodyPayload, Op};
use rtdb_types::{Anomaly, Result, StructFields, Value};
use sha2::{Digest, Sha256};

use super::TransactSubmission;

pub(crate) async fn prepare(submission: &mut TransactSubmission, vault: &CustodyVault) -> Result<()> {
    let fingerprint = submission.transaction.logical_sha256()?;
    submission.record_logical_admission(fingerprint);
    for action in submission.transaction.ops.iter().chain(&submission.transaction.otherwise) {
        if let Op::CreateCustodyGroup { group } = action {
            vault.create(*group).await?;
        }
    }
    for (arm, actions) in [&mut submission.transaction.ops, &mut submission.transaction.otherwise].into_iter().enumerate() {
        for (action, op) in actions.iter_mut().enumerate() {
            let Op::Put { rows, .. } = op else { continue };
            for (index, row) in rows.iter_mut().enumerate() {
                if row.protected_columns.is_empty() { continue; }
                let group = row.custody_group.ok_or_else(missing_group)?;
                let mut protected = StructFields::new();
                for column in &row.protected_columns {
                    let Some(Cell::Value(value)) = row.cells.remove(column) else {
                        return Err(Anomaly::incorrect("rtdb/custody-payload", "A protected column requires a literal cell"));
                    };
                    protected.insert(column.clone(), value);
                }
                let clear = serde_json::to_vec(&protected).map_err(payload_error)?;
                let binding = binding(fingerprint, arm, action, index);
                let envelope = vault.seal(group, binding, &clear).await?;
                row.custody_payload = Some(CustodyPayload { binding, envelope });
            }
        }
    }
    Ok(())
}

pub(crate) async fn open(vault: &CustodyVault, group: [u8; 16], stored: &CustodyPayload) -> Result<StructFields<Value>> {
    let clear = vault.open(group, stored.binding, &stored.envelope).await?;
    serde_json::from_slice(&clear.bytes).map_err(payload_error)
}

pub(crate) async fn snapshot(
    view: &BranchView<'_>, memory: &Arc<MemoryBudget>, vault: &CustodyVault,
) -> Result<(Snapshot, BTreeSet<CustodyGroupId>)> {
    let mut opened = BTreeMap::new();
    let mut groups = BTreeSet::new();
    for (table, _) in view.tables() {
        for id in view.ids(table) {
            for event in view.history(table, id) {
                let Some(image) = event.image.as_ref() else { continue };
                let Some(payload) = &image.custody_payload else { continue };
                let group = image.custody_group.ok_or_else(missing_group)?;
                groups.insert(group);
                if let std::collections::btree_map::Entry::Vacant(entry) = opened.entry((group, payload.binding)) {
                    entry.insert(open(vault, group, payload).await?);
                }
            }
        }
    }
    let snapshot = rtdb_query::branch_snapshot_with(view, memory, |image| {
        let mut cells = image.cells.clone();
        if let Some(payload) = &image.custody_payload {
            let group = image.custody_group.ok_or_else(missing_group)?;
            let protected = opened.get(&(group, payload.binding)).ok_or_else(|| {
                Anomaly::fault("rtdb/custody-payload", "The selected image has no opened payload")
            })?;
            cells.extend(protected.iter().map(|(name, value)| (name.clone(), value.clone())));
        }
        Ok(cells)
    })?;
    Ok((snapshot, groups))
}

pub(crate) async fn check_delivery(index: &BranchIndex, vault: &CustodyVault, groups: &BTreeSet<CustodyGroupId>) -> Result<()> {
    for &group in groups {
        if !index.custody_group_live(group)? {
            return Err(rtdb_crypt::erased_under_view());
        }
        vault.check_readable(group).await?;
    }
    Ok(())
}

fn binding(fingerprint: [u8; 32], arm: usize, operation: usize, row: usize) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rtdb/v1/custody-row\0");
    hash.update(fingerprint);
    for index in [arm, operation, row] {
        hash.update((index as u64).to_be_bytes());
    }
    hash.finalize().into()
}

fn missing_group() -> Anomaly {
    Anomaly::incorrect("rtdb/custody-payload", "A protected payload requires a custody group")
}

fn payload_error(error: serde_json::Error) -> Anomaly {
    Anomaly::fault("rtdb/custody-payload", error.to_string())
}

#[cfg(test)]
mod tests {
    use rtdb_crypt::LocalKms;
    use rtdb_tx::transact::{Outcome, ROW_CUSTODY, Resolution, Row, Transact, TransactState, TransactionRecord, Window};
    use rtdb_types::{EntityId, TableRef, Timestamp, rtdb_v1::{At, ClaimSealer, Layer, MAIN_BRANCH}};
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn protected_cells_are_sealed_before_append_and_revocation_blocks_delivery() {
        let incarnation = [4; 16];
        let group = [5; 16];
        let table = TableRef::new("public", "documents");
        let claims = ClaimSealer::new(incarnation, [6; 32]);
        let vault = CustodyVault::new(Uuid::from_bytes(incarnation), Arc::new(LocalKms::new(|| 10)));
        let request = |key, ops| TransactSubmission {
            transaction: Transact {
                key: vec![key; 16], require: Vec::new(), window: Window::default(),
                guards: Vec::new(), ops, otherwise: Vec::new(), requires: Vec::new(), admission: None,
            },
            attempt: Uuid::from_u128(key.into()), submitted_at: Timestamp(10), source: Default::default(),
        };
        let mut first = request(1, vec![
            Op::CreateCustodyGroup { group },
            Op::Put {
                branch: MAIN_BRANCH.into(), table: table.clone(), if_absent: false,
                rows: vec![Row {
                    id: EntityId::Integer(1), valid: None,
                    cells: StructFields::from([("private".into(), Cell::Value(Value::Utf8("protected-string".into())))]),
                    custody_group: Some(group), protected_columns: vec!["private".into()], custody_payload: None,
                }],
            },
        ]);
        let mut state = TransactState::new(incarnation);
        let mut retry = request(1, first.transaction.ops.clone());
        prepare(&mut first, &vault).await.unwrap();
        let Resolution::Append(append) = state.resolve(&first.transaction, 1, Timestamp(10), &[ROW_CUSTODY], &claims).unwrap()
            else { panic!("a protected write must append"); };
        let bytes = append.record().encode().unwrap();
        assert!(!bytes.windows(b"protected-string".len()).any(|bytes| bytes == b"protected-string"));
        assert_eq!(state.apply(1, TransactionRecord::decode(&bytes).unwrap()).unwrap().unwrap().outcome, Outcome::Then);
        prepare(&mut retry, &vault).await.unwrap();
        assert!(matches!(state.resolve(&retry.transaction, 2, Timestamp(11), &[ROW_CUSTODY], &claims).unwrap(), Resolution::Answer(receipt) if receipt.replayed));

        let view = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(10)).unwrap();
        let row = view.row(&table, &EntityId::Integer(1), Timestamp(10)).unwrap();
        let image = row.image.as_ref().unwrap();
        let private = open(&vault, group, image.custody_payload.as_ref().unwrap()).await.unwrap();
        assert_eq!(private.get("private"), Some(&Value::Utf8("protected-string".into())));
        let (_, groups) = snapshot(&view, &MemoryBudget::new(1 << 22), &vault).await.unwrap();
        check_delivery(state.index(), &vault, &groups).await.unwrap();

        let revoke = request(2, vec![Op::RevokeCustodyGroup { group }]);
        let Resolution::Append(append) = state.resolve(&revoke.transaction, 2, Timestamp(11), &[ROW_CUSTODY], &claims).unwrap()
            else { panic!("a revocation must append"); };
        state.apply(2, append.into_record()).unwrap();
        assert_eq!(check_delivery(state.index(), &vault, &groups).await.unwrap_err(), rtdb_crypt::erased_under_view());
    }
}
