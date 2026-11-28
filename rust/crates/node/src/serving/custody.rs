use std::{collections::{BTreeMap, BTreeSet, btree_map::Entry}, sync::Arc};

use rtdb_columnar::MemoryBudget;
use rtdb_crypt::CustodyVault;
use rtdb_index::{Snapshot, branches::{BranchEvent, BranchIndex, BranchView}};
use rtdb_tx::transact::{CustodyGroupId, CustodyPayload};
use rtdb_types::{Anomaly, Result, StructFields, TableRef, Timestamp, Value};
use sha2::{Digest, Sha256};

use super::{ChangePage, custody_release::CustodyContributors};

pub(crate) use rtdb_tx::transact::prepare_custody as prepare;

struct OpenedSnapshotPayload {
    cells: StructFields<Value>,
    remaining_puts: usize,
}

pub(crate) async fn open(vault: &CustodyVault, group: [u8; 16], stored: &CustodyPayload) -> Result<StructFields<Value>> {
    let clear = vault.open(group, stored.binding, &stored.envelope).await?;
    serde_json::from_slice(&clear.bytes).map_err(payload_error)
}

pub(crate) async fn snapshot(
    view: &BranchView<'_>, memory: &Arc<MemoryBudget>, vault: &CustodyVault,
) -> Result<(Snapshot, BTreeSet<CustodyGroupId>, Option<CustodyContributors>)> {
    snapshot_with_contributors(view, memory, vault, |table, event| view.custody_event_contributor(table, event)).await
}

pub(crate) async fn snapshot_with_contributors(
    view: &BranchView<'_>, memory: &Arc<MemoryBudget>, vault: &CustodyVault,
    contributor: impl Fn(&TableRef, &BranchEvent) -> Option<(CustodyGroupId, [u8; 32])>,
) -> Result<(Snapshot, BTreeSet<CustodyGroupId>, Option<CustodyContributors>)> {
    let mut opened = BTreeMap::new();
    let mut groups = BTreeSet::new();
    let mut contributors = Some(CustodyContributors::new());
    for (table, _) in view.tables() {
        for id in view.ids(table) {
            for event in view.history(table, id) {
                let Some(image) = event.image.as_ref() else { continue };
                let Some(group) = image.custody_group else { continue };
                groups.insert(group);
                if let Some(selected) = &mut contributors {
                    match contributor(table, event) {
                        Some(contributor) if selected.len() < 512 || selected.contains(&contributor) => { selected.insert(contributor); }
                        _ => contributors = None,
                    }
                }
                let Some(payload) = &image.custody_payload else { continue };
                let opened = match opened.entry((group, payload.binding)) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => entry.insert(OpenedSnapshotPayload {
                        cells: open(vault, group, payload).await?,
                        remaining_puts: 0,
                    }),
                };
                if !event.valid.is_empty() && event.image_at(event.valid.lower()).is_some() {
                    opened.remaining_puts += 1;
                }
            }
        }
    }
    let snapshot = rtdb_query::branch_snapshot_with(view, memory, |image| {
        let mut cells = image.cells.clone();
        if let Some(payload) = &image.custody_payload {
            let group = image.custody_group.ok_or_else(missing_group)?;
            let Entry::Occupied(mut entry) = opened.entry((group, payload.binding)) else {
                return Err(Anomaly::fault("rtdb/custody-payload", "The selected image has no opened payload"));
            };
            if entry.get().remaining_puts == 1 {
                cells.extend(entry.remove().cells.into_iter().map(|(name, value)| (name.clone(), reuse_snapshot_cell(value))));
            } else {
                let payload = entry.get_mut();
                payload.remaining_puts -= 1;
                cells.extend(payload.cells.iter().map(|(name, value)| (name.clone(), value.clone())));
            }
        }
        Ok(cells)
    })?;
    Ok((snapshot, groups, contributors))
}

fn reuse_snapshot_cell(value: Value) -> Value {
    // Staging charges capacities, including excess space that a clone discards.
    let exact_capacity = match &value {
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) | Value::Transit(bytes) => bytes.capacity() == bytes.len(),
        Value::Utf8(text) | Value::Uri(text) => text.capacity() == text.len(),
        _ => false,
    };
    if exact_capacity { value } else { value.clone() }
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

pub(crate) async fn open_changes(index: &BranchIndex, vault: &CustodyVault, mut page: ChangePage) -> Result<ChangePage> {
    for row in page.groups.iter_mut().flat_map(|group| &mut group.rows) {
        for image in [&mut row.before, &mut row.after] {
            let Some(group) = image.as_ref().and_then(|image| image.custody_group) else { continue };
            if !index.custody_group_live(group)? {
                *image = None;
                continue;
            }
            page.custody_groups.insert(group);
            let image = image.as_mut().expect("a custody group belongs to an image");
            if let Some(payload) = &image.custody_payload {
                let cells = open(vault, group, payload).await?;
                let image = Arc::make_mut(image);
                image.custody_payload = None;
                image.cells.extend(cells);
            }
        }
    }
    Ok(page)
}

pub(crate) async fn reconcile_revocations(index: &BranchIndex, vault: &CustodyVault, now: Timestamp) -> Result<()> {
    for (group, basis) in index.custody_revocations() {
        let deadline = index.exact(basis)?.system_time;
        vault.retire(group, deadline.0).await?;
        if now >= deadline && index.custody_releases_drained(group) {
            vault.destroy(group).await?;
        }
    }
    Ok(())
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
    use sha2::{Digest, Sha256};
    use rtdb_tx::transact::{Cell, Op, Transact};
    use rtdb_tx::transact::{CustodyPlacement, Outcome, ROW_CUSTODY, Resolution, Row, TransactState, TransactionRecord, Window};
    use rtdb_types::{EntityId, TableRef, rtdb_v1::{At, ClaimSealer, Layer, MAIN_BRANCH}};
    use uuid::Uuid;

    use super::*;
    use crate::serving::{ChangeGroup, ChangeKind, ChangeRow};

    #[test]
    fn snapshot_reuses_binary_storage_at_the_same_charge_as_a_clone() {
        let exact = vec![0, 0xff].into_boxed_slice().into_vec();
        let address = exact.as_ptr();
        let Value::Binary(moved) = reuse_snapshot_cell(Value::Binary(exact)) else { unreachable!(); };
        assert_eq!(moved.as_ptr(), address);

        let mut spare = Vec::with_capacity(1024);
        spare.extend([0, 0xff]);
        let expected = Value::Binary(spare.clone());
        let actual = reuse_snapshot_cell(Value::Binary(spare));
        assert_eq!(actual, expected);
        assert_eq!(actual.retained_bytes().unwrap(), expected.retained_bytes().unwrap());
    }

    #[tokio::test]
    async fn repeated_custody_payloads_keep_branch_history_and_expiry_boundaries() -> Result<()> {
        use rtdb_index::branches::{BranchCut, BranchEffect, RowImage, WrittenVersion};
        use rtdb_types::{EventOp, Interval, rtdb_v1::{Basis, WRITTEN_BASIS_COLUMN}};

        let incarnation = [4; 16];
        let group = [5; 16];
        let binding = [6; 32];
        let table = TableRef::new("public", "documents");
        let vault = CustodyVault::new(Uuid::from_bytes(incarnation), Arc::new(LocalKms::new(|| 11)));
        vault.create(group).await?;
        let protected = StructFields::from([
            ("secret".into(), Value::Binary(vec![0, 0xff])),
            ("nullable".into(), Value::Null),
        ]);
        let envelope = vault.seal(group, binding, &serde_json::to_vec(&protected).unwrap()).await?;
        let mut index = BranchIndex::new(incarnation);
        let cut = |position| BranchCut { basis: Basis::new(incarnation, position), system_time: Timestamp(position as i64) };
        index.advance(cut(1))?;
        index.apply(BranchEffect::CreateCustodyGroup { group })?;
        for (position, branch, valid) in [
            (1, MAIN_BRANCH, Interval::new(Timestamp(10), Timestamp(40))?),
            (2, "child", Interval::new(Timestamp(20), Timestamp(50))?),
            (3, "child", Interval::new(Timestamp(35), Timestamp(50))?),
        ] {
            if position > 1 { index.advance(cut(position))?; }
            if position == 2 {
                index.apply(BranchEffect::Fork {
                    child: "child".into(), from: (MAIN_BRANCH.into(), cut(1).basis), writable: true, expires_at: None,
                })?;
            }
            let id = EntityId::Integer(1);
            index.apply(BranchEffect::Write {
                branch: branch.into(), table: table.clone(),
                event: BranchEvent {
                    id: id.clone(), valid, cut: cut(position), written: WrittenVersion::Branch,
                    image: Some(Arc::new(RowImage {
                        cells: StructFields::from([("_id".into(), id.as_value())]),
                        expires_at: Some(Timestamp(35)), custody_group: Some(group),
                        custody_payload: Some(CustodyPayload { binding, envelope: envelope.clone() }),
                    })),
                },
            })?;
        }

        let memory = MemoryBudget::new(8 << 20);
        for (layer, expected) in [(Layer::Merged, [2, 3]), (Layer::Local, [1, 2])] {
            let view = index.view("child", At::Latest, layer, Timestamp(50))?;
            let (actual, groups, _) = snapshot(&view, &memory, &vault).await?;
            assert_eq!(groups, BTreeSet::from([group]));
            let scalar = rtdb_query::branch_snapshot_with(&view, &memory, |image| {
                let mut cells = image.cells.clone();
                cells.extend(protected.clone());
                Ok(cells)
            })?;
            let batches = |snapshot: &Snapshot| {
                snapshot.iter_segments(&table).flat_map(|segment| segment.batches().cloned()).collect::<Vec<_>>()
            };
            let actual = batches(&actual);
            let scalar = batches(&scalar);
            assert_eq!(actual.len(), scalar.len());
            for (actual, scalar) in actual.iter().zip(scalar.iter()) {
                assert_eq!(actual.arrow(), scalar.arrow());
            }
            let mut puts = Vec::new();
            let mut deletes = 0;
            for batch in &actual {
                for row in 0..batch.len() {
                    let header = batch.header(row)?;
                    if header.op == EventOp::Put {
                        let cells = batch.view().read_put_document(row)?;
                        assert_eq!(cells.get("secret"), protected.get("secret"));
                        assert_eq!(cells.get("nullable"), protected.get("nullable"));
                        assert_eq!(header.valid_time.upper(), Timestamp(35));
                        puts.push((header.system_from.0, cells[WRITTEN_BASIS_COLUMN].clone()));
                    } else {
                        assert_eq!(header.op, EventOp::Delete);
                        assert_eq!(header.valid_time.lower(), Timestamp(35));
                        deletes += 1;
                    }
                }
            }
            assert_eq!([puts.len(), deletes], expected);
            for (position, written) in puts {
                assert_eq!(written, Value::Binary(cut(position as u64).basis.to_bytes().to_vec()));
            }
            assert!(view.history(&table, &EntityId::Integer(1)).iter().all(|event| {
                let image = event.image.as_ref().unwrap();
                !image.cells.contains_key("secret") && image.custody_payload.as_ref().unwrap().envelope == envelope
            }));
        }
        Ok(())
    }

    #[tokio::test]
    async fn ranked_capture_retains_inherited_and_shadowing_custody_groups_before_slicing() {
        let incarnation = [4; 16];
        let root_group = [5; 16];
        let shadow_group = [6; 16];
        let table = TableRef::new("public", "documents");
        let vault = CustodyVault::new(Uuid::from_bytes(incarnation), Arc::new(LocalKms::new(|| 11)));
        let claims = ClaimSealer::new(incarnation, [7; 32]);
        let place = |branch: &str, group: [u8; 16]| Op::PlaceCustodyRow {
            branch: branch.into(), table: table.clone(),
            row: Row {
                id: EntityId::Integer(1), valid: Some(rtdb_types::Interval::ALL.into()),
                cells: StructFields::from([("carrier".into(), Cell::Value(Value::Binary(group.to_vec())))]),
                custody_group: Some(group), protected_columns: vec!["carrier".into()], custody_payload: None,
            },
            placement: CustodyPlacement {
                placement_id: group, reservation: None, derived: None, carrier_column: "carrier".into(), link_column: None, commitments: None,
            },
        };
        let mut state = TransactState::new(incarnation);
        let root = Op::CreateCustodyGroup { group: root_group };
        let fork = Op::Fork { child: "child".into(), from: (MAIN_BRANCH.into(), rtdb_types::rtdb_v1::Basis::new(incarnation, 1)), writable: true, expires_at: None };
        for (position, ops) in [(1, vec![root, place(MAIN_BRANCH, root_group)]),
            (2, vec![fork, Op::CreateCustodyGroup { group: shadow_group }, place("child", shadow_group)])] {
            let mut request = Transact {
                key: vec![position as u8; 16], require: Vec::new(), window: Window::default(), guards: Vec::new(),
                ops, otherwise: Vec::new(), requires: Vec::new(), custody_request_digest: None, request_memory: None,
            };
            prepare(&mut request, &vault).await.unwrap();
            let Resolution::Append(append) = state.resolve(&request, position, Timestamp(10 + position as i64), &[ROW_CUSTODY], &claims).unwrap() else { panic!() };
            assert_eq!(state.apply(position, append.into_record()).unwrap().unwrap().outcome, Outcome::Then);
        }
        let view = state.index().view("child", At::Latest, Layer::Merged, Timestamp(20)).unwrap();
        let winner = view.row(&table, &EntityId::Integer(1), Timestamp(20)).unwrap();
        assert_eq!(winner.image.as_ref().unwrap().custody_group, Some(shadow_group));
        let (_, groups, contributors) = snapshot(&view, &MemoryBudget::new(1 << 22), &vault).await.unwrap();
        assert_eq!(groups, BTreeSet::from([root_group, shadow_group]));
        let expected = view.custody_placements().map(|entry| (entry.group, entry.occurrence.unwrap().commitment(entry.group))).collect();
        assert_eq!(contributors, Some(expected));
    }

    #[tokio::test]
    async fn scoped_capture_uses_real_occurrences_for_visible_query_and_change_images() {
        use rtdb_tx::transact::{Declaration, Visibility};
        use rtdb_types::rtdb_v1::LabelScope;
        use crate::serving::{ChangeImages, ChangeOrder, Changes, QuerySource, ServingScope};

        let incarnation = [4; 16];
        let visible = [5; 16];
        let hidden = [6; 16];
        let table = TableRef::new("public", "docs");
        let vault = CustodyVault::new(Uuid::from_bytes(incarnation), Arc::new(LocalKms::new(|| 10)));
        let claims = ClaimSealer::new(incarnation, [7; 32]);
        let place = |id, label, group: CustodyGroupId| Op::PlaceCustodyRow {
            branch: MAIN_BRANCH.into(), table: table.clone(),
            row: Row {
                id: EntityId::Integer(id), valid: Some(rtdb_types::Interval::ALL.into()),
                cells: StructFields::from([
                    ("label".into(), Cell::Value(Value::Binary(vec![label]))),
                    ("carrier".into(), Cell::Value(Value::Binary(group.to_vec()))),
                ]),
                custody_group: Some(group), protected_columns: vec!["carrier".into()], custody_payload: None,
            },
            placement: CustodyPlacement {
                placement_id: group, reservation: None, derived: None, carrier_column: "carrier".into(),
                link_column: None, commitments: None,
            },
        };
        let mut request = Transact {
            key: vec![1; 16], require: Vec::new(), window: Window::default(), guards: Vec::new(), requires: Vec::new(),
            ops: vec![
                Op::Declare { table: table.clone(), change: Declaration::Visibility(Visibility { column: "label".into() }) },
                Op::CreateCustodyGroup { group: visible }, Op::CreateCustodyGroup { group: hidden },
                place(1, 1, visible), place(2, 2, hidden),
            ],
            otherwise: Vec::new(), custody_request_digest: None, request_memory: None,
        };
        let mut state = TransactState::new(incarnation);
        prepare(&mut request, &vault).await.unwrap();
        let Resolution::Append(append) = state.resolve(&request, 1, Timestamp(10), &[ROW_CUSTODY], &claims).unwrap()
            else { panic!("the placements must order"); };
        assert_eq!(state.apply(1, append.into_record()).unwrap().unwrap().outcome, Outcome::Then);
        let index = state.index();
        let expected: CustodyContributors = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(11)).unwrap()
            .custody_placements().filter(|entry| entry.group == visible)
            .map(|entry| (entry.group, entry.occurrence.unwrap().commitment(entry.group))).collect();
        assert_eq!(expected.len(), 1);
        let scope = ServingScope {
            labels: LabelScope { read: BTreeSet::from([vec![1]]), ..Default::default() },
            receipt_namespace: [8; 32], custody_evidence: true, hold: None,
        };
        let source = QuerySource { branch: MAIN_BRANCH.into(), at: At::Latest, layer: Layer::Merged };
        let (view, _) = scope.query(index, source, Timestamp(11), &MemoryBudget::new(1 << 22), Some(&vault)).await.unwrap();
        assert_eq!(view.custody_contributors, Some(expected.clone()));
        let mut changes = Changes {
            branch: MAIN_BRANCH.into(), tables: BTreeSet::new(), after: None, until: None,
            order: ChangeOrder::Ascending, images: ChangeImages::Both, limit: 8, wait: std::time::Duration::ZERO,
        };
        let page = scope.changes(index, &changes).unwrap();
        assert_eq!(page.scoped_custody, Some((index.head().basis, expected)));
        changes.images = ChangeImages::None;
        let progress = scope.changes(index, &changes).unwrap();
        assert_eq!(progress.scoped_custody, Some((index.head().basis, CustodyContributors::new())));
    }

    #[tokio::test]
    async fn placement_preparation_commits_the_exact_binary_carrier_and_link() {
        let group = [5; 16];
        let vault = CustodyVault::new(Uuid::from_bytes([4; 16]), Arc::new(LocalKms::new(|| 11)));
        vault.create(group).await.unwrap();
        let carrier = b"independently sealed destination";
        let link = b"independently sealed link";
        let mut request = Transact {
            key: vec![1; 16], require: Vec::new(), window: Window::default(), guards: Vec::new(),
            ops: vec![Op::PlaceCustodyRow {
                branch: MAIN_BRANCH.into(), table: TableRef::new("public", "documents"),
                row: Row {
                    id: EntityId::Integer(1), valid: None, custody_group: Some(group), custody_payload: None,
                    cells: StructFields::from([
                        ("carrier".into(), Cell::Value(Value::Binary(carrier.to_vec()))),
                        ("link".into(), Cell::Value(Value::Binary(link.to_vec()))),
                    ]),
                    protected_columns: vec!["carrier".into(), "link".into()],
                },
                placement: CustodyPlacement {
                    placement_id: [6; 16], reservation: Some([7; 32]), derived: None, carrier_column: "carrier".into(),
                    link_column: Some("link".into()), commitments: None,
                },
            }],
            otherwise: Vec::new(), requires: Vec::new(), custody_request_digest: None, request_memory: None,
        };
        let logical = request.logical_sha256().unwrap();
        prepare(&mut request, &vault).await.unwrap();
        assert_eq!(request.custody_request_digest, Some(logical));
        let Op::PlaceCustodyRow { row, placement, .. } = &request.ops[0] else { unreachable!() };
        let commitments = placement.commitments.unwrap();
        assert_eq!(commitments.carrier_sha256, <[u8; 32]>::from(Sha256::digest(carrier)));
        assert_eq!(commitments.link_sha256, Some(Sha256::digest(link).into()));
        assert!(row.cells.is_empty());
        let payload = open(&vault, group, row.custody_payload.as_ref().unwrap()).await.unwrap();
        assert_eq!(payload.get("carrier"), Some(&Value::Binary(carrier.to_vec())));
        assert_eq!(payload.get("link"), Some(&Value::Binary(link.to_vec())));
    }

    #[tokio::test]
    async fn protected_cells_are_sealed_before_append_and_revocation_blocks_delivery() {
        let incarnation = [4; 16];
        let group = [5; 16];
        let table = TableRef::new("public", "documents");
        let claims = ClaimSealer::new(incarnation, [6; 32]);
        let vault = CustodyVault::new(Uuid::from_bytes(incarnation), Arc::new(LocalKms::new(|| 11)));
        let request = |key, ops| Transact {
            key: vec![key; 16], require: Vec::new(), window: Window::default(),
            guards: Vec::new(), ops, otherwise: Vec::new(), requires: Vec::new(),
            custody_request_digest: None, request_memory: None,
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
        let unsealed = state.resolve(&first, 1, Timestamp(10), &[ROW_CUSTODY], &claims);
        assert!(matches!(unsealed, Err(error) if error.code == "rtdb/custody-payload"));
        let mut retry = request(1, first.ops.clone());
        let mut retry_after_destruction = retry.clone();
        prepare(&mut first, &vault).await.unwrap();
        let Resolution::Append(append) = state.resolve(&first, 1, Timestamp(10), &[ROW_CUSTODY], &claims).unwrap()
            else { panic!("a protected write must append"); };
        let bytes = append.record().encode().unwrap();
        assert!(!bytes.windows(b"protected-string".len()).any(|bytes| bytes == b"protected-string"));
        assert_eq!(state.apply(1, TransactionRecord::decode(&bytes).unwrap()).unwrap().unwrap().outcome, Outcome::Then);
        prepare(&mut retry, &vault).await.unwrap();
        assert!(matches!(state.resolve(&retry, 2, Timestamp(11), &[ROW_CUSTODY], &claims).unwrap(), Resolution::Answer(receipt) if receipt.replayed));

        let view = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(10)).unwrap();
        let row = view.row(&table, &EntityId::Integer(1), Timestamp(10)).unwrap();
        let image = row.image.as_ref().unwrap();
        let private = open(&vault, group, image.custody_payload.as_ref().unwrap()).await.unwrap();
        assert_eq!(private.get("private"), Some(&Value::Utf8("protected-string".into())));
        let (_, groups, contributors) = snapshot(&view, &MemoryBudget::new(1 << 22), &vault).await.unwrap();
        assert!(contributors.is_none());
        check_delivery(state.index(), &vault, &groups).await.unwrap();
        let page = || ChangePage {
            read_custody: None,
            scoped_custody: None,
            groups: vec![ChangeGroup {
                cut: row.cut,
                rows: vec![ChangeRow {
                    table: table.clone(),
                    id: row.id.clone(),
                    valid: Some(row.valid),
                    kind: ChangeKind::Opened,
                    erased_written_basis: None,
                    before: None,
                    after: Some(Arc::clone(image)),
                }],
            }],
            through: row.cut.basis,
            complete: true,
            custody_groups: BTreeSet::new(),
            key_generation: None,
        };
        let opened_changes = open_changes(state.index(), &vault, page()).await.unwrap();
        let after = opened_changes.groups[0].rows[0].after.as_ref().unwrap();
        assert_eq!(after.cells.get("private"), private.get("private"));
        assert!(image.cells.get("private").is_none());
        assert!(image.custody_payload.is_some());
        assert_eq!(opened_changes.custody_groups, groups);
        let pending_changes = page();

        let inventory_sha256 = state.index().custody_inventory(group).unwrap().sha256;
        let revoke = request(2, vec![Op::RevokeCustodyGroup { group, inventory_sha256 }]);
        let Resolution::Append(append) = state.resolve(&revoke, 2, Timestamp(11), &[ROW_CUSTODY], &claims).unwrap()
            else { panic!("a revocation must append"); };
        state.apply(2, append.into_record()).unwrap();
        assert_eq!(check_delivery(state.index(), &vault, &groups).await.unwrap_err(), rtdb_crypt::erased_under_view());
        let redacted = open_changes(state.index(), &vault, pending_changes).await.unwrap();
        assert!(redacted.groups[0].rows[0].after.is_none());
        assert!(redacted.custody_groups.is_empty());
        let checkpoint = state.encode_checkpoint().unwrap();
        let reopened = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
        reconcile_revocations(reopened.index(), &vault, Timestamp(11)).await.unwrap();
        assert_eq!(vault.check_readable(group).await.unwrap_err(), rtdb_crypt::erased_under_view());
        prepare(&mut retry_after_destruction, &vault).await.unwrap();
        assert!(matches!(reopened.resolve(&retry_after_destruction, 3, Timestamp(12), &[ROW_CUSTODY], &claims).unwrap(),
            Resolution::Answer(receipt) if receipt.replayed && receipt.outcome == Outcome::Then));
    }

    #[tokio::test]
    async fn prepare_leaves_request_unsealed_when_a_later_row_is_erased() {
        let incarnation = [11; 16];
        let live_group = [12; 16];
        let erased_group = [13; 16];
        let table = TableRef::new("public", "documents");
        let vault = CustodyVault::new(Uuid::from_bytes(incarnation), Arc::new(LocalKms::new(|| 10)));
        vault.create(live_group).await.unwrap();
        vault.create(erased_group).await.unwrap();
        vault.retire(erased_group, 0).await.unwrap();
        vault.destroy(erased_group).await.unwrap();

        let row = |id: i64, group: [u8; 16], text: &str| Row {
            id: EntityId::Integer(id), valid: None,
            cells: StructFields::from([("private".into(), Cell::Value(Value::Utf8(text.into())))]),
            custody_group: Some(group), protected_columns: vec!["private".into()], custody_payload: None,
        };
        let mut request = Transact {
            key: vec![1; 16], require: Vec::new(), window: Window::default(), guards: Vec::new(),
            ops: vec![Op::Put {
                branch: MAIN_BRANCH.into(), table, if_absent: false,
                rows: vec![row(1, live_group, "first"), row(2, erased_group, "second")],
            }],
            otherwise: Vec::new(), requires: Vec::new(),
            custody_request_digest: None, request_memory: None,
        };
        let expected_digest = request.logical_sha256().unwrap();

        prepare(&mut request, &vault).await.unwrap();

        assert_eq!(request.custody_request_digest, Some(expected_digest));
        let [Op::Put { rows, .. }] = request.ops.as_slice() else { panic!("expected put"); };
        assert_eq!(rows.len(), 2);
        for (row, expected) in rows.iter().zip(["first", "second"]) {
            assert!(row.custody_payload.is_none());
            assert!(matches!(row.cells.get("private"), Some(Cell::Value(Value::Utf8(text))) if text == expected));
        }
    }
}
