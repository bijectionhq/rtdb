use std::{collections::BTreeSet, fmt::Write, io::Cursor, num::NonZeroU32, sync::Arc};

use sha2::{Digest, Sha256};

use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_format::{
    data::{EventBatch, StoredEvent, StoredOperation},
    ipc::{ArrowFile, inspect_file_reserved},
    log::proto::PublishedObjectVersion,
};
use rtdb_index::{IndexBatch, Snapshot};
use rtdb_storage::{HeldView, IoRegion, ObjectKey};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, EventOp, Interval, OwnedChange, OwnedChangeKind,
    OwnedRelation, OwnedSchema, PlacedAttemptScope, Result, StructFields, TableRef, Timestamp, Value,
};

use crate::{Documents, TxOp, source::Operation};
use super::{OpenTransaction, control_order::revision_matches, no_owned_principal, overflow};

const MAX_EVENTS: i64 = 4096;
const MAX_FILES: usize = 64;
const MAX_FILE_BYTES: usize = 32 << 20;
const MAX_INVENTORY_BYTES: usize = 16 << 20;

impl OpenTransaction<'_> {
    pub(super) async fn check_relation_adoption(
        &self, operations: &[Operation], scope: Option<PlacedAttemptScope>, owner: &str, clock_slot: bool,
    ) -> Result<()> {
        let mut adoptions = operations.iter().filter_map(|op| match op {
            Operation::Public(TxOp::OwnedAdoptRelation { source, destination, .. }) => Some((source, destination)),
            _ => None,
        });
        let Some((source, destination)) = adoptions.next() else { return Ok(()); };
        let controls = operations.iter().filter_map(|op| match op {
            Operation::Public(TxOp::HostedControl(control)) => Some(control),
            _ => None,
        });
        let frontier = |target: &OwnedRelation, present| {
            operations.iter().any(|op| matches!(op,
                Operation::Public(TxOp::OwnedCheck { relation, id: EntityId::String(id), expected, .. })
                    if relation.name() == rtdb_types::OWNED_ROOT_PUBLICATION_RELATION
                        && id.strip_prefix("fact_partition_head:") == Some(target.name())
                        && expected.is_some() == present))
        };
        let has_retry_marker = operations.iter().any(|operation| {
            let create = matches!(operation, Operation::Public(TxOp::OwnedInsertDocs { .. }
                | TxOp::OwnedArrowDocs { create_only: true, .. }) | Operation::OwnedWriteIids { create_only: true, .. }
                | Operation::OwnedArrowIids { create_only: true, .. });
            create && operation.owned_documents().is_some_and(|(relation, rows)|
                relation.name() == rtdb_types::OWNED_ATTEMPT_RELATION && rows > 0)
        });
        if adoptions.next().is_some() || scope.is_none() || scope != self.context.owned_witness_scope
            || !self.context.authorization.is_hosted_control_owner(owner) || clock_slot
            || source == destination || !source.is_root_data() || !destination.is_root_data()
            || !has_retry_marker
            || self.hosted_replay.is_some()
            || !controls.clone().any(crate::HostedControlWrite::replaces_root_catalog)
            || !controls.clone().any(|control| !control.is_check() && control.expected().is_some()
                && control.table().table == "bj_relation_declarations")
            || controls.clone().any(crate::HostedControlWrite::is_assertion_family_marker)
            || !frontier(source, true) || !frontier(destination, false)
            || operations.iter().any(|op| {
                let Some(target) = op.owned_relation() else { return false; };
                (target == source || target == destination)
                    && !matches!(op, Operation::Public(TxOp::OwnedCheck { .. } | TxOp::OwnedCheckRange { .. }))
            })
        {
            return Err(uncovered("Adoption needs one staged Root replacement and its ordered predecessor checks"));
        }
        let base = self.buffer.base_snapshot();
        let mut held_basis = None;
        for relation in [source, destination] {
            let actual = base.relation_event_frontier(&relation.table(owner))?;
            let mut witnesses = operations.iter().filter_map(|operation| match operation {
                Operation::Public(TxOp::OwnedCheckRange { relation: target, basis, frontier, .. })
                    if target == relation => Some((*basis, *frontier)),
                _ => None,
            });
            let stale = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/stale-range",
                "Adoption needs both exact population frontiers from one held cut");
            let (basis, expected) = witnesses.next().ok_or_else(stale)?;
            if !revision_matches(Some(actual), Some(expected)) || witnesses.next().is_some()
                || held_basis.replace(basis).is_some_and(|previous| previous != basis)
            {
                return Err(stale());
            }
        }
        let context = self.context.query_context(base, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        for relation in [source, destination] {
            if rtdb_query::read_owned_retirement(Arc::clone(&context), owner, relation).await?.is_some()
                || rtdb_query::read_owned_adoption(Arc::clone(&context), owner, relation).await?.is_some()
            {
                return Err(uncovered("A retired or already adopted relation cannot enter this adoption"));
            }
        }
        Ok(())
    }

    pub(super) async fn adopt_owned_relation(
        &mut self, source: &OwnedRelation, destination: &OwnedRelation, retain_micros: i64,
    ) -> Result<()> {
        let owner = self.options.user.as_deref().ok_or_else(no_owned_principal)?;
        let base = Arc::new(self.buffer.base_snapshot());
        let data = source.table(owner);
        let journal = source.change_journal(owner)?;
        let captured = base.owned_routing_inventory(owner, source)?;
        if !(1..=MAX_EVENTS).contains(&captured.data_events) || !(2..=MAX_EVENTS + 1).contains(&captured.journal_events) {
            return Err(uncovered("The source exceeds its data or journal event bound"));
        }
        let successor_roles = destination.physical_tables(&data).ok_or_else(|| uncovered("Invalid destination roles"))?;
        if successor_roles.iter().any(|table| base.contains_table(table) || base.trie_catalog().iter_tries(table).next().is_some()) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/stale-range",
                "The staged destination is no longer virgin"));
        }
        if captured.mask_events != 0 || base.contains_table(&OwnedRelation::obsolete_purge_table(owner))
            || [source.purge_floor_table(owner), source.lifetime_table(owner), source.capacity_table(owner),
                source.unique_table(owner), source.fixed_range_table(owner)].iter().any(|table| base.contains_table(table))
        {
            return Err(uncovered("The source has protected, expired or constrained history"));
        }
        let (previous, _previous) = self.read_owned_schema(owner, source).await?
            .ok_or_else(|| uncovered("The source declaration is missing"))?;
        let (successor, _successor) = self.read_owned_schema(owner, destination).await?
            .ok_or_else(|| uncovered("The destination declaration was not staged"))?;
        let source_orders = intrinsic_orders(source)?;
        let destination_orders = intrinsic_orders(destination)?;
        let orders = [
            (source_orders[0].as_str(), destination_orders[0].as_str()),
            (source_orders[1].as_str(), destination_orders[1].as_str()),
        ];
        let (field, codec) = previous.clear_compound_successor(&successor, &orders)?;
        if !field.strip_prefix("c_").is_some_and(|id| id.len() == 32 && id.bytes().all(lower_hex)) {
            return Err(uncovered("The changed column is not a stable clear fact field"));
        }
        self.check_adoption_routes(&base, owner, source, destination, [&source_orders, &destination_orders]).await?;
        rtdb_types::OwnedRetirement::at(self.tx_key().system_time, retain_micros)?;

        let store = self.context.storage.object_store().as_ref();
        let hold = HeldView::catalog_at(store, base.clone(), base.catalog_cut(), base.catalog_checkpoint()?, base.catalog_file_paths()).await?;
        let (mut data_history, journal_history) = IoRegion::reading(hold, async {
            let mut bytes_left = MAX_FILE_BYTES;
            let mut file_slots = MAX_FILES;
            let data = self.read_adoption_history(&base, &data, captured.data_events as usize, &mut bytes_left, &mut file_slots).await?;
            let journal = self.read_adoption_history(&base, &journal, captured.journal_events as usize, &mut bytes_left, &mut file_slots).await?;
            Ok::<_, Anomaly>((data, journal))
        }).await?;
        verify_journal(&data_history.events, &journal_history.events, &previous)?;
        let mut converted_bytes = MAX_INVENTORY_BYTES;
        for event in &mut data_history.events {
            self.context.check_cancelled()?;
            let StoredOperation::Put(document) = &mut event.operation else { continue; };
            previous.validate_row(document)?;
            for (index, (old, new)) in orders.iter().enumerate() {
                let Some(Value::Binary(key)) = document.remove(*old) else {
                    return Err(uncovered("The original is missing an intrinsic order key"));
                };
                if key.is_empty() != (index == 0) {
                    return Err(uncovered("The original order key has another role"));
                }
                document.insert((*new).into(), Value::Binary(key));
            }
            if let Some(Value::Binary(original)) = document.get(field) {
                if original.as_slice() != [2] {
                    let bytes = original.len().checked_mul(256).and_then(|bytes| bytes.checked_add(4096))
                        .ok_or_else(overflow)?;
                    converted_bytes = converted_bytes.checked_sub(bytes)
                        .ok_or_else(|| uncovered("The compound history exceeds its conversion budget"))?;
                    data_history.decoded.push(self.context.memory.reserve(bytes)?);
                    let converted = codec.convert(original)?;
                    document.insert(field.to_owned(), converted);
                }
            }
            successor.validate_row(document)?;
        }
        let key = self.tx_key();
        if data_history.events.iter().chain(&journal_history.events).any(|event| event.system_from >= key.system_time) {
            return Err(uncovered("Adoption must follow the original SQL history clock"));
        }
        let data = IndexBatch::copy_events(&data_history.events, &StructFields::new(), &self.context.memory)?;
        let journal = IndexBatch::copy_events(&journal_history.events, &StructFields::new(), &self.context.memory)?;
        self.retire_owned_relation(source, retain_micros).await?;
        self.buffer.adopt_owned_history(destination.table(owner), data)?;
        self.buffer.adopt_owned_history(destination.change_journal(owner)?, journal)?;
        let publication = OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?;
        let head = StructFields::from([
            ("_id".into(), Value::Utf8(format!("fact_partition_head:{}", destination.name()))),
        ]);
        let documents = Documents::new(vec![head], &self.context.memory)?;
        self.write_owned(&publication, &documents, None, Some(Timestamp::MIN), None, true).await?;
        self.buffer.put(OwnedRelation::adopted_catalog(owner), StructFields::from([
            ("_id".into(), Value::Utf8(destination.name().into())),
            ("predecessor".into(), Value::Utf8(source.name().into())),
            ("activation_tx".into(), Value::I64(key.tx_id.0)),
            ("activation_micros".into(), Value::I64(key.system_time.0)),
        ]), Interval::ALL)
    }

    async fn check_adoption_routes(
        &self, base: &Snapshot, owner: &str, source: &OwnedRelation, destination: &OwnedRelation, orders: [&[String; 2]; 2],
    ) -> Result<()> {
        let targets = [source.table(owner), destination.table(owner)];
        let context = self.context.query_context(base.clone(), self.tx_key().system_time,
            &self.options.default_tz, self.trace_parent)?.with_accelerators(Arc::from([]));
        let worlds = OwnedRelation::world_catalog().table(owner);
        if base.relation_event_frontier(&worlds)? > 1024 || rtdb_query::read_owned_worlds(Arc::clone(&context), worlds).await?
            .iter().any(|world| !world.dropped && world.until > self.tx_key().system_time
                && (world.schemas.relation(source).is_some() || world.schemas.relation(destination).is_some()))
        {
            return Err(uncovered("An installed World still selects an adoption relation"));
        }
        let mut installed = [BTreeSet::new(), BTreeSet::new()];
        for catalog in [OwnedRelation::path_catalog(owner), OwnedRelation::search_catalog(owner),
            OwnedRelation::spatial_catalog(owner), OwnedRelation::vector_catalog(owner)]
        {
            if base.relation_event_frontier(&catalog)? > 4096 {
                return Err(uncovered("The path catalog exceeds its adoption bound"));
            }
            let ordered = catalog.is_owned_path_catalog();
            let Some(mut paths) = rtdb_query::open_owned_path_catalog(Arc::clone(&context), &catalog, None).await? else { continue; };
            while let Some(rows) = paths.next_batch().await? {
                let batch = rows.batch();
                for row in 0..batch.num_rows() {
                    let cell = |name| ArrowCell::named(batch, name, row);
                    let Some(index) = targets.iter().position(|target|
                        cell("schema_name").and_then(ArrowCell::utf8) == Some(target.schema.as_str())
                            && cell("table_name").and_then(ArrowCell::utf8) == Some(target.table.as_str()))
                    else { continue; };
                    let intrinsic = cell("columns").and_then(ArrowCell::list).is_some_and(|mut columns| {
                        columns.len() == 2 && columns.next().and_then(|column| column.utf8())
                            .is_some_and(|column| orders[index].iter().any(|name| name == column)
                                && cell("_id").and_then(ArrowCell::utf8) == Some(column)
                                && installed[index].insert(column.to_owned()))
                            && columns.next().and_then(|column| column.utf8()) == Some("_id")
                    });
                    if !ordered || !intrinsic {
                        return Err(uncovered("An adoption would change an installed access path"));
                    }
                }
            }
        }
        if installed.iter().any(|paths| paths.len() != 2) {
            return Err(uncovered("An adoption requires both installed intrinsic orders"));
        }
        Ok(())
    }

    async fn read_adoption_history(
        &self, base: &Snapshot, table: &TableRef, expected: usize, bytes_left: &mut usize, files: &mut usize,
    ) -> Result<History> {
        let reserve = expected.checked_mul(1024).ok_or_else(overflow)?;
        let mut history = History {
            events: Vec::new(), _rows: self.context.memory.reserve(reserve)?,
            decoded: Vec::new(), bytes_left: MAX_INVENTORY_BYTES,
        };
        history.events.reserve_exact(expected);
        let catalog = base.trie_catalog();
        if catalog.iter_tries(table).take(MAX_FILES + 1).count() > MAX_FILES {
            return Err(uncovered("The source exceeds its trie inventory bound"));
        }
        // Compaction can replace the physical files without changing their
        // positioned events. The authenticated file census and journal check
        // must still cover every Original event before the successor is staged.
        for trie in catalog.current_tries(table) {
            *files = files.checked_sub(1).ok_or_else(|| uncovered("The source exceeds its file count bound"))?;
            let entry = base.table_entry(table).ok_or_else(|| uncovered("The trie has no admitted table entry"))?;
            let path = ObjectKey::new(entry.slug.data_key(&trie.key))?;
            let publication = trie.published_files.as_ref().and_then(|files| files.data.as_ref())
                .ok_or_else(|| uncovered("The source trie has no authenticated publication"))?;
            let rows = trie.metadata.as_ref().and_then(|meta| meta.row_count)
                .and_then(|count| usize::try_from(count).ok()).ok_or_else(|| uncovered("The source trie has no exact row count"))?;
            let length = usize::try_from(trie.data_file_size).map_err(|_| uncovered("Invalid source trie extent"))?;
            *bytes_left = bytes_left.checked_sub(length).ok_or_else(|| uncovered("The source exceeds its object extent bound"))?;
            self.read_adoption_file(&mut history, expected, rows, path, publication, length).await?;
        }
        for segment in base.iter_segments(table) {
            for batch in segment.batches() {
                self.context.check_cancelled()?;
                history.read(batch.view(), expected, &self.context.memory)?;
            }
        }
        history.events.sort_unstable_by_key(|event| event.position);
        if history.events.len() != expected || history.events.windows(2).any(|pair| pair[0].position == pair[1].position) {
            return Err(uncovered("The source inventory did not exhaust unique original events"));
        }
        Ok(history)
    }

    async fn read_adoption_file(
        &self, history: &mut History, expected: usize, rows: usize, key: ObjectKey,
        publication: &PublishedObjectVersion, length: usize,
    ) -> Result<()> {
        let memory = &self.context.memory;
        let store = self.context.storage.object_store().as_ref();
        let require_publication = || async {
            if store.retirement_publication(&key).await?.map(PublishedObjectVersion::from).as_ref() != Some(publication) {
                return Err(uncovered("The source trie publication changed"));
            }
            Ok::<_, Anomaly>(())
        };
        self.context.check_cancelled()?;
        require_publication().await?;
        let allocation = store.exact_object_allocation(&key, length)?;
        let charge = memory.reserve(allocation.retained_bytes.checked_add(allocation.scratch_bytes).ok_or_else(overflow)?)?;
        let bytes = IoRegion::retaining(charge.clone(), store.get_fresh_exact(&key, length)).await?;
        require_publication().await?;
        let mut file = Cursor::new(bytes);
        let layout = inspect_file_reserved(&mut file, |bytes| memory.reserve(bytes))?;
        let _opening = memory.reserve(layout.opening_allocation_bound()?)?;
        let mut file = ArrowFile::from_inspected_reader(file, None, layout)?;
        let before = history.events.len();
        for page in 0..file.page_count() {
            self.context.check_cancelled()?;
            let _page = memory.reserve(file.page_allocation_bound(page)?)?;
            history.read(&EventBatch::new(file.read_page(page)?)?, expected, memory)?;
        }
        if history.events.len() - before != rows {
            return Err(uncovered("The authenticated source trie has another row count"));
        }
        Ok(())
    }
}

fn lower_hex(byte: u8) -> bool {
    matches!(byte, b'0'..=b'9' | b'a'..=b'f')
}

fn intrinsic_orders(relation: &OwnedRelation) -> Result<[String; 2]> {
    let invalid = || uncovered("An adoption requires a certified Root layout identity");
    let digest = relation.name().strip_prefix("fact_").filter(|digest| digest.len() == 64 && digest.bytes().all(lower_hex)).ok_or_else(invalid)?;
    let mut layout = [0; 32];
    for (index, byte) in layout.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digest[index * 2..index * 2 + 2], 16).map_err(|_| invalid())?;
    }
    let order = |fields: &[&[u8]]| {
        let mut sha = Sha256::new();
        sha.update(b"bijection.native-root-catalog.v1");
        for part in [b"ordered".as_slice(), layout.as_slice()].into_iter().chain(fields.iter().copied()) {
            sha.update((part.len() as u64).to_be_bytes());
            sha.update(part);
        }
        let mut column = String::from("p_");
        for byte in &sha.finalize()[..16] {
            write!(&mut column, "{byte:02x}").expect("writing to a string cannot fail");
        }
        column
    };
    Ok([order(&[]), order(&[b"_creationTime"])])
}

struct History {
    events: Vec<StoredEvent>,
    _rows: Arc<MemoryReservation>,
    decoded: Vec<Arc<MemoryReservation>>,
    bytes_left: usize,
}

impl History {
    fn read(&mut self, batch: &EventBatch, limit: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        if batch.len() > limit - self.events.len() {
            return Err(uncovered("The source exceeds its admitted event frontier"));
        }
        for row in 0..batch.len() {
            let borrowed = batch.borrow_event(row)?;
            let bytes = borrowed.document_allocation_bound()?.checked_mul(8)
                .and_then(|size| size.checked_add(borrowed.physical_entity_key().map_or(0, |key| key.len())))
                .and_then(|size| size.checked_add(2048)).ok_or_else(overflow)?;
            self.bytes_left = self.bytes_left.checked_sub(bytes).ok_or_else(|| uncovered("The source exceeds its decoded history budget"))?;
            self.decoded.push(memory.reserve(bytes)?);
            let event = batch.read_event(row)?;
            if event.position.is_none() {
                return Err(uncovered("The source history contains an unpositioned event"));
            }
            self.events.push(event);
        }
        Ok(())
    }
}

fn verify_journal(data: &[StoredEvent], journal: &[StoredEvent], schema: &OwnedSchema) -> Result<()> {
    let invalid = || uncovered("The source journal does not cover the original positioned population");
    let mut through = 0;
    for (revision, entry) in journal.iter().enumerate() {
        let StoredOperation::Put(document) = &entry.operation else { return Err(invalid()); };
        if entry.valid_time != Interval::ALL || document.get("_id") != Some(&Value::I64(revision as i64))
            || entry.iid != EntityId::Integer(revision as i64).iid()
            || document.get("fixed_assertion_keys").is_some_and(|value| !matches!(value, Value::Undefined | Value::Null))
        {
            return Err(invalid());
        }
        match (revision, document.get("entry")) {
            (0, Some(Value::Null)) => (),
            (1.., Some(Value::List(touches))) => {
                if touches.is_empty() { return Err(invalid()); }
                for touch in touches.iter() {
                    let touch = OwnedChange::from_value(touch.as_ref())?;
                    let event = data.get(through).ok_or_else(invalid)?;
                    let header = event.header();
                    let kind = match header.op {
                        EventOp::Put => OwnedChangeKind::Put,
                        EventOp::Delete => OwnedChangeKind::Delete,
                        _ => return Err(invalid()),
                    };
                    schema.require_replayed_period(event.valid_time, event.system_from, header.op)?;
                    if event.iid != touch.id.iid() || event.entity_key.as_deref() != Some(touch.id.handle_bytes().as_slice())
                        || event.valid_time != touch.valid || kind != touch.kind
                        || event.position >= entry.position
                        || event.position.map(|position| position.transaction) != entry.position.map(|position| position.transaction)
                        || event.system_from != entry.system_from
                        || NonZeroU32::new(event.commit_position) != touch.assertion_ordinal
                    {
                        return Err(invalid());
                    }
                    through += 1;
                }
            }
            _ => return Err(invalid()),
        }
        if document.get("through_events") != Some(&Value::I64(through as i64)) { return Err(invalid()); }
    }
    if through != data.len() { return Err(invalid()); }
    Ok(())
}

pub(super) fn uncovered(reason: impl Into<String>) -> Anomaly {
    Anomaly::incorrect("rtdb.replication/fact-history-uncovered", reason)
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, sync::Arc};

    use rtdb_columnar::MemoryBudget;
    use rtdb_format::{data::{StoredEvent, StoredOperation}, envelope::ReplicaBody};
    use rtdb_index::PartitionState;
    use rtdb_query::QueryOptions;
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{
        DecimalValue, EntityId, EventPosition, Interval, OwnedChange, OwnedChangeKind, OwnedRelation,
        OwnedSchema, PlacedAttemptScope, ROOT_ROW_CARRIERS, Result, RootRowCarrier, SHARED_COLUMN_CARRIERS,
        SourcePosition, StructFields, TableRef, Timestamp, TransactionKey, TxId, Value,
    };
    use tokio_util::sync::CancellationToken;

    use crate::{Documents, HostedAuthority, HostedControlWrite, SourceTransaction, SubmitOptions, TxOp, bind_installed_authority};
    use super::{intrinsic_orders, verify_journal};
    use super::super::{OpenTransaction, OwnedPathKind, ResolveContext, TransactionAuthorization, TxResolver, attempt_marker_document};

    const PAYLOAD: &str = "c_0123456789abcdef0123456789abcdef";

    #[test]
    fn clear_compound_successor_refuses_protected_declaration_roles() -> Result<()> {
        use serde_json::json;

        let schema = |orders: [&str; 2], payload: serde_json::Value, protection: Option<(&str, &str)>| -> Result<OwnedSchema> {
            let mut fields = orders.map(|name|
                json!({"name": name, "type": {"kind": "binary"}, "optional": false, "nullable": false})
            ).to_vec();
            fields.push(json!({"name": PAYLOAD, "type": payload, "optional": true, "nullable": false}));
            if let Some((name, kind)) = protection {
                fields.push(json!({"name": name, "type": {"kind": kind}, "optional": true, "nullable": false}));
            }
            OwnedSchema::from_json(json!({
                "version": 2, "validity": "acceptance", "fields": fields,
            }).to_string().as_bytes())
        };
        let previous = ["population_before", "creation_before"];
        let successor = ["population_after", "creation_after"];
        let orders = [(previous[0], successor[0]), (previous[1], successor[1])];
        let binary = json!({"kind": "binary"});
        let typed = json!({"kind": "union", "legs": [
            {"kind": "binary"}, {"kind": "struct", "fields": [{
                "name": "v2_value", "type": {"kind": "list", "element": {"kind": "i64"}},
                "optional": false, "nullable": true,
            }]},
        ]});
        let before = schema(previous, binary.clone(), None)?;
        let after = schema(successor, typed.clone(), None)?;
        assert_eq!(before.clear_compound_successor(&after, &orders)?.0, PAYLOAD);
        assert!(schema(previous, json!({"kind": "sealed"}), None)?
            .clear_compound_successor(&after, &orders).is_err());
        for control in [
            ("sealed_payload", "opaque_binary"),
            ("membership_until", "binary"),
            ("label", "label"),
        ] {
            let before = schema(previous, binary.clone(), Some(control))?;
            let after = schema(successor, typed.clone(), Some(control))?;
            assert!(before.clear_compound_successor(&after, &orders).is_err(), "{control:?}");
        }
        for relation in [ROOT_ROW_CARRIERS, SHARED_COLUMN_CARRIERS] {
            assert!(intrinsic_orders(&OwnedRelation::new(relation)?).is_err());
        }
        Ok(())
    }

    #[test]
    fn positioned_binary_assertion_census_refuses_pruning_and_substitutions() -> Result<()> {
        let schema = OwnedSchema::from_json(br#"{
            "version": 2, "validity": "acceptance", "fields": [
                {"name": "membership_until", "type": {"kind": "binary"}, "optional": true, "nullable": false}
            ]
        }"#)?;
        assert!(schema.has_world_assertion_journal());
        let mut address = RootRowCarrier::table_prefix(&[8; 16]).to_vec();
        address.extend_from_slice(&[0, 255, 0, 255]);
        address.extend_from_slice(&[9; 32]);
        address.extend_from_slice(&[10; 32]);
        address.extend_from_slice(&[0, 1]);
        let carrier = RootRowCarrier::parse(&address).expect("complete protected Root address");
        let assertion = EntityId::Binary(carrier.assertion_key());
        let retirement = EntityId::Binary(carrier.retirement_key());
        let position = |tx, ordinal| EventPosition { transaction: TxId(tx), ordinal };
        let put = |id: &EntityId, position, valid_time, commit_position, mut fields: StructFields<Value>| {
            fields.insert("_id".into(), id.as_value());
            StoredEvent {
                position: Some(position), origin: None, entity_key: None, iid: id.iid(),
                system_from: Timestamp(200), valid_time, commit_position,
                operation: StoredOperation::Put(fields), recency: None,
            }
        };
        let valid = Interval::new(Timestamp(180), Timestamp::MAX)?;
        let mut data = [
            put(&assertion, position(10, 2), valid, 7, StructFields::from([
                ("membership_until".into(), Value::Binary(i64::MAX.to_be_bytes().to_vec())),
            ])),
            put(&retirement, position(11, 2), valid, 8, StructFields::new()),
        ];
        for (event, id) in data.iter_mut().zip([&assertion, &retirement]) {
            event.entity_key = Some(id.handle_bytes().into());
        }
        let entry = |revision, position, through, value| {
            put(&EntityId::Integer(revision), position, Interval::ALL, 0, StructFields::from([
                ("entry".into(), value), ("through_events".into(), Value::I64(through)),
            ]))
        };
        let touch = |id: &EntityId, ordinal| Value::list(vec![OwnedChange {
            id: id.clone(), kind: OwnedChangeKind::Put, valid, assertion_ordinal: NonZeroU32::new(ordinal),
        }.to_value()]);
        let journal = [
            entry(0, position(10, 3), 0, Value::Null),
            entry(1, position(10, 4), 1, touch(&assertion, 7)),
            entry(2, position(11, 3), 2, touch(&retirement, 8)),
        ];
        verify_journal(&data, &journal, &schema)?;
        assert!(verify_journal(&data, &journal[1..], &schema).is_err());
        assert!(verify_journal(&data, &journal[..2], &schema).is_err());
        assert!(verify_journal(&data[1..], &journal, &schema).is_err());

        for wrong in [touch(&assertion, 8), touch(&retirement, 7)] {
            let mut substituted = journal.clone();
            substituted[2] = entry(2, position(11, 3), 2, wrong);
            assert!(verify_journal(&data, &substituted, &schema).is_err());
        }
        let mut substituted = data.clone();
        substituted[1].entity_key = Some(assertion.handle_bytes().into());
        assert!(verify_journal(&substituted, &journal, &schema).is_err());
        data[1].valid_time = Interval::new(Timestamp(190), Timestamp::MAX)?;
        assert!(verify_journal(&data, &journal, &schema).is_err());
        data[1].valid_time = valid;
        data[1].position = Some(position(10, 5));
        assert!(verify_journal(&data, &journal, &schema).is_err());
        Ok(())
    }

    struct InstalledWriter;
    impl TransactionAuthorization for InstalledWriter {
        fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) }
        fn is_hosted_control_owner(&self, principal: &str) -> bool { principal == "owner" }
    }

    #[tokio::test]
    async fn adoption_replays_original_rows_and_extends_the_successor_journal() -> Result<()> {
        let owner = "owner";
        let scope = PlacedAttemptScope::new("field-adoption", 1)?;
        let memory = MemoryBudget::new(64 << 20);
        let context = ResolveContext {
            decision_clock: None, write_event_origins: true, placed_receipt_retirement: false,
            owned_witness_scope: Some(scope), world_constraints: None, database: "app".into(),
            storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(&memory), query_options: QueryOptions::default(), scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(InstalledWriter),
            execution_observer: None, tracer: None,
        };
        let options = SubmitOptions { user: Some(owner.into()), ..Default::default() };
        let source = OwnedRelation::new(format!("fact_{}", "12".repeat(32)))?;
        let target = OwnedRelation::new(format!("fact_{}", "34".repeat(32)))?;
        let [source_order, source_creation] = intrinsic_orders(&source)?;
        let [target_order, target_creation] = intrinsic_orders(&target)?;
        let schema = |order: &str, creation: &str, payload| -> Result<OwnedSchema> {
            let declaration = serde_json::json!({"version": 2, "validity": "acceptance", "fields": [
                {"name": order, "type": {"kind": "binary"}, "optional": false, "nullable": false},
                {"name": creation, "type": {"kind": "binary"}, "optional": false, "nullable": false},
                {"name": PAYLOAD, "type": payload, "optional": true, "nullable": false},
            ]});
            OwnedSchema::from_json(declaration.to_string().as_bytes())
        };
        let original_schema = schema(&source_order, &source_creation, serde_json::json!({"kind": "binary"}))?;
        let next_schema = schema(&target_order, &target_creation, serde_json::json!({"kind": "union", "legs": [
            {"kind": "binary"}, {"kind": "struct", "fields": [{
                "name": "v2_value", "type": {"kind": "list", "element": {"kind":"struct", "fields":[
                    {"name":"coefficient", "type":{"kind":"decimal","precision":38,"scale":0,"bitWidth":128},
                        "optional":false,"nullable":false},
                    {"name":"scale","type":{"kind":"i8"},"optional":false,"nullable":false}
                ]}}, "optional": false, "nullable": true,
            }]}
        ]}))?;
        let state = PartitionState::empty(Default::default())?;
        let key = |id, time| TransactionKey { tx_id: TxId(id), system_time: Timestamp(time) };
        let control = |id: &str, value| StructFields::from([
            ("_id".into(), Value::Utf8(id.into())), ("layout".into(), Value::I64(value)),
        ]);
        let mut install = OpenTransaction::new(state.snapshot(), key(1, 100), &options, &context)?;
        for (table, id) in [("bj_fact_root_owner", "root"), ("bj_relation_declarations", "tablet")] {
            install.buffer.put(TableRef::new("public", table), control(id, 1), Interval::ALL)?;
        }
        install.buffer.put(TableRef::new("public", "bj_installation"), StructFields::from([
            ("_id".into(), Value::Utf8("authority".into())), ("kind".into(), Value::Utf8("hosted".into())),
            ("incarnation".into(), Value::Binary(vec![7; 16])),
            ("writer".into(), Value::Utf8("11111111111111111111111111111111".into())),
        ]), Interval::ALL)?;
        install.declare_owned_relation(&source, &original_schema).await?;
        install.declare_owned_relation(&target, &next_schema).await?;
        for (relation, orders) in [(&source, [&source_order, &source_creation]), (&target, [&target_order, &target_creation])] {
            for column in orders {
                install.declare_owned_path(relation, &OwnedRelation::new(column.as_str())?,
                    &[column.to_string(), "_id".into()], OwnedPathKind::Ordered).await?;
            }
        }
        state.apply(&install.buffer.stage()?)?;
        let publications = OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?;
        let source_head = EntityId::String(format!("fact_partition_head:{}", source.name()));
        let target_head = EntityId::String(format!("fact_partition_head:{}", target.name()));
        let id = EntityId::Binary(vec![0, 0xff]);
        let document = |id: &EntityId, order: &str, creation: &str, payload: Option<Value>| {
            let mut row = StructFields::from([
                ("_id".into(), id.as_value()), (order.into(), Value::Binary(Vec::new())),
                (creation.into(), Value::Binary(vec![9])),
            ]);
            if let Some(payload) = payload { row.insert(PAYLOAD.into(), payload); }
            row
        };
        let mut original = OpenTransaction::new(state.snapshot(), key(2, 200), &options, &context)?;
        original.terms_time = Timestamp(180);
        let mut decimal = vec![12, 6, 0, 216, 254, 253, 252];
        decimal.resize(42, 255);
        decimal.extend_from_slice(&[0, 0]);
        let put = Documents::new(vec![
            document(&id, &source_order, &source_creation, Some(Value::Binary(decimal))),
            document(&EntityId::Binary(vec![0, 0xff, 1]), &source_order, &source_creation, None),
            document(&EntityId::Binary(vec![1]), &source_order, &source_creation, Some(Value::Binary(vec![2]))),
        ], &memory)?;
        original.write_owned(&source, &put, None, None, None, true).await?;
        let marker = Documents::new(vec![StructFields::from([("_id".into(), source_head.as_value())])], &memory)?;
        original.write_owned(&publications, &marker, None, Some(Timestamp::MIN), None, true).await?;
        original.write_owned_changes().await?;
        state.apply(&original.buffer.stage()?.assign_event_origins(&memory)?)?;
        let mut deletion = OpenTransaction::new(state.snapshot(), key(3, 200), &options, &context)?;
        deletion.terms_time = Timestamp(190);
        deletion.delete_owned(&source, &id, None, None).await?;
        deletion.write_owned_changes().await?;
        state.apply(&deletion.buffer.stage()?.assign_event_origins(&memory)?)?;
        let base = state.snapshot();
        let before = context.query_context(base.clone(), Timestamp(200), "UTC", None)?;
        let held_head = rtdb_query::read_owned_point_version(before, owner, &publications, &source_head, Interval::at(Timestamp(200))?).await?;
        assert!(held_head.is_some());
        let request = |source_frontier| -> Result<SourceTransaction> {
            let mut authority = [HostedControlWrite::check(
                OwnedRelation::new("bj_installation")?, scope, Some(Timestamp(100)),
                StructFields::from([("_id".into(), Value::Utf8("authority".into()))]),
            )?];
            let mut proof = [0x11; HostedAuthority::WIDTH];
            proof[..8].copy_from_slice(&100i64.to_be_bytes());
            proof[8..24].fill(7);
            bind_installed_authority(&mut authority, HostedAuthority::decode(&proof)?)?;
            let [authority] = authority;
            let mut ops = vec![
                TxOp::HostedControl(authority),
                TxOp::owned_deadline(Timestamp(1000)),
                TxOp::OwnedInsertDocs {
                    relation: OwnedRelation::new(rtdb_types::OWNED_ATTEMPT_RELATION)?,
                    documents: Documents::new(vec![attempt_marker_document("00112233445566778899aa", [8; 32])], &memory)?,
                    valid_from: Some(Timestamp::MIN), valid_to: None,
                },
                TxOp::OwnedAdoptRelation { source: source.clone(), destination: target.clone(), retain_micros: 5000 },
            ];
            for (table, id) in [("bj_fact_root_owner", "root"), ("bj_relation_declarations", "tablet")] {
                ops.push(TxOp::HostedControl(HostedControlWrite::new(
                    OwnedRelation::new(table)?, scope, Some(Timestamp(100)), control(id, 2),
                )?));
            }
            for (relation, id, expected, frontier) in [
                (&source, source_head.clone(), held_head, source_frontier),
                (&target, target_head.clone(), None, 0),
            ] {
                ops.push(TxOp::OwnedCheck { relation: publications.clone(), id, valid_at: Timestamp(200), expected, revealed: None });
                ops.push(TxOp::OwnedCheckRange {
                    relation: relation.clone(), basis: key(3, 200), scope, frontier, until: Timestamp(900), region: None,
                    revealed: None,
                });
            }
            Ok(SourceTransaction::new(ops, options.clone(), &memory)?.with_placed_seed(Some(scope.issue())))
        };
        let mut stale = TxResolver::new(base.tx_basis(), SourcePosition(3), None, &memory)?;
        let refused = stale.resolve_source(base.clone(), SourcePosition(4), Timestamp(300), request(3)?, &context).await?;
        assert_eq!(refused.result().error().map(|error| error.code.as_str()), Some("rtdb.owned/stale-range"));
        assert!(!refused.writes().tables().contains_key(&target.table(owner)));

        let mut resolver = TxResolver::new(base.tx_basis(), SourcePosition(3), None, &memory)?;
        let adopted = resolver.resolve_source(base.clone(), SourcePosition(4), Timestamp(300), request(4)?, &context).await?;
        assert!(adopted.result().committed(), "{:?}", adopted.result().error());
        let body = adopted.encode_replica_body(&memory, None)?;
        let ReplicaBody::ResolvedTx(wire) = body.body() else { unreachable!() };
        let mut incomplete = wire.clone();
        incomplete.table_data.remove(&target.change_journal(owner)?.schema_and_table());
        assert!(crate::decode_replica(&incomplete, base.tx_basis(), base.block_index(), &memory).is_err());
        let reopened = crate::decode_replica(wire, base.tx_basis(), base.block_index(), &memory)?;
        let table = target.table(owner);
        let data = reopened.writes.tables()[&table].batch().expect("adopted data");
        let journal = reopened.writes.tables()[&target.change_journal(owner)?].batch().expect("adopted journal");
        assert_eq!((data.len(), journal.len()), (4, 3));
        let stored = data.view().read_event(0)?;
        assert_eq!(stored.system_from, Timestamp(200));
        assert_eq!(stored.valid_time.lower(), Timestamp(180));
        let StoredOperation::Put(fields) = stored.operation else { unreachable!() };
        assert_eq!(fields.get("_id"), Some(&id.as_value()));
        let Some(Value::Struct(wrapper)) = fields.get(PAYLOAD) else { unreachable!() };
        let Some(Value::List(list)) = wrapper.get("v2_value") else { unreachable!() };
        let item = list.iter().next().unwrap();
        let Value::Struct(parts) = item.as_ref() else { unreachable!() };
        assert_eq!(list.len(), 1);
        assert_eq!(parts.get("coefficient"), Some(&Value::Decimal(DecimalValue::new(
            (-123i128).to_be_bytes().to_vec(), 38, 0, 128,
        )?)));
        assert_eq!(parts.get("scale"), Some(&Value::I8(2)));
        assert_eq!(fields.get(&target_order), Some(&Value::Binary(Vec::new())));
        let StoredOperation::Put(missing) = data.view().read_event(1)?.operation else { unreachable!() };
        let StoredOperation::Put(null) = data.view().read_event(2)?.operation else { unreachable!() };
        assert!(matches!(missing.get(PAYLOAD), Some(Value::Undefined) | None));
        assert_eq!(null.get(PAYLOAD), Some(&Value::Binary(vec![2])));
        assert_eq!(data.header(0)?.position.unwrap().transaction, TxId(2));
        assert_eq!(data.header(3)?.position.unwrap().transaction, TxId(3));
        assert_eq!(data.header(3)?.valid_time.lower(), Timestamp(190));
        assert_eq!(data.header(3)?.system_from, Timestamp(200));
        assert_eq!(data.view().origin(0)?, base.iter_segments(&source.table(owner)).next().unwrap().batches().next().unwrap().view().origin(0)?);
        assert!(!base.contains_table(&table));

        state.apply(&reopened.writes)?;
        let held = context.query_context(state.snapshot(), Timestamp(300), "UTC", None)?;
        assert_eq!(rtdb_query::read_owned_adoption(Arc::clone(&held), owner, &target).await?, Some((source.clone(), key(4, 300))));
        assert!(rtdb_query::read_owned_retirement(Arc::clone(&held), owner, &source).await?.is_some());
        let attempts = OwnedRelation::new(rtdb_types::OWNED_ATTEMPT_RELATION)?;
        let marker = EntityId::String("00112233445566778899aa".into());
        let receipt = rtdb_query::read_owned_point_version(held, owner, &attempts, &marker, Interval::at(Timestamp(300))?).await?;
        assert_eq!(receipt.map(|receipt| receipt.position.transaction), Some(TxId(4)));
        let current = state.snapshot();
        let mut retry = TxResolver::new(current.tx_basis(), SourcePosition(4), None, &memory)?;
        let repeat = retry.resolve_source(current, SourcePosition(5), Timestamp(400), request(4)?, &context).await?;
        assert!(!repeat.result().committed());
        assert!(!repeat.writes().tables().contains_key(&table));
        let mut next = OpenTransaction::new(state.snapshot(), key(5, 400), &options, &context)?;
        let put = Documents::new(vec![
            document(&EntityId::Binary(vec![2]), &target_order, &target_creation, None),
        ], &memory)?;
        next.write_owned(&target, &put, None, None, None, true).await?;
        next.write_owned_changes().await?;
        state.apply(&next.buffer.stage()?)?;
        let after = context.query_context(state.snapshot(), Timestamp(400), "UTC", None)?;
        let head = rtdb_query::read_owned_change_entry(after, owner, &target, 3).await?;
        assert_eq!(head.through_events, 5);
        assert_eq!(head.publication, key(5, 400));

        let mut erase = OpenTransaction::new(state.snapshot(), key(6, 500), &options, &context)?;
        erase.erase_owned(&target, &id, OwnedChangeKind::Erase).await?;
        erase.write_owned_changes().await?;
        state.apply(&erase.buffer.stage()?)?;
        for relation in [&source, &target] {
            assert_eq!(state.snapshot().relation_event_frontier(&relation.purge_table(owner))?, 1);
        }
        Ok(())
    }
}
