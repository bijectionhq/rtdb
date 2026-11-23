use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{catalog::{BlockCatalog, TableBlock}, envelope::{ReplicaBody, SourceBody, SourceMessage}, keys::{TableSlug, TrieKey, block_key, lex_hex}, log::proto};
use rtdb_plan::{CatalogInspection, CatalogRows, InspectionRequest, InspectionRow};
use rtdb_storage::{ObjectKey, ReadReservation, cache::CacheLease, log::LogRange};
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result, SourcePosition, TimeUnit, Value};

use crate::{Database, inspection_render as edn};

enum Input {
    Log { range: Box<dyn LogRange>, source: bool, tx_ops: bool },
    Blocks { next: i64, end: i64 },
    Table { name: String, block: i64, tries: bool },
    Tries { name: String, block: i64, partitions: Vec<rtdb_format::block::proto::Partition>, reservation: Arc<MemoryReservation>, partition: usize, trie: usize },
    Done,
}

struct Inspection { database: Arc<Database>, memory: Arc<MemoryBudget>, input: Input }

pub(crate) async fn open(database: Arc<Database>, request: InspectionRequest, memory: Arc<MemoryBudget>) -> Result<Box<dyn CatalogInspection>> {
    let input = match request {
        InspectionRequest::SourceLog { from, to, tx_ops } => Input::Log { range: database.logs.source_range(SourcePosition(from), SourcePosition(to)).await?, source: true, tx_ops },
        InspectionRequest::ReplicaLog { from, to } => Input::Log { range: database.logs.replica_range(ReplicaPosition(from), ReplicaPosition(to)).await?, source: false, tx_ops: false },
        InspectionRequest::BlockFiles { from, to } => Input::Blocks { next: from, end: to },
        InspectionRequest::TableBlock { table, block, tries } => Input::Table { name: table, block, tries },
    };
    Ok(Box::new(Inspection { database, memory, input }))
}

#[async_trait]
impl CatalogInspection for Inspection {
    async fn next(&mut self) -> Result<Option<CatalogRows<InspectionRow>>> {
        loop {
            match std::mem::replace(&mut self.input, Input::Done) {
                Input::Done => return Ok(None),
                Input::Log { mut range, source, tx_ops } => {
                    let bound = range.allocation_bound();
                    let reservation = self.memory.reserve(bound)?;
                    let Some(record) = range.next(ReadReservation::new(bound, reservation)).await? else { return Ok(None); };
                    self.input = Input::Log { range, source, tx_ops };
                    let reservation = self.memory.reserve(allocation(record.payload.len(), 64)?)?;
                    let position = if source { self.database.logs.source_position(record.metadata)?.0 } else { ReplicaPosition::from_epoch_offset(self.database.logs.replica_epoch, record.metadata.offset.0)?.0 };
                    let mut row = row([
                        ("msg_id", Value::I64(position)), ("log_offset", Value::I64(record.metadata.offset.0)), ("log_timestamp", timestamp(record.metadata.timestamp.0)),
                    ]);
                    let mut reservations = vec![reservation];
                    let (kind, message) = if source {
                        let message = SourceMessage::decode(&record.payload)?;
                        if tx_ops {
                            let (value, charge) = match &message {
                                Some(SourceMessage::Protobuf(SourceBody::Tx(tx))) => decode_operations(tx.tx_ops.as_deref(), &self.memory)?,
                                _ => (Value::Null, None),
                            };
                            if let Some(charge) = charge { reservations.push(charge); }
                            row.insert("tx_ops".into(), value);
                        }
                        source_message(message.as_ref())?
                    } else {
                        let record = self.database.logs.decode_replica(record)?;
                        match record {
                            None => ("unknown", edn::map([("type", ":unknown".into())])),
                            Some(record) => {
                                let record = self.database.logs.resolve_oversized(record).await?;
                                reservations.push(self.memory.reserve(allocation(record.retained_bytes(), 8)?)?);
                                replica_message(&record.envelope.message)?
                            }
                        }
                    };
                    row.insert("msg_type".into(), Value::Utf8(kind.into())); row.insert("msg".into(), Value::Utf8(message));
                    return page(row, reservations, &self.memory).map(Some);
                }
                Input::Blocks { mut next, end } => {
                    let window_end = next.saturating_add(1024).min(end);
                    let mut rows = Vec::new();
                    let mut reservations = Vec::new();
                    while next < window_end {
                        let block = next; next += 1;
                        self.input = Input::Blocks { next, end };
                        if next % 1024 == 0 { tokio::task::yield_now().await; }
                        let Some(bytes) = self.read(&block_key(BlockIndex(block))?).await? else { continue; };
                        let reservation = self.memory.reserve(allocation(bytes.as_slice().len(), 256)?)?;
                        let Ok(catalog) = BlockCatalog::decode(bytes.as_slice()) else { continue; };
                        let proto = catalog.as_proto();
                        let row = row([
                            ("block_idx", Value::Utf8(lex_hex(proto.block_index.unwrap_or_default() as u64))),
                            ("tx_id", proto.latest_completed_tx.as_ref().map_or(Value::Null, |key| Value::I64(key.tx_id.unwrap_or_default()))),
                            ("system_time", proto.latest_completed_tx.as_ref().map_or(Value::Null, |key| timestamp(key.system_time.unwrap_or_default()))),
                            ("latest_processed_msg_id", Value::I64(proto.latest_processed_msg_id.unwrap_or_default())),
                            ("table_names", Value::Utf8(edn::vector(proto.table_names.iter().map(|name| edn::quoted(name))))),
                            ("boundary_replica_msg_id", proto.boundary_replica_msg_id.map_or(Value::Null, Value::I64)),
                            ("file_size", Value::I64(bytes.as_slice().len() as i64)),
                        ]);
                        let retained = self.memory.reserve(Value::document_retained_bytes(&row)?.checked_add(512).ok_or_else(size_error)?)?;
                        drop(reservation);
                        rows.push(row); reservations.push(retained);
                    }
                    if rows.is_empty() { self.input = Input::Done; return Ok(None); }
                    self.input = Input::Blocks { next, end };
                    return pages(rows, reservations).map(Some);
                }
                Input::Table { name, block, tries } => {
                    let table = rtdb_format::catalog::legacy_table_ref(&name)?;
                    let value = self.database.partition.value();
                    let slug = value.table_catalog().entry(&table).map(|entry| entry.slug.clone()).unwrap_or(TableSlug::for_unrecorded_table(&table)?);
                    let Some(bytes) = self.read(&slug.block_key(BlockIndex(block))?).await? else { return Ok(None); };
                    let reservation = self.memory.reserve(allocation(bytes.as_slice().len(), 256)?)?;
                    let Ok(metadata) = TableBlock::decode_reserved(bytes.as_slice(), Arc::clone(&reservation)) else { return Ok(None); };
                    let partitions = partitions(&metadata)?;
                    if tries { self.input = Input::Tries { name, block, partitions, reservation, partition: 0, trie: 0 }; continue; }
                    let schema = match rtdb_format::ipc::read_schema_message(metadata.as_proto().arrow_schema.as_deref().unwrap_or_default()) { Ok(schema) => schema, Err(_) => return Ok(None) };
                    let fields = schema.fields().iter().map(|field| Ok(format!("{} {}", edn::quoted(field.name()), rtdb_columnar::from_field(field)?.to_edn()?))).collect::<Result<Vec<_>>>()?.join(", ");
                    let hlls = metadata.as_proto().column_name_to_hll.iter().map(|(name, bytes)| Ok(format!("{} {}", edn::quoted(name), rtdb_format::hll::Hll::from_bytes(bytes.clone())?.estimate() as i64))).collect::<Result<Vec<_>>>()?.join(", ");
                    return page(row([
                        ("table_name", Value::Utf8(name)), ("block_idx", Value::Utf8(lex_hex(block as u64))), ("row_count", Value::I64(metadata.row_count())),
                        ("fields", Value::Utf8(format!("{{{fields}}}"))), ("hlls", Value::Utf8(format!("{{{hlls}}}"))), ("partition_count", Value::I32(partitions.len() as i32)),
                    ]), vec![reservation], &self.memory).map(Some);
                }
                Input::Tries { name, block, partitions, reservation: metadata_reservation, mut partition, mut trie } => {
                    while partition < partitions.len() && trie >= partitions[partition].tries.len() { partition += 1; trie = 0; }
                    let Some(part) = partitions.get(partition) else { return Ok(None); };
                    let details = &part.tries[trie];
                    let reservation = self.memory.reserve(allocation(name.len().saturating_add(details.trie_key.as_ref().map_or(0, String::len)).saturating_add(part.part.as_ref().map_or(0, Vec::len)), 64)?)?;
                    let temporal = details.trie_metadata.as_ref().and_then(|metadata| metadata.temporal_metadata.as_ref());
                    let state = match details.trie_state { None => Value::Null, Some(state @ 0..=2) => Value::Utf8(["nascent", "live", "garbage"][state as usize].into()), Some(_) => return Err(Anomaly::incorrect("rtdb.node/inspection-trie-state", "Table block contains an unknown trie state")) };
                    let row = row([
                        ("table_name", Value::Utf8(name.clone())), ("block_idx", Value::Utf8(lex_hex(block as u64))), ("level", Value::I32(part.level.unwrap_or_default())),
                        ("recency", part.recency.map_or(Value::Null, |micros| Value::Utf8(edn::date(micros.div_euclid(86_400_000_000))))),
                        ("part", Value::Utf8(edn::vector(part.part.as_deref().unwrap_or_default().iter().map(u8::to_string)))),
                        ("trie_key", Value::Utf8(details.trie_key.clone().unwrap_or_default())), ("data_file_size", Value::I64(details.data_file_size.unwrap_or_default())),
                        ("trie_state", state),
                        ("row_count", temporal.map_or(Value::Null, |_| Value::I64(details.trie_metadata.as_ref().and_then(|metadata| metadata.row_count).unwrap_or_default()))),
                        ("temporal_metadata", temporal.map_or(Value::Null, |metadata| Value::Utf8(temporal_message(metadata)))),
                    ]);
                    self.input = Input::Tries { name, block, partitions, reservation: metadata_reservation, partition, trie: trie + 1 };
                    return page(row, vec![reservation], &self.memory).map(Some);
                }
            }
        }
    }
}

impl Inspection {
    async fn read(&mut self, key: &str) -> Result<Option<CacheLease>> {
        match self.database.storage.get(&ObjectKey::new(key)?).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error @ rtdb_storage::Error::MemoryLimit { .. }) => Err(error.into()),
            Err(_) => Ok(None),
        }
    }
}

fn decode_operations(bytes: Option<&[u8]>, memory: &Arc<MemoryBudget>) -> Result<(Value, Option<Arc<MemoryReservation>>)> {
    let Some(bytes) = bytes else { return Ok((Value::Null, None)); };
    let rows = match rtdb_tx::decode_operation_rows(bytes, memory) {
        Ok(rows) => rows,
        Err(error) if error.code == "rtdb/memory-budget-exceeded" => return Err(error),
        Err(_) => return Ok((Value::Utf8("<error decoding tx-ops>".into()), None)),
    };
    let bound = rows.rows().iter().try_fold(allocation(rows.allocation_bytes(), 8)?, |bytes,value| {
        bytes.checked_add(edn::operation_allocation_bound(value)?).ok_or_else(size_error)
    })?;
    let reservation = memory.reserve(bound)?;
    let rendered = rows.rows().iter().map(edn::operation).collect::<Result<Vec<_>>>();
    let value = match rendered { Ok(rows) => edn::vector(rows), Err(_) => "<error decoding tx-ops>".into() };
    Ok((Value::Utf8(value), Some(reservation)))
}

fn source_message(message: Option<&SourceMessage>) -> Result<(&'static str, String)> {
    Ok(match message {
        Some(SourceMessage::LegacyTransaction(_)) => ("legacy-tx", edn::map([("type", ":legacy-tx".into())])),
        Some(SourceMessage::Protobuf(SourceBody::Tx(tx))) => ("tx", edn::map([("type", ":tx".into()), ("default-tz", edn::quoted(tx.default_tz.as_deref().unwrap_or("UTC"))), ("user", tx.user.as_deref().map_or("nil".into(), edn::quoted)), ("system-time", tx.system_time_micros.map_or("nil".into(), edn::instant))])),
        Some(SourceMessage::Protobuf(SourceBody::FlushBlock(block))) => ("flush-block", edn::map([("type", ":flush-block".into()), ("expected-block-idx", block.expected_block_idx.unwrap_or_default().to_string())])),
        Some(SourceMessage::Protobuf(SourceBody::TriesAdded(tries))) => ("tries-added", tries_message(tries, false)),
        Some(SourceMessage::Protobuf(SourceBody::AttachDatabase(attach))) => ("attach-database", edn::map([("type", ":attach-database".into()), ("db-name", edn::quoted(attach.db_name.as_deref().unwrap_or_default())), ("config", edn::quoted(&format!("{:?}", attach.config)))])),
        Some(SourceMessage::Protobuf(SourceBody::ArtefactsAdded(artefacts))) => ("artefacts-added", artefacts_message(&ArtefactsMessage::Added(artefacts), false)),
        Some(SourceMessage::Protobuf(SourceBody::ArtefactsRemoved(artefacts))) => ("artefacts-removed", artefacts_message(&ArtefactsMessage::Removed(artefacts), false)),
        Some(SourceMessage::Protobuf(SourceBody::DetachDatabase(detach))) => ("detach-database", edn::map([("type", ":detach-database".into()), ("db-name", edn::quoted(detach.db_name.as_deref().unwrap_or_default()))])),
        None => ("unknown", edn::map([("type", ":unknown".into())])),
    })
}
fn replica_message(message: &ReplicaBody) -> Result<(&'static str, String)> {
    Ok(match message {
        ReplicaBody::ResolvedTx(tx) => {
            let error = tx.error.as_deref().filter(|bytes| !bytes.is_empty()).map(rtdb_format::transit::decode_anomaly).transpose()?;
            let mut fields = vec![("type", ":resolved-tx".into()), ("tx-id", tx.tx_id.unwrap_or_default().to_string()), ("system-time", edn::instant(tx.system_time_micros.unwrap_or_default())),
                ("committed", tx.committed.unwrap_or_default().to_string()), ("error", error.as_ref().map_or("nil".into(), |error| edn::quoted(&error.to_string()))), ("table-names", edn::vector(tx.table_data.keys().map(|name| edn::quoted(name))))];
            if let Some(op) = &tx.db_op { let (op, name) = match op { proto::resolved_tx::DbOp::AttachDatabase(attach) => (":attach", attach.db_name.as_deref()), proto::resolved_tx::DbOp::DetachDatabase(detach) => (":detach", detach.db_name.as_deref()) }; fields.push(("db-op", edn::map([("op", op.into()), ("db-name", edn::quoted(name.unwrap_or_default()))]))); }
            ("resolved-tx", edn::map(fields))
        }
        ReplicaBody::TriesAdded(tries) => ("tries-added", tries_message(tries, true)),
        ReplicaBody::BlockBoundary(block) => ("block-boundary", edn::map([("type", ":block-boundary".into()), ("block-index", block.block_index.unwrap_or_default().to_string()), ("latest-processed-msg-id", block.latest_processed_msg_id.unwrap_or_default().to_string())])),
        ReplicaBody::BlockUploaded(block) => ("block-uploaded", edn::map([("type", ":block-uploaded".into()), ("block-index", block.block_index.unwrap_or_default().to_string()), ("latest-processed-msg-id", block.latest_processed_msg_id.unwrap_or_default().to_string()), ("storage-version", block.storage_version.unwrap_or_default().to_string()), ("storage-epoch", block.storage_epoch.unwrap_or_default().to_string()), ("tries", tries(&block.tries))])),
        ReplicaBody::NoOp(_) => ("no-op", edn::map([("type", ":no-op".into())])),
        ReplicaBody::ArtefactsAdded(artefacts) => ("artefacts-added", artefacts_message(&ArtefactsMessage::Added(artefacts), true)),
        ReplicaBody::ArtefactsRemoved(artefacts) => ("artefacts-removed", artefacts_message(&ArtefactsMessage::Removed(artefacts), true)),
        ReplicaBody::TablesRetired(retired) => ("tables-retired", edn::map([("type", ":tables-retired".into()), ("table-names", edn::vector(retired.table_names.iter().map(|name| edn::quoted(name)))), ("as-of", edn::instant(retired.as_of_micros.unwrap_or_default()))])),
        _ => ("unknown", edn::map([("type", ":unknown".into())])),
    })
}
fn tries_message(message: &proto::TriesAdded, replica: bool) -> String {
    let mut fields = vec![("type", ":tries-added".into()), ("storage-version", message.storage_version.unwrap_or_default().to_string()), ("storage-epoch", message.storage_epoch.unwrap_or_default().to_string()), ("tries", tries(&message.tries))];
    if replica { fields.push(("source-msg-id", message.source_msg_id.unwrap_or_default().to_string())); }
    edn::map(fields)
}
enum ArtefactsMessage<'a> { Added(&'a proto::ArtefactsAdded), Removed(&'a proto::ArtefactsRemoved) }

fn artefacts_message(message: &ArtefactsMessage<'_>, replica: bool) -> String {
    let (kind, table_name, storage_version, storage_epoch, entries, source_msg_id) = match message {
        ArtefactsMessage::Added(message) => (":artefacts-added", &message.table_name, message.storage_version, message.storage_epoch, &message.entries, message.source_msg_id),
        ArtefactsMessage::Removed(message) => (":artefacts-removed", &message.table_name, message.storage_version, message.storage_epoch, &message.entries, message.source_msg_id),
    };
    let entries = edn::vector(entries.iter().map(|entry| {
        let record = entry.record.clone().unwrap_or_default();
        edn::map([("trie-key", edn::quoted(entry.trie_key.as_deref().unwrap_or_default())), ("kind", edn::quoted(record.kind())),
            ("definition-id", edn::quoted(record.definition_id())), ("definition-version", record.definition_version().to_string())])
    }));
    let mut fields = vec![("type", kind.into()), ("table-name", edn::quoted(table_name.as_deref().unwrap_or_default())),
        ("storage-version", storage_version.unwrap_or_default().to_string()), ("storage-epoch", storage_epoch.unwrap_or_default().to_string()), ("entries", entries)];
    if replica { fields.push(("source-msg-id", source_msg_id.unwrap_or_default().to_string())); }
    edn::map(fields)
}
fn tries(tries: &[proto::TrieDetails]) -> String { edn::vector(tries.iter().map(|trie| edn::map([("table-name", edn::quoted(trie.table_name.as_deref().unwrap_or_default())), ("trie-key", edn::quoted(trie.trie_key.as_deref().unwrap_or_default())), ("data-file-size", trie.data_file_size.unwrap_or_default().to_string())]))) }
fn temporal_message(metadata: &proto::TemporalMetadata) -> String {
    edn::map([("min-valid-from", edn::instant(metadata.min_valid_from.unwrap_or(i64::MAX))), ("max-valid-from", edn::instant(metadata.max_valid_from.unwrap_or(i64::MIN))),
        ("min-valid-to", edn::instant(metadata.min_valid_to.unwrap_or(i64::MAX))), ("max-valid-to", edn::instant(metadata.max_valid_to.unwrap_or(i64::MIN))),
        ("min-system-from", edn::instant(metadata.min_system_from.unwrap_or(i64::MAX))), ("max-system-from", edn::instant(metadata.max_system_from.unwrap_or(i64::MIN)))])
}
fn partitions(metadata: &TableBlock) -> Result<Vec<rtdb_format::block::proto::Partition>> {
    if !metadata.partitions().is_empty() { return Ok(metadata.partitions().to_vec()); }
    let mut groups = std::collections::BTreeMap::new();
    for details in &metadata.as_proto().tries {
        let key: TrieKey = details.trie_key.as_deref().unwrap_or_default().parse()?;
        let recency = key.recency().map(|date| date.epoch_day().checked_mul(86_400_000_000).ok_or_else(size_error)).transpose()?;
        groups.entry((key.level(), recency, key.part().to_vec())).or_insert_with(Vec::new).push(details.clone());
    }
    groups.into_iter().map(|((level, recency, part), tries)| Ok(rtdb_format::block::proto::Partition {
        level: Some(i32::try_from(level).map_err(|_| size_error())?), recency, part: Some(part), tries, max_block_index: None,
    })).collect()
}
fn page(row: InspectionRow, reservations: Vec<Arc<MemoryReservation>>, memory: &Arc<MemoryBudget>) -> Result<CatalogRows<InspectionRow>> {
    let retained = memory.reserve(Value::document_retained_bytes(&row)?.checked_add(512).ok_or_else(size_error)?)?;
    drop(reservations);
    pages(vec![row], vec![retained])
}
fn pages(rows: Vec<InspectionRow>, reservations: Vec<Arc<MemoryReservation>>) -> Result<CatalogRows<InspectionRow>> {
    let retained = rows.iter().try_fold(0usize, |bytes, row| bytes.checked_add(Value::document_retained_bytes(row)?).ok_or_else(size_error))?;
    let reserved = reservations.iter().try_fold(0usize, |bytes, reservation| bytes.checked_add(reservation.bytes()).ok_or_else(size_error))?;
    if retained > reserved { return Err(size_error()); }
    Ok(CatalogRows::new(rows, Arc::new(reservations)))
}
fn row<const N: usize>(fields: [(&str, Value); N]) -> InspectionRow { fields.into_iter().map(|(name, value)| (name.into(), value)).collect() }
fn timestamp(value: i64) -> Value { Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
fn allocation(bytes: usize, multiplier: usize) -> Result<usize> { bytes.checked_mul(multiplier).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(size_error) }
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.node/inspection-allocation", "Inspection allocation exceeds its reservation or addressable memory") }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Node, NodeOptions, NodeServices};
    use rtdb_format::block::proto::{Block, TableBlock as TableBlockProto};
    use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogUser, CatalogUsers};
    use prost::Message;

    struct Services;
    impl rtdb_tx::TransactionAuthorization for Services { fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) } }
    impl rtdb_maintenance::CollectionObserver for Services { fn table_failed(&self, _: Option<&rtdb_types::TableRef>, _: &Anomaly) { panic!() } }
    impl CatalogUsers for Services {
        fn users(&self) -> Result<CatalogRows<CatalogUser>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) }
        fn is_superuser(&self, _: &str) -> bool { false }
    }
    impl CatalogMetrics for Services { fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) } }

    #[tokio::test]
    async fn block_tables_cursor_stops_at_the_reference_empty_1024_candidate_window() {
        let services = NodeServices::local(Arc::new(Services), Arc::new(Services), Arc::new(Services), Arc::new(Services));
        let (node, _driver) = Node::open(NodeOptions::default(), services).await.unwrap();
        let database = node.catalog().database("xtdb").unwrap();
        let catalog = BlockCatalog::from_proto(Block { block_index: Some(1024), ..Default::default() }).unwrap();
        database.storage.object_store().put(&ObjectKey::new(block_key(BlockIndex(1024)).unwrap()).unwrap(), catalog.encode().into()).await.unwrap();
        let mut cursor = open(Arc::clone(&database), InspectionRequest::BlockFiles { from: 0, to: 2048 }, Arc::clone(node.memory())).await.unwrap();
        assert!(cursor.next().await.unwrap().is_none());
        let mut cursor = open(database, InspectionRequest::BlockFiles { from: 1024, to: 2048 }, Arc::clone(node.memory())).await.unwrap();
        let page = cursor.next().await.unwrap().unwrap();
        assert_eq!(page.rows().len(), 1);
        assert_eq!(page.rows()[0]["block_idx"], Value::Utf8(lex_hex(1024)));
        assert!(cursor.next().await.unwrap().is_none());
        node.shutdown();
    }

    #[test]
    fn table_block_file_tries_groups_legacy_tries_and_retains_recency_as_micros() {
        let date = rtdb_types::CivilDate::from_ymd(2024, 1, 1).unwrap();
        let key = TrieKey::new(2, Some(date), vec![1, 3], BlockIndex(10)).unwrap();
        let wire = TableBlockProto { row_count: Some(1), tries: vec![proto::TrieDetails { trie_key: Some(key.to_string()), ..Default::default() }], ..Default::default() };
        let metadata = TableBlock::decode(&wire.encode_to_vec()).unwrap();
        let partitions = partitions(&metadata).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].level, Some(2));
        assert_eq!(partitions[0].part, Some(vec![1, 3]));
        assert_eq!(partitions[0].recency, Some(1_704_067_200_000_000));
    }

    #[test]
    fn legacy_partition_inspection_supports_wide_recency_and_checks_midnight_overflow() {
        let date = rtdb_types::CivilDate::from_ymd(280_000, 1, 3).unwrap();
        let key = TrieKey::new(2, Some(date), vec![], BlockIndex(0)).unwrap();
        let wire = TableBlockProto { tries: vec![proto::TrieDetails { trie_key: Some(key.to_string()), ..Default::default() }], ..Default::default() };
        let metadata = TableBlock::decode(&wire.encode_to_vec()).unwrap();
        assert_eq!(partitions(&metadata).unwrap()[0].recency, Some(date.epoch_day() * 86_400_000_000));
        let wire = TableBlockProto { tries: vec![proto::TrieDetails { trie_key: Some("l02-r9999999991231-b00".into()), ..Default::default() }], ..Default::default() };
        let metadata = TableBlock::decode(&wire.encode_to_vec()).unwrap();
        assert!(partitions(&metadata).is_err());
    }
}
