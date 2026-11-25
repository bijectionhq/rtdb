use std::sync::Arc;

use async_trait::async_trait;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, builder::{Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder}};
use arrow_schema::{Field, Schema};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::{catalog::{BlockCatalog, TableBlock}, envelope::{ReplicaBody, SourceBody, SourceMessage}, keys::{TableSlug, TrieKey, block_key, lex_hex}, log::proto};
use rtdb_plan::{CatalogInspection, CatalogRows, InspectionRequest, InspectionRow};
use rtdb_storage::{ObjectKey, ReadReservation, cache::CacheLease, log::LogRange};
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result, SourcePosition};

use crate::{Database, inspection_render as edn};

enum Input {
    Log { range: Box<dyn LogRange>, source: bool, tx_ops: bool },
    Blocks { next: i64, end: i64, window_end: i64, found: bool },
    Table { name: String, block: i64, tries: bool },
    Tries { name: String, block: i64, partitions: Vec<rtdb_format::block::proto::Partition>, reservation: Arc<MemoryReservation>, partition: usize, trie: usize },
    Done,
}

struct Inspection { database: Arc<Database>, memory: Arc<MemoryBudget>, input: Input }

pub(crate) async fn open(database: Arc<Database>, request: InspectionRequest, memory: Arc<MemoryBudget>) -> Result<Box<dyn CatalogInspection>> {
    if database.object_store().local_erasure_custody().is_some() {
        return Err(Anomaly::unsupported("rtdb/erasure-inspection",
            "An enrolled Local erasure residence serves data through its captured catalog"));
    }
    let input = match request {
        InspectionRequest::SourceLog { from, to, tx_ops } => Input::Log { range: database.logs.source_range(SourcePosition(from), SourcePosition(to)).await?, source: true, tx_ops },
        InspectionRequest::ReplicaLog { from, to } => Input::Log { range: database.logs.replica_range(ReplicaPosition(from), ReplicaPosition(to)).await?, source: false, tx_ops: false },
        InspectionRequest::BlockFiles { from, to } => Input::Blocks { next: from, end: to, window_end: from.saturating_add(1024).min(to), found: false },
        InspectionRequest::TableBlock { table, block, tries } => Input::Table { name: table, block, tries },
    };
    Ok(Box::new(Inspection { database, memory, input }))
}

#[async_trait]
impl CatalogInspection for Inspection {
    async fn next(&mut self) -> Result<Option<CatalogRows<InspectionRow>>> {
        let Some(batch) = self.next_arrow(1).await? else { return Ok(None); };
        let mut bound = 4096usize;
        for (field, array) in batch.batch().schema_ref().fields().iter().zip(batch.batch().columns()) {
            bound = bound.checked_add(512).and_then(|n| n.checked_add(field.name().len())).ok_or_else(size_error)?;
            bound = bound.checked_add(rtdb_columnar::estimate_read_value_bytes(field, array.as_ref(), 0)?).ok_or_else(size_error)?;
        }
        let reservation = self.memory.reserve(bound)?;
        let row = batch.batch().schema_ref().fields().iter().zip(batch.batch().columns()).map(|(field, array)| {
            Ok((field.name().clone(), rtdb_columnar::read_value(field, array.as_ref(), 0)?))
        }).collect::<Result<InspectionRow>>()?;
        Ok(Some(CatalogRows::new(vec![row], reservation)))
    }

    fn supports_arrow(&self) -> bool { true }

    async fn next_arrow(&mut self, limit: usize) -> Result<Option<OwnedBatch>> {
        self.read_arrow(limit, None).await
    }

    async fn next_arrow_projected(&mut self, limit: usize, columns: &[rtdb_plan::Field]) -> Result<Option<OwnedBatch>> {
        self.read_arrow(limit, Some(columns)).await
    }
}

impl Inspection {
    async fn read_arrow(&mut self, limit: usize, projection: Option<&[rtdb_plan::Field]>) -> Result<Option<OwnedBatch>> {
        let mut page = InspectionPage::new(&self.memory, projection)?;
        while page.rows < limit {
            match std::mem::replace(&mut self.input, Input::Done) {
                Input::Done => break,
                Input::Log { mut range, source, tx_ops } => {
                    let bound = range.allocation_bound();
                    let reservation = self.memory.reserve(bound)?;
                    let Some(record) = range.next(ReadReservation::new(bound, reservation)).await? else { break; };
                    let reservation = self.memory.reserve(allocation(record.payload.len(), 64)?)?;
                    let position = if source { self.database.logs.source_position(record.metadata)?.0 } else { ReplicaPosition::from_epoch_offset(self.database.logs.replica_epoch, record.metadata.offset.0)?.0 };
                    let log_offset = record.metadata.offset.0;
                    let log_timestamp = record.metadata.timestamp.0;
                    let mut operations = None;
                    let mut reservations = vec![reservation];
                    let (kind, message) = if source {
                        let message = SourceMessage::decode(&record.payload)?;
                        if tx_ops && page.wants("tx_ops") {
                            if let Some(SourceMessage::Protobuf(SourceBody::Tx(tx))) = &message {
                                operations = tx.tx_ops.as_deref().map(|bytes| edn::operations(bytes, &self.memory)).transpose()?;
                            }
                        }
                        source_message(message.as_ref(), page.wants("msg"))
                    } else {
                        let record = self.database.logs.decode_replica(record)?;
                        match record {
                            None => ("unknown", unknown_message(page.wants("msg"))),
                            Some(record) => {
                                let record = self.database.logs.resolve_oversized(record).await?;
                                reservations.push(self.memory.reserve(allocation(record.retained_bytes(), 8)?)?);
                                replica_message(&record.envelope.message, page.wants("msg"))?
                            }
                        }
                    };
                    let cells = [
                        ("msg_id", Cell::I64(Some(position))),
                        ("log_offset", Cell::I64(Some(log_offset))),
                        ("log_timestamp", Cell::Timestamp(Some(log_timestamp))),
                        ("msg_type", Cell::Text(Some(kind))),
                        ("msg", Cell::Text(message.as_deref())),
                    ];
                    if source {
                        page.append(cells.into_iter().chain([("tx_ops", Cell::Text(operations.as_ref().map(edn::OperationText::as_str)))]))?;
                    } else {
                        page.append(cells)?;
                    }
                    self.input = Input::Log { range, source, tx_ops };
                }
                Input::Blocks { mut next, end, mut window_end, mut found } => {
                    if next == window_end {
                        if !found || next >= end { break; }
                        window_end = next.saturating_add(1024).min(end);
                        found = false;
                    }
                    if next >= window_end { break; }
                    let block = next;
                    next += 1;
                    self.input = Input::Blocks { next, end, window_end, found };
                    if next % 1024 == 0 { tokio::task::yield_now().await; }
                    let Some(bytes) = self.read(&block_key(BlockIndex(block))?).await? else { continue; };
                    let _reservation = self.memory.reserve(allocation(bytes.as_slice().len(), 256)?)?;
                    let Ok(catalog) = BlockCatalog::decode(bytes.as_slice()) else { continue; };
                    let proto = catalog.as_proto();
                    let block_idx = page.wants("block_idx").then(|| lex_hex(proto.block_index.unwrap_or_default() as u64));
                    let names = page.wants("table_names").then(|| edn::vector(proto.table_names.iter().map(|name| edn::quoted(name))));
                    page.append([
                        ("block_idx", Cell::Text(block_idx.as_deref())),
                        ("tx_id", Cell::I64(proto.latest_completed_tx.as_ref().map(|key| key.tx_id.unwrap_or_default()))),
                        ("system_time", Cell::Timestamp(proto.latest_completed_tx.as_ref().map(|key| key.system_time.unwrap_or_default()))),
                        ("latest_processed_msg_id", Cell::I64(Some(proto.latest_processed_msg_id.unwrap_or_default()))),
                        ("table_names", Cell::Text(names.as_deref())),
                        ("boundary_replica_msg_id", Cell::I64(proto.boundary_replica_msg_id)),
                        ("file_size", Cell::I64(Some(bytes.as_slice().len() as i64))),
                    ])?;
                    self.input = Input::Blocks { next, end, window_end, found: true };
                }
                Input::Table { name, block, tries } => {
                    let table = rtdb_format::catalog::legacy_table_ref(&name)?;
                    let value = self.database.partition.value();
                    let slug = value.table_catalog().entry(&table).map(|entry| entry.slug.clone()).unwrap_or(TableSlug::for_unrecorded_table(&table)?);
                    let Some(bytes) = self.read(&slug.block_key(BlockIndex(block))?).await? else { return Ok(None); };
                    let reservation = self.memory.reserve(allocation(bytes.as_slice().len(), 256)?)?;
                    let Ok(metadata) = TableBlock::decode_reserved(bytes.as_slice(), Arc::clone(&reservation)) else { return Ok(None); };
                    if tries {
                        self.input = Input::Tries { name, block, partitions: partitions(&metadata)?, reservation, partition: 0, trie: 0 };
                        continue;
                    }
                    let partition_count = if metadata.partitions().is_empty() { partitions(&metadata)?.len() } else { metadata.partitions().len() };
                    let schema = match rtdb_format::ipc::read_schema_message(metadata.as_proto().arrow_schema.as_deref().unwrap_or_default()) { Ok(schema) => schema, Err(_) => return Ok(None) };
                    let fields = table_fields(&schema, page.wants("fields"))?;
                    let hlls = table_hlls(&metadata, page.wants("hlls"))?;
                    let block_idx = page.wants("block_idx").then(|| lex_hex(block as u64));
                    page.append([
                        ("table_name", Cell::Text(Some(&name))), ("block_idx", Cell::Text(block_idx.as_deref())),
                        ("row_count", Cell::I64(Some(metadata.row_count()))), ("fields", Cell::Text(fields.as_deref())),
                        ("hlls", Cell::Text(hlls.as_deref())), ("partition_count", Cell::I32(partition_count as i32)),
                    ])?;
                    break;
                }
                Input::Tries { name, block, partitions, reservation: metadata_reservation, mut partition, mut trie } => {
                    while partition < partitions.len() && trie >= partitions[partition].tries.len() { partition += 1; trie = 0; }
                    let Some(part) = partitions.get(partition) else { break; };
                    let details = &part.tries[trie];
                    let _reservation = self.memory.reserve(allocation(name.len().saturating_add(details.trie_key.as_ref().map_or(0, String::len)).saturating_add(part.part.as_ref().map_or(0, Vec::len)), 64)?)?;
                    let temporal = details.trie_metadata.as_ref().and_then(|metadata| metadata.temporal_metadata.as_ref());
                    let state = match details.trie_state {
                        None => None,
                        Some(state @ 0..=2) => Some(["nascent", "live", "garbage"][state as usize]),
                        Some(_) => return Err(Anomaly::incorrect("rtdb.node/inspection-trie-state", "Table block contains an unknown trie state")),
                    };
                    let block_idx = page.wants("block_idx").then(|| lex_hex(block as u64));
                    let recency = part.recency.filter(|_| page.wants("recency")).map(|micros| edn::date(micros.div_euclid(86_400_000_000)));
                    let part_path = page.wants("part").then(|| edn::vector(part.part.as_deref().unwrap_or_default().iter().map(u8::to_string)));
                    let temporal_text = temporal.filter(|_| page.wants("temporal_metadata")).map(temporal_message);
                    page.append([
                        ("table_name", Cell::Text(Some(&name))), ("block_idx", Cell::Text(block_idx.as_deref())),
                        ("level", Cell::I32(part.level.unwrap_or_default())), ("recency", Cell::Text(recency.as_deref())),
                        ("part", Cell::Text(part_path.as_deref())), ("trie_key", Cell::Text(Some(details.trie_key.as_deref().unwrap_or_default()))),
                        ("data_file_size", Cell::I64(Some(details.data_file_size.unwrap_or_default()))), ("trie_state", Cell::Text(state)),
                        ("row_count", Cell::I64(temporal.map(|_| details.trie_metadata.as_ref().and_then(|metadata| metadata.row_count).unwrap_or_default()))),
                        ("temporal_metadata", Cell::Text(temporal_text.as_deref())),
                    ])?;
                    self.input = Input::Tries { name, block, partitions, reservation: metadata_reservation, partition, trie: trie + 1 };
                }
            }
        }
        page.finish()
    }

    async fn read(&mut self, key: &str) -> Result<Option<CacheLease>> {
        match self.database.storage.get(&ObjectKey::new(key)?).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error @ rtdb_storage::Error::MemoryLimit { .. }) => Err(error.into()),
            Err(_) => Ok(None),
        }
    }
}

fn table_fields(schema: &Schema, render: bool) -> Result<Option<String>> {
    use std::fmt::Write as _;

    let mut text = render.then(|| String::from("{"));
    for (index, field) in schema.fields().iter().enumerate() {
        let logical = rtdb_columnar::from_field(field)?;
        let form = logical.type_form()?;
        if let Some(text) = &mut text {
            if index != 0 { text.push_str(", "); }
            write!(text, "{} {form}", edn::quoted(field.name())).expect("writing to String");
        }
    }
    Ok(text.map(|mut text| { text.push('}'); text }))
}

fn table_hlls(metadata: &TableBlock, render: bool) -> Result<Option<String>> {
    use std::fmt::Write as _;

    let mut text = render.then(|| String::from("{"));
    for (index, (name, bytes)) in metadata.as_proto().column_name_to_hll.iter().enumerate() {
        let hll = rtdb_format::hll::Hll::from_bytes(bytes.clone())?;
        if let Some(text) = &mut text {
            if index != 0 { text.push_str(", "); }
            write!(text, "{} {}", edn::quoted(name), hll.estimate() as i64).expect("writing to String");
        }
    }
    Ok(text.map(|mut text| { text.push('}'); text }))
}

fn unknown_message(render: bool) -> Option<String> {
    render.then(|| "{:type :unknown}".into())
}

fn source_message(message: Option<&SourceMessage>, render: bool) -> (&'static str, Option<String>) {
    match message {
        Some(SourceMessage::LegacyTransaction(_)) => ("legacy-tx", render.then(|| "{:type :legacy-tx}".into())),
        Some(SourceMessage::Protobuf(SourceBody::Tx(tx))) => ("tx", render.then(|| edn::map([
            ("type", ":tx".into()),
            ("default-tz", edn::quoted(tx.default_tz.as_deref().unwrap_or("UTC"))),
            ("user", tx.user.as_deref().map_or("nil".into(), edn::quoted)),
        ]))),
        Some(SourceMessage::Protobuf(SourceBody::FlushBlock(block))) => ("flush-block", render.then(|| edn::map([
            ("type", ":flush-block".into()), ("expected-block-idx", block.expected_block_idx.unwrap_or_default().to_string()),
        ]))),
        Some(SourceMessage::Protobuf(SourceBody::TriesAdded(tries))) => ("tries-added", render.then(|| tries_message(tries, false))),
        Some(SourceMessage::Protobuf(SourceBody::AttachDatabase(attach))) => ("attach-database", render.then(|| edn::map([
            ("type", ":attach-database".into()), ("db-name", edn::quoted(attach.db_name.as_deref().unwrap_or_default())),
            ("config", edn::quoted(&format!("{:?}", attach.config))),
        ]))),
        Some(SourceMessage::Protobuf(SourceBody::ArtefactsAdded(artefacts))) => ("artefacts-added", render.then(|| artefacts_message(&ArtefactsMessage::Added(artefacts), false))),
        Some(SourceMessage::Protobuf(SourceBody::ArtefactsRemoved(artefacts))) => ("artefacts-removed", render.then(|| artefacts_message(&ArtefactsMessage::Removed(artefacts), false))),
        Some(SourceMessage::Protobuf(SourceBody::DetachDatabase(detach))) => ("detach-database", render.then(|| edn::map([
            ("type", ":detach-database".into()), ("db-name", edn::quoted(detach.db_name.as_deref().unwrap_or_default())),
        ]))),
        Some(SourceMessage::Protobuf(SourceBody::ServingTransact(_))) => ("serving-transact", render.then(|| "{:type :serving-transact}".into())),
        None => ("unknown", unknown_message(render)),
    }
}

fn replica_message(message: &ReplicaBody, render: bool) -> Result<(&'static str, Option<String>)> {
    Ok(match message {
        ReplicaBody::ResolvedTx(tx) => {
            let error = tx.error.as_deref().filter(|bytes| !bytes.is_empty()).map(rtdb_format::transit::decode_anomaly).transpose()?;
            ("resolved-tx", render.then(|| {
                let mut fields = vec![
                    ("type", ":resolved-tx".into()), ("tx-id", tx.tx_id.unwrap_or_default().to_string()),
                    ("system-time", edn::instant(tx.system_time_micros.unwrap_or_default())),
                    ("committed", tx.committed.unwrap_or_default().to_string()),
                    ("error", error.as_ref().map_or("nil".into(), |error| edn::quoted(&error.to_string()))),
                    ("table-names", edn::vector(tx.table_data.keys().map(|name| edn::quoted(name)))),
                ];
                if let Some(op) = &tx.db_op {
                    let (op, name) = match op {
                        proto::resolved_tx::DbOp::AttachDatabase(attach) => (":attach", attach.db_name.as_deref()),
                        proto::resolved_tx::DbOp::DetachDatabase(detach) => (":detach", detach.db_name.as_deref()),
                    };
                    fields.push(("db-op", edn::map([("op", op.into()), ("db-name", edn::quoted(name.unwrap_or_default()))])));
                }
                edn::map(fields)
            }))
        }
        ReplicaBody::TriesAdded(tries) => ("tries-added", render.then(|| tries_message(tries, true))),
        ReplicaBody::BlockBoundary(block) => ("block-boundary", render.then(|| edn::map([
            ("type", ":block-boundary".into()), ("block-index", block.block_index.unwrap_or_default().to_string()),
            ("latest-processed-msg-id", block.latest_processed_msg_id.unwrap_or_default().to_string()),
        ]))),
        ReplicaBody::BlockUploaded(block) => ("block-uploaded", render.then(|| edn::map([
            ("type", ":block-uploaded".into()), ("block-index", block.block_index.unwrap_or_default().to_string()),
            ("latest-processed-msg-id", block.latest_processed_msg_id.unwrap_or_default().to_string()),
            ("storage-version", block.storage_version.unwrap_or_default().to_string()),
            ("storage-epoch", block.storage_epoch.unwrap_or_default().to_string()), ("tries", tries(&block.tries)),
        ]))),
        ReplicaBody::NoOp(_) => ("no-op", render.then(|| "{:type :no-op}".into())),
        ReplicaBody::ArtefactsAdded(artefacts) => ("artefacts-added", render.then(|| artefacts_message(&ArtefactsMessage::Added(artefacts), true))),
        ReplicaBody::ArtefactsRemoved(artefacts) => ("artefacts-removed", render.then(|| artefacts_message(&ArtefactsMessage::Removed(artefacts), true))),
        ReplicaBody::TablesRetired(retired) => ("tables-retired", render.then(|| edn::map([
            ("type", ":tables-retired".into()),
            ("table-names", edn::vector(retired.table_names.iter().map(|name| edn::quoted(name)))),
            ("as-of", edn::instant(retired.as_of_micros.unwrap_or_default())),
        ]))),
        _ => ("unknown", unknown_message(render)),
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
enum Cell<'a> { Text(Option<&'a str>), I32(i32), I64(Option<i64>), Timestamp(Option<i64>) }

enum Column {
    Text(StringBuilder),
    I32(Int32Builder),
    I64(Int64Builder),
    Timestamp(TimestampMicrosecondBuilder),
}

impl Column {
    fn for_cell(cell: &Cell<'_>) -> Self {
        match cell {
            Cell::Text(_) => Self::Text(StringBuilder::with_capacity(0, 0)),
            Cell::I32(_) => Self::I32(Int32Builder::with_capacity(0)),
            Cell::I64(_) => Self::I64(Int64Builder::with_capacity(0)),
            Cell::Timestamp(_) => Self::Timestamp(TimestampMicrosecondBuilder::with_capacity(0).with_timezone("UTC")),
        }
    }

    fn append(&mut self, cell: Cell<'_>) -> Result<()> {
        match (self, cell) {
            (Self::Text(builder), Cell::Text(value)) => {
                let end = builder.values_slice().len().checked_add(value.map_or(0, str::len)).ok_or_else(size_error)?;
                i32::try_from(end).map_err(|_| size_error())?;
                builder.append_option(value);
            }
            (Self::I32(builder), Cell::I32(value)) => builder.append_value(value),
            (Self::I64(builder), Cell::I64(value)) => builder.append_option(value),
            (Self::Timestamp(builder), Cell::Timestamp(value)) => builder.append_option(value),
            _ => return Err(size_error()),
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Text(mut builder) => Arc::new(builder.finish()),
            Self::I32(mut builder) => Arc::new(builder.finish()),
            Self::I64(mut builder) => Arc::new(builder.finish()),
            Self::Timestamp(mut builder) => Arc::new(builder.finish()),
        }
    }
}

struct InspectionPage<'a> {
    projection: Option<&'a [rtdb_plan::Field]>,
    rows: usize,
    columns: Vec<(&'static str, Column)>,
    reservation: Arc<MemoryReservation>,
}

impl<'a> InspectionPage<'a> {
    fn new(memory: &Arc<MemoryBudget>, projection: Option<&'a [rtdb_plan::Field]>) -> Result<Self> {
        Ok(Self { projection, rows: 0, columns: Vec::new(), reservation: memory.reserve(0)? })
    }

    fn wants(&self, name: &str) -> bool { wants(self.projection, name) }

    fn append<'cell>(&mut self, fields: impl IntoIterator<Item = (&'static str, Cell<'cell>)>) -> Result<()> {
        let lease = Arc::get_mut(&mut self.reservation).expect("An inspection page owns its reservation");
        if self.rows == 0 { lease.grow_to(4096)?; }
        let projection = self.projection;
        for (index, (name, cell)) in fields.into_iter().filter(|(name, _)| wants(projection, name)).enumerate() {
            let payload = match &cell { Cell::Text(Some(value)) => value.len(), _ => 0 };
            let bytes = payload.checked_mul(4).and_then(|size| size.checked_add(512)).ok_or_else(size_error)?;
            lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
            if self.rows == 0 { self.columns.push((name, Column::for_cell(&cell))); }
            let (expected, column) = self.columns.get_mut(index).ok_or_else(size_error)?;
            if *expected != name { return Err(size_error()); }
            column.append(cell)?;
        }
        self.rows += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<Option<OwnedBatch>> {
        if self.rows == 0 { return Ok(None); }
        let (fields, arrays): (Vec<_>, Vec<_>) = self.columns.into_iter().map(|(name, column)| {
            let array = column.finish();
            let field = Field::new(name, array.data_type().clone(), array.null_count() != 0);
            (field, array)
        }).unzip();
        let options = RecordBatchOptions::new().with_row_count(Some(self.rows));
        let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)
            .map_err(|error| Anomaly::fault("rtdb.node/inspection-arrow", error.to_string()))?;
        let bytes = rtdb_columnar::shared_allocation_size(&batch)?.checked_add(4096).ok_or_else(size_error)?;
        let lease = Arc::get_mut(&mut self.reservation).expect("An inspection page owns its reservation");
        if bytes > lease.bytes() { lease.grow_to(bytes)?; } else { lease.shrink_to(bytes)?; }
        OwnedBatch::with_reservation(batch, self.reservation).map(Some)
    }
}
fn wants(projection: Option<&[rtdb_plan::Field]>, name: &str) -> bool {
    projection.is_none_or(|columns| columns.iter().any(|field| field.name == name))
}
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
        assert_eq!(page.rows()[0]["block_idx"], rtdb_types::Value::Utf8(lex_hex(1024)));
        assert!(cursor.next().await.unwrap().is_none());
        node.shutdown();
    }

    #[tokio::test]
    async fn arrow_surface_inspection_block_windows_return_all_records_in_order() {
        let services = NodeServices::local(Arc::new(Services), Arc::new(Services), Arc::new(Services), Arc::new(Services));
        let (node, _driver) = Node::open(NodeOptions::default(), services).await.unwrap();
        let database = node.catalog().database("xtdb").unwrap();
        for index in [0, 1024] {
            let catalog = BlockCatalog::from_proto(Block { block_index: Some(index), ..Default::default() }).unwrap();
            database.storage.object_store().put(&ObjectKey::new(block_key(BlockIndex(index)).unwrap()).unwrap(), catalog.encode().into()).await.unwrap();
        }
        let mut cursor = open(database, InspectionRequest::BlockFiles { from: 0, to: 3072 }, Arc::clone(node.memory())).await.unwrap();
        assert!(cursor.supports_arrow());
        let page = cursor.next_arrow(3).await.unwrap().unwrap();
        let ids = page.batch().column_by_name("block_idx").unwrap().as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(ids.iter().collect::<Vec<_>>(), [Some(lex_hex(0).as_str()), Some(lex_hex(1024).as_str())]);
        let names = page.batch().column_by_name("table_names").unwrap().as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(names.iter().collect::<Vec<_>>(), [Some("[]"), Some("[]")]);
        assert!(cursor.next_arrow(3).await.unwrap().is_none());
        node.shutdown();
    }

    #[test]
    fn arrow_surface_inspection_columns_keep_window_order_and_edn_strings() {
        let memory = MemoryBudget::new(1 << 20);
        let scan_field = |id, name: &str, scalar| rtdb_plan::Field {
            id: rtdb_plan::ColumnId(id), name: name.into(), data_type: rtdb_types::LogicalType::Scalar(scalar),
            qualifier: None, qualified_only: false, unnamed: false,
        };
        let fields = [
            scan_field(0, "msg_id", rtdb_types::ScalarType::I64),
            scan_field(1, "msg", rtdb_types::ScalarType::Utf8),
        ];
        let mut page = InspectionPage::new(&memory, Some(&fields)).unwrap();
        page.append([("msg_id", Cell::I64(Some(7))), ("msg", Cell::Text(Some("{:type :tx}"))), ("tx_ops", Cell::Text(Some("[]")))]).unwrap();
        page.append([("msg_id", Cell::I64(Some(9))), ("msg", Cell::Text(None)), ("tx_ops", Cell::Text(None))]).unwrap();
        let page = page.finish().unwrap().unwrap();
        assert_eq!(page.batch().num_columns(), 2);
        let ids = page.batch().column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        let text = page.batch().column(1).as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(ids.values().as_ref(), &[7, 9]);
        assert_eq!(text.iter().collect::<Vec<_>>(), [Some("{:type :tx}"), None]);
    }

    #[test]
    fn arrow_surface_inspection_preserves_rows_with_an_empty_projection() {
        let memory = MemoryBudget::new(1 << 20);
        let mut page = InspectionPage::new(&memory, Some(&[])).unwrap();
        for id in [7, 9] {
            page.append([("msg_id", Cell::I64(Some(id)))]).unwrap();
        }
        let page = page.finish().unwrap().unwrap();
        assert_eq!(page.batch().num_columns(), 0);
        assert_eq!(page.batch().num_rows(), 2);
    }

    #[test]
    fn unprojected_inspection_values_still_validate_persisted_fields() {
        let schema = Schema::new(vec![Field::new("unsupported", arrow_schema::DataType::UInt8, false)]);
        assert_eq!(table_fields(&schema, false).unwrap_err().code, "xtdb/unsupported-type-rendering");
        let wire = TableBlockProto { column_name_to_hll: [("bad".into(), vec![0; 3])].into(), ..Default::default() };
        let metadata = TableBlock::decode(&wire.encode_to_vec()).unwrap();
        assert_eq!(table_hlls(&metadata, false).unwrap_err().code, "rtdb.format/invalid-hll");
        let error = vec![0xc1];
        let expected = rtdb_format::transit::decode_anomaly(&error).unwrap_err().code;
        let body = ReplicaBody::ResolvedTx(proto::ResolvedTx { error: Some(error.into()), ..Default::default() });
        assert_eq!(replica_message(&body, false).unwrap_err().code, expected);
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
