use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_schema::Schema;
use prost::Message;
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Anomaly, BlockIndex, LogicalType, ReplicaPosition, Result, SourcePosition, TableRef, Term, TermSequence, Timestamp, TransactionKey, TxId};

use crate::generated::xtdb::{block::proto as pb, cols::proto as cpb, database::proto::DatabaseConfig};
use crate::keys::{TableSlug, TrieKey};
use crate::log::proto::{TrieDetails, TrieState};

#[macro_use]
mod checkpoint_files;

macro_rules! each_checkpoint_input {
    ($files:ident, $items:expr, $item:ident => $body:block, [$($spec:tt)*]) => {
        for $item in $items $body
    };
}

pub const FIRST_TABLE_OID: u32 = 16_384;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableEntry {
    pub oid: u32,
    pub table: TableRef,
    pub slug: TableSlug,
}

impl TableEntry {
    pub fn new(oid: u32, table: TableRef) -> Result<Self> {
        let slug = TableSlug::for_unrecorded_table(&table)?;
        Ok(Self { oid, table, slug })
    }

    pub fn from_proto(entry: &pb::TableEntry) -> Result<Self> {
        Ok(Self {
            oid: entry.oid.unwrap_or_default(),
            table: TableRef::new(entry.schema_name.clone().unwrap_or_default(), entry.table_name.clone().unwrap_or_default()),
            slug: TableSlug::recorded(entry.slug.clone().unwrap_or_default())?,
        })
    }

    pub fn to_proto(&self) -> pb::TableEntry {
        pb::TableEntry { oid: Some(self.oid), schema_name: Some(self.table.schema.clone()), table_name: Some(self.table.table.clone()), slug: Some(self.slug.as_str().to_owned()) }
    }
}

#[derive(Clone, Debug)]
pub struct BlockCatalog {
    wire: Arc<pb::Block>,
    publication_sha256: Option<[u8; 32]>,
    charge: Option<Arc<MemoryReservation>>,
}

impl PartialEq for BlockCatalog { fn eq(&self, other: &Self) -> bool { self.wire == other.wire } }

impl BlockCatalog {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut catalog = Self::from_proto(pb::Block::decode(unframed(bytes)).map_err(invalid)?)?;
        catalog.publication_sha256 = Some(publication_digest(bytes));
        Ok(catalog)
    }

    /// The reservation must cover the decoded graph and any derived values that retain its charge.
    pub fn decode_reserved(bytes: &[u8], reservation: Arc<MemoryReservation>) -> Result<Self> {
        let mut catalog = Self::decode(bytes)?;
        catalog.charge = Some(reservation);
        Ok(catalog)
    }

    pub fn retain_charge(&self) -> Option<Arc<MemoryReservation>> { self.charge.clone() }

    pub fn from_proto(wire: pb::Block) -> Result<Self> {
        if wire.block_index.unwrap_or_default() < 0 { return Err(invalid("Block index is negative")); }
        let catalog = Self { wire: Arc::new(wire), charge: None, publication_sha256: None };
        let entries = catalog.table_entries()?;
        if let Some(manifest) = &catalog.wire.table_block_manifest {
            let digests = &manifest.sha256_by_oid;
            if entries.len() != digests.len()
                || entries.iter().any(|entry| digests.get(&entry.oid).is_none_or(|digest| digest.len() != 32))
            {
                return Err(invalid("Table block manifest does not match the checkpoint registry"));
            }
        }
        Ok(catalog)
    }

    /// A catalog that records an RTDB-only feature starts with [crate::XTDB_REFUSAL_PREFIX], which XTDB's parser rejects.
    pub fn encode(&self) -> Vec<u8> { framed(&*self.wire, crate::envelope::block_uses_extension(&self.wire)) }
    /// Binds the catalog to the exact bytes that the writer will publish.
    pub fn encode_for_publication(&mut self) -> Vec<u8> {
        let bytes = self.encode();
        self.publication_sha256 = Some(publication_digest(&bytes));
        bytes
    }
    pub fn publication_sha256(&self) -> Option<[u8; 32]> { self.publication_sha256 }
    pub fn matches_checkpoint_root(&self, expected: [u8; 32], requires_manifest: bool) -> bool {
        crate::certified_checkpoint::matches_catalog(
            expected, self.publication_sha256, requires_manifest, self.wire.table_block_manifest.is_some(),
        )
    }
    pub fn as_proto(&self) -> &pb::Block { &self.wire }
    pub fn block_index(&self) -> BlockIndex { BlockIndex(self.wire.block_index.unwrap_or_default()) }
    pub fn boundary_term(&self) -> Term { Term(self.wire.term_id.unwrap_or_default()) }
    pub fn boundary_term_sequence(&self) -> Option<TermSequence> { self.wire.boundary_term_seq.map(TermSequence) }
    pub fn boundary_replica_position(&self) -> Option<ReplicaPosition> { self.wire.boundary_replica_msg_id.map(ReplicaPosition) }
    pub fn external_source_token(&self) -> Option<&[u8]> { self.wire.external_source_token.as_deref() }
    pub fn secondary_databases(&self) -> &BTreeMap<String, DatabaseConfig> { &self.wire.secondary_databases }
    pub fn table_block_digest(&self, oid: u32) -> Option<&[u8]> {
        self.wire.table_block_manifest.as_ref()?.sha256_by_oid.get(&oid).map(Vec::as_slice)
    }
    pub fn matches_table_block(&self, oid: u32, digest: &[u8; 32]) -> bool {
        self.wire.table_block_manifest.is_none() || crate::certified_checkpoint::matches_table(
            self.table_block_digest(oid).and_then(|expected| expected.try_into().ok()), *digest,
        )
    }

    pub fn latest_completed_tx(&self) -> Option<TransactionKey> {
        self.wire.latest_completed_tx.as_ref().map(|tx| TransactionKey {
            tx_id: TxId(tx.tx_id.unwrap_or_default()), system_time: Timestamp(tx.system_time.unwrap_or_default()),
        })
    }

    pub fn latest_source_position(&self) -> Option<SourcePosition> {
        self.wire.latest_processed_msg_id.map(SourcePosition)
            .or_else(|| self.latest_completed_tx().map(|tx| SourcePosition(tx.tx_id.0)))
    }
    /// The naming epoch of compaction outputs planned from this block's catalog.
    pub fn naming_epoch(&self) -> u64 { self.wire.naming_epoch.unwrap_or(0).max(0) as u64 }

    pub fn table_entries(&self) -> Result<Vec<TableEntry>> {
        let entries = if !self.wire.tables.is_empty() {
            self.wire.tables.iter().map(TableEntry::from_proto).collect::<Result<Vec<_>>>()?
        } else {
            let mut names = self.wire.table_names.clone();
            names.sort();
            names.iter().enumerate().map(|(index, name)| {
                let oid = FIRST_TABLE_OID.checked_add(u32::try_from(index).map_err(invalid)?).ok_or_else(|| invalid("Table OID space exhausted"))?;
                TableEntry::new(oid, legacy_table_ref(name)?)
            }).collect::<Result<Vec<_>>>()?
        };
        let mut tables = BTreeSet::new();
        let mut oids = BTreeSet::new();
        for entry in &entries {
            if !tables.insert(entry.table.clone()) || !oids.insert(entry.oid) {
                return Err(invalid("Block catalog contains duplicate table names or OIDs"));
            }
        }
        Ok(entries)
    }
}

pub use crate::publication_identity::same_publication_digest;

/// Digest of the encoded checkpoint root, before encryption or packing.
pub fn publication_digest(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

pub fn legacy_table_ref(name: &str) -> Result<TableRef> {
    let (schema, table) = name.split_once('/').unwrap_or(("public", name));
    if schema.is_empty() || table.is_empty() || table.contains('/') {
        return Err(invalid(format!("Invalid recorded table name: {name}")));
    }
    Ok(TableRef::new(schema, table))
}

/// The dependency set to preserve when copying a checkpoint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointContents {
    /// The primary objects retained by the original archive encoding.
    #[default]
    Primary,
    /// Primary objects, retained L0 inputs, and the artefacts named by usable tries.
    Replay,
}

#[derive(Clone, Debug)]
pub struct TableBlock {
    wire: Arc<pb::TableBlock>,
    charge: Option<Arc<MemoryReservation>>,
}

impl PartialEq for TableBlock { fn eq(&self, other: &Self) -> bool { self.wire == other.wire } }

fn checkpoint_trie_state(detail: &TrieDetails) -> Result<TrieState> {
    TrieState::try_from(detail.trie_state.unwrap_or_default())
        .map_err(|_| invalid("Unknown trie state in checkpoint"))
}

fn checkpoint_trie_key(detail: &TrieDetails) -> Result<TrieKey> {
    detail.trie_key.as_deref().unwrap_or_default().parse()
}

impl TrieDetails {
    pub fn encoded_size(&self) -> usize { self.encoded_len() }

    /// The objects of this trie that a checkpoint holding it preserves.
    pub fn checkpoint_files(&self, slug: &TableSlug, contents: CheckpointContents) -> Result<BTreeSet<String>> {
        let detail = self;
        trie_checkpoint_files!(detail, slug, contents; each_checkpoint_input)
    }
}

/// Resolves an installed artefact record to the file opened by its replayed catalog.
pub fn replay_artefact_file(
    slug: &TableSlug, trie: &TrieKey, record: &crate::log::proto::ArtefactRecord,
) -> Result<Option<String>> {
    if record.kind() == crate::entity_directory::RECORD_KIND || crate::term_directory::sealed_change(record).is_some() { return Ok(None); }
    if record.kind() == crate::search_manifest::RECORD_KIND {
        let change = crate::log::proto::SearchRunChange::from_artefact(record)?;
        if change.retire_only() { return Ok(None); }
        let run = change.run.ok_or_else(|| invalid("Search publication has no replay run"))?;
        return Ok(Some(run.object_key(slug)?));
    }
    Ok(Some(slug.recorded_artefact_key(trie, record)?))
}

pub const DECLARED_COLUMNS: &str = "io.rtdb.sql.declared-columns.v1";

pub fn schema_with_declarations(
    schema: Schema, declarations: &rtdb_types::StructFields<rtdb_types::LogicalType>,
) -> Result<Schema> {
    if schema.metadata().contains_key(DECLARED_COLUMNS) {
        return Err(invalid("The SQL declaration metadata is reserved"));
    }
    if declarations.is_empty() { return Ok(schema); }
    let mut metadata = schema.metadata().clone();
    metadata.insert(DECLARED_COLUMNS.into(), serde_json::to_string(declarations).map_err(invalid)?);
    Ok(schema.with_metadata(metadata))
}

pub fn schema_declarations(
    schema: &Schema,
) -> Result<rtdb_types::StructFields<rtdb_types::LogicalType>> {
    schema.metadata().get(DECLARED_COLUMNS)
        .map(|declarations| serde_json::from_str(declarations).map_err(invalid))
        .transpose().map(Option::unwrap_or_default)
}

impl TableBlock {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let wire = pb::TableBlock::decode(unframed(bytes)).map_err(invalid)?;
        if wire.row_count.unwrap_or_default() < 0 { return Err(invalid("Table row count is negative")); }
        Ok(Self { wire: Arc::new(wire), charge: None })
    }

    /// The reservation must cover the decoded graph and any derived values that retain its charge.
    pub fn decode_reserved(bytes: &[u8], reservation: Arc<MemoryReservation>) -> Result<Self> {
        let mut block = Self::decode(bytes)?;
        block.charge = Some(reservation);
        Ok(block)
    }

    pub fn retain_charge(&self) -> Option<Arc<MemoryReservation>> { self.charge.clone() }

    /// `types` supplies the legacy Arrow schema's field and union-leg order.
    /// A column absent from `types` is appended with its metadata type in name order.
    pub fn from_parts(columns: BTreeMap<String, cpb::ColumnMeta>, types: &rtdb_types::StructFields<rtdb_types::LogicalType>, row_count: i64, partitions: Vec<pb::Partition>) -> Result<Self> {
        Self::with_declarations(columns, types, &Default::default(), row_count, partitions)
    }

    pub fn with_declarations(
        columns: BTreeMap<String, cpb::ColumnMeta>,
        types: &rtdb_types::StructFields<rtdb_types::LogicalType>,
        declarations: &rtdb_types::StructFields<rtdb_types::LogicalType>,
        row_count: i64,
        partitions: Vec<pb::Partition>,
    ) -> Result<Self> {
        if row_count < 0 { return Err(invalid("Table row count is negative")); }
        let mut fields = Vec::with_capacity(columns.len());
        for (name, ty) in types.iter().filter(|(name, _)| columns.contains_key(*name)) {
            fields.push(rtdb_columnar::to_field(name, ty)?);
        }
        for (name, column) in columns.iter().filter(|(name, _)| !types.contains_key(*name)) {
            fields.push(rtdb_columnar::to_field(name, &crate::columns::logical_type(column)?)?);
        }
        let schema = schema_with_declarations(Schema::new(fields), declarations)?;
        let arrow_schema = crate::ipc::write_java_schema_message(&schema)?;
        let column_name_to_hll = columns.iter().filter_map(|(name, column)| column.hll.as_ref().map(|hll| (name.clone(), hll.clone()))).collect();
        Ok(Self { wire: Arc::new(pb::TableBlock { arrow_schema: Some(arrow_schema), row_count: Some(row_count), tries: Vec::new(), column_name_to_hll, partitions, columns, ..Default::default() }), charge: None })
    }

    pub fn declared_columns(&self) -> Result<rtdb_types::StructFields<rtdb_types::LogicalType>> {
        match self.wire.arrow_schema.as_deref() {
            Some(schema) if !schema.is_empty() => schema_declarations(&crate::ipc::read_schema_message(schema)?),
            _ => Ok(Default::default()),
        }
    }

    pub fn encoded_len(&self) -> usize { self.extension_prefix_len() + self.wire.encoded_len() }
    /// A table block that records an RTDB-only feature starts with [crate::XTDB_REFUSAL_PREFIX], which XTDB's parser rejects.
    pub fn encode(&self) -> Vec<u8> { framed(&*self.wire, self.extension_prefix_len() > 0) }
    fn extension_prefix_len(&self) -> usize {
        if crate::envelope::table_block_uses_extension(&self.wire) { crate::XTDB_REFUSAL_PREFIX.len() } else { 0 }
    }
    pub fn as_proto(&self) -> &pb::TableBlock { &self.wire }
    pub fn row_count(&self) -> i64 { self.wire.row_count.unwrap_or_default() }
    pub fn partitions(&self) -> &[pb::Partition] { &self.wire.partitions }

    pub fn with_retired_as_of(mut self, micros: Option<i64>) -> Self {
        if self.wire.retired_as_of_micros != micros { Arc::make_mut(&mut self.wire).retired_as_of_micros = micros; }
        self
    }

    pub fn with_search_runs(mut self, active: Vec<crate::log::proto::SearchRun>, retired: Vec<crate::log::proto::RetiredSearchRun>) -> Self {
        let wire = Arc::make_mut(&mut self.wire);
        wire.search_runs = active;
        wire.retired_search_runs = retired;
        self
    }

    pub fn with_entity_directory(mut self, active: Option<crate::log::proto::EntityDirectoryRoot>,
        retired: Vec<crate::log::proto::EntityDirectoryRoot>) -> Self {
        let wire = Arc::make_mut(&mut self.wire);
        wire.entity_directory = active;
        wire.retired_entity_directories = retired;
        self
    }

    pub async fn checkpoint_trie_files_with_pages(&self, entry: &TableEntry, contents: CheckpointContents,
        pages: &dyn crate::cow_tree::PageStore) -> Result<BTreeSet<String>> {
        let mut files = self.checkpoint_trie_files(entry, contents)?;
        if contents == CheckpointContents::Replay && let Some(directory) = &self.wire.entity_directory {
            let prefix = crate::entity_directory::prefix(&entry.slug);
            for page in crate::cow_tree::reachable(pages, &directory.reference()?).await? {
                files.insert(crate::cow_tree::page_key(&prefix, page));
            }
        }
        Ok(files)
    }

    pub fn with_term_directories(mut self, roots: Vec<crate::log::proto::TermDirectoryRoot>) -> Self {
        Arc::make_mut(&mut self.wire).term_directories = roots;
        self
    }

    pub fn with_retired_term_directories(mut self, retired: Vec<crate::log::proto::RetiredTermDirectory>) -> Self {
        if self.wire.retired_term_directories != retired { Arc::make_mut(&mut self.wire).retired_term_directories = retired; }
        self
    }

    pub fn trie_details(&self) -> impl Iterator<Item = &TrieDetails> {
        self.wire.partitions.iter().flat_map(|partition| &partition.tries)
            .chain(self.wire.tries.iter().filter(|_| self.wire.partitions.is_empty()))
    }

    /// Detaches the serving trie inventory from retired files and derived artefacts.
    pub fn for_primary_repair(&self) -> Self {
        fn retain_primary(tries: &mut Vec<TrieDetails>) {
            tries.retain(|try_| try_.trie_state != Some(TrieState::Garbage as i32));
            for try_ in tries { try_.artefacts.clear(); }
        }
        let mut wire = self.wire.as_ref().clone();
        retain_primary(&mut wire.tries);
        for partition in &mut wire.partitions { retain_primary(&mut partition.tries); }
        wire.search_runs.clear();
        wire.retired_search_runs.clear();
        wire.entity_directory = None;
        wire.retired_entity_directories.clear();
        wire.term_directories.clear();
        wire.retired_term_directories.clear();
        Self { wire: Arc::new(wire), charge: self.charge.clone() }
    }

    pub fn checkpoint_trie_files(&self, entry: &TableEntry, contents: CheckpointContents) -> Result<BTreeSet<String>> {
        let table = self;
        table_checkpoint_files!(table, entry, contents; each_checkpoint_input)
    }

    pub fn column_types(&self) -> Result<StructFields<LogicalType>> {
        if !self.wire.columns.is_empty() {
            self.wire.columns.iter().map(|(name, column)| Ok((name.clone(), crate::columns::logical_type(column)?))).collect()
        } else {
            let schema = crate::ipc::read_schema_message(self.wire.arrow_schema.as_deref().unwrap_or_default())?;
            schema.fields().iter().map(|field| Ok((field.name().clone(), rtdb_columnar::from_field(field)?))).collect()
        }
    }

    pub fn column_hlls(&self) -> BTreeMap<String, &[u8]> {
        if !self.wire.columns.is_empty() {
            self.wire.columns.iter().filter_map(|(name, column)| column.hll.as_deref().map(|hll| (name.clone(), hll))).collect()
        } else {
            self.wire.column_name_to_hll.iter().map(|(name, hll)| (name.clone(), hll.as_slice())).collect()
        }
    }
}

/// No encoded protobuf message starts with the refusal prefix, so a file that does is a framed one.
fn unframed(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(crate::XTDB_REFUSAL_PREFIX.as_slice()).unwrap_or(bytes)
}

fn framed(message: &impl Message, extension: bool) -> Vec<u8> {
    let prefix: &[u8] = if extension { &crate::XTDB_REFUSAL_PREFIX } else { &[] };
    let mut bytes = Vec::with_capacity(prefix.len() + message.encoded_len());
    bytes.extend_from_slice(prefix);
    message.encode(&mut bytes).expect("a Vec can hold the advertised encoded protobuf length");
    bytes
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-catalog", error.to_string())
}
