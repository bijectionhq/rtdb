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
        let mut catalog = Self::from_proto(pb::Block::decode(bytes).map_err(invalid)?)?;
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

    pub fn encode(&self) -> Vec<u8> { self.wire.encode_to_vec() }
    /// Binds the catalog to the exact bytes that the writer will publish.
    pub fn encode_for_publication(&mut self) -> Vec<u8> {
        let bytes = self.encode();
        self.publication_sha256 = Some(publication_digest(&bytes));
        bytes
    }
    pub fn publication_sha256(&self) -> Option<[u8; 32]> { self.publication_sha256 }
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
        self.wire.table_block_manifest.is_none() || self.table_block_digest(oid) == Some(digest.as_slice())
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

impl TrieDetails {
    pub fn encoded_size(&self) -> usize { self.encoded_len() }

    /// The objects of this trie that a checkpoint holding it preserves.
    pub fn checkpoint_files(&self, slug: &TableSlug, contents: CheckpointContents) -> Result<BTreeSet<String>> {
        let state = TrieState::try_from(self.trie_state.unwrap_or_default())
            .map_err(|_| invalid("Unknown trie state in checkpoint"))?;
        let trie: TrieKey = self.trie_key.as_deref().unwrap_or_default().parse()?;
        let mut files = BTreeSet::new();
        if state == TrieState::Garbage && (contents == CheckpointContents::Primary || trie.level() != 0) { return Ok(files); }
        files.insert(slug.data_key(&trie));
        files.insert(slug.metadata_key(&trie));
        if contents == CheckpointContents::Replay && state != TrieState::Garbage {
            for record in &self.artefacts {
                if let Some(path) = replay_artefact_file(slug, &trie, record)? { files.insert(path); }
            }
        }
        Ok(files)
    }
}

/// Resolves an installed artefact record to the file opened by its replayed catalog.
pub fn replay_artefact_file(
    slug: &TableSlug, trie: &TrieKey, record: &crate::log::proto::ArtefactRecord,
) -> Result<Option<String>> {
    if record.kind() == crate::search_manifest::RECORD_KIND {
        let change = crate::log::proto::SearchRunChange::from_artefact(record)?;
        if change.retire_only() { return Ok(None); }
        let run = change.run.ok_or_else(|| invalid("Search publication has no replay run"))?;
        return Ok(Some(run.object_key(slug)?));
    }
    Ok(Some(slug.recorded_artefact_key(trie, record)?))
}

impl TableBlock {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let wire = pb::TableBlock::decode(bytes).map_err(invalid)?;
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

    /// `types` supplies the Arrow schema's field types in their first-seen leg order, as the reference's block schema keeps them;
    /// the column metadata orders union legs by name. A column absent from `types` takes its metadata's type.
    pub fn from_parts(columns: BTreeMap<String, cpb::ColumnMeta>, types: &rtdb_types::StructFields<rtdb_types::LogicalType>, row_count: i64, partitions: Vec<pb::Partition>) -> Result<Self> {
        if row_count < 0 { return Err(invalid("Table row count is negative")); }
        let schema = Schema::new(columns.iter().map(|(name, column)| {
            let ty = match types.get(name) { Some(ty) => ty.clone(), None => crate::columns::logical_type(column)? };
            rtdb_columnar::to_field(name, &ty)
        }).collect::<Result<Vec<_>>>()?);
        let arrow_schema = crate::ipc::write_schema_message(&schema)?;
        let column_name_to_hll = columns.iter().filter_map(|(name, column)| column.hll.as_ref().map(|hll| (name.clone(), hll.clone()))).collect();
        Ok(Self { wire: Arc::new(pb::TableBlock { arrow_schema: Some(arrow_schema), row_count: Some(row_count), tries: Vec::new(), column_name_to_hll, partitions, columns, ..Default::default() }), charge: None })
    }

    pub fn encoded_len(&self) -> usize { self.wire.encoded_len() }
    pub fn encode(&self) -> Vec<u8> { self.wire.encode_to_vec() }
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
        Self { wire: Arc::new(wire), charge: self.charge.clone() }
    }

    pub fn checkpoint_trie_files(&self, entry: &TableEntry, contents: CheckpointContents) -> Result<BTreeSet<String>> {
        let mut files = BTreeSet::new();
        for detail in self.trie_details() {
            if entry.table.stores_expiring_transaction_metadata()
                && detail.trie_state == Some(TrieState::Garbage as i32)
            {
                continue;
            }
            files.append(&mut detail.checkpoint_files(&entry.slug, contents)?);
        }
        if contents == CheckpointContents::Replay {
            for run in &self.wire.search_runs { files.insert(run.object_key(&entry.slug)?); }
        }
        Ok(files)
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

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-catalog", error.to_string())
}
