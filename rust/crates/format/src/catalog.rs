use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_schema::Schema;
use prost::Message;
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Anomaly, BlockIndex, LogicalType, ReplicaPosition, Result, SourcePosition, TableRef, Term, TermSequence, Timestamp, TransactionKey, TxId};

use crate::generated::xtdb::{block::proto as pb, cols::proto as cpb, database::proto::DatabaseConfig};
use crate::keys::TableSlug;

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
    charge: Option<Arc<MemoryReservation>>,
}

impl PartialEq for BlockCatalog { fn eq(&self, other: &Self) -> bool { self.wire == other.wire } }

impl BlockCatalog {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Self::from_proto(pb::Block::decode(bytes).map_err(invalid)?)
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
        let catalog = Self { wire: Arc::new(wire), charge: None };
        catalog.table_entries()?;
        Ok(catalog)
    }

    pub fn encode(&self) -> Vec<u8> { self.wire.encode_to_vec() }
    pub fn as_proto(&self) -> &pb::Block { &self.wire }
    pub fn block_index(&self) -> BlockIndex { BlockIndex(self.wire.block_index.unwrap_or_default()) }
    pub fn boundary_term(&self) -> Term { Term(self.wire.term_id.unwrap_or_default()) }
    pub fn boundary_term_sequence(&self) -> Option<TermSequence> { self.wire.boundary_term_seq.map(TermSequence) }
    pub fn boundary_replica_position(&self) -> Option<ReplicaPosition> { self.wire.boundary_replica_msg_id.map(ReplicaPosition) }
    pub fn external_source_token(&self) -> Option<&[u8]> { self.wire.external_source_token.as_deref() }
    pub fn secondary_databases(&self) -> &BTreeMap<String, DatabaseConfig> { &self.wire.secondary_databases }

    pub fn latest_completed_tx(&self) -> Option<TransactionKey> {
        self.wire.latest_completed_tx.as_ref().map(|tx| TransactionKey {
            tx_id: TxId(tx.tx_id.unwrap_or_default()), system_time: Timestamp(tx.system_time.unwrap_or_default()),
        })
    }

    pub fn latest_source_position(&self) -> Option<SourcePosition> {
        self.wire.latest_processed_msg_id.map(SourcePosition)
            .or_else(|| self.latest_completed_tx().map(|tx| SourcePosition(tx.tx_id.0)))
    }

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

pub fn legacy_table_ref(name: &str) -> Result<TableRef> {
    let (schema, table) = name.split_once('/').unwrap_or(("public", name));
    if schema.is_empty() || table.is_empty() || table.contains('/') {
        return Err(invalid(format!("Invalid recorded table name: {name}")));
    }
    Ok(TableRef::new(schema, table))
}

#[derive(Clone, Debug)]
pub struct TableBlock {
    wire: Arc<pb::TableBlock>,
    charge: Option<Arc<MemoryReservation>>,
}

impl PartialEq for TableBlock { fn eq(&self, other: &Self) -> bool { self.wire == other.wire } }

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

    pub fn from_parts(columns: BTreeMap<String, cpb::ColumnMeta>, row_count: i64, partitions: Vec<pb::Partition>) -> Result<Self> {
        if row_count < 0 { return Err(invalid("Table row count is negative")); }
        let schema = Schema::new(columns.iter().map(|(name, column)| rtdb_columnar::to_field(name, &crate::columns::logical_type(column)?)).collect::<Result<Vec<_>>>()?);
        let arrow_schema = crate::ipc::write_schema_message(&schema)?;
        let column_name_to_hll = columns.iter().filter_map(|(name, column)| column.hll.as_ref().map(|hll| (name.clone(), hll.clone()))).collect();
        Ok(Self { wire: Arc::new(pb::TableBlock { arrow_schema: Some(arrow_schema), row_count: Some(row_count), tries: Vec::new(), column_name_to_hll, partitions, columns }), charge: None })
    }

    pub fn encode(&self) -> Vec<u8> { self.wire.encode_to_vec() }
    pub fn as_proto(&self) -> &pb::TableBlock { &self.wire }
    pub fn row_count(&self) -> i64 { self.wire.row_count.unwrap_or_default() }
    pub fn partitions(&self) -> &[pb::Partition] { &self.wire.partitions }

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
