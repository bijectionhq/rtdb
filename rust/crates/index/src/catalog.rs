use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_format::{block::proto as block_pb, catalog::{BlockCatalog, TableBlock, TableEntry, FIRST_TABLE_OID}, columns::column_from_type, keys::TableSlug};
use rtdb_types::{Anomaly, BlockIndex, LogicalType, Result, TableRef};

pub fn absent_type(types: &StructFields<LogicalType>) -> LogicalType {
    LogicalType::absent_contribution(types.values().any(|ty| *ty != LogicalType::Nothing))
}

pub fn merge_column_types(left: &StructFields<LogicalType>, right: &StructFields<LogicalType>) -> StructFields<LogicalType> {
    let left_absent = absent_type(left);
    let right_absent = absent_type(right);
    left.keys().chain(right.keys()).map(|name| {
        (name.clone(), left.get(name).unwrap_or(&left_absent).join(right.get(name).unwrap_or(&right_absent)))
    }).collect()
}

/// Joins declared column types into row-derived types; a declaration is not a row, so it adds no absence.
pub fn declare_column_types(rows: &StructFields<LogicalType>, declared: &StructFields<LogicalType>) -> StructFields<LogicalType> {
    let absent = absent_type(rows);
    let mut types = rows.clone();
    for (name, ty) in declared {
        let joined = types.get(name).unwrap_or(&absent).join(ty);
        types.insert(name.clone(), joined);
    }
    types
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableMeta {
    pub column_types: StructFields<LogicalType>,
    pub row_count: i64,
    pub hlls: BTreeMap<String, Vec<u8>>,
}

impl TableMeta {
    pub fn from_block(block: &TableBlock) -> Result<Self> {
        Ok(Self { column_types: block.column_types()?, row_count: block.row_count(),
            hlls: block.column_hlls().into_iter().map(|(name, bytes)| (name, bytes.to_vec())).collect() })
    }
    pub fn fold(&self, delta: &Self) -> Result<Self> {
        if delta.row_count < 0 { return Err(invalid("Negative block row delta")); }
        let mut hlls = self.hlls.clone();
        for (name, bytes) in &delta.hlls {
            hlls.insert(name.clone(), match hlls.get(name) {
                Some(previous) => rtdb_format::hll::combine(previous, bytes)?, None => bytes.clone(),
            });
        }
        Ok(Self { column_types: merge_column_types(&self.column_types, &delta.column_types),
            row_count: self.row_count.checked_add(delta.row_count).ok_or_else(|| invalid("Table row count overflow"))?, hlls })
    }
    pub fn to_block(&self, partitions: Vec<block_pb::Partition>) -> Result<TableBlock> {
        let columns = self.column_types.iter().map(|(name, ty)| Ok((name.clone(), column_from_type(ty, self.hlls.get(name).cloned())?))).collect::<Result<_>>()?;
        TableBlock::from_parts(columns, &self.column_types, self.row_count, partitions)
    }
    pub fn contributed_type(&self, name: &str) -> LogicalType {
        self.column_types.get(name).cloned().unwrap_or_else(|| absent_type(&self.column_types))
    }
}

#[derive(Clone, Debug, Default)]
pub struct TableCatalog {
    block: Option<Arc<BlockCatalog>>,
    _retained_catalogs: Vec<Arc<rtdb_columnar::MemoryReservation>>,
    tables: BTreeMap<TableRef, TableMeta>,
    entries: BTreeMap<TableRef, TableEntry>,
}

impl TableCatalog {
    pub fn load(block: Option<BlockCatalog>, table_blocks: &BTreeMap<TableRef, TableBlock>) -> Result<Self> {
        let entries: BTreeMap<_, _> = block.as_ref().map(BlockCatalog::table_entries).transpose()?.unwrap_or_default().into_iter().map(|entry| (entry.table.clone(), entry)).collect();
        if entries.keys().any(|table| TableRef::owned_schema(&table.schema) && !table.supported_owned_name()) {
            return Err(invalid("Unknown owned relation policy or invalid owned relation name in checkpoint"));
        }
        let expected: BTreeSet<_> = entries.keys().collect();
        let actual: BTreeSet<_> = table_blocks.keys().collect();
        if expected != actual { return Err(invalid("Table blocks do not match the block's recorded registry")); }
        let tables = table_blocks.iter().map(|(table, block)| Ok((table.clone(), TableMeta::from_block(block)?))).collect::<Result<_>>()?;
        Ok(Self { block: block.map(Arc::new), _retained_catalogs: table_blocks.values().filter_map(TableBlock::retain_charge).collect(), tables, entries })
    }
    pub fn block(&self) -> Option<&BlockCatalog> { self.block.as_deref() }
    pub fn block_index(&self) -> Option<BlockIndex> { self.block().map(BlockCatalog::block_index) }
    pub fn tables(&self) -> &BTreeMap<TableRef, TableMeta> { &self.tables }
    pub fn entries(&self) -> impl Iterator<Item = &TableEntry> { self.entries.values() }
    pub fn entry(&self, table: &TableRef) -> Option<&TableEntry> { self.entries.get(table) }
    pub fn metadata(&self, table: &TableRef) -> Option<&TableMeta> { self.tables.get(table) }
    pub fn contributed_type(&self, table: &TableRef, name: &str) -> LogicalType {
        self.tables.get(table).map_or(LogicalType::Nothing, |meta| meta.contributed_type(name))
    }
    pub fn with_entries_for(&self, tables: impl IntoIterator<Item = TableRef>) -> Result<Self> {
        let mut fresh: Vec<_> = tables.into_iter().filter(|table| !self.entries.contains_key(table)).collect::<BTreeSet<_>>().into_iter().collect();
        if fresh.is_empty() { return Ok(self.clone()); }
        fresh.sort_by_key(TableRef::schema_and_table);
        let mut next = self.clone();
        let mut oid = self.entries.values().map(|entry| entry.oid).max().map_or(Ok(FIRST_TABLE_OID), |oid| oid.checked_add(1).ok_or_else(|| invalid("Table OID space exhausted")))?;
        let mut used_slugs: BTreeSet<_> = self.entries.values().map(|entry| entry.slug.clone()).collect();
        for (index, table) in fresh.iter().enumerate() {
            let mut entry = TableEntry::new(oid, table.clone())?;
            if !used_slugs.insert(entry.slug.clone()) {
                let base = entry.slug.as_str();
                for suffix in 1_u64.. {
                    let candidate = TableSlug::recorded(format!("{base}~{suffix}"))?;
                    if used_slugs.insert(candidate.clone()) {
                        entry.slug = candidate;
                        break;
                    }
                }
            }
            next.entries.insert(table.clone(), entry);
            if index + 1 < fresh.len() { oid = oid.checked_add(1).ok_or_else(|| invalid("Table OID space exhausted"))?; }
        }
        Ok(next)
    }
    pub fn with_seeded_table(&self, table: TableRef, columns: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut next = self.with_entries_for([table.clone()])?;
        next.tables.entry(table).or_insert_with(|| TableMeta { column_types: columns.into_iter().map(|name| (name, LogicalType::Nothing)).collect(), ..TableMeta::default() });
        Ok(next)
    }
    pub fn with_system_tables(&self) -> Result<Self> {
        self.with_seeded_table(TableRef::new("xt", "txs"), ["_id", "system_time", "committed", "user_metadata", "error"].map(str::to_owned))?
            .with_seeded_table(TableRef::new("xt", "role_membership"), ["user", "role"].map(str::to_owned))
    }
    pub fn fold(&self, delta: &BTreeMap<TableRef, TableMeta>) -> Result<Self> {
        let mut next = self.with_entries_for(delta.keys().cloned())?;
        for (table, metadata) in delta {
            let merged = match next.tables.get(table) { Some(previous) => previous.fold(metadata)?, None => metadata.clone() };
            next.tables.insert(table.clone(), merged);
        }
        Ok(next)
    }
    pub fn adopt(&self, block: BlockCatalog, delta: &BTreeMap<TableRef, TableMeta>) -> Result<Self> {
        if self.block_index().is_some_and(|previous| previous >= block.block_index()) { return Err(invalid("Block has already been adopted or is stale")); }
        let mut next = self.fold(delta)?;
        let entries = block.table_entries()?;
        let recorded: BTreeSet<_> = entries.iter().map(|entry| &entry.table).collect();
        let expected: BTreeSet<_> = next.tables.keys().collect();
        if recorded != expected { return Err(invalid("Block's recorded tables do not match accumulated table metadata")); }
        for entry in entries {
            if next.entries.get(&entry.table).is_some_and(|known| known != &entry) { return Err(invalid("Block changes a previously allocated table OID or slug")); }
            next.entries.insert(entry.table.clone(), entry);
        }
        next.block = Some(Arc::new(block));
        Ok(next)
    }
}

fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.index/invalid-table-catalog", message.to_string()) }
