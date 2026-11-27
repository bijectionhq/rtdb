use std::collections::{BTreeMap, BTreeSet};

use rtdb_format::catalog::{BlockCatalog, FIRST_TABLE_OID, TableEntry, legacy_table_ref};
use rtdb_types::{BlockIndex, Result, TableRef};
use serde::Serialize;

type SlugCollisions = BTreeMap<String, Vec<TableRef>>;

#[derive(Serialize)]
pub(super) struct Report {
    block_index: BlockIndex,
    registry_source: RegistrySource,
    rtdb_only_root: bool,
    checkpoint_registry: Assessment,
    allocation_batch: Option<Assessment>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum RegistrySource {
    Recorded,
    NamesOnly,
}

#[derive(Serialize)]
struct Assessment {
    eligible: bool,
    relations: Vec<Relation>,
    xtdb_slug_collisions: SlugCollisions,
}

#[derive(Serialize)]
struct Relation {
    table: TableRef,
    native_oid: u32,
    xtdb_oid: u32,
    xtdb_slug: String,
}

impl Relation {
    fn compare(native_oid: u32, xtdb: &TableEntry) -> Self {
        Self { table: xtdb.table.clone(), native_oid, xtdb_oid: xtdb.oid, xtdb_slug: xtdb.slug.as_str().to_owned() }
    }
}

pub(super) fn preflight(block: &BlockCatalog, rtdb_only_root: bool, tables: &[TableRef]) -> Result<Report> {
    let entries = block.table_entries()?;
    let registry_source = if block.as_proto().tables.is_empty() { RegistrySource::NamesOnly } else { RegistrySource::Recorded };
    let legacy_oids = if matches!(registry_source, RegistrySource::NamesOnly) {
        let mut names: Vec<_> = block.as_proto().table_names.iter().collect();
        names.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
        names.into_iter().enumerate().map(|(i, name)| Ok((legacy_table_ref(name)?, oid_after(FIRST_TABLE_OID - 1, i)?)))
            .collect::<Result<BTreeMap<_, _>>>()?
    } else {
        BTreeMap::new()
    };
    let relations = entries.iter().map(|entry| {
        let mut relation = Relation::compare(entry.oid, entry);
        if let Some(legacy_oid) = legacy_oids.get(&entry.table) { relation.xtdb_oid = *legacy_oid; }
        relation
    }).collect();
    let checkpoint_registry = assess(!rtdb_only_root, relations, slug_collisions(entries.iter()));
    let allocation_batch = if tables.is_empty() {
        None
    } else {
        Some(assess_batch(&entries, tables, checkpoint_registry.eligible)?)
    };
    Ok(Report { block_index: block.block_index(), registry_source, rtdb_only_root, checkpoint_registry, allocation_batch })
}

fn assess_batch(entries: &[TableEntry], tables: &[TableRef], checkpoint_eligible: bool) -> Result<Assessment> {
    let mut known: BTreeSet<_> = entries.iter().map(|entry| &entry.table).collect();
    let mut last_oid = entries.iter().map(|entry| entry.oid).max().unwrap_or(FIRST_TABLE_OID - 1);
    let mut allocated = Vec::new();
    let startup_tables = [TableRef::new("xt", "txs"), TableRef::new("xt", "role_membership")];
    for table in &startup_tables {
        if known.insert(table) {
            last_oid = oid_after(last_oid, 0)?;
            allocated.push(TableEntry::new(last_oid, table.clone())?);
        }
    }

    let mut fresh: Vec<_> = tables.iter().filter(|table| known.insert(*table))
        .map(|table| (table.schema_and_table(), table)).collect();
    fresh.sort_by(|(a, _), (b, _)| a.cmp(b));
    let native_oids = fresh.iter().enumerate().map(|(i, (_, table))| Ok((*table, oid_after(last_oid, i)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    fresh.sort_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
    let mut relations = Vec::new();
    for (i, (_, table)) in fresh.into_iter().enumerate() {
        let entry = TableEntry::new(oid_after(last_oid, i)?, table.clone())?;
        relations.push(Relation::compare(native_oids[table], &entry));
        allocated.push(entry);
    }
    Ok(assess(checkpoint_eligible, relations, slug_collisions(entries.iter().chain(&allocated))))
}

fn oid_after(last: u32, index: usize) -> Result<u32> {
    u32::try_from(index).ok()
        .and_then(|index| last.checked_add(index)?.checked_add(1))
        .ok_or_else(|| super::invalid("Table OID space exhausted"))
}

fn assess(predecessor_eligible: bool, relations: Vec<Relation>, xtdb_slug_collisions: SlugCollisions) -> Assessment {
    let eligible = predecessor_eligible && xtdb_slug_collisions.is_empty()
        && relations.iter().all(|relation| relation.native_oid == relation.xtdb_oid);
    Assessment { eligible, relations, xtdb_slug_collisions }
}

fn slug_collisions<'a>(entries: impl IntoIterator<Item = &'a TableEntry>) -> SlugCollisions {
    let mut owners = BTreeMap::<_, Vec<_>>::new();
    for entry in entries {
        owners.entry(entry.slug.as_str()).or_default().push(&entry.table);
    }
    owners.into_iter().filter(|(_, tables)| tables.len() > 1).map(|(slug, mut tables)| {
        tables.sort();
        (slug.to_owned(), tables.into_iter().cloned().collect())
    }).collect()
}
