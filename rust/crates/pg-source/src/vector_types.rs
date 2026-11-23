use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;

use crate::wire::{Connection, protocol};

#[derive(Clone, Copy)]
pub(crate) enum VectorType {
    FloatArray(u32),
    Pgvector(Option<i32>),
}

struct CatalogType {
    oid: u32,
    base_namespace: String,
    base_name: String,
    layout: VectorType,
}

#[derive(Default)]
pub(crate) struct VectorTypes {
    types: Vec<CatalogType>,
    _charge: Option<Arc<MemoryReservation>>,
}

impl VectorTypes {
    pub async fn load(connection: &mut Connection, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let catalog = connection.query(
            "WITH RECURSIVE bases AS (
                 SELECT t.oid, n.nspname, t.typname
                 FROM pg_catalog.pg_type t
                 JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                 WHERE t.oid IN (1021, 1022)
                     OR (t.typname IN ('vector', 'halfvec') AND EXISTS (
                         SELECT 1 FROM pg_catalog.pg_depend d
                         JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid
                         WHERE d.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
                             AND d.objid = t.oid AND d.deptype = 'e'
                             AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                             AND e.extname = 'vector'
                     ))
             ), types(oid, base_oid, typmod) AS (
                 SELECT oid, oid, NULL::integer FROM bases
                 UNION ALL
                 SELECT d.oid, t.base_oid, COALESCE(t.typmod, d.typtypmod)
                 FROM types t JOIN pg_catalog.pg_type d ON d.typbasetype = t.oid
                 WHERE d.typtype = 'd'
             )
             SELECT t.oid, b.nspname, b.typname, t.base_oid, t.typmod
             FROM types t JOIN bases b ON b.oid = t.base_oid
             WHERE t.oid <> t.base_oid OR t.base_oid NOT IN (1021, 1022)"
        ).await?;
        if catalog.rows.is_empty() { return Ok(Self::default()); }
        let bytes = catalog.rows.iter().try_fold(512usize, |sum, row| {
            let size = row.encoded_size().checked_add(128).ok_or_else(|| protocol("pgvector metadata allocation overflow"))?;
            sum.checked_add(size).ok_or_else(|| protocol("pgvector metadata allocation overflow"))
        })?;
        let charge = memory.reserve(bytes)?;
        let mut types = Vec::with_capacity(catalog.rows.len());
        for row in &catalog.rows {
            let oid = row.cell(0)?.ok_or_else(|| protocol("pgvector type OID is null"))?.parse().map_err(protocol)?;
            let schema = row.cell(1)?.ok_or_else(|| protocol("pgvector type namespace is null"))?;
            let name = row.cell(2)?.ok_or_else(|| protocol("pgvector type name is null"))?;
            let base = row.cell(3)?.ok_or_else(|| protocol("Vector base type OID is null"))?.parse().map_err(protocol)?;
            let layout = match base {
                1021 | 1022 => VectorType::FloatArray(base),
                _ => VectorType::Pgvector(row.cell(4)?.map(str::parse).transpose().map_err(protocol)?),
            };
            types.push(CatalogType { oid, base_namespace: schema.to_owned(), base_name: name.to_owned(), layout });
        }
        Ok(Self { types, _charge: Some(charge) })
    }

    pub fn get(&self, oid: u32, qualified_name: Option<(&str, &str)>) -> Option<VectorType> {
        self.types.iter().find(|entry| {
            entry.oid == oid && qualified_name.is_none_or(|(schema, name)| {
                let schema = if schema.is_empty() { "pg_catalog" } else { schema };
                schema == entry.base_namespace && name == entry.base_name
            })
        }).map(|entry| entry.layout)
    }

    #[cfg(test)]
    pub fn fixture() -> Self {
        let types = [
            (20345, "embeddings", "vector", VectorType::Pgvector(None)),
            (20346, "embeddings", "halfvec", VectorType::Pgvector(None)),
            (20348, "embeddings", "vector", VectorType::Pgvector(Some(2))),
            (20349, "pg_catalog", "_float4", VectorType::FloatArray(1021)),
        ].into_iter().map(|(oid, namespace, name, layout)| CatalogType {
            oid, base_namespace: namespace.into(), base_name: name.into(), layout,
        }).collect();
        Self { types, _charge: None }
    }
}
