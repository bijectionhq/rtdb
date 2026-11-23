use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;

use crate::wire::{Connection, protocol};

#[derive(Default)]
pub(crate) struct VectorTypes {
    names: Vec<(u32, String, String)>,
    _charge: Option<Arc<MemoryReservation>>,
}

impl VectorTypes {
    pub async fn load(connection: &mut Connection, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let catalog = connection.query(
            "SELECT t.oid, n.nspname, t.typname
             FROM pg_catalog.pg_type t
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
             JOIN pg_catalog.pg_depend d ON d.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
                 AND d.objid = t.oid AND d.deptype = 'e'
             JOIN pg_catalog.pg_extension e ON d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                 AND d.refobjid = e.oid
             WHERE e.extname = 'vector' AND t.typname IN ('vector', 'halfvec')"
        ).await?;
        if catalog.rows.is_empty() { return Ok(Self::default()); }
        let bytes = catalog.rows.iter().try_fold(512usize, |sum, row| {
            let size = row.encoded_size().checked_add(128).ok_or_else(|| protocol("pgvector metadata allocation overflow"))?;
            sum.checked_add(size).ok_or_else(|| protocol("pgvector metadata allocation overflow"))
        })?;
        let charge = memory.reserve(bytes)?;
        let mut names = Vec::with_capacity(catalog.rows.len());
        for row in &catalog.rows {
            let oid = row.cell(0)?.ok_or_else(|| protocol("pgvector type OID is null"))?.parse().map_err(protocol)?;
            let schema = row.cell(1)?.ok_or_else(|| protocol("pgvector type namespace is null"))?;
            let name = row.cell(2)?.ok_or_else(|| protocol("pgvector type name is null"))?;
            names.push((oid, schema.to_owned(), name.to_owned()));
        }
        Ok(Self { names, _charge: Some(charge) })
    }

    pub fn matches(&self, oid: u32, qualified_name: Option<(&str, &str)>) -> bool {
        self.names.iter().any(|(id, schema, name)| {
            *id == oid && qualified_name.is_none_or(|qualified| qualified == (schema.as_str(), name.as_str()))
        })
    }

    #[cfg(test)]
    pub fn fixture() -> Self {
        Self { names: vec![(20345, "embeddings".into(), "vector".into()), (20346, "embeddings".into(), "halfvec".into())], _charge: None }
    }
}
