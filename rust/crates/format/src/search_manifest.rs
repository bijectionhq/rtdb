use std::collections::BTreeSet;

use prost::Message;
use rtdb_types::{Anomaly, BlockIndex, Result};

use crate::{keys::TableSlug, log::proto::{ArtefactRecord, SearchRun, SearchRunChange}};

pub const RECORD_KIND: &str = "logical-search";
pub const MAX_RUN_BLOCKS: usize = 256;

impl SearchRun {
    pub fn encoded_size(&self) -> usize { self.encoded_len() }
    pub fn identity(&self) -> Result<[u8; 32]> {
        self.digest.as_deref().and_then(|value| value.try_into().ok())
            .ok_or_else(|| invalid("Search object has no SHA-256 identity"))
    }

    pub fn validate(&self) -> Result<()> {
        self.identity()?;
        if self.definition_id().is_empty() || self.definition_id().len() > 512
            || self.blocks.is_empty() || self.blocks.len() > MAX_RUN_BLOCKS
            || self.blocks[0] < 0 || self.blocks.windows(2).any(|pair| pair[0] >= pair[1])
            || self.source_rows() <= 0 || self.file_size() <= 0 || self.rebuild() < 0
            || self.locator().is_empty() || self.locator().len() > 32 * 1024
        {
            return Err(invalid("Search run has an invalid definition, extent, or locator"));
        }
        Ok(())
    }

    pub fn blocks(&self) -> impl Iterator<Item = BlockIndex> + '_ {
        self.blocks.iter().copied().map(BlockIndex)
    }

    pub fn object_key(&self, slug: &TableSlug) -> Result<String> {
        self.validate()?;
        Ok(run_key(slug, self.identity()?))
    }
}

pub fn run_prefix(slug: &TableSlug) -> String { format!("{}/search-runs", slug.table_prefix()) }

pub fn run_key(slug: &TableSlug, digest: [u8; 32]) -> String {
    let name: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{}/{name}.arrow", run_prefix(slug))
}

pub fn is_run_key(key: &str) -> bool {
    key.strip_prefix("tables/").and_then(|tail| tail.split_once("/search-runs/"))
        .and_then(|(slug, file)| Some((slug, file.strip_suffix(".arrow")?)))
        .is_some_and(|(slug, name)| TableSlug::recorded(slug).is_ok() && name.len() == 64
            && name.bytes().all(|ch| ch.is_ascii_digit() || (b'a'..=b'f').contains(&ch)))
}

impl SearchRunChange {
    pub fn from_artefact(record: &ArtefactRecord) -> Result<Self> {
        let decoded = Self::decode(record.stats()).map_err(invalid)?;
        let run = decoded.run.as_ref().ok_or_else(|| invalid("Search publication has no run"))?;
        run.validate()?;
        if record.kind() != RECORD_KIND || run.definition_id() != record.definition_id()
            || run.definition_version != record.definition_version || decoded.replaces.len() > MAX_RUN_BLOCKS
            || decoded.replaces.iter().any(|digest| digest.len() != 32)
            || decoded.replaces.iter().collect::<BTreeSet<_>>().len() != decoded.replaces.len()
            || (decoded.retire_only() && !decoded.replaces.is_empty())
        {
            return Err(invalid("Search publication has inconsistent receipts"));
        }
        Ok(decoded)
    }

    pub fn to_artefact(&self) -> Result<ArtefactRecord> {
        let run = self.run.as_ref().ok_or_else(|| invalid("Search publication has no run"))?;
        Ok(ArtefactRecord {
            kind: Some(RECORD_KIND.into()),
            definition_id: run.definition_id.clone(),
            definition_version: run.definition_version,
            stats: Some(self.encode_to_vec()),
            ..Default::default()
        })
    }
}

fn invalid(reason: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/search-manifest", reason.to_string())
}
