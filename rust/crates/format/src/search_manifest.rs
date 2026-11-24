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
            || self.allocation.as_ref().is_some_and(|value| value.len() != 16)
            || self.locator().is_empty() || self.locator().len() > 32 * 1024
        {
            return Err(invalid("Search run has an invalid definition, extent, or locator"));
        }
        Ok(())
    }

    pub fn blocks(&self) -> impl Iterator<Item = BlockIndex> + '_ {
        self.blocks.iter().copied().map(BlockIndex)
    }

    pub fn pending_key(&self, slug: &TableSlug) -> Result<String> {
        let name = self.object_key(slug)?;
        Ok(name.replace("/search-runs/", "/search-pending/"))
    }

    pub fn object_key(&self, slug: &TableSlug) -> Result<String> {
        self.validate()?;
        let digest = self.identity()?;
        Ok(run_key(slug, digest, self.allocation.is_some()))
    }
}

pub const MAX_PENDING_BYTES: usize = 40 * 1024;

const STAGE_PREFIX: &[u8] = b"RTDB-search-stage-v2\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchStagingOwner {
    Directory { region: uuid::Uuid, writer_epoch: u64 },
    Local { attachment: uuid::Uuid },
}

pub struct StagedSearchRun {
    pub run: SearchRun,
    pub owner: Option<SearchStagingOwner>,
}

impl StagedSearchRun {
    pub fn encode(&self) -> Result<Vec<u8>> {
        Self::validate(&self.run)?;
        let mut bytes = STAGE_PREFIX.to_vec();
        match self.owner {
            Some(SearchStagingOwner::Directory { region, writer_epoch }) if !region.is_nil() && writer_epoch > 0 => {
                bytes.push(1);
                bytes.extend_from_slice(region.as_bytes());
                bytes.extend_from_slice(&writer_epoch.to_be_bytes());
            }
            Some(SearchStagingOwner::Local { attachment }) if !attachment.is_nil() => {
                bytes.push(2);
                bytes.extend_from_slice(attachment.as_bytes());
            }
            Some(_) => return Err(invalid("Search upload intent has invalid writer custody")),
            None => bytes.push(0),
        }
        self.run.encode(&mut bytes).map_err(invalid)?;
        if bytes.len() > MAX_PENDING_BYTES { return Err(invalid("Search upload intent exceeds its size limit")); }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let malformed = || invalid("Search upload intent has invalid writer custody");
        if bytes.len() > MAX_PENDING_BYTES { return Err(malformed()); }
        let (kind, bytes) = bytes.strip_prefix(STAGE_PREFIX).and_then(|rest| rest.split_first()).ok_or_else(malformed)?;
        let (owner, bytes) = match kind {
            0 => (None, bytes),
            1 => {
                let (region, bytes) = bytes.split_first_chunk::<16>().ok_or_else(malformed)?;
                let (epoch, bytes) = bytes.split_first_chunk::<8>().ok_or_else(malformed)?;
                let (region, writer_epoch) = (uuid::Uuid::from_bytes(*region), u64::from_be_bytes(*epoch));
                if region.is_nil() || writer_epoch == 0 { return Err(malformed()); }
                (Some(SearchStagingOwner::Directory { region, writer_epoch }), bytes)
            }
            2 => {
                let (attachment, bytes) = bytes.split_first_chunk::<16>().ok_or_else(malformed)?;
                let attachment = uuid::Uuid::from_bytes(*attachment);
                if attachment.is_nil() { return Err(malformed()); }
                (Some(SearchStagingOwner::Local { attachment }), bytes)
            }
            _ => return Err(malformed()),
        };
        let run = SearchRun::decode(bytes).map_err(invalid)?;
        Self::validate(&run)?;
        Ok(Self { run, owner })
    }

    fn validate(run: &SearchRun) -> Result<()> {
        run.validate()?;
        if run.allocation.is_none() || run.publication.is_some() {
            return Err(invalid("Search upload intent must name a fresh allocation"));
        }
        Ok(())
    }
}

pub fn pending_prefix(slug: &TableSlug) -> String { format!("{}/search-pending", slug.table_prefix()) }

pub fn run_prefix(slug: &TableSlug) -> String { format!("{}/search-runs", slug.table_prefix()) }

fn run_key(slug: &TableSlug, digest: [u8; 32], allocated: bool) -> String {
    let name: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    let suffix = if allocated { ".f16.arrow" } else { ".arrow" };
    format!("{}/{name}{suffix}", run_prefix(slug))
}

pub fn is_run_key(key: &str) -> bool {
    run_key_has_suffix(key, ".f16.arrow") || run_key_has_suffix(key, ".arrow")
}

pub fn is_ranged_run(key: &str) -> bool { run_key_has_suffix(key, ".f16.arrow") }

fn run_key_has_suffix(key: &str, suffix: &str) -> bool {
    key.strip_prefix("tables/").and_then(|tail| tail.split_once("/search-runs/"))
        .and_then(|(slug, file)| Some((slug, file.strip_suffix(suffix)?)))
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
            || (decoded.abandon() && (!decoded.retire_only() || run.allocation.is_none()))
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
