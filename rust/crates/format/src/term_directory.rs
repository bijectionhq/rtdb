use prost::Message;
use sha2::{Digest, Sha256};

use crate::{cow_tree::RootRef, keys::TableSlug, log::proto::{ArtefactRecord, SearchRun, SearchRunChange, TermDirectoryRoot}, search_manifest::RECORD_KIND};
use rtdb_types::{Anomaly, Result};

pub fn page_prefix(slug: &TableSlug) -> String { format!("{}/term-directory", slug.table_prefix()) }

impl TermDirectoryRoot {
    pub fn page_root(&self) -> Result<Option<RootRef>> {
        let bytes = self.root.as_deref().ok_or_else(|| invalid("Term directory has no root evidence"))?;
        if self.definition_id().is_empty() || self.definition_version.is_none() || self.runs() <= 0 {
            return Err(invalid("Term directory has an invalid generation or run count"));
        }
        if bytes.is_empty() { return Ok(None); }
        let root = RootRef::decode(bytes)?;
        let bound = root.summary.as_slice().try_into().map(u64::from_be_bytes).map(f64::from_bits);
        if !bound.is_ok_and(|bound| bound.is_finite() && bound > 0.0) {
            return Err(invalid("Term directory root has an invalid score bound"));
        }
        Ok(Some(root))
    }
}

pub fn publication_basis(root: Option<&TermDirectoryRoot>, runs: impl IntoIterator<Item = [u8; 32]>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"rtdb/search/term-directory/basis");
    let bytes = root.map_or_else(Vec::new, |root| root.encode_to_vec());
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
    let mut runs: Vec<_> = runs.into_iter().collect();
    runs.sort_unstable();
    for run in runs { digest.update(run); }
    digest.finalize().into()
}

pub fn valid_change(run: &SearchRun, root: &TermDirectoryRoot, replaces: Option<&[u8]>) -> bool {
    replaces.is_some_and(|token| token.len() == 32)
        && root.definition_id == run.definition_id && root.definition_version == run.definition_version
        && root.page_root().is_ok()
}

pub fn sealed_change(record: &ArtefactRecord) -> Option<SearchRunChange> {
    if record.kind() != RECORD_KIND { return None; }
    let change = SearchRunChange::from_artefact(record).ok()?;
    change.term_directory.as_ref()?;
    Some(change)
}

fn invalid(message: &str) -> Anomaly { Anomaly::incorrect("rtdb.format/term-directory", message) }
