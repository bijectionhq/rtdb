use rtdb_types::{Anomaly, BlockIndex, CatalogCut, PhysicalObjectVersion, ReplicaPosition, Result, TableRef, TerminalObjectPublication};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::keys::TRIE_RETIREMENT_DIRECTORY;

pub const MAXIMUM_INTENT_BYTES: usize = 1 << 20;
pub const MAXIMUM_RECEIPT_BYTES: usize = 8 + MAXIMUM_INTENT_BYTES;

#[derive(Serialize, Deserialize)]
pub struct RetirementFile {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<PhysicalObjectVersion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_publication: Option<TerminalObjectPublication>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub matches_publication: bool,
}

impl RetirementFile {
    pub fn release_order(&self) -> u8 {
        match self.key.split('/').nth(2) {
            Some("data") => 1,
            Some("meta") => 2,
            _ => 0,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct RetirementIntent {
    pub selected: CatalogCut,
    pub table: TableRef,
    pub tries: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<RetirementFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_reset: Option<ResetRetirementCut>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct ResetRetirementCut {
    pub block: BlockIndex,
    pub naming_epoch: u64,
}

impl ResetRetirementCut {
    pub fn admits(self, block: &crate::catalog::BlockCatalog, still_retained: bool) -> bool {
        reset::reset_allows_retirement(self.block.0, self.naming_epoch, block.block_index().0, block.naming_epoch(), still_retained)
    }
}

pub fn journal_directory(cut: CatalogCut) -> String {
    format!("{TRIE_RETIREMENT_DIRECTORY}/{}-{}-{}", cut.storage_version.0, cut.storage_epoch.0, cut.replica_epoch.0)
}

pub fn intent_digest(encoded: &[u8]) -> String {
    format!("{:x}", Sha256::digest(encoded))
}

impl RetirementIntent {
    pub fn installed_cut(&self, through: ReplicaPosition) -> Result<CatalogCut> {
        let selected = self.selected;
        if !step::receipt_follows_selection(selected.through.0, selected.replica_epoch.0, through.0, through.epoch().0) {
            return Err(Anomaly::incorrect("rtdb/invalid-trie-retirement",
                "The trie removal receipt is outside its selection's journal or does not advance it"));
        }
        Ok(CatalogCut { through, ..selected })
    }
}

pub struct InstalledRetirement {
    cut: CatalogCut,
    intent: RetirementIntent,
}

impl InstalledRetirement {
    pub fn decode(path: &str, receipt: &[u8]) -> Result<Self> {
        let invalid = |reason| Anomaly::incorrect("rtdb/invalid-trie-retirement", reason);
        if receipt.len() > MAXIMUM_RECEIPT_BYTES {
            return Err(invalid("The installed trie retirement exceeds the metadata size limit"));
        }
        let (position, intent) = receipt.split_first_chunk::<8>()
            .ok_or_else(|| invalid("The trie removal receipt has no installed position"))?;
        let plan: RetirementIntent = serde_json::from_slice(intent)
            .map_err(|_| invalid("The trie removal receipt has a malformed intent"))?;
        let prefix = journal_directory(plan.selected);
        if path != format!("{prefix}/installed/{}", intent_digest(intent)) {
            return Err(invalid("The trie removal receipt path names a different intent"));
        }
        let cut = plan.installed_cut(ReplicaPosition(i64::from_be_bytes(*position)))?;
        Ok(Self { cut, intent: plan })
    }

    pub fn cut(&self) -> CatalogCut { self.cut }

    pub fn files(&self) -> &[RetirementFile] { &self.intent.files }
}

#[path = "trie_retirement_step.rs"]
mod step;
#[path = "reset_retirement_step.rs"]
mod reset;

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{StorageEpoch, StorageVersion};

    #[test]
    fn an_installed_receipt_binds_the_plan_and_later_position() {
        let selected = CatalogCut {
            storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0),
            replica_epoch: StorageEpoch(0), through: ReplicaPosition(1),
        };
        let intent = RetirementIntent {
            selected,
            table: TableRef::new("public", "docs"),
            tries: vec!["l01-rc-b00".into()],
            files: vec![RetirementFile {
                key: "tables/docs/data/l01-rc-b00.arrow".into(), version: None, terminal_publication: None, matches_publication: false,
            }],
            after_reset: None,
        };
        let contents = serde_json::to_vec(&intent).unwrap();
        let path = format!("{}/installed/{}", journal_directory(selected), intent_digest(&contents));
        let receipt = |position: i64| [position.to_be_bytes().as_slice(), &contents].concat();
        let installed = InstalledRetirement::decode(&path, &receipt(2)).unwrap();
        assert_eq!(installed.cut().through, ReplicaPosition(2));
        assert_eq!(installed.files()[0].key, intent.files[0].key);
        assert!(InstalledRetirement::decode(&path, &receipt(1)).is_err());
        assert!(InstalledRetirement::decode(&format!("{path}0"), &receipt(2)).is_err());
    }
}
