use std::{collections::BTreeMap, fs::File, io::Read as _, path::PathBuf};

use journal_contract::Lsn;
use rtdb_stream_journal::authenticated::{CatalogRoot, Inclusion};
use rtdb_types::{Anomaly, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{arguments_error, binding, hex_digest, id, take};
use crate::multi::activation::{
    Binding,
    lineage::{Authorization, PublicationProof, SettlementCut, verify_settlement_at},
};

const MAX_EVIDENCE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(super) struct Verification {
    database: String,
    authority: Uuid,
    application_root: CatalogRoot,
    control_root: CatalogRoot,
    /// The closed cut the issuer expects, or none to accept an open floor.
    cut: Option<SettlementCut>,
    evidence: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Evidence {
    authorization: Authorization,
    publication: Option<PublicationProof>,
    inclusion: Option<Inclusion>,
}

impl Verification {
    pub(super) fn parse(database: String, mut fields: BTreeMap<String, String>) -> Result<Self> {
        let authority = id(take(&mut fields, "--authority-id")?)?;
        let application_root = CatalogRoot(hex_digest(take(&mut fields, "--application-root")?)?);
        let control_root = CatalogRoot(hex_digest(take(&mut fields, "--control-root")?)?);
        let predecessor = binding(&mut fields)?;
        let cut = match (
            fields.remove("--successor-incarnation"),
            fields.remove("--successor-revision"),
            fields.remove("--selected-end"),
        ) {
            (Some(incarnation), Some(revision), Some(end)) => Some(SettlementCut {
                predecessor,
                successor: Binding {
                    incarnation: id(incarnation)?,
                    revision: revision.parse().map_err(|_| arguments_error())?,
                },
                end: Lsn(end.parse().map_err(|_| arguments_error())?),
            }),
            (None, None, None) => None,
            _ => return Err(arguments_error()),
        };
        let evidence = take(&mut fields, "--evidence")?.into();
        if !fields.is_empty() {
            return Err(arguments_error());
        }
        Ok(Self {
            database, authority, application_root, control_root, cut, evidence,
        })
    }

    pub(super) fn run(self) -> Result<Value> {
        let mut bytes = Vec::new();
        if self.evidence.as_os_str() == "-" {
            std::io::stdin().lock().take(MAX_EVIDENCE_BYTES + 1).read_to_end(&mut bytes).map_err(|_| invalid_evidence())?;
        } else {
            let file = File::open(&self.evidence).map_err(|_| invalid_evidence())?;
            let metadata = file.metadata().map_err(|_| invalid_evidence())?;
            if !metadata.is_file() || metadata.len() > MAX_EVIDENCE_BYTES {
                return Err(invalid_evidence());
            }
            file.take(MAX_EVIDENCE_BYTES + 1).read_to_end(&mut bytes).map_err(|_| invalid_evidence())?;
        }
        if bytes.len() as u64 > MAX_EVIDENCE_BYTES {
            return Err(invalid_evidence());
        }
        let Evidence { authorization, publication, inclusion } = serde_json::from_slice(&bytes)
            .map_err(|_| invalid_evidence())?;
        let settlement = verify_settlement_at(
            self.authority, &self.database, self.control_root, self.application_root,
            authorization, publication.as_ref(), inclusion.as_ref(), self.cut,
        )?;
        Ok(json!({ "format": 1, "settlement": settlement }))
    }
}

fn invalid_evidence() -> Anomaly {
    Anomaly::incorrect("rtdb/lineage-evidence", "Expected bounded settlement evidence JSON")
}

#[cfg(test)]
mod tests {
    use journal_contract::{Epoch, StreamId};
    use rtdb_stream_journal::authenticated::Timing;

    use super::*;

    #[test]
    fn offline_settlement_preserves_pending_and_full_width_stream_ids() {
        let predecessor = Uuid::from_u128(1 << 100);
        let authorization = Authorization {
            catalog: Uuid::from_u128(1),
            stream: StreamId(predecessor.as_u128()),
            writer_epoch: Epoch(1),
            at: Lsn(1),
            payload_sha256: [5; 32],
            timing: Timing { exclusive_cutoff_micros: 100, receiver_certificate_sha256: [6; 32] },
        };
        let evidence = tempfile::NamedTempFile::new().unwrap();
        let verify = Verification {
            database: "lineagetest".into(),
            authority: Uuid::from_u128(2),
            application_root: CatalogRoot([3; 32]),
            control_root: CatalogRoot([4; 32]),
            cut: Some(SettlementCut {
                predecessor: Binding { incarnation: predecessor, revision: 1 },
                successor: Binding { incarnation: Uuid::from_u128(3), revision: 2 },
                end: Lsn(1),
            }),
            evidence: evidence.path().to_owned(),
        };
        let authorization = serde_json::to_string(&authorization).unwrap();
        std::fs::write(evidence.path(), format!(r#"{{"authorization":{authorization}}}"#)).unwrap();
        assert_eq!(verify.clone().run().unwrap(), json!({"format": 1, "settlement": "pending"}));

        std::fs::write(evidence.path(), format!(r#"{{"authorization":{authorization},"control_root":[]}}"#)).unwrap();
        assert_eq!(verify.run().unwrap_err().code, "rtdb/lineage-evidence");
    }
}
