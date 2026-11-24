use super::*;
use rtdb_index::branches::CustodyReleasePhase;

pub const CUSTODY_RELEASE_NAMESPACE: [u8; 32] = *b"rtdb/v1/custody-release-receipt\0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyWriterFence {
    pub table: TableRef,
    pub id: EntityId,
    pub valid_at: Timestamp,
    pub written: Basis,
}

impl CustodyWriterFence {
    pub fn matches(&self, index: &BranchIndex, now: Timestamp) -> Result<bool> {
        let current = now.max(index.head().system_time);
        let view = index.view(
            MAIN_BRANCH,
            At::Latest,
            Layer::Local,
            current,
        )?;
        Ok([self.valid_at, current].into_iter().all(|valid_at| {
            view.row(&self.table, &self.id, valid_at).is_some_and(|row| row.cut.basis == self.written)
        }))
    }

    pub(super) fn check_requirement(requirement: &Guard, index: &BranchIndex, now: Timestamp) -> Result<bool> {
        let Guard::Version { table, id, valid_at: Some(valid_at), expect: Version::Written(written), .. } = requirement else {
            return Ok(false);
        };
        Self { table: table.clone(), id: id.clone(), valid_at: *valid_at, written: *written }.matches(index, now)
    }

    fn requirement(&self) -> Guard {
        Guard::Version {
            branch: MAIN_BRANCH.into(),
            table: self.table.clone(),
            id: self.id.clone(),
            valid_at: Some(self.valid_at),
            expect: Version::Written(self.written),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedCustodyRelease {
    pub release: CustodyRelease,
    pub writer: CustodyWriterFence,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedCustodyAdmission {
    pub admission: CustodyResponseAdmission,
    pub writer: CustodyWriterFence,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TrustedCustodyCommand {
    Phase(TrustedCustodyRelease),
    Admit(TrustedCustodyAdmission),
}

impl TrustedCustodyCommand {
    pub fn into_request(self) -> Result<Transact> {
        Ok(match self {
            Self::Phase(command) => command.into_request(),
            Self::Admit(command) => {
                let key = command.admission.part_key()?.to_vec();
                request(key, Op::AdmitCustodyResponse(command.admission), command.writer)
            }
        })
    }
}

impl TrustedCustodyRelease {
    pub fn into_request(self) -> Transact {
        let discriminator = match self.release.phase {
            CustodyReleasePhase::Private => 0,
            CustodyReleasePhase::Promoted => 1,
            CustodyReleasePhase::Cancelled => 2,
            CustodyReleasePhase::Disposed { .. } => 3,
        };
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/custody-response-phase\0");
        hash.update(self.release.registration.attempt());
        hash.update([discriminator]);
        request(hash.finalize().to_vec(), Op::CustodyRelease(self.release), self.writer)
    }
}

fn request(key: Vec<u8>, op: Op, writer: CustodyWriterFence) -> Transact {
    Transact {
        key,
        require: Vec::new(),
        window: Window::default(),
        guards: Vec::new(),
        ops: vec![op],
        otherwise: Vec::new(),
        requires: vec![writer.requirement()],
        custody_request_digest: None,
        request_memory: None,
    }
}
