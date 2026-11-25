use std::{
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rtdb_auth::{OriginalFamilyReadbackGrant, OriginalFamilyReadbackProjection};
use rtdb_crypt::{CustodyVault, FileKms};
use rtdb_node::{
    Clock, SystemClock,
    serving::custody_release::{
        CustodyCarrierAuthority, CustodyReleaseRegistration, NativeAuthoring, NativeLandingOrder,
    },
};
use rtdb_types::{Anomaly, AnomalyCategory, Result, TableRef, Timestamp, rtdb_v1::Basis};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
const TERMINAL_ROOM: usize = 256 * 1024;
pub(super) const MAX_STAGES: usize = 128;
pub(super) const MAX_HOLDS: usize = 128;

pub(in crate::v1) struct NativeFamilyVault {
    root: PathBuf,
    keys: Arc<FileKms>,
}

impl NativeFamilyVault {
    pub(in crate::v1) fn open(path: &Path, authority: CustodyCarrierAuthority) -> Result<Self> {
        let directory = FileKms::open(path, || SystemClock.now().0)?;
        let root = directory.vault_path().to_owned();
        let incarnation = Uuid::from_bytes(authority.incarnation);
        let identity = root.join(format!("original-family-vault-{incarnation}.json"));
        let _lock = lock_file(&identity)?;
        match read_record::<CustodyCarrierAuthority>(&identity, 4096)? {
            Some(prior) if prior != authority => return Err(unavailable()),
            Some(_) => (),
            None => write_record(&identity, &authority, 4096)?,
        }
        let keys = root.join("original-family-keys");
        FileKms::create_directory(&keys)?;
        let keys = Arc::new(FileKms::open(&keys, || SystemClock.now().0)?);
        Ok(Self { root, keys })
    }

    pub(in crate::v1) fn bind_line_registry(&self, authority: CustodyCarrierAuthority, table: &TableRef) -> Result<()> {
        let id = Uuid::from_bytes(authority.incarnation);
        bind_record(&self.root.join(format!("original-family-line-registry-{id}.json")), table)
    }

    pub(super) async fn bind_line(
        &self,
        authority: CustodyCarrierAuthority,
        authoring: &NativeAuthoring,
    ) -> Result<()> {
        let Some(line) = &authoring.line else {
            return Ok(());
        };
        let id = Uuid::from_bytes(authority.incarnation);
        let generation = Uuid::from_bytes(line.generation);
        let path = self.root.join(format!("original-family-line-{id}-{generation}.json"));
        let branch = authoring.target.branch.clone();
        let created = authoring.branch_created;
        tokio::task::spawn_blocking(move || bind_record(&path, &(branch, created))).await.map_err(task_error)?
    }

    pub(super) async fn lock(&self, authority: CustodyCarrierAuthority, family: [u8; 16]) -> Result<LockedFamily> {
        let name = format!("family-{}-{}", Uuid::from_bytes(authority.incarnation), Uuid::from_bytes(family));
        let path = self.root.join(name).with_extension("json");
        tokio::task::spawn_blocking(move || {
            let lock = lock_file(&path)?;
            let record = read_record::<Family>(&path, MAX_RECORD_BYTES)?;
            if record.as_ref().is_some_and(|record| record.authority != authority || record.family != family) {
                return Err(unavailable());
            }
            Ok(LockedFamily { path, lock, record })
        })
        .await
        .map_err(task_error)?
    }

    pub(in crate::v1) async fn check_readback(
        &self, scope: &OriginalFamilyReadbackGrant, registry: &TableRef, admission: &TableRef,
        native_group_live: bool,
    ) -> Result<()> {
        let line = &scope.line;
        let name = format!("family-{}-{}.json", Uuid::from_bytes(line.authority.incarnation), Uuid::from_bytes(line.family));
        let path = self.root.join(name);
        let record = tokio::task::spawn_blocking(move || read_record::<ReadbackFamily>(&path, MAX_RECORD_BYTES))
            .await.map_err(task_error)??;
        let Some(record) = record else {
            return if native_group_live || scope.witness.is_some() { Err(unavailable()) } else { Ok(()) };
        };
        if record.authority != line.authority || record.family != line.family || record.group != line.group {
            return Err(unavailable());
        }
        if record.close.is_some() {
            return Err(rtdb_crypt::erased_under_view());
        }
        let authors = record.authors.iter().filter(|author| {
            author.order.target.branch == line.branch && author.order.branch_created.to_bytes() == line.created
                && author.order.group == line.group && author.order.line.as_ref().is_some_and(|binding| {
                    binding.generation == scope.generation && binding.creation_key == scope.creation_key
                        && &binding.registry == registry
                })
        });
        if native_group_live && authors.clone().next().is_none() {
            return Err(unavailable());
        }
        let Some(witness) = &scope.witness else { return Ok(()); };
        let key = &witness.receipt.key;
        let landing = |reserve: bool| record.landings.iter().find(|landing| {
            let order = &landing.order;
            let retained_key = if reserve { &order.reserve_key } else { &order.accept_key };
            retained_key == key && order.destination == witness.placement
                && order.review.cut.branch == line.branch && order.review.cut.basis.to_bytes() >= line.created
                && order.review.group == line.group
        });
        let admitted = match &witness.projection {
            OriginalFamilyReadbackProjection::Author { .. } => authors.clone().any(|author| {
                author.order.placement == witness.placement && &author.order.write_key == key
            }),
            OriginalFamilyReadbackProjection::Reserve {} => landing(true).is_some() || authors.clone().any(|author| {
                author.order.placement == witness.placement
                    && author.order.rewrite.as_ref().is_some_and(|rewrite| &rewrite.reserve_key == key)
            }),
            OriginalFamilyReadbackProjection::Landing { settlement, .. } => landing(false).is_some_and(|landing| {
                settlement.as_ref().is_none_or(|expected| {
                    let matches = |witness: &rtdb_node::serving::custody_release::NativeLandingWitness, id: &str| {
                        &witness.table == admission
                            && matches!(&witness.id, rtdb_types::EntityId::String(written) if written == id)
                            && witness.column == "admission" && witness.valid_at == Timestamp(0)
                    };
                    landing.body == Some(expected.body_sha256)
                        && matches(&landing.order.receipt, &expected.receipt)
                        && matches(&landing.order.proof, &expected.proof)
                })
            }),
        };
        if admitted { Ok(()) } else { Err(unavailable()) }
    }

    fn keys(&self, authority: CustodyCarrierAuthority, family: [u8; 16]) -> CustodyVault {
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/original-family-vault\0");
        hash.update(authority.incarnation);
        hash.update(authority.source_history);
        hash.update(authority.installation);
        hash.update(family);
        let namespace = hash.finalize();
        CustodyVault::new(Uuid::from_bytes(namespace[..16].try_into().expect("UUID width")), self.keys.clone())
    }

    pub(super) async fn seal(
        &self,
        authority: CustodyCarrierAuthority,
        family: [u8; 16],
        stage: [u8; 16],
        binding: [u8; 32],
        carrier: &[u8],
    ) -> Result<Sealed> {
        let keys = self.keys(authority, family);
        keys.create(stage).await?;
        let envelope = keys.seal(stage, binding, carrier).await?;
        Ok(Sealed { input: hash(carrier), digest: hash(&envelope), envelope: URL_SAFE_NO_PAD.encode(envelope) })
    }

    pub(super) async fn open_stage(
        &self,
        authority: CustodyCarrierAuthority,
        family: [u8; 16],
        stage: [u8; 16],
        binding: [u8; 32],
        envelope: &[u8],
    ) -> Result<rtdb_crypt::Plaintext> {
        self.keys(authority, family).open(stage, binding, envelope).await
    }

    pub(super) async fn check_readable(
        &self,
        authority: CustodyCarrierAuthority,
        family: [u8; 16],
        stage: [u8; 16],
    ) -> Result<()> {
        self.keys(authority, family).check_readable(stage).await
    }

    pub(super) async fn destroy_stage(
        &self,
        authority: CustodyCarrierAuthority,
        family: [u8; 16],
        stage: [u8; 16],
        closed_at: Timestamp,
    ) -> Result<()> {
        let keys = self.keys(authority, family);
        if let Err(error) = keys.create(stage).await
            && error.code != "rtdb/erased-under-view"
        {
            return Err(error);
        }
        keys.retire(stage, closed_at.0).await?;
        keys.destroy(stage).await
    }
}

#[derive(Deserialize)]
struct ReadbackFamily {
    authority: CustodyCarrierAuthority,
    family: [u8; 16],
    group: [u8; 16],
    authors: Vec<ReadbackAuthor>,
    landings: Vec<ReadbackLanding>,
    close: Option<serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
struct ReadbackAuthor {
    order: NativeAuthoring,
}

#[derive(Deserialize)]
struct ReadbackLanding {
    order: NativeLandingOrder,
    body: Option<[u8; 32]>,
}

pub(super) struct LockedFamily {
    path: PathBuf,
    lock: File,
    pub(super) record: Option<Family>,
}

impl LockedFamily {
    pub(super) fn family(&self) -> Result<&Family> {
        self.record.as_ref().ok_or_else(unavailable)
    }

    pub(super) async fn save(self, record: Family, terminal: bool) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            let Self { path, lock, .. } = self;
            let limit = if terminal { MAX_RECORD_BYTES } else { MAX_RECORD_BYTES - TERMINAL_ROOM };
            write_record(&path, &record, limit)?;
            drop(lock);
            Ok(())
        })
        .await
        .map_err(task_error)?
    }
}

fn bind_record<T: Serialize + serde::de::DeserializeOwned + PartialEq>(path: &Path, value: &T) -> Result<()> {
    let _lock = lock_file(path)?;
    match read_record::<T>(path, 4096)? {
        Some(prior) if &prior != value => Err(conflict()),
        Some(_) => Ok(()),
        None => write_record(path, value, 4096),
    }
}

fn lock_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))
        .map_err(io_error)?;
    file.lock().map_err(io_error)?;
    Ok(file)
}

fn read_record<T: serde::de::DeserializeOwned>(path: &Path, limit: usize) -> Result<Option<T>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes).map_err(io_error)?;
    if bytes.len() > limit {
        return Err(invalid_record());
    }
    Ok(Some(serde_json::from_slice(&bytes).map_err(|_| invalid_record())?))
}

fn write_record(path: &Path, record: &impl Serialize, limit: usize) -> Result<()> {
    let parent = path.parent().ok_or_else(invalid_record)?;
    let bytes = serde_json::to_vec(record).map_err(|_| invalid_record())?;
    if bytes.len() > limit {
        return Err(full());
    }
    let mut file = tempfile::NamedTempFile::new_in(parent).map_err(io_error)?;
    file.write_all(&bytes).and_then(|()| file.as_file().sync_all()).map_err(io_error)?;
    file.persist(path).map_err(|error| io_error(error.error))?;
    File::open(parent).and_then(|directory| directory.sync_all()).map_err(io_error)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Family {
    pub(super) authority: CustodyCarrierAuthority,
    pub(super) family: [u8; 16],
    pub(super) group: [u8; 16],
    pub(super) authors: Vec<Author>,
    pub(super) landings: Vec<Landing>,
    pub(super) holds: Vec<Hold>,
    pub(super) close: Option<Close>,
}

impl Family {
    pub(super) fn live(&self) -> Result<()> {
        if self.close.is_some() {
            return Err(rtdb_crypt::erased_under_view());
        }
        Ok(())
    }

    pub(super) fn inventory(&self) -> Result<[u8; 32]> {
        let bytes = serde_json::to_vec(&(self.authority, self.family, self.group, &self.authors, &self.landings))
            .map_err(|_| invalid_record())?;
        Ok(hash(&bytes))
    }

    pub(super) fn stages(&self) -> impl Iterator<Item = [u8; 16]> + '_ {
        self.authors
            .iter()
            .flat_map(|author| {
                std::iter::once(author.order.placement).chain(author.order.rewrite.as_ref().map(|rewrite| rewrite.link))
            })
            .chain(self.landings.iter().flat_map(|landing| [landing.order.destination, landing.order.link]))
    }

    pub(super) fn check_new_stages(&self, stages: &[[u8; 16]]) -> Result<()> {
        if self.stages().count() + stages.len() > MAX_STAGES {
            return Err(full());
        }
        if self.stages().any(|stage| stages.contains(&stage)) {
            return Err(conflict());
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Author {
    pub(super) assertion: [u8; 32],
    pub(super) disclosure: [u8; 32],
    pub(super) order: NativeAuthoring,
    pub(super) payload: Option<Sealed>,
    pub(super) link: Option<Sealed>,
    pub(super) confirmed: Option<Authored>,
}

impl Author {
    pub(super) fn same_enrollment(&self, other: &Self) -> bool {
        self.assertion == other.assertion && self.disclosure == other.disclosure && self.order == other.order
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Authored {
    pub(super) position: [u8; 32],
    pub(super) original: [u8; 32],
    pub(super) occurrence: [u8; 32],
    pub(super) evidence: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Landing {
    pub(super) assertion: [u8; 32],
    pub(super) order: NativeLandingOrder,
    pub(super) target: Vec<u8>,
    pub(super) member: Vec<u8>,
    pub(super) winner: [u8; 32],
    pub(super) source_carriers: [[u8; 32]; 2],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) retained_query: Option<RetainedReviewQuery>,
    pub(super) review_evidence: [u8; 32],
    pub(super) body: Option<[u8; 32]>,
    pub(super) payload: Option<Sealed>,
    pub(super) link: Option<Sealed>,
    pub(super) accepted: Option<Accepted>,
}

impl Landing {
    pub(super) fn same_reservation(&self, other: &Self) -> bool {
        self.assertion == other.assertion
            && self.order == other.order
            && self.target == other.target
            && self.member == other.member
            && self.winner == other.winner
            && self.source_carriers == other.source_carriers
            && self.retained_query == other.retained_query
            && self.review_evidence == other.review_evidence
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetainedReviewQuery {
    pub(super) proof: [u8; 32],
    pub(super) claim: [u8; 32],
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Sealed {
    pub(super) input: [u8; 32],
    pub(super) digest: [u8; 32],
    pub(super) envelope: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Accepted {
    pub(super) binding: Vec<u8>,
    pub(super) occurrence: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Hold {
    pub(super) registration: CustodyReleaseRegistration,
    pub(super) assertion: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Close {
    pub(super) key: Vec<u8>,
    pub(super) revoked_at: Basis,
    pub(super) native_inventory: [u8; 32],
    pub(super) family_inventory: [u8; 32],
    pub(super) closed_at: Timestamp,
    pub(super) drained: bool,
}

pub(super) fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/original-family-landing", "Original family custody is unavailable")
}

pub(super) fn conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/original-family-landing-conflict",
        "Original family custody evidence does not match",
    )
}

pub(super) fn invalid_record() -> Anomaly {
    Anomaly::fault("rtdb/original-family-vault", "The Original family vault record is invalid")
}

pub(super) fn full() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Busy,
        "rtdb/original-family-vault-full",
        "The Original family custody limit was reached",
    )
}

fn io_error(error: std::io::Error) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/original-family-vault-io", error.to_string())
}

fn task_error(error: tokio::task::JoinError) -> Anomaly {
    Anomaly::fault("rtdb/original-family-vault-task", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn family_vault_keeps_its_exact_authority_and_retired_stage_after_restart() -> Result<()> {
        let parent = tempfile::tempdir().map_err(io_error)?;
        let path = parent.path().join("private");
        FileKms::create_directory(&path)?;
        let authority = CustodyCarrierAuthority {
            incarnation: [1; 16],
            source_history: [2; 32],
            installation: [3; 16],
            registry: [4; 32],
            original_writer: [5; 32],
        };
        let family = [6; 16];
        let group = [7; 16];
        let key = [8; 16];
        let binding = [9; 32];
        let vault = NativeFamilyVault::open(&path, authority)?;
        let lock = vault.lock(authority, family).await?;
        assert!(lock.record.is_none());
        let record = Family {
            authority,
            family,
            group,
            authors: vec![],
            landings: vec![],
            holds: vec![],
            close: Some(Close {
                key: vec![10; 16],
                revoked_at: Basis::new(authority.incarnation, 2),
                native_inventory: [11; 32],
                family_inventory: [12; 32],
                closed_at: Timestamp(0),
                drained: true,
            }),
        };
        lock.save(record, true).await?;
        let sealed = vault.seal(authority, family, key, binding, b"sealed carrier").await?;
        let bytes = URL_SAFE_NO_PAD.decode(&sealed.envelope).unwrap();
        vault.destroy_stage(authority, family, key, Timestamp(0)).await?;
        let reopened = NativeFamilyVault::open(&path, authority)?;
        let lock = reopened.lock(authority, family).await?;
        assert!(lock.family()?.live().is_err());
        assert!(lock.family()?.close.as_ref().unwrap().drained);
        assert!(reopened.open_stage(authority, family, key, binding, &bytes).await.is_err());
        assert!(reopened.seal(authority, family, key, binding, b"replacement").await.is_err());
        drop(lock);
        let different = CustodyCarrierAuthority { original_writer: [13; 32], ..authority };
        assert!(reopened.lock(different, family).await.is_err());
        assert!(NativeFamilyVault::open(&path, different).is_err());
        Ok(())
    }
}
