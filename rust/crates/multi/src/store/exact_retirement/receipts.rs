use std::collections::{BTreeMap, BTreeSet};

use rtdb_directory::DeploymentRecord;
use rtdb_format::trie_retirement::{InstalledRetirement, MAXIMUM_RECEIPT_BYTES, journal_directory};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_types::{CatalogCut, PhysicalObjectVersion, ReplicaPosition, Result, StorageEpoch, StorageVersion};

use crate::{cipher::{Cipher, SEALED_OVERHEAD}, keys::KeyService};

use super::super::GarbageRecord;

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
struct InstalledCut(StorageVersion, StorageEpoch, StorageEpoch, ReplicaPosition);

impl From<CatalogCut> for InstalledCut {
    fn from(cut: CatalogCut) -> Self { Self(cut.storage_version, cut.storage_epoch, cut.replica_epoch, cut.through) }
}

#[derive(Clone)]
pub(super) struct Witness {
    pub(super) receipt: String,
    pub(super) version: PhysicalObjectVersion,
}

#[derive(Default)]
pub(super) struct ReceiptIndex {
    by_cut: BTreeMap<InstalledCut, BTreeMap<String, Option<Witness>>>,
}

impl ReceiptIndex {
    pub(super) async fn load<'a>(bucket: &dyn ObjectStore, keys: &dyn KeyService, installation: &DeploymentRecord,
        store: &str, releases: impl Iterator<Item = &'a GarbageRecord> + Send) -> Result<Self> {
        let mut directories = BTreeSet::new();
        let mut wanted: BTreeMap<InstalledCut, BTreeSet<&str>> = BTreeMap::new();
        for release in releases {
            let Some(floor) = &release.trie_floor else { continue; };
            directories.insert(journal_directory(floor.cut));
            wanted.entry(floor.cut.into()).or_default().insert(&release.key);
        }
        let mut index = Self::default();
        if directories.is_empty() { return Ok(index); }
        let Some(data_key) = keys.existing_key(&installation.key_id()).await? else { return Ok(index); };
        let cipher = Cipher::new(&data_key)?;
        for directory in directories {
            let prefix = ObjectKey::new(format!("{store}/{directory}/installed"))?;
            for object in bucket.list(Some(&prefix)).await? {
                let Some(installed) = read(bucket, &cipher, store, &object.key, object.size).await? else { continue; };
                let cut = InstalledCut::from(installed.cut());
                let Some(names) = wanted.get(&cut) else { continue; };
                for file in installed.files().iter().filter(|file| names.contains(file.key.as_str()) && file.matches_publication) {
                    let Some(version) = &file.version else { continue; };
                    let files = index.by_cut.entry(cut).or_default();
                    files.entry(file.key.clone()).and_modify(|witness| {
                        if witness.as_ref().is_some_and(|prior| prior.version != *version) { *witness = None; }
                    }).or_insert_with(|| Some(Witness { receipt: object.key.to_string(), version: version.clone() }));
                }
            }
        }
        Ok(index)
    }

    pub(super) fn get(&self, release: &GarbageRecord) -> Option<&Witness> {
        let cut = release.trie_floor.as_ref()?.cut.into();
        self.by_cut.get(&cut)?.get(&release.key)?.as_ref()
    }
}

pub(super) async fn verify(bucket: &dyn ObjectStore, keys: &dyn KeyService, installation: &DeploymentRecord,
    store: &str, release: &GarbageRecord, receipt: &str, version: &PhysicalObjectVersion) -> Result<bool> {
    let Some(data_key) = keys.existing_key(&installation.key_id()).await? else { return Ok(false); };
    let cipher = Cipher::new(&data_key)?;
    let receipt = ObjectKey::new(receipt)?;
    if !receipt.is_under(&ObjectKey::new(store)?) { return Err(super::changed()); }
    let header = match bucket.head_fresh(&receipt).await {
        Ok(header) => header,
        Err(Error::MissingObject(_)) => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let Some(installed) = read(bucket, &cipher, store, &receipt, header.size).await? else { return Ok(false); };
    if release.trie_floor.as_ref().is_none_or(|floor| installed.cut() != floor.cut) { return Ok(false); }
    let mut witnesses = installed.files().iter().filter(|file| file.key == release.key && file.matches_publication)
        .filter_map(|file| file.version.as_ref());
    Ok(witnesses.next() == Some(version) && witnesses.all(|observed| observed == version))
}

async fn read(bucket: &dyn ObjectStore, cipher: &Cipher, store: &str, receipt: &ObjectKey, size: u64) -> Result<Option<InstalledRetirement>> {
    let Some(relative) = receipt.as_str().strip_prefix(&format!("{store}/")) else { return Err(super::changed()); };
    let size = usize::try_from(size).ok().filter(|size| *size <= MAXIMUM_RECEIPT_BYTES + SEALED_OVERHEAD)
        .ok_or_else(super::changed)?;
    let sealed = match bucket.get_fresh_exact(receipt, size).await {
        Ok(sealed) => sealed,
        Err(Error::MissingObject(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let contents = cipher.open(relative, &sealed)?;
    Ok(Some(InstalledRetirement::decode(relative, &contents)?))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rtdb_directory::CatalogAdmissionFloor;
    use rtdb_format::{keys::CURRENT_STORAGE_VERSION, trie_retirement::{RetirementFile, RetirementIntent, intent_digest}};
    use rtdb_storage::MemoryStore;
    use rtdb_types::{CatalogCut, ReplicaPosition, S3ObjectNamespace, StorageEpoch, TableRef};

    use super::*;

    #[tokio::test]
    async fn a_legacy_release_recovers_only_its_authenticated_installed_version() {
        let files = tempfile::tempdir().unwrap();
        let keys = crate::keys::LocalKeyService::new(files.path().join("keys")).unwrap();
        let installation = DeploymentRecord::new("shop", "acme", vec![], 0);
        let material = keys.key(&installation.key_id()).await.unwrap();
        let cipher = Cipher::new(&material).unwrap();
        let owner = installation.storage_prefix();
        let store = format!("{owner}/v06");
        let selected = CatalogCut { storage_version: CURRENT_STORAGE_VERSION, storage_epoch: StorageEpoch(0),
            replica_epoch: StorageEpoch(0), through: ReplicaPosition(1) };
        let release = GarbageRecord { key: "tables/orders/data/l01-rc-b00.arrow".into(), released_at_ms: 0,
            trie_floor: Some(CatalogAdmissionFloor { residence: owner, object_namespace: uuid::Uuid::new_v4(),
                cut: CatalogCut { through: ReplicaPosition(2), ..selected } }) };
        let version = PhysicalObjectVersion { namespace: S3ObjectNamespace { bucket: "cell".into(), endpoint: None, region: None },
            key: format!("cell/{store}/{}", release.key), version: "original".into() };
        let intent = RetirementIntent { selected, table: TableRef::new("public", "orders"), tries: vec!["l01-rc-b00".into()],
            files: vec![RetirementFile { key: release.key.clone(), version: Some(version.clone()), terminal_publication: None, matches_publication: true }], after_reset: None };
        let encoded = serde_json::to_vec(&intent).unwrap();
        let relative = format!("{}/installed/{}", journal_directory(selected), intent_digest(&encoded));
        let source = ObjectKey::new(format!("{store}/{relative}")).unwrap();
        let mut bytes = 2i64.to_be_bytes().to_vec();
        bytes.extend(encoded);
        let bucket = MemoryStore::default();
        bucket.put(&source, cipher.seal_deterministic(&relative, &bytes).unwrap()).await.unwrap();
        let mut different_release = release.clone();
        different_release.trie_floor.as_mut().unwrap().cut.through = ReplicaPosition(3);
        let index = ReceiptIndex::load(&bucket, &keys, &installation, &store, [&release, &different_release].into_iter()).await.unwrap();
        let witness = index.get(&release).unwrap();
        assert_eq!(witness.receipt, source.as_str());
        assert_eq!(witness.version, version);
        assert!(index.get(&different_release).is_none());
        assert!(verify(&bucket, &keys, &installation, &store, &release, source.as_str(), &version).await.unwrap());
        let other = ObjectKey::new(format!("another-residence/v06/{relative}")).unwrap();
        bucket.put(&other, Bytes::from_static(b"another installation")).await.unwrap();
        assert!(verify(&bucket, &keys, &installation, &store, &release, other.as_str(), &version).await.is_err());
    }
}
