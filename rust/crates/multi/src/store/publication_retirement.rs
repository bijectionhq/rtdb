use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use rtdb_s3::VersionedInventory;
use super::replay_retirement::{ObservedPack, PackStorage};

use rtdb_directory::DeploymentRecord;
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectStore, sealing::PUBLICATION_ID_BYTES};
use rtdb_types::{Anomaly, Result, ServingPublication};

use crate::{
    keys::KeyService,
    packing::{held_back, is_pack_key, read_index},
    reference::{physical_objects, stores},
};

const HEADER_BYTES: u64 =
    (PUBLICATION_ID_BYTES + crate::cipher::object::SEALED_HEADER_BYTES) as u64;

/// Finds aliases of an immutable serving slot inside the directory lineage.
/// The requesting retirement owns the lineage reservation and its provider fence.
pub(super) struct PublicationCensus<'a> {
    keys: &'a dyn KeyService,
    peers: &'a [DeploymentRecord],
    io: &'a Arc<IoRegion>,
}

impl<'a> PublicationCensus<'a> {
    pub(super) fn new(
        keys: &'a dyn KeyService,
        peers: &'a [DeploymentRecord],
        io: &'a Arc<IoRegion>,
    ) -> Self {
        Self { keys, peers, io }
    }

    pub(super) async fn pack_plan(
        &self, bucket: &dyn ObjectStore, owner: &DeploymentRecord, pack: &ObservedPack<'_>,
        provider: &VersionedInventory,
    ) -> Result<Bytes> {
        let sealing = super::directory_publications(self.keys, owner, Arc::clone(self.io))?;
        let mut slots = BTreeMap::new();
        for (name, entry) in &pack.index.entries {
            let logical = ObjectKey::new(name)?;
            if !sealing.applies(&logical) { continue; }
            if entry.length < HEADER_BYTES { return Err(changed()); }
            let end = entry.offset.checked_add(HEADER_BYTES).ok_or_else(changed)?;
            let header = pack.read_range(entry.offset..end).await?;
            let (_, unit) = sealing.header(&logical, header).await?;
            slots.insert(name.clone(), unit);
        }
        match &pack.storage {
            PackStorage::Raw { version: Some(version), .. } => {
                for peer in self.peers.iter().filter(|peer| peer.org == owner.org && peer.lineage == owner.lineage) {
                    let residence = ObjectKey::new(peer.storage_prefix())?;
                    for store in provider.child_directories(&residence).await? {
                        let root = format!("{residence}/{store}");
                        self.exclude_versioned_aliases(provider, &root, version, &mut slots).await?;
                    }
                }
            }
            PackStorage::Terminal { .. } => {
                let candidates = std::mem::take(&mut slots);
                for (name, unit) in candidates {
                    let logical = ObjectKey::new(&name)?;
                    if !self.has_another_copy(bucket, owner, pack.key, &logical, unit, true).await? {
                        slots.insert(name, unit);
                    }
                }
            }
            _ => return Err(changed()),
        }
        let slots = slots.into_iter().collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&slots).map_err(|_| changed())?;
        if bytes.len() > 16 << 20 { return Err(changed()); }
        Ok(Bytes::from(bytes))
    }

    async fn exclude_versioned_aliases(
        &self, provider: &VersionedInventory, root: &str, retiring: &rtdb_types::PhysicalObjectVersion,
        slots: &mut BTreeMap<String, ServingPublication>,
    ) -> Result<()> {
        if slots.is_empty() { return Ok(()); }
        let mut excluded = Vec::new();
        for (name, expected) in slots.iter() {
            let copy = ObjectKey::new(format!("{root}/{name}"))?;
            for version in provider.data_versions(&copy).await? {
                if &version == retiring { continue; }
                let prefix = provider.read_version_prefix(&copy, &version, PUBLICATION_ID_BYTES as u64).await?;
                if ServingPublication::from_prefix(&prefix).ok_or_else(changed)? == *expected {
                    excluded.push(name.clone());
                    break;
                }
            }
        }
        for name in excluded.drain(..) { slots.remove(&name); }
        let packs = ObjectKey::new(format!("{root}/{}", crate::packing::PACK_DIRECTORY))?;
        for (copy, version, size) in provider.data_versions_under(&packs).await? {
            if &version == retiring { continue; }
            let index = crate::packing::read_pack_index_with(&copy, size, 16 << 20,
                |range| provider.read_version_range(&copy, &version, range)).await?;
            for (name, entry) in &index.entries {
                let Some(expected) = slots.get(name) else { continue; };
                if entry.length < HEADER_BYTES { return Err(changed()); }
                let end = entry.offset.checked_add(PUBLICATION_ID_BYTES as u64).ok_or_else(changed)?;
                let prefix = provider.read_version_range(&copy, &version, entry.offset..end).await?;
                if ServingPublication::from_prefix(&prefix).ok_or_else(changed)? == *expected {
                    excluded.push(name.clone());
                }
            }
            for name in excluded.drain(..) { slots.remove(&name); }
        }
        Ok(())
    }

    pub(super) async fn has_another_copy(
        &self,
        bucket: &dyn ObjectStore,
        owner: &DeploymentRecord,
        target: &ObjectKey,
        logical: &ObjectKey,
        origin: ServingPublication,
        packed: bool,
    ) -> Result<bool> {
        let may_be_packed =
            packed || held_back(logical.as_str()) || logical.as_str().starts_with("blocks/");
        for peer in self
            .peers
            .iter()
            .filter(|peer| peer.org == owner.org && peer.lineage == owner.lineage)
        {
            for store in stores(bucket, &peer.storage_prefix()).await? {
                let loose = ObjectKey::new(format!("{store}/{logical}"))?;
                if &loose != target && has_origin(bucket, &loose, 0, origin).await? {
                    return Ok(true);
                }
                if !may_be_packed {
                    continue;
                }
                for name in physical_objects(bucket, &store)
                    .await?
                    .into_iter()
                    .filter(|name| is_pack_key(name))
                {
                    let copy = ObjectKey::new(format!("{store}/{name}"))?;
                    if &copy == target {
                        continue;
                    }
                    let index = read_index(bucket, &copy).await?;
                    if let Some(member) = index.entries.get(logical.as_str()) {
                        if member.length < HEADER_BYTES {
                            return Err(changed());
                        }
                        if has_origin(bucket, &copy, member.offset, origin).await? {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        Ok(false)
    }
}

async fn has_origin(
    bucket: &dyn ObjectStore,
    key: &ObjectKey,
    offset: u64,
    expected: ServingPublication,
) -> Result<bool> {
    let end = offset
        .checked_add(PUBLICATION_ID_BYTES as u64)
        .ok_or_else(changed)?;
    match bucket.observe_range(key, offset..end).await {
        Ok(prefix) if prefix.value.len() == PUBLICATION_ID_BYTES => {
            Ok(ServingPublication::from_prefix(&prefix.value).ok_or_else(changed)? == expected)
        }
        Ok(_) => Err(changed()),
        Err(Error::MissingObject(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn changed() -> Anomaly {
    Anomaly::fault(
        "rtdb.multi/serving-slot-census",
        "The loose or packed serving slot has inconsistent physical custody",
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rtdb_storage::MemoryStore;

    use super::*;

    #[tokio::test]
    async fn an_alias_in_a_relocated_pack_retains_the_original_slot() {
        let owner = DeploymentRecord::new("parent", "acme", Vec::new(), 0);
        let mut fork = DeploymentRecord::new("fork", "acme", Vec::new(), 0);
        fork.lineage.clone_from(&owner.lineage);
        let bucket = MemoryStore::default();
        let name = ObjectKey::new("blocks/b00.binpb").unwrap();
        let target =
            ObjectKey::new(format!("{}/v06/~packs/b00.pack", owner.storage_prefix())).unwrap();
        let relocated =
            ObjectKey::new(format!("{}/v06/~packs/b00.pack", fork.storage_prefix())).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let keys = crate::keys::LocalKeyService::new(directory.path().join("keys")).unwrap();
        let io = IoRegion::new();
        let peers = [owner.clone(), fork];
        let census = PublicationCensus::new(&keys, &peers, &io);
        let origin = ServingPublication { origin: [7; 32], attempt: uuid::Uuid::new_v4() };
        let mut member = vec![0; HEADER_BYTES as usize];
        member[..PUBLICATION_ID_BYTES].copy_from_slice(&origin.prefix());
        let (body, _) = crate::packing::encode_pack(
            &BTreeMap::from([(
                name.to_string(),
                Bytes::from(member),
            )]),
            Vec::new(),
            0,
        );
        bucket.put(&relocated, body).await.unwrap();

        assert!(
            census
                .has_another_copy(&bucket, &owner, &target, &name, origin, true)
                .await
                .unwrap()
        );
        assert!(
            !census
                .has_another_copy(
                    &bucket,
                    &owner,
                    &target,
                    &name,
                    ServingPublication { attempt: uuid::Uuid::new_v4(), ..origin },
                    true
                )
                .await
                .unwrap()
        );
    }
}
