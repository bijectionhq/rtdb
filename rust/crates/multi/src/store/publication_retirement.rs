use std::sync::Arc;

use rtdb_directory::DeploymentRecord;
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectStore, sealing::PUBLICATION_ID_BYTES};
use rtdb_types::{Anomaly, Result};

use crate::{
    keys::{KeyService, UnitScope},
    packing::{PackIndex, held_back, is_pack_key, read_index},
    reference::{physical_objects, stores},
};

const HEADER_BYTES: u64 =
    (PUBLICATION_ID_BYTES + crate::cipher::object::SEALED_HEADER_BYTES) as u64;

/// A census of physical copies under the local volume lock and the directory's
/// lineage reservation. The exact release remains the deletion authority.
pub(super) struct LocalSlots<'a> {
    keys: &'a dyn KeyService,
    peers: &'a [DeploymentRecord],
    io: &'a Arc<IoRegion>,
}

impl<'a> LocalSlots<'a> {
    pub(super) fn new(
        keys: &'a dyn KeyService,
        peers: &'a [DeploymentRecord],
        io: &'a Arc<IoRegion>,
    ) -> Self {
        Self { keys, peers, io }
    }

    pub(super) async fn withdraw_last(
        &self,
        bucket: &dyn ObjectStore,
        owner: &DeploymentRecord,
        target: &ObjectKey,
        name: &str,
        pack: Option<&PackIndex>,
    ) -> Result<()> {
        let Some(namespace) = owner.storage_namespaces else {
            return Ok(());
        };
        if self.keys.namespace_id().await? != namespace.keys {
            return Err(changed());
        }
        let sealing = super::directory_publications(self.keys, owner, Arc::clone(self.io))?;
        let members: Vec<_> = match pack {
            Some(pack) => pack
                .entries
                .iter()
                .map(|(name, entry)| (name.as_str(), entry.offset, entry.length))
                .collect(),
            None => vec![(name, 0, bucket.head_fresh(target).await?.size)],
        };
        for (name, offset, length) in members {
            let logical = ObjectKey::new(name)?;
            if !sealing.applies(&logical) {
                continue;
            }
            if length < HEADER_BYTES {
                return Err(changed());
            }
            let end = offset.checked_add(HEADER_BYTES).ok_or_else(changed)?;
            let header = bucket.observe_range(target, offset..end).await?.value;
            let origin = match sealing.header(&logical, header).await {
                Ok((_, origin)) => origin,
                Err(Error::ErasedUnderView) => continue,
                Err(error) => return Err(error.into()),
            };

            if !self
                .has_another_copy(bucket, owner, target, &logical, origin, pack.is_some())
                .await?
            {
                let slot = UnitScope::directory_publication_slot(
                    &owner.org,
                    &owner.lineage,
                    origin,
                    &logical,
                );
                self.keys.withdraw_unit_key(slot, self.io).await?;
            }
        }
        Ok(())
    }

    async fn has_another_copy(
        &self,
        bucket: &dyn ObjectStore,
        owner: &DeploymentRecord,
        target: &ObjectKey,
        logical: &ObjectKey,
        origin: [u8; PUBLICATION_ID_BYTES],
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
    expected: [u8; PUBLICATION_ID_BYTES],
) -> Result<bool> {
    let end = offset
        .checked_add(PUBLICATION_ID_BYTES as u64)
        .ok_or_else(changed)?;
    match bucket.observe_range(key, offset..end).await {
        Ok(prefix) if prefix.value.len() == PUBLICATION_ID_BYTES => {
            Ok(prefix.value.as_ref() == expected.as_slice())
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
