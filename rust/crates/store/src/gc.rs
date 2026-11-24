use std::collections::{BTreeMap, BTreeSet};

use rtdb_format::journal::{ObjectId, ObjectKind, Pin, Record};
use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use crate::{ObjectKey, ObjectStore, VerifiedManifest, objects::require_kind};

/// The complete retained storage closure captured by the current writer or leader.
pub struct Reachability {
    database: Uuid,
    epoch: u64,
    objects: BTreeSet<ObjectId>,
    pins: BTreeMap<Uuid, Pin>,
    rewrite_files: bool,
}

impl Reachability {
    /// Build from already resolved roots, including every protected pin and retained record's dependencies.
    pub fn from_roots(
        database: Uuid,
        epoch: u64,
        objects: impl IntoIterator<Item = ObjectId>,
    ) -> Result<Self> {
        let mut reachable = Self {
            database,
            epoch,
            objects: BTreeSet::new(),
            pins: BTreeMap::new(),
            rewrite_files: false,
        };
        for id in objects {
            reachable.rewrite_files |= id.kind == ObjectKind::File;
            reachable.include(id)?;
        }
        Ok(reachable)
    }

    /// Resolve retained journal records from floor through a committed head after sealing.
    pub fn from_records<'a>(
        database: Uuid,
        epoch: u64,
        retained: &[Record],
        manifests: impl IntoIterator<Item = &'a VerifiedManifest>,
    ) -> Result<Self> {
        let manifests: BTreeMap<_, _> = manifests
            .into_iter()
            .map(|manifest| (manifest.id, manifest))
            .collect();
        let mut reachable = Self::from_roots(database, epoch, [])?;

        for (index, record) in retained.iter().enumerate() {
            match record {
                Record::Checkpoint(checkpoint) => {
                    let image = reachable.include_manifest(&manifests, checkpoint.manifest)?;
                    reachable.rewrite_files |= !image.manifest.files.is_empty();
                    if image.generation.number != checkpoint.key_generation {
                        return Err(Anomaly::fault(
                            "rtdb.store/checkpoint-generation",
                            "Checkpoint and manifest use different key generations",
                        ));
                    }
                    for pin in &image.manifest.pins {
                        if index == 0 {
                            reachable.include_pin(&manifests, pin)?;
                        } else {
                            let pinned = reachable.include_manifest(&manifests, pin.manifest)?;
                            reachable.rewrite_files |= !pinned.manifest.files.is_empty();
                        }
                    }
                }
                Record::RankedCatalog(catalog) => {
                    let image = reachable.include_manifest(&manifests, catalog.manifest)?;
                    if image.generation.number != catalog.key_generation || !image.manifest.pins.is_empty() {
                        return Err(Anomaly::fault("rtdb.store/ranked-catalog",
                            "Ranked catalogue root has an unexpected generation or pin"));
                    }
                }
                Record::Pin(pin) => reachable.include_pin(&manifests, pin)?,
                Record::Unpin { id } => {
                    reachable.pins.remove(id);
                }
                Record::Tx { objects, .. } => {
                    for &id in objects {
                        match id.kind {
                            ObjectKind::File => {
                                reachable.include(id)?;
                                reachable.rewrite_files = true;
                            }
                            ObjectKind::Manifest => {
                                let image = reachable.include_manifest(&manifests, id)?;
                                reachable.rewrite_files |= !image.manifest.files.is_empty();
                            }
                        }
                    }
                }
                Record::Lease { .. }
                | Record::WriterRelease { .. }
                | Record::DeploymentRetirement { .. }
                | Record::KeyRotate { .. }
                | Record::KeyRetirement { .. }
                | Record::Settle { .. }
                | Record::Refuse { .. } => {}
            }
        }
        Ok(reachable)
    }

    pub fn contains(&self, id: ObjectId) -> bool {
        self.objects.contains(&id)
    }

    pub fn requires_file_rewrite(&self) -> bool {
        self.rewrite_files
    }

    /// Active pins reconstructed by `from_records`.
    pub fn pins(&self) -> impl Iterator<Item = &Pin> {
        self.pins.values()
    }

    /// Check the full object closure of a proposed publication against the writer's current view.
    /// Current-epoch entries must come from this writer's allocator and have completed upload.
    pub fn validate_publication(&self, objects: impl IntoIterator<Item = ObjectId>) -> Result<()> {
        for id in objects {
            self.validate_scope(id)?;
            if id.epoch < self.epoch && !self.contains(id) {
                return Err(Anomaly::incorrect(
                    "rtdb.store/unreachable-publication",
                    format!("Publication would reintroduce unreachable object {id}"),
                ));
            }
        }
        Ok(())
    }

    /// Plan against an inventory of canonical keys. Incomplete inventory defers orphan cleanup.
    pub fn plan_gc<'a>(&self, listed: impl IntoIterator<Item = &'a ObjectKey>) -> GcPlan {
        let keys: BTreeSet<_> = listed
            .into_iter()
            .filter_map(|key| {
                let id = key.as_str().parse::<ObjectId>().ok()?;
                (id.database == self.database && id.epoch < self.epoch && !self.contains(id))
                    .then(|| key.clone())
            })
            .collect();
        GcPlan {
            keys: keys.into_iter().collect(),
        }
    }

    fn include_manifest<'a>(
        &mut self,
        manifests: &BTreeMap<ObjectId, &'a VerifiedManifest>,
        id: ObjectId,
    ) -> Result<&'a VerifiedManifest> {
        require_kind(id, ObjectKind::Manifest)?;
        self.validate_scope(id)?;
        let manifest = manifests.get(&id).ok_or_else(|| {
            Anomaly::fault(
                "rtdb.store/missing-manifest",
                format!("Reachability requires manifest {id}"),
            )
        })?;
        if self.objects.insert(id) {
            for &file in &manifest.manifest.files {
                require_kind(file, ObjectKind::File)?;
                self.include(file)?;
            }
        }
        Ok(manifest)
    }

    fn include_pin(
        &mut self,
        manifests: &BTreeMap<ObjectId, &VerifiedManifest>,
        pin: &Pin,
    ) -> Result<()> {
        let image = self.include_manifest(manifests, pin.manifest)?;
        self.rewrite_files |= !image.manifest.files.is_empty();
        if self.pins.insert(pin.id, pin.clone()).is_some() {
            return Err(Anomaly::fault(
                "rtdb.store/duplicate-pin",
                format!("Pin {} is already held", pin.id),
            ));
        }
        Ok(())
    }

    fn include(&mut self, id: ObjectId) -> Result<()> {
        self.validate_scope(id)?;
        self.objects.insert(id);
        Ok(())
    }

    fn validate_scope(&self, id: ObjectId) -> Result<()> {
        if id.database != self.database || id.epoch > self.epoch {
            return Err(Anomaly::fault(
                "rtdb.store/reachability-scope",
                format!("Object {id} is outside the collector scope"),
            ));
        }
        Ok(())
    }
}

/// Idempotent exact-key deletes that may complete after writer handoff.
/// Publication after planning must obey `Reachability::validate_publication`.
pub struct GcPlan {
    keys: Vec<ObjectKey>,
}

impl GcPlan {
    pub fn keys(&self) -> &[ObjectKey] {
        &self.keys
    }

    pub async fn execute(&self, store: &dyn ObjectStore) -> Result<()> {
        for key in &self.keys {
            match store.delete(key).await {
                Ok(()) | Err(rtdb_storage::Error::MissingObject(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
