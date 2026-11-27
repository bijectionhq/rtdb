use std::collections::{BTreeMap, BTreeSet};

use rtdb_format::journal::{Checkpoint, ObjectId, ObjectKind, Pin, Record};
use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use crate::{ObjectKey, ObjectStore, VerifiedManifest, objects::{key, require_kind}};

/// The complete retained storage closure captured by the current writer or leader.
pub struct Reachability {
    database: Uuid,
    epoch: u64,
    objects: BTreeSet<ObjectId>,
    pins: BTreeMap<Uuid, Pin>,
    rewrite_files: bool,
    pinned_files: bool,
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
            pinned_files: false,
        };
        for id in objects {
            reachable.rewrite_files |= id.kind == ObjectKind::File;
            reachable.include(id)?;
        }
        Ok(reachable)
    }

    /// Resolve retained journal records from floor through a committed head after sealing.
    /// A checkpoint at the floor is the replay base: its pins are the pins held before the floor.
    pub fn from_records<'a>(
        database: Uuid,
        epoch: u64,
        retained: &[Record],
        manifests: impl IntoIterator<Item = &'a VerifiedManifest>,
    ) -> Result<Self> {
        let base = match retained.first() {
            Some(Record::Checkpoint(checkpoint)) => Some(checkpoint),
            _ => None,
        };
        Self::from_retained(database, epoch, base, retained, manifests)
    }

    /// Resolve retained records over an explicit replay base. The base holds
    /// the state before the floor, including its pins. A deferred base is
    /// announced by a later retained record, or not yet retained by a reader
    /// whose head precedes the announcement.
    pub fn from_retained<'a>(
        database: Uuid,
        epoch: u64,
        base: Option<&Checkpoint>,
        retained: &[Record],
        manifests: impl IntoIterator<Item = &'a VerifiedManifest>,
    ) -> Result<Self> {
        let manifests: BTreeMap<_, _> = manifests
            .into_iter()
            .map(|manifest| (manifest.id, manifest))
            .collect();
        let mut reachable = Self::from_roots(database, epoch, [])?;
        if let Some(base) = base {
            let image = reachable.include_checkpoint(&manifests, base)?;
            for pin in &image.manifest.pins {
                reachable.include_pin(&manifests, pin)?;
            }
        }

        for record in retained {
            match record {
                Record::Checkpoint(checkpoint) => {
                    let image = reachable.include_checkpoint(&manifests, checkpoint)?;
                    for pin in &image.manifest.pins {
                        let pinned = reachable.include_manifest(&manifests, pin.manifest)?;
                        reachable.record_pinned_files(pinned);
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
                | Record::Recovered { .. }
                | Record::Commissioned
                | Record::StorePurgeFloor { .. }
                | Record::StorePurgeReclaim { .. }
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

    pub fn contains_files(&self) -> bool {
        self.files().next().is_some()
    }

    pub fn files(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.objects.iter().copied().filter(|id| id.kind == ObjectKind::File)
    }

    pub fn requires_file_rewrite(&self) -> bool {
        self.rewrite_files
    }

    pub fn contains_pinned_files(&self) -> bool {
        self.pinned_files
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
        self.plan(listed, |id| id.epoch < self.epoch)
    }

    /// Same-epoch candidates are the exact checkpoints superseded by a certified trim.
    pub fn plan_store_history_gc<'a>(
        &self,
        listed: impl IntoIterator<Item = &'a ObjectKey>,
        retired_checkpoints: &BTreeSet<ObjectId>,
    ) -> Result<GcPlan> {
        let mut keys: BTreeSet<_> = self.plan(listed, |id| id.kind == ObjectKind::Manifest && id.epoch < self.epoch)
            .keys.into_iter().collect();
        for &id in retired_checkpoints {
            if id.database == self.database && id.epoch == self.epoch && id.kind == ObjectKind::Manifest
                && !self.contains(id)
            {
                keys.insert(key(id)?);
            }
        }
        Ok(GcPlan { keys: keys.into_iter().collect() })
    }

    fn plan<'a>(
        &self,
        listed: impl IntoIterator<Item = &'a ObjectKey>,
        eligible: impl Fn(ObjectId) -> bool,
    ) -> GcPlan {
        let keys: BTreeSet<_> = listed
            .into_iter()
            .filter_map(|key| {
                let id = key.as_str().parse::<ObjectId>().ok()?;
                (id.database == self.database && eligible(id) && !self.contains(id))
                    .then(|| key.clone())
            })
            .collect();
        GcPlan {
            keys: keys.into_iter().collect(),
        }
    }

    fn include_checkpoint<'a>(
        &mut self,
        manifests: &BTreeMap<ObjectId, &'a VerifiedManifest>,
        checkpoint: &Checkpoint,
    ) -> Result<&'a VerifiedManifest> {
        let image = self.include_manifest(manifests, checkpoint.manifest)?;
        self.rewrite_files |= !image.manifest.files.is_empty();
        if image.generation.number != checkpoint.key_generation {
            return Err(Anomaly::fault(
                "rtdb.store/checkpoint-generation",
                "Checkpoint and manifest use different key generations",
            ));
        }
        Ok(image)
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
        self.record_pinned_files(image);
        if self.pins.insert(pin.id, pin.clone()).is_some() {
            return Err(Anomaly::fault(
                "rtdb.store/duplicate-pin",
                format!("Pin {} is already held", pin.id),
            ));
        }
        Ok(())
    }

    fn record_pinned_files(&mut self, image: &VerifiedManifest) {
        let has_files = !image.manifest.files.is_empty();
        self.rewrite_files |= has_files;
        self.pinned_files |= has_files;
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
