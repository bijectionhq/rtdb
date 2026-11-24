use std::{collections::BTreeSet, sync::Arc};

use bytes::Bytes;
use rtdb_crypt::{Generation, Kms, Plaintext};
use rtdb_format::journal::{Manifest, ObjectKind, RankedCatalog, Record};
use rtdb_index::branches::{BranchCut, BranchIndex};
use rtdb_store::{ObjectStore, read_manifest, read_observed_file};
use rtdb_types::{
    Anomaly, AnomalyCategory, MemoryBudget, MemoryReservation, PhysicalObjectVersion, Result,
    Timestamp, rtdb_v1::Basis,
};
use serde::{Deserialize, Serialize};

use super::MultiDatabase;

const MAX_CATALOG_BYTES: usize = 1 << 20;
const MAX_FILES_BYTES: usize = 256 << 20;

pub struct RankedBuild {
    pub index: BranchIndex,
    cut: BranchCut,
    epoch: u64,
    generation: u64,
}

pub struct RankedPublication {
    record: RankedCatalog,
    store: Arc<dyn ObjectStore>,
    kms: Arc<dyn Kms>,
}

pub struct RankedFiles {
    pub inventory: Vec<u8>,
    pub files: Vec<(Bytes, Option<PhysicalObjectVersion>)>,
    pub epoch: u64,
    pub generation: u64,
    pub cut: BranchCut,
    _inventory_memory: Arc<MemoryReservation>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    version: u8,
    files: Vec<File>,
    primary: Vec<u8>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct File {
    plaintext_bytes: usize,
    ciphertext_bytes: usize,
    version: Option<PhysicalObjectVersion>,
}

struct HeldFile {
    plaintext: Plaintext,
    _memory: Arc<MemoryReservation>,
}

impl AsRef<[u8]> for HeldFile {
    fn as_ref(&self) -> &[u8] {
        &self.plaintext.bytes
    }
}

impl MultiDatabase {
    pub fn ranked_build(&self) -> Result<Option<RankedBuild>> {
        let lease = self.authority()?;
        let state = self.state();
        let index = state.transactions.index();
        let cut = index.head();
        if cut.basis.position() == 0
            || index.has_custody_groups()
            || index.has_visibility_history()
            || state.transactions.pending_erasure().is_some()
            || state.transactions.recover_pending().is_some()
            || self.ranked_record(cut).is_some()
        {
            return Ok(None);
        }
        Ok(Some(RankedBuild {
            index: index.clone(),
            cut,
            epoch: lease.epoch.0,
            generation: state.generation,
        }))
    }

    pub(super) async fn publish_ranked_catalog(
        &mut self,
        build: RankedBuild,
        primary: Vec<u8>,
        files: Vec<Bytes>,
        memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        self.refresh().await?;
        let lease = self.authority()?;
        self.require_armed_rotation()?;
        let Some(current) = self.ranked_build()? else {
            return Ok(());
        };
        if (build.cut, build.epoch, build.generation)
            != (current.cut, current.epoch, current.generation)
        {
            return Ok(());
        }
        let total = files
            .iter()
            .fold(0usize, |total, file| total.saturating_add(file.len()));
        if primary.is_empty()
            || primary.len() > MAX_CATALOG_BYTES
            || files.is_empty()
            || total > MAX_FILES_BYTES
            || files.iter().any(Bytes::is_empty)
        {
            return Err(uncovered());
        }
        let mut members = Vec::with_capacity(files.len());
        let mut inventory = Inventory {
            version: 1,
            files: Vec::with_capacity(files.len()),
            primary,
        };
        for file in files {
            self.require_authority(lease)?;
            let _scratch = memory.reserve(file.len().saturating_mul(2).saturating_add(4096))?;
            let object = self
                .writer
                .as_mut()
                .ok_or_else(super::no_writer)?
                .objects
                .file(self.stream.kms.as_ref(), build.generation, &file)
                .await?;
            let (ciphertext_bytes, version) = object.put_observed(self.store.as_ref()).await?;
            members.push(object.id());
            inventory.files.push(File {
                plaintext_bytes: file.len(),
                ciphertext_bytes,
                version,
            });
        }
        self.require_authority(lease)?;
        let payload = serde_json::to_vec(&inventory).map_err(invalid)?;
        if payload.len() > MAX_CATALOG_BYTES {
            return Err(uncovered());
        }
        let manifest = self
            .writer
            .as_mut()
            .ok_or_else(super::no_writer)?
            .objects
            .manifest(
                self.stream.kms.as_ref(),
                build.generation,
                &Manifest {
                    files: members,
                    pins: Vec::new(),
                    payload,
                },
            )
            .await?;
        manifest.put(self.store.as_ref()).await?;
        self.append_control(
            lease,
            Record::RankedCatalog(RankedCatalog {
                manifest: manifest.id(),
                position: build.cut.basis.position(),
                system_time: build.cut.system_time.0,
                key_generation: build.generation,
            }),
        )
        .await?;
        Ok(())
    }

    pub fn ranked_publication(&self, cut: BranchCut) -> Option<RankedPublication> {
        let record = self.ranked_record(cut)?;
        Some(RankedPublication {
            record,
            store: Arc::clone(&self.store),
            kms: Arc::clone(&self.stream.kms),
        })
    }

    fn ranked_record(&self, cut: BranchCut) -> Option<RankedCatalog> {
        let state = self.committed.as_ref()?;
        if cut.basis.incarnation() != self.incarnation()
            || state.transactions.pending_erasure().is_some()
        {
            return None;
        }
        state.records.iter().rev().find_map(|record| match record {
            Record::RankedCatalog(catalog)
                if catalog.position == cut.basis.position()
                    && catalog.system_time == cut.system_time.0
                    && catalog.key_generation == state.generation =>
            {
                Some(*catalog)
            }
            _ => None,
        })
    }
}

impl RankedPublication {
    pub async fn open(&self, memory: &Arc<MemoryBudget>) -> Result<RankedFiles> {
        let record = self.record;
        let generation = Generation {
            database: record.manifest.database,
            number: record.key_generation,
        };
        self.kms.check_readable(generation).await?;
        let inventory_memory = memory.reserve(MAX_CATALOG_BYTES * 4)?;
        let image = read_manifest(self.store.as_ref(), self.kms.as_ref(), record.manifest).await?;
        let manifest = image.contents();
        if image.generation != generation
            || !manifest.pins.is_empty()
            || manifest.payload.len() > MAX_CATALOG_BYTES
        {
            return Err(uncovered());
        }
        let inventory: Inventory = serde_json::from_slice(&manifest.payload).map_err(invalid)?;
        if inventory.version != 1
            || inventory.files.len() != manifest.files.len()
            || inventory.files.is_empty()
            || manifest.files.iter().collect::<BTreeSet<_>>().len() != manifest.files.len()
            || inventory.files.iter().fold(0usize, |total, file| {
                total.saturating_add(file.plaintext_bytes)
            }) > MAX_FILES_BYTES
        {
            return Err(uncovered());
        }
        let mut files = Vec::with_capacity(manifest.files.len());
        for (&id, file) in manifest.files.iter().zip(inventory.files) {
            if id.database != generation.database
                || id.kind != ObjectKind::File
                || id.epoch != record.manifest.epoch
                || file.plaintext_bytes == 0
                || !(file.plaintext_bytes..=file.plaintext_bytes.saturating_add(4096))
                    .contains(&file.ciphertext_bytes)
            {
                return Err(uncovered());
            }
            let held = memory.reserve(file.plaintext_bytes)?;
            let _scratch = memory.reserve(file.ciphertext_bytes.saturating_mul(2))?;
            let plaintext = read_observed_file(
                self.store.as_ref(),
                self.kms.as_ref(),
                id,
                file.ciphertext_bytes,
                file.version.as_ref(),
            )
            .await?;
            if plaintext.generation != generation || plaintext.bytes.len() != file.plaintext_bytes {
                return Err(uncovered());
            }
            files.push((
                Bytes::from_owner(HeldFile {
                    plaintext,
                    _memory: held,
                }),
                file.version,
            ));
        }
        self.kms.check_readable(generation).await?;
        Ok(RankedFiles {
            inventory: inventory.primary,
            files,
            epoch: record.manifest.epoch,
            generation: generation.number,
            cut: BranchCut {
                basis: Basis::new(*generation.database.as_bytes(), record.position),
                system_time: Timestamp(record.system_time),
            },
            _inventory_memory: inventory_memory,
        })
    }
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.db/ranked-catalog", error.to_string())
}

fn uncovered() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.db/ranked-catalog",
        "The committed ranked catalogue is unavailable",
    )
}
