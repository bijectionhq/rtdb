use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use object_store::{
    Error as ProviderError, ObjectStore as ProviderStore, ObjectStoreExt, PutMode, PutOptions,
    UpdateVersion, path::Path,
};
use ring::digest::{Context, SHA256};
use rtdb_format::journal::{ObjectKind, RankedCatalog, Record};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{RemoteStore, provider_error};
use crate::{IoRegion, ObjectKey};

const PAGE_SIZE: usize = 100;
const MAX_ATTEMPTS: usize = 12;
const MAX_RECORD_BYTES: u64 = 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RankedAdmissionScope {
    pub incarnation: Uuid,
    pub activation_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClosedRankedAdmission {
    pub scope: RankedAdmissionScope,
    pub admitted: u64,
    pub inventory_sha256: [u8; 32],
}

#[derive(Debug)]
pub struct RankedAdmissionPage {
    pub catalogues: Vec<RankedCatalog>,
    pub next_after: Option<u64>,
}

/// The original store orders publication and closure on one conditional root.
#[derive(Clone)]
pub struct RankedAdmission {
    client: Arc<dyn ProviderStore>,
    region: Arc<IoRegion>,
    scope: RankedAdmissionScope,
    key: ObjectKey,
    path: Path,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    count: u64,
    inventory_sha256: [u8; 32],
    closed: bool,
}

impl RemoteStore {
    pub fn ranked_admission(&self, scope: RankedAdmissionScope) -> Result<RankedAdmission> {
        if scope.incarnation.is_nil() || scope.activation_sha256 == [0; 32] {
            return Err(invalid());
        }
        let identity = URL_SAFE_NO_PAD.encode(scope.activation_sha256);
        let key = ObjectKey::new(format!(
            "ranked-admission/v1/{identity}/{}",
            scope.incarnation
        ))?;
        Ok(RankedAdmission {
            client: self.objects.client.clone(),
            region: self.io.clone(),
            scope,
            path: self.objects.path(&key)?,
            key,
        })
    }
}

impl RankedAdmission {
    pub async fn commission(&self) -> Result<()> {
        let owner = self.clone();
        self.region
            .run_async(async move {
                Ok(async {
                    let root = owner.path.clone().join("root");
                    owner.create(&root, encode_root(Ledger::default())?).await?;
                    let (ledger, _) = owner.root().await?;
                    if ledger.closed {
                        return Err(closed());
                    }
                    Ok(())
                }
                .await)
            })
            .await?
    }

    /// An exact retry returns the original admission, including after closure.
    pub async fn publish(&self, catalogue: RankedCatalog) -> Result<u64> {
        let owner = self.clone();
        self.region
            .run_async(async move { Ok(owner.publish_catalogue(catalogue).await) })
            .await?
    }

    pub async fn close(&self) -> Result<ClosedRankedAdmission> {
        let owner = self.clone();
        self.region
            .run_async(async move { Ok(owner.close_admission().await) })
            .await?
    }

    /// Reads the next page of the provider's immutable admission prefix.
    pub async fn outcomes(
        &self,
        scope: ClosedRankedAdmission,
        after: u64,
    ) -> Result<RankedAdmissionPage> {
        let owner = self.clone();
        self.region
            .run_async(async move {
                Ok(async {
                    let (ledger, _) = owner.root().await?;
                    if !ledger.closed || scope != owner.receipt(ledger) || after > ledger.count {
                        return Err(invalid());
                    }
                    let end = after.saturating_add(PAGE_SIZE as u64).min(ledger.count);
                    let mut catalogues = Vec::with_capacity((end - after) as usize);
                    for sequence in after..end {
                        catalogues.push(owner.entry(sequence + 1).await?);
                    }
                    Ok(RankedAdmissionPage {
                        catalogues,
                        next_after: (end < ledger.count).then_some(end),
                    })
                }
                .await)
            })
            .await?
    }

    async fn publish_catalogue(&self, catalogue: RankedCatalog) -> Result<u64> {
        self.check_catalogue(catalogue)?;
        let encoded = Bytes::from(Record::RankedCatalog(catalogue).encode()?);
        let (initial, _) = self.root().await?;
        let mut checked = 0;
        if !initial.closed {
            let claim = self.path.clone().join("claims").join({
                let (position, system_time, generation) = coordinates(catalogue);
                format!("{position:016x}-{system_time:016x}-{generation:016x}")
            });
            if self.create(&claim, encoded.clone()).await? {
                checked = initial.count;
            } else if self.read_catalogue(&claim).await? != catalogue {
                return Err(conflicting());
            }
        }

        for _ in 0..MAX_ATTEMPTS {
            let (ledger, version) = self.root().await?;
            for sequence in checked..ledger.count {
                let admitted = self.entry(sequence + 1).await?;
                if coordinates(admitted) == coordinates(catalogue) {
                    return if admitted == catalogue {
                        Ok(sequence + 1)
                    } else {
                        Err(conflicting())
                    };
                }
            }
            checked = ledger.count;
            if ledger.closed {
                return Err(closed());
            }
            let sequence = ledger.count.checked_add(1).ok_or_else(invalid)?;
            let path = self.entry_path(sequence);
            self.create(&path, encoded.clone()).await?;
            let selected = self.read_catalogue(&path).await?;
            let mut hash = Context::new(&SHA256);
            hash.update(b"rtdb.ranked-admissions/v1\0");
            hash.update(&ledger.inventory_sha256);
            hash.update(&Record::RankedCatalog(selected).encode()?);
            let next = Ledger {
                count: sequence,
                inventory_sha256: hash.finish().as_ref().try_into().expect("SHA-256 width"),
                closed: false,
            };
            if self.replace_root(next, version).await? && selected == catalogue {
                return Ok(sequence);
            }
        }
        Err(contention())
    }

    async fn close_admission(&self) -> Result<ClosedRankedAdmission> {
        for _ in 0..MAX_ATTEMPTS {
            let (ledger, version) = self.root().await?;
            let closed = Ledger {
                closed: true,
                ..ledger
            };
            if ledger.closed || self.replace_root(closed, version).await? {
                return Ok(self.receipt(closed));
            }
        }
        Err(contention())
    }

    fn receipt(&self, ledger: Ledger) -> ClosedRankedAdmission {
        ClosedRankedAdmission {
            scope: self.scope,
            admitted: ledger.count,
            inventory_sha256: ledger.inventory_sha256,
        }
    }

    async fn root(&self) -> Result<(Ledger, UpdateVersion)> {
        let (bytes, version) = self.read(&self.path.clone().join("root")).await?;
        let ledger: Ledger = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if version.e_tag.is_none() || (ledger.count == 0) != (ledger.inventory_sha256 == [0; 32]) {
            return Err(invalid());
        }
        Ok((ledger, version))
    }

    async fn replace_root(&self, ledger: Ledger, version: UpdateVersion) -> Result<bool> {
        self.put(
            &self.path.clone().join("root"),
            encode_root(ledger)?,
            PutMode::Update(version),
        )
        .await
    }

    fn entry_path(&self, sequence: u64) -> Path {
        self.path
            .clone()
            .join("entries")
            .join(format!("{sequence:016x}"))
    }

    async fn entry(&self, sequence: u64) -> Result<RankedCatalog> {
        self.read_catalogue(&self.entry_path(sequence)).await
    }

    async fn read_catalogue(&self, path: &Path) -> Result<RankedCatalog> {
        let (bytes, _) = self.read(path).await?;
        let Record::RankedCatalog(catalogue) = Record::decode(&bytes)? else {
            return Err(invalid());
        };
        self.check_catalogue(catalogue)?;
        Ok(catalogue)
    }

    fn check_catalogue(&self, catalogue: RankedCatalog) -> Result<()> {
        if catalogue.manifest.database != self.scope.incarnation
            || catalogue.manifest.kind != ObjectKind::Manifest
            || catalogue.position == 0
            || catalogue.key_generation == 0
        {
            return Err(invalid());
        }
        Ok(())
    }

    async fn read(&self, path: &Path) -> Result<(Bytes, UpdateVersion)> {
        let reply = self
            .client
            .get(path)
            .await
            .map_err(|error| provider_error(&self.key, error))?;
        if reply.meta.size > MAX_RECORD_BYTES {
            return Err(invalid());
        }
        let version = UpdateVersion {
            e_tag: reply.meta.e_tag.clone(),
            version: reply.meta.version.clone(),
        };
        let bytes = reply
            .bytes()
            .await
            .map_err(|error| provider_error(&self.key, error))?;
        Ok((bytes, version))
    }

    async fn create(&self, path: &Path, bytes: Bytes) -> Result<bool> {
        self.put(path, bytes, PutMode::Create).await
    }

    async fn put(&self, path: &Path, bytes: Bytes, mode: PutMode) -> Result<bool> {
        let options = PutOptions {
            mode,
            ..Default::default()
        };
        match self.client.put_opts(path, bytes.into(), options).await {
            Ok(_) => Ok(true),
            Err(ProviderError::AlreadyExists { .. } | ProviderError::Precondition { .. }) => {
                Ok(false)
            }
            Err(error) => Err(provider_error(&self.key, error).into()),
        }
    }
}

fn encode_root(ledger: Ledger) -> Result<Bytes> {
    serde_json::to_vec(&ledger)
        .map(Bytes::from)
        .map_err(|_| invalid())
}

fn coordinates(catalogue: RankedCatalog) -> (u64, i64, u64) {
    (
        catalogue.position,
        catalogue.system_time,
        catalogue.key_generation,
    )
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.storage/ranked-admission",
        "The ranked publication or closed scope is invalid",
    )
}

fn conflicting() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.storage/ranked-admission",
        "This ranked cut has a different exact publication",
    )
}

fn closed() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.storage/ranked-admission-closed",
        "The original ranked publication scope is closed",
    )
}

fn contention() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Busy,
        "rtdb.storage/ranked-admission",
        "The original ranked publication scope kept changing",
    )
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use rtdb_format::journal::ObjectId;

    use super::*;

    fn open(provider: Arc<InMemory>, scope: RankedAdmissionScope) -> RankedAdmission {
        RemoteStore::new(provider, Some(ObjectKey::new("serving").unwrap()))
            .ranked_admission(scope)
            .unwrap()
    }

    fn scope() -> RankedAdmissionScope {
        RankedAdmissionScope {
            incarnation: Uuid::new_v4(),
            activation_sha256: [3; 32],
        }
    }

    fn catalogue(scope: RankedAdmissionScope, position: u64) -> RankedCatalog {
        RankedCatalog {
            manifest: ObjectId {
                database: scope.incarnation,
                kind: ObjectKind::Manifest,
                epoch: 1,
                seq: position,
                digest: [7; 32],
            },
            position,
            system_time: -1,
            key_generation: 1,
        }
    }

    #[tokio::test]
    async fn ranked_admission_recovers_the_closed_prefix_without_operation_ids() {
        let provider = Arc::new(InMemory::new());
        let scope = scope();
        let writer = open(provider.clone(), scope);
        assert!(writer.close().await.is_err());
        assert!(writer.publish(catalogue(scope, 1)).await.is_err());
        writer.commission().await.unwrap();
        for position in 1..=101 {
            assert_eq!(
                writer.publish(catalogue(scope, position)).await.unwrap(),
                position
            );
        }

        let custodian = open(provider.clone(), scope);
        let closed = custodian.close().await.unwrap();
        assert_eq!(closed.admitted, 101);
        assert_eq!(custodian.close().await.unwrap(), closed);
        assert!(custodian.commission().await.is_err());
        assert!(writer.publish(catalogue(scope, 102)).await.is_err());
        assert_eq!(writer.publish(catalogue(scope, 20)).await.unwrap(), 20);
        let mut substituted = catalogue(scope, 20);
        substituted.manifest.digest = [8; 32];
        assert!(writer.publish(substituted).await.is_err());

        let first = custodian.outcomes(closed, 0).await.unwrap();
        assert_eq!(
            first.catalogues,
            (1..=100).map(|at| catalogue(scope, at)).collect::<Vec<_>>()
        );
        let last = custodian
            .outcomes(closed, first.next_after.unwrap())
            .await
            .unwrap();
        assert_eq!(last.catalogues, [catalogue(scope, 101)]);
        assert_eq!(last.next_after, None);
        let wrong = open(
            provider,
            RankedAdmissionScope {
                activation_sha256: [8; 32],
                ..scope
            },
        );
        wrong.commission().await.unwrap();
        wrong.close().await.unwrap();
        assert!(wrong.outcomes(closed, 0).await.is_err());
    }

    #[tokio::test]
    async fn ranked_admission_close_excludes_an_uncommitted_provider_slot() {
        let provider = Arc::new(InMemory::new());
        let scope = scope();
        let writer = open(provider.clone(), scope);
        writer.commission().await.unwrap();
        writer.publish(catalogue(scope, 1)).await.unwrap();

        let interrupted = catalogue(scope, 2);
        let bytes = Bytes::from(Record::RankedCatalog(interrupted).encode().unwrap());
        let (_, earlier_version) = writer.root().await.unwrap();
        assert!(writer.create(&writer.entry_path(2), bytes).await.unwrap());

        let custodian = open(provider, scope);
        let closed = custodian.close().await.unwrap();
        let proposed = Ledger {
            count: 2,
            inventory_sha256: [9; 32],
            closed: false,
        };
        assert!(
            !writer
                .replace_root(proposed, earlier_version)
                .await
                .unwrap()
        );
        assert!(writer.publish(interrupted).await.is_err());
        assert_eq!(
            custodian.outcomes(closed, 0).await.unwrap().catalogues,
            [catalogue(scope, 1)]
        );
    }

    #[tokio::test]
    async fn ranked_admission_retries_a_single_provider_decision_under_concurrent_writers() {
        let provider = Arc::new(InMemory::new());
        let scope = scope();
        let first = open(provider.clone(), scope);
        let retry = open(provider.clone(), scope);
        let contender = open(provider, scope);
        first.commission().await.unwrap();
        let order = catalogue(scope, 1);
        let other = catalogue(scope, 2);
        let (first, second, third) = tokio::join!(
            first.publish(order),
            retry.publish(order),
            contender.publish(other)
        );
        assert_eq!(first.unwrap(), second.unwrap());
        let closed = contender.close().await.unwrap();
        let observed = contender.outcomes(closed, 0).await.unwrap().catalogues;
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[(third.unwrap() - 1) as usize], other);
    }
}
