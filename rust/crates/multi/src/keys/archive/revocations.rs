use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::ErrorKind,
    path::{Path, PathBuf},
};

use ring::digest::{Context, SHA256};
use uuid::Uuid;

use super::{expire_locked, read_schedule, schedule_conflict};
use crate::keys::{
    ArchiveRevocation, ArchiveRevocationOutcome, ArchiveRevocationPage, ArchiveRevocationScope,
    DESTROYED, KEY_BYTES, LocalKeyService, UnitScope, io, is_destroyed, malformed,
    require_general_key_namespace,
};
use rtdb_storage::IoRegion;
use rtdb_types::{Anomaly, AnomalyCategory, Result};

const PAGE_SIZE: usize = 100;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RevocationRecord {
    request: ArchiveRevocation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scheduled_at_ms: Option<i64>,
    revoked_at_ms: Option<i64>,
}

pub(super) fn read_revocation(path: &Path) -> Result<Option<RevocationRecord>> {
    match fs::read(path.with_extension("revocation")) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| malformed()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io(error)),
    }
}

pub(super) fn revocation_deadline(path: &Path) -> Result<Option<i64>> {
    if path
        .file_stem()
        .is_some_and(|name| name.as_encoded_bytes().starts_with(b"checkpoint-"))
        && let Some(record) = read_revocation(path)?
        && record.scheduled_at_ms.is_some()
    {
        return Ok(Some(record.request.not_before_ms));
    }
    read_schedule(path)
}

fn capture_key(id: &str) -> Option<(ArchiveRevocationScope, Uuid)> {
    let binding = id.strip_prefix("checkpoint-")?;
    let deployment = Uuid::parse_str(binding.get(..36)?).ok()?;
    let (incarnation, capture) = binding.get(36..)?.strip_prefix('-')?.split_once('-')?;
    let incarnation = incarnation.parse().ok()?;
    let capture = Uuid::parse_str(capture).ok()?;
    let scope = ArchiveRevocationScope {
        deployment,
        incarnation,
    };
    (id == scope.capture_key(capture)).then_some((scope, capture))
}

fn admission_closed() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.multi/archive-vault",
        "Archive revocation admission is closed",
    )
}

fn incomplete_inventory() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/archive-vault",
        "The archive vault has legacy revocations without exact retained orders",
    )
}

fn validate_order(id: &str, request: ArchiveRevocation) -> Result<()> {
    if request.operation_id.is_nil()
        || request.request_sha256 == [0; 32]
        || request.not_before_ms <= 0
        || capture_key(id).is_some_and(|(_, capture)| capture != request.operation_id)
    {
        return Err(Anomaly::incorrect(
            "rtdb.multi/archive-vault",
            "Invalid archive revocation order",
        ));
    }
    Ok(())
}

pub(in crate::keys) struct LockedScope {
    vault: LocalKeyService,
    scope: ArchiveRevocationScope,
    marker: PathBuf,
    _lock: File,
}

impl LockedScope {
    fn new(service: &LocalKeyService, scope: ArchiveRevocationScope) -> Result<Self> {
        service.require_service_root()?;
        if scope.deployment.is_nil() {
            return Err(Anomaly::incorrect(
                "rtdb.multi/archive-vault",
                "Invalid archive revocation scope",
            ));
        }
        let vault = service.archive_vault()?;
        let marker = vault.directory.join(format!(
            "archive-revocations-{}-{}.closed",
            scope.deployment, scope.incarnation
        ));
        let lock = vault.lock_key(&marker)?;
        Ok(Self {
            vault,
            scope,
            marker,
            _lock: lock,
        })
    }

    fn closed(&self) -> Result<Option<[u8; 32]>> {
        match fs::read(&self.marker) {
            Ok(bytes) => Ok(Some(bytes.try_into().map_err(|_| malformed())?)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io(error)),
        }
    }

    pub(in crate::keys) fn require_open(&self) -> Result<()> {
        if self.closed()?.is_some() {
            return Err(admission_closed());
        }
        Ok(())
    }

    pub(in crate::keys) fn require_legacy_withdrawal(&self, path: &Path) -> Result<()> {
        if let Some(record) = read_revocation(path)? {
            return if record.revoked_at_ms.is_some() {
                Ok(())
            } else {
                Err(schedule_conflict())
            };
        }
        self.require_open()
    }

    fn inventory(&self, after: Option<Uuid>) -> Result<([u8; 32], ArchiveRevocationPage)> {
        let mut keys = BTreeSet::new();
        for entry in fs::read_dir(&self.vault.directory).map_err(io)? {
            let path = entry.map_err(io)?.path();
            if !matches!(
                path.extension().and_then(|ext| ext.to_str()),
                Some("key" | "revocation" | "expires")
            ) {
                continue;
            }
            if let Some(id) = path.file_stem().and_then(|name| name.to_str())
                && let Some((scope, capture)) = capture_key(id)
                && scope == self.scope
            {
                keys.insert((capture, id.to_owned()));
            }
        }

        let mut fingerprint = Context::new(&SHA256);
        fingerprint.update(b"rtdb.multi/archive-revocation-scope/v1\0");
        fingerprint.update(self.scope.deployment.as_bytes());
        fingerprint.update(&self.scope.incarnation.to_be_bytes());
        let mut outcomes = Vec::new();
        let mut has_more = false;
        for (capture, id) in keys {
            let path = self.vault.path(&id)?;
            let _lock = self.vault.lock_key(&path)?;
            let material = zeroize::Zeroizing::new(fs::read(&path).map_err(io)?);
            let withdrawn = is_destroyed(&material);
            if !withdrawn && material.len() != KEY_BYTES {
                return Err(malformed());
            }
            let Some(record) = read_revocation(&path)? else {
                if withdrawn || read_schedule(&path)?.is_some() {
                    return Err(incomplete_inventory());
                }
                continue;
            };
            validate_order(&id, record.request)?;
            if record
                .revoked_at_ms
                .is_some_and(|time| !withdrawn || time < record.request.not_before_ms)
            {
                return Err(malformed());
            }
            fingerprint.update(capture.as_bytes());
            fingerprint.update(&record.request.request_sha256);
            fingerprint.update(&record.request.not_before_ms.to_be_bytes());
            if after.is_none_or(|after| capture > after) {
                if outcomes.len() < PAGE_SIZE {
                    outcomes.push(ArchiveRevocationOutcome {
                        request: record.request,
                        revoked_at_ms: record.revoked_at_ms,
                    });
                } else {
                    has_more = true;
                }
            }
        }
        let next_after = if has_more {
            outcomes.last().map(|item| item.request.operation_id)
        } else {
            None
        };
        let fingerprint = fingerprint
            .finish()
            .as_ref()
            .try_into()
            .expect("SHA-256 width");
        Ok((
            fingerprint,
            ArchiveRevocationPage {
                outcomes,
                next_after,
            },
        ))
    }
}

#[derive(Clone, Copy)]
pub(in crate::keys::archive) enum RevocationStep {
    Schedule(i64),
    WithdrawRoot,
    RetainOutcome,
}

impl LocalKeyService {
    pub(in crate::keys) fn lock_capture_revocations(
        &self,
        id: &str,
    ) -> Result<Option<LockedScope>> {
        capture_key(id)
            .map(|(scope, _)| LockedScope::new(self, scope))
            .transpose()
    }

    pub(in crate::keys) async fn close_exact_revocations(
        &self,
        scope: ArchiveRevocationScope,
    ) -> Result<()> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let scope = LockedScope::new(&service, scope)?;
            let (fingerprint, _) = scope.inventory(None)?;
            match scope.closed()? {
                Some(expected) if expected != fingerprint => Err(malformed()),
                Some(_) => scope.vault.sync_directory(),
                None => scope.vault.publish(&scope.marker, &fingerprint),
            }
        })
        .await
        .map_err(task_error)?
    }

    pub(in crate::keys) async fn exact_revocation_outcomes(
        &self,
        scope: ArchiveRevocationScope,
        after: Option<Uuid>,
    ) -> Result<ArchiveRevocationPage> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let scope = LockedScope::new(&service, scope)?;
            let expected = scope.closed()?.ok_or_else(|| {
                Anomaly::new(
                    AnomalyCategory::Unavailable,
                    "rtdb.multi/archive-vault",
                    "Archive revocation admission is still open",
                )
            })?;
            let (fingerprint, page) = scope.inventory(after)?;
            if fingerprint != expected {
                return Err(malformed());
            }
            scope.vault.sync_directory()?;
            Ok(page)
        })
        .await
        .map_err(task_error)?
    }

    pub(in crate::keys) async fn schedule_exact_revocation(
        &self,
        id: &str,
        request: ArchiveRevocation,
        due_at_ms: i64,
    ) -> Result<bool> {
        if capture_key(id).is_none() {
            return Err(Anomaly::incorrect(
                "rtdb.multi/archive-vault",
                "An exact archive schedule requires a native capture key",
            ));
        }
        Ok(self
            .update_archive_revocation(id, request, RevocationStep::Schedule(due_at_ms))
            .await?
            .is_some())
    }

    pub(in crate::keys) async fn revoke_archive(
        &self,
        id: &str,
        request: ArchiveRevocation,
    ) -> Result<i64> {
        if let Some(revoked_at) = self
            .update_archive_revocation(id, request, RevocationStep::WithdrawRoot)
            .await?
        {
            return Ok(revoked_at);
        }
        self.withdraw_local_scope(UnitScope::archive(id), &IoRegion::new())
            .await?;
        self.update_archive_revocation(id, request, RevocationStep::RetainOutcome)
            .await?
            .ok_or_else(malformed)
    }

    pub(in crate::keys::archive) async fn update_archive_revocation(
        &self,
        id: &str,
        request: ArchiveRevocation,
        step: RevocationStep,
    ) -> Result<Option<i64>> {
        require_general_key_namespace(id)?;
        validate_order(id, request)?;
        let service = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            service.require_service_root()?;
            let scope = service.lock_capture_revocations(&id)?;
            let raw_path = service.path(&id)?;
            let _raw_lock = service.lock_key(&raw_path)?;
            if !service.is_brokered(&id)? {
                return Err(Anomaly::unsupported(
                    "rtdb.multi/archive-vault",
                    "The capture has no operation-only key authority",
                ));
            }
            let vault = service.archive_vault()?;
            let path = vault.path(&id)?;
            let _lock = vault.lock_key(&path)?;
            update_revocation(&vault, &path, scope.as_ref(), request, step)
        })
        .await
        .map_err(task_error)?
    }
}

fn update_revocation(
    vault: &LocalKeyService,
    path: &Path,
    scope: Option<&LockedScope>,
    request: ArchiveRevocation,
    step: RevocationStep,
) -> Result<Option<i64>> {
    let legacy_schedule = read_schedule(path)?;
    let material = zeroize::Zeroizing::new(fs::read(path).map_err(io)?);
    let withdrawn = is_destroyed(&material);
    if !withdrawn && material.len() != KEY_BYTES {
        return Err(malformed());
    }
    let prior = read_revocation(path)?;
    if prior
        .as_ref()
        .is_some_and(|record| record.request != request)
    {
        return Err(Anomaly::new(
            AnomalyCategory::Conflict,
            "rtdb.multi/archive-vault",
            "The capture has another exact revocation order",
        ));
    }
    if let RevocationStep::Schedule(due_at_ms) = step {
        if due_at_ms <= 0
            || due_at_ms > request.not_before_ms
            || legacy_schedule.is_some_and(|prior| prior != due_at_ms)
            || prior
                .as_ref()
                .is_some_and(|record| record.scheduled_at_ms.or(legacy_schedule) != Some(due_at_ms))
        {
            return Err(schedule_conflict());
        }
    }
    if let Some(time) = prior.as_ref().and_then(|record| record.revoked_at_ms) {
        if !withdrawn || time < request.not_before_ms {
            return Err(malformed());
        }
        vault.sync_directory()?;
        return Ok(Some(time));
    }
    let mut record = match prior {
        Some(record) => record,
        None => {
            if let Some(scope) = scope {
                scope.require_open()?;
            }
            let scheduled_at_ms = match step {
                RevocationStep::Schedule(time) => Some(time),
                RevocationStep::WithdrawRoot if legacy_schedule.is_none() => None,
                _ => return Err(incomplete_inventory()),
            };
            if withdrawn
                && (legacy_schedule.is_none() || !matches!(step, RevocationStep::Schedule(_)))
            {
                return Err(incomplete_inventory());
            }
            let record = RevocationRecord {
                request,
                scheduled_at_ms,
                revoked_at_ms: None,
            };
            publish(vault, path, &record)?;
            record
        }
    };
    if let RevocationStep::Schedule(due_at_ms) = step {
        if legacy_schedule.is_none() && !withdrawn {
            vault.publish(&path.with_extension("expires"), &due_at_ms.to_be_bytes())?;
        } else {
            vault.sync_directory()?;
        }
        return expire_locked(vault, path);
    }
    let now_ms = rtdb_directory::now_ms();
    if now_ms < request.not_before_ms {
        return Err(Anomaly::new(
            AnomalyCategory::Unavailable,
            "rtdb.multi/archive-vault",
            "The archive authority is waiting for revocation",
        ));
    }
    match step {
        RevocationStep::WithdrawRoot => {
            if withdrawn {
                vault.sync_directory()?;
            } else {
                vault.publish(path, DESTROYED)?;
            }
            Ok(None)
        }
        RevocationStep::RetainOutcome => {
            if !withdrawn {
                return Err(malformed());
            }
            record.revoked_at_ms = Some(now_ms);
            publish(vault, path, &record)?;
            Ok(Some(now_ms))
        }
        RevocationStep::Schedule(_) => unreachable!("schedule handled above"),
    }
}

fn publish(vault: &LocalKeyService, path: &Path, record: &RevocationRecord) -> Result<()> {
    let bytes = serde_json::to_vec(record).map_err(|_| malformed())?;
    vault.publish(&path.with_extension("revocation"), &bytes)
}

fn task_error(error: tokio::task::JoinError) -> Anomaly {
    Anomaly::fault("rtdb.multi/archive-vault-task", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::KeyService;

    async fn capture(
        vault: &LocalKeyService,
        scope: ArchiveRevocationScope,
    ) -> Result<(String, ArchiveRevocation)> {
        let operation_id = Uuid::new_v4();
        let id = scope.capture_key(operation_id);
        vault.archive_key(&id, true, IoRegion::new()).await?;
        Ok((
            id,
            ArchiveRevocation {
                operation_id,
                request_sha256: [1; 32],
                not_before_ms: rtdb_directory::now_ms(),
            },
        ))
    }

    #[tokio::test]
    async fn capture_revocation_close_serializes_with_admission_and_survives_reopen() -> Result<()>
    {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let scope = ArchiveRevocationScope {
            deployment: Uuid::new_v4(),
            incarnation: 10,
        };
        let (id, request) = capture(&vault, scope).await?;
        let (late_id, late) = capture(&vault, scope).await?;
        let due = request.not_before_ms;

        let (scheduled, closed) = tokio::join!(
            vault.schedule_archive_revocation(&id, request, due),
            vault.close_archive_revocations(scope),
        );
        closed?;
        let admitted = match scheduled {
            Ok(expired) => {
                assert!(expired);
                true
            }
            Err(error) => {
                assert_eq!(error.category, AnomalyCategory::Conflict);
                false
            }
        };
        drop(vault);

        let reopened = LocalKeyService::with_archive_broker(directory.path())?;
        let page = reopened.archive_revocation_outcomes(scope, None).await?;
        assert_eq!(page.outcomes.len(), usize::from(admitted));
        assert!(page.next_after.is_none());
        assert!(
            reopened
                .reconcile_archive_revocation(&late_id, late)
                .await
                .is_err()
        );
        assert!(reopened.schedule_archive_key(&late_id, due).await.is_err());
        assert!(reopened.withdraw_archive_key(&late_id).await.is_err());
        if admitted {
            assert_eq!(
                page.outcomes[0],
                ArchiveRevocationOutcome {
                    request,
                    revoked_at_ms: None
                }
            );
            let time = reopened.reconcile_archive_revocation(&id, request).await?;
            let page = reopened.archive_revocation_outcomes(scope, None).await?;
            assert_eq!(
                page.outcomes[0],
                ArchiveRevocationOutcome {
                    request,
                    revoked_at_ms: Some(time)
                }
            );
            assert!(
                reopened
                    .archive_revocation_outcomes(scope, Some(request.operation_id))
                    .await?
                    .outcomes
                    .is_empty()
            );
        }
        let next_scope = ArchiveRevocationScope {
            incarnation: 11,
            ..scope
        };
        let (other_id, other) = capture(&reopened, next_scope).await?;
        assert!(
            reopened
                .reconcile_archive_revocation(&other_id, other)
                .await
                .is_ok()
        );
        Ok(())
    }

    #[tokio::test]
    async fn capture_revocation_close_requires_exact_legacy_inventory() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let scope = ArchiveRevocationScope {
            deployment: Uuid::new_v4(),
            incarnation: 10,
        };
        let (id, request) = capture(&vault, scope).await?;
        let due = request.not_before_ms;
        assert!(vault.schedule_archive_key(&id, due).await?);
        assert_eq!(
            vault
                .close_archive_revocations(scope)
                .await
                .unwrap_err()
                .category,
            AnomalyCategory::Unavailable
        );
        assert!(vault.schedule_archive_revocation(&id, request, due).await?);
        vault.close_archive_revocations(scope).await?;
        assert_eq!(
            vault
                .archive_revocation_outcomes(scope, None)
                .await?
                .outcomes
                .len(),
            1
        );
        let path = vault.archive_vault()?.path(&id)?;
        fs::remove_file(path.with_extension("revocation")).unwrap();
        fs::remove_file(path.with_extension("expires")).unwrap();
        fs::remove_file(path).unwrap();
        assert!(
            vault
                .archive_revocation_outcomes(scope, None)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn capture_revocation_pages_cover_the_closed_inventory_in_order() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let scope = ArchiveRevocationScope {
            deployment: Uuid::new_v4(),
            incarnation: 10,
        };
        let mut expected = Vec::new();
        for _ in 0..=PAGE_SIZE {
            let (id, request) = capture(&vault, scope).await?;
            vault
                .schedule_archive_revocation(&id, request, request.not_before_ms)
                .await?;
            expected.push(request.operation_id);
        }
        expected.sort_unstable();
        vault.close_archive_revocations(scope).await?;
        let first = vault.archive_revocation_outcomes(scope, None).await?;
        assert_eq!(first.outcomes.len(), PAGE_SIZE);
        assert_eq!(first.next_after, Some(expected[PAGE_SIZE - 1]));
        let second = vault
            .archive_revocation_outcomes(scope, first.next_after)
            .await?;
        assert!(second.next_after.is_none());
        let actual: Vec<_> = first
            .outcomes
            .into_iter()
            .chain(second.outcomes)
            .map(|outcome| outcome.request.operation_id)
            .collect();
        assert_eq!(actual, expected);
        Ok(())
    }
}
