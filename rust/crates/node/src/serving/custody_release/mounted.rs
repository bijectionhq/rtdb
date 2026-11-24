use std::{
    future::Future,
    sync::{Arc, Weak},
};

use parking_lot::Mutex;
use rtdb_storage::sealing::DeliveryDeadline;
use rtdb_types::Timestamp;
use sha2::{Digest, Sha256};

use super::*;
use crate::{
    Database, DatabaseCatalog,
    serving::{QuerySource, QueryView},
};

pub type CustodyContributors = BTreeSet<(CustodyGroupId, [u8; 32])>;

pub struct CustodyProviderInstallation {
    pub authority: CustodyCarrierAuthority,
    pub writer: OriginalWriterPoint,
    pub observations: Arc<dyn OriginalCarrierObservations>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredCustodyResponse {
    pub authority: CustodyCarrierAuthority,
    pub part: CustodyResponsePart,
    pub expires_at: Timestamp,
    pub frame_sha256: [u8; 32],
    pub contributors: CustodyContributors,
}

impl RegisteredCustodyResponse {
    fn require_frame(
        &self,
        registration: CustodyReleaseRegistration,
        contributors: &CustodyContributors,
        frame_sha256: [u8; 32],
    ) -> Result<()> {
        if self.authority != registration.authority
            || self.part != registration.part
            || self.expires_at != registration.expires_at
            || self.frame_sha256 != frame_sha256
            || self.contributors != *contributors
        {
            return Err(release_unavailable());
        }
        Ok(())
    }
}

pub fn custody_frame_commitment(frame: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rtdb/v1/custody-response-frame\0");
    hash.update(frame);
    hash.finalize().into()
}

struct InstalledProvider {
    installation: CustodyProviderInstallation,
    lifetime: CancellationToken,
}

#[derive(Default)]
pub(crate) struct CustodyProviderSlot(Mutex<Option<Arc<InstalledProvider>>>);

impl CustodyProviderSlot {
    pub(crate) fn close(&self, database: &CancellationToken) {
        let mut state = self.0.lock();
        database.cancel();
        if let Some(provider) = state.take() {
            provider.lifetime.cancel();
        }
    }
}

#[must_use = "dropping the mount withdraws its provider"]
pub struct CustodyProviderMount {
    database: Weak<Database>,
    provider: Arc<InstalledProvider>,
}

impl Drop for CustodyProviderMount {
    fn drop(&mut self) {
        if let Some(database) = self.database.upgrade() {
            let mut state = database.custody_provider.0.lock();
            if state
                .as_ref()
                .is_some_and(|provider| Arc::ptr_eq(provider, &self.provider))
            {
                *state = None;
            }
            self.provider.lifetime.cancel();
        } else {
            self.provider.lifetime.cancel();
        }
    }
}

pub struct CustodyResponseOwner {
    catalog: Arc<DatabaseCatalog>,
    database: Arc<Database>,
    provider: Arc<InstalledProvider>,
}

impl DatabaseCatalog {
    pub async fn mount_original_custody_provider(
        self: &Arc<Self>,
        database: &str,
        installation: CustodyProviderInstallation,
        cancellation: &CancellationToken,
    ) -> Result<CustodyProviderMount> {
        let attachment = self.database(database)?;
        let lifetime = attachment.custody_provider_lifetime();
        let journal = attachment.original_custody_carrier_journal(
            installation.authority,
            installation.writer.clone(),
            installation.observations.as_ref(),
        )?;
        let driver = attachment
            .serving_driver()
            .ok_or_else(release_unavailable)?;
        cancellation
            .run_until_cancelled(async {
                let writer = journal.writer_fence(cancellation).await?;
                let index = driver.current_custody(cancellation).await?;
                if !writer.matches(&index)? {
                    return Err(release_unavailable());
                }
                Ok(())
            })
            .await
            .ok_or_else(release_unavailable)??;
        self.with_current_attachment(&attachment, || {
            let mut state = attachment.custody_provider.0.lock();
            if state.is_some() || lifetime.is_cancelled() || cancellation.is_cancelled() {
                return Err(release_unavailable());
            }
            let provider = Arc::new(InstalledProvider {
                installation,
                lifetime,
            });
            *state = Some(Arc::clone(&provider));
            Ok(CustodyProviderMount {
                database: Arc::downgrade(&attachment),
                provider,
            })
        })
    }

    pub fn original_custody_response_owner(
        self: &Arc<Self>,
        database: &str,
    ) -> Result<CustodyResponseOwner> {
        let attachment = self.database(database)?;
        self.with_current_attachment(&attachment, || {
            let state = attachment.custody_provider.0.lock();
            let provider = state
                .as_ref()
                .filter(|provider| !provider.lifetime.is_cancelled())
                .ok_or_else(release_unavailable)?;
            Ok(CustodyResponseOwner {
                catalog: Arc::clone(self),
                database: Arc::clone(&attachment),
                provider: Arc::clone(provider),
            })
        })
    }
}

pub struct CapturedCustodyRank {
    provider: Arc<InstalledProvider>,
    view: QueryView,
}

impl CapturedCustodyRank {
    pub fn view(&self) -> &QueryView {
        &self.view
    }
}

pub struct MountedResponseRelease<'a> {
    owner: &'a CustodyResponseOwner,
    journal: AttachedCustodyCarrierJournal<'a>,
    clock: &'a dyn Clock,
    cancellation: &'a CancellationToken,
}

impl CustodyResponseOwner {
    pub fn response_release<'a>(
        &'a self,
        clock: &'a dyn Clock,
        cancellation: &'a CancellationToken,
    ) -> Result<MountedResponseRelease<'a>> {
        let installed = &self.provider.installation;
        let journal = self
            .database
            .original_custody_carrier_journal(
                installed.authority,
                installed.writer.clone(),
                installed.observations.as_ref(),
            )?
            .with_lifetime(&self.provider.lifetime)?;
        Ok(MountedResponseRelease {
            owner: self,
            journal,
            clock,
            cancellation,
        })
    }

    fn with_live_mount(&self, handoff: impl FnOnce() -> Result<()>) -> Result<()> {
        self.catalog.with_current_attachment(&self.database, || {
            let state = self.database.custody_provider.0.lock();
            if self.provider.lifetime.is_cancelled()
                || !state
                    .as_ref()
                    .is_some_and(|provider| Arc::ptr_eq(provider, &self.provider))
            {
                return Err(release_unavailable());
            }
            handoff()
        })
    }
}

impl MountedResponseRelease<'_> {
    pub fn release(&self) -> ResponseRelease<'_> {
        ResponseRelease {
            driver: self.journal.attachment(),
            original: &self.journal,
            clock: self.clock,
            cancellation: self.cancellation,
        }
    }

    async fn while_mounted<T>(&self, work: impl Future<Output = Result<T>>) -> Result<T> {
        self.owner
            .provider
            .lifetime
            .run_until_cancelled(self.cancellation.run_until_cancelled(work))
            .await
            .flatten()
            .ok_or_else(release_unavailable)?
    }

    pub async fn capture_ranked(&self, source: QuerySource) -> Result<CapturedCustodyRank> {
        let release = self.release();
        let authority = self.owner.provider.installation.authority;
        let view = self
            .while_mounted(async {
                release.current_with_writer(authority).await?;
                self.journal
                    .attachment()
                    .query(source, false, self.clock.now(), self.cancellation)
                    .await
            })
            .await?;
        if view.cut.basis.incarnation() != authority.incarnation
            || view.custody_contributors.is_none()
        {
            return Err(release_unavailable());
        }
        self.owner
            .with_live_mount(|| release.require_authority(authority))?;
        Ok(CapturedCustodyRank {
            provider: Arc::clone(&self.owner.provider),
            view,
        })
    }

    pub async fn handoff_ranked(
        &self,
        reads: &[&CapturedCustodyRank],
        registrations: &[CustodyReleaseRegistration],
        frame: &[u8],
        enqueue: impl FnOnce(&[u8], DeliveryDeadline) -> Result<()>,
    ) -> Result<()> {
        let first = reads.first().ok_or_else(release_unavailable)?;
        let mut contributors = CustodyContributors::new();
        for read in reads {
            if !Arc::ptr_eq(&read.provider, &self.owner.provider)
                || read.view.cut != first.view.cut
                || read.view.key_generation != first.view.key_generation
            {
                return Err(release_unavailable());
            }
            let selected = read
                .view
                .custody_contributors
                .as_ref()
                .ok_or_else(release_unavailable)?;
            for &contributor in selected {
                if contributors.insert(contributor) && contributors.len() > 512 {
                    return Err(release_unavailable());
                }
            }
        }
        self.handoff(
            &contributors,
            first.view.key_generation,
            registrations,
            frame,
            enqueue,
        )
        .await
    }

    /// The carrier owns the frame and synchronously enqueues it under the supplied transport deadline.
    async fn handoff(
        &self,
        contributors: &CustodyContributors,
        generation: Option<u64>,
        registrations: &[CustodyReleaseRegistration],
        frame: &[u8],
        enqueue: impl FnOnce(&[u8], DeliveryDeadline) -> Result<()>,
    ) -> Result<()> {
        let first = registrations.first().ok_or_else(release_unavailable)?;
        first.require_live_time(self.clock.now())?;
        let nanos = u64::try_from(first.expires_at.0)
            .ok()
            .and_then(|micros| micros.checked_mul(1_000))
            .ok_or_else(release_unavailable)?;
        let deadline = DeliveryDeadline::from_wire_nanos(nanos).ok_or_else(release_unavailable)?;
        let release = self.release();
        release.require_authority(first.authority)?;
        let expected_frame = custody_frame_commitment(frame);
        self.while_mounted(async {
            let manifest = self
                .owner
                .provider
                .installation
                .observations
                .registered_response(first.authority, first.part, self.cancellation)
                .await?;
            manifest.require_frame(*first, contributors, expected_frame)?;
            release
                .check_contributors(contributors, registrations, generation)
                .await
        })
        .await?;
        self.owner.with_live_mount(|| {
            release.require_authority(first.authority)?;
            first.require_live_time(self.clock.now())?;
            if !deadline.is_live() {
                return Err(release_unavailable());
            }
            enqueue(frame, deadline)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    #[test]
    fn registered_frame_rejects_omitted_contributors_and_transport_substitution() {
        let registration = CustodyReleaseRegistration {
            authority: CustodyCarrierAuthority {
                incarnation: [1; 16],
                source_history: [2; 32],
                installation: [3; 16],
                registry: [4; 32],
                original_writer: [5; 32],
            },
            part: CustodyResponsePart {
                transport_history: [6; 16],
                producer: [7; 16],
                consumer: [8; 16],
                response: [9; 16],
                ordinal: NonZeroU64::MIN,
            },
            group: [10; 16],
            original: [11; 32],
            occurrence: [12; 32],
            carrier_receipt_sha256: [13; 32],
            expires_at: Timestamp(14),
        };
        let reported = (registration.group, registration.occurrence);
        let contributors = [reported, ([15; 16], [16; 32])].into();
        let frame_sha256 = custody_frame_commitment(b"zero hits");
        let manifest = RegisteredCustodyResponse {
            authority: registration.authority,
            part: registration.part,
            expires_at: registration.expires_at,
            frame_sha256,
            contributors,
        };
        assert!(
            manifest
                .require_frame(registration, &manifest.contributors, frame_sha256)
                .is_ok()
        );
        assert!(
            manifest
                .require_frame(registration, &[reported].into(), frame_sha256)
                .is_err()
        );
        assert!(
            manifest
                .require_frame(
                    registration,
                    &manifest.contributors,
                    custody_frame_commitment(b"other frame")
                )
                .is_err()
        );
        let changed_part = CustodyResponsePart {
            ordinal: NonZeroU64::new(2).unwrap(),
            ..registration.part
        };
        assert!(
            manifest
                .require_frame(
                    CustodyReleaseRegistration {
                        part: changed_part,
                        ..registration
                    },
                    &manifest.contributors,
                    frame_sha256
                )
                .is_err()
        );
    }
}
