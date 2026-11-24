use std::num::NonZeroU64;

use sha2::{Digest, Sha256};

use super::*;

const MAX_LIVE_RELEASES: usize = 256;
const MAX_RELEASE_MICROS: i64 = 120_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustodyCarrierAuthority {
    pub incarnation: [u8; 16],
    pub source_history: [u8; 32],
    pub installation: [u8; 16],
    pub registry: [u8; 32],
    pub original_writer: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustodyResponsePart {
    pub transport_history: [u8; 16],
    pub producer: [u8; 16],
    pub consumer: [u8; 16],
    pub response: [u8; 16],
    pub ordinal: NonZeroU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustodyReleaseRegistration {
    pub authority: CustodyCarrierAuthority,
    pub group: CustodyGroupId,
    pub original: [u8; 32],
    pub occurrence: [u8; 32],
    pub carrier_receipt_sha256: [u8; 32],
    pub part: CustodyResponsePart,
    pub expires_at: Timestamp,
}

impl CustodyReleaseRegistration {
    pub fn attempt(self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/custody-response-attempt\0");
        hash.update(self.authority.incarnation);
        hash.update(self.authority.source_history);
        hash.update(self.group);
        hash.update(self.original);
        hash.update(self.occurrence);
        hash.update(self.part.transport_history);
        hash.update(self.part.producer);
        hash.update(self.part.consumer);
        hash.update(self.part.response);
        hash.update(self.part.ordinal.get().to_be_bytes());
        hash.finalize().into()
    }

    pub fn require_live_time(self, now: Timestamp) -> Result<()> {
        let remaining = self.expires_at.0.saturating_sub(now.0);
        if !(1..=MAX_RELEASE_MICROS).contains(&remaining) {
            return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb/custody-release-cutoff",
                "The response release cutoff is outside its live window",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CustodyReleasePhase {
    Private,
    Promoted,
    Cancelled,
    Disposed { evidence_sha256: [u8; 32] },
}

impl CustodyReleasePhase {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Cancelled | Self::Disposed { .. })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyRelease {
    pub registration: CustodyReleaseRegistration,
    pub phase: CustodyReleasePhase,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustodyResponseAdmission {
    pub registrations: Vec<CustodyReleaseRegistration>,
    pub frame_sha256: [u8; 32],
    pub read_proofs_sha256: Option<[u8; 32]>,
    pub selected_basis: Basis,
    pub key_generation: u64,
}

impl CustodyResponseAdmission {
    pub fn part_key(&self) -> Result<[u8; 32]> {
        let first = self.registrations.first().ok_or_else(release_conflict)?;
        let part = first.part;
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/custody-response-admission\0");
        hash.update(first.authority.incarnation);
        hash.update(part.transport_history);
        hash.update(part.producer);
        hash.update(part.consumer);
        hash.update(part.response);
        hash.update(part.ordinal.get().to_be_bytes());
        Ok(hash.finalize().into())
    }
}

impl BranchIndex {
    pub fn check_custody_response_admission(&self, admission: &CustodyResponseAdmission) -> Result<()> {
        let first = admission.registrations.first().ok_or_else(release_conflict)?;
        if admission.registrations.len() > 512 || admission.frame_sha256 == [0; 32]
            || admission.read_proofs_sha256 == Some([0; 32])
            || admission.selected_basis.incarnation() != first.authority.incarnation
            || admission.selected_basis >= self.head.basis
            || admission.registrations.windows(2).any(|pair| {
                (pair[0].group, pair[0].occurrence) >= (pair[1].group, pair[1].occurrence)
            })
        {
            return Err(release_conflict());
        }
        self.exact(admission.selected_basis)?;
        first.require_live_time(self.head.system_time)?;
        for &registration in &admission.registrations {
            if registration.authority != first.authority || registration.part != first.part
                || registration.expires_at != first.expires_at
                || self.custody_release_phase(registration)? != Some(CustodyReleasePhase::Promoted)
            {
                return Err(release_conflict());
            }
            self.live_custody(registration.group)?;
        }
        Ok(())
    }

    pub fn check_custody_release_input(
        &self,
        registration: CustodyReleaseRegistration,
    ) -> Result<()> {
        let group = self
            .custody
            .get(&registration.group)
            .ok_or_else(release_conflict)?;
        let authority = registration.authority;
        let part = registration.part;
        if authority.incarnation != self.head.basis.incarnation()
            || authority.source_history == [0; 32]
            || authority.installation == [0; 16]
            || authority.registry == [0; 32]
            || authority.original_writer == [0; 32]
            || [
                part.transport_history,
                part.producer,
                part.consumer,
                part.response,
            ]
            .contains(&[0; 16])
            || part.ordinal.get() == u64::MAX
            || registration.expires_at.0 <= 0
            || registration.carrier_receipt_sha256 == [0; 32]
            || group
                .original
                .as_ref()
                .map(|original| original.commitment(registration.group))
                != Some(registration.original)
            || group.occurrence(registration.group, registration.occurrence).is_none()
        {
            return Err(release_conflict());
        }
        Ok(())
    }

    pub fn check_custody_carrier_receipt(&self, registration: CustodyReleaseRegistration, receipt: Basis) -> Result<()> {
        self.check_custody_release_input(registration)?;
        let occurrence = self.custody[&registration.group].occurrence(registration.group, registration.occurrence);
        if !occurrence.is_some_and(|occurrence| occurrence.written == receipt) { return Err(release_conflict()); }
        Ok(())
    }

    pub fn custody_release_phase(
        &self,
        registration: CustodyReleaseRegistration,
    ) -> Result<Option<CustodyReleasePhase>> {
        self.check_custody_release_input(registration)?;
        let record = &self.custody[&registration.group];
        match record.releases.get(&registration.attempt()) {
            Some(prior) if prior.registration == registration => Ok(Some(prior.phase)),
            Some(_) => Err(release_conflict()),
            None => Ok(None),
        }
    }

    pub fn check_custody_release(&self, release: CustodyRelease, now: Timestamp) -> Result<bool> {
        use CustodyReleasePhase::*;

        let prior = self.custody_release_phase(release.registration)?;
        let group = release.registration.group;
        if !release.phase.terminal() {
            self.live_custody(group)?;
            release.registration.require_live_time(now)?;
        }
        match (prior, release.phase) {
            (None, Private) if self.custody[&group].live_releases < MAX_LIVE_RELEASES => Ok(true),
            (Some(Private), Promoted) | (None | Some(Private), Cancelled) => Ok(true),
            (None | Some(Private | Promoted), Disposed { evidence_sha256 })
                if evidence_sha256 != [0; 32] =>
            {
                Ok(true)
            }
            (Some(previous), next)
                if previous == next || previous == Promoted && next == Private =>
            {
                Ok(false)
            }
            _ => Err(release_conflict()),
        }
    }

    pub fn custody_releases_drained(&self, group: CustodyGroupId) -> bool {
        self.custody
            .get(&group)
            .is_some_and(|group| group.revoked.is_some() && group.live_releases == 0)
            && self.native_custody_copies_drained(group)
    }

    pub(super) fn apply_custody_release(&mut self, release: CustodyRelease) -> Result<()> {
        if !self.check_custody_release(release, self.head.system_time)? {
            return Err(Anomaly::fault(
                "rtdb/custody-fold",
                "Resolved release must change its durable phase",
            ));
        }
        let group = Arc::make_mut(
            self.custody
                .get_mut(&release.registration.group)
                .expect("checked custody group"),
        );
        let previous = group
            .releases
            .insert(release.registration.attempt(), release);
        match (previous, release.phase) {
            (None, CustodyReleasePhase::Private) => group.live_releases += 1,
            (Some(prior), terminal) if !prior.phase.terminal() && terminal.terminal() => {
                group.live_releases -= 1
            }
            _ => (),
        }
        Ok(())
    }
}

fn release_conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/custody-release",
        "Response release differs from its original registration or durable phase",
    )
}
