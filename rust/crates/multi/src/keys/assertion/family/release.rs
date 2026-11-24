//! Durable response holds in the same vault CAS as the original family fence.

use std::{num::NonZeroU64, time::SystemTime};

use rtdb_types::RootLandingBinding;

use super::*;

const MAX_LIVE_MS: u64 = 120_000;
const MAX_HOLDS: usize = 256;
const TERMINAL_HEADER: &[u8; 4] = b"RFT1";

/// These coordinates come from the installed carrier journal adapter.
/// The source installation is distinct from a response's cell history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CarrierJournalIdentity {
    pub deployment: Uuid,
    pub source_history: [u8; 32],
    pub installation: [u8; 16],
    pub registry: [u8; 32],
    pub original_writer: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FamilyResponsePart {
    pub cell_history: [u8; 16],
    pub usher: [u8; 16],
    pub conductor: [u8; 16],
    pub response: [u8; 16],
    pub ordinal: NonZeroU64,
}

impl FamilyResponsePart {
    pub fn to_bytes(self) -> [u8; 72] {
        let mut bytes = [0; 72];
        bytes[..16].copy_from_slice(&self.cell_history);
        bytes[16..32].copy_from_slice(&self.usher);
        bytes[32..48].copy_from_slice(&self.conductor);
        bytes[48..64].copy_from_slice(&self.response);
        bytes[64..].copy_from_slice(&self.ordinal.get().to_be_bytes());
        bytes
    }

    fn validate(self) -> Result<()> {
        if [self.cell_history, self.usher, self.conductor, self.response].contains(&[0; 16])
            || self.ordinal.get() == u64::MAX
        {
            return Err(conflict());
        }
        Ok(())
    }
}

/// This trusted selection accompanies the ordinary authenticated carrier open.
/// The hold does not itself grant key use or authenticate a read cut.
#[derive(Clone, Copy)]
pub enum FamilyReleaseInput<'a> {
    Member(FamilyOccurrence),
    WorldRow { member: FamilyOccurrence, winner: Position, row: EnrolledWorldRow },
    WorldDerived { member: FamilyOccurrence, stage: AssertionStage, slot: WorldRowSlot<'a> },
    RootDerived { member: FamilyOccurrence, relation: &'a str, key: &'a [u8] },
    RootLanding { member: FamilyOccurrence, binding: RootLandingBinding },
}

impl FamilyReleaseInput<'_> {
    fn member(self) -> FamilyOccurrence {
        match self {
            Self::Member(member) | Self::WorldRow { member, .. } | Self::WorldDerived { member, .. }
            | Self::RootDerived { member, .. } | Self::RootLanding { member, .. } => member,
        }
    }

    pub(super) fn require_enrolled(self, family: &Family) -> Result<()> {
        let member = self.member();
        if family.admitted_member(member.assertion) != Some(member) {
            return Err(conflict());
        }
        let enrolled = match self {
            Self::Member(_) => true,
            Self::WorldRow { winner, row, .. } => {
                winner.line_generation == *row.world.as_bytes()
                    && if row == family.original {
                        winner == member.position && row.assertion == member.assertion && row.capability == member.capability
                    } else {
                        winner != member.position && family.rewrites.iter().any(|rewrite| {
                            rewrite.member == member.assertion && rewrite.row == row
                        })
                    }
            }
            Self::WorldDerived { stage, slot, .. } => family.derived_capability(stage, member.assertion, slot),
            Self::RootDerived { relation, key, .. } => family.derived.iter().any(|row| {
                row.world.is_none() && row.capability.is_none() && row.member == member.assertion
                    && row.relation == relation && row.key == key
            }),
            Self::RootLanding { binding, .. } => return family.require_root_landing(member.capability, binding),
        };
        if !enrolled { return Err(conflict()); }
        Ok(())
    }

    pub(super) fn commitment(self) -> Result<[u8; 32]> {
        let member = self.member();
        let mut bytes = Context::new(&SHA256);
        bytes.update(b"rtdb.multi/family-release-input/v1\0");
        bytes.update(&member.position.to_bytes());
        bytes.update(&member.assertion.to_bytes());
        update_stage(&mut bytes, member.capability);
        match self {
            Self::Member(_) => bytes.update(&[0]),
            Self::WorldRow { winner, row, .. } => {
                bytes.update(&[1]);
                bytes.update(&winner.to_bytes());
                bytes.update(row.world.as_bytes());
                bytes.update(&row.assertion.to_bytes());
                update_stage(&mut bytes, row.capability);
                bytes.update(&row.slot_commitment);
                bytes.update(&row.scope_commitment);
                bytes.update(&row.codec.to_be_bytes());
            }
            Self::WorldDerived { stage, slot, .. } => {
                if slot.relation.len() > usize::from(u16::MAX) || slot.key.len() > usize::from(u16::MAX) {
                    return Err(conflict());
                }
                bytes.update(&[2]);
                update_stage(&mut bytes, stage);
                bytes.update(&world_row_slot_commitment(slot));
            }
            Self::RootDerived { relation, key, .. } => {
                bytes.update(&[3]);
                for field in [relation.as_bytes(), key] {
                    let length = u16::try_from(field.len()).map_err(|_| conflict())?;
                    if length == 0 { return Err(conflict()); }
                    bytes.update(&length.to_be_bytes());
                    bytes.update(field);
                }
            }
            Self::RootLanding { binding, .. } => {
                if member.position != binding.position || member.assertion != binding.assertion
                    || member.capability.identity().as_bytes() != &binding.stage
                {
                    return Err(conflict());
                }
                bytes.update(&[4]);
                bytes.update(&binding.to_bytes());
            }
        }
        Ok(finish(bytes))
    }
}

fn update_stage(bytes: &mut Context, stage: AssertionStage) {
    bytes.update(stage.deployment().as_bytes());
    bytes.update(&stage.incarnation().get().to_be_bytes());
    bytes.update(stage.identity().as_bytes());
}

fn finish(bytes: Context) -> [u8; 32] {
    bytes.finish().as_ref().try_into().expect("SHA-256 commitment")
}

#[derive(Clone, Copy)]
pub struct FamilyReleaseRegistration {
    owner: CarrierJournalIdentity,
    client_family: Uuid,
    part: FamilyResponsePart,
    origin: [u8; 32],
    attempt: [u8; 32],
    input: [u8; 32],
    binding: [u8; 32],
    expires_at_ms: u64,
}

impl FamilyReleaseRegistration {
    pub fn owner(self) -> CarrierJournalIdentity { self.owner }
    pub fn client_family(self) -> Uuid { self.client_family }
    pub fn part(self) -> FamilyResponsePart { self.part }
    pub fn origin(self) -> [u8; 32] { self.origin }
    pub fn attempt(self) -> [u8; 32] { self.attempt }

    pub(crate) fn require_input(self, family: Uuid, input: FamilyReleaseInput<'_>) -> Result<()> {
        if self.client_family != family || self.input != input.commitment()? { return Err(conflict()); }
        Ok(())
    }

    fn new(owner: CarrierJournalIdentity, client_family: Uuid, part: FamilyResponsePart, input: [u8; 32], expires_at_ms: u64) -> Self {
        let mut origin = Context::new(&SHA256);
        origin.update(b"rtdb.multi/family-release-provider/v1\0");
        origin.update(owner.deployment.as_bytes());
        origin.update(&owner.source_history);
        let origin = finish(origin);
        let mut attempt = Context::new(&SHA256);
        attempt.update(b"rtdb.multi/family-release-attempt/v1\0");
        attempt.update(&origin);
        attempt.update(client_family.as_bytes());
        attempt.update(&input);
        attempt.update(&part.to_bytes());
        let attempt = finish(attempt);
        let mut binding = Context::new(&SHA256);
        binding.update(b"rtdb.multi/family-release-binding/v1\0");
        binding.update(&attempt);
        binding.update(&owner.installation);
        binding.update(&owner.registry);
        binding.update(&owner.original_writer);
        binding.update(&expires_at_ms.to_be_bytes());
        Self { owner, client_family, part, origin, attempt, input, binding: finish(binding), expires_at_ms }
    }

    /// Canonical nonsecret address for the original recovery registry.
    pub fn address(self) -> [u8; 188] {
        let mut bytes = [0; 188];
        bytes[..4].copy_from_slice(b"RFR1");
        bytes[4..20].copy_from_slice(self.owner.deployment.as_bytes());
        bytes[20..52].copy_from_slice(&self.owner.source_history);
        bytes[52..68].copy_from_slice(self.client_family.as_bytes());
        bytes[68..84].copy_from_slice(&self.owner.installation);
        bytes[84..116].copy_from_slice(&self.owner.registry);
        bytes[116..148].copy_from_slice(&self.owner.original_writer);
        bytes[148..156].copy_from_slice(&self.expires_at_ms.to_be_bytes());
        bytes[156..].copy_from_slice(&self.input);
        bytes
    }

    pub fn from_address(origin: [u8; 32], attempt: [u8; 32], part: FamilyResponsePart, address: &[u8]) -> Result<Self> {
        let address: &[u8; 188] = address.try_into().map_err(|_| conflict())?;
        let owner = CarrierJournalIdentity {
            deployment: Uuid::from_bytes(address[4..20].try_into().expect("deployment width")),
            source_history: address[20..52].try_into().expect("source history width"),
            installation: address[68..84].try_into().expect("installation width"),
            registry: address[84..116].try_into().expect("registry width"),
            original_writer: address[116..148].try_into().expect("writer width"),
        };
        let client_family = Uuid::from_bytes(address[52..68].try_into().expect("family width"));
        let expires = u64::from_be_bytes(address[148..156].try_into().expect("expiry width"));
        let input: [u8; 32] = address[156..].try_into().expect("input commitment width");
        part.validate()?;
        if client_family.is_nil() || expires == 0 || input == [0; 32] { return Err(conflict()); }
        let registration = Self::new(owner, client_family, part, input, expires);
        if &registration.address() != address || registration.origin != origin || registration.attempt != attempt {
            return Err(conflict());
        }
        Ok(registration)
    }

    fn family(self) -> Uuid { family_vault_address(self.owner.deployment, self.client_family) }

    fn terminal(self) -> AssertionVaultRecord {
        AssertionVaultRecord::ReleaseTerminal { family: self.family(), attempt: self.attempt }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FamilyReleaseObservation {
    Live,
    Disposed,
}

/// The installed original carrier journal adapter is the proof owner.
/// It must authenticate the exact registration and original writer at a
/// current barrier. A missing row or unavailable authority is an error.
#[async_trait::async_trait]
pub trait FamilyReleaseAuthority: Send + Sync {
    fn identity(&self) -> CarrierJournalIdentity;

    async fn observe(&self, registration: FamilyReleaseRegistration) -> Result<FamilyReleaseObservation>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum ReleasePhase { Private, Promoted, Cancelled, Disposed }

impl ReleasePhase {
    fn terminal_byte(self) -> Option<u8> {
        match self {
            Self::Cancelled => Some(0),
            Self::Disposed => Some(1),
            Self::Private | Self::Promoted => None,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReleaseHold {
    attempt: [u8; 32],
    binding: [u8; 32],
    phase: ReleasePhase,
}

impl<'a> AssertionFamilies<'a> {
    pub fn with_release_authority<'b>(self, authority: &'b dyn FamilyReleaseAuthority) -> AssertionFamilies<'b> where 'a: 'b {
        AssertionFamilies {
            journal: self.journal, region: self.region, original: self.original, release_authority: Some(authority),
        }
    }

    /// Return coordinates to register before the first potentially durable hold.
    pub async fn prepare_family_release(
        &self, client_family: Uuid, input: FamilyReleaseInput<'_>, part: FamilyResponsePart, expires_at_ms: u64,
    ) -> Result<FamilyReleaseRegistration> {
        let authority = self.release_authority.ok_or_else(unavailable)?;
        let owner = authority.identity();
        self.require_release_owner(owner)?;
        part.validate()?;
        if client_family.is_nil() { return Err(conflict()); }
        check_expiry(expires_at_ms)?;
        let registration = FamilyReleaseRegistration::new(owner, client_family, part, input.commitment()?, expires_at_ms);
        let (family, _) = self.read_live_release(registration).await?;
        self.require_release_input(input, &family).await?;
        check_expiry(expires_at_ms)?;
        Ok(registration)
    }

    pub async fn stage_family_release(
        &self, registration: FamilyReleaseRegistration, input: FamilyReleaseInput<'_>,
    ) -> Result<()> {
        if registration.input != input.commitment()? { return Err(conflict()); }
        self.require_release_observation(registration, FamilyReleaseObservation::Live).await?;
        for _ in 0..ATTEMPTS {
            let (mut family, etag) = self.read_live_release(registration).await?;
            self.require_release_input(input, &family).await?;
            if self.read_release_terminal(registration).await?.is_some() { return Err(conflict()); }
            let found = family.release_holds.iter().find(|hold| hold.attempt == registration.attempt);
            if let Some(hold) = found {
                if hold.binding != registration.binding || hold.phase.terminal_byte().is_some() { return Err(conflict()); }
                return self.check_family_release(registration, false).await;
            }
            if family.release_holds.len() >= MAX_HOLDS { return Err(too_large()); }
            family.release_holds.push(ReleaseHold {
                attempt: registration.attempt, binding: registration.binding, phase: ReleasePhase::Private,
            });
            if self.write(registration.family(), Some(&etag), &family).await? {
                return self.check_family_release(registration, false).await;
            }
        }
        Err(contention())
    }

    async fn require_release_input(&self, input: FamilyReleaseInput<'_>, family: &Family) -> Result<()> {
        if let FamilyReleaseInput::RootLanding { binding, .. } = input {
            self.require_position_open(binding.position).await?;
        }
        input.require_enrolled(family)
    }

    pub async fn promote_family_release(&self, registration: FamilyReleaseRegistration) -> Result<()> {
        self.require_release_observation(registration, FamilyReleaseObservation::Live).await?;
        for _ in 0..ATTEMPTS {
            let (mut family, etag) = self.read_live_release(registration).await?;
            let hold = family.release_holds.iter_mut().find(|hold| hold.attempt == registration.attempt)
                .filter(|hold| hold.binding == registration.binding && hold.phase.terminal_byte().is_none())
                .ok_or_else(conflict)?;
            if hold.phase == ReleasePhase::Promoted {
                return self.require_family_release(registration).await;
            }
            hold.phase = ReleasePhase::Promoted;
            if self.write(registration.family(), Some(&etag), &family).await? {
                return self.require_family_release(registration).await;
            }
        }
        Err(contention())
    }

    /// The delivery owner calls this immediately before releasing the held part.
    pub async fn require_family_release(&self, registration: FamilyReleaseRegistration) -> Result<()> {
        self.check_family_release(registration, true).await
    }

    async fn check_family_release(&self, registration: FamilyReleaseRegistration, promoted: bool) -> Result<()> {
        self.require_release_observation(registration, FamilyReleaseObservation::Live).await?;
        let (family, _) = self.read_live_release(registration).await?;
        if !family.release_holds.iter().any(|hold| hold.attempt == registration.attempt
            && hold.binding == registration.binding && if promoted {
                hold.phase == ReleasePhase::Promoted
            } else {
                hold.phase.terminal_byte().is_none()
            })
        {
            return Err(conflict());
        }
        check_expiry(registration.expires_at_ms)
    }

    pub async fn cancel_private_family_release(&self, registration: FamilyReleaseRegistration) -> Result<()> {
        if let Some(prior) = self.confirmed_terminal(registration).await? {
            if prior != ReleasePhase::Cancelled { return Err(conflict()); }
            return self.prune_terminal_hold(registration).await;
        }
        self.require_release_observation(registration, FamilyReleaseObservation::Live).await?;
        self.finish_family_release(registration, ReleasePhase::Cancelled).await
    }

    pub async fn settle_family_release(&self, registration: FamilyReleaseRegistration) -> Result<()> {
        if self.confirmed_terminal(registration).await?.is_some() {
            return self.prune_terminal_hold(registration).await;
        }
        self.require_release_observation(registration, FamilyReleaseObservation::Disposed).await?;
        self.finish_family_release(registration, ReleasePhase::Disposed).await
    }

    /// Recover only the terminal path from the original nonsecret registry row.
    pub async fn recover_family_release(
        &self, origin: [u8; 32], attempt: [u8; 32], part: FamilyResponsePart, address: &[u8],
    ) -> Result<()> {
        let registration = FamilyReleaseRegistration::from_address(origin, attempt, part, address)?;
        self.settle_family_release(registration).await
    }

    async fn finish_family_release(&self, registration: FamilyReleaseRegistration, terminal: ReleasePhase) -> Result<()> {
        // Put the terminal phase through the family CAS before writing the durable
        // acknowledgement. A stage holding the former revision cannot succeed.
        let family_id = registration.family();
        for _ in 0..ATTEMPTS {
            let (mut family, etag) = self.read_current(family_id, true).await?.ok_or_else(conflict)?;
            if let Some(prior) = self.read_release_terminal(registration).await? {
                if terminal == ReleasePhase::Cancelled && prior != terminal { return Err(conflict()); }
                return self.prune_terminal_hold(registration).await;
            }
            let prior = family.release_holds.iter_mut().find(|hold| hold.attempt == registration.attempt);
            let changed = match prior {
                Some(hold) => {
                    if hold.binding != registration.binding
                        || (terminal == ReleasePhase::Cancelled
                            && !matches!(hold.phase, ReleasePhase::Private | ReleasePhase::Cancelled))
                    {
                        return Err(conflict());
                    }
                    if hold.phase.terminal_byte().is_some() {
                        return self.acknowledge_terminal(registration, hold.phase).await;
                    }
                    hold.phase = terminal;
                    true
                }
                None if family.fenced.is_some() => false,
                None => {
                    if family.release_holds.len() >= MAX_HOLDS { return Err(too_large()); }
                    family.release_holds.push(ReleaseHold {
                        attempt: registration.attempt, binding: registration.binding, phase: terminal,
                    });
                    true
                }
            };
            if !changed || self.write(family_id, Some(&etag), &family).await? {
                return self.acknowledge_terminal(registration, terminal).await;
            }
        }
        Err(contention())
    }

    async fn acknowledge_terminal(&self, registration: FamilyReleaseRegistration, phase: ReleasePhase) -> Result<()> {
        let mut value = Vec::from(TERMINAL_HEADER.as_slice());
        value.push(phase.terminal_byte().ok_or_else(conflict)?);
        value.extend_from_slice(&registration.binding);
        let written = self.journal.write_record(registration.terminal(), &self.region, None, value.into()).await?;
        if !written && self.read_release_terminal(registration).await? != Some(phase) { return Err(conflict()); }
        self.prune_terminal_hold(registration).await
    }

    async fn prune_terminal_hold(&self, registration: FamilyReleaseRegistration) -> Result<()> {
        for _ in 0..ATTEMPTS {
            let (mut family, etag) = self.read_current(registration.family(), true).await?.ok_or_else(conflict)?;
            let Some(index) = family.release_holds.iter().position(|hold| hold.attempt == registration.attempt)
            else { return Ok(()); };
            let hold = &family.release_holds[index];
            if hold.binding != registration.binding || hold.phase.terminal_byte().is_none() { return Err(conflict()); }
            family.release_holds.swap_remove(index);
            // Local and S3 vault revisions may be content identities. Keep
            // the emptied family distinct from its pre-staging CAS value.
            family.release_sequence = family.release_sequence.checked_add(1).ok_or_else(too_large)?;
            if self.write(registration.family(), Some(&etag), &family).await? { return Ok(()); }
        }
        Err(contention())
    }

    async fn confirmed_terminal(&self, registration: FamilyReleaseRegistration) -> Result<Option<ReleasePhase>> {
        let authority = self.release_authority.ok_or_else(unavailable)?;
        self.require_release_owner(registration.owner)?;
        if authority.identity() != registration.owner { return Err(unavailable()); }
        self.read_release_terminal(registration).await
    }

    async fn read_release_terminal(&self, registration: FamilyReleaseRegistration) -> Result<Option<ReleasePhase>> {
        let Some((value, _)) = self.journal.read_record(registration.terminal(), &self.region).await?
        else { return Ok(None); };
        let body = value.strip_prefix(TERMINAL_HEADER).ok_or_else(malformed)?;
        let (&phase, binding) = body.split_first().ok_or_else(malformed)?;
        if binding != registration.binding.as_slice() { return Err(conflict()); }
        match phase {
            0 => Ok(Some(ReleasePhase::Cancelled)),
            1 => Ok(Some(ReleasePhase::Disposed)),
            _ => Err(malformed()),
        }
    }

    /// A vault-side drain check. RTDB's delivery owner must separately drain
    /// its Flight and worker output before a release holder signs a disposition.
    pub async fn family_release_holds_drained(&self, client_family: Uuid, expected: AssertionFamilyFenceMarker) -> Result<bool> {
        let original = self.original.as_ref().ok_or_else(unavailable)?;
        let authority = self.release_authority.ok_or_else(unavailable)?;
        self.require_release_owner(authority.identity())?;
        if self.current_release_marker(client_family).await? != Some(expected) { return Err(unavailable()); }
        let family_id = family_vault_address(original.deployment, client_family);
        let (family, _) = self.read_current(family_id, true).await?.ok_or_else(conflict)?;
        if family.fenced != Some(expected.order) || family.fence_inventory(expected.order).vault_inventory() != expected.inventory {
            return Err(conflict());
        }
        Ok(family.release_holds.is_empty())
    }

    fn require_release_owner(&self, owner: CarrierJournalIdentity) -> Result<()> {
        let original = self.original.as_ref().ok_or_else(unavailable)?;
        if owner.deployment != original.deployment || owner.source_history != original.reader.history_fingerprint()
            || owner.installation == [0; 16] || owner.registry == [0; 32] || owner.original_writer == [0; 32]
        {
            return Err(unavailable());
        }
        Ok(())
    }

    async fn require_release_observation(
        &self, registration: FamilyReleaseRegistration, expected: FamilyReleaseObservation,
    ) -> Result<()> {
        let authority = self.release_authority.ok_or_else(unavailable)?;
        self.require_release_owner(registration.owner)?;
        if authority.identity() != registration.owner || authority.observe(registration).await? != expected {
            return Err(unavailable());
        }
        Ok(())
    }

    async fn read_live_release(&self, registration: FamilyReleaseRegistration) -> Result<(Family, String)> {
        check_expiry(registration.expires_at_ms)?;
        let (record, etag) = self.read_current(registration.family(), true).await?.ok_or_else(conflict)?;
        record.require_live()?;
        if self.current_release_marker(registration.client_family).await?.is_some() {
            return Err(rtdb_storage::Error::ErasedUnderView.into());
        }
        Ok((record, etag))
    }

    async fn current_release_marker(&self, family: Uuid) -> Result<Option<AssertionFamilyFenceMarker>> {
        let original = self.original.as_ref().ok_or_else(unavailable)?;
        original.reader.current(original.deployment, original.cancellation).await?
            .assertion_family_fence(family).await
    }
}

struct OriginalFamilyDelivery {
    original: OriginalAssertionFamilyReader,
    deployment: Uuid,
    family: Uuid,
    cancellation: CancellationToken,
}

#[async_trait::async_trait]
impl rtdb_storage::sealing::PlaintextReleaseCheck for OriginalFamilyDelivery {
    async fn require_current(&self) -> Result<()> {
        let current = self.original.current(self.deployment, &self.cancellation).await?;
        current.require_unretired_family_source().await?;
        if current.assertion_family_fence(self.family).await?.is_some() {
            return Err(rtdb_storage::Error::ErasedUnderView.into());
        }
        Ok(())
    }
}

impl AssertionFamilies<'_> {
    /// The exact input was admitted at open. Attach its original marker and
    /// original output cutoff to the existing plaintext delivery scope.
    pub(crate) fn track_family_delivery(&self, family: Uuid) -> Result<()> {
        let original = self.original.as_ref().ok_or_else(unavailable)?;
        let mut key = Context::new(&SHA256);
        key.update(b"rtdb.multi/family-plaintext-delivery/v1\0");
        key.update(&original.reader.history_fingerprint());
        key.update(family.as_bytes());
        rtdb_storage::sealing::PlaintextView::track_release_check(finish(key), Arc::new(OriginalFamilyDelivery {
            original: original.reader.clone(), deployment: original.deployment, family,
            cancellation: original.cancellation.clone(),
        }))
    }
}

fn check_expiry(expires_at_ms: u64) -> Result<()> {
    let elapsed = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_err(|_| unavailable())?;
    let now = u64::try_from(elapsed.as_millis()).map_err(|_| unavailable())?;
    if expires_at_ms <= now || expires_at_ms - now > MAX_LIVE_MS { return Err(unavailable()); }
    Ok(())
}

fn unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/assertion-family-release",
        "The original family response release authority is unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration(family: Uuid, deployment: Uuid) -> FamilyReleaseRegistration {
        let owner = CarrierJournalIdentity {
            deployment, source_history: [1; 32], installation: [2; 16], registry: [3; 32], original_writer: [4; 32],
        };
        let part = FamilyResponsePart {
            cell_history: [5; 16], usher: [6; 16], conductor: [7; 16], response: [8; 16], ordinal: NonZeroU64::MIN,
        };
        FamilyReleaseRegistration::new(owner, family, part, [9; 32], 10)
    }

    #[test]
    fn recovery_binds_the_exact_part_and_address() {
        let offered = registration(Uuid::new_v4(), Uuid::new_v4());
        let address = offered.address();
        let recovered = FamilyReleaseRegistration::from_address(offered.origin(), offered.attempt(), offered.part(), &address).unwrap();
        assert_eq!(recovered.address(), address);
        assert_eq!(recovered.binding, offered.binding);
        let another_part = FamilyResponsePart { ordinal: NonZeroU64::new(2).unwrap(), ..offered.part() };
        assert!(FamilyReleaseRegistration::from_address(offered.origin(), offered.attempt(), another_part, &address).is_err());

        let later = FamilyReleaseRegistration::new(offered.owner(), offered.client_family, offered.part(), offered.input, 11);
        assert_eq!(later.attempt(), offered.attempt());
        assert_ne!(later.binding, offered.binding);
    }

    #[tokio::test]
    async fn pruning_a_cancelled_hold_leaves_no_reusable_family_cas_revision() -> Result<()> {
        use crate::keys::KeyService;

        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let families = vault.assertion_families(IoRegion::new()).unwrap();
        let deployment = Uuid::new_v4();
        let offered = registration(Uuid::new_v4(), deployment);
        let family_id = offered.family();
        families.enroll_original(family_id, EnrolledWorldRow {
            world: Uuid::new_v4(), assertion: AssertionId::from_bytes([1; 32]),
            capability: AssertionStage::new(deployment, NonZeroU64::MIN, Uuid::new_v4())?,
            scope_commitment: [2; 32], slot_commitment: [3; 32], codec: 1,
        }).await?;
        let (prior, revision) = families.read(family_id).await?.unwrap();
        families.finish_family_release(offered, ReleasePhase::Cancelled).await?;
        assert_eq!(families.read_release_terminal(offered).await?, Some(ReleasePhase::Cancelled));
        let (current, _) = families.read(family_id).await?.unwrap();
        assert!(current.release_holds.is_empty());
        assert!(!families.write(family_id, Some(&revision), &prior).await?);
        families.finish_family_release(offered, ReleasePhase::Cancelled).await?;
        assert!(families.require_family_release(offered).await.is_err());
        Ok(())
    }
}
