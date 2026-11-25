//! Original-owned custody transfer for archived rank contributors.

use std::{collections::BTreeSet, num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use ring::digest::{Context, SHA256, digest};
use rtdb_node::{OriginalAssertionFamilyReader, RetiredOriginalFamilyRead, RetiredOriginalFamilySource};
use rtdb_storage::{IoRegion, sealing::{PlaintextReleaseCheck, PlaintextView}};
use rtdb_types::{Anomaly, AnomalyCategory, OwnedRelation, Position, Result, RootRowCarrier, TransactionKey};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{family_custody::framed_digest, keys::KeyService};

mod landed;
mod wire;

use landed::{LandedRankTransfer, LandedRankTransferContributor};
pub(crate) use wire::Frame as WorldRankFrame;

use super::{
    ATTEMPTS, FamilyReleaseInput, AssertionAuthoring, AssertionEnvelope, AssertionFamilies, AssertionKeyService, AssertionStage,
    AssertionVaultRecord, DerivedRow, Family, FamilyOccurrence, MAX_RECORD_BYTES, WorldRowSlot,
    contention, family_vault_address, malformed, too_large, world_row_scope,
    world_row_slot_commitment,
};

const MAX_CONTRIBUTORS: usize = 512;
const MAX_TRANSFERS: usize = 64;
const WORLD_CODEC: u32 = u32::from_be_bytes(*b"BJWR");
const WORLD_PAYLOAD: &str = "fact_unit_column_payloads";
const MAX_CARRIER_BYTES: usize = 1 << 20;
const MAX_RANK_KEY_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorldRankTransferSource {
    pub original: RetiredOriginalFamilySource,
    pub custody_incarnation: NonZeroU64,
    pub archive: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorldRankTransferDestination {
    pub deployment: Uuid,
    pub installation: Uuid,
    pub custody_incarnation: NonZeroU64,
    pub hold: [u8; 32],
    pub preparation: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorldRankTransferContributor {
    pub family: Uuid,
    pub occurrence: FamilyOccurrence,
    pub transaction: TransactionKey,
    pub rank_position: Position,
    pub rank: DerivedRow,
    pub disclosure: [u8; 32],
    pub founder_carrier_sha256: [u8; 32],
    pub rank_carrier_sha256: [u8; 32],
    /// The authenticated source binds the files, physical event ordinals,
    /// assertion positions, journal, clock and both full identities.
    pub frozen_effects_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorldRankTransfer {
    pub source: WorldRankTransferSource,
    pub destination: WorldRankTransferDestination,
    pub contributors: Vec<WorldRankTransferContributor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RankTransferAgreement {
    World(WorldRankTransfer),
    Landed(LandedRankTransfer),
}

#[derive(Clone, Copy)]
pub(crate) enum RankTransferMember<'a> {
    World(&'a WorldRankTransferContributor),
    Landed(&'a LandedRankTransferContributor),
}

impl RankTransferAgreement {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.starts_with(landed::AGREEMENT) {
            Ok(Self::Landed(LandedRankTransfer::from_bytes(bytes)?))
        } else {
            Ok(Self::World(WorldRankTransfer::from_bytes(bytes)?))
        }
    }

    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Self::World(agreement) => agreement.to_bytes(),
            Self::Landed(agreement) => agreement.to_bytes(),
        }
    }

    pub(crate) fn source(&self) -> WorldRankTransferSource {
        match self {
            Self::World(agreement) => agreement.source,
            Self::Landed(agreement) => agreement.source,
        }
    }

    pub(crate) fn destination(&self) -> WorldRankTransferDestination {
        match self {
            Self::World(agreement) => agreement.destination,
            Self::Landed(agreement) => agreement.destination,
        }
    }

    pub(crate) fn binding(&self) -> Result<WorldRankTransferBinding> {
        match self {
            Self::World(agreement) => agreement.binding(),
            Self::Landed(agreement) => Ok(WorldRankTransferBinding {
                address: WorldRankTransferBinding::landed_archive_address(
                    agreement.source.original.history, agreement.source.archive,
                ),
                agreement: framed_digest(b"rtdb.multi/landed-rank-transfer-agreement/v1\0", &[&agreement.to_bytes()?]),
            }),
        }
    }

    pub(crate) fn inventory(&self) -> Result<[u8; 32]> {
        match self {
            Self::World(agreement) => agreement.inventory(),
            Self::Landed(agreement) => agreement.inventory(),
        }
    }

    fn members(&self) -> impl Iterator<Item = RankTransferMember<'_>> {
        let (world, landed) = match self {
            Self::World(agreement) => (agreement.contributors.as_slice(), None),
            Self::Landed(agreement) => (&[][..], Some(&agreement.member)),
        };
        world.iter().map(RankTransferMember::World).chain(landed.map(RankTransferMember::Landed))
    }

    pub(crate) fn member(&self, family: Uuid) -> Result<RankTransferMember<'_>> {
        match self {
            Self::World(agreement) => agreement.contributors.binary_search_by_key(&family, |member| member.family)
                .map(|index| RankTransferMember::World(&agreement.contributors[index]))
                .map_err(|_| unavailable()),
            Self::Landed(agreement) if agreement.member.family == family => Ok(RankTransferMember::Landed(&agreement.member)),
            Self::Landed(_) => Err(unavailable()),
        }
    }
}

impl<'a> RankTransferMember<'a> {
    fn family(self) -> Uuid {
        match self {
            Self::World(member) => member.family,
            Self::Landed(member) => member.family,
        }
    }

    fn transaction(self, source: WorldRankTransferSource) -> TransactionKey {
        match self {
            Self::World(member) => member.transaction,
            Self::Landed(_) => source.original.carrier,
        }
    }

    fn require_member(self, family: &Family) -> Result<()> {
        match self {
            Self::World(member) => check_member(family, member),
            Self::Landed(member) => member.require_member(family),
        }
    }

    pub(crate) fn release_input(self) -> Result<FamilyReleaseInput<'a>> {
        Ok(match self {
            Self::World(member) => FamilyReleaseInput::WorldDerived {
                member: member.occurrence,
                stage: member.rank.capability.ok_or_else(unavailable)?,
                slot: WorldRowSlot {
                    world: member.rank.world.ok_or_else(unavailable)?,
                    relation: &member.rank.relation,
                    key: &member.rank.key,
                },
            },
            Self::Landed(member) => member.release_input(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldRankTransferBinding {
    pub address: [u8; 32],
    pub agreement: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptedWorldRankReplay {
    pub transaction: TransactionKey,
    pub receipt: [u8; 32],
    pub backing: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorldRankTransferState {
    Prepared,
    Installed { replay: AcceptedWorldRankReplay, copy_drain: [u8; 32] },
    Discarded { copy_drain: [u8; 32] },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorldRankSourceCensus {
    pub source: WorldRankTransferSource,
    pub binding: WorldRankTransferBinding,
    pub inventory: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorldRankDestinationCustody {
    pub destination: WorldRankTransferDestination,
    pub binding: WorldRankTransferBinding,
    pub state: WorldRankTransferState,
    pub generation: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorldRankSourceDrain {
    pub source: WorldRankTransferSource,
    pub binding: WorldRankTransferBinding,
    pub destination: WorldRankTransferState,
    pub generation: [u8; 32],
    pub copy_drain: [u8; 32],
}

/// Trusted host observations. The source census includes every retained rank
/// contributor from all frozen files, including shadowed history. The
/// destination observes its own ordered hold and accepted replay receipt.
#[async_trait::async_trait]
pub trait WorldRankTransferAuthority: Send + Sync {
    async fn source_census(
        &self, source: WorldRankTransferSource, cancellation: &CancellationToken,
    ) -> Result<WorldRankSourceCensus>;
    async fn destination_custody(
        &self, destination: WorldRankTransferDestination, cancellation: &CancellationToken,
    ) -> Result<WorldRankDestinationCustody>;
    async fn source_copy_drain(
        &self, source: WorldRankTransferSource, cancellation: &CancellationToken,
    ) -> Result<WorldRankSourceDrain>;
}

pub struct WorldRankTransferRead<'a> {
    pub contributor: &'a WorldRankTransferContributor,
    pub carrier: &'a [u8],
    pub slot: WorldRowSlot<'a>,
    pub replay: AcceptedWorldRankReplay,
    pub generation: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Preparing,
    Bound,
    Drained { destination: WorldRankTransferState, source_drain: [u8; 32], generation: [u8; 32] },
}

struct TransferRecord {
    agreement: RankTransferAgreement,
    phase: Phase,
    delivery_revoked: bool,
}

impl TransferRecord {
    fn live_delivery(&self, expected: AcceptedWorldRankReplay, generation: [u8; 32]) -> Result<WorldRankTransferState> {
        let Phase::Drained { destination: terminal @ WorldRankTransferState::Installed { replay, .. },
            generation: accepted, .. } = self.phase else { return Err(unavailable()); };
        if self.delivery_revoked || generation == [0; 32] || accepted != generation || replay != expected {
            return Err(unavailable());
        }
        Ok(terminal)
    }
}

impl WorldRankTransferBinding {
    pub(crate) fn archive_address(history: [u8; 32], archive: [u8; 32]) -> [u8; 32] {
        transfer_archive_address(b"rtdb.multi/world-rank-transfer-archive/v1\0", history, archive)
    }

    pub(crate) fn landed_archive_address(history: [u8; 32], archive: [u8; 32]) -> [u8; 32] {
        transfer_archive_address(b"rtdb.multi/landed-rank-transfer-archive/v1\0", history, archive)
    }
}

fn transfer_archive_address(domain: &[u8], history: [u8; 32], archive: [u8; 32]) -> [u8; 32] {
    let mut address = Context::new(&SHA256);
    address.update(domain);
    address.update(&history);
    address.update(&archive);
    finish(address)
}

impl WorldRankTransfer {
    pub fn binding(&self) -> Result<WorldRankTransferBinding> {
        Ok(WorldRankTransferBinding {
            address: WorldRankTransferBinding::archive_address(self.source.original.history, self.source.archive),
            agreement: framed_digest(b"rtdb.multi/world-rank-transfer-agreement/v1\0", &[&self.to_bytes()?]),
        })
    }

    pub fn inventory(&self) -> Result<[u8; 32]> {
        self.validate()?;
        let mut inventory = Vec::new();
        self.put_inventory(&mut inventory)?;
        Ok(framed_digest(b"rtdb.multi/world-rank-transfer-inventory/v1\0", &[&inventory]))
    }

    fn validate(&self) -> Result<()> {
        validate_header(self.source, self.destination)?;
        let source = self.source.original;
        if !(1..=MAX_CONTRIBUTORS).contains(&self.contributors.len())
            || self.contributors.windows(2).any(|pair| pair[0].family >= pair[1].family)
        {
            return Err(conflict());
        }
        let mut slots = BTreeSet::new();
        let mut positions = BTreeSet::new();
        for member in &self.contributors {
            let world = Uuid::from_bytes(member.occurrence.position.line_generation);
            let stage = member.rank.capability.ok_or_else(conflict)?;
            let rank = &member.rank;
            if member.family.is_nil() || world.is_nil() || member.occurrence.assertion.to_bytes() == [0; 32]
                || !valid_key(member.transaction) || !source.carrier.at_or_before(member.transaction)
                || member.transaction == source.fence || !member.transaction.at_or_before(source.fence)
                || member.occurrence.position.publication != member.transaction.system_time.0 as u64
                || member.rank_position.publication != member.occurrence.position.publication
                || member.rank_position.line_generation != *world.as_bytes()
                || !positions.insert((member.transaction.tx_id, member.occurrence.position.ordinal))
                || !positions.insert((member.transaction.tx_id, member.rank_position.ordinal))
                || rank.world != Some(world) || rank.member != member.occurrence.assertion
                || !is_rank_relation(&rank.relation)
                || rank.key.len() > MAX_RANK_KEY_BYTES || RootRowCarrier::parse(&rank.key).is_none()
                || !slots.insert((world, rank.relation.as_str(), rank.key.as_slice()))
                || !scoped(stage, self.source) || !scoped(member.occurrence.capability, self.source)
                || stage == member.occurrence.capability
                || member.disclosure == [0; 32] || member.founder_carrier_sha256 == [0; 32]
                || member.rank_carrier_sha256 == [0; 32] || member.frozen_effects_sha256 == [0; 32]
            {
                return Err(conflict());
            }
        }
        if self.contributors.iter().all(|member| member.transaction != source.carrier) {
            return Err(conflict());
        }
        Ok(())
    }
}

fn validate_header(source: WorldRankTransferSource, target: WorldRankTransferDestination) -> Result<()> {
    let original = source.original;
    if original.deployment.is_nil() || original.history == [0; 32] || original.installation.is_nil()
        || original.handoff == [0; 32] || original.audience_tablet == [0; 16]
        || original.row_custody == [0; 32] || source.archive == [0; 32]
        || ![original.carrier, original.fence, original.through, original.captured].into_iter().all(valid_key)
        || original.carrier == original.fence || !original.carrier.at_or_before(original.fence)
        || !original.fence.at_or_before(original.through) || !original.through.at_or_before(original.captured)
        || target.deployment.is_nil() || target.deployment == original.deployment
        || target.installation != original.target_installation || target.installation == original.installation
        || target.installation.is_nil() || target.hold == [0; 32] || target.preparation == [0; 32]
    {
        return Err(conflict());
    }
    Ok(())
}

impl WorldRankTransferContributor {
    /// Used by the trusted archive census with the exact two frozen cells.
    /// Only the key wrapper is authenticated, and neither projection nor
    /// founding payload is opened.
    pub async fn authenticate_carriers(
        &self, founder: &[u8], rank: &[u8], keys: &dyn AssertionKeyService, region: &Arc<IoRegion>,
    ) -> Result<()> {
        let address = RootRowCarrier::parse(&self.rank.key).ok_or_else(conflict)?.fence_name();
        let slot = WorldRowSlot {
            world: self.rank.world.ok_or_else(conflict)?, relation: WORLD_PAYLOAD, key: address.as_bytes(),
        };
        let stage = self.occurrence.capability;
        let envelope = carrier_body(self, founder, b"BJWC1", stage, self.founder_carrier_sha256)?;
        verify_envelope(self, stage, slot, envelope, keys, region).await?;
        verify_projection(self, rank, keys, region).await?;
        Ok(())
    }
}

impl WorldRankTransferState {
    pub(crate) fn terminal(self) -> bool {
        match self {
            Self::Prepared => false,
            Self::Installed { replay, copy_drain } => valid_key(replay.transaction)
                && replay.receipt != [0; 32] && replay.backing != [0; 32] && copy_drain != [0; 32],
            Self::Discarded { copy_drain } => copy_drain != [0; 32],
        }
    }
}

impl AssertionFamilies<'_> {
    /// Both observations must name the same complete census before the
    /// Original can make an installed successor eligible for a read.
    pub async fn prepare_world_rank_transfer(
        &self, agreement: &WorldRankTransfer, authority: &dyn WorldRankTransferAuthority,
    ) -> Result<WorldRankTransferBinding> {
        self.prepare_rank_transfer(&RankTransferAgreement::World(agreement.clone()), authority).await
    }

    pub(crate) async fn prepare_rank_transfer(
        &self, agreement: &RankTransferAgreement, authority: &dyn WorldRankTransferAuthority,
    ) -> Result<WorldRankTransferBinding> {
        let binding = agreement.binding()?;
        let source = agreement.source();
        let cancellation = self.transfer_cancellation(source)?;
        let original = self.retired_transfer_source(source).await?;
        if let Some((prior, _)) = self.read_rank_transfer(binding.address).await? {
            require_agreement(&prior, agreement)?;
            if prior.delivery_revoked { return Err(unavailable()); }
            match prior.phase {
                Phase::Bound => {
                    self.verify_rank_members(agreement, &original, Some(binding)).await?;
                    confirm_prepared(agreement, binding, authority, cancellation).await?;
                    return Ok(binding);
                }
                Phase::Preparing => (),
                Phase::Drained { .. } => return Err(conflict()),
            }
        }
        confirm_prepared(agreement, binding, authority, cancellation).await?;
        let mut publications = agreement.members().map(|member| member.transaction(source)).collect::<Vec<_>>();
        publications.sort_unstable_by_key(|key| key.tx_id);
        publications.dedup();
        original.require_pre_fence_publications(&publications).await?;
        self.verify_rank_members(agreement, &original, None).await?;
        let record = TransferRecord {
            agreement: agreement.clone(), phase: Phase::Preparing, delivery_revoked: false,
        };
        self.write_rank_transfer(binding, None, &record).await?;
        for _ in 0..ATTEMPTS {
            let (mut record, etag) = self.exact_rank_transfer(binding, agreement).await?;
            if record.delivery_revoked { return Err(unavailable()); }
            match record.phase {
                Phase::Bound => {
                    let original = self.retired_transfer_source(source).await?;
                    self.verify_rank_members(agreement, &original, Some(binding)).await?;
                    confirm_prepared(agreement, binding, authority, cancellation).await?;
                    return Ok(binding);
                }
                Phase::Preparing => (),
                Phase::Drained { .. } => return Err(conflict()),
            }
            for member in agreement.members() {
                let id = family_vault_address(source.original.deployment, member.family());
                self.update(id, |family| {
                    member.require_member(family)?;
                    if family.rank_transfers.contains(&binding) {
                        return Ok(((), false));
                    }
                    if family.rank_transfers.len() == MAX_TRANSFERS {
                        return Err(too_large());
                    }
                    family.rank_transfers.insert(binding);
                    Ok(((), true))
                }).await?;
            }
            let original = self.retired_transfer_source(source).await?;
            self.verify_rank_members(agreement, &original, Some(binding)).await?;
            confirm_prepared(agreement, binding, authority, cancellation).await?;
            record.phase = Phase::Bound;
            if self.write_rank_transfer(binding, Some(&etag), &record).await? {
                return Ok(binding);
            }
        }
        Err(contention())
    }

    /// A durable accepted replay or explicit discard must have positive copy
    /// drains on both sides. Fence settlement waits for this terminal.
    pub async fn record_world_rank_transfer_drain(
        &self, agreement: &WorldRankTransfer, authority: &dyn WorldRankTransferAuthority,
    ) -> Result<()> {
        self.record_rank_transfer_drain(&RankTransferAgreement::World(agreement.clone()), authority).await
    }

    pub(crate) async fn record_rank_transfer_drain(
        &self, agreement: &RankTransferAgreement, authority: &dyn WorldRankTransferAuthority,
    ) -> Result<()> {
        let binding = agreement.binding()?;
        let cancellation = self.transfer_cancellation(agreement.source())?;
        self.retired_transfer_source(agreement.source()).await?;
        for _ in 0..ATTEMPTS {
            let (mut record, etag) = self.exact_rank_transfer(binding, agreement).await?;
            let observed = observe_destination(agreement.destination(), binding, authority, cancellation).await?;
            let destination = observed.state;
            if observed.generation == [0; 32] || !destination.terminal()
                || (matches!(destination, WorldRankTransferState::Installed { .. }) && record.phase == Phase::Preparing)
            {
                return Err(conflict());
            }
            let drain = cancellation.run_until_cancelled(authority.source_copy_drain(agreement.source(), cancellation))
                .await.ok_or_else(unavailable)??;
            if drain.source != agreement.source() || drain.binding != binding
                || drain.destination != destination || drain.generation != observed.generation
                || drain.copy_drain == [0; 32]
            {
                return Err(conflict());
            }
            let terminal = Phase::Drained {
                destination, source_drain: drain.copy_drain, generation: observed.generation,
            };
            if matches!(record.phase, Phase::Drained { .. }) {
                return if record.phase == terminal { Ok(()) } else { Err(conflict()) };
            }
            record.phase = terminal;
            if self.write_rank_transfer(binding, Some(&etag), &record).await? {
                return Ok(());
            }
        }
        Err(contention())
    }

    pub(crate) async fn rank_transfer_agreement(
        &self, binding: WorldRankTransferBinding,
    ) -> Result<RankTransferAgreement> {
        let (record, _) = self.read_rank_transfer(binding.address).await?.ok_or_else(unavailable)?;
        if record.agreement.binding()? != binding {
            return Err(conflict());
        }
        Ok(record.agreement)
    }

    /// A successor owner closes delivery before retiring its accepted backing.
    /// A stale state proof cannot re-enable delivery after this vault CAS.
    pub(crate) async fn revoke_rank_transfer_delivery(&self, agreement: &RankTransferAgreement) -> Result<()> {
        let binding = agreement.binding()?;
        self.retired_transfer_source(agreement.source()).await?;
        for _ in 0..ATTEMPTS {
            let (mut record, etag) = self.exact_rank_transfer(binding, agreement).await?;
            if record.delivery_revoked {
                return Ok(());
            }
            record.delivery_revoked = true;
            if self.write_rank_transfer(binding, Some(&etag), &record).await? {
                return Ok(());
            }
        }
        Err(contention())
    }

    pub(crate) async fn open_transferred_world_rank(
        &self, agreement: Arc<RankTransferAgreement>, read: WorldRankTransferRead<'_>, carrier: Bytes,
        keys: Arc<dyn KeyService>, authority: Arc<dyn WorldRankTransferAuthority>,
    ) -> Result<Bytes> {
        let RankTransferAgreement::World(world) = agreement.as_ref() else { return Err(conflict()); };
        let member = read.contributor;
        let slot = read.slot;
        let rank = &member.rank;
        if read.carrier != carrier.as_ref() || !world.contributors.contains(member)
            || rank.world != Some(slot.world) || rank.relation != slot.relation || rank.key.as_slice() != slot.key
        {
            return Err(conflict());
        }
        if !PlaintextView::has_delivery_scope() || read.generation == [0; 32] {
            return Err(unavailable());
        }
        let binding = agreement.binding()?;
        let (record, _) = self.exact_rank_transfer(binding, &agreement).await?;
        let terminal = record.live_delivery(read.replay, read.generation)?;
        let original = self.retired_transfer_source(agreement.source()).await?;
        self.verify_rank_members(&agreement, &original, Some(binding)).await?;
        let assertion_keys = keys.assertion_keys().ok_or_else(unavailable)?;
        let envelope = verify_projection(member, read.carrier, assertion_keys, &self.region).await?;
        self.track_rank_delivery(agreement, binding, terminal, read.generation, Arc::clone(&keys), authority).await?;
        let stage = member.rank.capability.ok_or_else(conflict)?;
        let envelope_offset = carrier.len() - envelope.len();
        assertion_keys.open_assertion_for_resolution(stage, AssertionAuthoring {
            assertion_id: member.occurrence.assertion,
            scope_commitment: world_row_scope(slot, &member.disclosure),
            codec: WORLD_CODEC,
        }, &self.region, AssertionEnvelope::try_from(carrier.slice(envelope_offset..))?).await
    }

    pub(crate) async fn open_transferred_landed_rank(
        &self, agreement: Arc<RankTransferAgreement>, replay: AcceptedWorldRankReplay, generation: [u8; 32],
        payload: Bytes, keys: Arc<dyn KeyService>, authority: Arc<dyn WorldRankTransferAuthority>,
    ) -> Result<Bytes> {
        let RankTransferAgreement::Landed(landed) = agreement.as_ref() else { return Err(conflict()); };
        landed.member.require_payload(&payload)?;
        let binding = agreement.binding()?;
        let (record, _) = self.exact_rank_transfer(binding, &agreement).await?;
        let terminal = record.live_delivery(replay, generation)?;
        self.track_rank_delivery(agreement, binding, terminal, generation, keys, authority).await?;
        Ok(payload)
    }

    pub(crate) async fn require_landed_rank_release(
        &self, agreement: &RankTransferAgreement, authority: &dyn WorldRankTransferAuthority,
    ) -> Result<()> {
        if !matches!(agreement, RankTransferAgreement::Landed(_)) { return Err(conflict()); }
        let binding = agreement.binding()?;
        let (record, _) = self.exact_rank_transfer(binding, agreement).await?;
        let Phase::Drained {
            destination: terminal @ WorldRankTransferState::Installed { replay, .. }, generation, ..
        } = record.phase else { return Err(unavailable()); };
        record.live_delivery(replay, generation)?;
        self.require_rank_delivery_current(agreement, binding, terminal, generation, authority).await
    }

    async fn track_rank_delivery(
        &self, agreement: Arc<RankTransferAgreement>, binding: WorldRankTransferBinding,
        terminal: WorldRankTransferState, generation: [u8; 32], keys: Arc<dyn KeyService>,
        authority: Arc<dyn WorldRankTransferAuthority>,
    ) -> Result<()> {
        if !PlaintextView::has_delivery_scope() { return Err(unavailable()); }
        let WorldRankTransferState::Installed { replay, .. } = terminal else { return Err(unavailable()); };
        let owner = self.original.as_ref().ok_or_else(unavailable)?;
        let check = Arc::new(TransferredRankDelivery {
            original: owner.reader.clone(), cancellation: owner.cancellation.clone(), authority,
            keys, region: Arc::clone(&self.region), agreement, binding, terminal, generation,
        });
        check.require_current().await?;
        let key = framed_digest(b"rtdb.multi/world-rank-transfer-delivery/v1\0", &[
            &binding.address, &binding.agreement, &replay.receipt, &replay.backing, &generation,
        ]);
        PlaintextView::track_release_check(key, check)
    }

    async fn require_rank_delivery_current(
        &self, agreement: &RankTransferAgreement, binding: WorldRankTransferBinding,
        terminal: WorldRankTransferState, generation: [u8; 32], authority: &dyn WorldRankTransferAuthority,
    ) -> Result<()> {
        let cancellation = self.transfer_cancellation(agreement.source())?;
        let observed = observe_destination(agreement.destination(), binding, authority, cancellation).await?;
        require_current_destination(observed, terminal, generation)?;
        let (record, _) = self.exact_rank_transfer(binding, agreement).await?;
        let WorldRankTransferState::Installed { replay, .. } = terminal else { return Err(unavailable()); };
        if record.live_delivery(replay, generation)? != terminal {
            return Err(unavailable());
        }
        let original = self.retired_transfer_source(agreement.source()).await?;
        self.verify_rank_members(agreement, &original, Some(binding)).await
    }

    pub(super) async fn world_rank_transfers_drained(&self, family: &Family) -> Result<bool> {
        for &binding in &family.rank_transfers {
            let (record, _) = self.read_rank_transfer(binding.address).await?.ok_or_else(unavailable)?;
            if record.agreement.binding()? != binding { return Err(conflict()); }
            if !matches!(record.phase, Phase::Drained { .. }) { return Ok(false); }
        }
        Ok(true)
    }

    fn transfer_cancellation(&self, source: WorldRankTransferSource) -> Result<&CancellationToken> {
        let owner = self.original.as_ref().ok_or_else(unavailable)?;
        if owner.deployment != source.original.deployment
            || owner.reader.history_fingerprint() != source.original.history || owner.cancellation.is_cancelled()
        {
            return Err(unavailable());
        }
        Ok(owner.cancellation)
    }

    async fn retired_transfer_source(&self, source: WorldRankTransferSource) -> Result<RetiredOriginalFamilyRead> {
        let cancellation = self.transfer_cancellation(source)?;
        let owner = self.original.as_ref().ok_or_else(unavailable)?;
        owner.reader.current_retired(&source.original, cancellation).await
    }

    async fn verify_rank_members(
        &self, agreement: &RankTransferAgreement, original: &RetiredOriginalFamilyRead,
        binding: Option<WorldRankTransferBinding>,
    ) -> Result<()> {
        for member in agreement.members() {
            require_no_fence(original, member.family()).await?;
            match member {
                RankTransferMember::World(member) => {
                    self.require_position_open(member.occurrence.position).await?;
                    self.require_position_open(member.rank_position).await?;
                }
                RankTransferMember::Landed(member) => {
                    self.require_position_open(member.landing.position).await?;
                    self.require_position_open(member.carrier_position).await?;
                    self.require_position_open(member.rank_position).await?;
                }
            }
            let id = family_vault_address(agreement.source().original.deployment, member.family());
            let (record, _) = self.read_current(id, true).await?.ok_or_else(conflict)?;
            member.require_member(&record)?;
            if binding.is_some_and(|binding| !record.rank_transfers.contains(&binding)) {
                return Err(conflict());
            }
        }
        Ok(())
    }

    async fn exact_rank_transfer(
        &self, binding: WorldRankTransferBinding, agreement: &RankTransferAgreement,
    ) -> Result<(TransferRecord, String)> {
        let (record, etag) = self.read_rank_transfer(binding.address).await?.ok_or_else(unavailable)?;
        require_agreement(&record, agreement)?;
        Ok((record, etag))
    }

    async fn read_rank_transfer(&self, address: [u8; 32]) -> Result<Option<(TransferRecord, String)>> {
        let Some((bytes, etag)) = self.journal.read_record(
            AssertionVaultRecord::WorldRankTransfer(address), &self.region,
        ).await? else { return Ok(None); };
        if bytes.len() > MAX_RECORD_BYTES { return Err(too_large()); }
        let record = TransferRecord::from_bytes(&bytes)?;
        if record.agreement.binding()?.address != address
            || matches!(record.phase, Phase::Drained { destination, source_drain, generation }
                if !destination.terminal() || source_drain == [0; 32] || generation == [0; 32])
        {
            return Err(malformed());
        }
        Ok(Some((record, etag)))
    }

    async fn write_rank_transfer(
        &self, binding: WorldRankTransferBinding, previous: Option<&str>, record: &TransferRecord,
    ) -> Result<bool> {
        self.journal.write_record(
            AssertionVaultRecord::WorldRankTransfer(binding.address), &self.region, previous, record.to_bytes()?.into(),
        ).await
    }
}

struct TransferredRankDelivery {
    original: OriginalAssertionFamilyReader,
    cancellation: CancellationToken,
    authority: Arc<dyn WorldRankTransferAuthority>,
    keys: Arc<dyn KeyService>,
    region: Arc<IoRegion>,
    agreement: Arc<RankTransferAgreement>,
    binding: WorldRankTransferBinding,
    terminal: WorldRankTransferState,
    generation: [u8; 32],
}

#[async_trait::async_trait]
impl PlaintextReleaseCheck for TransferredRankDelivery {
    async fn require_current(&self) -> Result<()> {
        let families = self.keys.assertion_families(Arc::clone(&self.region)).ok_or_else(unavailable)?
            .with_original_reader(&self.original, self.agreement.source().original.deployment, &self.cancellation);
        families.require_rank_delivery_current(
            &self.agreement, self.binding, self.terminal, self.generation, self.authority.as_ref(),
        ).await
    }
}

fn require_current_destination(
    observed: WorldRankDestinationCustody, terminal: WorldRankTransferState, generation: [u8; 32],
) -> Result<()> {
    if observed.state != terminal || observed.generation != generation || generation == [0; 32] {
        return Err(unavailable());
    }
    Ok(())
}

async fn require_no_fence(original: &RetiredOriginalFamilyRead, family: Uuid) -> Result<()> {
    if original.current().assertion_family_fence(family).await?.is_some() {
        return Err(rtdb_storage::Error::ErasedUnderView.into());
    }
    Ok(())
}

async fn confirm_prepared(
    agreement: &RankTransferAgreement, binding: WorldRankTransferBinding,
    authority: &dyn WorldRankTransferAuthority, cancellation: &CancellationToken,
) -> Result<()> {
    let source = cancellation.run_until_cancelled(authority.source_census(agreement.source(), cancellation))
        .await.ok_or_else(unavailable)??;
    if source != (WorldRankSourceCensus { source: agreement.source(), binding, inventory: agreement.inventory()? })
        || observe_destination(agreement.destination(), binding, authority, cancellation).await?.state != WorldRankTransferState::Prepared
    {
        return Err(conflict());
    }
    Ok(())
}

async fn observe_destination(
    destination: WorldRankTransferDestination, binding: WorldRankTransferBinding,
    authority: &dyn WorldRankTransferAuthority, cancellation: &CancellationToken,
) -> Result<WorldRankDestinationCustody> {
    let observed = cancellation.run_until_cancelled(authority.destination_custody(destination, cancellation))
        .await.ok_or_else(unavailable)??;
    if observed.destination != destination || observed.binding != binding { return Err(conflict()); }
    Ok(observed)
}

fn check_member(family: &Family, member: &WorldRankTransferContributor) -> Result<()> {
    family.require_live()?;
    family.require_unclaimed()?;
    let row = RootRowCarrier::parse(&member.rank.key).ok_or_else(conflict)?.fence_name();
    let world = member.rank.world.ok_or_else(conflict)?;
    let slot = WorldRowSlot { world, relation: WORLD_PAYLOAD, key: row.as_bytes() };
    if family.original.assertion != member.occurrence.assertion
        || family.admitted_member(member.occurrence.assertion) != Some(member.occurrence)
        || family.original.world != world || family.original.codec != WORLD_CODEC
        || family.original.slot_commitment != world_row_slot_commitment(slot)
        || family.original.scope_commitment != world_row_scope(slot, &member.disclosure)
        || !family.rewrites.is_empty() || !family.landings.is_empty()
        || family.derived.as_slice() != std::slice::from_ref(&member.rank)
    {
        return Err(conflict());
    }
    Ok(())
}

async fn verify_projection<'a>(
    member: &WorldRankTransferContributor, carrier: &'a [u8], keys: &dyn AssertionKeyService,
    region: &Arc<IoRegion>,
) -> Result<&'a [u8]> {
    let stage = member.rank.capability.ok_or_else(conflict)?;
    let body = carrier_body(member, carrier, b"BJWP1", stage, member.rank_carrier_sha256)?;
    let (disclosure, envelope) = body.split_first_chunk::<32>().ok_or_else(conflict)?;
    if *disclosure != member.disclosure { return Err(conflict()); }
    let slot = WorldRowSlot {
        world: member.rank.world.ok_or_else(conflict)?, relation: &member.rank.relation, key: &member.rank.key,
    };
    verify_envelope(member, stage, slot, envelope, keys, region).await?;
    Ok(envelope)
}

fn carrier_body<'a>(
    member: &WorldRankTransferContributor, carrier: &'a [u8], tag: &[u8],
    capability: AssertionStage, expected: [u8; 32],
) -> Result<&'a [u8]> {
    if carrier.len() > MAX_CARRIER_BYTES || digest(&SHA256, carrier).as_ref() != expected.as_slice() {
        return Err(conflict());
    }
    let body = carrier.strip_prefix(tag).ok_or_else(conflict)?;
    let (family, body) = body.split_first_chunk::<16>().ok_or_else(conflict)?;
    let (stage, body) = body.split_first_chunk::<40>().ok_or_else(conflict)?;
    let (assertion, body) = body.split_first_chunk::<32>().ok_or_else(conflict)?;
    let (source, body) = body.split_first_chunk::<32>().ok_or_else(conflict)?;
    if family != member.family.as_bytes() || stage[..16] != *capability.deployment().as_bytes()
        || stage[16..24] != capability.incarnation().get().to_be_bytes()
        || stage[24..] != *capability.identity().as_bytes()
        || assertion != source || *assertion != member.occurrence.assertion.to_bytes()
    {
        return Err(conflict());
    }
    Ok(body)
}

async fn verify_envelope(
    member: &WorldRankTransferContributor, stage: AssertionStage, slot: WorldRowSlot<'_>,
    envelope: &[u8], keys: &dyn AssertionKeyService, region: &Arc<IoRegion>,
) -> Result<()> {
    keys.verify_assertion_envelope(stage, AssertionAuthoring {
        assertion_id: member.occurrence.assertion,
        scope_commitment: world_row_scope(slot, &member.disclosure),
        codec: WORLD_CODEC,
    }, region, AssertionEnvelope::try_from(Bytes::copy_from_slice(envelope))?).await
}

fn scoped(stage: AssertionStage, source: WorldRankTransferSource) -> bool {
    stage.deployment() == source.original.deployment && stage.incarnation() == source.custody_incarnation
}

fn is_rank_relation(name: &str) -> bool {
    OwnedRelation::new(name).is_ok() && name.strip_prefix("fact_")
        .and_then(|name| name.strip_suffix("_search"))
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|c| c.is_ascii_digit() || matches!(c, b'a'..=b'f')))
}

fn valid_key(key: TransactionKey) -> bool { key.tx_id.0 >= 0 && key.system_time.0 > 0 }

fn require_agreement(record: &TransferRecord, agreement: &RankTransferAgreement) -> Result<()> {
    if record.agreement != *agreement { return Err(conflict()); }
    Ok(())
}

fn finish(hash: Context) -> [u8; 32] {
    hash.finish().as_ref().try_into().expect("SHA-256 commitment")
}

fn conflict() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/world-rank-transfer",
        "World rank transfer differs from its exact Original custody agreement")
}

fn unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/world-rank-transfer-unavailable",
        "World rank transfer has no complete current custody proof")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::{EnrolledWorldRow, LandingReservation, LocalKeyService};
    use rtdb_types::{AssertionId, RootFamilyDerivativeId, RootLandingBinding, RootLandingTarget, Timestamp, TxId};

    fn key(tx: i64, micros: i64) -> TransactionKey {
        TransactionKey { tx_id: TxId(tx), system_time: Timestamp(micros) }
    }

    fn id(byte: u8) -> Uuid { Uuid::from_bytes([byte; 16]) }

    fn agreement() -> Result<WorldRankTransfer> {
        let incarnation = NonZeroU64::MIN;
        let deployment = id(1);
        let world = id(8);
        let position = Position { line_generation: *world.as_bytes(), publication: 40, ordinal: NonZeroU64::MIN };
        let occurrence = FamilyOccurrence {
            position, assertion: AssertionId::from_bytes([9; 32]),
            capability: AssertionStage::new(deployment, incarnation, id(10))?,
        };
        let mut address = RootRowCarrier::table_prefix(&[11; 16]).to_vec();
        address.extend_from_slice(&[0, 255, 0, 255]);
        address.extend_from_slice(&[12; 64]);
        address.extend_from_slice(&[0, 1]);
        Ok(WorldRankTransfer {
            source: WorldRankTransferSource {
                original: RetiredOriginalFamilySource {
                    deployment, history: [2; 32], installation: id(3), target_installation: id(4),
                    audience_tablet: [5; 16], row_custody: [6; 32], handoff: [7; 32],
                    carrier: key(4, 40), fence: key(5, 50), through: key(6, 60), captured: key(7, 70),
                },
                custody_incarnation: incarnation, archive: [13; 32],
            },
            destination: WorldRankTransferDestination {
                deployment: id(14), installation: id(4), custody_incarnation: incarnation, hold: [15; 32],
                preparation: [16; 32],
            },
            contributors: vec![WorldRankTransferContributor {
                family: id(17), occurrence, transaction: key(4, 40),
                rank_position: Position { ordinal: NonZeroU64::new(2).unwrap(), ..position },
                rank: DerivedRow {
                    member: occurrence.assertion, world: Some(world), relation: format!("fact_{}_search", "a".repeat(64)),
                    key: address,
                    capability: Some(AssertionStage::new(deployment, incarnation, id(18))?),
                },
                disclosure: [19; 32], founder_carrier_sha256: [20; 32], rank_carrier_sha256: [21; 32],
                frozen_effects_sha256: [22; 32],
            }],
        })
    }

    fn landed_agreement() -> Result<LandedRankTransfer> {
        let world = agreement()?;
        let founding = &world.contributors[0];
        let stage = AssertionStage::new(world.source.original.deployment, world.source.custody_incarnation, id(29))?;
        let assertion = AssertionId::from_bytes([30; 32]);
        let position = Position { line_generation: [31; 16], ..founding.occurrence.position };
        let landing = RootLandingBinding {
            position, assertion, stage: *stage.identity().as_bytes(),
            target: RootLandingTarget { receipt: [32; 32], review_slot: 0, address_extent: [33; 32], disclosure: [34; 32] },
            payload_digest: [35; 32], link_digest: [36; 32],
        };
        let key = [RootFamilyDerivativeId::PREFIX, founding.family.as_bytes(),
            &assertion.to_bytes(), founding.rank.key.as_slice()].concat();
        Ok(LandedRankTransfer {
            source: world.source,
            destination: world.destination,
            member: LandedRankTransferContributor {
                family: founding.family, stage, landing,
                carrier_position: Position { ordinal: NonZeroU64::new(2).unwrap(), ..position },
                rank_position: Position { ordinal: NonZeroU64::new(3).unwrap(), ..position },
                relation: founding.rank.relation.clone(), key,
                carrier_sha256: [37; 32], rank_payload_sha256: [38; 32], frozen_effects_sha256: [39; 32],
            },
        })
    }

    #[test]
    fn landed_agreement_binds_one_exact_non_expiring_original_and_held_target() -> Result<()> {
        let landed = landed_agreement()?;
        let agreement = RankTransferAgreement::Landed(landed.clone());
        let bytes = agreement.to_bytes()?;
        let binding = agreement.binding()?;
        assert_eq!(&bytes[..4], b"WRT2");
        assert_eq!(&bytes[372..374], &1u16.to_be_bytes());
        assert_eq!(&bytes[742..750], &i64::MAX.to_be_bytes());
        assert_eq!(&bytes[750..758], &i64::MAX.to_be_bytes());
        assert_eq!(bytes.len(), 858 + landed.member.relation.len() + landed.member.key.len());
        assert_eq!(RankTransferAgreement::from_bytes(&bytes)?, agreement);
        assert!(WorldRankTransfer::from_bytes(&bytes).is_err());

        let world = WorldRankTransferBinding::archive_address(landed.source.original.history, landed.source.archive);
        assert_ne!(binding.address, world);
        for offset in [373, 742, 750] {
            let mut invalid = bytes.clone();
            invalid[offset] ^= 1;
            assert!(RankTransferAgreement::from_bytes(&invalid).is_err());
        }
        assert!(RankTransferAgreement::from_bytes(&bytes[..bytes.len() - 1]).is_err());
        let mut extra = bytes;
        extra.push(0);
        assert!(RankTransferAgreement::from_bytes(&extra).is_err());

        let mut other = landed.clone();
        let suffix = other.member.key.len() - 3;
        other.member.key[suffix] ^= 1;
        let other = RankTransferAgreement::Landed(other);
        assert_ne!(other.binding()?.agreement, binding.agreement);
        assert_ne!(other.inventory()?, agreement.inventory()?);
        let mut other = landed.clone();
        other.member.frozen_effects_sha256[0] ^= 1;
        let other = RankTransferAgreement::Landed(other);
        assert_ne!(other.binding()?.agreement, binding.agreement);
        assert_ne!(other.inventory()?, agreement.inventory()?);
        let mut other = landed.clone();
        other.destination.hold[0] ^= 1;
        let other = RankTransferAgreement::Landed(other);
        assert_ne!(other.binding()?.agreement, binding.agreement);
        assert_eq!(other.inventory()?, agreement.inventory()?);
        let mut wrong = landed.clone();
        wrong.member.rank_position.publication += 1;
        assert!(RankTransferAgreement::Landed(wrong).binding().is_err());
        let mut wrong = landed.clone();
        wrong.member.rank_position = wrong.member.landing.position;
        assert!(RankTransferAgreement::Landed(wrong).binding().is_err());
        let mut wrong = landed;
        wrong.member.key[5] ^= 1;
        assert!(RankTransferAgreement::Landed(wrong).binding().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn landed_rank_enrollment_requires_the_scoped_carrier_and_exact_accepted_derivative() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let families = vault.assertion_families(Arc::clone(&region)).unwrap();
        let world = agreement()?;
        let founder = world.contributors[0].occurrence;
        let mut landed = landed_agreement()?;
        let member = &mut landed.member;
        let family = family_vault_address(landed.source.original.deployment, member.family);
        let original_position = Position { publication: 30, ..founder.position };
        let reservation = LandingReservation {
            source: original_position, destination: member.landing.assertion, capability: member.stage,
            link_capability: AssertionStage::new(member.stage.deployment(), member.stage.incarnation(), id(41))?,
            review_commitment: [42; 32],
        };
        families.enroll_original(family, EnrolledWorldRow {
            world: Uuid::from_bytes(founder.position.line_generation), assertion: founder.assertion,
            capability: founder.capability, scope_commitment: [43; 32], slot_commitment: [44; 32], codec: WORLD_CODEC,
        }).await?;
        families.register(family, FamilyOccurrence { position: original_position, ..founder }).await?;
        families.reserve_landing(family, reservation).await?;

        let target = member.landing.target;
        let envelope = Bytes::from(families.seal_root_landing_payload(
            family, reservation, target, &vault, Bytes::from_static(b"ranked Root carrier"),
        ).await?);
        member.landing.payload_digest = finish_bytes(&envelope);
        let reviewed = Position { publication: 35, ..original_position };
        let link = Bytes::from(families.seal_root_landing_link(
            family, reservation, target, reviewed, member.landing.payload_digest, &vault,
        ).await?);
        member.landing.link_digest = finish_bytes(&link);
        let mut carrier = b"BJRL1".to_vec();
        carrier.extend_from_slice(member.family.as_bytes());
        wire::put_stage(&mut carrier, member.stage);
        carrier.extend_from_slice(&member.landing.assertion.to_bytes());
        carrier.extend_from_slice(&target.to_bytes());
        carrier.extend_from_slice(&member.landing.link_digest);
        carrier.extend_from_slice(&envelope);
        member.carrier_sha256 = finish_bytes(&carrier);
        member.rank_payload_sha256 = finish_bytes(b"frozen rank payload");

        member.authenticate_carrier(&carrier, &vault, &region).await?;
        member.require_payload(b"frozen rank payload")?;
        assert!(member.require_payload(b"other rank payload").is_err());
        let (pending, _) = families.read(family).await?.unwrap();
        assert!(member.require_member(&pending).is_err());
        families.activate_root_landing(family, reservation, member.landing, link, &vault).await?;
        let (unregistered, _) = families.read(family).await?.unwrap();
        assert!(member.require_member(&unregistered).is_err());
        families.register_derived(family, &[DerivedRow {
            member: member.landing.assertion, world: None, capability: None,
            relation: member.relation.clone(), key: member.key.clone(),
        }]).await?;
        let (active, _) = families.read(family).await?.unwrap();
        member.require_member(&active)?;
        let FamilyReleaseInput::RootDerived { member: occurrence, .. } = member.release_input()
            else { unreachable!(); };
        let point = FamilyReleaseInput::RootLanding { member: occurrence, binding: member.landing };
        assert_ne!(point.commitment()?, member.release_input().commitment()?);
        let mut other = member.clone();
        other.landing.link_digest[0] ^= 1;
        assert!(other.require_member(&active).is_err());
        assert!(other.authenticate_carrier(&carrier, &vault, &region).await.is_err());
        other = member.clone();
        other.relation = format!("fact_{}_search", "b".repeat(64));
        assert!(other.require_member(&active).is_err());
        other = member.clone();
        let suffix = other.key.len() - 3;
        other.key[suffix] ^= 1;
        assert!(other.require_member(&active).is_err());
        let position = member.landing.position;

        let agreement = RankTransferAgreement::Landed(landed);
        let binding = agreement.binding()?;
        let mut record = TransferRecord { agreement, phase: Phase::Preparing, delivery_revoked: false };
        assert!(families.write_rank_transfer(binding, None, &record).await?);
        families.update(family, |family| Ok(((), family.rank_transfers.insert(binding)))).await?;
        let fence = families.fence(family, position, Uuid::new_v4()).await?;
        assert!(fence.rank_transfers.contains(&binding));
        let (fenced, _) = families.read(family).await?.unwrap();
        let RankTransferAgreement::Landed(landed) = &record.agreement else { unreachable!(); };
        assert!(landed.member.require_member(&fenced).is_err());
        assert!(!families.world_rank_transfers_drained(&fenced).await?);

        let destination = installation();
        let WorldRankTransferState::Installed { replay, .. } = destination else { unreachable!(); };
        let generation = [45; 32];
        assert!(record.live_delivery(replay, generation).is_err());
        record.phase = Phase::Drained { destination, source_drain: [46; 32], generation };
        assert_eq!(record.live_delivery(replay, generation)?, destination);
        assert!(record.live_delivery(replay, [47; 32]).is_err());
        record.delivery_revoked = true;
        let (_, etag) = families.read_rank_transfer(binding.address).await?.unwrap();
        assert!(families.write_rank_transfer(binding, Some(&etag), &record).await?);
        let (current, _) = families.read_rank_transfer(binding.address).await?.unwrap();
        assert!(current.live_delivery(replay, generation).is_err());
        assert!(families.world_rank_transfers_drained(&fenced).await?);
        Ok(())
    }

    fn finish_bytes(bytes: &[u8]) -> [u8; 32] {
        digest(&SHA256, bytes).as_ref().try_into().unwrap()
    }

    fn installation() -> WorldRankTransferState {
        WorldRankTransferState::Installed {
            replay: AcceptedWorldRankReplay { transaction: key(103, 500), receipt: [23; 32], backing: [24; 32] },
            copy_drain: [25; 32],
        }
    }

    struct CensusFixture {
        source: WorldRankSourceCensus,
        destination: WorldRankDestinationCustody,
    }

    #[async_trait::async_trait]
    impl WorldRankTransferAuthority for CensusFixture {
        async fn source_census(
            &self, _source: WorldRankTransferSource, _cancellation: &CancellationToken,
        ) -> Result<WorldRankSourceCensus> {
            Ok(self.source)
        }

        async fn destination_custody(
            &self, _destination: WorldRankTransferDestination, _cancellation: &CancellationToken,
        ) -> Result<WorldRankDestinationCustody> {
            Ok(self.destination)
        }

        async fn source_copy_drain(
            &self, _source: WorldRankTransferSource, _cancellation: &CancellationToken,
        ) -> Result<WorldRankSourceDrain> {
            Err(unavailable())
        }
    }

    #[tokio::test]
    async fn preparation_requires_the_full_census_and_reciprocal_hold() -> Result<()> {
        let world = agreement()?;
        let agreement = RankTransferAgreement::World(world.clone());
        let binding = agreement.binding()?;
        let cancellation = CancellationToken::new();
        let mut evidence = CensusFixture {
            source: WorldRankSourceCensus { source: world.source, binding, inventory: agreement.inventory()? },
            destination: WorldRankDestinationCustody {
                destination: world.destination, binding, state: WorldRankTransferState::Prepared, generation: [31; 32],
            },
        };
        confirm_prepared(&agreement, binding, &evidence, &cancellation).await?;
        let exact = evidence.source.inventory;
        evidence.source.inventory = [0; 32];
        assert!(confirm_prepared(&agreement, binding, &evidence, &cancellation).await.is_err());
        evidence.source.inventory = exact;
        evidence.destination.destination.hold = [0; 32];
        assert!(confirm_prepared(&agreement, binding, &evidence, &cancellation).await.is_err());
        evidence.destination.destination = world.destination;
        evidence.destination.state = installation();
        assert!(confirm_prepared(&agreement, binding, &evidence, &cancellation).await.is_err());
        Ok(())
    }

    #[test]
    fn agreement_binds_complete_binary_slot_and_refuses_a_backfill() -> Result<()> {
        let original = agreement()?;
        let binding = original.binding()?;
        let mut substitute = original.clone();
        substitute.contributors[0].rank.key[1] ^= 1;
        assert_eq!(substitute.binding()?.address, binding.address);
        assert_ne!(substitute.binding()?.agreement, binding.agreement);
        assert_ne!(substitute.inventory()?, original.inventory()?);
        substitute = original.clone();
        substitute.contributors[0].rank_position.publication += 1;
        assert!(substitute.binding().is_err());
        substitute = original.clone();
        substitute.destination.installation = id(99);
        assert!(substitute.binding().is_err());
        substitute = original.clone();
        substitute.contributors[0].rank.world = None;
        assert!(substitute.binding().is_err());
        substitute = original.clone();
        substitute.contributors.push(substitute.contributors[0].clone());
        assert!(substitute.binding().is_err());
        Ok(())
    }

    #[test]
    fn raw_agreement_keeps_complete_binary_keys_and_rejects_trailing_bytes() -> Result<()> {
        let agreement = agreement()?;
        let bytes = agreement.to_bytes()?;
        assert_eq!(&bytes[..4], b"WRT1");
        assert_eq!(RankTransferAgreement::from_bytes(&bytes)?.to_bytes()?, bytes);
        let binary = agreement.contributors[0].rank.key.as_slice();
        assert!(bytes.windows(binary.len()).any(|value| value == binary));
        let round_trip = WorldRankTransfer::from_bytes(&bytes)?;
        assert_eq!(round_trip, agreement);
        assert_eq!(round_trip.binding()?, agreement.binding()?);
        assert_eq!(round_trip.inventory()?, agreement.inventory()?);
        assert!(WorldRankTransfer::from_bytes(&bytes[..bytes.len() - 1]).is_err());
        let mut extra = bytes;
        extra.push(0);
        assert!(WorldRankTransfer::from_bytes(&extra).is_err());
        Ok(())
    }

    #[test]
    fn delivery_requires_the_accepted_receipt_and_current_generation() -> Result<()> {
        let agreement = agreement()?;
        let state = installation();
        let observed = WorldRankDestinationCustody {
            destination: agreement.destination, binding: agreement.binding()?, state, generation: [26; 32],
        };
        require_current_destination(observed, state, [26; 32])?;
        assert!(require_current_destination(observed, state, [27; 32]).is_err());
        let WorldRankTransferState::Installed { replay, copy_drain } = state else { unreachable!() };
        let other = WorldRankTransferState::Installed {
            replay: AcceptedWorldRankReplay { receipt: [28; 32], ..replay }, copy_drain,
        };
        assert!(require_current_destination(observed, other, [26; 32]).is_err());
        assert!(require_current_destination(
            WorldRankDestinationCustody { state: WorldRankTransferState::Prepared, ..observed }, state, [26; 32],
        ).is_err());
        Ok(())
    }

    #[test]
    fn a_positive_drain_binds_one_delivery_generation_until_irreversible_revoke() -> Result<()> {
        let accepted = installation();
        let WorldRankTransferState::Installed { replay, .. } = accepted else { unreachable!(); };
        let mut record = TransferRecord {
            agreement: RankTransferAgreement::World(agreement()?),
            phase: Phase::Bound,
            delivery_revoked: false,
        };
        let generation = [11; 32];
        assert!(record.live_delivery(replay, generation).is_err());
        record.phase = Phase::Drained { destination: accepted, source_drain: [12; 32], generation };
        assert_eq!(record.live_delivery(replay, generation)?, accepted);
        assert!(record.live_delivery(replay, [13; 32]).is_err());
        assert!(record.live_delivery(AcceptedWorldRankReplay { backing: [14; 32], ..replay }, generation).is_err());
        record.delivery_revoked = true;

        let durable = TransferRecord::from_bytes(&record.to_bytes()?)?;
        assert!(durable.live_delivery(replay, generation).is_err());
        assert!(matches!(durable.phase, Phase::Drained { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn exact_carriers_and_pending_copy_participate_in_the_original_vault_fence() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let families = vault.assertion_families(Arc::clone(&region)).unwrap();
        let mut agreement = agreement()?;
        let member = &mut agreement.contributors[0];
        let world = member.rank.world.unwrap();
        let base = RootRowCarrier::parse(&member.rank.key).unwrap().fence_name();
        let founder_slot = WorldRowSlot { world, relation: WORLD_PAYLOAD, key: base.as_bytes() };
        let rank_slot = WorldRowSlot { world, relation: &member.rank.relation, key: &member.rank.key };
        let author = |slot| AssertionAuthoring { assertion_id: member.occurrence.assertion,
            scope_commitment: world_row_scope(slot, &member.disclosure), codec: WORLD_CODEC };
        let founder_stage = member.occurrence.capability;
        let rank_stage = member.rank.capability.unwrap();
        let head = |magic: &[u8], stage: AssertionStage| {
            [magic, member.family.as_bytes(), stage.deployment().as_bytes(), &stage.incarnation().get().to_be_bytes(),
                stage.identity().as_bytes(), &member.occurrence.assertion.to_bytes(), &member.occurrence.assertion.to_bytes()].concat()
        };
        let mut founder = head(b"BJWC1", founder_stage);
        let mut rank = head(b"BJWP1", rank_stage);
        rank.extend_from_slice(&member.disclosure);
        let founder_envelope = vault.seal_assertion_stage(founder_stage, author(founder_slot), &region, Bytes::from_static(b"founder")).await?;
        let rank_envelope = vault.seal_assertion_stage(rank_stage, author(rank_slot), &region, Bytes::from_static(b"rank")).await?;
        founder.extend_from_slice(&Bytes::from(founder_envelope));
        rank.extend_from_slice(&Bytes::from(rank_envelope));
        let enrolled = EnrolledWorldRow {
            world, assertion: member.occurrence.assertion, capability: founder_stage, scope_commitment: author(founder_slot).scope_commitment,
            slot_commitment: world_row_slot_commitment(founder_slot), codec: WORLD_CODEC,
        };
        member.founder_carrier_sha256 = digest(&SHA256, &founder).as_ref().try_into().unwrap();
        member.rank_carrier_sha256 = digest(&SHA256, &rank).as_ref().try_into().unwrap();
        member.authenticate_carriers(&founder, &rank, &vault, &region).await?;
        let mut wrong = member.clone();
        wrong.rank.key[1] ^= 1;
        assert!(wrong.authenticate_carriers(&founder, &rank, &vault, &region).await.is_err());
        let id = family_vault_address(agreement.source.original.deployment, member.family);
        families.enroll_original(id, enrolled).await?;
        families.register_derived(id, std::slice::from_ref(&member.rank)).await?;
        let (pending, _) = families.read(id).await?.unwrap();
        assert!(check_member(&pending, member).is_err());
        families.register(id, member.occurrence).await?;
        let (accepted, _) = families.read(id).await?.unwrap();
        check_member(&accepted, member)?;
        let binding = agreement.binding()?;
        let preparing = TransferRecord {
            agreement: RankTransferAgreement::World(agreement.clone()), phase: Phase::Preparing, delivery_revoked: false,
        };
        assert!(families.write_rank_transfer(binding, None, &preparing).await?);
        families.update(id, |family| Ok(((), family.rank_transfers.insert(binding)))).await?;
        let order = Uuid::new_v4();
        let fence = families.fence(id, agreement.contributors[0].occurrence.position, order).await?;
        assert_eq!(fence.rank_transfers, BTreeSet::from([binding]));
        assert_ne!(accepted.fence_inventory(order).vault_inventory(), fence.vault_inventory());
        let (fenced, _) = families.read(id).await?.unwrap();
        assert!(!families.world_rank_transfers_drained(&fenced).await?);
        assert!(check_member(&fenced, &agreement.contributors[0]).is_err());
        let (_, etag) = families.exact_rank_transfer(binding, &RankTransferAgreement::World(agreement.clone())).await?;
        let discarded = TransferRecord {
            agreement: RankTransferAgreement::World(agreement.clone()),
            phase: Phase::Drained {
                destination: WorldRankTransferState::Discarded { copy_drain: [29; 32] },
                source_drain: [30; 32], generation: [31; 32],
            },
            delivery_revoked: true,
        };
        assert!(families.write_rank_transfer(binding, Some(&etag), &discarded).await?);
        assert!(families.world_rank_transfers_drained(&fenced).await?);
        vault.retire_assertion_stage(rank_stage, &region).await?;
        assert!(agreement.contributors[0].authenticate_carriers(&founder, &rank, &vault, &region).await.is_err());
        Ok(())
    }
}
