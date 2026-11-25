//! Original-owned transfer of founding World rank custody.

use std::{collections::BTreeSet, num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use ring::digest::{Context, SHA256, digest};
use rtdb_node::{OriginalAssertionFamilyReader, RetiredOriginalFamilyRead, RetiredOriginalFamilySource};
use rtdb_storage::{IoRegion, sealing::{PlaintextReleaseCheck, PlaintextView}};
use rtdb_types::{Anomaly, AnomalyCategory, OwnedRelation, Position, Result, RootRowCarrier, TransactionKey};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::keys::KeyService;

use super::{
    ATTEMPTS, AssertionAuthoring, AssertionEnvelope, AssertionFamilies, AssertionKeyService, AssertionStage,
    AssertionVaultRecord, DerivedRow, Family, FamilyOccurrence, MAX_RECORD_BYTES, WorldRowSlot,
    contention, family_vault_address, malformed, position, too_large, world_row_scope,
    world_row_slot_commitment,
};

const MAX_CONTRIBUTORS: usize = 512;
const MAX_TRANSFERS: usize = 64;
const WORLD_CODEC: u32 = u32::from_be_bytes(*b"BJWR");
const WORLD_PAYLOAD: &str = "fact_unit_column_payloads";
const MAX_CARRIER_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldRankTransferSource {
    pub original: RetiredOriginalFamilySource,
    pub custody_incarnation: NonZeroU64,
    pub archive: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldRankTransferDestination {
    pub deployment: Uuid,
    pub installation: Uuid,
    pub custody_incarnation: NonZeroU64,
    pub hold: [u8; 32],
    pub preparation: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldRankTransferContributor {
    pub family: Uuid,
    pub occurrence: FamilyOccurrence,
    pub transaction: TransactionKey,
    #[serde(with = "position")]
    pub rank_position: Position,
    pub rank: DerivedRow,
    pub disclosure: [u8; 32],
    pub founder_carrier_sha256: [u8; 32],
    pub rank_carrier_sha256: [u8; 32],
    /// The authenticated source binds the files, physical event ordinals,
    /// assertion positions, journal, clock and both full identities.
    pub frozen_effects_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldRankTransfer {
    pub source: WorldRankTransferSource,
    pub destination: WorldRankTransferDestination,
    pub contributors: Vec<WorldRankTransferContributor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldRankTransferBinding {
    pub address: [u8; 32],
    pub agreement: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedWorldRankReplay {
    pub transaction: TransactionKey,
    pub receipt: [u8; 32],
    pub backing: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Phase {
    Preparing,
    Bound,
    Drained { destination: WorldRankTransferState, source_drain: [u8; 32] },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferRecord {
    agreement: WorldRankTransfer,
    phase: Phase,
}

impl WorldRankTransfer {
    pub fn binding(&self) -> Result<WorldRankTransferBinding> {
        self.validate()?;
        let source = self.source.original;
        let mut address = Context::new(&SHA256);
        address.update(b"rtdb.multi/world-rank-transfer-archive/v1\0");
        address.update(&source.history);
        address.update(&self.source.archive);
        Ok(WorldRankTransferBinding {
            address: finish(address),
            agreement: commitment(b"rtdb.multi/world-rank-transfer-agreement/v1\0", self)?,
        })
    }

    pub fn inventory(&self) -> Result<[u8; 32]> {
        self.validate()?;
        commitment(b"rtdb.multi/world-rank-transfer-inventory/v1\0", &self.contributors)
    }

    fn validate(&self) -> Result<()> {
        let source = self.source.original;
        let target = self.destination;
        if source.deployment.is_nil() || source.history == [0; 32] || source.installation.is_nil()
            || source.handoff == [0; 32] || source.audience_tablet == [0; 16]
            || source.row_custody == [0; 32] || self.source.archive == [0; 32]
            || ![source.carrier, source.fence, source.through, source.captured].into_iter().all(valid_key)
            || source.carrier == source.fence || !source.carrier.at_or_before(source.fence)
            || !source.fence.at_or_before(source.through) || !source.through.at_or_before(source.captured)
            || target.deployment.is_nil() || target.deployment == source.deployment
            || target.installation != source.target_installation || target.installation == source.installation
            || target.installation.is_nil() || target.hold == [0; 32] || target.preparation == [0; 32]
            || !(1..=MAX_CONTRIBUTORS).contains(&self.contributors.len())
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
                || rank.key.len() > 256 || RootRowCarrier::parse(&rank.key).is_none()
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
        verify_projection(self, rank, keys, region).await
    }
}

impl WorldRankTransferState {
    fn terminal(self) -> bool {
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
        let binding = agreement.binding()?;
        let cancellation = self.transfer_cancellation(agreement.source)?;
        let original = self.retired_transfer_source(agreement.source).await?;
        if let Some((prior, _)) = self.read_rank_transfer(binding.address).await? {
            require_agreement(&prior, agreement)?;
            match prior.phase {
                Phase::Bound | Phase::Drained { destination: WorldRankTransferState::Installed { .. }, .. } =>
                    return Ok(binding),
                Phase::Preparing => (),
                Phase::Drained { .. } => return Err(conflict()),
            }
        }
        confirm_prepared(agreement, binding, authority, cancellation).await?;
        let mut publications = agreement.contributors.iter().map(|member| member.transaction).collect::<Vec<_>>();
        publications.sort_unstable_by_key(|key| key.tx_id);
        publications.dedup();
        original.require_pre_fence_publications(&publications).await?;
        self.verify_rank_members(agreement, &original, None).await?;
        let record = TransferRecord { agreement: agreement.clone(), phase: Phase::Preparing };
        self.write_rank_transfer(binding, None, &record).await?;
        for _ in 0..ATTEMPTS {
            let (mut record, etag) = self.exact_rank_transfer(binding, agreement).await?;
            match record.phase {
                Phase::Bound => return Ok(binding),
                Phase::Preparing => (),
                Phase::Drained { .. } => return Err(conflict()),
            }
            for member in &agreement.contributors {
                let id = family_vault_address(agreement.source.original.deployment, member.family);
                self.update(id, |family| {
                    check_member(family, member)?;
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
            let original = self.retired_transfer_source(agreement.source).await?;
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
        let binding = agreement.binding()?;
        let cancellation = self.transfer_cancellation(agreement.source)?;
        self.retired_transfer_source(agreement.source).await?;
        for _ in 0..ATTEMPTS {
            let (mut record, etag) = self.exact_rank_transfer(binding, agreement).await?;
            if matches!(record.phase, Phase::Drained { .. }) {
                return Ok(());
            }
            let destination = observe_destination(agreement.destination, binding, authority, cancellation).await?.state;
            if !destination.terminal()
                || (matches!(destination, WorldRankTransferState::Installed { .. }) && record.phase != Phase::Bound)
            {
                return Err(conflict());
            }
            let drain = cancellation.run_until_cancelled(authority.source_copy_drain(agreement.source, cancellation))
                .await.ok_or_else(unavailable)??;
            if drain.source != agreement.source || drain.binding != binding
                || drain.destination != destination || drain.copy_drain == [0; 32]
            {
                return Err(conflict());
            }
            record.phase = Phase::Drained { destination, source_drain: drain.copy_drain };
            if self.write_rank_transfer(binding, Some(&etag), &record).await? {
                return Ok(());
            }
        }
        Err(contention())
    }

    /// Register the accepted destination generation and every contributing
    /// Original fence in the existing release scope before opening a rank.
    pub async fn require_world_rank_transfer_read(
        &self, agreement: Arc<WorldRankTransfer>, read: WorldRankTransferRead<'_>,
        keys: Arc<dyn KeyService>, authority: Arc<dyn WorldRankTransferAuthority>,
    ) -> Result<()> {
        if !PlaintextView::has_delivery_scope() || read.generation == [0; 32] {
            return Err(unavailable());
        }
        let binding = agreement.binding()?;
        let cancellation = self.transfer_cancellation(agreement.source)?;
        let (record, _) = self.exact_rank_transfer(binding, &agreement).await?;
        let Phase::Drained { destination: terminal @ WorldRankTransferState::Installed { replay, .. }, .. } = record.phase
        else { return Err(unavailable()); };
        let rank = &read.contributor.rank;
        if replay != read.replay || !agreement.contributors.contains(read.contributor)
            || rank.world != Some(read.slot.world) || rank.relation != read.slot.relation
            || rank.key.as_slice() != read.slot.key
        {
            return Err(conflict());
        }
        let original = self.retired_transfer_source(agreement.source).await?;
        self.verify_rank_members(&agreement, &original, Some(binding)).await?;
        let assertion_keys = keys.assertion_keys().ok_or_else(unavailable)?;
        verify_projection(read.contributor, read.carrier, assertion_keys, &self.region).await?;

        let owner = self.original.as_ref().ok_or_else(unavailable)?;
        let check = Arc::new(TransferredRankDelivery {
            original: owner.reader.clone(), cancellation: cancellation.clone(), authority,
            keys, region: Arc::clone(&self.region), agreement, binding, terminal, generation: read.generation,
        });
        check.require_current().await?;
        let key = commitment(b"rtdb.multi/world-rank-transfer-delivery/v1\0", &(binding, replay, read.generation))?;
        PlaintextView::track_release_check(key, check)
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
        &self, agreement: &WorldRankTransfer, original: &RetiredOriginalFamilyRead,
        binding: Option<WorldRankTransferBinding>,
    ) -> Result<()> {
        for member in &agreement.contributors {
            require_no_fence(original, member.family).await?;
            self.require_position_open(member.occurrence.position).await?;
            self.require_position_open(member.rank_position).await?;
            let id = family_vault_address(agreement.source.original.deployment, member.family);
            let (record, _) = self.read_current(id, true).await?.ok_or_else(conflict)?;
            check_member(&record, member)?;
            if binding.is_some_and(|binding| !record.rank_transfers.contains(&binding)) {
                return Err(conflict());
            }
        }
        Ok(())
    }

    async fn exact_rank_transfer(
        &self, binding: WorldRankTransferBinding, agreement: &WorldRankTransfer,
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
        let record: TransferRecord = serde_json::from_slice(&bytes).map_err(|_| malformed())?;
        if record.agreement.binding()?.address != address
            || matches!(record.phase, Phase::Drained { destination, source_drain }
                if !destination.terminal() || source_drain == [0; 32])
        {
            return Err(malformed());
        }
        Ok(Some((record, etag)))
    }

    async fn write_rank_transfer(
        &self, binding: WorldRankTransferBinding, previous: Option<&str>, record: &TransferRecord,
    ) -> Result<bool> {
        self.journal.write_record(
            AssertionVaultRecord::WorldRankTransfer(binding.address), &self.region, previous, encode(record)?.into(),
        ).await
    }
}

struct TransferredRankDelivery {
    original: OriginalAssertionFamilyReader,
    cancellation: CancellationToken,
    authority: Arc<dyn WorldRankTransferAuthority>,
    keys: Arc<dyn KeyService>,
    region: Arc<IoRegion>,
    agreement: Arc<WorldRankTransfer>,
    binding: WorldRankTransferBinding,
    terminal: WorldRankTransferState,
    generation: [u8; 32],
}

#[async_trait::async_trait]
impl PlaintextReleaseCheck for TransferredRankDelivery {
    async fn require_current(&self) -> Result<()> {
        let agreement = &self.agreement;
        let observed = observe_destination(agreement.destination, self.binding, self.authority.as_ref(), &self.cancellation).await?;
        require_current_destination(observed, self.terminal, self.generation)?;
        let families = self.keys.assertion_families(Arc::clone(&self.region)).ok_or_else(unavailable)?
            .with_original_reader(&self.original, agreement.source.original.deployment, &self.cancellation);
        let (record, _) = families.exact_rank_transfer(self.binding, agreement).await?;
        if !matches!(record.phase, Phase::Drained { destination, .. } if destination == self.terminal) {
            return Err(unavailable());
        }
        let original = families.retired_transfer_source(agreement.source).await?;
        families.verify_rank_members(agreement, &original, Some(self.binding)).await
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
    agreement: &WorldRankTransfer, binding: WorldRankTransferBinding,
    authority: &dyn WorldRankTransferAuthority, cancellation: &CancellationToken,
) -> Result<()> {
    let source = cancellation.run_until_cancelled(authority.source_census(agreement.source, cancellation))
        .await.ok_or_else(unavailable)??;
    if source != (WorldRankSourceCensus { source: agreement.source, binding, inventory: agreement.inventory()? })
        || observe_destination(agreement.destination, binding, authority, cancellation).await?.state != WorldRankTransferState::Prepared
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

async fn verify_projection(
    member: &WorldRankTransferContributor, carrier: &[u8], keys: &dyn AssertionKeyService,
    region: &Arc<IoRegion>,
) -> Result<()> {
    let stage = member.rank.capability.ok_or_else(conflict)?;
    let body = carrier_body(member, carrier, b"BJWP1", stage, member.rank_carrier_sha256)?;
    let (disclosure, envelope) = body.split_first_chunk::<32>().ok_or_else(conflict)?;
    if *disclosure != member.disclosure { return Err(conflict()); }
    let slot = WorldRowSlot {
        world: member.rank.world.ok_or_else(conflict)?, relation: &member.rank.relation, key: &member.rank.key,
    };
    verify_envelope(member, stage, slot, envelope, keys, region).await
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

fn require_agreement(record: &TransferRecord, agreement: &WorldRankTransfer) -> Result<()> {
    if record.agreement != *agreement { return Err(conflict()); }
    Ok(())
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| malformed())?;
    if bytes.len() > MAX_RECORD_BYTES { return Err(too_large()); }
    Ok(bytes)
}

fn commitment(domain: &[u8], value: &impl Serialize) -> Result<[u8; 32]> {
    let mut hash = Context::new(&SHA256);
    hash.update(domain);
    hash.update(&encode(value)?);
    Ok(finish(hash))
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
    use crate::keys::{EnrolledWorldRow, LocalKeyService};
    use rtdb_types::{AssertionId, Timestamp, TxId};

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
        let agreement = agreement()?;
        let binding = agreement.binding()?;
        let cancellation = CancellationToken::new();
        let mut evidence = CensusFixture {
            source: WorldRankSourceCensus { source: agreement.source, binding, inventory: agreement.inventory()? },
            destination: WorldRankDestinationCustody {
                destination: agreement.destination, binding, state: WorldRankTransferState::Prepared, generation: [31; 32],
            },
        };
        confirm_prepared(&agreement, binding, &evidence, &cancellation).await?;
        let exact = evidence.source.inventory;
        evidence.source.inventory = [0; 32];
        assert!(confirm_prepared(&agreement, binding, &evidence, &cancellation).await.is_err());
        evidence.source.inventory = exact;
        evidence.destination.destination.hold = [0; 32];
        assert!(confirm_prepared(&agreement, binding, &evidence, &cancellation).await.is_err());
        evidence.destination.destination = agreement.destination;
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
        let preparing = TransferRecord { agreement: agreement.clone(), phase: Phase::Preparing };
        assert!(families.write_rank_transfer(binding, None, &preparing).await?);
        families.update(id, |family| Ok(((), family.rank_transfers.insert(binding)))).await?;
        let order = Uuid::new_v4();
        let fence = families.fence(id, agreement.contributors[0].occurrence.position, order).await?;
        assert_eq!(fence.rank_transfers, BTreeSet::from([binding]));
        assert_ne!(accepted.fence_inventory(order).vault_inventory(), fence.vault_inventory());
        let (fenced, _) = families.read(id).await?.unwrap();
        assert!(!families.world_rank_transfers_drained(&fenced).await?);
        assert!(check_member(&fenced, &agreement.contributors[0]).is_err());
        let (_, etag) = families.exact_rank_transfer(binding, &agreement).await?;
        let discarded = TransferRecord {
            agreement: agreement.clone(),
            phase: Phase::Drained { destination: WorldRankTransferState::Discarded { copy_drain: [29; 32] }, source_drain: [30; 32] },
        };
        assert!(families.write_rank_transfer(binding, Some(&etag), &discarded).await?);
        assert!(families.world_rank_transfers_drained(&fenced).await?);
        vault.retire_assertion_stage(rank_stage, &region).await?;
        assert!(agreement.contributors[0].authenticate_carriers(&founder, &rank, &vault, &region).await.is_err());
        Ok(())
    }
}
