use std::sync::Arc;

use rtdb_directory::{CheckpointWorldRetention, DeploymentRecord, Directory};
use rtdb_node::{AppliedReadFloor, OriginalAssertionFamilyReader};
use rtdb_storage::{
    IoRegion, sealing::{DeliveryDeadline, InheritedRootRankCut, InheritedRootRankProof, PlaintextReleaseCheck, PlaintextView},
};
use rtdb_types::{
    EntityId, Interval, OwnedPointVersion, OwnedRelation, OwnedWorld, PlacedAttemptScope, Result,
    ROOT_ROW_CARRIERS, RootFamilyDerivativeId, RootLandingBinding, RootRowCarrier, TransactionKey, WorldClockMode,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{archive::matches_retention, family_custody::framed_digest, keys::{AssertionStage, KeyService}};

use super::RootRankCustody;

struct InheritedRankDelivery {
    directory: Arc<dyn Directory>,
    keys: Arc<dyn KeyService>,
    original: OriginalAssertionFamilyReader,
    cancellation: CancellationToken,
    region: Arc<IoRegion>,
    family: Uuid,
    stage: AssertionStage,
    binding: RootLandingBinding,
    relation: OwnedRelation,
    selector: EntityId,
    carrier: EntityId,
    journal: EntityId,
    proof: InheritedRootRankProof,
}

pub(super) async fn admit(
    authority: &RootRankCustody, family: Uuid, stage: AssertionStage, binding: RootLandingBinding,
    relation: &str, selector: &[u8], proof: InheritedRootRankProof,
) -> Result<()> {
    let unavailable = OwnedWorld::unavailable;
    let selected = RootFamilyDerivativeId::parse(selector).map_err(|_| unavailable())?.ok_or_else(unavailable)?;
    let carrier = RootRowCarrier::parse(selected.address).ok_or_else(unavailable)?;
    if !PlaintextView::has_delivery_scope() || proof.cut.writer != authority.writer
        || stage.deployment() != authority.deployment || selected.family != *family.as_bytes()
        || selected.member != binding.assertion
    {
        return Err(unavailable());
    }
    let cutoff = u64::try_from(proof.cut.until.0).ok()
        .and_then(|micros| micros.checked_mul(1000))
        .and_then(DeliveryDeadline::from_wire_nanos)
        .ok_or_else(unavailable)?;
    let key = framed_digest(b"rtdb.multi/inherited-root-rank-delivery/v1\0", &[
        &proof.cut.world.0, &proof.cut.pin.0, &proof.cut.registration.position.to_bytes(),
        &proof.rank.position.to_bytes(), &proof.valid.0.to_be_bytes(),
        &(relation.len() as u32).to_be_bytes(), relation.as_bytes(), selector,
    ]);
    let check = Arc::new(InheritedRankDelivery {
        directory: Arc::clone(&authority.directory),
        keys: Arc::clone(&authority.keys),
        original: authority.original.clone(),
        cancellation: authority.cancellation.clone(),
        region: Arc::clone(&authority.region),
        family, stage, binding,
        relation: OwnedRelation::new(relation)?,
        selector: EntityId::Binary(selector.to_vec()),
        carrier: EntityId::Binary(selected.address.to_vec()),
        journal: EntityId::Binary(carrier.assertion_key()),
        proof,
    });
    check.require_current().await?;
    if !cutoff.is_live() { return Err(unavailable()); }
    PlaintextView::track_release_check_until(key, check, cutoff)
}

#[async_trait::async_trait]
impl PlaintextReleaseCheck for InheritedRankDelivery {
    async fn require_current(&self) -> Result<()> {
        let unavailable = OwnedWorld::unavailable;
        let cut = self.proof.cut;
        let deployment = self.stage.deployment();
        let bound = self.directory.deployment(&deployment.to_string()).await?.ok_or_else(unavailable)?;
        let retained = current_pin(&bound, deployment, cut)?;
        let transaction = transaction_key(self.proof.journal);
        require_source_occurrence(&self.proof, self.binding)?;
        let floor = AppliedReadFloor::new(deployment, vec![transaction, cut.basis, transaction_key(cut.registration)])?;
        let current = self.original.current_covering(&floor, &self.cancellation).await?;
        current.require_unretired_family_source().await?;
        if current.assertion_family_fence(self.family).await?.is_some() {
            return Err(rtdb_storage::Error::ErasedUnderView.into());
        }
        let world = current.world_registration(cut.world).await?.ok_or_else(unavailable)?;
        if world.dropped || world.pin != cut.pin || !matches_retention(&world, retained) {
            return Err(unavailable());
        }
        require_registration(cut, current.world_registration_version(cut.world).await?)?;
        let valid = Interval::at(self.proof.valid)?;
        let journal = OwnedRelation::new(self.relation.name().strip_suffix("_search").ok_or_else(unavailable)?)?;
        current.require_rank_erasure_projection(&self.relation, &self.carrier, valid, cut.basis).await?;
        current.require_unerased_root_point(&OwnedRelation::new(ROOT_ROW_CARRIERS)?, &self.carrier, valid, self.proof.carrier).await?;
        current.require_unerased_root_point(&journal, &self.journal, valid, self.proof.journal).await?;
        current.require_unerased_root_point(&self.relation, &self.selector, valid, self.proof.rank).await?;
        let EntityId::Binary(selector) = &self.selector else { return Err(unavailable()); };
        let families = self.keys.assertion_families(Arc::clone(&self.region)).ok_or_else(unavailable)?
            .with_original_reader(&self.original, deployment, &self.cancellation);
        families.require_root_rank_landing(
            super::family_vault_address(deployment, self.family), self.stage, self.binding, self.relation.name(), selector,
        ).await?;
        let installed = self.directory.deployment(&deployment.to_string()).await?.ok_or_else(unavailable)?;
        if current_pin(&installed, deployment, cut)? != retained { return Err(unavailable()); }
        Ok(())
    }
}

fn current_pin(
    record: &DeploymentRecord, deployment: Uuid, cut: InheritedRootRankCut,
) -> Result<&CheckpointWorldRetention> {
    let unavailable = OwnedWorld::unavailable;
    if Uuid::parse_str(&record.id).ok() != Some(deployment)
        || record.writer_pool().is_none() || record.holder.is_none()
        || record.incarnation != cut.incarnation || record.log_epoch != cut.log_epoch
        || record.storage_generation != cut.storage_generation
        || PlacedAttemptScope::new(&record.stream("replica"), record.epoch)? != cut.writer
        || cut.until.0 <= rtdb_directory::now_ms().saturating_mul(1000)
    {
        return Err(unavailable());
    }
    record.checkpoint_worlds.get(&Uuid::from_bytes(cut.pin.0)).filter(|pin| {
        pin.world.as_bytes() == &cut.world.0 && pin.basis == cut.basis && pin.until == cut.until
            && pin.mode == WorldClockMode::Retained && pin.ancestors.is_empty() && pin.parent_publication.is_some()
    }).ok_or_else(unavailable)
}

fn transaction_key(point: OwnedPointVersion) -> TransactionKey {
    TransactionKey { tx_id: point.position.transaction, system_time: point.history }
}

fn require_registration(cut: InheritedRootRankCut, observed: Option<OwnedPointVersion>) -> Result<()> {
    if observed != Some(cut.registration) { return Err(OwnedWorld::unavailable()); }
    Ok(())
}

fn require_source_occurrence(proof: &InheritedRootRankProof, binding: RootLandingBinding) -> Result<()> {
    let journal = transaction_key(proof.journal);
    let registration = transaction_key(proof.cut.registration);
    if journal.system_time.0 <= 0 || journal.system_time.0 as u64 != binding.position.publication
        || transaction_key(proof.carrier) != journal || transaction_key(proof.rank) != journal
        || !journal.at_or_before(proof.cut.basis) || !proof.cut.basis.at_or_before(registration)
        || proof.cut.basis == registration || proof.carrier.position == proof.journal.position
        || proof.carrier.position == proof.rank.position || proof.journal.position == proof.rank.position
    {
        return Err(OwnedWorld::unavailable());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rtdb_types::{EventPosition, Iid, Timestamp, TxId};

    use super::*;

    #[test]
    fn inherited_rank_pin_rejects_substitute_destination_generations_and_registration() -> Result<()> {
        let mut record = DeploymentRecord::new("installed-root", "org", Vec::new(), 0);
        record.epoch = 5;
        record.holder = Some(rtdb_directory::MemberId("writer".into()));
        record.storage_generation = Some(Uuid::from_bytes([3; 16]));
        let deployment = Uuid::parse_str(&record.id).unwrap();
        let world = Uuid::from_bytes([1; 16]);
        let pin = Uuid::from_bytes([2; 16]);
        let basis = TransactionKey { tx_id: TxId(20), system_time: Timestamp(100) };
        let until = Timestamp::MAX;
        record.checkpoint_worlds.insert(pin, CheckpointWorldRetention {
            fork_reservation: None,
            world,
            ancestors: Vec::new(),
            owner: "owner".into(),
            capture: Uuid::from_bytes([4; 16]),
            manifest_sha256: "a".repeat(43),
            schema_digest: [5; 32],
            suffix: None,
            basis,
            parent_publication: Some(basis.system_time),
            mode: WorldClockMode::Retained,
            until,
        });
        let registration = OwnedPointVersion {
            position: EventPosition { transaction: TxId(21), ordinal: 1 },
            history: Timestamp(105),
        };
        let cut = InheritedRootRankCut {
            world: Iid(*world.as_bytes()),
            pin: Iid(*pin.as_bytes()),
            basis,
            registration,
            writer: PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?,
            incarnation: record.incarnation,
            log_epoch: record.log_epoch,
            storage_generation: record.storage_generation,
            until,
        };
        assert_eq!(current_pin(&record, deployment, cut)?, &record.checkpoint_worlds[&pin]);
        let substitute = |change: fn(&mut DeploymentRecord)| {
            let mut substituted = record.clone();
            change(&mut substituted);
            substituted
        };
        for substituted in [
            substitute(|record| record.holder = None),
            substitute(|record| record.epoch = 6),
            substitute(|record| record.log_epoch += 1),
            substitute(|record| record.incarnation += 1),
            substitute(|record| record.storage_generation = Some(Uuid::from_bytes([6; 16]))),
        ] {
            assert!(current_pin(&substituted, deployment, cut).is_err());
        }
        assert!(current_pin(&record, deployment, InheritedRootRankCut { pin: cut.world, ..cut }).is_err());
        require_registration(cut, Some(registration))?;
        let revised = OwnedPointVersion {
            position: EventPosition { transaction: TxId(22), ..registration.position },
            ..registration
        };
        assert!(require_registration(cut, Some(revised)).is_err());
        assert!(require_registration(cut, None).is_err());
        Ok(())
    }
}
