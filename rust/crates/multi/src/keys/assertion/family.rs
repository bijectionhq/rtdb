//! Private current custody for a Proposal occurrence and its landed copies.

use std::{collections::BTreeSet, fs, io::ErrorKind, sync::Arc};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use ring::digest::{SHA256, digest};
use rtdb_storage::IoRegion;
use rtdb_types::{Anomaly, AnomalyCategory, AssertionId, PlacementBinding, Position, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{AssertionAuthoring, AssertionEnvelope, AssertionKeyService, AssertionStage, LocalKeyService, io};
use crate::keys::malformed;

pub(super) const LOCAL_PREFIX: &str = "assertion-family-";
pub(in crate::keys) const MAX_RECORD_BYTES: usize = 1024 * 1024;
const ATTEMPTS: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyOccurrence {
    #[serde(with = "position")]
    pub position: Position,
    #[serde(with = "assertion_id")]
    pub assertion: AssertionId,
    pub capability: AssertionStage,
}

/// The ordered review owner supplies the admitted slot commitment. A separate
/// capability protects the link control so its ciphertext can outlive the link.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingReservation {
    #[serde(with = "position")]
    pub source: Position,
    #[serde(with = "assertion_id")]
    pub destination: AssertionId,
    pub capability: AssertionStage,
    pub link_capability: AssertionStage,
    pub review_commitment: [u8; 32],
}

/// The private control body for a reviewed source slot and its new placement.
/// The ordered batch publishes only its separately sealed envelope.
pub struct LandingLink {
    pub intent: [u8; 32],
    pub source: Position,
    pub destination: Position,
    pub family: Uuid,
    pub review_slot: u64,
}

impl LandingLink {
    const HEADER: &[u8] = b"RTLL\x01";
    const CODEC: u32 = u32::from_be_bytes(*b"LNK1");

    pub async fn seal(
        &self, reservation: LandingReservation, destination: PlacementBinding,
        destination_scope: [u8; 32], keys: &dyn AssertionKeyService, region: &Arc<IoRegion>,
    ) -> Result<AssertionEnvelope> {
        if self.family.is_nil() || self.intent == [0; 32]
            || self.source != reservation.source || self.destination != destination.position
            || destination.assertion != reservation.destination
            || destination.stage != *reservation.capability.stage.as_bytes()
            || reservation.link_capability == reservation.capability
            || reservation.link_capability.deployment != reservation.capability.deployment
            || reservation.link_capability.incarnation != reservation.capability.incarnation
        {
            return Err(conflict());
        }
        let mut body = Vec::with_capacity(Self::HEADER.len() + 32 + 2 * Position::WIDTH + 16 + 8);
        body.extend_from_slice(Self::HEADER);
        body.extend_from_slice(&self.intent);
        body.extend_from_slice(&self.source.to_bytes());
        body.extend_from_slice(&self.destination.to_bytes());
        body.extend_from_slice(self.family.as_bytes());
        body.extend_from_slice(&self.review_slot.to_be_bytes());
        let authoring = AssertionAuthoring {
            assertion_id: reservation.destination,
            scope_commitment: destination_scope,
            codec: Self::CODEC,
        };
        keys.seal_assertion_stage(reservation.link_capability, authoring, region, body.into()).await
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FamilyFence {
    pub order: Uuid,
    pub positions: BTreeSet<Position>,
    pub assertion_capabilities: BTreeSet<AssertionStage>,
    pub link_capabilities: BTreeSet<AssertionStage>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Landing {
    reservation: LandingReservation,
    accepted: Option<AcceptedLanding>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptedLanding {
    #[serde(with = "position")]
    position: Position,
    placement_digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Family {
    original: FamilyOccurrence,
    landings: Vec<Landing>,
    fenced: Option<Uuid>,
}

impl Family {
    fn contains(&self, position: Position) -> bool {
        self.original.position == position
            || self.landings.iter().any(|landing| landing.accepted.as_ref().is_some_and(|accepted| accepted.position == position))
    }

    fn stage_used(&self, stage: AssertionStage) -> bool {
        self.original.capability == stage || self.landings.iter().any(|landing| {
            landing.reservation.capability == stage || landing.reservation.link_capability == stage
        })
    }

    fn fence_inventory(&self, order: Uuid) -> FamilyFence {
        let mut positions = BTreeSet::from([self.original.position]);
        let mut assertion_capabilities = BTreeSet::from([self.original.capability]);
        let mut link_capabilities = BTreeSet::new();
        for landing in &self.landings {
            positions.extend(landing.accepted.as_ref().map(|accepted| accepted.position));
            assertion_capabilities.insert(landing.reservation.capability);
            link_capabilities.insert(landing.reservation.link_capability);
        }
        FamilyFence { order, positions, assertion_capabilities, link_capabilities }
    }

    fn require_live(&self) -> Result<()> {
        if self.fenced.is_some() {
            return Err(rtdb_storage::Error::ErasedUnderView.into());
        }
        Ok(())
    }
}

#[async_trait::async_trait]
pub(in crate::keys) trait FamilyJournal: Send + Sync {
    async fn read_family(&self, id: Uuid, region: &Arc<IoRegion>) -> Result<Option<(Bytes, String)>>;
    async fn write_family(
        &self, id: Uuid, region: &Arc<IoRegion>, previous: Option<&str>, bytes: Bytes,
    ) -> Result<bool>;
}

/// Original vault operations for the trusted publication and reaper owners.
pub struct AssertionFamilies<'a> {
    journal: &'a dyn FamilyJournal,
    region: Arc<IoRegion>,
}

impl<'a> AssertionFamilies<'a> {
    pub(in crate::keys) fn new(journal: &'a dyn FamilyJournal, region: Arc<IoRegion>) -> Self {
        Self { journal, region }
    }

    pub async fn register(&self, family: Uuid, original: FamilyOccurrence) -> Result<()> {
        for _ in 0..ATTEMPTS {
            if let Some((prior, _)) = self.read(family).await? {
                if prior.original != original {
                    return Err(conflict());
                }
                return prior.require_live();
            }
            let record = Family { original, landings: Vec::new(), fenced: None };
            if self.write(family, None, &record).await? {
                return Ok(());
            }
        }
        Err(contention())
    }

    /// Enroll both destination and link stages before either can be provisioned.
    pub async fn reserve_landing(&self, family: Uuid, reservation: LandingReservation) -> Result<()> {
        self.update(family, |record| {
            record.require_live()?;
            if !record.contains(reservation.source) || reservation.review_commitment == [0; 32]
                || reservation.capability == reservation.link_capability
                || reservation.capability.deployment != reservation.link_capability.deployment
                || reservation.capability.incarnation != reservation.link_capability.incarnation
            {
                return Err(conflict());
            }
            if let Some(prior) = record.landings.iter().find(|entry| {
                entry.reservation.destination == reservation.destination
            }) {
                return if prior.reservation == reservation { Ok(((), false)) } else { Err(conflict()) };
            }
            if record.original.assertion == reservation.destination
                || record.stage_used(reservation.capability)
                || record.stage_used(reservation.link_capability)
            {
                return Err(conflict());
            }
            record.landings.push(Landing { reservation, accepted: None });
            Ok(((), true))
        }).await
    }

    /// Called after the owner has independently authenticated the same ordered
    /// destination binding and private link. This operation serializes against
    /// the current fence and does not verify publication evidence itself.
    pub async fn activate_ordered_landing(
        &self, family: Uuid, placement: PlacementBinding, reservation: LandingReservation,
    ) -> Result<()> {
        self.update(family, |record| {
            record.require_live()?;
            if placement.assertion != reservation.destination
                || placement.stage != *reservation.capability.stage.as_bytes()
            {
                return Err(conflict());
            }
            let index = record.landings.iter().position(|landing| landing.reservation == reservation)
                .ok_or_else(conflict)?;
            let mut placement_digest = [0; 32];
            placement_digest.copy_from_slice(digest(&SHA256, &placement.to_bytes()).as_ref());
            if let Some(prior) = &record.landings[index].accepted {
                return if prior.placement_digest == placement_digest {
                    Ok(((), false))
                } else {
                    Err(conflict())
                };
            }
            if record.contains(placement.position) {
                return Err(conflict());
            }
            record.landings[index].accepted = Some(AcceptedLanding {
                position: placement.position, placement_digest,
            });
            Ok(((), true))
        }).await
    }

    /// Current check for the exact occurrence already admitted for release.
    /// The caller owns the final release fence and draining previously admitted work.
    pub async fn require_live_occurrence(&self, family: Uuid, occurrence: FamilyOccurrence) -> Result<()> {
        let (record, _) = self.read(family).await?.ok_or_else(conflict)?;
        record.require_live()?;
        if record.original == occurrence || record.landings.iter().any(|landing| {
            landing.reservation.destination == occurrence.assertion
                && landing.reservation.capability == occurrence.capability
                && landing.accepted.as_ref().is_some_and(|accepted| accepted.position == occurrence.position)
        }) {
            Ok(())
        } else {
            Err(conflict())
        }
    }

    /// The selected original, or any active landed copy, closes the same
    /// family. The custody roster includes enrolled stages even before order.
    pub async fn fence(&self, family: Uuid, selected: Position, order: Uuid) -> Result<FamilyFence> {
        self.update(family, |record| {
            if order.is_nil() || !record.contains(selected) {
                return Err(conflict());
            }
            let first_order = record.fenced.unwrap_or(order);
            let changed = record.fenced.is_none();
            record.fenced = Some(first_order);
            Ok((record.fence_inventory(first_order), changed))
        }).await
    }

    async fn read(&self, family: Uuid) -> Result<Option<(Family, String)>> {
        if family.is_nil() {
            return Err(conflict());
        }
        let Some((bytes, etag)) = self.journal.read_family(family, &self.region).await? else {
            return Ok(None);
        };
        let state = serde_json::from_slice(&bytes)
            .map_err(|_| Anomaly::fault("rtdb.multi/assertion-family-record", "Invalid family custody record"))?;
        Ok(Some((state, etag)))
    }

    async fn write(&self, family: Uuid, previous: Option<&str>, record: &Family) -> Result<bool> {
        let bytes = serde_json::to_vec(record)
            .map_err(|_| Anomaly::fault("rtdb.multi/assertion-family-record", "Cannot encode family custody record"))?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(Anomaly::incorrect(
                "rtdb.multi/assertion-family-size", "The assertion family exceeds its custody inventory limit",
            ));
        }
        self.journal.write_family(family, &self.region, previous, bytes.into()).await
    }

    async fn update<T>(
        &self, family: Uuid, edit: impl Fn(&mut Family) -> Result<(T, bool)>,
    ) -> Result<T> {
        for _ in 0..ATTEMPTS {
            let (mut record, etag) = self.read(family).await?.ok_or_else(conflict)?;
            let (result, changed) = edit(&mut record)?;
            if !changed || self.write(family, Some(&etag), &record).await? {
                return Ok(result);
            }
        }
        Err(contention())
    }
}

fn conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict, "rtdb.multi/assertion-family-mismatch",
        "The assertion does not match the original family custody",
    )
}

fn contention() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict, "rtdb.multi/assertion-family-contention",
        "The original family custody changed concurrently",
    )
}

fn local_record(vault: &LocalKeyService, path: &std::path::Path) -> Result<Option<(Bytes, String)>> {
    match fs::read(path) {
        Ok(bytes) if bytes.len() <= MAX_RECORD_BYTES => {
            vault.sync_directory()?;
            let etag = URL_SAFE_NO_PAD.encode(digest(&SHA256, &bytes));
            Ok(Some((bytes.into(), etag)))
        }
        Ok(_) => Err(malformed()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io(error)),
    }
}

#[async_trait::async_trait]
impl FamilyJournal for LocalKeyService {
    async fn read_family(&self, id: Uuid, region: &Arc<IoRegion>) -> Result<Option<(Bytes, String)>> {
        self.require_assertion_broker()?;
        let owner = self.clone();
        region.run_blocking(move || Ok((|| {
            let vault = owner.archive_vault()?;
            let path = vault.path(&format!("{LOCAL_PREFIX}{id}"))?;
            let _lock = vault.lock_key_shared(&path)?;
            local_record(&vault, &path)
        })())).await?
    }

    async fn write_family(
        &self, id: Uuid, region: &Arc<IoRegion>, previous: Option<&str>, bytes: Bytes,
    ) -> Result<bool> {
        self.require_assertion_broker()?;
        let owner = self.clone();
        let previous = previous.map(str::to_owned);
        region.run_blocking(move || Ok((|| {
            let vault = owner.archive_vault()?;
            let path = vault.path(&format!("{LOCAL_PREFIX}{id}"))?;
            let _lock = vault.lock_key(&path)?;
            if local_record(&vault, &path)?.as_ref().map(|(_, etag)| etag) != previous.as_ref() {
                return Ok(false);
            }
            vault.publish(&path, &bytes)?;
            Ok(true)
        })())).await?
    }
}

mod position {
    use rtdb_types::Position;
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

    pub fn serialize<S: Serializer>(position: &Position, serializer: S) -> Result<S::Ok, S::Error> {
        position.to_bytes().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Position, D::Error> {
        Position::from_bytes(<[u8; Position::WIDTH]>::deserialize(deserializer)?).map_err(Error::custom)
    }
}

mod assertion_id {
    use rtdb_types::AssertionId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(assertion: &AssertionId, serializer: S) -> Result<S::Ok, S::Error> {
        assertion.to_bytes().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<AssertionId, D::Error> {
        Ok(AssertionId::from_bytes(<[u8; AssertionId::WIDTH]>::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::keys::KeyService;

    fn position(publication: u64) -> Position {
        Position { line_generation: [1; 16], publication, ordinal: NonZeroU64::MIN }
    }

    fn stage(realm: Uuid) -> AssertionStage {
        AssertionStage::new(realm, NonZeroU64::MIN, Uuid::new_v4()).unwrap()
    }

    fn placement(reservation: LandingReservation) -> PlacementBinding {
        PlacementBinding {
            assertion: reservation.destination,
            stage: *reservation.capability.stage.as_bytes(),
            capsule_digest: [3; 32],
            position: position(2),
            clock_commitment: [4; 32],
            address_extent_commitment: [5; 32],
            disclosure_scope: [6; 32],
            membership_commitment: [7; 32],
        }
    }

    #[tokio::test]
    async fn either_active_end_closes_the_family_and_fences_enrolled_stages() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let families = vault.assertion_families(Arc::clone(&region)).unwrap();
        let proposal = Uuid::new_v4();
        let destination = Uuid::new_v4();
        for close_from_copy in [false, true] {
            let family = Uuid::new_v4();
            let original = FamilyOccurrence {
                position: position(1),
                assertion: AssertionId::from_bytes([1; 32]),
                capability: stage(proposal),
            };
            let first = LandingReservation {
                source: original.position,
                destination: AssertionId::from_bytes([2; 32]),
                capability: stage(destination),
                link_capability: stage(destination),
                review_commitment: [2; 32],
            };
            let pending = LandingReservation {
                destination: AssertionId::from_bytes([8; 32]),
                capability: stage(destination),
                link_capability: stage(destination),
                ..first
            };
            let binding = placement(first);
            let copy = FamilyOccurrence {
                position: binding.position, assertion: first.destination, capability: first.capability,
            };
            families.register(family, original).await?;
            families.reserve_landing(family, first).await?;
            assert!(families.require_live_occurrence(family, copy).await.is_err());
            families.activate_ordered_landing(family, binding, first).await?;
            families.activate_ordered_landing(family, binding, first).await?;
            let changed = PlacementBinding { address_extent_commitment: [9; 32], ..binding };
            assert!(families.activate_ordered_landing(family, changed, first).await.is_err());
            families.require_live_occurrence(family, copy).await?;
            families.reserve_landing(family, pending).await?;

            let closed = if close_from_copy { copy.position } else { original.position };
            let fence = families.fence(family, closed, Uuid::new_v4()).await?;
            assert_eq!(fence.positions, BTreeSet::from([original.position, copy.position]));
            assert_eq!(fence.assertion_capabilities, BTreeSet::from([
                original.capability, first.capability, pending.capability,
            ]));
            assert_eq!(fence.link_capabilities, BTreeSet::from([
                first.link_capability, pending.link_capability,
            ]));

            let reopened = LocalKeyService::with_archive_broker(directory.path())?;
            let reopened = reopened.assertion_families(Arc::clone(&region)).unwrap();
            assert_eq!(reopened.fence(family, original.position, Uuid::new_v4()).await?, fence);
            assert_eq!(reopened.fence(family, copy.position, Uuid::new_v4()).await?, fence);
            assert!(reopened.register(family, original).await.is_err());
            assert!(reopened.require_live_occurrence(family, original).await.is_err());
            assert!(reopened.require_live_occurrence(family, copy).await.is_err());
            assert!(reopened.activate_ordered_landing(family, placement(pending), pending).await.is_err());
            assert!(reopened.reserve_landing(family, pending).await.is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn the_link_can_be_retired_without_destroying_the_destination() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let realm = Uuid::new_v4();
        let reservation = LandingReservation {
            source: position(1),
            destination: AssertionId::from_bytes([2; 32]),
            capability: stage(realm),
            link_capability: stage(realm),
            review_commitment: [2; 32],
        };
        let scope = [3; 32];
        let binding = placement(reservation);
        let link = LandingLink {
            intent: [4; 32], source: reservation.source, destination: binding.position,
            family: Uuid::new_v4(), review_slot: 0,
        };
        let protected = link.seal(reservation, binding, scope, &vault, &region).await?;
        assert_eq!(link.seal(reservation, binding, scope, &vault, &region).await?, protected);

        let authoring = AssertionAuthoring {
            assertion_id: reservation.destination, scope_commitment: scope, codec: 1,
        };
        let body = Bytes::from_static(b"reviewed assertion");
        let destination = vault.seal_assertion_stage(reservation.capability, authoring, &region, body.clone()).await?;
        vault.retire_assertion_stage(reservation.link_capability, &region).await?;
        assert!(link.seal(reservation, binding, scope, &vault, &region).await.is_err());
        assert_eq!(
            vault.open_assertion_for_resolution(reservation.capability, authoring, &region, destination).await?,
            body,
        );
        Ok(())
    }
}
