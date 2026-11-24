use std::{collections::BTreeMap, num::NonZeroU64};

use rtdb_columnar::ArrowCell;
use rtdb_db::PinnedExport;
use rtdb_index::branches::BranchIndex;
use rtdb_node::{
    ManagedCheckpointProvider, ManagedCheckpointRead, MultiServing, serving::ServingDriver,
};
use rtdb_storage::sealing::{PlaintextReleaseCheck, PlaintextView};
use rtdb_types::{
    TableRef, Value,
    rtdb_v1::{Basis, MAIN_BRANCH, ManagedCheckpointReservation},
};

use crate::keys::KeyService;

use super::{transfer::MAX_CONTRIBUTORS, *};

const ORIGINAL_CARRIER: &[u8] = b"BJWC1";
const DERIVED_CARRIER: &[u8] = b"BJWP1";
const WORLD_CODEC: u32 = u32::from_be_bytes(*b"BJWR");

/// The installed host supplies a private managed catalog reservation and the original reader.
pub struct ManagedFamilyTransfer<'a> {
    families: AssertionFamilies<'a>,
    keys: &'a dyn AssertionKeyService,
    original: &'a OriginalAssertionFamilyReader,
    custody_incarnation: NonZeroU64,
    reservation: ManagedCheckpointReservation,
    cancellation: &'a CancellationToken,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Census {
    agreement: FamilyTransfer,
    retained_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Installation {
    basis: Basis,
    backing_sha256: [u8; 32],
    source_drain_sha256: [u8; 32],
    destination_drain_sha256: [u8; 32],
}

impl Installation {
    fn terminal(self) -> FamilyTransferDestinationState {
        FamilyTransferDestinationState::Installed {
            backing_sha256: self.backing_sha256,
            drain_sha256: self.destination_drain_sha256,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reverse {
    reservation: ManagedCheckpointReservation,
    phase: Phase,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Phase {
    Capturing,
    Prepared(Census),
    Installed {
        census: Census,
        installation: Installation,
    },
}

impl Phase {
    fn proof(&self) -> Result<(&Census, Option<Installation>)> {
        match self {
            Self::Capturing => Err(unavailable()),
            Self::Prepared(census) => Ok((census, None)),
            Self::Installed {
                census,
                installation,
            } => Ok((census, Some(*installation))),
        }
    }
}

#[async_trait::async_trait]
impl ManagedCheckpointProvider for ManagedFamilyTransfer<'_> {
    async fn install(&self, export: &PinnedExport, destination: &MultiServing) -> Result<Basis> {
        self.install_exact(export, destination).await
    }

    async fn recover_installation(&self, destination: &MultiServing) -> Result<Basis> {
        let (reverse, etag) = self.read_reverse().await?.ok_or_else(unavailable)?;
        self.finish(destination, reverse, etag).await
    }

    async fn require_read(
        &self,
        destination: &Arc<MultiServing>,
        read: ManagedCheckpointRead<'_>,
    ) -> Result<()> {
        self.check_read(destination, read).await
    }

    async fn release_source(&self, source: &MultiServing) -> Result<()> {
        let (reverse, _) = self.read_reverse().await?.ok_or_else(unavailable)?;
        let (census, installed) = reverse.phase.proof()?;
        let installation = installed.ok_or_else(unavailable)?;
        if installation != self.installation(census, installation.basis)? {
            return Err(transfer_conflict());
        }
        self.families
            .require_checkpoint_transfer_terminal(
                &census.agreement,
                installation.terminal(),
                installation.source_drain_sha256,
            )
            .await?;
        source.release_managed_checkpoint(self.reservation).await
    }
}

impl<'a> ManagedFamilyTransfer<'a> {
    pub fn new(
        keys: &'a dyn KeyService,
        region: Arc<IoRegion>,
        original: &'a OriginalAssertionFamilyReader,
        custody_incarnation: NonZeroU64,
        reservation: ManagedCheckpointReservation,
        cancellation: &'a CancellationToken,
    ) -> Result<Self> {
        let deployment = Uuid::from_bytes(reservation.source.basis.incarnation());
        if deployment.is_nil()
            || reservation.source.until.is_some()
            || reservation.source.pin == [0; 16]
            || reservation.source.manifest_sha256 == [0; 32]
            || reservation.target.incarnation == [0; 16]
            || reservation.target.incarnation == *deployment.as_bytes()
            || reservation.target.request == [0; 32]
            || reservation.reserved_at.incarnation() == [0; 16]
            || reservation.reserved_at.position() == 0
        {
            return Err(transfer_conflict());
        }
        let families = keys
            .assertion_families(region)
            .ok_or_else(unavailable)?
            .with_original_reader(original, deployment, cancellation);
        let keys = keys.assertion_keys().ok_or_else(unavailable)?;
        Ok(Self {
            families,
            keys,
            original,
            custody_incarnation,
            reservation,
            cancellation,
        })
    }

    /// The private destination stays reserved while the host calls this method.
    async fn install_exact(
        &self,
        export: &PinnedExport,
        destination: &MultiServing,
    ) -> Result<Basis> {
        export.require_managed_reservation(self.reservation)?;
        if let Some((reverse, etag)) = self.read_reverse().await? {
            return self.finish(destination, reverse, etag).await;
        }
        destination
            .prepare_managed_checkpoint(self.reservation)
            .await?;
        let capturing = Reverse {
            reservation: self.reservation,
            phase: Phase::Capturing,
        };
        if !self.write_reverse(None, &capturing).await? {
            return self.recover_installation(destination).await;
        }
        let (_, etag) = self.read_reverse().await?.ok_or_else(unavailable)?;
        let census = self.source_census(export).await?;
        destination
            .prepare_managed_checkpoint(self.reservation)
            .await?;
        let prepared = Reverse {
            reservation: self.reservation,
            phase: Phase::Prepared(census),
        };
        if !self.write_reverse(Some(&etag), &prepared).await? {
            return Err(transfer_conflict());
        }
        let (reverse, etag) = self.read_reverse().await?.ok_or_else(unavailable)?;
        let (census, installed) = reverse.phase.proof()?;
        let observations = Observations {
            provider: self,
            census,
            installed,
            source: Some(export),
        };
        self.families
            .prepare_checkpoint_transfer(&census.agreement, &observations, self.cancellation)
            .await?;
        destination
            .restore_managed_checkpoint(export, self.reservation)
            .await?;
        self.finish(destination, reverse, etag).await
    }

    async fn finish(
        &self,
        destination: &MultiServing,
        reverse: Reverse,
        etag: String,
    ) -> Result<Basis> {
        let (census, prior) = reverse.phase.proof()?;
        let (basis, view, generation) = destination
            .managed_checkpoint_view(self.reservation)
            .await?;
        let installation = self.installation(census, basis)?;
        if let Some(prior) = prior {
            if prior != installation {
                return Err(transfer_conflict());
            }
        } else {
            // The only copy worker reads the source before appending this restore terminal.
            if view.head().basis != basis || retained_digest(&view)? != census.retained_sha256 {
                return Err(transfer_conflict());
            }
            destination
                .check_read_delivery(Some(generation), &BTreeSet::new(), self.cancellation)
                .await?;
            let terminal = Reverse {
                reservation: self.reservation,
                phase: Phase::Installed {
                    census: census.clone(),
                    installation,
                },
            };
            if !self.write_reverse(Some(&etag), &terminal).await? {
                let (prior, _) = self.read_reverse().await?.ok_or_else(unavailable)?;
                if prior.phase.proof()? != (census, Some(installation)) {
                    return Err(transfer_conflict());
                }
            }
        }
        let observations = Observations {
            provider: self,
            census,
            installed: Some(installation),
            source: None,
        };
        self.families
            .record_checkpoint_transfer_drain(
                &census.agreement,
                installation.terminal(),
                &observations,
                self.cancellation,
            )
            .await?;
        Ok(basis)
    }

    /// Register all original checks inside the caller's protected plaintext delivery scope.
    async fn check_read(
        &self,
        destination: &Arc<MultiServing>,
        read: ManagedCheckpointRead<'_>,
    ) -> Result<()> {
        if !PlaintextView::has_delivery_scope() {
            return Err(unavailable());
        }
        let (reverse, _) = self.read_reverse().await?.ok_or_else(unavailable)?;
        let (census, installed) = reverse.phase.proof()?;
        let (basis, _, current_generation) = destination
            .managed_checkpoint_view(self.reservation)
            .await?;
        let installation = self.installation(census, basis)?;
        if read.generation != current_generation || installed != Some(installation) {
            return Err(unavailable());
        }
        let agreement = &census.agreement;
        self.families
            .require_checkpoint_transfer_terminal(
                agreement,
                installation.terminal(),
                installation.source_drain_sha256,
            )
            .await?;
        if self.observe_original().await? != agreement.source {
            return Err(unavailable());
        }
        let (world, relation, key) = self.carrier_slot(read.branch, read.table, read.id)?;
        let slot = WorldRowSlot {
            world,
            relation: relation.name(),
            key,
        };
        let carrier = Carrier::decode(CarrierKind::for_relation(slot.relation)?, read.carrier)?;
        let (occurrence, derived) = self.authenticate(agreement.source, slot, &carrier).await?;
        let contributor = agreement
            .contributors
            .iter()
            .find(|member| member.family == carrier.family && member.occurrence == occurrence)
            .ok_or_else(unavailable)?;
        let observations = Observations {
            provider: self,
            census,
            installed,
            source: None,
        };
        self.families
            .require_checkpoint_transfer_read(
                agreement,
                FamilyTransferRead {
                    contributor,
                    derived: derived.as_ref(),
                    envelope: read.carrier,
                },
                &observations,
                self.cancellation,
            )
            .await?;
        let check = Arc::new(DestinationRead {
            driver: Arc::clone(destination),
            reservation: self.reservation,
            basis,
            generation: read.generation,
        });
        check.require_current().await?;
        let key = commitment(
            b"rtdb.multi/managed-family-destination-delivery/v1\0",
            &(self.reservation, basis, read.generation),
        )?;
        PlaintextView::track_release_check(key, check)
    }

    async fn source_census(&self, export: &PinnedExport) -> Result<Census> {
        if !export.manifest().files.is_empty() || !export.manifest().pins.is_empty() {
            return Err(unavailable());
        }
        let source = self.observe_original().await?;
        let (_, checkpoint) = export.exact_checkpoint().await?;
        let index = checkpoint.index();
        if index.has_custody_groups() {
            return Err(unavailable());
        }
        let retained_sha256 = retained_digest(index)?;
        let contributors = self.census_carriers(index, source).await?;
        let destination = self.destination(source, retained_sha256)?;
        let agreement = FamilyTransfer {
            source,
            destination,
            contributors,
        };
        agreement.fanout()?;
        export.check_readable().await?;
        Ok(Census {
            agreement,
            retained_sha256,
        })
    }

    async fn observe_original(&self) -> Result<FamilyTransferSource> {
        let checkpoint = self.reservation.source;
        let deployment = Uuid::from_bytes(checkpoint.basis.incarnation());
        let current = self.original.current(deployment, self.cancellation).await?;
        let point = current.carrier_journal_installation().await?;
        let (row, _) = point.row.ok_or_else(unavailable)?;
        let cell = |name| ArrowCell::named(row.batch(), name, 0);
        if cell("kind").and_then(ArrowCell::utf8) != Some("hosted") {
            return Err(unavailable());
        }
        let installation: [u8; 16] = cell("incarnation")
            .and_then(ArrowCell::binary)
            .and_then(|bytes| bytes.try_into().ok())
            .filter(|bytes| *bytes != [0; 16])
            .ok_or_else(unavailable)?;
        let writer = cell("writer")
            .and_then(ArrowCell::utf8)
            .filter(|writer| !writer.is_empty() && writer.len() <= 128)
            .ok_or_else(unavailable)?;
        let mut hash = Context::new(&SHA256);
        hash.update(b"rtdb.multi/carrier-writer/v1\0");
        hash.update(&installation);
        hash.update(writer.as_bytes());
        Ok(FamilyTransferSource {
            deployment,
            custody_incarnation: self.custody_incarnation,
            history: self.original.history_fingerprint(),
            installation,
            original_writer: finish_hash(hash),
            checkpoint,
        })
    }

    async fn census_carriers(
        &self,
        index: &BranchIndex,
        source: FamilyTransferSource,
    ) -> Result<Vec<FamilyTransferContributor>> {
        let mut inventory = BTreeMap::new();
        let mut derived_count = 0;
        for (branch, table, event, image) in index.retained_images() {
            if self.cancellation.is_cancelled() {
                return Err(unavailable());
            }
            if image.custody_group.is_some() || image.custody_payload.is_some() {
                return Err(unavailable());
            }
            if !TableRef::owned_schema(&table.schema) {
                if image
                    .cells
                    .iter()
                    .any(|(name, value)| unsupported_cell(name, value))
                {
                    return Err(unavailable());
                }
                continue;
            }
            let (world, relation, key) = self.carrier_slot(branch, table, &event.id)?;
            if image.cells.len() != 1 {
                return Err(unavailable());
            }
            let kind = CarrierKind::for_relation(relation.name())?;
            let Some(Value::Binary(bytes)) = image.cells.get(kind.column()) else {
                return Err(unavailable());
            };
            let slot = WorldRowSlot {
                world,
                relation: relation.name(),
                key,
            };
            let carrier = Carrier::decode(kind, bytes)?;
            let (occurrence, derived) = self.authenticate(source, slot, &carrier).await?;
            let draft = inventory
                .entry((carrier.family, occurrence.position, occurrence.assertion))
                .or_insert_with(|| Draft {
                    occurrence,
                    original: None,
                    derived: BTreeMap::new(),
                });
            if draft.occurrence != occurrence {
                return Err(transfer_conflict());
            }
            let sha256 = digest(&SHA256, bytes)
                .as_ref()
                .try_into()
                .expect("SHA-256 commitment");
            match derived {
                Some(derived) => {
                    if insert_exact(draft.derived.entry(derived), sha256)? {
                        derived_count += 1;
                    }
                }
                None if draft.original.is_none() => draft.original = Some(sha256),
                None if draft.original == Some(sha256) => (),
                None => return Err(transfer_conflict()),
            }
            if inventory.len() > MAX_CONTRIBUTORS || derived_count > MAX_DERIVED_ROWS {
                return Err(too_large());
            }
        }
        inventory
            .into_iter()
            .map(|((family, _, _), draft)| {
                Ok(FamilyTransferContributor {
                    family,
                    occurrence: draft.occurrence,
                    envelope_sha256: draft.original.ok_or_else(unavailable)?,
                    derived: draft
                        .derived
                        .into_iter()
                        .map(|(row, envelope_sha256)| FamilyTransferArtifact {
                            row,
                            envelope_sha256,
                        })
                        .collect(),
                })
            })
            .collect()
    }

    fn carrier_slot<'row>(
        &self,
        branch: &str,
        table: &TableRef,
        id: &'row EntityId,
    ) -> Result<(Uuid, OwnedRelation, &'row [u8])> {
        let physical = table.owned_storage_name().ok_or_else(unavailable)?;
        let relation = OwnedRelation::from_source(physical.to_owned())?;
        let EntityId::String(key) = id else {
            return Err(unavailable());
        };
        if branch != MAIN_BRANCH
            || relation.table(self.original.owner()) != *table
            || key.is_empty()
            || key.len() > 512
        {
            return Err(unavailable());
        }
        let world = Uuid::from_bytes(relation.world().ok_or_else(unavailable)?.0);
        Ok((
            world,
            relation.root_relation().ok_or_else(unavailable)?,
            key.as_bytes(),
        ))
    }

    async fn authenticate(
        &self,
        source: FamilyTransferSource,
        slot: WorldRowSlot<'_>,
        carrier: &Carrier<'_>,
    ) -> Result<(FamilyOccurrence, Option<DerivedRow>)> {
        if carrier.stage.deployment() != source.deployment
            || carrier.stage.incarnation() != source.custody_incarnation
        {
            return Err(transfer_conflict());
        }
        let address = family_vault_address(source.deployment, carrier.family);
        let (family, _) = self
            .families
            .read_current(address, true)
            .await?
            .ok_or_else(unavailable)?;
        family.require_live()?;
        family.require_unclaimed()?;
        if family.original.assertion != carrier.assertion {
            return Err(unavailable());
        }
        let occurrence = family
            .admitted_member(carrier.assertion)
            .ok_or_else(unavailable)?;
        self.families
            .require_position_open(occurrence.position)
            .await?;
        let (scope_commitment, derived) = match carrier.disclosure {
            Some(disclosure) => {
                let row = DerivedRow {
                    member: carrier.assertion,
                    world: Some(slot.world),
                    relation: slot.relation.to_owned(),
                    key: slot.key.to_vec(),
                    capability: Some(carrier.stage),
                };
                if !family.derived.contains(&row) {
                    return Err(transfer_conflict());
                }
                (world_row_scope(slot, &disclosure), Some(row))
            }
            None => {
                let original = &family.original;
                if original.world != slot.world
                    || original.capability != carrier.stage
                    || original.slot_commitment != world_row_slot_commitment(slot)
                    || original.codec != WORLD_CODEC
                {
                    return Err(transfer_conflict());
                }
                (original.scope_commitment, None)
            }
        };
        self.keys
            .verify_assertion_envelope(
                carrier.stage,
                AssertionAuthoring {
                    assertion_id: carrier.assertion,
                    scope_commitment,
                    codec: WORLD_CODEC,
                },
                &self.families.region,
                Bytes::copy_from_slice(carrier.envelope).try_into()?,
            )
            .await?;
        Ok((occurrence, derived))
    }

    fn installation(&self, census: &Census, basis: Basis) -> Result<Installation> {
        if basis.incarnation() != self.reservation.target.incarnation
            || basis.position() != self.reservation.source.basis.position()
        {
            return Err(transfer_conflict());
        }
        let fanout = census.agreement.fanout()?;
        let backing_sha256 = commitment(
            b"rtdb.multi/managed-family-destination-backing/v1\0",
            &(
                self.reservation,
                fanout.agreement_sha256,
                census.retained_sha256,
                basis,
            ),
        )?;
        let terminal = (fanout.agreement_sha256, backing_sha256);
        Ok(Installation {
            basis,
            backing_sha256,
            source_drain_sha256: commitment(
                b"rtdb.multi/managed-family-source-copy-drain/v1\0",
                &terminal,
            )?,
            destination_drain_sha256: commitment(
                b"rtdb.multi/managed-family-destination-copy-drain/v1\0",
                &terminal,
            )?,
        })
    }

    fn destination(
        &self,
        source: FamilyTransferSource,
        retained_sha256: [u8; 32],
    ) -> Result<FamilyTransferDestination> {
        let target = self.reservation.target;
        Ok(FamilyTransferDestination {
            deployment: Uuid::from_bytes(target.incarnation),
            incarnation: target.incarnation,
            preparation_sha256: commitment(
                b"rtdb.multi/managed-family-destination-preparation/v1\0",
                &(self.reservation, source, retained_sha256),
            )?,
        })
    }

    async fn read_reverse(&self) -> Result<Option<(Reverse, String)>> {
        let address =
            AssertionVaultRecord::ManagedCheckpointTransfer(self.reservation.target.incarnation);
        let Some((bytes, etag)) = self
            .families
            .journal
            .read_record(address, &self.families.region)
            .await?
        else {
            return Ok(None);
        };
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(too_large());
        }
        let reverse: Reverse = serde_json::from_slice(&bytes).map_err(|_| malformed())?;
        if reverse.reservation != self.reservation {
            return Err(transfer_conflict());
        }
        if let Ok((census, _)) = reverse.phase.proof() {
            let source = census.agreement.source;
            if source.checkpoint != self.reservation.source
                || *source.deployment.as_bytes() != self.reservation.source.basis.incarnation()
                || source.custody_incarnation != self.custody_incarnation
                || source.history != self.original.history_fingerprint()
                || census.retained_sha256 == [0; 32]
                || census.agreement.destination
                    != self.destination(source, census.retained_sha256)?
            {
                return Err(transfer_conflict());
            }
        }
        Ok(Some((reverse, etag)))
    }

    async fn write_reverse(&self, etag: Option<&str>, reverse: &Reverse) -> Result<bool> {
        let address =
            AssertionVaultRecord::ManagedCheckpointTransfer(self.reservation.target.incarnation);
        let bytes = serde_json::to_vec(reverse).map_err(|_| malformed())?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(too_large());
        }
        self.families
            .journal
            .write_record(address, &self.families.region, etag, bytes.into())
            .await
    }
}

struct DestinationRead {
    driver: Arc<MultiServing>,
    reservation: ManagedCheckpointReservation,
    basis: Basis,
    generation: u64,
}

#[async_trait::async_trait]
impl PlaintextReleaseCheck for DestinationRead {
    async fn require_current(&self) -> Result<()> {
        let (basis, _, generation) = self
            .driver
            .managed_checkpoint_view(self.reservation)
            .await?;
        if basis != self.basis || generation != self.generation {
            return Err(unavailable());
        }
        Ok(())
    }
}

struct Observations<'probe, 'host> {
    provider: &'probe ManagedFamilyTransfer<'host>,
    census: &'probe Census,
    installed: Option<Installation>,
    source: Option<&'probe PinnedExport>,
}

#[async_trait::async_trait]
impl FamilyTransferAuthority for Observations<'_, '_> {
    async fn source_export(
        &self,
        source: FamilyTransferSource,
        cancellation: &CancellationToken,
    ) -> Result<FamilyTransferSourceObservation> {
        let export = self.source.ok_or_else(unavailable)?;
        let agreement = &self.census.agreement;
        if cancellation.is_cancelled()
            || source != agreement.source
            || source != self.provider.observe_original().await?
        {
            return Err(unavailable());
        }
        export.require_managed_reservation(self.provider.reservation)?;
        export.check_readable().await?;
        Ok(FamilyTransferSourceObservation {
            source,
            agreement_sha256: agreement.fanout()?.agreement_sha256,
            inventory_sha256: agreement.inventory_sha256()?,
        })
    }

    async fn source_copy_drain(
        &self,
        source: FamilyTransferSource,
        cancellation: &CancellationToken,
    ) -> Result<FamilyTransferSourceDrain> {
        let agreement = &self.census.agreement;
        let installed = self.installed.ok_or_else(unavailable)?;
        if cancellation.is_cancelled() || source != agreement.source {
            return Err(unavailable());
        }
        Ok(FamilyTransferSourceDrain {
            source,
            agreement_sha256: agreement.fanout()?.agreement_sha256,
            destination: installed.terminal(),
            drain_sha256: installed.source_drain_sha256,
        })
    }

    async fn destination_custody(
        &self,
        destination: FamilyTransferDestination,
        cancellation: &CancellationToken,
    ) -> Result<FamilyTransferDestinationObservation> {
        let agreement = &self.census.agreement;
        if cancellation.is_cancelled() || destination != agreement.destination {
            return Err(unavailable());
        }
        Ok(FamilyTransferDestinationObservation {
            destination,
            agreement_sha256: agreement.fanout()?.agreement_sha256,
            state: self.installed.map_or(
                FamilyTransferDestinationState::Prepared,
                Installation::terminal,
            ),
        })
    }
}

struct Draft {
    occurrence: FamilyOccurrence,
    original: Option<[u8; 32]>,
    derived: BTreeMap<DerivedRow, [u8; 32]>,
}

fn insert_exact(
    entry: std::collections::btree_map::Entry<'_, DerivedRow, [u8; 32]>,
    commitment: [u8; 32],
) -> Result<bool> {
    use std::collections::btree_map::Entry;
    match entry {
        Entry::Vacant(entry) => {
            entry.insert(commitment);
            Ok(true)
        }
        Entry::Occupied(entry) if *entry.get() == commitment => Ok(false),
        Entry::Occupied(_) => Err(transfer_conflict()),
    }
}

#[derive(Clone, Copy)]
enum CarrierKind {
    Original,
    Derived,
}

impl CarrierKind {
    fn for_relation(name: &str) -> Result<Self> {
        match name {
            "fact_unit_column_payloads" => Ok(Self::Original),
            name if name.ends_with("_search") => Ok(Self::Derived),
            _ => Err(unavailable()),
        }
    }

    fn column(self) -> &'static str {
        match self {
            Self::Original => "sealed_payload",
            Self::Derived => "sealed_world_projection",
        }
    }
}

struct Carrier<'a> {
    family: Uuid,
    stage: AssertionStage,
    assertion: AssertionId,
    disclosure: Option<[u8; 32]>,
    envelope: &'a [u8],
}

impl<'a> Carrier<'a> {
    fn decode(kind: CarrierKind, bytes: &'a [u8]) -> Result<Self> {
        let prefix = match kind {
            CarrierKind::Original => ORIGINAL_CARRIER,
            CarrierKind::Derived => DERIVED_CARRIER,
        };
        let bytes = bytes.strip_prefix(prefix).ok_or_else(unavailable)?;
        let (family, bytes) = bytes.split_first_chunk::<16>().ok_or_else(unavailable)?;
        let (deployment, bytes) = bytes.split_first_chunk::<16>().ok_or_else(unavailable)?;
        let (incarnation, bytes) = bytes.split_first_chunk::<8>().ok_or_else(unavailable)?;
        let (stage, bytes) = bytes.split_first_chunk::<16>().ok_or_else(unavailable)?;
        let (assertion, bytes) = bytes.split_first_chunk::<32>().ok_or_else(unavailable)?;
        let (member, bytes) = bytes.split_first_chunk::<32>().ok_or_else(unavailable)?;
        let family = Uuid::from_bytes(*family);
        if family.is_nil() || assertion != member {
            return Err(unavailable());
        }
        let incarnation =
            NonZeroU64::new(u64::from_be_bytes(*incarnation)).ok_or_else(unavailable)?;
        let stage = AssertionStage::new(
            Uuid::from_bytes(*deployment),
            incarnation,
            Uuid::from_bytes(*stage),
        )?;
        let (disclosure, envelope) = match kind {
            CarrierKind::Original => (None, bytes),
            CarrierKind::Derived => {
                let (disclosure, envelope) =
                    bytes.split_first_chunk::<32>().ok_or_else(unavailable)?;
                (Some(*disclosure), envelope)
            }
        };
        Ok(Self {
            family,
            stage,
            assertion: AssertionId::from_bytes(*assertion),
            disclosure,
            envelope,
        })
    }
}

fn unsupported_cell(name: &str, value: &Value) -> bool {
    matches!(
        name,
        "sealed_payload" | "sealed_world_projection" | "sealed_root_projection"
    ) || contains_carrier(value)
}

fn contains_carrier(value: &Value) -> bool {
    match value {
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => {
            [ORIGINAL_CARRIER, DERIVED_CARRIER, b"BJRL1", b"RTAS\x02"]
                .into_iter()
                .any(|prefix| bytes.starts_with(prefix))
        }
        Value::Struct(fields) => fields
            .iter()
            .any(|(name, value)| unsupported_cell(name, value)),
        Value::Set(values) => values.iter().any(contains_carrier),
        Value::Map { entries, .. } => entries
            .iter()
            .any(|(key, value)| contains_carrier(key) || contains_carrier(value)),
        Value::List(values) | Value::FixedSizeList(values) => values
            .view()
            .iter()
            .any(|value| contains_carrier(value.as_ref())),
        _ => false,
    }
}

fn retained_digest(index: &BranchIndex) -> Result<[u8; 32]> {
    let mut hash = Context::new(&SHA256);
    hash.update(b"rtdb.multi/managed-family-retained-images/v1\0");
    for (branch, table, event, image) in index.retained_images() {
        let bytes = serde_json::to_vec(&(
            branch,
            table,
            &event.id,
            event.valid,
            event.cut.basis.position(),
            event.cut.system_time,
            event.written,
            image,
        ))
        .map_err(|_| malformed())?;
        hash.update(&(bytes.len() as u64).to_be_bytes());
        hash.update(&bytes);
    }
    Ok(finish_hash(hash))
}

fn commitment(domain: &[u8], value: &impl Serialize) -> Result<[u8; 32]> {
    let mut hash = Context::new(&SHA256);
    hash.update(domain);
    hash.update(&serde_json::to_vec(value).map_err(|_| malformed())?);
    Ok(finish_hash(hash))
}

fn finish_hash(hash: Context) -> [u8; 32] {
    hash.finish()
        .as_ref()
        .try_into()
        .expect("SHA-256 commitment")
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/managed-family-transfer",
        "The managed transfer has no complete original census and reciprocal custody proof",
    )
}

fn transfer_conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.multi/managed-family-transfer-conflict",
        "The managed transfer differs from the exact reserved checkpoint and custody proof",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn census_accepts_only_exact_world_original_and_derived_carrier_forms() -> Result<()> {
        let family = Uuid::from_u128(1);
        let deployment = Uuid::from_u128(2);
        let stage = Uuid::from_u128(3);
        let assertion = [4; 32];
        let mut coordinates = Vec::new();
        coordinates.extend_from_slice(family.as_bytes());
        coordinates.extend_from_slice(deployment.as_bytes());
        coordinates.extend_from_slice(&5u64.to_be_bytes());
        coordinates.extend_from_slice(stage.as_bytes());
        coordinates.extend_from_slice(&assertion);
        coordinates.extend_from_slice(&assertion);
        let original = [ORIGINAL_CARRIER, coordinates.as_slice(), b"body"].concat();
        let derived = [
            DERIVED_CARRIER,
            coordinates.as_slice(),
            &[6; 32],
            b"projection",
        ]
        .concat();

        let carrier = Carrier::decode(CarrierKind::Original, &original)?;
        assert_eq!(carrier.family, family);
        assert_eq!(
            carrier.stage,
            AssertionStage::new(deployment, NonZeroU64::new(5).unwrap(), stage)?
        );
        assert_eq!(carrier.assertion, AssertionId::from_bytes(assertion));
        assert_eq!(carrier.disclosure, None);
        let carrier = Carrier::decode(CarrierKind::Derived, &derived)?;
        assert_eq!(carrier.disclosure, Some([6; 32]));
        assert_eq!(carrier.envelope, b"projection");

        assert!(Carrier::decode(CarrierKind::Original, &derived).is_err());
        coordinates[16 + 40 + 32] ^= 1;
        let rewrite = [ORIGINAL_CARRIER, coordinates.as_slice(), b"body"].concat();
        assert!(Carrier::decode(CarrierKind::Original, &rewrite).is_err());
        assert!(contains_carrier(&Value::List(
            vec![Value::Binary(derived)].into()
        )));
        Ok(())
    }
}
