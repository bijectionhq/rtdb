use std::num::NonZeroU64;

use rtdb_types::rtdb_v1::PinnedCheckpointIdentity;

use super::*;

const MAX_CONTRIBUTORS: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyTransferSource {
    pub deployment: Uuid,
    pub custody_incarnation: NonZeroU64,
    pub history: [u8; 32],
    pub installation: [u8; 16],
    pub original_writer: [u8; 32],
    pub checkpoint: PinnedCheckpointIdentity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyTransferDestination {
    pub deployment: Uuid,
    pub incarnation: [u8; 16],
    pub preparation_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyTransferContributor {
    pub family: Uuid,
    pub occurrence: FamilyOccurrence,
    pub envelope_sha256: [u8; 32],
    pub derived: Vec<FamilyTransferArtifact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyTransferArtifact {
    pub row: DerivedRow,
    pub envelope_sha256: [u8; 32],
}

pub struct FamilyTransferRead<'a> {
    pub contributor: &'a FamilyTransferContributor,
    pub derived: Option<&'a DerivedRow>,
    pub envelope: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyTransfer {
    pub source: FamilyTransferSource,
    pub destination: FamilyTransferDestination,
    pub contributors: Vec<FamilyTransferContributor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyTransferFanout {
    pub address: [u8; 32],
    pub agreement_sha256: [u8; 32],
    pub destination: FamilyTransferDestination,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FamilyTransferSourceObservation {
    pub source: FamilyTransferSource,
    pub agreement_sha256: [u8; 32],
    pub inventory_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FamilyTransferSourceDrain {
    pub source: FamilyTransferSource,
    pub agreement_sha256: [u8; 32],
    pub destination: FamilyTransferDestinationState,
    pub drain_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FamilyTransferDestinationObservation {
    pub destination: FamilyTransferDestination,
    pub agreement_sha256: [u8; 32],
    pub state: FamilyTransferDestinationState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum FamilyTransferDestinationState {
    Prepared,
    Installed {
        backing_sha256: [u8; 32],
        drain_sha256: [u8; 32],
    },
    Discarded {
        drain_sha256: [u8; 32],
    },
}

/// The host observes the complete pinned inventory at the source and the reciprocal binding at the destination.
#[async_trait::async_trait]
pub trait FamilyTransferAuthority: Send + Sync {
    async fn source_export(
        &self,
        source: FamilyTransferSource,
        cancellation: &CancellationToken,
    ) -> Result<FamilyTransferSourceObservation>;
    async fn source_copy_drain(
        &self,
        source: FamilyTransferSource,
        cancellation: &CancellationToken,
    ) -> Result<FamilyTransferSourceDrain>;
    async fn destination_custody(
        &self,
        destination: FamilyTransferDestination,
        cancellation: &CancellationToken,
    ) -> Result<FamilyTransferDestinationObservation>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum TransferPhase {
    Preparing,
    Bound,
    Drained {
        destination: FamilyTransferDestinationState,
        source_sha256: [u8; 32],
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferRecord {
    agreement: FamilyTransfer,
    phase: TransferPhase,
}

impl FamilyTransfer {
    pub fn inventory_sha256(&self) -> Result<[u8; 32]> {
        self.validate()?;
        commitment(
            b"rtdb.multi/family-transfer-inventory/v1\0",
            &self.contributors,
        )
    }

    pub fn fanout(&self) -> Result<FamilyTransferFanout> {
        self.validate()?;
        let mut address = Context::new(&SHA256);
        address.update(b"rtdb.multi/family-transfer-pin/v1\0");
        address.update(self.source.deployment.as_bytes());
        address.update(&self.source.checkpoint.basis.incarnation());
        address.update(&self.source.checkpoint.pin);
        Ok(FamilyTransferFanout {
            address: address
                .finish()
                .as_ref()
                .try_into()
                .expect("SHA-256 commitment"),
            agreement_sha256: commitment(b"rtdb.multi/family-transfer-agreement/v1\0", self)?,
            destination: self.destination,
        })
    }

    fn validate(&self) -> Result<()> {
        let source = self.source;
        let destination = self.destination;
        if source.deployment.is_nil()
            || source.history == [0; 32]
            || source.installation == [0; 16]
            || source.original_writer == [0; 32]
            || source.checkpoint.basis.incarnation() == [0; 16]
            || source.checkpoint.pin == [0; 16]
            || source.checkpoint.manifest_sha256 == [0; 32]
            || source.checkpoint.until.is_some()
            || destination.deployment.is_nil()
            || destination.deployment == source.deployment
            || destination.incarnation == [0; 16]
            || destination.incarnation == source.checkpoint.basis.incarnation()
            || destination.preparation_sha256 == [0; 32]
            || !(1..=MAX_CONTRIBUTORS).contains(&self.contributors.len())
        {
            return Err(transfer_conflict());
        }
        let member_key = |member: &FamilyTransferContributor| {
            (
                member.family,
                member.occurrence.position,
                member.occurrence.assertion,
            )
        };
        if self
            .contributors
            .windows(2)
            .any(|pair| member_key(&pair[0]) >= member_key(&pair[1]))
        {
            return Err(transfer_conflict());
        }
        let stage_owned = |stage: AssertionStage| {
            stage.deployment() == source.deployment
                && stage.incarnation() == source.custody_incarnation
        };
        let mut derived_count = 0usize;
        for member in &self.contributors {
            derived_count = derived_count.saturating_add(member.derived.len());
            if member.family.is_nil()
                || member.envelope_sha256 == [0; 32]
                || !stage_owned(member.occurrence.capability)
                || derived_count > MAX_DERIVED_ROWS
                || member
                    .derived
                    .windows(2)
                    .any(|pair| pair[0].row >= pair[1].row)
                || member.derived.iter().any(|artifact| {
                    artifact.envelope_sha256 == [0; 32]
                        || !artifact.row.well_formed()
                        || artifact.row.member != member.occurrence.assertion
                        || artifact
                            .row
                            .capability
                            .is_some_and(|stage| !stage_owned(stage))
                })
            {
                return Err(transfer_conflict());
            }
        }
        Ok(())
    }
}

impl FamilyTransferDestinationState {
    fn terminal(self) -> bool {
        match self {
            Self::Prepared => false,
            Self::Installed {
                backing_sha256,
                drain_sha256,
            } => backing_sha256 != [0; 32] && drain_sha256 != [0; 32],
            Self::Discarded { drain_sha256 } => drain_sha256 != [0; 32],
        }
    }
}

impl AssertionFamilies<'_> {
    pub async fn prepare_checkpoint_transfer(
        &self,
        agreement: &FamilyTransfer,
        authority: &dyn FamilyTransferAuthority,
        cancellation: &CancellationToken,
    ) -> Result<FamilyTransferFanout> {
        let fanout = agreement.fanout()?;
        self.require_transfer_original(agreement.source)?;
        if cancellation.is_cancelled() {
            return Err(transfer_unavailable());
        }
        if let Some((record, _)) = self.read_transfer(fanout.address).await? {
            if record.agreement != *agreement {
                return Err(transfer_conflict());
            }
            match record.phase {
                TransferPhase::Bound
                | TransferPhase::Drained {
                    destination: FamilyTransferDestinationState::Installed { .. },
                    ..
                } => {
                    return Ok(fanout);
                }
                TransferPhase::Preparing => (),
                TransferPhase::Drained { .. } => return Err(transfer_conflict()),
            }
        }
        self.confirm_prepared_transfer(agreement, fanout, authority, cancellation)
            .await?;
        self.verify_transfer_members(agreement, None).await?;

        let preparing = TransferRecord {
            agreement: agreement.clone(),
            phase: TransferPhase::Preparing,
        };
        self.write_transfer(fanout.address, None, &preparing)
            .await?;
        let (mut record, etag) = self.exact_transfer(agreement, fanout).await?;
        if record.phase == TransferPhase::Bound {
            return Ok(fanout);
        }
        if record.phase != TransferPhase::Preparing {
            return Err(transfer_conflict());
        }

        for contributor in &agreement.contributors {
            if cancellation.is_cancelled() {
                return Err(transfer_unavailable());
            }
            let family = family_vault_address(agreement.source.deployment, contributor.family);
            self.update(family, |record| {
                check_member(record, contributor)?;
                if let Some(prior) = record
                    .transfers
                    .iter()
                    .find(|prior| prior.address == fanout.address)
                {
                    return if *prior == fanout {
                        Ok(((), false))
                    } else {
                        Err(transfer_conflict())
                    };
                }
                record.transfers.push(fanout);
                Ok(((), true))
            })
            .await?;
        }
        self.verify_transfer_members(agreement, Some(fanout))
            .await?;
        self.confirm_prepared_transfer(agreement, fanout, authority, cancellation)
            .await?;
        record.phase = TransferPhase::Bound;
        if !self
            .write_transfer(fanout.address, Some(&etag), &record)
            .await?
        {
            let (prior, _) = self.exact_transfer(agreement, fanout).await?;
            if prior.phase != TransferPhase::Bound {
                return Err(transfer_conflict());
            }
        }
        Ok(fanout)
    }

    pub async fn record_checkpoint_transfer_drain(
        &self,
        agreement: &FamilyTransfer,
        terminal: FamilyTransferDestinationState,
        authority: &dyn FamilyTransferAuthority,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let fanout = agreement.fanout()?;
        self.require_transfer_original(agreement.source)?;
        if !terminal.terminal() {
            return Err(transfer_conflict());
        }
        for _ in 0..ATTEMPTS {
            let (mut record, etag) = self.exact_transfer(agreement, fanout).await?;
            match record.phase {
                TransferPhase::Drained {
                    destination: prior, ..
                } => {
                    return if prior == terminal {
                        Ok(())
                    } else {
                        Err(transfer_conflict())
                    };
                }
                TransferPhase::Preparing
                    if matches!(terminal, FamilyTransferDestinationState::Installed { .. }) =>
                {
                    return Err(transfer_conflict());
                }
                _ => (),
            }
            if self
                .observe_destination(fanout, authority, cancellation)
                .await?
                != terminal
            {
                return Err(transfer_conflict());
            }
            let source = cancellation
                .run_until_cancelled(authority.source_copy_drain(agreement.source, cancellation))
                .await
                .ok_or_else(transfer_unavailable)??;
            if source.source != agreement.source
                || source.agreement_sha256 != fanout.agreement_sha256
                || source.destination != terminal
                || source.drain_sha256 == [0; 32]
            {
                return Err(transfer_conflict());
            }
            record.phase = TransferPhase::Drained {
                destination: terminal,
                source_sha256: source.drain_sha256,
            };
            if self
                .write_transfer(fanout.address, Some(&etag), &record)
                .await?
            {
                return Ok(());
            }
        }
        Err(contention())
    }

    pub async fn require_checkpoint_transfer_drained(
        &self,
        agreement: &FamilyTransfer,
    ) -> Result<()> {
        let fanout = agreement.fanout()?;
        self.require_transfer_original(agreement.source)?;
        let (record, _) = self.exact_transfer(agreement, fanout).await?;
        if !matches!(record.phase, TransferPhase::Drained { .. }) {
            return Err(transfer_unavailable());
        }
        Ok(())
    }

    /// Validates inherited original ciphertext for a host that will also own the final delivery barrier.
    pub async fn require_checkpoint_transfer_read(
        &self,
        agreement: &FamilyTransfer,
        read: FamilyTransferRead<'_>,
        authority: &dyn FamilyTransferAuthority,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let fanout = agreement.fanout()?;
        self.require_transfer_original(agreement.source)?;
        let (record, _) = self.exact_transfer(agreement, fanout).await?;
        let TransferPhase::Drained {
            destination: terminal @ FamilyTransferDestinationState::Installed { .. },
            ..
        } = record.phase
        else {
            return Err(transfer_unavailable());
        };
        let member = read.contributor;
        let expected_envelope = match read.derived {
            None => member.envelope_sha256,
            Some(row) => {
                member
                    .derived
                    .iter()
                    .find(|artifact| &artifact.row == row)
                    .ok_or_else(transfer_conflict)?
                    .envelope_sha256
            }
        };
        if !agreement.contributors.contains(member)
            || read.envelope.is_empty()
            || digest(&SHA256, read.envelope).as_ref() != expected_envelope.as_slice()
            || agreement
                .contributors
                .iter()
                .flat_map(|member| &member.derived)
                .any(|artifact| artifact.row.capability.is_none())
            || self
                .observe_destination(fanout, authority, cancellation)
                .await?
                != terminal
        {
            return Err(transfer_unavailable());
        }
        self.verify_transfer_members(agreement, Some(fanout))
            .await?;
        if cancellation.is_cancelled() {
            return Err(transfer_unavailable());
        }
        if self.original.is_some() {
            for family in agreement
                .contributors
                .iter()
                .map(|member| member.family)
                .collect::<BTreeSet<_>>()
            {
                self.track_family_delivery(family)?;
            }
        }
        Ok(())
    }

    pub(super) async fn checkpoint_transfers_drained(&self, family: &Family) -> Result<bool> {
        for fanout in &family.transfers {
            let Some((record, _)) = self.read_transfer(fanout.address).await? else {
                return Err(transfer_unavailable());
            };
            if record.agreement.fanout()? != *fanout {
                return Err(transfer_conflict());
            }
            if !matches!(record.phase, TransferPhase::Drained { .. }) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn require_transfer_original(&self, source: FamilyTransferSource) -> Result<()> {
        if self.original.as_ref().is_some_and(|original| {
            original.deployment != source.deployment
                || original.reader.history_fingerprint() != source.history
                || original.cancellation.is_cancelled()
        }) {
            return Err(transfer_unavailable());
        }
        Ok(())
    }

    async fn verify_transfer_members(
        &self,
        agreement: &FamilyTransfer,
        fanout: Option<FamilyTransferFanout>,
    ) -> Result<()> {
        let current = if let Some(original) = &self.original {
            Some(
                original
                    .reader
                    .current(original.deployment, original.cancellation)
                    .await?,
            )
        } else {
            None
        };
        let mut last_family = None;
        for member in &agreement.contributors {
            if last_family != Some(member.family) {
                if let Some(current) = &current
                    && current
                        .assertion_family_fence(member.family)
                        .await?
                        .is_some()
                {
                    return Err(rtdb_storage::Error::ErasedUnderView.into());
                }
                last_family = Some(member.family);
            }
            let id = family_vault_address(agreement.source.deployment, member.family);
            self.require_position_open(member.occurrence.position)
                .await?;
            let (family, _) = self
                .read_current(id, true)
                .await?
                .ok_or_else(transfer_conflict)?;
            check_member(&family, member)?;
            if fanout.is_some_and(|forward| !family.transfers.contains(&forward)) {
                return Err(transfer_conflict());
            }
        }
        Ok(())
    }

    async fn observe_destination(
        &self,
        fanout: FamilyTransferFanout,
        authority: &dyn FamilyTransferAuthority,
        cancellation: &CancellationToken,
    ) -> Result<FamilyTransferDestinationState> {
        let observed = cancellation
            .run_until_cancelled(authority.destination_custody(fanout.destination, cancellation))
            .await
            .ok_or_else(transfer_unavailable)??;
        if observed.destination != fanout.destination
            || observed.agreement_sha256 != fanout.agreement_sha256
        {
            return Err(transfer_conflict());
        }
        Ok(observed.state)
    }

    async fn confirm_prepared_transfer(
        &self,
        agreement: &FamilyTransfer,
        fanout: FamilyTransferFanout,
        authority: &dyn FamilyTransferAuthority,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let observed = cancellation
            .run_until_cancelled(authority.source_export(agreement.source, cancellation))
            .await
            .ok_or_else(transfer_unavailable)??;
        if observed.source != agreement.source
            || observed.agreement_sha256 != fanout.agreement_sha256
            || observed.inventory_sha256 != agreement.inventory_sha256()?
            || self
                .observe_destination(fanout, authority, cancellation)
                .await?
                != FamilyTransferDestinationState::Prepared
        {
            return Err(transfer_conflict());
        }
        Ok(())
    }

    async fn exact_transfer(
        &self,
        agreement: &FamilyTransfer,
        fanout: FamilyTransferFanout,
    ) -> Result<(TransferRecord, String)> {
        let (record, etag) = self
            .read_transfer(fanout.address)
            .await?
            .ok_or_else(transfer_unavailable)?;
        if record.agreement != *agreement {
            return Err(transfer_conflict());
        }
        Ok((record, etag))
    }

    async fn read_transfer(&self, address: [u8; 32]) -> Result<Option<(TransferRecord, String)>> {
        let Some((bytes, etag)) = self
            .journal
            .read_record(
                AssertionVaultRecord::CheckpointTransfer(address),
                &self.region,
            )
            .await?
        else {
            return Ok(None);
        };
        let record: TransferRecord = serde_json::from_slice(&bytes).map_err(|_| malformed())?;
        if record.agreement.fanout()?.address != address
            || matches!(record.phase, TransferPhase::Drained { destination, source_sha256 } if !destination.terminal() || source_sha256 == [0; 32])
        {
            return Err(malformed());
        }
        Ok(Some((record, etag)))
    }

    async fn write_transfer(
        &self,
        address: [u8; 32],
        previous: Option<&str>,
        record: &TransferRecord,
    ) -> Result<bool> {
        self.journal
            .write_record(
                AssertionVaultRecord::CheckpointTransfer(address),
                &self.region,
                previous,
                encode(record)?.into(),
            )
            .await
    }
}

fn check_member(family: &Family, member: &FamilyTransferContributor) -> Result<()> {
    family.require_live()?;
    family.require_unclaimed()?;
    if family.admitted_member(member.occurrence.assertion) != Some(member.occurrence)
        || member
            .derived
            .iter()
            .any(|artifact| !family.derived.contains(&artifact.row))
    {
        return Err(transfer_conflict());
    }
    Ok(())
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| malformed())?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(too_large());
    }
    Ok(bytes)
}

fn commitment(domain: &[u8], value: &impl Serialize) -> Result<[u8; 32]> {
    let mut hash = Context::new(&SHA256);
    hash.update(domain);
    hash.update(&encode(value)?);
    Ok(hash
        .finish()
        .as_ref()
        .try_into()
        .expect("SHA-256 commitment"))
}

fn transfer_conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.multi/family-checkpoint-transfer",
        "The checkpoint transfer differs from its original custody agreement",
    )
}

fn transfer_unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/family-checkpoint-transfer-unavailable",
        "The checkpoint transfer has no complete current custody proof",
    )
}

#[cfg(test)]
mod tests {
    use rtdb_types::rtdb_v1::Basis;

    use crate::keys::KeyService;

    use super::*;

    struct Observations {
        source: Option<FamilyTransferSourceObservation>,
        source_drain: Option<FamilyTransferSourceDrain>,
        destination: Option<FamilyTransferDestinationObservation>,
    }

    impl Observations {
        fn new(agreement: &FamilyTransfer, state: FamilyTransferDestinationState) -> Self {
            let agreement_sha256 = agreement.fanout().unwrap().agreement_sha256;
            Self {
                source: Some(FamilyTransferSourceObservation {
                    source: agreement.source,
                    agreement_sha256,
                    inventory_sha256: agreement.inventory_sha256().unwrap(),
                }),
                source_drain: state.terminal().then_some(FamilyTransferSourceDrain {
                    source: agreement.source,
                    agreement_sha256,
                    destination: state,
                    drain_sha256: [24; 32],
                }),
                destination: Some(FamilyTransferDestinationObservation {
                    destination: agreement.destination,
                    agreement_sha256,
                    state,
                }),
            }
        }
    }

    #[async_trait::async_trait]
    impl FamilyTransferAuthority for Observations {
        async fn source_export(
            &self,
            _: FamilyTransferSource,
            _: &CancellationToken,
        ) -> Result<FamilyTransferSourceObservation> {
            self.source.ok_or_else(transfer_unavailable)
        }

        async fn source_copy_drain(
            &self,
            _: FamilyTransferSource,
            _: &CancellationToken,
        ) -> Result<FamilyTransferSourceDrain> {
            self.source_drain.ok_or_else(transfer_unavailable)
        }

        async fn destination_custody(
            &self,
            _: FamilyTransferDestination,
            _: &CancellationToken,
        ) -> Result<FamilyTransferDestinationObservation> {
            self.destination.ok_or_else(transfer_unavailable)
        }
    }

    fn agreement() -> FamilyTransfer {
        let original = Uuid::from_u128(1);
        let stage =
            |id| AssertionStage::new(original, NonZeroU64::MIN, Uuid::from_u128(id)).unwrap();
        let world = Uuid::from_u128(2);
        let contributors = [10u8, 11]
            .into_iter()
            .map(|id| {
                let assertion = AssertionId::from_bytes([id; 32]);
                FamilyTransferContributor {
                    family: Uuid::from_u128(id.into()),
                    occurrence: FamilyOccurrence {
                        position: Position {
                            line_generation: *world.as_bytes(),
                            publication: id.into(),
                            ordinal: NonZeroU64::MIN,
                        },
                        assertion,
                        capability: stage(id.into()),
                    },
                    envelope_sha256: digest(&SHA256, &[id]).as_ref().try_into().unwrap(),
                    derived: vec![FamilyTransferArtifact {
                        row: DerivedRow {
                            member: assertion,
                            world: Some(world),
                            relation: "rows_search".into(),
                            key: vec![id],
                            capability: Some(stage(u128::from(id) + 10)),
                        },
                        envelope_sha256: digest(&SHA256, &[id, 1]).as_ref().try_into().unwrap(),
                    }],
                }
            })
            .collect();
        FamilyTransfer {
            source: FamilyTransferSource {
                deployment: original,
                custody_incarnation: NonZeroU64::MIN,
                history: [1; 32],
                installation: [2; 16],
                original_writer: [3; 32],
                checkpoint: PinnedCheckpointIdentity {
                    basis: Basis::new([4; 16], 12),
                    pin: [5; 16],
                    manifest_sha256: [6; 32],
                    until: None,
                },
            },
            destination: FamilyTransferDestination {
                deployment: Uuid::from_u128(3),
                incarnation: [7; 16],
                preparation_sha256: [8; 32],
            },
            contributors,
        }
    }

    async fn enroll(
        families: &AssertionFamilies<'_>,
        agreement: &FamilyTransfer,
        member: &FamilyTransferContributor,
    ) -> Result<Uuid> {
        let family = family_vault_address(agreement.source.deployment, member.family);
        let occurrence = member.occurrence;
        families
            .enroll_original(
                family,
                EnrolledWorldRow {
                    world: Uuid::from_bytes(occurrence.position.line_generation),
                    assertion: occurrence.assertion,
                    capability: occurrence.capability,
                    scope_commitment: [1; 32],
                    slot_commitment: [2; 32],
                    codec: 1,
                },
            )
            .await?;
        families.register(family, occurrence).await?;
        let derived = member
            .derived
            .iter()
            .map(|artifact| artifact.row.clone())
            .collect::<Vec<_>>();
        families.register_derived(family, &derived).await?;
        Ok(family)
    }

    fn read<'a>(
        contributor: &'a FamilyTransferContributor,
        envelope: &'a [u8],
    ) -> FamilyTransferRead<'a> {
        FamilyTransferRead {
            contributor,
            derived: None,
            envelope,
        }
    }

    #[tokio::test]
    async fn checkpoint_transfer_replays_exact_custody_and_keeps_every_family_in_the_fence()
    -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let families = vault.assertion_families(IoRegion::new()).unwrap();
        let agreement = agreement();
        let first = &agreement.contributors[0];
        let second = &agreement.contributors[1];
        let first_family = enroll(&families, &agreement, first).await?;
        let second_family = enroll(&families, &agreement, second).await?;
        let cancellation = CancellationToken::new();
        let order = Uuid::from_u128(100);
        let old_fence = families
            .plan_fence(second_family, second.occurrence.position, order)
            .await?;
        let prepared = Observations::new(&agreement, FamilyTransferDestinationState::Prepared);
        let fanout = families
            .prepare_checkpoint_transfer(&agreement, &prepared, &cancellation)
            .await?;
        assert!(
            families
                .require_checkpoint_transfer_read(
                    &agreement,
                    read(first, &[10]),
                    &prepared,
                    &cancellation
                )
                .await
                .is_err()
        );
        assert!(
            families
                .require_checkpoint_transfer_drained(&agreement)
                .await
                .is_err()
        );
        for (id, member) in [(first_family, first), (second_family, second)] {
            let plan = families
                .plan_fence(id, member.occurrence.position, order)
                .await?;
            assert_eq!(plan.transfers, BTreeSet::from([fanout]));
            assert!(
                plan.assertion_capabilities
                    .contains(&member.occurrence.capability)
            );
            assert_eq!(
                plan.derived,
                member
                    .derived
                    .iter()
                    .map(|artifact| artifact.row.clone())
                    .collect::<BTreeSet<_>>()
            );
            assert!(
                !families
                    .checkpoint_transfers_drained(&families.read(id).await?.unwrap().0)
                    .await?
            );
        }
        assert_ne!(
            families
                .plan_fence(second_family, second.occurrence.position, order)
                .await?
                .vault_inventory(),
            old_fence.vault_inventory()
        );

        let installed = FamilyTransferDestinationState::Installed {
            backing_sha256: [19; 32],
            drain_sha256: [20; 32],
        };
        let observed = Observations::new(&agreement, installed);
        let mut destination_only = Observations::new(&agreement, installed);
        destination_only.source_drain = None;
        assert!(
            families
                .record_checkpoint_transfer_drain(
                    &agreement,
                    installed,
                    &destination_only,
                    &cancellation
                )
                .await
                .is_err()
        );
        families
            .record_checkpoint_transfer_drain(&agreement, installed, &observed, &cancellation)
            .await?;
        let restarted = LocalKeyService::with_archive_broker(directory.path())?;
        let restarted = restarted.assertion_families(IoRegion::new()).unwrap();
        let unavailable = Observations {
            source: None,
            source_drain: None,
            destination: None,
        };
        assert_eq!(
            restarted
                .prepare_checkpoint_transfer(&agreement, &unavailable, &cancellation)
                .await?,
            fanout
        );
        restarted
            .record_checkpoint_transfer_drain(&agreement, installed, &unavailable, &cancellation)
            .await?;
        restarted
            .require_checkpoint_transfer_drained(&agreement)
            .await?;
        assert!(
            restarted
                .require_checkpoint_transfer_read(
                    &agreement,
                    read(first, &[10]),
                    &unavailable,
                    &cancellation
                )
                .await
                .is_err()
        );
        restarted
            .require_checkpoint_transfer_read(
                &agreement,
                read(first, &[10]),
                &observed,
                &cancellation,
            )
            .await?;
        assert!(
            restarted
                .require_checkpoint_transfer_read(
                    &agreement,
                    read(first, &[11]),
                    &observed,
                    &cancellation
                )
                .await
                .is_err()
        );
        let derivative = &first.derived[0].row;
        restarted
            .require_checkpoint_transfer_read(
                &agreement,
                FamilyTransferRead {
                    derived: Some(derivative),
                    ..read(first, &[10, 1])
                },
                &observed,
                &cancellation,
            )
            .await?;
        assert!(
            restarted
                .require_checkpoint_transfer_read(
                    &agreement,
                    FamilyTransferRead {
                        derived: Some(derivative),
                        ..read(first, &[10])
                    },
                    &observed,
                    &cancellation
                )
                .await
                .is_err()
        );

        let mut other = agreement.clone();
        other.destination.preparation_sha256 = [21; 32];
        assert!(
            restarted
                .prepare_checkpoint_transfer(
                    &other,
                    &Observations::new(&other, FamilyTransferDestinationState::Prepared),
                    &cancellation
                )
                .await
                .is_err()
        );
        other = agreement.clone();
        other.source.checkpoint.basis =
            Basis::new(agreement.source.checkpoint.basis.incarnation(), 13);
        assert!(
            restarted
                .prepare_checkpoint_transfer(
                    &other,
                    &Observations::new(&other, FamilyTransferDestinationState::Prepared),
                    &cancellation
                )
                .await
                .is_err()
        );
        assert!(
            restarted
                .record_checkpoint_transfer_drain(
                    &agreement,
                    FamilyTransferDestinationState::Discarded {
                        drain_sha256: [22; 32]
                    },
                    &observed,
                    &cancellation
                )
                .await
                .is_err()
        );
        let fence = restarted
            .fence(second_family, second.occurrence.position, order)
            .await?;
        assert_eq!(fence.transfers, BTreeSet::from([fanout]));
        assert!(
            restarted
                .require_checkpoint_transfer_read(
                    &agreement,
                    read(first, &[10]),
                    &observed,
                    &cancellation
                )
                .await
                .is_err()
        );
        restarted
            .require_checkpoint_transfer_drained(&agreement)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_transfer_refuses_unknown_or_partial_bindings_and_recovers_discard()
    -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let families = vault.assertion_families(IoRegion::new()).unwrap();
        let agreement = agreement();
        let first = &agreement.contributors[0];
        let first_family = enroll(&families, &agreement, first).await?;
        let cancellation = CancellationToken::new();
        let fanout = agreement.fanout()?;
        let prepared = Observations::new(&agreement, FamilyTransferDestinationState::Prepared);
        assert!(
            families
                .prepare_checkpoint_transfer(&agreement, &prepared, &cancellation)
                .await
                .is_err()
        );
        assert!(families.read_transfer(fanout.address).await?.is_none());
        assert!(
            families
                .read(first_family)
                .await?
                .unwrap()
                .0
                .transfers
                .is_empty()
        );
        enroll(&families, &agreement, &agreement.contributors[1]).await?;

        let mut partial = agreement.clone();
        partial.contributors.pop();
        assert!(
            families
                .prepare_checkpoint_transfer(&partial, &prepared, &cancellation)
                .await
                .is_err()
        );
        let mut expiring = agreement.clone();
        expiring.source.checkpoint.until = Some(rtdb_types::Timestamp(100));
        assert!(
            families
                .prepare_checkpoint_transfer(&expiring, &prepared, &cancellation)
                .await
                .is_err()
        );
        let mut missing_reverse =
            Observations::new(&agreement, FamilyTransferDestinationState::Prepared);
        missing_reverse.destination = None;
        assert!(
            families
                .prepare_checkpoint_transfer(&agreement, &missing_reverse, &cancellation)
                .await
                .is_err()
        );

        families
            .write_transfer(
                fanout.address,
                None,
                &TransferRecord {
                    agreement: agreement.clone(),
                    phase: TransferPhase::Preparing,
                },
            )
            .await?;
        families
            .update(first_family, |family| {
                family.transfers.push(fanout);
                Ok(((), true))
            })
            .await?;
        let terminal = FamilyTransferDestinationState::Installed {
            backing_sha256: [19; 32],
            drain_sha256: [20; 32],
        };
        let installed = Observations::new(&agreement, terminal);
        assert!(
            families
                .record_checkpoint_transfer_drain(&agreement, terminal, &installed, &cancellation)
                .await
                .is_err()
        );
        assert!(
            families
                .require_checkpoint_transfer_read(
                    &agreement,
                    read(first, &[10]),
                    &installed,
                    &cancellation
                )
                .await
                .is_err()
        );
        families
            .fence(
                first_family,
                first.occurrence.position,
                Uuid::from_u128(101),
            )
            .await?;
        assert!(
            families
                .prepare_checkpoint_transfer(&agreement, &prepared, &cancellation)
                .await
                .is_err()
        );
        assert!(
            !families
                .checkpoint_transfers_drained(&families.read(first_family).await?.unwrap().0)
                .await?
        );
        let terminal = FamilyTransferDestinationState::Discarded {
            drain_sha256: [23; 32],
        };
        families
            .record_checkpoint_transfer_drain(
                &agreement,
                terminal,
                &Observations::new(&agreement, terminal),
                &cancellation,
            )
            .await?;
        assert!(
            families
                .checkpoint_transfers_drained(&families.read(first_family).await?.unwrap().0)
                .await?
        );
        families
            .require_checkpoint_transfer_drained(&agreement)
            .await?;
        assert!(
            families
                .require_checkpoint_transfer_read(
                    &agreement,
                    read(first, &[10]),
                    &installed,
                    &cancellation
                )
                .await
                .is_err()
        );
        Ok(())
    }
}
