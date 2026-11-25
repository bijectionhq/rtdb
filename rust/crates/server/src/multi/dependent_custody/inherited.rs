use rtdb_db::NativeDependentOriginal;
use rtdb_node::{
    Clock, MultiServing, Node, SystemClock,
    serving::{
        ServingDriver,
        custody_release::{
            CustodyCarrierAuthority, CustodyContributors, CustodyReleaseRegistration,
            CustodyResponseAdmission, CustodyResponseOwner, CustodyResponsePart,
            InheritedContributor, InheritedFamily, InheritedFamilyPoint, OriginalReceipt,
        },
    },
};
use rtdb_tx::transact::{CustodyPayload, CustodyWriterFence, Receipt};
use rtdb_types::{
    Interval, Timestamp,
    rtdb_v1::{
        DependentCustodyReceipt, NativeCheckpointCustody, NativeDependentRead,
        NativeDependentSourceGeneration,
    },
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::*;
use crate::multi::catalog::Status;

const MAX_CONTRIBUTORS: usize = 32;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InheritedProfile {
    pub(super) client_certificate_sha256: [u8; 32],
    pub(super) transport_client_certificate_sha256: Option<[u8; 32]>,
    pub(super) source_incarnation: Uuid,
    pub(super) destination_incarnation: Uuid,
    pub(super) destination: String,
    pub(super) owner: [u8; 32],
    pub(super) application_scope: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedReadProof {
    pub read: NativeDependentRead,
    pub owner: [u8; 32],
    pub application_scope: [u8; 32],
    pub enrollment: DependentCustodyReceipt,
    pub authority: CustodyCarrierAuthority,
    pub original_writer: Basis,
    pub source_generation: NativeDependentSourceGeneration,
    pub original: [u8; 32],
    pub carrier_receipt: OriginalReceipt,
    pub carrier_sha256: [u8; 32],
    pub link_sha256: Option<[u8; 32]>,
    pub envelope_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedRelease {
    pub proof: InheritedReadProof,
    pub part: CustodyResponsePart,
    pub frame_sha256: [u8; 32],
    pub expires_at: Timestamp,
}

impl InheritedRelease {
    fn check(&self, now: Timestamp) -> Result<()> {
        if self.frame_sha256 == [0; 32]
            || [
                self.part.transport_history,
                self.part.producer,
                self.part.consumer,
                self.part.response,
            ]
            .contains(&[0; 16])
            || self.expires_at <= now
            || self.expires_at.0 > now.0.saturating_add(120_000_000)
        {
            return Err(refused());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedRegistration {
    pub release: InheritedRelease,
    pub receipt: DependentCustodyReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedCarrierBinding {
    pub family: InheritedFamily,
    pub contributors: Vec<InheritedContributor>,
    pub read_sha256: [u8; 32],
}

impl InheritedCarrierBinding {
    fn new(
        proof: &InheritedReadProof,
        family: InheritedFamily,
        contributors: Vec<InheritedContributor>,
    ) -> Result<Self> {
        let read_sha256 = Self::commitment(proof, &family, &contributors)?;
        Ok(Self {
            family,
            contributors,
            read_sha256,
        })
    }

    fn commitment(
        proof: &InheritedReadProof,
        family: &InheritedFamily,
        contributors: &[InheritedContributor],
    ) -> Result<[u8; 32]> {
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/native-dependent-carrier\0");
        hash.update(encode(&(proof, family, contributors))?);
        Ok(hash.finalize().into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedAdmission {
    pub release: InheritedRelease,
    pub binding: InheritedCarrierBinding,
    pub nonce: [u8; 32],
}

impl InheritedAdmission {
    fn registrations(&self) -> Result<Vec<CustodyReleaseRegistration>> {
        let proof = &self.release.proof;
        let members = &self.binding.contributors;
        if self.nonce == [0; 32]
            || self.release.expires_at > self.binding.family.expires_at
            || self.release.frame_sha256 == [0; 32]
            || !(1..=MAX_CONTRIBUTORS).contains(&members.len())
            || members
                .windows(2)
                .any(|pair| pair[0].occurrence >= pair[1].occurrence)
            || !members.iter().any(|member| {
                member.occurrence == proof.read.occurrence
                    && member.receipt == proof.carrier_receipt
            })
            || self.binding.read_sha256
                != InheritedCarrierBinding::commitment(proof, &self.binding.family, members)?
        {
            return Err(refused());
        }
        Ok(members
            .iter()
            .map(|member| CustodyReleaseRegistration {
                authority: proof.authority,
                group: proof.read.scope.group,
                original: proof.original,
                occurrence: member.occurrence,
                carrier_receipt_sha256: member.receipt.commitment(),
                part: self.release.part,
                expires_at: self.release.expires_at,
            })
            .collect())
    }

    fn require_live(&self, now: Timestamp) -> Result<()> {
        self.release.check(now)?;
        self.binding.family.require_live(now)
    }

    fn admission_commitment(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/native-dependent-admission\0");
        hash.update(self.binding.read_sha256);
        hash.update(self.nonce);
        hash.finalize().into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedAdmissionRegistration {
    pub admission: InheritedAdmission,
    pub receipt: DependentCustodyReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedDisposition {
    pub admission: InheritedAdmission,
    pub registration: DependentCustodyReceipt,
    pub receipt: DependentCustodyReceipt,
    pub disposition_sha256: [u8; 32],
}

fn positive(receipt: DependentCustodyReceipt) -> bool {
    receipt.key != [0; 32] && receipt.evidence_sha256 != [0; 32]
}

impl InheritedRegistration {
    fn check(&self, release: &InheritedRelease) -> Result<()> {
        if &self.release != release
            || self.receipt.key == [0; 32]
            || self.receipt.evidence_sha256 == [0; 32]
        {
            return Err(refused());
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InheritedReadRequest {
    pub(super) version: u32,
    pub(super) read: NativeDependentRead,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InheritedReleaseRequest {
    pub(super) version: u32,
    pub(super) release: InheritedRelease,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InheritedCarrierRequest {
    pub(super) version: u32,
    pub(super) proof: InheritedReadProof,
    pub(super) family: InheritedFamily,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InheritedAdmissionRequest {
    pub(super) version: u32,
    pub(super) admission: InheritedAdmission,
}

#[derive(Serialize)]
pub(super) struct InheritedRow {
    written: Basis,
    valid: Interval,
    carrier: Vec<u8>,
    link: Option<Vec<u8>>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum InheritedResult {
    SourceGeneration {
        source_generation: NativeDependentSourceGeneration,
    },
    Read {
        proof: InheritedReadProof,
    },
    ReleaseChecked {
        receipt: DependentCustodyReceipt,
        expires_at: Timestamp,
    },
    Carrier {
        row: InheritedRow,
        binding: InheritedCarrierBinding,
    },
    Admitted {
        basis: Basis,
        registrations: Vec<CustodyReleaseRegistration>,
    },
    Recorded {
        basis: Basis,
        registrations: Vec<CustodyReleaseRegistration>,
    },
    Disposed {
        receipt: DependentCustodyReceipt,
    },
}

struct InheritedSource {
    source_name: String,
    source: Arc<MultiServing>,
    custody: NativeCheckpointCustody,
    binding: [u8; 32],
    owner: CustodyResponseOwner,
    authority: CustodyCarrierAuthority,
    writer: CustodyWriterFence,
}

struct InheritedCarrier {
    source_name: String,
    owner: CustodyResponseOwner,
    point: InheritedFamilyPoint,
    payload: CustodyPayload,
}

fn original_receipt(receipt: Receipt) -> OriginalReceipt {
    OriginalReceipt {
        key: receipt.key,
        basis: receipt.basis,
        system_time: receipt.system_time,
    }
}

impl InheritedProfile {
    pub(super) fn authorizes(&self, path: &str, certificate: [u8; 32]) -> bool {
        if matches!(path, INHERITED_ADMIT | INHERITED_DISPOSE) {
            self.transport_client_certificate_sha256 == Some(certificate)
        } else {
            self.client_certificate_sha256 == certificate
        }
    }

    pub(super) async fn source_generation(
        &self,
        runtime: &MultiRuntime,
        scope: NativeDependentCustodyScope,
    ) -> Result<InheritedResult> {
        let (source, expected) = runtime
            .native_dependent_source(&self.destination, scope.reservation, scope.group)
            .await?;
        if scope != expected {
            return Err(refused());
        }
        let source_generation = source.native_dependent_source_generation(scope).await?;
        Ok(InheritedResult::SourceGeneration { source_generation })
    }

    pub(super) async fn read(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        read: NativeDependentRead,
        cancellation: &CancellationToken,
    ) -> Result<InheritedResult> {
        let proof = self.proof(runtime, node, read, cancellation).await?;
        Ok(InheritedResult::Read { proof })
    }

    pub(super) async fn release(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        release: InheritedRelease,
        cancellation: &CancellationToken,
    ) -> Result<InheritedResult> {
        release.check(node.now())?;
        let current = self
            .proof(runtime, node, release.proof.read.clone(), cancellation)
            .await?;
        if current != release.proof {
            return Err(refused());
        }
        let observer = runtime
            .native_dependent_custody
            .as_deref()
            .ok_or_else(pending)?;
        let observed = observer
            .observe_inherited_registration(&release, cancellation)
            .await?;
        observed.check(&release)?;
        let current = self
            .proof(runtime, node, release.proof.read.clone(), cancellation)
            .await?;
        if current != release.proof {
            return Err(refused());
        }
        release.check(node.now())?;
        Ok(InheritedResult::ReleaseChecked {
            receipt: observed.receipt,
            expires_at: release.expires_at,
        })
    }

    pub(super) async fn carrier(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        proof: InheritedReadProof,
        family: InheritedFamily,
        cancellation: &CancellationToken,
    ) -> Result<InheritedResult> {
        family.require_live(node.now())?;
        let observer = runtime
            .native_dependent_custody
            .as_deref()
            .ok_or_else(pending)?;
        let enrollment = observer
            .observe_inherited_enrollment(&proof, cancellation)
            .await?;
        if enrollment != proof.enrollment || !positive(enrollment) {
            return Err(refused());
        }
        let source = self
            .carrier_evidence(runtime, node, &proof, cancellation)
            .await?;
        let selected = source
            .point
            .contributors
            .iter()
            .find(|member| member.occurrence == proof.read.occurrence)
            .ok_or_else(refused)?;
        let mounted = source.owner.response_release(&SystemClock, cancellation)?;
        let (carrier, link) = mounted
            .open_inherited_carrier(
                &source.point,
                &family,
                &source.payload,
                proof.envelope_sha256,
            )
            .await?;
        let row = InheritedRow {
            written: Basis::new(
                proof.read.scope.destination.incarnation(),
                selected.receipt.basis.position(),
            ),
            valid: selected.valid,
            carrier,
            link,
        };
        let binding = InheritedCarrierBinding::new(&proof, family, source.point.contributors)?;
        let current = self
            .current_binding(runtime, node, &proof, &binding, cancellation)
            .await?;
        if current.mount_id() != source.owner.mount_id() {
            return Err(refused());
        }
        Ok(InheritedResult::Carrier { row, binding })
    }

    pub(super) async fn admit(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        admission: InheritedAdmission,
        cancellation: &CancellationToken,
    ) -> Result<InheritedResult> {
        let registrations = admission.registrations()?;
        admission.require_live(node.now())?;
        let proof = &admission.release.proof;
        let observer = runtime
            .native_dependent_custody
            .as_deref()
            .ok_or_else(pending)?;
        let observed = observer
            .observe_inherited_admission(&admission, cancellation)
            .await?;
        if observed.admission != admission || !positive(observed.receipt) {
            return Err(refused());
        }
        let owner = self
            .current_binding(runtime, node, proof, &admission.binding, cancellation)
            .await?;
        let mounted = owner.response_release(&SystemClock, cancellation)?;
        let contributors: CustodyContributors = registrations
            .iter()
            .map(|reg| (reg.group, reg.occurrence))
            .collect();
        let frame = &admission.release;
        let commitment = admission.admission_commitment();
        let cutoff = mounted
            .registered_scoped_cutoff(
                frame.part,
                frame.frame_sha256,
                commitment,
                &contributors,
                frame.expires_at,
            )
            .await?;
        if cutoff != frame.expires_at {
            return Err(refused());
        }
        let order = CustodyResponseAdmission {
            registrations: registrations.clone(),
            frame_sha256: frame.frame_sha256,
            read_proofs_sha256: Some(commitment),
            selected_basis: proof.source_generation.pinned_source,
            key_generation: proof.source_generation.key_generation,
        };
        mounted.prepare_scoped_registrations(&order, true).await?;

        let current = observer
            .observe_inherited_admission(&admission, cancellation)
            .await?;
        if current != observed {
            return Err(refused());
        }
        let current = self
            .current_binding(runtime, node, proof, &admission.binding, cancellation)
            .await?;
        if current.mount_id() != owner.mount_id() {
            return Err(refused());
        }
        admission.require_live(node.now())?;
        let receipt = mounted
            .admit_registered_response(&contributors, order)
            .await?;
        if receipt.replayed {
            return Ok(InheritedResult::Recorded {
                basis: receipt.basis,
                registrations,
            });
        }

        let current = observer
            .observe_inherited_admission(&admission, cancellation)
            .await?;
        if current != observed {
            return Err(refused());
        }
        let current = self
            .current_binding(runtime, node, proof, &admission.binding, cancellation)
            .await?;
        if current.mount_id() != owner.mount_id() {
            return Err(refused());
        }
        admission.require_live(node.now())?;
        Ok(InheritedResult::Admitted {
            basis: receipt.basis,
            registrations,
        })
    }

    pub(super) async fn dispose(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        admission: InheritedAdmission,
        cancellation: &CancellationToken,
    ) -> Result<InheritedResult> {
        let registrations = admission.registrations()?;
        let proof = &admission.release.proof;
        let scope = proof.read.scope;
        let observer = runtime
            .native_dependent_custody
            .as_deref()
            .ok_or_else(pending)?;
        let terminal = observer
            .observe_inherited_disposition(&admission, cancellation)
            .await?;
        if terminal.admission != admission
            || !positive(terminal.registration)
            || !positive(terminal.receipt)
            || terminal.disposition_sha256 == [0; 32]
        {
            return Err(refused());
        }
        let selected = runtime
            .managed_entry(&self.destination, self.destination_incarnation)
            .await?;
        let transfer = selected.managed_transfer()?;
        let (source, expected) = runtime
            .native_dependent_source(&self.destination, scope.reservation, scope.group)
            .await?;
        if scope != expected {
            return Err(refused());
        }
        let Some((enrollment, _)) = source.native_dependent_custody(scope).await? else {
            return Err(refused());
        };
        if enrollment.owner != self.owner
            || enrollment.application_scope != self.application_scope
            || enrollment.receipt != proof.enrollment
            || proof.owner != self.owner
            || proof.application_scope != self.application_scope
            || registrations.iter().any(|registration| {
                !enrollment
                    .members
                    .iter()
                    .any(|member| member.native.occurrence == Some(registration.occurrence))
            })
        {
            return Err(refused());
        }
        source.require_managed_checkpoint_writer().await?;
        let owner = Self::source_owner(node, &transfer.source, source.as_ref())?;
        let mounted = owner.response_release(&SystemClock, cancellation)?;
        for registration in registrations {
            mounted
                .dispose_inherited(registration, terminal.disposition_sha256)
                .await?;
        }
        Ok(InheritedResult::Disposed {
            receipt: terminal.receipt,
        })
    }

    async fn current_binding(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        proof: &InheritedReadProof,
        binding: &InheritedCarrierBinding,
        cancellation: &CancellationToken,
    ) -> Result<CustodyResponseOwner> {
        let source = self
            .carrier_evidence(runtime, node, proof, cancellation)
            .await?;
        if source.point.contributors != binding.contributors {
            return Err(refused());
        }
        source
            .owner
            .response_release(&SystemClock, cancellation)?
            .require_inherited_family(&source.point, &binding.family)
            .await?;
        let current = self
            .proof(runtime, node, proof.read.clone(), cancellation)
            .await?;
        if &current != proof
            || node
                .catalog()
                .original_custody_response_owner(&source.source_name)?
                .mount_id()
                != source.owner.mount_id()
        {
            return Err(refused());
        }
        Ok(source.owner)
    }

    async fn carrier_evidence(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        proof: &InheritedReadProof,
        cancellation: &CancellationToken,
    ) -> Result<InheritedCarrier> {
        let scope = proof.read.scope;
        if !runtime
            .native_dependent_custody
            .as_deref()
            .is_some_and(|observer| observer.enrollment_uses_source_generation(scope))
        {
            return Err(refused());
        }
        let source = self
            .select(runtime, node, &proof.read, cancellation)
            .await?;
        let evidence = source
            .source
            .native_dependent_carrier(&source.custody, &proof.read, &source.writer, source.binding)
            .await?;
        let current = self.confirm_proof(
            node,
            &source,
            proof.read.clone(),
            evidence.original,
            cancellation,
        )?;
        if &current != proof {
            return Err(refused());
        }
        let contributors = evidence
            .contributors
            .into_iter()
            .map(|(member, receipt)| InheritedContributor {
                occurrence: member.commitment(scope.group),
                placement: member.placement_id,
                branch: member.target.branch,
                table: member.target.table,
                id: member.target.id,
                valid: member.target.valid,
                ordinal: member.ordinal,
                receipt: original_receipt(receipt),
                carrier_sha256: member.carrier_sha256,
                link_sha256: member.link_sha256,
            })
            .collect();
        let point = InheritedFamilyPoint {
            authority: proof.authority,
            read: proof.read.clone(),
            enrollment: proof.enrollment,
            original_writer: proof.original_writer,
            source_generation: proof.source_generation,
            contributors,
        };
        Ok(InheritedCarrier {
            source_name: source.source_name,
            owner: source.owner,
            point,
            payload: evidence.payload,
        })
    }

    async fn proof(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        read: NativeDependentRead,
        cancellation: &CancellationToken,
    ) -> Result<InheritedReadProof> {
        let source = self.select(runtime, node, &read, cancellation).await?;
        let current = source
            .source
            .native_dependent_original(&source.custody, &read, &source.writer)
            .await?;
        self.confirm_proof(node, &source, read, current, cancellation)
    }

    fn source_owner(
        node: &Node,
        source_name: &str,
        source: &MultiServing,
    ) -> Result<CustodyResponseOwner> {
        let catalog = node.catalog();
        let attachment = catalog.database(source_name)?;
        let driver: &dyn ServingDriver = source;
        if !attachment
            .serving_driver()
            .is_some_and(|attached| std::ptr::addr_eq(attached, driver))
        {
            return Err(refused());
        }
        catalog.original_custody_response_owner(source_name)
    }

    async fn select(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        read: &NativeDependentRead,
        cancellation: &CancellationToken,
    ) -> Result<InheritedSource> {
        let (source_name, custody, binding) = self.destination(runtime, read).await?;
        let source = runtime
            .ready_driver(&source_name, self.source_incarnation)
            .await?;
        let owner = Self::source_owner(node, &source_name, source.as_ref())?;
        let mounted = owner.response_release(&SystemClock, cancellation)?;
        let authority = mounted.release().original.authority(source.as_ref())?;
        let writer = mounted.original_writer_fence().await?;

        if self.destination(runtime, read).await? != (source_name.clone(), custody.clone(), binding)
        {
            return Err(refused());
        }
        Ok(InheritedSource {
            source_name,
            source,
            custody,
            binding,
            owner,
            authority,
            writer,
        })
    }

    fn confirm_proof(
        &self,
        node: &Node,
        source: &InheritedSource,
        read: NativeDependentRead,
        current: NativeDependentOriginal,
        cancellation: &CancellationToken,
    ) -> Result<InheritedReadProof> {
        if current.owner != self.owner
            || current.application_scope != self.application_scope
            || node
                .catalog()
                .original_custody_response_owner(&source.source_name)?
                .mount_id()
                != source.owner.mount_id()
            || cancellation.is_cancelled()
        {
            return Err(refused());
        }
        Ok(InheritedReadProof {
            read,
            owner: current.owner,
            application_scope: current.application_scope,
            enrollment: current.enrollment,
            authority: source.authority,
            original_writer: source.writer.written,
            source_generation: current.source_generation,
            original: current.original,
            carrier_receipt: original_receipt(current.receipt),
            carrier_sha256: current.carrier_sha256,
            link_sha256: current.link_sha256,
            envelope_sha256: current.envelope_sha256,
        })
    }

    async fn destination(
        &self,
        runtime: &MultiRuntime,
        read: &NativeDependentRead,
    ) -> Result<(String, NativeCheckpointCustody, [u8; 32])> {
        let scope = read.scope;
        let selected = runtime
            .managed_entry(&self.destination, self.destination_incarnation)
            .await?;
        let Status::ManagedTransferReady { transfer, basis } = selected.status else {
            return Err(pending());
        };
        if basis != scope.destination
            || transfer.reservation != scope.reservation
            || runtime
                .native_checkpoint_provider(&self.destination, &transfer)
                .is_none()
        {
            return Err(refused());
        }
        let destination = runtime
            .ready_driver(&self.destination, self.destination_incarnation)
            .await?;
        let (restored, view, generation) = destination
            .managed_checkpoint_view(scope.reservation)
            .await?;
        if restored != scope.destination || generation != read.destination_generation {
            return Err(refused());
        }
        let (custody, binding) = view.native_dependent_destination(read, SystemClock.now())?;
        Ok((transfer.source, custody.clone(), binding))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use rtdb_types::{
        EntityId, TableRef,
        rtdb_v1::{
            ManagedCheckpointReservation, ManagedCheckpointTarget, PinnedCheckpointIdentity,
        },
    };

    use super::*;

    #[test]
    fn inherited_admission_binds_the_nonce_and_all_native_receipts() {
        let pinned = Basis::new([1; 16], 20);
        let destination = Basis::new([2; 16], 20);
        let source_generation = NativeDependentSourceGeneration {
            pinned_source: pinned,
            writer_epoch: 2,
            key_generation: 5,
        };
        let scope = NativeDependentCustodyScope {
            reservation: ManagedCheckpointReservation {
                source: PinnedCheckpointIdentity {
                    basis: pinned,
                    pin: [3; 16],
                    manifest_sha256: [4; 32],
                    until: None,
                },
                target: ManagedCheckpointTarget {
                    incarnation: destination.incarnation(),
                    request: [5; 32],
                },
                reserved_at: Basis::new([6; 16], 2),
            },
            destination,
            source_enrolled_at: Basis::new(pinned.incarnation(), 22),
            group: [7; 16],
            checkpoint_group_sha256: [8; 32],
        };
        let enrollment = DependentCustodyReceipt {
            key: [9; 32],
            evidence_sha256: [10; 32],
        };
        let contributor = |n| InheritedContributor {
            occurrence: [n; 32],
            placement: [n; 16],
            branch: "main".into(),
            table: TableRef::new("public", "items"),
            id: EntityId::Integer(i64::from(n)),
            valid: Interval::ALL,
            ordinal: u64::from(n),
            receipt: OriginalReceipt {
                key: vec![n; 16],
                basis: pinned,
                system_time: Timestamp(20),
            },
            carrier_sha256: [n; 32],
            link_sha256: None,
        };
        let root = contributor(11);
        let selected = contributor(12);
        let proof = InheritedReadProof {
            read: NativeDependentRead {
                scope,
                destination_generation: 5,
                branch: selected.branch.clone(),
                branch_created: Basis::new(destination.incarnation(), 0),
                table: selected.table.clone(),
                id: selected.id.clone(),
                valid_at: Timestamp(20),
                placement: selected.placement,
                occurrence: selected.occurrence,
                application_key: [13; 32],
            },
            owner: [14; 32],
            application_scope: [15; 32],
            enrollment,
            authority: CustodyCarrierAuthority {
                incarnation: pinned.incarnation(),
                source_history: [16; 32],
                installation: [17; 16],
                registry: [18; 32],
                original_writer: [19; 32],
            },
            original_writer: pinned,
            source_generation,
            original: root.occurrence,
            carrier_receipt: selected.receipt.clone(),
            carrier_sha256: selected.carrier_sha256,
            link_sha256: None,
            envelope_sha256: [20; 32],
        };
        let family = InheritedFamily {
            generation: [21; 32],
            receipt: enrollment,
            reader: enrollment,
            expires_at: Timestamp(120),
        };
        let binding =
            InheritedCarrierBinding::new(&proof, family, vec![root.clone(), selected.clone()])
                .unwrap();
        let part = CustodyResponsePart {
            transport_history: [22; 16],
            producer: [23; 16],
            consumer: [24; 16],
            response: [25; 16],
            ordinal: NonZeroU64::MIN,
        };
        let mut admission = InheritedAdmission {
            release: InheritedRelease {
                proof,
                part,
                frame_sha256: [26; 32],
                expires_at: Timestamp(100),
            },
            binding,
            nonce: [27; 32],
        };
        let registrations = admission.registrations().unwrap();
        assert_eq!(
            registrations
                .iter()
                .map(|reg| reg.carrier_receipt_sha256)
                .collect::<Vec<_>>(),
            [root.receipt.commitment(), selected.receipt.commitment()]
        );
        assert!(
            registrations
                .iter()
                .all(|reg| reg.part == part && reg.expires_at == Timestamp(100))
        );
        let commitment = admission.admission_commitment();
        admission.nonce = [28; 32];
        assert_ne!(admission.admission_commitment(), commitment);
        admission.binding.contributors.remove(0);
        assert!(admission.registrations().is_err());
    }

    #[test]
    fn inherited_transport_requires_its_separate_leaf() {
        let profile = InheritedProfile {
            client_certificate_sha256: [1; 32],
            transport_client_certificate_sha256: Some([2; 32]),
            source_incarnation: Uuid::from_u128(1),
            destination_incarnation: Uuid::from_u128(2),
            destination: "destination".into(),
            owner: [3; 32],
            application_scope: [4; 32],
        };
        assert!(profile.authorizes(INHERITED_CARRIER, [1; 32]));
        assert!(!profile.authorizes(INHERITED_ADMIT, [1; 32]));
        assert!(profile.authorizes(INHERITED_ADMIT, [2; 32]));
        assert!(profile.authorizes(INHERITED_DISPOSE, [2; 32]));
        assert!(!profile.authorizes(INHERITED_CARRIER, [2; 32]));
    }
}
