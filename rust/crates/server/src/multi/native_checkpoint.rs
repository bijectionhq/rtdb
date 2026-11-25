use rtdb_tx::transact::Receipt;
use rtdb_types::rtdb_v1::{
    NativeCheckpointCustody, NativeCheckpointMember, NativeCustodyFence,
    NativeDependentCustodyDisposition, NativeDependentCustodyEnrollment,
    NativeDependentCustodyOrder, NativeDependentCustodyScope, NativeDependentSourceGeneration,
};

use super::{catalog::ManagedTransfer, managed_transfer::reservation_conflict, *};

#[derive(Clone, Copy)]
pub(super) struct NativeCheckpointSource;

pub(super) struct NativeCheckpointProvider<'a> {
    runtime: &'a MultiRuntime,
    name: &'a str,
    transfer: &'a ManagedTransfer,
}

/// The commissioned observer authenticates both records in the destination owner's durable custody.
#[async_trait::async_trait]
pub trait NativeDependentCustodyAuthority: Send + Sync {
    fn enrollment_uses_source_generation(&self, _scope: NativeDependentCustodyScope) -> bool {
        false
    }

    async fn observe_enrollment(
        &self,
        scope: NativeDependentCustodyScope,
        native: &[NativeCheckpointMember],
        cancellation: &CancellationToken,
    ) -> Result<NativeDependentCustodyEnrollment>;

    async fn observe_enrollment_in_generation(
        &self,
        _scope: NativeDependentCustodyScope,
        _native: &[NativeCheckpointMember],
        _generation: NativeDependentSourceGeneration,
        _cancellation: &CancellationToken,
    ) -> Result<NativeDependentCustodyEnrollment> {
        Err(dependent_required())
    }

    async fn observe_disposition(
        &self,
        enrollment: &NativeDependentCustodyEnrollment,
        fence: &NativeCustodyFence,
        cancellation: &CancellationToken,
    ) -> Result<NativeDependentCustodyDisposition>;

    async fn observe_inherited_registration(
        &self,
        _release: &super::InheritedRelease,
        _cancellation: &CancellationToken,
    ) -> Result<super::InheritedRegistration> {
        Err(dependent_required())
    }

    async fn observe_inherited_admission(
        &self, _admission: &super::InheritedAdmission, _cancellation: &CancellationToken,
    ) -> Result<super::InheritedAdmissionRegistration> {
        Err(dependent_required())
    }

    async fn observe_inherited_enrollment(
        &self, _proof: &super::InheritedReadProof, _cancellation: &CancellationToken,
    ) -> Result<rtdb_types::rtdb_v1::DependentCustodyReceipt> {
        Err(dependent_required())
    }

    async fn observe_inherited_disposition(
        &self, _admission: &super::InheritedAdmission, _cancellation: &CancellationToken,
    ) -> Result<super::InheritedDisposition> {
        Err(dependent_required())
    }
}

impl MultiRuntime {
    pub fn with_native_dependent_custody(
        mut self, authority: Arc<dyn NativeDependentCustodyAuthority>,
    ) -> Result<Self> {
        if self.native_dependent_custody.is_some() { return Err(reservation_conflict()); }
        self.native_dependent_custody = Some(authority);
        Ok(self)
    }

    pub async fn settle_native_dependent_custody(
        &self,
        destination: &str,
        reservation: ManagedCheckpointReservation,
        group: [u8; 16],
        fence: &NativeCustodyFence,
        cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        let (source, scope) = self.native_dependent_source(destination, reservation, group).await?;
        self.settle_native_dependent_scope(&source, scope, fence, cancellation).await
    }

    pub(super) async fn native_dependent_source(
        &self, destination: &str, reservation: ManagedCheckpointReservation, group: [u8; 16],
    ) -> Result<(Arc<MultiServing>, NativeDependentCustodyScope)> {
        let selected = self.managed_entry(destination, Uuid::from_bytes(reservation.target.incarnation)).await?;
        let transfer = selected.managed_transfer()?;
        if transfer.reservation != reservation
            || self.native_checkpoint_provider(destination, &transfer).is_none()
        {
            return Err(reservation_conflict());
        }
        let source = self.transfer_source_writer(&transfer).await?;
        let Some((custody, Some(basis))) = source.native_checkpoint_forward(reservation).await? else {
            return Err(terminal_required());
        };
        Ok((source, dependent_scope(&custody, basis, group)?))
    }

    pub(super) async fn settle_native_dependent_scope(
        &self, source: &MultiServing, scope: NativeDependentCustodyScope,
        fence: &NativeCustodyFence, cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        source.require_native_dependent_fence(scope, fence).await?;
        if let Some(terminal) = source.native_dependent_custody_status(scope, fence).await? {
            return Ok(terminal);
        }

        let enrollment = match source.native_dependent_custody(scope).await? {
            Some((enrollment, _)) => enrollment,
            None => self.enroll_native_dependent(source, scope, cancellation).await?,
        };
        let authority = self.native_dependent_custody.as_deref().ok_or_else(dependent_required)?;
        let disposition = cancellation.run_until_cancelled(authority.observe_disposition(&enrollment, fence, cancellation))
            .await.ok_or_else(transfer_interrupted)??;
        if disposition.scope != scope || &disposition.fence != fence {
            return Err(dependent_required());
        }
        source.record_native_dependent_custody(NativeDependentCustodyOrder::Dispose(disposition)).await
    }

    async fn enroll_native_dependent(
        &self,
        source: &MultiServing,
        scope: NativeDependentCustodyScope,
        cancellation: &CancellationToken,
    ) -> Result<NativeDependentCustodyEnrollment> {
        let authority = self.native_dependent_custody.as_deref().ok_or_else(dependent_required)?;
        let roster = source.native_dependent_custody_roster(scope).await?;
        let generation = if authority.enrollment_uses_source_generation(scope) {
            Some(source.native_dependent_source_generation(scope).await?)
        } else {
            None
        };
        let enrollment = cancellation.run_until_cancelled(async {
            match generation {
                Some(generation) => authority.observe_enrollment_in_generation(scope, &roster, generation, cancellation).await,
                None => authority.observe_enrollment(scope, &roster, cancellation).await,
            }
        })
            .await.ok_or_else(transfer_interrupted)??;
        if enrollment.scope != scope { return Err(dependent_required()); }
        source.record_native_dependent_in_generation(NativeDependentCustodyOrder::Enroll(enrollment.clone()), generation).await?;
        Ok(enrollment)
    }

    pub(super) fn native_checkpoint_provider<'a>(
        &'a self,
        destination: &'a str,
        transfer: &'a ManagedTransfer,
    ) -> Option<NativeCheckpointProvider<'a>> {
        let source = Uuid::from_bytes(transfer.reservation.source.basis.incarnation());
        self.native_checkpoint_source
            .filter(|_| !self.managed_sources.contains_key(&source))
            .map(|_| NativeCheckpointProvider {
                runtime: self,
                name: destination,
                transfer,
            })
    }

    pub(super) async fn claim_peer_native_copy(
        &self,
        source: &str,
        destination: &str,
        reservation: ManagedCheckpointReservation,
    ) -> Result<NativeCheckpointCustody> {
        let runtime = self.clone();
        let (source, destination) = (source.to_owned(), destination.to_owned());
        self.managed_operations
            .spawn(async move {
                let selected = runtime
                    .managed_entry(
                        &destination,
                        Uuid::from_bytes(reservation.target.incarnation),
                    )
                    .await?;
                let transfer = selected.managed_transfer()?;
                if !matches!(selected.status, Status::TransferPending(_))
                    || transfer.source != source
                    || transfer.reservation != reservation
                    || runtime
                        .native_checkpoint_provider(&destination, &transfer)
                        .is_none()
                {
                    return Err(reservation_conflict());
                }
                runtime
                    .transfer_source_writer(&transfer)
                    .await?
                    .claim_native_checkpoint_copy(reservation)
                    .await
            })
            .await
            .map_err(|_| transfer_interrupted())?
    }

    async fn transfer_source_writer(
        &self,
        transfer: &ManagedTransfer,
    ) -> Result<Arc<MultiServing>> {
        self.ready_driver(
            &transfer.source,
            Uuid::from_bytes(transfer.reservation.source.basis.incarnation()),
        )
        .await
    }
}

impl NativeCheckpointProvider<'_> {
    pub(super) async fn require_publication(&self, basis: Basis) -> Result<()> {
        self.runtime.transfer_source_writer(self.transfer).await?
            .require_native_dependent_publication(self.transfer.reservation, basis).await
    }

    pub(super) async fn install_or_recover(&self, destination: &MultiServing) -> Result<Basis> {
        let reservation = self.transfer.reservation;
        let source = self.runtime.transfer_source_writer(self.transfer).await?;
        if destination
            .restored_checkpoint_basis(reservation.source.manifest_sha256)
            .await?
            .is_none()
        {
            destination.prepare_managed_checkpoint(reservation).await?;
            let remote = match &self.runtime.peer {
                Some(peer) => {
                    peer.forward_native_copy(&self.transfer.source, self.name, reservation)
                        .await?
                }
                None => None,
            };
            let custody = match remote {
                Some(custody) => custody,
                None => {
                    self.runtime
                        .claim_peer_native_copy(&self.transfer.source, self.name, reservation)
                        .await?
                }
            };
            if source.native_checkpoint_forward(reservation).await? != Some((custody.clone(), None))
            {
                return Err(terminal_required());
            }
            let export = source.open_managed_checkpoint(reservation).await?;
            destination
                .restore_native_checkpoint(&export, &custody)
                .await?;
        }
        let installed = destination
            .native_checkpoint_installation(reservation)
            .await?;
        let Some((forward, terminal)) = source.native_checkpoint_forward(reservation).await? else {
            return Err(terminal_required());
        };
        if &forward != installed.custody()
            || terminal.is_some_and(|terminal| terminal != installed.basis())
        {
            return Err(terminal_required());
        }
        Ok(installed.basis())
    }

    pub(super) async fn release_source(&self, source: &MultiServing) -> Result<()> {
        let reservation = self.transfer.reservation;
        let destination = self
            .runtime
            .ensure_driver(self.name, Uuid::from_bytes(reservation.target.incarnation))
            .await?;
        let installed = destination
            .native_checkpoint_installation(reservation)
            .await?;
        let custody = installed.custody().clone();
        let basis = installed.basis();
        source.record_native_checkpoint_drain(installed).await?;
        let cancellation = CancellationToken::new();
        for &(group, _) in &custody.groups {
            let scope = dependent_scope(&custody, basis, group)?;
            if source.native_dependent_custody(scope).await?.is_none() {
                self.runtime.enroll_native_dependent(source, scope, &cancellation).await?;
            }
        }
        source.release_managed_checkpoint(reservation).await
    }
}

fn dependent_scope(
    custody: &NativeCheckpointCustody,
    destination: Basis,
    group: [u8; 16],
) -> Result<NativeDependentCustodyScope> {
    let member = custody.groups.binary_search_by_key(&group, |(group, _)| *group)
        .map_err(|_| dependent_required())?;
    Ok(NativeDependentCustodyScope {
        reservation: custody.reservation,
        destination,
        source_enrolled_at: custody.enrolled_at,
        group,
        checkpoint_group_sha256: custody.groups[member].1,
    })
}

fn dependent_required() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/native-dependent-custody",
        "The exact destination custody enrollment or positive disposition is unavailable",
    )
}

fn terminal_required() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/native-checkpoint-terminal",
        "The exact original and destination checkpoint records do not prove a native copy terminal",
    )
}

pub(super) fn inventory_required() -> Anomaly {
    Anomaly::unsupported(
        "rtdb/native-checkpoint-inventory",
        "The source has no local native custody groups for the mounted provider",
    )
}
