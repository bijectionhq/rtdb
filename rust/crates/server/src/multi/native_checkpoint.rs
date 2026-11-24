use rtdb_storage::sealing::{PlaintextReleaseCheck, PlaintextView};
use rtdb_types::rtdb_v1::NativeCheckpointCustody;

use super::{catalog::ManagedTransfer, managed_transfer::reservation_conflict, *};

#[derive(Clone, Copy)]
pub(super) struct NativeCheckpointSource;

pub(super) struct NativeCheckpointProvider<'a> {
    runtime: &'a MultiRuntime,
    name: &'a str,
    transfer: &'a ManagedTransfer,
}

impl MultiRuntime {
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
        source.record_native_checkpoint_drain(installed).await?;
        source.release_managed_checkpoint(reservation).await
    }

    pub(super) async fn require_read(
        &self,
        destination: &Arc<MultiServing>,
        read: ManagedCheckpointRead<'_>,
    ) -> Result<()> {
        if !PlaintextView::has_delivery_scope() {
            return Err(terminal_required());
        }
        let reservation = self.transfer.reservation;
        let (basis, view, generation) = destination.managed_checkpoint_view(reservation).await?;
        let custody = view
            .inherited_custody()
            .filter(|custody| custody.reservation == reservation)
            .ok_or_else(terminal_required)?;
        if generation != read.generation {
            return Err(rtdb_crypt::erased_under_view());
        }
        let (group, occurrence) =
            view.native_inherited_carrier(read.branch, read.table, read.id, read.carrier)?;
        let source = self.runtime.transfer_source_writer(self.transfer).await?;
        let check = Arc::new(InheritedRead {
            source,
            destination: Arc::clone(destination),
            custody: custody.clone(),
            basis,
            generation,
            group,
            occurrence,
        });
        check.require_current().await?;
        let mut key = Sha256::new();
        key.update(b"rtdb/v1/native-checkpoint-read\0");
        key.update(basis.to_bytes());
        key.update(generation.to_be_bytes());
        key.update(group);
        key.update(occurrence);
        PlaintextView::track_release_check(key.finalize().into(), check)
    }
}

struct InheritedRead {
    source: Arc<MultiServing>,
    destination: Arc<MultiServing>,
    custody: NativeCheckpointCustody,
    basis: Basis,
    generation: u64,
    group: [u8; 16],
    occurrence: [u8; 32],
}

#[async_trait::async_trait]
impl PlaintextReleaseCheck for InheritedRead {
    async fn require_current(&self) -> Result<()> {
        let (basis, view, generation) = self
            .destination
            .managed_checkpoint_view(self.custody.reservation)
            .await?;
        if basis != self.basis
            || generation != self.generation
            || view.inherited_custody() != Some(&self.custody)
        {
            return Err(rtdb_crypt::erased_under_view());
        }
        self.source
            .check_native_export_read(&self.custody, self.basis, self.group, self.occurrence)
            .await?;
        let (_, _, generation) = self
            .destination
            .managed_checkpoint_view(self.custody.reservation)
            .await?;
        if generation != self.generation {
            return Err(rtdb_crypt::erased_under_view());
        }
        Ok(())
    }
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
