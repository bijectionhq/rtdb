use std::{collections::BTreeMap, sync::Arc};

use rtdb_crypt::tokens::DeploymentErasureEvidence;
use rtdb_db::{DeploymentRetirement, MultiDatabase};
use rtdb_tx::transact::{Cell, Guard, Op, Outcome, Receipt, Row, Transact, Version, Window};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Result, StructFields, TableRef, Timestamp, Value,
    rtdb_v1::{At, Basis, Layer, MAIN_BRANCH, ManagedCheckpointReservation,
        ManagedCheckpointTarget, PinnedCheckpointIdentity},
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::classic_ingress::{HeldReady, ReadySelection};
use super::classic_opening::{self, Phase as OpeningPhase, Record as OpeningRecord, Transition as OpeningTransition, Witness as OpeningWitness};

pub(super) struct Catalog {
    database: Mutex<MultiDatabase>,
    incarnation: Uuid,
    peer: Option<Arc<super::peer::Peer>>,
}

#[derive(Clone)]
pub(super) struct Deployment {
    pub database: Uuid,
    pub status: Status,
    pub written: Basis,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Status {
    Reserved,
    Ready,
    ClassicCandidate {
        request: [u8; 32],
        projection: [u8; 32],
        held: Option<Box<ReadySelection>>,
        opening: Option<Box<OpeningRecord>>,
    },
    TransferPending(Transfer),
    TransferReady {
        request: [u8; 32],
        basis: Basis,
    },
    ManagedTransferReady { transfer: ManagedTransfer, basis: Basis },
    Retiring(Retirement),
    Erased {
        retirement: Retirement,
        confirmed: Timestamp,
    },
}

impl Status {
    pub fn ready(&self) -> bool {
        matches!(self, Self::Ready | Self::TransferReady { .. } | Self::ManagedTransferReady { .. })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct Transfer {
    pub request: [u8; 32],
    pub source: String,
    pub incarnation: Uuid,
    pub export: TransferExport,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) enum TransferExport {
    Fork {
        pin: Uuid,
        basis: Basis,
        retain_until: Option<Timestamp>,
    },
    Ticket {
        manifest: [u8; 32],
        ticket: Vec<u8>,
    },
    Managed {
        pin: Uuid,
        basis: Basis,
        manifest: Option<[u8; 32]>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ManagedTransfer {
    pub source: String,
    pub reservation: ManagedCheckpointReservation,
}

impl Deployment {
    pub(super) fn managed_transfer(&self) -> Result<ManagedTransfer> {
        match &self.status {
            Status::ManagedTransferReady { transfer, .. } => Ok(transfer.clone()),
            Status::TransferPending(Transfer {
                request, source, incarnation,
                export: TransferExport::Managed { pin, basis, manifest: Some(manifest) },
            }) if basis.incarnation() == *incarnation.as_bytes() => Ok(ManagedTransfer {
                source: source.clone(),
                reservation: ManagedCheckpointReservation {
                    source: PinnedCheckpointIdentity {
                        basis: *basis, pin: *pin.as_bytes(), manifest_sha256: *manifest, until: None,
                    },
                    target: ManagedCheckpointTarget { incarnation: *self.database.as_bytes(), request: *request },
                    reserved_at: self.written,
                },
            }),
            _ => Err(conflict()),
        }
    }
}

impl std::fmt::Debug for Transfer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Transfer")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Retirement {
    pub terminal: u64,
    pub generation: u64,
    pub destroy_after: Timestamp,
}

impl Catalog {
    pub(super) fn new(database: MultiDatabase, peer: Option<Arc<super::peer::Peer>>) -> Self {
        let incarnation = Uuid::from_bytes(database.incarnation());
        Self {
            database: Mutex::new(database),
            incarnation,
            peer,
        }
    }

    pub(super) async fn apply_peer_write(
        &self,
        incarnation: [u8; 16],
        request: &Transact,
    ) -> Result<Receipt> {
        let [
            Op::Put {
                branch,
                table: relation,
                rows,
                ..
            },
            suffix @ ..,
        ] = request.ops.as_slice()
        else {
            return Err(invalid_catalog());
        };
        if *self.incarnation.as_bytes() != incarnation
            || branch.as_str() != MAIN_BRANCH
            || relation != &table()
            || rows.len() != 1
            || !request.otherwise.is_empty()
        {
            return Err(invalid_catalog());
        }
        match suffix {
            [] => (),
            [Op::Put {
                branch,
                table,
                rows: reverse,
                if_absent: true,
            }] if branch.as_str() == MAIN_BRANCH
                && table == &incarnations()
                && reverse.len() == 1
                && matches!(rows[0].cells.get("database"),
                    Some(Cell::Value(Value::Uuid(uuid))) if reverse[0].id == EntityId::Uuid(*uuid))
                && matches!((&rows[0].id, reverse[0].cells.get("deployment")),
                    (EntityId::String(name), Some(Cell::Value(Value::Utf8(owner)))) if name == owner) => (),
            _ => return Err(invalid_catalog()),
        }
        self.database.lock().await.transact(request).await
    }

    pub(super) fn incarnation(&self) -> Uuid { self.incarnation }

    pub(super) fn deployment_id(&self, name: &str) -> Uuid {
        identity(b"deployment", self.incarnation, name)
    }

    pub(super) async fn tick(
        &self,
        live: &std::collections::BTreeSet<std::net::IpAddr>,
    ) -> Result<rtdb_db::Role> {
        self.database.lock().await.tick(live).await
    }

    pub(super) async fn is_writer(&self) -> bool {
        matches!(self.database.lock().await.role(), rtdb_db::Role::Writer(_))
    }

    pub(super) async fn watch_applied(&self) -> rtdb_db::AppliedWatch {
        self.database.lock().await.watch_applied()
    }

    pub(super) async fn next_deadline(
        &self,
        live_nodes: &std::collections::BTreeSet<std::net::IpAddr>,
    ) -> Option<std::time::Duration> {
        self.database.lock().await.next_deadline(live_nodes)
    }

    pub(super) async fn read(&self) -> Result<BTreeMap<String, Deployment>> {
        read(&mut *self.database.lock().await).await
    }

    pub(super) async fn read_deployment(&self, name: &str, database: Uuid) -> Result<Deployment> {
        let mut control = self.database.lock().await;
        let (index, generation) = control.fresh_view().await?;
        let at = index.head().system_time;
        let view = index.view(MAIN_BRANCH, At::Latest, Layer::Local, at)?;
        let row = view.row(&table(), &EntityId::String(name.to_owned()), at).ok_or_else(conflict)?;
        let image = row.image_at(at).ok_or_else(invalid_catalog)?;
        let deployment = decode_deployment(name, row.cut.basis, &image.cells)?;
        control.check_generation(generation).await?;
        check_identity(deployment, database)
    }

    /// Selects a stable incarnation before any provisioner opens its data stream.
    pub(super) async fn reserve(&self, name: &str, database: Uuid) -> Result<Deployment> {
        self.reserve_status(name, database, Status::Reserved).await
    }

    pub(super) async fn reserve_classic_candidate(
        &self, name: &str, database: Uuid, request: [u8; 32], projection: [u8; 32],
    ) -> Result<Deployment> {
        let status = Status::ClassicCandidate { request, projection, held: None, opening: None };
        let selected = self.reserve_status(name, database, status).await?;
        if !matches!(&selected.status,
            Status::ClassicCandidate { request: prior, projection: original, .. }
                if *prior == request && *original == projection)
        {
            return Err(conflict());
        }
        Ok(selected)
    }

    pub(super) async fn classic_held_ready(
        &self, name: &str, database: Uuid, binding: [u8; 32], ready: &ReadySelection, record: bool,
    ) -> Result<Option<HeldReady>> {
        if !ready.valid_for(self.incarnation, database) {
            return Err(conflict());
        }
        let mut control = self.database.lock().await;
        let mut selected = check_identity(find(&mut control, name).await?, database)?;
        if let Status::ClassicCandidate { request, projection, held: None, opening: None } = &selected.status
            && *request == binding && selected.written == ready.reserved_at
        {
            if !record { return Ok(None); }
            let status = Status::ClassicCandidate {
                request: binding, projection: *projection, held: Some(Box::new(ready.clone())), opening: None,
            };
            write(self, &mut control, name, database, status, Version::Written(selected.written)).await?;
            selected = check_identity(find(&mut control, name).await?, database)?;
        }
        match selected.status {
            Status::ClassicCandidate { request, held: Some(prior), opening: None, .. }
                if request == binding && prior.as_ref() == ready => Ok(Some(HeldReady {
                    binding_sha256: binding,
                    ready: *prior,
                    recorded_at: selected.written,
                })),
            _ => Err(conflict()),
        }
    }

    pub(super) async fn classic_opening(
        &self, witness: &OpeningWitness, transition: OpeningTransition,
    ) -> Result<Option<(OpeningRecord, Basis)>> {
        if !witness.valid(self.incarnation) { return Err(classic_opening::denied()); }
        let name = witness.name();
        let database = witness.database();
        let mut control = self.database.lock().await;
        let selected = check_identity(find(&mut control, name).await?, database)?;
        let Status::ClassicCandidate { request, projection, held: Some(held), opening } = selected.status
            else { return Err(classic_opening::denied()); };
        if request != witness.held_ready.binding_sha256 || *held != witness.held_ready.ready {
            return Err(classic_opening::denied());
        }
        let next = if let Some(prior) = opening {
            if &prior.witness != witness { return Err(classic_opening::denied()); }
            match (transition, &prior.phase) {
                (OpeningTransition::Finish { profile, fence }, OpeningPhase::Opening { profile: retained })
                    if profile == *retained && witness.valid_fence(&fence) => {
                    OpeningPhase::Opened { profile, workload_fence: fence }
                }
                (OpeningTransition::Finish { profile, fence }, OpeningPhase::Opened { profile: retained, workload_fence })
                    if profile == *retained && &fence == workload_fence => {
                    return Ok(Some((*prior, selected.written)));
                }
                (OpeningTransition::Reserve(profile), OpeningPhase::Opening { profile: retained })
                    if profile == *retained => return Ok(Some((*prior, selected.written))),
                (OpeningTransition::Observe, _)
                | (OpeningTransition::Reserve(_) | OpeningTransition::Exclude, OpeningPhase::Excluded)
                | (OpeningTransition::Reserve(_), OpeningPhase::Opened { .. }) => return Ok(Some((*prior, selected.written))),
                _ => return Err(classic_opening::denied()),
            }
        } else {
            if selected.written != witness.held_ready.recorded_at { return Err(classic_opening::denied()); }
            match transition {
                OpeningTransition::Observe => return Ok(None),
                OpeningTransition::Reserve(profile) if profile != [0; 32] => OpeningPhase::Opening { profile },
                OpeningTransition::Exclude => OpeningPhase::Excluded,
                _ => return Err(classic_opening::denied()),
            }
        };
        let expected = OpeningRecord { witness: witness.clone(), phase: next };
        let status = Status::ClassicCandidate { request, projection, held: Some(held), opening: Some(Box::new(expected.clone())) };
        write(self, &mut control, name, database, status, Version::Written(selected.written)).await?;
        let selected = check_identity(find(&mut control, name).await?, database)?;
        match selected.status {
            Status::ClassicCandidate { opening: Some(record), .. } if *record == expected => Ok(Some((*record, selected.written))),
            _ => Err(classic_opening::denied()),
        }
    }

    pub(super) async fn reserve_transfer(
        &self,
        name: &str,
        database: Uuid,
        transfer: Transfer,
    ) -> Result<Deployment> {
        self.reserve_status(name, database, Status::TransferPending(transfer))
            .await
    }

    async fn reserve_status(
        &self,
        name: &str,
        database: Uuid,
        status: Status,
    ) -> Result<Deployment> {
        if database.is_nil() || database == self.incarnation {
            return Err(Anomaly::incorrect(
                "rtdb/multi-catalog-identity",
                "The deployment must use a distinct, nonzero incarnation",
            ));
        }
        let mut control = self.database.lock().await;
        let entries = read(&mut control).await?;
        if let Some(existing) = entries.get(name) {
            return check_identity(existing.clone(), database);
        }
        if entries.values().any(|entry| entry.database == database) {
            return Err(conflict());
        }
        write(self, &mut control, name, database, status, Version::Absent).await?;
        let selected = read(&mut control)
            .await?
            .remove(name)
            .ok_or_else(conflict)?;
        check_identity(selected, database)
    }

    pub(super) async fn mark_transfer_ready(
        &self,
        name: &str,
        database: Uuid,
        request: [u8; 32],
        basis: Basis,
    ) -> Result<()> {
        if basis.incarnation() != *database.as_bytes() {
            return Err(invalid_catalog());
        }
        let mut control = self.database.lock().await;
        let selected = check_identity(find(&mut control, name).await?, database)?;
        let ready = Status::TransferReady { request, basis };
        match &selected.status {
            status if *status == ready => return Ok(()),
            Status::TransferPending(Transfer {
                request: prior,
                export: TransferExport::Ticket { .. },
                ..
            }) if *prior == request => (),
            _ => return Err(conflict()),
        }
        write(
            self,
            &mut control,
            name,
            database,
            ready.clone(),
            Version::Written(selected.written),
        )
        .await?;
        if check_identity(find(&mut control, name).await?, database)?.status != ready {
            return Err(conflict());
        }
        Ok(())
    }

    pub(super) async fn bind_transfer(
        &self,
        name: &str,
        database: Uuid,
        request: [u8; 32],
        manifest: [u8; 32],
        ticket: Vec<u8>,
    ) -> Result<Status> {
        let mut control = self.database.lock().await;
        let selected = check_identity(find(&mut control, name).await?, database)?;
        let mut pending = match &selected.status {
            Status::TransferReady { request: prior, .. } if *prior == request => {
                return Ok(selected.status);
            }
            Status::TransferPending(pending) if pending.request == request => pending.clone(),
            _ => return Err(conflict()),
        };
        match pending.export {
            TransferExport::Fork { .. } => (),
            TransferExport::Ticket { manifest: prior, .. } if prior == manifest => return Ok(selected.status),
            _ => return Err(conflict()),
        }
        pending.export = TransferExport::Ticket { manifest, ticket };
        write(
            self,
            &mut control,
            name,
            database,
            Status::TransferPending(pending),
            Version::Written(selected.written),
        )
        .await?;
        let selected = check_identity(find(&mut control, name).await?, database)?;
        match &selected.status {
            Status::TransferReady { request: prior, .. } if *prior == request => (),
            Status::TransferPending(Transfer {
                request: prior,
                export:
                    TransferExport::Ticket {
                        manifest: original, ..
                    },
                ..
            }) if *prior == request && *original == manifest => (),
            _ => return Err(conflict()),
        }
        Ok(selected.status)
    }

    pub(super) async fn bind_managed_transfer(
        &self, name: &str, target: ManagedCheckpointTarget, identity: PinnedCheckpointIdentity,
    ) -> Result<Deployment> {
        if identity.until.is_some() || identity.manifest_sha256 == [0; 32] {
            return Err(conflict());
        }
        let database = Uuid::from_bytes(target.incarnation);
        let mut control = self.database.lock().await;
        let mut selected = check_identity(find(&mut control, name).await?, database)?;
        if let Status::TransferPending(pending) = &selected.status
            && let TransferExport::Managed { pin, basis, manifest: None } = pending.export
        {
            if pending.request != target.request || identity.pin != *pin.as_bytes()
                || identity.basis != basis || basis.incarnation() != *pending.incarnation.as_bytes()
            {
                return Err(conflict());
            }
            let bound = Transfer {
                export: TransferExport::Managed { pin, basis, manifest: Some(identity.manifest_sha256) },
                ..pending.clone()
            };
            write(self, &mut control, name, database, Status::TransferPending(bound), Version::Written(selected.written)).await?;
            selected = check_identity(find(&mut control, name).await?, database)?;
        }
        let reservation = selected.managed_transfer()?.reservation;
        if reservation.source != identity || reservation.target != target {
            return Err(conflict());
        }
        Ok(selected)
    }

    pub(super) async fn mark_managed_transfer_ready(
        &self, name: &str, transfer: ManagedTransfer, basis: Basis,
    ) -> Result<()> {
        let reservation = transfer.reservation;
        let database = Uuid::from_bytes(reservation.target.incarnation);
        if basis.incarnation() != reservation.target.incarnation
            || basis.position() != reservation.source.basis.position()
            || reservation.reserved_at.incarnation() != *self.incarnation.as_bytes()
        {
            return Err(conflict());
        }
        let mut control = self.database.lock().await;
        let selected = check_identity(find(&mut control, name).await?, database)?;
        if selected.managed_transfer()? != transfer {
            return Err(conflict());
        }
        let ready = Status::ManagedTransferReady { transfer, basis };
        if selected.status != ready {
            if selected.written != reservation.reserved_at || !matches!(selected.status, Status::TransferPending(_)) {
                return Err(conflict());
            }
            write(self, &mut control, name, database, ready.clone(), Version::Written(reservation.reserved_at)).await?;
        }
        if check_identity(find(&mut control, name).await?, database)?.status != ready {
            return Err(conflict());
        }
        Ok(())
    }

    /// Called only after the provisioner observes the reserved stream and its key authority.
    pub(super) async fn mark_ready(&self, name: &str, database: Uuid) -> Result<()> {
        let mut control = self.database.lock().await;
        let selected = read(&mut control)
            .await?
            .remove(name)
            .ok_or_else(invalid_catalog)?;
        let selected = check_identity(selected, database)?;
        match selected.status {
            Status::Ready => return Ok(()),
            Status::Reserved => (),
            _ => return Err(closed()),
        }
        write(
            self,
            &mut control,
            name,
            database,
            Status::Ready,
            Version::Written(selected.written),
        )
        .await?;
        let ready = read(&mut control)
            .await?
            .remove(name)
            .ok_or_else(invalid_catalog)?;
        if check_identity(ready, database)?.status != Status::Ready {
            return Err(conflict());
        }
        Ok(())
    }

    pub(super) async fn mark_retiring(
        &self,
        name: &str,
        event: DeploymentRetirement,
        destroy_after: Timestamp,
    ) -> Result<Deployment> {
        let mut control = self.database.lock().await;
        let selected = check_identity(find(&mut control, name).await?, event.incarnation)?;
        let retirement = Retirement {
            terminal: event.terminal.0,
            generation: event.generation,
            destroy_after,
        };
        if selected.status.ready() {
            write(
                self,
                &mut control,
                name,
                selected.database,
                Status::Retiring(retirement),
                Version::Written(selected.written),
            )
            .await?;
        }
        let selected = find(&mut control, name).await?;
        match selected.status {
            Status::Retiring(prior)
            | Status::Erased {
                retirement: prior, ..
            } if prior == retirement => Ok(selected),
            _ => Err(conflict()),
        }
    }

    pub(super) async fn mark_erased(
        &self,
        name: &str,
        retirement: Retirement,
        confirmed: Timestamp,
    ) -> Result<Deployment> {
        let mut control = self.database.lock().await;
        let selected = find(&mut control, name).await?;
        if selected.status == Status::Retiring(retirement) {
            write(
                self,
                &mut control,
                name,
                selected.database,
                Status::Erased {
                    retirement,
                    confirmed,
                },
                Version::Written(selected.written),
            )
            .await?;
        }
        let selected = find(&mut control, name).await?;
        match selected.status {
            Status::Erased {
                retirement: prior, ..
            } if prior == retirement => Ok(selected),
            _ => Err(conflict()),
        }
    }

    pub(super) async fn erasure_evidence(
        &self,
        name: &str,
        selected: Deployment,
    ) -> Result<(Vec<u8>, Timestamp)> {
        let Status::Erased {
            retirement,
            confirmed,
        } = selected.status
        else {
            return Err(conflict());
        };
        let tokens = self.database.lock().await.token_authority();
        let proof = tokens
            .seal_deployment_erasure(DeploymentErasureEvidence {
                catalog_basis: selected.written,
                deployment: selected.database,
                name_sha256: Sha256::digest(name.as_bytes()).into(),
                terminal: retirement.terminal,
                generation: retirement.generation,
                unreadable_by: confirmed,
            })
            .await?;
        Ok((proof, confirmed))
    }
}

pub(super) fn identity(kind: &[u8], namespace: Uuid, name: &str) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"rtdb/multi/catalog/v1\0");
    hash.update((kind.len() as u64).to_be_bytes());
    hash.update(kind);
    hash.update(namespace.as_bytes());
    hash.update(name.as_bytes());
    let digest = hash.finalize();
    let mut bytes: [u8; 16] = digest[..16].try_into().expect("128-bit UUID");
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

async fn read(control: &mut MultiDatabase) -> Result<BTreeMap<String, Deployment>> {
    let (index, generation) = control.fresh_view().await?;
    let at = index.head().system_time;
    let view = index.view(MAIN_BRANCH, At::Latest, Layer::Local, at)?;
    let table = table();
    let mut entries = BTreeMap::new();
    for id in view.ids(&table) {
        let EntityId::String(name) = id else {
            return Err(invalid_catalog());
        };
        let row = view.row(&table, id, at).ok_or_else(invalid_catalog)?;
        let image = row.image_at(at).ok_or_else(invalid_catalog)?;
        entries.insert(name.clone(), decode_deployment(name, row.cut.basis, &image.cells)?);
    }
    control.check_generation(generation).await?;
    Ok(entries)
}

fn decode_deployment(name: &str, written: Basis, cells: &StructFields<Value>) -> Result<Deployment> {
    let (Some(Value::Uuid(database)), Some(Value::Bool(ready))) = (cells.get("database"), cells.get("ready")) else {
        return Err(invalid_catalog());
    };
    let status = decode_status(*ready, *database, cells)?;
    let database = Uuid::from_bytes(*database);
    let catalog = Uuid::from_bytes(written.incarnation());
    if let Status::ClassicCandidate { held: Some(held), .. } = &status
        && (!held.valid_for(catalog, database) || held.reserved_at >= written)
    {
        return Err(invalid_catalog());
    }
    if let Status::ClassicCandidate { request, held, opening: Some(opening), .. } = &status {
        let witness = &opening.witness;
        if !witness.valid(catalog) || witness.name() != name || witness.database() != database
            || witness.held_ready.binding_sha256 != *request
            || held.as_deref() != Some(&witness.held_ready.ready)
            || witness.held_ready.recorded_at >= written
            || matches!(&opening.phase, OpeningPhase::Opening { profile } | OpeningPhase::Opened { profile, .. } if *profile == [0; 32])
            || matches!(&opening.phase, OpeningPhase::Opened { workload_fence, .. } if !witness.valid_fence(workload_fence))
        {
            return Err(invalid_catalog());
        }
    }
    if let Status::ManagedTransferReady { transfer, .. } = &status {
        let reserved_at = transfer.reservation.reserved_at;
        if reserved_at.incarnation() != written.incarnation() || reserved_at >= written {
            return Err(invalid_catalog());
        }
    }
    Ok(Deployment { database, status, written })
}

async fn find(control: &mut MultiDatabase, name: &str) -> Result<Deployment> {
    read(control).await?.remove(name).ok_or_else(|| {
        Anomaly::new(
            AnomalyCategory::NotFound,
            "rtdb/unknown-deployment",
            "The deployment is absent from the ordered catalog",
        )
    })
}

fn decode_status(ready: bool, database: [u8; 16], cells: &StructFields<Value>) -> Result<Status> {
    if let Some(request) = cells.get("classic_request") {
        let (false, Value::Binary(request), Some(Value::Binary(projection))) =
            (ready, request, cells.get("classic_projection")) else { return Err(invalid_catalog()); };
        let request = request.as_slice().try_into().map_err(|_| invalid_catalog())?;
        let projection = projection.as_slice().try_into().map_err(|_| invalid_catalog())?;
        if request == [0; 32] || projection == [0; 32] { return Err(invalid_catalog()); }
        let held = match cells.get("classic_held_ready") {
            None => None,
            Some(Value::Binary(bytes)) => Some(serde_json::from_slice(bytes).map_err(|_| invalid_catalog())?),
            _ => return Err(invalid_catalog()),
        };
        let opening = match cells.get("classic_opening") {
            None => None,
            Some(Value::Binary(bytes)) => Some(serde_json::from_slice(bytes).map_err(|_| invalid_catalog())?),
            _ => return Err(invalid_catalog()),
        };
        return Ok(Status::ClassicCandidate { request, projection, held, opening });
    }
    if let Some(request) = cells.get("transfer_request") {
        let Value::Binary(request) = request else {
            return Err(invalid_catalog());
        };
        let request = request
            .as_slice()
            .try_into()
            .map_err(|_| invalid_catalog())?;
        if let Some(managed) = cells.get("transfer_managed") {
            if *managed != Value::Bool(true) {
                return Err(invalid_catalog());
            }
            return decode_managed_transfer(ready, database, request, cells);
        }
        return match (
            ready,
            cells.get("transfer_basis"),
            cells.get("transfer_source"),
            cells.get("transfer_incarnation"),
        ) {
            (true, Some(Value::Binary(basis)), None, None) => Ok(Status::TransferReady {
                request,
                basis: Basis::from_bytes(basis)?,
            }),
            (false, None, Some(Value::Utf8(source)), Some(Value::Uuid(incarnation))) => {
                Ok(Status::TransferPending(Transfer {
                    request,
                    source: source.clone(),
                    incarnation: Uuid::from_bytes(*incarnation),
                    export: decode_transfer_export(cells)?,
                }))
            }
            _ => Err(invalid_catalog()),
        };
    }
    if cells.get("transfer_managed").is_some() {
        return Err(invalid_catalog());
    }
    let retirement = match (
        cells.get("terminal"),
        cells.get("generation"),
        cells.get("destroy_after"),
    ) {
        (None, None, None) if cells.get("confirmed").is_none() => {
            return Ok(if ready {
                Status::Ready
            } else {
                Status::Reserved
            });
        }
        (Some(Value::U64(terminal)), Some(Value::U64(generation)), Some(Value::I64(after)))
            if !ready && *terminal > 0 && *generation > 0 =>
        {
            Retirement {
                terminal: *terminal,
                generation: *generation,
                destroy_after: Timestamp(*after),
            }
        }
        _ => return Err(invalid_catalog()),
    };
    match cells.get("confirmed") {
        None => Ok(Status::Retiring(retirement)),
        Some(Value::I64(confirmed)) if *confirmed >= retirement.destroy_after.0 => {
            Ok(Status::Erased {
                retirement,
                confirmed: Timestamp(*confirmed),
            })
        }
        _ => Err(invalid_catalog()),
    }
}

fn decode_managed_transfer(
    ready: bool, database: [u8; 16], request: [u8; 32], cells: &StructFields<Value>,
) -> Result<Status> {
    let (Some(Value::Utf8(source)), Some(Value::Uuid(incarnation)),
        Some(Value::Uuid(pin)), Some(Value::Binary(basis))) = (
        cells.get("transfer_source"), cells.get("transfer_incarnation"),
        cells.get("transfer_pin"), cells.get("transfer_fork_basis"),
    ) else {
        return Err(invalid_catalog());
    };
    let basis = Basis::from_bytes(basis)?;
    if *incarnation == database || basis.incarnation() != *incarnation || *pin == [0; 16]
        || request == [0; 32] || cells.get("transfer_ticket").is_some()
        || cells.get("transfer_retain_until").is_some()
    {
        return Err(invalid_catalog());
    }
    let manifest: Option<[u8; 32]> = match cells.get("transfer_manifest") {
        None => None,
        Some(Value::Binary(bytes)) => Some(bytes.as_slice().try_into().map_err(|_| invalid_catalog())?),
        _ => return Err(invalid_catalog()),
    };
    if manifest == Some([0; 32]) {
        return Err(invalid_catalog());
    }
    match (ready, cells.get("transfer_reserved_at"), cells.get("transfer_basis"), manifest) {
        (false, None, None, manifest) => Ok(Status::TransferPending(Transfer {
            request, source: source.clone(), incarnation: Uuid::from_bytes(*incarnation),
            export: TransferExport::Managed { pin: Uuid::from_bytes(*pin), basis, manifest },
        })),
        (true, Some(Value::Binary(reserved_at)), Some(Value::Binary(installed)), Some(manifest)) => {
            let reserved_at = Basis::from_bytes(reserved_at)?;
            let installed = Basis::from_bytes(installed)?;
            if reserved_at.position() == 0 || installed != Basis::new(database, basis.position()) {
                return Err(invalid_catalog());
            }
            Ok(Status::ManagedTransferReady {
                transfer: ManagedTransfer {
                    source: source.clone(),
                    reservation: ManagedCheckpointReservation {
                        source: PinnedCheckpointIdentity { basis, pin: *pin, manifest_sha256: manifest, until: None },
                        target: ManagedCheckpointTarget { incarnation: database, request },
                        reserved_at,
                    },
                },
                basis: installed,
            })
        }
        _ => Err(invalid_catalog()),
    }
}

fn decode_transfer_export(cells: &StructFields<Value>) -> Result<TransferExport> {
    let get = |name| cells.get(name);
    match (
        get("transfer_manifest"),
        get("transfer_ticket"),
        get("transfer_pin"),
        get("transfer_fork_basis"),
    ) {
        (Some(Value::Binary(manifest)), Some(Value::Binary(ticket)), None, None) => {
            Ok(TransferExport::Ticket {
                manifest: manifest
                    .as_slice()
                    .try_into()
                    .map_err(|_| invalid_catalog())?,
                ticket: ticket.clone(),
            })
        }
        (None, None, Some(Value::Uuid(pin)), Some(Value::Binary(basis))) => {
            let retain_until = match get("transfer_retain_until") {
                None => None,
                Some(Value::I64(until)) => Some(Timestamp(*until)),
                _ => return Err(invalid_catalog()),
            };
            Ok(TransferExport::Fork {
                pin: Uuid::from_bytes(*pin),
                basis: Basis::from_bytes(basis)?,
                retain_until,
            })
        }
        _ => Err(invalid_catalog()),
    }
}

async fn write(
    catalog: &Catalog,
    control: &mut MultiDatabase,
    name: &str,
    database: Uuid,
    status: Status,
    expect: Version,
) -> Result<()> {
    let id = EntityId::String(name.into());
    let mut cells = StructFields::from([
        (
            "database".into(),
            Cell::Value(Value::Uuid(*database.as_bytes())),
        ),
        ("ready".into(), Cell::Value(Value::Bool(status.ready()))),
    ]);
    let phase = match status {
        Status::Reserved => b"reserve".to_vec(),
        Status::Ready => b"ready".to_vec(),
        Status::ClassicCandidate { request, projection, held, opening } => {
            cells.insert("classic_request".into(), Cell::Value(Value::Binary(request.to_vec())));
            cells.insert("classic_projection".into(), Cell::Value(Value::Binary(projection.to_vec())));
            if let Some(held) = held {
                let bytes = serde_json::to_vec(&held).map_err(|_| invalid_catalog())?;
                cells.insert("classic_held_ready".into(), Cell::Value(Value::Binary(bytes)));
                if let Some(opening) = opening {
                    let phase = match opening.phase {
                        OpeningPhase::Opening { .. } => b"classic-opening".as_slice(),
                        OpeningPhase::Opened { .. } => b"classic-opened".as_slice(),
                        OpeningPhase::Excluded => b"classic-open-excluded".as_slice(),
                    };
                    let bytes = serde_json::to_vec(&opening).map_err(|_| invalid_catalog())?;
                    cells.insert("classic_opening".into(), Cell::Value(Value::Binary(bytes)));
                    phase.to_vec()
                } else {
                    b"classic-held-ready".to_vec()
                }
            } else {
                b"classic-candidate".to_vec()
            }
        }
        Status::TransferPending(transfer) => {
            cells.insert(
                "transfer_request".into(),
                Cell::Value(Value::Binary(transfer.request.to_vec())),
            );
            cells.insert(
                "transfer_source".into(),
                Cell::Value(Value::Utf8(transfer.source)),
            );
            cells.insert(
                "transfer_incarnation".into(),
                Cell::Value(Value::Uuid(*transfer.incarnation.as_bytes())),
            );
            match transfer.export {
                TransferExport::Fork {
                    pin,
                    basis,
                    retain_until,
                } => {
                    cells.insert(
                        "transfer_pin".into(),
                        Cell::Value(Value::Uuid(*pin.as_bytes())),
                    );
                    cells.insert(
                        "transfer_fork_basis".into(),
                        Cell::Value(Value::Binary(basis.to_bytes().to_vec())),
                    );
                    if let Some(until) = retain_until {
                        cells.insert(
                            "transfer_retain_until".into(),
                            Cell::Value(Value::I64(until.0)),
                        );
                    }
                    b"transfer-fork".to_vec()
                }
                TransferExport::Ticket { manifest, ticket } => {
                    cells.insert(
                        "transfer_manifest".into(),
                        Cell::Value(Value::Binary(manifest.to_vec())),
                    );
                    cells.insert("transfer_ticket".into(), Cell::Value(Value::Binary(ticket)));
                    b"transfer-ticket".to_vec()
                }
                TransferExport::Managed { pin, basis, manifest } => {
                    add_managed_export(&mut cells, *pin.as_bytes(), basis, manifest);
                    if manifest.is_some() {
                        b"transfer-managed-bound".to_vec()
                    } else {
                        b"transfer-managed-reserve".to_vec()
                    }
                }
            }
        }
        Status::TransferReady { request, basis } => {
            cells.insert(
                "transfer_request".into(),
                Cell::Value(Value::Binary(request.to_vec())),
            );
            cells.insert(
                "transfer_basis".into(),
                Cell::Value(Value::Binary(basis.to_bytes().to_vec())),
            );
            b"transfer-ready".to_vec()
        }
        Status::ManagedTransferReady { transfer, basis } => {
            let reservation = transfer.reservation;
            cells.insert("transfer_request".into(), Cell::Value(Value::Binary(reservation.target.request.to_vec())));
            cells.insert("transfer_source".into(), Cell::Value(Value::Utf8(transfer.source)));
            cells.insert("transfer_incarnation".into(), Cell::Value(Value::Uuid(reservation.source.basis.incarnation())));
            cells.insert("transfer_reserved_at".into(), Cell::Value(Value::Binary(reservation.reserved_at.to_bytes().to_vec())));
            cells.insert("transfer_basis".into(), Cell::Value(Value::Binary(basis.to_bytes().to_vec())));
            add_managed_export(&mut cells, reservation.source.pin, reservation.source.basis, Some(reservation.source.manifest_sha256));
            b"transfer-managed-ready".to_vec()
        }
        Status::Retiring(retirement) => {
            add_retirement(&mut cells, retirement);
            b"retiring".to_vec()
        }
        Status::Erased {
            retirement,
            confirmed,
        } => {
            add_retirement(&mut cells, retirement);
            cells.insert("confirmed".into(), Cell::Value(Value::I64(confirmed.0)));
            [b"erased".as_slice(), &confirmed.0.to_be_bytes()].concat()
        }
    };
    let mut request = Transact {
        key: identity(&phase, database, name).as_bytes().to_vec(),
        require: Vec::new(),
        window: Window::default(),
        guards: vec![Guard::Version {
            branch: MAIN_BRANCH.into(),
            table: table(),
            id: id.clone(),
            valid_at: None,
            expect,
        }],
        ops: vec![Op::Put {
            branch: MAIN_BRANCH.into(),
            table: table(),
            rows: vec![Row {
                id,
                valid: None,
                custody_group: None,
                protected_columns: Vec::new(),
                custody_payload: None,
                cells,
            }],
            if_absent: false,
        }],
        otherwise: Vec::new(),
        requires: Vec::new(),
        custody_request_digest: None,
        request_memory: None,
    };
    if expect == Version::Absent {
        // A second name could be admitted on another peer after its catalog
        // read. Reserve both identities in the same ordered decision.
        let id = EntityId::Uuid(*database.as_bytes());
        request.key = identity(&[b"claim-incarnation/".as_slice(), &phase].concat(), database, name)
            .as_bytes()
            .to_vec();
        request.guards.push(Guard::Version {
            branch: MAIN_BRANCH.into(),
            table: incarnations(),
            id: id.clone(),
            valid_at: None,
            expect: Version::Absent,
        });
        request.ops.push(Op::Put {
            branch: MAIN_BRANCH.into(),
            table: incarnations(),
            rows: vec![Row {
                id,
                valid: None,
                custody_group: None,
                protected_columns: Vec::new(),
                custody_payload: None,
                cells: StructFields::from([(
                    "deployment".into(),
                    Cell::Value(Value::Utf8(name.into())),
                )]),
            }],
            if_absent: true,
        });
    }
    let receipt = match &catalog.peer {
        Some(peer) => match peer.forward_catalog(catalog.incarnation, &request).await? {
            Some(receipt) => receipt,
            None => control.transact(&request).await?,
        },
        None => control.transact(&request).await?,
    };
    match receipt.outcome {
        Outcome::Then | Outcome::Otherwise { .. } => Ok(()),
        Outcome::StoreDenied | Outcome::StoreWriterClosed { .. } =>
            Err(Anomaly::fault("rtdb/catalog", "A catalog transaction cannot settle a private Store decision")),
        Outcome::Refused { error } => Err(error),
    }
}

fn add_managed_export(cells: &mut StructFields<Cell>, pin: [u8; 16], basis: Basis, manifest: Option<[u8; 32]>) {
    cells.insert("transfer_managed".into(), Cell::Value(Value::Bool(true)));
    cells.insert("transfer_pin".into(), Cell::Value(Value::Uuid(pin)));
    cells.insert("transfer_fork_basis".into(), Cell::Value(Value::Binary(basis.to_bytes().to_vec())));
    if let Some(manifest) = manifest {
        cells.insert("transfer_manifest".into(), Cell::Value(Value::Binary(manifest.to_vec())));
    }
}

fn add_retirement(cells: &mut StructFields<Cell>, retirement: Retirement) {
    cells.insert(
        "terminal".into(),
        Cell::Value(Value::U64(retirement.terminal)),
    );
    cells.insert(
        "generation".into(),
        Cell::Value(Value::U64(retirement.generation)),
    );
    cells.insert(
        "destroy_after".into(),
        Cell::Value(Value::I64(retirement.destroy_after.0)),
    );
}

pub(super) fn closed() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::NotFound,
        "rtdb/deployment-retired",
        "The deployment has permanently closed admission",
    )
}

fn table() -> TableRef {
    TableRef::new("public", "deployments")
}

fn incarnations() -> TableRef {
    TableRef::new("public", "deployment_incarnations")
}

fn check_identity(entry: Deployment, database: Uuid) -> Result<Deployment> {
    if entry.database == database {
        Ok(entry)
    } else {
        Err(conflict())
    }
}

pub(super) fn conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/multi-catalog-conflict",
        "The deployment name or incarnation is already assigned",
    )
}

fn invalid_catalog() -> Anomaly {
    Anomaly::fault(
        "rtdb/multi-catalog-record",
        "The ordered deployment catalog contains an invalid record",
    )
}
