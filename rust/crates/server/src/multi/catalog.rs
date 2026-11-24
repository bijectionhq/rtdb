use std::{collections::BTreeMap, sync::Arc};

use rtdb_crypt::tokens::DeploymentErasureEvidence;
use rtdb_db::{DeploymentRetirement, MultiDatabase};
use rtdb_tx::transact::{Cell, Guard, Op, Outcome, Receipt, Row, Transact, Version, Window};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Result, StructFields, TableRef, Timestamp, Value,
    rtdb_v1::{At, Basis, Layer, MAIN_BRANCH},
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

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
    TransferPending(Transfer),
    TransferReady {
        request: [u8; 32],
        basis: Basis,
    },
    Retiring(Retirement),
    Erased {
        retirement: Retirement,
        confirmed: Timestamp,
    },
}

impl Status {
    pub fn ready(&self) -> bool {
        matches!(self, Self::Ready | Self::TransferReady { .. })
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

    pub(super) fn deployment_id(&self, name: &str) -> Uuid {
        identity(b"deployment", self.incarnation, name)
    }

    pub(super) async fn tick(
        &self,
        live: &std::collections::BTreeSet<std::net::IpAddr>,
    ) -> Result<rtdb_db::Role> {
        self.database.lock().await.tick(live).await
    }

    pub(super) async fn read(&self) -> Result<BTreeMap<String, Deployment>> {
        read(&mut *self.database.lock().await).await
    }

    /// Selects a stable incarnation before any provisioner opens its data stream.
    pub(super) async fn reserve(&self, name: &str, database: Uuid) -> Result<Deployment> {
        self.reserve_status(name, database, Status::Reserved).await
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
        if let TransferExport::Ticket {
            manifest: prior, ..
        } = pending.export
        {
            return if prior == manifest {
                Ok(selected.status)
            } else {
                Err(conflict())
            };
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
        let (Some(Value::Uuid(database)), Some(Value::Bool(ready))) =
            (image.cells.get("database"), image.cells.get("ready"))
        else {
            return Err(invalid_catalog());
        };
        entries.insert(
            name.clone(),
            Deployment {
                database: Uuid::from_bytes(*database),
                status: decode_status(*ready, &image.cells)?,
                written: row.cut.basis,
            },
        );
    }
    control.check_generation(generation).await?;
    Ok(entries)
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

fn decode_status(ready: bool, cells: &StructFields<Value>) -> Result<Status> {
    if let Some(request) = cells.get("transfer_request") {
        let Value::Binary(request) = request else {
            return Err(invalid_catalog());
        };
        let request = request
            .as_slice()
            .try_into()
            .map_err(|_| invalid_catalog())?;
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
    let receipt = if let Some(peer) = &catalog.peer
        && let Some(receipt) = peer.forward_catalog(catalog.incarnation, &request).await?
    {
        receipt
    } else {
        control.transact(&request).await?
    };
    match receipt.outcome {
        Outcome::Then | Outcome::Otherwise { .. } => Ok(()),
        Outcome::Refused { error } => Err(error),
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
