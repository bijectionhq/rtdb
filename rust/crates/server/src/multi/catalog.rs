use std::{collections::BTreeMap, sync::Arc};

use rtdb_crypt::tokens::DeploymentErasureEvidence;
use rtdb_db::{DeploymentRetirement, MultiDatabase};
use rtdb_tx::transact::{Cell, Guard, Op, Outcome, Row, Transact, Version, Window};
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
}

#[derive(Clone, Copy)]
pub(super) struct Deployment {
    pub database: Uuid,
    pub status: Status,
    pub written: Basis,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Status {
    Reserved,
    Ready,
    Retiring(Retirement),
    Erased {
        retirement: Retirement,
        confirmed: Timestamp,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Retirement {
    pub terminal: u64,
    pub generation: u64,
    pub destroy_after: Timestamp,
}

impl Catalog {
    pub(super) fn new(database: MultiDatabase) -> Self {
        let incarnation = Uuid::from_bytes(database.incarnation());
        Self {
            database: Mutex::new(database),
            incarnation,
        }
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
        if database.is_nil() || database == self.incarnation {
            return Err(Anomaly::incorrect(
                "rtdb/multi-catalog-identity",
                "The deployment must use a distinct, nonzero incarnation",
            ));
        }
        let mut control = self.database.lock().await;
        let entries = read(&mut control).await?;
        if let Some(existing) = entries.get(name) {
            return check_identity(*existing, database);
        }
        if entries.values().any(|entry| entry.database == database) {
            return Err(conflict());
        }
        write(
            &mut control,
            name,
            database,
            Status::Reserved,
            Version::Absent,
        )
        .await?;
        let selected = read(&mut control)
            .await?
            .remove(name)
            .ok_or_else(invalid_catalog)?;
        check_identity(selected, database)
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
        if selected.status == Status::Ready {
            write(
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

async fn write(
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
        (
            "ready".into(),
            Cell::Value(Value::Bool(status == Status::Ready)),
        ),
    ]);
    let phase = match status {
        Status::Reserved => b"reserve".to_vec(),
        Status::Ready => b"ready".to_vec(),
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
    let request = Transact {
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
    match control.transact(&request).await?.outcome {
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

fn check_identity(entry: Deployment, database: Uuid) -> Result<Deployment> {
    if entry.database == database {
        Ok(entry)
    } else {
        Err(conflict())
    }
}

fn conflict() -> Anomaly {
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
