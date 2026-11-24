use std::{collections::BTreeMap, sync::Arc};

use rtdb_db::MultiDatabase;
use rtdb_tx::transact::{Cell, Guard, Op, Outcome, Row, Transact, Version, Window};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Result, StructFields, TableRef, Value,
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
    pub ready: bool,
    written: Basis,
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
        write(&mut control, name, database, false, Version::Absent).await?;
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
        if selected.ready {
            return Ok(());
        }
        write(
            &mut control,
            name,
            database,
            true,
            Version::Written(selected.written),
        )
        .await?;
        let ready = read(&mut control)
            .await?
            .remove(name)
            .ok_or_else(invalid_catalog)?;
        if !check_identity(ready, database)?.ready {
            return Err(conflict());
        }
        Ok(())
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
                ready: *ready,
                written: row.cut.basis,
            },
        );
    }
    control.check_generation(generation).await?;
    Ok(entries)
}

async fn write(
    control: &mut MultiDatabase,
    name: &str,
    database: Uuid,
    ready: bool,
    expect: Version,
) -> Result<()> {
    let id = EntityId::String(name.into());
    let phase = if ready {
        b"ready".as_slice()
    } else {
        b"reserve".as_slice()
    };
    let request = Transact {
        key: identity(phase, database, name).as_bytes().to_vec(),
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
                cells: StructFields::from([
                    (
                        "database".into(),
                        Cell::Value(Value::Uuid(*database.as_bytes())),
                    ),
                    ("ready".into(), Cell::Value(Value::Bool(ready))),
                ]),
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
