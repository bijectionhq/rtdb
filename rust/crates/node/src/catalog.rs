use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use rtdb_format::{database::proto::DatabaseConfig, log::proto::resolved_tx::DbOp};
use rtdb_replication::DatabaseEffects;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};

use crate::Database;

#[derive(Clone, Debug)]
pub enum Membership {
    Durable { primary: DatabaseConfig, skipped: BTreeSet<String> },
    Fixed(BTreeMap<String, DatabaseConfig>),
    /// Catalog membership is ordered by the journal runtime. Local attachment follows that order.
    Managed(BTreeMap<String, DatabaseConfig>),
    /// The node holds exactly what a placement authority outside it attaches and detaches locally, and nothing starts open.
    /// Instructions carried by a database's log are refused, as for a fixed catalog.
    Placed,
}

#[derive(Clone)]
enum DriverLifetime { Running, Joined }
#[derive(Clone)]
enum Entry { Open { database: Arc<Database>, driver: DriverLifetime }, Skipped(DatabaseConfig), Detaching(Arc<Database>) }

#[derive(Clone, Default)]
struct DatabaseSet { entries: BTreeMap<String, Entry> }

pub struct DatabaseCatalog {
    value: RwLock<Arc<DatabaseSet>>,
    membership: Membership,
}

impl DatabaseCatalog {
    /// Pins an exact export on the installed database for a trusted control-plane caller.
    #[cfg(feature = "multi")]
    pub async fn export_deployment_checkpoint(
        &self, name: &str, pin: uuid::Uuid, basis: rtdb_types::rtdb_v1::Basis,
        until: Option<rtdb_types::Timestamp>, cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<Vec<u8>> {
        let attachment = self.database(name)?;
        let driver = attachment.serving_driver().ok_or_else(export_unavailable)?;
        let ticket = driver.export_checkpoint(pin, basis, until, cancellation).await?;
        self.with_current_attachment(&attachment, || Ok(ticket))
    }

    /// Restores into an independently provisioned destination and returns its applied basis.
    #[cfg(feature = "multi")]
    pub async fn restore_deployment_checkpoint(
        &self, source: &str, destination: &str, ticket: Vec<u8>,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<rtdb_types::rtdb_v1::Basis> {
        let original = self.database(source)?;
        let restored = self.database(destination)?;
        self.restore_checkpoint(&original, &restored, ticket, cancellation).await
    }

    /// Pins one ordered source cut and creates its copy in an independently provisioned deployment.
    /// The source pin retains its supplied lifetime across retries and destination recovery.
    #[cfg(feature = "multi")]
    pub async fn fork_deployment_checkpoint(
        &self, source: &str, destination: &str, pin: uuid::Uuid, basis: rtdb_types::rtdb_v1::Basis,
        until: Option<rtdb_types::Timestamp>, cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<rtdb_types::rtdb_v1::Basis> {
        let original = self.database(source)?;
        let restored = self.database(destination)?;
        let driver = original.serving_driver().ok_or_else(export_unavailable)?;
        let target = restored.serving_driver().ok_or_else(export_unavailable)?;
        if driver.incarnation() == target.incarnation() {
            return Err(Anomaly::incorrect("rtdb/restore-same-deployment", "Fork needs a distinct deployment"));
        }
        let ticket = driver.export_checkpoint(pin, basis, until, cancellation).await?;
        self.restore_checkpoint(&original, &restored, ticket, cancellation).await
    }

    #[cfg(feature = "multi")]
    pub async fn deployment_checkpoint_source(
        &self, name: &str, cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<Option<rtdb_db::CheckpointSource>> {
        let attachment = self.database(name)?;
        let driver = attachment.serving_driver().ok_or_else(export_unavailable)?;
        let source = driver.checkpoint_source(cancellation).await?;
        self.with_current_attachment(&attachment, || Ok(source))
    }

    #[cfg(feature = "multi")]
    async fn restore_checkpoint(
        &self, original: &Arc<Database>, restored: &Arc<Database>, ticket: Vec<u8>,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<rtdb_types::rtdb_v1::Basis> {
        let source = original.serving_driver().ok_or_else(export_unavailable)?;
        let target = restored.serving_driver().ok_or_else(export_unavailable)?;
        let export = source.open_checkpoint_export(ticket, cancellation).await?;
        self.with_current_attachment(original, || Ok(()))?;
        self.with_current_attachment(restored, || Ok(()))?;
        let cut = target.restore_checkpoint(export, cancellation).await?;
        self.with_current_attachment(restored, || Ok(cut.basis))
    }

    pub(crate) fn new(membership: Membership) -> Arc<Self> {
        Arc::new(Self { value: RwLock::new(Arc::new(DatabaseSet::default())), membership })
    }
    pub fn database_names(&self) -> BTreeSet<String> { self.open_databases().into_keys().collect() }
    /// Whether this node's databases are tenants placed by an outside authority, each visible only to its own sessions.
    pub fn is_placed(&self) -> bool { matches!(self.membership, Membership::Placed) }
    pub fn is_managed(&self) -> bool { matches!(self.membership, Membership::Managed(_)) }
    pub fn database(&self, name: &str) -> Result<Arc<Database>> {
        match self.value.read().entries.get(name) {
            Some(Entry::Open { database, .. }) => Ok(Arc::clone(database)),
            _ => Err(Anomaly::incorrect("xtdb/unknown-db", format!("Unknown database: {name}")).with_context("db-name", name.into())),
        }
    }
    pub fn open_databases(&self) -> BTreeMap<String, Arc<Database>> {
        self.value.read().entries.iter().filter_map(|(name, entry)| match entry { Entry::Open { database, .. } => Some((name.clone(), Arc::clone(database))), _ => None }).collect()
    }
    /// Captures open database owners under one catalog read and reserves the result container before allocation.
    /// Returned database owners and the reservation remain live through every clone of the snapshot.
    pub fn open_database_snapshot(&self, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<rtdb_plan::CatalogRows<Arc<Database>>> {
        let value = self.value.read();
        let bound = value.entries.len().checked_mul(std::mem::size_of::<Arc<Database>>() * 2).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| Anomaly::incorrect("rtdb.node/catalog-allocation", "Database snapshot exceeds addressable memory"))?;
        let reservation = memory.reserve(bound)?;
        let mut databases = Vec::with_capacity(value.entries.len());
        for entry in value.entries.values() { if let Entry::Open { database, .. } = entry { databases.push(Arc::clone(database)); } }
        Ok(rtdb_plan::CatalogRows::new(databases, reservation))
    }
    pub fn secondary_databases(&self) -> BTreeMap<String, DatabaseConfig> {
        if matches!(self.membership, Membership::Fixed(_) | Membership::Managed(_) | Membership::Placed) { return BTreeMap::new(); }
        self.value.read().entries.iter().filter_map(|(name, entry)| {
            if name == "xtdb" { return None; }
            let configuration = match entry { Entry::Open { database, .. } => database.configuration(), Entry::Skipped(configuration) => configuration, Entry::Detaching(_) => return None };
            Some((name.clone(), configuration.clone()))
        }).collect()
    }
    pub(crate) fn skipped(&self, name: &str) -> bool {
        matches!(&self.membership, Membership::Durable { skipped, .. } if skipped.contains(name))
    }
    pub fn check_attach(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> {
        self.require_mutable()?;
        self.check_new_name(name, configuration)
    }
    /// Whether a name is still occupied by a database whose detach is draining it.
    pub fn is_draining(&self, name: &str) -> bool { matches!(self.value.read().entries.get(name), Some(Entry::Detaching(_))) }
    pub(crate) fn check_local_attach(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> {
        if !matches!(self.membership, Membership::Placed | Membership::Managed(_)) { self.require_mutable()?; }
        self.check_new_name(name, configuration)
    }
    fn check_new_name(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> {
        if let Some(entry) = self.value.read().entries.get(name) {
            return Err(match entry {
                Entry::Detaching(_) => Anomaly::new(AnomalyCategory::Conflict, "xtdb/db-being-detached", "Database is still being detached — retry once the previous detach has completed"),
                _ => Anomaly::new(AnomalyCategory::Conflict, "xtdb/db-exists", "Database already exists"),
            }.with_context("db-name", name.into()));
        }
        crate::resources::validate_database_shape(name, configuration)
    }
    pub fn check_detach(&self, name: &str) -> Result<()> {
        self.require_mutable()?;
        if name == "xtdb" { return Err(Anomaly::incorrect("xtdb/cannot-detach-primary", "Cannot detach the primary 'xtdb' database").with_context("db-name", name.into())); }
        match self.value.read().entries.get(name) {
            Some(Entry::Open { .. } | Entry::Skipped(_)) => Ok(()),
            _ => Err(no_database(name)),
        }
    }
    pub(crate) fn with_current_attachment<T>(&self, attachment: &Arc<Database>, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        let current = self.value.read();
        if !matches!(current.entries.get(attachment.name()), Some(Entry::Open { database, driver: DriverLifetime::Running })
            if Arc::ptr_eq(database, attachment))
        {
            return Err(no_database(attachment.name()));
        }
        operation()
    }

    fn require_mutable(&self) -> Result<()> {
        if !matches!(self.membership, Membership::Durable { .. }) { return Err(Anomaly::unsupported("xtdb/fixed-database-catalog", "This catalog does not accept membership instructions from a database")); }
        Ok(())
    }
    pub(crate) fn insert_open(&self, database: Arc<Database>) -> Result<()> {
        self.check_new_name(database.name(), database.configuration())?;
        self.insert(database.name().to_owned(), Entry::Open { database, driver: DriverLifetime::Running })
    }
    pub(crate) fn insert_skipped(&self, name: String, configuration: DatabaseConfig) -> Result<()> {
        self.check_new_name(&name, &configuration)?;
        self.insert(name, Entry::Skipped(configuration))
    }
    fn insert(&self, name: String, entry: Entry) -> Result<()> {
        let mut value = self.value.write();
        if value.entries.contains_key(&name) { return Err(Anomaly::fault("rtdb.node/catalog-writer", "Database name was inserted outside the catalog owner's order")); }
        let mut next = value.as_ref().clone();
        next.entries.insert(name, entry);
        *value = Arc::new(next);
        Ok(())
    }
    pub(crate) fn begin_detach(&self, name: &str) -> Result<Option<Arc<Database>>> {
        self.check_detach(name)?;
        self.remove_for_detach(name)
    }
    pub(crate) fn begin_local_detach(&self, name: &str) -> Result<Option<Arc<Database>>> {
        if !matches!(self.membership, Membership::Placed | Membership::Managed(_)) { return Err(Anomaly::unsupported("rtdb.node/local-detach", "This catalog does not support local detach")); }
        self.remove_for_detach(name)
    }
    fn remove_for_detach(&self, name: &str) -> Result<Option<Arc<Database>>> {
        let mut value = self.value.write();
        let mut next = value.as_ref().clone();
        let (cancel, drain) = match next.entries.remove(name) {
            Some(Entry::Open { database, driver }) => {
                next.entries.insert(name.into(), Entry::Detaching(Arc::clone(&database)));
                (Some(database), matches!(driver, DriverLifetime::Joined))
            }
            Some(Entry::Skipped(_)) => (None, false),
            _ => return Err(no_database(name)),
        };
        *value = Arc::new(next);
        drop(value);
        if let Some(database) = &cancel { database.cancel(); }
        Ok(if drain { cancel } else { None })
    }
    pub(crate) fn driver_finished(&self, database: &Arc<Database>) -> bool {
        let mut value = self.value.write();
        let mut next = value.as_ref().clone();
        match next.entries.get(database.name()) {
            Some(Entry::Detaching(detaching)) if Arc::ptr_eq(detaching, database) => return true,
            Some(Entry::Open { database: open, .. }) if Arc::ptr_eq(open, database) => {
                next.entries.insert(database.name().into(), Entry::Open { database: Arc::clone(database), driver: DriverLifetime::Joined });
            }
            _ => return false,
        }
        *value = Arc::new(next);
        false
    }
    pub(crate) fn teardown_finished(&self, database: &Arc<Database>) {
        let mut value = self.value.write();
        if !matches!(value.entries.get(database.name()), Some(Entry::Detaching(detaching)) if Arc::ptr_eq(detaching, database)) { return; }
        let mut next = value.as_ref().clone();
        next.entries.remove(database.name());
        *value = Arc::new(next);
    }
    pub(crate) fn held_databases(&self) -> Vec<Arc<Database>> {
        self.value.read().entries.values().filter_map(|entry| match entry {
            Entry::Open { database, .. } | Entry::Detaching(database) => Some(Arc::clone(database)),
            Entry::Skipped(_) => None,
        }).collect()
    }
    pub(crate) fn cancel_all(&self) {
        for entry in self.value.read().entries.values() {
            if let Entry::Open { database, .. } | Entry::Detaching(database) = entry { database.cancel(); }
        }
    }
    pub(crate) fn clear_after_join(&self) { *self.value.write() = Arc::new(DatabaseSet::default()); }
}

pub(crate) enum CatalogCommand {
    Apply { operation: DbOp, completion: oneshot::Sender<Result<()>> },
    AttachLocal { name: String, configuration: DatabaseConfig, slot: Option<OwnedSemaphorePermit>, completion: oneshot::Sender<Result<Option<Arc<Database>>>> },
    DetachLocal { name: String, completion: oneshot::Sender<Result<()>> },
}

pub(crate) struct CatalogEffects { pub catalog: Arc<DatabaseCatalog>, pub commands: mpsc::Sender<CatalogCommand>, pub primary: bool }

#[async_trait]
impl DatabaseEffects for CatalogEffects {
    fn check_db_op(&self, operation: &DbOp) -> Result<()> {
        if !self.primary { return Ok(()); }
        match operation {
            DbOp::AttachDatabase(attach) => self.catalog.check_attach(attach.db_name.as_deref().unwrap_or_default(), attach.config.as_ref().unwrap_or(&DatabaseConfig::default())),
            DbOp::DetachDatabase(detach) => self.catalog.check_detach(detach.db_name.as_deref().unwrap_or_default()),
        }
    }
    async fn apply_db_op(&self, operation: &DbOp) -> Result<()> {
        if !self.primary { return Ok(()); }
        let (completion, result) = oneshot::channel();
        self.commands.send(CatalogCommand::Apply { operation: operation.clone(), completion }).await.map_err(|_| closed())?;
        result.await.map_err(|_| closed())?
    }
    fn secondary_databases(&self) -> Result<BTreeMap<String, DatabaseConfig>> {
        Ok(if self.primary { self.catalog.secondary_databases() } else { BTreeMap::new() })
    }
}

fn no_database(name: &str) -> Anomaly { Anomaly::new(AnomalyCategory::NotFound, "xtdb/no-such-db", "Database does not exist").with_context("db-name", name.into()) }
fn closed() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/node-closed", "Node is closed") }

#[cfg(feature = "multi")]
fn export_unavailable() -> Anomaly {
    Anomaly::unsupported("rtdb/export-checkpoint", "The deployment has no checkpoint export driver")
}
