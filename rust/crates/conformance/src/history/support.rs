use async_trait::async_trait;
use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
use rtdb_maintenance::CollectionObserver;
use rtdb_node::{Clock, DatabaseAccess, Membership, NativeResources, Node, NodeOptions, NodeServices, ResourceFactory, SessionIdentity};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_tx::TransactionAuthorization;
use rtdb_types::{Anomaly, Result, TableRef, Timestamp};
use std::{path::Path, sync::{Arc, atomic::{AtomicI64, Ordering}}, time::Duration};
use super::{Error, store::{CountingResources, Reads}};

pub(super) struct Identity;
impl TransactionAuthorization for Identity { fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) } }
#[async_trait]
impl SessionIdentity for Identity {
    fn user(&self) -> Option<&str> { Some("xtdb") }
    async fn revalidate(&self, _: Timestamp) -> Result<()> { Ok(()) }
    fn authorize_database(&self, _: &str, _: DatabaseAccess) -> Result<()> { Ok(()) }
}
struct Catalog;
impl CatalogUsers for Catalog {
    fn users(&self) -> Result<CatalogRows<CatalogUser>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) }
    fn is_superuser(&self, _: &str) -> bool { true }
}
impl CatalogMetrics for Catalog { fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) } }
#[derive(Default)]
struct Observer { failure: parking_lot::Mutex<Option<Anomaly>> }
impl CollectionObserver for Observer {
    fn table_failed(&self, _: Option<&TableRef>, error: &Anomaly) { self.failure.lock().get_or_insert_with(|| error.clone()); }
}
pub(super) struct FixtureClock(pub AtomicI64);
impl Clock for FixtureClock { fn now(&self) -> Timestamp { Timestamp(self.0.load(Ordering::SeqCst)) } }

pub(super) struct RunningNode { pub node: Node, owner: tokio::task::JoinHandle<Result<()>>, observer: Arc<Observer> }
impl RunningNode {
    pub async fn open(directory: &Path, clock: Arc<FixtureClock>, reads: Arc<Reads>) -> std::result::Result<Self, Error> {
        let resources = Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false));
        Self::open_with_resources(directory, clock, reads, resources, false).await
    }
    pub async fn open_with_resources(directory: &Path, clock: Arc<FixtureClock>, reads: Arc<Reads>, resources: Arc<dyn ResourceFactory>, read_only: bool) -> std::result::Result<Self, Error> {
        Self::open_configured(directory, clock, reads, resources, read_only, |_| {}).await
    }
    pub(super) async fn open_configured(directory: &Path, clock: Arc<FixtureClock>, reads: Arc<Reads>, resources: Arc<dyn ResourceFactory>, read_only: bool, configure: impl FnOnce(&mut NodeOptions)) -> std::result::Result<Self, Error> {
        let mut options = NodeOptions::default();
        options.read_only = read_only;
        options.replication.may_lead = !read_only;
        options.membership = Membership::Durable { primary: DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.join("log").to_string_lossy().into_owned()), epoch: None })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(directory.join("storage").to_string_lossy().into_owned()), epoch: None })), ..Default::default()
        }, skipped: Default::default() };
        options.automatic_compaction = false; options.automatic_gc = false;
        options.compaction_threads = 1;
        options.replication.assertion_interval = Duration::from_millis(10);
        options.replication.flush_timeout = Duration::from_secs(24 * 60 * 60);
        options.replication.election_seed = 42;
        configure(&mut options);
        let resources = Arc::new(CountingResources { inner: resources, reads });
        let observer = Arc::new(Observer::default());
        let services = NodeServices { resources, clock,
            ..NodeServices::local(Arc::new(Identity), observer.clone(), Arc::new(Catalog), Arc::new(Catalog)) };
        let (node, driver) = Node::open(options, services).await?;
        Ok(Self { node, owner: tokio::spawn(driver.run()), observer })
    }
    pub async fn close(self) -> std::result::Result<(), Error> {
        self.node.shutdown();
        let joined = match self.owner.await { Ok(result) => result.map_err(Error::from), Err(error) => Err(Error::from(error)) };
        let observed = self.observer.failure.lock().take().map_or(Ok(()), |error| Err(Error::from(error)));
        super::combine(joined, observed)
    }
}
