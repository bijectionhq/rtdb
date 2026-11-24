use std::{collections::VecDeque, sync::Arc, time::Duration};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_query::QueryContext;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use uuid::Uuid;

const CAPACITY: usize = 128;
const HOLD_FOR: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(crate) struct SqlSnapshots {
    entries: parking_lot::Mutex<VecDeque<(Uuid, Arc<SqlSnapshot>)>>,
}

pub(crate) struct SqlSnapshot {
    pub(crate) owner: Option<Arc<str>>,
    pub(crate) view: rtdb_query::SqlSnapshotView,
    expires: tokio::time::Instant,
    _charge: Arc<MemoryReservation>,
}

impl SqlSnapshots {
    pub(crate) fn issue(self: &Arc<Self>, owner: Option<Arc<str>>, view: rtdb_query::SqlSnapshotView, memory: &Arc<MemoryBudget>) -> Result<String> {
        let bytes = view.retained_bytes()?.checked_add(owner.as_ref().map_or(0, |owner| owner.len()) + 256).ok_or_else(unretained)?;
        let charge = memory.reserve(bytes)?;
        let id = Uuid::new_v4();
        let expires = tokio::time::Instant::now() + HOLD_FOR;
        let entry = Arc::new(SqlSnapshot { owner, view, expires, _charge: charge });
        let evicted = {
            let mut entries = self.entries.lock();
            let evicted = (entries.len() >= CAPACITY).then(|| entries.pop_front()).flatten();
            entries.push_back((id, entry));
            evicted
        };
        drop(evicted);
        Ok(format!("rtdb-snapshot:{id}"))
    }

    pub(crate) fn start(self: &Arc<Self>, cancellation: tokio_util::sync::CancellationToken) {
        let registry = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = ticks.tick() => {}
                }
                let Some(registry) = registry.upgrade() else { break; };
                let now = tokio::time::Instant::now();
                loop {
                    let entry = {
                        let mut entries = registry.entries.lock();
                        if entries.front().is_some_and(|(_, entry)| entry.expires <= now) {
                            entries.pop_front()
                        } else {
                            None
                        }
                    };
                    if entry.is_none() { break; }
                }
            }
        });
    }

    pub(crate) fn select(&self, token: &str, owner: Option<&str>, database: &str) -> Result<Arc<SqlSnapshot>> {
        let id = token.strip_prefix("rtdb-snapshot:").filter(|suffix| suffix.len() == 36)
            .and_then(|suffix| Uuid::parse_str(suffix).ok()).ok_or_else(unretained)?;
        self.entries.lock().iter().find_map(|(key, entry)| {
            (*key == id && entry.owner.as_deref() == owner && entry.view.database() == database
                && tokio::time::Instant::now() < entry.expires)
                .then(|| Arc::clone(entry))
        }).ok_or_else(unretained)
    }

    pub(crate) fn with_issuer(self: &Arc<Self>, context: Arc<QueryContext>, owner: Option<&str>, memory: &Arc<MemoryBudget>) -> Arc<QueryContext> {
        let registry = Arc::downgrade(self);
        let owner = owner.map(Arc::<str>::from);
        let memory = Arc::downgrade(memory);
        context.with_snapshot_token_issuer(Arc::new(move |view| {
            let registry = registry.upgrade().ok_or_else(unretained)?;
            let memory = memory.upgrade().ok_or_else(unretained)?;
            registry.issue(owner.clone(), view, &memory)
        }))
    }
}

pub(crate) fn unretained() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/snapshot-unretained", "The exact SQL snapshot cannot be readmitted")
}

pub(crate) struct SqlSnapshotAdmission {
    pub(crate) node: crate::Node,
    pub(crate) registry: Arc<SqlSnapshots>,
    pub(crate) identity: Arc<dyn crate::SessionIdentity>,
}

#[async_trait::async_trait]
impl rtdb_query::SnapshotAdmission for SqlSnapshotAdmission {
    async fn reopen(&self, token: &str, current: &QueryContext) -> Result<rtdb_query::SqlSnapshotView> {
        use rtdb_plan::Catalog;
        let retained = self.registry.select(token, self.identity.user(), current.catalog.default_database())?;
        if current.timezone != retained.view.timezone {
            return Err(unretained());
        }
        self.node.admit_retained_catalog(&retained.view.catalog, self.identity.as_ref(), &current.cancellation).await?;
        Ok(retained.view.clone())
    }
}
