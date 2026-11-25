use std::sync::Arc;
#[cfg(feature = "multi")]
use std::time::Duration;

use rtdb_node::Database;
use rtdb_query::QueryContext;
use rtdb_types::{Timestamp, rtdb_v1::Basis};
use tokio::{sync::OwnedSemaphorePermit, time::Instant};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use uuid::Uuid;

use super::{error, proto};
use crate::{
    lease::{Leased, Lookup},
    service::{Call, Service},
};

pub(super) const FEATURE: &str = "query.exact-scope";
pub(crate) const OPEN: &str = "rtdb.v1.OpenExactRead";
pub(crate) const CLOSE: &str = "rtdb.v1.CloseExactRead";
#[cfg(feature = "multi")]
const MAX_TTL_MILLIS: u32 = 2_000;

pub(crate) struct ExactRead {
    pub(crate) session: String,
    database: Arc<Database>,
    pub(super) basis: Basis,
    pub(super) time: Timestamp,
    pub(super) generation: u64,
    pub(super) assignment: proto::FollowerProgress,
    track: bool,
    context: Arc<QueryContext>,
    deadline: Instant,
    cancellation: CancellationToken,
    _permit: OwnedSemaphorePermit,
}

impl ExactRead {
    pub(super) fn check(&self) -> Result<(), Status> {
        if self.cancellation.is_cancelled() || Instant::now() >= self.deadline {
            return Err(expired());
        }
        self.context.check_cancelled().map_err(error::anomaly)
    }

    pub(super) fn page_context(
        &self,
        basis: Basis,
        track: bool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<QueryContext>, Status> {
        self.require_cut(basis, track)?;
        Ok(self.context.for_wire_scope_page(cancellation))
    }

    pub(super) fn require_cut(&self, basis: Basis, track: bool) -> Result<(), Status> {
        self.check()?;
        if self.basis != basis || self.track != track {
            return Err(error::incorrect(
                "Query does not match the exact read scope",
            ));
        }
        Ok(())
    }
}

impl Leased for ExactRead {
    fn close(&self) {
        self.cancellation.cancel();
    }

    fn hard_deadline(&self) -> Option<Instant> {
        Some(self.deadline)
    }
}

impl Drop for ExactRead {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl Service {
    #[cfg(not(feature = "multi"))]
    pub(super) async fn open_exact_read(
        &self,
        _: &Call,
        _: proto::OpenExactReadRequest,
    ) -> Result<proto::OpenExactReadResponse, Status> {
        Err(error::unsupported(FEATURE))
    }

    #[cfg(feature = "multi")]
    pub(super) async fn open_exact_read(
        &self,
        call: &Call,
        request: proto::OpenExactReadRequest,
    ) -> Result<proto::OpenExactReadResponse, Status> {
        use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryOptions};

        let route = self.follower().ok_or_else(|| error::unsupported(FEATURE))?;
        if !(1..=MAX_TTL_MILLIS).contains(&request.ttl_millis) {
            return Err(error::incorrect(
                "Exact read TTL must be between 1 and 2000 milliseconds",
            ));
        }
        let basis = request
            .basis
            .ok_or_else(|| error::incorrect("Exact read requires a Basis"))?;
        let ttl = Duration::from_millis(u64::from(request.ttl_millis));
        let deadline = Instant::now() + ttl;
        let permit = Arc::clone(&self.0.owned_read_slots)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("All exact read scopes are in use"))?;
        let cancellation = self.0.cancellation.child_token();
        let guard = cancellation.clone().drop_guard();
        let basis = Basis::from_bytes(&basis.value).map_err(error::anomaly)?;
        let (database, cut, generation, context) = tokio::time::timeout_at(deadline, async {
            let database = route.current(&self.0.node).map_err(error::follower)?;
            let view = route
                .reader
                .retained_view(basis, self.0.node.now(), &call.cancellation)
                .await
                .map_err(error::follower)?;
            if view.cut.basis != basis {
                return Err(expired());
            }
            let snapshot = rtdb_query::retain_catalog_snapshot(view.snapshot, database.storage())
                .await
                .map_err(error::anomaly)?;
            let time = view.cut.system_time;
            let captured = DatabaseSnapshot {
                partitions: vec![snapshot],
                storage: Arc::clone(database.storage()),
            };
            let name = call.database.clone();
            let catalog = CatalogSnapshot::new(
                name.clone(),
                std::collections::BTreeMap::from([(name, captured)]),
                false,
            )
            .map_err(error::anomaly)?;
            let context = QueryContext::new(
                catalog,
                Arc::clone(call.memory()),
                time,
                "UTC",
                QueryOptions::default(),
                &cancellation,
            )
            .map_err(error::anomaly)?
            .with_wire_cut(time, time.into())
            .map_err(error::anomaly)?;
            context
                .retain_wire_scope(deadline.into_std())
                .await
                .map_err(error::anomaly)?;
            self.check_attachment(call, &database)?;
            let generation = view.key_generation.ok_or_else(expired)?;
            route
                .reader
                .check_retained_delivery(generation, &call.cancellation)
                .await
                .map_err(error::follower)?;
            Ok((database, view.cut, generation, context))
        })
        .await
        .map_err(|_| expired())??;

        let scope = Uuid::new_v4();
        let read = Arc::new(ExactRead {
            session: call.session.id.clone(),
            database,
            basis,
            time: cut.system_time,
            generation,
            assignment: super::query::follower_assignment(route),
            track: request.track,
            context,
            deadline,
            cancellation,
            _permit: permit,
        });
        call.session.register_read(|| {
            read.check()?;
            self.check_attachment(call, &read.database)?;
            self.0
                .exact_reads
                .insert(scope, read, ttl, ttl, false, 16)
                .map_err(error::anomaly)
        })?;
        drop(guard.disarm());
        Ok(proto::OpenExactReadResponse {
            scope: scope.as_bytes().to_vec(),
        })
    }

    pub(super) fn exact_read(
        &self,
        call: &Call,
        database: &Arc<Database>,
        handle: &[u8],
    ) -> Result<Arc<ExactRead>, Status> {
        let scope = read_handle(handle)?;
        match self.0.exact_reads.lookup(&scope, false) {
            Lookup::Found(read)
                if read.session == call.session.id && Arc::ptr_eq(&read.database, database) =>
            {
                read.check()?;
                Ok(read)
            }
            Lookup::Found(_) | Lookup::Expired(_) | Lookup::Unknown => Err(expired()),
        }
    }

    pub(super) fn close_exact_read(&self, call: &Call, handle: &[u8]) -> Result<Vec<u8>, Status> {
        if !self.follower_only() {
            return Err(error::unsupported(FEATURE));
        }
        let scope = read_handle(handle)?;
        if let Lookup::Found(read) = self.0.exact_reads.lookup(&scope, false) {
            if read.session != call.session.id {
                return Err(expired());
            }
            if let Lookup::Found(closed) = self.0.exact_reads.take(&scope) {
                closed.close();
            }
        }
        Ok(Vec::new())
    }
}

fn read_handle(handle: &[u8]) -> Result<Uuid, Status> {
    Uuid::from_slice(handle).map_err(|_| error::incorrect("Invalid exact read scope"))
}

fn expired() -> Status {
    error::not_found(
        "rtdb/exact-scope-expired",
        "Open a retained exact read scope",
    )
}
