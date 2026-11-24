use std::sync::Arc;

use rtdb_columnar::ArrowCell;
use rtdb_plan::Catalog;
use rtdb_query::{HostedControlPoint, QueryCursor};
use rtdb_tx::HostedControlWrite;
use rtdb_types::{
    ASSERTION_FAMILY_FENCES, Anomaly, AnomalyCategory, AssertionFamilyFenceMarker, EntityId, Iid,
    OwnedWorld, PlacedAttempt, PlacedAttemptScope, Result, TableRef, TransactionKey,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    AppliedReadFloor, CatalogReadCut, Database, NativeSession, Node, OwnedReadSession, QueryBasis, SessionIdentity, TimeBasis,
    covers_transaction, hosted_source_unavailable, prepare_query,
};

mod retired_original;
pub use retired_original::{RetiredOriginalFamilyRead, RetiredOriginalFamilySource};

const INSTALLATION: &str = "bj_installation";
const RESTORE: &str = "bj_rtdb_restore_hold";
const ARCHIVE_FENCE: &str = "archive-source-fence.v1";

#[derive(Clone, Copy)]
pub enum RootCatalogRead {
    Installation,
    RestoreHold,
    RestoreRelease,
    Custody,
    Owner,
    Admission,
    ProgramSchema,
    Declarations,
}

impl RootCatalogRead {
    pub fn relation(self) -> &'static str {
        match self {
            Self::Installation => "bj_installation",
            Self::RestoreHold | Self::RestoreRelease => "bj_rtdb_restore_hold",
            Self::Custody => "bj_rtdb_custody_binding",
            Self::Owner => "bj_fact_root_owner",
            Self::Admission => "bj_fact_root_admission",
            Self::ProgramSchema => "bj_fact_root_program",
            Self::Declarations => "bj_relation_declarations",
        }
    }

    pub fn maximum_rows(self) -> usize {
        match self {
            Self::Declarations => 128,
            _ => 1,
        }
    }

    fn query(self) -> String {
        let (columns, condition) = match self {
            Self::Installation => ("incarnation, kind, writer, _system_from", "_id = 'authority'"),
            Self::RestoreHold => ("binding", "_id = 'restore'"),
            Self::RestoreRelease => ("_system_from", "_id = 'released'"),
            Self::Custody => ("fingerprint", "_id = 'authority'"),
            Self::Owner => ("layout_digest, audience_tablet, _system_from", "_id = 'root'"),
            Self::Admission => ("admission, _system_from", "_id = 'application'"),
            Self::ProgramSchema => (
                "_id, generation, artifact, closure_digest, closure, application_schema, source_reference, environment_contract, _system_from",
                "_id = 'active'",
            ),
            Self::Declarations => ("_id, table_name, world, declaration, _system_from, names", "world = 0"),
        };
        format!("SELECT {columns} FROM public.{} WHERE {condition} LIMIT {}", self.relation(), self.maximum_rows() + 1)
    }
}

impl OwnedReadSession {
    pub(super) async fn original_inventory_provider_guard(
        &self, scope: PlacedAttemptScope,
    ) -> Result<impl Fn() -> Result<()> + Send + Sync + use<>> {
        let unavailable = hosted_source_unavailable;
        if self.witness_scope != Some(scope) { return Err(unavailable()); }
        let catalog = self.context.catalog.database(&self.database)?;
        let [held] = catalog.partitions.as_slice() else { return Err(unavailable()); };
        let installation = TableRef::new("public", INSTALLATION);
        let restore = TableRef::new("public", RESTORE);
        let expected = [held.relation_event_frontier(&installation)?, held.relation_event_frontier(&restore)?];
        let point = read_carrier_point(&self.context, INSTALLATION, "authority").await?;
        let (row, revision) = point.row.ok_or_else(unavailable)?;
        let row = row.batch();
        let field = |name| ArrowCell::named(row, name, 0);
        if revision.0 <= 0
            || field("kind").and_then(ArrowCell::utf8) != Some("hosted")
            || !field("incarnation").and_then(ArrowCell::binary).is_some_and(|id| id.len() == 16 && id.iter().any(|&byte| byte != 0))
            || !field("writer").and_then(ArrowCell::utf8).is_some_and(|id|
                id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()))
        {
            return Err(unavailable());
        }
        for marker in ["restore", "released", ARCHIVE_FENCE] {
            if rtdb_query::read_hosted_control_point(
                Arc::clone(&self.context), restore.clone(), &EntityId::String(marker.into()), false,
            ).await?.row.is_some() {
                return Err(unavailable());
            }
        }
        let receipt = self.node.await_durable_source(&self.database, self.identity.as_ref(), true,
            Some(&self.attachment), &self.cancellation).await?.ok_or_else(unavailable)?;
        let applied = receipt.applied.filter(|applied| covers_transaction(*applied, self.present))
            .ok_or_else(unavailable)?;
        let node = self.node.clone();
        let database = self.database.clone();
        let attachment = self.attachment.clone();
        let guard = move || {
            let attached = node.attached_database(&database, Some(&attachment))?;
            let current = attached.snapshot();
            if attached.placed_writer_scope() != Some(scope)
                || !current.tx_basis().is_some_and(|tip| covers_transaction(tip, applied))
                || [current.relation_event_frontier(&installation)?, current.relation_event_frontier(&restore)?] != expected
            {
                return Err(unavailable());
            }
            Ok(())
        };
        guard()?;
        Ok(guard)
    }

    pub async fn open_root_catalog(&self, read: RootCatalogRead) -> Result<QueryCursor> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let options = rtdb_sql::PrepareOptions {
            default_timezone: self.context.timezone.clone(),
            ..Default::default()
        };
        let cursor = prepare_query(&read.query(), &self.context, &options)?
            .execute(Arc::clone(&self.context), Vec::new()).await?;
        self.check()?;
        Ok(cursor)
    }

    /// Read an admitted control on the original hold or one of its public prefixes.
    pub async fn hosted_control_point(
        &self,
        target: &HostedControlWrite,
        at: Option<rtdb_types::TransactionKey>,
    ) -> Result<HostedControlPoint> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let unavailable = || {
            Anomaly::new(
                AnomalyCategory::Forbidden,
                "rtdb.hosted/control-scope",
                "Hosted controls require their original live placed hold",
            )
        };
        if Some(target.scope()) != self.witness_scope.or(self.applied_scope) {
            return Err(unavailable());
        }
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(unavailable());
        };
        if snapshot.tx_basis() != Some(self.present) {
            return Err(unavailable());
        }
        let context = if let Some(at) = at {
            let marker = rtdb_types::OwnedRelation::new(rtdb_types::OWNED_PUBLIC_ROOT_VERSIONS_RELATION)?;
            if self.lookup_publication(&marker, super::OwnedPublicationLookup::Position(at)).await?.is_none() {
                return Err(unavailable());
            }
            self.context.with_owned_transaction_cut(at, &self.context.cancellation)?
        } else {
            Arc::clone(&self.context)
        };
        let limit = at.unwrap_or(self.present).system_time;
        let point = rtdb_query::read_hosted_control_point(
            context,
            target.table(),
            target.id(),
            true,
        )
        .await?;
        if point
            .row
            .as_ref()
            .is_some_and(|(_, revision)| *revision > limit)
        {
            return Err(unavailable());
        }
        self.check()?;
        Ok(point)
    }
}

pub struct CurrentAssertionFamilyRead {
    context: Arc<rtdb_query::QueryContext>,
    owner: String,
    applied_source: Option<TransactionKey>,
}

impl CurrentAssertionFamilyRead {
    pub async fn require_unretired_family_source(&self) -> Result<()> {
        let catalog = self.context.catalog.database(self.context.catalog.default_database())?;
        let [partition] = catalog.partitions.as_slice() else { return Err(hosted_source_unavailable()); };
        if partition.contains_table(&TableRef::new("public", RESTORE))
            && self.carrier_point(RESTORE, ARCHIVE_FENCE).await?.row.is_some()
        {
            return Err(hosted_source_unavailable());
        }
        Ok(())
    }

    pub fn applied_source(&self) -> Option<TransactionKey> {
        self.applied_source
    }

    pub async fn carrier_journal_installation(&self) -> Result<HostedControlPoint> {
        let restored = self.carrier_point("bj_rtdb_restore_hold", "restore").await?;
        if restored.row.is_some() { return Err(hosted_source_unavailable()); }
        self.carrier_point(INSTALLATION, "authority").await
    }

    pub async fn carrier_journal_registration(&self, key: &str) -> Result<HostedControlPoint> {
        self.carrier_point("bj_external_key_recovery", key).await
    }

    pub async fn retired_family_custody_enrollment(&self, key: &str) -> Result<HostedControlPoint> {
        let id = key.strip_prefix("fenced-family-custody/")
            .or_else(|| key.strip_prefix("fenced-family-replay/")).ok_or_else(hosted_source_unavailable)?;
        if id.len() != 64 || !id.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()) {
            return Err(hosted_source_unavailable());
        }
        self.carrier_point(RESTORE, key).await
    }

    async fn carrier_point(&self, relation: &str, key: &str) -> Result<HostedControlPoint> {
        read_carrier_point(&self.context, relation, key).await
    }

    pub async fn assertion_family_fence(&self, family: Uuid) -> Result<Option<AssertionFamilyFenceMarker>> {
        let unavailable = hosted_source_unavailable;
        let selected = rtdb_query::read_hosted_control_point(
            Arc::clone(&self.context),
            TableRef::new("public", ASSERTION_FAMILY_FENCES),
            &EntityId::String(family.simple().to_string()),
            true,
        ).await?;
        self.context.check_cancelled()?;
        selected.row.map(|(row, _)| {
            let body = rtdb_columnar::ArrowCell::named(row.batch(), "body", 0)
                .and_then(|cell| cell.binary()).ok_or_else(unavailable)?;
            AssertionFamilyFenceMarker::decode(body).map_err(|_| unavailable())
        }).transpose()
    }

    pub async fn world_registration(&self, world: Iid) -> Result<Option<OwnedWorld>> {
        let registration = rtdb_query::read_owned_world(Arc::clone(&self.context), &self.owner, world).await?;
        self.context.check_cancelled()?;
        Ok(registration.map(|(world, _)| world))
    }
}

async fn read_carrier_point(
    context: &Arc<rtdb_query::QueryContext>, relation: &str, key: &str,
) -> Result<HostedControlPoint> {
    rtdb_query::read_hosted_control_point(
        Arc::clone(context), TableRef::new("public", relation), &EntityId::String(key.to_owned()), true,
    ).await
}

#[derive(Clone)]
pub struct OriginalAssertionFamilyReader {
    node: Node,
    database: String,
    owner: String,
    identity: Arc<dyn SessionIdentity>,
    attachment: std::sync::Weak<Database>,
    scope: PlacedAttemptScope,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssertionFenceStatus {
    Pending,
    Aborted,
    Ordered,
}

impl OriginalAssertionFamilyReader {
    pub fn history_fingerprint(&self) -> [u8; 32] {
        self.scope.history_fingerprint()
    }

    pub async fn current(&self, deployment: Uuid, cancellation: &CancellationToken) -> Result<CurrentAssertionFamilyRead> {
        self.current_read(deployment, None, cancellation).await
    }

    /// Opens a fresh Original control view containing every transaction in `floor`.
    /// The caller supplies the floor from its authenticated Original history.
    pub async fn current_covering(
        &self, floor: &AppliedReadFloor, cancellation: &CancellationToken,
    ) -> Result<CurrentAssertionFamilyRead> {
        self.current_read(floor.history(), Some(floor), cancellation).await
    }

    async fn current_read(
        &self, deployment: Uuid, floor: Option<&AppliedReadFloor>, cancellation: &CancellationToken,
    ) -> Result<CurrentAssertionFamilyRead> {
        let unavailable = hosted_source_unavailable;
        let receipt = self.node.await_durable_source(
            &self.database, self.identity.as_ref(), true, Some(&self.attachment), cancellation,
        ).await?.ok_or_else(unavailable)?;
        if Uuid::parse_str(&receipt.history).ok() != Some(deployment) {
            return Err(unavailable());
        }
        let basis = QueryBasis {
            snapshot: TimeBasis::default(), replica_capture: None, resume_token: None,
            has_await_floor: false, snapshot_time: None, current_time: self.node.query_instant(),
            timezone: self.node.factory.options.default_timezone.clone(),
        };
        let context = self.node.query_context_in_placement(
            &self.database, &basis, self.identity.as_ref(), Some(&self.attachment), CatalogReadCut::Current, cancellation,
        ).await?;
        let catalog = context.catalog.database(&self.database)?;
        let [partition] = catalog.partitions.as_slice() else { return Err(unavailable()); };
        let head = partition.tx_basis();
        if receipt.applied.is_some_and(|applied| head.is_none_or(|tip| !covers_transaction(tip, applied))) {
            return Err(unavailable());
        }
        if let Some(floor) = floor {
            let applied = receipt.applied.filter(|applied| covers_transaction(*applied, floor.minimum()))
                .ok_or_else(unavailable)?;
            floor.require_transactions(&context, head.ok_or_else(unavailable)?, Some(applied)).await?;
        }
        context.check_cancelled()?;
        Ok(CurrentAssertionFamilyRead { context, owner: self.owner.clone(), applied_source: receipt.applied })
    }

    pub async fn fence_status(
        &self, deployment: Uuid, family: Uuid, attempt: PlacedAttempt,
        marker: AssertionFamilyFenceMarker, cancellation: &CancellationToken,
    ) -> Result<AssertionFenceStatus> {
        let unavailable = hosted_source_unavailable;
        if !self.scope.same_history(attempt.scope()) || !attempt.belongs_to(Some(&self.owner)) {
            return Err(unavailable());
        }
        let current = self.current(deployment, cancellation).await?;
        if let Some(ordered) = current.assertion_family_fence(family).await? {
            if ordered != marker { return Err(unavailable()); }
            return Ok(AssertionFenceStatus::Ordered);
        }
        if current.applied_source().is_some_and(|key| key.tx_id.0 >= attempt.position().0) {
            Ok(AssertionFenceStatus::Aborted)
        } else {
            Ok(AssertionFenceStatus::Pending)
        }
    }
}

impl NativeSession {
    pub fn assertion_family_reader(&self) -> Result<OriginalAssertionFamilyReader> {
        let unavailable = hosted_source_unavailable;
        let owner = self.state.user().filter(|owner| !owner.is_empty()).ok_or_else(unavailable)?.to_owned();
        let database = self.service.database()?;
        let scope = database.placed_writer_scope().ok_or_else(unavailable)?;
        Ok(OriginalAssertionFamilyReader {
            node: self.service.node.clone(), database: self.state.database().to_owned(), owner,
            identity: Arc::clone(&self.service.identity), attachment: Arc::downgrade(&database), scope,
        })
    }

    pub async fn current_assertion_family_read(&self, deployment: Uuid) -> Result<CurrentAssertionFamilyRead> {
        self.assertion_family_reader()?.current(deployment, self.state.cancellation()).await
    }
}
