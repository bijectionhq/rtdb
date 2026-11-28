use std::{future::Future, pin::Pin, time::Duration};

use rtdb_flight_sql::FrozenWorldWinner;
use rtdb_types::{OwnedIdentityScan, ROOT_ROW_CARRIERS, RootRowCarrier, SharedColumnCarrierPrefix};
use rtdb_node::{NodeOptions, OwnedChangeEdge, OwnedCountRequest, OwnedKeyBound, OwnedPathColumns, OwnedRangeCount, OwnedRangePage, OwnedRangeRequest, RetainedKeyBound};
use rtdb_plan::Catalog;
use rtdb_storage::{cache::CacheBudget, sealing::{InheritedRootRankCut, PlaintextDependencies, PlaintextScopes, PlaintextView}};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::*;

#[path = "held_overlay_merge.rs"]
mod overlay_merge;
#[path = "held_projection.rs"]
mod projection;
#[path = "held_shared_columns.rs"]
mod shared_columns;

use projection::WorldProjectionCustody;

use crate::archive::read_world::{ArchiveLifetimeRead, CheckpointPoint, read_checkpoint_point_in_context, require_relation_purge_history, with_checkpoint_context};

#[derive(Clone)]
pub(crate) struct WorldArchive {
    pub directory: Arc<dyn Directory>,
    pub storage: CellStorage,
    pub keys: Arc<dyn KeyService>,
    pub hosted_task: Option<HostedTask>,
    pub options: NodeOptions,
}

#[async_trait::async_trait]
impl rtdb_node::WorldConstraintArchive for WorldArchive {
    async fn require_root_landing_derivative(
        &self, derivative: rtdb_tx::PreparedFamilyDerivative<'_>,
    ) -> Result<()> {
        let unavailable = OwnedWorld::unavailable;
        let installed = self.directory.deployment_by_name(derivative.database).await?.ok_or_else(unavailable)?;
        let deployment = Uuid::parse_str(&installed.id).map_err(|_| unavailable())?;
        if PlacedAttemptScope::new(&installed.stream("replica"), installed.epoch)? != derivative.placement
            || installed.writer_pool().is_none()
        {
            return Err(unavailable());
        }
        let family = crate::keys::family_vault_address(deployment, Uuid::from_bytes(derivative.selector.family));
        let families = self.keys.assertion_families(rtdb_storage::IoRegion::new()).ok_or_else(unavailable)?;
        families.require_reserved_root_derivative(
            family, derivative.selector.member, derivative.relation, derivative.key,
        ).await
    }

    async fn claim_assertion_family_fence(&self, claim: rtdb_node::AssertionFenceClaim<'_>) -> Result<()> {
        let unavailable = OwnedWorld::unavailable;
        let installed = self.directory.deployment_by_name(claim.database).await?.ok_or_else(unavailable)?;
        let deployment = Uuid::parse_str(&installed.id).map_err(|_| unavailable())?;
        let expected = PlacedAttemptScope::new(&installed.stream("replica"), installed.epoch)?;
        if claim.attempt.scope() != expected || installed.writer_pool().is_none() {
            return Err(unavailable());
        }
        let winner_line = claim.winner.as_ref().map_or_else(
            || crate::keys::assertion_root_line(deployment, installed.log_epoch),
            |winner| winner.world.0,
        );
        if claim.marker.winner.line_generation != winner_line {
            return Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Conflict,
                "rtdb.owned/assertion-family-fence-marker", "The retirement and physical winner belong to different lines"));
        }
        let client_family = Uuid::from_bytes(claim.family);
        let family = crate::keys::family_vault_address(deployment, client_family);
        let region = rtdb_storage::IoRegion::new();
        let families = self.keys.assertion_families(region).ok_or_else(unavailable)?;
        let winner = match (claim.winner, claim.root_carrier) {
            (Some(winner), None) => Some((winner, self.keys.assertion_keys().ok_or_else(unavailable)?)),
            (None, Some(carrier)) => {
                families.require_root_retirement_carrier(family, client_family, claim.marker, carrier).await?;
                None
            }
            _ => return Err(unavailable()),
        };
        families.claim_fence(family, client_family, claim.marker, claim.attempt, claim.durable_predecessor, claim.root_erases, winner).await
    }

    async fn check(&self, check: rtdb_node::ArchivedWorldConstraints<'_>) -> Result<()> {
        let unavailable = OwnedWorld::unavailable;
        let installed = self.directory.deployment_by_name(check.database).await?.ok_or_else(unavailable)?;
        if PlacedAttemptScope::new(&installed.stream("replica"), installed.epoch)? != check.placement {
            return Err(unavailable());
        }
        let world = check.world;
        let pin = Uuid::from_bytes(world.pin.0);
        let retained = installed.checkpoint_worlds.get(&pin).ok_or_else(unavailable)?;
        if retained.owner != check.owner || !matches_retention(world, retained)
            || check.ordered >= retained.until
        {
            return Err(unavailable());
        }
        let deployment = Uuid::parse_str(&installed.id).map_err(|_| unavailable())?;
        let parent = parent_media(deployment, retained);
        admit_world_media(&installed, &self.storage, deployment, retained)?;
        let resources = CheckpointReadResources {
            node: &self.options, memory: check.memory, cache: Arc::clone(&check.cache), cancellation: check.cancellation,
        };
        let ancestors = &retained.ancestors;
        let placement = check.placement;
        let guard = |current: &DeploymentRecord| {
            let current_pin = current.checkpoint_worlds.get(&pin).ok_or_else(unavailable)?;
            if current_pin != retained
                || PlacedAttemptScope::new(&current.stream("replica"), current.epoch)? != placement
                || now() >= current_pin.until
            {
                return Err(unavailable());
            }
            admit_world_media(current, &self.storage, deployment, current_pin)
        };
        guard(&installed)?;
        let result = with_checkpoint_context(
            self.directory.as_ref(), &self.storage, self.keys.as_ref(), self.hosted_task.as_ref(),
            &parent, resources.clone(), |context| async move {
                if ancestors.is_empty() { return check.validate(context).await; }
                with_frozen_writers(
                    self, deployment, ancestors, resources, Vec::new(), |frozen| async move {
                        let layers = {
                            let relation = check.relation();
                            let schema = world.schemas.relation(relation).ok_or_else(unavailable)?;
                            let inherited = InheritedRelation::open(
                                Arc::clone(&context), frozen, check.owner, relation, ancestors, schema, check.memory,
                            ).await?;
                            inherited.writers().map(|layer| (Arc::clone(&layer.overlay), layer.shadow.clone()))
                                .collect::<Vec<_>>()
                        };
                        check.validate_layers(context, &layers).await
                    },
                ).await
            },
        ).await;
        let current = self.directory.deployment(&installed.id).await?.ok_or_else(unavailable)?;
        guard(&current)?;
        result
    }
}

fn frozen_prefix(
    layers: &[rtdb_directory::CheckpointWorldAncestor], selected: Option<Timestamp>,
) -> &[rtdb_directory::CheckpointWorldAncestor] {
    match selected {
        Some(at) => &layers[..(layers.partition_point(|layer| layer.publication.system_time < at) + 1).min(layers.len())],
        None => layers,
    }
}

// Keep every underlying admission live until the final merged result drains.
fn with_frozen_writers<'a, T, F, Fut>(
    archive: &'a WorldArchive,
    deployment: Uuid,
    remaining: &'a [rtdb_directory::CheckpointWorldAncestor],
    resources: CheckpointReadResources<'a>,
    mut opened: Vec<Arc<rtdb_query::QueryContext>>,
    run: F,
) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>
where
    T: Send + 'a,
    F: FnOnce(Vec<Arc<rtdb_query::QueryContext>>) -> Fut + Send + 'a,
    Fut: Future<Output = Result<T>> + Send + 'a,
{
    Box::pin(async move {
        let Some((ancestor, rest)) = remaining.split_first() else { return run(opened).await; };
        let media = ancestor_media(deployment, ancestor);
        with_checkpoint_context(
            archive.directory.as_ref(), &archive.storage, archive.keys.as_ref(), archive.hosted_task.as_ref(),
            &media, resources.clone(), |layer| {
                opened.push(layer);
                with_frozen_writers(archive, deployment, rest, resources, opened, run)
            },
        ).await
    })
}

pub(crate) struct HeldCheckpointWorld {
    open: OpenWorld,
    projections: Arc<WorldProjectionCustody>,
    memory: Arc<MemoryBudget>,
    requests: mpsc::Sender<ArchiveRequest>,
    cancellation: CancellationToken,
}

struct ArchiveRequest {
    operation: ArchiveRead,
    plaintext: PlaintextScopes,
}

struct ArchivePoint {
    relation: OwnedRelation,
    id: EntityId,
    valid: Interval,
    lifetime: Option<ArchiveLifetimeRead>,
    expected_schema: Option<rtdb_types::OwnedSchema>,
    guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    reply: oneshot::Sender<Result<Option<WorldFact>>>,
}

enum ArchiveRead {
    Coverage {
        frontiers: Vec<(OwnedRelation, i64)>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<()>>,
    },
    ParentAsOf {
        requested: Timestamp,
        schemas: Vec<(OwnedRelation, rtdb_types::OwnedSchema)>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<Option<Timestamp>>>,
    },
    ParentScheduleAt {
        relation: OwnedRelation,
        id: EntityId,
        system: Timestamp,
        valid: Interval,
        schema: rtdb_types::OwnedSchema,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<Vec<(Interval, Timestamp)>>>,
    },
    ParentTouchImagesAt {
        relation: OwnedRelation,
        path: OwnedRelation,
        id: EntityId,
        selected: Timestamp,
        before: Timestamp,
        group: Timestamp,
        valid: Timestamp,
        images: bool,
        schema: rtdb_types::OwnedSchema,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<[Option<WorldFact>; 2]>>,
    },
    ParentChangesAt {
        relation: OwnedRelation,
        selected: Timestamp,
        after: OwnedChangeEdge,
        through: OwnedChangeEdge,
        limit: usize,
        reverse: bool,
        schema: rtdb_types::OwnedSchema,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<rtdb_node::OwnedMergedChanges>>,
    },
    ParentVersionsAt {
        relation: OwnedRelation,
        id: EntityId,
        selected: Timestamp,
        request: rtdb_node::OwnedVersionRequest,
        schema: rtdb_types::OwnedSchema,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<rtdb_flight_sql::WorldParentVersions>>,
    },
    ParentPointAt {
        system: Timestamp,
        point: ArchivePoint,
    },
    AcceptedScope {
        schemas: Vec<(OwnedRelation, rtdb_types::OwnedSchema)>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<()>>,
    },
    Point(ArchivePoint),
    Ancestor {
        relation: OwnedRelation,
        read: AncestorRead,
        selected: Option<Timestamp>,
        schema: rtdb_types::OwnedSchema,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<AncestorResult>>,
    },
    PathColumns {
        relation: OwnedRelation,
        path: OwnedRelation,
        expected_schema: Option<rtdb_types::OwnedSchema>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<OwnedPathColumns>>,
    },
    Search {
        inherited_rank: Option<InheritedRootRankCut>,
        partitions: Vec<(OwnedRelation, OwnedRelation)>,
        frontiers: Vec<i64>,
        query: String,
        overlay: Option<rtdb_node::HeldTextCorpus>,
        historical: bool,
        ancestor_schemas: Option<Vec<rtdb_types::OwnedSchema>>,
        ancestor_system: Option<Timestamp>,
        offset: usize,
        limit: usize,
        valid: Timestamp,
        parent_at: Option<(Timestamp, Vec<(OwnedRelation, rtdb_types::OwnedSchema)>)>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<rtdb_node::OwnedSearchBatch>>,
    },
    Nearest {
        inherited_rank: Option<InheritedRootRankCut>,
        partitions: Vec<rtdb_node::nearest::NearestPartition>,
        frontiers: Vec<i64>,
        query: rtdb_node::nearest::NearestQuery,
        overlay: Option<rtdb_node::nearest::HeldVectorOverlay>,
        historical: bool,
        ancestor_schemas: Option<Vec<rtdb_types::OwnedSchema>>,
        ancestor_system: Option<Timestamp>,
        valid: Timestamp,
        parent_at: Option<(Timestamp, Vec<(OwnedRelation, rtdb_types::OwnedSchema)>)>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<rtdb_node::OwnedSearchBatch>>,
    },
    SearchColumns {
        partitions: Vec<(OwnedRelation, OwnedRelation)>,
        frontiers: Vec<i64>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<(Vec<String>, rtdb_plan::Analyzer)>>,
    },
    Ordered {
        relation: OwnedRelation,
        path: OwnedRelation,
        frontier: i64,
        lower: Option<RetainedKeyBound>,
        upper: Option<RetainedKeyBound>,
        valid: Timestamp,
        earlier: Option<(Timestamp, rtdb_types::OwnedSchema)>,
        read: OrderedRead,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<OrderedResult>>,
    },
    Boundaries {
        relation: OwnedRelation,
        id: EntityId,
        valid: Interval,
        expected_schema: Option<rtdb_types::OwnedSchema>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<Vec<Timestamp>>>,
    },
    Identities {
        relation: OwnedRelation,
        at: Timestamp,
        after: OwnedIdentityScan<'static>,
        fetch: usize,
        protected: Option<(Vec<u8>, rtdb_types::OwnedSchema)>,
        earlier: Option<(Timestamp, rtdb_types::OwnedSchema)>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<rtdb_query::OwnedIdentityCandidates>>,
    },
}

#[derive(Clone, Copy)]
enum ClockCut {
    System(Timestamp),
    Publication(TransactionKey),
}

impl ClockCut {
    fn system(self) -> Timestamp {
        match self {
            Self::System(system) => system,
            Self::Publication(publication) => publication.system_time,
        }
    }
}

enum AncestorRead {
    Point { id: EntityId, valid: Interval, prove_winner: bool },
    Changes { after: OwnedChangeEdge, through: OwnedChangeEdge, limit: usize, reverse: bool },
    Clock(ClockCut),
    SelectClock(Timestamp),
    PathTouch { id: EntityId, path: OwnedRelation, group: Timestamp, valid: Timestamp, images: bool },
    Version { id: EntityId, from: Timestamp, through: Timestamp, group: Timestamp },
    Schedule { id: EntityId, valid: Interval },
    Identities { valid: Timestamp, after: OwnedIdentityScan<'static>, limit: usize },
    OverlayIdentities { valid: Timestamp, after: OwnedIdentityScan<'static>, limit: usize, prefix: Option<String> },
    Range {
        path: OwnedRelation,
        lower: Option<RetainedKeyBound>,
        upper: Option<RetainedKeyBound>,
        after: Option<RetainedKeyBound>,
        descending: bool,
        limit: usize,
        valid: Timestamp,
    },
}

enum AncestorResult {
    Point(Option<WorldFact>, bool, Option<FrozenWorldWinner>),
    Changes(rtdb_node::OwnedMergedChanges),
    Clock(rtdb_query::ArchivedHeldClock),
    SelectedClock(Option<(TransactionKey, Timestamp)>),
    PathTouch([(Option<WorldFact>, bool); 2]),
    Version(Timestamp, Vec<(Interval, WorldFact, bool)>),
    Schedule(Vec<(Interval, Timestamp)>),
    Identities(WorldIdentityPage),
    Range(OwnedRangePage),
}

enum OrderedRead {
    Page { after: Option<RetainedKeyBound>, descending: bool, limit: usize },
    Count,
    Measure,
}

enum OrderedResult {
    Page(OwnedRangePage),
    Count(u64),
    Measure(OwnedRangeCount),
}

pub(crate) struct WorldIdentityPage {
    pub keys: Vec<EntityId>,
    pub more: bool,
    pub reservations: Vec<Arc<MemoryReservation>>,
}

impl HeldCheckpointWorld {
    async fn send_archive(&self, operation: ArchiveRead) -> Result<()> {
        self.requests.send(ArchiveRequest { operation, plaintext: PlaintextScopes::capture() })
            .await.map_err(|_| OwnedWorld::unavailable())
    }

    pub fn present(&self) -> TransactionKey { self.open.snapshot.present() }

    pub fn application_publication(&self) -> Option<(Timestamp, Timestamp)> {
        self.open.publication.map(|(reading, publication)| (reading, publication.system_time))
    }
    pub fn initial_clock(&self) -> Timestamp { self.open.record.initial_clock() }
    pub fn clock_mode(&self) -> WorldClockMode { self.open.record.mode }

    pub async fn retained_child_cut(
        &self, scope: rtdb_types::PlacedAttemptScope, publication: TransactionKey,
    ) -> Result<rtdb_node::OwnedForkCut> {
        self.check()?;
        if self.open.record.ancestors.is_empty() && publication == self.open.record.basis {
            let cut = self.open.snapshot.fork_inherited_root_cut(scope, &self.open.record).await?;
            self.check()?;
            return Ok(cut);
        }
        let archived = if self.open.record.ancestor_covering(publication).is_some() {
            let relations = self.open.record.schemas.relations().map(|(name, _)| OwnedRelation::new(name))
                .collect::<Result<Vec<_>>>()?;
            Some(self.archived_accepted_clock(&relations, ClockCut::Publication(publication)).await?)
        } else {
            None
        };
        let cut = self.open.snapshot.fork_overlay_cut(
            scope, &self.open.record, publication, archived.as_ref(),
        ).await?;
        self.check()?;
        Ok(cut)
    }

    pub fn parent_basis(&self) -> TransactionKey { self.open.retained.basis }
    pub fn ancestor(&self) -> Option<(Iid, Timestamp)> {
        self.open.record.ancestor().map(|ancestor| (Iid(ancestor.world), ancestor.publication.system_time))
    }
    pub fn ancestors(&self) -> Vec<(Iid, Timestamp)> {
        self.open.record.ancestors.iter().map(|layer| (Iid(layer.world), layer.publication.system_time)).collect()
    }

    fn uses_frozen_overlay(&self, system: Option<Timestamp>) -> bool {
        self.ancestor().is_some_and(|(_, inherited)| {
            !self.clock_mode().is_writable() || system.is_some_and(|system| system <= inherited)
        })
    }

    fn frozen_system(&self, system: Option<Timestamp>) -> Option<Timestamp> {
        system.filter(|system| self.uses_frozen_overlay(Some(*system)))
    }

    pub fn parent_pin(&self) -> Iid { self.open.record.pin }
    pub fn until(&self) -> Timestamp { self.open.retained.until }
    pub fn evidence_until(&self) -> Timestamp { self.open.snapshot.evidence_until().min(self.until()) }

    pub fn check(&self) -> Result<()> {
        self.open.check()?;
        if self.cancellation.is_cancelled() || self.requests.is_closed() {
            return Err(OwnedWorld::unavailable());
        }
        Ok(())
    }

    pub fn check_relation(&self, relation: &OwnedRelation) -> Result<()> {
        self.check()?;
        self.open.snapshot.check_relation(relation)
    }

    async fn inherited_root_rank_cut(&self) -> Result<Option<InheritedRootRankCut>> {
        let open = &self.open;
        if open.record.mode != WorldClockMode::Retained || !open.record.ancestors.is_empty()
            || open.record.fork_lineage.is_empty() || open.record.parent_publication.is_none()
        {
            return Ok(None);
        }
        let registration = open.snapshot.world_registration_version(open.record.id).await?
            .filter(|registration| registration.history == open.registered)
            .ok_or_else(OwnedWorld::unavailable)?;
        Ok(Some(InheritedRootRankCut {
            world: open.record.id,
            pin: open.record.pin,
            basis: open.retained.basis,
            registration,
            writer: open.writer,
            incarnation: open.incarnation,
            log_epoch: open.log_epoch,
            storage_generation: open.storage_generation,
            until: open.retained.until,
        }))
    }

    pub async fn changes(
        &self, relations: &[OwnedRelation], after: OwnedChangeEdge, through: OwnedChangeEdge, limit: usize, descending: bool,
    ) -> Result<rtdb_node::OwnedMergedChanges> {
        self.check()?;
        let guard = self.open.snapshot.relation_release_guard(relations)?;
        if let Some(ancestor) = self.open.record.ancestor() {
            if !self.clock_mode().is_writable() || after < ancestor.publication.into() {
                return Err(OwnedWorld::unavailable());
            }
            self.archived_accepted_clock(relations, ClockCut::Publication(ancestor.publication)).await?;
        } else {
            self.require_accepted_archive_scope(relations).await?;
        }
        let page = self.open.snapshot.world_merged_changes_ordered(self.open.record.id, relations, after, through, limit, descending).await?;
        guard()?;
        self.check()?;
        Ok(page)
    }

    async fn require_archive_coverage(
        &self, frontiers: Vec<(OwnedRelation, i64)>, guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::Coverage {
            frontiers, guard, reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?
    }

    pub fn close(&self) {
        self.cancellation.cancel();
        self.open.snapshot.close();
    }

    pub async fn point(&self, relation: &OwnedRelation, id: &EntityId, valid_at: Timestamp, observed: bool) -> Result<WorldPoint> {
        if self.uses_frozen_overlay(None) {
            return self.ancestor_point(relation, id, valid_at, observed).await;
        }
        self.check_relation(relation)?;
        let (local, witness) = self.open.read_local(relation, id, valid_at, &self.memory, observed).await?;
        let (fact, shadowed) = self.resolve_local(relation, id, valid_at, local, None).await?;
        self.check_relation(relation)?;
        Ok(WorldPoint { present: self.present(), fact, until: None, shadowed, witness })
    }

    async fn resolve_local(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Timestamp,
        local: Option<Option<WorldFact>>, schema: Option<&rtdb_types::OwnedSchema>,
    ) -> Result<(Option<WorldFact>, bool)> {
        let inherited = if self.ancestor().is_some() {
            let point = self.ancestor_point(relation, id, valid, false).await?;
            Some((point.fact, point.shadowed))
        } else { None };
        match (local, inherited) {
            (Some(fact), _) => Ok((fact, true)),
            (None, Some(inherited)) => Ok(inherited),
            (None, None) => Ok((self.archive_point_checked(relation, id, valid, None, schema).await?, false)),
        }
    }

    pub async fn path_touch(
        &self, relation: &OwnedRelation, path: &OwnedRelation, id: &EntityId,
        group: Timestamp, valid: Timestamp, images: bool,
    ) -> Result<[(Option<WorldFact>, bool); 2]> {
        if self.uses_frozen_overlay(Some(group)) {
            let (result, _) = self.read_ancestor(relation, AncestorRead::PathTouch {
                id: id.clone(), path: path.clone(), group, valid, images,
            }).await?;
            let AncestorResult::PathTouch(sides) = result else { return Err(OwnedWorld::unavailable()); };
            return Ok(sides);
        }
        self.check_relation(relation)?;
        let schema = self.open.record.schemas.relation(relation).ok_or_else(OwnedWorld::unavailable)?;
        if images && (schema.has_opaque_columns() || schema.lifetime_micros().is_some()) {
            return Err(OwnedWorld::unavailable());
        }
        let (columns, [before, after]) = self.open.snapshot.world_path_touch(
            self.open.record.id, relation, path, id, group, valid,
        ).await?;
        self.require_parent_path(relation, path, &columns, images.then_some(schema)).await?;
        let mut sides = [(None, false), (None, false)];
        for (index, (local, side)) in [before, after].into_iter().zip(&mut sides).enumerate() {
            let local = decode_point(local.row.into_iter().collect(), id, &self.memory, None)?;
            if images && index == 1 && local.as_ref().is_none_or(|fact| fact.revision != group) {
                return Err(OwnedWorld::unavailable());
            }
            let local = match local {
                Some(fact) => Some(self.open.local_fact(relation, fact).await?),
                None => None,
            };
            let (mut fact, shadowed) = self.resolve_local(relation, id, valid, local, None).await?;
            if let Some(fact) = &mut fact {
                if fact.row.schema_ref().index_of("_id").is_err() { return Err(OwnedWorld::unavailable()); }
                if images {
                    fact.validate(&schema)?;
                } else {
                    fact.select_path(&columns, true)?;
                }
            }
            *side = (fact, shadowed);
        }
        self.check_relation(relation)?;
        Ok(sides)
    }

    async fn require_parent_path(
        &self, relation: &OwnedRelation, path: &OwnedRelation, columns: &[String], expected_schema: Option<&rtdb_types::OwnedSchema>,
    ) -> Result<()> {
        let guard = self.open.snapshot.relation_release_guard([relation])?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::PathColumns {
            relation: relation.clone(), path: path.clone(), expected_schema: expected_schema.cloned(), guard, reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let parent_columns = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        let schema = self.open.record.schemas.relation(relation).ok_or_else(OwnedWorld::unavailable)?;
        if &*parent_columns != columns || columns.last().is_none_or(|column| column != "_id")
            || columns[..columns.len() - 1].iter().any(|column| schema.path_column_type(column).is_none())
        {
            return Err(OwnedWorld::unavailable());
        }
        Ok(())
    }

    pub async fn preflight_accepted_scope(
        &self, relations: &[OwnedRelation], system: Timestamp,
    ) -> Result<(TransactionKey, Timestamp)> {
        self.check()?;
        if let Some(layer) = self.open.record.ancestor_at_system(system) {
            let archived = self.archived_accepted_clock(relations, ClockCut::System(system)).await?;
            return Ok((archived.publication(), archived.reading(layer, system)?));
        }
        if let Some(ancestor) = self.open.record.ancestor() {
            self.archived_accepted_clock(relations, ClockCut::Publication(ancestor.publication)).await?;
        }
        let publication = self.open.snapshot.certify_world_clock_publication(&self.open.record, relations, system).await?;
        self.require_accepted_archive_scope(relations).await?;
        Ok(publication)
    }

    async fn archived_accepted_clock(
        &self, relations: &[OwnedRelation], cut: ClockCut,
    ) -> Result<rtdb_query::ArchivedHeldClock> {
        let unavailable = OwnedWorld::unavailable;
        let ancestor = match cut {
            ClockCut::System(system) => self.open.record.ancestor_at_system(system),
            ClockCut::Publication(publication) => self.open.record.ancestor_covering(publication),
        }.ok_or_else(unavailable)?;
        let mut certified = None;
        for relation in relations {
            let (result, _) = self.read_ancestor(relation, AncestorRead::Clock(cut)).await?;
            let AncestorResult::Clock(clock) = result else { return Err(unavailable()); };
            let reading = clock.reading(ancestor, cut.system())?;
            let winner = (clock.publication(), reading);
            if certified.replace((clock, winner)).is_some_and(|(_, previous)| previous != winner) {
                return Err(unavailable());
            }
        }
        certified.map(|(clock, _)| clock).ok_or_else(unavailable)
    }

    pub async fn select_accepted_scope(&self, relations: &[OwnedRelation], requested: Timestamp)
        -> Result<Option<(TransactionKey, Timestamp)>> {
        self.check()?;
        if self.ancestor().is_some() {
            if self.clock_mode().is_writable() && requested > self.initial_clock()
                && let Some(selected) = self.open.snapshot.select_world_accepted_scope(
                    &self.open.record, relations, requested,
                ).await?
            {
                self.require_accepted_archive_scope(relations).await?;
                return Ok(Some(selected));
            }
            let first = relations.first().ok_or_else(OwnedWorld::unavailable)?;
            let (result, _) = self.read_ancestor(first, AncestorRead::SelectClock(requested)).await?;
            let AncestorResult::SelectedClock(selected) = result else { return Err(OwnedWorld::unavailable()); };
            self.require_accepted_archive_scope(relations).await?;
            if let Some((publication, reading)) = selected {
                let layer = self.open.record.ancestor_covering(publication).ok_or_else(OwnedWorld::unavailable)?;
                let archived = self.archived_accepted_clock(relations, ClockCut::Publication(publication)).await?;
                if archived.reading(layer, publication.system_time)? != reading {
                    return Err(OwnedWorld::unavailable());
                }
            }
            return Ok(selected);
        }
        let selected = self.open.snapshot.select_world_accepted_scope(&self.open.record, relations, requested).await?;
        self.require_accepted_archive_scope(relations).await?;
        Ok(selected)
    }

    async fn require_accepted_archive_scope(&self, relations: &[OwnedRelation]) -> Result<()> {
        let guard = self.open.snapshot.relation_release_guard(relations)?;
        let mut schemas = Vec::with_capacity(relations.len());
        for relation in relations {
            let schema = self.open.record.schemas.relation(relation).ok_or_else(OwnedWorld::unavailable)?;
            schemas.push((relation.clone(), schema.clone()));
        }
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::AcceptedScope {
            schemas, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        guard()?;
        self.check()
    }

    pub async fn select_parent_as_of(
        &self, relations: &[OwnedRelation], requested: Timestamp,
    ) -> Result<Option<Timestamp>> {
        self.check()?;
        if !(1..=32).contains(&relations.len()) || relations.iter().collect::<std::collections::BTreeSet<_>>().len() != relations.len()
            || requested > self.parent_basis().system_time
        {
            return Err(OwnedWorld::unavailable());
        }
        let guard = self.open.snapshot.relation_release_guard(relations)?;
        let mut schemas = Vec::with_capacity(relations.len());
        for relation in relations {
            schemas.push((relation.clone(), self.world_point_schema(relation)?.clone()));
            let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
            if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        }
        if let Some(ancestor) = self.open.record.ancestor()
            && relations.iter().any(|relation| matches!(relation.name(), "fact_unit_column_payloads" | ROOT_ROW_CARRIERS))
        {
            self.archived_accepted_clock(relations, ClockCut::Publication(ancestor.publication)).await?;
        }
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::ParentAsOf {
            requested, schemas, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let selected = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        guard()?;
        self.check()?;
        Ok(selected)
    }

    pub async fn parent_point_at(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Timestamp, system: Timestamp,
        original_scope: Option<PlacedAttemptScope>,
    ) -> Result<Option<WorldFact>> {
        self.check_relation(relation)?;
        if system > self.parent_basis().system_time { return Err(OwnedWorld::unavailable()); }
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let schema = if matches!(relation.name(), "fact_unit_column_payloads" | ROOT_ROW_CARRIERS) {
            if relation.name() == ROOT_ROW_CARRIERS
                && !matches!(id, EntityId::Binary(key) if RootRowCarrier::parse(key).is_some())
            { return Err(OwnedWorld::unavailable()); }
            self.require_original_carrier_scope(&witness, original_scope).await?;
            if let Some(ancestor) = self.open.record.ancestor() {
                self.archived_accepted_clock(std::slice::from_ref(relation), ClockCut::Publication(ancestor.publication)).await?;
            }
            let schema = self.world_point_schema(relation)?;
            if !relation.admits_world_ciphertext_points(schema) { return Err(OwnedWorld::unavailable()); }
            schema
        } else {
            self.historical_parent_schema(relation)?
        };
        let guard = self.open.snapshot.parent_point_release_guard(relation, id, system).await?;
        let valid = Interval::at(valid)?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::ParentPointAt {
            system, point: ArchivePoint {
                relation: relation.clone(), id: id.clone(), valid, lifetime: None,
                expected_schema: Some(schema.clone()), guard: Arc::clone(&guard), reply,
            },
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let fact = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        if let Some(fact) = &fact {
            if fact.revision > system { return Err(OwnedWorld::unavailable()); }
            fact.validate(&schema)?;
        }
        guard()?;
        self.check_relation(relation)?;
        Ok(fact)
    }

    pub async fn parent_schedule_at(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Interval, system: Timestamp,
    ) -> Result<(Vec<(Interval, Timestamp)>, OwnedWorldWitness)> {
        self.check_relation(relation)?;
        if valid.is_empty() || system > self.parent_basis().system_time { return Err(OwnedWorld::unavailable()); }
        let schema = self.historical_parent_schema(relation)?.clone();
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let guard = self.open.snapshot.parent_point_release_guard(relation, id, system).await?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::ParentScheduleAt {
            relation: relation.clone(), id: id.clone(), system, valid, schema, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let schedule = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        guard()?;
        self.check_relation(relation)?;
        Ok((schedule, witness))
    }
     pub async fn parent_touch_images_at(
        &self, relation: &OwnedRelation, path: &OwnedRelation, id: &EntityId,
        selected: Timestamp, before: Timestamp, group: Timestamp, valid: Timestamp, images: bool,
    ) -> Result<[Option<WorldFact>; 2]> {
        self.check_relation(relation)?;
        if before.next()? != group || group > selected || selected > self.parent_basis().system_time {
            return Err(OwnedWorld::unavailable());
        }
        let schema = self.historical_parent_schema(relation)?.clone();
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let guard = self.open.snapshot.parent_point_release_guard(relation, id, before).await?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::ParentTouchImagesAt {
            relation: relation.clone(), path: path.clone(), id: id.clone(), selected, before, group,
            valid, images, schema, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let images = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        guard()?;
        self.check_relation(relation)?;
        Ok(images)
    }

    pub async fn parent_changes_at(
        &self, relation: &OwnedRelation, selected: Timestamp, after: OwnedChangeEdge,
        through: OwnedChangeEdge, limit: usize, reverse: bool,
    ) -> Result<(rtdb_node::OwnedMergedChanges, OwnedWorldWitness)> {
        self.check_relation(relation)?;
        if after > through || through.time > selected || selected > self.parent_basis().system_time {
            return Err(OwnedWorld::unavailable());
        }
        let schema = self.historical_parent_schema(relation)?.clone();
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let guard = self.open.snapshot.relation_release_guard([relation])?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::ParentChangesAt {
            relation: relation.clone(), selected, after, through, limit, reverse, schema, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let page = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        guard()?;
        self.check_relation(relation)?;
        Ok((page, witness))
    }

    pub async fn ancestor_changes(
        &self, relation: &OwnedRelation, selected: Timestamp, after: OwnedChangeEdge,
        through: OwnedChangeEdge, limit: usize, reverse: bool,
    ) -> Result<(rtdb_node::OwnedMergedChanges, OwnedWorldWitness)> {
        if self.ancestor().is_none_or(|(_, cap)| cap != selected) {
            return Err(OwnedWorld::unavailable());
        }
        let (result, witness) = self.read_ancestor(
            relation, AncestorRead::Changes { after, through, limit, reverse },
        ).await?;
        let AncestorResult::Changes(page) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((page, witness))
    }

    pub async fn parent_versions_at(
        &self, relation: &OwnedRelation, id: &EntityId, selected: Timestamp, request: rtdb_node::OwnedVersionRequest,
    ) -> Result<(rtdb_flight_sql::WorldParentVersions, OwnedWorldWitness)> {
        self.check_relation(relation)?;
        if selected > self.parent_basis().system_time || request.system.upper() > selected.next()? {
            return Err(OwnedWorld::unavailable());
        }
        let schema = self.historical_parent_schema(relation)?.clone();
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let guard = self.open.snapshot.parent_point_release_guard(relation, id, request.system.lower()).await?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::ParentVersionsAt {
            relation: relation.clone(), id: id.clone(), selected, request, schema, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let page = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        guard()?;
        self.check_relation(relation)?;
        Ok((page, witness))
    }

    fn historical_parent_schema(&self, relation: &OwnedRelation) -> Result<&rtdb_types::OwnedSchema> {
        self.open.record.schemas.relation(relation)
            .filter(|schema| relation.is_root_data() && !schema.has_opaque_columns() && schema.lifetime_micros().is_none())
            .ok_or_else(OwnedWorld::unavailable)
    }

    pub async fn accepted_point(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Timestamp,
        system: Timestamp, original_scope: Option<PlacedAttemptScope>,
    ) -> Result<(Option<WorldFact>, bool, Option<FrozenWorldWinner>)> {
        if relation.name() == ROOT_ROW_CARRIERS { return Err(OwnedWorld::unavailable()); }
        if self.uses_frozen_overlay(Some(system)) {
            let protected = relation.name() == "fact_unit_column_payloads";
            let schema = self.world_point_schema(relation)?;
            if protected && !relation.admits_world_ciphertext_points(schema) { return Err(OwnedWorld::unavailable()); }
            let prove_winner = protected || (original_scope.is_some() && schema.has_world_assertion_journal()
                && matches!(id, EntityId::Binary(key) if RootRowCarrier::is_assertion_key(key)));
            if prove_winner {
                let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
                self.require_original_carrier_scope(&witness, original_scope).await?;
                self.archived_accepted_clock(std::slice::from_ref(relation), ClockCut::System(system)).await?;
            }
            let (result, _) = self.read_ancestor_at(relation, AncestorRead::Point {
                id: id.clone(), valid: Interval::at(valid)?, prove_winner,
            }, Some(system)).await?;
            let AncestorResult::Point(fact, shadowed, winner) = result else { return Err(OwnedWorld::unavailable()); };
            if prove_winner && shadowed {
                let winner = winner.ok_or_else(OwnedWorld::unavailable)?;
                let layer = self.open.record.ancestor_at_system(winner.publication.system_time)
                    .ok_or_else(OwnedWorld::unavailable)?;
                if winner.author.0 != layer.world || (protected && fact.as_ref().is_some_and(|fact|
                    fact.assertion.is_none_or(|assertion| assertion.line != Some(winner.author)
                        || assertion.held_reading != Some(winner.reading))))
                {
                    return Err(OwnedWorld::unavailable());
                }
            }
            return Ok((fact, shadowed, winner));
        }
        self.check_relation(relation)?;
        let schema = self.world_point_schema(relation)?;
        let cut = self.open.snapshot.certify_world_accepted_cut(&self.open.record, relation, system).await?;
        let guard = self.open.snapshot.relation_release_guard([relation])?;
        self.require_archive_coverage(vec![(relation.clone(), 0)], guard).await?;
        self.open.snapshot.check_point_purge_floor(relation, id, self.parent_basis().system_time).await?;
        let (fact, shadowed) = self.point_at_accepted_cut(relation, id, valid, &cut, schema).await?;
        Ok((fact, shadowed, None))
    }

    async fn require_original_carrier_scope(
        &self, witness: &OwnedWorldWitness, scope: Option<PlacedAttemptScope>,
    ) -> Result<()> {
        if !scope.is_some_and(|scope| witness.matches_scope(scope)) {
            return Err(OwnedWorld::unavailable());
        }
        self.projections.require_live_original().await
    }

    async fn point_at_accepted_cut(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Timestamp,
        cut: &rtdb_node::OwnedWorldAcceptedCut<'_>, schema: &rtdb_types::OwnedSchema,
    ) -> Result<(Option<WorldFact>, bool)> {
        let local = cut.shadow(id, valid).await?;
        let local = decode_point(local.row.into_iter().collect(), id, &self.memory, None)?;
        let local = match local {
            Some(fact) => Some(self.open.local_fact(relation, fact).await?),
            None => None,
        };
        let (fact, shadowed) = self.resolve_local(relation, id, valid, local, Some(schema)).await?;
        if let Some(fact) = &fact {
            let upper = if shadowed { cut.system_time() } else { self.parent_basis().system_time };
            if fact.revision > upper { return Err(OwnedWorld::unavailable()); }
            fact.validate(&schema)?;
        }
        self.check_relation(relation)?;
        Ok((fact, shadowed))
    }

    async fn read_ancestor(
        &self, relation: &OwnedRelation, read: AncestorRead,
    ) -> Result<(AncestorResult, OwnedWorldWitness)> {
        self.read_ancestor_at(relation, read, None).await
    }

    async fn read_ancestor_at(
        &self, relation: &OwnedRelation, read: AncestorRead, selected: Option<Timestamp>,
    ) -> Result<(AncestorResult, OwnedWorldWitness)> {
        self.check_relation(relation)?;
        let shared_columns = relation.name() == "fact_unit_column_payloads"
            && matches!(&read, AncestorRead::OverlayIdentities { prefix: Some(_), .. });
        let schema = if shared_columns
            || matches!(&read, AncestorRead::Point { .. } | AncestorRead::Clock(_) | AncestorRead::SelectClock(_))
        {
            self.world_point_schema(relation)?
        } else {
            self.historical_parent_schema(relation)?
        }.clone();
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let guard = match &read {
            AncestorRead::Point { id, .. } | AncestorRead::Schedule { id, .. }
                | AncestorRead::Version { id, .. } | AncestorRead::PathTouch { id, .. } =>
                self.open.snapshot.parent_point_release_guard(relation, id, self.open.retained.basis.system_time).await?,
            AncestorRead::Identities { .. } | AncestorRead::OverlayIdentities { .. }
                | AncestorRead::Range { .. } | AncestorRead::Changes { .. }
                | AncestorRead::Clock(_) | AncestorRead::SelectClock(_) =>
                self.open.snapshot.relation_release_guard([relation])?,
        };
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::Ancestor {
            relation: relation.clone(), read, selected, schema, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let read = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        guard()?;
        self.check_relation(relation)?;
        Ok((read, witness))
    }

    async fn ancestor_point(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Timestamp, observed: bool,
    ) -> Result<WorldPoint> {
        let (result, witness) = self.read_ancestor(relation, AncestorRead::Point {
            id: id.clone(), valid: Interval::at(valid)?, prove_winner: false,
        }).await?;
        let AncestorResult::Point(fact, shadowed, _) = result else { return Err(OwnedWorld::unavailable()); };
        Ok(WorldPoint { present: self.present(), fact, until: None, shadowed, witness: observed.then_some(witness) })
    }

    pub async fn parent_point(
        &self, relation: &OwnedRelation, id: &EntityId, valid_at: Timestamp, observed: bool,
        original_scope: Option<PlacedAttemptScope>,
    ) -> Result<WorldPoint> {
        if relation.name() == rtdb_types::SHARED_COLUMN_CARRIERS {
            let unavailable = OwnedWorld::unavailable;
            if !matches!(id, EntityId::Binary(key) if SharedColumnCarrierPrefix::of_identity(key).is_some()) {
                return Err(unavailable());
            }
            let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
            self.require_original_carrier_scope(&witness, original_scope).await?;
            let schema = self.shared_root_schema(relation)?;
            let fact = self.archive_point_checked(relation, id, valid_at, None, Some(schema)).await?;
            self.check_parent_point(relation, None)?;
            return Ok(WorldPoint { present: self.present(), fact, until: None, shadowed: false,
                witness: observed.then_some(witness) });
        }
        if relation.name() == ROOT_ROW_CARRIERS {
            let unavailable = OwnedWorld::unavailable;
            if !matches!(id, EntityId::Binary(key) if RootRowCarrier::parse(key).is_some()) {
                return Err(unavailable());
            }
            let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
            self.require_original_carrier_scope(&witness, original_scope).await?;
            let schema = self.world_point_schema(relation)?;
            let fact = self.archive_point_checked(relation, id, valid_at, None, Some(schema)).await?;
            self.check_parent_point(relation, None)?;
            return Ok(WorldPoint { present: self.present(), fact, until: None, shadowed: false,
                witness: observed.then_some(witness) });
        }
        if self.ancestor().is_some() {
            let mut point = self.ancestor_point(relation, id, valid_at, observed).await?;
            // This route asks for the alias's inherited image.
            point.shadowed = false;
            return Ok(point);
        }
        self.root_parent_point(relation, id, valid_at, observed).await
    }

    pub async fn ancestor_root_point(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Timestamp,
    ) -> Result<WorldPoint> {
        if self.ancestor().is_none() { return Err(OwnedWorld::unavailable()); }
        self.historical_parent_schema(relation)?;
        if self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?.purge_frontier() != 0 {
            return Err(OwnedWorld::unavailable());
        }
        self.root_parent_point(relation, id, valid, false).await
    }

    async fn root_parent_point(
        &self, relation: &OwnedRelation, id: &EntityId, valid_at: Timestamp, observed: bool,
    ) -> Result<WorldPoint> {
        self.check()?;
        let policy = self.open.snapshot.lifetime_point_policy(relation).await?;
        let lifetime = policy.map(|policy| ArchiveLifetimeRead { policy, present: self.present().system_time });
        let basis = self.parent_basis().system_time;
        if lifetime.is_some() && (self.clock_mode() != WorldClockMode::Retained || valid_at != basis) {
            return Err(OwnedWorld::unavailable());
        }
        let witness = if observed {
            if lifetime.is_some() || self.evidence_until() <= self.present().system_time {
                return Err(OwnedWorld::unavailable());
            }
            Some(self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?)
        } else {
            None
        };
        let fact = self.archive_point(relation, id, valid_at, lifetime).await?;
        let until = fact.as_ref().zip(policy).map(|(fact, policy)| {
            let validity = fact.lifetime.filter(|valid| fact.revision <= basis && valid.contains(basis))
                .ok_or_else(OwnedWorld::unavailable)?;
            if validity.lower() <= policy.expired_created_through {
                return Err(expired_lifetime());
            }
            let expiry = validity.upper();
            if expiry <= self.present().system_time {
                return Err(expired_lifetime());
            }
            Ok(expiry)
        }).transpose()?;
        self.check_parent_point(relation, until)?;
        Ok(WorldPoint { present: self.present(), fact, until, shadowed: false, witness })
    }

    pub fn check_parent_point(&self, relation: &OwnedRelation, until: Option<Timestamp>) -> Result<()> {
        self.check()?;
        self.open.snapshot.check_live_point_relation(relation)?;
        if until.is_some_and(|until| until <= now()) {
            return Err(expired_lifetime());
        }
        Ok(())
    }

    async fn archive_point(&self, relation: &OwnedRelation, id: &EntityId, valid_at: Timestamp, lifetime: Option<ArchiveLifetimeRead>) -> Result<Option<WorldFact>> {
        self.archive_point_checked(relation, id, valid_at, lifetime, None).await
    }

    async fn archive_point_checked(
        &self, relation: &OwnedRelation, id: &EntityId, valid_at: Timestamp,
        lifetime: Option<ArchiveLifetimeRead>, expected_schema: Option<&rtdb_types::OwnedSchema>,
    ) -> Result<Option<WorldFact>> {
        let valid = Interval::at(valid_at)?;
        let guard = self.open.snapshot.parent_point_release_guard(relation, id, self.open.retained.basis.system_time).await?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::Point(ArchivePoint {
            relation: relation.clone(), id: id.clone(), valid, lifetime, expected_schema: expected_schema.cloned(), guard, reply,
        }))).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?
    }
    pub async fn ranked_search(
        &self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, staged: Option<rtdb_node::TextShadows<'_>>,
    ) -> Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)> {
        if staged.is_some() && (self.clock_mode() != WorldClockMode::Proposal || valid != self.present().system_time) {
            return Err(OwnedWorld::unavailable());
        }
        self.ranked_search_on_cut(partitions, query, offset, limit, valid, None, None, staged).await
    }

    pub async fn ranked_search_at(
        &self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp, parent_only: bool,
    ) -> Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)> {
        self.ranked_search_on_cut(partitions, query, offset, limit, valid, Some((system, parent_only)), None, None).await
    }

    pub async fn ranked_search_parent_at(
        &self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp,
    ) -> Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)> {
        self.ranked_search_on_cut(partitions, query, offset, limit, valid, None, Some(system), None).await
    }

    async fn ranked_search_on_cut(
        &self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, earlier: Option<(Timestamp, bool)>, parent_at: Option<Timestamp>,
        staged: Option<rtdb_node::TextShadows<'_>>,
    ) -> Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)> {
        self.check()?;
        if offset > 4096 || !(1..=128).contains(&limit) || !(1..=32).contains(&partitions.len())
            || query.len() > 4096
            || partitions.iter().map(|(relation, _)| relation).collect::<std::collections::BTreeSet<_>>().len() != partitions.len()
        {
            return Err(OwnedWorld::unavailable());
        }
        Interval::at(valid)?;
        if let Some((system, parent_only)) = earlier {
            if (parent_only && system != self.parent_basis().system_time)
                || (!parent_only && !self.clock_mode().is_writable() && self.ancestor().is_none())
                || partitions.iter().any(|(relation, _)| self.world_point_schema(relation).is_err())
            {
                return Err(OwnedWorld::unavailable());
            }
            let relations = partitions.iter().map(|(relation, _)| relation.clone()).collect::<Vec<_>>();
            self.require_accepted_archive_scope(&relations).await?;
        }
        let guard = self.open.snapshot.relation_release_guard(partitions.iter().map(|(relation, _)| relation))?;
        let witnesses = if earlier.is_some() || parent_at.is_some() {
            let mut witnesses = Vec::with_capacity(partitions.len());
            for (relation, _) in &partitions {
                witnesses.push(self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?);
            }
            witnesses
        } else {
            self.open.snapshot.world_relation_witnesses(
                self.open.record.id, partitions.iter().map(|(relation, _)| relation),
            ).await?
        };
        let frontiers = witnesses.iter().map(OwnedWorldWitness::purge_frontier).collect::<Vec<_>>();
        let parent_at = if let Some(system) = parent_at {
            if system > self.parent_basis().system_time || frontiers.iter().any(|frontier| *frontier != 0) {
                return Err(OwnedWorld::unavailable());
            }
            let schemas = partitions.iter().map(|(relation, _)| {
                Ok((relation.clone(), self.world_point_schema(relation)?.clone()))
            }).collect::<Result<Vec<_>>>()?;
            Some((system, schemas))
        } else { None };
        let ancestor_system = self.frozen_system(earlier.filter(|(_, parent_only)| !parent_only).map(|(system, _)| system));
        let ancestor_schemas = if self.ancestor().is_some() && parent_at.is_none()
            && !earlier.is_some_and(|(_, parent_only)| parent_only)
        {
            if frontiers.iter().any(|frontier| *frontier != 0) {
                return Err(OwnedWorld::unavailable());
            }
            Some(partitions.iter().map(|(relation, _)| {
                self.world_point_schema(relation).cloned()
            }).collect::<Result<Vec<_>>>()?)
        } else {
            None
        };
        let historical = parent_at.is_some();
        let writable = self.clock_mode().is_writable();
        let use_overlay = writable && !historical
            && !earlier.is_some_and(|(system, parent_only)| parent_only || self.uses_frozen_overlay(Some(system)));
        let (columns, analyzer) = if use_overlay || earlier.is_some() {
            self.search_columns(&partitions, &frontiers, &guard).await?
        } else {
            (Vec::new(), rtdb_plan::Analyzer::Simple)
        };
        let overlay = if use_overlay {
            let memory = Arc::clone(&self.memory);
            let corpus = match staged {
                Some(staged) => staged.into_corpus(memory, analyzer, &query, || self.check())?,
                None => rtdb_node::HeldTextCorpus::new(memory, analyzer, &query)?,
            };
            Some(self.search_overlay(&partitions, &columns, valid, corpus, &guard,
                earlier.map(|(system, _)| system)).await?)
        } else {
            None
        };
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::Search {
            inherited_rank: self.inherited_root_rank_cut().await?,
            partitions, frontiers, query, overlay, historical: earlier.is_some() || historical,
            ancestor_schemas, ancestor_system, offset, limit, valid, parent_at, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let page = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        self.check()?;
        guard()?;
        Ok((page, witnesses))
    }

    async fn search_overlay(
        &self, partitions: &[(OwnedRelation, OwnedRelation)], columns: &[String], valid: Timestamp,
        mut corpus: rtdb_node::HeldTextCorpus,
        guard: &Arc<dyn Fn() -> Result<()> + Send + Sync>, system: Option<Timestamp>,
    ) -> Result<rtdb_node::HeldTextCorpus> {
        let mut remaining = 4096usize;
        for (partition, ((relation, _), column)) in partitions.iter().zip(columns).enumerate() {
            let cut = if let Some(system) = system {
                Some(self.open.snapshot.certify_world_accepted_cut(&self.open.record, relation, system).await?)
            } else {
                None
            };
            let mut after = None::<EntityId>;
            loop {
                guard()?;
                let scan = after.as_ref().map(OwnedIdentityScan::seek).transpose()?.unwrap_or(OwnedIdentityScan::Unbounded);
                // This is the physical shadow relation: its typed tombstones
                // are still present identities and also mask the archived row.
                let candidates = if let Some(cut) = &cut {
                    cut.overlay_identities(valid, &scan, 128).await?
                } else {
                    self.open.snapshot.world_identity_candidates(
                        self.open.record.id, relation, valid, &scan, 128,
                    ).await?
                };
                if cut.is_some() || self.open.record.schemas.relation(relation)
                    .is_some_and(|schema| relation.admits_world_ciphertext_points(schema))
                {
                    remaining = remaining.checked_sub(candidates.keys.len()).ok_or_else(OwnedWorld::unavailable)?;
                }
                for key in &candidates.keys {
                    let id = key.clone();
                    if corpus.contains_shadow(partition, &id) { continue; }
                    let fact = self.search_shadow(relation, &id, valid, cut.as_ref()).await?;
                    self.projections.shadow_text(relation, column, fact.as_ref(), id, partition, &mut corpus, &self.memory, system.is_some()).await?;
                }
                if candidates.keys.len() < 128 { break; }
                after = Some(candidates.keys.last().cloned().ok_or_else(OwnedWorld::unavailable)?);
            }
        }
        guard()?;
        Ok(corpus)
    }

    pub async fn nearest(
        &self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, staged: Option<rtdb_node::nearest::HeldVectorOverlay>,
    ) -> Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)> {
        if staged.is_some() && (self.clock_mode() != WorldClockMode::Proposal || valid != self.present().system_time) {
            return Err(OwnedWorld::unavailable());
        }
        self.nearest_on_cut(partitions, query, valid, None, None, staged).await
    }

    pub async fn nearest_at(
        &self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, system: Timestamp, parent_only: bool,
    ) -> Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)> {
        self.nearest_on_cut(partitions, query, valid, Some((system, parent_only)), None, None).await
    }

    pub async fn nearest_parent_at(
        &self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, system: Timestamp,
    ) -> Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)> {
        self.nearest_on_cut(partitions, query, valid, None, Some(system), None).await
    }

    /// The exact cosine ranking of the effective world view at one cut. Local
    /// world rows shadow their retained parent rows, as in the text search
    /// corpus, before the archived parent is ranked.
    async fn nearest_on_cut(
        &self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, earlier: Option<(Timestamp, bool)>, parent_at: Option<Timestamp>,
        staged: Option<rtdb_node::nearest::HeldVectorOverlay>,
    ) -> Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)> {
        self.check()?;
        if !(1..=32).contains(&partitions.len())
            || partitions.iter().map(|partition| &partition.relation).collect::<std::collections::BTreeSet<_>>().len() != partitions.len()
            || partitions.iter().any(|partition| partition.filters.len() != query.filter_count())
        {
            return Err(OwnedWorld::unavailable());
        }
        Interval::at(valid)?;
        let relations = partitions.iter().map(|partition| partition.relation.clone()).collect::<Vec<_>>();
        if let Some((system, parent_only)) = earlier {
            if (parent_only && system != self.parent_basis().system_time)
                || (!parent_only && !self.clock_mode().is_writable() && self.ancestor().is_none())
                || relations.iter().any(|relation| self.world_point_schema(relation).is_err())
            {
                return Err(OwnedWorld::unavailable());
            }
            self.require_accepted_archive_scope(&relations).await?;
        }
        let guard = self.open.snapshot.relation_release_guard(relations.iter())?;
        let witnesses = if earlier.is_some() || parent_at.is_some() {
            let mut witnesses = Vec::with_capacity(relations.len());
            for relation in &relations {
                witnesses.push(self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?);
            }
            witnesses
        } else {
            self.open.snapshot.world_relation_witnesses(self.open.record.id, relations.iter()).await?
        };
        let frontiers = witnesses.iter().map(OwnedWorldWitness::purge_frontier).collect::<Vec<_>>();
        let parent_at = if let Some(system) = parent_at {
            if system > self.parent_basis().system_time || frontiers.iter().any(|frontier| *frontier != 0) {
                return Err(OwnedWorld::unavailable());
            }
            let schemas = relations.iter().map(|relation| {
                Ok((relation.clone(), self.world_point_schema(relation)?.clone()))
            }).collect::<Result<Vec<_>>>()?;
            Some((system, schemas))
        } else { None };
        let ancestor_system = self.frozen_system(earlier.filter(|(_, parent_only)| !parent_only).map(|(system, _)| system));
        let ancestor_schemas = if self.ancestor().is_some() && parent_at.is_none()
            && !earlier.is_some_and(|(_, parent_only)| parent_only)
        {
            if frontiers.iter().any(|frontier| *frontier != 0) {
                return Err(OwnedWorld::unavailable());
            }
            Some(relations.iter().map(|relation| self.world_point_schema(relation).cloned())
                .collect::<Result<Vec<_>>>()?)
        } else {
            None
        };
        let historical = earlier.is_some() || parent_at.is_some();
        let query = query.exact();
        let writable = self.clock_mode().is_writable();
        let use_overlay = writable && parent_at.is_none()
            && !earlier.is_some_and(|(system, parent_only)| parent_only || self.uses_frozen_overlay(Some(system)));
        let overlay = if use_overlay {
            let shadows = match staged {
                Some(staged) => staged,
                None => rtdb_node::nearest::HeldVectorOverlay::new(&self.memory)?,
            };
            Some(self.nearest_overlay(&partitions, &query, valid, shadows, &guard, earlier.map(|(system, _)| system)).await?)
        } else {
            None
        };
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::Nearest {
            inherited_rank: self.inherited_root_rank_cut().await?,
            partitions, frontiers, query, overlay, historical,
            ancestor_schemas, ancestor_system, valid, parent_at, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let page = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        self.check()?;
        guard()?;
        Ok((page, witnesses))
    }

    /// Shadow every local world identity with its local row, mirroring
    /// `search_overlay`: typed tombstones mask the archived row.
    async fn nearest_overlay(
        &self, partitions: &[rtdb_node::nearest::NearestPartition], query: &rtdb_node::nearest::NearestQuery, valid: Timestamp,
        mut shadows: rtdb_node::nearest::HeldVectorOverlay,
        guard: &Arc<dyn Fn() -> Result<()> + Send + Sync>, system: Option<Timestamp>,
    ) -> Result<rtdb_node::nearest::HeldVectorOverlay> {
        let mut remaining = 4096usize;
        for (partition, selected) in partitions.iter().enumerate() {
            let relation = &selected.relation;
            let cut = if let Some(system) = system {
                Some(self.open.snapshot.certify_world_accepted_cut(&self.open.record, relation, system).await?)
            } else {
                None
            };
            let mut after = None::<EntityId>;
            loop {
                guard()?;
                let scan = after.as_ref().map(OwnedIdentityScan::seek).transpose()?.unwrap_or(OwnedIdentityScan::Unbounded);
                let candidates = if let Some(cut) = &cut {
                    cut.overlay_identities(valid, &scan, 128).await?
                } else {
                    self.open.snapshot.world_identity_candidates(
                        self.open.record.id, relation, valid, &scan, 128,
                    ).await?
                };
                if cut.is_some() || self.open.record.schemas.relation(relation)
                    .is_some_and(|schema| relation.admits_world_ciphertext_points(schema))
                {
                    remaining = remaining.checked_sub(candidates.keys.len()).ok_or_else(OwnedWorld::unavailable)?;
                }
                for key in &candidates.keys {
                    let id = key.clone();
                    if shadows.contains_shadow(partition, &id) { continue; }
                    let fact = self.search_shadow(relation, &id, valid, cut.as_ref()).await?;
                    self.projections.shadow_nearest(selected, query, fact.as_ref(), id, partition, &mut shadows, &self.memory, system.is_some()).await?;
                }
                if candidates.keys.len() < 128 { break; }
                after = Some(candidates.keys.last().cloned().ok_or_else(OwnedWorld::unavailable)?);
            }
        }
        guard()?;
        Ok(shadows)
    }

    async fn search_shadow(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Timestamp, cut: Option<&rtdb_node::OwnedWorldAcceptedCut<'_>>,
    ) -> Result<Option<WorldFact>> {
        if let Some(cut) = cut {
            let shadow = cut.shadow(id, valid).await?;
            let fact = decode_point(shadow.row.into_iter().collect(), id, &self.memory, None)?
                .ok_or_else(OwnedWorld::unavailable)?;
            fact.into_shadow(self.open.record.id)
        } else {
            let (shadow, _) = self.open.read_local(relation, id, valid, &self.memory, false).await?;
            shadow.ok_or_else(OwnedWorld::unavailable)
        }
    }

    fn world_point_schema(&self, relation: &OwnedRelation) -> Result<&rtdb_types::OwnedSchema> {
        self.open.record.schemas.relation(relation)
            .filter(|schema| relation.is_root_data() && schema.lifetime_micros().is_none()
                && (!schema.has_opaque_columns() || relation.admits_world_ciphertext_points(schema)))
            .ok_or_else(OwnedWorld::unavailable)
    }

    async fn search_columns(
        &self, partitions: &[(OwnedRelation, OwnedRelation)], frontiers: &[i64],
        guard: &Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> Result<(Vec<String>, rtdb_plan::Analyzer)> {
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::SearchColumns {
            partitions: partitions.to_vec(), frontiers: frontiers.to_vec(), guard: Arc::clone(guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let (columns, analyzer) = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        if columns.len() != partitions.len() || partitions.iter().zip(&columns).any(|((relation, _), column)| {
            !self.open.record.schemas.relation(relation)
                .and_then(|schema| schema.search_column_logical_type(column)).is_some()
        }) {
            return Err(OwnedWorld::unavailable());
        }
        Ok((columns, analyzer))
    }

    pub async fn range(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'_>, valid: Timestamp,
    ) -> Result<(OwnedRangePage, OwnedWorldWitness)> {
        let read = OrderedRead::Page {
            after: request.after.map(OwnedKeyBound::retained),
            descending: request.descending, limit: request.limit,
        };
        let (result, witness) = self.ordered(relation, path, request.lower, request.upper, valid, read, None).await?;
        let OrderedResult::Page(page) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((page, witness))
    }

    pub async fn overlay_range(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'_>, valid: Timestamp,
    ) -> Result<(OwnedRangePage, OwnedWorldWitness)> {
        self.check_relation(&relation)?;
        if self.uses_frozen_overlay(None) {
            return self.ancestor_overlay_range(&relation, path, request, valid, None).await;
        }
        if !self.clock_mode().is_writable() { return Err(OwnedWorld::unavailable()); }
        let world = self.open.record.id;
        let witness = self.open.snapshot.world_relation_witnesses(world, [&relation])
            .await?.pop().ok_or_else(OwnedWorld::unavailable)?;
        self.require_overlay_coverage(&relation, &witness).await?;
        let page = if self.ancestor().is_some() {
            self.merged_overlay_range(&relation, &path, request, valid, None).await?
        } else {
            self.open.snapshot.range_world_overlay(world, &relation, &path, request, valid).await?
        };
        self.check_relation(&relation)?;
        Ok((page, witness))
    }

    pub async fn overlay_range_at(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'_>,
        valid: Timestamp, system: Timestamp,
    ) -> Result<OwnedRangePage> {
        if self.uses_frozen_overlay(Some(system)) {
            return self.ancestor_overlay_range(&relation, path, request, valid, Some(system))
                .await.map(|(page, _)| page);
        }
        self.check_relation(&relation)?;
        let cut = self.open.snapshot.certify_world_accepted_cut(&self.open.record, &relation, system).await?;
        self.require_accepted_archive_scope(std::slice::from_ref(&relation)).await?;
        let page = if self.ancestor().is_some() {
            self.merged_overlay_range(&relation, &path, request, valid, Some(&cut)).await?
        } else {
            cut.overlay_range(&path, request, valid).await?
        };
        let columns = page.schema.fields().iter().map(|field| field.name().clone()).collect::<Vec<_>>();
        self.require_parent_path(&relation, &path, &columns, None).await?;
        self.check_relation(&relation)?;
        Ok(page)
    }

    async fn ancestor_overlay_range(
        &self, relation: &OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'_>,
        valid: Timestamp, selected: Option<Timestamp>,
    ) -> Result<(OwnedRangePage, OwnedWorldWitness)> {
        let (result, witness) = self.read_ancestor_at(relation, AncestorRead::Range {
            path,
            lower: request.lower.map(OwnedKeyBound::retained),
            upper: request.upper.map(OwnedKeyBound::retained),
            after: request.after.map(OwnedKeyBound::retained), descending: request.descending,
            limit: request.limit, valid,
        }, selected).await?;
        let AncestorResult::Range(page) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((page, witness))
    }

    async fn require_overlay_coverage(&self, relation: &OwnedRelation, witness: &OwnedWorldWitness) -> Result<()> {
        let guard = self.open.snapshot.relation_release_guard([relation])?;
        self.require_archive_coverage(vec![(relation.clone(), witness.purge_frontier())], guard).await
    }

    pub async fn count(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'_>,
    ) -> Result<(u64, OwnedWorldWitness)> {
        let (result, witness) = self.ordered(relation, path, request.lower, request.upper, request.valid_at, OrderedRead::Count, None).await?;
        let OrderedResult::Count(count) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((count, witness))
    }

    pub async fn measure(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'_>,
    ) -> Result<(OwnedRangeCount, OwnedWorldWitness)> {
        if self.clock_mode().is_writable() { return Err(OwnedWorld::unavailable()); }
        let (result, witness) = self.ordered(
            relation, path, request.lower, request.upper, request.valid_at, OrderedRead::Measure, None,
        ).await?;
        let OrderedResult::Measure(measured) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((measured, witness))
    }

    pub async fn parent_range_at(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'_>,
        valid: Timestamp, system: Timestamp,
    ) -> Result<(OwnedRangePage, OwnedWorldWitness)> {
        let read = OrderedRead::Page {
            after: request.after.map(OwnedKeyBound::retained),
            descending: request.descending, limit: request.limit,
        };
        let (result, witness) = self.ordered(relation, path, request.lower, request.upper, valid, read, Some(system)).await?;
        let OrderedResult::Page(page) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((page, witness))
    }

    pub async fn parent_count_at(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'_>, system: Timestamp,
    ) -> Result<(u64, OwnedWorldWitness)> {
        let (result, witness) = self.ordered(
            relation, path, request.lower, request.upper, request.valid_at, OrderedRead::Count, Some(system),
        ).await?;
        let OrderedResult::Count(count) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((count, witness))
    }

    async fn ordered(
        &self, relation: OwnedRelation, path: OwnedRelation, lower: Option<OwnedKeyBound<'_>>,
        upper: Option<OwnedKeyBound<'_>>, valid: Timestamp, read: OrderedRead, system: Option<Timestamp>,
    ) -> Result<(OrderedResult, OwnedWorldWitness)> {
        self.check_relation(&relation)?;
        Interval::at(valid)?;
        let guard = self.open.snapshot.relation_release_guard([&relation])?;
        if self.ancestor().is_some() {
            self.historical_parent_schema(&relation)?;
            self.require_accepted_archive_scope(std::slice::from_ref(&relation)).await?;
        }
        let (witness, earlier) = if let Some(system) = system {
            let schema = self.historical_parent_schema(&relation)?;
            if system > self.parent_basis().system_time { return Err(OwnedWorld::unavailable()); }
            let witness = self.open.snapshot.world_parent_witness(self.open.record.id, &relation).await?;
            if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
            (witness, Some((system, schema.clone())))
        } else {
            let witness = self.open.snapshot.world_relation_witnesses(self.open.record.id, [&relation])
                .await?.pop().ok_or_else(OwnedWorld::unavailable)?;
            (witness, None)
        };
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::Ordered {
            relation, path, frontier: witness.purge_frontier(),
            lower: lower.map(OwnedKeyBound::retained),
            upper: upper.map(OwnedKeyBound::retained),
            valid, earlier, read, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let result = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        self.check()?;
        guard()?;
        Ok((result, witness))
    }

    pub async fn version(
        &self, relation: &OwnedRelation, id: &EntityId, from: Timestamp, through: Timestamp, group: Timestamp,
    ) -> Result<(Timestamp, Vec<(Interval, WorldFact, bool)>)> {
        if self.uses_frozen_overlay(Some(group)) {
            let (result, _) = self.read_ancestor(relation, AncestorRead::Version {
                id: id.clone(), from, through: self.ancestor().map_or(through, |(_, cap)| through.min(cap)), group,
            }).await?;
            let AncestorResult::Version(clock, spans) = result else { return Err(OwnedWorld::unavailable()); };
            return Ok((clock, spans));
        }
        self.check_relation(relation)?;
        let schema = self.open.record.schemas.relation(relation).ok_or_else(OwnedWorld::unavailable)?;
        if schema.has_opaque_columns() || schema.lifetime_micros().is_some() {
            return Err(OwnedWorld::unavailable());
        }
        let guard = self.open.snapshot.relation_release_guard([relation])?;
        self.require_archive_coverage(vec![(relation.clone(), 0)], Arc::clone(&guard)).await?;
        self.open.snapshot.check_point_purge_floor(relation, id, self.parent_basis().system_time).await?;
        let clock = self.open.snapshot.certify_world_accepted_scope(
            &self.open.record, std::slice::from_ref(relation), group,
        ).await?;
        let version = self.open.snapshot.world_version(self.open.record.id, relation, id, from, through, group).await?;
        let valid = Interval::ALL;
        let local = read_boundaries(version.boundaries().await?, valid).await?;
        let parent = self.inherited_boundaries(relation, id, valid, Some(schema.clone())).await?;
        let boundaries = merge_boundaries(parent, local, valid)?;
        let mut bytes = 0usize;
        let mut slices = Vec::with_capacity(boundaries.len().saturating_sub(1));
        let _framing = self.memory.reserve(boundaries.len().saturating_mul(256))?;
        for window in boundaries.windows(2) {
            guard()?;
            let local = version.shadow_at(window[0]).await?;
            let local = decode_point(local.row.into_iter().collect(), id, &self.memory, None)?;
            let local = match local {
                Some(fact) => Some(self.open.local_fact(relation, fact).await?),
                None => None,
            };
            let (fact, shadowed) = self.resolve_local(relation, id, window[0], local, Some(schema)).await?;
            if let Some(fact) = fact {
                if fact.revision > group { return Err(OwnedWorld::unavailable()); }
                fact.validate(&schema)?;
                bytes = bytes.checked_add(fact.document_bytes()?)
                    .filter(|bytes| *bytes <= 8 << 20).ok_or_else(history_limit)?;
                slices.push((Interval::new(window[0], window[1])?, fact, shadowed));
            }
        }
        guard()?;
        self.check_relation(relation)?;
        Ok((clock, slices))
    }

    async fn inherited_boundaries(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Interval, schema: Option<rtdb_types::OwnedSchema>,
    ) -> Result<Vec<Timestamp>> {
        if self.ancestor().is_none() {
            return self.archive_boundaries(relation, id, valid, schema).await;
        }
        let (result, _) = self.read_ancestor(relation, AncestorRead::Schedule { id: id.clone(), valid }).await?;
        let AncestorResult::Schedule(timeline) = result else { return Err(OwnedWorld::unavailable()); };
        Ok(timeline.into_iter().flat_map(|(span, _)| [span.lower(), span.upper()]).collect())
    }

    async fn archive_boundaries(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Interval, expected_schema: Option<rtdb_types::OwnedSchema>,
    ) -> Result<Vec<Timestamp>> {
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::Boundaries {
            relation: relation.clone(), id: id.clone(), valid, expected_schema,
            guard: self.open.snapshot.relation_release_guard([relation])?, reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?
    }

    pub async fn valid_during(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Interval, parent_only: bool,
    ) -> Result<(Vec<(Interval, Timestamp)>, Option<OwnedWorldWitness>)> {
        self.valid_during_on_cut(relation, id, valid, parent_only, None).await
    }

    pub async fn valid_during_at(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Interval, system: Timestamp,
    ) -> Result<Vec<(Interval, Timestamp)>> {
        let (visible, _) = self.valid_during_on_cut(relation, id, valid, false, Some(system)).await?;
        Ok(visible)
    }

    async fn valid_during_on_cut(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Interval,
        parent_only: bool, system: Option<Timestamp>,
    ) -> Result<(Vec<(Interval, Timestamp)>, Option<OwnedWorldWitness>)> {
        self.check_relation(relation)?;
        if valid.is_empty() { return Err(OwnedWorld::unavailable()); }
        if self.ancestor().is_some() && (parent_only || self.uses_frozen_overlay(system)) {
            let (result, witness) = self.read_ancestor_at(relation, AncestorRead::Schedule { id: id.clone(), valid }, self.frozen_system(system)).await?;
            let AncestorResult::Schedule(timeline) = result else { return Err(OwnedWorld::unavailable()); };
            return Ok((timeline, Some(witness)));
        }
        if parent_only && self.open.snapshot.lifetime_point_policy(relation).await?.is_some() {
            return Err(OwnedWorld::unavailable());
        }
        self.open.snapshot.check_point_purge_floor(relation, id, self.open.retained.basis.system_time).await?;
        let earlier = if let Some(system) = system {
            let cut = self.open.snapshot.certify_world_accepted_cut(&self.open.record, relation, system).await?;
            let schema = self.open.record.schemas.relation(relation).ok_or_else(OwnedWorld::unavailable)?;
            let guard = self.open.snapshot.relation_release_guard([relation])?;
            self.require_archive_coverage(vec![(relation.clone(), 0)], guard).await?;
            Some((cut, schema))
        } else {
            None
        };
        let local = if let Some((cut, _)) = &earlier {
            read_boundaries(cut.boundaries(id, valid).await?, valid).await?
        } else if parent_only {
            Vec::new()
        } else {
            let scan = self.open.snapshot.open_world_shadow_boundaries(self.open.record.id, relation, id, valid).await?;
            read_boundaries(scan, valid).await?
        };
        let parent = self.inherited_boundaries(relation, id, valid, earlier.as_ref().map(|(_, schema)| (**schema).clone())).await?;
        let boundaries = merge_boundaries(parent, local, valid)?;
        let mut visible = Vec::with_capacity(boundaries.len().saturating_sub(1));
        for window in boundaries.windows(2) {
            let fact = if let Some((cut, schema)) = &earlier {
                self.point_at_accepted_cut(relation, id, window[0], cut, schema).await?.0
            } else if parent_only {
                self.archive_point(relation, id, window[0], None).await?
            } else {
                self.point(relation, id, window[0], false).await?.fact
            };
            if let Some(fact) = fact {
                visible.push((Interval::new(window[0], window[1])?, fact.revision));
            }
        }
        let witness = if earlier.is_none() && self.evidence_until() > self.present().system_time {
            Some(if parent_only {
                self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?
            } else {
                self.open.snapshot.world_relation_witnesses(self.open.record.id, [relation])
                    .await?.pop().ok_or_else(OwnedWorld::unavailable)?
            })
        } else {
            None
        };
        self.check_relation(relation)?;
        Ok((visible, witness))
    }

    pub async fn identity_page(
        &self, relation: &OwnedRelation, valid: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize,
    ) -> Result<WorldIdentityPage> {
        if self.uses_frozen_overlay(None) {
            Interval::at(valid)?;
            if !(1..=128).contains(&limit) { return Err(OwnedWorld::unavailable()); }
            let (result, _) = self.read_ancestor(relation, AncestorRead::Identities {
                valid, after: after.clone().into_owned(), limit,
            }).await?;
            let AncestorResult::Identities(page) = result else { return Err(OwnedWorld::unavailable()); };
            return Ok(page);
        }
        self.identity_page_on_cut(relation, valid, after, limit, None).await
    }

    pub async fn identity_page_at(
        &self, relation: &OwnedRelation, valid: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize,
        system: Timestamp, parent_only: bool,
    ) -> Result<WorldIdentityPage> {
        self.check_relation(relation)?;
        Interval::at(valid)?;
        if !(1..=128).contains(&limit) || !relation.is_root_data() ||
            self.open.record.schemas.relation(relation)
                .is_none_or(|schema| schema.has_opaque_columns() || schema.lifetime_micros().is_some())
        {
            return Err(OwnedWorld::unavailable());
        }
        if self.uses_frozen_overlay(Some(system)) && !parent_only {
            let (result, _) = self.read_ancestor_at(relation, AncestorRead::Identities {
                valid, after: after.clone().into_owned(), limit,
            }, Some(system)).await?;
            let AncestorResult::Identities(page) = result else { return Err(OwnedWorld::unavailable()); };
            return Ok(page);
        }
        if parent_only {
            if system != self.parent_basis().system_time { return Err(OwnedWorld::unavailable()); }
            let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
            self.require_overlay_coverage(relation, &witness).await?;
            self.require_accepted_archive_scope(std::slice::from_ref(relation)).await?;
            let parent = self.archive_identity_candidates(relation, valid, after, limit + 1, None, None).await?;
            for id in &parent.keys {
                self.open.snapshot.check_point_purge_floor(relation, id, system).await?;
            }
            return self.finish_overlay_identities(relation, parent, limit);
        }
        let cut = self.open.snapshot.certify_world_accepted_cut(&self.open.record, relation, system).await?;
        self.require_accepted_archive_scope(std::slice::from_ref(relation)).await?;
        self.identity_page_on_cut(relation, valid, after, limit, Some(&cut)).await
    }

    pub async fn parent_identity_page_at(
        &self, relation: &OwnedRelation, valid: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize, system: Timestamp,
    ) -> Result<WorldIdentityPage> {
        self.check_relation(relation)?;
        Interval::at(valid)?;
        if !(1..=128).contains(&limit) || system > self.parent_basis().system_time {
            return Err(OwnedWorld::unavailable());
        }
        let schema = self.historical_parent_schema(relation)?.clone();
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let candidates = self.archive_identity_candidates(relation, valid, after, limit + 1, Some((system, schema)), None).await?;
        for id in &candidates.keys {
            self.open.snapshot.check_point_purge_floor(relation, id, system).await?;
        }
        self.finish_overlay_identities(relation, candidates, limit)
    }

    async fn archive_identity_candidates(
        &self, relation: &OwnedRelation, valid: Timestamp, after: &OwnedIdentityScan<'_>, fetch: usize,
        earlier: Option<(Timestamp, rtdb_types::OwnedSchema)>,
        prefix: Option<SharedColumnCarrierPrefix<'_>>,
    ) -> Result<rtdb_query::OwnedIdentityCandidates> {
        let protected = match prefix {
            Some(prefix) => Some((prefix.as_bytes().to_vec(), self.shared_root_schema(relation)?.clone())),
            None => None,
        };
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.send_archive(ArchiveRead::Identities {
            relation: relation.clone(), at: valid, after: after.clone().into_owned(), fetch, earlier,
            protected,
            guard: self.open.snapshot.relation_release_guard([relation])?, reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?
    }

    async fn identity_page_on_cut(
        &self, relation: &OwnedRelation, valid: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize,
        cut: Option<&rtdb_node::OwnedWorldAcceptedCut<'_>>,
    ) -> Result<WorldIdentityPage> {
        self.check_relation(relation)?;
        Interval::at(valid)?;
        if !(1..=128).contains(&limit) { return Err(OwnedWorld::unavailable()); }
        const FETCH: usize = 129;
        let mut cursor = None::<(EntityId, Arc<MemoryReservation>)>;
        let mut keys = Vec::with_capacity(limit);
        let mut bytes = limit * 128;
        let mut more = false;
        let mut complete = false;
        let mut reservations = vec![self.memory.reserve(bytes)?];
        for _ in 0..32 {
            let next = cursor.as_ref().map(|(key, _)| OwnedIdentityScan::seek(key)).transpose()?;
            let after = next.as_ref().unwrap_or(after);
            let local = if let Some(cut) = cut {
                cut.overlay_identities(valid, after, FETCH).await?
            } else {
                self.open.snapshot.world_identity_candidates(self.open.record.id, relation, valid, after, FETCH).await?
            };
            let (parent, _parent_charges) = if self.ancestor().is_some() {
                let (result, _) = self.read_ancestor(relation, AncestorRead::Identities {
                    valid, after: after.clone().into_owned(), limit: FETCH,
                }).await?;
                let AncestorResult::Identities(page) = result else { return Err(OwnedWorld::unavailable()); };
                (page.keys, page.reservations)
            } else {
                self.archive_identity_candidates(relation, valid, after, FETCH, None, None).await?.into_parts()
            };

            let (candidates, through) = identity_candidates(&parent, &local.keys, FETCH);
            for key in candidates {
                let id = key.clone();
                let shadow = if let Some(cut) = cut {
                    let read = cut.shadow(&id, valid).await?;
                    decode_point(read.row.into_iter().collect(), &id, &self.memory, None)?
                        .map(|fact| fact.into_shadow(self.open.record.id).map(|shadow| shadow.is_some()))
                        .transpose()?
                } else {
                    self.open.read_local(relation, &id, valid, &self.memory, false).await?.0
                        .map(|fact| fact.is_some())
                };
                let visible = match shadow {
                    Some(visible) => visible,
                    None => {
                        if local.keys.binary_search(key).is_ok() || parent.binary_search(key).is_err() {
                            return Err(OwnedWorld::unavailable());
                        }
                        self.open.snapshot.check_point_purge_floor(relation, &id, self.open.retained.basis.system_time).await?;
                        true
                    }
                };
                if visible {
                    if keys.len() == limit {
                        more = true;
                        break;
                    }
                    bytes = bytes.checked_add(key.retained_bytes()).filter(|size| *size <= (8 << 20)).ok_or_else(OwnedWorld::unavailable)?;
                    reservations.push(self.memory.reserve(key.retained_bytes())?);
                    keys.push(key.clone());
                }
            }
            if more || through.is_none() {
                complete = true;
                break;
            }
            if let Some(through) = through {
                let charge = self.memory.reserve(through.retained_bytes())?;
                cursor = Some((through.clone(), charge));
            }
        }
        if !complete { return Err(OwnedWorld::unavailable()); }
        self.check_relation(relation)?;
        Ok(WorldIdentityPage { keys, more, reservations })
    }

    pub async fn overlay_identities(
        &self, relation: &OwnedRelation, valid: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize,
    ) -> Result<WorldIdentityPage> {
        self.check_relation(relation)?;
        if !(1..=128).contains(&limit) { return Err(OwnedWorld::unavailable()); }
        if self.uses_frozen_overlay(None) {
            Interval::at(valid)?;
            let (result, _) = self.read_ancestor(relation, AncestorRead::OverlayIdentities {
                valid, after: after.clone().into_owned(), limit, prefix: None,
            }).await?;
            let AncestorResult::Identities(page) = result else { return Err(OwnedWorld::unavailable()); };
            return Ok(page);
        }
        self.require_overlay_identity_coverage(relation).await?;
        if !self.clock_mode().is_writable() {
            self.check_relation(relation)?;
            return Ok(WorldIdentityPage { keys: Vec::new(), more: false, reservations: Vec::new() });
        }
        let candidates = self.open.snapshot.world_identity_candidates(
            self.open.record.id, relation, valid, after, limit + 1,
        ).await?;
        self.merge_overlay_identities(relation, valid, after, limit, candidates, None).await
    }

    pub(crate) async fn local_overlay_identity_page(
        &self, relation: &OwnedRelation, valid: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize,
    ) -> Result<Option<rtdb_node::OwnedIdentityPageOutput>> {
        if self.ancestor().is_some() || !self.clock_mode().is_writable() || matches!(after, OwnedIdentityScan::Unbounded) {
            return Ok(None);
        }
        self.check_relation(relation)?;
        if !(1..=128).contains(&limit) { return Err(OwnedWorld::unavailable()); }
        self.require_overlay_identity_coverage(relation).await?;
        let page = self.open.snapshot.world_identity_page_output(self.open.record.id, relation, valid, after, limit).await?;
        self.check_relation(relation)?;
        Ok(Some(page))
    }

    async fn require_overlay_identity_coverage(&self, relation: &OwnedRelation) -> Result<()> {
        let witness = self.open.snapshot.world_relation_witnesses(self.open.record.id, [relation])
            .await?.pop().ok_or_else(OwnedWorld::unavailable)?;
        self.require_overlay_coverage(relation, &witness).await
    }

    pub async fn overlay_identities_at(
        &self, relation: &OwnedRelation, valid: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize, system: Timestamp,
    ) -> Result<WorldIdentityPage> {
        self.check_relation(relation)?;
        if !(1..=128).contains(&limit) { return Err(OwnedWorld::unavailable()); }
        if self.uses_frozen_overlay(Some(system)) {
            let (result, _) = self.read_ancestor_at(relation, AncestorRead::OverlayIdentities {
                valid, after: after.clone().into_owned(), limit, prefix: None,
            }, Some(system)).await?;
            let AncestorResult::Identities(page) = result else { return Err(OwnedWorld::unavailable()); };
            return Ok(page);
        }
        let cut = self.open.snapshot.certify_world_accepted_cut(&self.open.record, relation, system).await?;
        self.require_accepted_archive_scope(std::slice::from_ref(relation)).await?;
        let candidates = cut.overlay_identities(valid, after, limit + 1).await?;
        self.merge_overlay_identities(relation, valid, after, limit, candidates, None).await
    }

    async fn merge_overlay_identities(
        &self, relation: &OwnedRelation, valid: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize,
        local: rtdb_query::OwnedIdentityCandidates,
        prefix: Option<&str>,
    ) -> Result<WorldIdentityPage> {
        if self.ancestor().is_none() {
            return self.finish_overlay_identities(relation, local, limit);
        }
        let (result, _) = self.read_ancestor(relation, AncestorRead::OverlayIdentities {
            valid, after: after.clone().into_owned(), limit, prefix: prefix.map(str::to_owned),
        }).await?;
        let AncestorResult::Identities(mut inherited) = result else { return Err(OwnedWorld::unavailable()); };
        let (keys, reservations) = local.into_parts();
        inherited.keys.extend(keys);
        inherited.reservations.extend(reservations);
        inherited.keys.sort_unstable();
        inherited.keys.dedup();
        inherited.more |= inherited.keys.len() > limit;
        inherited.keys.truncate(limit);
        self.check_relation(relation)?;
        Ok(inherited)
    }

    fn finish_overlay_identities(
        &self, relation: &OwnedRelation, candidates: rtdb_query::OwnedIdentityCandidates, limit: usize,
    ) -> Result<WorldIdentityPage> {
        let (mut keys, reservations) = candidates.into_parts();
        let more = keys.len() > limit;
        keys.truncate(limit);
        self.check_relation(relation)?;
        Ok(WorldIdentityPage { keys, more, reservations })
    }

}

fn expired_lifetime() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.owned/lifetime-expired",
        "The fact at this checkpoint has expired")
}

fn search_text<'a>(fact: &'a WorldFact, column: &str) -> Result<Option<&'a str>> {
    search_row_text(&fact.row, column)
}

fn search_row_text<'a>(row: &'a RecordBatch, column: &str) -> Result<Option<&'a str>> {
    let Some(cell) = ArrowCell::named(row, column, 0) else { return Ok(None); };
    let logical = rtdb_columnar::from_field(cell.field)?;
    match rtdb_columnar::owned_text_cell(&logical, cell.field, cell.array, cell.row)? {
        rtdb_columnar::TextCell::Missing => Ok(None),
        rtdb_columnar::TextCell::Utf8(text) => Ok(Some(text)),
        rtdb_columnar::TextCell::Other => Err(OwnedWorld::unavailable()),
    }
}

struct InheritedRelation<'a> {
    root: Arc<rtdb_query::QueryContext>,
    overlay_media: Arc<rtdb_query::QueryContext>,
    overlay: Arc<rtdb_query::QueryContext>,
    owner: &'a str,
    relation: &'a OwnedRelation,
    shadow: OwnedRelation,
    world: Iid,
    initial_clock: Timestamp,
    floor: Timestamp,
    overlay_basis: Timestamp,
    publication: TransactionKey,
    root_basis: TransactionKey,
    earlier: Vec<Self>,
    schema: &'a rtdb_types::OwnedSchema,
    memory: &'a Arc<MemoryBudget>,
}

impl<'a> InheritedRelation<'a> {
    async fn open(
        root: Arc<rtdb_query::QueryContext>, overlays: Vec<Arc<rtdb_query::QueryContext>>, owner: &'a str,
        relation: &'a OwnedRelation, ancestors: &[rtdb_directory::CheckpointWorldAncestor],
        schema: &'a rtdb_types::OwnedSchema, memory: &'a Arc<MemoryBudget>,
    ) -> Result<Self> {
        if overlays.len() != ancestors.len() { return Err(OwnedWorld::unavailable()); }
        let mut writers = Vec::with_capacity(ancestors.len());
        let database = root.catalog.database(root.catalog.default_database())?;
        let [partition] = database.partitions.as_slice() else { return Err(OwnedWorld::unavailable()); };
        let mut floor = partition.tx_basis().ok_or_else(OwnedWorld::unavailable)?;
        for (index, (overlay, ancestor)) in overlays.into_iter().zip(ancestors).enumerate() {
            writers.push(Self::open_writer(
                Arc::clone(&root), overlay, owner, relation, ancestor, &ancestors[..index], floor, schema, memory,
            ).await?);
            floor = ancestor.publication;
        }
        let mut latest = writers.pop().ok_or_else(OwnedWorld::unavailable)?;
        latest.earlier = writers;
        Ok(latest)
    }

    /// Newest first, so the first assertion for an identity wins.
    fn writers(&self) -> impl Iterator<Item = &Self> {
        std::iter::once(self).chain(self.earlier.iter().rev())
    }

    async fn open_writer(
        root: Arc<rtdb_query::QueryContext>, overlay: Arc<rtdb_query::QueryContext>, owner: &'a str,
        relation: &'a OwnedRelation, ancestor: &rtdb_directory::CheckpointWorldAncestor,
        earlier: &[rtdb_directory::CheckpointWorldAncestor], floor: TransactionKey,
        schema: &'a rtdb_types::OwnedSchema, memory: &'a Arc<MemoryBudget>,
    ) -> Result<Self> {
        let unavailable = OwnedWorld::unavailable;
        let world = Iid(*ancestor.world.as_bytes());
        let shadow = relation.in_world(world)?;
        let root_cut = root.current_time.as_micros()?;
        let media_cut = overlay.current_time.as_micros()?;
        if media_cut != ancestor.basis.system_time || floor.tx_id >= ancestor.publication.tx_id
            || !floor.at_or_before(ancestor.publication) {
            return Err(unavailable());
        }
        check_archive_coverage(&root, owner, [(relation, 0)])?;
        check_archive_coverage(&overlay, owner, [(relation, 0), (&shadow, 0)])?;
        require_parent_journal(&root, owner, relation, root_cut).await?;
        require_parent_journal(&overlay, owner, &shadow, floor.system_time).await?;
        let overlay_media = overlay;
        let overlay = overlay_media.with_owned_transaction_cut(ancestor.publication, &overlay_media.cancellation)?;
        let (record, reading, _charge) = rtdb_query::read_held_world_publication(
            &overlay, owner, world, ancestor.publication,
        ).await?;
        if reading != ancestor.logical || !ancestor.publication.at_or_before(ancestor.basis) {
            return Err(unavailable());
        }
        let root_schema = rtdb_query::read_owned_relation_schema(Arc::clone(&root), owner, relation).await?;
        let overlay_schema = rtdb_query::read_owned_relation_schema(Arc::clone(&overlay), owner, relation).await?;
        if record.dropped || record.pin.0 != *ancestor.pin.as_bytes()
            || record.ancestors.len() != earlier.len()
            || record.ancestors.iter().zip(earlier).any(|(registered, retained)| {
                registered.world != *retained.world.as_bytes() || registered.pin != *retained.pin.as_bytes()
                    || registered.logical != retained.logical || registered.source_root != retained.source_root
                    || registered.publication.system_time > retained.publication.system_time
                    || (registered.basis == retained.basis && registered.publication != retained.publication)
            })
            || !matches!(record.mode, WorldClockMode::Held { .. }) || record.schemas.relation(relation) != Some(schema)
            || record.basis != ancestor.source_root || ancestor.source_root.system_time > root_cut
            || !root_schema.is_some_and(|(actual, _)| &actual == schema)
            || !overlay_schema.is_some_and(|(actual, _)| &actual == schema)
        {
            return Err(unavailable());
        }
        Ok(Self {
            root, overlay_media, overlay, owner, relation, shadow, world, initial_clock: record.initial_clock(),
            floor: floor.system_time, overlay_basis: ancestor.publication.system_time,
            publication: ancestor.publication, root_basis: floor, earlier: Vec::new(), schema, memory,
        })
    }

    async fn shadow(&self, id: &EntityId, valid: Interval) -> Result<Option<Option<WorldFact>>> {
        Ok(self.shadow_with_winner(id, valid, false).await?.map(|(fact, _)| fact))
    }

    async fn shadow_with_winner(
        &self, id: &EntityId, valid: Interval, prove_winner: bool,
    ) -> Result<Option<(Option<WorldFact>, Option<FrozenWorldWinner>)>> {
        let unavailable = OwnedWorld::unavailable;
        let mut shadow = None;
        if let Some(mut rows) = rtdb_query::open_owned_assertion_point(
            Arc::clone(&self.overlay), self.owner, &self.shadow, id, Some(valid), false,
        ).await? {
            while let Some(batch) = rows.next_batch().await? {
                if batch.batch().num_rows() == 0 { continue; }
                if shadow.is_some() { return Err(unavailable()); }
                let position = prove_winner.then(|| rtdb_query::owned_row_event_position(batch.batch(), 0)).transpose()?;
                shadow = decode_point(vec![batch], id, self.memory, None)?.map(|fact| (fact, position));
            }
        }
        self.overlay.check_cancelled()?;
        let Some((shadow, position)) = shadow else { return Ok(None); };
        let revision = shadow.revision;
        if revision > self.overlay_basis { return Err(unavailable()); }
        // Validate the physical shadow marker even when it projects to a negative.
        let mut fact = shadow.into_shadow(self.world)?;
        if let Some(fact) = &fact { fact.validate(self.schema)?; }
        let assertion = fact.as_mut().and_then(|fact| fact.assertion.as_mut());
        let mut winner = None;
        if position.is_some() || assertion.is_some() {
            let (_, publication, reading) = self.accepted_group(revision).await?;
            if let Some(assertion) = assertion { assertion.held_reading = Some(reading); }
            if let Some(position) = position {
                if position.transaction != publication.tx_id { return Err(unavailable()); }
                winner = Some(FrozenWorldWinner {
                    author: self.world, publication, event_ordinal: position.ordinal, reading,
                });
            }
        }
        Ok(Some((fact, winner)))
    }

    async fn point(&self, id: &EntityId, valid: Interval) -> Result<Option<WorldFact>> {
        Ok(self.resolve(id, valid).await?.0)
    }

    async fn resolve(&self, id: &EntityId, valid: Interval) -> Result<(Option<WorldFact>, bool)> {
        let (fact, shadowed, _) = self.resolve_with_winner(id, valid, false).await?;
        Ok((fact, shadowed))
    }

    async fn resolve_with_winner(
        &self, id: &EntityId, valid: Interval, prove_winner: bool,
    ) -> Result<(Option<WorldFact>, bool, Option<FrozenWorldWinner>)> {
        if let Some((fact, winner)) = self.resolve_shadow_with_winner(id, valid, prove_winner).await? {
            return Ok((fact, true, winner));
        }
        let unavailable = OwnedWorld::unavailable;
        let batches = read_checkpoint_point_in_context(Arc::clone(&self.root), CheckpointPoint {
            owner: self.owner, relation: self.relation, id, valid: Some(valid),
        }, None).await?;
        let fact = decode_point(batches.unwrap_or_default(), id, self.memory, None)?;
        if let Some(fact) = &fact {
            if fact.revision > self.root.current_time.as_micros()? { return Err(unavailable()); }
            fact.validate(self.schema)?;
        }
        self.root.check_cancelled()?;
        Ok((fact, false, None))
    }

    async fn resolve_shadow(&self, id: &EntityId, valid: Interval) -> Result<Option<Option<WorldFact>>> {
        Ok(self.resolve_shadow_with_winner(id, valid, false).await?.map(|(fact, _)| fact))
    }

    async fn resolve_shadow_with_winner(
        &self, id: &EntityId, valid: Interval, prove_winner: bool,
    ) -> Result<Option<(Option<WorldFact>, Option<FrozenWorldWinner>)>> {
        for writer in self.writers() {
            if let Some(shadow) = writer.shadow_with_winner(id, valid, prove_winner).await? { return Ok(Some(shadow)); }
        }
        Ok(None)
    }

    fn writer_at(&self, system: Timestamp) -> Result<&Self> {
        self.writers().find(|writer| writer.floor < system && system <= writer.overlay_basis)
            .ok_or_else(OwnedWorld::unavailable)
    }

    fn cut_at(mut self, system: Timestamp) -> Result<Self> {
        if self.floor < system && system <= self.overlay_basis { return Ok(self); }
        let index = self.earlier.iter().position(|writer| writer.floor < system && system <= writer.overlay_basis)
            .ok_or_else(OwnedWorld::unavailable)?;
        self.earlier.truncate(index + 1);
        let mut selected = self.earlier.pop().ok_or_else(OwnedWorld::unavailable)?;
        selected.earlier = self.earlier;
        Ok(selected)
    }

    async fn changes(
        &self, after: impl Into<OwnedChangeEdge>, through: impl Into<OwnedChangeEdge>,
        limit: usize, reverse: bool,
    ) -> Result<rtdb_node::OwnedMergedChanges> {
        let unavailable = OwnedWorld::unavailable;
        let after = after.into();
        let through = through.into();
        let root = self.earlier.first().map_or(self.root_basis, |writer| writer.root_basis);
        if after < root.into() || after > through || through.time > self.overlay_basis
            || (through.time == self.overlay_basis && matches!(through.order, rtdb_node::ChangeEdgeOrder::Publication { .. })
                && through > self.publication.into())
        {
            return Err(unavailable());
        }
        if after == through {
            let writer = self.writers().find(|writer|
                OwnedChangeEdge::from(writer.root_basis) <= after && after.time <= writer.overlay_basis)
                .ok_or_else(unavailable)?;
            return rtdb_node::archived_changes_at_cut(
                Arc::clone(&writer.overlay_media), self.owner, &writer.shadow, after, through, limit, reverse,
            ).await;
        }
        let mut page: Option<rtdb_node::OwnedMergedChanges> = None;
        let newest = self.writers().collect::<Vec<_>>();
        for index in 0..newest.len() {
            let writer = newest[if reverse { index } else { newest.len() - 1 - index }];
            let lower = OwnedChangeEdge::from(writer.root_basis).max(after);
            let upper = OwnedChangeEdge::from(writer.publication).min(through);
            if lower >= upper { continue; }
            let remaining = limit.checked_sub(page.as_ref().map_or(0, |page| page.commits.len()))
                .ok_or_else(unavailable)?;
            let band = rtdb_node::archived_changes_at_cut(
                Arc::clone(&writer.overlay_media), self.owner, &writer.shadow, lower, upper, remaining.max(1), reverse,
            ).await?;
            if remaining == 0 && let Some(next) = band.commits.first() {
                page.as_mut().ok_or_else(unavailable)?.stop_before(next.publication)?;
                break;
            }
            match &mut page {
                Some(page) => page.append_archive_band(band, reverse)?,
                None => page = Some(band),
            }
            if page.as_ref().is_some_and(|page| !page.is_complete()) { break; }
        }
        let mut page = page.ok_or_else(unavailable)?;
        page.complete_at(if reverse { after } else { through });
        Ok(page)
    }

    async fn version(
        mut self, id: &EntityId, from: Timestamp, through: Timestamp, group: Timestamp,
    ) -> Result<(Timestamp, Vec<(Interval, WorldFact, bool)>)> {
        let unavailable = OwnedWorld::unavailable;
        if from >= group || group > through || through > self.overlay_basis {
            return Err(unavailable());
        }
        self.changes(from, from, 1, false).await?;
        self = self.cut_at(group)?;
        let before = Timestamp(group.0.checked_sub(1).ok_or_else(unavailable)?);
        let page = self.changes(before, group, 1, false).await?;
        if !page.is_time_projectable() { return Err(unavailable()); }
        let [commit] = page.commits.as_slice() else { return Err(unavailable()); };
        let [changes] = commit.changes.as_slice() else { return Err(unavailable()); };
        if commit.publication.system_time != group || changes.relation != *self.relation
            || !changes.touches.iter().any(|touch| &touch.id == id)
        {
            return Err(unavailable());
        }
        let (at, _, clock) = self.accepted_group(group).await?;
        self.overlay = at;
        self.overlay_basis = group;
        let boundaries = self.boundaries(id, Interval::ALL).await?;
        let mut timeline = Vec::with_capacity(boundaries.len().saturating_sub(1));
        let mut bytes = 0usize;
        for span in boundaries.windows(2) {
            let (fact, shadowed) = self.resolve(id, Interval::at(span[0])?).await?;
            if let Some(fact) = fact {
                bytes = bytes.checked_add(fact.document_bytes()?)
                    .filter(|bytes| *bytes <= 8 << 20).ok_or_else(history_limit)?;
                timeline.push((Interval::new(span[0], span[1])?, fact, shadowed));
            }
        }
        self.overlay.check_cancelled()?;
        Ok((clock, timeline))
    }

    async fn select_clock(&self, requested: Timestamp) -> Result<Option<(TransactionKey, Timestamp)>> {
        for writer in self.writers().filter(|writer| writer.initial_clock < requested) {
            if let Some((publication, reading)) = rtdb_query::select_owned_world_clock(
                &writer.overlay, self.owner, writer.world, writer.root_basis, writer.initial_clock, requested,
            ).await? {
                return Ok(Some((publication, reading)));
            }
        }
        Ok(None)
    }

    async fn accepted_group(&self, group: Timestamp) -> Result<(Arc<rtdb_query::QueryContext>, TransactionKey, Timestamp)> {
        let unavailable = OwnedWorld::unavailable;
        if group <= self.floor || group > self.overlay_basis {
            return Err(unavailable());
        }
        let at = self.overlay.with_owned_system_cut(group, &self.overlay.cancellation)?;
        let (clock, publication) = rtdb_query::read_owned_world_clock_publication(Arc::clone(&at), self.owner, self.world)
            .await?.filter(|(clock, revision)| revision.system_time == group && *clock > self.initial_clock)
            .ok_or_else(unavailable)?;
        Ok((at.with_owned_transaction_cut(publication, &at.cancellation)?, publication, clock))
    }

    async fn at(mut self, selected: Option<Timestamp>) -> Result<Self> {
        if let Some(system) = selected {
            self = self.cut_at(system)?;
            self.changes(system, system, 1, false).await?;
            self.overlay = self.accepted_group(system).await?.0;
            self.overlay_basis = system;
        }
        Ok(self)
    }

    async fn path_touch(
        mut self, id: &EntityId, path: &OwnedRelation, group: Timestamp, valid: Timestamp, images: bool,
    ) -> Result<[(Option<WorldFact>, bool); 2]> {
        let unavailable = OwnedWorld::unavailable;
        self = self.cut_at(group)?;
        let (accepted, _, clock) = self.accepted_group(group).await?;
        if clock != valid { return Err(unavailable()); }
        let before = Timestamp(group.0.checked_sub(1).ok_or_else(unavailable)?);
        let columns = rtdb_node::archived_path_touch_at_cut(
            Arc::clone(&self.overlay_media), self.owner, &self.shadow, path, id,
            self.overlay_basis, before, group, valid,
        ).await?;
        rtdb_node::stable_owned_path_columns(&self.root, &accepted, self.owner, self.relation, path).await?;
        let instant = Interval::at(valid)?;
        self.overlay = self.overlay.with_owned_system_cut(before, &self.overlay.cancellation)?;
        self.overlay_basis = before;
        let prior = self.resolve(id, instant).await?;
        self.overlay = accepted;
        self.overlay_basis = group;
        let after = self.resolve(id, instant).await?;
        if !after.1 || after.0.as_ref().is_some_and(|fact| fact.revision != group) {
            return Err(unavailable());
        }
        let mut sides = [prior, after];
        if !images {
            for (fact, _) in &mut sides {
                if let Some(fact) = fact {
                    fact.select_path(&columns, true)?;
                }
            }
        }
        Ok(sides)
    }

    async fn overlay_identity_page(
        &self, valid: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize, prefix: Option<&str>,
    ) -> Result<WorldIdentityPage> {
        let mut keys = Vec::new();
        let mut reservations = Vec::new();
        for writer in self.writers() {
            let page = rtdb_query::owned_identity_candidates_in_prefix(
                Arc::clone(&writer.overlay), self.owner, &writer.shadow, writer.overlay_basis, valid, after, limit + 1,
                prefix.map(rtdb_columnar::EntityIdRef::String),
            ).await?;
            let (local, charges) = page.into_parts();
            keys.extend(local);
            reservations.extend(charges);
        }
        keys.sort_unstable();
        keys.dedup();
        let more = keys.len() > limit;
        keys.truncate(limit);
        Ok(WorldIdentityPage { keys, more, reservations })
    }

    async fn own_range(
        &self, path: &OwnedRelation, request: OwnedRangeRequest<'_>, valid: Timestamp,
    ) -> Result<OwnedRangePage> {
        rtdb_node::stable_owned_path_columns(
            &self.root, &self.overlay, self.owner, self.relation, path,
        ).await?;
        rtdb_node::range_owned_cut(
            Arc::clone(&self.overlay), self.owner, &self.shadow, path, request, valid,
        ).await
    }

    async fn identities(&self, valid: Timestamp, after: OwnedIdentityScan<'static>, limit: usize) -> Result<WorldIdentityPage> {
        const FETCH: usize = 129;
        let root_basis = self.root.current_time.as_micros()?;
        let mut keys = Vec::with_capacity(limit);
        let mut bytes = limit * 128;
        let mut reservations = vec![self.memory.reserve(bytes)?];
        let mut cursor = None::<(EntityId, Arc<MemoryReservation>)>;
        for _ in 0..32 {
            let next = cursor.as_ref().map(|(key, _)| OwnedIdentityScan::seek(key)).transpose()?;
            let after = next.as_ref().unwrap_or(&after);
            let parent = rtdb_query::owned_identity_candidates(
                Arc::clone(&self.root), self.owner, self.relation, root_basis, valid, after, FETCH,
            ).await?;
            let overlay = self.overlay_identity_page(valid, after, FETCH, None).await?;
            let (candidates, through) = identity_candidates(&parent.keys, &overlay.keys, FETCH);
            for key in candidates {
                let id = key.clone();
                if self.point(&id, Interval::at(valid)?).await?.is_none() { continue; }
                if keys.len() == limit {
                    return Ok(WorldIdentityPage { keys, more: true, reservations });
                }
                bytes = bytes.checked_add(key.retained_bytes()).filter(|bytes| *bytes <= 8 << 20)
                    .ok_or_else(OwnedWorld::unavailable)?;
                reservations.push(self.memory.reserve(key.retained_bytes())?);
                keys.push(key.clone());
            }
            let Some(through) = through else { return Ok(WorldIdentityPage { keys, more: false, reservations }); };
            cursor = Some((through.clone(), self.memory.reserve(through.retained_bytes())?));
        }
        Err(OwnedWorld::unavailable())
    }

    async fn search_shadows(
        &self, path: &OwnedRelation, partition: usize, valid: Timestamp,
        corpus: &mut rtdb_node::HeldTextCorpus, remaining: &mut usize,
        projections: &WorldProjectionCustody, historical: bool,
    ) -> Result<()> {
        let unavailable = OwnedWorld::unavailable;
        let column = rtdb_node::owned_text_column(
            Arc::clone(&self.root), self.owner, self.relation, path,
        ).await?;
        for writer in self.writers() {
            if column != rtdb_node::owned_text_column(
                Arc::clone(&writer.overlay), self.owner, self.relation, path,
            ).await? { return Err(unavailable()); }
        }
        if self.schema.search_column_logical_type(&column).is_none() {
            return Err(unavailable());
        }
        let instant = Interval::at(valid)?;
        let mut cursor = None;
        loop {
            let after = cursor.as_ref().map(|(key, _): &(EntityId, Arc<MemoryReservation>)| OwnedIdentityScan::seek(key))
                .transpose()?.unwrap_or(OwnedIdentityScan::Unbounded);
            let page = self.overlay_identity_page(valid, &after, 128, None).await?;
            *remaining = remaining.checked_sub(page.keys.len()).ok_or_else(unavailable)?;
            for key in &page.keys {
                let id = key.clone();
                if corpus.contains_shadow(partition, &id) { continue; }
                let shadow = self.resolve_shadow(&id, instant).await?.ok_or_else(unavailable)?;
                projections.shadow_text(self.relation, &column, shadow.as_ref(), id, partition, corpus, self.memory, historical).await?;
            }
            if !page.more { return Ok(()); }
            let key = page.keys.last().ok_or_else(unavailable)?;
            cursor = Some((key.clone(), self.memory.reserve(key.retained_bytes())?));
        }
    }

    async fn nearest_shadows(
        &self, selected: &rtdb_node::nearest::NearestPartition, query: &rtdb_node::nearest::NearestQuery, partition: usize, valid: Timestamp,
        shadows: &mut rtdb_node::nearest::HeldVectorOverlay, remaining: &mut usize,
        projections: &WorldProjectionCustody, historical: bool,
    ) -> Result<()> {
        let unavailable = OwnedWorld::unavailable;
        let instant = Interval::at(valid)?;
        let mut cursor = None;
        loop {
            let after = cursor.as_ref().map(|(key, _): &(EntityId, Arc<MemoryReservation>)| OwnedIdentityScan::seek(key))
                .transpose()?.unwrap_or(OwnedIdentityScan::Unbounded);
            let page = self.overlay_identity_page(valid, &after, 128, None).await?;
            *remaining = remaining.checked_sub(page.keys.len()).ok_or_else(unavailable)?;
            for key in &page.keys {
                let id = key.clone();
                if shadows.contains_shadow(partition, &id) { continue; }
                let shadow = self.resolve_shadow(&id, instant).await?.ok_or_else(unavailable)?;
                projections.shadow_nearest(selected, query, shadow.as_ref(), id, partition, shadows, self.memory, historical).await?;
            }
            if !page.more { return Ok(()); }
            let key = page.keys.last().ok_or_else(unavailable)?;
            cursor = Some((key.clone(), self.memory.reserve(key.retained_bytes())?));
        }
    }

    async fn boundaries(&self, id: &EntityId, valid: Interval) -> Result<Vec<Timestamp>> {
        let root_boundaries = rtdb_query::open_owned_valid_boundaries(
            Arc::clone(&self.root), self.owner, self.relation, id, valid, self.root.current_time.as_micros()?,
        ).await?;
        let mut boundaries = read_boundaries(root_boundaries, valid).await?;
        for layer in self.writers() {
            let local = rtdb_query::open_owned_valid_boundaries(
                Arc::clone(&layer.overlay), layer.owner, &layer.shadow, id, valid, layer.overlay_basis,
            ).await?;
            boundaries = merge_boundaries(boundaries, read_boundaries(local, valid).await?, valid)?;
        }
        Ok(boundaries)
    }

    async fn schedule(&self, id: &EntityId, valid: Interval) -> Result<Vec<(Interval, Timestamp)>> {
        let boundaries = self.boundaries(id, valid).await?;
        let mut timeline = Vec::with_capacity(boundaries.len().saturating_sub(1));
        for span in boundaries.windows(2) {
            if let Some(fact) = self.point(id, Interval::at(span[0])?).await? {
                timeline.push((Interval::new(span[0], span[1])?, fact.revision));
            }
        }
        Ok(timeline)
    }
}

fn identity_candidates<'a>(
    parent: &'a [EntityId], overlay: &'a [EntityId], fetch: usize,
) -> (impl Iterator<Item = &'a EntityId>, Option<&'a EntityId>) {
    let through = [parent, overlay].into_iter()
        .filter(|keys| keys.len() == fetch).filter_map(|keys| keys.last()).min();
    let candidates: std::collections::BTreeSet<_> = parent.iter().chain(overlay).collect();
    (candidates.into_iter().take_while(move |key| through.is_none_or(|through| *key <= through)), through)
}

fn check_archive_coverage<'a>(
    context: &rtdb_query::QueryContext, owner: &str,
    frontiers: impl IntoIterator<Item = (&'a OwnedRelation, i64)>,
) -> Result<()> {
    require_relation_purge_history(context, owner)?;
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(OwnedWorld::unavailable()); };
    for (relation, expected) in frontiers {
        if snapshot.owned_routing_inventory(owner, relation)?.mask_events != expected {
            return Err(OwnedWorld::unavailable());
        }
    }
    Ok(())
}

async fn exact_historical_parent_context(
    context: Arc<rtdb_query::QueryContext>, owner: &str, system: Timestamp,
    schemas: &[(OwnedRelation, rtdb_types::OwnedSchema)],
) -> Result<Arc<rtdb_query::QueryContext>> {
    let (context, selected) = historical_parent_context(context, owner, system, schemas).await?
        .ok_or_else(OwnedWorld::unavailable)?;
    if selected != system { return Err(OwnedWorld::unavailable()); }
    Ok(context)
}

// Journal reads may select the exact pinned physical cut. Publication-based
// selection still requires a Root publication at the requested coordinate.
async fn parent_journal_context(
    context: Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation,
    system: Timestamp, expected: &rtdb_types::OwnedSchema,
) -> Result<()> {
    let parent = context.current_time.as_micros()?;
    if system != parent {
        exact_historical_parent_context(context, owner, system, &[(relation.clone(), expected.clone())]).await?;
        return Ok(());
    }
    require_historical_parent_archive(&context, parent)?;
    check_archive_coverage(&context, owner, [(relation, 0)])?;
    let actual = rtdb_query::read_owned_relation_schema(Arc::clone(&context), owner, relation).await?;
    if !actual.is_some_and(|(actual, _)| &actual == expected) {
        return Err(OwnedWorld::unavailable());
    }
    context.check_cancelled()
}

fn require_historical_parent_archive(
    context: &rtdb_query::QueryContext, parent: Timestamp,
) -> Result<()> {
    let unavailable = OwnedWorld::unavailable;
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(unavailable()); };
    if snapshot.tx_basis().is_none_or(|basis| basis.system_time != parent)
        || snapshot.system_time_cap() != Some(parent)
        || snapshot.relation_event_frontier(&rtdb_types::TableRef::new("public", "bj_rtdb_restore_hold"))? != 0
    {
        return Err(unavailable());
    }
    Ok(())
}

async fn historical_parent_context(
    context: Arc<rtdb_query::QueryContext>, owner: &str, requested: Timestamp,
    schemas: &[(OwnedRelation, rtdb_types::OwnedSchema)],
) -> Result<Option<(Arc<rtdb_query::QueryContext>, Timestamp)>> {
    let unavailable = OwnedWorld::unavailable;
    let parent = context.current_time.as_micros()?;
    if requested > parent { return Err(unavailable()); }
    check_archive_coverage(&context, owner, schemas.iter().map(|(relation, _)| (relation, 0)))?;
    require_historical_parent_archive(&context, parent)?;
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(unavailable()); };
    let publication = OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?;
    if snapshot.relation_event_frontier(&publication.purge_table(owner))? != 0 {
        return Err(unavailable());
    }
    require_parent_journal(&context, owner, &publication, requested).await?;
    let requested_context = context.with_owned_system_cut(requested, &context.cancellation)?;
    let Some(selected) = rtdb_query::read_owned_root_publication(Arc::clone(&requested_context), owner).await? else {
        requested_context.check_cancelled()?;
        if requested == parent { return Err(unavailable()); }
        return Ok(None);
    };
    // The pinned physical position is eligible only if it was itself an
    // application publication. A later checkpoint cannot stand in for it.
    if selected.system_time > requested || (requested == parent && snapshot.tx_basis() != Some(selected)) {
        return Err(unavailable());
    }
    let capped = context.with_owned_transaction_cut(selected, &context.cancellation)?;
    require_parent_journal(&context, owner, &publication, selected.system_time).await?;
    let selected = selected.system_time;
    for (relation, expected) in schemas {
        require_parent_journal(&context, owner, relation, selected).await?;
        let actual = rtdb_query::read_owned_relation_schema(Arc::clone(&capped), owner, relation).await?;
        if !actual.is_some_and(|(schema, _)| &schema == expected) { return Err(unavailable()); }
    }
    capped.check_cancelled()?;
    Ok(Some((capped, selected)))
}

async fn require_parent_journal(
    context: &Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation, selected: Timestamp,
) -> Result<()> {
    let unavailable = OwnedWorld::unavailable;
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(unavailable()); };
    let inventory = snapshot.owned_routing_inventory(owner, relation)?;
    if inventory.journal_exhausted() {
        return Ok(());
    }
    let data_events = inventory.data_events;
    let entries = inventory.journal_events;
    if entries < 2 { return Err(unavailable()); }
    let floor = rtdb_query::read_owned_change_entry(Arc::clone(context), owner, relation, 0).await?;
    let first = rtdb_query::read_owned_journal_header(Arc::clone(context), owner, relation, 1).await?;
    let head = rtdb_query::read_owned_change_entry(Arc::clone(context), owner, relation, entries - 1).await?;
    if floor.value != Value::Null || floor.publication != first.publication
        || floor.through_events >= first.through_events || (floor.through_events != 0 && selected < floor.publication.system_time)
        || head.through_events != data_events || head.publication.system_time > context.current_time.as_micros()?
        || (entries > 2 && (head.publication.tx_id <= first.publication.tx_id
            || !first.publication.at_or_before(head.publication) || head.through_events <= first.through_events))
        || (entries == 2 && (head.publication != first.publication || head.through_events != first.through_events))
        || !matches!(head.value, Value::List(touches) if !touches.is_empty())
    {
        return Err(unavailable());
    }
    Ok(())
}

const MAX_HISTORY_SLICES: usize = 128;

fn history_limit() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::new(
        rtdb_types::AnomalyCategory::Busy, "rtdb.world/history-size",
        "The world history interval exceeds its boundary limit",
    )
}

fn merge_boundaries(parent: Vec<Timestamp>, local: Vec<Timestamp>, valid: Interval) -> Result<Vec<Timestamp>> {
    // A parent boundary inside a local shadow cannot affect the selected timeline.
    let mut boundaries: Vec<_> = parent.into_iter().filter(|boundary| {
        !local.chunks_exact(2).any(|span| span[0] < *boundary && *boundary < span[1])
    }).collect();
    boundaries.extend(local);
    boundaries.extend([valid.lower(), valid.upper()]);
    boundaries.sort_unstable();
    boundaries.dedup();
    if boundaries.len() > MAX_HISTORY_SLICES + 1 { return Err(history_limit()); }
    Ok(boundaries)
}

async fn read_boundaries(cursor: Option<rtdb_query::QueryCursor>, valid: Interval) -> Result<Vec<Timestamp>> {
    let mut boundaries = Vec::new();
    let Some(mut cursor) = cursor else { return Ok(boundaries); };
    let mut prior = valid.lower();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        let read = |name, row| {
            let cell = ArrowCell::named(batch, name, row).ok_or_else(OwnedWorld::unavailable)?;
            if let Some(value) = rtdb_query::owned_micros(cell) { return Ok(value); }
            if name == "_valid_to" && cell.logical_type()? == rtdb_types::LogicalType::Null {
                return Ok(Timestamp::MAX);
            }
            match rtdb_columnar::read_value(cell.field, cell.array, cell.row)? {
                Value::Timestamp { value, unit: rtdb_types::TimeUnit::Microsecond, .. } => Ok(Timestamp(value)),
                Value::Null if name == "_valid_to" => Ok(Timestamp::MAX),
                _ => Err(OwnedWorld::unavailable()),
            }
        };
        for row in 0..batch.num_rows() {
            if boundaries.len() == MAX_HISTORY_SLICES * 2 { return Err(history_limit()); }
            let from = read("_valid_from", row)?;
            let to = read("_valid_to", row)?;
            if from < prior || from >= to || to > valid.upper() { return Err(OwnedWorld::unavailable()); }
            boundaries.extend([from, to]);
            prior = to;
        }
    }
    Ok(boundaries)
}

async fn send_archive_answer<T>(plaintext: &PlaintextView, reply: oneshot::Sender<Result<T>>, answer: Result<T>) {
    if reply.is_closed() { return; }
    let _grant = if answer.is_ok() {
        match plaintext.grant().await {
            Ok(grant) => Some(grant),
            Err(error) => { let _ = reply.send(Err(error)); return; }
        }
    } else {
        None
    };
    let _ = reply.send(answer);
}

impl Drop for HeldCheckpointWorld {
    fn drop(&mut self) { self.close(); }
}

pub(crate) async fn open_held_checkpoint_world(
    archive: WorldArchive, session: &mut NativeSession, deployment: Uuid, world: Uuid,
    memory: Arc<MemoryBudget>, cache: Arc<CacheBudget>,
) -> Result<HeldCheckpointWorld> {
    let open = open_world(archive.directory.as_ref(), &archive.storage, session, deployment, world).await?;
    let parent = open.parent(deployment);
    let ancestors = open.retained.ancestors.clone();
    let owner = open.retained.owner.clone();
    let parent_time = parent.basis.system_time;
    let remaining = open.retained.until.0.saturating_sub(now().0).max(0) as u64;
    let lifetime = Duration::from_micros(remaining).min(Duration::from_secs(60));
    let deadline = tokio::time::Instant::now() + lifetime;
    let cancellation = session.state().cancellation().child_token();
    // On RPC cancellation the worker still completes the storage-region drain.
    let opening = cancellation.clone().drop_guard();
    let projections = Arc::new(WorldProjectionCustody::new(
        Arc::clone(&archive.keys), deployment, open.incarnation, session.assertion_family_reader()?, cancellation.clone(),
    ));
    let archive_projections = Arc::clone(&projections);
    let worker = cancellation.clone();
    let backing_memory = Arc::clone(&memory);
    let (requests, mut pending) = mpsc::channel::<ArchiveRequest>(1);
    let (ready, opened) = oneshot::channel();
    tokio::spawn(async move {
        let timer = worker.clone();
        let expire = async {
            if timer.run_until_cancelled(tokio::time::sleep_until(deadline)).await.is_some() {
                timer.cancel();
            }
        };
        let serve = async {
            let projections = archive_projections.as_ref();
            let ancestor_cache = Arc::clone(&cache);
            let archive = &archive;
            let mut ready = Some(ready);
            let outcome = with_checkpoint_context(
                archive.directory.as_ref(), &archive.storage, archive.keys.as_ref(), archive.hosted_task.as_ref(),
                &parent,
                CheckpointReadResources { node: &archive.options, memory: &backing_memory, cache, cancellation: &worker },
                |context| {
                    let opened = ready.take().expect("one archive open");
                    let scope = &worker;
                    let memory = &backing_memory;
                    async move {
                        let dependencies = PlaintextDependencies::capture(memory)?;
                        if scope.is_cancelled() || tokio::time::Instant::now() >= deadline
                            || opened.send(Ok(())).is_err()
                        {
                            return Ok(());
                        }
                        loop {
                            let request = tokio::select! {
                                _ = scope.cancelled() => break,
                                point = pending.recv() => match point { Some(point) => point, None => break },
                            };
                            let ArchiveRequest { operation, plaintext: caller } = request;
                            let plaintext = PlaintextView::new(Arc::clone(memory));
                            macro_rules! scoped_read {
                                ($read:expr) => {
                                    caller.reading(plaintext.reading(async {
                                        dependencies.admit()?;
                                        $read.await
                                    }))
                                };
                            }
                            match operation {
                                ArchiveRead::Ancestor { relation, read, selected, schema, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let needed = selected.or(match &read {
                                            AncestorRead::Clock(cut) => Some(cut.system()),
                                            AncestorRead::Version { group, .. }
                                                | AncestorRead::PathTouch { group, .. } => Some(*group),
                                            AncestorRead::Changes { through, .. } => Some(through.time),
                                            _ => None,
                                        });
                                        let ancestors = if let AncestorRead::Clock(ClockCut::Publication(publication)) = &read {
                                            let through = ancestors.iter().position(|layer| publication.at_or_before(layer.publication))
                                                .ok_or_else(OwnedWorld::unavailable)?;
                                            &ancestors[..=through]
                                        } else if let AncestorRead::Changes { through, .. } = &read {
                                            let layers = (ancestors.partition_point(|layer|
                                                OwnedChangeEdge::from(layer.publication) < *through) + 1).min(ancestors.len());
                                            &ancestors[..layers]
                                        } else {
                                            frozen_prefix(&ancestors, needed)
                                        };
                                        with_frozen_writers(
                                            archive, deployment, ancestors,
                                            CheckpointReadResources {
                                                node: &archive.options, memory, cache: Arc::clone(&ancestor_cache), cancellation: scope,
                                            },
                                            Vec::new(),
                                            |overlays| {
                                                let root = Arc::clone(&context).with_owned_read_guard(Arc::clone(&guard));
                                                let overlays = overlays.into_iter().map(|overlay|
                                                    overlay.with_owned_read_guard(Arc::clone(&guard))).collect();
                                                let (owner, relation, schema) = (&owner, &relation, &schema);
                                                async move {
                                                    let inherited = InheritedRelation::open(root, overlays, owner, relation, ancestors, schema, memory)
                                                        .await?.at(selected).await?;
                                                    match read {
                                                        AncestorRead::Point { id, valid, prove_winner } => {
                                                            let (fact, shadowed, winner) = inherited.resolve_with_winner(&id, valid, prove_winner).await?;
                                                            Ok(AncestorResult::Point(fact, shadowed, winner))
                                                        },
                                                        AncestorRead::Changes { after, through, limit, reverse } =>
                                                            Ok(AncestorResult::Changes(inherited.changes(after, through, limit, reverse).await?)),
                                                        AncestorRead::Version { id, from, through, group } => {
                                                            let (clock, spans) = {
                                                                let through = through.min(inherited.overlay_basis);
                                                                inherited.version(&id, from, through, group).await?
                                                            };
                                                            Ok(AncestorResult::Version(clock, spans))
                                                        },
                                                        AncestorRead::Clock(cut) => {
                                                            let group = cut.system();
                                                            let (writer, exact) = match cut {
                                                                ClockCut::System(_) => (inherited.writer_at(group)?, None),
                                                                ClockCut::Publication(publication) => {
                                                                    let writer = inherited.writers().find(|writer|
                                                                        writer.root_basis.tx_id < publication.tx_id
                                                                            && publication.at_or_before(writer.publication))
                                                                        .ok_or_else(OwnedWorld::unavailable)?;
                                                                    (writer, Some(publication))
                                                                }
                                                            };
                                                            rtdb_node::archived_changes_at_cut(
                                                                Arc::clone(&writer.overlay_media), owner, &writer.shadow,
                                                                group, group, 1, false,
                                                            ).await?;
                                                            let clock = rtdb_query::read_archived_held_clock(
                                                                &writer.overlay_media, owner, writer.world, writer.publication, group, exact,
                                                            ).await?;
                                                            Ok(AncestorResult::Clock(clock))
                                                        },
                                                        AncestorRead::SelectClock(requested) =>
                                                            Ok(AncestorResult::SelectedClock(inherited.select_clock(requested).await?)),
                                                        AncestorRead::PathTouch { id, path, group, valid, images } =>
                                                            Ok(AncestorResult::PathTouch(inherited.path_touch(&id, &path, group, valid, images).await?)),
                                                        AncestorRead::Schedule { id, valid } =>
                                                            Ok(AncestorResult::Schedule(inherited.schedule(&id, valid).await?)),
                                                        AncestorRead::Identities { valid, after, limit } =>
                                                            Ok(AncestorResult::Identities(inherited.identities(valid, after, limit).await?)),
                                                        AncestorRead::OverlayIdentities { valid, after, limit, prefix } =>
                                                            Ok(AncestorResult::Identities(inherited.overlay_identity_page(
                                                                valid, &after, limit, prefix.as_deref(),
                                                            ).await?)),
                                                        AncestorRead::Range { path, lower, upper, after, descending, limit, valid } => {
                                                            let request = OwnedRangeRequest {
                                                                lower: lower.as_ref().map(RetainedKeyBound::borrowed),
                                                                upper: upper.as_ref().map(RetainedKeyBound::borrowed),
                                                                after: after.as_ref().map(RetainedKeyBound::borrowed),
                                                                descending, limit,
                                                            };
                                                            Ok(AncestorResult::Range(inherited.overlay_range(&path, request, valid).await?))
                                                        },
                                                    }
                                                }
                                            },
                                        ).await
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::Coverage { frontiers, guard, reply } => {
                                    let coverage = scoped_read!(async {
                                        guard().and_then(|()| check_archive_coverage(
                                            &context, &owner, frontiers.iter().map(|(relation, frontier)| (relation, *frontier)),
                                        ))
                                    }).await;
                                    send_archive_answer(&plaintext, reply, coverage).await;
                                }
                                ArchiveRead::ParentAsOf { requested, schemas, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        Ok(historical_parent_context(context, &owner, requested, &schemas).await?
                                            .map(|(_, selected)| selected))
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::ParentScheduleAt { relation, id, system, valid, schema, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let schemas = [(relation.clone(), schema)];
                                        let context = exact_historical_parent_context(
                                            Arc::clone(&context).with_owned_read_guard(guard), &owner, system, &schemas,
                                        ).await?;
                                        let cursor = rtdb_query::open_owned_valid_boundaries(
                                            Arc::clone(&context), &owner, &relation, &id, valid, system,
                                        ).await?;
                                        let boundaries = read_boundaries(cursor, valid).await?;
                                        let mut schedule = Vec::with_capacity(boundaries.len() / 2);
                                        for span in boundaries.chunks_exact(2) {
                                            let interval = Interval::new(span[0], span[1])?;
                                            let batches = read_checkpoint_point_in_context(
                                                Arc::clone(&context), CheckpointPoint {
                                                    owner: &owner, relation: &relation, id: &id, valid: Some(Interval::at(span[0])?),
                                                }, None,
                                            ).await?;
                                            let fact = decode_point(batches.unwrap_or_default(), &id, memory, None)?
                                                .filter(|fact| fact.revision <= system).ok_or_else(OwnedWorld::unavailable)?;
                                            fact.validate(&schemas[0].1)?;
                                            schedule.push((interval, fact.revision));
                                        }
                                        context.check_cancelled()?;
                                        Ok(schedule)
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::ParentTouchImagesAt { relation, path, id, selected, before, group, valid, images, schema, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        parent_journal_context(Arc::clone(&context), &owner, &relation, selected, &schema).await?;
                                        let columns = rtdb_node::archived_path_touch_at_cut(
                                            Arc::clone(&context), &owner, &relation, &path, &id, selected, before, group, valid,
                                        ).await?;
                                        if columns.last().is_none_or(|column| column != "_id")
                                            || columns[..columns.len() - 1].iter().any(|column| schema.path_column_type(column).is_none())
                                        {
                                            return Err(OwnedWorld::unavailable());
                                        }
                                        let valid = Interval::at(valid)?;
                                        let read_image = |system: Timestamp| {
                                            let context = Arc::clone(&context);
                                            let owner = &owner;
                                            let relation = &relation;
                                            let id = &id;
                                            let schema = &schema;
                                            let columns = &columns;
                                            async move {
                                                let capped = context.with_owned_system_cut(system, &context.cancellation)?;
                                                let actual = rtdb_query::read_owned_relation_schema(Arc::clone(&capped), owner, relation).await?;
                                                if !actual.is_some_and(|(actual, _)| &actual == schema) {
                                                    return Err(OwnedWorld::unavailable());
                                                }
                                                let batches = if images {
                                                    read_checkpoint_point_in_context(capped, CheckpointPoint {
                                                        owner, relation, id, valid: Some(valid),
                                                    }, None).await?.unwrap_or_default()
                                                } else {
                                                    let database = capped.catalog.database(capped.catalog.default_database())?;
                                                    let [snapshot] = database.partitions.as_slice() else {
                                                        return Err(OwnedWorld::unavailable());
                                                    };
                                                    if snapshot.contains_table(&relation.lifetime_table(owner)) {
                                                        return Err(OwnedWorld::unavailable());
                                                    }
                                                    let names: Vec<_> = columns.iter().map(String::as_str).collect();
                                                    let mut batches = Vec::new();
                                                    if let Some(mut cursor) = rtdb_query::open_owned_point(
                                                        capped, owner, relation, id, Some(valid), Some(&names),
                                                    ).await? {
                                                        while let Some(batch) = cursor.next_batch().await? { batches.push(batch); }
                                                    }
                                                    batches
                                                };
                                                let mut fact = decode_point(batches, id, memory, None)?;
                                                if let Some(fact) = &mut fact {
                                                    if fact.revision > system || (system == group && fact.revision != group) {
                                                        return Err(OwnedWorld::unavailable());
                                                    }
                                                    if images {
                                                        fact.validate(&schema)?;
                                                    } else {
                                                        fact.select_path(columns, false)?;
                                                    }
                                                }
                                                Ok(fact)
                                            }
                                        };
                                        Ok([read_image(before).await?, read_image(group).await?])
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::ParentChangesAt { relation, selected, after, through, limit, reverse, schema, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        parent_journal_context(Arc::clone(&context), &owner, &relation, selected, &schema).await?;
                                        rtdb_node::archived_changes_at_cut(context, &owner, &relation, after, through, limit, reverse).await
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::ParentVersionsAt { relation, id, selected, request, schema, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        parent_journal_context(Arc::clone(&context), &owner, &relation, selected, &schema).await?;
                                        require_parent_journal(&context, &owner, &relation, request.system.lower()).await?;
                                        let page = rtdb_node::archived_versions_at_cut(
                                            Arc::clone(&context), &owner, &relation, &id, selected, request,
                                        ).await?;
                                        let mut versions = Vec::with_capacity(page.versions.len());
                                        let mut remaining = 8usize << 20;
                                        for mut version in page.versions {
                                            let version_context = context.with_owned_system_cut(version.system_time, &context.cancellation)?;
                                            let version_schema = rtdb_query::read_owned_relation_schema(version_context, &owner, &relation).await?;
                                            if !version_schema.is_some_and(|(actual, _)| actual == schema) {
                                                return Err(OwnedWorld::unavailable());
                                            }
                                            let mut timeline = Vec::new();
                                            let mut rows = 0;
                                            while let Some(batch) = version.timeline.next_batch().await? {
                                                if batch.batch().num_rows() == 0 { continue; }
                                                rows += batch.batch().num_rows();
                                                if rows > MAX_HISTORY_SLICES { return Err(history_limit()); }
                                                remaining = remaining.checked_sub(batch.batch().get_array_memory_size())
                                                    .ok_or_else(history_limit)?;
                                                timeline.push(batch);
                                            }
                                            versions.push(rtdb_flight_sql::WorldParentVersion {
                                                sequence: version.revision, system: version.system_time, timeline,
                                            });
                                        }
                                        context.check_cancelled()?;
                                        Ok(rtdb_flight_sql::WorldParentVersions {
                                            floor: page.floor, next_revision: page.next_revision,
                                            done: page.done, scanned: page.scanned, versions,
                                        })
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::ParentPointAt { system, point } => {
                                    let ArchivePoint { relation, id, valid, guard, expected_schema, mut reply, .. } = point;
                                    let read = async {
                                        guard()?;
                                        let schema = expected_schema.ok_or_else(OwnedWorld::unavailable)?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        let schemas = [(relation.clone(), schema)];
                                        let context = exact_historical_parent_context(context, &owner, system, &schemas).await?;
                                        let batches = read_checkpoint_point_in_context(context, CheckpointPoint {
                                            owner: &owner, relation: &relation, id: &id, valid: Some(valid),
                                        }, None).await?;
                                        decode_point(batches.unwrap_or_default(), &id, memory, None)
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::AcceptedScope { schemas, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        check_archive_coverage(&context, &owner, schemas.iter().map(|(relation, _)| (relation, 0)))?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        for (relation, expected) in schemas {
                                            require_parent_journal(&context, &owner, &relation, parent_time).await?;
                                            let actual = rtdb_query::read_owned_relation_schema(Arc::clone(&context), &owner, &relation).await?;
                                            if !actual.is_some_and(|(schema, _)| schema == expected) {
                                                return Err(OwnedWorld::unavailable());
                                            }
                                        }
                                        context.check_cancelled()
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::PathColumns { relation, path, expected_schema, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        if let Some(expected) = expected_schema {
                                            let schema = rtdb_query::read_owned_relation_schema(Arc::clone(&context), &owner, &relation).await?;
                                            if !schema.is_some_and(|(schema, _)| schema == expected) {
                                                return Err(OwnedWorld::unavailable());
                                            }
                                        }
                                        rtdb_node::path_columns_at_cut(&context, &owner, &relation, &path).await
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::Point(ArchivePoint { relation, id, valid, lifetime, expected_schema, guard, mut reply }) => {
                                    let read = async {
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        if let Some(expected) = expected_schema {
                                            let actual = rtdb_query::read_owned_relation_schema(Arc::clone(&context), &owner, &relation).await?;
                                            if !actual.is_some_and(|(schema, _)| schema == expected) {
                                                return Err(OwnedWorld::unavailable());
                                            }
                                        }
                                        let batches = read_checkpoint_point_in_context(context, CheckpointPoint {
                                            owner: &owner, relation: &relation, id: &id, valid: Some(valid),
                                        }, lifetime).await?;
                                        decode_point(batches.unwrap_or_default(), &id, memory, lifetime.map(|live| live.policy.duration))
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::Search { inherited_rank, partitions, frontiers, query, overlay, historical, ancestor_schemas, ancestor_system, offset, limit, valid, parent_at, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        check_archive_coverage(&context, &owner,
                                            partitions.iter().map(|(relation, _)| relation).zip(frontiers))?;
                                        let scoped = Arc::clone(&context).with_owned_read_guard(Arc::clone(&guard));
                                        let scoped = if let Some((system, schemas)) = parent_at {
                                            exact_historical_parent_context(scoped, &owner, system, &schemas).await?
                                        } else { scoped };
                                        let overlay = if let Some(schemas) = ancestor_schemas {
                                            let ancestors = frozen_prefix(&ancestors, ancestor_system);
                                            let corpus = with_frozen_writers(
                                                archive, deployment, ancestors,
                                                CheckpointReadResources {
                                                    node: &archive.options, memory, cache: Arc::clone(&ancestor_cache), cancellation: scope,
                                                },
                                                Vec::new(),
                                                |archived| {
                                                    let archived = archived.into_iter().map(|layer|
                                                        layer.with_owned_read_guard(Arc::clone(&guard))).collect::<Vec<_>>();
                                                    let (root, owner, partitions, query) =
                                                        (&scoped, &owner, &partitions, &query);
                                                    async move {
                                                        let mut corpus = match overlay {
                                                            Some(corpus) => corpus,
                                                            None => {
                                                                let analyzer = rtdb_node::owned_search_analyzer(Arc::clone(root), owner, partitions).await?;
                                                                rtdb_node::HeldTextCorpus::new(Arc::clone(memory), analyzer, query)?
                                                            }
                                                        };
                                                        let mut remaining = 4096;
                                                        for (index, ((relation, path), schema)) in partitions.iter().zip(schemas).enumerate() {
                                                            let inherited = InheritedRelation::open(
                                                                Arc::clone(root), archived.clone(), owner, relation, ancestors, &schema, memory,
                                                            ).await?.at(ancestor_system).await?;
                                                            inherited.search_shadows(path, index, valid, &mut corpus, &mut remaining, projections, historical).await?;
                                                        }
                                                        Ok(corpus)
                                                    }
                                                },
                                            ).await?;
                                            Some(corpus)
                                        } else {
                                            overlay
                                        };
                                        let page = PlaintextView::reading_inherited_root_rank(inherited_rank,
                                            rtdb_node::rank_owned_overlay_batch(scoped, &owner, &partitions, &query, offset, limit, Some(valid), overlay),
                                        ).await?;
                                        guard()?;
                                        Ok(page)
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::Nearest { inherited_rank, partitions, frontiers, query, overlay, historical, ancestor_schemas, ancestor_system, valid, parent_at, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        check_archive_coverage(&context, &owner,
                                            partitions.iter().map(|partition| &partition.relation).zip(frontiers))?;
                                        let scoped = Arc::clone(&context).with_owned_read_guard(Arc::clone(&guard));
                                        let scoped = if let Some((system, schemas)) = parent_at {
                                            exact_historical_parent_context(scoped, &owner, system, &schemas).await?
                                        } else { scoped };
                                        let overlay = if let Some(schemas) = ancestor_schemas {
                                            let ancestors = frozen_prefix(&ancestors, ancestor_system);
                                            let shadows = with_frozen_writers(
                                                archive, deployment, ancestors,
                                                CheckpointReadResources {
                                                    node: &archive.options, memory, cache: Arc::clone(&ancestor_cache), cancellation: scope,
                                                },
                                                Vec::new(),
                                                |archived| {
                                                    let archived = archived.into_iter().map(|layer|
                                                        layer.with_owned_read_guard(Arc::clone(&guard))).collect::<Vec<_>>();
                                                    let (root, owner, partitions, query) = (&scoped, &owner, &partitions, &query);
                                                    async move {
                                                        let mut shadows = match overlay {
                                                            Some(shadows) => shadows,
                                                            None => rtdb_node::nearest::HeldVectorOverlay::new(memory)?,
                                                        };
                                                        let mut remaining = 4096;
                                                        for (index, (selected, schema)) in partitions.iter().zip(schemas).enumerate() {
                                                            let inherited = InheritedRelation::open(
                                                                Arc::clone(root), archived.clone(), owner, &selected.relation, ancestors, &schema, memory,
                                                            ).await?.at(ancestor_system).await?;
                                                            inherited.nearest_shadows(selected, query, index, valid, &mut shadows, &mut remaining, projections, historical).await?;
                                                        }
                                                        Ok(shadows)
                                                    }
                                                },
                                            ).await?;
                                            Some(shadows)
                                        } else {
                                            overlay
                                        };
                                        let (page, _) = PlaintextView::reading_inherited_root_rank(inherited_rank,
                                            rtdb_node::nearest::owned_nearest_batch(scoped, &owner, &partitions, &query, Some(valid), overlay),
                                        ).await?;
                                        guard()?;
                                        Ok(page)
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::SearchColumns { partitions, frontiers, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        check_archive_coverage(&context, &owner,
                                            partitions.iter().map(|(relation, _)| relation).zip(frontiers))?;
                                        let scoped = Arc::clone(&context).with_owned_read_guard(Arc::clone(&guard));
                                        let mut columns = Vec::with_capacity(partitions.len());
                                        for (relation, path) in &partitions {
                                            columns.push(rtdb_node::owned_text_column(
                                                Arc::clone(&scoped), &owner, relation, path,
                                            ).await?);
                                        }
                                        let analyzer = rtdb_node::owned_search_analyzer(Arc::clone(&scoped), &owner, &partitions).await?;
                                        guard()?;
                                        Ok((columns, analyzer))
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::Ordered { relation, path, frontier, lower, upper, valid, earlier, read, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        check_archive_coverage(&context, &owner, [(&relation, frontier)])?;
                                        let context = Arc::clone(&context).with_owned_read_guard(Arc::clone(&guard));
                                        let context = if let Some((system, schema)) = earlier {
                                            let schemas = [(relation.clone(), schema)];
                                            exact_historical_parent_context(context, &owner, system, &schemas).await?
                                        } else { context };
                                        let (lower, upper) = (lower.as_ref().map(RetainedKeyBound::borrowed),
                                            upper.as_ref().map(RetainedKeyBound::borrowed));
                                        let result = match read {
                                            OrderedRead::Page { after, descending, limit } => {
                                                let request = OwnedRangeRequest {
                                                    lower, upper, after: after.as_ref().map(RetainedKeyBound::borrowed),
                                                    descending, limit,
                                                };
                                                OrderedResult::Page(rtdb_node::range_owned_cut(context, &owner, &relation, &path, request, valid).await?)
                                            }
                                            OrderedRead::Count => {
                                                let request = OwnedCountRequest { lower, upper, valid_at: valid };
                                                let counted = rtdb_node::count_owned_cut(context, &owner, &relation, &path, request).await?;
                                                OrderedResult::Count(counted.count)
                                            }
                                            OrderedRead::Measure => {
                                                let request = OwnedCountRequest { lower, upper, valid_at: valid };
                                                let result = rtdb_node::measure_owned_cut(
                                                    context, &owner, &relation, &path, request,
                                                ).await?;
                                                OrderedResult::Measure(result)
                                            }
                                        };
                                        guard()?;
                                        Ok(result)
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::Boundaries { relation, id, valid, expected_schema, guard, mut reply } => {
                                    let read = async {
                                        require_relation_purge_history(&context, &owner)?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        if let Some(expected) = expected_schema {
                                            let schema = rtdb_query::read_owned_relation_schema(Arc::clone(&context), &owner, &relation).await?;
                                            if !schema.is_some_and(|(schema, _)| schema == expected) {
                                                return Err(OwnedWorld::unavailable());
                                            }
                                        }
                                        let cursor = rtdb_query::open_owned_valid_boundaries(
                                            context,
                                            &owner, &relation, &id, valid, parent_time,
                                        ).await?;
                                        read_boundaries(cursor, valid).await
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                                ArchiveRead::Identities { relation, at, after, fetch, protected, earlier, guard, mut reply } => {
                                    let read = async {
                                        require_relation_purge_history(&context, &owner)?;
                                        let scoped = Arc::clone(&context).with_owned_read_guard(guard);
                                        let prefix = if let Some((prefix, schema)) = &protected {
                                            let actual = rtdb_query::read_owned_relation_schema(Arc::clone(&scoped), &owner, &relation).await?;
                                            if !actual.is_some_and(|(actual, _)| &actual == schema) {
                                                return Err(OwnedWorld::unavailable());
                                            }
                                            Some(rtdb_columnar::EntityIdRef::Binary(prefix))
                                        } else { None };
                                        let (scoped, system) = if let Some((system, schema)) = earlier {
                                            let schemas = [(relation.clone(), schema)];
                                            (exact_historical_parent_context(scoped, &owner, system, &schemas).await?, system)
                                        } else { (scoped, parent_time) };
                                        rtdb_query::owned_identity_candidates_in_prefix(scoped, &owner, &relation, system,
                                            at, &after, fetch, prefix,
                                        ).await
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = scoped_read!(read) => result,
                                    };
                                    send_archive_answer(&plaintext, reply, response).await;
                                }
                            }
                        }
                        Ok(())
                    }
                },
            ).await;
            if let Some(ready) = ready {
                let _ = ready.send(outcome);
            }
            worker.cancel();
        };
        tokio::join!(expire, serve);
    });
    opened.await.map_err(|_| OwnedWorld::unavailable())??;
    let held = HeldCheckpointWorld { open, projections, memory, requests, cancellation };
    held.check()?;
    let _ = opening.disarm();
    Ok(held)
}

#[cfg(test)]
mod ancestor_boundaries_tests {
    use super::*;

    #[test]
    fn world_search_uses_the_exact_text_of_a_complete_replacement() -> Result<()> {
        let body = |value: Option<&str>| -> ArrayRef {
            Arc::new(arrow_array::StructArray::from(vec![(
                Arc::new(arrow_schema::Field::new(rtdb_types::OWNED_FACT_TEXT_VALUE, arrow_schema::DataType::Utf8, true)),
                Arc::new(arrow_array::StringArray::from(vec![value])) as ArrayRef,
            )]))
        };
        let row = RecordBatch::try_from_iter([
            ("body", body(Some("café\0🙂"))),
            ("null", body(None)),
            ("empty", body(Some(""))),
            ("binary", Arc::new(arrow_array::BinaryArray::from_vec(vec![b"a"])) as ArrayRef),
        ]).map_err(rtdb_columnar::arrow_error)?;
        assert_eq!(search_row_text(&row, "body")?, Some("café\0🙂"));
        assert_eq!(search_row_text(&row, "empty")?, Some(""));
        assert_eq!(search_row_text(&row, "null")?, None);
        assert_eq!(search_row_text(&row, "missing")?, None);
        assert!(search_row_text(&row, "binary").is_err());
        Ok(())
    }

    #[test]
    fn merged_identities_stop_at_the_first_unseen_source() {
        let parent = ["a", "c"].map(|value| EntityId::String(value.to_owned()));
        let overlay = ["b", "e"].map(|value| EntityId::String(value.to_owned()));
        let (candidates, through) = identity_candidates(&parent, &overlay, 2);
        assert_eq!(through, Some(&EntityId::String("c".to_owned())));
        assert_eq!(candidates.cloned().collect::<Vec<_>>(), ["a", "b", "c"].map(|value| EntityId::String(value.to_owned())));
    }

    #[test]
    fn retained_overlay_masks_parent_boundaries_inside_its_extent() -> Result<()> {
        let parent = [0, 4, 7, 10].into_iter().map(Timestamp).collect();
        let overlay = [1, 8].into_iter().map(Timestamp).collect();
        let valid = Interval::new(Timestamp(0), Timestamp(10))?;
        assert_eq!(merge_boundaries(parent, overlay, valid)?,
            [0, 1, 8, 10].into_iter().map(Timestamp).collect::<Vec<_>>());
        Ok(())
    }
}
