use std::time::Duration;

use rtdb_node::{NodeOptions, OwnedCountRequest, OwnedRangeCount, OwnedRangePage, OwnedRangeRequest};
use rtdb_plan::Catalog;
use rtdb_storage::cache::CacheBudget;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::*;
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
    async fn check(&self, check: rtdb_node::ArchivedWorldConstraints<'_>) -> Result<()> {
        let unavailable = OwnedWorld::unavailable;
        let installed = self.directory.deployment_by_name(check.database).await?.ok_or_else(unavailable)?;
        if PlacedAttemptScope::new(&installed.stream("replica"), installed.epoch)? != check.placement {
            return Err(unavailable());
        }
        let world = check.world;
        let pin = Uuid::from_bytes(world.pin.0);
        let retained = installed.checkpoint_worlds.get(&pin).ok_or_else(unavailable)?;
        if retained.ancestor.is_some() || world.ancestor.is_some()
            || retained.world.as_bytes() != &world.id.0
            || retained.owner != check.owner
            || retained.capture.as_bytes() != &world.capture.0
            || retained.suffix.map(|suffix| Iid(*suffix.as_bytes())) != world.suffix
            || retained.manifest_sha256 != world.manifest
            || retained.schema_digest != world.schemas.digest()
            || retained.basis != world.basis
            || retained.parent_publication != world.parent_publication
            || retained.mode != world.mode
            || retained.until != world.until
            || check.ordered >= retained.until
        {
            return Err(unavailable());
        }
        let parent = CheckpointReadWorld {
            deployment: Uuid::parse_str(&installed.id).map_err(|_| unavailable())?,
            capture: retained.capture,
            manifest_sha256: retained.manifest_sha256.clone(),
            suffix: retained.suffix,
            basis: retained.basis,
        };
        admit_capture(&installed, &self.storage, &parent)?;
        let resources = CheckpointReadResources {
            node: &self.options, memory: check.memory, cache: Arc::clone(&check.cache), cancellation: check.cancellation,
        };
        with_checkpoint_context(
            self.directory.as_ref(), &self.storage, self.keys.as_ref(), self.hosted_task.as_ref(),
            &parent, resources, |context| check.validate(context),
        ).await
    }
}

pub(crate) struct HeldCheckpointWorld {
    open: OpenWorld,
    memory: Arc<MemoryBudget>,
    requests: mpsc::Sender<ArchiveRead>,
    cancellation: CancellationToken,
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
        after: Timestamp,
        through: Timestamp,
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
        schema: rtdb_types::OwnedSchema,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<AncestorResult>>,
    },
    PathColumns {
        relation: OwnedRelation,
        path: OwnedRelation,
        expected_schema: Option<rtdb_types::OwnedSchema>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<Vec<String>>>,
    },
    Search {
        partitions: Vec<(OwnedRelation, OwnedRelation)>,
        frontiers: Vec<i64>,
        query: String,
        overlay: Option<rtdb_node::HeldTextCorpus>,
        ancestor_schemas: Option<Vec<rtdb_types::OwnedSchema>>,
        offset: usize,
        limit: usize,
        valid: Timestamp,
        parent_at: Option<(Timestamp, Vec<(OwnedRelation, rtdb_types::OwnedSchema)>)>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<rtdb_node::OwnedSearchPage>>,
    },
    SearchColumns {
        partitions: Vec<(OwnedRelation, OwnedRelation)>,
        frontiers: Vec<i64>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<Vec<String>>>,
    },
    Ordered {
        relation: OwnedRelation,
        path: OwnedRelation,
        frontier: i64,
        lower: Option<StructFields<Value>>,
        upper: Option<StructFields<Value>>,
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
        after: Option<String>,
        fetch: usize,
        earlier: Option<(Timestamp, rtdb_types::OwnedSchema)>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<rtdb_query::OwnedIdentityCandidates>>,
    },
}

enum AncestorRead {
    Point { id: EntityId, valid: Interval },
    Schedule { id: EntityId, valid: Interval },
    Identities { valid: Timestamp, after: Option<String>, limit: usize },
    OverlayIdentities { valid: Timestamp, after: Option<String>, limit: usize },
    Range {
        path: OwnedRelation,
        lower: Option<StructFields<Value>>,
        upper: Option<StructFields<Value>>,
        after: Option<StructFields<Value>>,
        descending: bool,
        limit: usize,
        valid: Timestamp,
    },
}

enum AncestorResult {
    Point(Option<WorldFact>),
    Schedule(Vec<(Interval, Timestamp)>),
    Identities(WorldIdentityPage),
    Range(OwnedRangePage),
}

enum OrderedRead {
    Page { after: Option<StructFields<Value>>, descending: bool, limit: usize },
    Count,
    Measure,
}

enum OrderedResult {
    Page(OwnedRangePage),
    Count(u64),
    Measure(OwnedRangeCount),
}

pub(crate) struct WorldIdentityPage {
    pub keys: Vec<String>,
    pub more: bool,
    pub reservations: Vec<Arc<MemoryReservation>>,
}

impl HeldCheckpointWorld {
    pub fn present(&self) -> TransactionKey { self.open.snapshot.present() }

    pub fn application_clock(&self) -> Option<Timestamp> { self.open.clock }
    pub fn initial_clock(&self) -> Timestamp { self.open.record.initial_clock() }
    pub fn clock_mode(&self) -> WorldClockMode { self.open.record.mode }

    pub async fn prepare_overlay_cut(
        &self, scope: rtdb_types::PlacedAttemptScope, system: Timestamp,
    ) -> Result<rtdb_node::OwnedForkCut> {
        self.check()?;
        let cut = self.open.snapshot.fork_overlay_cut(scope, &self.open.record, system).await?;
        self.check()?;
        Ok(cut)
    }

    pub fn parent_basis(&self) -> TransactionKey { self.open.retained.basis }
    pub fn ancestor(&self) -> Option<(Iid, TransactionKey)> {
        self.open.record.ancestor.as_ref().map(|ancestor| (Iid(ancestor.world), ancestor.basis))
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

    pub async fn changes(
        &self, relations: &[OwnedRelation], after: Timestamp, through: Timestamp, limit: usize, descending: bool,
    ) -> Result<rtdb_node::OwnedMergedChanges> {
        self.check()?;
        let guard = self.open.snapshot.relation_release_guard(relations)?;
        self.require_archive_coverage(Vec::new(), Arc::clone(&guard)).await?;
        let page = self.open.snapshot.world_merged_changes_ordered(self.open.record.id, relations, after, through, limit, descending).await?;
        guard()?;
        self.check()?;
        Ok(page)
    }

    async fn require_archive_coverage(
        &self, frontiers: Vec<(OwnedRelation, i64)>, guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Coverage {
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
        if self.ancestor().is_some() {
            return self.ancestor_point(relation, id, valid_at, observed).await;
        }
        self.check_relation(relation)?;
        let (local, witness) = self.open.read_local(relation, id, valid_at, &self.memory, observed).await?;
        let shadowed = local.is_some();
        let fact = if let Some(fact) = local {
            fact
        } else {
            self.archive_point(relation, id, valid_at, None).await?
        };
        self.check_relation(relation)?;
        Ok(WorldPoint { present: self.present(), fact, until: None, shadowed, witness })
    }

    pub async fn path_touch(
        &self, relation: &OwnedRelation, path: &OwnedRelation, id: &EntityId,
        group: Timestamp, valid: Timestamp, images: bool,
    ) -> Result<[(Option<WorldFact>, bool); 2]> {
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
            let shadowed = local.is_some();
            let mut fact = match local {
                Some(fact) => match WorldShadow::from_record(fact.row)? {
                    WorldShadow::Deleted => None,
                    WorldShadow::Present(row) => Some(WorldFact {
                        row, revision: fact.revision, lifetime: None, _reservation: fact._reservation,
                    }),
                },
                None => self.archive_point(relation, id, valid, None).await?,
            };
            if let Some(fact) = &mut fact {
                if !fact.row.contains_key("_id") { return Err(OwnedWorld::unavailable()); }
                if images {
                    schema.validate_row(&fact.row)?;
                } else {
                    fact.row.retain(|name, _| name == "_id" || columns.contains(name));
                    for column in &columns {
                        if !fact.row.contains_key(column) { fact.row.insert(column.clone(), Value::Null); }
                    }
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
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::PathColumns {
            relation: relation.clone(), path: path.clone(), expected_schema: expected_schema.cloned(), guard, reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let parent_columns = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        let schema = self.open.record.schemas.relation(relation).ok_or_else(OwnedWorld::unavailable)?;
        if parent_columns.as_slice() != columns || columns.last().is_none_or(|column| column != "_id")
            || columns[..columns.len() - 1].iter().any(|column| schema.path_column_type(column).is_none())
        {
            return Err(OwnedWorld::unavailable());
        }
        Ok(())
    }

    pub async fn preflight_accepted_scope(&self, relations: &[OwnedRelation], system: Timestamp) -> Result<Timestamp> {
        self.check()?;
        let clock = self.open.snapshot.certify_world_accepted_scope(&self.open.record, relations, system).await?;
        self.require_accepted_archive_scope(relations).await?;
        Ok(clock)
    }

    pub async fn select_accepted_scope(&self, relations: &[OwnedRelation], requested: Timestamp)
        -> Result<Option<(Timestamp, Timestamp)>> {
        self.check()?;
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
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::AcceptedScope {
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
            schemas.push((relation.clone(), self.historical_parent_schema(relation)?.clone()));
            let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
            if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        }
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::ParentAsOf {
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
    ) -> Result<Option<WorldFact>> {
        self.check_relation(relation)?;
        if system > self.parent_basis().system_time { return Err(OwnedWorld::unavailable()); }
        let schema = self.historical_parent_schema(relation)?;
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let guard = self.open.snapshot.parent_point_release_guard(relation, id, system).await?;
        let valid = Interval::at(valid)?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::ParentPointAt {
            system, point: ArchivePoint {
                relation: relation.clone(), id: id.clone(), valid, lifetime: None,
                expected_schema: Some(schema.clone()), guard: Arc::clone(&guard), reply,
            },
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let fact = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        if let Some(fact) = &fact {
            if fact.revision > system { return Err(OwnedWorld::unavailable()); }
            schema.validate_row(&fact.row)?;
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
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::ParentScheduleAt {
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
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::ParentTouchImagesAt {
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
        &self, relation: &OwnedRelation, selected: Timestamp, after: Timestamp,
        through: Timestamp, limit: usize, reverse: bool,
    ) -> Result<(rtdb_node::OwnedMergedChanges, OwnedWorldWitness)> {
        self.check_relation(relation)?;
        if after > through || through > selected || selected > self.parent_basis().system_time {
            return Err(OwnedWorld::unavailable());
        }
        let schema = self.historical_parent_schema(relation)?.clone();
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let guard = self.open.snapshot.relation_release_guard([relation])?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::ParentChangesAt {
            relation: relation.clone(), selected, after, through, limit, reverse, schema, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let page = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        guard()?;
        self.check_relation(relation)?;
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
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::ParentVersionsAt {
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
        system: Timestamp,
    ) -> Result<(Option<WorldFact>, bool)> {
        self.check_relation(relation)?;
        let schema = self.open.record.schemas.relation(relation).ok_or_else(OwnedWorld::unavailable)?;
        if schema.has_opaque_columns() || schema.lifetime_micros().is_some() {
            return Err(OwnedWorld::unavailable());
        }
        let cut = self.open.snapshot.certify_world_accepted_cut(&self.open.record, relation, system).await?;
        let guard = self.open.snapshot.relation_release_guard([relation])?;
        self.require_archive_coverage(vec![(relation.clone(), 0)], guard).await?;
        self.open.snapshot.check_point_purge_floor(relation, id, self.parent_basis().system_time).await?;
        self.point_at_accepted_cut(relation, id, valid, &cut, schema).await
    }

    async fn point_at_accepted_cut(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Timestamp,
        cut: &rtdb_node::OwnedWorldAcceptedCut<'_>, schema: &rtdb_types::OwnedSchema,
    ) -> Result<(Option<WorldFact>, bool)> {
        let local = cut.shadow(id, valid).await?;
        let local = decode_point(local.row.into_iter().collect(), id, &self.memory, None)?;
        let shadowed = local.is_some();
        let fact = match local {
            Some(fact) => match WorldShadow::from_record(fact.row)? {
                WorldShadow::Deleted => None,
                WorldShadow::Present(row) => Some(WorldFact {
                    row, revision: fact.revision, lifetime: None, _reservation: fact._reservation,
                }),
            },
            None => self.archive_point_checked(relation, id, valid, None, Some(schema)).await?,
        };
        if let Some(fact) = &fact {
            let upper = if shadowed { cut.system_time() } else { self.parent_basis().system_time };
            if fact.revision > upper { return Err(OwnedWorld::unavailable()); }
            schema.validate_row(&fact.row)?;
        }
        self.check_relation(relation)?;
        Ok((fact, shadowed))
    }

    async fn read_ancestor(
        &self, relation: &OwnedRelation, read: AncestorRead,
    ) -> Result<(AncestorResult, OwnedWorldWitness)> {
        self.check_relation(relation)?;
        let schema = self.historical_parent_schema(relation)?.clone();
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let guard = match &read {
            AncestorRead::Point { id, .. } | AncestorRead::Schedule { id, .. } =>
                self.open.snapshot.parent_point_release_guard(relation, id, self.open.retained.basis.system_time).await?,
            AncestorRead::Identities { .. } | AncestorRead::OverlayIdentities { .. } | AncestorRead::Range { .. } =>
                self.open.snapshot.relation_release_guard([relation])?,
        };
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Ancestor {
            relation: relation.clone(), read, schema, guard: Arc::clone(&guard), reply,
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
            id: id.clone(), valid: Interval::at(valid)?,
        }).await?;
        let AncestorResult::Point(fact) = result else { return Err(OwnedWorld::unavailable()); };
        Ok(WorldPoint { present: self.present(), fact, until: None, shadowed: false, witness: observed.then_some(witness) })
    }

    pub async fn parent_point(
        &self, relation: &OwnedRelation, id: &EntityId, valid_at: Timestamp, observed: bool,
    ) -> Result<WorldPoint> {
        if self.ancestor().is_some() {
            return self.ancestor_point(relation, id, valid_at, observed).await;
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
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Point(ArchivePoint {
            relation: relation.clone(), id: id.clone(), valid, lifetime, expected_schema: expected_schema.cloned(), guard, reply,
        }))).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?
    }
    pub async fn ranked_search(
        &self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, staged: Option<rtdb_node::HeldTextCorpus>,
    ) -> Result<(rtdb_node::OwnedSearchPage, Vec<OwnedWorldWitness>)> {
        if staged.is_some() && (self.clock_mode() != WorldClockMode::Proposal || valid != self.present().system_time) {
            return Err(OwnedWorld::unavailable());
        }
        self.ranked_search_on_cut(partitions, query, offset, limit, valid, None, None, staged).await
    }

    pub async fn ranked_search_at(
        &self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp, parent_only: bool,
    ) -> Result<(rtdb_node::OwnedSearchPage, Vec<OwnedWorldWitness>)> {
        self.ranked_search_on_cut(partitions, query, offset, limit, valid, Some((system, parent_only)), None, None).await
    }

    pub async fn ranked_search_parent_at(
        &self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp,
    ) -> Result<(rtdb_node::OwnedSearchPage, Vec<OwnedWorldWitness>)> {
        self.ranked_search_on_cut(partitions, query, offset, limit, valid, None, Some(system), None).await
    }

    async fn ranked_search_on_cut(
        &self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, earlier: Option<(Timestamp, bool)>, parent_at: Option<Timestamp>,
        staged: Option<rtdb_node::HeldTextCorpus>,
    ) -> Result<(rtdb_node::OwnedSearchPage, Vec<OwnedWorldWitness>)> {
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
                || (!parent_only && !self.clock_mode().is_writable())
                || partitions.iter().any(|(relation, _)| self.open.record.schemas.relation(relation)
                    .is_none_or(|schema| schema.has_opaque_columns() || schema.lifetime_micros().is_some()))
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
                Ok((relation.clone(), self.historical_parent_schema(relation)?.clone()))
            }).collect::<Result<Vec<_>>>()?;
            Some((system, schemas))
        } else { None };
        let ancestor_schemas = if self.ancestor().is_some() && parent_at.is_none() {
            if earlier.is_some() || frontiers.iter().any(|frontier| *frontier != 0) {
                return Err(OwnedWorld::unavailable());
            }
            Some(partitions.iter().map(|(relation, _)| {
                self.historical_parent_schema(relation).cloned()
            }).collect::<Result<Vec<_>>>()?)
        } else {
            None
        };
        let historical = parent_at.is_some();
        let writable = self.clock_mode().is_writable();
        let use_overlay = writable && !historical && !earlier.is_some_and(|(_, parent_only)| parent_only);
        let columns = if use_overlay || earlier.is_some() {
            self.search_columns(&partitions, &frontiers, &guard).await?
        } else {
            Vec::new()
        };
        let has_staged = staged.is_some();
        let overlay = if use_overlay {
            let corpus = match staged {
                Some(staged) => staged,
                None => rtdb_node::HeldTextCorpus::new(Arc::clone(&self.memory), &query)?,
            };
            Some(self.search_overlay(&partitions, &columns, valid, corpus, &guard,
                earlier.map(|(system, _)| system)).await?)
        } else {
            None
        };
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Search {
            partitions: partitions.clone(), frontiers, query: query.clone(), overlay,
            ancestor_schemas, offset, limit, valid, parent_at, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let page = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let page = match page {
            Err(error) if !has_staged && !historical && earlier.is_none() && writable && error.code == "rtdb.owned/search-uncovered" => {
                self.rank_visible_documents(&partitions, &columns, &query, offset, limit, valid, &guard).await?
            }
            other => other?,
        };
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
            let mut after = None;
            loop {
                guard()?;
                // This is the physical shadow relation: its typed tombstones
                // are still present identities and also mask the archived row.
                let candidates = if let Some(cut) = &cut {
                    cut.overlay_identities(valid, after.as_deref(), 128).await?
                } else {
                    self.open.snapshot.world_identity_candidates(
                        self.open.record.id, relation, valid, after.as_deref(), 128,
                    ).await?
                };
                if cut.is_some() {
                    remaining = remaining.checked_sub(candidates.keys.len()).ok_or_else(OwnedWorld::unavailable)?;
                }
                for key in &candidates.keys {
                    let id = EntityId::String(key.clone());
                    if corpus.contains_shadow(partition, &id) { continue; }
                    if let Some(cut) = &cut {
                        let shadow = cut.shadow(&id, valid).await?;
                        let fact = decode_point(shadow.row.into_iter().collect(), &id, &self.memory, None)?
                            .ok_or_else(OwnedWorld::unavailable)?;
                        let row = WorldShadow::from_record(fact.row)?;
                        let text = match &row {
                            WorldShadow::Deleted => None,
                            WorldShadow::Present(row) => search_row_text(row, column)?,
                        };
                        corpus.shadow(partition, id, text)?;
                    } else {
                        let (shadow, _) = self.open.read_local(relation, &id, valid, &self.memory, false).await?;
                        let shadow = shadow.ok_or_else(OwnedWorld::unavailable)?;
                        let text = shadow.as_ref().map(|fact| search_text(fact, column)).transpose()?.flatten();
                        corpus.shadow(partition, id, text)?;
                    }
                }
                if candidates.keys.len() < 128 { break; }
                after = Some(candidates.keys.last().cloned().ok_or_else(OwnedWorld::unavailable)?);
            }
        }
        guard()?;
        Ok(corpus)
    }

    async fn search_columns(
        &self, partitions: &[(OwnedRelation, OwnedRelation)], frontiers: &[i64],
        guard: &Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> Result<Vec<String>> {
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::SearchColumns {
            partitions: partitions.to_vec(), frontiers: frontiers.to_vec(), guard: Arc::clone(guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let columns = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        if columns.len() != partitions.len() || partitions.iter().zip(&columns).any(|((relation, _), column)| {
            !self.open.record.schemas.relation(relation)
                .and_then(|schema| schema.path_column_logical_type(column))
                .is_some_and(|kind| kind.is_clear_search_text())
        }) {
            return Err(OwnedWorld::unavailable());
        }
        Ok(columns)
    }

    async fn rank_visible_documents(
        &self, partitions: &[(OwnedRelation, OwnedRelation)], columns: &[String], query: &str,
        offset: usize, limit: usize, valid: Timestamp,
        guard: &Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> Result<rtdb_node::OwnedSearchPage> {
        let mut corpus = rtdb_node::HeldTextCorpus::new(Arc::clone(&self.memory), query)?;
        for (partition, ((relation, _), column)) in partitions.iter().zip(columns).enumerate() {
            let mut after = None;
            loop {
                guard()?;
                let page = self.identity_page(relation, valid, after.as_deref(), 128).await?;
                for key in &page.keys {
                    let id = EntityId::String(key.clone());
                    let point = self.point(relation, &id, valid, false).await?;
                    let fact = point.fact.ok_or_else(OwnedWorld::unavailable)?;
                    corpus.observe(partition, id, search_text(&fact, column)?)?;
                }
                if !page.more { break; }
                after = Some(page.keys.last().cloned().ok_or_else(OwnedWorld::unavailable)?);
            }
        }
        guard()?;
        corpus.finish(offset, limit)
    }

    pub async fn range(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'_>, valid: Timestamp,
    ) -> Result<(OwnedRangePage, OwnedWorldWitness)> {
        let read = OrderedRead::Page {
            after: request.after.cloned(), descending: request.descending, limit: request.limit,
        };
        let (result, witness) = self.ordered(relation, path, request.lower, request.upper, valid, read, None).await?;
        let OrderedResult::Page(page) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((page, witness))
    }

    pub async fn overlay_range(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'_>, valid: Timestamp,
    ) -> Result<(OwnedRangePage, OwnedWorldWitness)> {
        self.check_relation(&relation)?;
        if self.ancestor().is_some() {
            let (result, witness) = self.read_ancestor(&relation, AncestorRead::Range {
                path, lower: request.lower.cloned(), upper: request.upper.cloned(),
                after: request.after.cloned(), descending: request.descending,
                limit: request.limit, valid,
            }).await?;
            let AncestorResult::Range(page) = result else { return Err(OwnedWorld::unavailable()); };
            return Ok((page, witness));
        }
        if !self.clock_mode().is_writable() { return Err(OwnedWorld::unavailable()); }
        let world = self.open.record.id;
        let witness = self.open.snapshot.world_relation_witnesses(world, [&relation])
            .await?.pop().ok_or_else(OwnedWorld::unavailable)?;
        self.require_overlay_coverage(&relation, &witness).await?;
        let page = self.open.snapshot.range_world_overlay(world, &relation, &path, request, valid).await?;
        self.check_relation(&relation)?;
        Ok((page, witness))
    }

    pub async fn overlay_range_at(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'_>,
        valid: Timestamp, system: Timestamp,
    ) -> Result<OwnedRangePage> {
        self.check_relation(&relation)?;
        let cut = self.open.snapshot.certify_world_accepted_cut(&self.open.record, &relation, system).await?;
        self.require_accepted_archive_scope(std::slice::from_ref(&relation)).await?;
        let page = cut.overlay_range(&path, request, valid).await?;
        let columns = page.schema.fields().iter().map(|field| field.name().clone()).collect::<Vec<_>>();
        self.require_parent_path(&relation, &path, &columns, None).await?;
        self.check_relation(&relation)?;
        Ok(page)
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
            after: request.after.cloned(), descending: request.descending, limit: request.limit,
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
        &self, relation: OwnedRelation, path: OwnedRelation, lower: Option<&StructFields<Value>>,
        upper: Option<&StructFields<Value>>, valid: Timestamp, read: OrderedRead, system: Option<Timestamp>,
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
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Ordered {
            relation, path, frontier: witness.purge_frontier(), lower: lower.cloned(), upper: upper.cloned(),
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
        let parent = self.archive_boundaries(relation, id, valid, Some(schema.clone())).await?;
        let boundaries = merge_boundaries(parent, local, valid)?;
        let mut bytes = 0usize;
        let mut slices = Vec::with_capacity(boundaries.len().saturating_sub(1));
        let _framing = self.memory.reserve(boundaries.len().saturating_mul(256))?;
        for window in boundaries.windows(2) {
            guard()?;
            let local = version.shadow_at(window[0]).await?;
            let local = decode_point(local.row.into_iter().collect(), id, &self.memory, None)?;
            let shadowed = local.is_some();
            let fact = match local {
                Some(fact) => match WorldShadow::from_record(fact.row)? {
                    WorldShadow::Deleted => None,
                    WorldShadow::Present(row) => Some(WorldFact {
                        row, revision: fact.revision, lifetime: None, _reservation: fact._reservation,
                    }),
                },
                None => self.archive_point(relation, id, window[0], None).await?,
            };
            if let Some(fact) = fact {
                if fact.revision > group { return Err(OwnedWorld::unavailable()); }
                schema.validate_row(&fact.row)?;
                bytes = bytes.checked_add(Value::document_retained_bytes(&fact.row)?)
                    .filter(|bytes| *bytes <= 8 << 20).ok_or_else(history_limit)?;
                slices.push((Interval::new(window[0], window[1])?, fact, shadowed));
            }
        }
        guard()?;
        self.check_relation(relation)?;
        Ok((clock, slices))
    }

    async fn archive_boundaries(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Interval, expected_schema: Option<rtdb_types::OwnedSchema>,
    ) -> Result<Vec<Timestamp>> {
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Boundaries {
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
        if self.ancestor().is_some() {
            if system.is_some() { return Err(OwnedWorld::unavailable()); }
            let (result, witness) = self.read_ancestor(relation, AncestorRead::Schedule { id: id.clone(), valid }).await?;
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
        let parent = self.archive_boundaries(relation, id, valid, earlier.as_ref().map(|(_, schema)| (**schema).clone())).await?;
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
        &self, relation: &OwnedRelation, valid: Timestamp, after: Option<&str>, limit: usize,
    ) -> Result<WorldIdentityPage> {
        if self.ancestor().is_some() {
            Interval::at(valid)?;
            if !(1..=128).contains(&limit) { return Err(OwnedWorld::unavailable()); }
            let (result, _) = self.read_ancestor(relation, AncestorRead::Identities {
                valid, after: after.map(str::to_owned), limit,
            }).await?;
            let AncestorResult::Identities(page) = result else { return Err(OwnedWorld::unavailable()); };
            return Ok(page);
        }
        self.identity_page_on_cut(relation, valid, after, limit, None).await
    }

    pub async fn identity_page_at(
        &self, relation: &OwnedRelation, valid: Timestamp, after: Option<&str>, limit: usize,
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
        if parent_only {
            if system != self.parent_basis().system_time { return Err(OwnedWorld::unavailable()); }
            let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
            self.require_overlay_coverage(relation, &witness).await?;
            self.require_accepted_archive_scope(std::slice::from_ref(relation)).await?;
            let parent = self.archive_identity_candidates(relation, valid, after, limit + 1, None).await?;
            for key in &parent.keys {
                let id = EntityId::String(key.clone());
                self.open.snapshot.check_point_purge_floor(relation, &id, system).await?;
            }
            return self.finish_overlay_identities(relation, parent, limit);
        }
        let cut = self.open.snapshot.certify_world_accepted_cut(&self.open.record, relation, system).await?;
        self.require_accepted_archive_scope(std::slice::from_ref(relation)).await?;
        self.identity_page_on_cut(relation, valid, after, limit, Some(&cut)).await
    }

    pub async fn parent_identity_page_at(
        &self, relation: &OwnedRelation, valid: Timestamp, after: Option<&str>, limit: usize, system: Timestamp,
    ) -> Result<WorldIdentityPage> {
        self.check_relation(relation)?;
        Interval::at(valid)?;
        if !(1..=128).contains(&limit) || system > self.parent_basis().system_time {
            return Err(OwnedWorld::unavailable());
        }
        let schema = self.historical_parent_schema(relation)?.clone();
        let witness = self.open.snapshot.world_parent_witness(self.open.record.id, relation).await?;
        if witness.purge_frontier() != 0 { return Err(OwnedWorld::unavailable()); }
        let candidates = self.archive_identity_candidates(relation, valid, after, limit + 1, Some((system, schema))).await?;
        for key in &candidates.keys {
            let id = EntityId::String(key.clone());
            self.open.snapshot.check_point_purge_floor(relation, &id, system).await?;
        }
        self.finish_overlay_identities(relation, candidates, limit)
    }

    async fn archive_identity_candidates(
        &self, relation: &OwnedRelation, valid: Timestamp, after: Option<&str>, fetch: usize,
        earlier: Option<(Timestamp, rtdb_types::OwnedSchema)>,
    ) -> Result<rtdb_query::OwnedIdentityCandidates> {
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Identities {
            relation: relation.clone(), at: valid, after: after.map(str::to_owned), fetch, earlier,
            guard: self.open.snapshot.relation_release_guard([relation])?, reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?
    }

    async fn identity_page_on_cut(
        &self, relation: &OwnedRelation, valid: Timestamp, after: Option<&str>, limit: usize,
        cut: Option<&rtdb_node::OwnedWorldAcceptedCut<'_>>,
    ) -> Result<WorldIdentityPage> {
        self.check_relation(relation)?;
        Interval::at(valid)?;
        if !(1..=128).contains(&limit) { return Err(OwnedWorld::unavailable()); }
        const FETCH: usize = 129;
        let mut cursor = after.map(|key| (key.to_owned(), None));
        let mut keys = Vec::with_capacity(limit);
        let mut bytes = limit * 128;
        let mut more = false;
        let mut complete = false;
        let mut reservations = vec![self.memory.reserve(bytes)?];
        for _ in 0..32 {
            let after = cursor.as_ref().map(|(key, _)| key.as_str());
            let local = if let Some(cut) = cut {
                cut.overlay_identities(valid, after, FETCH).await?
            } else {
                self.open.snapshot.world_identity_candidates(self.open.record.id, relation, valid, after, FETCH).await?
            };
            let parent = self.archive_identity_candidates(relation, valid, after, FETCH, None).await?;

            let (candidates, through) = identity_candidates(&parent.keys, &local.keys, FETCH);
            for key in candidates {
                let id = EntityId::String(key.clone());
                let shadow = if let Some(cut) = cut {
                    let read = cut.shadow(&id, valid).await?;
                    decode_point(read.row.into_iter().collect(), &id, &self.memory, None)?
                        .map(|fact| WorldShadow::from_record(fact.row).map(|shadow| matches!(shadow, WorldShadow::Present(_))))
                        .transpose()?
                } else {
                    self.open.read_local(relation, &id, valid, &self.memory, false).await?.0
                        .map(|fact| fact.is_some())
                };
                let visible = match shadow {
                    Some(visible) => visible,
                    None => {
                        if local.keys.binary_search(key).is_ok() || parent.keys.binary_search(key).is_err() {
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
                    bytes = bytes.checked_add(key.len()).filter(|size| *size <= (8 << 20)).ok_or_else(OwnedWorld::unavailable)?;
                    reservations.push(self.memory.reserve(key.len())?);
                    keys.push(key.clone());
                }
            }
            if more || through.is_none() {
                complete = true;
                break;
            }
            if let Some(through) = through {
                let charge = self.memory.reserve(through.len())?;
                cursor = Some((through.clone(), Some(charge)));
            }
        }
        if !complete { return Err(OwnedWorld::unavailable()); }
        self.check_relation(relation)?;
        Ok(WorldIdentityPage { keys, more, reservations })
    }

    pub async fn overlay_identities(
        &self, relation: &OwnedRelation, valid: Timestamp, after: Option<&str>, limit: usize,
    ) -> Result<WorldIdentityPage> {
        self.check_relation(relation)?;
        if !(1..=128).contains(&limit) { return Err(OwnedWorld::unavailable()); }
        if self.ancestor().is_some() {
            Interval::at(valid)?;
            let (result, _) = self.read_ancestor(relation, AncestorRead::OverlayIdentities {
                valid, after: after.map(str::to_owned), limit,
            }).await?;
            let AncestorResult::Identities(page) = result else { return Err(OwnedWorld::unavailable()); };
            return Ok(page);
        }
        let witness = self.open.snapshot.world_relation_witnesses(self.open.record.id, [relation])
            .await?.pop().ok_or_else(OwnedWorld::unavailable)?;
        self.require_overlay_coverage(relation, &witness).await?;
        if !self.clock_mode().is_writable() {
            self.check_relation(relation)?;
            return Ok(WorldIdentityPage { keys: Vec::new(), more: false, reservations: Vec::new() });
        }
        let candidates = self.open.snapshot.world_identity_candidates(
            self.open.record.id, relation, valid, after, limit + 1,
        ).await?;
        self.finish_overlay_identities(relation, candidates, limit)
    }

    pub async fn overlay_identities_at(
        &self, relation: &OwnedRelation, valid: Timestamp, after: Option<&str>, limit: usize, system: Timestamp,
    ) -> Result<WorldIdentityPage> {
        self.check_relation(relation)?;
        if !(1..=128).contains(&limit) { return Err(OwnedWorld::unavailable()); }
        let cut = self.open.snapshot.certify_world_accepted_cut(&self.open.record, relation, system).await?;
        self.require_accepted_archive_scope(std::slice::from_ref(relation)).await?;
        let candidates = cut.overlay_identities(valid, after, limit + 1).await?;
        self.finish_overlay_identities(relation, candidates, limit)
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
    search_row_text(fact.row(), column)
}

fn search_row_text<'a>(row: &'a StructFields<Value>, column: &str) -> Result<Option<&'a str>> {
    match row.get(column) {
        Some(Value::Utf8(text)) => Ok(Some(text)),
        None | Some(Value::Null) => Ok(None),
        _ => Err(OwnedWorld::unavailable()),
    }
}

struct InheritedRelation<'a> {
    root: Arc<rtdb_query::QueryContext>,
    overlay: Arc<rtdb_query::QueryContext>,
    owner: &'a str,
    relation: &'a OwnedRelation,
    shadow: OwnedRelation,
    overlay_basis: Timestamp,
    schema: &'a rtdb_types::OwnedSchema,
    memory: &'a Arc<MemoryBudget>,
}

impl<'a> InheritedRelation<'a> {
    async fn open(
        root: Arc<rtdb_query::QueryContext>, overlay: Arc<rtdb_query::QueryContext>, owner: &'a str,
        relation: &'a OwnedRelation, ancestor: &rtdb_directory::CheckpointWorldAncestor,
        schema: &'a rtdb_types::OwnedSchema, memory: &'a Arc<MemoryBudget>,
    ) -> Result<Self> {
        let unavailable = OwnedWorld::unavailable;
        check_archive_coverage(&root, owner, [(relation, 0)])?;
        check_archive_coverage(&overlay, owner, [(relation, 0)])?;
        let world = Iid(*ancestor.world.as_bytes());
        let (record, _charge) = rtdb_query::read_owned_world(Arc::clone(&overlay), owner, world)
            .await?.ok_or_else(unavailable)?;
        let clock = rtdb_query::read_owned_world_clock_revision(Arc::clone(&overlay), owner, world).await?;
        let root_schema = rtdb_query::read_owned_relation_schema(Arc::clone(&root), owner, relation).await?;
        if record.dropped || record.ancestor.is_some() || record.pin.0 != *ancestor.pin.as_bytes()
            || !matches!(record.mode, WorldClockMode::Held { .. }) || record.schemas.relation(relation) != Some(schema)
            || record.basis != ancestor.source_root || ancestor.source_root.system_time > root.current_time.as_micros()?
            || clock != Some((ancestor.logical, ancestor.basis.system_time))
            || !root_schema.is_some_and(|(actual, _)| &actual == schema)
        {
            return Err(unavailable());
        }
        let shadow = relation.in_world(world)?;
        Ok(Self { root, overlay, owner, relation, shadow, overlay_basis: ancestor.basis.system_time, schema, memory })
    }

    async fn shadow(&self, id: &EntityId, valid: Interval) -> Result<Option<Option<WorldFact>>> {
        let unavailable = OwnedWorld::unavailable;
        let mut shadow = None;
        if let Some(mut rows) = rtdb_query::open_owned_point(
            Arc::clone(&self.overlay), self.owner, &self.shadow, id, Some(valid), None,
        ).await? {
            while let Some(batch) = rows.next_batch().await? {
                if batch.batch().num_rows() == 0 { continue; }
                if shadow.is_some() { return Err(unavailable()); }
                shadow = decode_point(vec![batch], id, self.memory, None)?;
            }
        }
        self.overlay.check_cancelled()?;
        if let Some(shadow) = shadow {
            if shadow.revision > self.overlay_basis { return Err(unavailable()); }
            let fact = match WorldShadow::from_record(shadow.row)? {
                WorldShadow::Deleted => None,
                WorldShadow::Present(row) => {
                    self.schema.validate_row(&row)?;
                    Some(WorldFact { row, revision: shadow.revision, lifetime: None, _reservation: shadow._reservation })
                }
            };
            return Ok(Some(fact));
        }
        Ok(None)
    }

    async fn point(&self, id: &EntityId, valid: Interval) -> Result<Option<WorldFact>> {
        if let Some(shadow) = self.shadow(id, valid).await? {
            return Ok(shadow);
        }
        let unavailable = OwnedWorld::unavailable;
        let batches = read_checkpoint_point_in_context(Arc::clone(&self.root), CheckpointPoint {
            owner: self.owner, relation: self.relation, id, valid: Some(valid),
        }, None).await?;
        let fact = decode_point(batches.unwrap_or_default(), id, self.memory, None)?;
        if let Some(fact) = &fact {
            if fact.revision > self.root.current_time.as_micros()? { return Err(unavailable()); }
            self.schema.validate_row(&fact.row)?;
        }
        self.root.check_cancelled()?;
        Ok(fact)
    }

    async fn overlay_identity_page(
        &self, valid: Timestamp, after: Option<&str>, limit: usize,
    ) -> Result<WorldIdentityPage> {
        let page = rtdb_query::owned_identity_candidates(
            Arc::clone(&self.overlay), self.owner, &self.shadow, self.overlay_basis, valid, after, limit + 1,
        ).await?;
        let (mut keys, reservations) = page.into_parts();
        let more = keys.len() > limit;
        keys.truncate(limit);
        Ok(WorldIdentityPage { keys, more, reservations })
    }

    async fn overlay_range(
        &self, path: &OwnedRelation, request: OwnedRangeRequest<'_>, valid: Timestamp,
    ) -> Result<OwnedRangePage> {
        rtdb_node::stable_owned_path_columns(
            &self.root, &self.overlay, self.owner, self.relation, path,
        ).await?;
        rtdb_node::range_owned_cut(
            Arc::clone(&self.overlay), self.owner, &self.shadow, path, request, valid,
        ).await
    }

    async fn identities(&self, valid: Timestamp, after: Option<String>, limit: usize) -> Result<WorldIdentityPage> {
        const FETCH: usize = 129;
        let root_basis = self.root.current_time.as_micros()?;
        let mut keys = Vec::with_capacity(limit);
        let mut bytes = limit * 128;
        let mut reservations = vec![self.memory.reserve(bytes)?];
        let mut cursor = after.map(|key| {
            let charge = self.memory.reserve(key.len())?;
            Ok::<_, rtdb_types::Anomaly>((key, charge))
        }).transpose()?;
        for _ in 0..32 {
            let after = cursor.as_ref().map(|(key, _)| key.as_str());
            let parent = rtdb_query::owned_identity_candidates(
                Arc::clone(&self.root), self.owner, self.relation, root_basis, valid, after, FETCH,
            ).await?;
            let overlay = rtdb_query::owned_identity_candidates(
                Arc::clone(&self.overlay), self.owner, &self.shadow, self.overlay_basis, valid, after, FETCH,
            ).await?;
            let (candidates, through) = identity_candidates(&parent.keys, &overlay.keys, FETCH);
            for key in candidates {
                let id = EntityId::String(key.clone());
                if self.point(&id, Interval::at(valid)?).await?.is_none() { continue; }
                if keys.len() == limit {
                    return Ok(WorldIdentityPage { keys, more: true, reservations });
                }
                bytes = bytes.checked_add(key.len()).filter(|bytes| *bytes <= 8 << 20)
                    .ok_or_else(OwnedWorld::unavailable)?;
                reservations.push(self.memory.reserve(key.len())?);
                keys.push(key.clone());
            }
            let Some(through) = through else { return Ok(WorldIdentityPage { keys, more: false, reservations }); };
            cursor = Some((through.clone(), self.memory.reserve(through.len())?));
        }
        Err(OwnedWorld::unavailable())
    }

    async fn search_shadows(
        &self, path: &OwnedRelation, partition: usize, valid: Timestamp,
        corpus: &mut rtdb_node::HeldTextCorpus, remaining: &mut usize,
    ) -> Result<()> {
        let unavailable = OwnedWorld::unavailable;
        let column = rtdb_node::owned_text_column(
            Arc::clone(&self.root), self.owner, self.relation, path,
        ).await?;
        let archived = rtdb_node::owned_text_column(
            Arc::clone(&self.overlay), self.owner, self.relation, path,
        ).await?;
        if column != archived || !self.schema.path_column_logical_type(&column)
            .is_some_and(|kind| kind.is_clear_search_text())
        {
            return Err(unavailable());
        }
        let instant = Interval::at(valid)?;
        let mut cursor = None;
        loop {
            let after = cursor.as_ref().map(|(key, _): &(String, Arc<MemoryReservation>)| key.as_str());
            let page = self.overlay_identity_page(valid, after, 128).await?;
            *remaining = remaining.checked_sub(page.keys.len()).ok_or_else(unavailable)?;
            for key in &page.keys {
                let id = EntityId::String(key.clone());
                let shadow = self.shadow(&id, instant).await?.ok_or_else(unavailable)?;
                let text = shadow.as_ref().map(|row| search_text(row, &column)).transpose()?.flatten();
                corpus.shadow(partition, id, text)?;
            }
            if !page.more { return Ok(()); }
            let key = page.keys.last().ok_or_else(unavailable)?;
            cursor = Some((key.clone(), self.memory.reserve(key.len())?));
        }
    }

    async fn schedule(&self, id: &EntityId, valid: Interval) -> Result<Vec<(Interval, Timestamp)>> {
        let root_boundaries = rtdb_query::open_owned_valid_boundaries(
            Arc::clone(&self.root), self.owner, self.relation, id, valid, self.root.current_time.as_micros()?,
        ).await?;
        let overlay_boundaries = rtdb_query::open_owned_valid_boundaries(
            Arc::clone(&self.overlay), self.owner, &self.shadow, id, valid, self.overlay_basis,
        ).await?;
        let boundaries = merge_boundaries(
            read_boundaries(root_boundaries, valid).await?,
            read_boundaries(overlay_boundaries, valid).await?, valid,
        )?;
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
    parent: &'a [String], overlay: &'a [String], fetch: usize,
) -> (impl Iterator<Item = &'a String>, Option<&'a String>) {
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
        if snapshot.relation_event_frontier(&relation.purge_table(owner))? != expected {
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

async fn historical_parent_context(
    context: Arc<rtdb_query::QueryContext>, owner: &str, requested: Timestamp,
    schemas: &[(OwnedRelation, rtdb_types::OwnedSchema)],
) -> Result<Option<(Arc<rtdb_query::QueryContext>, Timestamp)>> {
    let unavailable = OwnedWorld::unavailable;
    let parent = context.current_time.as_micros()?;
    if requested > parent { return Err(unavailable()); }
    check_archive_coverage(&context, owner, schemas.iter().map(|(relation, _)| (relation, 0)))?;
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(unavailable()); };
    if snapshot.tx_basis().is_none_or(|basis| basis.system_time != parent)
        || snapshot.system_time_cap() != Some(parent)
        || snapshot.relation_event_frontier(&rtdb_types::TableRef::new("public", "bj_rtdb_restore_hold"))? != 0
    {
        return Err(unavailable());
    }
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
    if selected > requested || (requested == parent && selected != parent) {
        return Err(unavailable());
    }
    require_parent_journal(&context, owner, &publication, selected).await?;
    let capped = context.with_owned_system_cut(selected, &context.cancellation)?;
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
    let table = relation.table(owner);
    let journal = relation.change_journal(owner)?;
    let data_events = snapshot.relation_event_frontier(&table)?;
    let entries = snapshot.relation_event_frontier(&journal)?;
    if entries == 0 && data_events == 0 && snapshot.trie_entries(&table).is_empty()
        && snapshot.trie_entries(&journal).is_empty()
    {
        return Ok(());
    }
    if entries < 2 { return Err(unavailable()); }
    let floor = rtdb_query::read_owned_change_entry(Arc::clone(context), owner, relation, 0).await?;
    let first = rtdb_query::read_owned_journal_header(Arc::clone(context), owner, relation, 1).await?;
    let head = rtdb_query::read_owned_change_entry(Arc::clone(context), owner, relation, entries - 1).await?;
    if floor.value != Value::Null || floor.system_time != first.committed_at
        || floor.through_events >= first.through_events || (floor.through_events != 0 && selected < floor.system_time)
        || head.through_events != data_events || head.system_time > context.current_time.as_micros()?
        || (entries > 2 && (head.system_time <= first.committed_at || head.through_events <= first.through_events))
        || (entries == 2 && (head.system_time != first.committed_at || head.through_events != first.through_events))
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
            let (slot, field) = batch.schema_ref().column_with_name(name).ok_or_else(OwnedWorld::unavailable)?;
            match rtdb_columnar::read_value(field, batch.column(slot).as_ref(), row)? {
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

impl Drop for HeldCheckpointWorld {
    fn drop(&mut self) { self.close(); }
}

pub(crate) async fn open_held_checkpoint_world(
    archive: WorldArchive, session: &mut NativeSession, deployment: Uuid, world: Uuid,
    memory: Arc<MemoryBudget>, cache: Arc<CacheBudget>,
) -> Result<HeldCheckpointWorld> {
    let open = open_world(archive.directory.as_ref(), &archive.storage, session, deployment, world).await?;
    let parent = open.parent(deployment);
    let ancestor = open.retained.ancestor.clone();
    let owner = open.retained.owner.clone();
    let parent_time = parent.basis.system_time;
    let remaining = open.retained.until.0.saturating_sub(now().0).max(0) as u64;
    let lifetime = Duration::from_micros(remaining).min(Duration::from_secs(60));
    let deadline = tokio::time::Instant::now() + lifetime;
    let cancellation = session.state().cancellation().child_token();
    // On RPC cancellation the worker still completes the storage-region drain.
    let opening = cancellation.clone().drop_guard();
    let worker = cancellation.clone();
    let backing_memory = Arc::clone(&memory);
    let (requests, mut pending) = mpsc::channel::<ArchiveRead>(1);
    let (ready, opened) = oneshot::channel();
    tokio::spawn(async move {
        let timer = worker.clone();
        let expire = async {
            if timer.run_until_cancelled(tokio::time::sleep_until(deadline)).await.is_some() {
                timer.cancel();
            }
        };
        let serve = async {
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
                            match request {
                                ArchiveRead::Ancestor { relation, read, schema, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let ancestor = ancestor.as_ref().ok_or_else(OwnedWorld::unavailable)?;
                                        let media = ancestor_media(deployment, ancestor);
                                        with_checkpoint_context(
                                            archive.directory.as_ref(), &archive.storage, archive.keys.as_ref(),
                                            archive.hosted_task.as_ref(), &media,
                                            CheckpointReadResources {
                                                node: &archive.options, memory, cache: Arc::clone(&ancestor_cache), cancellation: scope,
                                            },
                                            |overlay| {
                                                let root = Arc::clone(&context).with_owned_read_guard(Arc::clone(&guard));
                                                let overlay = overlay.with_owned_read_guard(Arc::clone(&guard));
                                                let (owner, relation, schema) = (&owner, &relation, &schema);
                                                async move {
                                                    let inherited = InheritedRelation::open(root, overlay, owner, relation, ancestor, schema, memory).await?;
                                                    match read {
                                                        AncestorRead::Point { id, valid } =>
                                                            Ok(AncestorResult::Point(inherited.point(&id, valid).await?)),
                                                        AncestorRead::Schedule { id, valid } =>
                                                            Ok(AncestorResult::Schedule(inherited.schedule(&id, valid).await?)),
                                                        AncestorRead::Identities { valid, after, limit } =>
                                                            Ok(AncestorResult::Identities(inherited.identities(valid, after, limit).await?)),
                                                        AncestorRead::OverlayIdentities { valid, after, limit } =>
                                                            Ok(AncestorResult::Identities(inherited.overlay_identity_page(valid, after.as_deref(), limit).await?)),
                                                        AncestorRead::Range { path, lower, upper, after, descending, limit, valid } => {
                                                            let request = OwnedRangeRequest {
                                                                lower: lower.as_ref(), upper: upper.as_ref(), after: after.as_ref(),
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
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
                                }
                                ArchiveRead::Coverage { frontiers, guard, reply } => {
                                    let coverage = guard().and_then(|()| check_archive_coverage(
                                        &context, &owner, frontiers.iter().map(|(relation, frontier)| (relation, *frontier)),
                                    ));
                                    let _ = reply.send(coverage);
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
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
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
                                            schemas[0].1.validate_row(&fact.row)?;
                                            schedule.push((interval, fact.revision));
                                        }
                                        context.check_cancelled()?;
                                        Ok(schedule)
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
                                }
                                ArchiveRead::ParentTouchImagesAt { relation, path, id, selected, before, group, valid, images, schema, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        check_archive_coverage(&context, &owner, [(&relation, 0)])?;
                                        let selected_context = exact_historical_parent_context(Arc::clone(&context), &owner, selected, &[]).await?;
                                        let selected_schema = rtdb_query::read_owned_relation_schema(selected_context, &owner, &relation).await?;
                                        if !selected_schema.is_some_and(|(actual, _)| actual == schema) {
                                            return Err(OwnedWorld::unavailable());
                                        }
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
                                                        schema.validate_row(&fact.row)?;
                                                    } else {
                                                        for column in columns {
                                                            if !fact.row.contains_key(column) { fact.row.insert(column.clone(), Value::Null); }
                                                        }
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
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
                                }
                                ArchiveRead::ParentChangesAt { relation, selected, after, through, limit, reverse, schema, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        check_archive_coverage(&context, &owner, [(&relation, 0)])?;
                                        let capped = exact_historical_parent_context(Arc::clone(&context), &owner, selected, &[]).await?;
                                        let actual = rtdb_query::read_owned_relation_schema(capped, &owner, &relation).await?;
                                        if !actual.is_some_and(|(actual, _)| actual == schema) {
                                            return Err(OwnedWorld::unavailable());
                                        }
                                        rtdb_node::archived_changes_at_cut(context, &owner, &relation, after, through, limit, reverse).await
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
                                }
                                ArchiveRead::ParentVersionsAt { relation, id, selected, request, schema, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        let schemas = [(relation.clone(), schema)];
                                        exact_historical_parent_context(Arc::clone(&context), &owner, selected, &schemas).await?;
                                        require_parent_journal(&context, &owner, &relation, request.system.lower()).await?;
                                        let page = rtdb_node::archived_versions_at_cut(
                                            Arc::clone(&context), &owner, &relation, &id, selected, request,
                                        ).await?;
                                        let mut versions = Vec::with_capacity(page.versions.len());
                                        let mut remaining = 8usize << 20;
                                        for mut version in page.versions {
                                            let version_context = context.with_owned_system_cut(version.system_time, &context.cancellation)?;
                                            let version_schema = rtdb_query::read_owned_relation_schema(version_context, &owner, &relation).await?;
                                            if !version_schema.is_some_and(|(schema, _)| schema == schemas[0].1) {
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
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
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
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
                                }
                                ArchiveRead::AcceptedScope { schemas, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        check_archive_coverage(&context, &owner, schemas.iter().map(|(relation, _)| (relation, 0)))?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        for (relation, expected) in schemas {
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
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
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
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
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
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
                                }
                                ArchiveRead::Search { partitions, frontiers, query, overlay, ancestor_schemas, offset, limit, valid, parent_at, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        check_archive_coverage(&context, &owner,
                                            partitions.iter().map(|(relation, _)| relation).zip(frontiers))?;
                                        let scoped = Arc::clone(&context).with_owned_read_guard(Arc::clone(&guard));
                                        let scoped = if let Some((system, schemas)) = parent_at {
                                            exact_historical_parent_context(scoped, &owner, system, &schemas).await?
                                        } else { scoped };
                                        let overlay = if let Some(schemas) = ancestor_schemas {
                                            let ancestor = ancestor.as_ref().ok_or_else(OwnedWorld::unavailable)?;
                                            let media = ancestor_media(deployment, ancestor);
                                            let corpus = with_checkpoint_context(
                                                archive.directory.as_ref(), &archive.storage, archive.keys.as_ref(),
                                                archive.hosted_task.as_ref(), &media,
                                                CheckpointReadResources {
                                                    node: &archive.options, memory, cache: Arc::clone(&ancestor_cache), cancellation: scope,
                                                },
                                                |overlay| {
                                                    let overlay = overlay.with_owned_read_guard(Arc::clone(&guard));
                                                    let (root, owner, partitions, query) = (&scoped, &owner, &partitions, &query);
                                                    async move {
                                                        let mut corpus = rtdb_node::HeldTextCorpus::new(Arc::clone(memory), query)?;
                                                        let mut remaining = 4096;
                                                        for (index, ((relation, path), schema)) in partitions.iter().zip(schemas).enumerate() {
                                                            let inherited = InheritedRelation::open(
                                                                Arc::clone(root), Arc::clone(&overlay), owner, relation, ancestor, &schema, memory,
                                                            ).await?;
                                                            inherited.search_shadows(path, index, valid, &mut corpus, &mut remaining).await?;
                                                        }
                                                        Ok(corpus)
                                                    }
                                                },
                                            ).await?;
                                            Some(corpus)
                                        } else {
                                            overlay
                                        };
                                        let page = rtdb_node::rank_owned_overlay_cut(scoped, &owner, &partitions, &query, offset, limit, Some(valid), overlay).await?;
                                        guard()?;
                                        Ok(page)
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
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
                                        guard()?;
                                        Ok(columns)
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
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
                                        let (lower, upper) = (lower.as_ref(), upper.as_ref());
                                        let result = match read {
                                            OrderedRead::Page { after, descending, limit } => {
                                                let request = OwnedRangeRequest { lower, upper, after: after.as_ref(), descending, limit };
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
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
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
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
                                }
                                ArchiveRead::Identities { relation, at, after, fetch, earlier, guard, mut reply } => {
                                    let read = async {
                                        require_relation_purge_history(&context, &owner)?;
                                        let scoped = Arc::clone(&context).with_owned_read_guard(guard);
                                        let (scoped, system) = if let Some((system, schema)) = earlier {
                                            let schemas = [(relation.clone(), schema)];
                                            (exact_historical_parent_context(scoped, &owner, system, &schemas).await?, system)
                                        } else { (scoped, parent_time) };
                                        rtdb_query::owned_identity_candidates(scoped, &owner, &relation, system,
                                            at, after.as_deref(), fetch,
                                        ).await
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
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
    let held = HeldCheckpointWorld { open, memory, requests, cancellation };
    held.check()?;
    let _ = opening.disarm();
    Ok(held)
}

#[cfg(test)]
mod ancestor_boundaries_tests {
    use super::*;

    #[test]
    fn merged_identities_stop_at_the_first_unseen_source() {
        let parent = ["a".to_owned(), "c".to_owned()];
        let overlay = ["b".to_owned(), "e".to_owned()];
        let (candidates, through) = identity_candidates(&parent, &overlay, 2);
        assert_eq!(through.map(String::as_str), Some("c"));
        assert_eq!(candidates.map(String::as_str).collect::<Vec<_>>(), ["a", "b", "c"]);
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
