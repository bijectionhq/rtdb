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
    guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    reply: oneshot::Sender<Result<Option<WorldFact>>>,
}

enum ArchiveRead {
    Coverage {
        frontiers: Vec<(OwnedRelation, i64)>,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<()>>,
    },
    Point(ArchivePoint),
    PathColumns {
        relation: OwnedRelation,
        path: OwnedRelation,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<Vec<String>>>,
    },
    Search {
        partitions: Vec<(OwnedRelation, OwnedRelation)>,
        frontiers: Vec<i64>,
        query: String,
        overlay: Option<rtdb_node::HeldTextCorpus>,
        offset: usize,
        limit: usize,
        valid: Timestamp,
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
        read: OrderedRead,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<OrderedResult>>,
    },
    Boundaries {
        relation: OwnedRelation,
        id: EntityId,
        valid: Interval,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<Vec<Timestamp>>>,
    },
    Identities {
        relation: OwnedRelation,
        at: Timestamp,
        after: Option<String>,
        fetch: usize,
        guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: oneshot::Sender<Result<rtdb_query::OwnedIdentityCandidates>>,
    },
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

    pub fn parent_basis(&self) -> TransactionKey { self.open.retained.basis }
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
        group: Timestamp, valid: Timestamp,
    ) -> Result<[(Option<WorldFact>, bool); 2]> {
        self.check_relation(relation)?;
        let (columns, [before, after]) = self.open.snapshot.world_path_touch(
            self.open.record.id, relation, path, id, group, valid,
        ).await?;
        self.require_parent_path(relation, path, &columns).await?;
        let mut sides = [(None, false), (None, false)];
        for (local, side) in [before, after].into_iter().zip(&mut sides) {
            let local = decode_point(local.row.into_iter().collect(), id, &self.memory, None)?;
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
                fact.row.retain(|name, _| name == "_id" || columns.contains(name));
                for column in &columns {
                    if !fact.row.contains_key(column) { fact.row.insert(column.clone(), Value::Null); }
                }
            }
            *side = (fact, shadowed);
        }
        self.check_relation(relation)?;
        Ok(sides)
    }

    pub async fn path_touch_after(
        &self, relation: &OwnedRelation, path: &OwnedRelation, id: &EntityId,
        group: Timestamp, valid: Timestamp,
    ) -> Result<WorldFact> {
        self.check_relation(relation)?;
        let schema = self.open.record.schemas.relation(relation).ok_or_else(OwnedWorld::unavailable)?;
        if schema.has_opaque_columns() || schema.lifetime_micros().is_some() {
            return Err(OwnedWorld::unavailable());
        }
        let (columns, local) = self.open.snapshot.world_path_touch_after(
            self.open.record.id, relation, path, id, group, valid,
        ).await?;
        self.require_parent_path(relation, path, &columns).await?;
        let fact = decode_point(local.row.into_iter().collect(), id, &self.memory, None)?
            .filter(|fact| fact.revision == group).ok_or_else(OwnedWorld::unavailable)?;
        let WorldShadow::Present(row) = WorldShadow::from_record(fact.row)? else {
            return Err(OwnedWorld::unavailable());
        };
        schema.validate_row(&row)?;
        self.check_relation(relation)?;
        Ok(WorldFact { row, revision: fact.revision, lifetime: None, _reservation: fact._reservation })
    }

    async fn require_parent_path(
        &self, relation: &OwnedRelation, path: &OwnedRelation, columns: &[String],
    ) -> Result<()> {
        let guard = self.open.snapshot.relation_release_guard([relation])?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::PathColumns {
            relation: relation.clone(), path: path.clone(), guard, reply,
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

    pub async fn parent_point(
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
        let valid = Interval::at(valid_at)?;
        let guard = self.open.snapshot.parent_point_release_guard(relation, id, self.open.retained.basis.system_time).await?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Point(ArchivePoint {
            relation: relation.clone(), id: id.clone(), valid, lifetime, guard, reply,
        }))).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?
    }
    pub async fn ranked_search(
        &self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp,
    ) -> Result<(rtdb_node::OwnedSearchPage, Vec<OwnedWorldWitness>)> {
        self.check()?;
        if offset > 4096 || !(1..=128).contains(&limit) || !(1..=32).contains(&partitions.len())
            || query.len() > 4096
            || partitions.iter().map(|(relation, _)| relation).collect::<std::collections::BTreeSet<_>>().len() != partitions.len()
        {
            return Err(OwnedWorld::unavailable());
        }
        Interval::at(valid)?;
        let guard = self.open.snapshot.relation_release_guard(partitions.iter().map(|(relation, _)| relation))?;
        let witnesses = self.open.snapshot.world_relation_witnesses(
            self.open.record.id, partitions.iter().map(|(relation, _)| relation),
        ).await?;
        let frontiers = witnesses.iter().map(OwnedWorldWitness::purge_frontier).collect::<Vec<_>>();
        let writable = self.clock_mode().is_writable();
        let columns = if writable {
            self.search_columns(&partitions, &frontiers, &guard).await?
        } else {
            Vec::new()
        };
        let overlay = if writable {
            Some(self.search_overlay(&partitions, &columns, valid, &query, &guard).await?)
        } else {
            None
        };
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Search {
            partitions: partitions.clone(), frontiers, query: query.clone(), overlay,
            offset, limit, valid, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let page = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let page = match page {
            Err(error) if writable && error.code == "rtdb.owned/search-uncovered" => {
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
        query: &str, guard: &Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> Result<rtdb_node::HeldTextCorpus> {
        let mut corpus = rtdb_node::HeldTextCorpus::new(Arc::clone(&self.memory), query)?;
        for (partition, ((relation, _), column)) in partitions.iter().zip(columns).enumerate() {
            let mut after = None;
            loop {
                guard()?;
                // This is the physical shadow relation: its typed tombstones
                // are still present identities and also mask the archived row.
                let candidates = self.open.snapshot.world_identity_candidates(
                    self.open.record.id, relation, valid, after.as_deref(), 128,
                ).await?;
                for key in &candidates.keys {
                    let id = EntityId::String(key.clone());
                    let (shadow, _) = self.open.read_local(relation, &id, valid, &self.memory, false).await?;
                    let shadow = shadow.ok_or_else(OwnedWorld::unavailable)?;
                    let text = shadow.as_ref().map(|fact| search_text(fact, column)).transpose()?.flatten();
                    corpus.shadow(partition, id, text)?;
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
        let (result, witness) = self.ordered(relation, path, request.lower, request.upper, valid, read).await?;
        let OrderedResult::Page(page) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((page, witness))
    }

    pub async fn overlay_range(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'_>, valid: Timestamp,
    ) -> Result<(OwnedRangePage, OwnedWorldWitness)> {
        self.check_relation(&relation)?;
        if !self.clock_mode().is_writable() { return Err(OwnedWorld::unavailable()); }
        let world = self.open.record.id;
        let witness = self.open.snapshot.world_relation_witnesses(world, [&relation])
            .await?.pop().ok_or_else(OwnedWorld::unavailable)?;
        self.require_overlay_coverage(&relation, &witness).await?;
        let page = self.open.snapshot.range_world_overlay(world, &relation, &path, request, valid).await?;
        self.check_relation(&relation)?;
        Ok((page, witness))
    }

    async fn require_overlay_coverage(&self, relation: &OwnedRelation, witness: &OwnedWorldWitness) -> Result<()> {
        let guard = self.open.snapshot.relation_release_guard([relation])?;
        self.require_archive_coverage(vec![(relation.clone(), witness.purge_frontier())], guard).await
    }

    pub async fn count(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'_>,
    ) -> Result<(u64, OwnedWorldWitness)> {
        let (result, witness) = self.ordered(relation, path, request.lower, request.upper, request.valid_at, OrderedRead::Count).await?;
        let OrderedResult::Count(count) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((count, witness))
    }

    pub async fn measure(
        &self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'_>,
    ) -> Result<(OwnedRangeCount, OwnedWorldWitness)> {
        if self.clock_mode().is_writable() { return Err(OwnedWorld::unavailable()); }
        let (result, witness) = self.ordered(
            relation, path, request.lower, request.upper, request.valid_at, OrderedRead::Measure,
        ).await?;
        let OrderedResult::Measure(measured) = result else { return Err(OwnedWorld::unavailable()); };
        Ok((measured, witness))
    }

    async fn ordered(
        &self, relation: OwnedRelation, path: OwnedRelation, lower: Option<&StructFields<Value>>,
        upper: Option<&StructFields<Value>>, valid: Timestamp, read: OrderedRead,
    ) -> Result<(OrderedResult, OwnedWorldWitness)> {
        self.check_relation(&relation)?;
        Interval::at(valid)?;
        let guard = self.open.snapshot.relation_release_guard([&relation])?;
        let witness = self.open.snapshot.world_relation_witnesses(
            self.open.record.id, [&relation],
        ).await?.pop().ok_or_else(OwnedWorld::unavailable)?;
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Ordered {
            relation, path, frontier: witness.purge_frontier(), lower: lower.cloned(), upper: upper.cloned(),
            valid, read, guard: Arc::clone(&guard), reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let result = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        self.check()?;
        guard()?;
        Ok((result, witness))
    }

    pub async fn valid_during(
        &self, relation: &OwnedRelation, id: &EntityId, valid: Interval, parent_only: bool,
    ) -> Result<(Vec<(Interval, Timestamp)>, Option<OwnedWorldWitness>)> {
        self.check_relation(relation)?;
        if valid.is_empty() { return Err(OwnedWorld::unavailable()); }
        if parent_only && self.open.snapshot.lifetime_point_policy(relation).await?.is_some() {
            return Err(OwnedWorld::unavailable());
        }
        self.open.snapshot.check_point_purge_floor(relation, id, self.open.retained.basis.system_time).await?;
        let local = if parent_only {
            Vec::new()
        } else {
            let scan = self.open.snapshot.open_world_shadow_boundaries(self.open.record.id, relation, id, valid).await?;
            read_boundaries(scan, valid).await?
        };
        let (reply, result) = oneshot::channel();
        self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Boundaries {
            relation: relation.clone(), id: id.clone(), valid,
            guard: self.open.snapshot.relation_release_guard([relation])?, reply,
        })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
        let parent = self.cancellation.run_until_cancelled(result).await
            .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;
        // A parent boundary inside a local shadow cannot affect the selected timeline.
        let mut boundaries: Vec<_> = parent.into_iter().filter(|boundary| {
            !local.chunks_exact(2).any(|span| span[0] < *boundary && *boundary < span[1])
        }).collect();
        boundaries.extend(local);
        boundaries.extend([valid.lower(), valid.upper()]);
        boundaries.sort_unstable();
        boundaries.dedup();
        if boundaries.len() > MAX_HISTORY_SLICES + 1 { return Err(history_limit()); }
        let mut visible = Vec::with_capacity(boundaries.len().saturating_sub(1));
        for window in boundaries.windows(2) {
            let fact = if parent_only {
                self.archive_point(relation, id, window[0], None).await?
            } else {
                self.point(relation, id, window[0], false).await?.fact
            };
            if let Some(fact) = fact {
                visible.push((Interval::new(window[0], window[1])?, fact.revision));
            }
        }
        let witness = if self.evidence_until() > self.present().system_time {
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
            let local = self.open.snapshot.world_identity_candidates(
                self.open.record.id, relation, valid, cursor.as_ref().map(|(key, _)| key.as_str()), FETCH,
            ).await?;
            let (reply, result) = oneshot::channel();
            self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Identities {
                relation: relation.clone(), at: valid, after: cursor.as_ref().map(|(key, _)| key.clone()), fetch: FETCH,
                guard: self.open.snapshot.relation_release_guard([relation])?, reply,
            })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
            let parent = self.cancellation.run_until_cancelled(result).await
                .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;

            // A full input page can have unseen keys beyond its last key.
            let through = [
                (local.keys.len() == FETCH).then(|| local.keys.last()).flatten(),
                (parent.keys.len() == FETCH).then(|| parent.keys.last()).flatten(),
            ].into_iter().flatten().min();
            let candidates: std::collections::BTreeSet<_> = parent.keys.iter().chain(&local.keys).collect();
            for key in candidates.into_iter().take_while(|key| through.is_none_or(|through| *key <= through)) {
                let id = EntityId::String(key.clone());
                let (shadow, _) = self.open.read_local(relation, &id, valid, &self.memory, false).await?;
                let visible = match shadow {
                    Some(fact) => fact.is_some(),
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
    match fact.row().get(column) {
        Some(Value::Utf8(text)) => Ok(Some(text)),
        None | Some(Value::Null) => Ok(None),
        _ => Err(OwnedWorld::unavailable()),
    }
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

const MAX_HISTORY_SLICES: usize = 128;

fn history_limit() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::new(
        rtdb_types::AnomalyCategory::Busy, "rtdb.world/history-size",
        "The world history interval exceeds its boundary limit",
    )
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
                                ArchiveRead::Coverage { frontiers, guard, reply } => {
                                    let coverage = guard().and_then(|()| check_archive_coverage(
                                        &context, &owner, frontiers.iter().map(|(relation, frontier)| (relation, *frontier)),
                                    ));
                                    let _ = reply.send(coverage);
                                }
                                ArchiveRead::PathColumns { relation, path, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        let context = Arc::clone(&context).with_owned_read_guard(guard);
                                        rtdb_node::path_columns_at_cut(&context, &owner, &relation, &path).await
                                    };
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = read => result,
                                    };
                                    let _ = reply.send(response);
                                }
                                ArchiveRead::Point(ArchivePoint { relation, id, valid, lifetime, guard, mut reply }) => {
                                    let read = async {
                                        let batches = read_checkpoint_point_in_context(Arc::clone(&context).with_owned_read_guard(guard), CheckpointPoint {
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
                                ArchiveRead::Search { partitions, frontiers, query, overlay, offset, limit, valid, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        check_archive_coverage(&context, &owner,
                                            partitions.iter().map(|(relation, _)| relation).zip(frontiers))?;
                                        let scoped = Arc::clone(&context).with_owned_read_guard(Arc::clone(&guard));
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
                                ArchiveRead::Ordered { relation, path, frontier, lower, upper, valid, read, guard, mut reply } => {
                                    let read = async {
                                        guard()?;
                                        check_archive_coverage(&context, &owner, [(&relation, frontier)])?;
                                        let context = Arc::clone(&context).with_owned_read_guard(Arc::clone(&guard));
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
                                ArchiveRead::Boundaries { relation, id, valid, guard, mut reply } => {
                                    let read = async {
                                        require_relation_purge_history(&context, &owner)?;
                                        let cursor = rtdb_query::open_owned_valid_boundaries(
                                            Arc::clone(&context).with_owned_read_guard(guard),
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
                                ArchiveRead::Identities { relation, at, after, fetch, guard, mut reply } => {
                                    let read = async {
                                        require_relation_purge_history(&context, &owner)?;
                                        rtdb_query::owned_identity_candidates(
                                            Arc::clone(&context).with_owned_read_guard(guard), &owner, &relation, parent_time,
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
