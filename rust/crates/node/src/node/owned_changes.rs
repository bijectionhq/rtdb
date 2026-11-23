use std::{collections::BTreeSet, sync::Arc};

use rtdb_columnar::MemoryReservation;
use rtdb_index::Snapshot;
use rtdb_plan::Catalog;
use rtdb_query::{OwnedJournalHeader, read_owned_change_entry, read_owned_journal_header};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Interval, OWNED_CHANGE_GROUP_BYTES,
    OWNED_CHANGE_PAGE_BYTES, OWNED_CHANGE_PAGE_GROUPS, OwnedChange, OwnedChangeGroup,
    OwnedRelation, Result, Timestamp, Value, change_feed_uncovered,
};

use super::{
    OwnedChangePage, OwnedReadSession, OwnedVersion, OwnedVersionOrder, OwnedVersionPage,
    OwnedVersionRequest, historical_owned_read,
};

pub const OWNED_MERGED_CHANGE_RELATIONS: usize = 32;

pub struct OwnedMergedChanges {
    pub commits: Vec<OwnedChangeCommit>,
    pub retained_bytes: usize,
    _reservations: Vec<Arc<MemoryReservation>>,
    complete: bool,
    complete_boundary: Timestamp,
}

impl OwnedMergedChanges {
    pub fn is_complete(&self) -> bool { self.complete }

    pub fn covered_through(&self) -> Result<Timestamp> {
        if self.complete {
            return Ok(self.complete_boundary);
        }
        self.commits
            .last()
            .map(|commit| commit.revision)
            .ok_or_else(change_feed_uncovered)
    }
}

pub struct OwnedChangeCommit {
    pub revision: Timestamp,
    pub changes: Vec<OwnedRelationChanges>,
}

pub struct OwnedRelationChanges {
    pub relation: OwnedRelation,
    sequence: u64,
    pub touches: Vec<OwnedChange>,
}

struct CoveredJournal {
    floor: OwnedJournalHeader,
    first: OwnedJournalHeader,
    head: OwnedJournalHeader,
    head_revision: i64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ChangeScan { Forward, RetainedStart, Reverse }

struct Journal<'a> {
    relation: &'a OwnedRelation,
    revision: i64,
    head: i64,
    head_through: i64,
    previous_through: i64,
    next: OwnedJournalHeader,
}

/// One exact admitted local revision of one held-world identity.
pub struct OwnedWorldVersionRead<'a> {
    held: &'a OwnedReadSession,
    world: rtdb_types::Iid,
    relation: &'a OwnedRelation,
    id: &'a EntityId,
    system: Timestamp,
}

impl OwnedWorldVersionRead<'_> {
    pub async fn boundaries(&self) -> Result<Option<rtdb_query::QueryCursor>> {
        rtdb_query::open_owned_valid_boundaries(
            self.held.relation_context(self.relation)?, &self.held.owner,
            &self.relation.in_world(self.world)?, self.id, Interval::ALL, self.system,
        ).await
    }

    pub async fn shadow_at(&self, valid: Timestamp) -> Result<super::OwnedWorldShadowRead> {
        self.held.read_world_shadow_at(self.world, self.relation, self.id, valid, self.system, false).await
    }
}

pub struct ExpiryCandidate {
    pub identity: Vec<u8>,
    pub nomination: String,
    pub created: Timestamp,
    pub expiry: Timestamp,
}

pub struct ExpiryCandidatePage {
    pub candidates: Vec<ExpiryCandidate>,
    pub through: Timestamp,
    pub diagnostic_cutoff: Timestamp,
    pub state: &'static str,
    pub next_eligible: Option<Timestamp>,
    _charges: Vec<Arc<rtdb_columnar::MemoryReservation>>,
}

impl OwnedReadSession {
    /// Reads whole ordered owner-relation groups after an exclusive system time.
    pub async fn changes(
        &self,
        relation: &OwnedRelation,
        after: Timestamp,
        limit: usize,
    ) -> Result<OwnedChangePage> {
        let page = self
            .merged_changes(std::slice::from_ref(relation), after, limit)
            .await?;
        let covered_through = page.covered_through()?;
        let groups = page
            .commits
            .into_iter()
            .map(|commit| {
                let change = commit
                    .changes
                    .into_iter()
                    .next()
                    .ok_or_else(change_feed_uncovered)?;
                Ok(OwnedChangeGroup {
                    revision: change.sequence,
                    system_time: commit.revision,
                    touches: change.touches,
                })
            })
            .collect::<Result<_>>()?;
        Ok(OwnedChangePage {
            groups,
            covered_through,
            retained_bytes: page.retained_bytes,
            _reservations: page._reservations,
        })
    }

    /// Filters a covered journal page by complete identity and valid-time overlap.
    /// The cursor advances over examined source groups even when all their touches are outside the region.
    pub async fn changes_in_region(
        &self, relation: &OwnedRelation, after: Timestamp, limit: usize,
        lower: Option<&EntityId>, upper: Option<&EntityId>, valid: Interval,
    ) -> Result<OwnedChangePage> {
        if valid.is_empty() || lower.zip(upper).is_some_and(|(lower, upper)| lower >= upper) {
            return Err(Anomaly::incorrect("rtdb.owned/change-region", "A change region requires nonempty identity and valid-time bounds"));
        }
        let mut page = self.changes(relation, after, limit).await?;
        page.groups.retain_mut(|group| {
            group.touches.retain(|touch| {
                lower.is_none_or(|lower| &touch.id >= lower)
                    && upper.is_none_or(|upper| &touch.id < upper)
                    && touch.valid.intersects(valid)
            });
            !group.touches.is_empty()
        });
        Ok(page)
    }

    /// A bounded diagnostic page, not authority to sweep or advance a floor.
    pub async fn expired_candidate_page(
        &self, relation: &OwnedRelation, after: Timestamp, groups: usize,
        scope: rtdb_types::PlacedAttemptScope,
    ) -> Result<ExpiryCandidatePage> {
        const MAX_CANDIDATES: usize = 64;
        self.require_unscoped_root()?;
        self.admit_read().await?;
        if self.witness_scope != Some(scope) || !(1..=8).contains(&groups) {
            return Err(change_feed_uncovered());
        }
        let policy = self.lifetime_point_policy(relation).await?.ok_or_else(change_feed_uncovered)?;
        let cutoff = rtdb_types::OwnedLifetimeFloor::expiry_replay_cutoff(self.present.system_time);
        let context = self.journal_context(relation)?;
        let mut candidates = Vec::new();
        let mut charges = Vec::new();
        let mut through = after;
        let mut state = "bounded";
        let mut next_eligible = None;
        for _ in 0..groups {
            // Decode one accepted group. Its successors are unnecessary
            // while it still has a candidate or needs a later held present.
            let page = self.merged_change_journals(
                std::slice::from_ref(relation), through, self.present.system_time, 1, if through == Timestamp::MIN { ChangeScan::RetainedStart } else { ChangeScan::Forward },
            ).await?;
            let Some(group) = page.commits.first() else {
                through = page.covered_through()?;
                state = "quiescent";
                break;
            };
            let [change] = group.changes.as_slice() else { return Err(change_feed_uncovered()); };
            let expiry = Timestamp(group.revision.0.checked_add(policy.duration).ok_or_else(change_feed_uncovered)?);
            let valid = Interval::new(group.revision, expiry)?;
            for touch in &change.touches {
                match touch.kind {
                    rtdb_types::OwnedChangeKind::Put if touch.valid == valid => (),
                    rtdb_types::OwnedChangeKind::Expired(original)
                        if original.lower().0.checked_add(policy.duration) == Some(original.upper().0)
                            && original.upper() <= rtdb_types::OwnedLifetimeFloor::expiry_replay_cutoff(group.revision) => (),
                    _ => return Err(change_feed_uncovered()),
                }
            }
            for touch in change.touches.iter().filter(|touch| touch.kind == rtdb_types::OwnedChangeKind::Put) {
                if rtdb_query::owned_purge_floor(Arc::clone(&context), &self.owner, relation, &touch.id).await?.is_some() {
                    continue;
                }
                if expiry > cutoff {
                    state = "waiting";
                    next_eligible = rtdb_types::OwnedLifetimeFloor::expiry_replay_eligible_present(expiry);
                    break;
                }
                let (nomination, created, expires) = self.nominate_expired_identity(relation, &touch.id, scope).await?;
                if created != group.revision || expires != expiry {
                    return Err(change_feed_uncovered());
                }
                let bytes = touch.id.as_value().retained_bytes()?.saturating_mul(2).saturating_add(512);
                charges.push(context.memory.reserve(bytes)?);
                candidates.push(ExpiryCandidate { identity: touch.id.handle_bytes(), nomination, created, expiry });
                state = "pending";
                if candidates.len() == MAX_CANDIDATES { break; }
            }
            if state != "bounded" {
                // A nomination is not proof of erasure. On the next hold
                // revisit this entire group and skip only durable purges.
                break;
            }
            through = group.revision;
            if page.complete {
                through = page.covered_through()?;
                state = "quiescent";
                break;
            }
        }
        self.check_live_change_relations(std::slice::from_ref(relation))?;
        Ok(ExpiryCandidatePage { candidates, through, diagnostic_cutoff: cutoff, state, next_eligible, _charges: charges })
    }

    /// Reads exact local overlay commits. The archived parent is immutable;
    /// changes on the root after the fork are outside this world's journal.
    pub async fn world_merged_changes(
        &self, world: rtdb_types::Iid, relations: &[OwnedRelation],
        after: Timestamp, through: Timestamp, limit: usize,
    ) -> Result<OwnedMergedChanges> {
        self.world_merged_changes_ordered(world, relations, after, through, limit, false).await
    }

    pub async fn world_merged_changes_ordered(
        &self, world: rtdb_types::Iid, relations: &[OwnedRelation],
        after: Timestamp, through: Timestamp, limit: usize, descending: bool,
    ) -> Result<OwnedMergedChanges> {
        let (record, _charge) = self.world_registration(world).await?
            .filter(|(record, _)| !record.dropped && self.present.system_time < record.until)
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        self.require_change_bounds(after, through)?;
        let fork = record.inherited_system();
        if after < fork {
            return Err(historical_owned_read::below_floor(fork));
        }
        if !(1..=OWNED_MERGED_CHANGE_RELATIONS).contains(&relations.len()) {
            return Err(change_feed_uncovered());
        }
        let guard = self.relation_release_guard(relations)?;
        guard()?;
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else { return Err(change_feed_uncovered()); };
        let scoped = relations.iter().map(|relation| {
            if record.schemas.relation(relation).is_none()
                || snapshot.relation_event_frontier(&relation.purge_table(&self.owner))? != 0 {
                return Err(change_feed_uncovered());
            }
            relation.in_world(world)
        }).collect::<Result<Vec<_>>>()?;
        let scan = if descending { ChangeScan::Reverse } else { ChangeScan::Forward };
        let mut page = self.merged_change_journals(&scoped, after, through, limit, scan).await?;
        for commit in &mut page.commits {
            for group in &mut commit.changes {
                group.relation = group.relation.root_relation().ok_or_else(change_feed_uncovered)?;
            }
        }
        guard()?;
        Ok(page)
    }

    /// Merges only the preadmitted owner relations at this held cut.
    pub async fn merged_changes(
        &self,
        relations: &[OwnedRelation],
        after: Timestamp,
        limit: usize,
    ) -> Result<OwnedMergedChanges> {
        self.merged_changes_at(relations, after, self.present.system_time, limit).await
    }

    /// Merges only the certified interval at or below the original held present.
    pub async fn merged_changes_at(
        &self, relations: &[OwnedRelation], after: Timestamp, through: Timestamp, limit: usize,
    ) -> Result<OwnedMergedChanges> {
        if relations.iter().any(|relation| relation.world().is_some() || relation.is_world_catalog()) {
            return Err(change_feed_uncovered());
        }
        self.merged_change_journals(relations, after, through, limit, ChangeScan::Forward).await
    }

    /// Merges whole Root groups in descending order within (after, through].
    pub async fn merged_changes_descending(
        &self, relations: &[OwnedRelation], after: Timestamp, through: Timestamp, limit: usize,
    ) -> Result<OwnedMergedChanges> {
        if relations.iter().any(|relation| relation.world().is_some() || relation.is_world_catalog()) {
            return Err(change_feed_uncovered());
        }
        self.merged_change_journals(relations, after, through, limit, ChangeScan::Reverse).await
    }

    /// Admits an exact ordinary journal touch and returns its stable declared path projection.
    pub async fn path_touch_columns(
        &self, relation: &OwnedRelation, path: &OwnedRelation, id: &EntityId,
        before: Timestamp, group: Timestamp, valid: Timestamp,
    ) -> Result<Vec<String>> {
        if before.next()? != group || self.lifetime_point_policy(relation).await?.is_some() {
            return Err(change_feed_uncovered());
        }
        let page = self.merged_changes_at(std::slice::from_ref(relation), before, group, 1).await?;
        require_path_touch(&page, relation, id, group, valid)?;
        let prior = self.ordered_root_context(relation, before).await?;
        let accepted = self.ordered_root_context(relation, group).await?;
        crate::ordered_index::stable_owned_path_columns(&prior, &accepted, &self.owner, relation, path).await
    }

    /// Resolve local winners on both sides of one certified world commit.
    /// Only the held archive owner can resolve their inherited parent.
    pub async fn world_path_touch(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, path: &OwnedRelation,
        id: &EntityId, group: Timestamp, valid: Timestamp,
    ) -> Result<(Vec<String>, [super::OwnedWorldShadowRead; 2])> {
        let (before, columns) = self.certify_world_path_touch(world, relation, path, id, group, valid).await?;
        let before = self.read_world_shadow_at(world, relation, id, valid, before, false).await?;
        let after = self.read_world_shadow_at(world, relation, id, valid, group, false).await?;
        Ok((columns, [before, after]))
    }

    async fn certify_world_path_touch(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, path: &OwnedRelation,
        id: &EntityId, group: Timestamp, valid: Timestamp,
    ) -> Result<(Timestamp, Vec<String>)> {
        let before = Timestamp(group.0.checked_sub(1).ok_or_else(change_feed_uncovered)?);
        let page = self.world_merged_changes(world, std::slice::from_ref(relation), before, group, 1).await?;
        require_path_touch(&page, relation, id, group, valid)?;
        let context = self.relation_context(relation)?;
        let prior = context.with_owned_system_cut(before, &context.cancellation)?;
        let accepted = context.with_owned_system_cut(group, &context.cancellation)?;
        let columns = crate::ordered_index::stable_owned_path_columns(&prior, &accepted, &self.owner, relation, path).await?;
        Ok((before, columns))
    }

    /// Certifies a whole accepted local group and the complete address within it.
    pub async fn world_version<'a>(
        &'a self, world: rtdb_types::Iid, relation: &'a OwnedRelation, id: &'a EntityId,
        from: Timestamp, through: Timestamp, system: Timestamp,
    ) -> Result<OwnedWorldVersionRead<'a>> {
        self.require_change_bounds(from, through)?;
        if system <= from || system > through { return Err(change_feed_uncovered()); }
        let before = Timestamp(system.0.checked_sub(1).ok_or_else(change_feed_uncovered)?);
        let page = self.world_merged_changes(world, std::slice::from_ref(relation), before, system, 1).await?;
        let [commit] = page.commits.as_slice() else { return Err(change_feed_uncovered()); };
        let [changes] = commit.changes.as_slice() else { return Err(change_feed_uncovered()); };
        if commit.revision != system || changes.relation != *relation || !changes.touches.iter().any(|touch| {
            touch.id == *id && matches!(touch.kind, rtdb_types::OwnedChangeKind::Put | rtdb_types::OwnedChangeKind::Delete)
        }) {
            return Err(change_feed_uncovered());
        }
        // The exact branch journal also certifies the selected window floor,
        // including a journal with a nonzero retained starting frontier.
        self.world_merged_changes(world, std::slice::from_ref(relation), from, from, 1).await?;
        Ok(OwnedWorldVersionRead { held: self, world, relation, id, system })
    }

    pub fn check_live_change_relations(&self, relations: &[OwnedRelation]) -> Result<()> {
        self.relation_release_guard_for(relations, true)?()
    }

    fn require_change_bounds(&self, after: Timestamp, through: Timestamp) -> Result<()> {
        if after > through || through > self.present.system_time {
            return Err(change_feed_uncovered());
        }
        Ok(())
    }

    async fn merged_change_journals(
        &self, relations: &[OwnedRelation], after: Timestamp, upper: Timestamp,
        limit: usize, scan: ChangeScan,
    ) -> Result<OwnedMergedChanges> {
        if !(1..=OWNED_CHANGE_PAGE_GROUPS).contains(&limit)
            || !(1..=OWNED_MERGED_CHANGE_RELATIONS).contains(&relations.len())
        {
            return Err(Anomaly::incorrect(
                "rtdb.owned/merged-change-limits",
                "Invalid merged change page bounds",
            ));
        }
        self.require_change_bounds(after, upper)?;
        self.admit_read().await?;
        for relation in relations.iter().filter(|relation| relation.world().is_none()) {
            self.lifetime_point_policy(relation).await?;
        }
        let guard = self.relation_release_guard_for(relations, true)?;
        let guarded = Arc::clone(&self.context).with_owned_read_guard(guard);
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(change_feed_uncovered());
        };
        if snapshot.tx_basis() != Some(self.present) {
            return Err(change_feed_uncovered());
        }

        let initial_bytes = (relations.len() + limit) * 512;
        let initial_charge = self.context.memory.reserve(initial_bytes)?;
        let names: BTreeSet<_> = relations.iter().collect();
        if names.len() != relations.len() {
            return Err(Anomaly::incorrect(
                "rtdb.owned/merged-change-relations",
                "Merged change relations must be distinct",
            ));
        }
        let reverse = scan == ChangeScan::Reverse;
        let mut journals = Vec::with_capacity(relations.len());
        for relation in names {
            guarded.check_cancelled()?;
            let journal = if reverse {
                self.seek_changes_reverse(snapshot, relation, after, upper).await?
            } else {
                self.seek_changes(snapshot, relation, after, scan == ChangeScan::RetainedStart).await?
            };
            if let Some(journal) = journal { journals.push(journal); }
        }
        let in_window = |journal: &Journal<'_>| journal.active()
            && if reverse { journal.next.committed_at > after } else { journal.next.committed_at <= upper };

        let mut page = OwnedMergedChanges {
            commits: Vec::with_capacity(limit),
            retained_bytes: initial_bytes,
            _reservations: vec![initial_charge],
            complete: false,
            complete_boundary: if reverse { after } else { upper },
        };
        while page.commits.len() < limit {
            guarded.check_cancelled()?;
            let Some(revision) = journals
                .iter()
                .filter(|journal| in_window(journal))
                .map(|journal| journal.next.committed_at)
                .reduce(|a, b| if reverse { a.max(b) } else { a.min(b) })
            else {
                break;
            };
            let mut changes = Vec::new();
            let mut charges = Vec::new();
            let mut bytes = 0;
            for journal in journals
                .iter_mut()
                .filter(|journal| journal.active() && journal.next.committed_at == revision)
            {
                let entry = read_owned_change_entry(
                    Arc::clone(&guarded),
                    &self.owner,
                    journal.relation,
                    journal.revision,
                )
                .await?;
                let Value::List(touches) = entry.value else {
                    return Err(change_feed_uncovered());
                };
                let through = entry.through_events;
                let events = i64::try_from(touches.len()).map_err(|_| change_feed_uncovered())?;
                if events == 0
                    || entry.system_time != revision
                    || through != journal.next.through_events
                    || journal.previous_through.checked_add(events) != Some(through)
                {
                    return Err(change_feed_uncovered());
                }
                let group_bytes = touches.iter().try_fold(0usize, |bytes, touch| {
                    bytes
                        .checked_add(touch.retained_bytes()?)
                        .and_then(|bytes| bytes.checked_add(128))
                        .filter(|bytes| *bytes <= OWNED_CHANGE_GROUP_BYTES)
                        .ok_or_else(change_feed_uncovered)
                })?;
                let retained = group_bytes
                    .checked_add(512)
                    .ok_or_else(change_feed_uncovered)?;
                bytes += retained;
                if bytes > OWNED_CHANGE_PAGE_BYTES - page.retained_bytes {
                    if page.commits.is_empty() {
                        return Err(Anomaly::incorrect(
                            "rtdb.owned/merged-change-commit-size",
                            "A visible commit exceeds the merged change page budget",
                        ));
                    }
                    guarded.check_cancelled()?;
                    return Ok(page);
                }

                let charge = self.context.memory.reserve(retained)?;
                let touches = touches
                    .iter()
                    .map(|value| OwnedChange::from_value(value.as_ref()))
                    .collect::<Result<Vec<_>>>()?;
                let sequence =
                    u64::try_from(journal.revision).map_err(|_| change_feed_uncovered())?;
                changes.push(OwnedRelationChanges {
                    relation: journal.relation.clone(),
                    sequence,
                    touches,
                });
                charges.push(charge);
                if reverse {
                    journal.retreat(self).await?;
                } else {
                    journal.advance(self).await?;
                }
            }
            page.retained_bytes += bytes;
            page._reservations.extend(charges);
            page.commits.push(OwnedChangeCommit { revision, changes });
        }
        guarded.check_cancelled()?;
        page.complete = journals.iter().all(|journal| !in_window(journal));
        Ok(page)
    }

    pub(super) async fn certify_historical_cut(
        &self,
        relation: &OwnedRelation,
        at: Timestamp,
    ) -> Result<Option<Timestamp>> {
        self.check_relation(relation)?;
        if at > self.present.system_time {
            return Err(change_feed_uncovered());
        }
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(change_feed_uncovered());
        };
        let Some(covered) = self.covered_journal(snapshot, relation, self.present.system_time).await? else {
            self.check_relation(relation)?;
            return Ok(None);
        };
        let floor = (covered.floor.through_events != 0).then_some(covered.floor.committed_at);
        if let Some(floor) = floor.filter(|floor| at < *floor) {
            return Err(historical_owned_read::below_floor(floor));
        }
        self.require_journal_head(relation, covered.head_revision, covered.head)
            .await?;
        self.check_relation(relation)?;
        Ok(floor)
    }

    pub(super) async fn covered_range_anchor(
        &self,
        relation: &OwnedRelation,
        frontier: i64,
    ) -> Result<i64> {
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(change_feed_uncovered());
        };
        let Some(covered) = self
            .covered_journal(snapshot, relation, self.present.system_time)
            .await?
        else {
            return if frontier == 0 {
                Ok(0)
            } else {
                Err(change_feed_uncovered())
            };
        };
        if covered.head.through_events != frontier {
            return Err(change_feed_uncovered());
        }
        self.require_journal_head(relation, covered.head_revision, covered.head)
            .await?;
        Ok(covered.head_revision)
    }

    /// Pages source groups touching one identity within a certified system window.
    pub async fn versions(
        &self,
        relation: &OwnedRelation,
        id: &EntityId,
        request: OwnedVersionRequest,
    ) -> Result<OwnedVersionPage> {
        self.admit_access().await?;
        self.require_scoped_relation(relation)?;
        let mut page = read_versions_at_cut(
            self.journal_context(relation)?, &self.owner, relation, id,
            self.system_time(), request, || self.relation_context(relation),
        ).await?;
        page.witness = self.relation_witness(relation)?;
        Ok(page)
    }

    async fn version_boundary(
        &self, relation: &OwnedRelation, journal: &CoveredJournal, time: Timestamp,
    ) -> Result<i64> {
        journal_boundary(&self.journal_context(relation)?, &self.owner, relation, journal, time).await
    }

    async fn covered_journal(
        &self, snapshot: &Snapshot, relation: &OwnedRelation, after: Timestamp,
    ) -> Result<Option<CoveredJournal>> {
        if snapshot.tx_basis() != Some(self.present) { return Err(change_feed_uncovered()); }
        cover_journal(&self.journal_context(relation)?, snapshot, &self.owner, relation, self.present.system_time, after).await
    }

    async fn seek_changes<'a>(
        &self,
        snapshot: &Snapshot,
        relation: &'a OwnedRelation,
        after: Timestamp,
        from_retained_start: bool,
    ) -> Result<Option<Journal<'a>>> {
        let Some(CoveredJournal {
            floor,
            first,
            head,
            head_revision,
        }) = self.covered_journal(snapshot, relation, if from_retained_start { self.present.system_time } else { after }).await?
        else {
            return Ok(None);
        };
        if from_retained_start {
            return Ok(Some(Journal {
                relation, revision: 1, head: head_revision, head_through: head.through_events,
                previous_through: floor.through_events, next: first,
            }));
        }
        if after >= head.committed_at {
            self.require_journal_head(relation, head_revision, head)
                .await?;
            return Ok(None);
        }

        let (mut low, mut high) = (1, head_revision);
        while low < high {
            self.check()?;
            let mid = low + (high - low) / 2;
            let point = self.journal_header(relation, mid).await?;
            if !ordered_or_same(mid, 1, point, first)
                || !ordered_or_same(head_revision, mid, head, point)
            {
                return Err(change_feed_uncovered());
            }
            if point.committed_at > after {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        let next = self.journal_header(relation, low).await?;
        let previous = if low == 1 {
            floor
        } else {
            self.journal_header(relation, low - 1).await?
        };
        if next.committed_at <= after
            || next.through_events <= previous.through_events
            || (low > 1
                && (previous.committed_at > after || next.committed_at <= previous.committed_at))
            || !ordered_or_same(head_revision, low, head, next)
        {
            return Err(change_feed_uncovered());
        }
        Ok(Some(Journal {
            relation,
            revision: low,
            head: head_revision,
            head_through: head.through_events,
            previous_through: previous.through_events,
            next,
        }))
    }

    async fn seek_changes_reverse<'a>(
        &self, snapshot: &Snapshot, relation: &'a OwnedRelation, after: Timestamp, upper: Timestamp,
    ) -> Result<Option<Journal<'a>>> {
        let Some(covered) = self.covered_journal(snapshot, relation, after).await? else { return Ok(None); };
        if after == upper || upper < covered.first.committed_at {
            return Ok(None);
        }
        // Head checks and binary seek read headers only. No later payload
        // is needed to prove the exact upper boundary.
        let revision = if upper >= covered.head.committed_at {
            covered.head_revision
        } else {
            self.version_boundary(relation, &covered, upper.next()?).await? - 1
        };
        if revision < 1 { return Err(change_feed_uncovered()); }
        let next = self.journal_header(relation, revision).await?;
        if next.committed_at > upper
            || !ordered_or_same(covered.head_revision, revision, covered.head, next)
        {
            return Err(change_feed_uncovered());
        }
        if next.committed_at <= after { return Ok(None); }
        let previous = if revision == 1 { covered.floor }
            else { self.journal_header(relation, revision - 1).await? };
        if previous.through_events >= next.through_events
            || (revision > 1 && previous.committed_at >= next.committed_at)
        {
            return Err(change_feed_uncovered());
        }
        Ok(Some(Journal {
            relation, revision, head: covered.head_revision, head_through: covered.head.through_events,
            previous_through: previous.through_events, next,
        }))
    }

    async fn require_journal_head(
        &self, relation: &OwnedRelation, revision: i64, head: OwnedJournalHeader,
    ) -> Result<()> {
        validate_journal_head(&self.journal_context(relation)?, &self.owner, relation, revision, head).await
    }

    fn journal_context(&self, relation: &OwnedRelation) -> Result<Arc<rtdb_query::QueryContext>> {
        // Only the journal projects lifetime history here. The guarded data
        // and version readers still require their independent validity scope.
        let guard = self.relation_release_guard_for([relation], true)?;
        Ok(Arc::clone(self.retained_root_context()).with_owned_read_guard(guard))
    }

    async fn journal_header(
        &self,
        relation: &OwnedRelation,
        revision: i64,
    ) -> Result<OwnedJournalHeader> {
        read_owned_journal_header(self.journal_context(relation)?, &self.owner, relation, revision).await
    }
}

/// Read whole accepted groups from a pinned Root or world relation archive.
pub async fn archived_changes_at_cut(
    context: Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation,
    after: Timestamp, through: Timestamp, limit: usize, reverse: bool,
) -> Result<OwnedMergedChanges> {
    let present = context.current_time.as_micros()?;
    let root = relation.root_relation().unwrap_or_else(|| relation.clone());
    if !root.is_root_data() || after > through || through > present
        || !(1..=OWNED_CHANGE_PAGE_GROUPS).contains(&limit)
    {
        return Err(change_feed_uncovered());
    }
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(change_feed_uncovered()); };
    if snapshot.system_time_cap() != Some(present) { return Err(change_feed_uncovered()); }
    let initial = (1 + limit) * 512;
    let mut page = OwnedMergedChanges {
        commits: Vec::with_capacity(limit),
        retained_bytes: initial,
        _reservations: vec![context.memory.reserve(initial)?],
        complete: true,
        complete_boundary: if reverse { after } else { through },
    };
    let Some(journal) = cover_journal(&context, snapshot, owner, relation, present, after).await? else {
        context.check_cancelled()?;
        return Ok(page);
    };
    let first = if after >= journal.head.committed_at { journal.head_revision + 1 }
        else { journal_boundary(&context, owner, relation, &journal, after.next()?).await? };
    let end = if through >= journal.head.committed_at { journal.head_revision + 1 }
        else { journal_boundary(&context, owner, relation, &journal, through.next()?).await? };
    if first > end { return Err(change_feed_uncovered()); }
    let count = usize::try_from(end - first).map_err(|_| change_feed_uncovered())?;
    for offset in 0..count.min(limit) {
        context.check_cancelled()?;
        let offset = i64::try_from(offset).map_err(|_| change_feed_uncovered())?;
        let seq = if reverse { end - 1 - offset } else { first + offset };
        let header = read_owned_journal_header(Arc::clone(&context), owner, relation, seq).await?;
        let previous = if seq == 1 { journal.floor } else {
            read_owned_journal_header(Arc::clone(&context), owner, relation, seq - 1).await?
        };
        let entry = read_owned_change_entry(Arc::clone(&context), owner, relation, seq).await?;
        let Value::List(touches) = entry.value else { return Err(change_feed_uncovered()); };
        let events = i64::try_from(touches.len()).map_err(|_| change_feed_uncovered())?;
        if events == 0 || entry.system_time != header.committed_at || entry.through_events != header.through_events
            || previous.through_events.checked_add(events) != Some(header.through_events)
            || (seq == 1 && previous.committed_at != header.committed_at)
            || (seq > 1 && previous.committed_at >= header.committed_at)
            || !ordered_or_same(journal.head_revision, seq, journal.head, header)
            || header.committed_at <= after || header.committed_at > through
        {
            return Err(change_feed_uncovered());
        }
        let bytes = touches.iter().try_fold(0usize, |bytes, touch| {
            bytes.checked_add(touch.retained_bytes()?).and_then(|bytes| bytes.checked_add(128))
                .filter(|bytes| *bytes <= OWNED_CHANGE_GROUP_BYTES).ok_or_else(change_feed_uncovered)
        })? + 512;
        if bytes > OWNED_CHANGE_PAGE_BYTES - page.retained_bytes {
            if page.commits.is_empty() { return Err(change_feed_uncovered()); }
            break;
        }
        let reservation = context.memory.reserve(bytes)?;
        let touches = touches.iter().map(|value| OwnedChange::from_value(value.as_ref())).collect::<Result<Vec<_>>>()?;
        if touches.iter().any(|touch| !matches!(touch.kind,
            rtdb_types::OwnedChangeKind::Put | rtdb_types::OwnedChangeKind::Delete))
        {
            return Err(change_feed_uncovered());
        }
        page.retained_bytes += bytes;
        page._reservations.push(reservation);
        page.commits.push(OwnedChangeCommit {
            revision: header.committed_at,
            changes: vec![OwnedRelationChanges { relation: root.clone(), sequence: seq as u64, touches }],
        });
    }
    page.complete = page.commits.len() == count;
    context.check_cancelled()?;
    Ok(page)
}

/// Certify a touched full identity and one unchanged installed path at both physical cuts.
pub async fn archived_path_touch_at_cut(
    context: Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation,
    id: &EntityId, selected: Timestamp, before: Timestamp, group: Timestamp, valid: Timestamp,
) -> Result<Vec<String>> {
    if before.next()? != group || group > selected { return Err(change_feed_uncovered()); }
    let page = archived_changes_at_cut(Arc::clone(&context), owner, relation, before, group, 1, false).await?;
    let root = relation.root_relation().unwrap_or_else(|| relation.clone());
    require_path_touch(&page, &root, id, group, valid)?;
    let prior = context.with_owned_system_cut(before, &context.cancellation)?;
    let accepted = context.with_owned_system_cut(group, &context.cancellation)?;
    let selected = context.with_owned_system_cut(selected, &context.cancellation)?;
    crate::ordered_index::stable_owned_path_columns(&prior, &accepted, owner, relation, path).await?;
    crate::ordered_index::stable_owned_path_columns(&accepted, &selected, owner, relation, path).await
}

/// Pages a single Root address in a caller-admitted, pinned archive generation.
pub async fn archived_versions_at_cut(
    context: Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    selected: Timestamp, request: OwnedVersionRequest,
) -> Result<OwnedVersionPage> {
    // No earlier archive window can prove absence across a missing base.
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(change_feed_uncovered()); };
    if snapshot.system_time_cap() != Some(context.current_time.as_micros()?) { return Err(change_feed_uncovered()); }
    read_versions_at_cut(Arc::clone(&context), owner, relation, id, selected, request, || Ok(Arc::clone(&context))).await
}

async fn read_versions_at_cut(
    journal_context: Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    selected: Timestamp, request: OwnedVersionRequest,
    timeline_context: impl Fn() -> Result<Arc<rtdb_query::QueryContext>>,
) -> Result<OwnedVersionPage> {
    let OwnedVersionRequest {
        system,
        after,
        scan_limit,
        result_limit,
        order,
    } = request;
    if relation.world().is_some()
        || relation.is_world_catalog()
        || system.upper() > selected.next()?
        || !(1..=OWNED_CHANGE_PAGE_GROUPS).contains(&scan_limit)
        || !(1..=scan_limit).contains(&result_limit)
    {
        return Err(Anomaly::incorrect(
            "rtdb.owned/version-window",
            "Versions require a system window at or before the held cut and up to 128 groups",
        ));
    }
    let database = journal_context.catalog.database(journal_context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(change_feed_uncovered()); };
    let present = journal_context.current_time.as_micros()?;
    if selected > present { return Err(change_feed_uncovered()); }
    let mut page = OwnedVersionPage {
        floor: Timestamp::MIN,
        next_revision: after,
        done: true,
        scanned: 0,
        witness: None,
        versions: Vec::with_capacity(result_limit),
        _reservation: journal_context.memory.reserve(result_limit * 512)?,
    };
    if system.is_empty() {
        if after != 0 {
            return Err(change_feed_uncovered());
        }
        return Ok(page);
    }

    let purge_floor = rtdb_query::owned_purge_floor(
        Arc::clone(&journal_context), owner, relation, id,
    ).await?;
    let journal = cover_journal(&journal_context, snapshot, owner, relation, present, present).await?;
    let history_floor = journal.as_ref()
        .filter(|journal| journal.floor.through_events != 0)
        .map(|journal| journal.floor.committed_at);
    page.floor = purge_floor.max(history_floor).unwrap_or(Timestamp::MIN);
    if system.lower() < page.floor {
        return Err(Anomaly::new(
            AnomalyCategory::Unavailable,
            "rtdb.owned/version-floor",
            "The requested system window precedes the certified owned history floor",
        ).with_context("history-floor-micros", page.floor.0.into()));
    }
    if let Some(journal) = journal {
        validate_journal_head(&journal_context, owner, relation, journal.head_revision, journal.head)
            .await?;
        let first = journal_boundary(&journal_context, owner, relation, &journal, system.lower())
            .await?;
        let end = journal_boundary(&journal_context, owner, relation, &journal, system.upper())
            .await?;
        if first > end {
            return Err(change_feed_uncovered());
        }
        let revisions = first..end;
        let after = i64::try_from(after).map_err(|_| change_feed_uncovered())?;
        if after != 0 && !revisions.contains(&after) {
            return Err(change_feed_uncovered());
        }
        let (mut current, step) = match order {
            OwnedVersionOrder::Ascending => (if after == 0 { first } else { after + 1 }, 1),
            OwnedVersionOrder::Descending => (if after == 0 { end - 1 } else { after - 1 }, -1),
        };
        while page.scanned < scan_limit
            && page.versions.len() < result_limit
            && revisions.contains(&current)
        {
            journal_context.check_cancelled()?;
            let entry = read_owned_change_entry(
                Arc::clone(&journal_context),
                owner,
                relation,
                current,
            )
            .await?;
            let Value::List(touches) = entry.value else {
                return Err(change_feed_uncovered());
            };
            let count = i64::try_from(touches.len()).map_err(|_| change_feed_uncovered())?;
            let header = OwnedJournalHeader {
                committed_at: entry.system_time,
                through_events: entry.through_events,
            };
            let previous = if current == 1 {
                journal.floor
            } else {
                read_owned_journal_header(Arc::clone(&journal_context), owner, relation, current - 1).await?
            };
            if count == 0
                || previous.through_events.checked_add(count) != Some(header.through_events)
                || (current > 1 && !ordered_or_same(current, current - 1, header, previous))
                || !ordered_or_same(current, 1, header, journal.first)
                || !ordered_or_same(journal.head_revision, current, journal.head, header)
                || !system.contains(header.committed_at)
            {
                return Err(change_feed_uncovered());
            }
            let mut touched = false;
            for touch in touches.iter() {
                touched |= &OwnedChange::from_value(touch.as_ref())?.id == id;
            }
            let revision = u64::try_from(current).map_err(|_| change_feed_uncovered())?;
            if touched {
                let timeline = rtdb_query::open_owned_valid_during_at(
                    timeline_context()?,
                    owner,
                    relation,
                    id,
                    Interval::ALL,
                    header.committed_at,
                )
                .await?
                .ok_or_else(change_feed_uncovered)?;
                page.versions.push(OwnedVersion {
                    revision,
                    system_time: header.committed_at,
                    timeline,
                });
            }
            page.next_revision = revision;
            page.scanned += 1;
            current += step;
        }
        page.done = !revisions.contains(&current);
    } else if after != 0 {
        return Err(change_feed_uncovered());
    }
    Ok(page)
}

async fn journal_boundary(
    context: &Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation,
    journal: &CoveredJournal, time: Timestamp,
) -> Result<i64> {
    let end = journal.head_revision + 1;
    if time <= journal.first.committed_at {
        return Ok(1);
    }
    if time > journal.head.committed_at {
        return Ok(end);
    }
    let (mut low, mut high) = (2, journal.head_revision);
    while low < high {
        context.check_cancelled()?;
        let mid = low + (high - low) / 2;
        let header = read_owned_journal_header(Arc::clone(context), owner, relation, mid).await?;
        if !ordered_or_same(mid, 1, header, journal.first)
            || !ordered_or_same(journal.head_revision, mid, journal.head, header)
        {
            return Err(change_feed_uncovered());
        }
        if header.committed_at < time {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    if low > 1 && read_owned_journal_header(Arc::clone(context), owner, relation, low - 1).await?.committed_at >= time {
        return Err(change_feed_uncovered());
    }
    if low < end && read_owned_journal_header(Arc::clone(context), owner, relation, low).await?.committed_at < time {
        return Err(change_feed_uncovered());
    }
    Ok(low)
}

async fn cover_journal(
    context: &Arc<rtdb_query::QueryContext>, snapshot: &Snapshot, owner: &str, relation: &OwnedRelation,
    present: Timestamp, after: Timestamp,
) -> Result<Option<CoveredJournal>> {
    if snapshot.tx_basis().is_none_or(|basis| basis.system_time != present) || after > present {
        return Err(change_feed_uncovered());
    }
    let journal = relation.change_journal(owner)?;
    let table = relation.table(owner);
    let root_frontier = snapshot.relation_event_frontier(&table)?;
    let journal_frontier = snapshot.relation_event_frontier(&journal)?;
    if journal_frontier == 0
        && root_frontier == 0
        && snapshot.trie_entries(&table).is_empty()
        && snapshot.trie_entries(&journal).is_empty()
    {
        return Ok(None);
    }
    if journal_frontier < 2 {
        return Err(change_feed_uncovered());
    }

    let floor = read_owned_journal_header(Arc::clone(context), owner, relation, 0).await?;
    let first = read_owned_journal_header(Arc::clone(context), owner, relation, 1).await?;
    let head_revision = journal_frontier - 1;
    let head = read_owned_journal_header(Arc::clone(context), owner, relation, head_revision).await?;
    if floor.committed_at != first.committed_at
        || floor.through_events >= first.through_events
        || !ordered_or_same(head_revision, 1, head, first)
        || head.through_events != root_frontier
        || head.committed_at > present
        || (after < floor.committed_at && floor.through_events != 0)
    {
        return Err(change_feed_uncovered());
    }
    let entry =
        read_owned_change_entry(Arc::clone(context), owner, relation, 0).await?;
    if entry.value != Value::Null
        || entry.system_time != floor.committed_at
        || entry.through_events != floor.through_events
    {
        return Err(change_feed_uncovered());
    }
    Ok(Some(CoveredJournal {
        floor,
        first,
        head,
        head_revision,
    }))
}

async fn validate_journal_head(
    context: &Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation,
    revision: i64, head: OwnedJournalHeader,
) -> Result<()> {
    let entry =
        read_owned_change_entry(Arc::clone(context), owner, relation, revision)
            .await?;
    if entry.system_time != head.committed_at
        || entry.through_events != head.through_events
        || !matches!(entry.value, Value::List(touches) if !touches.is_empty())
    {
        return Err(change_feed_uncovered());
    }
    Ok(())
}

fn require_path_touch(
    page: &OwnedMergedChanges, relation: &OwnedRelation, id: &EntityId, group: Timestamp, valid: Timestamp,
) -> Result<()> {
    let [commit] = page.commits.as_slice() else { return Err(change_feed_uncovered()); };
    let [changes] = commit.changes.as_slice() else { return Err(change_feed_uncovered()); };
    if commit.revision != group || changes.relation != *relation || !changes.touches.iter().any(|touch| {
        touch.id == *id && touch.valid.lower() <= valid && valid < touch.valid.upper()
            && matches!(touch.kind, rtdb_types::OwnedChangeKind::Put | rtdb_types::OwnedChangeKind::Delete)
    }) {
        return Err(change_feed_uncovered());
    }
    Ok(())
}

impl Journal<'_> {
    fn active(&self) -> bool {
        (1..=self.head).contains(&self.revision)
    }

    async fn retreat(&mut self, held: &OwnedReadSession) -> Result<()> {
        self.revision -= 1;
        if !self.active() { return Ok(()); }
        let next = held.journal_header(self.relation, self.revision).await?;
        let previous = held.journal_header(self.relation, self.revision - 1).await?;
        if next.through_events != self.previous_through
            || !ordered_or_same(self.revision + 1, self.revision, self.next, next)
            || previous.through_events >= next.through_events
            || (self.revision > 1 && previous.committed_at >= next.committed_at)
            || (self.revision == 1 && previous.committed_at != next.committed_at)
        {
            return Err(change_feed_uncovered());
        }
        self.next = next;
        self.previous_through = previous.through_events;
        Ok(())
    }

    async fn advance(&mut self, held: &OwnedReadSession) -> Result<()> {
        self.revision += 1;
        self.previous_through = self.next.through_events;
        if !self.active() {
            return Ok(());
        }
        let next = held.journal_header(self.relation, self.revision).await?;
        if next.committed_at <= self.next.committed_at
            || next.through_events <= self.previous_through
            || next.committed_at > held.present.system_time
            || next.through_events > self.head_through
            || (self.revision == self.head && next.through_events != self.head_through)
        {
            return Err(change_feed_uncovered());
        }
        self.next = next;
        Ok(())
    }
}

fn ordered_or_same(higher: i64, lower: i64, a: OwnedJournalHeader, b: OwnedJournalHeader) -> bool {
    if higher == lower {
        a == b
    } else {
        a.committed_at > b.committed_at && a.through_events > b.through_events
    }
}
