use std::{collections::BTreeSet, sync::Arc};

use rtdb_columnar::MemoryReservation;
use rtdb_index::Snapshot;
use rtdb_plan::Catalog;
use rtdb_query::{OwnedJournalHeader, read_owned_change_entry, read_owned_journal_header};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Interval, OWNED_CHANGE_GROUP_BYTES,
    OWNED_CHANGE_PAGE_BYTES, OWNED_CHANGE_PAGE_GROUPS, OwnedChange, OwnedChangeGroup,
    OwnedRelation, Result, Timestamp, TransactionKey, TxId, Value, change_feed_uncovered,
};

use crate::ordered_index::OwnedPathColumns;

use super::{
    OwnedChangePage, OwnedReadSession, OwnedVersion, OwnedVersionOrder, OwnedVersionPage,
    OwnedVersionRequest, historical_owned_read,
};

mod group;

pub const OWNED_MERGED_CHANGE_RELATIONS: usize = 32;

/// A SQL as-of edge covers the whole history-time band. A publication edge ends
/// at the named original transaction inside that band.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OwnedChangeEdge {
    pub time: Timestamp,
    // The last variant sorts after every actual publication at the same time.
    pub order: ChangeEdgeOrder,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ChangeEdgeOrder {
    Publication { position: TxId, included: bool },
    AsOf,
}

impl From<Timestamp> for OwnedChangeEdge {
    fn from(time: Timestamp) -> Self { Self { time, order: ChangeEdgeOrder::AsOf } }
}

impl From<TransactionKey> for OwnedChangeEdge {
    fn from(key: TransactionKey) -> Self {
        Self { time: key.system_time, order: ChangeEdgeOrder::Publication { position: key.tx_id, included: true } }
    }
}

impl OwnedChangeEdge {
    pub fn before(key: TransactionKey) -> Self {
        Self { time: key.system_time, order: ChangeEdgeOrder::Publication { position: key.tx_id, included: false } }
    }

    fn belongs_to(self, head: TransactionKey) -> bool {
        self.time <= head.system_time && match self.order {
            ChangeEdgeOrder::Publication { position, .. } => position.0 >= 0 && position <= head.tx_id
                && (position != head.tx_id || self.time == head.system_time),
            ChangeEdgeOrder::AsOf => true,
        }
    }
}

/// Selector in one admitted relation's publication journal.
#[derive(Clone, Copy)]
pub enum OwnedPublicationLookup {
    Latest,
    Sequence(u64),
    Position(TransactionKey),
    AtOrBefore(Timestamp),
}

pub struct OwnedMergedChanges {
    pub commits: Vec<OwnedChangeCommit>,
    pub retained_bytes: usize,
    _reservations: Vec<Arc<MemoryReservation>>,
    complete: bool,
    complete_boundary: OwnedChangeEdge,
    // A caller projecting back to an as-of edge cannot cross an unexamined tie.
    tied_continuation: bool,
}

impl OwnedMergedChanges {
    pub fn is_complete(&self) -> bool { self.complete }

    pub fn covered_edge(&self, descending: bool) -> Result<OwnedChangeEdge> {
        if self.complete {
            return Ok(self.complete_boundary);
        }
        self.commits.last().map(|commit| {
            if descending { OwnedChangeEdge::before(commit.publication) } else { commit.publication.into() }
        }).ok_or_else(change_feed_uncovered)
    }

    pub fn is_time_projectable(&self) -> bool {
        !self.tied_continuation && self.commits.windows(2).all(|pair|
            pair[0].publication.system_time != pair[1].publication.system_time)
    }

    pub fn covered_through(&self) -> Result<Timestamp> {
        if !self.is_time_projectable() { return Err(change_feed_uncovered()); }
        Ok(self.covered_edge(false)?.time)
    }

    /// Join a later scan band after this band's certified edge, in scan order.
    /// If the next whole band exceeds the page budget, resume from the last emitted group.
    pub fn append_archive_band(&mut self, mut next: Self, reverse: bool) -> Result<()> {
        if !self.complete
            || next.commits.first().is_some_and(|commit| {
                let edge = OwnedChangeEdge::from(commit.publication);
                if reverse { edge > self.complete_boundary } else { edge <= self.complete_boundary }
            })
        {
            return Err(change_feed_uncovered());
        }
        if next.commits.is_empty() {
            if !next.complete { return Err(change_feed_uncovered()); }
            self.complete_boundary = next.complete_boundary;
            return Ok(());
        }
        if self.commits.is_empty() {
            *self = next;
            return Ok(());
        }
        if next.retained_bytes > OWNED_CHANGE_PAGE_BYTES - self.retained_bytes {
            self.stop_before(next.commits[0].publication)?;
            return Ok(());
        }
        self.retained_bytes += next.retained_bytes;
        self.commits.append(&mut next.commits);
        self._reservations.append(&mut next._reservations);
        self.complete_boundary = next.complete_boundary;
        self.complete = next.complete;
        self.tied_continuation = next.tied_continuation;
        Ok(())
    }

    pub fn stop_before(&mut self, next: TransactionKey) -> Result<()> {
        let last = self.commits.last().ok_or_else(change_feed_uncovered)?;
        self.complete = false;
        self.tied_continuation = last.publication.system_time == next.system_time;
        Ok(())
    }

    /// Extend complete coverage across the remaining empty portion of the
    /// requested window, after the caller exhausts every selected writer.
    pub fn complete_at(&mut self, edge: OwnedChangeEdge) {
        if self.complete { self.complete_boundary = edge; }
    }

    /// A single-relation page carries its examined coverage independently of visible groups.
    pub fn into_relation_page(self) -> Result<OwnedChangePage> {
        let covered_through = self.covered_through()?;
        let groups = self.commits.into_iter().map(|commit| {
            let mut changes = commit.changes.into_iter();
            let change = changes.next().ok_or_else(change_feed_uncovered)?;
            if changes.next().is_some() {
                return Err(change_feed_uncovered());
            }
            Ok(OwnedChangeGroup {
                revision: change.sequence,
                system_time: commit.publication.system_time,
                touches: change.touches,
            })
        }).collect::<Result<_>>()?;
        Ok(OwnedChangePage {
            groups, covered_through,
            retained_bytes: self.retained_bytes,
            _reservations: self._reservations,
        })
    }
}

impl OwnedChangePage {
    pub fn retain_identity_region(
        &mut self, lower: Option<&EntityId>, upper: Option<&EntityId>, valid: Interval,
    ) -> Result<()> {
        if valid.is_empty() || lower.zip(upper).is_some_and(|(lower, upper)| lower >= upper) {
            return Err(Anomaly::incorrect("rtdb.owned/change-region", "A change region requires nonempty identity and valid-time bounds"));
        }
        self.groups.retain_mut(|group| {
            group.touches.retain(|touch| {
                lower.is_none_or(|lower| &touch.id >= lower)
                    && upper.is_none_or(|upper| &touch.id < upper)
                    && touch.valid.intersects(valid)
            });
            !group.touches.is_empty()
        });
        Ok(())
    }
}

pub struct OwnedChangeCommit {
    pub publication: TransactionKey,
    pub changes: Vec<OwnedRelationChanges>,
}

pub struct OwnedRelationChanges {
    pub relation: OwnedRelation,
    pub sequence: u64,
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
    publication: TransactionKey,
}

impl OwnedWorldVersionRead<'_> {
    pub async fn boundaries(&self) -> Result<Option<rtdb_query::QueryCursor>> {
        let context = self.held.relation_context(self.relation)?;
        let context = context.with_owned_transaction_cut(self.publication, &context.cancellation)?;
        rtdb_query::open_owned_valid_boundaries(
            context, &self.held.owner,
            &self.relation.in_world(self.world)?, self.id, Interval::ALL, self.publication.system_time,
        ).await
    }

    pub async fn shadow_at(&self, valid: Timestamp) -> Result<super::OwnedWorldShadowRead> {
        self.held.require_unscoped_root()?;
        self.held.admit_read().await?;
        let context = self.held.relation_context(self.relation)?;
        let context = context.with_owned_transaction_cut(self.publication, &context.cancellation)?;
        self.held.read_world_shadow_in_context(
            context, self.world, self.relation, self.id, valid, self.publication.system_time, false,
        ).await
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
        page.into_relation_page()
    }

    /// Filters a covered journal page by complete identity and valid-time overlap.
    /// The cursor advances over examined source groups even when all their touches are outside the region.
    pub async fn changes_in_region(
        &self, relation: &OwnedRelation, after: Timestamp, limit: usize,
        lower: Option<&EntityId>, upper: Option<&EntityId>, valid: Interval,
    ) -> Result<OwnedChangePage> {
        let mut page = self.changes(relation, after, limit).await?;
        page.retain_identity_region(lower, upper, valid)?;
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
                std::slice::from_ref(relation), through.into(), self.present.system_time.into(), 1, if through == Timestamp::MIN { ChangeScan::RetainedStart } else { ChangeScan::Forward },
            ).await?;
            let Some(group) = page.commits.first() else {
                through = page.covered_through()?;
                state = "quiescent";
                break;
            };
            let [change] = group.changes.as_slice() else { return Err(change_feed_uncovered()); };
            let expiry = Timestamp(group.publication.system_time.0.checked_add(policy.duration).ok_or_else(change_feed_uncovered)?);
            let valid = Interval::new(group.publication.system_time, expiry)?;
            for touch in &change.touches {
                match touch.kind {
                    rtdb_types::OwnedChangeKind::Put if touch.valid == valid => (),
                    rtdb_types::OwnedChangeKind::Expired(original)
                        if original.lower().0.checked_add(policy.duration) == Some(original.upper().0)
                            && original.upper() <= rtdb_types::OwnedLifetimeFloor::expiry_replay_cutoff(group.publication.system_time) => (),
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
                if created != group.publication.system_time || expires != expiry {
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
            through = page.covered_through()?;
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
        self.world_merged_changes_ordered(world, relations, after.into(), through.into(), limit, false).await
    }

    pub async fn world_merged_changes_ordered(
        &self, world: rtdb_types::Iid, relations: &[OwnedRelation],
        after: OwnedChangeEdge, through: OwnedChangeEdge, limit: usize, descending: bool,
    ) -> Result<OwnedMergedChanges> {
        let (record, _charge) = self.world_registration(world).await?
            .filter(|(record, _)| !record.dropped && self.present.system_time < record.until)
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        self.require_change_bounds(after.time, through.time)?;
        let fork = record.inherited_position().into();
        if after < fork {
            return Err(historical_owned_read::below_floor(record.inherited_system()));
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
        self.merged_changes_window(relations, after.into(), through.into(), limit, false).await
    }

    pub async fn merged_changes_descending(
        &self, relations: &[OwnedRelation], after: Timestamp, through: Timestamp, limit: usize,
    ) -> Result<OwnedMergedChanges> {
        self.merged_changes_window(relations, after.into(), through.into(), limit, true).await
    }

    /// Merge whole Root publications within the authorized held interval.
    pub async fn merged_changes_window(
        &self, relations: &[OwnedRelation], after: OwnedChangeEdge, through: OwnedChangeEdge,
        limit: usize, descending: bool,
    ) -> Result<OwnedMergedChanges> {
        if relations.iter().any(|relation| relation.world().is_some() || relation.is_world_catalog()) {
            return Err(change_feed_uncovered());
        }
        let scan = if descending { ChangeScan::Reverse } else { ChangeScan::Forward };
        self.merged_change_journals(relations, after, through, limit, scan).await
    }

    /// Read a clear point on one side of an exact publication in the held Root.
    pub async fn open_root_cut_point(
        &self, relation: &OwnedRelation, id: &EntityId, publication: TransactionKey,
        valid: Timestamp, before: bool,
    ) -> Result<Option<rtdb_query::QueryCursor>> {
        let context = self.root_cut_context(relation, id, publication, before).await?;
        let valid = Interval::at(valid)?;
        rtdb_query::open_owned_assertion_point(context, &self.owner, relation, id, Some(valid), false).await
    }

    async fn root_cut_context(
        &self, relation: &OwnedRelation, id: &EntityId, publication: TransactionKey, before: bool,
    ) -> Result<Arc<rtdb_query::QueryContext>> {
        let held = self.historical_publication().unwrap_or(self.present);
        if !publication.at_or_before(held) {
            return Err(change_feed_uncovered());
        }
        let system = publication.system_time;
        let context = self.position_read_context(relation, id, system, system).await?;
        let cut = if before {
            let previous = publication.tx_id.0.checked_sub(1).filter(|id| *id >= 0)
                .ok_or_else(change_feed_uncovered)?;
            TransactionKey { tx_id: TxId(previous), ..publication }
        } else {
            publication
        };
        self.certify_historical_position(relation, cut).await?;
        context.with_owned_transaction_cut(cut, &context.cancellation)
    }

    /// Admits a touched Root address on both sides of the exact journal publication.
    pub async fn path_touch_columns(
        &self, relation: &OwnedRelation, path: &OwnedRelation, id: &EntityId,
        publication: TransactionKey, valid: Timestamp,
    ) -> Result<OwnedPathColumns> {
        let page = self.merged_changes_window(
            std::slice::from_ref(relation), OwnedChangeEdge::before(publication),
            publication.into(), 1, false,
        ).await?;
        require_path_touch(&page, relation, id, publication.system_time, valid)?;
        let [commit] = page.commits.as_slice() else { return Err(change_feed_uncovered()); };
        if commit.publication != publication || !page.is_complete() {
            return Err(change_feed_uncovered());
        }
        let prior = self.root_cut_context(relation, id, publication, true).await?;
        let accepted = self.root_cut_context(relation, id, publication, false).await?;
        crate::ordered_index::stable_owned_path_columns(&prior, &accepted, &self.owner, relation, path).await
    }

    /// Resolve local winners on both sides of one certified world commit.
    /// Only the held archive owner can resolve their inherited parent.
    pub async fn world_path_touch(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, path: &OwnedRelation,
        id: &EntityId, group: Timestamp, valid: Timestamp,
    ) -> Result<(OwnedPathColumns, [super::OwnedWorldShadowRead; 2])> {
        let (before, columns) = self.certify_world_path_touch(world, relation, path, id, group, valid).await?;
        let before = self.read_world_shadow_at(world, relation, id, valid, before, false).await?;
        let after = self.read_world_shadow_at(world, relation, id, valid, group, false).await?;
        Ok((columns, [before, after]))
    }

    async fn certify_world_path_touch(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, path: &OwnedRelation,
        id: &EntityId, group: Timestamp, valid: Timestamp,
    ) -> Result<(Timestamp, OwnedPathColumns)> {
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
        if !page.is_time_projectable() { return Err(change_feed_uncovered()); }
        let [commit] = page.commits.as_slice() else { return Err(change_feed_uncovered()); };
        let [changes] = commit.changes.as_slice() else { return Err(change_feed_uncovered()); };
        if commit.publication.system_time != system || changes.relation != *relation || !changes.touches.iter().any(|touch| {
            touch.id == *id && matches!(touch.kind, rtdb_types::OwnedChangeKind::Put | rtdb_types::OwnedChangeKind::Delete)
        }) {
            return Err(change_feed_uncovered());
        }
        // The exact branch journal also certifies the selected window floor,
        // including a journal with a nonzero retained starting frontier.
        self.world_merged_changes(world, std::slice::from_ref(relation), from, from, 1).await?;
        Ok(OwnedWorldVersionRead { held: self, world, relation, id, publication: commit.publication })
    }

    /// Read the scoped journal sequence together with the exact publication
    /// that assigned it. Reading the header consumes no touch payloads.
    pub async fn table_publication_sequence(
        &self, relation: &OwnedRelation, publication: TransactionKey,
    ) -> Result<u64> {
        if relation.world().is_some() || relation.is_world_catalog() {
            return Err(change_feed_uncovered());
        }
        self.require_scoped_relation(relation)?;
        self.admit_access().await?;
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(change_feed_uncovered());
        };
        let journal = self.covered_journal(snapshot, relation, self.present.system_time)
            .await?.ok_or_else(change_feed_uncovered)?;
        if journal.head.publication != publication {
            return Err(change_feed_uncovered());
        }
        self.check_live_change_relations(std::slice::from_ref(relation))?;
        u64::try_from(journal.head_revision).map_err(|_| change_feed_uncovered())
    }

    /// Resolve a relation-local version on the same held journal in either direction.
    pub async fn lookup_publication(
        &self, relation: &OwnedRelation, lookup: OwnedPublicationLookup,
    ) -> Result<Option<(u64, TransactionKey)>> {
        self.admit_access().await?;
        self.require_scoped_relation(relation)?;
        if relation.world().is_some() || relation.is_world_catalog() { return Err(change_feed_uncovered()); }
        let context = self.journal_context(relation)?;
        let database = context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else { return Err(change_feed_uncovered()); };
        let Some(journal) = self.covered_journal(snapshot, relation, self.present.system_time).await?
        else { return Ok(None); };
        let position_limit = self.historical_publication().unwrap_or(self.present);
        let sequence = match lookup {
            OwnedPublicationLookup::Latest => journal_boundary_for(
                &context, &self.owner, relation, &journal,
                |entry| entry.publication.at_or_before(position_limit),
            ).await? - 1,
            OwnedPublicationLookup::AtOrBefore(time) => journal_boundary_for(
                &context, &self.owner, relation, &journal,
                |entry| entry.publication.system_time <= time && entry.publication.at_or_before(position_limit),
            ).await? - 1,
            OwnedPublicationLookup::Sequence(sequence) =>
                i64::try_from(sequence).map_err(|_| change_feed_uncovered())?,
            OwnedPublicationLookup::Position(position) => {
                journal_boundary_for(&context, &self.owner, relation, &journal,
                    |entry| entry.publication.tx_id < position.tx_id).await?
            }
        };
        if !(1..=journal.head_revision).contains(&sequence) { return Ok(None); }
        let header = self.journal_header(relation, sequence).await?;
        if !ordered_or_same(journal.head_revision, sequence, journal.head, header)
            || !ordered_or_same(sequence, 1, header, journal.first)
        {
            return Err(change_feed_uncovered());
        }
        self.check_live_change_relations(std::slice::from_ref(relation))?;
        if !header.publication.at_or_before(position_limit) || matches!(
            lookup, OwnedPublicationLookup::Position(position) if position != header.publication
        ) {
            return Ok(None);
        }
        Ok(Some((u64::try_from(sequence).map_err(|_| change_feed_uncovered())?, header.publication)))
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
        &self, relations: &[OwnedRelation], after: OwnedChangeEdge, upper: OwnedChangeEdge,
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
        if after > upper || !after.belongs_to(self.present) || !upper.belongs_to(self.present) {
            return Err(change_feed_uncovered());
        }
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
            && if reverse { OwnedChangeEdge::from(journal.next.publication) > after } else { OwnedChangeEdge::from(journal.next.publication) <= upper };

        let mut page = OwnedMergedChanges {
            commits: Vec::with_capacity(limit),
            retained_bytes: initial_bytes,
            _reservations: vec![initial_charge],
            complete: false,
            complete_boundary: if reverse { after } else { upper },
            tied_continuation: false,
        };
        let mut positions = Vec::with_capacity(journals.len());
        let select_next = |positions: &mut Vec<_>, journals: &[Journal<'_>]| {
            positions.clear();
            positions.extend(journals.iter().map(|journal| {
                in_window(journal).then_some(journal.next.publication.tx_id.0)
            }));
            group::next_group(positions, reverse)
        };
        while page.commits.len() < limit {
            guarded.check_cancelled()?;
            let Some(selected) = select_next(&mut positions, &journals) else {
                break;
            };
            let publication = journals[selected].next.publication;
            let mut changes = Vec::new();
            let mut charges = Vec::new();
            let mut bytes = 0;
            for journal in journals
                .iter_mut()
                .filter(|journal| journal.active() && journal.next.publication.tx_id == publication.tx_id)
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
                if !group::admits_fragment(
                    publication.tx_id.0,
                    publication.system_time.0,
                    group::Fragment {
                        position: journal.next.publication.tx_id.0,
                        history_time: journal.next.publication.system_time.0,
                        through_events: journal.next.through_events,
                    },
                    group::Fragment {
                        position: entry.publication.tx_id.0,
                        history_time: entry.publication.system_time.0,
                        through_events: through,
                    },
                    journal.previous_through,
                    events,
                ) {
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
                        ).with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded));
                    }
                    page.stop_before(publication)?;
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
            page.commits.push(OwnedChangeCommit { publication, changes });
        }
        guarded.check_cancelled()?;
        page.complete = select_next(&mut positions, &journals).is_none();
        if !page.complete && let Some(last) = page.commits.last() {
            page.tied_continuation = journals.iter().any(|journal|
                in_window(journal) && journal.next.publication.system_time == last.publication.system_time);
        }
        Ok(page)
    }

    pub(super) async fn certify_historical_position(
        &self, relation: &OwnedRelation, cut: TransactionKey,
    ) -> Result<()> {
        self.check_relation(relation)?;
        let root = self.retained_root_context();
        let database = root.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else { return Err(change_feed_uncovered()); };
        if let Some((_, activation)) = rtdb_query::read_owned_adoption(Arc::clone(root), &self.owner, relation).await?
            && !activation.at_or_before(cut)
        {
            return Err(historical_owned_read::below_floor(activation.system_time));
        }
        if let Some(covered) = self.covered_journal(snapshot, relation, self.present.system_time).await?
            && covered.floor.through_events != 0
            && !covered.floor.publication.at_or_before(cut)
        {
            return Err(historical_owned_read::below_floor(covered.floor.publication.system_time));
        }
        self.check_relation(relation)
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
        let activation = rtdb_query::read_owned_adoption(Arc::clone(self.retained_root_context()), &self.owner, relation)
            .await?.map(|(_, activation)| activation.system_time);
        if let Some(floor) = activation.filter(|floor| at < *floor) {
            return Err(historical_owned_read::below_floor(floor));
        }
        let Some(covered) = self.covered_journal(snapshot, relation, self.present.system_time).await? else {
            self.check_relation(relation)?;
            return Ok(None);
        };
        let floor = (covered.floor.through_events != 0).then_some(covered.floor.publication.system_time).max(activation);
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
            self.system_time(), self.historical_publication().map(|cut| cut.tx_id),
            request, || self.relation_context(relation),
        ).await?;
        page.witness = self.relation_witness(relation)?;
        Ok(page)
    }

    async fn covered_journal(
        &self, snapshot: &Snapshot, relation: &OwnedRelation, after: Timestamp,
    ) -> Result<Option<CoveredJournal>> {
        if snapshot.tx_basis() != Some(self.present) { return Err(change_feed_uncovered()); }
        cover_journal(&self.journal_context(relation)?, snapshot, &self.owner, relation, self.present.system_time, after.into()).await
    }

    async fn seek_changes<'a>(
        &self,
        snapshot: &Snapshot,
        relation: &'a OwnedRelation,
        after: OwnedChangeEdge,
        from_retained_start: bool,
    ) -> Result<Option<Journal<'a>>> {
        let Some(CoveredJournal {
            floor,
            first,
            head,
            head_revision,
        }) = self.covered_journal(snapshot, relation, if from_retained_start { self.present.system_time } else { after.time }).await?
        else {
            return Ok(None);
        };
        if !from_retained_start && floor.through_events != 0 && after < floor.publication.into() {
            return Err(change_feed_uncovered());
        }
        if from_retained_start {
            return Ok(Some(Journal {
                relation, revision: 1, head: head_revision, head_through: head.through_events,
                previous_through: floor.through_events, next: first,
            }));
        }
        if after >= OwnedChangeEdge::from(head.publication) {
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
            if OwnedChangeEdge::from(point.publication) > after {
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
        if OwnedChangeEdge::from(next.publication) <= after
            || next.through_events <= previous.through_events
            || (low > 1
                && (OwnedChangeEdge::from(previous.publication) > after || !ordered_or_same(low, low - 1, next, previous)))
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
        &self, snapshot: &Snapshot, relation: &'a OwnedRelation, after: OwnedChangeEdge, upper: OwnedChangeEdge,
    ) -> Result<Option<Journal<'a>>> {
        let Some(covered) = self.covered_journal(snapshot, relation, after.time).await? else { return Ok(None); };
        if covered.floor.through_events != 0 && after < covered.floor.publication.into() {
            return Err(change_feed_uncovered());
        }
        if after == upper || upper < OwnedChangeEdge::from(covered.first.publication) {
            return Ok(None);
        }
        // Head checks and binary seek read headers only. No later payload
        // is needed to prove the exact upper boundary.
        let revision = if upper >= OwnedChangeEdge::from(covered.head.publication) {
            covered.head_revision
        } else {
            journal_boundary_for(&self.journal_context(relation)?, &self.owner, relation, &covered,
                |header| OwnedChangeEdge::from(header.publication) <= upper).await? - 1
        };
        if revision < 1 { return Err(change_feed_uncovered()); }
        let next = self.journal_header(relation, revision).await?;
        if OwnedChangeEdge::from(next.publication) > upper
            || !ordered_or_same(covered.head_revision, revision, covered.head, next)
        {
            return Err(change_feed_uncovered());
        }
        if OwnedChangeEdge::from(next.publication) <= after { return Ok(None); }
        let previous = if revision == 1 { covered.floor }
            else { self.journal_header(relation, revision - 1).await? };
        if previous.through_events >= next.through_events
            || (revision > 1 && !ordered_or_same(revision, revision - 1, next, previous))
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
    after: impl Into<OwnedChangeEdge>, through: impl Into<OwnedChangeEdge>, limit: usize, reverse: bool,
) -> Result<OwnedMergedChanges> {
    let present = context.owned_held_cut().ok_or_else(change_feed_uncovered)?;
    let after = after.into();
    let through = through.into();
    let root = relation.root_relation().unwrap_or_else(|| relation.clone());
    if !root.is_root_data() || after > through || through.time > present
        || !(1..=OWNED_CHANGE_PAGE_GROUPS).contains(&limit)
    {
        return Err(change_feed_uncovered());
    }
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(change_feed_uncovered()); };
    let head = snapshot.tx_basis().ok_or_else(change_feed_uncovered)?;
    if snapshot.system_time_cap() != Some(present) || !after.belongs_to(head) || !through.belongs_to(head) {
        return Err(change_feed_uncovered());
    }
    let initial = (1 + limit) * 512;
    let mut page = OwnedMergedChanges {
        commits: Vec::with_capacity(limit),
        retained_bytes: initial,
        _reservations: vec![context.memory.reserve(initial)?],
        complete: true,
        complete_boundary: if reverse { after } else { through },
        tied_continuation: false,
    };
    let Some(journal) = cover_journal(&context, snapshot, owner, relation, present, after).await? else {
        context.check_cancelled()?;
        return Ok(page);
    };
    let first = journal_boundary_for(&context, owner, relation, &journal,
        |header| OwnedChangeEdge::from(header.publication) <= after).await?;
    let end = journal_boundary_for(&context, owner, relation, &journal,
        |header| OwnedChangeEdge::from(header.publication) <= through).await?;
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
        if events == 0 || entry.publication != header.publication || entry.through_events != header.through_events
            || previous.through_events.checked_add(events) != Some(header.through_events)
            || (seq == 1 && previous.publication != header.publication)
            || (seq > 1 && !ordered_or_same(seq, seq - 1, header, previous))
            || !ordered_or_same(journal.head_revision, seq, journal.head, header)
            || OwnedChangeEdge::from(header.publication) <= after || OwnedChangeEdge::from(header.publication) > through
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
            publication: header.publication,
            changes: vec![OwnedRelationChanges { relation: root.clone(), sequence: seq as u64, touches }],
        });
    }
    if page.commits.len() < count && let Some(last) = page.commits.last() {
        let offset = i64::try_from(page.commits.len()).map_err(|_| change_feed_uncovered())?;
        let next = if reverse { end - 1 - offset } else { first + offset };
        let next = read_owned_journal_header(Arc::clone(&context), owner, relation, next).await?;
        page.tied_continuation = next.publication.system_time == last.publication.system_time;
    }
    page.complete = page.commits.len() == count;
    context.check_cancelled()?;
    Ok(page)
}

/// Certify a touched full identity and one unchanged installed path at both physical cuts.
pub async fn archived_path_touch_at_cut(
    context: Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation,
    id: &EntityId, selected: Timestamp, before: Timestamp, group: Timestamp, valid: Timestamp,
) -> Result<OwnedPathColumns> {
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
    if context.owned_held_cut().is_none() { return Err(change_feed_uncovered()); }
    read_versions_at_cut(
        Arc::clone(&context), owner, relation, id, selected, context.owned_position_cut(),
        request, || Ok(Arc::clone(&context)),
    ).await
}

async fn read_versions_at_cut(
    journal_context: Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId,
    selected: Timestamp, selected_position: Option<TxId>, request: OwnedVersionRequest,
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
    let present = journal_context.owned_held_cut().ok_or_else(change_feed_uncovered)?;
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
    let journal = cover_journal(&journal_context, snapshot, owner, relation, present, present.into()).await?;
    let history_floor = journal.as_ref()
        .filter(|journal| journal.floor.through_events != 0)
        .map(|journal| journal.floor.publication.system_time);
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
        let first = journal_boundary_for(&journal_context, owner, relation, &journal, |header| {
            header.publication.system_time < system.lower()
                && selected_position.is_none_or(|through| header.publication.tx_id <= through)
        }).await?;
        let end = journal_boundary_for(&journal_context, owner, relation, &journal, |header| {
            header.publication.system_time < system.upper()
                && selected_position.is_none_or(|through| header.publication.tx_id <= through)
        }).await?;
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
                publication: entry.publication,
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
                || !system.contains(header.publication.system_time)
            {
                return Err(change_feed_uncovered());
            }
            let mut touched = false;
            for touch in touches.iter() {
                touched |= &OwnedChange::from_value(touch.as_ref())?.id == id;
            }
            let revision = u64::try_from(current).map_err(|_| change_feed_uncovered())?;
            if touched {
                let context = timeline_context()?;
                let context = context.with_owned_transaction_cut(header.publication, &context.cancellation)?;
                let timeline = rtdb_query::open_owned_valid_during_at(
                    context,
                    owner,
                    relation,
                    id,
                    Interval::ALL,
                    header.publication.system_time,
                )
                .await?
                .ok_or_else(change_feed_uncovered)?;
                page.versions.push(OwnedVersion {
                    revision,
                    system_time: header.publication.system_time,
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

async fn journal_boundary_for(
    context: &Arc<rtdb_query::QueryContext>, owner: &str, relation: &OwnedRelation,
    journal: &CoveredJournal, consumed: impl Fn(OwnedJournalHeader) -> bool,
) -> Result<i64> {
    let end = journal.head_revision + 1;
    if !consumed(journal.first) {
        return Ok(1);
    }
    if consumed(journal.head) {
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
        if consumed(header) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    if low > 1 && !consumed(read_owned_journal_header(Arc::clone(context), owner, relation, low - 1).await?) {
        return Err(change_feed_uncovered());
    }
    if low < end && consumed(read_owned_journal_header(Arc::clone(context), owner, relation, low).await?) {
        return Err(change_feed_uncovered());
    }
    Ok(low)
}

async fn cover_journal(
    context: &Arc<rtdb_query::QueryContext>, snapshot: &Snapshot, owner: &str, relation: &OwnedRelation,
    present: Timestamp, after: OwnedChangeEdge,
) -> Result<Option<CoveredJournal>> {
    let head_cut = snapshot.tx_basis().filter(|basis| basis.system_time == present)
        .ok_or_else(change_feed_uncovered)?;
    if after.time > present { return Err(change_feed_uncovered()); }
    let inventory = snapshot.owned_routing_inventory(owner, relation)?;
    if inventory.journal_exhausted() {
        return Ok(None);
    }
    let root_frontier = inventory.data_events;
    let journal_frontier = inventory.journal_events;
    if journal_frontier < 2 {
        return Err(change_feed_uncovered());
    }

    let floor = read_owned_journal_header(Arc::clone(context), owner, relation, 0).await?;
    let first = read_owned_journal_header(Arc::clone(context), owner, relation, 1).await?;
    let head_revision = journal_frontier - 1;
    let head = read_owned_journal_header(Arc::clone(context), owner, relation, head_revision).await?;
    if floor.publication != first.publication
        || floor.through_events >= first.through_events
        || !ordered_or_same(head_revision, 1, head, first)
        || head.through_events != root_frontier
        || !head.publication.at_or_before(head_cut)
        || (after < OwnedChangeEdge::from(floor.publication) && floor.through_events != 0)
    {
        return Err(change_feed_uncovered());
    }
    let entry =
        read_owned_change_entry(Arc::clone(context), owner, relation, 0).await?;
    if entry.value != Value::Null
        || entry.publication != floor.publication
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
    if entry.publication != head.publication
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
    if !page.is_time_projectable() { return Err(change_feed_uncovered()); }
    let [commit] = page.commits.as_slice() else { return Err(change_feed_uncovered()); };
    let [changes] = commit.changes.as_slice() else { return Err(change_feed_uncovered()); };
    if commit.publication.system_time != group || changes.relation != *relation || !changes.touches.iter().any(|touch| {
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
            || (self.revision > 1 && !ordered_or_same(self.revision, self.revision - 1, next, previous))
            || (self.revision == 1 && previous.publication != next.publication)
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
        if !ordered_or_same(self.revision, self.revision - 1, next, self.next)
            || next.through_events <= self.previous_through
            || !next.publication.at_or_before(held.present)
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
        b.publication.tx_id < a.publication.tx_id && b.publication.at_or_before(a.publication)
            && a.through_events > b.through_events
    }
}

#[cfg(test)]
#[test]
fn journal_order_distinguishes_tied_transaction_positions() {
    let first = OwnedJournalHeader {
        publication: TransactionKey { tx_id: rtdb_types::TxId(7), system_time: Timestamp(10) },
        through_events: 1,
    };
    let second = OwnedJournalHeader {
        publication: TransactionKey { tx_id: rtdb_types::TxId(8), system_time: Timestamp(10) },
        through_events: 2,
    };
    assert!(ordered_or_same(2, 1, second, first));
    assert!(!ordered_or_same(1, 1, second, first));
    assert!(!ordered_or_same(2, 1, first, second));
}

#[cfg(test)]
#[test]
fn positions_resume_inside_a_band_in_both_directions() {
    let time = Timestamp(30);
    let first = TransactionKey { tx_id: TxId(5), system_time: time };
    let second = TransactionKey { tx_id: TxId(6), system_time: time };
    let lower = OwnedChangeEdge::from(first);
    let upper = OwnedChangeEdge::from(time);
    assert!(lower < second.into() && OwnedChangeEdge::from(second) <= upper);
    assert!(lower <= OwnedChangeEdge::before(second) && OwnedChangeEdge::before(second) < second.into());
    assert!(OwnedChangeEdge::before(first) < lower);
}

#[cfg(test)]
#[test]
fn filtered_relation_page_keeps_the_examined_world_boundary() {
    let relation = OwnedRelation::new("facts").unwrap();
    let valid = Interval::from_micros(10, 20).unwrap();
    let page = OwnedMergedChanges {
        commits: vec![OwnedChangeCommit {
            publication: TransactionKey { tx_id: rtdb_types::TxId(2), system_time: Timestamp(30) },
            changes: vec![OwnedRelationChanges {
                relation,
                sequence: 1,
                touches: vec![OwnedChange {
                    id: EntityId::String("fact".into()),
                    kind: rtdb_types::OwnedChangeKind::Put,
                    valid,
                    assertion_ordinal: None,
                }],
            }],
        }],
        retained_bytes: 0,
        _reservations: Vec::new(),
        complete: false,
        complete_boundary: Timestamp(40).into(),
        tied_continuation: false,
    };
    let mut page = page.into_relation_page().unwrap();
    page.retain_identity_region(None, None, Interval::from_micros(20, 21).unwrap()).unwrap();
    assert!(page.groups.is_empty());
    assert_eq!(page.covered_through, Timestamp(30));
}
