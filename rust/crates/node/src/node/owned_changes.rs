use std::{collections::BTreeSet, sync::Arc};

use rtdb_columnar::MemoryReservation;
use rtdb_index::Snapshot;
use rtdb_query::{OwnedJournalHeader, read_owned_change_entry, read_owned_journal_header};
use rtdb_types::{
    Anomaly, OWNED_CHANGE_GROUP_BYTES, OWNED_CHANGE_PAGE_BYTES, OWNED_CHANGE_PAGE_GROUPS,
    OwnedChange, OwnedChangeGroup, OwnedRelation, Result, Timestamp, Value, change_feed_uncovered,
};

use super::{OwnedChangePage, OwnedReadSession};

pub const OWNED_MERGED_CHANGE_RELATIONS: usize = 32;

pub struct OwnedMergedChanges {
    pub commits: Vec<OwnedChangeCommit>,
    pub retained_bytes: usize,
    _reservations: Vec<Arc<MemoryReservation>>,
    complete: bool,
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

struct Journal<'a> {
    relation: &'a OwnedRelation,
    revision: i64,
    head: i64,
    head_through: i64,
    previous_through: i64,
    next: OwnedJournalHeader,
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
        let covered_through = if page.complete {
            self.present.system_time
        } else {
            page.commits
                .last()
                .ok_or_else(change_feed_uncovered)?
                .revision
        };
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

    /// Merges only the preadmitted owner relations at this held cut.
    pub async fn merged_changes(
        &self,
        relations: &[OwnedRelation],
        after: Timestamp,
        limit: usize,
    ) -> Result<OwnedMergedChanges> {
        if !(1..=OWNED_CHANGE_PAGE_GROUPS).contains(&limit)
            || !(1..=OWNED_MERGED_CHANGE_RELATIONS).contains(&relations.len())
        {
            return Err(Anomaly::incorrect(
                "rtdb.owned/merged-change-limits",
                "Invalid merged change page bounds",
            ));
        }
        self.admit_read().await?;
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(change_feed_uncovered());
        };
        if snapshot.tx_basis() != Some(self.present) || after > self.present.system_time {
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
        let mut journals = Vec::with_capacity(relations.len());
        for relation in names {
            self.check()?;
            if let Some(journal) = self.seek_changes(snapshot, relation, after).await? {
                journals.push(journal);
            }
        }

        let mut page = OwnedMergedChanges {
            commits: Vec::with_capacity(limit),
            retained_bytes: initial_bytes,
            _reservations: vec![initial_charge],
            complete: false,
        };
        while page.commits.len() < limit {
            self.check()?;
            let Some(revision) = journals
                .iter()
                .filter(|journal| journal.active())
                .map(|journal| journal.next.committed_at)
                .min()
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
                    Arc::clone(&self.context),
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
                    self.check()?;
                    return Ok(page);
                }

                let charge = self.context.memory.reserve(retained)?;
                let touches = touches
                    .iter()
                    .map(OwnedChange::from_value)
                    .collect::<Result<Vec<_>>>()?;
                let sequence =
                    u64::try_from(journal.revision).map_err(|_| change_feed_uncovered())?;
                changes.push(OwnedRelationChanges {
                    relation: journal.relation.clone(),
                    sequence,
                    touches,
                });
                charges.push(charge);
                journal.advance(self).await?;
            }
            page.retained_bytes += bytes;
            page._reservations.extend(charges);
            page.commits.push(OwnedChangeCommit { revision, changes });
        }
        self.check()?;
        page.complete = journals.iter().all(|journal| !journal.active());
        Ok(page)
    }

    async fn seek_changes<'a>(
        &self,
        snapshot: &Snapshot,
        relation: &'a OwnedRelation,
        after: Timestamp,
    ) -> Result<Option<Journal<'a>>> {
        let journal = relation.change_journal(&self.owner)?;
        let table = relation.table(&self.owner);
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

        let floor = self.journal_header(relation, 0).await?;
        let first = self.journal_header(relation, 1).await?;
        let head_revision = journal_frontier - 1;
        let head = self.journal_header(relation, head_revision).await?;
        if floor.committed_at != first.committed_at
            || floor.through_events >= first.through_events
            || !ordered_or_same(head_revision, 1, head, first)
            || head.through_events != root_frontier
            || head.committed_at > self.present.system_time
            || (after < floor.committed_at && floor.through_events != 0)
        {
            return Err(change_feed_uncovered());
        }
        let entry =
            read_owned_change_entry(Arc::clone(&self.context), &self.owner, relation, 0).await?;
        if entry.value != Value::Null
            || entry.system_time != floor.committed_at
            || entry.through_events != floor.through_events
        {
            return Err(change_feed_uncovered());
        }
        if after >= head.committed_at {
            let entry = read_owned_change_entry(
                Arc::clone(&self.context),
                &self.owner,
                relation,
                head_revision,
            )
            .await?;
            if entry.system_time != head.committed_at
                || entry.through_events != head.through_events
                || !matches!(entry.value, Value::List(touches) if !touches.is_empty())
            {
                return Err(change_feed_uncovered());
            }
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

    async fn journal_header(
        &self,
        relation: &OwnedRelation,
        revision: i64,
    ) -> Result<OwnedJournalHeader> {
        read_owned_journal_header(Arc::clone(&self.context), &self.owner, relation, revision).await
    }
}

impl Journal<'_> {
    fn active(&self) -> bool {
        self.revision <= self.head
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
