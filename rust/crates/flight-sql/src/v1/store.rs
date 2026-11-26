//! `rtdb.store.v1`: versioned ordered bytes with read, conditional append and watch.
//!
//! The three actions carry no caller semantics. A read returns ordered entries
//! at one version, ascending or descending by key; an append commits its writes
//! only when no write committed after `since` intersects its read ranges and
//! the writer's clock at the ordering point lies in its window, and the
//! resolver stamps it there (`rtdb_tx::transact::Op::Store`); a watch reports
//! the next version that wrote into its ranges. Every append outcome but
//! `outside` is a receipt under its key, a write-free validation included, so a
//! retry returns the first one.
//!
//! A store grant's caller (a placement fence or a provisioning grant,
//! `rtdb_auth::StoreScope`) is confined to its scope: every read range,
//! append guard and watch range lies inside one read prefix, every written
//! key and bounded counter under one write prefix, and its appends carry no
//! derives, whose keys the scope cannot bound. Anything else refuses with
//! `rtdb.store/outside-grant` before it is read or ordered.

use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use rtdb_index::branches::store::{
    DERIVE_DIGEST, KeyRange, MAX_PAGE_ENTRIES, Projection, StoreBound, StoreRead, StoreWrite, ValueFilter, check_bounds,
    check_derives, check_placeholders, store_table,
};
use rtdb_node::{DatabaseAccess, serving::{ChangeImages, ChangeOrder, Changes}};
use rtdb_tx::transact::{
    Guard, Op, Outcome, STORE_BOUND, STORE_OUTSIDE_EARLY, STORE_OUTSIDE_LATE, StoreAppend, Transact, Window,
};
use rtdb_auth::StoreScope;
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Timestamp, rtdb_v1::{At, Basis, MAIN_BRANCH}};
use tonic::Status;

use super::{MAX_CHANGE_GROUPS, MAX_WAIT, check_key, error};
use crate::{rtdb::store::v1 as proto, service::{Call, Service}};

pub(crate) const READ: &str = "rtdb.store.v1.Read";
pub(crate) const APPEND: &str = "rtdb.store.v1.Append";
pub(crate) const WATCH: &str = "rtdb.store.v1.Watch";
pub(crate) const ACTIONS: [&str; 3] = [READ, APPEND, WATCH];

fn decode<M: Message + Default>(bytes: Bytes) -> Result<M, Status> {
    M::decode(bytes).map_err(|_| error::incorrect("Invalid protobuf request"))
}

fn range(range: proto::KeyRange) -> Result<KeyRange, Status> {
    let range = KeyRange { start: range.start, end: range.end };
    range.check().map_err(error::anomaly)?;
    Ok(range)
}

fn ranges(ranges: Vec<proto::KeyRange>) -> Result<Vec<KeyRange>, Status> {
    ranges.into_iter().map(range).collect()
}

/// Refuse unless `scope` (none for an unscoped principal) admits `admitted`.
fn within(scope: Option<&StoreScope>, admitted: impl FnOnce(&StoreScope) -> bool) -> Result<(), Status> {
    if scope.is_none_or(admitted) {
        return Ok(());
    }
    Err(error::anomaly(Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb.store/outside-grant",
        "The store grant's scope does not cover the request's keys",
    )))
}

fn reads_within(scope: Option<&StoreScope>, ranges: &[KeyRange]) -> Result<(), Status> {
    within(scope, |scope| ranges.iter().all(|range| scope.admits_read(&range.start, range.end.as_deref())))
}

fn incarnation(bytes: &[u8]) -> Result<[u8; 16], Status> {
    bytes.try_into().map_err(|_| error::incorrect("A store incarnation has 16 bytes"))
}

impl Service {
    pub(super) async fn store_action(&self, call: &Call, name: &str, body: Bytes) -> Result<Vec<u8>, Status> {
        match name {
            READ => self.store_read(call, decode(body)?).await,
            APPEND => self.store_append(call, decode(body)?).await,
            WATCH => self.store_watch(call, decode(body)?).await,
            _ => Err(error::unsupported("rtdb.store.v1")),
        }
    }

    /// Selects the unscoped serving driver and checks a stated incarnation.
    /// A store grant is admitted on its exact database and current
    /// incarnation while it lives, and its scope returned: the action keeps
    /// every key it touches inside it before any effect.
    async fn store_database<'c>(
        &self, call: &'c Call, access: DatabaseAccess, stated: Option<&[u8]>,
    ) -> Result<(std::sync::Arc<rtdb_node::Database>, [u8; 16], Option<&'c StoreScope>), Status> {
        if call.session.caller.principal.label_scope().is_some() {
            return Err(error::unsupported("rtdb.store.v1 on a scoped route"));
        }
        let database = self.wire_attachment(call, access).await?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let current = driver.incarnation();
        let scope = match call.session.caller.principal.workload_grant().filter(|grant| grant.is_store_grant()) {
            Some(grant) => Some(grant.admit_store(&call.database, current, self.0.node.now()).map_err(error::anomaly)?),
            None => None,
        };
        if let Some(stated) = stated.filter(|stated| !stated.is_empty())
            && incarnation(stated)? != current
        {
            return Err(Status::failed_precondition("The store incarnation changed"));
        }
        Ok((database, current, scope))
    }

    async fn store_read(&self, call: &Call, request: proto::ReadRequest) -> Result<Vec<u8>, Status> {
        if request.ranges.is_empty() {
            return Err(error::incorrect("A read names at least one range"));
        }
        if request.at.is_some() && request.incarnation.is_empty() {
            return Err(error::incorrect("A versioned read states its incarnation"));
        }
        let (database, incarnation, scope) = self.store_database(call, DatabaseAccess::Read, Some(&request.incarnation)).await?;
        let read_ranges = ranges(request.ranges)?;
        reads_within(scope, &read_ranges)?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let mut at = request.at.map_or(At::Latest, |at| At::Exactly(Basis::new(incarnation, at)));
        let limit = (request.limit as usize).clamp(1, MAX_PAGE_ENTRIES);
        let projection = match proto::Projection::try_from(request.projection)
            .map_err(|_| error::incorrect("Invalid store projection"))?
        {
            proto::Projection::Values => Projection::Values,
            proto::Projection::Keys => Projection::Keys,
            proto::Projection::Slice => Projection::Slice { start: request.slice_start, len: request.slice_len },
        };
        let filter = request.filter.as_deref()
            .map(ValueFilter::decode)
            .transpose()
            .map_err(|refused| error::incorrect(&refused.to_string()))?;
        let mut pages = Vec::with_capacity(read_ranges.len());
        let mut read_at = None;
        let clock = self.0.node.now();
        for key_range in read_ranges {
            let after = request.after.clone().filter(|after| key_range.contains(after));
            let read = StoreRead { range: key_range, after, limit, projection, filter: filter.clone(), reverse: request.reverse };
            let page = driver.read_store(read, at, clock, &call.cancellation).await.map_err(error::anomaly)?;
            // Every later range reads the cut the first one selected.
            at = At::Exactly(page.cut.basis);
            read_at = Some(page.cut.basis.position());
            pages.push(proto::RangePage {
                entries: page.entries.into_iter().map(|entry| proto::Entry {
                    key: entry.key, value: entry.value, written: entry.written.position(),
                }).collect(),
                resume: page.resume,
            });
        }
        self.check_attachment(call, &database)?;
        Ok(proto::ReadReply {
            incarnation: incarnation.to_vec(),
            at: read_at.unwrap_or_default(),
            pages,
            clock_micros: Some(clock.0),
        }.encode_to_vec())
    }

    async fn store_append(&self, call: &Call, request: proto::AppendRequest) -> Result<Vec<u8>, Status> {
        check_key(&request.key)?;
        if request.incarnation.is_empty() {
            return Err(error::incorrect("An append states its incarnation"));
        }
        let writes = request.writes.into_iter()
            .map(|write| StoreWrite { key: write.key, value: write.value, key_stamp: write.key_stamp, value_stamp: write.value_stamp })
            .collect::<Vec<_>>();
        let bounds = request.bounds.into_iter()
            .map(|bound| StoreBound { key: bound.key, delta: bound.delta, min: bound.min, max: bound.max })
            .collect::<Vec<_>>();
        if let Some(digest) = request.derive_digest.as_deref()
            && digest != DERIVE_DIGEST
        {
            return Err(error::anomaly(rtdb_types::Anomaly::incorrect(
                "rtdb.store/derive-version",
                format!("The append expects derive code {digest} but this store runs {DERIVE_DIGEST}"),
            )));
        }
        // Placeholder, bound, derive and window errors are the request's, decided before anything is ordered.
        check_placeholders(&writes).map_err(error::anomaly)?;
        check_bounds(&bounds).map_err(error::anomaly)?;
        check_derives(&request.derives, &writes).map_err(error::anomaly)?;
        let window = request.window.map_or(Window::default(), |window| Window {
            not_before: window.not_before.map(Timestamp),
            before: window.before.map(Timestamp),
        });
        if window.not_before.zip(window.before).is_some_and(|(start, end)| start >= end) {
            return Err(error::incorrect("The append's window is empty"));
        }
        // Every append records its outcome under its key, a write-free one included.
        let validation = writes.is_empty() && bounds.is_empty() && request.derives.is_empty();
        let (database, incarnation, scope) = self.store_database(call, DatabaseAccess::Write, Some(&request.incarnation)).await?;
        let read_guards = ranges(request.reads)?;
        reads_within(scope, &read_guards)?;
        within(scope, |scope| {
            request.derives.is_empty()
                && writes.iter().all(|write| scope.admits_write(&write.key, write.key_stamp))
                && bounds.iter().all(|bound| scope.admits_write(&bound.key, None))
        })?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let since = Basis::new(incarnation, request.since);
        let guards = read_guards.into_iter()
            .map(|range| Guard::Unwritten { range, since })
            .collect::<Vec<_>>();
        let transaction = Transact {
            key: request.key,
            require: Vec::new(),
            window: Window::default(),
            guards,
            ops: vec![Op::Store(StoreAppend { writes, bounds, derives: request.derives, window })],
            otherwise: Vec::new(),
            requires: Vec::new(),
            custody_request_digest: None,
            request_memory: Some(call.request_reservation()),
        };
        let decided = driver.transact(transaction, &call.cancellation).await;
        let outcome = match decided {
            Ok(receipt) => {
                self.check_attachment(call, &database)?;
                match receipt.outcome {
                    Outcome::Then if validation => proto::append_reply::Outcome::Validated(receipt.basis.position()),
                    Outcome::Then => proto::append_reply::Outcome::Committed(proto::Stamp {
                        micros: receipt.system_time.0, version: receipt.basis.position(),
                    }),
                    Outcome::Otherwise { failed } => {
                        let first = failed.first().copied().ok_or_else(|| Status::internal("A failed append names no read"))?;
                        proto::append_reply::Outcome::Conflict(u32::try_from(first).map_err(|_| Status::internal("Read index overflow"))?)
                    }
                    Outcome::Refused { error } if error.code == STORE_BOUND => {
                        let bound = error.context.get("bound").and_then(|value| value.as_u64())
                            .ok_or_else(|| Status::internal("A failed bound names no index"))?;
                        proto::append_reply::Outcome::Bound(u32::try_from(bound).map_err(|_| Status::internal("Bound index overflow"))?)
                    }
                    Outcome::Refused { error } => return Err(error::anomaly(error)),
                }
            }
            Err(outside) if outside.code == STORE_OUTSIDE_EARLY => proto::append_reply::Outcome::Outside(proto::Side::Early as i32),
            Err(outside) if outside.code == STORE_OUTSIDE_LATE => proto::append_reply::Outcome::Outside(proto::Side::Late as i32),
            Err(error) => return Err(error::anomaly(error)),
        };
        Ok(proto::AppendReply { incarnation: incarnation.to_vec(), outcome: Some(outcome) }.encode_to_vec())
    }

    async fn store_watch(&self, call: &Call, request: proto::WatchRequest) -> Result<Vec<u8>, Status> {
        if request.ranges.is_empty() {
            return Err(error::incorrect("A watch names at least one range"));
        }
        if request.incarnation.is_empty() {
            return Err(error::incorrect("A watch states its incarnation"));
        }
        let (database, incarnation, scope) = self.store_database(call, DatabaseAccess::Read, Some(&request.incarnation)).await?;
        let watched = ranges(request.ranges)?;
        reads_within(scope, &watched)?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let wait = Duration::from_millis(request.wait_millis).min(MAX_WAIT);
        let deadline = tokio::time::Instant::now() + wait;
        let mut after = Basis::new(incarnation, request.after);
        let touched = loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let page = driver.changes(Changes {
                branch: MAIN_BRANCH.into(),
                tables: [store_table()].into(),
                after: Some(after),
                until: None,
                order: ChangeOrder::Ascending,
                images: ChangeImages::Keys,
                limit: MAX_CHANGE_GROUPS as usize,
                wait: remaining,
            }, &call.cancellation).await.map_err(error::anomaly)?;
            let hit = page.groups.iter().find(|group| group.rows.iter().any(|row| match &row.id {
                EntityId::Binary(key) => watched.iter().any(|range| range.contains(key)),
                _ => false,
            })).map(|group| group.cut.basis);
            if let Some(hit) = hit {
                after = hit;
                break Some(hit.position());
            }
            let progressed = page.through > after;
            after = after.max(page.through);
            if remaining.is_zero() || !progressed && page.complete {
                break None;
            }
        };
        self.check_attachment(call, &database)?;
        Ok(proto::WatchReply {
            incarnation: incarnation.to_vec(),
            touched,
            through: after.position(),
        }.encode_to_vec())
    }
}
