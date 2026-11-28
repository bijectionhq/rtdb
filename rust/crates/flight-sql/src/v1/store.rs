//! `rtdb.store.v1`: versioned ordered bytes with read, conditional append, watch, rank and size accounting.
//!
//! The actions carry no caller semantics. A read returns ordered entries
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
//! append guard, watch range and ranked scan lies inside one read prefix,
//! every written key and bounded counter under one write prefix, and its appends carry no
//! derives, whose keys the scope cannot bound. Anything else refuses with
//! `rtdb.store/outside-grant` before it is read or ordered.
//!
//! `Rank` runs Bijection's vendored `store_read::rank` (vendor/bijection)
//! over the same ordered entries: the ranking is Bijection's code, the store
//! only reads ranges at versions for it. A program for other ranking code is
//! refused; the server refuses to start with a vendored copy whose compiled
//! digest differs from the recorded one.

use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use rtdb_index::branches::store::{
    DERIVE_DIGEST, KeyRange, MAX_PAGE_ENTRIES, Projection, StoreBound, StorePage, StoreRead, StoreWrite, ValueFilter, check_bounds,
    check_derives, check_placeholders, store_table,
};
use rtdb_node::{DatabaseAccess, SessionIdentity, serving::{ChangeImages, ChangeOrder, Changes}};
use rtdb_tx::transact::{
    Guard, Op, Outcome, STORE_BOUND, STORE_CLAIM, STORE_OUTSIDE_EARLY, STORE_OUTSIDE_LATE,
    StoreAppend, StoreDenial, StoreDenialCheck, Transact, Window,
};
use rtdb_auth::StoreScope;
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Timestamp, rtdb_v1::{At, Basis, MAIN_BRANCH}};
use tonic::Status;

use super::{MAX_CHANGE_GROUPS, MAX_WAIT, check_key, error};
use crate::{rtdb::store::v1 as proto, service::{Call, Service}};

pub(crate) mod access;
#[cfg(feature = "multi")]
mod hold_version;
mod purge;
mod sizes;
#[cfg(feature = "multi")]
pub(crate) mod timed_append;
mod writer;

pub(crate) const READ: &str = "rtdb.store.v1.Read";
pub(crate) const APPEND: &str = "rtdb.store.v1.Append";
#[cfg(feature = "multi")]
pub(crate) const TIMED_APPEND_CAPABILITY: &str = "x-rtdb-store-timed-append";
pub(crate) const STORE_EXCHANGE_HEADER: &str = "x-rtdb-store-exchange";
pub(crate) const TIMED_APPEND_VERSION: &str = "timed-append-v1";
pub(crate) const WATCH: &str = "rtdb.store.v1.Watch";
pub(crate) const RANK: &str = "rtdb.store.v1.Rank";
pub(crate) use purge::{PURGE_FLOOR, PURGE_WRITER_HEADER};
pub(crate) use writer::{CLAIM_WRITER, FENCE_WRITER, WRITER_FENCE_HEADER};
pub(super) use sizes::STORAGE_SIZES;
pub(crate) const ACTIONS: [&str; 8] = [READ, APPEND, WATCH, RANK, STORAGE_SIZES, PURGE_FLOOR, FENCE_WRITER, CLAIM_WRITER];

/// The `RANK_DIGEST` recorded with the vendored copy (vendor/bijection).
const VENDORED_RANK_DIGEST: &str = include_str!("../../../../vendor/bijection/RANK_DIGEST");

/// Refuse to serve with a vendored ranking crate whose compiled digest is not
/// the one recorded from Bijection: a local edit of the copy, or a copy and
/// record out of step.
pub(crate) fn check_rank_digest() -> rtdb_types::Result<()> {
    if store_read::rank::RANK_DIGEST != VENDORED_RANK_DIGEST.trim() {
        return Err(rtdb_types::Anomaly::unsupported(
            "rtdb.store/rank-version",
            "The vendored Bijection ranking code does not match its recorded RANK_DIGEST",
        ));
    }
    Ok(())
}

struct StoreBatch {
    basis: Basis,
    pages: Vec<StorePage>,
    #[cfg(feature = "multi")]
    delivery: Option<rtdb_node::FollowerStoreDelivery>,
    #[cfg(feature = "multi")]
    hold: Option<rtdb_node::serving::StoreReadTurn>,
}

/// Retain the selected cut until the action response is handed to Flight.
#[cfg(feature = "multi")]
pub(crate) struct StoreReplyGuard {
    pub(crate) database: std::sync::Arc<rtdb_node::Database>,
    pub(crate) basis: Basis,
    pub(crate) delivery: Option<rtdb_node::FollowerStoreDelivery>,
    pub(crate) hold: Option<rtdb_node::serving::StoreReadTurn>,
}

#[cfg(feature = "multi")]
struct RetainedStoreReply {
    bytes: Bytes,
    _guard: StoreReplyGuard,
}

#[cfg(feature = "multi")]
impl AsRef<[u8]> for RetainedStoreReply {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

#[cfg(feature = "multi")]
fn include_delivery(
    admitted: &mut Option<rtdb_node::FollowerStoreDelivery>,
    read: rtdb_node::FollowerStoreDelivery,
) -> Result<(), Status> {
    match admitted {
        Some(admitted) => admitted.include(read).map_err(error::follower),
        None => {
            *admitted = Some(read);
            Ok(())
        },
    }
}

struct StoreRanges<'a> {
    service: &'a Service,
    driver: &'a dyn rtdb_node::serving::ServingDriver,
    scope: Option<&'a StoreScope>,
    incarnation: [u8; 16],
    now: rtdb_types::Timestamp,
    cancellation: &'a tokio_util::sync::CancellationToken,
    oldest: std::sync::Mutex<Option<Basis>>,
    #[cfg(feature = "multi")]
    delivery: std::sync::Mutex<Option<rtdb_node::FollowerStoreDelivery>>,
    #[cfg(feature = "multi")]
    hold: std::sync::Mutex<Option<rtdb_node::serving::StoreReadTurn>>,
}

impl store_read::source::RangeSource for StoreRanges<'_> {
    fn read(&self, read: store_read::source::RangeRead) -> impl std::future::Future<Output = anyhow::Result<store_read::source::RangePages>> + Send {
        async move {
            let at = read.at.map_or(At::Latest, |at| At::Exactly(Basis::new(self.incarnation, at.0)));
            let limit = read.limit.clamp(1, MAX_PAGE_ENTRIES);
            let read_ranges: Vec<_> = read.ranges.into_iter()
                .map(|range| KeyRange { start: range.start, end: range.end })
                .collect();
            for range in &read_ranges {
                range.check().map_err(error::anomaly)?;
            }
            reads_within(self.scope, &read_ranges)?;
            let mut reads = read_ranges.into_iter().map(|range| {
                let after = read.after.clone().filter(|after| range.contains(after));
                StoreRead { range, after, limit, projection: Projection::Values, filter: None, reverse: false }
            });
            let batch = self.service.read_store_batch(self.driver, &mut reads, at, self.incarnation, self.now, self.cancellation).await?;
            {
                let mut oldest = self.oldest.lock().map_err(|_| Status::internal("Rank delivery state is unavailable"))?;
                #[cfg(feature = "multi")]
                {
                    let mut held = self.hold.lock().map_err(|_| Status::internal("Rank retention state is unavailable"))?;
                    if oldest.is_some() && (held.is_some() != batch.hold.is_some()
                        || held.as_ref().zip(batch.hold.as_ref()).is_some_and(|(prior, turn)| !prior.same_writer(turn)))
                    {
                        return Err(Status::unavailable("The Store writer changed during rank").into());
                    }
                    if let Some(turn) = batch.hold
                        && held.as_ref().is_none_or(|prior| turn.basis() < prior.basis())
                    {
                        *held = Some(turn);
                    }
                }
                *oldest = Some(oldest.map_or(batch.basis, |prior| prior.min(batch.basis)));
            }
            #[cfg(feature = "multi")]
            if let Some(delivery) = batch.delivery {
                let mut admitted = self.delivery.lock().map_err(|_| Status::internal("Rank delivery state is unavailable"))?;
                include_delivery(&mut admitted, delivery)?;
            }
            let at = store_read::store::Version(batch.basis.position());
            let pages = batch.pages.into_iter().map(|page| store_read::source::RangePage {
                entries: page.entries.into_iter().map(|entry| store_read::store::Entry {
                    key: entry.key,
                    value: entry.value,
                    written: store_read::store::Version(entry.written.position()),
                }).collect(),
                resume: page.resume,
            }).collect();
            Ok(store_read::source::RangePages { at, pages })
        }
    }
}

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

fn read_batch_basis(at: At, incarnation: [u8; 16], expected_pages: usize, pages: &[StorePage]) -> Result<Basis, Status> {
    let first = pages.first().filter(|_| pages.len() == expected_pages)
        .ok_or_else(|| Status::internal("A Store read must return one page per range in a nonempty batch"))?;
    let basis = first.cut.basis;
    if pages.iter().any(|page| page.cut.basis.incarnation() != incarnation) {
        return Err(Status::failed_precondition("The store incarnation changed"));
    }
    if matches!(at, At::Exactly(expected) if basis != expected)
        || pages.iter().any(|page| page.cut.basis != basis)
    {
        return Err(Status::internal("A Store read must return every page at the selected version"));
    }
    Ok(basis)
}

impl Service {
    pub(super) async fn store_action(&self, call: &Call, name: &str, body: Bytes) -> Result<Vec<u8>, Status> {
        match name {
            READ => self.store_read(call, decode(body)?).await,
            APPEND => self.store_append(call, decode(body)?).await,
            WATCH => self.store_watch(call, decode(body)?).await,
            RANK => self.store_rank(call, decode(body)?).await,
            STORAGE_SIZES => self.store_storage_sizes(call, decode(body)?).await,
            PURGE_FLOOR => self.store_purge_floor(call, decode(body)?).await,
            FENCE_WRITER => self.store_fence_writer(call, decode(body)?).await,
            CLAIM_WRITER => self.store_claim_writer(call, decode(body)?).await,
            #[cfg(feature = "multi")]
            hold_version::ACTION if hold_version::AVAILABLE => self.store_hold_version(call, decode(body)?).await,
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
        let database = if self.follower_only() || call.session.fresh_replica {
            if call.session.store_release.is_none() || !matches!(access, DatabaseAccess::Read) {
                return Err(error::unsupported("rtdb.v1.route"));
            }
            call.session.caller.principal.authorize_database(&call.database, access).map_err(error::anomaly)?;
            let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
            attachment.ok_or_else(|| error::unsupported("rtdb.v1.route"))?
        } else {
            self.wire_attachment(call, access).await?
        };
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let current = driver.incarnation();
        if let Some(expected) = call.session.timed_store_incarnation
            && (expected != current || !call.session.holds_attachment(&database))
        {
            return Err(Status::failed_precondition("The timed Store session's writer attachment changed"));
        }
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

    async fn read_store_batch(
        &self, driver: &dyn rtdb_node::serving::ServingDriver,
        reads: &mut (dyn ExactSizeIterator<Item = StoreRead> + Send), at: At, incarnation: [u8; 16], now: Timestamp,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<StoreBatch, Status> {
        let expected_pages = reads.len();
        #[cfg(feature = "multi")]
        let (pages, delivery, hold) = if let Some(follower) = self.follower() {
            let mut page_at = at;
            let mut pages = Vec::with_capacity(expected_pages);
            let mut delivery = None;
            for read in reads {
                let (page, received) = follower.reader.read_store(read, page_at, now, cancellation).await.map_err(error::follower)?;
                include_delivery(&mut delivery, received)?;
                page_at = At::Exactly(page.cut.basis);
                pages.push(page);
            }
            (pages, delivery, None)
        } else {
            let hold = driver.enter_local_store_read(at, cancellation).await.map_err(error::anomaly)?;
            let at = hold.as_ref().map_or(at, |hold| At::Exactly(hold.basis()));
            let pages = driver.read_store_batch(reads, at, now, cancellation).await.map_err(error::anomaly)?;
            (pages, None, hold)
        };
        #[cfg(not(feature = "multi"))]
        let pages = driver.read_store_batch(reads, at, now, cancellation).await.map_err(error::anomaly)?;
        let basis = read_batch_basis(at, incarnation, expected_pages, &pages)?;
        Ok(StoreBatch {
            basis, pages,
            #[cfg(feature = "multi")]
            delivery,
            #[cfg(feature = "multi")]
            hold,
        })
    }

    #[cfg(feature = "multi")]
    async fn check_follower_store_reply(
        &self, delivery: Option<&rtdb_node::FollowerStoreDelivery>,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<(), Status> {
        if let Some(delivery) = delivery {
            let follower = self.follower().ok_or_else(|| Status::unavailable("The assigned Store follower is unavailable"))?;
            follower.reader.check_store_delivery(delivery, cancellation).await.map_err(error::follower)?;
        }
        Ok(())
    }

    #[cfg(feature = "multi")]
    pub(crate) async fn check_store_reply_guard(&self, call: &Call, guard: &StoreReplyGuard) -> Result<(), Status> {
        let driver = guard.database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        self.check_follower_store_reply(guard.delivery.as_ref(), &call.cancellation).await?;
        self.check_local_store_reply(driver, guard.basis, &call.cancellation, guard.hold.as_ref()).await?;
        self.check_attachment(call, &guard.database)
    }

    #[cfg(feature = "multi")]
    pub(super) fn store_reply_stream(
        &self, call: Call, guard: StoreReplyGuard, bytes: Bytes,
    ) -> Result<tonic::Response<crate::service::Output<arrow_flight::Result>>, Status> {
        let headers = call.response(())?.metadata().clone();
        let service = self.clone();
        let stream: crate::service::Output<_> = Box::pin(async_stream::try_stream! {
            service.check_store_reply_guard(&call, &guard).await.map_err(|status| call.error(status))?;
            let body = Bytes::from_owner(RetainedStoreReply { bytes, _guard: guard });
            yield arrow_flight::Result { body };
        });
        let mut response = tonic::Response::new(stream);
        *response.metadata_mut() = headers;
        Ok(response)
    }

    async fn check_local_store_reply(
        &self, driver: &dyn rtdb_node::serving::ServingDriver, basis: Basis,
        cancellation: &tokio_util::sync::CancellationToken,
        #[cfg(feature = "multi")] turn: Option<&rtdb_node::serving::StoreReadTurn>,
    ) -> Result<(), Status> {
        #[cfg(feature = "multi")]
        if self.follower().is_some() {
            return Ok(());
        }
        driver.check_local_store_basis(
            basis, cancellation,
            #[cfg(feature = "multi")]
            turn,
        ).await.map_err(error::anomaly)
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
        let at = request.at.map_or(At::Latest, |at| At::Exactly(Basis::new(incarnation, at)));
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
        let mut reads = read_ranges.into_iter().map(|range| {
            let after = request.after.clone().filter(|after| range.contains(after));
            StoreRead { range, after, limit, projection, filter: filter.clone(), reverse: request.reverse }
        });
        let clock = self.0.node.now();
        let batch = self.read_store_batch(driver, &mut reads, at, incarnation, clock, &call.cancellation).await?;
        let pages = batch.pages.into_iter().map(|page| proto::RangePage {
            entries: page.entries.into_iter().map(|entry| proto::Entry {
                key: entry.key, value: entry.value, written: entry.written.position(),
            }).collect(),
            resume: page.resume,
        }).collect();
        let reply = proto::ReadReply {
            incarnation: incarnation.to_vec(),
            at: batch.basis.position(),
            pages,
            clock_micros: Some(clock.0),
        }.encode_to_vec();
        #[cfg(feature = "multi")]
        {
            let guard = StoreReplyGuard { database, basis: batch.basis, delivery: batch.delivery, hold: batch.hold };
            self.check_store_reply_guard(call, &guard).await?;
            call.retain_store_reply(guard)?;
        }
        #[cfg(not(feature = "multi"))]
        self.check_local_store_reply(
            driver, batch.basis, &call.cancellation,
        ).await?;
        #[cfg(not(feature = "multi"))]
        self.check_attachment(call, &database)?;
        Ok(reply)
    }

    async fn store_append(&self, call: &Call, request: proto::AppendRequest) -> Result<Vec<u8>, Status> {
        let input = StoreAppendInput::parse(request)?;
        let (database, incarnation, scope) = self.store_database(call, DatabaseAccess::Write, Some(&input.request.incarnation)).await?;
        let admits_retained_basis = call.session.caller.verified_credential
            && scope.is_none() && !call.session.fresh_replica && !self.follower_only();
        let (transaction, validation) = input.transaction(incarnation, scope, admits_retained_basis, call.request_reservation())?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let decided = driver.transact(transaction, &call.cancellation).await;
        if decided.is_ok() {
            self.check_attachment(call, &database)?;
        }
        Ok(append_reply(incarnation, decided, validation)?.encode_to_vec())
    }
}

struct StoreAppendInput {
    request: proto::AppendRequest,
    writes: Vec<StoreWrite>,
    bounds: Vec<StoreBound>,
    alternative: Option<StoreDenial>,
    window: Window,
}

impl StoreAppendInput {
    fn parse(mut request: proto::AppendRequest) -> Result<Self, Status> {
        check_key(&request.key)?;
        if request.incarnation.is_empty() {
            return Err(error::incorrect("An append states its incarnation"));
        }
        let writes = std::mem::take(&mut request.writes).into_iter()
            .map(|write| StoreWrite { key: write.key, value: write.value, key_stamp: write.key_stamp, value_stamp: write.value_stamp })
            .collect::<Vec<_>>();
        let bounds = std::mem::take(&mut request.bounds).into_iter()
            .map(|bound| StoreBound { key: bound.key, delta: bound.delta, min: bound.min, max: bound.max })
            .collect::<Vec<_>>();
        let alternative = request.claim_alternative.take().map(|alternative| {
            let check = match alternative.bound {
                Some(ordinal) if alternative.claims.is_empty() => StoreDenialCheck::Bound(ordinal as usize),
                None if !alternative.claims.is_empty() => StoreDenialCheck::Claims(alternative.claims.into_iter().collect()),
                _ => return Err(error::incorrect("A denial names either a bound or fixed claim keys")),
            };
            let writes = alternative.writes.into_iter()
                .map(|write| StoreWrite { key: write.key, value: write.value, key_stamp: write.key_stamp, value_stamp: write.value_stamp })
                .collect();
            Ok(StoreDenial { check, writes, derives: alternative.derives })
        }).transpose()?;
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
        let window = request.window.take().map_or(Window::default(), |window| Window {
            not_before: window.not_before.map(Timestamp),
            before: window.before.map(Timestamp),
        });
        if window.not_before.zip(window.before).is_some_and(|(start, end)| start >= end) {
            return Err(error::incorrect("The append's window is empty"));
        }
        Ok(Self { request, writes, bounds, alternative, window })
    }

    fn transaction(
        self,
        incarnation: [u8; 16],
        scope: Option<&StoreScope>,
        admits_retained_basis: bool,
        request_memory: std::sync::Arc<rtdb_columnar::MemoryReservation>,
    ) -> Result<(Transact, bool), Status> {
        let Self { request, writes, bounds, alternative, window } = self;
        let validation = writes.is_empty() && bounds.is_empty() && request.derives.is_empty();
        let read_guards = ranges(request.reads)?;
        reads_within(scope, &read_guards)?;
        within(scope, |scope| {
            alternative.is_none() && request.derives.is_empty()
                && writes.iter().all(|write| scope.admits_write(&write.key, write.key_stamp))
                && bounds.iter().all(|bound| scope.admits_write(&bound.key, None))
        })?;
        let since = Basis::new(incarnation, request.since);
        let mut guards = read_guards.into_iter()
            .map(|range| Guard::Unwritten { range, since })
            .collect::<Vec<_>>();
        if let Some(required) = request.required_basis {
            if !admits_retained_basis {
                return Err(Status::permission_denied("An ordered retained basis requires an unscoped writer"));
            }
            if required > request.since {
                return Err(error::incorrect("The required basis cannot exceed the append predecessor"));
            }
            guards.push(Guard::RetainedStoreBasis { basis: Basis::new(incarnation, required) });
        }
        if let Some(binding) = request.writer_fence {
            let fence = writer::fence(binding.epoch, Some(binding.writer))?;
            guards.push(if binding.generation == 0 {
                if !binding.claim.is_empty() { return Err(error::incorrect("A writer claim requires its ordered generation")); }
                Guard::StoreWriter { incarnation, fence }
            } else {
                let rtdb_index::branches::store::StoreWriterClaim { fence, generation, claim } =
                    writer::ordered_claim(fence, binding.generation, &binding.claim)?;
                Guard::StoreWriterGeneration { incarnation, fence, generation, claim }
            });
        }
        let transaction = Transact {
            key: request.key,
            require: Vec::new(),
            window: Window::default(),
            guards,
            ops: vec![Op::Store(StoreAppend { writes, bounds, derives: request.derives, window, alternative })],
            otherwise: Vec::new(),
            requires: Vec::new(),
            custody_request_digest: None,
            request_memory: Some(request_memory),
        };
        Ok((transaction, validation))
    }
}

fn append_reply(
    incarnation: [u8; 16],
    decided: rtdb_types::Result<rtdb_tx::transact::Receipt>,
    validation: bool,
) -> Result<proto::AppendReply, Status> {
    let outcome = match decided {
            Ok(receipt) => match receipt.outcome {
                    Outcome::Then if validation => proto::append_reply::Outcome::Validated(receipt.basis.position()),
                    Outcome::Then => proto::append_reply::Outcome::Committed(proto::Stamp {
                        micros: receipt.store_reading.ok_or_else(|| Status::internal("The Store receipt has no clock reading"))?.0,
                        version: receipt.basis.position(),
                    }),
                    Outcome::Otherwise { failed } => {
                        let first = failed.first().copied().ok_or_else(|| Status::internal("A failed append names no read"))?;
                        proto::append_reply::Outcome::Conflict(u32::try_from(first).map_err(|_| Status::internal("Read index overflow"))?)
                    }
                    Outcome::StoreDenied => {
                        proto::append_reply::Outcome::ClaimDenied(proto::Stamp {
                            micros: receipt.store_reading.ok_or_else(|| Status::internal("The Store receipt has no clock reading"))?.0,
                            version: receipt.basis.position(),
                        })
                    }
                    Outcome::Refused { error } if error.code == STORE_BOUND => {
                        let bound = error.context.get("bound").and_then(|value| value.as_u64())
                            .ok_or_else(|| Status::internal("A failed bound names no index"))?;
                        proto::append_reply::Outcome::Bound(u32::try_from(bound).map_err(|_| Status::internal("Bound index overflow"))?)
                    }
                    Outcome::Refused { error } if error.code == STORE_CLAIM => {
                        let derive = error.context.get("derive").and_then(|value| value.as_u64())
                            .ok_or_else(|| Status::internal("A failed claim names no derive"))?;
                        proto::append_reply::Outcome::Claimed(u32::try_from(derive).map_err(|_| Status::internal("Derive index overflow"))?)
                    }
                    Outcome::Refused { error } => return Err(error::anomaly(error)),
            },
            Err(outside) if outside.code == STORE_OUTSIDE_EARLY => proto::append_reply::Outcome::Outside(proto::Side::Early as i32),
            Err(outside) if outside.code == STORE_OUTSIDE_LATE => proto::append_reply::Outcome::Outside(proto::Side::Late as i32),
            Err(error) => return Err(error::anomaly(error)),
    };
    Ok(proto::AppendReply { incarnation: incarnation.to_vec(), outcome: Some(outcome) })
}

impl Service {
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
        let original = after;
        #[cfg(feature = "multi")]
        let hold = if self.follower().is_none() {
            driver.enter_local_store_read(At::Exactly(original), &call.cancellation).await.map_err(error::anomaly)?
        } else {
            None
        };
        #[cfg(feature = "multi")]
        let mut delivery = None;
        let touched = loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let request = Changes {
                branch: MAIN_BRANCH.into(),
                tables: [store_table()].into(),
                after: Some(after),
                until: None,
                order: ChangeOrder::Ascending,
                images: ChangeImages::Keys,
                limit: MAX_CHANGE_GROUPS as usize,
                wait: remaining,
            };
            #[cfg(feature = "multi")]
            let page = if let Some(follower) = self.follower() {
                let (page, received) = follower.reader.store_changes(request, &call.cancellation).await.map_err(error::follower)?;
                include_delivery(&mut delivery, received)?;
                page
            } else {
                driver.changes(request, &call.cancellation).await.map_err(error::anomaly)?
            };
            #[cfg(not(feature = "multi"))]
            let page = driver.changes(request, &call.cancellation).await.map_err(error::anomaly)?;
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
        let reply = proto::WatchReply {
            incarnation: incarnation.to_vec(),
            touched,
            through: after.position(),
        }.encode_to_vec();
        #[cfg(feature = "multi")]
        {
            let guard = StoreReplyGuard { database, basis: original, delivery, hold };
            self.check_store_reply_guard(call, &guard).await?;
            call.retain_store_reply(guard)?;
        }
        #[cfg(not(feature = "multi"))]
        self.check_local_store_reply(
            driver, original, &call.cancellation,
        ).await?;
        #[cfg(not(feature = "multi"))]
        self.check_attachment(call, &database)?;
        Ok(reply)
    }

    async fn store_rank(&self, call: &Call, request: proto::RankRequest) -> Result<Vec<u8>, Status> {
        if request.incarnation.is_empty() {
            return Err(error::incorrect("A ranked read states its incarnation"));
        }
        let program = store_read::rank::RankRequest::decode(&request.program).map_err(|refused| error::incorrect(&refused.to_string()))?;
        let (database, incarnation, scope) = self.store_database(call, DatabaseAccess::Read, Some(&request.incarnation)).await?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let source = StoreRanges {
            service: self, driver, scope, incarnation, now: self.0.node.now(), cancellation: &call.cancellation,
            oldest: std::sync::Mutex::new(None),
            #[cfg(feature = "multi")]
            delivery: std::sync::Mutex::new(None),
            #[cfg(feature = "multi")]
            hold: std::sync::Mutex::new(None),
        };
        let reply = store_read::rank::rank(&source, program).await.map_err(|refused| {
            let refused = match refused.downcast::<Status>() {
                Ok(status) => return status,
                Err(refused) => refused,
            };
            let message = refused.to_string();
            if message.starts_with("rtdb.store/rank-version") {
                Status::failed_precondition(message)
            } else {
                error::incorrect(&message)
            }
        })?;
        let reply = reply.encode().map_err(|failed| Status::internal(failed.to_string()))?;
        let reply = proto::RankReply { incarnation: incarnation.to_vec(), reply }.encode_to_vec();
        #[cfg(feature = "multi")]
        let delivery = source.delivery.into_inner().map_err(|_| Status::internal("Rank delivery state is unavailable"))?;
        #[cfg(feature = "multi")]
        let hold = source.hold.into_inner().map_err(|_| Status::internal("Rank retention state is unavailable"))?;
        if let Some(oldest) = source.oldest.into_inner().map_err(|_| Status::internal("Rank delivery state is unavailable"))? {
            #[cfg(feature = "multi")]
            {
                let guard = StoreReplyGuard { database, basis: oldest, delivery, hold };
                self.check_store_reply_guard(call, &guard).await?;
                call.retain_store_reply(guard)?;
                return Ok(reply);
            }
            #[cfg(not(feature = "multi"))]
            self.check_local_store_reply(
                driver, oldest, &call.cancellation,
            ).await?;
        }
        self.check_attachment(call, &database)?;
        Ok(reply)
    }
}

#[cfg(test)]
mod rank_tests {
    /// The vendored ranking code is the copy recorded from Bijection, and
    /// its pins are the versions this workspace locks.
    #[test]
    fn the_vendored_ranking_code_is_bijections_recorded_copy() {
        super::check_rank_digest().expect("vendor/bijection/RANK_DIGEST is the compiled digest");
        let lock = include_str!("../../../../Cargo.lock");
        for pin in store_read::rank::PINS.lines() {
            let (name, version) = pin.split_once('=').expect("a pin is name=version");
            assert!(lock.contains(&format!("name = \"{name}\"\nversion = \"{version}\"")), "{name} {version} is not locked");
        }
    }
}

#[cfg(test)]
mod read_batch_tests {
    use super::*;
    use rtdb_index::branches::BranchCut;
    use tonic::Code;

    const INCARNATION: [u8; 16] = [7; 16];

    fn page(basis: Basis) -> StorePage {
        StorePage { cut: BranchCut { basis, system_time: Timestamp(0) }, entries: Vec::new(), resume: None }
    }

    #[test]
    fn empty_range_pages_can_carry_version_zero() {
        let basis = Basis::new(INCARNATION, 0);
        let pages = [page(basis), page(basis)];
        for at in [At::Latest, At::Exactly(basis)] {
            assert_eq!(read_batch_basis(at, INCARNATION, 2, &pages).unwrap(), basis);
        }
    }

    #[test]
    fn read_batches_refuse_missing_ranges_and_inconsistent_bases() {
        let basis = Basis::new(INCARNATION, 3);
        let later = Basis::new(INCARNATION, 4);
        let foreign = Basis::new([8; 16], 3);
        for (at, expected_pages, pages, code) in [
            (At::Latest, 1, vec![], Code::Internal),
            (At::Latest, 2, vec![page(basis)], Code::Internal),
            (At::Latest, 1, vec![page(basis), page(basis)], Code::Internal),
            (At::Latest, 2, vec![page(basis), page(later)], Code::Internal),
            (At::Exactly(basis), 1, vec![page(later)], Code::Internal),
            (At::Latest, 2, vec![page(basis), page(foreign)], Code::FailedPrecondition),
        ] {
            assert_eq!(read_batch_basis(at, INCARNATION, expected_pages, &pages).unwrap_err().code(), code);
        }
    }
}
