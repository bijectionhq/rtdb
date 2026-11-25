use std::{
    collections::{BTreeSet, VecDeque},
    mem::size_of,
    sync::{Arc, Mutex},
};

use arrow_array::{
    Array, ArrayRef, RecordBatch,
    cast::AsArray,
    types::{
        ArrowPrimitiveType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_flight::FlightData;
use arrow_schema::{DataType, Field, Schema};
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_crypt::tokens::{PageToken, ReceiverClockObservation, TokenIssuer};
use rtdb_node::{Database, PreparedSql, ResultSignaturePolicy, serving::{QuerySource, QueryView, ServingDriver, ServingScope, ranked_ivf}};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_tx::transact::CustodyGroupId;
use rtdb_types::{LogicalType, TableRef, Timestamp, Value, rtdb_v1::{At, Basis, ClaimEvidence, ClaimFootprint, ClaimScope, LabelScope, Layer}};
use sha2::{Digest, Sha256};
use tonic::{Response, Status};

use crate::{
    service::{Call, Output, Service},
    wire,
};

use super::{
    MAX_PAGE_ROWS, MAX_WAIT, error, head,
    original_family_landing::{MANAGED_FEATURE, is_managed_family},
    proto::{self, at::Selection},
    tokens::{self, cursor_expired},
    trailer,
};

#[cfg(feature = "multi")]
pub(super) fn follower_progress(route: &crate::follower::FollowerRoute, progress: Option<rtdb_node::MultiFollowerProgress>)
    -> Option<(u64, proto::FollowerProgress)> {
    progress.map(|progress| (progress.applied_position, proto::FollowerProgress {
        database: route.assignment.database.clone(),
        incarnation: route.assignment.incarnation.as_bytes().to_vec(),
        placement_epoch: route.assignment.placement_epoch,
        assignment: route.assignment.assignment.as_bytes().to_vec(),
        endpoint: route.endpoint.to_string(),
        writer_head: Some(progress.writer_head.into()),
        writer_head_position: progress.writer_head_position,
        retained_from: Some(progress.retained_from.into()),
        retained_from_position: progress.retained_from.position(),
    }))
}

#[derive(Default)]
pub(super) struct PlanCache(Mutex<VecDeque<(uuid::Uuid, PreparedSql)>>);

impl PlanCache {
    pub fn prepare(
        &self,
        scope: uuid::Uuid,
        sql: &str,
        context: &Arc<QueryContext>,
        types: &[LogicalType],
    ) -> Result<PreparedSql, Status> {
        {
            let mut cached = self.0.lock().expect("plan cache lock");
            if let Some(index) = cached.iter().position(|(name, plan)| {
                *name == scope && plan.source() == sql && plan.parameter_types() == types
            }) {
                let entry = cached.remove(index).expect("matching prepared statement");
                let plan = entry.1.clone();
                cached.push_back(entry);
                return Ok(plan);
            }
        }
        let options = rtdb_sql::PrepareOptions {
            parameter_types: types.iter().cloned().enumerate().collect(),
            ..Default::default()
        };
        let prepared = PreparedSql::prepare(sql, context, options, ResultSignaturePolicy::Dynamic)
            .map_err(error::anomaly)?;
        if prepared.is_dml() || prepared.selects_read_settings() {
            return Err(error::incorrect(
                "Query requires read-only SQL and uses Source for its read settings",
            ));
        }
        let mut cached = self.0.lock().expect("plan cache lock");
        if cached.len() == 32 {
            cached.pop_front();
        }
        cached.push_back((scope, prepared.clone()));
        Ok(prepared)
    }
}

fn fingerprint(query: &proto::QueryRequest) -> [u8; 32] {
    let mut hash = Sha256::new();
    if let Some(source) = &query.source {
        hash.update(Sha256::digest(source.encode_to_vec()));
    }
    hash.update(Sha256::digest(query.sql.as_bytes()));
    if let Some(params) = &query.params_ipc {
        hash.update(Sha256::digest(params));
    }
    hash.update([query.track.into()]);
    if query.observe_receiver_clock { hash.update(b"receiver-clock"); }
    if query.custody_release_evidence { hash.update(b"custody-release-evidence"); }
    if let Some(review) = &query.custody_review {
        hash.update(b"custody-review");
        hash.update(Sha256::digest(review.encode_to_vec()));
    }
    if let Some(hold) = &query.scoped_hold { hash.update(b"scoped-hold"); hash.update(hold); }
    if let Some(read) = &query.exact_read_scope { hash.update(b"exact-read"); hash.update(read); }
    if query.require_clear_rank_input { hash.update(b"clear-rank-input"); }
    if query.observe_applied_position { hash.update(b"observe-applied-position"); }
    if query.verify_ranked_artefacts { hash.update(b"ranked-artefact-readiness"); }
    if let Some(ranked) = &query.ranked_ivf {
        hash.update(b"ranked-ivf");
        hash.update(Sha256::digest(ranked.encode_to_vec()));
    }
    hash.finalize().into()
}

#[cfg(test)]
#[test]
fn continuation_fingerprint_binds_the_exact_read_scope() {
    let mut request = proto::QueryRequest {
        sql: "SELECT _id FROM items ORDER BY _id".into(),
        exact_read_scope: Some(vec![1; 16]),
        ..Default::default()
    };
    let first = fingerprint(&request);
    request.page = Some(proto::Page { limit: 1, after: vec![3] });
    assert_eq!(first, fingerprint(&request));
    request.exact_read_scope = Some(vec![2; 16]);
    assert_ne!(first, fingerprint(&request));
}

struct OpenedQuery {
    cursor: QueryCursor,
    database: Arc<Database>,
    issuer: Option<TokenIssuer>,
    labels: Option<LabelScope>,
    page: PageToken,
    limit: u64,
    trailer: proto::QueryTrailer,
    custody_groups: BTreeSet<CustodyGroupId>,
    key_generation: Option<u64>,
    scoped_hold_until: Option<Timestamp>,
    exact_read: Option<Arc<super::exact_read::ExactRead>>,
}

impl Service {
    async fn query_view(
        &self, call: &Call, driver: &dyn ServingDriver, source: QuerySource, visibility: Option<&ServingScope>,
        request: &proto::QueryRequest, now: Timestamp,
    ) -> Result<(QueryView, Option<(u64, proto::FollowerProgress)>), Status> {
        #[cfg(feature = "multi")]
        if let Some(route) = self.follower() {
            let (view, progress) = if request.verify_ranked_artefacts {
                route.reader.query_ranked_artefacts(&call.database, source, now, request.observe_applied_position, &call.cancellation).await
            } else {
                route.reader.query(source.at, now, request.observe_applied_position, &call.cancellation).await
            }.map_err(error::follower)?;
            return Ok((view, follower_progress(route, progress)));
        }
        if call.session.caller.principal.workload_grant().is_some() {
            let (_, cut) = self.workload_root_cut(call).await?;
            let view = driver.query_workload_root(source, cut, now, &call.cancellation).await.map_err(error::anomaly)?;
            return Ok((view, None));
        }
        let view = match visibility {
            Some(scope) => driver.query_scoped(source, request.track, now, scope, &call.cancellation).await,
            None if request.verify_ranked_artefacts => driver.query_ranked_artefacts(&call.database, source, now, &call.cancellation).await,
            None => driver.query(source, false, now, &call.cancellation).await,
        }.map_err(error::anomaly)?;
        Ok((view, None))
    }

    async fn ranked_view(&self, call: &Call, driver: &dyn ServingDriver, source: QuerySource,
        target: &ranked_ivf::Target, now: Timestamp, observe: bool,
    ) -> Result<(QueryView, ranked_ivf::Catalog, Option<(u64, proto::FollowerProgress)>), Status> {
        #[cfg(feature = "multi")]
        if let Some(route) = self.follower() {
            let (view, catalog, progress) = route.reader.query_ranked_ivf(source, target, now, observe, &call.cancellation)
                .await.map_err(error::follower)?;
            return Ok((view, catalog, follower_progress(route, progress)));
        }
        let _ = observe;
        let (view, catalog) = driver.query_ranked_ivf(source, target, now, &call.cancellation).await.map_err(error::anomaly)?;
        Ok((view, catalog, None))
    }

    pub(super) async fn v1_query(
        &self,
        call: Call,
        request: proto::QueryRequest,
    ) -> Result<Response<Output<FlightData>>, Status> {
        let OpenedQuery { mut cursor, database, issuer, labels, page, limit, trailer: mut trailer_msg, custody_groups, key_generation, scoped_hold_until, exact_read } = self
            .open_v1_query(&call, request)
            .await
            .map_err(|status| call.error(status))?;
        let memory = call.memory().clone();
        let schema = wire::plan_schema(cursor.schema(), &memory).map_err(error::map_status)?;
        let initial = cursor.delivery_grant().await.map_err(error::anomaly)?;
        let (headers, cutoff) = call
            .delivery_headers(&initial, true)
            .map_err(error::map_status)?;
        let service = self.clone();
        let stream: Output<FlightData> = Box::pin(async_stream::try_stream! {
            let mut encoder = wire::Encoder::new(&schema, &memory).map_err(error::map_status)?;
            let schema_frame = wire::schema_data(&schema, &memory).map_err(error::map_status)?;
            service.check_scoped_query_delivery(&call, &database, page.basis, key_generation, &custody_groups, exact_read.as_deref()).await?;
            service.check_attachment(&call, &database)?;
            service.check_hold_deadline(scoped_hold_until)?;
            call.handoff(initial, cutoff).map_err(error::map_status)?;
            yield schema_frame;
            let mut skip = page.offset;
            let mut remaining = limit;
            let mut sent = 0_u64;
            loop {
                service.check_attachment(&call, &database)?;
                let encoded = cursor.encode_next_batch(|batch| -> Result<_, Status> {
                    let rows = batch.batch().num_rows() as u64;
                    let discard = rows.min(skip);
                    skip -= discard;
                    let rows = rows - discard;
                    let take = rows.min(remaining);
                    remaining -= take;
                    sent += take;
                    let data = if take == 0 { Vec::new() } else {
                        let slice = batch.slice(discard as usize, take as usize).map_err(error::anomaly)?;
                        let declared = rtdb_columnar::conform_batch(slice, &schema, &memory).map_err(error::anomaly)?;
                        encoder.declared_batch(&declared, &memory).map_err(error::map_status)?
                    };
                    Ok((data, rows > take))
                }).await.map_err(error::anomaly)?.transpose()?;
                let Some((data, more)) = encoded else { break; };
                for item in data {
                    if let Some(issuer) = &issuer { issuer.check_readable().await.map_err(error::anomaly)?; }
                    let grant = cursor.delivery_grant().await.map_err(error::anomaly)?;
                    service.check_scoped_query_delivery(&call, &database, page.basis, key_generation, &custody_groups, exact_read.as_deref()).await?;
                    service.check_attachment(&call, &database)?;
                    service.check_hold_deadline(scoped_hold_until)?;
                    call.handoff(grant, cutoff).map_err(error::map_status)?;
                    yield item;
                }
                if more {
                    let next = PageToken { offset: page.offset.checked_add(sent).ok_or_else(|| error::incorrect("Page offset overflow"))?, ..page };
                    let issuer = issuer.as_ref().ok_or_else(|| error::unsupported("query.page"))?;
                    trailer_msg.next = Some(issuer.seal_page_in(&next, labels.as_ref()).await.map_err(error::anomaly)?);
                    break;
                }
            }
            if skip != 0 { Err(cursor_expired())?; }
            if let Some(issuer) = &issuer { issuer.check_readable().await.map_err(error::anomaly)?; }
            let grant = cursor.delivery_grant().await.map_err(error::anomaly)?;
            service.check_scoped_query_delivery(&call, &database, page.basis, key_generation, &custody_groups, exact_read.as_deref()).await?;
            service.check_attachment(&call, &database)?;
            service.check_hold_deadline(scoped_hold_until)?;
            call.handoff(grant, cutoff).map_err(error::map_status)?;
            yield trailer(trailer_msg);
        });
        let mut response = Response::new(stream);
        *response.metadata_mut() = headers;
        Ok(response)
    }

    fn check_hold_deadline(&self, deadline: Option<Timestamp>) -> Result<(), Status> {
        if deadline.is_some_and(|deadline| self.0.node.now() >= deadline) {
            return Err(super::scoped_hold::hold_unavailable());
        }
        Ok(())
    }

    async fn check_scoped_query_delivery(
        &self, call: &Call, database: &Database, basis: Basis, generation: Option<u64>,
        groups: &BTreeSet<CustodyGroupId>, read: Option<&super::exact_read::ExactRead>,
    ) -> Result<(), Status> {
        if let Some(read) = read {
            read.check()?;
            #[cfg(feature = "multi")]
            if let Some(route) = self.follower() {
                route.reader.check_retained_delivery(read.generation, &call.cancellation)
                    .await.map_err(error::follower)?;
                return read.check();
            }
            return Err(error::unsupported(super::exact_read::FEATURE));
        }
        self.check_query_delivery(call, database, basis, generation, groups).await
    }

    pub(super) async fn check_query_delivery(&self, call: &Call, database: &Database, _basis: Basis, generation: Option<u64>, groups: &BTreeSet<CustodyGroupId>) -> Result<(), Status> {
        #[cfg(feature = "multi")]
        if let Some(route) = self.follower() {
            let generation = generation.ok_or_else(|| error::unavailable("The follower cut has no readable generation"))?;
            return route.reader.check_delivery(_basis, generation, self.0.node.now(), &call.cancellation).await.map_err(error::follower);
        }
        if is_managed_family(call) {
            let family = self.managed_family(call).await?;
            if !std::ptr::eq(family.database.as_ref(), database) || _basis > family.head
                || _basis.incarnation() != family.head.incarnation()
                || groups.len() != 1 || !groups.contains(&family.scope.group)
            {
                return Err(error::unsupported(MANAGED_FEATURE));
            }
            let driver = database.serving_driver().ok_or_else(|| error::unsupported(MANAGED_FEATURE))?;
            let index = driver.current_custody(&call.cancellation).await.map_err(error::anomaly)?;
            rtdb_tx::transact::workload::require_family_branch(&index, &family.scope.branch,
                family.created()?, self.0.node.now()).map_err(error::anomaly)?;
        } else if call.session.caller.principal.workload_grant().is_some() {
            let (admitted, cut) = self.workload_root_cut(call).await?;
            if !std::ptr::eq(admitted.as_ref(), database) || !groups.is_empty()
                || _basis.incarnation() != cut.basis.incarnation() || _basis > cut.basis
            {
                return Err(error::unsupported("managed-workload.read-root"));
            }
        }
        check_read_delivery(call, database, generation, groups).await
    }

    async fn open_v1_query(
        &self,
        call: &Call,
        mut request: proto::QueryRequest,
    ) -> Result<OpenedQuery, Status> {
        let source = request
            .source
            .as_ref()
            .ok_or_else(|| error::incorrect("Query requires a Source"))?;
        let layer = proto::Layer::try_from(source.layer)
            .map_err(|_| error::incorrect("Unknown source layer"))?;
        let claim_layer = match layer { proto::Layer::Merged => Layer::Merged, proto::Layer::Local => Layer::Local };
        let selection = source.at.as_ref().and_then(|at| at.selection.as_ref());
        if source.at.is_some() && selection.is_none() {
            return Err(error::incorrect("Empty At selector"));
        }
        if request.exact_read_scope.is_some() && (
            !self.follower_only() || !matches!(selection, Some(Selection::Exactly(_))) ||
            request.scoped_hold.is_some() || request.observe_receiver_clock || request.custody_release_evidence ||
            request.custody_review.is_some() || request.require_clear_rank_input ||
            request.verify_ranked_artefacts || request.ranked_ivf.is_some()
        ) {
            return Err(error::unsupported(super::exact_read::FEATURE));
        }
        let workload = call.session.caller.principal.workload_grant();
        let managed_family = is_managed_family(call);
        if managed_family && (
            !matches!(selection, Some(Selection::Exactly(_))) || !request.track
                || request.custody_review.is_none() || !request.custody_release_evidence
                || request.page.is_some() || request.scoped_hold.is_some() || request.observe_receiver_clock
                || request.observe_applied_position || request.require_clear_rank_input
                || request.verify_ranked_artefacts || request.ranked_ivf.is_some()
        ) {
            return Err(error::unsupported(MANAGED_FEATURE));
        }
        if workload.is_some() && !managed_family && (
            !matches!(selection, Some(Selection::Exactly(_) | Selection::Linearizable(_) | Selection::AtLeast(_)))
                || request.custody_release_evidence
                || request.require_clear_rank_input
                || request.verify_ranked_artefacts
                || request.ranked_ivf.is_some()
        ) {
            return Err(error::unsupported("managed-workload.read-root"));
        }
        if workload.is_some_and(rtdb_auth::VerifiedWorkloadGrant::is_custody_reader)
            && (!matches!(selection, Some(Selection::Exactly(_))) || request.track || request.page.is_some()
                || request.observe_receiver_clock || request.observe_applied_position)
        {
            return Err(error::unsupported("managed-workload.read-custody"));
        }
        if self.follower_only() {
            if matches!(selection, Some(Selection::Linearizable(_) | Selection::AsOfMicros(_))) {
                return Err(error::unsupported("query.follower-source"));
            }
            if request.observe_receiver_clock || request.custody_release_evidence {
                return Err(error::unsupported("query.follower-authority"));
            }
        }
        let family = if managed_family {
            let family = self.managed_family(call).await?;
            if source.branch != family.scope.branch || layer != proto::Layer::Merged {
                return Err(error::unsupported(MANAGED_FEATURE));
            }
            Some(family)
        } else {
            None
        };
        let database = match &family {
            Some(family) => Arc::clone(&family.database),
            None => self.wire_database(call, &source.branch, layer).await?,
        };
        let exact_read = request.exact_read_scope.as_deref()
            .map(|handle| self.exact_read(call, &database, handle)).transpose()?;
        let driver = database.serving_driver();
        let mut visibility = driver.map(|driver| self.serving_scope(call, driver)).transpose()?.flatten();
        let scoped_hold = match request.scoped_hold.as_deref() {
            Some(handle) => {
                let driver = driver.ok_or_else(super::scoped_hold::hold_unavailable)?;
                let scope = visibility.as_ref().ok_or_else(super::scoped_hold::hold_unavailable)?;
                let Some(Selection::Exactly(basis)) = selection else { return Err(super::scoped_hold::hold_unavailable()); };
                let hold = self.read_scoped_hold(call, &database, driver, scope, handle).await?;
                if basis.value.as_slice() != hold.basis.to_bytes() {
                    return Err(super::scoped_hold::hold_unavailable());
                }
                Some(hold)
            }
            None => None,
        };
        if request.custody_review.is_some() && (
            !request.custody_release_evidence || !request.track || request.page.is_some()
                || visibility.is_some() || claim_layer != Layer::Merged
                || !matches!(selection, Some(Selection::Exactly(_)))
                || driver.is_none_or(|driver| !driver.features().contains(&rtdb_tx::transact::ROW_CUSTODY))
        ) {
            return Err(error::unsupported(rtdb_tx::transact::REVIEWED_CUSTODY_PLACEMENT));
        }
        if request.require_clear_rank_input && (
            !request.track || !matches!(selection, Some(Selection::Exactly(_)))
                || request.custody_release_evidence || visibility.is_some()
                || driver.is_none_or(|driver| !driver.supports_query_tokens() || !driver.features().contains(&super::CLEAR_RANK_INPUT))
                || !database.wire_tokens_supported()
        ) {
            return Err(error::unsupported(super::CLEAR_RANK_INPUT));
        }
        if request.verify_ranked_artefacts && (
            !request.require_clear_rank_input || request.page.is_some()
                || driver.is_none_or(|driver| !driver.features().contains(&super::RANKED_ARTEFACT_READINESS))
        ) {
            return Err(error::unsupported(super::RANKED_ARTEFACT_READINESS));
        }
        if request.ranked_ivf.is_some() {
            if !request.require_clear_rank_input
                || source.branch != rtdb_types::rtdb_v1::MAIN_BRANCH || claim_layer != Layer::Merged
                || driver.is_none_or(|driver| !driver.supports_ranked_ivf())
            {
                return Err(error::unsupported(super::RANKED_IVF));
            }
            if !request.sql.is_empty() || request.params_ipc.is_some() || request.page.is_some() || request.verify_ranked_artefacts {
                return Err(error::incorrect("Ranked IVF supplies its own query body and returns a single bounded window"));
            }
        }
        if request.observe_receiver_clock && (
            driver.is_none_or(|driver| !driver.supports_query_tokens())
                || visibility.is_some()
                || !database.wire_tokens_supported()
        ) {
            return Err(error::unsupported(super::RECEIVER_CLOCK));
        }
        if request.observe_applied_position && (
            !call.session.caller.verified_credential || visibility.is_some()
                || call.session.audience_source().is_some() || call.session.fresh_replica
                || driver.is_none_or(|driver| !driver.supports_query_tokens())
                || !database.wire_tokens_supported()
        ) {
            return Err(error::unsupported(super::APPLIED_POSITION));
        }
        if request.custody_release_evidence {
            if visibility.is_some() {
                self.authorize_scoped_custody(call)?;
            } else {
                if !managed_family { self.custody_route(call)?; }
                if driver.is_none_or(|driver| !driver.supports_query_tokens()) || !database.wire_tokens_supported() {
                    return Err(error::unsupported("custody release proof"));
                }
            }
        }
        if let Some(scope) = &mut visibility {
            scope.custody_evidence = request.custody_release_evidence;
            scope.hold = scoped_hold.as_ref().map(|hold| hold.id);
        }
        let authority = if request.track || request.page.is_some() || request.observe_receiver_clock || request.custody_release_evidence && visibility.is_none() {
            if !database.wire_tokens_supported() || driver.is_some_and(|driver| !driver.supports_query_tokens()) {
                return Err(error::unsupported(if request.track { "query.track" } else { "query.page" }));
            }
            Some(database.token_authority().await.map_err(error::anomaly)?)
        } else { None };
        let raw_scope = match driver {
            Some(driver) => uuid::Uuid::from_bytes(driver.incarnation()),
            None => database.wire_incarnation().await.map_err(error::anomaly)?,
        };
        let scope = if let Some(grant) = workload.filter(|_| managed_family) {
            uuid::Uuid::from_bytes(*grant.receipt_namespace().first_chunk::<16>().expect("receipt namespace width"))
        } else {
            visibility.as_ref().map_or(raw_scope, |scope| {
                uuid::Uuid::from_bytes(scope.labels.main_read_incarnation(*raw_scope.as_bytes()))
            })
        };
        let workload_binding = workload.filter(|_| !managed_family).map(|grant| {
            let driver = driver.ok_or_else(|| error::unsupported("managed-workload.read-root"))?;
            let (authority, _, _) = grant.assignment(&call.database, driver.incarnation(), self.0.node.now())
                .map_err(error::anomaly)?;
            authority.assignment_binding().map_err(error::anomaly)?
                .ok_or_else(|| error::unsupported("managed-workload.read-root"))
        }).transpose()?;
        let request_hash = {
            let _encoding = request.ranked_ivf.as_ref().map(|ranked| call.reserve(ranked.encoded_len())).transpose()?;
            let request_hash = fingerprint(&request);
            if let Some((grant, binding)) = workload.zip(workload_binding) {
                let mut hash = Sha256::new();
                hash.update(b"rtdb.v1/workload-root-query\0");
                hash.update(grant.receipt_namespace());
                hash.update(binding);
                hash.update(request_hash);
                hash.finalize().into()
            } else {
                request_hash
            }
        };
        let ranked_ivf = request.ranked_ivf.take().map(read_ranked_ivf).transpose()?;
        let (limit, after) = match &request.page {
            Some(page) if (1..=MAX_PAGE_ROWS).contains(&page.limit) => {
                (u64::from(page.limit), page.after.as_slice())
            }
            Some(_) => {
                return Err(error::incorrect(
                    "Query page limit is outside Capabilities limits",
                ));
            }
            None => (u64::MAX, &[][..]),
        };
        let (continuation, issuer) = match authority {
            Some(authority) if !after.is_empty() => {
                let (page, issuer) = authority.resume_page_in(after, visibility.as_ref().map(|scope| &scope.labels)).await.map_err(error::anomaly)?;
                if page.request != request_hash {
                    return Err(error::incorrect("Continuation belongs to a different query"));
                }
                (Some(page), Some(issuer))
            }
            Some(authority) => (None, Some(authority.issuer().await.map_err(error::anomaly)?)),
            None => (None, None),
        };
        let mut ranked_catalog = None;
        let mut family_region = None;
        let (context, page, cut, scoped_claim, custody_groups, key_generation, custody_contributors, scoped_source_basis, follower_evidence) = if let Some(read) = &exact_read {
            let (source, _, offset) = applied_source(source, claim_layer, selection, continuation, read.time)?;
            let At::Exactly(basis) = source.at else {
                return Err(error::unsupported(super::exact_read::FEATURE));
            };
            let context = read.page_context(basis, request.track, &call.cancellation)?;
            let page = PageToken { basis, clock: read.time, offset, request: request_hash };
            let progress = request.observe_applied_position.then(|| read.progress.clone());
            (context, page, read.time, None, BTreeSet::new(), Some(read.generation), None, None, progress)
        } else if let Some(driver) = driver {
            let now = self.0.node.now();
            let exact = continuation.is_none() && matches!(selection, Some(Selection::Exactly(_)));
            let (query_source, clock, offset) = applied_source(source, claim_layer, selection, continuation, now)?;
            let (view, follower_evidence) = if let Some(family) = &family {
                let selection = request.custody_review.as_ref().ok_or_else(|| error::unsupported(MANAGED_FEATURE))?;
                let (view, region) = self.managed_family_review(call, family, query_source, selection).await?;
                family_region = Some(region);
                (view, None)
            } else if let Some(ranked) = &ranked_ivf {
                let (view, catalog, progress) = self.ranked_view(call, driver, query_source, &ranked.target, now, request.observe_applied_position).await?;
                ranked_catalog = Some(catalog);
                (view, progress)
            } else {
                self.query_view(call, driver, query_source, visibility.as_ref(), &request, now).await?
            };
            let cut = view.cut.system_time;
            let mounted = ranked_catalog.as_ref().and_then(ranked_ivf::Catalog::mounted_storage);
            let (context, clock) = if let Some(storage) = mounted {
                let snapshot = rtdb_query::DatabaseSnapshot { partitions: vec![view.snapshot], storage: Arc::clone(storage) };
                let context = self.scoped_wire_context(call, snapshot, cut)?
                    .with_wire_cut(cut, cut.into()).map_err(error::anomaly)?;
                (context, cut)
            } else if visibility.is_some() {
                let clock = scoped_hold.as_ref().map_or_else(|| continuation.map_or(now, |page| page.clock), |hold| hold.valid_at);
                let snapshot = rtdb_query::DatabaseSnapshot { partitions: vec![view.snapshot], storage: Arc::clone(database.storage()) };
                let context = self.scoped_wire_context(call, snapshot, clock)?
                    .with_wire_cut(cut, clock.into()).map_err(error::anomaly)?;
                (context, clock)
            } else {
                let clock = if exact { cut } else { clock };
                let context = if self.follower_only() || workload.is_some() {
                    let snapshot = rtdb_query::DatabaseSnapshot { partitions: vec![view.snapshot], storage: Arc::clone(database.storage()) };
                    self.scoped_wire_context(call, snapshot, clock)?.with_wire_cut(cut, clock.into())
                } else {
                    self.wire_context(call, &database, false).await?.with_wire_snapshot(view.snapshot, cut, clock.into())
                }.map_err(error::anomaly)?;
                (context, clock)
            };
            let page = PageToken { basis: view.cut.basis, clock, offset, request: request_hash };
            (context, page, cut, view.claim, view.custody_groups, view.key_generation, view.custody_contributors, view.scoped_source_basis, follower_evidence)
        } else {
            let minimum = match (continuation, selection) {
                (Some(page), _) => Some(tokens::read_basis(scope, &page.basis.into())?),
                (None, Some(Selection::AtLeast(basis))) => Some(tokens::read_basis(scope, basis)?),
                _ => None,
            };
            if let Some(minimum) = minimum.filter(|minimum| *minimum != Timestamp::MIN) {
                self.0.node.wire_await_time(
                    &call.database, call.session.caller.principal.as_ref(), minimum, MAX_WAIT, &call.cancellation,
                ).await.map_err(error::anomaly)?;
            }
            let context = self
                .wire_context(
                    call,
                    &database,
                    continuation.is_none() && matches!(selection, Some(Selection::Linearizable(_))),
                )
                .await?;
            let present = head(&context, &call.database)?;
            let page = match continuation {
                Some(page) => page,
                None => {
                    let now = self.0.node.now();
                    let (cut, clock) = match selection {
                        Some(Selection::AsOfMicros(time)) => {
                            (present.min(Timestamp(*time)), Timestamp(*time))
                        }
                        Some(Selection::Exactly(basis)) => {
                            let cut = tokens::read_basis(scope, basis)?;
                            (cut, cut)
                        }
                        _ => (present, now),
                    };
                    PageToken {
                        basis: tokens::basis(scope, cut),
                        clock,
                        offset: 0,
                        request: request_hash,
                    }
                }
            };
            let cut = tokens::read_basis(scope, &page.basis.into())?;
            if cut > present {
                return Err(error::unavailable(
                    "Database has not applied the requested basis",
                ));
            }
            let context = context
                .with_wire_cut(cut, page.clock.into())
                .map_err(error::anomaly)?;
            (context, page, cut, None, BTreeSet::new(), None, None, None, None)
        };
        if request.require_clear_rank_input && (
            !custody_groups.is_empty() || !custody_contributors.as_ref().is_some_and(BTreeSet::is_empty)
        ) {
            return Err(error::unsupported(super::CLEAR_RANK_INPUT));
        }
        let (cursor, footprint, ranked_result) = if let Some(ranked) = ranked_ivf {
            let generation = ranked.target.generation;
            let (cursor, count, approximate) = ranked_catalog.ok_or_else(|| error::unsupported(super::RANKED_IVF))?
                .execute(context, ranked).await.map_err(error::anomaly)?;
            let result = proto::RankedIvfResult { generation, approximate, candidate_count: count as u32 };
            (cursor, ClaimFootprint::default(), Some(result))
        } else {
            let parameters = request.params_ipc.map(|bytes| {
                Parameters::read(Buffer::from(call.retain_request_bytes(bytes)), &context.memory)
            }).transpose()?;
            let types = parameters.as_ref().map_or(&[][..], |params| params.types.as_slice());
            let prepared = self.0.v1.plans.prepare(scope, &request.sql, &context, types)?;
            let (cursor, footprint) = open_query(prepared, context, parameters.as_ref(), request.track).await?;
            (cursor, footprint, None)
        };
        let claim = if visibility.is_some() && request.track {
            let claim = scoped_claim.ok_or_else(|| error::unsupported("query.track"))?;
            Some(proto::Claim { value: claim.into_bytes() })
        } else if request.track {
            let issuer = issuer.as_ref().ok_or_else(|| error::unsupported("query.track"))?;
            let evidence = ClaimEvidence {
                scope: ClaimScope { branch: source.branch.clone(), basis: page.basis, layer: claim_layer },
                footprint,
            };
            let claim = if let Some(family) = &family {
                let region = family_region.as_ref().ok_or_else(|| error::unsupported(MANAGED_FEATURE))?;
                if !evidence.footprint.regions().is_some_and(|regions| regions.len() == 1 && regions.contains(region)) {
                    return Err(error::unsupported(MANAGED_FEATURE));
                }
                family.seal_review_claim(call, issuer, &evidence, request_hash).await?
            } else if let Some(binding) = workload_binding {
                issuer.seal_workload_claim(&evidence, binding).await.map_err(error::anomaly)?
            } else {
                issuer.seal_claim(&evidence).await.map_err(error::anomaly)?
            };
            let value = claim.into_bytes();
            Some(proto::Claim { value })
        } else { None };
        let receiver_clock = if request.observe_receiver_clock && continuation.is_none() {
            let issuer = issuer.as_ref().ok_or_else(|| error::unsupported(super::RECEIVER_CLOCK))?;
            let observed_at = self.0.node.receiver_time().await.map_err(error::anomaly)?;
            let proof = issuer.seal_receiver_clock(ReceiverClockObservation {
                basis: page.basis,
                system_time: cut,
                observed_at,
                request: request_hash,
                session: Sha256::digest(call.session.id.as_bytes()).into(),
            }).await.map_err(error::anomaly)?;
            Some(proto::ReceiverClockObservation { observed_at_micros: observed_at.0, proof })
        } else { None };
        let (custody_release_proof, custody_release_inventory) = if request.custody_release_evidence {
            let contributors = custody_contributors.ok_or_else(|| error::unsupported("complete custody release inventory"))?;
            if visibility.is_some() {
                let source = scoped_source_basis.ok_or_else(|| error::unsupported("complete custody release inventory"))?;
                let proof = self.scoped_custody_read(call, &database, page.basis, source, key_generation, contributors).await?;
                (Some(proof), None)
            } else {
                let issuer = issuer.as_ref().ok_or_else(|| error::unsupported("custody release proof"))?;
                let review = request.custody_review.as_ref().map(|review| (source.branch.as_str(), review));
                let (proof, inventory) = self.custody_read_proof(call, &database, issuer, page.basis, key_generation,
                    contributors, review, claim.as_ref()).await?;
                (Some(proof), Some(inventory))
            }
        } else { (None, None) };
        let (applied_position, follower_progress) = match follower_evidence {
            Some((position, progress)) => (Some(position), Some(progress)),
            None => (request.observe_applied_position.then(|| page.basis.position()), None),
        };
        let trailer = proto::QueryTrailer {
            basis: Some(page.basis.into()),
            system_time_micros: cut.0,
            claim,
            next: None,
            receiver_clock,
            custody_release_proof,
            custody_release_inventory,
            applied_position,
            follower_progress,
            ranked_ivf: ranked_result,
        };
        Ok(OpenedQuery { cursor, database, issuer, labels: visibility.map(|scope| scope.labels), page, limit, trailer, custody_groups, key_generation, scoped_hold_until: scoped_hold.map(|hold| hold.expires_at), exact_read })
    }
}

fn read_ranked_ivf(ranked: proto::RankedIvf) -> Result<ranked_ivf::Request, Status> {
    let request = ranked_ivf::Request {
        target: ranked_ivf::Target { table: TableRef::new(ranked.schema, ranked.table), index: ranked.index, generation: ranked.generation },
        valid: Timestamp(ranked.valid_at_micros.ok_or_else(|| error::incorrect("Ranked IVF requires a valid instant"))?),
        k: ranked.k as usize,
        probe: ranked.probe,
        filters: ranked.filters.into_iter().map(|filter| (filter.column, filter.values)).collect(),
        staged: ranked.staged.into_iter().map(|staged| ranked_ivf::Staged {
            identity_key: staged.identity_key,
            replacement: staged.replacement.map(|replacement| ranked_ivf::Replacement {
                embedding: replacement.embedding,
                filter_values: replacement.filter_values.into_iter().map(|cell| cell.value).collect(),
            }),
        }).collect(),
    };
    request.validate().map_err(error::anomaly)?;
    Ok(request)
}

pub(super) async fn check_read_delivery(call: &Call, database: &Database, generation: Option<u64>, groups: &BTreeSet<CustodyGroupId>) -> Result<(), Status> {
    if generation.is_none() && groups.is_empty() { return Ok(()); }
    let driver = database.serving_driver().ok_or_else(|| error::unsupported("read delivery authority"))?;
    driver.check_read_delivery(generation, groups, &call.cancellation).await.map_err(error::anomaly)
}

fn applied_source(
    source: &proto::Source,
    layer: Layer,
    selection: Option<&Selection>,
    continuation: Option<PageToken>,
    now: Timestamp,
) -> Result<(QuerySource, Timestamp, u64), Status> {
    let read_basis = |basis: &proto::Basis| Basis::from_bytes(&basis.value).map_err(error::anomaly);
    let (at, clock, offset) = match continuation {
        Some(page) => (At::Exactly(page.basis), page.clock, page.offset),
        None => match selection {
            None | Some(Selection::Latest(_)) => (At::Latest, now, 0),
            Some(Selection::AtLeast(basis)) => (At::AtLeast(read_basis(basis)?), now, 0),
            Some(Selection::Exactly(basis)) => (At::Exactly(read_basis(basis)?), now, 0),
            Some(Selection::Linearizable(_)) => (At::Linearizable, now, 0),
            Some(Selection::AsOfMicros(time)) => (At::AsOf(Timestamp(*time)), Timestamp(*time), 0),
        },
    };
    Ok((
        QuerySource {
            branch: source.branch.clone(),
            at,
            layer,
        },
        clock,
        offset,
    ))
}

struct Parameters {
    batch: OwnedBatch,
    types: wire::ParameterTypes,
}

impl Parameters {
    fn read(bytes: Buffer, memory: &Arc<MemoryBudget>) -> Result<Self, Status> {
        let stream =
            rtdb_format::ipc::OwnedStream::read_buffer(bytes, memory).map_err(error::anomaly)?;
        let [batch] = stream.pages() else {
            return Err(error::incorrect(
                "Parameters require exactly one Arrow batch",
            ));
        };
        if batch.batch().num_rows() != 1 {
            return Err(error::incorrect("Parameters require exactly one row"));
        }
        let types =
            wire::ParameterTypes::from_batch(batch.batch(), memory).map_err(error::map_status)?;
        Ok(Self {
            batch: batch.clone(),
            types,
        })
    }

    fn validate_columns(&self, memory: &Arc<MemoryBudget>) -> Result<(), Status> {
        wire::validate_argument_columns(self.batch.batch(), self.types.as_slice(), memory)
            .map_err(error::map_status)
    }

    fn binds_arrow_without_row_admission(&self) -> bool {
        let batch = self.batch.batch();
        !batch.columns().is_empty()
            && batch
                .schema_ref()
                .fields()
                .iter()
                .zip(batch.columns())
                .all(|(field, column)| {
                    field.metadata().is_empty()
                        && matches!(
                            field.data_type(),
                            DataType::Boolean
                                | DataType::Int8
                                | DataType::Int16
                                | DataType::Int32
                                | DataType::Int64
                                | DataType::Float32
                                | DataType::Float64
                        )
                        && column.is_valid(0)
                })
    }

    fn can_bind_arrow(&self) -> bool {
        let batch = self.batch.batch();
        batch.num_columns() != 0
            && batch
                .schema_ref()
                .fields()
                .iter()
                .zip(batch.columns())
                .zip(self.types.as_slice())
                .all(|((field, array), logical)| match field.data_type() {
                    DataType::Union(..) | DataType::RunEndEncoded(..) => false,
                    DataType::List(child) => {
                        plain_float_list(field, child)
                            && array.is_valid(0)
                            && array.as_list::<i32>().value_length(0) > 0
                    }
                    DataType::FixedSizeList(child, width) => {
                        plain_float_list(field, child) && array.is_valid(0) && *width > 0
                    }
                    _ => match logical {
                        LogicalType::Scalar(_) | LogicalType::Null => true,
                        LogicalType::Maybe(inner) => {
                            matches!(inner.as_ref(), LogicalType::Scalar(_))
                        }
                        _ => false,
                    },
                })
    }

    fn reserve_scalar_row_bound(
        &self,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Arc<MemoryReservation>, Status> {
        let batch = self.batch.batch();
        let base = wire::add(
            1024,
            wire::mul(batch.num_columns(), 3 * size_of::<Value>())?,
        )?;
        let bound = batch
            .schema_ref()
            .fields()
            .iter()
            .zip(batch.columns())
            .try_fold(base, |bytes, (field, array)| {
                let read = rtdb_columnar::estimate_read_value_bytes(field, array.as_ref(), 0)
                    .map_err(error::anomaly)?;
                let copy = rtdb_columnar::estimate_read_cell_bytes(field, array.as_ref(), 0)
                    .map_err(error::anomaly)?;
                wire::add(bytes, wire::add(read, copy)?)
            })?;
        memory.reserve(bound).map_err(error::anomaly)
    }

    async fn try_open_arrow(
        &self,
        prepared: &PreparedSql,
        context: &Arc<QueryContext>,
    ) -> Result<Option<QueryCursor>, Status> {
        let types = self.types.as_slice();
        let skip_row_admission = self.binds_arrow_without_row_admission();
        // Use the scalar path to report its original error if its eager row admission may fail.
        let _scalar_row = if skip_row_admission {
            None
        } else {
            if prepared.parameter_count() != types.len() || !self.can_bind_arrow() {
                return Ok(None);
            }
            let Ok(row) = self.reserve_scalar_row_bound(&context.memory) else {
                return Ok(None);
            };
            Some(row)
        };
        let result = if skip_row_admission {
            prepared
                .try_open_query_arrow(Arc::clone(context), &self.batch, types)
                .await
        } else {
            let batch = self.batch.batch();
            let signed = if batch
                .schema_ref()
                .fields()
                .iter()
                .any(|field| field.data_type().is_unsigned_integer())
            {
                let Ok(metadata) = wire::schema_bound(batch.schema_ref())
                    .and_then(|bytes| context.memory.reserve(bytes).map_err(error::anomaly))
                else {
                    return Ok(None);
                };
                let Some(signed) = self.signed_batch() else {
                    return Ok(None);
                };
                Some(signed.with_schema_reservation(metadata))
            } else {
                None
            };
            // The prepared plan keeps the declared types. Where it has no declaration, the
            // cursor falls back to the same observed type it would get from a decoded value.
            let observed = types
                .iter()
                .zip(batch.columns())
                .map(|(logical, array)| {
                    if array.is_null(0) {
                        LogicalType::Null
                    } else {
                        match logical {
                            LogicalType::Maybe(inner) => rtdb_columnar::execution_type(inner),
                            other => rtdb_columnar::execution_type(other),
                        }
                    }
                })
                .collect::<Vec<_>>();
            prepared
                .try_open_query_arrow(
                    Arc::clone(context),
                    signed.as_ref().unwrap_or(&self.batch),
                    &observed,
                )
                .await
        };
        match result {
            Err(error)
                if error.code == "rtdb.query/column-plan"
                    || (!skip_row_admission && error.code == "rtdb/memory-budget-exceeded") =>
            {
                Ok(None)
            }
            result => result.map_err(error::anomaly),
        }
    }

    fn signed_batch(&self) -> Option<OwnedBatch> {
        let batch = self.batch.batch();
        let mut fields = batch.schema_ref().fields().to_vec();
        let mut arrays = batch.columns().to_vec();
        for (field, array) in fields.iter_mut().zip(&mut arrays) {
            let signed: ArrayRef = match field.data_type() {
                DataType::UInt8 => signed_view::<UInt8Type, Int8Type>(array),
                DataType::UInt16 => signed_view::<UInt16Type, Int16Type>(array),
                DataType::UInt32 => signed_view::<UInt32Type, Int32Type>(array),
                DataType::UInt64 => signed_view::<UInt64Type, Int64Type>(array),
                _ => continue,
            };
            *field = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(signed.data_type().clone()),
            );
            *array = signed;
        }
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema_ref().metadata().clone(),
        ));
        let signed = RecordBatch::try_new(schema, arrays).ok()?;
        Some(self.batch.with_retained_buffers(signed))
    }
}

fn signed_view<Unsigned: ArrowPrimitiveType, Signed: ArrowPrimitiveType>(
    array: &ArrayRef,
) -> ArrayRef {
    let array = array.as_primitive::<Unsigned>();
    let values = ScalarBuffer::from(array.values().inner().clone());
    Arc::new(arrow_array::PrimitiveArray::<Signed>::new(
        values,
        array.nulls().cloned(),
    ))
}

fn plain_float_list(field: &Field, child: &Field) -> bool {
    field.metadata().is_empty()
        && child.metadata().is_empty()
        && child.name() == "$data$"
        && !child.is_nullable()
        && matches!(child.data_type(), DataType::Float32 | DataType::Float64)
}

async fn open_query(
    prepared: PreparedSql,
    context: Arc<QueryContext>,
    parameters: Option<&Parameters>,
    track: bool,
) -> Result<(QueryCursor, ClaimFootprint), Status> {
    let (row, types, _row_charge) = if let Some(parameters) = parameters {
        parameters.validate_columns(&context.memory)?;
        if !track && let Some(cursor) = parameters.try_open_arrow(&prepared, &context).await? {
            return Ok((cursor, ClaimFootprint::default()));
        }
        let types = parameters.types.as_slice();
        let (row, charge) = wire::first_query_row(parameters.batch.batch(), types, &context.memory)
            .map_err(error::map_status)?;
        (row, types, Some(charge))
    } else {
        (Vec::new(), &[][..], None)
    };
    if track {
        prepared.open_tracked_query_columns(context, row, types).await.map_err(error::anomaly)
    } else {
        let cursor = prepared.open_query_columns_typed(context, row, types).await.map_err(error::anomaly)?;
        Ok((cursor, ClaimFootprint::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array,
        ListArray, NullArray, StringArray, UInt64Array,
    };
    use arrow_buffer::OffsetBuffer;

    #[test]
    fn plain_parameter_validation_needs_no_additional_schema_budget() {
        let memory = MemoryBudget::new(1 << 20);
        let batch =
            RecordBatch::try_from_iter([("p", Arc::new(Int64Array::from(vec![42])) as ArrayRef)])
                .unwrap();
        let ipc = rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
            .unwrap();
        let parameters = Parameters::read(Buffer::from_vec(ipc), &memory).unwrap();
        let _remaining = memory.reserve(memory.available_bytes()).unwrap();

        parameters.validate_columns(&memory).unwrap();
    }

    #[test]
    fn arrow_binding_accepts_scalar_and_type_preserving_float_list_profiles() {
        let memory = MemoryBudget::new(1 << 20);
        let signed = Arc::new(Int64Array::from(vec![Some(42)])) as ArrayRef;
        let item = Arc::new(Field::new("$data$", DataType::Float32, false));
        let list = |child, components: Vec<f32>| -> ArrayRef {
            let end = i32::try_from(components.len()).unwrap();
            Arc::new(ListArray::new(
                child,
                OffsetBuffer::new(ScalarBuffer::from(vec![0, end])),
                Arc::new(Float32Array::from(components)),
                None,
            ))
        };
        let fixed = Arc::new(FixedSizeListArray::new(
            Arc::clone(&item),
            2,
            Arc::new(Float32Array::from(vec![-0.0, 1.0])),
            None,
        )) as ArrayRef;
        for (other, annotated, expected) in [
            (
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(Float32Array::from(vec![-0.0])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
                true,
                true,
            ),
            (
                Arc::new(Float32Array::from(vec![None])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(Int64Array::from(vec![None])) as ArrayRef,
                false,
                true,
            ),
            (Arc::new(NullArray::new(1)) as ArrayRef, false, true),
            (
                Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(StringArray::from(vec!["text"])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(BinaryArray::from(vec![&b"\0\xff"[..]])) as ArrayRef,
                false,
                true,
            ),
            (list(Arc::clone(&item), vec![-0.0, 1.0]), false, true),
            (fixed, false, true),
            (list(Arc::clone(&item), vec![]), false, false),
            (
                list(
                    Arc::new(item.as_ref().clone().with_nullable(true)),
                    vec![1.0],
                ),
                false,
                false,
            ),
            (list(item, vec![1.0]), true, false),
        ] {
            let mut field = Field::new("b", other.data_type().clone(), true);
            if annotated {
                field = field.with_metadata([("tag".into(), "parameter".into())].into());
            }
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                field,
            ]));
            let batch = RecordBatch::try_new(schema, vec![Arc::clone(&signed), other]).unwrap();
            let ipc =
                rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
                    .unwrap();
            let parameters = Parameters::read(Buffer::from_vec(ipc), &memory).unwrap();
            assert_eq!(parameters.can_bind_arrow(), expected);
        }
    }

    #[test]
    fn unsigned_views_keep_the_ipc_values_and_validity() {
        let memory = MemoryBudget::new(1 << 20);
        for value in [Some(u64::MAX), None] {
            let source = Arc::new(UInt64Array::from(vec![value])) as ArrayRef;
            let batch = RecordBatch::try_from_iter([("u", source)]).unwrap();
            let bytes =
                rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
                    .unwrap();
            let parameters = Parameters::read(Buffer::from_vec(bytes), &memory).unwrap();
            let signed = parameters.signed_batch().unwrap();
            let source = parameters
                .batch
                .batch()
                .column(0)
                .as_primitive::<UInt64Type>();
            let array = signed.batch().column(0).as_primitive::<Int64Type>();
            assert_eq!(
                source.values().as_ptr().cast::<i64>(),
                array.values().as_ptr()
            );
            assert_eq!(array.iter().next(), Some(value.map(|value| value as i64)));
            assert_eq!(
                parameters.batch.batch().schema_ref().field(0).data_type(),
                &DataType::UInt64
            );
        }
    }

    #[test]
    fn direct_binding_checks_eager_payload_bounds_without_retaining_row_memory() {
        let memory = MemoryBudget::new(1 << 22);
        let text = "x".repeat(1 << 14);
        let batch = RecordBatch::try_from_iter([(
            "p",
            Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef,
        )])
        .unwrap();
        let ipc = rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
            .unwrap();
        let parameters = Parameters::read(Buffer::from_vec(ipc), &memory).unwrap();
        let small = MemoryBudget::new(1 << 14);
        assert!(parameters.reserve_scalar_row_bound(&small).is_err());
        assert_eq!(small.used_bytes(), 0);
        let before = memory.used_bytes();
        let reserved = parameters.reserve_scalar_row_bound(&memory).unwrap();
        assert!(reserved.bytes() > text.len());
        drop(reserved);
        assert_eq!(memory.used_bytes(), before);
    }

    #[test]
    fn parameter_values_keep_their_input_allocation_and_both_charges() {
        let batch =
            RecordBatch::try_from_iter([("p", Arc::new(Int64Array::from(vec![42])) as ArrayRef)])
                .unwrap();
        let bytes = rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
            .unwrap();
        let start = bytes.as_ptr() as usize;
        let input = start..start + bytes.len();
        let input_memory = MemoryBudget::new(1 << 20);
        let decode_memory = MemoryBudget::new(1 << 20);
        let bytes = wire::retain(bytes, input_memory.reserve(4096).unwrap());
        let parameters = Parameters::read(Buffer::from(bytes), &decode_memory).unwrap();
        let array = Arc::clone(parameters.batch.batch().column(0));
        drop(parameters);

        let values = array.as_any().downcast_ref::<Int64Array>().unwrap().values();
        assert!(input.contains(&(values.as_ptr() as usize)));
        assert_eq!(values.as_ref(), &[42]);
        assert_eq!(input_memory.used_bytes(), 4096);
        assert!(decode_memory.used_bytes() > 0);
        drop(array);
        assert_eq!(input_memory.used_bytes(), 0);
        assert_eq!(decode_memory.used_bytes(), 0);
    }
}
