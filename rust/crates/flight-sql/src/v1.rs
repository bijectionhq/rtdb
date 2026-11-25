use std::{sync::Arc, time::Duration};

use arrow_flight::{self as flight, FlightData, sql::Any};
use bytes::Bytes;
use prost::{Message, Name};
use rtdb_node::serving::{FundingSubmission, ROW_VISIBILITY, ServingDriver, ServingScope};
use rtdb_node::{Database, DatabaseAccess, SessionIdentity};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
use rtdb_tx::transact::{DECISION_CLOCK_OFFER, FundingCommand, PHYSICAL_ERASURE, REVIEWED_CUSTODY_PLACEMENT, ROW_CUSTODY, TIMED_TRANSACT, TRANSACT_REQUIRES};
use rtdb_types::{Timestamp, rtdb_v1::{CLEAR_RANK_INPUT, RANKED_ARTEFACT_READINESS, RANKED_IVF}};
use tonic::{Response, Status, metadata::MetadataMap};

use crate::{
    rtdb::v1 as proto,
    service::{Call, Output, Service, result_stream},
};

mod changes;
pub(crate) mod custody;
pub(crate) mod original_family_landing;
pub(crate) mod error;
mod query;
mod scoped_hold;
mod tokens;
mod appointment;
mod transact;
mod workload;

const API: &str = "rtdb.v1";
const RECEIVER_CLOCK: &str = "query.receiver-clock";
const APPLIED_POSITION: &str = "query.applied-position";
const FOLLOWER_PROGRESS: &str = "query.follower-progress";
pub const OWNED_HOSTED_FEATURE: &str = "owned.hosted";
pub const RETIRED_ORIGINAL_FAMILY_FEATURE: &str = "owned.retired-original-family";
pub const RETIRED_ORIGINAL_FAMILY_RECOVERY_FEATURE: &str = "owned.retired-original-family-recovery";
const MAX_PAGE_ROWS: u32 = 4096;
const MAX_CHANGE_GROUPS: u32 = 256;
const MAX_WAIT: Duration = Duration::from_secs(30);
pub(crate) const CAPABILITIES: &str = "rtdb.v1.Capabilities";
pub(crate) const MANAGED_CREATE: &str = "rtdb.admin.v1.CreateDeployment";
pub(crate) const MANAGED_WORKLOAD: &str = "rtdb.v1.ManagedWorkload";
pub(crate) const TX_STATUS: &str = "rtdb.v1.TxStatus";
pub(crate) const OFFER_DECISION_CLOCK: &str = "rtdb.v1.OfferDecisionClock";
pub(crate) const ACTIONS: [&str; 7] = [
    CAPABILITIES,
    "rtdb.v1.Transact",
    TX_STATUS,
    OFFER_DECISION_CLOCK,
    appointment::PREPARE,
    appointment::ACTIVATE,
    MANAGED_WORKLOAD,
];

#[derive(Default)]
pub(crate) struct State {
    plans: query::PlanCache,
    scoped_reads: custody::ScopedReads,
}

pub(crate) enum Ticket {
    Query(Bytes),
    Changes(Bytes),
}

pub(crate) fn ticket(bytes: &Bytes) -> Option<Ticket> {
    let envelope = Any::decode(bytes.clone()).ok()?;
    match envelope.type_url.rsplit_once('/')?.1 {
        "rtdb.v1.QueryRequest" => Some(Ticket::Query(envelope.value)),
        "rtdb.v1.ChangesRequest" => Some(Ticket::Changes(envelope.value)),
        _ => None,
    }
}

pub(crate) fn is_action(name: &str) -> bool {
    ACTIONS.contains(&name) || name == custody::RELEASE || name == scoped_hold::ACTION || name == original_family_landing::ACTION || name.starts_with("rtdb.admin.v1.")
}

fn decode<M: Message + Default + Name>(bytes: Bytes) -> Result<M, Status> {
    M::decode(bytes).map_err(|_| error::incorrect("Invalid protobuf request"))
}

fn funding_owner(call: &Call) -> Option<[u8; 32]> {
    use sha2::{Digest, Sha256};

    let caller = &call.session.caller;
    if !caller.verified_credential { return None; }
    if let Some(scope) = caller.principal.label_scope() {
        return Some(scope.receipt_namespace());
    }
    let mut digest = Sha256::new();
    digest.update(b"rtdb.v1/decision-clock-owner\0");
    digest.update(caller.principal.user_id()?.as_bytes());
    Some(digest.finalize().into())
}

impl Service {
    pub(crate) async fn v1_action(
        &self,
        call: Call,
        action: flight::Action,
    ) -> Result<Response<Output<flight::Result>>, Status> {
        let outcome = match action.r#type.as_str() {
            CAPABILITIES if original_family_landing::is_managed_family(&call) => async {
                decode::<proto::CapabilitiesRequest>(action.body)?;
                self.managed_family(&call).await?;
                Ok(proto::CapabilitiesResponse {
                    api: API.into(),
                    features: [
                        original_family_landing::FEATURE, original_family_landing::MANAGED_FEATURE,
                        ROW_CUSTODY, REVIEWED_CUSTODY_PLACEMENT, "query", "query.track",
                    ].map(str::to_owned).into(),
                    limits: Some(proto::Limits {
                        max_page_rows: 0, max_change_groups: 0, max_wait_millis: 0,
                        min_key_bytes: 16, max_key_bytes: 64,
                    }),
                    server_time_micros: self.0.node.now().0,
                }.encode_to_vec())
            }.await,
            CAPABILITIES if call.session.caller.principal.workload_grant().is_some() => async {
                decode::<proto::CapabilitiesRequest>(action.body)?;
                let database = self.require_workload_root_read(&call).await?;
                let driver = database.serving_driver().ok_or_else(|| error::unsupported("managed-workload.read-root"))?;
                let custody_reader = call.session.caller.principal.workload_grant()
                    .is_some_and(rtdb_auth::VerifiedWorkloadGrant::is_custody_reader);
                let mut features = if custody_reader {
                    vec!["managed-workload.read-custody", "query", rtdb_tx::transact::ROW_CUSTODY]
                } else {
                    vec!["managed-workload.read-root", "query", "query.linearizable", "changes", changes::PROGRESS_ONLY]
                };
                if !custody_reader && driver.supports_query_tokens() && database.wire_tokens_supported() {
                    features.extend(["query.track", "query.page", RECEIVER_CLOCK, APPLIED_POSITION, changes::APPLIED_POSITIONS]);
                }
                if !custody_reader && driver.features().contains(&rtdb_types::rtdb_v1::QUERY_WRITTEN_BASIS) {
                    features.push(rtdb_types::rtdb_v1::QUERY_WRITTEN_BASIS);
                }
                Ok(proto::CapabilitiesResponse {
                    api: API.into(),
                    features: features.into_iter().map(str::to_owned).collect(),
                    limits: Some(proto::Limits {
                        max_page_rows: MAX_PAGE_ROWS,
                        max_change_groups: if custody_reader { 0 } else { MAX_CHANGE_GROUPS },
                        max_wait_millis: MAX_WAIT.as_millis() as u64,
                        min_key_bytes: 16,
                        max_key_bytes: 64,
                    }),
                    server_time_micros: self.0.node.now().0,
                }.encode_to_vec())
            }.await,
            CAPABILITIES => async {
                decode::<proto::CapabilitiesRequest>(action.body)?;
                let mut features = Vec::new();
                if !self.follower_only() {
                    if self.supports_hosted_upload(&call) && self.0.config.owned_world_host.is_some() {
                        features.push(OWNED_HOSTED_FEATURE.into());
                    }
                    if self.supports_retired_original_family(&call) {
                        features.push(RETIRED_ORIGINAL_FAMILY_FEATURE.into());
                        if self.supports_hosted_upload(&call) {
                            features.push(RETIRED_ORIGINAL_FAMILY_RECOVERY_FEATURE.into());
                        }
                    }
                }
                if self.follower_only() {
                    features.extend(["query", "query.track", "query.page", APPLIED_POSITION, FOLLOWER_PROGRESS].map(str::to_owned));
                    features.extend([CLEAR_RANK_INPUT, RANKED_ARTEFACT_READINESS].map(str::to_owned));
                    #[cfg(feature = "multi")]
                    if self.follower().is_some_and(|route| route.reader.supports_ranked_ivf()) {
                        features.push(RANKED_IVF.into());
                    }
                } else if call.session.audience_source().is_none() && !call.session.fresh_replica {
                    features.extend(
                        [
                            "query",
                            "changes",
                            changes::PROGRESS_ONLY,
                            changes::SHARED_IMAGES,
                            "sql.history",
                            "sql.search",
                            "sql.search-table",
                            "sql.nearest-table",
                            "sql.cosine-distance",
                        ]
                        .map(str::to_owned),
                    );
                    if let Ok(database) = self.0.node.catalog().database(&call.database) {
                        if let Some(driver) = database.serving_driver() {
                            features = driver
                                .features()
                                .iter()
                                .map(|feature| (*feature).to_owned())
                                .collect();
                            if driver.supports_row_visibility() && !features.iter().any(|feature| feature == ROW_VISIBILITY) {
                                features.push(ROW_VISIBILITY.into());
                            }
                            if driver.features().contains(&ROW_CUSTODY) {
                                if database.read_only() || driver.current_custody(&call.cancellation).await.is_err() {
                                    features.retain(|feature| feature != ROW_CUSTODY);
                                } else if self.reviewed_custody_writer(&call, &database).await.is_ok() {
                                    features.push(REVIEWED_CUSTODY_PLACEMENT.into());
                                    if original_family_landing::installed_native_authority(&database)
                                        && self.custody_route(&call).is_ok_and(|route| {
                                            route.family_vault.is_some() && route.family_line_registry.is_some()
                                        })
                                    {
                                        features.push(original_family_landing::FEATURE.into());
                                    }
                                }
                            }
                        } else if database.wire_linearizable_reads() {
                            features.push("query.linearizable".into());
                        }
                        if features.iter().any(|feature| feature == "query") {
                            features.extend(["sql.search-analyzer-stats", "sql.cosine-distance-f32", "sql.ranked-window", "sql.spatial-predicates"]
                                .map(str::to_owned));
                        }
                        if features.iter().any(|feature| feature == "query") && database.wire_tokens_supported()
                            && database.serving_driver().is_none_or(|driver| driver.supports_query_tokens())
                        {
                            for feature in ["query.track", "query.page"] {
                                if !features.iter().any(|existing| existing == feature) {
                                    features.push(feature.into());
                                }
                            }
                            if database.serving_driver().is_some() {
                                features.push(RECEIVER_CLOCK.into());
                                if database.serving_driver().is_some_and(|driver| driver.supports_ranked_ivf()) {
                                    features.push(RANKED_IVF.into());
                                }
                                if call.session.caller.verified_credential {
                                    features.push(APPLIED_POSITION.into());
                                    if features.iter().any(|feature| feature == "changes") {
                                        features.push(changes::APPLIED_POSITIONS.into());
                                    }
                                }
                            }
                        } else {
                            features.retain(|feature| !matches!(feature.as_str(), "query.track" | "query.page" | CLEAR_RANK_INPUT | RANKED_ARTEFACT_READINESS));
                        }
                    }
                }
                let scoped = call.session.caller.principal.label_scope().is_some();
                if scoped {
                    let admitted = self.0.node.catalog().database(&call.database).is_ok_and(|database| {
                        database.serving_driver().is_some_and(|driver| self.serving_scope(&call, driver).is_ok())
                    });
                    features.retain(|feature| admitted && matches!(feature.as_str(), ROW_VISIBILITY | "query" |
                        "query.track" | "query.page" | "query.linearizable" | "changes" | "transact" |
                        TIMED_TRANSACT | TRANSACT_REQUIRES | PHYSICAL_ERASURE | rtdb_types::rtdb_v1::QUERY_WRITTEN_BASIS | "tx-status" | "sql.history"));
                    if admitted && features.iter().any(|feature| feature == "transact") {
                        features.push(scoped_hold::FEATURE.into());
                    }
                    if admitted && self.authorize_scoped_custody(&call).is_ok() {
                        features.push(custody::SCOPED_CUSTODY.into());
                    }
                }
                if funding_owner(&call).is_some() && let Some(clock) = self.0.node.funded_offer_clock()
                    && features.iter().any(|feature| feature == "transact")
                    && (scoped || call.session.caller.principal.authorize_database(&call.database, DatabaseAccess::Write).is_ok())
                    && let Ok(database) = self.0.node.catalog().database(&call.database)
                    && !database.read_only()
                    && let Some(driver) = database.serving_driver()
                    && driver.supports_funded_transact().await
                    && clock.funded_offers_for(driver.incarnation()).await
                {
                    features.push(DECISION_CLOCK_OFFER.into());
                    if appointment::owner(&call).is_some() && driver.supports_funded_appointment().await {
                        features.push(rtdb_tx::transact::FUNDED_APPOINTMENT.into());
                    }
                }
                Ok(proto::CapabilitiesResponse {
                    api: API.into(),
                    features,
                    limits: Some(proto::Limits {
                        max_page_rows: MAX_PAGE_ROWS,
                        max_change_groups: if self.follower_only() { 0 } else { MAX_CHANGE_GROUPS },
                        max_wait_millis: MAX_WAIT.as_millis() as u64,
                        min_key_bytes: 16,
                        max_key_bytes: 64,
                    }),
                    server_time_micros: if scoped { 0 } else { self.0.node.now().0 },
                }
                .encode_to_vec())
            }.await,
            scoped_hold::ACTION => self.scoped_hold_action(&call, &action.body).await,
            custody::RELEASE => self.custody_release_action(&call, &action.body).await,
            original_family_landing::ACTION => self.original_family_landing(&call, &action.body).await,
            OFFER_DECISION_CLOCK => match decode(action.body) {
                Ok(request) => self.v1_offer_decision_clock(&call, request).await,
                Err(status) => Err(status),
            },
            appointment::PREPARE => match decode(action.body) {
                Ok(request) => self.prepare_funded_appointment(&call, request).await,
                Err(status) => Err(status),
            },
            appointment::ACTIVATE => match decode(action.body) {
                Ok(request) => self.activate_funded_appointment(&call, request).await,
                Err(status) => Err(status),
            },
            "rtdb.v1.Transact" => self.v1_transact(&call, action.body).await,
            MANAGED_WORKLOAD => self.managed_workload_action(&call, action.body).await,
            TX_STATUS => match decode(action.body) {
                Ok(request) => self.v1_tx_status(&call, request).await,
                Err(status) => Err(status),
            },
            #[cfg(feature = "multi")]
            name if crate::admin::ACTIONS.contains(&name) => self.admin_action(&call, name, action.body).await,
            _ => Err(error::unsupported("admin")),
        };
        let result = outcome.map_err(|status| call.error(status))?;
        let charge = call
            .reserve(result.len())
            .map_err(|status| call.error(error::map_status(status)))?;
        result_stream(
            call,
            vec![Ok(flight::Result {
                body: crate::wire::retain(result, charge),
            })],
        )
        .map_err(error::map_status)
    }

    async fn v1_transact(&self, call: &Call, source: Bytes) -> Result<Vec<u8>, Status> {
        let mut request: proto::TransactRequest = decode(source)?;
        check_key(&request.key)?;
        if let Some(appointment) = request.funded_appointment.take() {
            if request.decision_clock_offer.is_some() {
                return Err(error::incorrect("Transact may consume one funding instrument"));
            }
            return self.consume_funded_appointment(call, request, appointment).await;
        }
        if let Some(booking) = request.decision_clock_offer.take() {
            let booking = booking.as_slice().try_into().map_err(|_| error::incorrect("Invalid DecisionClock booking"))?;
            let receipt = self.funded_transact(call, request, FundingCommand::Consume { booking }).await?;
            return Ok(self.funded_wire_receipt(call, receipt).encode_to_vec());
        }
        let access = if request.ops.is_empty() && request.otherwise.is_empty() {
            DatabaseAccess::Read
        } else {
            DatabaseAccess::Write
        };
        let database = self.wire_attachment(call, access).await?;
        let driver = database.serving_driver().ok_or_else(|| {
            error::unsupported(request.require.first().map_or("transact", String::as_str))
        })?;
        let mut transaction = transact::request(request, call.memory())?;
        transaction.request_memory = Some(call.request_reservation());
        if transaction.uses_reviewed_custody() {
            if !driver.features().contains(&ROW_CUSTODY) {
                return Err(error::unsupported(REVIEWED_CUSTODY_PLACEMENT));
            }
            self.authenticate_reviewed_custody(call, &database, &transaction).await?;
        }
        let receipt = match self.serving_scope(call, driver)? {
            Some(scope) => driver.transact_scoped(transaction, scope, &call.cancellation).await,
            None => driver.transact(transaction, &call.cancellation).await,
        }.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        Ok(transact::receipt(receipt).encode_to_vec())
    }

    async fn v1_offer_decision_clock(&self, call: &Call, request: proto::OfferDecisionClockRequest) -> Result<Vec<u8>, Status> {
        let transaction = request.transaction.ok_or_else(|| error::incorrect("A DecisionClock offer requires its transaction"))?;
        let predecessor = request.predecessor.ok_or_else(|| error::incorrect("A DecisionClock offer requires its exact predecessor"))?;
        let predecessor = rtdb_types::rtdb_v1::Basis::from_bytes(&predecessor.value).map_err(error::anomaly)?;
        let command = FundingCommand::Book { predecessor, booking: *uuid::Uuid::new_v4().as_bytes() };
        let receipt = self.funded_transact(call, transaction, command).await?;
        let offer = receipt.decision_clock.ok_or_else(|| error::unsupported(DECISION_CLOCK_OFFER))?;
        Ok(transact::decision_clock(offer, call.session.caller.principal.label_scope().is_some()).encode_to_vec())
    }

    async fn funded_transact(
        &self, call: &Call, request: proto::TransactRequest, command: FundingCommand,
    ) -> Result<rtdb_tx::transact::Receipt, Status> {
        let owner = funding_owner(call).ok_or_else(|| error::unsupported(DECISION_CLOCK_OFFER))?;
        let database = self.wire_attachment(call, DatabaseAccess::Write).await?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported(DECISION_CLOCK_OFFER))?;
        let clock = self.0.node.funded_offer_clock().ok_or_else(|| error::unsupported(DECISION_CLOCK_OFFER))?;
        let scope = self.serving_scope(call, driver)?;
        let mut request = transact::request(request, call.memory())?;
        request.check_funded_shape(scope.is_some()).map_err(error::anomaly)?;
        request.request_memory = Some(call.request_reservation());
        let receipt = driver.transact_funded(request, scope, FundingSubmission { command, owner, clock }, &call.cancellation)
            .await.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        Ok(receipt)
    }

    fn funded_wire_receipt(&self, call: &Call, receipt: rtdb_tx::transact::Receipt) -> proto::Receipt {
        let evidence = receipt.decision_clock.filter(|offer| {
            !offer.is_appointment() && funding_owner(call).is_some_and(|owner| offer.belongs_to(owner))
        });
        let mut receipt = transact::receipt(receipt);
        receipt.decision_clock = evidence.map(|offer| transact::decision_clock(
            offer, call.session.caller.principal.label_scope().is_some(),
        ));
        receipt
    }

    async fn v1_tx_status(
        &self,
        call: &Call,
        request: proto::TxStatusRequest,
    ) -> Result<Vec<u8>, Status> {
        check_key(&request.key)?;
        if call.session.caller.principal.workload_grant().is_some() {
            return self.managed_workload_status(call, request.key).await;
        }
        let database = self.wire_attachment(call, DatabaseAccess::Write).await?;
        let driver = database
            .serving_driver()
            .ok_or_else(|| error::unsupported("tx-status"))?;
        let receipt = match self.serving_scope(call, driver)? {
            Some(scope) => driver.tx_status_scoped(&request.key, &scope, &call.cancellation).await,
            None => driver.tx_status(&request.key, &call.cancellation).await,
        }.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        if receipt.decision_clock.is_some_and(rtdb_tx::transact::FundedReceipt::is_appointment) {
            return Ok(appointment::terminal(call, receipt)?.encode_to_vec());
        }
        Ok(self.funded_wire_receipt(call, receipt).encode_to_vec())
    }

    pub(crate) async fn v1_get(
        &self,
        call: Call,
        ticket: Ticket,
        metadata: &MetadataMap,
    ) -> Result<Response<Output<FlightData>>, Status> {
        match ticket {
            Ticket::Query(body) => match decode(body) {
                Ok(request) => self.v1_query(call, request).await,
                Err(status) => Err(call.error(status)),
            },
            Ticket::Changes(body) => match decode(body) {
                Ok(request) => self.v1_changes(call, request, metadata).await,
                Err(status) => Err(call.error(status)),
            },
        }
    }

    async fn wire_database(
        &self,
        call: &Call,
        branch: &str,
        layer: proto::Layer,
    ) -> Result<Arc<Database>, Status> {
        if !rtdb_types::rtdb_v1::valid_branch_name(branch) {
            return Err(error::incorrect("Invalid branch name"));
        }
        #[cfg(feature = "multi")]
        if let Some(follower) = self.follower() {
            if branch != rtdb_types::rtdb_v1::MAIN_BRANCH || layer != proto::Layer::Merged {
                return Err(error::unsupported("query.follower-source"));
            }
            return follower.current(&self.0.node).map_err(error::follower);
        }
        if call.session.caller.principal.workload_grant().is_some() {
            if branch != rtdb_types::rtdb_v1::MAIN_BRANCH || layer != proto::Layer::Merged {
                return Err(error::unsupported("managed-workload.read-root"));
            }
            return self.require_workload_root_read(call).await;
        }
        let database = self.wire_attachment(call, DatabaseAccess::Read).await?;
        if database.serving_driver().is_none() {
            if branch != "main" {
                return Err(error::unsupported("branches"));
            }
            if layer != proto::Layer::Merged {
                return Err(error::unsupported("branch.local-layer"));
            }
        }
        Ok(database)
    }

    async fn wire_attachment(
        &self,
        call: &Call,
        access: DatabaseAccess,
    ) -> Result<Arc<Database>, Status> {
        if self.follower_only() || call.session.audience_source().is_some() || call.session.fresh_replica {
            return Err(error::unsupported("rtdb.v1.route"));
        }
        let scoped = call.session.caller.principal.label_scope().is_some();
        if !scoped {
            call.session.caller.principal.authorize_database(&call.database, access).map_err(error::anomaly)?;
        }
        self.admit_database(&call.database)
            .await
            .map_err(error::map_status)?;
        let database = self.0.node.catalog().database(&call.database).map_err(error::anomaly)?;
        call.require_imported_workload(&database)?;
        if scoped {
            let driver = database.serving_driver().ok_or_else(|| error::unsupported(ROW_VISIBILITY))?;
            self.serving_scope(call, driver)?;
        }
        Ok(database)
    }

    fn serving_scope(&self, call: &Call, driver: &dyn ServingDriver) -> Result<Option<ServingScope>, Status> {
        let Some(scope) = call.session.caller.principal.label_scope() else { return Ok(None); };
        scope.admit(&call.database, driver.incarnation()).map_err(error::anomaly)?;
        scope.revalidate(self.0.node.now()).map_err(error::anomaly)?;
        if !driver.supports_row_visibility() { return Err(error::unsupported(ROW_VISIBILITY)); }
        Ok(Some(ServingScope { labels: scope.labels().clone(), receipt_namespace: scope.receipt_namespace(), custody_evidence: false, hold: None }))
    }

    fn scoped_wire_context(&self, call: &Call, snapshot: DatabaseSnapshot, clock: Timestamp) -> Result<Arc<QueryContext>, Status> {
        let name = call.database.clone();
        let catalog = CatalogSnapshot::new(name.clone(), std::collections::BTreeMap::from([(name, snapshot)]), false)
            .map_err(error::anomaly)?;
        QueryContext::new(catalog, Arc::clone(call.memory()), clock, "UTC", QueryOptions::default(), &call.cancellation)
            .map_err(error::anomaly)
    }

    async fn wire_context(
        &self,
        call: &Call,
        database: &Arc<Database>,
        linearizable: bool,
    ) -> Result<Arc<QueryContext>, Status> {
        if call.session.caller.principal.label_scope().is_some() { return Err(error::unsupported(ROW_VISIBILITY)); }
        if linearizable && !database.wire_linearizable_reads() {
            return Err(error::unsupported("query.linearizable"));
        }
        let context = call
            .cancellation
            .run_until_cancelled(self.0.node.wire_query_context(
                &call.database,
                call.session.caller.principal.as_ref(),
                linearizable,
                &call.cancellation,
            ))
            .await
            .ok_or_else(crate::error::cancelled)
            .map_err(error::map_status)?
            .map_err(error::anomaly)?;
        self.check_attachment(call, database)?;
        Ok(context)
    }

    fn check_attachment(&self, call: &Call, database: &Arc<Database>) -> Result<(), Status> {
        call.require_imported_workload(database)?;
        if let Some(scope) = call.session.caller.principal.label_scope() {
            scope.revalidate(self.0.node.now()).map_err(error::anomaly)?;
        }
        if call.cancellation.is_cancelled() {
            return Err(error::map_status(crate::error::cancelled()));
        }
        if self
            .0
            .node
            .catalog()
            .database(&call.database)
            .is_ok_and(|current| Arc::ptr_eq(&current, database))
        {
            return Ok(());
        }
        Err(error::unavailable(
            "Database attachment changed during the request",
        ))
    }
}

fn check_key(key: &[u8]) -> Result<(), Status> {
    if (16..=64).contains(&key.len()) {
        Ok(())
    } else {
        Err(error::incorrect("Key must contain 16 to 64 bytes"))
    }
}

fn head(context: &QueryContext, database: &str) -> Result<Timestamp, Status> {
    let database = context.catalog.database(database).map_err(error::anomaly)?;
    Ok(database.partitions[0]
        .tx_basis()
        .map_or(Timestamp::MIN, |basis| basis.system_time))
}

fn trailer(message: impl Message) -> FlightData {
    FlightData {
        app_metadata: message.encode_to_vec().into(),
        ..Default::default()
    }
}
