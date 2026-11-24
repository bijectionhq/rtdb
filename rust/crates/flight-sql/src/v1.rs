use std::{sync::Arc, time::Duration};

use arrow_flight::{self as flight, FlightData, sql::Any};
use bytes::Bytes;
use prost::{Message, Name};
use rtdb_node::{Database, DatabaseAccess, SessionIdentity};
use rtdb_query::QueryContext;
use rtdb_types::Timestamp;
use tonic::{Response, Status, metadata::MetadataMap};

use crate::{
    rtdb::v1 as proto,
    service::{Call, Output, Service, result_stream},
};

mod changes;
pub(crate) mod error;
mod query;
mod tokens;

const API: &str = "rtdb.v1";
pub const OWNED_HOSTED_FEATURE: &str = "owned.hosted";
pub const RETIRED_ORIGINAL_FAMILY_FEATURE: &str = "owned.retired-original-family";
const MAX_PAGE_ROWS: u32 = 4096;
const MAX_CHANGE_GROUPS: u32 = 256;
const MAX_WAIT: Duration = Duration::from_secs(30);
pub(crate) const ACTIONS: [&str; 3] = [
    "rtdb.v1.Capabilities",
    "rtdb.v1.Transact",
    "rtdb.v1.TxStatus",
];

#[derive(Default)]
pub(crate) struct State {
    tokens: tokens::Tokens,
    plans: query::PlanCache,
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
    ACTIONS.contains(&name) || name.starts_with("rtdb.admin.v1.")
}

fn decode<M: Message + Default + Name>(bytes: Bytes) -> Result<M, Status> {
    M::decode(bytes).map_err(|_| error::incorrect("Invalid protobuf request"))
}

impl Service {
    pub(crate) fn v1_action(
        &self,
        call: Call,
        action: flight::Action,
    ) -> Result<Response<Output<flight::Result>>, Status> {
        let outcome = match action.r#type.as_str() {
            "rtdb.v1.Capabilities" => decode::<proto::CapabilitiesRequest>(action.body).map(|_| {
                let mut features = Vec::new();
                if self.supports_hosted_upload(&call) && self.0.config.owned_world_host.is_some() {
                    features.push(OWNED_HOSTED_FEATURE.into());
                }
                if self.supports_retired_original_family(&call) {
                    features.push(RETIRED_ORIGINAL_FAMILY_FEATURE.into());
                }
                if call.session.audience_source().is_none() && !call.session.fresh_replica {
                    features.extend(
                        [
                            "query",
                            "query.track",
                            "query.page",
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
                    if self
                        .0
                        .node
                        .catalog()
                        .database(&call.database)
                        .is_ok_and(|database| database.wire_linearizable_reads())
                    {
                        features.push("query.linearizable".into());
                    }
                }
                proto::CapabilitiesResponse {
                    api: API.into(),
                    features,
                    limits: Some(proto::Limits {
                        max_page_rows: MAX_PAGE_ROWS,
                        max_change_groups: MAX_CHANGE_GROUPS,
                        max_wait_millis: MAX_WAIT.as_millis() as u64,
                        min_key_bytes: 16,
                        max_key_bytes: 64,
                    }),
                    server_time_micros: self.0.node.now().0,
                }
                .encode_to_vec()
            }),
            "rtdb.v1.Transact" => {
                decode::<proto::TransactRequest>(action.body).and_then(|request| {
                    check_key(&request.key)?;
                    Err(error::unsupported(
                        request.require.first().map_or("transact", String::as_str),
                    ))
                })
            }
            "rtdb.v1.TxStatus" => {
                decode::<proto::TxStatusRequest>(action.body).and_then(|request| {
                    check_key(&request.key)?;
                    Err(error::unsupported("tx-status"))
                })
            }
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
        if call.session.audience_source().is_some() || call.session.fresh_replica {
            return Err(error::unsupported("rtdb.v1.route"));
        }
        if branch.is_empty() {
            return Err(error::incorrect("Source requires a branch"));
        }
        if branch != "main" {
            return Err(error::unsupported("branches"));
        }
        if layer != proto::Layer::Merged {
            return Err(error::unsupported("branch.local-layer"));
        }
        call.session
            .caller
            .principal
            .authorize_database(&call.database, DatabaseAccess::Read)
            .map_err(error::anomaly)?;
        self.admit_database(&call.database)
            .await
            .map_err(error::map_status)?;
        self.0
            .node
            .catalog()
            .database(&call.database)
            .map_err(error::anomaly)
    }

    async fn wire_context(
        &self,
        call: &Call,
        database: &Arc<Database>,
        linearizable: bool,
    ) -> Result<Arc<QueryContext>, Status> {
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
