use std::{sync::Arc, time::Duration};

use arrow_flight::{self as flight, FlightData, sql::Any};
use bytes::Bytes;
use prost::{Message, Name};
use rtdb_node::serving::TransactSubmission;
use rtdb_node::{Database, DatabaseAccess, SessionIdentity};
use rtdb_query::QueryContext;
use rtdb_types::Timestamp;
use tonic::{Response, Status};

use crate::{
    rtdb::v1 as proto,
    service::{Call, Output, Service, result_stream},
};

mod changes;
pub(crate) mod error;
mod query;
mod tokens;
mod transact;

const API: &str = "rtdb.v1";
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
    pub(crate) async fn v1_action(
        &self,
        call: Call,
        action: flight::Action,
    ) -> Result<Response<Output<flight::Result>>, Status> {
        let outcome = match action.r#type.as_str() {
            "rtdb.v1.Capabilities" => decode::<proto::CapabilitiesRequest>(action.body).map(|_| {
                let mut features = Vec::new();
                if call.session.audience_source().is_none() && !call.session.fresh_replica {
                    features.extend(
                        [
                            "query",
                            "changes",
                            changes::PROGRESS_ONLY,
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
                            features = driver.features().iter().map(|feature| (*feature).to_owned()).collect();
                        } else if database.wire_linearizable_reads() {
                            features.push("query.linearizable".into());
                        }
                        if database.wire_tokens_supported() {
                            for feature in ["query.track", "query.page"] {
                                if !features.iter().any(|existing| existing == feature) {
                                    features.push(feature.into());
                                }
                            }
                        }
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
            "rtdb.v1.Transact" => self.v1_transact(&call, action.body).await,
            "rtdb.v1.TxStatus" => match decode(action.body) {
                Ok(request) => self.v1_tx_status(&call, request).await,
                Err(status) => Err(status),
            },
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
        let submitted_at = self.0.node.now();
        let request: proto::TransactRequest = decode(source.clone())?;
        check_key(&request.key)?;
        let access = if request.ops.is_empty() && request.otherwise.is_empty() {
            DatabaseAccess::Read
        } else {
            DatabaseAccess::Write
        };
        let database = self.wire_attachment(call, access).await?;
        let driver = database.serving_driver().ok_or_else(|| {
            error::unsupported(request.require.first().map_or("transact", String::as_str))
        })?;
        let transaction = transact::request(request, call.memory())?;
        let request = TransactSubmission {
            transaction,
            attempt: uuid::Uuid::new_v4(),
            submitted_at,
            source: call.retain_request(source),
        };
        let receipt = driver
            .transact(request, &call.cancellation)
            .await
            .map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        Ok(transact::receipt(receipt).encode_to_vec())
    }

    async fn v1_tx_status(
        &self,
        call: &Call,
        request: proto::TxStatusRequest,
    ) -> Result<Vec<u8>, Status> {
        check_key(&request.key)?;
        let database = self.wire_attachment(call, DatabaseAccess::Write).await?;
        let driver = database
            .serving_driver()
            .ok_or_else(|| error::unsupported("tx-status"))?;
        let receipt = driver
            .tx_status(&request.key, &call.cancellation)
            .await
            .map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        Ok(transact::receipt(receipt).encode_to_vec())
    }

    pub(crate) async fn v1_get(
        &self,
        call: Call,
        ticket: Ticket,
    ) -> Result<Response<Output<FlightData>>, Status> {
        match ticket {
            Ticket::Query(body) => match decode(body) {
                Ok(request) => self.v1_query(call, request).await,
                Err(status) => Err(call.error(status)),
            },
            Ticket::Changes(body) => match decode(body) {
                Ok(request) => self.v1_changes(call, request).await,
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
        if call.session.audience_source().is_some() || call.session.fresh_replica {
            return Err(error::unsupported("rtdb.v1.route"));
        }
        call.session
            .caller
            .principal
            .authorize_database(&call.database, access)
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
