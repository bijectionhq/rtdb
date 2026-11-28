use std::sync::Arc;

use rtdb_node::{
    Database, DatabaseAccess,
    serving::{ServingDriver, ServingScope},
};
use rtdb_tx::transact::{Outcome, ScopedHoldCommand, Transact};
use rtdb_types::{EntityId, TableRef, Timestamp, rtdb_v1::Basis};
use serde::Deserialize;
use tonic::Status;

use crate::service::{Call, Service};

use super::error;

pub(super) const ACTION: &str = "rtdb.v1.ScopedHold";
pub(super) const FEATURE: &str = "row-visibility.exact-hold";

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Retain {
        hold_id: [u8; 16],
        basis: Vec<u8>,
        valid_at_micros: i64,
        expires_at_micros: i64,
        publication: Publication,
    },
    Reopen {
        hold_id: [u8; 16],
    },
    Release {
        hold_id: [u8; 16],
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Publication {
    table: String,
    row_id: Vec<u8>,
    written_basis: Vec<u8>,
}

pub(super) fn hold_unavailable() -> Status {
    Status::not_found("The projected hold is unavailable")
}

impl Service {
    pub(super) async fn scoped_hold_action(
        &self,
        call: &Call,
        bytes: &[u8],
    ) -> Result<Vec<u8>, Status> {
        if bytes.len() > 8 * 1024 {
            return Err(error::incorrect(
                "Projected hold request exceeds its byte bound",
            ));
        }
        let _charge = call.reserve(bytes.len() * 3 + 1024)?;
        let request: Request = serde_json::from_slice(bytes)
            .map_err(|_| error::incorrect("Invalid projected hold request"))?;
        let database = self.wire_attachment(call, DatabaseAccess::Read).await?;
        let driver = database.serving_driver().ok_or_else(hold_unavailable)?;
        let scope = self
            .serving_scope(call, driver)?
            .ok_or_else(hold_unavailable)?;
        if !call.session.caller.verified_credential {
            return Err(hold_unavailable());
        }
        let reply = match request {
            Request::Retain {
                hold_id,
                basis,
                valid_at_micros,
                expires_at_micros,
                publication,
            } => {
                let (schema, table) = publication
                    .table
                    .split_once('.')
                    .filter(|(schema, table)| {
                        !schema.is_empty() && !table.is_empty() && !table.contains('.')
                    })
                    .ok_or_else(|| error::incorrect("The publication needs a qualified table"))?;
                let command = ScopedHoldCommand::Retain(rtdb_node::serving::ScopedHold {
                    id: hold_id,
                    owner: scope.receipt_namespace,
                    basis: Basis::from_bytes(&basis).map_err(error::anomaly)?,
                    valid_at: Timestamp(valid_at_micros),
                    expires_at: Timestamp(expires_at_micros),
                    publication_table: TableRef::new(schema, table),
                    publication_id: EntityId::from_handle_bytes(&publication.row_id)
                        .map_err(error::anomaly)?,
                    publication_written: Basis::from_bytes(&publication.written_basis)
                        .map_err(error::anomaly)?,
                });
                self.order_scoped_hold(call, driver, &scope, command)
                    .await?;
                let hold = self
                    .read_scoped_hold(call, &database, driver, &scope, &hold_id)
                    .await?;
                hold_reply(&hold)
            }
            Request::Reopen { hold_id } => {
                let hold = self
                    .read_scoped_hold(call, &database, driver, &scope, &hold_id)
                    .await?;
                hold_reply(&hold)
            }
            Request::Release { hold_id } => {
                let command = ScopedHoldCommand::Release {
                    id: hold_id,
                    owner: scope.receipt_namespace,
                };
                self.order_scoped_hold(call, driver, &scope, command)
                    .await?;
                serde_json::json!({"state": "released"})
            }
        };
        self.check_attachment(call, &database)?;
        serde_json::to_vec(&reply).map_err(|_| Status::internal("Cannot encode the projected hold"))
    }

    async fn order_scoped_hold(
        &self,
        call: &Call,
        driver: &dyn ServingDriver,
        scope: &ServingScope,
        command: ScopedHoldCommand,
    ) -> Result<(), Status> {
        let mut request = Transact::scoped_hold(command);
        request.request_memory = Some(call.request_reservation());
        let receipt = driver
            .transact_scoped(request, scope.clone(), &call.cancellation)
            .await
            .map_err(error::anomaly)?;
        match receipt.outcome {
            Outcome::Then => Ok(()),
            Outcome::Refused { error } => Err(super::error::anomaly(error)),
            Outcome::Otherwise { .. } | Outcome::StoreDenied => Err(hold_unavailable()),
        }
    }

    pub(super) async fn read_scoped_hold(
        &self,
        call: &Call,
        database: &Arc<Database>,
        driver: &dyn ServingDriver,
        scope: &ServingScope,
        handle: &[u8],
    ) -> Result<rtdb_node::serving::ScopedHold, Status> {
        let id = handle.try_into().map_err(|_| hold_unavailable())?;
        let index = driver
            .current_custody(&call.cancellation)
            .await
            .map_err(error::anomaly)?;
        let hold = index
            .open_scoped_hold(
                id,
                scope.receipt_namespace,
                &scope.labels,
                self.0.node.now(),
            )
            .map_err(|_| hold_unavailable())?;
        self.check_attachment(call, database)?;
        Ok(hold.clone())
    }
}

fn hold_reply(hold: &rtdb_node::serving::ScopedHold) -> serde_json::Value {
    serde_json::json!({
        "hold_id": hold.id,
        "basis": hold.basis.to_bytes(),
        "valid_at_micros": hold.valid_at.0,
        "expires_at_micros": hold.expires_at.0,
    })
}
