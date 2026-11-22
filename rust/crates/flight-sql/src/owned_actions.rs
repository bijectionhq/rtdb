use std::{collections::BTreeSet, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_flight::Result as ActionResult;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Schema};
use prost::Message;
use rtdb_columnar::OwnedBatch;
use rtdb_format::ipc::{OwnedStream, write_arrow_stream};
use rtdb_node::{NativeSession, TransactionStatus};
use rtdb_tx::{Documents, TxOp};
use rtdb_types::{OwnedRelation, TimeUnit, Timestamp, Value};
use tonic::Status;

use crate::{
    error::{arrow, invalid, status, unsupported},
    service::{Call, Service},
    upload, wire,
};

pub(crate) const CREATE_OWNED_V1: &str = "rtdb.owned.Create.v1";
pub(crate) const COMMIT_OWNED_V1: &str = "rtdb.owned.Commit.v1";
pub(crate) const READ_OWNED_V1: &str = "rtdb.owned.ReadPoint.v1";
const OWNED_PRESENT: &str = "rtdb.owned.present_micros";
const OWNED_VALID: &str = "rtdb.owned.valid_micros";
const OWNED_REVISION: &str = "rtdb.owned.revision_micros";

fn split_relation(body: &[u8]) -> Result<(OwnedRelation, &[u8]), Status> {
    let (length, rest) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Owned action has no relation length"))?;
    let length = usize::from(u16::from_be_bytes(*length));
    let (name, arrow_bytes) = rest.split_at_checked(length).ok_or_else(|| invalid("Owned action relation is truncated"))?;
    if length > 96 { return Err(invalid("Owned action relation exceeds its length limit")); }
    let name = std::str::from_utf8(name).map_err(arrow)?;
    Ok((OwnedRelation::new(name).map_err(status)?, arrow_bytes))
}

fn request_batch(call: &Call, arrow: &[u8]) -> Result<OwnedBatch, Status> {
    let mut stream = OwnedStream::read(arrow, call.service.memory()).map_err(status)?.into_pages();
    let [batch] = stream.as_slice() else {
        return Err(invalid("Owned action requires exactly one Arrow record batch"));
    };
    if batch.batch().num_rows() == 0 { return Err(invalid("Owned action requires a nonempty Arrow record batch")); }
    let schema = batch.batch().schema();
    let names = schema.fields().iter().map(|field| field.name().as_str()).collect::<BTreeSet<_>>();
    if names.len() != schema.fields().len() || !names.contains("_id") {
        return Err(invalid("Owned action requires unique document columns and an _id column"));
    }
    Ok(stream.pop().expect("validated one batch"))
}

fn require_idle(native: &NativeSession) -> Result<(), Status> {
    if native.state().auto_commit() && native.state().status() == TransactionStatus::Idle { return Ok(()); }
    Err(Status::failed_precondition("Owned actions require an idle autocommit session"))
}

impl Service {
    pub(crate) async fn create_owned_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, arrow)?;
        let (rows, _decoded) = upload::documents(batch.batch(), self)?;
        let operation = TxOp::OwnedInsertDocs {
            relation, documents: Documents::new(rows, self.memory()).map_err(status)?,
            valid_from: None, valid_to: None,
        };
        self.owned_write_reply(call, vec![operation]).await
    }

    pub(crate) async fn commit_owned_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, rest) = split_relation(body)?;
        let (deadline, arrow) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Owned commit has no deadline"))?;
        let deadline = Timestamp(i64::from_be_bytes(*deadline));
        let batch = request_batch(call, arrow)?;
        let (rows, _decoded) = upload::documents(batch.batch(), self)?;
        let operations = rows.len().checked_mul(2).and_then(|count| count.checked_add(1))
            .ok_or_else(|| invalid("Owned commit has too many operations"))?;
        let _operations = call.reserve(wire::mul(operations, std::mem::size_of::<TxOp>() + 512)?)?;
        let mut writes = Vec::with_capacity(operations);
        writes.push(TxOp::OwnedDeadline { until: deadline });
        for mut row in rows {
            let id = row.get("_id").ok_or_else(|| invalid("Owned commit row has no _id"))?.entity_id().map_err(status)?;
            let Value::Utf8(operation) = row.remove("_op").ok_or_else(|| invalid("Owned commit row has no _op"))? else {
                return Err(invalid("Owned commit _op must be Utf8"));
            };
            let valid_at = optional_micros(row.remove("_read_valid_micros"))?;
            let expected = optional_micros(row.remove("_read_system_micros"))?;
            match valid_at {
                Some(valid_at) => writes.push(TxOp::OwnedCheck { relation: relation.clone(), id: id.clone(), valid_at, expected }),
                None if expected.is_some() => return Err(invalid("Owned point expectation has no valid instant")),
                None => (),
            }
            let valid_from = valid_endpoint(row.remove("_valid_from"))?;
            let valid_to = valid_endpoint(row.remove("_valid_to"))?;
            if operation == "check" {
                if valid_at.is_none() || valid_from.is_some() || valid_to.is_some() || has_payload(&row) {
                    return Err(invalid("Owned check requires one observation and no mutation fields"));
                }
                continue;
            }
            let operation = match operation.as_str() {
                "create" => TxOp::OwnedInsertDocs {
                    relation: relation.clone(), documents: Documents::new(vec![row], self.memory()).map_err(status)?, valid_from, valid_to,
                },
                "replace" => TxOp::OwnedReplaceDocs {
                    relation: relation.clone(), documents: Documents::new(vec![row], self.memory()).map_err(status)?, valid_from, valid_to,
                },
                "delete" => {
                    if has_payload(&row) { return Err(invalid("Owned delete carries document values")); }
                    TxOp::OwnedDelete { relation: relation.clone(), id, valid_from, valid_to }
                }
                _ => return Err(invalid("Unknown owned commit operation")),
            };
            writes.push(operation);
        }
        self.owned_write_reply(call, writes).await
    }

    async fn owned_write_reply(&self, call: &Call, operations: Vec<TxOp>) -> Result<ActionResult, Status> {
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let outcome = native.state_mut().execute_tx(operations, Default::default()).await.map_err(status)?;
        if let Some(error) = outcome.error() { return Err(status(error.clone())); }
        if !outcome.committed() { return Err(Status::aborted("Owned write did not commit")); }
        let tx = outcome.tx_key();
        let schema = Arc::new(Schema::new(vec![
            Field::new("tx_id", DataType::Int64, false),
            Field::new("system_time_micros", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(Int64Array::from(vec![tx.tx_id.0])),
            Arc::new(Int64Array::from(vec![tx.system_time.0])),
        ]).map_err(arrow)?;
        self.owned_action_reply(&schema, Some(&batch))
    }

    pub(crate) async fn read_owned_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, arrow)?;
        let batch = batch.batch();
        if batch.num_columns() != 1 || batch.num_rows() != 1 {
            return Err(invalid("Owned point action requires one _id cell"));
        }
        let field = batch.schema_ref().field(0);
        let values = batch.column(0).as_ref();
        let _identity = call.reserve(wire::add(
            4096, rtdb_columnar::estimate_read_value_bytes(field, values, 0).map_err(status)?,
        )?)?;
        let id = rtdb_columnar::read_execution_value(field, values, 0).map_err(status)?.entity_id().map_err(status)?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let (present, result) = native.open_owned_point(&relation, &id).await.map_err(status)?;
        let Some(mut cursor) = result else {
            return self.owned_point_reply(&Schema::empty(), None, present, None);
        };
        let mut result = None;
        while let Some(batch) = cursor.next_batch().await.map_err(status)? {
            if batch.batch().num_rows() == 0 { continue; }
            if result.is_some() || batch.batch().num_rows() != 1 {
                return Err(Status::internal("Owned point lookup returned multiple rows"));
            }
            result = Some(batch);
        }
        if let Some(batch) = result {
            let revision = rtdb_query::owned_row_revision(batch.batch(), 0, &id, self.memory()).map_err(status)?;
            self.owned_point_reply(batch.batch().schema_ref(), Some(batch.batch()), present, Some(revision))
        } else {
            let schema = wire::plan_schema(cursor.runtime_schema(), self.memory())?;
            self.owned_point_reply(&schema, None, present, None)
        }
    }

    fn owned_point_reply(&self, schema: &Schema, batch: Option<&RecordBatch>, present: Timestamp, revision: Option<Timestamp>) -> Result<ActionResult, Status> {
        let _metadata = self.memory().reserve(wire::schema_bound(schema)?).map_err(status)?;
        let mut metadata = schema.metadata().clone();
        metadata.insert(OWNED_PRESENT.into(), present.0.to_string());
        metadata.insert(OWNED_VALID.into(), present.0.to_string());
        if let Some(revision) = revision { metadata.insert(OWNED_REVISION.into(), revision.0.to_string()); }
        let columns: Vec<_> = schema.fields().iter().enumerate()
            .filter(|(_, field)| field.name() != "_system_from")
            .map(|(index, _)| index).collect();
        let schema = Arc::new(schema.project(&columns).map_err(arrow)?.with_metadata(metadata));
        let batch = batch.map(|batch| RecordBatch::try_new(Arc::clone(&schema), columns.iter().map(|&index| Arc::clone(batch.column(index))).collect()))
            .transpose().map_err(arrow)?;
        self.owned_action_reply(&schema, batch.as_ref())
    }

    fn owned_action_reply(&self, schema: &Schema, batch: Option<&RecordBatch>) -> Result<ActionResult, Status> {
        let bound = wire::add(wire::schema_bound(schema)?, wire::mul(batch.map_or(0, RecordBatch::get_array_memory_size), 8)?)?;
        let _encoding = self.memory().reserve(bound).map_err(status)?;
        let pages = batch.map(std::slice::from_ref).unwrap_or_default();
        let bytes = write_arrow_stream(schema, pages, IpcWriteOptions::default()).map_err(status)?;
        let reply = ActionResult { body: bytes.into() };
        if reply.encoded_len() > self.0.config.maximum_message_bytes {
            return Err(Status::resource_exhausted("Owned action response exceeds the Flight message limit"));
        }
        let retained = self.memory().reserve(reply.body.len()).map_err(status)?;
        Ok(ActionResult { body: wire::retain(reply.body, retained) })
    }
}

fn optional_micros(value: Option<Value>) -> Result<Option<Timestamp>, Status> {
    match value {
        Some(Value::I64(time)) => Ok(Some(Timestamp(time))),
        None | Some(Value::Null | Value::Undefined) => Ok(None),
        _ => Err(invalid("Owned observation requires signed microsecond integers")),
    }
}

fn valid_endpoint(value: Option<Value>) -> Result<Option<Timestamp>, Status> {
    match value {
        Some(Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some(_) }) => Ok(Some(Timestamp(value))),
        None | Some(Value::Null | Value::Undefined) => Ok(None),
        _ => Err(invalid("Owned mutation validity bounds must be zoned microsecond timestamps")),
    }
}

fn has_payload(row: &rtdb_types::StructFields<Value>) -> bool {
    row.iter().any(|(name, value)| name != "_id" && !matches!(value, Value::Null | Value::Undefined))
}
