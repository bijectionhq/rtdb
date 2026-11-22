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
use rtdb_types::{OwnedRelation, Timestamp};
use tonic::Status;

use crate::{
    error::{arrow, invalid, status, unsupported},
    service::{Call, Service},
    upload, wire,
};

pub(crate) const CREATE_OWNED_V1: &str = "rtdb.owned.Create.v1";
pub(crate) const READ_OWNED_V1: &str = "rtdb.owned.ReadPoint.v1";
const OWNED_PRESENT: &str = "rtdb.owned.present_micros";

fn request(call: &Call, body: &[u8]) -> Result<(OwnedRelation, OwnedBatch), Status> {
    let (length, rest) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Owned action has no relation length"))?;
    let length = usize::from(u16::from_be_bytes(*length));
    let (name, arrow_bytes) = rest.split_at_checked(length).ok_or_else(|| invalid("Owned action relation is truncated"))?;
    if length > 96 { return Err(invalid("Owned action relation exceeds its length limit")); }
    let name = std::str::from_utf8(name).map_err(arrow)?;
    let relation = OwnedRelation::new(name).map_err(status)?;
    let mut stream = OwnedStream::read(arrow_bytes, call.service.memory()).map_err(status)?.into_pages();
    let [batch] = stream.as_slice() else {
        return Err(invalid("Owned action requires exactly one Arrow record batch"));
    };
    if batch.batch().num_rows() == 0 { return Err(invalid("Owned action requires a nonempty Arrow record batch")); }
    let schema = batch.batch().schema();
    let names = schema.fields().iter().map(|field| field.name().as_str()).collect::<BTreeSet<_>>();
    if names.len() != schema.fields().len() || !names.contains("_id") {
        return Err(invalid("Owned action requires unique document columns and an _id column"));
    }
    Ok((relation, stream.pop().expect("validated one batch")))
}

fn require_idle(native: &NativeSession) -> Result<(), Status> {
    if native.state().auto_commit() && native.state().status() == TransactionStatus::Idle { return Ok(()); }
    Err(Status::failed_precondition("Owned actions require an idle autocommit session"))
}

impl Service {
    pub(crate) async fn create_owned_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, batch) = request(call, body)?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let (rows, _decoded) = upload::documents(batch.batch(), self)?;
        let operation = TxOp::OwnedInsertDocs {
            relation, documents: Documents::new(rows, self.memory()).map_err(status)?,
            valid_from: None, valid_to: None,
        };
        let outcome = native.state_mut().execute_tx(vec![operation], Default::default()).await.map_err(status)?;
        if let Some(error) = outcome.error() { return Err(status(error.clone())); }
        if !outcome.committed() { return Err(Status::aborted("Owned insert did not commit")); }
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
        let (relation, batch) = request(call, body)?;
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
            return self.owned_point_reply(&Schema::empty(), None, present);
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
            self.owned_point_reply(batch.batch().schema_ref(), Some(batch.batch()), present)
        } else {
            let schema = wire::plan_schema(cursor.runtime_schema(), self.memory())?;
            self.owned_point_reply(&schema, None, present)
        }
    }

    fn owned_point_reply(&self, schema: &Schema, batch: Option<&RecordBatch>, present: Timestamp) -> Result<ActionResult, Status> {
        let _metadata = self.memory().reserve(wire::schema_bound(schema)?).map_err(status)?;
        let mut metadata = schema.metadata().clone();
        metadata.insert(OWNED_PRESENT.into(), present.0.to_string());
        let schema = Arc::new(schema.clone().with_metadata(metadata));
        let batch = batch.map(|batch| RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec()))
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
