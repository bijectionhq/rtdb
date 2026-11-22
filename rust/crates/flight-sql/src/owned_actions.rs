use std::{collections::BTreeSet, sync::Arc, time::Duration};

use arrow_array::{Int64Array, RecordBatch};
use arrow_flight::Result as ActionResult;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use prost::Message;
use rtdb_columnar::{MemoryReservation, OwnedBatch};
use rtdb_format::ipc::{OwnedStream, write_arrow_stream};
use rtdb_node::{NativeSession, OwnedRangeRequest, OwnedReadSession, TransactionStatus};
use rtdb_query::QueryCursor;
use rtdb_tx::{Documents, TxOp};
use rtdb_types::{EntityId, OwnedRelation, StructFields, TimeUnit, Timestamp, Value};
use tokio::sync::OwnedSemaphorePermit;
use tonic::Status;
use uuid::Uuid;

use crate::{
    error::{arrow, invalid, status, unsupported},
    lease::{Leased, Lookup},
    service::{Call, Service, found},
    state::ResourceOwner,
    upload, wire,
};

pub(crate) const DECLARE_OWNED_SEARCH_PATH_V1: &str = "rtdb.owned.DeclareSearchPath.v1";
pub(crate) const DECLARE_OWNED_PATH_V1: &str = "rtdb.owned.DeclarePath.v1";
pub(crate) const CREATE_OWNED_V1: &str = "rtdb.owned.Create.v1";
pub(crate) const COMMIT_OWNED_V1: &str = "rtdb.owned.Commit.v1";
pub(crate) const COMMIT_OWNED_V2: &str = "rtdb.owned.Commit.v2";
const MAX_COMMIT_GROUPS: usize = 32;
const MAX_GROUPED_COMMIT_ROWS: usize = 4096;

#[derive(Clone, Copy)]
pub(crate) enum CommitFormat {
    Single,
    Grouped,
}

struct CommitRequest<'a> {
    deadline: Timestamp,
    groups: Vec<(OwnedRelation, &'a [u8])>,
}

impl<'a> CommitRequest<'a> {
    fn parse(body: &'a [u8], format: CommitFormat) -> Result<Self, Status> {
        let (single, body) = match format {
            CommitFormat::Single => {
                let (relation, rest) = split_relation(body)?;
                (Some(relation), rest)
            },
            CommitFormat::Grouped => (None, body),
        };
        let (deadline, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Owned commit has no deadline"))?;
        let deadline = Timestamp(i64::from_be_bytes(*deadline));
        if let Some(relation) = single {
            return Ok(Self { deadline, groups: vec![(relation, body)] });
        }
        let (count, mut rest) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Owned commit has no group count"))?;
        let count = usize::from(u16::from_be_bytes(*count));
        if !(1..=MAX_COMMIT_GROUPS).contains(&count) {
            return Err(invalid("Owned commit requires one to 32 relation groups"));
        }
        let mut groups = Vec::with_capacity(count);
        for _ in 0..count {
            let (relation, framed) = split_relation(rest)?;
            if groups.iter().any(|(previous, _)| previous == &relation) {
                return Err(invalid("Owned commit has a duplicate relation"));
            }
            let (length, framed) = framed.split_first_chunk::<4>().ok_or_else(|| invalid("Owned commit group has no Arrow length"))?;
            let length = u32::from_be_bytes(*length) as usize;
            let (arrow, tail) = framed.split_at_checked(length).ok_or_else(|| invalid("Owned commit group Arrow stream is truncated"))?;
            if arrow.is_empty() {
                return Err(invalid("Owned commit group Arrow stream is empty"));
            }
            groups.push((relation, arrow));
            rest = tail;
        }
        if !rest.is_empty() {
            return Err(invalid("Owned commit has trailing bytes"));
        }
        Ok(Self { deadline, groups })
    }
}

pub(crate) const READ_OWNED_V1: &str = "rtdb.owned.ReadPoint.v1";
pub(crate) const OPEN_OWNED_READ_V1: &str = "rtdb.owned.OpenRead.v1";
pub(crate) const HELD_OWNED_POINT_V1: &str = "rtdb.owned.ReadHeldPoint.v1";
pub(crate) const CLOSE_OWNED_READ_V1: &str = "rtdb.owned.CloseRead.v1";
pub(crate) const HELD_OWNED_RANGE_V1: &str = "rtdb.owned.ReadHeldRange.v1";
pub(crate) const HELD_OWNED_RANGE_V2: &str = "rtdb.owned.ReadHeldRange.v2";
const READ_IDLE: Duration = Duration::from_secs(60);
const OWNED_PRESENT: &str = "rtdb.owned.present_micros";
const OWNED_VALID: &str = "rtdb.owned.valid_micros";
const OWNED_REVISION: &str = "rtdb.owned.revision_micros";

pub(crate) struct OwnedReadResource {
    snapshot: OwnedReadSession,
    owner: ResourceOwner,
    session: String,
    database: String,
    _permit: OwnedSemaphorePermit,
    _reservation: Arc<MemoryReservation>,
}

impl Leased for OwnedReadResource {
    fn close(&self) { self.snapshot.close(); }
}

fn split_relation(body: &[u8]) -> Result<(OwnedRelation, &[u8]), Status> {
    let (length, rest) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Owned action has no relation length"))?;
    let length = usize::from(u16::from_be_bytes(*length));
    let (name, arrow_bytes) = rest.split_at_checked(length).ok_or_else(|| invalid("Owned action relation is truncated"))?;
    if length > 96 { return Err(invalid("Owned action relation exceeds its length limit")); }
    let name = std::str::from_utf8(name).map_err(arrow)?;
    Ok((OwnedRelation::new(name).map_err(status)?, arrow_bytes))
}

fn decode_batch(call: &Call, arrow: &[u8]) -> Result<OwnedBatch, Status> {
    let mut stream = OwnedStream::read(arrow, call.service.memory()).map_err(status)?.into_pages();
    let [batch] = stream.as_slice() else {
        return Err(invalid("Owned action requires exactly one Arrow record batch"));
    };
    if batch.batch().num_rows() == 0 { return Err(invalid("Owned action requires a nonempty Arrow record batch")); }
    let schema = batch.batch().schema();
    let names = schema.fields().iter().map(|field| field.name().as_str()).collect::<BTreeSet<_>>();
    if names.len() != schema.fields().len() {
        return Err(invalid("Owned action requires unique column names"));
    }
    Ok(stream.pop().expect("validated one batch"))
}

fn request_batch(call: &Call, arrow: &[u8]) -> Result<OwnedBatch, Status> {
    let batch = decode_batch(call, arrow)?;
    if batch.batch().schema_ref().index_of("_id").is_err() {
        return Err(invalid("Owned action requires an _id column"));
    }
    Ok(batch)
}

fn compound_bound(value: Option<&Value>) -> Result<Option<&StructFields<Value>>, Status> {
    match value {
        Some(Value::Null) => Ok(None),
        Some(Value::Struct(fields)) => Ok(Some(fields)),
        _ => Err(invalid("Owned full-key range endpoints must be structs or null")),
    }
}

fn require_idle(native: &NativeSession) -> Result<(), Status> {
    if native.state().auto_commit() && native.state().status() == TransactionStatus::Idle { return Ok(()); }
    Err(Status::failed_precondition("Owned actions require an idle autocommit session"))
}

impl Service {
    pub(crate) async fn declare_owned_path_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, arrow)?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 2 {
            return Err(invalid("Owned path declaration requires one row with _id and columns"));
        }
        let (mut rows, _decoded) = upload::documents(batch.batch(), self)?;
        let row = rows.pop().expect("validated one row");
        let (Some(Value::Utf8(name)), Some(Value::List(columns))) = (row.get("_id"), row.get("columns")) else {
            return Err(invalid("Owned path declaration requires a text name and text column list"));
        };
        let columns = columns.iter().map(|column| match column {
            Value::Utf8(column) => Ok(column.clone()),
            _ => Err(invalid("Owned path key column is not text")),
        }).collect::<Result<Vec<_>, _>>()?;
        let name = OwnedRelation::new(name.as_str()).map_err(status)?;
        self.owned_write_reply(call, vec![TxOp::OwnedDeclarePath { relation, name, columns }]).await
    }

    pub(crate) async fn declare_owned_search_path_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, arrow)?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 2 {
            return Err(invalid("Owned search path declaration requires one row with _id and column"));
        }
        let (rows, _decoded) = upload::documents(batch.batch(), self)?;
        let (Some(Value::Utf8(name)), Some(Value::Utf8(column))) = (rows[0].get("_id"), rows[0].get("column")) else {
            return Err(invalid("Owned search path declaration requires a text name and column"));
        };
        let name = OwnedRelation::new(name.as_str()).map_err(status)?;
        self.owned_write_reply(call, vec![TxOp::OwnedDeclareSearchPath { relation, name, column: column.clone() }]).await
    }

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

    pub(crate) async fn commit_owned_action(&self, call: &Call, body: &[u8], format: CommitFormat) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let group_limit = match format {
            CommitFormat::Single => 1,
            CommitFormat::Grouped => MAX_COMMIT_GROUPS,
        };
        let _framing = call.reserve(group_limit * 512)?;
        let CommitRequest { deadline, groups } = CommitRequest::parse(body, format)?;
        let mut rows_left = match format {
            CommitFormat::Single => usize::MAX,
            CommitFormat::Grouped => MAX_GROUPED_COMMIT_ROWS,
        };
        let mut writes = vec![TxOp::OwnedDeadline { until: deadline }];
        let mut retained = Vec::with_capacity(groups.len());
        for (relation, arrow) in groups {
            let batch = request_batch(call, arrow)?;
            let rows = batch.batch().num_rows();
            rows_left = rows_left.checked_sub(rows)
                .ok_or_else(|| invalid("Owned commit exceeds its total row limit"))?;
            let operations = wire::mul(rows, 2)?;
            let operations_charge = call.reserve(wire::mul(operations, std::mem::size_of::<TxOp>() + 512)?)?;
            writes.try_reserve_exact(operations)
                .map_err(|_| Status::resource_exhausted("Owned commit operation allocation failed"))?;
            let (rows, documents_charge) = upload::documents(batch.batch(), self)?;
            self.append_owned_commit_group(&mut writes, &relation, rows)?;
            retained.push((operations_charge, documents_charge));
        }
        self.owned_write_reply(call, writes).await
    }

    fn append_owned_commit_group(
        &self,
        writes: &mut Vec<TxOp>,
        relation: &OwnedRelation,
        rows: Vec<rtdb_types::StructFields<Value>>,
    ) -> Result<(), Status> {
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
        Ok(())
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

    pub(crate) async fn open_owned_read_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if !body.is_empty() { return Err(invalid("Owned read open takes an empty body")); }
        let permit = Arc::clone(&self.0.owned_read_slots).try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("The node has too many retained owned reads"))?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let reservation = call.reserve(wire::add(2048, wire::add(call.session.id.len(), call.database.len())?)?)?;
        let reply_charge = call.reserve(64)?;
        let snapshot = native.open_owned_read().await.map_err(status)?;
        let handle = Uuid::new_v4();
        let mut reply = Vec::with_capacity(24);
        reply.extend_from_slice(handle.as_bytes());
        reply.extend_from_slice(&snapshot.present().system_time.0.to_be_bytes());
        let held = Arc::new(OwnedReadResource {
            snapshot, owner: call.session.owner(), session: call.session.id.clone(), database: call.database.clone(),
            _permit: permit, _reservation: reservation,
        });
        self.0.owned_reads.insert(Bytes::copy_from_slice(handle.as_bytes()), held, READ_IDLE, READ_IDLE, false, 16).map_err(status)?;
        Ok(ActionResult { body: wire::retain(reply, reply_charge) })
    }

    fn owned_read(&self, call: &Call, handle: &Bytes) -> Result<Arc<OwnedReadResource>, Status> {
        let entry = found(self.0.owned_reads.lookup(handle, false), "owned read")?;
        if entry.session != call.session.id || entry.database != call.database || !call.session.owns(&entry.owner) {
            return Err(Status::not_found("unknown owned read"));
        }
        Ok(entry)
    }

    pub(crate) async fn held_owned_point_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held point has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Lookup::Found(entry) = self.0.owned_reads.take(&handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, id, _identity) = point_request(call, request)?;
        let cursor = held.snapshot.open_point(&relation, &id).await.map_err(status)?;
        let reply = self.finish_owned_point(held.snapshot.present().system_time, &id, cursor).await?;
        held.snapshot.check().map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_range_action(&self, call: &Call, body: &[u8], full_key: bool) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held range has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Lookup::Found(entry) = self.0.owned_reads.take(&handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, arrow) = split_relation(request)?;
        let batch = decode_batch(call, arrow)?;
        let expected_fields = if full_key { 6 } else { 4 };
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != expected_fields {
            return Err(invalid("Owned range requires one row with the fields for its action version"));
        }
        let (mut rows, _decoded) = upload::domain_documents(batch.batch(), self)?;
        let row = rows.pop().expect("validated one row");
        let (Some(Value::Utf8(path)), Some(lower), Some(upper), Some(Value::I64(limit))) =
            (row.get("path"), row.get("lower"), row.get("upper"), row.get("limit"))
        else { return Err(invalid("Owned range fields have invalid types")); };
        let path = OwnedRelation::new(path.as_str()).map_err(status)?;
        let limit = usize::try_from(*limit).map_err(|_| invalid("Owned range limit is out of range"))?;
        let page = if full_key {
            let Some(Value::Bool(descending)) = row.get("descending") else {
                return Err(invalid("Owned range requires a boolean descending field"));
            };
            let request = OwnedRangeRequest {
                lower: compound_bound(Some(lower))?,
                upper: compound_bound(Some(upper))?,
                after: compound_bound(row.get("after"))?,
                descending: *descending,
                limit,
            };
            held.snapshot.range_page_by_key(&relation, &path, request).await
        } else {
            let lower = (!matches!(lower, Value::Null | Value::Undefined)).then_some(lower);
            let upper = (!matches!(upper, Value::Null | Value::Undefined)).then_some(upper);
            held.snapshot.range(&relation, &path, lower, upper, limit).await
        }.map_err(status)?;
        let reply = self.owned_point_reply(
            &page.schema, page.batch.as_ref().map(OwnedBatch::batch), held.snapshot.present().system_time, None,
        )?;
        held.snapshot.check().map_err(status)?;
        Ok(reply)
    }

    pub(crate) fn close_owned_read_action(&self, call: &Call, body: &[u8]) -> Result<(), Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if body.len() != 16 { return Err(invalid("Owned read close requires one 16-byte handle")); }
        let handle = Bytes::copy_from_slice(body);
        self.owned_read(call, &handle)?;
        let entry = found(self.0.owned_reads.take(&handle), "owned read")?;
        entry.close();
        Ok(())
    }

    pub(crate) async fn read_owned_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, id, _identity) = point_request(call, body)?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let snapshot = native.open_owned_read().await.map_err(status)?;
        let cursor = snapshot.open_point(&relation, &id).await.map_err(status)?;
        let reply = self.finish_owned_point(snapshot.present().system_time, &id, cursor).await?;
        snapshot.check().map_err(status)?;
        Ok(reply)
    }

    async fn finish_owned_point(&self, present: Timestamp, id: &EntityId, result: Option<QueryCursor>) -> Result<ActionResult, Status> {
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
            let revision = rtdb_query::owned_row_revision(batch.batch(), 0, id, self.memory()).map_err(status)?;
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

fn point_request(call: &Call, body: &[u8]) -> Result<(OwnedRelation, EntityId, Arc<MemoryReservation>), Status> {
    let (relation, arrow) = split_relation(body)?;
    let batch = request_batch(call, arrow)?;
    let batch = batch.batch();
    if batch.num_columns() != 1 || batch.num_rows() != 1 {
        return Err(invalid("Owned point action requires one _id cell"));
    }
    let field = batch.schema_ref().field(0);
    let values = batch.column(0).as_ref();
    let identity = call.reserve(wire::add(4096, rtdb_columnar::estimate_read_value_bytes(field, values, 0).map_err(status)?)?)?;
    let id = rtdb_columnar::read_execution_value(field, values, 0).map_err(status)?.entity_id().map_err(status)?;
    Ok((relation, id, identity))
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

#[cfg(test)]
mod commit_tests;
