use std::{collections::BTreeSet, sync::Arc, time::Duration};

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow_flight::Result as ActionResult;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use prost::Message;
use rtdb_columnar::{MemoryReservation, OwnedBatch};
use rtdb_format::ipc::{OwnedStream, write_arrow_stream};
use rtdb_node::{NativeSession, OwnedCountRequest, OwnedRangeRequest, OwnedReadSession, TransactionStatus};
use rtdb_query::QueryCursor;
use rtdb_tx::{Documents, TxOp};
use rtdb_types::{EntityId, LogicalType, OwnedRelation, ScalarType, StructFields, TimeUnit, Timestamp, Value};
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

pub(crate) const DECLARE_OWNED_UNIQUE_PATH_V1: &str = "rtdb.owned.DeclareUniquePath.v1";
pub(crate) const DECLARE_OWNED_SEARCH_PATH_V1: &str = "rtdb.owned.DeclareSearchPath.v1";
pub(crate) const DECLARE_OWNED_RELATION_V1: &str = "rtdb.owned.DeclareRelation.v1";
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
pub(crate) const HELD_OWNED_POINT_V2: &str = "rtdb.owned.ReadHeldPoint.v2";
pub(crate) const CLOSE_OWNED_READ_V1: &str = "rtdb.owned.CloseRead.v1";
pub(crate) const SEARCH_HELD_OWNED_V1: &str = "rtdb.owned.SearchHeld.v1";
pub(crate) const HELD_OWNED_RANGE_V1: &str = "rtdb.owned.ReadHeldRange.v1";
pub(crate) const HELD_OWNED_RANGE_V2: &str = "rtdb.owned.ReadHeldRange.v2";
pub(crate) const HELD_OWNED_COUNT_V1: &str = "rtdb.owned.CountHeldRange.v1";
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
    pub(crate) async fn declare_owned_relation_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, json) = split_relation(body)?;
        if json.len() > rtdb_types::OwnedSchema::MAX_JSON_BYTES {
            return Err(invalid("Owned declaration exceeds its byte limit"));
        }
        let _decoded = call.reserve(wire::add(wire::mul(json.len(), 8)?, 4096)?)?;
        let schema = rtdb_types::OwnedSchema::from_json(json).map_err(status)?;
        self.owned_write_reply(call, vec![TxOp::OwnedDeclareRelation { relation, schema }]).await
    }

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

    pub(crate) async fn declare_owned_single_column_path_action(&self, call: &Call, body: &[u8], unique: bool) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, arrow)?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 2 {
            return Err(invalid("Owned path declaration requires one row with _id and column"));
        }
        let (rows, _decoded) = upload::documents(batch.batch(), self)?;
        let (Some(Value::Utf8(name)), Some(Value::Utf8(column))) = (rows[0].get("_id"), rows[0].get("column")) else {
            return Err(invalid("Owned path declaration requires a text name and column"));
        };
        let name = OwnedRelation::new(name.as_str()).map_err(status)?;
        let column = column.clone();
        let operation = if unique {
            TxOp::OwnedDeclareUniquePath { relation, name, column }
        } else {
            TxOp::OwnedDeclareSearchPath { relation, name, column }
        };
        self.owned_write_reply(call, vec![operation]).await
    }

    pub(crate) async fn create_owned_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, arrow)?;
        let (mut rows, _decoded) = upload::owned_documents(batch.batch(), self)?;
        for row in &mut rows { strip_owned_absence(row)?; }
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
            let (rows, documents_charge) = upload::owned_documents(batch.batch(), self)?;
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
            strip_owned_absence(&mut row)?;
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

    pub(crate) async fn held_owned_point_action(&self, call: &Call, body: &[u8], with_valid: bool) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held point has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Lookup::Found(entry) = self.0.owned_reads.take(&handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, id, valid, _identity) = point_request(call, request, with_valid)?;
        self.execute_owned_point(&held.snapshot, &relation, &id, valid).await
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
        let present = held.snapshot.present().system_time;
        let reply = self.owned_point_reply(&page.schema, page.batch.as_ref().map(OwnedBatch::batch), present, present, None)?;
        held.snapshot.check().map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_count_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held count has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Lookup::Found(entry) = self.0.owned_reads.take(&handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, arrow) = split_relation(request)?;
        let batch = decode_batch(call, arrow)?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 4 {
            return Err(invalid("Owned count requires one row with path, lower, upper and valid_micros"));
        }
        let (mut rows, _decoded) = upload::domain_documents(batch.batch(), self)?;
        let row = rows.pop().expect("validated one row");
        let (Some(Value::Utf8(path)), Some(lower), Some(upper), Some(Value::I64(valid))) =
            (row.get("path"), row.get("lower"), row.get("upper"), row.get("valid_micros"))
        else { return Err(invalid("Owned count fields have invalid types")); };
        let path = OwnedRelation::new(path.as_str()).map_err(status)?;
        let request = OwnedCountRequest {
            lower: compound_bound(Some(lower))?,
            upper: compound_bound(Some(upper))?,
            valid_at: Timestamp(*valid),
        };
        let counted = held.snapshot.count_range(&relation, &path, request).await.map_err(status)?;
        let schema = Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(UInt64Array::from(vec![counted.count]))]).map_err(arrow)?;
        let reply = self.owned_point_reply(&schema, Some(&batch), counted.present, counted.valid_at, None)?;
        held.snapshot.check().map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn search_held_owned_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held search has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Lookup::Found(entry) = self.0.owned_reads.take(&handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, arrow_bytes)?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 3 {
            return Err(invalid("Owned held search requires one row with path, query, and limit"));
        }
        let (rows, _decode) = upload::domain_documents(batch.batch(), self)?;
        let row = &rows[0];
        let (Some(Value::Utf8(path)), Some(Value::Utf8(query)), Some(Value::I64(limit))) =
            (row.get("path"), row.get("query"), row.get("limit"))
        else { return Err(invalid("Owned held search fields have invalid types")); };
        let path = OwnedRelation::new(path.as_str()).map_err(status)?;
        let limit = usize::try_from(*limit).map_err(|_| invalid("Owned held search limit is out of range"))?;
        let hits = held.snapshot.ranked_search(&relation, &path, query, limit).await.map_err(status)?;
        let id_type = LogicalType::from_legs([ScalarType::Utf8, ScalarType::Keyword, ScalarType::I64, ScalarType::Uuid].map(LogicalType::Scalar));
        let schema = Arc::new(Schema::new(vec![
            rtdb_columnar::to_field("_id", &id_type).map_err(status)?,
            Field::new("score", DataType::Float64, false),
        ]));
        let mut charge = call.reserve(4096)?;
        let output = if hits.is_empty() {
            None
        } else {
            let mut ids = Vec::with_capacity(hits.len());
            for hit in &hits {
                let id = hit.id.as_value();
                let reservation = Arc::get_mut(&mut charge).expect("search output owns its reservation");
                reservation.grow_to(wire::add(reservation.bytes(), wire::mul(id.retained_bytes().map_err(status)?, 4)?)?).map_err(status)?;
                ids.push(id);
            }
            let ids = rtdb_columnar::build_array(&id_type, &ids).map_err(status)?;
            let scores = Arc::new(Float64Array::from_iter_values(hits.iter().map(|hit| hit.score)));
            Some(RecordBatch::try_new(Arc::clone(&schema), vec![ids, scores]).map_err(arrow)?)
        };
        let present = held.snapshot.present().system_time;
        let reply = self.owned_point_reply(&schema, output.as_ref(), present, present, None)?;
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
        let (relation, id, _, _identity) = point_request(call, body, false)?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let snapshot = native.open_owned_read().await.map_err(status)?;
        self.execute_owned_point(&snapshot, &relation, &id, None).await
    }

    async fn execute_owned_point(
        &self, snapshot: &OwnedReadSession, relation: &OwnedRelation, id: &EntityId, valid: Option<Timestamp>,
    ) -> Result<ActionResult, Status> {
        let present = snapshot.present().system_time;
        let cursor = snapshot.open_point_at(relation, id, valid).await.map_err(status)?;
        let reply = self.finish_owned_point(present, valid.unwrap_or(present), id, cursor).await?;
        snapshot.check().map_err(status)?;
        Ok(reply)
    }

    async fn finish_owned_point(&self, present: Timestamp, valid: Timestamp, id: &EntityId, result: Option<QueryCursor>) -> Result<ActionResult, Status> {
        let Some(mut cursor) = result else {
            return self.owned_point_reply(&Schema::empty(), None, present, valid, None);
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
            self.owned_point_reply(batch.batch().schema_ref(), Some(batch.batch()), present, valid, Some(revision))
        } else {
            let schema = wire::plan_schema(cursor.runtime_schema(), self.memory())?;
            self.owned_point_reply(&schema, None, present, valid, None)
        }
    }

    fn owned_point_reply(&self, schema: &Schema, batch: Option<&RecordBatch>, present: Timestamp, valid: Timestamp, revision: Option<Timestamp>) -> Result<ActionResult, Status> {
        let _metadata = self.memory().reserve(wire::schema_bound(schema)?).map_err(status)?;
        let mut metadata = schema.metadata().clone();
        metadata.insert(OWNED_PRESENT.into(), present.0.to_string());
        metadata.insert(OWNED_VALID.into(), valid.0.to_string());
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

fn point_request(
    call: &Call, body: &[u8], with_valid: bool,
) -> Result<(OwnedRelation, EntityId, Option<Timestamp>, Arc<MemoryReservation>), Status> {
    let (relation, arrow) = split_relation(body)?;
    let batch = request_batch(call, arrow)?;
    let batch = batch.batch();
    let expected_columns = if with_valid { 2 } else { 1 };
    if batch.num_columns() != expected_columns || batch.num_rows() != 1 {
        return Err(invalid("Owned point action requires one row with the fields for its action version"));
    }
    let (column, field) = batch.schema_ref().column_with_name("_id").expect("validated _id");
    let values = batch.column(column).as_ref();
    let identity = call.reserve(wire::add(4096, rtdb_columnar::estimate_read_value_bytes(field, values, 0).map_err(status)?)?)?;
    let id = rtdb_columnar::read_execution_value(field, values, 0).map_err(status)?.entity_id().map_err(status)?;
    let valid = if with_valid {
        let (column, field) = batch.schema_ref().column_with_name("valid_micros")
            .ok_or_else(|| invalid("Owned held point v2 requires valid_micros"))?;
        if field.data_type() != &DataType::Int64 || !field.is_nullable() || !field.metadata().is_empty() {
            return Err(invalid("Owned held point valid_micros must be nullable Int64"));
        }
        let values = batch.column(column).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("Owned held point valid_micros must be nullable Int64"))?;
        (!values.is_null(0)).then(|| Timestamp(values.value(0)))
    } else {
        None
    };
    Ok((relation, id, valid, identity))
}

fn strip_owned_absence(row: &mut rtdb_types::StructFields<Value>) -> Result<(), Status> {
    let absent = match row.remove("_absent") {
        None | Some(Value::Null | Value::Undefined) => return Ok(()),
        Some(Value::List(fields)) if fields.len() <= 128 => fields,
        _ => return Err(invalid("Owned _absent must be a list of at most 128 field names")),
    };
    for field in absent {
        let Value::Utf8(name) = field else { return Err(invalid("Owned _absent field must be text")); };
        if name.starts_with('_') || !matches!(row.get(name.as_str()), Some(Value::Null | Value::Undefined)) {
            return Err(invalid("Owned _absent may name only distinct null payload cells"));
        }
        row.remove(name.as_str());
    }
    Ok(())
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
