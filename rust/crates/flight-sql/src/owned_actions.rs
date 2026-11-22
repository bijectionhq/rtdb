use std::{collections::{BTreeMap, BTreeSet}, sync::{Arc, Mutex}, time::Duration};

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow_flight::Result as ActionResult;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use prost::Message;
use rtdb_columnar::{MemoryReservation, OwnedBatch};
use rtdb_format::ipc::{OwnedStream, write_arrow_stream};
use rtdb_node::{NativeSession, OwnedCountRequest, OwnedRangeRequest, OwnedRangeWitness, OwnedReadSession, OwnedValidHistory, TransactionStatus};
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
pub(crate) const COMMIT_OWNED_V3: &str = "rtdb.owned.Commit.v3";
const MAX_RANGE_WITNESSES: usize = 32;
const OWNED_RANGE_WITNESS: &str = "rtdb.owned.range_witness";
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
        Ok(Self { deadline, groups: owned_groups(body, false)? })
    }
}

fn owned_groups(body: &[u8], require_writes: bool) -> Result<Vec<(OwnedRelation, &[u8])>, Status> {
    let (count, mut rest) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Owned commit has no group count"))?;
    let count = usize::from(u16::from_be_bytes(*count));
    if count > MAX_COMMIT_GROUPS || (require_writes && count == 0) {
        return Err(invalid("Owned commit has an invalid relation group count"));
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
    Ok(groups)
}

pub(crate) const READ_OWNED_V1: &str = "rtdb.owned.ReadPoint.v1";
pub(crate) const OPEN_OWNED_READ_V1: &str = "rtdb.owned.OpenRead.v1";
pub(crate) const HELD_OWNED_POINT_V1: &str = "rtdb.owned.ReadHeldPoint.v1";
pub(crate) const HELD_OWNED_POINT_V2: &str = "rtdb.owned.ReadHeldPoint.v2";
pub(crate) const HELD_OWNED_HISTORICAL_POINT_V1: &str = "rtdb.owned.ReadHeldHistoricalPoint.v1";
pub(crate) const HELD_OWNED_VALID_DURING_V1: &str = "rtdb.owned.ReadHeldValidDuring.v1";
pub(crate) const HELD_OWNED_VERSIONS_V1: &str = "rtdb.owned.ReadHeldVersions.v1";
pub(crate) const CLOSE_OWNED_READ_V1: &str = "rtdb.owned.CloseRead.v1";
pub(crate) const READ_WORLD_POINT_V1: &str = "rtdb.owned.ReadWorldPoint.v1";
pub(crate) const OPEN_WORLD_READ_V1: &str = "rtdb.owned.OpenWorldRead.v1";
pub(crate) const HELD_WORLD_POINT_V1: &str = "rtdb.owned.ReadHeldWorldPoint.v1";
pub(crate) const CLOSE_WORLD_READ_V1: &str = "rtdb.owned.CloseWorldRead.v1";
pub(crate) const DROP_WORLD_V1: &str = "rtdb.owned.DropWorld.v1";
pub(crate) const FORK_WORLD_V1: &str = "rtdb.owned.ForkWorld.v1";
pub(crate) const COMMIT_WORLD_V1: &str = "rtdb.owned.CommitWorld.v1";
pub(crate) const SEARCH_HELD_OWNED_V1: &str = "rtdb.owned.SearchHeld.v1";
pub(crate) const HELD_OWNED_RANGE_V1: &str = "rtdb.owned.ReadHeldRange.v1";
pub(crate) const HELD_OWNED_RANGE_V2: &str = "rtdb.owned.ReadHeldRange.v2";
pub(crate) const HELD_OWNED_COUNT_V1: &str = "rtdb.owned.CountHeldRange.v1";
pub(crate) const HELD_OWNED_CHANGES_V2: &str = "rtdb.owned.ReadHeldChanges.v2";
const READ_IDLE: Duration = Duration::from_secs(60);
const OWNED_PRESENT: &str = "rtdb.owned.present_micros";
const OWNED_VALID: &str = "rtdb.owned.valid_micros";
const OWNED_VALID_FROM: &str = "rtdb.owned.valid_from_micros";
const OWNED_VALID_TO: &str = "rtdb.owned.valid_to_micros";
const MAX_HISTORY_SLICES: usize = 4096;
const OWNED_VERSION_REVISION: &str = "rtdb.owned.version_revision";
const OWNED_VERSION_TIME: &str = "rtdb.owned.version_micros";
const OWNED_REVISION: &str = "rtdb.owned.revision_micros";
const OWNED_CHANGE_COVERAGE: &str = "rtdb.owned.change_coverage_micros";

pub(crate) struct OwnedReadResource {
    snapshot: OwnedReadSession,
    owner: ResourceOwner,
    session: String,
    database: String,
    witnesses: Mutex<BTreeMap<String, RetainedRangeWitness>>,
    _permit: OwnedSemaphorePermit,
    _reservation: Arc<MemoryReservation>,
}

struct RetainedRangeWitness {
    nonce: Uuid,
    evidence: OwnedRangeWitness,
    _reservation: Arc<MemoryReservation>,
}

type Witnesses = Vec<(Arc<OwnedReadResource>, OwnedRangeWitness)>;

impl OwnedReadResource {
    fn remember(&self, call: &Call, relation: &OwnedRelation, evidence: OwnedRangeWitness) -> Result<Uuid, Status> {
        self.snapshot.check().map_err(status)?;
        let mut witnesses = self.witnesses.lock().map_err(|_| Status::internal("Owned range witness storage is unavailable"))?;
        if let Some(witness) = witnesses.get(relation.name()) {
            return Ok(witness.nonce);
        }
        if witnesses.len() >= MAX_RANGE_WITNESSES {
            return Err(Status::resource_exhausted("The held read has too many witnessed relations"));
        }
        let nonce = Uuid::new_v4();
        if witnesses.values().any(|witness| witness.nonce == nonce) {
            return Err(Status::unavailable("Could not issue an owned range witness"));
        }
        let reservation = call.reserve(wire::add(512, relation.name().len() * 2)?)?;
        witnesses.insert(relation.name().to_owned(), RetainedRangeWitness {
            nonce, evidence, _reservation: reservation,
        });
        Ok(nonce)
    }
}

enum HistoryRead<'a> {
    Observed { read: OwnedValidHistory<'a>, held: &'a OwnedReadResource, relation: &'a OwnedRelation },
    Version(QueryCursor),
}

impl HistoryRead<'_> {
    async fn next_batch(&mut self) -> rtdb_types::Result<Option<OwnedBatch>> {
        match self {
            Self::Observed { read, .. } => read.next_batch().await,
            Self::Version(cursor) => cursor.next_batch().await,
        }
    }

    fn runtime_schema(&self) -> Option<&rtdb_plan::Schema> {
        match self {
            Self::Observed { read, .. } => read.runtime_schema(),
            Self::Version(cursor) => Some(cursor.runtime_schema()),
        }
    }

    fn witness_nonce(&self, call: &Call) -> Result<Option<Uuid>, Status> {
        match self {
            Self::Observed { read, held, relation } => {
                read.witness().map_err(status)?.map(|evidence| held.remember(call, relation, evidence)).transpose()
            }
            Self::Version(_) => Ok(None),
        }
    }
}

fn witnessed_schema(
    call: &Call, held: &OwnedReadResource, relation: &OwnedRelation, schema: &Schema,
    evidence: Option<OwnedRangeWitness>,
) -> Result<Option<(Schema, Arc<MemoryReservation>)>, Status> {
    let Some(evidence) = evidence else { return Ok(None); };
    let charge = call.reserve(wire::add(wire::schema_bound(schema)?, 256)?)?;
    let nonce = held.remember(call, relation, evidence)?;
    let mut metadata = schema.metadata().clone();
    metadata.insert(OWNED_RANGE_WITNESS.into(), nonce.simple().to_string());
    Ok(Some((schema.clone().with_metadata(metadata), charge)))
}

impl Leased for OwnedReadResource {
    fn close(&self) { self.snapshot.close(); }
}

pub(crate) struct OwnedWorldReadResource {
    snapshot: Box<dyn crate::OwnedWorldRead>,
    owner: ResourceOwner,
    session: String,
    database: String,
    _permit: OwnedSemaphorePermit,
    _reservation: Arc<MemoryReservation>,
}

impl Leased for OwnedWorldReadResource {
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
        self.commit_owned(call, body, format, Vec::new()).await
    }

    pub(crate) async fn commit_owned_witnessed_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (length, mut body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Owned commit has no witness count"))?;
        let count = usize::from(u16::from_be_bytes(*length));
        if !(1..=MAX_RANGE_WITNESSES).contains(&count) {
            return Err(invalid("Owned commit requires one to 32 range witnesses"));
        }
        let _framing = call.reserve(count * 512)?;
        let mut witnesses = Vec::with_capacity(count);
        for _ in 0..count {
            let (handle, tail) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned range witness has no held handle"))?;
            let (nonce, tail) = tail.split_first_chunk::<16>().ok_or_else(|| invalid("Owned range witness has no nonce"))?;
            body = tail;
            let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
            held.snapshot.check().map_err(status)?;
            let nonce = Uuid::from_bytes(*nonce);
            let witness = held.witnesses.lock().map_err(|_| Status::internal("Owned range witness storage is unavailable"))?
                .values().find(|witness| witness.nonce == nonce)
                .map(|witness| witness.evidence.clone()).ok_or_else(|| Status::not_found("Unknown owned range witness"))?;
            witnesses.push((held, witness));
        }
        self.commit_owned(call, body, CommitFormat::Grouped, witnesses).await
    }

    async fn commit_owned(&self, call: &Call, body: &[u8], format: CommitFormat, witnesses: Witnesses) -> Result<ActionResult, Status> {
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
        self.owned_write_reply_with_witnesses(call, writes, witnesses).await
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
        self.owned_write_reply_with_witnesses(call, operations, Vec::new()).await
    }

    async fn owned_write_reply_with_witnesses(
        &self, call: &Call, mut operations: Vec<TxOp>, witnesses: Witnesses,
    ) -> Result<ActionResult, Status> {
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        if !witnesses.is_empty() {
            let scope = native.placed_history().map_err(status)?;
            operations.try_reserve_exact(witnesses.len())
                .map_err(|_| Status::resource_exhausted("Owned witness allocation failed"))?;
            for (held, evidence) in &witnesses {
                held.snapshot.check().map_err(status)?;
                if !evidence.matches_scope(scope) {
                    return Err(Status::failed_precondition("Owned range witness belongs to another placement"));
                }
                operations.push(evidence.clone().operation());
            }
        }
        let outcome = native.state_mut().execute_tx(operations, Default::default()).await.map_err(status)?;
        if let Some(error) = outcome.error() { return Err(status(error.clone())); }
        if !outcome.committed() { return Err(Status::aborted("Owned write did not commit")); }
        self.owned_tx_reply(outcome.tx_key())
    }

    fn owned_tx_reply(&self, tx: rtdb_types::TransactionKey) -> Result<ActionResult, Status> {
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
            witnesses: Mutex::default(), _permit: permit, _reservation: reservation,
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
        let (relation, id, valid, _identity) = point_request(call, request, if with_valid { PointTime::Optional } else { PointTime::Default })?;
        self.execute_owned_point(&held.snapshot, &relation, &id, valid).await
    }

    pub(crate) async fn held_owned_historical_point_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned historical point has no handle"))?;
        let (system, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Owned historical point has no system time"))?;
        let system = Timestamp(i64::from_be_bytes(*system));
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, valid, _identity) = point_request(call, request, PointTime::Optional)?;
        let valid = valid.unwrap_or(system);
        let cursor = held.snapshot.open_historical_point(&relation, &id, system, valid).await.map_err(status)?;
        let reply = self.finish_owned_point(system, valid, &id, cursor).await?;
        held.snapshot.check().map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_valid_during_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        use rtdb_types::Interval;

        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned history has no held read handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Lookup::Found(entry) = self.0.owned_reads.take(&handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = request_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 3 || batch.num_rows() != 1 {
            return Err(invalid("Owned history requires one row with _id and two valid endpoints"));
        }
        let (id, _identity) = point_identity(call, batch)?;
        let from = history_request_endpoint(batch, "valid_from_micros")?;
        let to = history_request_endpoint(batch, "valid_to_micros")?;
        let valid = Interval::new(from, to).map_err(status)?;
        if valid.is_empty() { return Err(invalid("Owned history requires a nonempty valid window")); }

        let present = held.snapshot.present().system_time;
        let history = held.snapshot.open_valid_during_observed(&relation, &id, valid).await.map_err(status)?;
        let read = HistoryRead::Observed { read: history, held: &held, relation: &relation };
        let reply = self.history_reply(call, &id, read, valid, present, None).await?;
        held.snapshot.check().map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_versions_action(&self, call: &Call, body: &[u8]) -> Result<Vec<ActionResult>, Status> {
        use rtdb_types::Interval;

        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned versions has no held handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Lookup::Found(entry) = self.0.owned_reads.take(&handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = request_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 5 || batch.num_rows() != 1 {
            return Err(invalid("Owned versions requires one row with identity, system endpoints, and pagination fields"));
        }
        let (id, _identity) = point_identity(call, batch)?;
        let from = history_request_endpoint(batch, "system_from_micros")?;
        let to = history_request_endpoint(batch, "system_to_micros")?;
        let system = Interval::new(from, to).map_err(status)?;
        let after = u64::try_from(history_request_integer(batch, "after_revision")?)
            .map_err(|_| invalid("Owned version revision must be nonnegative"))?;
        let limit = usize::try_from(history_request_integer(batch, "limit")?)
            .map_err(|_| invalid("Owned version limit is out of range"))?;
        let page = held.snapshot.versions(&relation, &id, system, after, limit).await.map_err(status)?;
        let present = held.snapshot.present().system_time;
        let _framing = call.reserve(wire::mul(page.versions.len() + 1, 512)?)?;
        let header_charge = call.reserve(64)?;
        let mut header = Vec::with_capacity(17);
        header.extend_from_slice(&page.next_revision.to_be_bytes());
        header.push(u8::from(page.done));
        header.extend_from_slice(&page.floor.0.to_be_bytes());
        let header = ActionResult { body: wire::retain(header, header_charge) };
        let mut bytes = header.encoded_len();
        let mut results = vec![header];
        for version in page.versions {
            let reply = self.history_reply(call, &id, HistoryRead::Version(version.timeline), Interval::ALL, present,
                Some((version.revision, version.system_time))).await?;
            bytes = wire::add(bytes, reply.encoded_len())?;
            if bytes > self.0.config.maximum_message_bytes {
                return Err(Status::resource_exhausted("Owned version page exceeds the Flight response limit"));
            }
            results.push(reply);
        }
        held.snapshot.check().map_err(status)?;
        Ok(results)
    }

    async fn history_reply(
        &self, call: &Call, id: &EntityId, mut read: HistoryRead<'_>, valid: rtdb_types::Interval,
        present: Timestamp, version: Option<(u64, Timestamp)>,
    ) -> Result<ActionResult, Status> {
        let (from, to) = (valid.lower(), valid.upper());
        let as_of = version.map_or(present, |(_, system)| system);
        let mut pages = Vec::new();
        let mut rows = 0usize;
        let mut retained_bytes = 0usize;
        let mut last_end = from;
        let empty_schema;
        while let Some(page) = read.next_batch().await.map_err(status)? {
            let batch = page.batch();
            if batch.num_rows() == 0 { continue; }
            rows = wire::add(rows, batch.num_rows())?;
            retained_bytes = wire::add(retained_bytes, batch.get_array_memory_size())?;
            if rows > MAX_HISTORY_SLICES || retained_bytes > self.0.config.maximum_message_bytes {
                return Err(Status::resource_exhausted("Owned history exceeds its slice or response limit"));
            }
            for row in 0..batch.num_rows() {
                let start = history_row_time(batch, "_valid_from", row)?.ok_or_else(history_invalid)?;
                let end = history_row_time(batch, "_valid_to", row)?.unwrap_or(Timestamp::MAX);
                let revision = rtdb_query::owned_row_revision(batch, row, id, self.memory()).map_err(status)?;
                if start < last_end || start >= end || end > to || revision > as_of {
                    return Err(history_invalid());
                }
                last_end = end;
            }
            pages.push(page);
        }
        let schema = if let Some(page) = pages.first() {
            page.batch().schema()
        } else if let Some(schema) = read.runtime_schema() {
            empty_schema = wire::plan_schema(schema, self.memory())?;
            Arc::new((*empty_schema).clone())
        } else {
            Arc::new(Schema::empty())
        };
        let _metadata = call.reserve(wire::add(wire::schema_bound(&schema)?, wire::mul(pages.len(), 512)?)?)?;
        let mut metadata = schema.metadata().clone();
        metadata.insert(OWNED_PRESENT.into(), present.0.to_string());
        metadata.insert(OWNED_VALID_FROM.into(), from.0.to_string());
        metadata.insert(OWNED_VALID_TO.into(), to.0.to_string());
        if let Some((revision, time)) = version {
            metadata.insert(OWNED_VERSION_REVISION.into(), revision.to_string());
            metadata.insert(OWNED_VERSION_TIME.into(), time.0.to_string());
        }
        if let Some(nonce) = read.witness_nonce(call)? {
            metadata.insert(OWNED_RANGE_WITNESS.into(), nonce.simple().to_string());
        }
        let schema = Arc::new(schema.as_ref().clone().with_metadata(metadata));
        let response = pages.iter().map(|page| {
            RecordBatch::try_new(Arc::clone(&schema), page.batch().columns().to_vec()).map_err(arrow)
        }).collect::<Result<Vec<_>, _>>()?;
        self.owned_stream_reply(&schema, &response)
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
        let (page, evidence) = if full_key {
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
            held.snapshot.range_page_by_key_observed(&relation, &path, request).await
        } else {
            let lower = (!matches!(lower, Value::Null | Value::Undefined)).then_some(lower);
            let upper = (!matches!(upper, Value::Null | Value::Undefined)).then_some(upper);
            held.snapshot.range(&relation, &path, lower, upper, limit).await.map(|page| (page, None))
        }.map_err(status)?;
        let present = held.snapshot.present().system_time;
        let observed = witnessed_schema(call, &held, &relation, &page.schema, evidence)?;
        let schema = observed.as_ref().map_or(page.schema.as_ref(), |(schema, _)| schema);
        let reply = self.owned_point_reply(schema, page.batch.as_ref().map(OwnedBatch::batch), present, present, None)?;
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
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, arrow_bytes)?;
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
        let (counted, evidence) = held.snapshot.count_range_observed(&relation, &path, request).await.map_err(status)?;
        let schema = Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(UInt64Array::from(vec![counted.count]))]).map_err(arrow)?;
        let observed = witnessed_schema(call, &held, &relation, &schema, evidence)?;
        let output_schema = observed.as_ref().map_or(schema.as_ref(), |(schema, _)| schema);
        let reply = self.owned_point_reply(output_schema, Some(&batch), counted.present, counted.valid_at, None)?;
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

    pub(crate) async fn held_owned_changes_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned change read has no held handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Lookup::Found(entry) = self.0.owned_reads.take(&handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, cursor) = split_relation(request)?;
        let (after, rest) = cursor.split_first_chunk::<8>().ok_or_else(|| invalid("Owned change read has no system time"))?;
        let (limit, tail) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Owned change read has no group limit"))?;
        if !tail.is_empty() { return Err(invalid("Owned change read has trailing bytes")); }
        let page = held.snapshot.changes(&relation, Timestamp(i64::from_be_bytes(*after)), usize::from(u16::from_be_bytes(*limit)))
            .await.map_err(status)?;
        let reply = self.owned_changes_reply(&page, held.snapshot.present().system_time)?;
        held.snapshot.check().map_err(status)?;
        Ok(reply)
    }

    fn owned_changes_reply(&self, page: &rtdb_node::OwnedChangePage, present: Timestamp) -> Result<ActionResult, Status> {
        use rtdb_types::{LogicalType, ScalarType};

        let logical_id = LogicalType::from_legs([ScalarType::I64, ScalarType::Utf8, ScalarType::Keyword, ScalarType::Uuid]
            .into_iter().map(LogicalType::Scalar));
        let touches_type = LogicalType::list(LogicalType::Struct(StructFields::from([
            ("_id".into(), logical_id),
            ("op".into(), LogicalType::Scalar(ScalarType::Utf8)),
            ("valid_from_micros".into(), LogicalType::Scalar(ScalarType::I64)),
            ("valid_to_micros".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
        ])));
        let schema = Arc::new(Schema::new(vec![
            Field::new("revision", DataType::UInt64, false),
            Field::new("system_time_micros", DataType::Int64, false),
            rtdb_columnar::to_field("touches", &touches_type).map_err(status)?,
        ]).with_metadata(std::collections::HashMap::from([
            (OWNED_PRESENT.into(), present.0.to_string()),
            (OWNED_CHANGE_COVERAGE.into(), page.covered_through.0.to_string()),
        ])));
        if page.groups.is_empty() { return self.owned_action_reply(&schema, None); }
        let _rows = self.memory().reserve(wire::add(wire::mul(page.retained_bytes, 2)?, 8192)?).map_err(status)?;
        let revisions = UInt64Array::from(page.groups.iter().map(|group| group.revision).collect::<Vec<_>>());
        let times = Int64Array::from(page.groups.iter().map(|group| group.system_time.0).collect::<Vec<_>>());
        let touches: Vec<_> = page.groups.iter().map(|group| Value::List(
            group.touches.iter().map(rtdb_types::OwnedChange::to_value).collect(),
        )).collect();
        let _arrow = self.memory().reserve(rtdb_columnar::estimate_builder_bytes(&touches_type, &touches).map_err(status)?)
            .map_err(status)?;
        let touches = rtdb_columnar::build_array(&touches_type, &touches).map_err(status)?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(revisions), Arc::new(times), touches]).map_err(arrow)?;
        self.owned_action_reply(&schema, Some(&batch))
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

    pub(crate) async fn read_world_point_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let (world, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned world point has no world UUID"))?;
        let (relation, id, valid, _identity) = point_request(call, body, PointTime::Required)?;
        let valid_at = valid.ok_or_else(|| invalid("Owned world point requires a valid_micros instant"))?;
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let result = host.read_point(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            crate::WorldPoint { world: Uuid::from_bytes(*world), relation: &relation, id: &id, valid_at },
        ).await.map_err(status)?;
        self.world_point_reply(call, result, valid_at)
    }

    pub(crate) async fn open_world_read_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let world: [u8; 16] = body.try_into().map_err(|_| invalid("Owned world read open requires a world UUID"))?;
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let permit = Arc::clone(&self.0.owned_read_slots).try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("The node has too many retained owned reads"))?;
        let reservation = call.reserve(wire::add(2048, wire::add(call.session.id.len(), call.database.len())?)?)?;
        let reply_charge = call.reserve(64)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let snapshot = host.open_world_read(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            Uuid::from_bytes(world),
        ).await.map_err(status)?;
        let handle = Uuid::new_v4();
        let mut reply = Vec::with_capacity(24);
        reply.extend_from_slice(handle.as_bytes());
        reply.extend_from_slice(&snapshot.present().0.to_be_bytes());
        let held = Arc::new(OwnedWorldReadResource {
            snapshot, owner: call.session.owner(), session: call.session.id.clone(), database: call.database.clone(),
            _permit: permit, _reservation: reservation,
        });
        self.0.owned_world_reads.insert(Bytes::copy_from_slice(handle.as_bytes()), held, READ_IDLE, READ_IDLE, false, 16).map_err(status)?;
        Ok(ActionResult { body: wire::retain(reply, reply_charge) })
    }

    fn owned_world_read(&self, call: &Call, handle: &Bytes) -> Result<Arc<OwnedWorldReadResource>, Status> {
        let entry = found(self.0.owned_world_reads.lookup(handle, false), "owned world read")?;
        if entry.session != call.session.id || entry.database != call.database || !call.session.owns(&entry.owner) {
            return Err(Status::not_found("unknown owned world read"));
        }
        Ok(entry)
    }

    pub(crate) async fn held_world_point_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned world point has no held handle"))?;
        let held = self.owned_world_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, valid, _identity) = point_request(call, body, PointTime::Required)?;
        let valid_at = valid.ok_or_else(|| invalid("Owned world point requires a valid_micros instant"))?;
        let result = held.snapshot.read_point(&relation, &id, valid_at).await.map_err(status)?;
        if result.present != held.snapshot.present() {
            return Err(Status::internal("Owned world point changed its held present"));
        }
        let reply = self.world_point_reply(call, result, valid_at)?;
        held.snapshot.check().map_err(status)?;
        Ok(reply)
    }

    pub(crate) fn close_world_read_action(&self, call: &Call, body: &[u8]) -> Result<(), Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        if body.len() != 16 { return Err(invalid("Owned world read close requires one held handle")); }
        let handle = Bytes::copy_from_slice(body);
        self.owned_world_read(call, &handle)?;
        let entry = found(self.0.owned_world_reads.take(&handle), "owned world read")?;
        entry.close();
        Ok(())
    }

    fn world_point_reply(&self, call: &Call, result: crate::WorldPointResult, valid_at: Timestamp) -> Result<ActionResult, Status> {
        if let Some(row) = result.row {
            let crate::WorldPointRow { fields: row, revision, reservation: _retained } = row;
            let bytes = Value::document_retained_bytes(&row).map_err(status)?;
            let _encoding = call.reserve(wire::add(4096, wire::mul(bytes, 8)?)?)?;
            let mut fields = Vec::with_capacity(row.len());
            let mut columns = Vec::with_capacity(row.len());
            for (name, value) in &row {
                let logical = value.logical_type();
                fields.push(rtdb_columnar::to_field(name, &logical).map_err(status)?);
                columns.push(rtdb_columnar::build_array(&logical, std::slice::from_ref(value)).map_err(status)?);
            }
            let schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(arrow)?;
            self.owned_point_reply(&schema, Some(&batch), result.present, valid_at, Some(revision))
        } else {
            self.owned_point_reply(&Schema::empty(), None, result.present, valid_at, None)
        }
    }

    pub(crate) async fn drop_world_action(&self, call: &Call, body: &[u8]) -> Result<(), Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let bytes: [u8; 16] = body.try_into().map_err(|_| invalid("Owned world drop requires one world UUID"))?;
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        host.drop_world(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            Uuid::from_bytes(bytes),
        ).await.map_err(status)
    }

    pub(crate) async fn fork_world_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let (capture, rest) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned world fork has no capture UUID"))?;
        let until: [u8; 8] = rest.try_into().map_err(|_| invalid("Owned world fork requires one expiry instant"))?;
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let reply_charge = call.reserve(64)?;
        let world = host.fork_world(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            Uuid::from_bytes(*capture), Timestamp(i64::from_be_bytes(until)),
        ).await.map_err(status)?;
        Ok(ActionResult { body: wire::retain(Bytes::copy_from_slice(world.as_bytes()), reply_charge) })
    }

    pub(crate) async fn commit_world_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        use rtdb_node::OwnedWorldWrite;
        use rtdb_types::{Interval, WorldShadow};

        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let (world, groups) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned world commit has no world UUID"))?;
        let _framing = call.reserve(MAX_COMMIT_GROUPS * 512)?;
        let mut writes = Vec::new();
        let mut reservations = Vec::new();
        for (relation, arrow) in owned_groups(groups, true)? {
            let batch = request_batch(call, arrow)?;
            let batch = batch.batch();
            let schema = batch.schema_ref();
            let field = |name| schema.column_with_name(name).map(|(_, field)| field.data_type());
            if schema.fields().len() != 5 || field("_op") != Some(&DataType::Utf8)
                || field("_valid_from_micros") != Some(&DataType::Int64)
                || field("_valid_to_micros") != Some(&DataType::Int64)
                || !matches!(field("fact"), Some(DataType::Struct(_)))
            {
                return Err(invalid("Owned world commit requires _id, _op, two Int64 valid endpoints, and a fact struct"));
            }
            if batch.num_rows() > MAX_GROUPED_COMMIT_ROWS - writes.len() {
                return Err(invalid("Owned world commit exceeds its total row limit"));
            }
            let charge = call.reserve(wire::mul(batch.num_rows(), std::mem::size_of::<OwnedWorldWrite>() + 512)?)?;
            writes.try_reserve_exact(batch.num_rows()).map_err(|_| Status::resource_exhausted("Owned world commit allocation failed"))?;
            let (rows, decoded) = upload::owned_documents(batch, self)?;
            reservations.push((charge, decoded));
            for mut row in rows {
                let id = row.remove("_id").ok_or_else(|| invalid("Owned world write has no _id"))?.entity_id().map_err(status)?;
                let (Some(Value::Utf8(operation)), Some(Value::I64(from)), Some(Value::I64(to)), Some(payload)) = (
                    row.remove("_op"), row.remove("_valid_from_micros"), row.remove("_valid_to_micros"), row.remove("fact"),
                ) else { return Err(invalid("Owned world write requires a typed operation and explicit valid endpoints")); };
                let valid = Interval::from_micros(from, to).map_err(status)?;
                if valid.is_empty() { return Err(invalid("Owned world write needs a nonempty valid interval")); }
                let shadow = match (operation.as_str(), payload) {
                    ("delete", Value::Null) => WorldShadow::Deleted,
                    ("replace", Value::Struct(mut fields)) => {
                        strip_owned_absence(&mut fields)?;
                        if fields.keys().any(|name| name.starts_with('_')) {
                            return Err(invalid("Owned world fact contains a reserved field"));
                        }
                        let mut fact = StructFields::from([("_id".into(), id.as_value())]);
                        fact.extend(fields);
                        WorldShadow::Present(fact)
                    }
                    _ => return Err(invalid("Owned world write requires replace with a fact or delete with a null fact")),
                };
                writes.push(OwnedWorldWrite { relation: relation.clone(), id, shadow, valid });
            }
        }
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let committed = host.commit_world(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            Uuid::from_bytes(*world), writes,
        ).await.map_err(status)?;
        self.owned_tx_reply(committed)
    }

    pub(crate) async fn read_owned_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, id, _, _identity) = point_request(call, body, PointTime::Default)?;
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
        self.owned_stream_reply(schema, batch.map(std::slice::from_ref).unwrap_or_default())
    }

    fn owned_stream_reply(&self, schema: &Schema, pages: &[RecordBatch]) -> Result<ActionResult, Status> {
        let bytes = pages.iter().try_fold(0, |bytes, batch| wire::add(bytes, batch.get_array_memory_size()))?;
        let bound = wire::add(wire::schema_bound(schema)?, wire::mul(bytes, 8)?)?;
        let _encoding = self.memory().reserve(bound).map_err(status)?;
        let bytes = write_arrow_stream(schema, pages, IpcWriteOptions::default()).map_err(status)?;
        let reply = ActionResult { body: bytes.into() };
        if reply.encoded_len() > self.0.config.maximum_message_bytes {
            return Err(Status::resource_exhausted("Owned action response exceeds the Flight message limit"));
        }
        let retained = self.memory().reserve(reply.body.len()).map_err(status)?;
        Ok(ActionResult { body: wire::retain(reply.body, retained) })
    }
}

#[derive(Clone, Copy)]
enum PointTime { Default, Optional, Required }

fn point_request(
    call: &Call, body: &[u8], time: PointTime,
) -> Result<(OwnedRelation, EntityId, Option<Timestamp>, Arc<MemoryReservation>), Status> {
    let (relation, arrow) = split_relation(body)?;
    let batch = request_batch(call, arrow)?;
    let batch = batch.batch();
    let expected_columns = if matches!(time, PointTime::Default) { 1 } else { 2 };
    if batch.num_columns() != expected_columns || batch.num_rows() != 1 {
        return Err(invalid("Owned point action requires one row with the fields for its action version"));
    }
    let (id, identity) = point_identity(call, batch)?;
    let valid = if !matches!(time, PointTime::Default) {
        let (column, field) = batch.schema_ref().column_with_name("valid_micros")
            .ok_or_else(|| invalid("Owned point requires valid_micros"))?;
        if field.data_type() != &DataType::Int64 || !field.metadata().is_empty()
            || (matches!(time, PointTime::Optional) && !field.is_nullable())
        {
            return Err(invalid("Owned point valid_micros has an invalid Int64 field"));
        }
        let values = batch.column(column).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("Owned point valid_micros must be Int64"))?;
        let valid = (!values.is_null(0)).then(|| Timestamp(values.value(0)));
        if matches!(time, PointTime::Required) && valid.is_none() {
            return Err(invalid("Owned world point requires a valid_micros instant"));
        }
        valid
    } else {
        None
    };
    Ok((relation, id, valid, identity))
}

fn point_identity(call: &Call, batch: &RecordBatch) -> Result<(EntityId, Arc<MemoryReservation>), Status> {
    let (column, field) = batch.schema_ref().column_with_name("_id").expect("validated _id");
    let values = batch.column(column).as_ref();
    let identity = call.reserve(wire::add(4096, rtdb_columnar::estimate_read_value_bytes(field, values, 0).map_err(status)?)?)?;
    let id = rtdb_columnar::read_execution_value(field, values, 0).map_err(status)?.entity_id().map_err(status)?;
    Ok((id, identity))
}

fn history_request_endpoint(batch: &RecordBatch, name: &str) -> Result<Timestamp, Status> {
    history_request_integer(batch, name).map(Timestamp)
}

fn history_request_integer(batch: &RecordBatch, name: &str) -> Result<i64, Status> {
    let (column, field) = batch.schema_ref().column_with_name(name)
        .ok_or_else(|| invalid("Owned history is missing a required integer field"))?;
    if field.data_type() != &DataType::Int64 || !field.metadata().is_empty() {
        return Err(invalid("Owned history integer fields must be Int64"));
    }
    let values = batch.column(column).as_any().downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("Owned history integer fields must be Int64"))?;
    if values.is_null(0) { return Err(invalid("Owned history integer fields must be non-null")); }
    Ok(values.value(0))
}

fn history_row_time(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<Timestamp>, Status> {
    let (column, _) = batch.schema_ref().column_with_name(name).ok_or_else(history_invalid)?;
    let values = batch.column(column).as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(history_invalid)?;
    Ok((!values.is_null(row)).then(|| Timestamp(values.value(row))))
}

fn history_invalid() -> Status {
    Status::internal("Owned history returned invalid or overlapping slices")
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
