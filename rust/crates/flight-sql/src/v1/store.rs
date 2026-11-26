//! `rtdb.store.v1`: versioned ordered bytes with read, conditional append and watch.
//!
//! The three actions carry no caller semantics. A read returns ordered entries
//! at one version; an append commits its writes only when no write committed
//! after `since` intersects its read ranges; a watch reports the next version
//! that wrote into its ranges.

use std::{sync::Arc, time::Duration};

use arrow_array::{ArrayRef, BinaryArray, RecordBatch, UInt64Array};
use arrow_ipc::writer::StreamWriter;

use bytes::Bytes;
use prost::Message;
use rtdb_index::branches::store::{KeyRange, MAX_PAGE_ENTRIES, Projection, StoreEntry, StoreRead, VALUE_COLUMN, ValueFilter, store_table};
use rtdb_node::{DatabaseAccess, serving::{ChangeImages, ChangeOrder, Changes}};
use rtdb_tx::transact::{Cell, Guard, Op, Outcome, Row, Transact, Window};
use rtdb_types::{EntityId, StructFields, Value, rtdb_v1::{At, Basis, MAIN_BRANCH}};
use tonic::Status;

use super::{MAX_CHANGE_GROUPS, MAX_WAIT, check_key, error};
use crate::{rtdb::store::v1 as proto, service::{Call, Service}};

pub(crate) const READ: &str = "rtdb.store.v1.Read";
pub(crate) const APPEND: &str = "rtdb.store.v1.Append";
pub(crate) const WATCH: &str = "rtdb.store.v1.Watch";
pub(crate) const ACTIONS: [&str; 3] = [READ, APPEND, WATCH];

fn decode<M: Message + Default>(bytes: Bytes) -> Result<M, Status> {
    M::decode(bytes).map_err(|_| error::incorrect("Invalid protobuf request"))
}

fn range(range: proto::KeyRange) -> Result<KeyRange, Status> {
    let range = KeyRange { start: range.start, end: range.end };
    range.check().map_err(error::anomaly)?;
    Ok(range)
}

fn ranges(ranges: Vec<proto::KeyRange>) -> Result<Vec<KeyRange>, Status> {
    ranges.into_iter().map(range).collect()
}

fn incarnation(bytes: &[u8]) -> Result<[u8; 16], Status> {
    bytes.try_into().map_err(|_| error::incorrect("A store incarnation has 16 bytes"))
}

/// One page as an Arrow IPC stream: `key` Binary, `written` UInt64 and
/// nullable `value` Binary.
fn arrow_page(entries: Vec<StoreEntry>) -> Result<Vec<u8>, Status> {
    let keys = BinaryArray::from_iter_values(entries.iter().map(|entry| entry.key.as_slice()));
    let written = UInt64Array::from_iter_values(entries.iter().map(|entry| entry.written.position()));
    let values = BinaryArray::from_iter(entries.iter().map(|entry| entry.value.as_deref()));
    let batch = RecordBatch::try_from_iter([
        ("key", Arc::new(keys) as ArrayRef),
        ("written", Arc::new(written) as ArrayRef),
        ("value", Arc::new(values) as ArrayRef),
    ]).map_err(crate::error::arrow)?;
    let mut writer = StreamWriter::try_new(Vec::new(), batch.schema().as_ref()).map_err(crate::error::arrow)?;
    writer.write(&batch).map_err(crate::error::arrow)?;
    writer.into_inner().map_err(crate::error::arrow)
}

impl Service {
    pub(super) async fn store_action(&self, call: &Call, name: &str, body: Bytes) -> Result<Vec<u8>, Status> {
        match name {
            READ => self.store_read(call, decode(body)?).await,
            APPEND => self.store_append(call, decode(body)?).await,
            WATCH => self.store_watch(call, decode(body)?).await,
            _ => Err(error::unsupported("rtdb.store.v1")),
        }
    }

    /// Selects the unscoped serving driver and checks a stated incarnation.
    async fn store_database(
        &self, call: &Call, access: DatabaseAccess, stated: Option<&[u8]>,
    ) -> Result<(std::sync::Arc<rtdb_node::Database>, [u8; 16]), Status> {
        if call.session.caller.principal.label_scope().is_some() {
            return Err(error::unsupported("rtdb.store.v1 on a scoped route"));
        }
        let database = self.wire_attachment(call, access).await?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let current = driver.incarnation();
        if let Some(grant) = call.session.caller.principal.workload_grant().filter(|grant| grant.is_store_fence()) {
            grant.admit_store_fence(&call.database, current, self.0.node.now()).map_err(error::anomaly)?;
        }
        if let Some(stated) = stated.filter(|stated| !stated.is_empty())
            && incarnation(stated)? != current
        {
            return Err(Status::failed_precondition("The store incarnation changed"));
        }
        Ok((database, current))
    }

    async fn store_read(&self, call: &Call, request: proto::ReadRequest) -> Result<Vec<u8>, Status> {
        if request.ranges.is_empty() {
            return Err(error::incorrect("A read names at least one range"));
        }
        if request.at.is_some() && request.incarnation.is_empty() {
            return Err(error::incorrect("A versioned read states its incarnation"));
        }
        let (database, incarnation) = self.store_database(call, DatabaseAccess::Read, Some(&request.incarnation)).await?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let mut at = request.at.map_or(At::Latest, |at| At::Exactly(Basis::new(incarnation, at)));
        let limit = (request.limit as usize).clamp(1, MAX_PAGE_ENTRIES);
        let projection = match proto::Projection::try_from(request.projection)
            .map_err(|_| error::incorrect("Invalid store projection"))?
        {
            proto::Projection::Values => Projection::Values,
            proto::Projection::Keys => Projection::Keys,
            proto::Projection::Slice => Projection::Slice { start: request.slice_start, len: request.slice_len },
        };
        let filter = request.filter.as_deref()
            .map(ValueFilter::decode)
            .transpose()
            .map_err(|refused| error::incorrect(&refused.to_string()))?;
        let mut pages = Vec::with_capacity(request.ranges.len());
        let mut read_at = None;
        for key_range in ranges(request.ranges)? {
            let after = request.after.clone().filter(|after| key_range.contains(after));
            let read = StoreRead { range: key_range, after, limit, projection, filter: filter.clone() };
            let page = driver.read_store(read, at, self.0.node.now(), &call.cancellation).await.map_err(error::anomaly)?;
            // Every later range reads the cut the first one selected.
            at = At::Exactly(page.cut.basis);
            read_at = Some(page.cut.basis.position());
            pages.push(if request.arrow {
                proto::RangePage { entries: Vec::new(), resume: page.resume, arrow_ipc: arrow_page(page.entries)? }
            } else {
                proto::RangePage {
                    entries: page.entries.into_iter().map(|entry| proto::Entry {
                        key: entry.key, value: entry.value, written: entry.written.position(),
                    }).collect(),
                    resume: page.resume,
                    arrow_ipc: Default::default(),
                }
            });
        }
        self.check_attachment(call, &database)?;
        Ok(proto::ReadReply {
            incarnation: incarnation.to_vec(),
            at: read_at.unwrap_or_default(),
            pages,
        }.encode_to_vec())
    }

    async fn store_append(&self, call: &Call, request: proto::AppendRequest) -> Result<Vec<u8>, Status> {
        check_key(&request.key)?;
        if request.incarnation.is_empty() {
            return Err(error::incorrect("An append states its incarnation"));
        }
        let access = if request.writes.is_empty() { DatabaseAccess::Read } else { DatabaseAccess::Write };
        let (database, incarnation) = self.store_database(call, access, Some(&request.incarnation)).await?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let since = Basis::new(incarnation, request.since);
        let guards = ranges(request.reads)?.into_iter()
            .map(|range| Guard::Unwritten { range, since })
            .collect::<Vec<_>>();
        let table = store_table();
        let mut rows = Vec::new();
        let mut deletes = Vec::new();
        for write in request.writes {
            let id = EntityId::Binary(write.key);
            match write.value {
                Some(value) => rows.push(Row {
                    id,
                    valid: None,
                    cells: StructFields::from_iter([(VALUE_COLUMN.to_owned(), Cell::Value(Value::Binary(value)))]),
                    custody_group: None,
                    protected_columns: Vec::new(),
                    custody_payload: None,
                }),
                None => deletes.push(id),
            }
        }
        let mut ops = Vec::new();
        if !rows.is_empty() {
            ops.push(Op::Put { branch: MAIN_BRANCH.into(), table: table.clone(), rows, if_absent: false });
        }
        if !deletes.is_empty() {
            ops.push(Op::Delete { branch: MAIN_BRANCH.into(), table, ids: deletes, valid: None });
        }
        let transaction = Transact {
            key: request.key,
            require: Vec::new(),
            window: Window::default(),
            guards,
            ops,
            otherwise: Vec::new(),
            requires: Vec::new(),
            custody_request_digest: None,
            request_memory: Some(call.request_reservation()),
        };
        let receipt = driver.transact(transaction, &call.cancellation).await.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        let outcome = match receipt.outcome {
            Outcome::Then => proto::append_reply::Outcome::Committed(receipt.basis.position()),
            Outcome::Otherwise { failed } => {
                let first = failed.first().copied().ok_or_else(|| Status::internal("A failed append names no read"))?;
                proto::append_reply::Outcome::Conflict(u32::try_from(first).map_err(|_| Status::internal("Read index overflow"))?)
            }
            Outcome::Refused { error } => return Err(error::anomaly(error)),
        };
        Ok(proto::AppendReply { incarnation: incarnation.to_vec(), outcome: Some(outcome) }.encode_to_vec())
    }

    async fn store_watch(&self, call: &Call, request: proto::WatchRequest) -> Result<Vec<u8>, Status> {
        if request.ranges.is_empty() {
            return Err(error::incorrect("A watch names at least one range"));
        }
        if request.incarnation.is_empty() {
            return Err(error::incorrect("A watch states its incarnation"));
        }
        let (database, incarnation) = self.store_database(call, DatabaseAccess::Read, Some(&request.incarnation)).await?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported("rtdb.store.v1"))?;
        let watched = ranges(request.ranges)?;
        let wait = Duration::from_millis(request.wait_millis).min(MAX_WAIT);
        let deadline = tokio::time::Instant::now() + wait;
        let mut after = Basis::new(incarnation, request.after);
        let touched = loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let page = driver.changes(Changes {
                branch: MAIN_BRANCH.into(),
                tables: [store_table()].into(),
                after: Some(after),
                until: None,
                order: ChangeOrder::Ascending,
                images: ChangeImages::Keys,
                limit: MAX_CHANGE_GROUPS as usize,
                wait: remaining,
            }, &call.cancellation).await.map_err(error::anomaly)?;
            let hit = page.groups.iter().find(|group| group.rows.iter().any(|row| match &row.id {
                EntityId::Binary(key) => watched.iter().any(|range| range.contains(key)),
                _ => false,
            })).map(|group| group.cut.basis);
            if let Some(hit) = hit {
                after = hit;
                break Some(hit.position());
            }
            let progressed = page.through > after;
            after = after.max(page.through);
            if remaining.is_zero() || !progressed && page.complete {
                break None;
            }
        };
        self.check_attachment(call, &database)?;
        Ok(proto::WatchReply {
            incarnation: incarnation.to_vec(),
            touched,
            through: after.position(),
        }.encode_to_vec())
    }
}
