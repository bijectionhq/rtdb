use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use arrow_array::{
    Array, BinaryArray, BooleanArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    cast::AsArray, types::TimestampMicrosecondType,
};
use arrow_flight::FlightData;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit as ArrowTime};
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::{Database, PreparedSql, ResultSignaturePolicy};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_types::{TableRef, TimeUnit, Timestamp, Value};
use tonic::{Response, Status};

use crate::{
    service::{Call, Output, Service},
    wire,
};

use super::{
    MAX_CHANGE_GROUPS, MAX_WAIT, error, head, proto,
    tokens::{self, cursor_expired},
    trailer,
};

pub(super) const PROGRESS_ONLY: &str = "changes.progress-only";
const MAX_PACKED_BYTES: usize = 1 << 20;

struct Request {
    tables: BTreeSet<TableRef>,
    after: Timestamp,
    until: Option<Timestamp>,
    descending: bool,
    images: proto::Images,
    progress_only: bool,
    limit: usize,
    wait: Duration,
}

struct Page {
    frames: Vec<(usize, Vec<FlightData>)>,
    readers: Vec<QueryCursor>,
    has_changes: bool,
    through: Timestamp,
    complete: bool,
}

struct ReservedRow {
    batch: RecordBatch,
    charge: Arc<MemoryReservation>,
}

enum ChangeRow {
    Reserved(ReservedRow),
    Encoded(Vec<FlightData>),
}

#[derive(Clone, Copy)]
enum Boundary {
    Opened,
    Closed,
}

impl Boundary {
    fn column(self) -> &'static str {
        match self {
            Self::Opened => "_system_from",
            Self::Closed => "_system_to",
        }
    }
}

impl Service {
    pub(super) async fn v1_changes(
        &self,
        call: Call,
        request: proto::ChangesRequest,
    ) -> Result<Response<Output<FlightData>>, Status> {
        let (database, page) = self
            .open_change_page(&call, request)
            .await
            .map_err(|status| call.error(status))?;
        let through = tokens::basis(database.wire_incarnation(), page.through).into();
        let service = self.clone();
        let schema = schema();
        let first = page.readers[0]
            .delivery_grant()
            .await
            .map_err(error::anomaly)?;
        let (headers, cutoff) = call
            .delivery_headers(&first, true)
            .map_err(error::map_status)?;
        let stream: Output<FlightData> = Box::pin(async_stream::try_stream! {
            service.check_attachment(&call, &database)?;
            call.handoff(first, cutoff).map_err(error::map_status)?;
            yield wire::schema_data(&schema, call.memory()).map_err(error::map_status)?;
            for (reader, frames) in page.frames {
                for frame in frames {
                    service.check_attachment(&call, &database)?;
                    call.handoff(page.readers[reader].delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
                    yield frame;
                }
            }
            service.check_attachment(&call, &database)?;
            for reader in page.readers {
                call.handoff(reader.delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
            }
            yield trailer(proto::ChangesTrailer { through: Some(through), complete: page.complete });
        });
        let mut response = Response::new(stream);
        *response.metadata_mut() = headers;
        Ok(response)
    }

    async fn open_change_page(
        &self,
        call: &Call,
        request: proto::ChangesRequest,
    ) -> Result<(Arc<Database>, Page), Status> {
        let database = self
            .wire_database(call, &request.branch, proto::Layer::Merged)
            .await?;
        let scope = database.wire_incarnation();
        let tables = request
            .tables
            .iter()
            .map(|name| {
                if name.len() > 512 {
                    return Err(error::incorrect("Table name is too long"));
                }
                TableRef::parse(name).map_err(error::anomaly)
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if !(1..=16).contains(&tables.len()) || request.tables.len() != tables.len() {
            return Err(error::incorrect(
                "Changes requires one to sixteen distinct tables",
            ));
        }
        let read_basis = |basis| tokens::read_basis(scope, basis).map_err(|_| cursor_expired());
        let after = request
            .after
            .as_ref()
            .map(read_basis)
            .transpose()?
            .unwrap_or(Timestamp::MIN);
        let until = request.until.as_ref().map(read_basis).transpose()?;
        let wait = Duration::from_millis(request.wait_millis.unwrap_or_default());
        let order = proto::Order::try_from(request.order)
            .map_err(|_| error::incorrect("Unknown change order"))?;
        let images = proto::Images::try_from(request.images)
            .map_err(|_| error::incorrect("Unknown change images"))?;
        if wait > MAX_WAIT
            || !(1..=MAX_CHANGE_GROUPS).contains(&request.limit)
            || until.is_some_and(|until| until < after)
        {
            return Err(error::incorrect("Invalid Changes interval, limit or wait"));
        }
        if request.progress_only && images != proto::Images::None {
            return Err(error::incorrect("Progress-only Changes requires IMAGES_NONE"));
        }
        let request = Request {
            tables,
            after,
            until,
            wait,
            images,
            progress_only: request.progress_only,
            descending: order == proto::Order::Descending,
            limit: request.limit as usize,
        };
        let pack_limit = self.0.config.maximum_message_bytes.min(MAX_PACKED_BYTES);
        let deadline = tokio::time::Instant::now() + request.wait;
        loop {
            let context = self.wire_context(call, &database, false).await?;
            let present = head(&context, &call.database)?;
            let catalog = context
                .catalog
                .database(&call.database)
                .map_err(error::anomaly)?;
            let snapshot = &catalog.partitions[0];
            let floor = snapshot
                .table_catalog()
                .block()
                .and_then(|block| block.latest_completed_tx())
                .map_or(Timestamp::MIN, |tx| tx.system_time);
            if request.after < floor {
                return Err(cursor_expired());
            }
            if request.after > present {
                return Err(error::unavailable(
                    "Database has not applied the Changes cursor",
                ));
            }
            let upper = request.until.unwrap_or(present).min(present);
            let page = change_page(context, &request, upper, call.memory(), pack_limit).await?;
            if page.has_changes
                || request.until.is_some_and(|until| until <= present)
                || tokio::time::Instant::now() >= deadline
            {
                return Ok((database, page));
            }
            let next = Timestamp(present.0.saturating_add(1));
            let update = tokio::time::timeout_at(
                deadline,
                database
                    .progress()
                    .await_system_time(next, &call.cancellation),
            )
            .await;
            match update {
                Ok(Ok(_)) => continue,
                Ok(Err(anomaly)) => return Err(error::anomaly(anomaly)),
                Err(_) => return Ok((database, page)),
            }
        }
    }
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("table", DataType::Utf8, false),
        Field::new(
            "system_time",
            DataType::Timestamp(ArrowTime::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("opened", DataType::Boolean, false),
        Field::new("key_ipc", DataType::Binary, true),
        Field::new("before_ipc", DataType::Binary, true),
        Field::new("after_ipc", DataType::Binary, true),
    ])
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn time_value(time: Timestamp) -> Value {
    Value::Timestamp {
        value: time.0,
        unit: TimeUnit::Microsecond,
        timezone: Some("UTC".into()),
    }
}

async fn change_page(
    context: Arc<QueryContext>,
    request: &Request,
    upper: Timestamp,
    memory: &Arc<MemoryBudget>,
    pack_limit: usize,
) -> Result<Page, Status> {
    let schema = Arc::new(schema());
    let mut encoder = (!request.progress_only)
        .then(|| wire::Encoder::new(&schema, memory))
        .transpose()
        .map_err(error::map_status)?;
    let mut groups = BTreeMap::<Timestamp, Vec<_>>::new();
    let mut readers = Vec::new();
    let direction = if request.descending { "DESC" } else { "ASC" };
    for table in &request.tables {
        let table_name = format!("{}.{}", table.schema, table.table);
        for boundary in [Boundary::Opened, Boundary::Closed] {
            let name = format!("{}.{}", quote(&table.schema), quote(&table.table));
            let column = boundary.column();
            let image = match boundary {
                Boundary::Opened => {
                    matches!(request.images, proto::Images::After | proto::Images::Both)
                }
                Boundary::Closed => {
                    matches!(request.images, proto::Images::Before | proto::Images::Both)
                }
            };
            let projection = if image {
                ", _id, _valid_from, _valid_to, *"
            } else if request.images != proto::Images::None {
                ", _id"
            } else {
                ""
            };
            let sql = format!(
                "SELECT {column}{projection} FROM {name} FOR ALL VALID_TIME FOR ALL SYSTEM_TIME WHERE {column} > $1 AND {column} <= $2 ORDER BY {column} {direction}"
            );
            let prepared = PreparedSql::prepare(
                &sql,
                &context,
                Default::default(),
                ResultSignaturePolicy::Dynamic,
            )
            .map_err(error::anomaly)?;
            let mut cursor = prepared
                .open_query(
                    context.clone(),
                    vec![time_value(request.after), time_value(upper)],
                )
                .await
                .map_err(error::anomaly)?;
            let reader_index = readers.len();
            let mut reader_groups = 0;
            let mut last_time = None;
            let mut more = false;
            while !more
                && let Some(()) = cursor
                    .encode_next_batch(|batch| -> Result<(), Status> {
                        let batch = batch.batch();
                        let projection = batch.schema_ref();
                        let (time_index, time_field) =
                            projection.column_with_name(column).ok_or_else(|| {
                                error::map_status(Status::internal(
                                    "Changes projection omitted system time",
                                ))
                            })?;
                        let key_index = projection.column_with_name("_id").map(|(index, _)| index);
                        let image_projection = if image {
                            (0..batch.num_columns())
                                .filter(|index| *index != time_index)
                                .collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        };
                        for row in 0..batch.num_rows() {
                            let value = rtdb_columnar::read_value(
                                time_field,
                                batch.column(time_index).as_ref(),
                                row,
                            )
                            .map_err(error::anomaly)?;
                            let time = rtdb_expr::timestamp_micros(&value, &context.timezone)
                                .map_err(error::anomaly)?;
                            if last_time != Some(time) {
                                reader_groups += 1;
                                last_time = Some(time);
                            }
                            let rows = groups.entry(time).or_default();
                            if reader_groups > request.limit {
                                more = true;
                                break;
                            }
                            let Some(encoder) = encoder.as_mut() else {
                                continue;
                            };
                            let key = key_index
                                .map(|index| write_image(batch, row, &[index], memory))
                                .transpose()?;
                            let image = image
                                .then(|| write_image(batch, row, &image_projection, memory))
                                .transpose()?;
                            let opened = matches!(boundary, Boundary::Opened);
                            let (before, after) =
                                if opened { (None, image) } else { (image, None) };
                            let batch = outer_row(
                                Arc::clone(&schema),
                                &table_name,
                                time,
                                opened,
                                [key.as_deref(), before.as_deref(), after.as_deref()],
                            )
                            .map_err(|e| error::map_status(Status::internal(e.to_string())))?;
                            let charge =
                                wire::reserve_batch(&batch, memory).map_err(error::map_status)?;
                            // Hold the singleton charge through the merge so later reads see the same admission.
                            let row = if charge.bytes() <= pack_limit {
                                ChangeRow::Reserved(ReservedRow { batch, charge })
                            } else {
                                let data = encoder
                                    .reserved_batch(&batch, charge)
                                    .map_err(error::map_status)?;
                                ChangeRow::Encoded(data)
                            };
                            rows.push((reader_index, row));
                        }
                        Ok(())
                    })
                    .await
                    .map_err(error::anomaly)?
                    .transpose()?
            {}
            readers.push(cursor);
        }
    }
    let has_changes = !groups.is_empty();
    let has_more = groups.len() > request.limit;
    while groups.len() > request.limit {
        if request.descending {
            groups.pop_first();
        } else {
            groups.pop_last();
        }
    }
    let through = if request.descending {
        if has_more {
            Timestamp(
                groups
                    .first_key_value()
                    .expect("nonempty change page")
                    .0
                    .0
                    .saturating_sub(1),
            )
        } else {
            request.after
        }
    } else if has_more {
        *groups.last_key_value().expect("nonempty change page").0
    } else {
        upper
    };
    let complete = !has_more && request.until.is_none_or(|until| until <= upper);
    let frames = match encoder {
        Some(encoder) => encode_page(groups, request.descending, encoder, memory, pack_limit)?,
        None => Vec::new(),
    };
    Ok(Page {
        frames,
        readers,
        has_changes,
        through,
        complete,
    })
}

fn outer_row(
    schema: SchemaRef,
    table: &str,
    time: Timestamp,
    opened: bool,
    [key, before, after]: [Option<&[u8]>; 3],
) -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![table])),
            Arc::new(TimestampMicrosecondArray::from(vec![time.0]).with_timezone("UTC")),
            Arc::new(BooleanArray::from(vec![opened])),
            Arc::new(BinaryArray::from(vec![key])),
            Arc::new(BinaryArray::from(vec![before])),
            Arc::new(BinaryArray::from(vec![after])),
        ],
    )
}

fn encode_page(
    groups: BTreeMap<Timestamp, Vec<(usize, ChangeRow)>>,
    descending: bool,
    mut encoder: wire::Encoder,
    memory: &Arc<MemoryBudget>,
    pack_limit: usize,
) -> Result<Vec<(usize, Vec<FlightData>)>, Status> {
    let mut groups = groups.into_values();
    let mut rows = std::iter::from_fn(|| {
        if descending {
            groups.next_back()
        } else {
            groups.next()
        }
    })
    .flatten()
    .peekable();
    let mut frames = Vec::new();
    while let Some((reader, row)) = rows.next() {
        let first = match row {
            ChangeRow::Encoded(data) => {
                frames.push((reader, data));
                continue;
            }
            ChangeRow::Reserved(row) => row,
        };
        let mut bound = first.charge.bytes();
        let mut run = vec![first];
        while let Some((_, ChangeRow::Reserved(row))) = rows.next_if(|(next_reader, next)| {
            *next_reader == reader
                && matches!(next, ChangeRow::Reserved(row) if row.charge.bytes() <= pack_limit - bound)
        }) {
            bound += row.charge.bytes();
            run.push(row);
        }
        if run.len() > 1
            && let Some(data) = pack_rows(&run, bound, &mut encoder, memory, pack_limit)
        {
            frames.push((reader, data));
        } else {
            for row in run {
                let data = encoder
                    .reserved_batch(&row.batch, row.charge)
                    .map_err(error::map_status)?;
                frames.push((reader, data));
            }
        }
    }
    Ok(frames)
}

fn pack_rows(
    rows: &[ReservedRow],
    bound: usize,
    encoder: &mut wire::Encoder,
    memory: &Arc<MemoryBudget>,
    pack_limit: usize,
) -> Option<Vec<FlightData>> {
    // Packing is optional under memory pressure. The single-row fallback already holds its charges.
    let _scratch = memory.reserve(bound).ok()?;
    let batch = pack_batch(rows[0].batch.schema(), rows).ok()?;
    let frames = encoder.batch(&batch, memory).ok()?;
    frames
        .iter()
        .all(|frame| frame.encoded_len() <= pack_limit)
        .then_some(frames)
}

fn pack_batch(schema: SchemaRef, rows: &[ReservedRow]) -> Result<RecordBatch, ArrowError> {
    let tables = rows
        .iter()
        .map(|row| row.batch.column(0).as_string::<i32>().value(0));
    let times = rows
        .iter()
        .map(|row| row.batch.column(1).as_primitive::<TimestampMicrosecondType>().value(0));
    let opened = rows
        .iter()
        .map(|row| row.batch.column(2).as_boolean().value(0));
    let images = |column| -> BinaryArray {
        rows.iter()
            .map(|row| {
                let array = row.batch.column(column).as_binary::<i32>();
                array.is_valid(0).then(|| array.value(0))
            })
            .collect()
    };
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(tables)),
            Arc::new(TimestampMicrosecondArray::from_iter_values(times).with_timezone("UTC")),
            Arc::new(opened.collect::<BooleanArray>()),
            Arc::new(images(3)),
            Arc::new(images(4)),
            Arc::new(images(5)),
        ],
    )
}

fn write_image(
    batch: &RecordBatch,
    row: usize,
    projection: &[usize],
    memory: &Arc<MemoryBudget>,
) -> Result<bytes::Bytes, Status> {
    let batch = batch
        .slice(row, 1)
        .project(projection)
        .map_err(|e| error::map_status(Status::internal(e.to_string())))?;
    let size = wire::add(
        wire::mul(batch.get_array_memory_size(), 4).map_err(error::map_status)?,
        wire::schema_bound(batch.schema().as_ref()).map_err(error::map_status)?,
    )
    .map_err(error::map_status)?;
    let charge = memory.reserve(size).map_err(error::anomaly)?;
    let bytes = rtdb_format::ipc::write_stream(batch.schema().as_ref(), &[batch])
        .map_err(error::anomaly)?;
    Ok(wire::retain(bytes, charge))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_rows_fall_back_to_singletons_when_packing_has_no_scratch() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Arc::new(schema());
        let encoder = wire::Encoder::new(&schema, &memory).unwrap();
        let time = Timestamp(1);
        let rows = (0..2)
            .map(|_| {
                let batch = outer_row(Arc::clone(&schema), "public.rows", time, true, [None; 3])
                    .unwrap();
                let charge = wire::reserve_batch(&batch, &memory).unwrap();
                (0, ChangeRow::Reserved(ReservedRow { batch, charge }))
            })
            .collect();
        let filler = memory.reserve(memory.available_bytes()).unwrap();
        let frames = encode_page(
            BTreeMap::from([(time, rows)]),
            false,
            encoder,
            &memory,
            MAX_PACKED_BYTES,
        )
        .unwrap();
        assert_eq!(frames.len(), 2);
        assert!(
            frames
                .iter()
                .all(|(reader, frames)| *reader == 0 && frames.len() == 1)
        );
        drop((frames, filler));
        assert_eq!(memory.used_bytes(), 0);
    }
}
