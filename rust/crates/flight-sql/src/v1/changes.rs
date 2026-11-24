use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use arrow_array::{BinaryArray, BooleanArray, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_flight::FlightData;
use arrow_schema::{DataType, Field, Schema, TimeUnit as ArrowTime};
use rtdb_node::{Database, PreparedSql, ResultSignaturePolicy};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_types::{TableRef, TimeUnit, Timestamp, Value};
use tonic::{Response, Status};

use crate::{
    service::{Call, Output, Service},
    wire,
};

use super::{MAX_CHANGE_GROUPS, MAX_WAIT, error, head, proto, tokens::cursor_expired, trailer};

struct Request {
    tables: BTreeSet<TableRef>,
    after: Timestamp,
    until: Option<Timestamp>,
    descending: bool,
    images: proto::Images,
    limit: usize,
    wait: Duration,
}

struct Page {
    frames: BTreeMap<Timestamp, Vec<(usize, Vec<FlightData>)>>,
    readers: Vec<QueryCursor>,
    through: Timestamp,
    complete: bool,
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
        let (database, page, descending) = self
            .open_change_page(&call, request)
            .await
            .map_err(|status| call.error(status))?;
        let mut entries = page.frames.into_iter().collect::<Vec<_>>();
        if descending {
            entries.reverse();
        }
        let through = self
            .0
            .v1
            .tokens
            .basis(database.wire_incarnation(), page.through);
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
            for (_, rows) in entries {
                for (reader, frames) in rows {
                    for frame in frames {
                        service.check_attachment(&call, &database)?;
                        call.handoff(page.readers[reader].delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
                        yield frame;
                    }
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
    ) -> Result<(Arc<Database>, Page, bool), Status> {
        let database = self
            .wire_database(call, &request.branch, proto::Layer::Merged)
            .await?;
        let scope = database.wire_incarnation();
        let tokens = &self.0.v1.tokens;
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
        let read_basis = |basis| {
            tokens
                .read_basis(scope, basis)
                .map_err(|_| cursor_expired())
        };
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
        let request = Request {
            tables,
            after,
            until,
            wait,
            images,
            descending: order == proto::Order::Descending,
            limit: request.limit as usize,
        };
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
            let page = change_page(context, &request, upper, call.memory()).await?;
            if !page.frames.is_empty()
                || request.until.is_some_and(|until| until <= present)
                || tokio::time::Instant::now() >= deadline
            {
                return Ok((database, page, request.descending));
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
                Err(_) => return Ok((database, page, request.descending)),
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
    memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<Page, Status> {
    let schema = schema();
    let mut encoder = wire::Encoder::new(&schema, memory).map_err(error::map_status)?;
    let mut frames = BTreeMap::<Timestamp, Vec<_>>::new();
    let mut readers = Vec::new();
    let direction = if request.descending { "DESC" } else { "ASC" };
    for table in &request.tables {
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
            let mut groups = 0;
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
                        let image_projection = (0..batch.num_columns())
                            .filter(|index| *index != time_index)
                            .collect::<Vec<_>>();
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
                                groups += 1;
                                last_time = Some(time);
                            }
                            if groups > request.limit {
                                frames.entry(time).or_default();
                                more = true;
                                break;
                            }
                            let key = key_index
                                .map(|index| write_image(batch, row, &[index], memory))
                                .transpose()?;
                            let image = image
                                .then(|| write_image(batch, row, &image_projection, memory))
                                .transpose()?;
                            let opened = matches!(boundary, Boundary::Opened);
                            let (before, after) =
                                if opened { (None, image) } else { (image, None) };
                            let table_name = format!("{}.{}", table.schema, table.table);
                            let batch = RecordBatch::try_new(
                                Arc::new(schema.clone()),
                                vec![
                                    Arc::new(StringArray::from(vec![table_name])),
                                    Arc::new(
                                        TimestampMicrosecondArray::from(vec![time.0])
                                            .with_timezone("UTC"),
                                    ),
                                    Arc::new(BooleanArray::from(vec![opened])),
                                    Arc::new(BinaryArray::from(vec![key.as_deref()])),
                                    Arc::new(BinaryArray::from(vec![before.as_deref()])),
                                    Arc::new(BinaryArray::from(vec![after.as_deref()])),
                                ],
                            )
                            .map_err(|e| error::map_status(Status::internal(e.to_string())))?;
                            let data = encoder.batch(&batch, memory).map_err(error::map_status)?;
                            frames.entry(time).or_default().push((reader_index, data));
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
    let has_more = frames.len() > request.limit;
    while frames.len() > request.limit {
        if request.descending {
            frames.pop_first();
        } else {
            frames.pop_last();
        }
    }
    let through = if request.descending {
        if has_more {
            Timestamp(
                frames
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
        *frames.last_key_value().expect("nonempty change page").0
    } else {
        upper
    };
    let complete = !has_more && request.until.is_none_or(|until| until <= upper);
    Ok(Page {
        frames,
        readers,
        through,
        complete,
    })
}

fn write_image(
    batch: &RecordBatch,
    row: usize,
    projection: &[usize],
    memory: &Arc<rtdb_columnar::MemoryBudget>,
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
