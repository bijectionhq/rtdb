use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use arrow_array::{
    BinaryArray, BooleanArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_flight::FlightData;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit as ArrowTime};
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::ipc::RowStreamEncoder;
use rtdb_node::serving::{ChangeImages, ChangeKind, ChangeOrder, Changes};
use rtdb_node::{Database, PreparedSql, ResultSignaturePolicy};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_types::{StructFields, TableRef, TimeUnit, Timestamp, Value, rtdb_v1::Basis};
use tonic::{
    Response, Status,
    metadata::{MetadataMap, MetadataValue},
};

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
pub(super) const SHARED_IMAGES: &str = "changes.images.stream-v1";
const IMAGE_HEADER: &str = "x-rtdb-changes-images";
const IMAGE_STREAM: &str = "stream-v1";
const IMAGE_END: [u8; 4] = [0; 4];
const MAX_PACKED_BYTES: usize = 1 << 20;

struct Request {
    tables: BTreeSet<TableRef>,
    after: Timestamp,
    until: Option<Timestamp>,
    descending: bool,
    images: proto::Images,
    progress_only: bool,
    shared_images: bool,
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
    table: Arc<str>,
    time: Timestamp,
    opened: bool,
    images: [Option<Image>; 3],
    charge: Arc<MemoryReservation>,
}

impl ReservedRow {
    fn encode(
        self,
        schema: &SchemaRef,
        encoder: &mut wire::Encoder,
    ) -> Result<Vec<FlightData>, Status> {
        let batch = pack_batch(Arc::clone(schema), std::slice::from_ref(&self)).map_err(batch_error)?;
        let Self { images, charge, .. } = self;
        drop(images);
        encoder
            .reserved_batch(&batch, charge)
            .map_err(error::map_status)
    }

    fn shares_streams_with(&self, other: &Self) -> bool {
        self.images
            .iter()
            .zip(&other.images)
            .all(|(left, right)| match (left, right) {
                (None, None) => true,
                (Some(left), Some(right)) => left.shares_schema_with(right),
                _ => false,
            })
    }
}

struct Image {
    bytes: Vec<u8>,
    stream: Option<Arc<RowStreamEncoder>>,
}

impl Image {
    fn legacy_len(&self) -> usize {
        let framing = self.stream.as_ref().map_or(0, |stream| {
            stream.schema_frame().len() + IMAGE_END.len()
        });
        self.bytes.len() + framing
    }

    fn shares_schema_with(&self, other: &Self) -> bool {
        let (Some(left), Some(right)) = (&self.stream, &other.stream) else {
            return false;
        };
        !left.has_dictionaries() && !right.has_dictionaries()
            && (Arc::ptr_eq(left, right) || left.schema_frame() == right.schema_frame())
    }
}

enum ImageWriter {
    Binary,
    Stream(Option<Arc<RowStreamEncoder>>),
}

impl ImageWriter {
    fn new(shared_images: bool) -> Self {
        if shared_images {
            Self::Stream(None)
        } else {
            Self::Binary
        }
    }
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
        metadata: &MetadataMap,
    ) -> Result<Response<Output<FlightData>>, Status> {
        let offered = metadata.get_all(IMAGE_HEADER);
        let mut offered = offered.iter();
        let shared_images = !request.progress_only
            && request.images != proto::Images::None as i32
            && offered.next().is_some_and(|value| value == IMAGE_STREAM)
            && offered.next().is_none();
        let database = self
            .wire_database(&call, &request.branch, proto::Layer::Merged)
            .await
            .map_err(|status| call.error(status))?;
        if database.serving_driver().is_some() {
            return self.applied_changes(call, database, request).await;
        }
        let (database, page) = self
            .open_change_page(&call, database, request, shared_images)
            .await
            .map_err(|status| call.error(status))?;
        let scope = database.wire_incarnation().await.map_err(error::anomaly)?;
        let through = tokens::basis(scope, page.through).into();
        let service = self.clone();
        let schema = schema();
        let first = page.readers[0]
            .delivery_grant()
            .await
            .map_err(error::anomaly)?;
        let (mut headers, cutoff) = call
            .delivery_headers(&first, true)
            .map_err(error::map_status)?;
        if shared_images {
            headers.insert(IMAGE_HEADER, MetadataValue::from_static(IMAGE_STREAM));
        }
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
        database: Arc<Database>,
        request: proto::ChangesRequest,
        shared_images: bool,
    ) -> Result<(Arc<Database>, Page), Status> {
        let scope = database.wire_incarnation().await.map_err(error::anomaly)?;
        let tables = tables(&request.tables)?;
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
            shared_images,
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

    async fn applied_changes(
        &self,
        call: Call,
        database: Arc<Database>,
        request: proto::ChangesRequest,
    ) -> Result<Response<Output<FlightData>>, Status> {
        let progress_only = request.progress_only;
        let (page, images, context, initial, headers, cutoff) = async {
            let driver = database.serving_driver().expect("checked serving driver");
            let visibility = self.serving_scope(&call, driver)?;
            let incarnation = visibility.as_ref().map_or_else(|| driver.incarnation(), |scope| {
                scope.labels.main_read_incarnation(driver.incarnation())
            });
            let tables = tables(&request.tables)?;
            let read_basis = |basis: proto::Basis| {
                Basis::from_bytes(&basis.value)
                    .ok()
                    .filter(|basis| basis.incarnation() == incarnation)
                    .ok_or_else(cursor_expired)
            };
            let after = request.after.map(read_basis).transpose()?;
            let until = request.until.map(read_basis).transpose()?;
            let wait = Duration::from_millis(request.wait_millis.unwrap_or_default());
            if wait > MAX_WAIT
                || !(1..=MAX_CHANGE_GROUPS).contains(&request.limit)
                || after.zip(until).is_some_and(|(after, until)| until < after)
            {
                return Err(error::incorrect("Invalid Changes interval, limit or wait"));
            }
            let order = match proto::Order::try_from(request.order)
                .map_err(|_| error::incorrect("Unknown change order"))?
            {
                proto::Order::Ascending => ChangeOrder::Ascending,
                proto::Order::Descending => ChangeOrder::Descending,
            };
            let images = match proto::Images::try_from(request.images)
                .map_err(|_| error::incorrect("Unknown change images"))?
            {
                proto::Images::None => ChangeImages::None,
                proto::Images::Keys => ChangeImages::Keys,
                proto::Images::Before => ChangeImages::Before,
                proto::Images::After => ChangeImages::After,
                proto::Images::Both => ChangeImages::Both,
            };
            if progress_only && images != ChangeImages::None {
                return Err(error::incorrect("Progress-only Changes requires IMAGES_NONE"));
            }
            let change = Changes {
                        branch: request.branch,
                        tables,
                        after,
                        until,
                        order,
                        images,
                        limit: request.limit as usize,
                        wait,
            };
            let (page, context) = if let Some(scope) = &visibility {
                let now = self.0.node.now();
                let page = driver.changes_scoped(change, scope, &call.cancellation).await.map_err(error::anomaly)?;
                let source = rtdb_node::serving::QuerySource {
                    branch: rtdb_types::rtdb_v1::MAIN_BRANCH.into(), at: rtdb_types::rtdb_v1::At::Latest,
                    layer: rtdb_types::rtdb_v1::Layer::Merged,
                };
                let view = driver.query_scoped(source, false, now, scope, &call.cancellation).await.map_err(error::anomaly)?;
                let snapshot = rtdb_query::DatabaseSnapshot { partitions: vec![view.snapshot], storage: Arc::clone(database.storage()) };
                (page, self.scoped_wire_context(&call, snapshot, now)?.with_plaintext())
            } else {
                let context = self.wire_context(&call, &database, false).await?.with_plaintext();
                let page = context.reading(driver.changes(change, &call.cancellation)).await.map_err(error::anomaly)?;
                (page, context)
            };
            self.check_attachment(&call, &database)?;
            let initial = context.delivery_grant().await.map_err(error::anomaly)?;
            let (headers, cutoff) = call
                .delivery_headers(&initial, true)
                .map_err(error::map_status)?;
            Ok((page, images, context, initial, headers, cutoff))
        }
        .await
        .map_err(|status| call.error(status))?;
        let mut fields = schema().fields().to_vec();
        fields.push(Arc::new(Field::new("erased", DataType::Boolean, false)));
        let schema = Arc::new(Schema::new(fields));
        let service = self.clone();
        let memory = Arc::clone(call.memory());
        let stream: Output<FlightData> = Box::pin(async_stream::try_stream! {
            let mut encoder = wire::Encoder::new(&schema, &memory).map_err(error::map_status)?;
            service.check_attachment(&call, &database)?;
            call.handoff(initial, cutoff).map_err(error::map_status)?;
            yield wire::schema_data(&schema, &memory).map_err(error::map_status)?;
            for group in page.groups.into_iter().filter(|_| !progress_only) {
                for row in group.rows {
                    service.check_attachment(&call, &database)?;
                    let erased = row.kind == ChangeKind::Erased;
                    let id = row.id.as_value();
                    let key = (images != ChangeImages::None).then(|| write_values([("_id", &id)], &memory)).transpose()?;
                    let before = if erased || !matches!(images, ChangeImages::Before | ChangeImages::Both) { None }
                        else { row.before.as_ref().map(|cells| write_change_image(cells, &id, row.valid, &memory)).transpose()? };
                    let after = if erased || !matches!(images, ChangeImages::After | ChangeImages::Both) { None }
                        else { row.after.as_ref().map(|cells| write_change_image(cells, &id, row.valid, &memory)).transpose()? };
                    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
                        Arc::new(StringArray::from(vec![format!("{}.{}", row.table.schema, row.table.table)])),
                        Arc::new(TimestampMicrosecondArray::from(vec![group.cut.system_time.0]).with_timezone("UTC")),
                        Arc::new(BooleanArray::from(vec![row.kind == ChangeKind::Opened])),
                        Arc::new(BinaryArray::from(vec![key.as_deref()])),
                        Arc::new(BinaryArray::from(vec![before.as_deref()])),
                        Arc::new(BinaryArray::from(vec![after.as_deref()])),
                        Arc::new(BooleanArray::from(vec![erased])),
                    ]).map_err(|error| error::map_status(Status::internal(error.to_string())))?;
                    for frame in encoder.batch(&batch, &memory).map_err(error::map_status)? {
                        call.handoff(context.delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
                        yield frame;
                    }
                }
            }
            service.check_attachment(&call, &database)?;
            call.handoff(context.delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
            yield trailer(proto::ChangesTrailer { through: Some(page.through.into()), complete: page.complete });
        });
        let mut response = Response::new(stream);
        *response.metadata_mut() = headers;
        Ok(response)
    }
}

fn write_change_image(
    cells: &StructFields<Value>,
    id: &Value,
    valid: Option<rtdb_types::Interval>,
    memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<bytes::Bytes, Status> {
    let from = valid.map(|valid| time_value(valid.lower()));
    let to = valid.map(|valid| time_value(valid.upper()));
    let values = [
        ("_id", Some(id)),
        ("_valid_from", from.as_ref()),
        ("_valid_to", to.as_ref()),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|value| (name, value)))
    .chain(cells.iter().filter_map(|(name, value)| {
        (!matches!(name.as_str(), "_id" | "_valid_from" | "_valid_to"))
            .then_some((name.as_str(), value))
    }));
    write_values(values, memory)
}

fn write_values<'a>(
    values: impl IntoIterator<Item = (&'a str, &'a Value)>,
    memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<bytes::Bytes, Status> {
    let values = values.into_iter().collect::<Vec<_>>();
    let workspace = values.iter().try_fold(0, |size, (name, value)| {
        let bytes = value.retained_bytes().map_err(error::anomaly)?;
        let field = wire::add(name.len(), bytes).map_err(error::map_status)?;
        wire::add(size, field).map_err(error::map_status)
    })?;
    let _workspace = memory
        .reserve(wire::mul(workspace, 4).map_err(error::map_status)?)
        .map_err(error::anomaly)?;
    let (fields, columns) = values
        .into_iter()
        .map(|(name, value)| {
            let ty = value.logical_type();
            Ok((
                rtdb_columnar::to_field(name, &ty).map_err(error::anomaly)?,
                rtdb_columnar::build_array_refs(&ty, &[value]).map_err(error::anomaly)?,
            ))
        })
        .collect::<Result<Vec<_>, Status>>()?
        .into_iter()
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| error::map_status(Status::internal(error.to_string())))?;
    write_image(
        &batch,
        0,
        &(0..batch.num_columns()).collect::<Vec<_>>(),
        memory,
    )
}

fn tables(names: &[String]) -> Result<BTreeSet<TableRef>, Status> {
    if !(1..=16).contains(&names.len()) {
        return Err(error::incorrect(
            "Changes requires one to sixteen distinct tables",
        ));
    }
    let tables = names
        .iter()
        .map(|name| {
            if name.len() > 512 {
                return Err(error::incorrect("Table name is too long"));
            }
            TableRef::parse(name).map_err(error::anomaly)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if names.len() != tables.len() {
        return Err(error::incorrect(
            "Changes requires one to sixteen distinct tables",
        ));
    }
    Ok(tables)
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
        let table_name: Arc<str> = format!("{}.{}", table.schema, table.table).into();
        let row_base = encoder
            .as_ref()
            .map(|_| singleton_base(&schema, &table_name))
            .transpose()?;
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
                        let mut key_writer = ImageWriter::new(request.shared_images);
                        let mut image_writer = ImageWriter::new(request.shared_images);
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
                            let (Some(encoder), Some(row_base)) = (encoder.as_mut(), row_base) else {
                                continue;
                            };
                            let (key, _key_scratch) = key_index
                                .map(|index| {
                                    write_image(&mut key_writer, batch, row, &[index], memory)
                                })
                                .transpose()?
                                .unzip();
                            let (image, _image_scratch) = image
                                .then(|| {
                                    write_image(&mut image_writer, batch, row, &image_projection, memory)
                                })
                                .transpose()?
                                .unzip();
                            let opened = matches!(boundary, Boundary::Opened);
                            let (before, after) =
                                if opened { (None, image) } else { (image, None) };
                            let images = [key, before, after];
                            let lengths = images
                                .each_ref()
                                .map(|image| image.as_ref().map(Image::legacy_len));
                            let bound = singleton_bound(row_base, lengths)?;
                            let charge = memory
                                .reserve(bound)
                                .map_err(crate::error::status)
                                .map_err(error::map_status)?;
                            let row = ReservedRow {
                                table: Arc::clone(&table_name),
                                time,
                                opened,
                                images,
                                charge,
                            };
                            let row = if bound <= pack_limit {
                                ChangeRow::Reserved(row)
                            } else {
                                ChangeRow::Encoded(row.encode(&schema, encoder)?)
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
        Some(encoder) => encode_page(
            groups,
            request.descending,
            request.shared_images,
            &schema,
            encoder,
            memory,
            pack_limit,
        )?,
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

fn batch_error(error: ArrowError) -> Status {
    error::map_status(Status::internal(error.to_string()))
}

fn singleton_base(schema: &SchemaRef, table: &str) -> Result<usize, Status> {
    let images = std::array::from_fn(|_| BinaryArray::from(vec![Some(&[][..])]));
    let batch = outer_row(Arc::clone(schema), table, Timestamp(0), false, images)
        .map_err(batch_error)?;
    wire::batch_bound(&batch).map_err(error::map_status)
}

fn singleton_bound(base: usize, image_lengths: [Option<usize>; 3]) -> Result<usize, Status> {
    // Arrow's one-element BinaryArray collect starts with 1024 value bytes.
    // A null adds one 64-byte bitmap, and a single append grows the value Vec at most once.
    const INITIAL_VALUES: usize = 1024;
    let extra = image_lengths.into_iter().try_fold(0, |extra, len| {
        let growth = match len {
            None => 64,
            Some(len) if len > INITIAL_VALUES => len.max(2 * INITIAL_VALUES) - INITIAL_VALUES,
            Some(_) => 0,
        };
        wire::add(extra, growth)
    });
    extra
        .and_then(|extra| wire::add(base, wire::mul(extra, 4)?))
        .map_err(error::map_status)
}

fn outer_row(
    schema: SchemaRef,
    table: &str,
    time: Timestamp,
    opened: bool,
    [key, before, after]: [BinaryArray; 3],
) -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![table])),
            Arc::new(TimestampMicrosecondArray::from(vec![time.0]).with_timezone("UTC")),
            Arc::new(BooleanArray::from(vec![opened])),
            Arc::new(key),
            Arc::new(before),
            Arc::new(after),
        ],
    )
}

fn encode_page(
    groups: BTreeMap<Timestamp, Vec<(usize, ChangeRow)>>,
    descending: bool,
    shared_images: bool,
    schema: &SchemaRef,
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
                && matches!(next, ChangeRow::Reserved(row)
                    if row.charge.bytes() <= pack_limit - bound
                        && (!shared_images || run[0].shares_streams_with(row)))
        }) {
            bound += row.charge.bytes();
            run.push(row);
        }
        if run.len() > 1
            && let Some(data) = pack_rows(schema, &run, bound, &mut encoder, memory, pack_limit)
        {
            frames.push((reader, data));
        } else {
            for row in run {
                frames.push((reader, row.encode(schema, &mut encoder)?));
            }
        }
    }
    Ok(frames)
}

fn pack_rows(
    schema: &SchemaRef,
    rows: &[ReservedRow],
    bound: usize,
    encoder: &mut wire::Encoder,
    memory: &Arc<MemoryBudget>,
    pack_limit: usize,
) -> Option<Vec<FlightData>> {
    // Packing is optional under memory pressure. The single-row fallback already holds its charges.
    let _scratch = memory.reserve(bound).ok()?;
    let batch = pack_batch(Arc::clone(schema), rows).ok()?;
    let frames = encoder.batch(&batch, memory).ok()?;
    frames
        .iter()
        .all(|frame| frame.encoded_len() <= pack_limit)
        .then_some(frames)
}

fn pack_batch(schema: SchemaRef, rows: &[ReservedRow]) -> Result<RecordBatch, ArrowError> {
    let [key, before, after] = [image_array(rows, 0)?, image_array(rows, 1)?, image_array(rows, 2)?];
    if let [row] = rows {
        return outer_row(schema, &row.table, row.time, row.opened, [key, before, after]);
    }
    let tables = rows.iter().map(|row| row.table.as_ref());
    let times = rows.iter().map(|row| row.time.0);
    let opened = rows.iter().map(|row| row.opened);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(tables)),
            Arc::new(TimestampMicrosecondArray::from_iter_values(times).with_timezone("UTC")),
            Arc::new(opened.collect::<BooleanArray>()),
            Arc::new(key),
            Arc::new(before),
            Arc::new(after),
        ],
    )
}

fn image_array(rows: &[ReservedRow], column: usize) -> Result<BinaryArray, ArrowError> {
    let images = rows.iter().map(|row| row.images[column].as_ref());
    let stream = rows[0].images[column]
        .as_ref()
        .and_then(|image| image.stream.as_ref());
    let Some(stream) = stream else {
        return Ok(images.map(|image| image.map(|image| image.bytes.as_slice())).collect());
    };
    let messages = images.flatten().map(|image| image.bytes.as_slice());
    let len = stream.schema_frame().len()
        + messages.clone().map(<[u8]>::len).sum::<usize>()
        + IMAGE_END.len();
    let end = i32::try_from(len).map_err(|_| ArrowError::OffsetOverflowError(len))?;
    let mut bytes = Vec::with_capacity(len);
    bytes.extend_from_slice(stream.schema_frame());
    for message in messages {
        bytes.extend_from_slice(message);
    }
    bytes.extend_from_slice(&IMAGE_END);
    let mut offsets = vec![end; rows.len() + 1];
    offsets[0] = 0;
    let nulls = (rows.len() > 1).then(|| {
        let mut valid = vec![false; rows.len()];
        valid[0] = true;
        NullBuffer::from(valid)
    });
    BinaryArray::try_new(OffsetBuffer::new(offsets.into()), bytes.into(), nulls)
}

fn write_image(
    writer: &mut ImageWriter,
    batch: &RecordBatch,
    row: usize,
    projection: &[usize],
    memory: &Arc<MemoryBudget>,
) -> Result<(Image, Arc<MemoryReservation>), Status> {
    let batch = batch
        .slice(row, 1)
        .project(projection)
        .map_err(batch_error)?;
    let size = wire::add(
        wire::mul(batch.get_array_memory_size(), 4).map_err(error::map_status)?,
        wire::schema_bound(batch.schema().as_ref()).map_err(error::map_status)?,
    )
    .map_err(error::map_status)?;
    let charge = memory.reserve(size).map_err(error::anomaly)?;
    let image = match writer {
        ImageWriter::Binary => Image {
            bytes: rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
                .map_err(error::anomaly)?,
            stream: None,
        },
        ImageWriter::Stream(encoder) => {
            let encoder = match encoder {
                Some(encoder) => encoder,
                None => encoder.insert(Arc::new(
                    RowStreamEncoder::new(batch.schema_ref()).map_err(error::anomaly)?,
                )),
            };
            Image {
                bytes: encoder.encode(&batch).map_err(error::anomaly)?,
                stream: Some(Arc::clone(encoder)),
            }
        }
    };
    Ok((image, charge))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, builder::StringDictionaryBuilder, types::Int32Type};

    #[test]
    fn descriptor_reservation_equals_the_legacy_singleton_charge() {
        let schema = Arc::new(schema());
        let table = "public.a_table_name_long_enough_to_cross_an_arrow_string_buffer_boundary";
        let base = singleton_base(&schema, table).unwrap();
        for len in [0, 1, 1023, 1024, 1025, 2048, 2049, MAX_PACKED_BYTES] {
            for [key, before, after] in [[true, false, false], [true, true, false], [false; 3]] {
                let bytes = vec![0; len];
                let lengths = [key, before, after].map(|present| present.then_some(len));
                let images = lengths.map(|len| BinaryArray::from(vec![len.map(|_| bytes.as_slice())]));
                let batch = outer_row(Arc::clone(&schema), table, Timestamp(7), true, images).unwrap();
                assert_eq!(
                    singleton_bound(base, lengths).unwrap(),
                    wire::batch_bound(&batch).unwrap(),
                );
            }
        }
    }

    #[test]
    fn retained_rows_fall_back_to_singletons_when_packing_has_no_scratch() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Arc::new(schema());
        let encoder = wire::Encoder::new(&schema, &memory).unwrap();
        let time = Timestamp(1);
        let table: Arc<str> = Arc::from("public.rows");
        let base = singleton_base(&schema, &table).unwrap();
        let rows = (0..2)
            .map(|_| {
                let charge = memory.reserve(singleton_bound(base, [None; 3]).unwrap()).unwrap();
                let row = ReservedRow {
                    table: Arc::clone(&table),
                    time,
                    opened: true,
                    images: [None, None, None],
                    charge,
                };
                (0, ChangeRow::Reserved(row))
            })
            .collect();
        let filler = memory.reserve(memory.available_bytes()).unwrap();
        let frames = encode_page(
            BTreeMap::from([(time, rows)]),
            false,
            false,
            &schema,
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

    #[test]
    fn dictionary_images_keep_their_independent_streams() {
        let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
        dictionary.append("one").unwrap();
        dictionary.append("two").unwrap();
        let values: ArrayRef = Arc::new(dictionary.finish());
        let batch = RecordBatch::try_from_iter([("_id", values)]).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let mut shared = ImageWriter::new(true);
        let mut legacy = ImageWriter::new(false);
        let mut make_row = |row| {
            let (image, charge) = write_image(&mut shared, &batch, row, &[0], &memory).unwrap();
            let (legacy, _) = write_image(&mut legacy, &batch, row, &[0], &memory).unwrap();
            assert_eq!(image.legacy_len(), legacy.bytes.len());
            let row = ReservedRow {
                table: Arc::from("public.rows"),
                time: Timestamp(1),
                opened: true,
                images: [Some(image), None, None],
                charge,
            };
            let encoded = image_array(std::slice::from_ref(&row), 0).unwrap();
            assert_eq!(encoded.value(0), legacy.bytes.as_slice());
            row
        };
        let first = make_row(0);
        let second = make_row(1);
        assert!(!first.shares_streams_with(&second));
    }
}
