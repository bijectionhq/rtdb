use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    sync::Arc,
    time::Duration,
};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_flight::FlightData;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit as ArrowTime};
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::ipc::{PreparedStreamRow, RowStreamEncoder};
use rtdb_node::{Database, PreparedSql, ResultSignaturePolicy};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_types::{TableRef, TimeUnit, Timestamp, Value};
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
const MAX_FIXED_IMAGE_FIELDS: usize = 4;
const MAX_FIXED_IMAGE_BYTES: usize = 1024;
const MAX_FIXED_IMAGE_SCRATCH: usize = 24 << 10;
const MIN_FIXED_IMAGE_ROW_BOUND: usize = MAX_FIXED_IMAGE_SCRATCH + 8 * MAX_FIXED_IMAGE_BYTES;

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
    fn finish_images(&mut self) -> Result<(), Status> {
        for image in &mut self.images {
            if let Some(Image::Deferred(deferred)) = image {
                *image = Some(deferred.encode()?);
            }
        }
        Ok(())
    }

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

enum Image {
    Binary(Vec<u8>),
    Shared {
        stream: Arc<RowStreamEncoder>,
        row: PreparedStreamRow,
    },
    Deferred(DeferredImage),
}

impl Image {
    fn encode(batch: &RecordBatch, stream: Option<&Arc<RowStreamEncoder>>) -> Result<Self, Status> {
        match stream {
            Some(stream) => Ok(Self::Shared {
                stream: Arc::clone(stream),
                row: stream.prepare(batch).map_err(error::anomaly)?,
            }),
            None => rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(batch))
                .map(Self::Binary)
                .map_err(error::anomaly),
        }
    }

    fn legacy_len(&self) -> usize {
        match self {
            Self::Binary(bytes) => bytes.len(),
            Self::Shared { stream, row } => {
                stream.schema_frame().len() + row.framed_len() + IMAGE_END.len()
            }
            Self::Deferred(image) => image.legacy_len,
        }
    }

    fn shares_schema_with(&self, other: &Self) -> bool {
        let (Self::Shared { stream: left, .. }, Self::Shared { stream: right, .. }) = (self, other)
        else {
            return false;
        };
        !left.has_dictionaries() && !right.has_dictionaries()
            && (Arc::ptr_eq(left, right) || left.schema_frame() == right.schema_frame())
    }
}

struct DeferredImage {
    schema: SchemaRef,
    values: [i64; MAX_FIXED_IMAGE_FIELDS],
    stream: Option<Arc<RowStreamEncoder>>,
    legacy_len: usize,
}

impl DeferredImage {
    fn fixed_values(
        batch: &RecordBatch,
        schema_bound: usize,
    ) -> Option<[i64; MAX_FIXED_IMAGE_FIELDS]> {
        const CELL_SCRATCH: usize = 4 * (size_of::<Int64Array>() + size_of::<i64>());
        let fields = batch.num_columns();
        let schema = batch.schema_ref();
        if !(1..=MAX_FIXED_IMAGE_FIELDS).contains(&fields)
            || schema_bound > MAX_FIXED_IMAGE_SCRATCH - CELL_SCRATCH * fields
            || !schema.metadata().is_empty()
        {
            return None;
        }
        let mut values = [0; MAX_FIXED_IMAGE_FIELDS];
        for ((value, field), array) in values.iter_mut().zip(schema.fields()).zip(batch.columns()) {
            if array.null_count() != 0 || !field.metadata().is_empty() {
                return None;
            }
            *value = match field.data_type() {
                DataType::Int64 => array.as_any().downcast_ref::<Int64Array>()?.value(0),
                DataType::Timestamp(ArrowTime::Microsecond, _) => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()?
                    .value(0),
                _ => return None,
            };
        }
        Some(values)
    }

    fn encode(&self) -> Result<Image, Status> {
        let columns = self
            .schema
            .fields()
            .iter()
            .zip(self.values)
            .map(|(field, value)| -> ArrayRef {
                match field.data_type() {
                    DataType::Int64 => Arc::new(Int64Array::from(vec![value])),
                    DataType::Timestamp(ArrowTime::Microsecond, timezone) => Arc::new(
                        TimestampMicrosecondArray::from(vec![value])
                            .with_timezone_opt(timezone.clone()),
                    ),
                    _ => unreachable!("deferred images have a fixed-width layout"),
                }
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(batch_error)?;
        Image::encode(&batch, self.stream.as_ref())
    }
}

enum ImageEncoding {
    Binary,
    Stream(Option<Arc<RowStreamEncoder>>),
}

struct ImageProjection<'a> {
    source: &'a RecordBatch,
    columns: &'a [usize],
    projected: Option<RecordBatch>,
    schema_bound: Option<usize>,
    fixed_len: Option<usize>,
    encoding: ImageEncoding,
}

impl<'a> ImageProjection<'a> {
    fn new(source: &'a RecordBatch, columns: &'a [usize], shared_images: bool) -> Self {
        Self {
            source,
            columns,
            projected: None,
            schema_bound: None,
            fixed_len: None,
            encoding: if shared_images {
                ImageEncoding::Stream(None)
            } else {
                ImageEncoding::Binary
            },
        }
    }

    fn prepare(
        &mut self,
        row: usize,
        memory: &Arc<MemoryBudget>,
    ) -> Result<(Image, Arc<MemoryReservation>), Status> {
        let projected = match &mut self.projected {
            Some(projected) => projected,
            None => self
                .projected
                .insert(self.source.project(self.columns).map_err(batch_error)?),
        };
        let batch = projected.slice(row, 1);
        let arrays = wire::mul(batch.get_array_memory_size(), 4).map_err(error::map_status)?;
        let schema_bound = match self.schema_bound {
            Some(bound) => bound,
            None => {
                let bound = wire::schema_bound(batch.schema_ref()).map_err(error::map_status)?;
                self.schema_bound = Some(bound);
                bound
            }
        };
        let size = wire::add(arrays, schema_bound).map_err(error::map_status)?;
        let charge = memory.reserve(size).map_err(error::anomaly)?;
        let stream = match &mut self.encoding {
            ImageEncoding::Binary => None,
            ImageEncoding::Stream(encoder) => {
                let encoder = match encoder {
                    Some(encoder) => encoder,
                    None => encoder.insert(Arc::new(
                        RowStreamEncoder::new(batch.schema_ref()).map_err(error::anomaly)?,
                    )),
                };
                Some(&*encoder)
            }
        };
        let values = DeferredImage::fixed_values(&batch, schema_bound);
        if let (Some(values), Some(legacy_len)) = (values, self.fixed_len) {
            return Ok((
                Image::Deferred(DeferredImage {
                    schema: batch.schema(),
                    values,
                    stream: stream.cloned(),
                    legacy_len,
                }),
                charge,
            ));
        }
        let image = Image::encode(&batch, stream)?;
        // For present fixed-width cells, uncompressed framing depends only on the projection.
        // Use a successful encode to establish its exact length under the original scratch charge.
        if values.is_some() {
            let length = image.legacy_len();
            self.fixed_len = (length <= MAX_FIXED_IMAGE_BYTES).then_some(length);
        }
        Ok((image, charge))
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
        let (database, page) = self
            .open_change_page(&call, request, shared_images)
            .await
            .map_err(|status| call.error(status))?;
        let through = tokens::basis(database.wire_incarnation(), page.through).into();
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
        request: proto::ChangesRequest,
        shared_images: bool,
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
                        let key_projection =
                            projection.column_with_name("_id").map(|(index, _)| [index]);
                        let image_projection = if image {
                            (0..batch.num_columns())
                                .filter(|index| *index != time_index)
                                .collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        };
                        let mut key_writer = key_projection.as_ref().map(|columns| {
                            ImageProjection::new(batch, columns, request.shared_images)
                        });
                        let mut image_writer = image.then(|| {
                            ImageProjection::new(batch, &image_projection, request.shared_images)
                        });
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
                            let (key, _key_scratch) = key_writer
                                .as_mut()
                                .map(|writer| writer.prepare(row, memory))
                                .transpose()?
                                .unzip();
                            let (image, _image_scratch) = image_writer
                                .as_mut()
                                .map(|writer| writer.prepare(row, memory))
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
                            let mut row = ReservedRow {
                                table: Arc::clone(&table_name),
                                time,
                                opened,
                                images,
                                charge,
                            };
                            // Only defer small images together so their scratch fits in the
                            // singleton charge before allocating the outer batch.
                            if !(MIN_FIXED_IMAGE_ROW_BOUND..=pack_limit).contains(&bound)
                                || row
                                    .images
                                    .iter()
                                    .flatten()
                                    .any(|image| !matches!(image, Image::Deferred(_)))
                            {
                                row.finish_images()?;
                            }
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
    mut groups: BTreeMap<Timestamp, Vec<(usize, ChangeRow)>>,
    descending: bool,
    shared_images: bool,
    schema: &SchemaRef,
    mut encoder: wire::Encoder,
    memory: &Arc<MemoryBudget>,
    pack_limit: usize,
) -> Result<Vec<(usize, Vec<FlightData>)>, Status> {
    for (_, row) in groups.values_mut().flatten() {
        if let ChangeRow::Reserved(row) = row {
            row.finish_images()?;
        }
    }
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
    let Some(Image::Shared { stream, .. }) = &rows[0].images[column] else {
        return Ok(images
            .map(|image| match image {
                Some(Image::Binary(bytes)) => Some(bytes.as_slice()),
                None => None,
                Some(_) => unreachable!("packed rows use the same encoded image carrier"),
            })
            .collect());
    };
    let messages = images.flatten().map(|image| match image {
        Image::Shared { row, .. } => row,
        _ => unreachable!("packed rows use the same encoded image carrier"),
    });
    let len = stream.schema_frame().len()
        + messages.clone().map(PreparedStreamRow::framed_len).sum::<usize>()
        + IMAGE_END.len();
    let end = i32::try_from(len).map_err(|_| ArrowError::OffsetOverflowError(len))?;
    let mut bytes = Vec::with_capacity(len);
    bytes.extend_from_slice(stream.schema_frame());
    for message in messages {
        message.append_to(&mut bytes);
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, builder::StringDictionaryBuilder, types::Int32Type};

    #[test]
    fn fixed_images_preserve_scratch_admission_and_singleton_bytes_when_deferred() {
        let batch = RecordBatch::try_from_iter([
            (
                "_id",
                Arc::new(Int64Array::from(vec![Some(1), Some(-42), None])) as ArrayRef,
            ),
            (
                "_valid_from",
                Arc::new(
                    TimestampMicrosecondArray::from(vec![2, 170, 191])
                        .with_timezone("America/New_York"),
                ) as ArrayRef,
            ),
        ])
        .unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let schema = Arc::new(schema());
        let table: Arc<str> = Arc::from("public.rows");
        let base = singleton_base(&schema, &table).unwrap();
        assert!(base >= MIN_FIXED_IMAGE_ROW_BOUND);
        let candidate = batch.slice(1, 1);
        let scratch_bound = wire::batch_bound(&candidate).unwrap();
        let expected =
            rtdb_format::ipc::write_stream(candidate.schema_ref(), std::slice::from_ref(&candidate))
                .unwrap();

        for shared in [false, true] {
            let mut projection = ImageProjection::new(&batch, &[0, 1], shared);
            let (first, first_scratch) = projection.prepare(0, &memory).unwrap();
            assert!(!matches!(&first, Image::Deferred(_)));
            drop(first_scratch);

            let restricted = MemoryBudget::new(scratch_bound - 1);
            let expected_error = error::anomaly(restricted.reserve(scratch_bound).err().unwrap());
            let error = projection.prepare(1, &restricted).err().unwrap();
            assert_eq!(error.code(), expected_error.code());
            assert_eq!(error.details(), expected_error.details());

            let (image, scratch) = projection.prepare(1, &memory).unwrap();
            assert!(matches!(&image, Image::Deferred(_)));
            assert_eq!(scratch.bytes(), scratch_bound);
            assert_eq!(image.legacy_len(), expected.len());
            let charge = memory
                .reserve(singleton_bound(base, [Some(expected.len()), None, None]).unwrap())
                .unwrap();
            let mut row = ReservedRow {
                table: Arc::clone(&table),
                time: Timestamp(1),
                opened: true,
                images: [Some(image), None, None],
                charge,
            };
            drop(scratch);
            row.finish_images().unwrap();
            let materialized = row.images[0].as_ref().unwrap();
            assert_eq!(materialized.legacy_len(), expected.len());
            assert_eq!(first.shares_schema_with(materialized), shared);
            let singleton = image_array(std::slice::from_ref(&row), 0).unwrap();
            assert_eq!(singleton.value(0), expected.as_slice());

            let (null, _) = projection.prepare(2, &memory).unwrap();
            assert!(!matches!(null, Image::Deferred(_)));
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn shared_images_keep_the_released_bytes_across_input_batches() {
        let long = "value".repeat(500);
        let batch = RecordBatch::try_from_iter([
            (
                "_id",
                Arc::new(StringArray::from(vec!["one", "two", "three"])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec![Some("short"), None, Some(long.as_str())]))
                    as ArrayRef,
            ),
        ])
        .unwrap();
        let inputs = [batch.slice(0, 2), batch.slice(2, 1)];
        let memory = MemoryBudget::new(1 << 20);
        let outer_schema = Arc::new(schema());
        let table: Arc<str> = Arc::from("public.rows");
        let base = singleton_base(&outer_schema, &table).unwrap();
        let released = RowStreamEncoder::new(batch.schema_ref()).unwrap();
        let mut expected = released.schema_frame().to_vec();
        let mut rows = Vec::new();
        for input in &inputs {
            let mut shared = ImageProjection::new(input, &[0, 1], true);
            let mut ordinary = ImageProjection::new(input, &[0, 1], false);
            for index in 0..input.num_rows() {
                let candidate = input.slice(index, 1);
                let legacy = rtdb_format::ipc::write_stream(
                    candidate.schema_ref(),
                    std::slice::from_ref(&candidate),
                )
                .unwrap();
                let (image, scratch) = shared.prepare(index, &memory).unwrap();
                let (binary, binary_scratch) = ordinary.prepare(index, &memory).unwrap();
                let image_bound = wire::batch_bound(&candidate).unwrap();
                assert_eq!(scratch.bytes(), image_bound);
                assert_eq!(binary_scratch.bytes(), image_bound);
                let Image::Binary(bytes) = binary else {
                    panic!("expected an ordinary image");
                };
                assert_eq!(bytes, legacy);
                assert_eq!(image.legacy_len(), legacy.len());
                expected.extend_from_slice(&released.encode(&candidate).unwrap());
                let charge = memory
                    .reserve(singleton_bound(base, [Some(legacy.len()), None, None]).unwrap())
                    .unwrap();
                let row = ReservedRow {
                    table: Arc::clone(&table),
                    time: Timestamp(1),
                    opened: true,
                    images: [Some(image), None, None],
                    charge,
                };
                let singleton = image_array(std::slice::from_ref(&row), 0).unwrap();
                assert_eq!(singleton.value(0), legacy.as_slice());
                rows.push(row);
            }
        }
        expected.extend_from_slice(&IMAGE_END);
        assert!(rows[0].shares_streams_with(&rows[2]));
        let packed = image_array(&rows, 0).unwrap();
        assert_eq!(packed.value(0), expected.as_slice());
        assert!(packed.is_null(1));
        assert!(packed.is_null(2));
        drop(rows);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn image_scratch_admission_precedes_a_schema_encoding_error() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["one"]));
        let mut batch = RecordBatch::try_from_iter([("_id", values)]).unwrap();
        batch.schema_metadata_mut().insert(
            rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY.into(),
            "0".into(),
        );
        let candidate = batch.slice(0, 1).project(&[0]).unwrap();
        let bound = wire::batch_bound(&candidate).unwrap();
        for shared in [false, true] {
            let mut images = ImageProjection::new(&batch, &[0], shared);
            let memory = MemoryBudget::new(bound - 1);
            let expected = error::anomaly(memory.reserve(bound).err().unwrap());
            let rejection = images.prepare(0, &memory).err().unwrap();
            assert_eq!(rejection.code(), expected.code());
            assert_eq!(rejection.details(), expected.details());
            assert_eq!(memory.used_bytes(), 0);

            let memory = MemoryBudget::new(bound);
            let expected = error::anomaly(
                rtdb_format::ipc::write_stream(candidate.schema_ref(), std::slice::from_ref(&candidate))
                    .unwrap_err(),
            );
            let rejection = images.prepare(0, &memory).err().unwrap();
            assert_eq!(rejection.code(), expected.code());
            assert_eq!(rejection.details(), expected.details());
            assert_eq!(memory.used_bytes(), 0);
        }
    }

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
        let mut shared = ImageProjection::new(&batch, &[0], true);
        let mut legacy = ImageProjection::new(&batch, &[0], false);
        let mut make_row = |row| {
            let (image, charge) = shared.prepare(row, &memory).unwrap();
            let (legacy, _) = legacy.prepare(row, &memory).unwrap();
            let Image::Binary(legacy) = legacy else {
                panic!("expected an ordinary image");
            };
            assert_eq!(image.legacy_len(), legacy.len());
            let row = ReservedRow {
                table: Arc::from("public.rows"),
                time: Timestamp(1),
                opened: true,
                images: [Some(image), None, None],
                charge,
            };
            let encoded = image_array(std::slice::from_ref(&row), 0).unwrap();
            assert_eq!(encoded.value(0), legacy.as_slice());
            row
        };
        let first = make_row(0);
        let second = make_row(1);
        assert!(!first.shares_streams_with(&second));
    }
}
