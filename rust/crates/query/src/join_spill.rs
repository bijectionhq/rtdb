use std::{mem::size_of, sync::Arc};

use arrow_array::{Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use rtdb_columnar::MemoryReservation;
use rtdb_format::ipc::RecordSpans;
use rtdb_plan::Schema;
use rtdb_types::{LogicalType, Result, ScalarType};

use crate::{
    QueryContext,
    columnar::{Column, ColumnBatch, relayout_output},
    invalid,
    join_storage::partition_count,
    runtime_schema::schema_bound,
    set_keys::{JoinHash, Key, KeyColumns},
    spill::{SpillFile, SpillReader, SpillWriter},
};

const FRAME_ROWS: usize = 1024;

pub(crate) fn compatible_schema(schema: &Schema) -> bool {
    fn compatible(ty: &LogicalType) -> bool {
        match ty {
            LogicalType::Null | LogicalType::Scalar(
                ScalarType::Bool | ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64
                | ScalarType::F32 | ScalarType::F64 | ScalarType::Utf8 | ScalarType::Binary
                | ScalarType::FixedSizeBinary(_) | ScalarType::Uuid
            ) => true,
            LogicalType::Maybe(ty) => compatible(ty),
            LogicalType::List { kind: rtdb_types::ListKind::List | rtdb_types::ListKind::FixedSizeList(_), element } => compatible(element),
            LogicalType::Struct(fields) => fields.values().all(compatible),
            _ => false,
        }
    }
    schema.fields().iter().all(|field| compatible(&field.data_type))
}

pub(crate) fn layout_error() -> rtdb_types::Anomaly {
    invalid("join-spill-layout", "Join spill input requires another Arrow layout")
}

struct SpillSchema {
    physical: SchemaRef,
    logical: Schema,
    batch_metadata: usize,
    _reservation: Arc<MemoryReservation>,
}

impl SpillSchema {
    fn new(schema: &Schema, context: &QueryContext) -> Result<Self> {
        let logical_metadata = schema_bound(schema)?;
        let bytes = logical_metadata.checked_mul(16).and_then(|size| size.checked_add(4096)).ok_or_else(size_error)?;
        let reservation = context.memory.reserve(bytes)?;
        let fields = schema.fields().iter().map(|field| {
            rtdb_columnar::to_field(&field.name, &field.data_type)
        }).collect::<Result<Vec<_>>>()?;
        let batch_metadata = schema.len().checked_mul(size_of::<Column>())
            .and_then(|bytes| bytes.checked_add(logical_metadata)).ok_or_else(size_error)?;
        Ok(Self {
            physical: Arc::new(ArrowSchema::new(fields)),
            logical: schema.clone(),
            batch_metadata,
            _reservation: reservation,
        })
    }

    fn record(&self, source: &ColumnBatch, context: &QueryContext) -> Result<Option<(RecordBatch, Vec<Arc<MemoryReservation>>, Arc<MemoryReservation>)>> {
        if source.columns.len() != self.logical.len() { return Ok(None); }
        let metadata = source.columns.len().checked_mul(192).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
        let reservation = context.memory.reserve(metadata)?;
        let mut arrays = Vec::with_capacity(source.columns.len());
        let mut copied = Vec::new();
        for ((column, logical), target) in source.columns.iter().zip(self.logical.fields()).zip(self.physical.fields()) {
            let same_layout = |field: &arrow_schema::Field, ty: &LogicalType| {
                field.data_type() == target.data_type() && field.metadata() == target.metadata()
                    && ty.clone().maybe() == logical.data_type.clone().maybe()
            };
            let array = match column {
                Column::Arrow { array, field, logical: Some(ty) } if same_layout(field, ty) => Arc::clone(array),
                Column::Selected(selected) if selected.source_layouts().all(|(_, field, ty)| ty.is_some_and(|ty| same_layout(field, ty))) => {
                    let Some((array, charge)) = relayout_output(column, &logical.data_type, target, &context.memory)? else {
                        return Ok(None);
                    };
                    copied.push(charge);
                    array
                }
                _ => return Ok(None),
            };
            if !target.is_nullable() && array.logical_null_count() != 0 { return Ok(None); }
            arrays.push(array);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(source.len));
        let record = RecordBatch::try_new_with_options(Arc::clone(&self.physical), arrays, &options).map_err(|_| layout_error())?;
        Ok(Some((record, copied, reservation)))
    }

    fn read(&self, reader: &mut SpillReader) -> Result<Option<ColumnBatch>> {
        let Some((marker, _charge)) = reader.read_frame(1)? else { return Ok(None); };
        if marker == [3] {
            let record = reader.read_record(&self.physical, self.batch_metadata)?
                .ok_or_else(|| invalid("join-spill", "Join spill ended before its frame"))?;
            return ColumnBatch::from_owned(record).map(Some);
        }
        reader.read_columns_frame(&marker, self.logical.len())
    }

}

struct SpillRecord {
    record: RecordBatch,
    _source: ColumnBatch,
    _copied: Vec<Arc<MemoryReservation>>,
    _reservation: Arc<MemoryReservation>,
}

pub(crate) struct BlockWriter {
    writer: SpillWriter,
    schema: SpillSchema,
    blocks: usize,
    block_rows: i64,
    threshold: i64,
    queued: Vec<SpillRecord>,
    queued_rows: usize,
    queued_bytes: usize,
    _inventory: Arc<MemoryReservation>,
}

impl BlockWriter {
    pub fn new(schema: &Schema, context: &QueryContext) -> Result<Self> {
        Ok(Self {
            writer: context.spill.create()?,
            schema: SpillSchema::new(schema, context)?,
            blocks: 0,
            block_rows: 0,
            threshold: context.options.join_build_rows,
            queued: Vec::new(),
            queued_rows: 0,
            queued_bytes: 0,
            _inventory: context.memory.reserve(FRAME_ROWS.checked_mul(size_of::<SpillRecord>()).ok_or_else(size_error)?)?,
        })
    }

    pub fn append(&mut self, batch: ColumnBatch, context: &QueryContext) -> Result<()> {
        if batch.len == 0 { return Ok(()); }
        let rows = i64::try_from(batch.len).map_err(|_| size_error())?;
        self.block_rows = self.block_rows.checked_add(rows).ok_or_else(size_error)?;
        if self.block_rows > self.threshold {
            self.blocks = self.blocks.checked_add(1).ok_or_else(size_error)?;
            self.block_rows = 0;
        }

        let layout = if compatible_schema(&self.schema.logical) { self.schema.record(&batch, context)? } else { None };
        let Some((record, copied, reservation)) = layout else {
            self.flush(context)?;
            return self.writer.write_columns(&batch, &self.schema.logical, &context.memory);
        };
        let record = SpillRecord { record, _source: batch, _copied: copied, _reservation: reservation };
        let rows = record.record.num_rows();
        let bytes = rtdb_columnar::shared_allocation_size(&record.record)?;
        if !self.queued.is_empty() && (self.queued_rows.saturating_add(rows) > FRAME_ROWS
            || self.queued_bytes.saturating_add(bytes) > context.memory.limit_bytes() / 16) {
            self.flush(context)?;
        }
        self.queued_rows = self.queued_rows.checked_add(rows).ok_or_else(size_error)?;
        self.queued_bytes = self.queued_bytes.checked_add(bytes).ok_or_else(size_error)?;
        if self.queued.is_empty() { self.queued.reserve_exact(FRAME_ROWS); }
        self.queued.push(record);
        if self.queued_rows >= FRAME_ROWS { self.flush(context)?; }
        Ok(())
    }

    fn flush(&mut self, context: &QueryContext) -> Result<()> {
        context.check_cancelled()?;
        match self.queued.as_slice() {
            [] => return Ok(()),
            [record] => self.writer.write_single_join_record(&record.record, &context.memory)?,
            many => {
                let _references = context.memory.reserve(many.len().checked_mul(size_of::<&RecordBatch>()).ok_or_else(size_error)?)?;
                let records = many.iter().map(|record| &record.record).collect::<Vec<_>>();
                if let Some(spans) = RecordSpans::concat(&records, 64, &context.memory)? {
                    self.writer.write_frame(&[3])?;
                    self.writer.write_spanned_record(&spans)?;
                } else {
                    write_compacted_records(&self.schema, &mut self.writer, &records, self.queued_bytes, context)?;
                }
            }
        }
        self.queued.clear();
        self.queued_rows = 0;
        self.queued_bytes = 0;
        Ok(())
    }

    pub fn finish(mut self, context: &QueryContext) -> Result<SpilledBlocks> {
        self.flush(context)?;
        let blocks = self.blocks.checked_add(usize::from(self.block_rows != 0)).ok_or_else(size_error)?;
        Ok(SpilledBlocks { file: self.writer.finish()?, blocks, schema: self.schema })
    }
}

fn write_compacted_records(
    schema: &SpillSchema, writer: &mut SpillWriter, records: &[&RecordBatch], queued_bytes: usize, context: &QueryContext,
) -> Result<()> {
    let bytes = queued_bytes.checked_mul(4).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(size_error)?;
    let _compact = context.memory.reserve(bytes)?;
    let batch = arrow_select::concat::concat_batches(&schema.physical, records.iter().copied())
        .map_err(|error| match error {
            arrow_schema::ArrowError::OffsetOverflowError(_) => layout_error(),
            error => invalid("join-spill", error),
        })?;
    writer.write_frame(&[3])?;
    writer.write_record(&batch, &context.memory)
}

pub(crate) struct SpilledBlocks {
    file: Arc<SpillFile>,
    blocks: usize,
    schema: SpillSchema,
}

impl SpilledBlocks {
    pub fn shuffle(
        self, minimum: usize, context: &QueryContext,
        hash: impl Fn(&ColumnBatch, usize) -> Result<u32>,
    ) -> Result<Partitions> {
        self.shuffle_with(minimum, context, |batch, partitions| {
            partition_rows(batch.len, partitions, context, |row| hash(batch, row))
        })
    }

    pub fn shuffle_keys(
        self, minimum: usize, context: &QueryContext, positions: &[usize], projected: bool, hashing: JoinHash,
    ) -> Result<Partitions> {
        self.shuffle_with(minimum, context, |batch, partitions| {
            if !positions.is_empty()
                && let Some(keys) = KeyColumns::projection(batch, positions).bind_typed(context)?
            {
                let _scratch = context.memory.reserve(batch.len.checked_mul(size_of::<u32>()).ok_or_else(size_error)?)?;
                let mut hashes = vec![0; batch.len];
                keys.hash_join_into(&mut hashes, context, hashing)?;
                return partition_rows(batch.len, partitions, context, |row| Ok(hashes[row]));
            }
            partition_rows(batch.len, partitions, context, |row| {
                Key::for_columns(batch, row, projected, positions, context)?.join_hash(hashing)
            })
        })
    }

    fn shuffle_with(
        self, minimum: usize, context: &QueryContext,
        partition: impl Fn(&ColumnBatch, &mut [Vec<(usize, usize)>]) -> Result<()>,
    ) -> Result<Partitions> {
        let memory = &context.memory;
        let count = partition_count(self.blocks, minimum)?;
        let base = count.checked_mul(size_of::<Vec<u64>>()).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(base)?;
        let mut offsets = vec![Vec::new(); count];
        let width = self.schema.logical.len();
        let _columns = memory.reserve(width.checked_mul(size_of::<usize>()).ok_or_else(size_error)?)?;
        let positions: Vec<_> = (0..width).collect();
        let mut input = self.file.reader(Arc::clone(memory))?;
        let mut writer = context.spill.create()?;
        while let Some(batch) = self.schema.read(&mut input)? {
            let scratch = count.checked_mul(size_of::<Vec<(usize, usize)>>())
                .and_then(|bytes| batch.len.checked_mul(4 * size_of::<(usize, usize)>()).and_then(|rows| bytes.checked_add(rows)))
                .ok_or_else(size_error)?;
            let _picks = memory.reserve(scratch)?;
            let mut partitions = vec![Vec::new(); count];
            partition(&batch, &mut partitions)?;
            for (picks, offsets) in partitions.into_iter().zip(&mut offsets) {
                if picks.is_empty() { continue; }
                context.check_cancelled()?;
                let selected = ColumnBatch::take_rows(&[&batch], &picks, &positions, memory)?;
                let at = writer.position()?;
                if compatible_schema(&self.schema.logical)
                    && let Some((record, _copied, _reservation)) = self.schema.record(&selected, context)? {
                    writer.write_single_join_record(&record, memory)?;
                } else {
                    writer.write_columns(&selected, &self.schema.logical, memory)?;
                }
                let lease = Arc::get_mut(&mut reservation).expect("The shuffle owns its frame inventory");
                lease.grow_to(lease.bytes().checked_add(4 * size_of::<u64>()).ok_or_else(size_error)?)?;
                offsets.push(at);
            }
        }
        Ok(Partitions { reader: writer.finish()?.reader(Arc::clone(memory))?, offsets, schema: self.schema, _reservation: reservation })
    }
}

fn partition_rows(
    rows: usize, partitions: &mut [Vec<(usize, usize)>], context: &QueryContext,
    hash: impl Fn(usize) -> Result<u32>,
) -> Result<()> {
    let mask = partitions.len() - 1;
    for row in 0..rows {
        context.check_cancelled_at(row)?;
        partitions[hash(row)? as usize & mask].push((0, row));
    }
    Ok(())
}

pub(crate) struct Partitions {
    reader: SpillReader,
    offsets: Vec<Vec<u64>>,
    schema: SpillSchema,
    _reservation: Arc<MemoryReservation>,
}

impl Partitions {
    pub fn count(&self) -> usize { self.offsets.len() }
    pub fn frames(&self, partition: usize) -> usize { self.offsets[partition].len() }

    pub fn read(&mut self, partition: usize, frame: usize) -> Result<ColumnBatch> {
        let offset = self.offsets.get(partition).and_then(|frames| frames.get(frame))
            .ok_or_else(|| invalid("join-spill", "Join spill frame is absent"))?;
        self.reader.seek(*offset)?;
        self.schema.read(&mut self.reader)?
            .ok_or_else(|| invalid("join-spill", "Join spill ended before its frame"))
    }
}

fn size_error() -> rtdb_types::Anomaly { invalid("join-size", "Join spill exceeds supported row or allocation bounds") }
