use std::{collections::BTreeMap, hash::{DefaultHasher, Hash, Hasher}, ops::Range, sync::Arc};

use arrow_array::{
    ArrayRef, NullArray, RecordBatch, RecordBatchOptions,
    TimestampSecondArray, TimestampMillisecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    builder::{BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_types::{LogicalType, Result, TableRef, Value};

use super::{ArrowChanges, DecodedTransaction, RowKind, overflow};
use crate::{Lsn, wire::protocol};

#[derive(Clone, Debug)]
pub(super) struct Run {
    pub batch: usize,
    pub rows: Range<usize>,
}

#[derive(Default)]
pub(super) struct PendingTransaction {
    groups: Vec<PendingBatch>,
    relations: BTreeMap<(u32, RowKind, u64), Vec<usize>>,
    order: Vec<Run>,
}

impl PendingTransaction {
    pub fn push(
        &mut self, id: u32, table: &TableRef, kind: RowKind, cells: Vec<(Field, DecodedCell)>,
        mut reservation: Arc<MemoryReservation>, metadata: usize,
    ) -> Result<()> {
        let mut layout = DefaultHasher::new();
        for (field, _) in &cells { field.hash(&mut layout); }
        let candidates = self.relations.entry((id, kind, layout.finish())).or_default();
        let existing = candidates.iter().copied().find(|&index| self.groups[index].matches(table, &cells));
        let batch = existing.unwrap_or(self.groups.len());
        let new_run = self.order.last().is_none_or(|run| run.batch != batch);
        let charge = Arc::get_mut(&mut reservation).expect("decoded row owns its reservation");
        if new_run { charge.grow_to(charge.bytes().checked_add(128).ok_or_else(overflow)?)?; }
        let row = if let Some(index) = existing {
            charge.shrink_to(charge.bytes() - metadata)?;
            let group = &mut self.groups[index];
            let row = group.count;
            Arc::get_mut(&mut group.reservation).expect("pending batch owns its reservation").absorb(charge)?;
            group.push(cells);
            row
        } else {
            let mut group = PendingBatch::new(table.clone(), kind, &cells, reservation, metadata);
            group.push(cells);
            self.groups.push(group);
            candidates.push(batch);
            0
        };
        if new_run { self.order.push(Run { batch, rows: row..row + 1 }); }
        else { self.order.last_mut().expect("previous run").rows.end += 1; }
        Ok(())
    }

    pub fn finish(self, lsn: Lsn, time: rtdb_types::Timestamp) -> Result<Option<DecodedTransaction>> {
        if self.order.is_empty() { return Ok(None); }
        let batches = self.groups.into_iter().map(PendingBatch::finish).collect::<Result<_>>()?;
        Ok(Some(DecodedTransaction { lsn, commit_time: Some(time), batches, order: Some(self.order) }))
    }
}

struct PendingBatch {
    table: TableRef,
    kind: RowKind,
    count: usize,
    metadata: usize,
    columns: Vec<PendingColumn>,
    reservation: Arc<MemoryReservation>,
}

impl PendingBatch {
    fn new(
        table: TableRef, kind: RowKind, cells: &[(Field, DecodedCell)], reservation: Arc<MemoryReservation>, metadata: usize,
    ) -> Self {
        let columns = cells.iter().map(|(field, cell)| PendingColumn {
            field: field.clone(),
            values: ColumnValues::new(cell),
        }).collect();
        Self { table, kind, count: 0, metadata, columns, reservation }
    }

    fn matches(&self, table: &TableRef, cells: &[(Field, DecodedCell)]) -> bool {
        self.table == *table && self.columns.len() == cells.len()
            && self.columns.iter().zip(cells).all(|(column, (field, cell))| column.field == *field && column.values.accepts(cell))
    }

    fn push(&mut self, cells: Vec<(Field, DecodedCell)>) {
        for (column, (_, cell)) in self.columns.iter_mut().zip(cells) { column.values.push(cell); }
        self.count += 1;
    }

    fn finish(self) -> Result<ArrowChanges> {
        let Self { table, kind, count, metadata, columns, mut reservation } = self;
        let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().map(|column| {
            let array = column.values.finish(&column.field, count)?;
            Ok((column.field, array))
        }).collect::<Result<Vec<_>>>()?.into_iter().unzip();
        let options = RecordBatchOptions::new().with_row_count(Some(count));
        let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options).map_err(protocol)?;
        let retained = batch.get_array_memory_size().checked_add(metadata)
            .and_then(|bytes| bytes.checked_add(count.checked_mul(128)?)).ok_or_else(overflow)?;
        let charge = Arc::get_mut(&mut reservation).expect("pending batch owns its reservation");
        if retained < charge.bytes() { charge.shrink_to(retained)?; }
        Ok(ArrowChanges { table, kind, rows: OwnedBatch::with_reservation(batch, reservation)? })
    }
}

pub(super) enum DecodedCell {
    Scalar(Value),
    Nested(ArrayRef),
}

impl DecodedCell {
    pub fn from_value(name: &str, value: Value, memory: &Arc<MemoryBudget>) -> Result<(Field, Self)> {
        let logical = LogicalType::join_all([&value.logical_type()]);
        let field = rtdb_columnar::to_field(name, &logical)?;
        let bound = rtdb_columnar::estimate_builder_bytes(&logical, std::slice::from_ref(&value))?;
        let _scratch = memory.reserve(bound)?;
        let cell = match value {
            Value::Null | Value::Bool(_) | Value::I16(_) | Value::I32(_) | Value::I64(_) |
            Value::F32(_) | Value::F64(_) | Value::Utf8(_) | Value::Binary(_) | Value::Transit(_) |
            Value::Uuid(_) | Value::Timestamp { .. } => Self::Scalar(value),
            _ => Self::Nested(rtdb_columnar::copy_array(&logical, &[value])?),
        };
        Ok((field, cell))
    }

    pub fn retained(&self) -> Result<usize> {
        let bytes = match self {
            Self::Scalar(Value::Utf8(text)) => text.len(),
            Self::Scalar(Value::Binary(bytes) | Value::Transit(bytes)) => bytes.len(),
            Self::Scalar(_) => 0,
            Self::Nested(array) => array.get_array_memory_size(),
        };
        bytes.checked_mul(4).and_then(|bytes| bytes.checked_add(128)).ok_or_else(overflow)
    }
}

struct PendingColumn {
    field: Field,
    values: ColumnValues,
}

macro_rules! columns {
    ($($variant:ident($builder:ty, $new:expr, $($value:ident)|+)),+ $(,)?) => {
        enum ColumnValues {
            $($variant($builder),)+
            Null,
            Uuid(FixedSizeBinaryBuilder),
            Timestamp(Vec<i64>),
            Nested(Vec<ArrayRef>),
        }

        impl ColumnValues {
            fn new(cell: &DecodedCell) -> Self {
                match cell {
                    $(DecodedCell::Scalar($(Value::$value(_))|+) => Self::$variant($new),)+
                    DecodedCell::Scalar(Value::Null) => Self::Null,
                    DecodedCell::Scalar(Value::Uuid(_)) => Self::Uuid(FixedSizeBinaryBuilder::with_capacity(0, 16)),
                    DecodedCell::Scalar(Value::Timestamp { .. }) => Self::Timestamp(Vec::new()),
                    DecodedCell::Nested(_) => Self::Nested(Vec::new()),
                    _ => unreachable!("decoded WAL scalar has a column builder"),
                }
            }

            fn accepts(&self, cell: &DecodedCell) -> bool {
                matches!((self, cell),
                    $((Self::$variant(_), DecodedCell::Scalar($(Value::$value(_))|+)) |)+
                    (Self::Null, DecodedCell::Scalar(Value::Null)) |
                    (Self::Uuid(_), DecodedCell::Scalar(Value::Uuid(_))) |
                    (Self::Timestamp(_), DecodedCell::Scalar(Value::Timestamp { .. })) |
                    (Self::Nested(_), DecodedCell::Nested(_))
                )
            }

            fn push(&mut self, cell: DecodedCell) {
                match (self, cell) {
                    $((Self::$variant(builder), DecodedCell::Scalar($(Value::$value(value))|+)) => builder.append_value(value),)+
                    (Self::Null, DecodedCell::Scalar(Value::Null)) => {},
                    (Self::Uuid(builder), DecodedCell::Scalar(Value::Uuid(uuid))) => {
                        builder.append_value(uuid).expect("UUID has the column width");
                    }
                    (Self::Timestamp(values), DecodedCell::Scalar(Value::Timestamp { value, .. })) => values.push(value),
                    (Self::Nested(cells), DecodedCell::Nested(array)) => cells.push(array),
                    _ => unreachable!("pending column accepts its decoded layout"),
                }
            }

            fn finish(self, field: &Field, count: usize) -> Result<ArrayRef> {
                match self {
                    $(Self::$variant(mut builder) => Ok(Arc::new(builder.finish())),)+
                    Self::Null => Ok(Arc::new(NullArray::new(count))),
                    Self::Uuid(mut builder) => Ok(Arc::new(builder.finish())),
                    Self::Timestamp(values) => timestamp_column(field, values),
                    Self::Nested(mut cells) if cells.len() == 1 => Ok(cells.remove(0)),
                    Self::Nested(cells) => {
                        let arrays = cells.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
                        arrow_select::concat::concat(&arrays).map_err(protocol)
                    }
                }
            }
        }
    };
}

columns!(
    Bool(BooleanBuilder, BooleanBuilder::with_capacity(0), Bool),
    Int16(Int16Builder, Int16Builder::with_capacity(0), I16),
    Int32(Int32Builder, Int32Builder::with_capacity(0), I32),
    Int64(Int64Builder, Int64Builder::with_capacity(0), I64),
    Float32(Float32Builder, Float32Builder::with_capacity(0), F32),
    Float64(Float64Builder, Float64Builder::with_capacity(0), F64),
    Text(StringBuilder, StringBuilder::with_capacity(0, 0), Utf8),
    Bytes(BinaryBuilder, BinaryBuilder::with_capacity(0, 0), Binary | Transit),
);

fn timestamp_column(field: &Field, values: Vec<i64>) -> Result<ArrayRef> {
    let DataType::Timestamp(unit, timezone) = field.data_type() else {
        return Err(protocol("Timestamp builder has a non-timestamp field"));
    };
    let timezone = timezone.as_deref();
    Ok(match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values).with_timezone_opt(timezone)),
        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(timezone)),
        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(timezone)),
        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(timezone)),
    })
}
