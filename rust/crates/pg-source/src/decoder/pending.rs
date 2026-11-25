use std::{collections::BTreeMap, hash::{DefaultHasher, Hash, Hasher}, ops::Range, sync::Arc};

use arrow_array::{
    ArrayRef, FixedSizeListArray, NullArray, RecordBatch, RecordBatchOptions,
    TimestampSecondArray, TimestampMillisecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    builder::{BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_pg_codec::{WalListColumn, WalListRow, tstzrange_field};
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
        &mut self, id: u32, table: &TableRef, kind: RowKind, cells: Vec<(Field, DecodedCell<'_>)>,
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
            let group = &mut self.groups[index];
            group.check_append(&cells)?;
            let row = group.count;
            Arc::get_mut(&mut group.reservation).expect("pending batch owns its reservation").absorb(charge)?;
            group.push(cells);
            let charge = Arc::get_mut(&mut group.reservation).expect("pending batch owns its reservation");
            charge.shrink_to(charge.bytes() - metadata)?;
            row
        } else {
            let mut group = PendingBatch::new(table.clone(), kind, &cells, reservation, metadata);
            group.check_append(&cells)?;
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
        table: TableRef, kind: RowKind, cells: &[(Field, DecodedCell<'_>)], reservation: Arc<MemoryReservation>, metadata: usize,
    ) -> Self {
        let columns = cells.iter().map(|(field, cell)| PendingColumn {
            field: field.clone(),
            values: ColumnValues::new(field, cell),
        }).collect();
        Self { table, kind, count: 0, metadata, columns, reservation }
    }

    fn matches(&self, table: &TableRef, cells: &[(Field, DecodedCell<'_>)]) -> bool {
        self.table == *table && self.columns.len() == cells.len()
            && self.columns.iter().zip(cells).all(|(column, (field, cell))| column.field == *field && column.values.accepts(cell))
    }

    fn check_append(&self, cells: &[(Field, DecodedCell<'_>)]) -> Result<()> {
        self.columns.iter().zip(cells).try_for_each(|(column, (_, cell))| column.values.check_append(cell))
    }

    fn push(&mut self, cells: Vec<(Field, DecodedCell<'_>)>) {
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

pub(super) enum DecodedCell<'a> {
    Scalar(Value),
    Nested(ArrayRef),
    List(WalListRow<'a>),
    ArrayText(&'a str),
    Range([i64; 2]),
}

impl DecodedCell<'_> {
    pub fn from_value(name: &str, value: Value, memory: &Arc<MemoryBudget>) -> Result<(Field, Self)> {
        if let Value::TimestampTzRange { lower, upper } = &value {
            return Ok((tstzrange_field(name, false)?, Self::Range([*lower, *upper])));
        }
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
            Self::List(row) => row.builder_bytes()?,
            Self::ArrayText(text) => text.len(),
            Self::Range(bounds) => std::mem::size_of_val(bounds),
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
            List(WalListColumn),
            ArrayText(StringBuilder),
            Range(Vec<i64>),
        }

        impl ColumnValues {
            fn new(field: &Field, cell: &DecodedCell<'_>) -> Self {
                match cell {
                    $(DecodedCell::Scalar($(Value::$value(_))|+) => Self::$variant($new),)+
                    DecodedCell::Scalar(Value::Null) => Self::Null,
                    DecodedCell::Scalar(Value::Uuid(_)) => Self::Uuid(FixedSizeBinaryBuilder::with_capacity(0, 16)),
                    DecodedCell::Scalar(Value::Timestamp { .. }) => Self::Timestamp(Vec::new()),
                    DecodedCell::Nested(_) => WalListColumn::new(field).map_or_else(|| Self::Nested(Vec::new()), Self::List),
                    DecodedCell::List(_) => Self::List(WalListColumn::new(field).expect("WAL list field has a builder")),
                    DecodedCell::ArrayText(_) => Self::ArrayText(StringBuilder::with_capacity(0, 0)),
                    DecodedCell::Range(_) => Self::Range(Vec::new()),
                    _ => unreachable!("decoded WAL scalar has a column builder"),
                }
            }

            fn accepts(&self, cell: &DecodedCell<'_>) -> bool {
                matches!((self, cell),
                    $((Self::$variant(_), DecodedCell::Scalar($(Value::$value(_))|+)) |)+
                    (Self::Null, DecodedCell::Scalar(Value::Null)) |
                    (Self::Uuid(_), DecodedCell::Scalar(Value::Uuid(_))) |
                    (Self::Timestamp(_), DecodedCell::Scalar(Value::Timestamp { .. })) |
                    (Self::Nested(_), DecodedCell::Nested(_)) |
                    (Self::List(_), DecodedCell::List(_) | DecodedCell::Nested(_)) |
                    (Self::ArrayText(_), DecodedCell::ArrayText(_)) |
                    (Self::Range(_), DecodedCell::Range(_))
                )
            }

            fn check_append(&self, cell: &DecodedCell<'_>) -> Result<()> {
                match (self, cell) {
                    (Self::List(builder), DecodedCell::List(row)) => builder.check_row(row),
                    (Self::List(builder), DecodedCell::Nested(array)) => builder.check_array(array.as_ref()),
                    (Self::ArrayText(builder), DecodedCell::ArrayText(text)) => {
                        let end = builder.values_slice().len().checked_add(text.len()).ok_or_else(overflow)?;
                        i32::try_from(end).map_err(protocol)?;
                        Ok(())
                    }
                    _ => Ok(()),
                }
            }

            fn push(&mut self, cell: DecodedCell<'_>) {
                match (self, cell) {
                    $((Self::$variant(builder), DecodedCell::Scalar($(Value::$value(value))|+)) => builder.append_value(value),)+
                    (Self::Null, DecodedCell::Scalar(Value::Null)) => {},
                    (Self::Uuid(builder), DecodedCell::Scalar(Value::Uuid(uuid))) => {
                        builder.append_value(uuid).expect("UUID has the column width");
                    }
                    (Self::Timestamp(values), DecodedCell::Scalar(Value::Timestamp { value, .. })) => values.push(value),
                    (Self::Nested(cells), DecodedCell::Nested(array)) => cells.push(array),
                    (Self::List(builder), DecodedCell::List(row)) => builder.push(row),
                    (Self::List(builder), DecodedCell::Nested(array)) => builder.push_array(array.as_ref()),
                    (Self::ArrayText(builder), DecodedCell::ArrayText(text)) => builder.append_value(text),
                    (Self::Range(values), DecodedCell::Range(bounds)) => values.extend_from_slice(&bounds),
                    _ => unreachable!("pending column accepts its decoded layout"),
                }
            }

            fn finish(self, field: &Field, count: usize) -> Result<ArrayRef> {
                match self {
                    $(Self::$variant(mut builder) => Ok(Arc::new(builder.finish())),)+
                    Self::Null => Ok(Arc::new(NullArray::new(count))),
                    Self::Uuid(mut builder) => Ok(Arc::new(builder.finish())),
                    Self::Timestamp(values) => timestamp_column(field, values),
                    Self::List(builder) => builder.finish(field),
                    Self::ArrayText(mut builder) => Ok(Arc::new(builder.finish())),
                    Self::Range(values) => range_column(field, values),
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

fn range_column(field: &Field, values: Vec<i64>) -> Result<ArrayRef> {
    let DataType::FixedSizeList(child, 2) = field.data_type() else {
        return Err(protocol("Range builder has a field other than a pair of timestamps"));
    };
    let values = timestamp_column(child, values)?;
    let array = FixedSizeListArray::try_new(Arc::clone(child), 2, values, None).map_err(protocol)?;
    Ok(Arc::new(array))
}
