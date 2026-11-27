//! Equi-joins of batch inputs on column equalities.
//! Build rows are indexed in input order and the probe gathers its matches from the saved column positions.
use std::{borrow::Cow, cmp::Ordering, collections::VecDeque, ops::{ControlFlow, Range}};
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{BinaryOp, Expr, JoinKind};
use arrow_array::{
    Array,
    cast::AsArray,
    types::{
        Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
        DurationNanosecondType, DurationSecondType, IntervalYearMonthType, Time32MillisecondType, Time32SecondType,
        Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};
use rtdb_types::{IntervalUnit, LogicalType, Result, ScalarType, TimeUnit, Value};

use crate::columnar::{
    BatchCursor, BoxBatchCursor, Column, ColumnBatch, gather_column_rows, gather_column_rows_iter,
    gather_join_probe, gather_nullable_column, poll_nested_batch,
};
use crate::context::Execution;
use crate::invalid;
use crate::join_key::JoinLayout;
use crate::join_spill::{BlockWriter, Partitions, ReplayStats};
use crate::join_storage::{HashIndex, combine_cell_hash};
use crate::operators::{BatchOutput, build_batch_input};
use crate::pushdown::{PushdownBuilder, Pushdowns};
use crate::runtime_schema::EmittedRelation;
use crate::scalar_key::{ScalarColumn, ScalarKey};
use crate::set_keys::{JoinHash, Key, KeyColumns, TypedKeys, direct_key_equal};

/// For each equality, whether its first operand is the build side's key.
pub(crate) fn key_order(layout: &JoinLayout) -> Option<Vec<bool>> {
    layout.comparisons.iter().take(layout.left.key_count()).map(|comparison| match comparison {
        Expr::Binary { op: BinaryOp::Eq, left, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column { id: first, .. }, Expr::Column { .. }) => Some(layout.left.schema.get(*first).is_some()),
            _ => None,
        },
        _ => None,
    }).collect()
}

enum ThetaOperand {
    Build(usize),
    Probe(usize),
    Literal(Option<ThetaValue<'static>>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ThetaKind {
    Number,
    Text,
    Bool,
    DateDay,
    DateMilli,
    Timestamp(TimeUnit, bool),
    Time(TimeUnit),
    Duration(TimeUnit),
    YearMonth,
}

impl ThetaKind {
    fn scalar(ty: &ScalarType) -> Option<Self> {
        Some(match ty {
            ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64 | ScalarType::F32 | ScalarType::F64 => Self::Number,
            ScalarType::Utf8 => Self::Text,
            ScalarType::Bool => Self::Bool,
            ScalarType::DateDay => Self::DateDay,
            ScalarType::DateMilli => Self::DateMilli,
            ScalarType::Timestamp { unit, timezone } => Self::Timestamp(*unit, timezone.is_some()),
            ScalarType::Time { unit, .. } => Self::Time(*unit),
            ScalarType::Duration(unit) => Self::Duration(*unit),
            ScalarType::Interval(IntervalUnit::YearMonth) => Self::YearMonth,
            _ => return None,
        })
    }

    fn temporal_ticks(self, array: &dyn Array, row: usize) -> Option<i64> {
        use ArrowTimeUnit as A;
        use TimeUnit as T;
        macro_rules! ticks {
            ($ty:ty) => { array.as_primitive_opt::<$ty>()?.value(row) as i64 };
        }
        Some(match (self, array.data_type()) {
            (Self::DateDay, DataType::Date32) => ticks!(Date32Type),
            (Self::DateMilli, DataType::Date64) => ticks!(Date64Type).div_euclid(rtdb_types::java_date::MILLIS_PER_DAY),
            (Self::Timestamp(T::Second, zoned), DataType::Timestamp(A::Second, zone)) if zoned == zone.is_some() => ticks!(TimestampSecondType),
            (Self::Timestamp(T::Millisecond, zoned), DataType::Timestamp(A::Millisecond, zone)) if zoned == zone.is_some() => ticks!(TimestampMillisecondType),
            (Self::Timestamp(T::Microsecond, zoned), DataType::Timestamp(A::Microsecond, zone)) if zoned == zone.is_some() => ticks!(TimestampMicrosecondType),
            (Self::Timestamp(T::Nanosecond, zoned), DataType::Timestamp(A::Nanosecond, zone)) if zoned == zone.is_some() => ticks!(TimestampNanosecondType),
            (Self::Time(T::Second), DataType::Time32(A::Second)) => ticks!(Time32SecondType),
            (Self::Time(T::Millisecond), DataType::Time32(A::Millisecond)) => ticks!(Time32MillisecondType),
            (Self::Time(T::Microsecond), DataType::Time64(A::Microsecond)) => ticks!(Time64MicrosecondType),
            (Self::Time(T::Nanosecond), DataType::Time64(A::Nanosecond)) => ticks!(Time64NanosecondType),
            (Self::Duration(T::Second), DataType::Duration(A::Second)) => ticks!(DurationSecondType),
            (Self::Duration(T::Millisecond), DataType::Duration(A::Millisecond)) => ticks!(DurationMillisecondType),
            (Self::Duration(T::Microsecond), DataType::Duration(A::Microsecond)) => ticks!(DurationMicrosecondType),
            (Self::Duration(T::Nanosecond), DataType::Duration(A::Nanosecond)) => ticks!(DurationNanosecondType),
            (Self::YearMonth, DataType::Interval(arrow_schema::IntervalUnit::YearMonth)) => ticks!(IntervalYearMonthType),
            _ => return None,
        })
    }

    fn arrow(self, column: &Column, row: usize) -> Option<Option<ThetaValue<'_>>> {
        if matches!(self, Self::Number | Self::Text | Self::Bool) {
            return Some(match (self, ScalarKey::read(column, row)?) {
                (_, ScalarKey::Null) => None,
                (Self::Number, ScalarKey::Signed(value)) => Some(ThetaValue::integer(value)),
                (Self::Number, ScalarKey::Float(value)) => Some(ThetaValue::Number(ThetaNumber::Float(value))),
                (Self::Text, ScalarKey::Text(value)) => Some(ThetaValue::Text { value: Cow::Borrowed(value), _charge: None }),
                (Self::Bool, ScalarKey::Bool(value)) => Some(ThetaValue::Bool(value)),
                _ => return None,
            });
        }
        let Some((array, field, logical, row)) = column.arrow_cell(row)? else { return Some(None); };
        if array.is_null(row) || (field.data_type() == &DataType::Null && field.is_nullable()) { return Some(None); }
        if field.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY)
            || logical.is_some_and(|logical| {
                let logical = crate::columnar::float_vector::nonnull(logical);
                !matches!(logical, LogicalType::Scalar(scalar) if Self::scalar(scalar) == Some(self))
            })
        {
            return None;
        }
        self.temporal_ticks(array, row).map(|ticks| Some(ThetaValue::integer(ticks)))
    }

    fn fixed_value(self, value: &Value) -> Option<ThetaValue<'static>> {
        Some(match (self, value) {
            (Self::Number, value) => ThetaValue::Number(ThetaNumber::from_value(value)?),
            (Self::Bool, Value::Bool(value)) => ThetaValue::Bool(*value),
            (Self::DateDay | Self::DateMilli, value) => ThetaValue::integer(value.date_days()?),
            (Self::Timestamp(expected, zoned), Value::Timestamp { value, unit, timezone })
                if expected == *unit && zoned == timezone.is_some() => ThetaValue::integer(*value),
            (Self::Time(expected), Value::Time { value, unit, .. })
            | (Self::Duration(expected), Value::Duration { value, unit })
                if expected == *unit => ThetaValue::integer(*value),
            (Self::YearMonth, Value::IntervalYearMonth(value)) => ThetaValue::integer(i64::from(*value)),
            _ => return None,
        })
    }

    fn literal(self, value: &Value, memory: &Arc<MemoryBudget>) -> Option<Option<ThetaValue<'static>>> {
        Some(match value {
            Value::Null => None,
            Value::Utf8(value) if self == Self::Text => {
                let charge = memory.reserve(value.len()).ok()?;
                Some(ThetaValue::Text { value: Cow::Owned(value.clone()), _charge: Some(charge) })
            }
            Value::Date(_) => return None,
            value => Some(self.fixed_value(value)?),
        })
    }
}

#[derive(Clone, Copy)]
enum ThetaNumber {
    Integer(i128),
    Float(f64),
}

impl ThetaNumber {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::F32(value) => Some(Self::Float(f64::from(*value))),
            Value::F64(value) => Some(Self::Float(*value)),
            _ => rtdb_expr::integer(value).map(Self::Integer),
        }
    }

    fn compare(self, other: Self, equality: bool) -> Option<Ordering> {
        fn floats(left: f64, right: f64, equality: bool) -> Option<Ordering> {
            if equality { return left.partial_cmp(&right); }
            if left.is_nan() || right.is_nan() { return None; }
            Some(left.total_cmp(&right))
        }
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => Some(left.cmp(&right)),
            (Self::Integer(left), Self::Float(right)) => floats(left as f64, right, equality),
            (Self::Float(left), Self::Integer(right)) => floats(left, right as f64, equality),
            (Self::Float(left), Self::Float(right)) => floats(left, right, equality),
        }
    }
}

enum ThetaValue<'a> {
    Number(ThetaNumber),
    Text { value: Cow<'a, str>, _charge: Option<Arc<MemoryReservation>> },
    Bool(bool),
}

impl ThetaValue<'_> {
    fn integer(value: i64) -> Self { Self::Number(ThetaNumber::Integer(i128::from(value))) }

    fn borrowed(&self) -> ThetaValue<'_> {
        match self {
            Self::Number(value) => ThetaValue::Number(*value),
            Self::Text { value, .. } => ThetaValue::Text { value: Cow::Borrowed(value), _charge: None },
            Self::Bool(value) => ThetaValue::Bool(*value),
        }
    }

    fn compare(&self, other: &Self, equality: bool) -> Result<Option<Ordering>> {
        Ok(match (self, other) {
            (Self::Number(left), Self::Number(right)) => left.compare(*right, equality),
            (Self::Text { value: left, .. }, Self::Text { value: right, .. }) => Some(left.as_bytes().cmp(right.as_bytes())),
            (Self::Bool(left), Self::Bool(right)) => Some(left.cmp(right)),
            _ => return Err(invalid("join-key", "Join predicate comparison operands have different types")),
        })
    }
}

struct ThetaTest { left: ThetaOperand, right: ThetaOperand, op: BinaryOp, kind: ThetaKind }

#[derive(Default)]
pub(crate) struct ThetaTests {
    tests: Vec<ThetaTest>,
    use_residual_keys: bool,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl ThetaTests {
    fn is_empty(&self) -> bool { self.tests.is_empty() }
}

/// Whether no value of the type can hold a date, which is all that admitting a projected input's dates changes.
fn date_free(ty: &LogicalType) -> bool {
    match ty {
        LogicalType::Nothing | LogicalType::Null => true,
        LogicalType::Scalar(ScalarType::DateDay | ScalarType::DateMilli) => false,
        LogicalType::Scalar(_) => true,
        LogicalType::List { element, .. } => date_free(element),
        LogicalType::Struct(fields) => fields.values().all(date_free),
        LogicalType::Maybe(value) | LogicalType::RunEndEncoded { value, .. } => date_free(value),
        LogicalType::Union(legs) => legs.iter().all(date_free),
    }
}

pub(crate) fn theta_tests(layout: &JoinLayout, memory: &Arc<MemoryBudget>) -> Option<ThetaTests> {
    fn operand<'a>(layout: &'a JoinLayout, expr: &Expr, memory: &Arc<MemoryBudget>)
        -> Option<(ThetaOperand, Cow<'a, LogicalType>)> {
        let operand = match expr {
            Expr::Column { id, .. } => {
                if let Some(position) = layout.left.schema.position(*id) {
                    (ThetaOperand::Build(position), &layout.left.schema.fields()[position].data_type)
                } else {
                    let position = layout.right.schema.position(*id)?;
                    (ThetaOperand::Probe(position), &layout.right.schema.fields()[position].data_type)
                }
            }
            Expr::Literal(value) => {
                let ty = value.logical_type();
                let kind = comparison(&ty, &ty)?;
                return Some((ThetaOperand::Literal(kind.literal(value, memory)?), Cow::Owned(ty)));
            }
            _ => return None,
        };
        Some((operand.0, Cow::Borrowed(operand.1)))
    }
    fn scalar(ty: &LogicalType) -> Option<Option<&ScalarType>> {
        match ty {
            LogicalType::Scalar(ty) => Some(Some(ty)),
            LogicalType::Null => Some(None),
            LogicalType::Maybe(ty) => scalar(ty),
            _ => None,
        }
    }
    fn comparison(left: &LogicalType, right: &LogicalType) -> Option<ThetaKind> {
        match (scalar(left)?, scalar(right)?) {
            (None, None) => Some(ThetaKind::Number),
            (Some(ty), None) | (None, Some(ty)) => ThetaKind::scalar(ty),
            (Some(left), Some(right)) if left == right => ThetaKind::scalar(left),
            (Some(left), Some(right))
                if ThetaKind::scalar(left) == Some(ThetaKind::Number)
                    && ThetaKind::scalar(right) == Some(ThetaKind::Number) => Some(ThetaKind::Number),
            _ => None,
        }
    }
    fn tests(layout: &JoinLayout, expr: &Expr, output: &mut ThetaTests, memory: &Arc<MemoryBudget>) -> Option<()> {
        let Expr::Binary { op, left, right } = expr else { return None; };
        if *op == BinaryOp::And {
            tests(layout, left, output, memory)?;
            return tests(layout, right, output, memory);
        }
        if !matches!(op, BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq) {
            return None;
        }
        let (left, left_type) = operand(layout, left, memory)?;
        let (right, right_type) = operand(layout, right, memory)?;
        let kind = comparison(&left_type, &right_type)?;
        let bytes = output.tests.len().checked_add(1)?.checked_mul(4 * size_of::<ThetaTest>())?;
        if let Some(reservation) = &mut output._reservation {
            Arc::get_mut(reservation)?.grow_to(bytes).ok()?;
        } else {
            output._reservation = Some(memory.reserve(bytes).ok()?);
        }
        output.tests.push(ThetaTest { left, right, op: *op, kind });
        output.use_residual_keys |= kind != ThetaKind::Number;
        Some(())
    }
    let mut output = ThetaTests::default();
    for expr in layout.comparisons.iter().skip(layout.left.key_count()) { tests(layout, expr, &mut output, memory)?; }
    Some(output)
}

#[test]
fn theta_binding_accepts_plain_comparable_types_and_declines_temporal_coercions() {
    use rtdb_plan::{ColumnId, Field, Relation, Schema};

    let input = |id, ty| {
        Relation::empty(Schema::new(vec![Field {
            id: ColumnId(id),
            name: format!("v{id}"),
            qualifier: None,
            qualified_only: false,
            unnamed: false,
            data_type: LogicalType::Scalar(ty).maybe(),
        }]).unwrap())
    };
    let timestamp = |unit, timezone: Option<&str>| ScalarType::Timestamp {
        unit, timezone: timezone.map(str::to_owned),
    };
    let micros = timestamp(TimeUnit::Microsecond, Some("UTC"));
    let seconds = timestamp(TimeUnit::Second, Some("UTC"));
    let local = timestamp(TimeUnit::Microsecond, None);
    let memory = MemoryBudget::new(1 << 20);
    for (left, right, admitted) in [
        (ScalarType::Utf8, ScalarType::Utf8, true),
        (ScalarType::Bool, ScalarType::Bool, true),
        (ScalarType::DateDay, ScalarType::DateDay, true),
        (ScalarType::DateMilli, ScalarType::DateMilli, true),
        (micros.clone(), micros.clone(), true),
        (micros.clone(), seconds, false),
        (micros, local, false),
        (ScalarType::Utf8, ScalarType::Keyword, false),
        (ScalarType::DateDay, ScalarType::DateMilli, false),
        (ScalarType::Duration(TimeUnit::Second), ScalarType::Duration(TimeUnit::Second), true),
        (ScalarType::I64, ScalarType::F32, true),
        (ScalarType::Interval(IntervalUnit::MonthDayNano), ScalarType::Interval(IntervalUnit::MonthDayNano), false),
    ] {
        let left = input(0, left);
        let right = input(1, right);
        let condition = Expr::binary(BinaryOp::Lt, left.schema().fields()[0].expression(), right.schema().fields()[0].expression());
        let layout = JoinLayout::new(&left, &right, Some(&condition), &memory).unwrap();
        assert_eq!(theta_tests(&layout, &memory).is_some(), admitted, "{condition:?}");
    }
    assert_eq!(memory.used_bytes(), 0);
}

pub(crate) struct Residual {
    schema: rtdb_plan::Schema,
    expressions: Vec<Expr>,
    sides: Vec<(bool, usize)>,
    _reservation: Arc<MemoryReservation>,
}

struct PreparedResidual<'a> {
    comparisons: Vec<rtdb_expr::ExpressionInvocation<'a>>,
    sides: &'a [(bool, usize)],
    _metadata: rtdb_expr::EvaluationScope,
}

struct ResidualCandidate {
    build: (usize, usize),
    probe: usize,
    known: bool,
}

struct JoinRow<'a> {
    build: &'a ColumnBatch,
    build_row: usize,
    probe: &'a ColumnBatch,
    probe_row: usize,
    sides: &'a [(bool, usize)],
    admit_dates: [bool; 2],
}

impl JoinRow<'_> {
    fn column(&self, index: usize) -> Option<(&Column, usize, bool)> {
        let &(on_build, column) = self.sides.get(index)?;
        if on_build { Some((self.build.columns.get(column)?, self.build_row, self.admit_dates[0])) }
        else { Some((self.probe.columns.get(column)?, self.probe_row, self.admit_dates[1])) }
    }
}

impl rtdb_expr::RowInput for JoinRow<'_> {
    fn arrow_cell(&self, index: usize) -> Option<rtdb_columnar::ArrowCell<'_>> {
        let (column, row, _) = self.column(index)?;
        column.expression_cell(row)
    }

    fn value(&self, index: usize, allocation: Option<&rtdb_expr::EvaluationScope>) -> Result<Option<Value>> {
        self.column(index).map(|(column, row, admit_dates)| column.expression_value(row, admit_dates, allocation)).transpose()
    }
}

impl Residual {
    pub fn new(layout: &JoinLayout, execution: &Execution) -> Result<Self> {
        let expressions = &layout.comparisons[layout.left.key_count()..];
        let bytes = expressions.iter().try_fold(crate::runtime_schema::schema_bound(&layout.theta_schema)?, |bytes, expr| {
            bytes.checked_add(crate::runtime_schema::expression_bound(expr)?).ok_or_else(size_error)
        })?;
        let reservation = execution.context.memory.reserve(bytes.checked_mul(4).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(size_error)?)?;
        let columns = expressions.iter().flat_map(Expr::referenced_columns).collect::<std::collections::BTreeSet<_>>();
        let mut sides = Vec::with_capacity(columns.len());
        let mut fields = Vec::with_capacity(columns.len());
        for id in columns {
            if let Some(position) = layout.left.schema.position(id) {
                fields.push(layout.left.schema.fields()[position].clone());
                sides.push((true, position));
            } else if let Some(position) = layout.right.schema.position(id) {
                fields.push(layout.right.schema.fields()[position].clone());
                sides.push((false, position));
            } else {
                return Err(invalid("join-key", "A join predicate column is absent"));
            }
        }
        Ok(Self { schema: rtdb_plan::Schema::new(fields)?, expressions: expressions.to_vec(), sides, _reservation: reservation })
    }

    fn prepare(&self, execution: &Execution) -> Result<PreparedResidual<'_>> {
        let metadata = execution.scope()?;
        metadata.reserve(self.expressions.len().checked_mul(std::mem::size_of::<rtdb_expr::ExpressionInvocation<'_>>()).ok_or_else(size_error)?)?;
        let comparisons = self.expressions.iter().map(|expr| execution.prepare_expression(expr, &self.schema, &metadata)).collect::<Result<_>>()?;
        Ok(PreparedResidual { comparisons, sides: &self.sides, _metadata: metadata })
    }
}

#[derive(Clone)]
pub(crate) struct ProbeInput { pub runtime: Arc<EmittedRelation>, pub execution: Execution, pub pushdowns: Option<Arc<Pushdowns>>, pub output: BatchOutput }

enum State {
    Build { build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout },
    Probe { table: Table, probe: BoxBatchCursor },
    Disk(DiskJoin),
    Done,
}

struct Table {
    batches: Vec<ColumnBatch>,
    starts: Vec<usize>,
    index: HashIndex,
    anti_key_layout: Option<usize>,
    rows: usize,
    unmatched: Option<(Vec<u8>, Arc<MemoryReservation>)>,
    _starts: Arc<MemoryReservation>,
    _inventory: Arc<MemoryReservation>,
}

struct HashedProbe<'a> {
    keys: TypedKeys<'a>,
    hashes: Vec<u32>,
    _reservation: Arc<MemoryReservation>,
}

#[derive(Clone, Copy)]
enum ProbeCandidates<'a> {
    BuildOrder,
    HashBuckets(Option<&'a HashedProbe<'a>>),
    AntiHashBuckets,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AntiKeyKind {
    Null,
    Bool,
    Signed,
    Float,
    Bytes,
}

impl AntiKeyKind {
    fn of(column: &Column) -> Option<Self> {
        let reader = match column {
            Column::Arrow { array, field, logical } => ScalarColumn::bind(array.as_ref(), field, logical.as_ref())?,
            Column::Selected(selected) => {
                let mut sources = selected.source_layouts();
                let Some((array, field, logical)) = sources.next() else { return Some(Self::Null); };
                if sources.next().is_some() { return None; }
                ScalarColumn::bind(array, field, logical)?
            }
            Column::Values(_) => return None,
        };
        Some(match reader {
            ScalarColumn::Null => Self::Null,
            ScalarColumn::Bool(_) => Self::Bool,
            ScalarColumn::I8(_) | ScalarColumn::I16(_) | ScalarColumn::I32(_) | ScalarColumn::I64(_) => Self::Signed,
            ScalarColumn::F32(_) | ScalarColumn::F64(_) => Self::Float,
            ScalarColumn::Text(_) | ScalarColumn::Bytes(_) => Self::Bytes,
        })
    }
}

impl Table {
    fn position(&self, ordinal: usize) -> (usize, usize) {
        let batch = self.starts.partition_point(|&start| start <= ordinal) - 1;
        (batch, ordinal - self.starts[batch])
    }
}

struct DiskJoin {
    build: Partitions,
    probe: Partitions,
    table: Option<Table>,
    build_partition: usize,
    probe_partition: usize,
    ready: Option<(VecDeque<ColumnBatch>, Arc<MemoryReservation>)>,
}

impl DiskJoin {
    async fn next(&mut self, spec: &mut Spec) -> Result<Option<ColumnBatch>> {
        let context = &spec.execution.context;
        let memory = &context.memory;
        loop {
            context.check_cancelled()?;
            if let Some(batch) = self.ready.as_mut().and_then(|(batches, _)| batches.pop_front()) {
                return Ok(Some(batch));
            }
            self.ready = None;
            let builds = self.build.count();
            if self.build_partition == builds { return Ok(None); }
            if self.probe_partition == self.probe.count() / builds {
                let table = self.table.take().expect("The completed partition has a build table");
                self.build_partition += 1;
                self.probe_partition = 0;
                if let Some(unmatched) = spec.unmatched_build(&table)? { return Ok(Some(unmatched)); }
                continue;
            }
            let inventory = |frames: usize| frames.checked_mul(2 * std::mem::size_of::<ColumnBatch>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error);
            if self.table.is_none() {
                let count = self.build.frames(self.build_partition);
                let reservation = memory.reserve(inventory(count)?)?;
                let mut batches = Vec::with_capacity(count);
                let mut rows = 0usize;
                if let Some(stats) = &mut spec.spill_stats {
                    stats.replay.start_build_partition();
                }
                for frame in 0..count {
                    context.check_cancelled()?;
                    let batch = self.build.read(self.build_partition, frame)?;
                    if let Some(stats) = &mut spec.spill_stats {
                        stats.replay.build_frame(batch.len);
                    }
                    rows = rows.checked_add(batch.len).ok_or_else(size_error)?;
                    batches.push(batch);
                }
                let mut table = spec.table(batches, rows, reservation)?;
                if spec.tracks_build() {
                    let charge = memory.reserve(rows)?;
                    table.unmatched = Some((vec![1; rows], charge));
                }
                self.table = Some(table);
            }
            let table = self.table.as_mut().expect("The build partition is loaded");
            let partition = self.probe_partition * builds + self.build_partition;
            let count = self.probe.frames(partition);
            let reservation = memory.reserve(inventory(count)?)?;
            let mut ready = VecDeque::with_capacity(count);
            let mut rows = 0usize;
            // A probe partition's error must surface before any of its matches.
            for frame in 0..count {
                context.check_cancelled()?;
                let batch = self.probe.read(partition, frame)?;
                if let Some(stats) = &mut spec.spill_stats {
                    stats.replay.probe_frame();
                }
                if let Some(batch) = spec.probe_batch(table, batch).await? {
                    rows = rows.checked_add(batch.len).filter(|rows| *rows <= i32::MAX as usize).ok_or_else(size_error)?;
                    ready.push_back(batch);
                }
            }
            self.ready = Some((ready, reservation));
            self.probe_partition += 1;
        }
    }
}

pub(crate) struct BatchJoin { state: State, spec: Spec }

#[derive(Default)]
struct JoinSpillStats {
    bytes_written: u64,
    replay: ReplayStats,
}

struct Spec {
    order: Vec<bool>, build_keys: Vec<usize>, probe_keys: Vec<usize>, widths: [usize; 2], kind: JoinKind, count_only: bool,
    projected: [bool; 2], swapped: bool, _original_layout_charge: Option<Arc<MemoryReservation>>, theta: ThetaTests, residual: Option<Residual>, execution: Execution,
    hash: JoinHash,
    spill_stats: Option<JoinSpillStats>,
}

impl BatchJoin {
    pub fn new(build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout, order: Vec<bool>, projected: [bool; 2], execution: Execution) -> Self {
        let build_keys = layout.left.key_positions().to_vec();
        let probe_keys = layout.right.key_positions().to_vec();
        let widths = [layout.left.original.len(), layout.right.original.len()];
        let spill_stats = crate::trace::operator_evidence_enabled(&execution.context).then(JoinSpillStats::default);
        Self { state: State::Build { build, probe, layout }, spec: Spec { order, build_keys, probe_keys, widths, kind: JoinKind::Inner, count_only: false,
            projected, swapped: false, _original_layout_charge: None, theta: ThetaTests::default(), residual: None, execution, hash: JoinHash::Vector,
            spill_stats } }
    }
}

impl BatchJoin {
    pub fn swapped(mut self, original_layout_charge: Option<Arc<MemoryReservation>>) -> Self {
        self.spec.swapped = original_layout_charge.is_some();
        self.spec._original_layout_charge = original_layout_charge;
        self
    }

    pub fn with_kind(mut self, kind: JoinKind) -> Self { self.spec.kind = kind; self }

    pub fn counting_rows(mut self) -> Self { self.spec.count_only = true; self }

    pub fn with_theta(mut self, theta: ThetaTests) -> Self { self.spec.theta = theta; self }
    pub fn with_residual(mut self, residual: Option<Residual>) -> Self { self.spec.residual = residual; self }

}

impl Spec {
    fn preserves_probe(&self) -> bool {
        crate::join::preserves_probe(self.kind, !self.swapped)
    }
    fn tracks_build(&self) -> bool {
        crate::join::tracks_build(self.kind, !self.swapped)
    }
    fn cell(&self, column: &Column, row: usize, side: usize) -> Result<Value> {
        let mut value = column.value(row)?;
        if self.projected[side] { value.admit_expression_dates()?; }
        Ok(value)
    }

    async fn build(&mut self, build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout) -> Result<State> {
        self.hash = self.hash_policy(&layout);
        let mut build = layout.left.project_batches(build, &self.execution);
        let memory = Arc::clone(&self.execution.context.memory);
        let threshold = self.execution.context.options.join_build_rows;
        let mut batches = Vec::new();
        let mut rows = 0usize;
        let mut inventory = memory.reserve(256)?;
        while let Some(batch) = poll_nested_batch(build.as_mut()).await? {
            self.execution.context.check_cancelled()?;
            batch.validate_readable(&self.execution.context)?;
            rows = rows.checked_add(batch.len).ok_or_else(size_error)?;
            let bytes = batches.len().checked_add(1).and_then(|count| count.checked_mul(4 * std::mem::size_of::<ColumnBatch>()))
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            Arc::get_mut(&mut inventory).expect("The join owns its batch inventory").grow_to(bytes)?;
            batches.push(batch);
            if i64::try_from(rows).map_or(true, |rows| rows > threshold) {
                drop(inventory);
                return self.spill_arrow(batches, build, probe, &layout).await.map(State::Disk);
            }
        }
        let mut table = self.table(batches, rows, inventory)?;
        let blooms = matches!(self.kind, JoinKind::Inner | JoinKind::Semi) || self.tracks_build() && self.kind != JoinKind::Full;
        let mut filters = PushdownBuilder::new(layout.condition.as_ref(), &layout.left.schema, &layout.right.original,
            probe.runtime.relation(), i64::try_from(rows).map_err(|_| size_error())?, blooms, &self.execution)?;
        if blooms {
            for batch in &table.batches {
                for row in 0..batch.len {
                    self.execution.context.check_cancelled_at(row)?;
                    filters.insert_arrow(&layout.left.schema, batch, row, &memory)?;
                }
            }
        }
        let filters = filters.finish(&memory)?;
        let pushdowns = Pushdowns::merge(probe.pushdowns.as_ref(), filters, &memory)?;
        let probe_batches = match build_batch_input(Arc::clone(&probe.runtime), probe.execution.clone(), pushdowns.clone(), probe.output).await? {
            Some((batches, projected)) if projected == self.projected[1] => batches,
            _ => return Err(rtdb_types::Anomaly::fault("rtdb.query/batch-join", "A batch join's probe input did not open as batches")),
        };
        if self.tracks_build() {
            let charge = memory.reserve(rows)?;
            table.unmatched = Some((vec![1; rows], charge));
        }
        Ok(State::Probe { table, probe: layout.right.project_batches(probe_batches, &self.execution) })
    }

    fn table(&self, batches: Vec<ColumnBatch>, rows: usize, inventory: Arc<MemoryReservation>) -> Result<Table> {
        let context = &self.execution.context;
        let starts_bytes = batches.len().checked_mul(std::mem::size_of::<usize>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let _hashes = context.memory.reserve(rows.checked_mul(std::mem::size_of::<u32>()).ok_or_else(size_error)?)?;
        let reservation = context.memory.reserve(starts_bytes)?;
        let mut hashes = Vec::with_capacity(rows);
        let mut starts = Vec::with_capacity(batches.len());
        for batch in &batches {
            starts.push(hashes.len());
            if batch.len == 0 { continue; }
            if !self.build_keys.is_empty()
                && let Some(keys) = KeyColumns::projection(batch, &self.build_keys).bind_typed(context)? {
                let start = hashes.len();
                hashes.resize(start.checked_add(batch.len).ok_or_else(size_error)?, 0);
                keys.hash_join_into(&mut hashes[start..], context, self.hash)?;
            } else {
                for row in 0..batch.len {
                    context.check_cancelled_at(row)?;
                    hashes.push(self.key_hash(batch, row, 0)?);
                }
            }
        }
        let index = HashIndex::new(&hashes, &context.memory)?;
        let anti_key_layout = self.anti_build_layout(&batches);
        Ok(Table { batches, starts, index, anti_key_layout, rows, unmatched: None, _starts: reservation, _inventory: inventory })
    }

    fn direct_anti_equality(&self) -> bool {
        self.kind == JoinKind::Anti && !self.build_keys.is_empty() && self.theta.is_empty() && self.residual.is_none()
            && self.build_keys.iter().all(|&position| position < self.widths[0])
            && self.probe_keys.iter().all(|&position| position < self.widths[1])
    }

    fn hash_policy(&self, layout: &JoinLayout) -> JoinHash {
        fn scalar(ty: &LogicalType) -> bool {
            match ty {
                LogicalType::Null | LogicalType::Scalar(
                    ScalarType::Bool | ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64
                    | ScalarType::F32 | ScalarType::F64 | ScalarType::Utf8 | ScalarType::Binary
                ) => true,
                LogicalType::Maybe(value) | LogicalType::RunEndEncoded { value, .. } => scalar(value),
                LogicalType::Union(legs) => legs.iter().all(scalar),
                _ => false,
            }
        }
        if (self.kind != JoinKind::Anti || self.direct_anti_equality())
            && self.build_keys.iter().all(|&key| scalar(&layout.left.schema.fields()[key].data_type))
            && self.probe_keys.iter().all(|&key| scalar(&layout.right.schema.fields()[key].data_type))
        {
            JoinHash::CanonicalZero
        } else {
            JoinHash::Vector
        }
    }

    fn anti_build_layout(&self, batches: &[ColumnBatch]) -> Option<usize> {
        if !self.direct_anti_equality() {
            return None;
        }
        let mut batches = batches.iter().enumerate().filter(|(_, batch)| batch.len != 0);
        let (first, layout) = batches.next()?;
        for &position in &self.build_keys {
            let kind = AntiKeyKind::of(layout.columns.get(position)?)?;
            if batches.clone().any(|(_, batch)| batch.columns.get(position).and_then(AntiKeyKind::of) != Some(kind)) {
                return None;
            }
        }
        Some(first)
    }

    fn anti_probe_compatible(&self, table: &Table, probe: &ColumnBatch) -> bool {
        let Some(layout) = table.anti_key_layout else { return false; };
        let build = &table.batches[layout];
        use AntiKeyKind::{Float, Null, Signed};
        self.build_keys.iter().zip(&self.probe_keys).all(|(&left, &right)| {
            match (AntiKeyKind::of(&build.columns[left]), probe.columns.get(right).and_then(AntiKeyKind::of)) {
                (Some(Float), Some(Float | Signed | Null)) | (Some(Signed | Null), Some(Float)) => self.hash == JoinHash::CanonicalZero,
                (Some(Float), _) | (_, Some(Float)) => false,
                (Some(left), Some(right)) => left == right || left == Null || right == Null,
                _ => false,
            }
        })
    }

    fn anti_probe_hash(&self, probe: &ColumnBatch, row: usize) -> Option<u32> {
        self.probe_keys.iter().try_fold(0, |hash, &position| {
            match ScalarKey::read(&probe.columns[position], row).expect("An admitted anti key has a scalar reader") {
                ScalarKey::Null => None,
                key => Some(combine_cell_hash(hash, self.hash.scalar(key) as i32)),
            }
        })
    }

    async fn spill_arrow(
        &mut self, held: Vec<ColumnBatch>, mut rest: BoxBatchCursor, probe: ProbeInput, layout: &JoinLayout,
    ) -> Result<DiskJoin> {
        let context = &self.execution.context;
        let mut build = BlockWriter::new(&layout.left.schema, context)?;
        for batch in held { build.append(batch, context)?; }
        while let Some(batch) = poll_nested_batch(rest.as_mut()).await? {
            batch.validate_readable(context)?;
            build.append(batch, context)?;
        }
        drop(rest);
        let mut bytes_written = self.spill_stats.as_mut().map(|stats| &mut stats.bytes_written);
        let build = build.finish_counted(context, bytes_written.as_deref_mut())?;
        let build = build.shuffle_keys(1, context, &self.build_keys, self.projected[0], self.hash, bytes_written.as_deref_mut())?;
        let Some((input, projected)) = build_batch_input(probe.runtime, probe.execution, probe.pushdowns, probe.output).await? else {
            return Err(crate::join_spill::layout_error());
        };
        if projected != self.projected[1] { return Err(crate::join_spill::layout_error()); }
        let mut input = layout.right.project_batches(input, &self.execution);
        let mut probe = BlockWriter::new(&layout.right.schema, context)?;
        while let Some(batch) = poll_nested_batch(input.as_mut()).await? {
            batch.validate_readable(context)?;
            probe.append(batch, context)?;
        }
        let probe = probe.finish_counted(context, bytes_written.as_deref_mut())?;
        let probe = probe.shuffle_keys(build.count(), context, &self.probe_keys, self.projected[1], self.hash, bytes_written)?;
        Ok(DiskJoin { build, probe, table: None, build_partition: 0, probe_partition: 0, ready: None })
    }

    fn key_hash(&self, batch: &ColumnBatch, row: usize, side: usize) -> Result<u32> {
        self.key(batch, row, side)?.join_hash(self.hash)
    }

    fn key<'a>(&'a self, batch: &'a ColumnBatch, row: usize, side: usize) -> Result<Key<'a>> {
        let positions = if side == 0 { &self.build_keys } else { &self.probe_keys };
        Key::for_columns(batch, row, self.projected[side], positions, &self.execution.context)
    }

    fn hash_probe<'a>(&'a self, probe: &'a ColumnBatch) -> Result<Option<HashedProbe<'a>>> {
        if self.kind == JoinKind::Anti || probe.len == 0 || self.probe_keys.is_empty() { return Ok(None); }
        let context = &self.execution.context;
        let Some(keys) = KeyColumns::projection(probe, &self.probe_keys).bind_typed(context)? else { return Ok(None); };
        let reservation = context.memory.reserve(probe.len.checked_mul(size_of::<u32>()).ok_or_else(size_error)?)?;
        let mut hashes = vec![0; probe.len];
        keys.hash_join_into(&mut hashes, context, self.hash)?;
        Ok(Some(HashedProbe { keys, hashes, _reservation: reservation }))
    }

    fn hashed_probe_key<'a>(&'a self, probe: &'a ColumnBatch, row: usize, hashed: Option<&'a HashedProbe<'_>>) -> Result<(u32, Key<'a>)> {
        if let Some(hashed) = hashed {
            Ok((hashed.hashes[row], hashed.keys.key(row, self.projected[1])))
        } else {
            let key = self.key(probe, row, 1)?;
            Ok((key.join_hash(self.hash)?, key))
        }
    }

    async fn probe_batch(&self, table: &mut Table, probe: ColumnBatch) -> Result<Option<ColumnBatch>> {
        let context = &self.execution.context;
        probe.validate_readable(context)?;
        let residual = self.residual.as_ref().map(|predicate| predicate.prepare(&self.execution)).transpose()?;
        match self.kind {
            JoinKind::Semi | JoinKind::Anti => return self.probe_membership(table, probe, residual.as_ref()).await,
            JoinKind::Left | JoinKind::Right | JoinKind::Full | JoinKind::Single => return self.probe_outer(table, &probe, residual.as_ref()).await,
            _ => {}
        }
        let hashed = self.hash_probe(&probe)?;
        let mut matches = InnerMatches::new(self.count_only);
        if self.theta.is_empty() && residual.is_none() {
            self.probe_arrow_keys(table, &probe, hashed.as_ref(), &mut matches)?;
        } else if let Some(predicates) = self.residual.as_ref().filter(|predicates| residual.is_some()
            && (self.projected == [false, false] || predicates.schema.fields().iter().all(|field| date_free(&field.data_type))))
            && self.probe_residual_columns(table, &probe, hashed.as_ref(), predicates, &mut matches).await.is_ok()
        {
        } else {
            // The row path also reports the error a failed column-wise pass met, as it would have met it first.
            matches = InnerMatches::new(self.count_only);
            let equality = self.execution.scope()?;
            for row in 0..probe.len {
                context.check_cancelled_at(row)?;
                self.matching_build_rows(table, &probe, ProbeCandidates::HashBuckets(hashed.as_ref()), row, &equality, residual.as_ref(), |_, position| {
                    matches.push(position, &context.memory)?;
                    Ok(ControlFlow::Continue(()))
                }).await?;
                matches.end_row(row, &context.memory)?;
            }
        }
        drop(hashed);
        match matches {
            InnerMatches::Count(0) => Ok(None),
            InnerMatches::Count(rows) => ColumnBatch::settled(Vec::new(), rows, context.memory.reserve(256)?).map(Some),
            InnerMatches::Positions { build, probe: runs } => {
                self.gather_matches(build.rows.len(), Some((&probe, runs)), |position| {
                    gather_column_rows(|batch| &table.batches[batch], &build.rows, position, &context.memory)
                })
            }
        }
    }

    /// An inner join's residual over the key-matched pairs of a probe batch, column-wise, a bounded run of probe rows at a time.
    /// As in the row path, a pair meets each conjunct until one is false; an unknown key or conjunct lets it go on,
    /// for the later conjuncts' errors, but it never matches.
    async fn probe_residual_columns(
        &self, table: &Table, probe: &ColumnBatch, hashed: Option<&HashedProbe<'_>>, residual: &Residual, matches: &mut InnerMatches,
    ) -> Result<()> {
        const PAIRS_PER_PASS: usize = 1 << 16;
        let context = &self.execution.context;
        let memory = &context.memory;
        let equality = self.execution.scope()?;
        let mut candidates = JoinPositions::new();
        let mut first_row = 0;
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let hash = match hashed {
                Some(hashed) => hashed.hashes[row],
                None => self.key_hash(probe, row, 1)?,
            };
            for (candidate, ordinal) in table.index.candidates(hash).enumerate() {
                context.check_cancelled_at(candidate)?;
                let (batch, offset) = table.position(ordinal);
                let Some(keys) = self.residual_keys(&table.batches[batch], offset, probe, row, &equality)? else { continue; };
                candidates.push(ResidualCandidate { build: (batch, offset), probe: row, known: keys }, memory)?;
            }
            if candidates.rows.len() >= PAIRS_PER_PASS || row + 1 == probe.len {
                self.residual_pass(table, probe, residual, &mut candidates.rows, first_row..row + 1, matches).await?;
                candidates = JoinPositions::new();
                first_row = row + 1;
            }
        }
        Ok(())
    }

    async fn residual_pass(&self, table: &Table, probe: &ColumnBatch, residual: &Residual,
        candidates: &mut Vec<ResidualCandidate>, rows: Range<usize>, matches: &mut InnerMatches) -> Result<()> {
        let memory = &self.execution.context.memory;
        for predicate in &residual.expressions {
            if candidates.is_empty() { break; }
            let mut columns = Vec::with_capacity(residual.sides.len());
            let mut charges = Vec::with_capacity(residual.sides.len());
            for &(on_build, position) in &residual.sides {
                let (column, charge) = if on_build {
                    let rows = candidates.iter().map(|candidate| candidate.build);
                    gather_column_rows_iter(|batch| &table.batches[batch], rows, position, memory)?
                } else {
                    let rows = candidates.iter().map(|candidate| (0, candidate.probe));
                    gather_column_rows_iter(|_| probe, rows, position, memory)?
                };
                columns.push(column);
                charges.push(charge);
            }
            let pairs_batch = ColumnBatch::settled(columns, candidates.len(), memory.reserve(256)?)?;
            let (truths, _truths) = crate::columnar::predicate_truth(&pairs_batch, predicate, &residual.schema, &self.execution).await?;
            let mut truths = truths.into_iter();
            candidates.retain_mut(|candidate| match truths.next().expect("Each candidate has a truth value") {
                Some(false) => false,
                Some(true) => true,
                None => { candidate.known = false; true }
            });
        }
        let mut kept = candidates.iter().filter(|candidate| candidate.known).peekable();
        for row in rows {
            while let Some(candidate) = kept.next_if(|candidate| candidate.probe == row) {
                matches.push(candidate.build, memory)?;
            }
            matches.end_row(row, memory)?;
        }
        Ok(())
    }

    async fn probe_membership(&self, table: &Table, probe: ColumnBatch, residual: Option<&PreparedResidual<'_>>) -> Result<Option<ColumnBatch>> {
        let context = &self.execution.context;
        let memory = &context.memory;
        let mut span = 0..0;
        let mut selected = JoinPositions::new();
        let equality = self.execution.scope()?;
        let hashed = self.hash_probe(&probe)?;
        let candidates = match self.kind {
            JoinKind::Anti if self.anti_probe_compatible(table, &probe) => ProbeCandidates::AntiHashBuckets,
            JoinKind::Anti => ProbeCandidates::BuildOrder,
            _ => ProbeCandidates::HashBuckets(hashed.as_ref()),
        };
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let matched = self.matching_build_rows(table, &probe, candidates, row, &equality, residual, |_, _| Ok(ControlFlow::Break(()))).await?;
            if matched != (self.kind == JoinKind::Semi) { continue; }
            if span.is_empty() {
                span = row..row + 1;
            } else if selected.rows.is_empty() && row == span.end {
                span.end += 1;
            } else {
                if selected.rows.is_empty() {
                    for row in span.clone() {
                        context.check_cancelled_at(row)?;
                        selected.push((0, row), memory)?;
                    }
                }
                selected.push((0, row), memory)?;
            }
        }
        drop(hashed);
        if span.is_empty() { return Ok(None); }
        let width = self.widths[1];
        if span == (0..probe.len) && width == probe.columns.len()
            && probe.columns.iter().all(|column| !matches!(column, Column::Values(_)))
        {
            return Ok(Some(probe));
        }
        let _columns = memory.reserve(width.checked_mul(std::mem::size_of::<usize>()).ok_or_else(size_error)?)?;
        let columns = (0..width).collect::<Vec<_>>();
        if selected.rows.is_empty() {
            if let Some(output) = probe.take_sorted_span(span.clone(), &columns, memory)? { return Ok(Some(output)); }
            for row in span {
                context.check_cancelled_at(row)?;
                selected.push((0, row), memory)?;
            }
        }
        ColumnBatch::take_rows(&[&probe], &selected.rows, &columns, memory).map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    async fn matching_build_rows(
        &self, table: &Table, probe: &ColumnBatch, candidates: ProbeCandidates<'_>, row: usize, equality: &rtdb_expr::EvaluationScope,
        residual: Option<&PreparedResidual<'_>>,
        mut accept: impl FnMut(usize, (usize, usize)) -> Result<ControlFlow<()>>,
    ) -> Result<bool> {
        let context = &self.execution.context;
        let checked_before_probe = usize::from(matches!(candidates, ProbeCandidates::AntiHashBuckets));
        let mut key = None;
        let mut hashed = match candidates {
            ProbeCandidates::BuildOrder => None,
            ProbeCandidates::HashBuckets(hashed) => {
                let (hash, probe_key) = self.hashed_probe_key(probe, row, hashed)?;
                key = Some(probe_key);
                Some(table.index.candidates(hash))
            }
            ProbeCandidates::AntiHashBuckets => {
                context.check_cancelled()?;
                let Some(hash) = self.anti_probe_hash(probe, row) else { return Ok(false); };
                Some(table.index.candidates(hash))
            }
        };
        let mut all = 0..table.rows;
        let candidates = std::iter::from_fn(|| match &mut hashed { Some(rows) => rows.next(), None => all.next() });
        let mut matched = false;
        for (candidate, ordinal) in candidates.enumerate() {
            context.check_cancelled_at(candidate + checked_before_probe)?;
            let (batch, offset) = table.position(ordinal);
            let build = &table.batches[batch];
            let accepted = if let Some(residual) = residual {
                self.matches_residual(residual, build, offset, probe, row, equality).await?
            } else if self.theta.use_residual_keys {
                let Some(keys) = self.residual_keys(build, offset, probe, row, equality)? else { continue; };
                self.matches_theta(build, offset, probe, row)? && keys
            } else {
                if key.is_none() { key = Some(self.key(probe, row, 1)?); }
                let probe_key = key.as_ref().expect("The probe key has been read");
                let build_key = self.key(build, offset, 0)?;
                self.equal_keys(&build_key, probe_key, Some(equality))? && self.matches_theta(build, offset, probe, row)?
            };
            if accepted {
                matched = true;
                if accept(ordinal, (batch, offset))?.is_break() { break; }
            }
        }
        Ok(matched)
    }

    async fn matches_residual(&self, residual: &PreparedResidual<'_>, build: &ColumnBatch, build_row: usize,
        probe: &ColumnBatch, probe_row: usize, equality: &rtdb_expr::EvaluationScope) -> Result<bool> {
        let Some(keys) = self.residual_keys(build, build_row, probe, probe_row, equality)? else { return Ok(false); };
        let mut unknown = !keys;
        let input = JoinRow { build, build_row, probe, probe_row, sides: residual.sides, admit_dates: self.projected };
        for comparison in &residual.comparisons {
            let scope = self.execution.scope()?;
            let value = self.execution.evaluate_invocation_row(&scope, comparison, rtdb_expr::ExpressionRow::Borrowed(&input)).await?;
            if matches!(value, Value::Null | Value::Undefined) { unknown = true; }
            else if !rtdb_expr::is_true(&value)? { return Ok(false); }
        }
        Ok(!unknown)
    }

    /// The equality keys of a residual join's pair: `None` when a key is unequal, `Some(false)` when one is unknown.
    fn residual_keys(&self, build: &ColumnBatch, build_row: usize, probe: &ColumnBatch, probe_row: usize, equality: &rtdb_expr::EvaluationScope)
        -> Result<Option<bool>> {
        let memory = &self.execution.context.memory;
        let mut unknown = false;
        for ((&left, &right), &build_first) in self.build_keys.iter().zip(&self.probe_keys).zip(&self.order) {
            let (left, right) = (&build.columns[left], &probe.columns[right]);
            let equal = if let Some(typed) = direct_key_equal(left, build_row, right, probe_row, self.preserves_probe()) {
                typed
            } else {
                let _scratch = memory.reserve(left.bound(build_row)?.checked_add(right.bound(probe_row)?).ok_or_else(size_error)?)?;
                let left = self.cell(left, build_row, 0)?;
                let right = self.cell(right, probe_row, 1)?;
                let (left, right) = if build_first { (&left, &right) } else { (&right, &left) };
                let left_null = matches!(left, Value::Null | Value::Undefined);
                let right_null = matches!(right, Value::Null | Value::Undefined);
                if self.preserves_probe() && (left_null || right_null) {
                    Some(left_null && right_null)
                } else {
                    let equal = rtdb_expr::sql_eq_in_timezone(left, right, &self.execution.context.timezone, equality)?;
                    equality.reset()?;
                    equal
                }
            };
            match equal { Some(false) => return Ok(None), None => unknown = true, Some(true) => {} }
        }
        Ok(Some(!unknown))
    }

    #[allow(clippy::too_many_arguments)]
    fn theta_value<'a>(&self, operand: &'a ThetaOperand, kind: ThetaKind,
        build: &'a ColumnBatch, build_row: usize, probe: &'a ColumnBatch, probe_row: usize) -> Result<Option<ThetaValue<'a>>> {
        let (column, row, side) = match operand {
            ThetaOperand::Literal(value) => return Ok(value.as_ref().map(ThetaValue::borrowed)),
            ThetaOperand::Build(position) => (&build.columns[*position], build_row, 0),
            ThetaOperand::Probe(position) => (&probe.columns[*position], probe_row, 1),
        };
        if let Some(value) = kind.arrow(column, row) { return Ok(value); }
        if kind == ThetaKind::Text && let Column::Values(values) = column
            && let Some(Value::Utf8(value)) = values.get(row)
        {
            return Ok(Some(ThetaValue::Text { value: Cow::Borrowed(value), _charge: None }));
        }

        let charge = self.execution.context.memory.reserve(column.bound(row)?)?;
        let value = self.cell(column, row, side)?;
        match value {
            Value::Null | Value::Undefined => Ok(None),
            Value::Utf8(value) if kind == ThetaKind::Text => Ok(Some(ThetaValue::Text { value: Cow::Owned(value), _charge: Some(charge) })),
            _ => kind.fixed_value(&value).map(Some)
                .ok_or_else(|| invalid("join-key", "Unexpected join predicate comparison operand")),
        }
    }

    fn matches_theta(&self, build: &ColumnBatch, build_row: usize, probe: &ColumnBatch, probe_row: usize) -> Result<bool> {
        let mut known = true;
        for test in &self.theta.tests {
            let left = self.theta_value(&test.left, test.kind, build, build_row, probe, probe_row)?;
            if self.theta.use_residual_keys && left.is_none() {
                known = false;
                continue;
            }
            let right = self.theta_value(&test.right, test.kind, build, build_row, probe, probe_row)?;
            let (Some(left), Some(right)) = (left, right) else {
                if !self.theta.use_residual_keys { return Ok(false); }
                known = false;
                continue;
            };
            let order = left.compare(&right, matches!(test.op, BinaryOp::Eq | BinaryOp::NotEq))?;
            let accepted = match test.op {
                BinaryOp::Eq => order == Some(Ordering::Equal),
                BinaryOp::NotEq => order != Some(Ordering::Equal),
                BinaryOp::Lt => order == Some(Ordering::Less),
                BinaryOp::LtEq => matches!(order, Some(Ordering::Less | Ordering::Equal)),
                BinaryOp::Gt => order == Some(Ordering::Greater),
                BinaryOp::GtEq => matches!(order, Some(Ordering::Greater | Ordering::Equal)),
                _ => unreachable!("The comparison was checked"),
            };
            if !accepted { return Ok(false); }
        }
        Ok(known)
    }

    async fn probe_outer(&self, table: &mut Table, probe: &ColumnBatch, residual: Option<&PreparedResidual<'_>>) -> Result<Option<ColumnBatch>> {
        let context = &self.execution.context;
        let mut build = JoinPositions::new();
        let mut runs = ProbeRuns::new();
        let equality = self.execution.scope()?;
        let hashed = self.hash_probe(probe)?;
        let mut unmatched = table.unmatched.take();
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let mut matched_once = false;
            let matched = self.matching_build_rows(table, probe, ProbeCandidates::HashBuckets(hashed.as_ref()), row, &equality, residual, |ordinal, position| {
                if matched_once && self.kind == JoinKind::Single { return Err(crate::cardinality_violation()); }
                matched_once = true;
                if let Some((unmatched, _)) = &mut unmatched { unmatched[ordinal] = 0; }
                build.push_outer(Some(position), &context.memory)?;
                Ok(ControlFlow::Continue(()))
            }).await?;
            if !matched && self.preserves_probe() { build.push_outer(None, &context.memory)?; }
            runs.end_row(row, build.rows.len(), &context.memory)?;
        }
        drop(hashed);
        table.unmatched = unmatched;
        self.gather_matches(build.rows.len(), Some((probe, runs)), |position| {
            gather_nullable_column(|batch| &table.batches[batch], &build.rows, position, &context.memory)
        })
    }

    fn unmatched_build(&self, table: &Table) -> Result<Option<ColumnBatch>> {
        let Some((unmatched, _)) = &table.unmatched else { return Ok(None); };
        let context = &self.execution.context;
        let mut build = JoinPositions::new();
        for (ordinal, flag) in unmatched.iter().enumerate() {
            context.check_cancelled_at(ordinal)?;
            if *flag != 0 { build.push_outer(Some(table.position(ordinal)), &context.memory)?; }
        }
        self.gather_matches(build.rows.len(), None, |position| {
            gather_nullable_column(|batch| &table.batches[batch], &build.rows, position, &context.memory)
        })
    }

    fn gather_matches(
        &self, count: usize, mut probe: Option<(&ColumnBatch, ProbeRuns)>,
        mut gather_build: impl FnMut(usize) -> Result<(Column, Arc<MemoryReservation>)>,
    ) -> Result<Option<ColumnBatch>> {
        if count == 0 { return Ok(None); }
        let context = &self.execution.context;
        context.check_cancelled()?;
        let memory = &context.memory;
        let width = self.widths[0].checked_add(self.widths[1]).ok_or_else(size_error)?;
        let bound = width.checked_mul(std::mem::size_of::<Column>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(bound)?;
        let lease = Arc::get_mut(&mut reservation).expect("The join owns its output reservation");
        let mut columns = Vec::with_capacity(width);
        for side in if self.swapped { [1, 0] } else { [0, 1] } {
            if side == 1 {
                gather_join_probe(probe.take(), count, self.widths[1], &mut columns, lease)?;
            } else {
                for position in 0..self.widths[0] {
                    let (column, mut charge) = gather_build(position)?;
                    let bytes = column.retained_bytes()?;
                    let charge = Arc::get_mut(&mut charge).expect("The gather owns its output reservation");
                    if bytes <= charge.bytes() { charge.shrink_to(bytes)?; } else { charge.grow_to(bytes)?; }
                    lease.absorb(charge)?;
                    columns.push(column);
                }
            }
        }
        ColumnBatch::settled(columns, count, reservation).map(Some)
    }

    fn probe_arrow_keys(&self, table: &Table, probe: &ColumnBatch, hashed: Option<&HashedProbe<'_>>, matches: &mut InnerMatches) -> Result<()> {
        let context = &self.execution.context;
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let (hash, probe_key) = self.hashed_probe_key(probe, row, hashed)?;
            for (candidate, ordinal) in table.index.candidates(hash).enumerate() {
                context.check_cancelled_at(candidate)?;
                let (batch_index, build_row) = table.position(ordinal);
                let build_key = self.key(&table.batches[batch_index], build_row, 0)?;
                if self.equal_keys(&build_key, &probe_key, None)? {
                    matches.push((batch_index, build_row), &context.memory)?;
                }
            }
            matches.end_row(row, &context.memory)?;
        }
        Ok(())
    }

    fn equal_keys(&self, build: &Key<'_>, probe: &Key<'_>, equality: Option<&rtdb_expr::EvaluationScope>) -> Result<bool> {
        let mut unknown = false;
        for (index, build_first) in self.order.iter().enumerate() {
            let (left, right) = if *build_first { (build, probe) } else { (probe, build) };
            match left.equal_at(right, index, self.preserves_probe(), &self.execution.context, equality)? {
                None => unknown = true,
                Some(false) => return Ok(false),
                Some(true) => {}
            }
        }
        Ok(!unknown)
    }
}

enum InnerMatches {
    Count(usize),
    Positions { build: JoinPositions<(usize, usize)>, probe: ProbeRuns },
}

impl InnerMatches {
    fn new(count_only: bool) -> Self {
        if count_only { Self::Count(0) }
        else { Self::Positions { build: JoinPositions::new(), probe: ProbeRuns::new() } }
    }

    fn push(&mut self, position: (usize, usize), memory: &Arc<MemoryBudget>) -> Result<()> {
        match self {
            Self::Count(count) => { *count = count.checked_add(1).ok_or_else(size_error)?; Ok(()) }
            Self::Positions { build, .. } => build.push(position, memory),
        }
    }

    fn end_row(&mut self, row: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        match self {
            Self::Count(_) => Ok(()),
            Self::Positions { build, probe } => probe.end_row(row, build.rows.len(), memory),
        }
    }
}

struct JoinPositions<T> {
    rows: Vec<T>,
    reservation: Option<Arc<MemoryReservation>>,
}

impl<T> JoinPositions<T> {
    fn new() -> Self {
        Self { rows: Vec::new(), reservation: None }
    }

    fn push(&mut self, row: T, memory: &Arc<MemoryBudget>) -> Result<()> {
        self.push_with_limit(row, usize::MAX, memory)
    }

    fn push_with_limit(&mut self, row: T, limit: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        if self.rows.len() == limit { return Err(size_error()); }
        if self.rows.len() == self.rows.capacity() {
            let capacity = self.rows.capacity().saturating_mul(2).max(16).min(limit);
            let bytes = capacity.checked_mul(std::mem::size_of::<T>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            let replacement = self.rows.capacity().checked_mul(std::mem::size_of::<T>()).ok_or_else(size_error)?;
            let _replacement = (replacement != 0).then(|| memory.reserve(replacement)).transpose()?;
            if let Some(reservation) = &mut self.reservation {
                Arc::get_mut(reservation).expect("The join owns its positions").grow_to(bytes)?;
            } else {
                self.reservation = Some(memory.reserve(bytes)?);
            }
            self.rows.try_reserve_exact(capacity - self.rows.len()).map_err(|_| size_error())?;
        }
        self.rows.push(row);
        Ok(())
    }
}

impl JoinPositions<Option<(usize, usize)>> {
    fn push_outer(&mut self, row: Option<(usize, usize)>, memory: &Arc<MemoryBudget>) -> Result<()> {
        self.push_with_limit(row, i32::MAX as usize, memory)
    }
}

struct ProbeRun {
    row: usize,
    end: usize,
}

pub(crate) struct ProbeRuns {
    identity_len: usize,
    runs: JoinPositions<ProbeRun>,
}

#[derive(Clone)]
pub(crate) struct ProbeRunSpans<'a> {
    identity_len: usize,
    runs: std::slice::Iter<'a, ProbeRun>,
    rows: Range<usize>,
}

impl Iterator for ProbeRunSpans<'_> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rows.is_empty() { return None; }
        let start = self.rows.start;
        if start < self.identity_len {
            self.rows.start += 1;
            return Some((start, 1));
        }
        let run = self.runs.next().expect("The probe runs cover the selection");
        self.rows.start = run.end.min(self.rows.end);
        Some((run.row, self.rows.start - start))
    }
}

impl ProbeRuns {
    fn new() -> Self { Self { identity_len: 0, runs: JoinPositions::new() } }

    fn end_row(&mut self, row: usize, end: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        let previous_end = self.runs.rows.last().map_or(self.identity_len, |run| run.end);
        if previous_end == end { return Ok(()); }
        if self.runs.rows.is_empty() && row == self.identity_len && end - previous_end == 1 {
            self.identity_len = end;
            return Ok(());
        }
        self.runs.push(ProbeRun { row, end }, memory)
    }

    fn run_at(&self, output_row: usize) -> usize {
        if output_row < self.identity_len
            || self.runs.rows.last().is_some_and(|run| run.end - self.identity_len == self.runs.rows.len())
        {
            return output_row;
        }
        self.identity_len + self.runs.rows.partition_point(|run| run.end <= output_row)
    }

    fn row_at_run(&self, run: usize) -> usize {
        if run < self.identity_len { run } else { self.runs.rows[run - self.identity_len].row }
    }

    pub(crate) fn input_row(&self, output_row: usize) -> usize {
        self.row_at_run(self.run_at(output_row))
    }

    pub(crate) fn contiguous(&self, rows: &std::ops::Range<usize>) -> bool {
        let first = self.run_at(rows.start);
        let last = self.run_at(rows.end - 1);
        last - first + 1 == rows.len() && self.row_at_run(last) - self.row_at_run(first) + 1 == rows.len()
    }

    pub(crate) fn owner(&self) -> Option<&Arc<MemoryReservation>> {
        self.runs.reservation.as_ref()
    }

    pub(crate) fn spans(&self, rows: Range<usize>) -> ProbeRunSpans<'_> {
        let first = if rows.is_empty() || rows.end <= self.identity_len {
            0
        } else {
            self.run_at(rows.start.max(self.identity_len)) - self.identity_len
        };
        ProbeRunSpans { identity_len: self.identity_len, runs: self.runs.rows[first..].iter(), rows }
    }

    pub(crate) fn picks(&self) -> impl Iterator<Item = Option<(usize, usize)>> + Clone + '_ {
        let identity = (0..self.identity_len).map(|row| Some((0, row)));
        let tail = self.runs.rows.iter().scan(self.identity_len, |start, run| {
            let count = run.end - *start;
            *start = run.end;
            Some(std::iter::repeat_n(Some((0, run.row)), count))
        }).flatten();
        identity.chain(tail)
    }
}

#[async_trait::async_trait]
impl BatchCursor for BatchJoin {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        loop {
            self.spec.execution.context.check_cancelled()?;
            match std::mem::replace(&mut self.state, State::Done) {
                State::Build { build, probe, layout } => { self.state = self.spec.build(build, probe, layout).await?; }
                State::Probe { mut table, mut probe } => {
                    let Some(batch) = poll_nested_batch(probe.as_mut()).await? else { return self.spec.unmatched_build(&table) };
                    let output = self.spec.probe_batch(&mut table, batch).await?;
                    self.state = State::Probe { table, probe };
                    if let Some(output) = output { return Ok(Some(output)); }
                }
                State::Disk(mut join) => {
                    let output = join.next(&mut self.spec).await?;
                    if output.is_some() { self.state = State::Disk(join); }
                    return Ok(output);
                }
                State::Done => return Ok(None),
            }
        }
    }

    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        if let Some(stats) = &self.spec.spill_stats {
            crate::trace::OperatorCounter::SpillBytesWritten.record(span, stats.bytes_written);
            stats.replay.record(span);
        }
    }
}

fn size_error() -> rtdb_types::Anomaly { invalid("join-size", "Join output exceeds addressable memory") }

#[test]
fn join_probe_positions_keep_identity_gaps_and_fanout_in_output_order() {
    let memory = MemoryBudget::new(1 << 20);
    let mut rows = ProbeRuns::new();
    rows.end_row(0, 1, &memory).unwrap();
    rows.end_row(1, 2, &memory).unwrap();
    assert!(rows.owner().is_none());
    assert!(rows.contiguous(&(0..2)));
    rows.end_row(2, 2, &memory).unwrap();
    rows.end_row(3, 3, &memory).unwrap();
    assert_eq!(rows.input_row(2), 3);
    assert!(!rows.contiguous(&(1..3)));
    assert_eq!(rows.spans(1..3).collect::<Vec<_>>(), [(1, 1), (3, 1)]);

    rows.end_row(4, 5, &memory).unwrap();
    rows.end_row(5, 6, &memory).unwrap();
    assert_eq!((0..6).map(|row| rows.input_row(row)).collect::<Vec<_>>(), [0, 1, 3, 4, 4, 5]);
    assert_eq!(rows.picks().flatten().map(|(_, row)| row).collect::<Vec<_>>(), [0, 1, 3, 4, 4, 5]);
    assert!(rows.contiguous(&(2..4)));
    assert!(!rows.contiguous(&(3..6)));

    for start in 0..=6 {
        for end in start..=6 {
            let sequential = rows.spans(start..end).flat_map(|(row, count)| std::iter::repeat_n(row, count));
            assert!(sequential.eq((start..end).map(|row| rows.input_row(row))));
        }
    }
}

#[test]
fn slicing_a_skewed_probe_run_retains_its_selected_source_and_null_positions() {
    use arrow_array::{ArrayRef, Int64Array};
    use arrow_schema::{DataType, Field};
    use crate::columnar::SelectedColumn;

    let memory = MemoryBudget::new(1 << 13);
    let source = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])) as ArrayRef;
    let source_owner = memory.reserve(512).unwrap();
    let field = Arc::new(Field::new("payload", DataType::Int64, true));
    let logical = LogicalType::Scalar(ScalarType::I64).maybe();
    let input = Column::Selected(SelectedColumn::new(
        vec![Some((source, field, Some(logical), Some(Arc::clone(&source_owner)))), None],
        Arc::new(vec![(0, 2), (1, 0), (0, 0), (0, 1)]),
    ));
    let input = ColumnBatch::settled(vec![input], 4, memory.reserve(2048).unwrap()).unwrap();
    let mut runs = ProbeRuns::new();
    let skew_end = 1 << 20;
    for (row, end) in [1, skew_end, skew_end + 1, skew_end + 2].into_iter().enumerate() {
        runs.end_row(row, end, &memory).unwrap();
    }
    let run_owner = Arc::downgrade(runs.owner().unwrap());
    let source_lease = Arc::downgrade(&source_owner);
    drop(source_owner);
    let mut columns = Vec::with_capacity(1);
    let mut output_owner = memory.reserve(512).unwrap();
    gather_join_probe(
        Some((&input, runs)), skew_end + 2, 1, &mut columns, Arc::get_mut(&mut output_owner).unwrap(),
    ).unwrap();
    let output = ColumnBatch::settled(columns, skew_end + 2, output_owner).unwrap();
    drop(input);

    let output = output.slice(skew_end - 2, 4, &memory).unwrap();
    let Column::Selected(selected) = &output.columns[0] else { panic!("Expected selected probe payloads"); };
    let offsets = selected.arrow_cells().map(|cell| cell.map(|(_, _, _, row)| row)).collect::<Vec<_>>();
    assert_eq!(offsets, [None, None, Some(0), Some(1)]);
    let from_cells = selected.arrow_cells().map(|cell| {
        cell.map_or(Ok(Value::Null), |(array, field, logical, row)| {
            rtdb_columnar::read_execution_value_as(logical.unwrap(), field, array, row)
        }).unwrap()
    }).collect::<Vec<_>>();
    assert_eq!(from_cells, [Value::Null, Value::Null, Value::I64(10), Value::Null]);
    for (row, expected) in from_cells.into_iter().enumerate().rev() {
        assert_eq!(output.columns[0].value(row).unwrap(), expected);
    }
    assert!(run_owner.upgrade().is_some());
    assert!(source_lease.upgrade().is_some());
    drop(output);
    assert!(run_owner.upgrade().is_none());
    assert!(source_lease.upgrade().is_none());
    assert_eq!(memory.used_bytes(), 0);
}

