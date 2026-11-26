//! Column-at-a-time projection of scalar calls and searched CASE over plain scalar cells, through
//! `rtdb_expr::kernels`, which share the scalar functions' per-value code.
//! `None` leaves the batch to the row evaluator: any shape, layout or cell outside these kernels, and any cell the
//! evaluator would fail on, so that it raises that error in row order.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, NullArray, StringArray, TimestampMicrosecondArray, new_null_array};
use arrow_schema::{DataType, Field as ArrowField, TimeUnit as ArrowUnit};
use rtdb_expr::kernels;
use rtdb_plan::{Expr, Schema};
use rtdb_types::{LogicalType, ScalarType, TimeUnit, Value};

use super::{Column, ColumnBatch, SelectedColumn, plain_scalar};

/// A batch-length column of one plain scalar type, or of nulls only.
#[derive(Clone)]
enum Vector {
    Null,
    I32(Int32Array),
    I64(Int64Array),
    F64(Float64Array),
    Text(StringArray),
    Bool(BooleanArray),
    /// Microsecond timestamps read under their logical type's zone label.
    Micros(TimestampMicrosecondArray, Option<String>),
}

impl Vector {
    fn scalar(&self) -> Option<ScalarType> {
        Some(match self {
            Self::Null => return None,
            Self::I32(_) => ScalarType::I32,
            Self::I64(_) => ScalarType::I64,
            Self::F64(_) => ScalarType::F64,
            Self::Text(_) => ScalarType::Utf8,
            Self::Bool(_) => ScalarType::Bool,
            Self::Micros(_, zone) => ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: zone.clone() },
        })
    }

    fn array(&self) -> Option<ArrayRef> {
        Some(match self {
            Self::Null => return None,
            Self::I32(array) => Arc::new(array.clone()),
            Self::I64(array) => Arc::new(array.clone()),
            Self::F64(array) => Arc::new(array.clone()),
            Self::Text(array) => Arc::new(array.clone()),
            Self::Bool(array) => Arc::new(array.clone()),
            Self::Micros(array, zone) => Arc::new(array.clone().with_timezone_opt(zone.clone())),
        })
    }

    /// The vector of `array`'s cells read as `scalar`.
    fn of(array: &dyn Array, scalar: &ScalarType) -> Option<Self> {
        let any = array.as_any();
        Some(match scalar {
            ScalarType::I32 => Self::I32(any.downcast_ref::<Int32Array>()?.clone()),
            ScalarType::I64 => Self::I64(any.downcast_ref::<Int64Array>()?.clone()),
            ScalarType::F64 => Self::F64(any.downcast_ref::<Float64Array>()?.clone()),
            ScalarType::Utf8 => Self::Text(any.downcast_ref::<StringArray>()?.clone()),
            ScalarType::Bool => Self::Bool(any.downcast_ref::<BooleanArray>()?.clone()),
            ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone } => Self::Micros(any.downcast_ref::<TimestampMicrosecondArray>()?.clone(), timezone.clone()),
            _ => return None,
        })
    }

    /// Integers widened as the scalar functions widen their integer arguments.
    fn integers(&self) -> Option<Int64Array> {
        match self {
            Self::I64(array) => Some(array.clone()),
            Self::I32(array) => Some(array.unary(i64::from)),
            _ => None,
        }
    }

    /// Numbers as the scalar functions read a numeric argument as a double.
    fn doubles(&self) -> Option<Float64Array> {
        match self {
            Self::F64(array) => Some(array.clone()),
            Self::I64(array) => Some(array.unary(|value| value as f64)),
            Self::I32(array) => Some(array.unary(f64::from)),
            _ => None,
        }
    }
}

/// The scalar a stored layout's cells read as, `None` within `Some` for a layout of nulls only, as the predicate
/// kernel's cell reader takes it.
fn layout(array: &dyn Array, field: &ArrowField, logical: Option<&LogicalType>) -> Option<Option<ScalarType>> {
    let plain = |data_type: &DataType| -> Option<ScalarType> {
        let scalar = match data_type {
            DataType::Int32 => ScalarType::I32, DataType::Int64 => ScalarType::I64, DataType::Float64 => ScalarType::F64,
            DataType::Utf8 => ScalarType::Utf8, DataType::Boolean => ScalarType::Bool,
            _ => return None,
        };
        match logical {
            Some(logical) => (plain_scalar(logical) == Some(&scalar)).then_some(scalar),
            None => field.metadata().is_empty().then_some(scalar),
        }
    };
    Some(match array.data_type() {
        DataType::Null if field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null) => None,
        DataType::Timestamp(ArrowUnit::Microsecond, zone) => Some(match (logical, logical.and_then(plain_scalar)) {
            (Some(_), Some(scalar @ ScalarType::Timestamp { unit: TimeUnit::Microsecond, .. })) => scalar.clone(),
            (None, _) if field.metadata().is_empty() => ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: zone.as_ref().map(|zone| zone.to_string()) },
            _ => return None,
        }),
        data_type => Some(plain(data_type)?),
    })
}

fn read(column: &Column, declared: &LogicalType, len: usize) -> Option<Vector> {
    if *declared == LogicalType::Null { return Some(Vector::Null); }
    let expected = plain_scalar(declared)?;
    match column {
        Column::Arrow { array, field, logical } => match layout(array.as_ref(), field, logical.as_ref())? {
            None => Some(Vector::Null),
            Some(scalar) if scalar == *expected => Vector::of(array.as_ref(), expected),
            Some(_) => None,
        },
        Column::Selected(selected) => read_selected(selected, expected),
        Column::Values(values) => read_values(values, expected, len),
    }
}

/// Gathers a selection whose sources all read as `expected` or as nulls into one vector.
fn read_selected(selected: &SelectedColumn, expected: &ScalarType) -> Option<Vector> {
    let mut data_type = None;
    let mut readable = Vec::with_capacity(selected.sources.len());
    for source in selected.sources.iter() {
        let typed = match source {
            None => false,
            Some((array, field, logical, _)) => match layout(array.as_ref(), field, logical.as_ref())? {
                None => false,
                Some(scalar) if scalar == *expected => {
                    if *data_type.get_or_insert_with(|| array.data_type().clone()) != *array.data_type() { return None; }
                    true
                }
                Some(_) => return None,
            },
        };
        readable.push(typed);
    }
    let Some(data_type) = data_type else { return Some(Vector::Null) };
    let nulls = new_null_array(&data_type, 1);
    let null = selected.sources.len();
    let arrays: Vec<&dyn Array> = selected.sources.iter().zip(&readable)
        .map(|(source, typed)| match source { Some((array, ..)) if *typed => array.as_ref(), _ => nulls.as_ref() })
        .chain(std::iter::once(nulls.as_ref())).collect();
    let cells: Vec<(usize, usize)> = selected.cells.iter().map(|(source, row)| if readable[source] { (source, row) } else { (null, 0) }).collect();
    Vector::of(super::interleave(&arrays, &cells).ok()?.as_ref(), expected)
}

fn read_values(values: &[Value], expected: &ScalarType, len: usize) -> Option<Vector> {
    if values.len() != len { return None; }
    fn cells<'a, T>(values: &'a [Value], read: impl Fn(&'a Value) -> Option<T>) -> Option<Vec<Option<T>>> {
        values.iter().map(|value| match value { Value::Null => Some(None), value => read(value).map(Some) }).collect()
    }
    Some(match expected {
        ScalarType::I32 => Vector::I32(cells(values, |value| match value { Value::I32(value) => Some(*value), _ => None })?.into()),
        ScalarType::I64 => Vector::I64(cells(values, |value| match value { Value::I64(value) => Some(*value), _ => None })?.into()),
        ScalarType::F64 => Vector::F64(cells(values, |value| match value { Value::F64(value) => Some(*value), _ => None })?.into()),
        ScalarType::Bool => Vector::Bool(cells(values, |value| match value { Value::Bool(value) => Some(*value), _ => None })?.into()),
        ScalarType::Utf8 => Vector::Text(cells(values, |value| match value { Value::Utf8(value) => Some(value.as_str()), _ => None })?.into()),
        ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone } => Vector::Micros(cells(values, |value| match value {
            Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: zone } if zone == timezone => Some(*value),
            _ => None,
        })?.into(), timezone.clone()),
        _ => return None,
    })
}

/// Every row's copy of a constant.
fn constant(value: &Value, len: usize) -> Option<Vector> {
    Some(match value {
        Value::Null => Vector::Null,
        Value::I32(value) => Vector::I32(Int32Array::from_value(*value, len)),
        Value::I64(value) => Vector::I64(Int64Array::from_value(*value, len)),
        Value::F64(value) => Vector::F64(Float64Array::from_value(*value, len)),
        Value::Bool(value) => Vector::Bool(BooleanArray::from(vec![*value; len])),
        Value::Utf8(value) => Vector::Text(StringArray::from_iter_values(std::iter::repeat_n(value.as_str(), len))),
        Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone } => Vector::Micros(TimestampMicrosecondArray::from_value(*value, len), timezone.clone()),
        _ => return None,
    })
}

struct Evaluation<'a> { schema: &'a Schema, batch: &'a ColumnBatch, parameters: rtdb_expr::Parameters<'a> }

impl Evaluation<'_> {
    fn vector(&self, expr: &Expr) -> Option<Vector> {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || self.vector_at(expr))
    }

    fn vector_at(&self, expr: &Expr) -> Option<Vector> {
        let len = self.batch.len;
        let vector = match expr {
            Expr::Column { id, data_type } => read(&self.batch.columns[self.schema.position(*id)?], data_type, len)?,
            Expr::Literal(value) => constant(value, len)?,
            Expr::Parameter { index, .. } => constant(self.parameters.known(*index)?, len)?,
            Expr::Call { name, args, .. } => self.call(&name.to_lowercase().replace('-', "_"), args)?,
            Expr::Case { branches, otherwise } => self.case(branches, otherwise)?,
            expr => {
                let column = crate::vector::column(expr, self.schema, self.batch, self.parameters)?;
                read(&column, &expr.data_type(), len)?
            }
        };
        // Each node yields its declared type, as the evaluator's cells do.
        let declared = expr.data_type();
        match vector.scalar() {
            None => declared.nullable().then_some(vector),
            Some(scalar) => (plain_scalar(&declared) == Some(&scalar)).then_some(vector),
        }
    }

    fn call(&self, name: &str, args: &[Expr]) -> Option<Vector> {
        if name == "extract" {
            let [Expr::Literal(Value::Utf8(field)), source] = args else { return None };
            return match self.vector(source)? {
                Vector::Null => Some(Vector::Null),
                Vector::Micros(values, zone) => kernels::extract_timestamps(field, &values, zone.as_deref()).map(Vector::I32),
                _ => None,
            };
        }
        if name == "round" {
            let scale = match args {
                [_] => 0,
                [_, Expr::Literal(Value::I64(scale))] => *scale,
                [_, Expr::Literal(Value::I32(scale))] => i64::from(*scale),
                _ => return None,
            };
            return match self.vector(&args[0])? {
                Vector::Null => Some(Vector::Null),
                Vector::F64(values) => kernels::round_doubles(&values, scale).map(Vector::F64),
                // Rounding an integer to no places is the integer itself.
                vector @ (Vector::I32(_) | Vector::I64(_)) if args.len() == 1 => Some(vector),
                _ => None,
            };
        }
        if !(matches!(name, "upper" | "lower" | "length" | "character_length" | "char_length" | "substring") || kernels::is_double_function(name)) { return None; }
        let arguments = args.iter().map(|arg| self.vector(arg)).collect::<Option<Vec<_>>>()?;
        // The scalar calls answer null for a null argument before reading the others.
        if arguments.iter().any(|argument| matches!(argument, Vector::Null)) { return Some(Vector::Null); }
        match (name, arguments.as_slice()) {
            ("abs", [Vector::F64(values)]) => kernels::double_function(name, values).map(Vector::F64),
            ("abs", [Vector::I64(values)]) => kernels::abs_integers(values).map(Vector::I64),
            ("abs", _) => None,
            ("upper" | "lower", [Vector::Text(values)]) => kernels::text_case(values, name == "upper").map(Vector::Text),
            ("length" | "character_length" | "char_length", [Vector::Text(values)]) => kernels::text_lengths(values).map(Vector::I32),
            ("substring", [Vector::Text(values), position, rest @ ..]) if rest.len() <= 1 => {
                let lengths = match rest { [length] => Some(length.integers()?), _ => None };
                kernels::text_substrings(values, &position.integers()?, lengths.as_ref()).map(Vector::Text)
            }
            (name, [argument]) => kernels::double_function(name, &argument.doubles()?).map(Vector::F64),
            _ => None,
        }
    }

    /// Each row takes the value of the first branch whose condition holds, as the evaluator's searched CASE does;
    /// every branch is computed for every row, so one failing anywhere leaves the batch to the evaluator.
    fn case(&self, branches: &[(Expr, Expr)], otherwise: &Expr) -> Option<Vector> {
        let len = self.batch.len;
        let truths = branches.iter()
            .map(|(condition, _)| super::kernel::truth(condition, self.schema, self.batch, self.parameters))
            .collect::<Option<Vec<_>>>()?;
        let values = branches.iter().map(|(_, value)| value).chain(std::iter::once(otherwise))
            .map(|value| self.vector(value)).collect::<Option<Vec<_>>>()?;
        let mut typed = values.iter().filter(|value| !matches!(value, Vector::Null));
        let Some(first) = typed.next() else { return Some(Vector::Null) };
        let scalar = first.scalar()?;
        if typed.any(|value| value.scalar().as_ref() != Some(&scalar)) { return None; }
        let template = first.array()?;
        let nulls = new_null_array(template.data_type(), len);
        let arrays = values.iter().map(|value| value.array()).collect::<Vec<_>>();
        let sources: Vec<&dyn Array> = arrays.iter().map(|array| array.as_deref().unwrap_or(nulls.as_ref())).collect();
        let cells: Vec<(usize, usize)> = (0..len)
            .map(|row| (truths.iter().position(|truth| truth[row] == Some(true)).unwrap_or(branches.len()), row))
            .collect();
        Vector::of(super::interleave(&sources, &cells).ok()?.as_ref(), &scalar)
    }
}

/// The column of `expr` over `batch`, typed as the evaluator's packed results are, or `None` where the evaluator must
/// compute it.
pub(crate) fn column(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Option<Column> {
    if !matches!(expr, Expr::Call { .. } | Expr::Case { .. }) { return None; }
    let vector = Evaluation { schema, batch, parameters }.vector(expr)?;
    let len = batch.len;
    let (array, logical): (ArrayRef, LogicalType) = match (vector.array(), vector.scalar()) {
        (Some(array), Some(scalar)) if array.null_count() < len => {
            let logical = LogicalType::Scalar(scalar);
            let logical = if array.null_count() == 0 { logical } else { logical.maybe() };
            (array, logical)
        }
        _ => (Arc::new(NullArray::new(len)), LogicalType::Null),
    };
    let field = rtdb_columnar::to_field("value", &logical).ok()?;
    (field.data_type() == array.data_type()).then(|| Column::Arrow { array, field: Arc::new(field), logical: Some(logical) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{BinaryOp, ColumnId, Field};
    use rtdb_types::Timestamp;

    struct Random(u64);
    impl Random {
        fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 }
        fn below(&mut self, bound: u64) -> u64 { self.next() % bound }
        fn pick<T: Clone>(&mut self, items: &[T]) -> T { items[self.below(items.len() as u64) as usize].clone() }
    }

    const UTC: Option<&str> = Some("UTC");
    fn types() -> [ScalarType; 6] {
        [ScalarType::I64, ScalarType::I32, ScalarType::F64, ScalarType::Utf8, ScalarType::Bool,
            ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("Europe/London".into()) }]
    }
    fn field(id: u32) -> Field {
        Field { id: ColumnId(id), name: format!("c{id}"), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::Scalar(types()[id as usize].clone()).maybe() }
    }
    fn column(id: u32) -> Expr { field(id).expression() }
    fn call(name: &str, args: Vec<Expr>) -> Expr {
        let return_type = rtdb_plan::function_type_with_args(name, &args).unwrap();
        Expr::Call { name: name.into(), args, return_type }
    }

    /// The cells of one column, laid out as the scan's three column shapes lay them out.
    fn shaped(random: &mut Random, array: ArrayRef, scalar: &ScalarType) -> Column {
        let logical = LogicalType::Scalar(scalar.clone()).maybe();
        let field = Arc::new(rtdb_columnar::to_field("c", &logical).unwrap());
        let arrow = Column::Arrow { array: Arc::clone(&array), field: Arc::clone(&field), logical: Some(logical.clone()) };
        match random.below(3) {
            0 => arrow,
            1 => Column::Values((0..array.len()).map(|row| arrow.value(row).unwrap()).collect()),
            _ => {
                // Two pages and an absent one, each row picked from where it lies.
                let len = array.len();
                let split = len / 2;
                let sources = vec![Some((array.slice(0, split), Arc::clone(&field), Some(logical.clone()), None)),
                    None, Some((array.slice(split, len - split), field, Some(logical), None))];
                let cells = (0..len).map(|row| if row % 11 == 5 { (1, 0) } else if row < split { (0, row) } else { (2, row - split) }).collect();
                Column::Selected(SelectedColumn::new(sources, Arc::new(cells)))
            }
        }
    }

    fn batch(random: &mut Random, rows: usize, memory: &Arc<MemoryBudget>) -> ColumnBatch {
        let null = |random: &mut Random| random.below(7) == 0;
        let mut i64s = Vec::new(); let mut i32s = Vec::new(); let mut f64s = Vec::new(); let mut texts = Vec::new(); let mut bools = Vec::new(); let mut times = Vec::new();
        let doubles = [0.0, -0.0, 2.5, -2.5, 1e300, -1e-300, f64::NAN, f64::INFINITY, 1234.5678, 9999.995, -16.0];
        let strings = ["", "a", "closed", "open", "ß", "straße", "name-0000007-éß", "İx", "😀x😀", "line\nbreak", "日本語"];
        for _ in 0..rows {
            i64s.push((!null(random)).then(|| match random.below(8) { 0 => i64::MAX, 1 => i64::MIN, _ => random.below(41) as i64 - 20 }));
            i32s.push((!null(random)).then(|| random.below(21) as i32 - 5));
            f64s.push((!null(random)).then(|| if random.below(2) == 0 { random.pick(&doubles) } else { (random.below(2_000_000) as f64) / 100.0 }));
            texts.push((!null(random)).then(|| random.pick(&strings).to_owned()));
            bools.push((!null(random)).then(|| random.below(2) == 0));
            // Around the 2021 London transitions, and anywhere within a few centuries.
            times.push((!null(random)).then(|| match random.below(3) {
                0 => 1_616_893_200_000_000 + random.below(7_200_000_000) as i64 - 3_600_000_000,
                1 => 1_635_642_000_000_000 + random.below(7_200_000_000) as i64 - 3_600_000_000,
                _ => (random.next() % 20_000_000_000_000_000) as i64 - 10_000_000_000_000_000,
            }));
        }
        let arrays: [ArrayRef; 6] = [Arc::new(Int64Array::from(i64s)), Arc::new(Int32Array::from(i32s)), Arc::new(Float64Array::from(f64s)),
            Arc::new(StringArray::from(texts)), Arc::new(BooleanArray::from(bools)),
            Arc::new(TimestampMicrosecondArray::from(times).with_timezone("Europe/London"))];
        let columns = arrays.into_iter().zip(types()).map(|(array, scalar)| shaped(random, array, &scalar)).collect();
        ColumnBatch::settled(columns, rows, memory.reserve(0).unwrap()).unwrap()
    }

    fn number(random: &mut Random, depth: usize) -> Expr {
        if depth == 0 || random.below(3) == 0 {
            return match random.below(5) {
                0 => column(0), 1 => column(1), 2 => column(2),
                3 => call("extract", vec![Expr::Literal(Value::Utf8(random.pick(&["YEAR", "MONTH", "DAY", "HOUR", "DOW", "WEEK", "TIMEZONE_HOUR", "EPOCH"]).into())), column(5)]),
                _ => call("length", vec![text(random, 0)]),
            };
        }
        let argument = number(random, depth - 1);
        match random.below(4) {
            0 => call(random.pick(&["sqrt", "ln", "exp", "sin", "atan", "floor", "ceil", "log10"]), vec![argument]),
            1 => if random.below(2) == 0 { call("round", vec![argument]) } else { call("round", vec![argument, Expr::Literal(Value::I64(random.below(9) as i64 - 3))]) },
            2 => call("abs", vec![argument]),
            _ => {
                let ty = argument.data_type();
                let other = if ty == LogicalType::Scalar(ScalarType::F64).maybe() { column(2) } else { argument.clone() };
                Expr::Case { branches: vec![(predicate(random), argument)], otherwise: Box::new(other) }
            }
        }
    }

    fn text(random: &mut Random, depth: usize) -> Expr {
        if depth == 0 || random.below(3) == 0 { return column(3); }
        let argument = text(random, depth - 1);
        match random.below(4) {
            0 => call("upper", vec![argument]),
            1 => call("lower", vec![argument]),
            2 => {
                let mut args = vec![argument, if random.below(4) == 0 { column(1) } else { Expr::Literal(Value::I64(random.below(12) as i64 - 3)) }];
                if random.below(3) != 0 { args.push(Expr::Literal(Value::I64(random.below(6) as i64 - 1))); }
                call("substring", args)
            }
            _ => Expr::Case { branches: vec![(predicate(random), argument)], otherwise: Box::new(Expr::Literal(Value::Utf8("other".into()))) },
        }
    }

    fn predicate(random: &mut Random) -> Expr {
        match random.below(3) {
            0 => column(4),
            1 => Expr::binary(BinaryOp::Eq, column(3), Expr::Literal(Value::Utf8(random.pick(&["closed", "ß", ""]).into()))),
            _ => Expr::binary(BinaryOp::Gt, column(random.below(3) as u32), Expr::Literal(Value::I64(random.below(9) as i64 - 4))),
        }
    }

    /// The integer CASE that sums matching rows.
    fn tally(random: &mut Random) -> Expr {
        Expr::Case { branches: vec![(predicate(random), Expr::Literal(Value::I64(1)))], otherwise: Box::new(Expr::Literal(Value::I64(random.below(2) as i64))) }
    }

    #[test]
    fn projected_calls_and_cases_agree_with_the_scalar_evaluator_on_every_batch_they_compute() {
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new((0..6).map(field).collect()).unwrap();
        let mut random = Random(0x2545_f491_4f6c_dd1d);
        let (mut computed, mut declined) = (0, 0);
        for round in 0..1500 {
            let batch = batch(&mut random, 40, &memory);
            let expr = match round % 3 { 0 => number(&mut random, 3), 1 => text(&mut random, 3), _ => tally(&mut random) };
            let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
            let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Values(&[]), parameter_types: None, current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC",
                outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None };
            let expected: Vec<rtdb_types::Result<Value>> = (0..batch.len).map(|row| {
                let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
                rtdb_expr::evaluate(&expr, &schema, &values, &context)
            }).collect();
            let Some(column) = super::column(&expr, &schema, &batch, rtdb_expr::Parameters::Values(&[])) else { declined += 1; continue };
            computed += 1;
            for (row, expected) in expected.into_iter().enumerate() {
                let expected = expected.unwrap_or_else(|error| panic!("the kernel computed {expr:?} where the evaluator raises {error:?}"));
                let actual = column.value(row).unwrap();
                let same = match (&expected, &actual) { (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(), (a, b) => a == b };
                assert!(same, "{expr:?} at row {row}: evaluator {expected:?}, kernel {actual:?}");
            }
        }
        assert!(computed > 300, "the kernels should compute many generated expressions: computed {computed}, declined {declined}");
    }

    #[test]
    fn like_truths_agree_with_the_scalar_evaluator_over_every_column_shape() {
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new((0..6).map(field).collect()).unwrap();
        let mut random = Random(0x0bad_cafe_dead_beef);
        let context = rtdb_expr::EvalContext::default();
        let mut computed = 0;
        for _ in 0..300 {
            let batch = batch(&mut random, 40, &memory);
            let pattern = random.pick(&["%7-é%", "%é%", "na%", "_", "%", "", "%ß", "line%", "%a_e%", "%\\%"]);
            let escape = if random.below(2) == 0 { Some('\\') } else { None };
            let expr = Expr::Like { expr: Box::new(column(3)), pattern: Box::new(Expr::Literal(Value::Utf8(pattern.into()))), escape,
                case_insensitive: random.below(3) == 0, negated: random.below(2) == 0 };
            let expected: Option<Vec<Option<bool>>> = (0..batch.len).map(|row| {
                let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
                match rtdb_expr::evaluate(&expr, &schema, &values, &context).ok()? { Value::Null => Some(None), Value::Bool(value) => Some(Some(value)), _ => None }
            }).collect();
            let actual = super::super::kernel::truth(&expr, &schema, &batch, rtdb_expr::Parameters::Values(&[]));
            if let Some(actual) = actual {
                computed += 1;
                assert_eq!(expected, Some(actual), "{expr:?}");
            }
        }
        assert!(computed > 200, "computed {computed}");
    }

    #[test]
    fn a_kernel_column_is_typed_as_the_evaluators_packed_results() {
        let memory = MemoryBudget::new(1 << 20);
        let field = Field { id: ColumnId(0), name: "ts".into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: UTC.map(str::to_owned) }) };
        let schema = Schema::new(vec![field.clone()]).unwrap();
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(1_577_836_800_000_000), Some(1_580_515_200_000_000)]).with_timezone("UTC"));
        let column = Column::Arrow { field: Arc::new(ArrowField::new("ts", array.data_type().clone(), false)), array, logical: Some(field.data_type.clone()) };
        let batch = ColumnBatch::settled(vec![column], 2, memory.reserve(0).unwrap()).unwrap();
        let month = call("extract", vec![Expr::Literal(Value::Utf8("MONTH".into())), field.expression()]);
        let Some(Column::Arrow { array, logical, .. }) = super::column(&month, &schema, &batch, rtdb_expr::Parameters::Values(&[])) else { panic!("EXTRACT over timestamps is a kernel") };
        assert_eq!(logical, Some(LogicalType::Scalar(ScalarType::I32)));
        assert_eq!(array.as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec(), vec![1, 2]);

        let null = call("extract", vec![Expr::Literal(Value::Utf8("MONTH".into())), Expr::Literal(Value::Null)]);
        let Some(Column::Arrow { logical, .. }) = super::column(&null, &schema, &batch, rtdb_expr::Parameters::Values(&[])) else { panic!("a null argument is a null column") };
        assert_eq!(logical, Some(LogicalType::Null));
    }
}
