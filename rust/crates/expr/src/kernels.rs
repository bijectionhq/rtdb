//! Column-at-a-time forms of scalar functions over whole Arrow columns of one plain type.
//! Each computes, cell by cell, what the scalar call computes for that cell's value, through the same implementation;
//! a null cell gives a null. A failing cell raises the scalar call's error for that cell, as the row evaluator would
//! on reaching it, and the rows a `Selection` leaves out are never read, as the row evaluator never evaluates a CASE
//! branch its row doesn't take.
//! `Ok(None)` is left only for a result whose layout these columns can't hold: text past `i32::MAX` bytes.

use arrow_array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, builder::StringBuilder};
use rtdb_plan::{BinaryOp, CastTarget};
use rtdb_types::{LogicalType, Result, ScalarType, Value};

/// The rows a kernel computes, `None` for every row; the others give null.
pub type Selection<'a> = Option<&'a [bool]>;

fn reads(selection: Selection<'_>, row: usize) -> bool { selection.is_none_or(|rows| rows[row]) }

pub use crate::temporal_functions::{AgeOperand, DateBin, DateBinInput, timestamp_truncation_zone, truncate_date32, truncate_date64, truncate_timestamp};

/// Truncates signed interval components in months, days and nanoseconds.
pub fn truncate_interval(field: rtdb_plan::TemporalField, months: i32, days: i32, nanos: i64) -> Result<(i32, i32, i64)> {
    let parts = crate::temporal_functions::truncate_interval_parts(field, crate::temporal::IntervalParts { months, days, nanos })?;
    Ok((parts.months, parts.days, parts.nanos))
}

/// Casts a physical timestamp cell using scalar cast rules for units and local time.
pub fn cast_timestamp_ticks(ticks: i64, source: rtdb_types::TimeUnit, source_zoned: bool, unit: rtdb_types::TimeUnit, zoned: bool, timezone: &str) -> Result<i64> {
    crate::cast::timestamp_raw(ticks, source, source_zoned, unit, zoned, timezone)
}

/// Casts an epoch day to midnight using the scalar cast's timezone and overflow rules.
pub fn cast_date_timestamp_ticks(days: i64, unit: rtdb_types::TimeUnit, zoned: bool, timezone: &str) -> Result<i64> {
    crate::cast::date_to_timestamp(days, unit, zoned, timezone)
}

pub fn calendar_age(end: AgeOperand<'_>, start: AgeOperand<'_>) -> Result<(i32, i32, i64)> {
    let parts = crate::temporal_functions::calendar_age(end, start)?;
    Ok((parts.months, parts.days, parts.nanos))
}

/// Each row's cell, `None` for a null or unselected row; the first failing cell, in row order, raises.
fn cells<T>(values: &dyn Array, selection: Selection<'_>, mut cell: impl FnMut(usize) -> Result<T>) -> Result<Vec<Option<T>>> {
    let mut cells = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        cells.push(if values.is_null(row) || !reads(selection, row) { None } else { Some(cell(row)?) });
    }
    Ok(cells)
}

/// EXTRACT(`field` FROM cell) over microsecond timestamps sharing the zone label `timezone`; `Ok(None)` where the field
/// doesn't apply to them or the label isn't a zone.
pub fn extract_timestamps(field: &str, values: &TimestampMicrosecondArray, timezone: Option<&str>, selection: Selection<'_>) -> Result<Option<Int32Array>> {
    crate::temporal_functions::extract_timestamps(field, values, timezone, selection)
}

pub use crate::functions::FloatMath;
pub use crate::temporal_functions::RangeBins;
pub use crate::period::validate_period_bounds;

/// A scalar function with a column-at-a-time form, resolved once from its call's name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Function { Extract, Round, Abs, Upper, Lower, Length, OctetLength, Substring, Floor, Ceil, Mod, Float(FloatMath) }

impl Function {
    /// The function a call of `name`, in any case and with hyphens or underscores, runs here.
    pub fn resolve(name: &str) -> Option<Self> {
        let name = crate::functions::canonical_name(name);
        Some(match &*name {
            "extract" => Self::Extract,
            "round" => Self::Round,
            "abs" => Self::Abs,
            "upper" => Self::Upper,
            "lower" => Self::Lower,
            "length" | "character_length" | "char_length" => Self::Length,
            "octet_length" => Self::OctetLength,
            "substring" => Self::Substring,
            "floor" => Self::Floor,
            "ceil" | "ceiling" => Self::Ceil,
            "mod" => Self::Mod,
            name => Self::Float(FloatMath::resolve(name)?),
        })
    }

    /// The function of one double this is, if it is one: the float math of one argument, `floor`, `ceil` and `abs`.
    /// An integer argument reaches these functions as the double nearest it.
    pub fn double(self) -> Option<fn(f64) -> f64> {
        match self {
            Self::Floor => Some(f64::floor),
            Self::Ceil => Some(f64::ceil),
            Self::Abs => Some(f64::abs),
            Self::Float(math) => math.unary(),
            Self::Extract | Self::Round | Self::Upper | Self::Lower | Self::Length | Self::OctetLength | Self::Substring | Self::Mod => None,
        }
    }
}

/// `function` of each double.
pub fn doubles(function: fn(f64) -> f64, values: &Float64Array) -> Float64Array { values.unary(function) }

/// ROUND(cell, `scale`) over doubles.
pub fn round_doubles(values: &Float64Array, scale: i64, selection: Selection<'_>) -> Result<Float64Array> {
    Ok(cells(values, selection, |row| round_double(values.value(row), scale))?.into())
}

pub fn round_double(value: f64, scale: i64) -> Result<f64> {
    crate::functions::round_float(value, scale)
}

/// ABS over 64-bit integers.
pub fn abs_integers(values: &Int64Array, selection: Selection<'_>) -> Result<Int64Array> {
    Ok(cells(values, selection, |row| values.value(row).checked_abs().ok_or_else(crate::numeric::overflow))?.into())
}

/// `left op right` over integers, for the arithmetic operators and MOD; each result must fit `result`, the integer type
/// the row evaluator gives the pair (the wider of the two), as an `Int64Array` holding values of that width.
pub fn integer_arithmetic(op: BinaryOp, left: &Int64Array, right: &Int64Array, result: &ScalarType, selection: Selection<'_>) -> Result<Int64Array> {
    let mut values = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if left.is_null(row) || right.is_null(row) || !reads(selection, row) { values.push(None); continue; }
        let value = crate::numeric::integer_arithmetic(op, i128::from(left.value(row)), i128::from(right.value(row)))?;
        crate::numeric::integer_cell(value, result)?;
        values.push(Some(value as i64));
    }
    Ok(values.into())
}

/// `left op right` over doubles, for the arithmetic operators and MOD.
pub fn float_arithmetic(op: BinaryOp, left: &Float64Array, right: &Float64Array, selection: Selection<'_>) -> Result<Float64Array> {
    let mut values = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if left.is_null(row) || right.is_null(row) || !reads(selection, row) { values.push(None); continue; }
        let value = crate::numeric::float_arithmetic(op, left.value(row), right.value(row))
            .unwrap_or_else(|| Err(crate::numeric::inapplicable("arithmetic", &[])))?;
        values.push(Some(value));
    }
    Ok(values.into())
}

/// Unary minus over integers of the type `result`.
pub fn negate_integers(values: &Int64Array, result: &ScalarType, selection: Selection<'_>) -> Result<Int64Array> {
    Ok(cells(values, selection, |row| {
        let value = i128::from(values.value(row)).checked_neg().ok_or_else(crate::numeric::overflow)?;
        crate::numeric::integer_cell(value, result)?;
        Ok(value as i64)
    })?.into())
}

/// CAST of integers to the integer type `target`, as an `Int64Array` holding values of that width; a cell outside it
/// raises the scalar cast's error.
pub fn cast_integers(values: &Int64Array, target: &ScalarType, selection: Selection<'_>) -> Result<Int64Array> {
    Ok(cells(values, selection, |row| {
        let value = values.value(row);
        if crate::numeric::integer_cell(i128::from(value), target).is_ok() { return Ok(value); }
        let cast = CastTarget::Fixed(LogicalType::Scalar(target.clone()));
        let cell = crate::pg_catalog::cast(&Value::I64(value), &cast, &crate::EvalContext::default())?;
        crate::numeric::integer(&cell).and_then(|value| i64::try_from(value).ok()).ok_or_else(|| crate::numeric::inapplicable("cast", &[&cell]))
    })?.into())
}

/// UPPER or LOWER over text, under the process default locale's case rules.
pub fn text_case(values: &StringArray, upper: bool, selection: Selection<'_>) -> Result<Option<StringArray>> {
    let mut language = None;
    let mut texts = TextColumn::new(values.len());
    for row in 0..values.len() {
        if values.is_null(row) || !reads(selection, row) { texts.null(); continue; }
        let language = match language { Some(language) => language, None => *language.insert(crate::string::case_language()?) };
        let text = values.value(row);
        if texts.push(&if upper { language.to_upper(text) } else { language.to_lower(text) }).is_none() { return Ok(None); }
    }
    Ok(Some(texts.finish()))
}

/// SUBSTRING(cell FROM position [FOR length]), the integer arguments being per-row columns of the same length.
pub fn text_substrings(values: &StringArray, positions: &Int64Array, lengths: Option<&Int64Array>, selection: Selection<'_>) -> Result<Option<StringArray>> {
    let mut texts = TextColumn::new(values.len());
    for row in 0..values.len() {
        let null = values.is_null(row) || positions.is_null(row) || lengths.is_some_and(|lengths| lengths.is_null(row));
        if null || !reads(selection, row) { texts.null(); continue; }
        let length = lengths.map(|lengths| lengths.value(row));
        if texts.push(crate::string::text_substring(values.value(row), positions.value(row), length)?).is_none() { return Ok(None); }
    }
    Ok(Some(texts.finish()))
}

/// Borrows the scalar Binary SUBSTRING result using the same signed index narrowing and byte limits.
pub fn binary_substring(source: &[u8], position: i64, length: Option<i64>) -> Result<&[u8]> {
    crate::string::binary_substring(source, position, length)
}

/// LENGTH in characters over text; a text vector's `i32` offsets bound every count.
pub fn text_lengths(values: &StringArray) -> Int32Array {
    values.iter().map(|text| text.map(|text| text.chars().count() as i32)).collect()
}

/// A LIKE pattern compiled once for many inputs.
#[derive(Debug)]
pub struct LikeMatcher { pattern: crate::string::pattern::LikePattern, negated: bool }

impl LikeMatcher {
    /// Token allocation bound before compiling a text pattern.
    pub fn compilation_bytes(pattern: &str) -> usize { crate::string::pattern::LikePattern::compilation_bytes(pattern) }

    /// The scalar LIKE's error where it rejects the pattern.
    pub fn compile(pattern: &str, escape: Option<char>, case_insensitive: bool, negated: bool) -> Result<Self> {
        let pattern = crate::string::pattern::LikePattern::compile(pattern, escape, case_insensitive, false)?;
        Ok(Self { pattern, negated })
    }

    /// Heap bytes retained by the compiled pattern, excluding this value.
    pub fn retained_bytes(&self) -> usize { self.pattern.token_storage_bytes() }

    /// Working allocation bound for one active matcher.
    pub fn scratch_bytes(&self) -> usize { self.pattern.matcher_scratch_bytes() }

    /// The truth of `text LIKE pattern` (or NOT LIKE) for each text in turn; the states are reused across calls.
    pub fn matcher(&self) -> impl FnMut(&str) -> bool + '_ {
        let (mut active, mut next) = (Vec::new(), Vec::new());
        move |text| self.pattern.matches_in(text, &mut active, &mut next) != self.negated
    }

    pub fn column(&self, values: &StringArray) -> BooleanArray {
        let mut matches = self.matcher();
        values.iter().map(|text| text.map(&mut matches)).collect()
    }
}

/// Text cells bounded as the evaluator's results are: past `i32::MAX` bytes its column is no longer one text vector.
struct TextColumn { builder: StringBuilder, remaining: usize }

impl TextColumn {
    fn new(len: usize) -> Self { Self { builder: StringBuilder::with_capacity(len, 0), remaining: i32::MAX as usize } }
    fn null(&mut self) { self.builder.append_null(); }
    fn push(&mut self, text: &str) -> Option<()> {
        self.remaining = self.remaining.checked_sub(text.len())?;
        self.builder.append_value(text);
        Some(())
    }
    fn finish(mut self) -> StringArray { self.builder.finish() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EvalContext, evaluate};
    use rtdb_plan::{ColumnId, Expr, Field, Schema};
    use rtdb_types::TimeUnit;

    struct Random(u64);
    impl Random {
        fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 }
        fn below(&mut self, bound: u64) -> u64 { self.next() % bound }
    }

    fn single(scalar: ScalarType) -> (Schema, Expr) {
        let data_type = LogicalType::Scalar(scalar).maybe();
        let field = Field { id: ColumnId(0), name: "c0".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: data_type.clone() };
        (Schema::new(vec![field]).unwrap(), Expr::Column { id: ColumnId(0), data_type })
    }

    fn call(name: &str, args: Vec<Expr>) -> Expr {
        let return_type = rtdb_plan::function_type_with_args(name, &args).unwrap();
        Expr::Call { name: name.into(), args, return_type }
    }

    /// Each cell's scalar evaluation, or the first failing cell's error in row order.
    fn scalar(expr: &Expr, schema: &Schema, cells: impl Iterator<Item = Value>) -> Result<Vec<Value>> {
        let context = EvalContext::default();
        cells.map(|cell| evaluate(expr, schema, &[cell], &context)).collect()
    }

    /// The kernel raised exactly the error the row evaluator raises first, or computed exactly its values.
    fn agree<T>(what: &str, expected: Result<Vec<Value>>, actual: Result<T>, values: impl FnOnce(T) -> Vec<Value>) -> bool {
        match (expected, actual) {
            (Ok(expected), Ok(actual)) => { same(&expected, values(actual).into_iter()); true }
            (Err(expected), Err(actual)) => { assert_eq!(expected.code, actual.code, "{what}"); false }
            (expected, actual) => panic!("{what}: evaluator succeeded {}, kernel succeeded {}", expected.is_ok(), actual.is_ok()),
        }
    }

    fn same(expected: &[Value], actual: impl Iterator<Item = Value>) {
        let actual: Vec<Value> = actual.collect();
        assert_eq!(expected.len(), actual.len());
        for (row, (expected, actual)) in expected.iter().zip(&actual).enumerate() {
            let same = match (expected, actual) { (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(), (a, b) => a == b };
            assert!(same, "row {row}: evaluator {expected:?}, kernel {actual:?}");
        }
    }

    const ZONES: [Option<&str>; 6] = [None, Some("UTC"), Some("Europe/London"), Some("America/New_York"), Some("+05:30"), Some("Australia/Lord_Howe")];
    const FIELDS: [&str; 15] = ["YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND", "DOW", "ISODOW", "DOY", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "EPOCH", "MILLENNIUM"];

    #[test]
    fn extracted_timestamp_fields_agree_with_the_scalar_call_across_zones_and_transitions() {
        let mut random = Random(0x9e37_79b9_7f4a_7c15);
        // Around the 2021 spring-forward and fall-back transitions of London and New York, and Lord Howe's half hour.
        let anchors = [1_616_893_200_000_000i64, 1_635_642_000_000_000, 1_615_705_200_000_000, 1_636_264_800_000_000, 1_617_465_600_000_000, 0, -1, i64::MIN, i64::MAX];
        let cells: Vec<Option<i64>> = (0..400).map(|_| match random.below(10) {
            0 => None,
            1 => Some(anchors[random.below(anchors.len() as u64) as usize]),
            2..=5 => Some(anchors[random.below(5) as usize] + random.below(7_200_000_000) as i64 - 3_600_000_000),
            _ => Some(random.next() as i64 >> random.below(20)),
        }).collect();
        let (mut computed, mut raised, mut declined) = (0, 0, 0);
        let int32s = |array: Int32Array| array.iter().map(|cell| cell.map_or(Value::Null, Value::I32)).collect::<Vec<_>>();
        for zone in ZONES {
            let (schema, column) = single(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: zone.map(str::to_owned) });
            let bounded: Vec<Option<i64>> = cells.iter().map(|cell| cell.map(|value| value.rem_euclid(4_000_000_000_000_000))).collect();
            for field in FIELDS {
                let expr = Expr::Call { name: "extract".into(), args: vec![Expr::Literal(Value::Utf8(field.into())), column.clone()],
                    return_type: LogicalType::Scalar(ScalarType::I32).maybe() };
                for cells in [&cells, &bounded] {
                    let timestamps = cells.iter().map(|cell| cell.map_or(Value::Null, |value| Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: zone.map(str::to_owned) }));
                    let expected = scalar(&expr, &schema, timestamps);
                    let values = TimestampMicrosecondArray::from(cells.clone()).with_timezone_opt(zone);
                    match extract_timestamps(field, &values, zone, None) {
                        Ok(None) => { assert!(expected.is_err(), "{field} in {zone:?} declined where the evaluator computes it"); declined += 1 }
                        Ok(Some(actual)) => { agree(field, expected, Ok(actual), int32s); computed += 1 }
                        Err(error) => { agree::<Int32Array>(field, expected, Err(error), int32s); raised += 1 }
                    }
                }
            }
        }
        assert!(computed > 40 && raised + declined > 0, "computed {computed}, raised {raised}, declined {declined}");
    }

    #[test]
    fn double_functions_and_rounding_agree_with_the_scalar_call_bit_for_bit() {
        let edges = [0.0, -0.0, 1.0, -1.0, 0.5, -0.5, 2.5, -2.5, 1e-300, 1e300, f64::MAX, f64::MIN_POSITIVE, f64::INFINITY, f64::NEG_INFINITY, f64::NAN, 1234.5678, -9999.995, 0.125];
        let mut random = Random(0xdead_beef_cafe_f00d);
        let cells: Vec<Option<f64>> = (0..300).map(|_| match random.below(6) {
            0 => None,
            1 | 2 => Some(edges[random.below(edges.len() as u64) as usize]),
            _ => Some(f64::from_bits(random.next()) % 1e6),
        }).collect();
        let values = Float64Array::from(cells.clone());
        let (schema, column) = single(ScalarType::F64);
        let doubles = || cells.iter().map(|cell| cell.map_or(Value::Null, Value::F64));
        for name in ["sqrt", "exp", "ln", "log10", "sin", "cos", "tan", "asin", "acos", "atan", "sinh", "cosh", "tanh", "floor", "ceil", "ceiling", "abs"] {
            let expected = scalar(&call(name, vec![column.clone()]), &schema, doubles()).expect(name);
            let function = Function::resolve(name).and_then(Function::double).expect(name);
            same(&expected, super::doubles(function, &values).iter().map(|cell| cell.map_or(Value::Null, Value::F64)));
        }
        for name in ["power", "log", "round", "upper", "extract"] {
            assert!(Function::resolve(name).and_then(Function::double).is_none(), "{name}");
        }
        let float64s = |array: Float64Array| array.iter().map(|cell| cell.map_or(Value::Null, Value::F64)).collect::<Vec<_>>();
        for scale in [-400, -3, -1, 0, 1, 2, 5, 15, 300, 400] {
            let expr = call("round", vec![column.clone(), Expr::Literal(Value::I64(scale))]);
            agree(&format!("scale {scale}"), scalar(&expr, &schema, doubles()), round_doubles(&values, scale, None), float64s);
        }
        let expected = scalar(&call("round", vec![column.clone()]), &schema, doubles()).unwrap();
        same(&expected, round_doubles(&values, 0, None).unwrap().iter().map(|cell| cell.map_or(Value::Null, Value::F64)));
    }

    #[test]
    fn function_names_resolve_in_every_spelling() {
        assert_eq!(Function::resolve("CHARACTER-LENGTH"), Some(Function::Length));
        assert_eq!(Function::resolve("Ceiling"), Some(Function::Ceil));
        assert_eq!(Function::resolve("log10"), Some(Function::Float(FloatMath::Log10)));
        assert_eq!(Function::resolve("coalesce"), None);
    }

    #[test]
    fn rounding_keeps_non_finite_doubles() {
        let values = Float64Array::from(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY]);
        for scale in [0, 3, -400] {
            let rounded = round_doubles(&values, scale, None).expect("a non-finite double rounds to itself");
            let bits: Vec<u64> = rounded.values().iter().map(|value| value.to_bits()).collect();
            assert_eq!(bits, values.values().iter().map(|value| value.to_bits()).collect::<Vec<_>>(), "scale {scale}");
        }
    }

    #[test]
    fn integer_abs_agrees_with_the_scalar_call_and_raises_its_overflow() {
        let (schema, column) = single(ScalarType::I64);
        let expr = call("abs", vec![column]);
        let int64s = |cells: &[Option<i64>]| cells.iter().map(|cell| cell.map_or(Value::Null, Value::I64)).collect::<Vec<_>>();
        let fine = [Some(0), Some(-5), None, Some(i64::MAX), Some(i64::MIN + 1)];
        let expected = scalar(&expr, &schema, int64s(&fine).into_iter()).unwrap();
        same(&expected, abs_integers(&Int64Array::from(fine.to_vec()), None).unwrap().iter().map(|cell| cell.map_or(Value::Null, Value::I64)));
        let overflowing = [Some(1), Some(i64::MIN)];
        let expected = scalar(&expr, &schema, int64s(&overflowing).into_iter()).unwrap_err();
        assert_eq!(abs_integers(&Int64Array::from(overflowing.to_vec()), None).unwrap_err().code, expected.code);
        // A row the selection leaves out is never read, as an untaken CASE branch isn't.
        let selected = abs_integers(&Int64Array::from(overflowing.to_vec()), Some(&[true, false][..])).unwrap();
        assert_eq!(selected.iter().collect::<Vec<_>>(), [Some(1), None]);
    }

    const EDGES: [Option<i64>; 12] = [None, Some(0), Some(1), Some(-1), Some(7), Some(-7), Some(i32::MAX as i64), Some(i32::MIN as i64),
        Some(i64::MAX), Some(i64::MIN), Some(1 << 40), Some(-(1 << 33))];

    #[test]
    fn integer_arithmetic_negation_and_casts_agree_with_the_scalar_evaluator() {
        let fields = [ScalarType::I64, ScalarType::I64].map(|scalar| LogicalType::Scalar(scalar).maybe());
        let schema = Schema::new(fields.iter().enumerate().map(|(id, data_type)| Field { id: ColumnId(id as u32), name: format!("c{id}"), qualifier: None,
            qualified_only: false, unnamed: false, data_type: data_type.clone() }).collect()).unwrap();
        let column = |id: u32| Expr::Column { id: ColumnId(id), data_type: fields[id as usize].clone() };
        let pairs: Vec<(Option<i64>, Option<i64>)> = EDGES.iter().flat_map(|left| EDGES.iter().map(move |right| (*left, *right))).collect();
        let (left, right): (Vec<_>, Vec<_>) = pairs.iter().copied().unzip();
        let (left, right) = (Int64Array::from(left), Int64Array::from(right));
        let context = EvalContext::default();
        let int64s = |array: Int64Array| array.iter().map(|cell| cell.map_or(Value::Null, Value::I64)).collect::<Vec<_>>();
        for op in [BinaryOp::Add, BinaryOp::Subtract, BinaryOp::Multiply, BinaryOp::Divide, BinaryOp::Modulo] {
            let expr = Expr::binary(op, column(0), column(1));
            for (row, (a, b)) in pairs.iter().enumerate() {
                let cells = [a.map_or(Value::Null, Value::I64), b.map_or(Value::Null, Value::I64)];
                let expected = evaluate(&expr, &schema, &cells, &context).map(|value| vec![value]);
                let actual = integer_arithmetic(op, &left.slice(row, 1), &right.slice(row, 1), &ScalarType::I64, None);
                agree(&format!("{a:?} {op:?} {b:?}"), expected, actual, int64s);
            }
        }
        let expr = Expr::Unary { op: rtdb_plan::UnaryOp::Negate, expr: Box::new(column(0)) };
        for cell in EDGES {
            let expected = evaluate(&expr, &schema, &[cell.map_or(Value::Null, Value::I64), Value::Null], &context).map(|value| vec![value]);
            agree(&format!("-{cell:?}"), expected, negate_integers(&Int64Array::from(vec![cell]), &ScalarType::I64, None), int64s);
        }
        let narrowed = Expr::Cast { expr: Box::new(column(0)), target: CastTarget::Fixed(LogicalType::Scalar(ScalarType::I32)) };
        for cell in EDGES {
            let expected = evaluate(&narrowed, &schema, &[cell.map_or(Value::Null, Value::I64), Value::Null], &context)
                .map(|value| vec![match value { Value::I32(value) => Value::I64(i64::from(value)), value => value }]);
            agree(&format!("CAST({cell:?} AS INTEGER)"), expected, cast_integers(&Int64Array::from(vec![cell]), &ScalarType::I32, None), int64s);
        }
    }

    #[test]
    fn float_arithmetic_agrees_with_the_scalar_evaluator_bit_for_bit() {
        let edges = [None, Some(0.0), Some(-0.0), Some(1.5), Some(-2.0), Some(f64::INFINITY), Some(f64::NAN), Some(1e308), Some(3.0)];
        let (schema, column) = single(ScalarType::F64);
        let context = EvalContext::default();
        for op in [BinaryOp::Add, BinaryOp::Subtract, BinaryOp::Multiply, BinaryOp::Divide, BinaryOp::Modulo] {
            for a in edges {
                for b in edges {
                    let expr = Expr::binary(op, column.clone(), Expr::Literal(b.map_or(Value::Null, Value::F64)));
                    let expected = evaluate(&expr, &schema, &[a.map_or(Value::Null, Value::F64)], &context).map(|value| vec![value]);
                    let actual = float_arithmetic(op, &Float64Array::from(vec![a]), &Float64Array::from(vec![b]), None);
                    agree(&format!("{a:?} {op:?} {b:?}"), expected, actual, |array| array.iter().map(|cell| cell.map_or(Value::Null, Value::F64)).collect());
                }
            }
        }
    }

    const TEXTS: [&str; 14] = ["", "a", "B", "ß", "straße", "Ǆx", "İstanbul", "ﬁne", "name-0000007-éß", "name-0000012-ab", "日本語テキスト", "e\u{301}", "😀x😀", "line\nbreak"];

    #[test]
    fn text_functions_agree_with_the_scalar_call_on_unicode() {
        let mut random = Random(0x0123_4567_89ab_cdef);
        let cells: Vec<Option<&str>> = (0..200).map(|_| (random.below(8) != 0).then(|| TEXTS[random.below(TEXTS.len() as u64) as usize])).collect();
        let values = StringArray::from(cells.clone());
        let (schema, column) = single(ScalarType::Utf8);
        let texts = || cells.iter().map(|cell| cell.map_or(Value::Null, |text| Value::Utf8(text.into())));
        let text_values = |array: StringArray| array.iter().map(|cell| cell.map_or(Value::Null, |text| Value::Utf8(text.into()))).collect::<Vec<_>>().into_iter();
        for upper in [true, false] {
            let expected = scalar(&call(if upper { "upper" } else { "lower" }, vec![column.clone()]), &schema, texts()).unwrap();
            same(&expected, text_values(text_case(&values, upper, None).unwrap().unwrap()));
        }
        let expected = scalar(&call("length", vec![column.clone()]), &schema, texts()).unwrap();
        same(&expected, text_lengths(&values).iter().map(|cell| cell.map_or(Value::Null, Value::I32)));
        for position in [i64::MIN, -3, 0, 1, 2, 6, 14, 100, i64::from(i32::MAX) + 7] {
            for length in [None, Some(-1), Some(0), Some(1), Some(3), Some(i64::from(i32::MAX)), Some(1 << 33)] {
                let mut args = vec![column.clone(), Expr::Literal(Value::I64(position))];
                if let Some(length) = length { args.push(Expr::Literal(Value::I64(length))); }
                let expected = scalar(&call("substring", args), &schema, texts());
                let positions = Int64Array::from(vec![position; cells.len()]);
                let lengths = length.map(|length| Int64Array::from(vec![length; cells.len()]));
                let actual = text_substrings(&values, &positions, lengths.as_ref(), None).map(|texts| texts.expect("the texts fit one vector"));
                agree(&format!("substring from {position} for {length:?}"), expected, actual, |texts| text_values(texts).collect());
            }
        }
    }

    #[test]
    fn like_matches_agree_with_the_scalar_like() {
        let mut random = Random(0x5555_aaaa_1234_9876);
        let cells: Vec<Option<&str>> = (0..120).map(|_| (random.below(8) != 0).then(|| TEXTS[random.below(TEXTS.len() as u64) as usize])).collect();
        let values = StringArray::from(cells.clone());
        let (schema, column) = single(ScalarType::Utf8);
        for pattern in ["%7-é%", "%", "_", "", "a", "%ß", "S%", "%a%e", "__%__", "%\\%%", "%x\\", "%e_", "line%", "%😀_"] {
            for (escape, case_insensitive, negated) in [(None, false, false), (Some('\\'), false, true), (Some('\\'), true, false), (None, true, true)] {
                let expr = Expr::Like { expr: Box::new(column.clone()), pattern: Box::new(Expr::Literal(Value::Utf8(pattern.into()))), escape, case_insensitive, negated };
                let expected = scalar(&expr, &schema, cells.iter().map(|cell| cell.map_or(Value::Null, |text| Value::Utf8(text.into())))).ok();
                let matcher = LikeMatcher::compile(pattern, escape, case_insensitive, negated).ok();
                assert_eq!(expected.is_some(), matcher.is_some(), "{pattern:?} escaped by {escape:?}");
                if let (Some(expected), Some(matcher)) = (expected, matcher) {
                    same(&expected, matcher.column(&values).iter().map(|cell| cell.map_or(Value::Null, Value::Bool)));
                }
            }
        }
    }
}
