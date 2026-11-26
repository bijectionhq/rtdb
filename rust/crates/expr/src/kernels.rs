//! Column-at-a-time forms of scalar functions over whole Arrow columns of one plain type.
//! Each computes, cell by cell, what the scalar call computes for that cell's value, through the same implementation;
//! a null cell gives a null.
//! `None` means the column is the scalar evaluator's to compute: some cell fails there, or its result has a layout
//! these columns can't hold, and only the evaluator reproduces that error or result in row order.

use arrow_array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, builder::StringBuilder};

/// EXTRACT(`field` FROM cell) over microsecond timestamps sharing the zone label `timezone`.
pub fn extract_timestamps(field: &str, values: &TimestampMicrosecondArray, timezone: Option<&str>) -> Option<Int32Array> {
    crate::temporal_functions::extract_timestamps(field, values, timezone)
}

/// A one-argument double function — the float math functions, `floor`, `ceil`/`ceiling` and `abs` — over doubles.
/// An integer argument reaches these functions as the double nearest it.
pub fn double_function(name: &str, values: &Float64Array) -> Option<Float64Array> {
    Some(values.unary(double(name)?))
}

pub fn is_double_function(name: &str) -> bool { double(name).is_some() }

fn double(name: &str) -> Option<fn(f64) -> f64> {
    let function: fn(f64) -> f64 = match name {
        "floor" => f64::floor,
        "ceil" | "ceiling" => f64::ceil,
        "abs" => f64::abs,
        name => crate::functions::unary_float_math(name)?,
    };
    Some(function)
}

/// ROUND(cell, `scale`) over doubles.
pub fn round_doubles(values: &Float64Array, scale: i64) -> Option<Float64Array> {
    let mut cells = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        cells.push(if values.is_null(row) { None } else { Some(crate::functions::round_float(values.value(row), scale).ok()?) });
    }
    Some(Float64Array::from(cells))
}

/// ABS over 64-bit integers.
pub fn abs_integers(values: &Int64Array) -> Option<Int64Array> {
    let mut cells = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        cells.push(if values.is_null(row) { None } else { Some(values.value(row).checked_abs()?) });
    }
    Some(Int64Array::from(cells))
}

/// UPPER or LOWER over text.
pub fn text_case(values: &StringArray, upper: bool) -> Option<StringArray> {
    let mut texts = TextColumn::new(values.len());
    for row in 0..values.len() {
        if values.is_null(row) { texts.null(); continue; }
        let text = values.value(row);
        texts.push(&if upper { text.to_uppercase() } else { text.to_lowercase() })?;
    }
    Some(texts.finish())
}

/// SUBSTRING(cell FROM position [FOR length]), the integer arguments being per-row columns of the same length.
pub fn text_substrings(values: &StringArray, positions: &Int64Array, lengths: Option<&Int64Array>) -> Option<StringArray> {
    let mut texts = TextColumn::new(values.len());
    for row in 0..values.len() {
        if values.is_null(row) || positions.is_null(row) || lengths.is_some_and(|lengths| lengths.is_null(row)) { texts.null(); continue; }
        let length = lengths.map(|lengths| lengths.value(row));
        texts.push(crate::string::text_substring(values.value(row), positions.value(row), length).ok()?)?;
    }
    Some(texts.finish())
}

/// LENGTH in characters over text.
pub fn text_lengths(values: &StringArray) -> Option<Int32Array> {
    let mut cells = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        cells.push(if values.is_null(row) { None } else { Some(i32::try_from(values.value(row).chars().count()).ok()?) });
    }
    Some(Int32Array::from(cells))
}

/// A LIKE pattern compiled once for many inputs.
pub struct LikeMatcher { pattern: crate::string::pattern::LikePattern, negated: bool }

impl LikeMatcher {
    /// `None` where the scalar LIKE rejects the pattern.
    pub fn compile(pattern: &str, escape: Option<char>, case_insensitive: bool, negated: bool) -> Option<Self> {
        let pattern = rtdb_types::Value::Utf8(pattern.to_owned());
        let pattern = crate::string::pattern::compile_like(&pattern, escape, case_insensitive, &crate::EvalContext::default()).ok()?;
        Some(Self { pattern, negated })
    }

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
    use rtdb_types::{LogicalType, ScalarType, TimeUnit, Value};

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

    /// Each cell's scalar evaluation: `None` where any cell fails.
    fn scalar(expr: &Expr, schema: &Schema, cells: impl Iterator<Item = Value>) -> Option<Vec<Value>> {
        let context = EvalContext::default();
        cells.map(|cell| evaluate(expr, schema, &[cell], &context).ok()).collect()
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
        let (mut computed, mut declined) = (0, 0);
        for zone in ZONES {
            let (schema, column) = single(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: zone.map(str::to_owned) });
            let values = TimestampMicrosecondArray::from(cells.clone()).with_timezone_opt(zone);
            for field in FIELDS {
                let expr = Expr::Call { name: "extract".into(), args: vec![Expr::Literal(Value::Utf8(field.into())), column.clone()],
                    return_type: LogicalType::Scalar(ScalarType::I32).maybe() };
                let timestamps = cells.iter().map(|cell| cell.map_or(Value::Null, |value| Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: zone.map(str::to_owned) }));
                let expected = scalar(&expr, &schema, timestamps);
                let actual = extract_timestamps(field, &values, zone);
                match (expected, actual) {
                    (Some(expected), Some(actual)) => { computed += 1; same(&expected, actual.iter().map(|cell| cell.map_or(Value::Null, Value::I32))) }
                    (None, None) => declined += 1,
                    (expected, actual) => panic!("{field} in {zone:?}: evaluator succeeded {}, kernel succeeded {}", expected.is_some(), actual.is_some()),
                }
                let bounded: Vec<Option<i64>> = cells.iter().map(|cell| cell.map(|value| value.rem_euclid(4_000_000_000_000_000))).collect();
                let expected = scalar(&expr, &schema, bounded.iter().map(|cell| cell.map_or(Value::Null, |value| Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: zone.map(str::to_owned) })));
                let actual = extract_timestamps(field, &TimestampMicrosecondArray::from(bounded).with_timezone_opt(zone), zone);
                match (expected, actual) {
                    (Some(expected), Some(actual)) => { computed += 1; same(&expected, actual.iter().map(|cell| cell.map_or(Value::Null, Value::I32))) }
                    (None, None) => declined += 1,
                    (expected, actual) => panic!("{field} in {zone:?}: evaluator succeeded {}, kernel succeeded {}", expected.is_some(), actual.is_some()),
                }
            }
        }
        assert!(computed > 40 && declined > 0, "computed {computed}, declined {declined}");
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
            same(&expected, double_function(name, &values).unwrap().iter().map(|cell| cell.map_or(Value::Null, Value::F64)));
        }
        assert!(double_function("power", &values).is_none());
        for scale in [-400, -3, -1, 0, 1, 2, 5, 15, 300, 400] {
            let expr = call("round", vec![column.clone(), Expr::Literal(Value::I64(scale))]);
            let expected = scalar(&expr, &schema, doubles());
            let actual = round_doubles(&values, scale);
            assert_eq!(expected.is_some(), actual.is_some(), "scale {scale}");
            if let (Some(expected), Some(actual)) = (expected, actual) { same(&expected, actual.iter().map(|cell| cell.map_or(Value::Null, Value::F64))); }
        }
        let expected = scalar(&call("round", vec![column.clone()]), &schema, doubles()).unwrap();
        same(&expected, round_doubles(&values, 0).unwrap().iter().map(|cell| cell.map_or(Value::Null, Value::F64)));
    }

    #[test]
    fn integer_abs_agrees_with_the_scalar_call_and_leaves_overflow_to_it() {
        let (schema, column) = single(ScalarType::I64);
        let expr = call("abs", vec![column]);
        let fine = [Some(0), Some(-5), None, Some(i64::MAX), Some(i64::MIN + 1)];
        let expected = scalar(&expr, &schema, fine.iter().map(|cell| cell.map_or(Value::Null, Value::I64))).unwrap();
        same(&expected, abs_integers(&Int64Array::from(fine.to_vec())).unwrap().iter().map(|cell| cell.map_or(Value::Null, Value::I64)));
        let overflowing = [Some(1), Some(i64::MIN)];
        assert!(scalar(&expr, &schema, overflowing.iter().map(|cell| cell.map_or(Value::Null, Value::I64))).is_none());
        assert!(abs_integers(&Int64Array::from(overflowing.to_vec())).is_none());
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
            same(&expected, text_values(text_case(&values, upper).unwrap()));
        }
        let expected = scalar(&call("length", vec![column.clone()]), &schema, texts()).unwrap();
        same(&expected, text_lengths(&values).unwrap().iter().map(|cell| cell.map_or(Value::Null, Value::I32)));
        for position in [i64::MIN, -3, 0, 1, 2, 6, 14, 100, i64::from(i32::MAX) + 7] {
            for length in [None, Some(-1), Some(0), Some(1), Some(3), Some(i64::from(i32::MAX)), Some(1 << 33)] {
                let mut args = vec![column.clone(), Expr::Literal(Value::I64(position))];
                if let Some(length) = length { args.push(Expr::Literal(Value::I64(length))); }
                let expected = scalar(&call("substring", args), &schema, texts());
                let positions = Int64Array::from(vec![position; cells.len()]);
                let lengths = length.map(|length| Int64Array::from(vec![length; cells.len()]));
                let actual = text_substrings(&values, &positions, lengths.as_ref());
                assert_eq!(expected.is_some(), actual.is_some(), "substring from {position} for {length:?}");
                if let (Some(expected), Some(actual)) = (expected, actual) { same(&expected, text_values(actual)); }
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
                let expected = scalar(&expr, &schema, cells.iter().map(|cell| cell.map_or(Value::Null, |text| Value::Utf8(text.into()))));
                let matcher = LikeMatcher::compile(pattern, escape, case_insensitive, negated);
                assert_eq!(expected.is_some(), matcher.is_some(), "{pattern:?} escaped by {escape:?}");
                if let (Some(expected), Some(matcher)) = (expected, matcher) {
                    same(&expected, matcher.column(&values).iter().map(|cell| cell.map_or(Value::Null, Value::Bool)));
                }
            }
        }
    }
}
