//! Page metadata tests for a scan's comparisons between a column and a constant, after the reference's metadata selector.
//! A test that returns false proves that no row of the page satisfies the scan predicates.

use rtdb_format::{bloom::{BLOOM_K, BloomFilter}, metadata::LeafStatistics};
use rtdb_plan::{BinaryOp, Expr, Scan, Schema};
use rtdb_types::{LogicalType, Result, ScalarType, Value};
use crate::context::Execution;

#[derive(Clone, Copy)]
enum Extremum { Min, Max }

/// A passive value built once per scan, with its constant operands already evaluated.
pub(crate) enum PageTest {
    Always,
    All(Vec<PageTest>),
    Any(Vec<PageTest>),
    /// `op(column's flavour extremum, value)` over the page's double-valued bounds.
    Bound { column: String, flavour: &'static str, extremum: Extremum, op: BinaryOp, value: f64, may_have_legacy_date64: bool },
    Bloom { column: String, positions: [u32; BLOOM_K as usize] },
    Presence { column: String, leg: &'static str },
}

impl PageTest {
    pub(crate) async fn for_scan(scan: &Scan, execution: &Execution) -> Result<Self> {
        let mut tests = Vec::with_capacity(scan.predicates.len());
        for predicate in &scan.predicates { tests.push(Box::pin(Self::build(predicate, scan, execution)).await?); }
        Ok(Self::all(tests))
    }

    async fn build(expr: &Expr, scan: &Scan, execution: &Execution) -> Result<Self> {
        let Expr::Binary { op, left, right } = expr else { return Ok(Self::Always) };
        match op {
            BinaryOp::And | BinaryOp::Or => {
                let left = Box::pin(Self::build(left, scan, execution)).await?;
                let right = Box::pin(Self::build(right, scan, execution)).await?;
                Ok(if *op == BinaryOp::And { Self::all(vec![left, right]) } else { Self::any(vec![left, right]) })
            }
            BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
                let column = |expr: &Expr| match expr { Expr::Column { id, .. } => scan.columns.iter().find(|field| field.id == *id), _ => None };
                let constant = |expr: &Expr| matches!(expr, Expr::Literal(_) | Expr::Parameter { .. });
                let (column, value, column_first) = match (column(left), column(right)) {
                    (Some(column), None) if constant(right) => (column, right, true),
                    (None, Some(column)) if constant(left) => (column, left, false),
                    _ => return Ok(Self::Always),
                };
                let value = execution.evaluate(&execution.scope()?, value, &Schema::default(), &[]).await?;
                let may_have_legacy_date64 = may_have_date64(&column.data_type);
                Self::comparison(*op, column.name.clone(), value, column_first, may_have_legacy_date64)
            }
            _ => Ok(Self::Always),
        }
    }

    fn comparison(op: BinaryOp, column: String, value: Value, column_first: bool, may_have_legacy_date64: bool) -> Result<Self> {
        use {BinaryOp as B, Extremum::*};
        let bound = |extremum, op| match meta_double(&value) {
            Some((flavour, value)) => Self::Bound { column: column.clone(), flavour, extremum, op, value, may_have_legacy_date64 },
            None => Self::Always,
        };
        Ok(match (op, column_first) {
            (B::Lt, true) => bound(Min, B::Lt), (B::LtEq, true) => bound(Min, B::LtEq),
            (B::Gt, true) => bound(Max, B::Gt), (B::GtEq, true) => bound(Max, B::GtEq),
            (B::Lt, false) => bound(Max, B::Gt), (B::LtEq, false) => bound(Max, B::GtEq),
            (B::Gt, false) => bound(Min, B::Lt), (B::GtEq, false) => bound(Min, B::LtEq),
            _ if is_bytes(&value) => Self::Bloom { column, positions: BloomFilter::byte_value_positions(&value)? },
            _ => {
                let mut tests = vec![bound(Min, B::LtEq), bound(Max, B::GtEq)];
                if column_first { if let Some(leg) = presence_leg(&value) { tests.push(Self::Presence { column, leg }); } }
                Self::all(tests)
            }
        })
    }

    fn all(tests: Vec<Self>) -> Self {
        let mut tests: Vec<_> = tests.into_iter().filter(|test| !matches!(test, Self::Always)).collect();
        if tests.len() <= 1 { tests.pop().unwrap_or(Self::Always) } else { Self::All(tests) }
    }
    fn any(tests: Vec<Self>) -> Self {
        if tests.iter().any(|test| matches!(test, Self::Always)) { Self::Always } else { Self::Any(tests) }
    }

    /// `columns` is a page's column metadata; a column the page doesn't hold satisfies no test.
    pub(crate) fn matches(&self, columns: &LeafStatistics<'_>) -> Result<bool> {
        Ok(match self {
            Self::Always => true,
            Self::All(tests) => { for test in tests { if !test.matches(columns)? { return Ok(false); } } true }
            Self::Any(tests) => { for test in tests { if test.matches(columns)? { return Ok(true); } } false }
            Self::Bound { column: name, flavour, extremum, op, value, may_have_legacy_date64 } => {
                let key = match extremum { Extremum::Min => "min", Extremum::Max => "max" };
                match columns.root_column(name).and_then(|column| column.bound(flavour, key)) {
                    Some(bound) => {
                        // Legacy Date64 extrema rounded partial pre-epoch days toward zero.
                        let bound = if *may_have_legacy_date64 && *flavour == "date-times"
                            && matches!(extremum, Extremum::Min) && bound <= 0.0 {
                            bound - 86_400.0
                        } else { bound };
                        match op { BinaryOp::Lt => bound < *value, BinaryOp::LtEq => bound <= *value, BinaryOp::Gt => bound > *value, _ => bound >= *value }
                    }
                    _ => false,
                }
            }
            Self::Bloom { column: name, positions } => match columns.root_column(name) {
                None => false,
                Some(column) => match column.bloom() {
                    Some(bytes) => BloomFilter::from_bytes(bytes)?.contains_positions(positions),
                    _ => true,
                },
            },
            Self::Presence { column: name, leg } => columns.root_column(name).and_then(|column| column.flag(leg)) == Some(true),
        })
    }
}

pub(crate) fn may_have_date64(ty: &LogicalType) -> bool {
    match ty {
        LogicalType::Scalar(ScalarType::DateMilli) => true,
        LogicalType::Maybe(value) | LogicalType::RunEndEncoded { value, .. } => may_have_date64(value),
        LogicalType::Union(legs) => legs.iter().any(may_have_date64),
        _ => false,
    }
}

/// The reference's `_meta_double`: the flavour column a value's bounds live in, and the value on that column's scale.
fn meta_double(value: &Value) -> Option<(&'static str, f64)> {
    let per_second = |unit: rtdb_types::TimeUnit| unit.units_per_second() as f64;
    Some(match value {
        Value::I8(v) => ("numbers", f64::from(*v)), Value::I16(v) => ("numbers", f64::from(*v)), Value::I32(v) => ("numbers", f64::from(*v)),
        Value::I64(v) => ("numbers", *v as f64), Value::F32(v) => ("numbers", f64::from(*v)), Value::F64(v) => ("numbers", *v),
        Value::Timestamp { value, unit, .. } => ("date-times", *value as f64 / per_second(*unit)),
        Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) => ("date-times", value.date_days()? as f64 * 86_400.0),
        Value::Time { value, unit, .. } => ("times-of-day", *value as f64 / per_second(*unit)),
        Value::Duration { value, unit } => ("durations", *value as f64 / per_second(*unit)),
        _ => return None,
    })
}
fn is_bytes(value: &Value) -> bool {
    matches!(value, Value::Utf8(_) | Value::Binary(_) | Value::FixedSizeBinary(_) | Value::Keyword(_) | Value::Uuid(_) | Value::Uri(_))
}
fn presence_leg(value: &Value) -> Option<&'static str> {
    match value { Value::Bool(_) => Some("bool"), Value::Transit(_) => Some("transit"), _ => None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::metadata::LeafColumns;
    use rtdb_types::{Keyword, StructFields};

    fn byte_statistics(column: &str, bloom: Option<Vec<u8>>) -> Value {
        let mut fields = StructFields::from([
            ("col-name".into(), Value::Utf8(column.into())),
            ("root-col?".into(), Value::Bool(true)),
        ]);
        if let Some(bloom) = bloom {
            fields.insert("bytes".into(), Value::Struct(StructFields::from([
                ("bloom".into(), Value::Binary(bloom)),
            ])));
        }
        Value::Struct(fields)
    }

    #[test]
    fn byte_equality_probes_match_persisted_column_blooms() {
        let empty = BloomFilter::default().to_bytes().unwrap();
        let miss = LeafColumns::new(vec![byte_statistics("payload", Some(empty))]);
        for value in [
            Value::Utf8("café".into()),
            Value::Binary(vec![0, 255, 8]),
            Value::FixedSizeBinary(vec![7, 0, 255]),
            Value::Keyword(Keyword::from_name("ns/café")),
            Value::Uuid([9; 16]),
            Value::Uri("https://example.test/a%20b".into()),
        ] {
            let ty = value.logical_type();
            let field = rtdb_columnar::to_field("payload", &ty).unwrap();
            let array = rtdb_columnar::build_array(&ty, std::slice::from_ref(&value)).unwrap();
            let mut columns = Vec::new();
            rtdb_format::metadata::column_metadata(&field, array.as_ref(), true, true, &mut columns).unwrap();
            let page = LeafColumns::new(columns);
            let stats = page.statistics().unwrap();
            assert!(stats.root_column("payload").unwrap().bloom().is_some(), "{value:?}");
            for column_first in [false, true] {
                let test = PageTest::comparison(BinaryOp::Eq, "payload".into(), value.clone(), column_first, false).unwrap();
                assert!(test.matches(&stats).unwrap(), "{value:?}");
                assert!(!test.matches(&miss.statistics().unwrap()).unwrap(), "{value:?}");
            }
        }
    }

    #[test]
    fn byte_equality_validates_blooms_only_when_reached() {
        let equality = |column: &str| PageTest::comparison(
            BinaryOp::Eq, column.into(), Value::Utf8("needle".into()), true, false,
        ).unwrap();
        let mut trailing = BloomFilter::default().to_bytes().unwrap();
        trailing.push(0);
        for bytes in [vec![0], trailing] {
            let page = LeafColumns::new(vec![
                byte_statistics("unfiltered", None),
                byte_statistics("payload", Some(bytes)),
            ]);
            let stats = page.statistics().unwrap();
            assert!(!equality("missing").matches(&stats).unwrap());
            assert!(equality("unfiltered").matches(&stats).unwrap());
            let all = PageTest::All(vec![equality("missing"), equality("payload")]);
            let any = PageTest::Any(vec![equality("unfiltered"), equality("payload")]);
            assert!(!all.matches(&stats).unwrap());
            assert!(any.matches(&stats).unwrap());
            assert_eq!(equality("payload").matches(&stats).unwrap_err().code, "rtdb.format/invalid-bloom");
            let transit = PageTest::comparison(BinaryOp::Eq, "payload".into(), Value::Transit(vec![1]), true, false).unwrap();
            assert!(!transit.matches(&stats).unwrap());
        }
    }

    #[test]
    fn previous_date64_statistics_keep_pre_epoch_partial_days_eligible() {
        let page = LeafColumns::new(vec![Value::Struct(StructFields::from([
            ("col-name".into(), Value::Utf8("d".into())),
            ("root-col?".into(), Value::Bool(true)),
            ("date-times".into(), Value::Struct(StructFields::from([
                ("min".into(), Value::F64(0.0)),
                ("max".into(), Value::F64(0.0)),
            ]))),
        ]))]);
        let stats = page.statistics().unwrap();
        let matches = |date, compatibility| {
            PageTest::comparison(BinaryOp::Eq, "d".into(), date, true, compatibility)
                .unwrap().matches(&stats).unwrap()
        };
        assert!(matches(Value::DateDay(-1), true));
        assert!(!matches(Value::DateDay(-1), false));
        assert!(!matches(Value::DateDay(-2), true));
        assert!(!matches(Value::DateDay(1), true));
    }
}
