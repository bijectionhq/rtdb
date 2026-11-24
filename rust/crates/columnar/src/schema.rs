use rtdb_types::{OrderedSet, StructFields};
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, UnionFields, UnionMode};
use rtdb_types::{Anomaly, IntervalUnit, ListKind, LogicalType, Result, RunEndWidth, ScalarType, TimeUnit};

pub const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
pub const EXTENSION_METADATA_KEY: &str = "ARROW:extension:metadata";

/// Encodes XTDB logical nullability, extension metadata, and physical run-end layouts.
/// Invalid union legs and unsupported Arrow layouts return a structured anomaly.
pub fn to_field(name: impl Into<String>, logical: &LogicalType) -> Result<Field> {
    logical.validate()?;
    let name = name.into();
    match logical {
        LogicalType::Nothing => Ok(Field::new(name, DataType::Null, false)),
        LogicalType::Null => Ok(Field::new(name, DataType::Null, true)),
        LogicalType::Maybe(value) => {
            let field = to_field(name, value)?;
            if matches!(field.data_type(), DataType::Union(_, _)) {
                let mut legs = value.legs();
                legs.insert(LogicalType::Null);
                return to_field(field.name(), &LogicalType::from_legs(legs));
            }
            if let LogicalType::RunEndEncoded { width, value } = value.as_ref() {
                return to_field(field.name(), &LogicalType::RunEndEncoded {
                    width: *width,
                    value: Box::new(value.as_ref().clone().maybe()),
                });
            }
            Ok(field.with_nullable(true))
        }
        LogicalType::RunEndEncoded { width, value } => {
            if matches!(value.as_ref(), LogicalType::Null | LogicalType::Nothing | LogicalType::RunEndEncoded { .. }) {
                return Err(Anomaly::unsupported("rtdb/run-end-type", "Null, bottom, and nested run-end layouts cannot be run-end encoded"));
            }
            let run_ends = Arc::new(Field::new("run_ends", match width {
                RunEndWidth::I16 => DataType::Int16,
                RunEndWidth::I32 => DataType::Int32,
                RunEndWidth::I64 => DataType::Int64,
            }, false));
            Ok(Field::new(name, DataType::RunEndEncoded(run_ends, Arc::new(to_field("values", value)?)), false))
        }
        LogicalType::Union(legs) => {
            let legs = legs.iter().filter(|t| !matches!(t, LogicalType::Nothing)).collect::<Vec<_>>();
            if legs.len() > 127 {
                return Err(Anomaly::unsupported("rtdb/too-many-union-legs", "Arrow unions support at most 127 XTDB legs"));
            }
            let non_null = legs.iter().filter(|t| !matches!(t, LogicalType::Null)).collect::<Vec<_>>();
            if non_null.len() <= 1 {
                let normalized = LogicalType::from_legs(legs.into_iter().cloned());
                return to_field(name, &normalized);
            }
            let fields = legs.iter().map(|leg| {
                if &leg.unencoded() != *leg {
                    return Err(Anomaly::unsupported("rtdb/encoded-union-leg", "Union legs must be plain at every depth"));
                }
                if matches!(leg, LogicalType::Union(_) | LogicalType::Maybe(_)) {
                    return Err(Anomaly::incorrect("rtdb/non-monomorphic-leg", "A union child must be monomorphic"));
                }
                Ok(Arc::new(to_field(leg_name(leg)?, leg)?))
            }).collect::<Result<Vec<_>>>()?;
            let ids = (0..fields.len()).map(|i| i as i8);
            let union = UnionFields::try_new(ids, fields).map_err(super::arrow_error)?;
            Ok(Field::new(name, DataType::Union(union, UnionMode::Dense), false))
        }
        LogicalType::Struct(children) => {
            let fields = children.iter().map(|(name, ty)| to_field(name, ty).map(Arc::new)).collect::<Result<Vec<_>>>()?;
            Ok(Field::new(name, DataType::Struct(Fields::from(fields)), false))
        }
        LogicalType::List { kind, element } => {
            let child_name = if matches!(kind, ListKind::Map { .. }) { "entries" } else { "$data$" };
            let child = Arc::new(to_field(child_name, element)?);
            let (data_type, extension) = match kind {
                ListKind::List => (DataType::List(child), None),
                ListKind::Set => (DataType::List(child), Some("set")),
                ListKind::FixedSizeList(n) if *n >= 0 => (DataType::FixedSizeList(child, *n), None),
                ListKind::FixedSizeList(_) => return Err(Anomaly::incorrect("rtdb/list-size", "Fixed-size list length must be nonnegative")),
                ListKind::TimestampTzRange => (DataType::FixedSizeList(child, 2), Some("xt/tstz-range")),
                ListKind::Map { sorted } => {
                    validate_map_entries(&child)?;
                    (DataType::Map(child, *sorted), None)
                }
            };
            Ok(with_extension(Field::new(name, data_type, false), extension))
        }
        LogicalType::Scalar(scalar) => {
            let (data_type, extension) = scalar_layout(scalar)?;
            let field = with_extension(Field::new(name, data_type, false), extension);
            match scalar { ScalarType::Decimal { precision, scale, bit_width } => crate::with_decimal_parameters(field, crate::DecimalParameters { precision: *precision, scale: *scale, bit_width: *bit_width }), _ => Ok(field) }
        }
    }
}

fn with_extension(field: Field, extension: Option<&str>) -> Field {
    match extension {
        None => field,
        Some(name) => field.with_metadata(HashMap::from([
            (EXTENSION_NAME_KEY.to_owned(), name.to_owned()),
            (EXTENSION_METADATA_KEY.to_owned(), String::new()),
        ])),
    }
}

pub(crate) fn arrow_unit(unit: TimeUnit) -> arrow_schema::TimeUnit {
    match unit {
        TimeUnit::Second => arrow_schema::TimeUnit::Second,
        TimeUnit::Millisecond => arrow_schema::TimeUnit::Millisecond,
        TimeUnit::Microsecond => arrow_schema::TimeUnit::Microsecond,
        TimeUnit::Nanosecond => arrow_schema::TimeUnit::Nanosecond,
    }
}

fn logical_unit(unit: &arrow_schema::TimeUnit) -> TimeUnit {
    match unit {
        arrow_schema::TimeUnit::Second => TimeUnit::Second,
        arrow_schema::TimeUnit::Millisecond => TimeUnit::Millisecond,
        arrow_schema::TimeUnit::Microsecond => TimeUnit::Microsecond,
        arrow_schema::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

fn scalar_layout(scalar: &ScalarType) -> Result<(DataType, Option<&'static str>)> {
    use ScalarType::*;
    Ok(match scalar {
        Bool => (DataType::Boolean, None),
        I8 => (DataType::Int8, None), I16 => (DataType::Int16, None),
        I32 => (DataType::Int32, None), I64 => (DataType::Int64, None),
        U8 => (DataType::UInt8, None), U16 => (DataType::UInt16, None),
        U32 => (DataType::UInt32, None), U64 => (DataType::UInt64, None),
        F16 => (DataType::Float16, None), F32 => (DataType::Float32, None), F64 => (DataType::Float64, None),
        Utf8 => (DataType::Utf8, None), Binary => (DataType::Binary, None),
        FixedSizeBinary(width) if *width >= 0 => (DataType::FixedSizeBinary(*width), None),
        FixedSizeBinary(_) => return Err(Anomaly::incorrect("rtdb/binary-width", "Fixed-size binary width must be nonnegative")),
        Decimal { precision, scale, bit_width: 32 } => (DataType::Decimal32(crate::decimal_storage_precision(*precision), crate::decimal_storage_scale(*scale)), None),
        Decimal { precision, scale, bit_width: 64 } => (DataType::Decimal64(crate::decimal_storage_precision(*precision), crate::decimal_storage_scale(*scale)), None),
        Decimal { precision, scale, bit_width: 128 } => (DataType::Decimal128(crate::decimal_storage_precision(*precision), crate::decimal_storage_scale(*scale)), None),
        Decimal { precision, scale, bit_width: 256 } => (DataType::Decimal256(crate::decimal_storage_precision(*precision), crate::decimal_storage_scale(*scale)), None),
        Decimal { .. } => return Err(Anomaly::unsupported("rtdb/decimal-width", "XTDB decimal storage requires 32, 64, 128, or 256 bits")),
        DateDay => (DataType::Date32, None), DateMilli => (DataType::Date64, None),
        Time { unit, bit_width: 32 } if matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) => (DataType::Time32(arrow_unit(*unit)), None),
        Time { unit, bit_width: 64 } if matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) => (DataType::Time64(arrow_unit(*unit)), None),
        Time { .. } => return Err(Anomaly::incorrect("rtdb/time-width", "Arrow time width must agree with its unit")),
        Timestamp { unit, timezone } => (DataType::Timestamp(arrow_unit(*unit), timezone.as_ref().map(|v| Arc::<str>::from(v.as_str()))), None),
        Duration(unit) => (DataType::Duration(arrow_unit(*unit)), None),
        Interval(unit) => (DataType::Interval(match unit {
            IntervalUnit::YearMonth => arrow_schema::IntervalUnit::YearMonth,
            IntervalUnit::DayTime => arrow_schema::IntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => arrow_schema::IntervalUnit::MonthDayNano,
        }), None),
        IntervalMonthDayMicro => (DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano), Some("xt/intervalDMMType")),
        Keyword => (DataType::Utf8, Some("xt/clj-keyword")),
        Uuid => (DataType::FixedSizeBinary(16), Some("uuid")),
        Uri => (DataType::Utf8, Some("uri")),
        Transit => (DataType::Binary, Some("xt/transit+msgpack")),
        Oid => (DataType::Int32, Some("xt/oid")),
        RegClass => (DataType::Int32, Some("xt/regclass")),
        RegProc => (DataType::Int32, Some("xt/regproc")),
    })
}

pub(crate) fn scalar_reader_accepts(scalar: &ScalarType, data_type: &DataType) -> bool {
    use DataType as D;
    use ScalarType as S;
    match (scalar, data_type) {
        (S::FixedSizeBinary(_), D::FixedSizeBinary(_)) => true,
        (S::Timestamp { unit, .. }, D::Timestamp(actual, _)) => arrow_unit(*unit) == *actual,
        (S::Time { unit, .. }, D::Time32(actual) | D::Time64(actual)) => arrow_unit(*unit) == *actual,
        (S::Decimal { bit_width: 32, .. }, D::Decimal32(..))
        | (S::Decimal { bit_width: 64, .. }, D::Decimal64(..))
        | (S::Decimal { bit_width: 128, .. }, D::Decimal128(..))
        | (S::Decimal { bit_width: 256, .. }, D::Decimal256(..)) => true,
        _ => scalar_layout(scalar).is_ok_and(|(expected, _)| expected == *data_type),
    }
}

fn validate_map_entries(field: &Field) -> Result<()> {
    match field.data_type() {
        DataType::Struct(children) if !field.is_nullable() && children.len() == 2 && children[0].name() == "key" && children[1].name() == "value" && !children[0].is_nullable() => Ok(()),
        _ => Err(Anomaly::incorrect("rtdb/map-entries", "Map entries must be a non-null struct with a non-null key followed by value")),
    }
}

/// Decodes the physical field without treating non-nullable Arrow Null as SQL NULL.
/// External metadata must pass the IPC or C Data ingress codec before this native-field API.
pub fn from_field(field: &Field) -> Result<LogicalType> {
    crate::decimal_field_parameters(field)?;
    let base = if let Some(extension) = field.metadata().get(EXTENSION_NAME_KEY) {
        let expected = match extension.as_str() {
            "xt/clj-keyword" => LogicalType::Scalar(ScalarType::Keyword),
            "uuid" => LogicalType::Scalar(ScalarType::Uuid),
            "uri" => LogicalType::Scalar(ScalarType::Uri),
            "xt/transit+msgpack" => LogicalType::Scalar(ScalarType::Transit),
            "xt/oid" => LogicalType::Scalar(ScalarType::Oid),
            "xt/regclass" => LogicalType::Scalar(ScalarType::RegClass),
            "xt/regproc" => LogicalType::Scalar(ScalarType::RegProc),
            "xt/intervalDMMType" => LogicalType::Scalar(ScalarType::IntervalMonthDayMicro),
            "set" => match field.data_type() {
                DataType::List(child) => LogicalType::List { kind: ListKind::Set, element: Box::new(from_field(child)?) },
                _ => return Err(extension_layout_error(extension)),
            },
            "xt/tstz-range" => match field.data_type() {
                DataType::FixedSizeList(child, 2) => LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(from_field(child)?) },
                _ => return Err(extension_layout_error(extension)),
            },
            _ => return Err(Anomaly::unsupported("rtdb/unknown-arrow-extension", format!("Unknown Arrow extension {extension}"))),
        };
        if matches!(&expected, LogicalType::Scalar(_)) && to_field(field.name(), &expected)?.data_type() != field.data_type() {
            return Err(extension_layout_error(extension));
        }
        expected
    } else {
        plain_field_type(field)?
    };
    let logical = if field.is_nullable() { base.maybe() } else { base };
    logical.validate()?;
    Ok(logical)
}

fn extension_layout_error(name: &str) -> Anomaly {
    Anomaly::incorrect("rtdb/extension-storage-type", format!("Arrow storage type disagrees with extension {name}"))
}

pub(crate) fn numeric_field_matches(field: &Field, logical: &LogicalType) -> Result<bool> {
    let DataType::Union(fields, UnionMode::Dense) = field.data_type() else {
        let ty = plain_field_type(field)?;
        return Ok(if field.is_nullable() && ty != LogicalType::Null {
            matches!(logical, LogicalType::Maybe(inner) if **inner == ty)
        } else {
            *logical == ty
        });
    };
    let (value, nullable) = match logical {
        LogicalType::Null => (None, true),
        LogicalType::Maybe(inner) => (Some(inner.as_ref()), true),
        LogicalType::Union(legs) => {
            let nullable = legs.contains(&LogicalType::Null);
            if legs.len() - usize::from(nullable) < 2 { return Ok(false); }
            (None, nullable)
        }
        ty => (Some(ty), false),
    };
    let mut seen_null = field.is_nullable() || fields.is_empty();
    let mut seen = [false; 129];
    let mut distinct = 0;
    for (_, child) in fields.iter() {
        let ty = plain_field_type(child)?;
        seen_null |= child.is_nullable();
        if ty == LogicalType::Null { continue; }
        let index = match logical {
            LogicalType::Union(legs) => legs.get_index_of(&ty),
            _ => (value == Some(&ty)).then_some(0),
        };
        let Some(present) = index.and_then(|index| seen.get_mut(index)) else { return Ok(false); };
        if !*present {
            *present = true;
            distinct += 1;
        }
    }
    let expected = match logical {
        LogicalType::Union(legs) => legs.len() - usize::from(nullable),
        _ => usize::from(value.is_some()),
    };
    Ok(nullable == seen_null && distinct == expected)
}

fn plain_field_type(field: &Field) -> Result<LogicalType> {
    use DataType::*;
    let scalar = match field.data_type() {
        Null => return Ok(if field.is_nullable() { LogicalType::Null } else { LogicalType::Nothing }),
        Boolean => ScalarType::Bool,
        Int8 => ScalarType::I8, Int16 => ScalarType::I16, Int32 => ScalarType::I32, Int64 => ScalarType::I64,
        UInt8 => ScalarType::U8, UInt16 => ScalarType::U16, UInt32 => ScalarType::U32, UInt64 => ScalarType::U64,
        Float16 => ScalarType::F16, Float32 => ScalarType::F32, Float64 => ScalarType::F64,
        Utf8 => ScalarType::Utf8, Binary => ScalarType::Binary, FixedSizeBinary(n) => ScalarType::FixedSizeBinary(*n),
        Decimal32(_, _) | Decimal64(_, _) | Decimal128(_, _) | Decimal256(_, _) => {
            let parameters = crate::decimal_field_parameters(field)?.expect("decimal field");
            ScalarType::Decimal { precision: parameters.precision, scale: parameters.scale, bit_width: parameters.bit_width }
        }
        Date32 => ScalarType::DateDay, Date64 => ScalarType::DateMilli,
        Time32(unit) => ScalarType::Time { unit: logical_unit(unit), bit_width: 32 },
        Time64(unit) => ScalarType::Time { unit: logical_unit(unit), bit_width: 64 },
        Timestamp(unit, timezone) => ScalarType::Timestamp { unit: logical_unit(unit), timezone: timezone.as_ref().map(|z| z.to_string()) },
        Duration(unit) => ScalarType::Duration(logical_unit(unit)),
        Interval(unit) => ScalarType::Interval(match unit {
            arrow_schema::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
            arrow_schema::IntervalUnit::DayTime => IntervalUnit::DayTime,
            arrow_schema::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        }),
        Struct(children) => {
            let mut fields = StructFields::new();
            for child in children {
                if fields.insert(child.name().clone(), from_field(child)?).is_some() {
                    return Err(Anomaly::incorrect("rtdb/duplicate-struct-field", format!("Duplicate struct field {}", child.name())));
                }
            }
            return Ok(LogicalType::Struct(fields));
        }
        List(child) => return Ok(LogicalType::List { kind: ListKind::List, element: Box::new(from_field(child)?) }),
        FixedSizeList(child, size) => return Ok(LogicalType::List { kind: ListKind::FixedSizeList(*size), element: Box::new(from_field(child)?) }),
        Map(child, sorted) => {
            validate_map_entries(child)?;
            return Ok(LogicalType::List { kind: ListKind::Map { sorted: *sorted }, element: Box::new(from_field(child)?) });
        }
        Union(children, UnionMode::Dense) => {
            let mut legs = OrderedSet::new();
            for (_, child) in children.iter() {
                let leg = from_field(child)?;
                if leg.unencoded() != leg {
                    return Err(Anomaly::unsupported("rtdb/encoded-union-leg", "Union legs must be plain at every depth"));
                }
                legs.extend(leg.legs());
            }
            return Ok(LogicalType::from_legs(legs));
        }
        RunEndEncoded(run_ends, values) => {
            let width = match run_ends.data_type() {
                Int16 => RunEndWidth::I16,
                Int32 => RunEndWidth::I32,
                Int64 => RunEndWidth::I64,
                _ => return Err(Anomaly::incorrect("rtdb/run-end-width", "Run ends must have type i16, i32, or i64")),
            };
            if run_ends.is_nullable() || run_ends.name() != "run_ends" || values.name() != "values" || field.is_nullable() {
                return Err(Anomaly::incorrect("rtdb/run-end-fields", "Run-end layout requires non-null run_ends and its values field"));
            }
            let value = from_field(values)?;
            if matches!(value, LogicalType::Null | LogicalType::Nothing | LogicalType::RunEndEncoded { .. }) {
                return Err(Anomaly::unsupported("rtdb/run-end-type", "Invalid run-end value type"));
            }
            return Ok(LogicalType::RunEndEncoded { width, value: Box::new(value) });
        }
        other => return Err(Anomaly::unsupported("rtdb/arrow-type", format!("Unsupported XTDB Arrow type: {other:?}"))),
    };
    Ok(LogicalType::Scalar(scalar))
}

fn unit_name(unit: TimeUnit) -> &'static str {
    match unit { TimeUnit::Second => "second", TimeUnit::Millisecond => "milli", TimeUnit::Microsecond => "micro", TimeUnit::Nanosecond => "nano" }
}

/// Returns the JVM ArrowTypes.toLeg spelling for a monomorphic logical type.
pub fn leg_name(logical: &LogicalType) -> Result<String> {
    let name = match logical {
        LogicalType::Null => "null".to_owned(),
        LogicalType::Struct(_) => "struct".to_owned(),
        LogicalType::List { kind, .. } => match kind {
            ListKind::List => "list".to_owned(), ListKind::Set => "set".to_owned(),
            ListKind::Map { sorted: true } => "map-sorted".to_owned(),
            ListKind::Map { sorted: false } => "map-unsorted".to_owned(),
            ListKind::FixedSizeList(n) => format!("fixed-size-list-{n}"),
            ListKind::TimestampTzRange => "tstz-range".to_owned(),
        },
        LogicalType::Scalar(scalar) => match scalar {
            ScalarType::Bool => "bool".to_owned(),
            ScalarType::I8 => "i8".to_owned(), ScalarType::I16 => "i16".to_owned(), ScalarType::I32 => "i32".to_owned(), ScalarType::I64 => "i64".to_owned(),
            ScalarType::U8 => "u8".to_owned(), ScalarType::U16 => "u16".to_owned(), ScalarType::U32 => "u32".to_owned(), ScalarType::U64 => "u64".to_owned(),
            ScalarType::F16 => "f16".to_owned(), ScalarType::F32 => "f32".to_owned(), ScalarType::F64 => "f64".to_owned(),
            ScalarType::Utf8 => "utf8".to_owned(), ScalarType::Binary => "binary".to_owned(),
            ScalarType::FixedSizeBinary(n) => format!("fixed-size-binary-{n}"),
            ScalarType::Decimal { precision, scale, bit_width } => format!("decimal-{precision}-{scale}-{bit_width}"),
            ScalarType::DateDay => "date-day".to_owned(), ScalarType::DateMilli => "date-milli".to_owned(),
            ScalarType::Time { unit, .. } => format!("time-local-{}", unit_name(*unit)),
            ScalarType::Timestamp { unit, timezone: None } => format!("timestamp-local-{}", unit_name(*unit)),
            ScalarType::Timestamp { unit, timezone: Some(zone) } => format!("timestamp-tz-{}-{}", unit_name(*unit), zone.to_lowercase().replace(['/', ':'], "_")),
            ScalarType::Duration(unit) => format!("duration-{}", unit_name(*unit)),
            ScalarType::Interval(unit) => format!("interval-{}", match unit { IntervalUnit::YearMonth => "year-month", IntervalUnit::DayTime => "day-time", IntervalUnit::MonthDayNano => "month-day-nano" }),
            ScalarType::IntervalMonthDayMicro => "interval-month-day-micro".to_owned(),
            ScalarType::Keyword => "keyword".to_owned(), ScalarType::Uuid => "uuid".to_owned(), ScalarType::Uri => "uri".to_owned(),
            ScalarType::Transit => "transit".to_owned(), ScalarType::Oid => "oid".to_owned(), ScalarType::RegClass => "regclass".to_owned(), ScalarType::RegProc => "regproc".to_owned(),
        },
        _ => return Err(Anomaly::incorrect("rtdb/non-monomorphic-leg", "Union leg naming requires a plain monomorphic type")),
    };
    Ok(name)
}
