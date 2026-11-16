use crate::{Edn, EdnKind, error};
use rtdb_plan::CastTarget;
use rtdb_types::{IntervalUnit, ListKind, LogicalType, Result, ScalarType as S, TimeUnit};

pub(crate) fn target(form: &Edn, options: Option<&Edn>) -> Result<CastTarget> {
    let EdnKind::Tagged { tag, value } = &form.kind else { return Err(error("xtdb/invalid-type", "Cast target requires #xt/type")); };
    if tag != "xt/type" { return Err(error("xtdb/invalid-type", "Cast target requires #xt/type")); }
    if options.is_some_and(|options| options.map().is_none() && !matches!(options.kind, EdnKind::Nil)) { return Err(error("xtql/malformed-cast", "Cast options must be a map")); }
    expression_target(data_type(value)?, options)
}

fn expression_target(data_type: LogicalType, options: Option<&Edn>) -> Result<CastTarget> {
    Ok(match data_type {
        LogicalType::Scalar(S::Decimal { .. }) => {
            let precision = options.and_then(|options| options.option("precision")).filter(|value| !matches!(value.kind, EdnKind::Nil)).map(integer).transpose()?
                .map(|precision| u64::try_from(precision).map_err(|_| error("xtdb.expression/unsupported-precision", "Decimal precision must be nonnegative"))).transpose()?;
            let scale = if precision.is_some() { options.and_then(|options| options.option("scale")).filter(|value| !matches!(value.kind, EdnKind::Nil)).map(integer).transpose()? } else { None };
            CastTarget::decimal(precision, scale)?
        }
        LogicalType::List { kind: ListKind::List, element } => CastTarget::List(Box::new(expression_target(*element, options)?)),
        data_type => CastTarget::fixed(data_type),
    })
}

fn integer(form: &Edn) -> Result<i64> {
    if let EdnKind::Integer(value) = form.kind { Ok(value) } else { Err(error("xtql/malformed-cast", "Type and cast numeric options require integers")) }
}

fn unit(form: &Edn) -> Result<TimeUnit> {
    match form.keyword() {
        Some("second") => Ok(TimeUnit::Second), Some("milli") => Ok(TimeUnit::Millisecond),
        Some("micro") => Ok(TimeUnit::Microsecond), Some("nano") => Ok(TimeUnit::Nanosecond),
        _ => Err(error("xtdb/invalid-type", "Unknown temporal unit")),
    }
}

fn data_type(form: &Edn) -> Result<LogicalType> {
    if let EdnKind::Set(legs) = &form.kind { return Ok(LogicalType::from_legs(legs.iter().map(data_type).collect::<Result<Vec<_>>>()?)); }
    let (head, args) = match &form.kind {
        EdnKind::Keyword(head) => (head.as_str(), &[][..]),
        EdnKind::Vector(values) => {
            let (head, args) = values.split_first().ok_or_else(|| error("xtdb/invalid-type", "Empty type specification"))?;
            (head.keyword().ok_or_else(|| error("xtdb/invalid-type", "Type specification requires a keyword"))?, args)
        }
        _ => return Err(error("xtdb/invalid-type", "Type specification requires a keyword, vector, or set")),
    };
    let arg = |index| args.get(index).ok_or_else(|| error("xtdb/invalid-type", "Type specification is missing an argument"));
    let scalar = match head {
        "null" => return Ok(LogicalType::Null), "nothing" => return Ok(LogicalType::Nothing),
        "?" => {
            let inner = if args.len() == 1 { data_type(arg(0)?)? } else { data_type(&Edn { span: form.span, kind: EdnKind::Vector(args.to_vec()) })? };
            return Ok(inner.maybe());
        }
        "list" | "set" => return Ok(LogicalType::List { kind: if head == "list" { ListKind::List } else { ListKind::Set }, element: Box::new(data_type(arg(0)?)?) }),
        "bool" => S::Bool, "i8" => S::I8, "i16" => S::I16, "i32" => S::I32, "i64" => S::I64,
        "f32" => S::F32, "f64" => S::F64, "utf8" => S::Utf8, "varbinary" => S::Binary,
        "keyword" => S::Keyword, "uuid" => S::Uuid, "uri" => S::Uri, "transit" => S::Transit,
        "oid" => S::Oid, "regclass" => S::RegClass, "regproc" => S::RegProc,
        "decimal" => S::Decimal {
            precision: u8::try_from(integer(arg(0)?)?).map_err(|_| error("xtdb/invalid-type", "Decimal precision exceeds native metadata"))?,
            scale: i32::try_from(integer(arg(1)?)?).map_err(|_| error("xtdb/invalid-type", "Decimal scale exceeds signed 32 bits"))?,
            bit_width: integer(arg(2)?)?.try_into().map_err(|_| error("xtdb/invalid-type", "Invalid decimal bit width"))?,
        },
        "date" => match arg(0)?.keyword() { Some("day") => S::DateDay, Some("milli") => S::DateMilli, _ => return Err(error("xtdb/invalid-type", "Unknown date unit")) },
        "timestamp-local" => S::Timestamp { unit: unit(arg(0)?)?, timezone: None },
        "instant" => S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) },
        "timestamp-tz" => {
            let EdnKind::String(timezone) = &arg(1)?.kind else { return Err(error("xtdb/invalid-type", "Timezone requires a string")); };
            S::Timestamp { unit: unit(arg(0)?)?, timezone: Some(timezone.clone()) }
        }
        "duration" => S::Duration(unit(arg(0)?)?),
        "time-local" => { let unit = unit(arg(0)?)?; S::Time { unit, bit_width: if unit >= TimeUnit::Microsecond { 64 } else { 32 } } }
        "interval" => match arg(0)?.keyword() {
            Some("year-month") => S::Interval(IntervalUnit::YearMonth), Some("day-time") => S::Interval(IntervalUnit::DayTime),
            Some("month-day-nano") => S::Interval(IntervalUnit::MonthDayNano), Some("month-day-micro") => S::IntervalMonthDayMicro,
            _ => return Err(error("xtdb/invalid-type", "Unknown interval unit")),
        },
        _ => return Err(error("xtdb/invalid-type", format!("Unsupported cast type {head}"))),
    };
    scalar.validate()?;
    Ok(LogicalType::Scalar(scalar))
}
