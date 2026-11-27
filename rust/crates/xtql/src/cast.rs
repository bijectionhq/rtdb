use crate::{Edn, EdnKind, error};
use rtdb_plan::CastTarget;
use rtdb_types::{IntervalUnit, ListKind, LogicalType, Result, RunEndWidth, ScalarType as S, TimeUnit};

pub(crate) fn target(form: &Edn, options: Option<&Edn>) -> Result<CastTarget> {
    let EdnKind::Tagged { tag, value } = &form.kind else { return Err(error("xtdb/invalid-type", "Cast target requires #xt/type")); };
    if tag != "xt/type" { return Err(error("xtdb/invalid-type", "Cast target requires #xt/type")); }
    if options.is_some_and(|options| options.map().is_none() && !matches!(options.kind, EdnKind::Nil)) { return Err(error("xtql/malformed-cast", "Cast options must be a map")); }
    expression_target(data_type(value)?, options)
}

fn expression_target(data_type: LogicalType, options: Option<&Edn>) -> Result<CastTarget> {
    let requested = (matches!(data_type, LogicalType::Maybe(_) | LogicalType::Union(_)) || data_type != data_type.unencoded()).then(|| data_type.clone());
    let dispatch = match data_type { LogicalType::RunEndEncoded { value, .. } => *value, value => value };
    let target = match dispatch {
        LogicalType::Scalar(S::Decimal { .. }) => {
            let precision = options.and_then(|options| options.option("precision")).filter(|value| !matches!(value.kind, EdnKind::Nil)).map(integer).transpose()?;
            let scale = if precision.is_some() { options.and_then(|options| options.option("scale")).filter(|value| !matches!(value.kind, EdnKind::Nil)).map(integer).transpose()? } else { None };
            CastTarget::decimal(precision, scale)?
        }
        LogicalType::List { kind: ListKind::List, element } => CastTarget::List(Box::new(expression_target(*element, options)?)),
        LogicalType::Scalar(scalar @ (S::Time { .. } | S::Timestamp { .. } | S::Duration(_))) => {
            use rtdb_plan::{CastTimezone, DeferredCast, TemporalKind};
            let precision = cast_option(options.and_then(|options| options.option("precision")))?;
            let (kind, unit) = match scalar {
                S::Time { unit, .. } => (TemporalKind::Time, unit),
                S::Timestamp { unit, timezone: None } => (TemporalKind::TimestampLocal, unit),
                S::Timestamp { unit, timezone: Some(zone) } => (TemporalKind::TimestampZone(CastTimezone::Explicit(zone)), unit),
                S::Duration(unit) => (TemporalKind::Duration, unit),
                _ => unreachable!(),
            };
            CastTarget::Deferred(DeferredCast::Temporal { kind, unit, precision })
        }
        LogicalType::Scalar(S::Interval(_) | S::IntervalMonthDayMicro) => interval_options(options)?,
        data_type => CastTarget::fixed(data_type),
    };
    Ok(match requested { Some(requested) => CastTarget::Checked { requested, target: Box::new(target) }, None => target })
}

pub(crate) fn interval_options(options: Option<&Edn>) -> Result<CastTarget> {
    let Some(options) = options.filter(|options| !matches!(options.kind, EdnKind::Nil)) else { return Ok(CastTarget::IntervalDefault); };
    if options.map().is_none() { return Err(error("xtql/malformed-cast", "Cast options must be a map")); }
    if options.map().is_some_and(|entries| entries.is_empty()) { return Ok(CastTarget::IntervalEmptyOptions); }
    Ok(CastTarget::Deferred(rtdb_plan::DeferredCast::Interval {
        start: cast_option(options.option("start-field"))?, end: cast_option(options.option("end-field"))?,
        leading: cast_option(options.option("leading-precision"))?, fractional: cast_option(options.option("fractional-precision"))?,
    }))
}

pub(crate) fn interval_alias(options: Option<&Edn>) -> Result<CastTarget> {
    let target = interval_options(options)?;
    if let CastTarget::Deferred(rtdb_plan::DeferredCast::Interval { start, end, fractional, .. }) = &target {
        let year_month = matches!(start.string(), Some("YEAR" | "MONTH")) && (matches!(end, rtdb_plan::CastOption::Null) || end.string() == Some("MONTH"));
        if !year_month && fractional.truthy() { fractional.checked_long()?; }
    }
    Ok(target)
}

pub(crate) fn session_timestamp(options: Option<&Edn>) -> Result<CastTarget> {
    use rtdb_plan::{CastTimezone,DeferredCast,TemporalKind};
    if options.is_some_and(|options| options.map().is_none() && !matches!(options.kind, EdnKind::Nil)) { return Err(error("xtql/malformed-cast", "Cast options must be a map")); }
    let unit = options.and_then(|options| options.option("unit")).map(unit).transpose()?.unwrap_or(TimeUnit::Microsecond);
    let precision = cast_option(options.and_then(|options| options.option("precision")))?;
    Ok(CastTarget::Deferred(DeferredCast::Temporal { kind: TemporalKind::TimestampZone(CastTimezone::Session), unit, precision }))
}

fn cast_option(form: Option<&Edn>) -> Result<rtdb_plan::CastOption> {
    use rtdb_plan::CastOption as O;
    let Some(form) = form else { return Ok(O::Null); };
    let double_long = |number: f64| (number >= i64::MIN as f64 && number <= i64::MAX as f64 || number.is_nan()).then_some(number as i64);
    Ok(match &form.kind {
        EdnKind::Nil => O::Null, EdnKind::Bool(value) => O::Boolean(*value), EdnKind::Integer(value) => O::Integer(*value),
        EdnKind::String(value) => O::String(value.clone()),
        EdnKind::Float(value) => O::Numeric { primitive: *value as i64, checked: double_long(*value), form: field_text(form, true)? },
        EdnKind::Decimal(value) => O::Numeric { primitive: decimal_long(value)?, checked: value.parse::<f64>().ok().and_then(double_long), form: format!("{value}M") },
        EdnKind::BigInteger(value) => O::Numeric { primitive: wrapping_decimal_integer(value), checked: value.parse::<i64>().ok(), form: format!("{value}N") },
        EdnKind::Character(value) => O::Character { value: *value as u32, form: field_text(form,true)? },
        _ => O::Other(option_text(form)?),
    })
}

fn wrapping_decimal_integer(text: &str) -> i64 {
    let (negative,digits) = text.strip_prefix('-').map_or((false,text),|digits| (true,digits));
    let value = digits.bytes().fold(0u64,|value,digit| value.wrapping_mul(10).wrapping_add(u64::from(digit - b'0')));
    if negative { value.wrapping_neg() as i64 } else { value as i64 }
}

fn decimal_long(text: &str) -> Result<i64> {
    use std::str::FromStr;
    let value = bigdecimal::BigDecimal::from_str(text).map_err(|_| error("xtql/malformed-cast","Invalid decimal option"))?;
    let (coefficient,scale) = value.as_bigint_and_exponent();
    let digits = coefficient.to_string();
    if scale > 0 {
        let count = digits.trim_start_matches('-').len();
        if scale as u64 >= count as u64 { return Ok(0); }
        return Ok(wrapping_decimal_integer(&digits[..digits.len() - scale as usize]));
    }
    if scale <= -64 { return Ok(0); }
    Ok(wrapping_decimal_integer(&digits).wrapping_mul(10i64.wrapping_pow((-scale) as u32)))
}

fn option_text(form: &Edn) -> Result<String> {
    match &form.kind {
        EdnKind::Tagged { tag, value } => Ok(format!("#{tag} {}", option_text(value)?)),
        EdnKind::AnonymousFunction(value) => Ok(format!("#{}", option_text(value)?)),
        EdnKind::Vector(values) | EdnKind::List(values) | EdnKind::Set(values) => {
            let (open,close) = match &form.kind { EdnKind::Vector(_) => ("[","]"), EdnKind::List(_) => ("(",")"), _ => ("#{","}") };
            Ok(format!("{open}{}{close}",values.iter().map(option_text).collect::<Result<Vec<_>>>()?.join(" ")))
        }
        EdnKind::Map(fields) => Ok(format!("{{{}}}", fields.iter().map(|(key,value)| Ok(format!("{} {}", option_text(key)?,option_text(value)?))).collect::<Result<Vec<_>>>()?.join(", "))),
        _ => field_text(form,true),
    }
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

pub(crate) fn data_type(form: &Edn) -> Result<LogicalType> {
    if let EdnKind::Tagged { tag, value } = &form.kind {
        return if tag == "xt/type" { data_type(value) } else { Err(error("xtdb/invalid-type", "Nested type tags require #xt/type")) };
    }
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
            let inner = data_type(&Edn { span: form.span, kind: EdnKind::Vector(args.to_vec()) })?;
            return Ok(inner.maybe());
        }
        "list" | "set" => return Ok(LogicalType::List { kind: if head == "list" { ListKind::List } else { ListKind::Set }, element: Box::new(data_type(arg(0)?)?) }),
        "struct" => {
            let fields = match args.first().map(|value| &value.kind) {
                None | Some(EdnKind::Nil) => &[][..],
                Some(EdnKind::Map(fields)) => fields.as_slice(),
                _ => return Err(error("xtdb/invalid-type", "Struct fields require a map")),
            };
            return Ok(LogicalType::Struct(fields.iter().map(|(name, value)| Ok((field_name(name)?, data_type(value)?))).collect::<Result<_>>()?));
        }
        "map" => {
            let element = args.last().ok_or_else(|| error("xtdb/invalid-type", "Map type requires its entry type"))?;
            let sorted = args.len() > 1 && args[0].option("sorted?").is_some_and(|value| !matches!(value.kind, EdnKind::Nil | EdnKind::Bool(false)));
            return Ok(LogicalType::List { kind: ListKind::Map { sorted }, element: Box::new(data_type(element)?) });
        }
        "ree" | "ree-i16" | "ree-i64" => return data_type(arg(0)?)?.run_end_encoded(match head { "ree-i16" => RunEndWidth::I16, "ree-i64" => RunEndWidth::I64, _ => RunEndWidth::I32 }),
        "tstz-range" => return Ok(LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(LogicalType::Scalar(S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) })) }),
        "bool" => S::Bool, "i8" => S::I8, "i16" => S::I16, "i32" => S::I32, "i64" => S::I64,
        "f32" => S::F32, "f64" => S::F64, "utf8" => S::Utf8, "varbinary" => S::Binary,
        "keyword" => S::Keyword, "uuid" => S::Uuid, "uri" => S::Uri, "transit" => S::Transit,
        "oid" => S::Oid, "regclass" => S::RegClass, "regproc" => S::RegProc,
        "iid" => S::FixedSizeBinary(16),
        "fixed-size-binary" => S::FixedSizeBinary(i32::try_from(integer(arg(0)?)?).map_err(|_| error("xtdb/invalid-type", "Fixed binary width exceeds signed 32 bits"))?),
        "decimal" => S::Decimal {
            precision: i32::try_from(integer(arg(0)?)?).map_err(|_| error("xtdb/invalid-type", "Decimal precision exceeds signed 32 bits"))?,
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

pub(crate) fn field_name(form: &Edn) -> Result<String> {
    field_text(form, false)
}

fn field_text(form: &Edn, readable: bool) -> Result<String> {
    Ok(match &form.kind {
        EdnKind::String(value) if readable => {
            let mut result = String::from("\"");
            for ch in value.chars() { match ch {
                '\"' => result.push_str("\\\""), '\\' => result.push_str("\\\\"), '\n' => result.push_str("\\n"), '\r' => result.push_str("\\r"),
                '\t' => result.push_str("\\t"), '\u{8}' => result.push_str("\\b"), '\u{c}' => result.push_str("\\f"), ch => result.push(ch),
            } }
            result.push('\"'); result
        }
        EdnKind::String(value) | EdnKind::Symbol(value) => value.clone(),
        EdnKind::Keyword(value) => format!(":{value}"),
        EdnKind::Integer(value) => value.to_string(),
        EdnKind::Float(value) => match () {
            () if readable && value.is_nan() => "##NaN".into(),
            () if readable && *value == f64::INFINITY => "##Inf".into(),
            () if readable && *value == f64::NEG_INFINITY => "##-Inf".into(),
            () => rtdb_types::java_f64_text(*value),
        },
        EdnKind::BigInteger(value) => {
            use std::str::FromStr;
            let mut text = num_bigint::BigInt::from_str(value).map_err(|_| error("xtdb/invalid-type", "Invalid integer field name"))?.to_string();
            if readable { text.push('N'); } text
        }
        EdnKind::Decimal(value) => {
            let mut text = decimal_field_text(value)?;
            if readable { text.push('M'); } text
        }
        EdnKind::Bool(value) => value.to_string(),
        EdnKind::Nil => if readable { "nil".into() } else { String::new() },
        EdnKind::Character(value) if readable => match value {
            '\n' => "\\newline".into(), ' ' => "\\space".into(), '\t' => "\\tab".into(), '\r' => "\\return".into(),
            '\u{8}' => "\\backspace".into(), '\u{c}' => "\\formfeed".into(), ch => format!("\\{ch}"),
        },
        EdnKind::Character(value) => value.to_string(),
        EdnKind::Vector(values) | EdnKind::List(values) | EdnKind::Set(values) => {
            let (open, close) = match &form.kind { EdnKind::Vector(_) => ("[", "]"), EdnKind::List(_) => ("(", ")"), _ => ("#{", "}") };
            format!("{open}{}{close}", values.iter().map(|value| field_text(value, true)).collect::<Result<Vec<_>>>()?.join(" "))
        }
        EdnKind::Map(fields) => format!("{{{}}}", fields.iter().map(|(key, value)| Ok(format!("{} {}", field_text(key, true)?, field_text(value, true)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        _ => return Err(error("xtdb/invalid-type", "Object-valued struct field names require their reader-specific string representation")),
    })
}

fn decimal_field_text(text: &str) -> Result<String> {
    use std::str::FromStr;
    let value = bigdecimal::BigDecimal::from_str(text).map_err(|_| error("xtdb/invalid-type", "Invalid decimal field name"))?;
    let (coefficient, scale) = value.as_bigint_and_exponent();
    let raw = coefficient.to_string();
    let (sign, digits) = raw.strip_prefix('-').map_or(("", raw.as_str()), |digits| ("-", digits));
    let exponent = i64::try_from(digits.len()).map_err(|_| error("xtdb/invalid-type", "Decimal field name exceeds addressable memory"))? - 1 - scale;
    if scale < 0 || exponent < -6 {
        let fraction = if digits.len() > 1 { format!(".{}", &digits[1..]) } else { String::new() };
        return Ok(format!("{sign}{}{fraction}E{}{exponent}", &digits[..1], if exponent >= 0 { "+" } else { "" }));
    }
    let point = digits.len() as i64 - scale;
    if point <= 0 { Ok(format!("{sign}0.{}{digits}", "0".repeat((-point) as usize))) }
    else if scale == 0 { Ok(raw) }
    else { Ok(format!("{sign}{}.{}", &digits[..point as usize], &digits[point as usize..])) }
}

#[cfg(test)]
mod tests {
    use rtdb_types::StructFields;
    use super::*;
    

    fn parse(source: &str) -> LogicalType { data_type(&crate::read(source).unwrap()).unwrap() }

    #[test]
    fn nested_tagged_struct_and_map_types_preserve_names_nullability_and_sort_metadata() {
        let element = LogicalType::Struct(StructFields::from([
            ("key".into(), LogicalType::Scalar(S::Utf8)), ("value".into(), LogicalType::Scalar(S::I64).maybe()),
        ]));
        assert_eq!(parse("[:map [:struct {key :utf8 \"value\" [:? :i64]}]]"), LogicalType::List { kind: ListKind::Map { sorted: false }, element: Box::new(element.clone()) });
        assert_eq!(parse("[:map {:sorted? :truthy} #xt/type [:struct {\"key\" #xt/type :utf8 \"value\" [:? :i64]}]]"), LogicalType::List { kind: ListKind::Map { sorted: true }, element: Box::new(element) });
        assert_eq!(parse("[:struct {:named :bool nil :null 12 :i32}]"), LogicalType::Struct(StructFields::from([
            (":named".into(), LogicalType::Scalar(S::Bool)), ("".into(), LogicalType::Null), ("12".into(), LogicalType::Scalar(S::I32)),
        ])));
    }

    #[test]
    fn type_forms_retain_run_end_child_layouts_and_outer_width_replacement() {
        let child = LogicalType::Scalar(S::I64).maybe().run_end_encoded(RunEndWidth::I16).unwrap();
        assert_eq!(parse("[:ree-i64 [:ree [:list [:ree-i16 [:? :i64]]]]]"), LogicalType::list(child).run_end_encoded(RunEndWidth::I64).unwrap());
        assert_eq!(parse("[:? :ree-i16 :i64]"), LogicalType::Scalar(S::I64).maybe().run_end_encoded(RunEndWidth::I16).unwrap());
        for source in ["[:ree :null]", "[:ree :nothing]", "[:? [:list :i64]]", "[:fixed-size-list 2 :i64]"] {
            assert!(data_type(&crate::read(source).unwrap()).is_err(), "{source}");
        }
    }

    #[test]
    fn fixed_binary_and_range_aliases_keep_reference_types() {
        assert_eq!(parse(":iid"), parse("[:fixed-size-binary 16]"));
        assert_eq!(parse(":tstz-range"), parse("[:tstz-range]"));
        assert_eq!(parse(":tstz-range"), LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(parse(":instant")) });
        for source in ["[:fixed-size-binary -1]", "[:fixed-size-binary 2147483648]", "[:map]"] {
            assert!(data_type(&crate::read(source).unwrap()).is_err(), "{source}");
        }
    }

    #[test]
    fn struct_field_key_printing_retains_edn_readable_children_and_java_numeric_text() {
        for (source, expected) in [
            ("nil", ""), ("1.0", "1.0"), ("1.00M", "1.00"), ("1E+9M", "1E+9"), ("0.0000001M", "1E-7"),
            ("[nil \"x\\n\" 12N 1.00M ##Inf]", "[nil \"x\\n\" 12N 1.00M ##Inf]"),
            ("{:a [1 2] \"b\" true}", "{:a [1 2], \"b\" true}"),
            ("012N", "10"), ("0xFFN", "255"), ("-16rFFN", "-255"), ("+2R101N", "5"),
            ("[012N 0xFFN -16rFFN]", "[10N 255N -255N]"),
        ] { assert_eq!(field_name(&crate::read(source).unwrap()).unwrap(), expected, "{source}"); }
        assert_eq!(parse("[:struct {012N :i64 [0xFFN] :bool}]"), LogicalType::Struct(StructFields::from([
            ("10".into(), LogicalType::Scalar(S::I64)), ("[255N]".into(), LogicalType::Scalar(S::Bool)),
        ])));
    }
}
