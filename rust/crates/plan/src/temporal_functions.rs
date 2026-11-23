use rtdb_types::StructFields;
use crate::{BinaryOp, Expr};
use rtdb_types::{Anomaly, IntervalUnit, LogicalType as L, Result, ScalarType as S, TimeUnit, Value, ZoneId};
use rtdb_types::transit_value::TransitValue as T;

/// A parsed EXTRACT or DATE_TRUNC field; applicability also depends on the function and source type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemporalField { Millennium, Century, Decade, Year, Quarter, Month, Week, Day, Hour, Minute, Second, Millisecond, Microsecond, Dow, IsoDow, Doy, TimezoneHour, TimezoneMinute, Epoch }
impl TemporalField {
    /// Parses the case-sensitive expression field spelling; SQL normalizes its grammar fields before calling this.
    /// Unknown fields return `xtdb.expression/invalid-temporal-field`.
    pub fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "MILLENNIUM" => Self::Millennium,"CENTURY" => Self::Century,"DECADE" => Self::Decade,"YEAR" => Self::Year,"QUARTER" => Self::Quarter,
            "MONTH" => Self::Month,"WEEK" => Self::Week,"DAY" => Self::Day,"HOUR" => Self::Hour,"MINUTE" => Self::Minute,"SECOND" => Self::Second,
            "MILLISECOND" => Self::Millisecond,"MICROSECOND" => Self::Microsecond,"DOW" => Self::Dow,"ISODOW" => Self::IsoDow,"DOY" => Self::Doy,
            "TIMEZONE_HOUR" => Self::TimezoneHour,"TIMEZONE_MINUTE" => Self::TimezoneMinute,"EPOCH" => Self::Epoch,
            _ => return Err(Anomaly::incorrect("xtdb.expression/invalid-temporal-field",format!("Unsupported temporal field: {value}"))),
        })
    }
    /// Tests field applicability after the caller has validated the function's argument types.
    /// `name` is the canonical lowercase `extract` or `date_trunc` name.
    pub fn supports(self, name: &str, source: &S) -> bool {
        use TemporalField::*;
        let date = matches!(self,Year | Quarter | Month | Week | Day | Dow | IsoDow | Doy);
        let clock = matches!(self,Hour | Minute | Second);
        if name == "date_trunc" { return matches!(self,Millennium | Century | Decade | Year | Quarter | Month | Week | Day | Hour | Minute | Second | Millisecond | Microsecond); }
        match source {
            S::DateDay | S::DateMilli => date,
            S::Timestamp { timezone,.. } => date || clock || timezone.is_some() && matches!(self,TimezoneHour | TimezoneMinute | Epoch),
            S::Time { .. } => clock,
            S::Interval(_) | S::IntervalMonthDayMicro => matches!(self,Year | Month | Day | Hour | Minute | Second),
            _ => false,
        }
    }

    pub fn validate_for(self, name: &str, source: &S) -> Result<()> {
        if self.supports(name, source) { return Ok(()); }
        if name == "extract" {
            let source_name = match source {
                S::DateDay | S::DateMilli => Some(("date", "date")),
                S::Timestamp { timezone: None, .. } => Some(("timestamp without timezone", "timestamp-local")),
                S::Time { .. } => Some(("time without timezone", "time-local")),
                S::Interval(_) | S::IntervalMonthDayMicro => Some(("interval", "interval")),
                _ => None,
            };
            if let Some((description, keyword)) = source_name {
                let field = self.sql_name();
                let field_data = match (source, self) {
                    (S::Timestamp { timezone: None, .. }, Self::TimezoneHour) => T::Keyword("timezone-hour".into()),
                    (S::Timestamp { timezone: None, .. }, Self::TimezoneMinute) => T::Keyword("timezone-minute".into()),
                    (S::Timestamp { timezone: None, .. }, Self::Epoch) => T::Keyword("epoch".into()),
                    _ => T::String(field.into()),
                };
                return Err(Anomaly::unsupported("xtdb.expression/extract-not-supported", format!("Extract \"{field}\" not supported for type {description}"))
                    .with_transit_context("field", &field_data)?
                    .with_transit_context("source-type", &T::Keyword(keyword.into()))?);
            }
        }
        Err(Anomaly::unsupported("xtdb.expression/extract-not-supported", format!("{self:?} is not supported for {source:?}")))
    }

    fn sql_name(self) -> &'static str {
        match self {
            Self::Millennium => "MILLENNIUM", Self::Century => "CENTURY", Self::Decade => "DECADE",
            Self::Year => "YEAR", Self::Quarter => "QUARTER", Self::Month => "MONTH", Self::Week => "WEEK",
            Self::Day => "DAY", Self::Hour => "HOUR", Self::Minute => "MINUTE", Self::Second => "SECOND",
            Self::Millisecond => "MILLISECOND", Self::Microsecond => "MICROSECOND",
            Self::Dow => "DOW", Self::IsoDow => "ISODOW", Self::Doy => "DOY",
            Self::TimezoneHour => "TIMEZONE_HOUR", Self::TimezoneMinute => "TIMEZONE_MINUTE", Self::Epoch => "EPOCH",
        }
    }
}

pub(crate) fn canonical(name: &str) -> &str {
    match name { "now" => "current_timestamp","localdate" => "local_date","localtime" => "local_time","localtimestamp" => "local_timestamp",other => other }
}
pub(crate) fn clock(name: &str) -> bool { matches!(canonical(name),"current_timestamp" | "current_time" | "current_date" | "current_timezone" | "local_timestamp" | "local_time" | "local_date") }
fn signed(ty: &L) -> bool { matches!(ty,L::Scalar(S::I8 | S::I16 | S::I32 | S::I64)) }
fn date_time(ty: &S) -> bool { matches!(ty,S::DateDay | S::DateMilli | S::Timestamp {..}) }
fn interval(ty: &S) -> bool { matches!(ty,S::Interval(_) | S::IntervalMonthDayMicro) }
fn mismatch(name: &str) -> Anomaly { Anomaly::incorrect("xtdb.expression/function-type-mismatch",format!("{name} not applicable to argument types")) }
fn arity(name: &str) -> Anomaly { Anomaly::incorrect("xtdb/invalid-function-arity",format!("Wrong argument count for {name}")) }

/// Returns RANGE_BINS cell metadata: microsecond UTC `_from`/`_to` and double `_weight`.
pub fn range_bin_element_type() -> L {
    L::Struct(StructFields::from([
        ("_from".into(),L::Scalar(S::Timestamp { unit: TimeUnit::Microsecond,timezone: Some("UTC".into()) })),
        ("_to".into(),L::Scalar(S::Timestamp { unit: TimeUnit::Microsecond,timezone: Some("UTC".into()) })),
        ("_weight".into(),L::Scalar(S::F64)),
    ]))
}
/// Binds CURRENT_TIMESTAMP's result zone for a query, or retains the session capability for a control expression.
/// Other expressions are returned unchanged; invalid explicit zones return the zone parser's anomaly.
pub fn bind_clock_timezone(expr: Expr, timezone: Option<&str>) -> Result<Expr> {
    let Expr::Call { name,return_type,.. } = &expr else { return Ok(expr); };
    let name = name.to_lowercase().replace('-',"_");
    if canonical(&name) != "current_timestamp" { return Ok(expr); }
    let L::Scalar(S::Timestamp { unit,.. }) = return_type else { return Ok(expr); };
    let zone = match timezone { Some(zone) => { rtdb_types::ZoneId::parse(zone)?;crate::CastTimezone::Explicit(zone.into()) },None => crate::CastTimezone::Session };
    let target = crate::TemporalCast { kind: crate::TemporalKind::TimestampZone(zone),unit: *unit,precision: None };
    Ok(Expr::Cast { expr: Box::new(expr),target: crate::CastTarget::Temporal(target) })
}
pub fn truncation_zone(timezone: &str) -> Result<ZoneId> {
    ZoneId::parse(timezone).map_err(|mut error| {
        error.code = "xtdb.expression/invalid-timezone".into();
        error.with_context("timezone", timezone.into())
    })
}

pub(crate) fn signature(name: &str,args: &[L]) -> Option<Result<L>> {
    let name = canonical(name);
    if !clock(name) && !matches!(name,"extract" | "date_trunc" | "age" | "date_bin" | "range_bins") { return None; }
    Some((|| {
        if clock(name) {
            let precisions = matches!(name,"current_timestamp" | "current_time" | "local_timestamp" | "local_time");
            if args.len() > usize::from(precisions) { return Err(arity(name)); }
            if let Some(ty) = args.first() {
                if ty.legs().iter().any(|ty| *ty != L::Null && !signed(ty)) { return Err(mismatch(name)); }
                if *ty == L::Null { return Ok(L::Null); }
            }
            return Ok(clock_type(name,6));
        }
        let valid = match name { "extract" | "age" => args.len() == 2,"date_trunc" | "date_bin" => (2..=3).contains(&args.len()),_ => (3..=4).contains(&args.len()) };
        if !valid { return Err(arity(name)); }
        if name == "date_bin" {
            let origin = args.get(2).cloned().unwrap_or(L::Scalar(S::Timestamp { unit: TimeUnit::Microsecond,timezone: Some("UTC".into()) }));
            let difference = arithmetic(BinaryOp::Subtract,&args[1],&origin)?;
            let quotient = arithmetic(BinaryOp::Divide,&difference,&args[0])?;
            return arithmetic(BinaryOp::Add,&origin,&arithmetic(BinaryOp::Multiply,&args[0],&quotient)?);
        }
        let mut out = L::Nothing;
        combinations(args,&mut Vec::with_capacity(args.len()),&mut |row| {
            if row.contains(&L::Null) { out = out.join(&L::Null);return Ok(()); }
            let ty = match name {
                "extract" | "date_trunc" => {
                    if row[0] != L::Scalar(S::Utf8) { return Err(mismatch(name)); }
                    let L::Scalar(source) = &row[1] else { return Err(mismatch(name)); };
                    if !(date_time(source) || interval(source) || name == "extract" && matches!(source,S::Time {..})) { return Err(mismatch(name)); }
                    if row.len() == 3 && (!matches!(source,S::Timestamp { timezone: Some(_),.. }) || row[2] != L::Scalar(S::Utf8)) { return Err(mismatch(name)); }
                    if name == "extract" { L::Scalar(S::I32) } else { row[1].clone() }
                }
                "age" => {
                    if !row.iter().all(|ty| matches!(ty,L::Scalar(source) if date_time(source))) { return Err(mismatch(name)); }
                    L::Scalar(if row.iter().any(|ty| matches!(ty,L::Scalar(S::Timestamp { unit: TimeUnit::Nanosecond,.. }))) { S::Interval(IntervalUnit::MonthDayNano) } else { S::IntervalMonthDayMicro })
                }
                _ => {
                    if !matches!(&row[0],L::Scalar(source) if interval(source)) || !row[1..].iter().all(|ty|matches!(ty,L::Scalar(source) if date_time(source))) { return Err(mismatch(name)); }
                    crate::TemporalCast { kind: crate::TemporalKind::Duration,unit: TimeUnit::Microsecond,precision: None }.validate_source(&row[0])?;
                    if row[1..].iter().all(|ty|matches!(ty,L::Scalar(S::Timestamp { timezone: Some(_),.. }))) && row[1..].iter().any(|ty|!matches!(ty,L::Scalar(S::Timestamp { unit: TimeUnit::Microsecond,.. }))) {
                        return Err(Anomaly::unsupported("xtdb.expression/range-bins-unit","Zoned RANGE_BINS inputs require microsecond units"));
                    }
                    L::list(range_bin_element_type())
                }
            };
            out = out.join(&ty);
            Ok(())
        })?;
        Ok(out)
    })())
}
fn arithmetic(op: BinaryOp,left: &L,right: &L) -> Result<L> {
    let mut out = L::Nothing;
    let numeric = |ty: &S| matches!(ty,S::I8 | S::I16 | S::I32 | S::I64 | S::F32 | S::F64 | S::Decimal {..});
    for left in left.legs() { for right in right.legs() {
        if left == L::Null || right == L::Null { out = out.join(&L::Null);continue; }
        let (L::Scalar(a),L::Scalar(b)) = (&left,&right) else { return Err(mismatch("date_bin")); };
        if crate::temporal_arithmetic_type(op,a,b).is_none() && !(numeric(a) && numeric(b)) { return Err(mismatch("date_bin")); }
        if op == BinaryOp::Divide && matches!(a,S::Duration(_)) && interval(b) {
            crate::TemporalCast { kind: crate::TemporalKind::Duration,unit: TimeUnit::Microsecond,precision: None }.validate_source(&right)?;
        }
        out = out.join(&crate::arithmetic_type(op,&left,&right));
    } }
    Ok(out)
}
fn combinations(args: &[L],row: &mut Vec<L>,visit: &mut impl FnMut(&[L])->Result<()>) -> Result<()> {
    let Some((head,tail)) = args.split_first() else { return visit(row); };
    for leg in head.legs() { row.push(leg);combinations(tail,row,visit)?;row.pop(); }
    Ok(())
}
pub(crate) fn with_args(name: &str,args: &[Expr],base: L) -> Result<L> {
    let name = name.to_lowercase().replace('-',"_");let name = canonical(&name);
    if clock(name) {
        if base == L::Null { return Ok(base); }
        let precision = args.first().map(literal_precision).transpose()?.unwrap_or(6);
        return Ok(clock_type(name,precision));
    }
    if matches!(name,"extract" | "date_trunc") {
        if args.iter().any(|arg|arg.data_type() == L::Null) { return Ok(base); }
        let Expr::Literal(Value::Utf8(field)) = &args[0] else { return Err(Anomaly::unsupported("xtdb.expression/non-literal-temporal-field","Temporal field must be a literal string")); };
        let field = TemporalField::parse(field)?;
        for leg in args[1].data_type().legs() {
            if let L::Scalar(source) = leg { field.validate_for(name, &source)?; }
        }
        if let Some(timezone) = args.get(2) {
            let Expr::Literal(Value::Utf8(timezone)) = timezone else { return Err(Anomaly::unsupported("xtdb.expression/non-literal-timezone","Truncation timezone must be a literal string")); };
            truncation_zone(timezone)?;
        }
    }
    Ok(base)
}
fn literal_precision(expr: &Expr) -> Result<u8> {
    let value = match expr {
        Expr::Literal(Value::I8(value)) => i64::from(*value),Expr::Literal(Value::I16(value)) => i64::from(*value),Expr::Literal(Value::I32(value)) => i64::from(*value),Expr::Literal(Value::I64(value)) => *value,
        Expr::Unary { op: crate::UnaryOp::Negate,expr } => match expr.as_ref() { Expr::Literal(Value::I64(value)) => value.checked_neg().ok_or_else(||mismatch("precision"))?,_ => return Err(nonliteral()) },
        _ => return Err(nonliteral()),
    };
    Ok(value.clamp(0,9) as u8)
}
fn nonliteral() -> Anomaly { Anomaly::unsupported("xtdb.expression/non-literal-precision","Clock precision must be a literal integer") }
fn clock_type(name: &str,precision: u8) -> L {
    let unit = match precision { 0 => TimeUnit::Second,1..=3 => TimeUnit::Millisecond,4..=6 => TimeUnit::Microsecond,_ => TimeUnit::Nanosecond };
    L::Scalar(match name {
        "current_date" | "local_date" => S::DateDay,"current_timezone" => S::Utf8,
        "current_time" | "local_time" => S::Time { unit,bit_width: if matches!(unit,TimeUnit::Second | TimeUnit::Millisecond) { 32 } else { 64 } },
        "local_timestamp" => S::Timestamp { unit,timezone: None },
        _ => S::Timestamp { unit,timezone: Some("UTC".into()) },
    })
}
