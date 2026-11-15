use crate::{Anomaly, IntervalUnit, ListKind, LogicalType, Result, RunEndWidth, ScalarType, TimeUnit};

impl LogicalType {
    /// EDN spelling used by `information_schema.columns.xt_type` and live column metadata.
    pub fn to_edn(&self) -> Result<String> {
        use LogicalType as L;
        use ScalarType as S;
        Ok(match self {
            L::Nothing => ":nothing".into(), L::Null => ":null".into(),
            L::Maybe(ty) => { let base = ty.to_edn()?; if let Some(base) = base.strip_prefix('[') { format!("[:? {base}") } else { format!("[:? {base}]") } }
            L::RunEndEncoded { width, value } => format!("[{} {}]", match width { RunEndWidth::I16 => ":ree-i16", RunEndWidth::I32 => ":ree", RunEndWidth::I64 => ":ree-i64" }, value.to_edn()?),
            L::Union(legs) => format!("#{{{}}}", legs.iter().map(Self::to_edn).collect::<Result<Vec<_>>>()?.join(" ")),
            L::Struct(fields) => format!("[:struct {{{}}}]", fields.iter().map(|(name, ty)| Ok(format!("{} {}", quoted(name)?, ty.to_edn()?))).collect::<Result<Vec<_>>>()?.join(", ")),
            L::List { kind, element } => match kind {
                ListKind::TimestampTzRange => ":tstz-range".into(),
                ListKind::List => format!("[:list {}]", element.to_edn()?),
                ListKind::Set => format!("[:set {}]", element.to_edn()?),
                ListKind::FixedSizeList(size) => format!("[:fixed-size-list {size} {}]", element.to_edn()?),
                ListKind::Map { sorted } => format!("[:map {{:sorted? {sorted}}} {}]", element.to_edn()?),
            },
            L::Scalar(ty) => match ty {
                S::Bool => ":bool".into(), S::I8 => ":i8".into(), S::I16 => ":i16".into(), S::I32 => ":i32".into(), S::I64 => ":i64".into(),
                S::F32 => ":f32".into(), S::F64 => ":f64".into(), S::Utf8 => ":utf8".into(), S::Binary => ":varbinary".into(),
                S::FixedSizeBinary(size) => format!("[:fixed-size-binary {size}]"),
                S::Decimal { precision, scale, bit_width } => format!("[:decimal {precision} {scale} {bit_width}]"),
                S::DateDay => "[:date :day]".into(), S::DateMilli => "[:date :milli]".into(),
                S::Time { unit, .. } => format!("[:time-local {}]", unit_keyword(*unit)),
                S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some(tz) } if tz == "UTC" => ":instant".into(),
                S::Timestamp { unit, timezone: Some(tz) } => format!("[:timestamp-tz {} {}]", unit_keyword(*unit), quoted(tz)?),
                S::Timestamp { unit, timezone: None } => format!("[:timestamp-local {}]", unit_keyword(*unit)),
                S::Duration(unit) => format!("[:duration {}]", unit_keyword(*unit)),
                S::Interval(unit) => format!("[:interval {}]", match unit { IntervalUnit::YearMonth => ":year-month", IntervalUnit::DayTime => ":day-time", IntervalUnit::MonthDayNano => ":month-day-nano" }),
                S::IntervalMonthDayMicro => "[:interval :month-day-micro]".into(),
                S::Keyword => ":keyword".into(), S::Uuid => ":uuid".into(), S::Uri => ":uri".into(), S::Transit => ":transit".into(),
                S::Oid => ":oid".into(), S::RegClass => ":regclass".into(), S::RegProc => ":regproc".into(),
                S::U8 | S::U16 | S::U32 | S::U64 | S::F16 => return Err(Anomaly::unsupported("xtdb/unsupported-type-rendering", "JVM type EDN does not represent this numeric type")),
            },
        })
    }
}
fn unit_keyword(unit: TimeUnit) -> &'static str { match unit { TimeUnit::Second => ":second", TimeUnit::Millisecond => ":milli", TimeUnit::Microsecond => ":micro", TimeUnit::Nanosecond => ":nano" } }
fn quoted(value: &str) -> Result<String> { serde_json::to_string(value).map_err(|error| Anomaly::fault("rtdb.types/type-rendering", error.to_string())) }
