use std::fmt;

use crate::{Anomaly, IntervalUnit, ListKind, LogicalType, Result, RunEndWidth, ScalarType, TimeUnit};

/// The structured form of an XTDB type literal.
/// Field and time zone names borrow the logical type being described.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeForm<'a> {
    Keyword(&'a str),
    Text(&'a str),
    Integer(i64),
    Boolean(bool),
    Vector(Vec<Self>),
    Set(Vec<Self>),
    Map(Vec<(Self, Self)>),
}

impl fmt::Display for TypeForm<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Keyword(value) => write!(f, ":{value}"),
            Self::Text(value) => quoted(value, f),
            Self::Integer(value) => write!(f, "{value}"),
            Self::Boolean(value) => write!(f, "{value}"),
            Self::Map(entries) => {
                f.write_str("{")?;
                for (index, (key, value)) in entries.iter().enumerate() {
                    if index != 0 { f.write_str(", ")?; }
                    write!(f, "{key} {value}")?;
                }
                f.write_str("}")
            }
            Self::Vector(elements) | Self::Set(elements) => {
                let (open, close) = if matches!(self, Self::Set(_)) { ("#{", "}") } else { ("[", "]") };
                f.write_str(open)?;
                for (index, element) in elements.iter().enumerate() {
                    if index != 0 { f.write_str(" ")?; }
                    write!(f, "{element}")?;
                }
                f.write_str(close)
            }
        }
    }
}

fn quoted(value: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("\"")?;
    let mut start = 0;
    for (index, ch) in value.char_indices() {
        let escape = match ch {
            '"' => "\\\"", '\\' => "\\\\", '\n' => "\\n", '\r' => "\\r", '\t' => "\\t",
            '\u{8}' => "\\b", '\u{c}' => "\\f", ch if ch < '\u{20}' => "",
            _ => continue,
        };
        f.write_str(&value[start..index])?;
        if escape.is_empty() { write!(f, "\\u{:04x}", u32::from(ch))?; } else { f.write_str(escape)?; }
        start = index + ch.len_utf8();
    }
    f.write_str(&value[start..])?;
    f.write_str("\"")
}

impl LogicalType {
    /// EDN spelling used by `information_schema.columns.xt_type` and live column metadata.
    pub fn to_edn(&self) -> Result<String> { Ok(self.type_form()?.to_string()) }

    /// Checks whether this type has an XTDB EDN spelling without allocating the form.
    pub fn validate_type_form(&self) -> Result<()> {
        match self {
            Self::List { kind: ListKind::TimestampTzRange, .. } => Ok(()),
            Self::List { element, .. } | Self::Maybe(element) | Self::RunEndEncoded { value: element, .. } => {
                element.validate_type_form()
            }
            Self::Struct(fields) => fields.values().try_for_each(Self::validate_type_form),
            Self::Union(legs) => legs.iter().try_for_each(Self::validate_type_form),
            Self::Scalar(scalar @ (ScalarType::U8 | ScalarType::U16 | ScalarType::U32 | ScalarType::U64 | ScalarType::F16)) => {
                scalar_type_form(scalar).map(|_| ())
            }
            Self::Scalar(_) | Self::Nothing | Self::Null => Ok(()),
        }
    }

    pub fn type_form(&self) -> Result<TypeForm<'_>> {
        use LogicalType as L;
        use TypeForm::{Boolean, Integer, Keyword, Map, Set, Text, Vector};
        Ok(match self {
            L::Nothing => Keyword("nothing"),
            L::Null => Keyword("null"),
            L::Maybe(ty) => {
                let mut elements = vec![Keyword("?")];
                match ty.type_form()? {
                    Vector(base) => elements.extend(base),
                    base => elements.push(base),
                }
                Vector(elements)
            }
            L::RunEndEncoded { width, value } => Vector(vec![Keyword(match width {
                RunEndWidth::I16 => "ree-i16", RunEndWidth::I32 => "ree", RunEndWidth::I64 => "ree-i64",
            }), value.type_form()?]),
            L::Union(legs) => Set(legs.iter().map(Self::type_form).collect::<Result<_>>()?),
            L::Struct(fields) => Vector(vec![Keyword("struct"), Map(fields.iter().map(|(name, ty)| Ok((Text(name), ty.type_form()?))).collect::<Result<_>>()?)]),
            L::List { kind, element } => match kind {
                ListKind::TimestampTzRange => Keyword("tstz-range"),
                ListKind::List => Vector(vec![Keyword("list"), element.type_form()?]),
                ListKind::Set => Vector(vec![Keyword("set"), element.type_form()?]),
                ListKind::FixedSizeList(size) => Vector(vec![Keyword("fixed-size-list"), Integer(i64::from(*size)), element.type_form()?]),
                ListKind::Map { sorted } => Vector(vec![Keyword("map"), Map(vec![(Keyword("sorted?"), Boolean(*sorted))]), element.type_form()?]),
            },
            L::Scalar(scalar) => scalar_type_form(scalar)?,
        })
    }
}

impl LogicalType {
    /// The leading keyword of the type's EDN spelling — `i64`, `utf8`, `timestamp-tz`, `list` — as `xtdb.types/col-type-head` names it.
    pub fn type_head(&self) -> String {
        match self.type_form() {
            Ok(TypeForm::Keyword(head)) => head.to_string(),
            Ok(TypeForm::Vector(elements)) => match elements.first() { Some(TypeForm::Keyword(head)) => head.to_string(), _ => format!("{self:?}").to_lowercase() },
            Ok(TypeForm::Set(_)) => "union".into(),
            _ => match self { Self::Scalar(scalar) => format!("{scalar:?}").to_lowercase(), _ => format!("{self:?}").to_lowercase() },
        }
    }
}

/// The reference's `codegen-call` default: `upper not applicable to types i64`, `+ not applicable to types i64 and utf8`.
pub fn function_type_mismatch<'a>(name: &str, types: impl IntoIterator<Item = &'a LogicalType>) -> Anomaly {
    let heads: Vec<String> = types.into_iter().map(LogicalType::type_head).collect();
    let listed = match heads.split_last() {
        None => String::new(),
        Some((last, [])) => last.clone(),
        Some((last, init)) => format!("{} and {last}", init.join(", ")),
    };
    Anomaly::incorrect("xtdb.expression/function-type-mismatch", format!("{name} not applicable to types {listed}"))
}

fn scalar_type_form(scalar: &ScalarType) -> Result<TypeForm<'_>> {
    use ScalarType as S;
    use TypeForm::{Integer, Keyword, Text, Vector};
    Ok(match scalar {
        S::Bool => Keyword("bool"), S::I8 => Keyword("i8"), S::I16 => Keyword("i16"), S::I32 => Keyword("i32"), S::I64 => Keyword("i64"),
        S::F32 => Keyword("f32"), S::F64 => Keyword("f64"), S::Utf8 => Keyword("utf8"), S::Binary => Keyword("varbinary"),
        S::FixedSizeBinary(size) => Vector(vec![Keyword("fixed-size-binary"), Integer(i64::from(*size))]),
        S::Decimal { precision, scale, bit_width } => Vector(vec![Keyword("decimal"), Integer(i64::from(*precision)), Integer(i64::from(*scale)), Integer(i64::from(*bit_width))]),
        S::DateDay => Vector(vec![Keyword("date"), Keyword("day")]),
        S::DateMilli => Vector(vec![Keyword("date"), Keyword("milli")]),
        S::Time { unit, .. } => Vector(vec![Keyword("time-local"), Keyword(unit_keyword(*unit))]),
        S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some(tz) } if tz == "UTC" => Keyword("instant"),
        S::Timestamp { unit, timezone: Some(tz) } => Vector(vec![Keyword("timestamp-tz"), Keyword(unit_keyword(*unit)), Text(tz)]),
        S::Timestamp { unit, timezone: None } => Vector(vec![Keyword("timestamp-local"), Keyword(unit_keyword(*unit))]),
        S::Duration(unit) => Vector(vec![Keyword("duration"), Keyword(unit_keyword(*unit))]),
        S::Interval(unit) => Vector(vec![Keyword("interval"), Keyword(match unit {
            IntervalUnit::YearMonth => "year-month", IntervalUnit::DayTime => "day-time", IntervalUnit::MonthDayNano => "month-day-nano",
        })]),
        S::IntervalMonthDayMicro => Vector(vec![Keyword("interval"), Keyword("month-day-micro")]),
        S::Keyword => Keyword("keyword"), S::Uuid => Keyword("uuid"), S::Uri => Keyword("uri"), S::Transit => Keyword("transit"),
        S::Oid => Keyword("oid"), S::RegClass => Keyword("regclass"), S::RegProc => Keyword("regproc"),
        S::U8 | S::U16 | S::U32 | S::U64 | S::F16 => return Err(Anomaly::unsupported("xtdb/unsupported-type-rendering", "JVM type EDN does not represent this numeric type")),
    })
}

fn unit_keyword(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "second", TimeUnit::Millisecond => "milli", TimeUnit::Microsecond => "micro", TimeUnit::Nanosecond => "nano",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_forms_keep_keywords_sets_and_borrowed_field_names() {
        use TypeForm::{Keyword, Map, Set, Text, Vector};
        let item = LogicalType::Scalar(ScalarType::I64).join(&LogicalType::Scalar(ScalarType::Utf8));
        let ty = LogicalType::Struct([("quoted \"field\"".into(), LogicalType::list(item).maybe())].into()).maybe();
        assert_eq!(ty.type_form().unwrap(), Vector(vec![Keyword("?"), Keyword("struct"), Map(vec![(
            Text("quoted \"field\""), Vector(vec![Keyword("?"), Keyword("list"), Set(vec![Keyword("i64"), Keyword("utf8")])]),
        )])]));
        assert_eq!(ty.to_edn().unwrap(), "[:? :struct {\"quoted \\\"field\\\"\" [:? :list #{:i64 :utf8}]}]");
    }

    #[test]
    fn type_form_validation_preserves_opaque_and_rendered_components() {
        let unsupported = LogicalType::Scalar(ScalarType::U8);
        let list = LogicalType::list(unsupported.clone()).maybe();
        assert_eq!(list.validate_type_form().unwrap_err().code, list.type_form().unwrap_err().code);
        let range = LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(unsupported) };
        range.validate_type_form().unwrap();
        assert_eq!(range.to_edn().unwrap(), ":tstz-range");
    }

    #[test]
    fn type_form_text_preserves_the_previous_escape_rules() {
        let text = "\u{1}😀\u{8}\u{c}\n\r\t\"\\ë";
        assert_eq!(TypeForm::Text(text).to_string(), serde_json::to_string(text).unwrap());
    }
}
