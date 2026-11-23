use crate::{Anomaly, ListKind, LogicalType, Result, ScalarType, TableRef, TimeUnit, clojure_string_hasheq, clojure_symbol_hasheq};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PgType {
    Default, Null, Int8, Int4, Int2, Float4, Float8, Bool, Text, VarChar, Numeric,
    Timestamp, TimestampTz, Date, Time, Interval, Duration, TsTzRange, Bytes,
    Int4s, Int8s, Texts, Uuid, Keyword, Oid, RegClass, RegProc, Json, JsonLd, Jsonb, Transit, Float4s, Float8s,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgTypeDescriptor {
    pub name: &'static str,
    pub oid: i32,
    pub length: i16,
    pub category: char,
    pub send: &'static str,
    pub receive: &'static str,
    pub array_oid: Option<i32>,
    pub element_oid: Option<i32>,
    pub input: Option<&'static str>,
    pub output: Option<&'static str>,
}

/// Catalog OIDs for names use Clojure's string hasheq followed by Java's wrapping absolute value.
pub fn pg_name_oid(name: &str) -> i32 {
    clojure_string_hasheq(name).wrapping_abs()
}

/// Persisted table OIDs take precedence; virtual tables use the Clojure symbol hash.
pub fn pg_table_oid(allocated: Option<i32>, table: &TableRef) -> i32 {
    allocated.unwrap_or_else(|| clojure_symbol_hasheq(Some(&table.schema), &table.table).wrapping_abs())
}

impl PgType {
    pub const ALL: &'static [Self] = &[
        Self::Int8, Self::Int4, Self::Int2, Self::Float4, Self::Float8, Self::Bool, Self::Text,
        Self::VarChar, Self::Numeric, Self::Timestamp, Self::TimestampTz, Self::Date, Self::Time,
        Self::Interval, Self::Duration, Self::TsTzRange, Self::Bytes, Self::Int4s, Self::Int8s,
        Self::Texts, Self::Uuid, Self::Keyword, Self::Oid, Self::RegClass, Self::RegProc,
        Self::Json, Self::Jsonb, Self::Transit, Self::Float4s, Self::Float8s,
    ];

    pub fn descriptor(self) -> PgTypeDescriptor {
        let (name, oid, length, category, send, receive) = match self {
            Self::Default => ("default", 0, -1, 'S', "", ""),
            Self::Null => ("null", 25, -1, 'S', "textsend", "textrecv"),
            Self::Int8 => ("int8", 20, 8, 'N', "int8send", "int8recv"),
            Self::Int4 => ("int4", 23, 4, 'N', "int4send", "int4recv"),
            Self::Int2 => ("int2", 21, 2, 'N', "int2send", "int2recv"),
            Self::Float4 => ("float4", 700, 4, 'N', "float4send", "float4recv"),
            Self::Float8 => ("float8", 701, 8, 'N', "float8send", "float8recv"),
            Self::Bool => ("bool", 16, 1, 'B', "boolsend", "boolrecv"),
            Self::Text => ("text", 25, -1, 'S', "textsend", "textrecv"),
            Self::VarChar => ("varchar", 1043, -1, 'S', "varcharsend", "varcharrecv"),
            Self::Numeric => ("numeric", 1700, -1, 'N', "numeric_send", "numeric_recv"),
            Self::Timestamp => ("timestamp", 1114, 8, 'D', "timestamp_send", "timestamp_recv"),
            Self::TimestampTz => ("timestamptz", 1184, 8, 'D', "timestamptz_send", "timestamptz_recv"),
            Self::Date => ("date", 1082, 4, 'D', "date_send", "date_recv"),
            Self::Time => ("time", 1083, 8, 'D', "time_send", "time_recv"),
            Self::Interval => ("interval", 1186, 16, 'T', "interval_send", "interval_recv"),
            Self::Duration => ("duration", 11112, 8, 'T', "duration_send", "duration_recv"),
            Self::TsTzRange => ("tstz-range", 3910, -1, 'R', "range_send", "range_recv"),
            Self::Bytes => ("bytea", 17, -1, 'U', "byteasend", "bytearecv"),
            Self::Int4s => ("_int4", 1007, -1, 'A', "array_send", "array_recv"),
            Self::Int8s => ("_int8", 1016, -1, 'A', "array_send", "array_recv"),
            Self::Float4s => ("_float4", 1021, -1, 'A', "array_send", "array_recv"),
            Self::Float8s => ("_float8", 1022, -1, 'A', "array_send", "array_recv"),
            Self::Texts => ("_text", 1009, -1, 'A', "array_send", "array_recv"),
            Self::Uuid => ("uuid", 2950, 16, 'U', "uuid_send", "uuid_recv"),
            Self::Keyword => ("keyword", 11111, -1, 'S', "keywordsend", "keywordrecv"),
            Self::Oid => ("oid", 26, -1, 'N', "oidsend", "oidrecv"),
            Self::RegClass => ("regclass", 2205, -1, 'N', "regclasssend", "regclassrecv"),
            Self::RegProc => ("regproc", 24, -1, 'N', "regprocsend", "regprocrecv"),
            Self::Json | Self::JsonLd => ("json", 114, -1, 'U', "json_send", "json_recv"),
            Self::Jsonb => ("jsonb", 3802, -1, 'U', "jsonb_send", "jsonb_recv"),
            Self::Transit => ("transit", 16384, -1, 'U', "transit_send", "transit_recv"),
        };
        PgTypeDescriptor { name, oid, length, category, send, receive,
            array_oid: match self { Self::Int8 => Some(1016), Self::Int4 => Some(1007), Self::Text => Some(1009), Self::Float4 => Some(1021), Self::Float8 => Some(1022), _ => None },
            element_oid: match self { Self::Int4s => Some(23), Self::Int8s => Some(20), Self::Texts => Some(25), Self::Float4s => Some(700), Self::Float8s => Some(701), _ => None },
            input: match self { Self::Int4s | Self::Int8s | Self::Texts | Self::Float4s | Self::Float8s => Some("array_in"), Self::Uuid => Some("uuid_in"), _ => None },
            output: match self { Self::Int4s | Self::Int8s | Self::Texts | Self::Float4s | Self::Float8s => Some("array_out"), Self::Uuid => Some("uuid_out"), _ => None },
        }
    }

    /// OID zero is the untyped protocol parameter; unknown OIDs remain absent.
    pub fn from_oid(oid: i32) -> Option<Self> {
        if oid == 0 { return Some(Self::Default); }
        Self::ALL.iter().copied().find(|ty| ty.descriptor().oid == oid)
    }

    /// The declared Arrow parameter type; JSON and Transit decode to value-dependent types.
    pub fn logical_type(self) -> Option<LogicalType> {
        use ScalarType as S;
        let scalar = match self {
            Self::Null => return Some(LogicalType::Null),
            Self::Json | Self::JsonLd | Self::Jsonb | Self::Transit => return None,
            Self::Default | Self::Text | Self::VarChar => S::Utf8,
            Self::Int8 => S::I64, Self::Int4 => S::I32, Self::Int2 => S::I16,
            Self::Float4 => S::F32, Self::Float8 => S::F64, Self::Bool => S::Bool,
            Self::Numeric => S::Decimal { precision: 32, scale: 0, bit_width: 128 },
            Self::Timestamp => S::Timestamp { unit: TimeUnit::Microsecond, timezone: None },
            Self::TimestampTz => S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) },
            Self::Date => S::DateDay, Self::Time => S::Time { unit: TimeUnit::Microsecond, bit_width: 64 },
            Self::Interval => S::IntervalMonthDayMicro, Self::Duration => S::Duration(TimeUnit::Microsecond),
            Self::TsTzRange => return Some(LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(Self::TimestampTz.logical_type()?) }),
            Self::Bytes => S::Binary,
            Self::Float4s => return Some(LogicalType::list(Self::Float4.logical_type()?.maybe())),
            Self::Float8s => return Some(LogicalType::list(Self::Float8.logical_type()?.maybe())),
            Self::Int4s | Self::Int8s | Self::Texts => return Some(LogicalType::list(match self {
                Self::Int4s => Self::Int4, Self::Int8s => Self::Int8, _ => Self::Text,
            }.logical_type()?)),
            Self::Uuid => S::Uuid, Self::Keyword => S::Keyword, Self::Oid => S::Oid,
            Self::RegClass => S::RegClass, Self::RegProc => S::RegProc,
        };
        Some(LogicalType::Scalar(scalar))
    }

    /// Maps union legs independently, widening mixed integer, float and integer-array widths.
    pub fn from_logical(ty: &LogicalType) -> Result<Self> {
        let types = ty.legs().iter().map(Self::from_mono).collect::<Result<BTreeSet<_>>>()?;
        let types: BTreeSet<_> = types.into_iter().filter(|ty| *ty != Self::Null).collect();
        if types.is_empty() { return Ok(Self::Null); }
        if types.len() == 1 { return Ok(*types.first().expect("one PostgreSQL type")); }
        Ok(if types.iter().all(|ty| matches!(ty, Self::Int2 | Self::Int4 | Self::Int8)) { Self::Int8 }
            else if types.iter().all(|ty| matches!(ty, Self::Float4 | Self::Float8)) { Self::Float8 }
            else if types.iter().all(|ty| matches!(ty, Self::Int4s | Self::Int8s)) { Self::Int8s }
            else if types.iter().all(|ty| matches!(ty, Self::Float4s | Self::Float8s)) { Self::Float8s }
            else { Self::Default })
    }

    fn from_mono(ty: &LogicalType) -> Result<Self> {
        use LogicalType as L;
        use ScalarType as S;
        Ok(match ty {
            L::Nothing | L::Null => Self::Null,
            L::List { kind: ListKind::TimestampTzRange, .. } => Self::TsTzRange,
            L::List { kind: ListKind::List | ListKind::FixedSizeList(_), element } => {
                let element = match element.as_ref() { L::Maybe(element) => element.as_ref(), element => element };
                match element { L::Scalar(S::I32 | S::U32) => Self::Int4s, L::Scalar(S::I64 | S::U64) => Self::Int8s, L::Scalar(S::Utf8) => Self::Texts,
                    L::Scalar(S::F32) => Self::Float4s, L::Scalar(S::F64) => Self::Float8s,
                    L::Union(_) if element.legs().iter().all(|leg| matches!(leg, L::Null | L::Scalar(S::F32 | S::F64))) => Self::Float8s,
                    _ => Self::Default,
                }
            }
            L::List { .. } | L::Struct(_) | L::RunEndEncoded { .. } => Self::Default,
            L::Maybe(_) | L::Union(_) => return Self::from_logical(ty),
            L::Scalar(scalar) => match scalar {
                S::I16 | S::U16 => Self::Int2, S::I32 | S::U32 => Self::Int4, S::I64 | S::U64 => Self::Int8,
                S::F32 => Self::Float4, S::F64 => Self::Float8, S::Utf8 => Self::Text,
                S::Binary | S::FixedSizeBinary(_) => Self::Bytes, S::Bool => Self::Bool,
                S::Decimal { .. } => Self::Numeric, S::DateDay | S::DateMilli => Self::Date,
                S::Time { .. } => Self::Time, S::Timestamp { timezone: None, .. } => Self::Timestamp,
                S::Timestamp { .. } => Self::TimestampTz, S::Interval(_) | S::IntervalMonthDayMicro => Self::Interval,
                S::Duration(_) => Self::Duration, S::Uuid => Self::Uuid, S::Keyword => Self::Keyword,
                S::Oid => Self::Oid, S::RegClass => Self::RegClass, S::RegProc => Self::RegProc,
                S::Transit => Self::Transit, S::Uri => Self::Default,
                S::I8 | S::U8 | S::F16 => return Err(Anomaly::fault("rtdb.types/pg-type", "PostgreSQL has no mapping for this numeric width")),
            },
        })
    }

    pub fn format_name(self) -> &'static str {
        match self {
            Self::Null | Self::Text => "text", Self::Int8 => "bigint", Self::Int4 => "integer", Self::Int2 => "smallint",
            Self::Float4 => "real", Self::Float8 => "double precision", Self::Bool => "boolean", Self::VarChar => "character varying",
            Self::Timestamp => "timestamp without time zone", Self::TimestampTz => "timestamp with time zone", Self::Time => "time without time zone",
            Self::TsTzRange => "tstzrange", Self::Int4s => "integer[]", Self::Int8s => "bigint[]", Self::Texts => "text[]", Self::Float4s => "real[]", Self::Float8s => "double precision[]",
            _ => self.descriptor().name,
        }
    }
    pub fn data_type(self) -> &'static str {
        match self { Self::Int4s | Self::Int8s | Self::Texts | Self::Float4s | Self::Float8s => "ARRAY", Self::Default | Self::Duration | Self::Keyword | Self::Transit => "USER-DEFINED", _ => self.format_name() }
    }
    pub fn format_type(self, modifier: i32) -> String {
        if modifier < 0 { return self.format_name().into(); }
        match self {
            Self::VarChar if modifier >= 4 => format!("character varying({})", modifier - 4),
            Self::Numeric if modifier >= 4 => format!("numeric({},{})", ((modifier - 4) >> 16) & 0xffff, (modifier - 4) & 0xffff),
            Self::Timestamp => format!("timestamp({modifier}) without time zone"),
            Self::TimestampTz => format!("timestamp({modifier}) with time zone"),
            Self::Time => format!("time({modifier}) without time zone"),
            Self::Interval if modifier & 0xffff != 0xffff => format!("interval({})", modifier & 0xffff),
            _ => self.format_name().into(),
        }
    }
    pub fn format_oid(oid: i32, modifier: i32) -> String {
        if oid == 0 { "-".into() } else { Self::from_oid(oid).map(|ty| ty.format_type(modifier)).unwrap_or_else(|| "???".into()) }
    }

    /// PostgreSQL Bind accepts a resolved null or an untyped prepared column.
    /// An empty Arrow list also preserves a prepared PostgreSQL array signature.
    pub fn accepts_result(self, actual: &LogicalType) -> Result<bool> {
        let resolved = Self::from_logical(actual)?;
        let actual = match actual { LogicalType::Maybe(value) => value.as_ref(), value => value };
        Ok(self == Self::Default || resolved == Self::Null || resolved == self ||
            (self.descriptor().category == 'A' && matches!(actual, LogicalType::List { kind: ListKind::List, element } if matches!(element.as_ref(), LogicalType::Null | LogicalType::Nothing))))
    }
}
