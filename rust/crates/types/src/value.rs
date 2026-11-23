use crate::StructFields;
use crate::{Anomaly, CalendarDate, CivilDate, DateUnit, EntityId, ListValue, IntervalUnit, Keyword, ListKind, ListView, LogicalType, Result, ScalarType, TimeUnit};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecimalValue {
    /// Signed two's-complement coefficient, most significant byte first.
    pub unscaled: Vec<u8>,
    pub precision: i32,
    pub scale: i32,
    pub bit_width: u16,
}

impl DecimalValue {
    /// Preserves raw vector metadata and checks coefficient width; ordinary writes validate precision separately.
    pub fn new(unscaled: Vec<u8>, precision: i32, scale: i32, bit_width: u16) -> Result<Self> {
        ScalarType::Decimal { precision, scale, bit_width }.validate()?;
        if unscaled.len() != usize::from(bit_width / 8) {
            return Err(Anomaly::incorrect("xtdb/invalid-decimal", "Decimal coefficient width does not match its type"));
        }
        Ok(Self { unscaled, precision, scale, bit_width })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Undefined, Null, Bool(bool),
    I8(i8), I16(i16), I32(i32), I64(i64),
    U8(u8), U16(u16), U32(u32), U64(u64),
    F16(u16), F32(#[serde(with = "float32_bits")] f32), F64(#[serde(with = "float64_bits")] f64),
    Utf8(String), Binary(Vec<u8>), FixedSizeBinary(Vec<u8>), Decimal(DecimalValue),
    Date(CalendarDate), DateDay(i32), DateMilli(i64),
    Time { value: i64, unit: TimeUnit, bit_width: u8 },
    Timestamp { value: i64, unit: TimeUnit, timezone: Option<String> },
    Duration { value: i64, unit: TimeUnit },
    IntervalYearMonth(i32),
    IntervalDayTime { days: i32, milliseconds: i32 },
    IntervalMonthDayNano { months: i32, days: i32, nanoseconds: i64 },
    IntervalMonthDayMicro { months: i32, days: i32, nanoseconds: i64 },
    Keyword(Keyword), Uuid([u8; 16]), Uri(String), Transit(Vec<u8>), Oid(i32), RegClass(i32), RegProc(i32),
    List(ListValue), Set(Vec<Value>), Map { entries: Vec<(Value, Value)>, sorted: bool },
    FixedSizeList(ListValue), TimestampTzRange { lower: i64, upper: i64 },
    Struct(StructFields<Value>),
}

mod float32_bits {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &f32, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_u32(value.to_bits())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<f32, D::Error> {
        u32::deserialize(deserializer).map(f32::from_bits)
    }
}

mod float64_bits {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_u64(value.to_bits())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<f64, D::Error> {
        u64::deserialize(deserializer).map(f64::from_bits)
    }
}

impl Value {
    /// Preserves calendar days and unit, using physical storage only when that representation is lossless.
    /// Wide values remain calendar objects until an explicit object or expression admission boundary.
    pub fn calendar_date(date: CivilDate, unit: DateUnit) -> Self {
        let date = CalendarDate::new(date, unit);
        date.physical_value().unwrap_or(Self::Date(date))
    }

    /// Returns epoch days for calendar objects and physical date cells.
    /// A millisecond value resolves to the UTC calendar day containing it.
    pub fn date_days(&self) -> Option<i64> {
        match self { Self::Date(value) => Some(value.date().epoch_day()), Self::DateDay(days) => Some(i64::from(*days)), Self::DateMilli(millis) => Some(millis.div_euclid(crate::java_date::MILLIS_PER_DAY)), _ => None }
    }

    /// Admits calendar objects in a completed expression result, recursively and without allocation.
    /// Errors leave earlier siblings admitted; the caller must discard the result on failure.
    pub fn admit_expression_dates(&mut self) -> Result<()> {
        match self {
            Self::Date(value) => *self = value.expression_value()?,
            Self::List(values) | Self::FixedSizeList(values) => if let Some(values) = values.values_mut() {
                for value in values { value.admit_expression_dates()?; }
            },
            Self::Set(values) => for value in values { value.admit_expression_dates()?; },
            Self::Struct(fields) => for value in fields.values_mut() { value.admit_expression_dates()?; },
            Self::Map { entries, .. } => for (key, value) in entries { key.admit_expression_dates()?; value.admit_expression_dates()?; },
            _ => (),
        }
        Ok(())
    }

    /// Checks physical date ranges recursively at a vector object-writer boundary.
    pub fn admit_object_dates(&mut self) -> Result<()> {
        match self {
            Self::Date(value) => *self = value.object_value()?,
            Self::List(values) | Self::FixedSizeList(values) => {
                if let Some(values) = values.values_mut() { values.iter_mut().try_for_each(Self::admit_object_dates)?; }
            }
            Self::Set(values) => values.iter_mut().try_for_each(Self::admit_object_dates)?,
            Self::Struct(fields) => fields.values_mut().try_for_each(Self::admit_object_dates)?,
            Self::Map { entries, .. } => for (key, value) in entries { key.admit_object_dates()?; value.admit_object_dates()?; },
            _ => (),
        }
        Ok(())
    }

    /// Applies one typed ValueBox write without traversing collection readers.
    /// Day values check signed32 narrowing; millisecond dates retain epoch days until Arrow output.
    /// Physical millisecond cells retain their raw storage value.
    /// An out-of-range day returns an overflow anomaly without changing the value; this allocates no storage.
    pub fn admit_box_date(&mut self) -> Result<()> {
        match self {
            Self::Date(value) if value.unit() == DateUnit::Day => *self = value.expression_value()?,
            _ => (),
        }
        Ok(())
    }

    /// Reports any calendar object still awaiting physical admission, including nested cells.
    pub fn has_calendar_object(&self) -> bool {
        match self {
            Self::Date(_) => true,
            Self::List(values) | Self::FixedSizeList(values) => matches!(values.view(), ListView::Values(values) if values.iter().any(Self::has_calendar_object)),
            Self::Set(values) => values.iter().any(Self::has_calendar_object),
            Self::Struct(fields) => fields.values().any(Self::has_calendar_object),
            Self::Map { entries, .. } => entries.iter().any(|(key,value)| key.has_calendar_object() || value.has_calendar_object()),
            _ => false,
        }
    }

    /// Charges owned payload capacities plus a conservative allowance for each B-tree node.
    /// The result excludes allocator bookkeeping and includes this value's inline storage.
    pub fn retained_bytes(&self) -> Result<usize> {
        self.heap_bytes()?.checked_add(std::mem::size_of::<Self>()).ok_or_else(size_overflow)
    }

    pub(crate) fn heap_bytes(&self) -> Result<usize> {
        let total = match self {
            Self::Utf8(value) | Self::Uri(value) => value.capacity(),
            Self::Keyword(value) => value.retained_bytes(),
            Self::Binary(value) | Self::FixedSizeBinary(value) | Self::Transit(value) => value.capacity(),
            Self::Decimal(value) => value.unscaled.capacity(),
            Self::Timestamp { timezone, .. } => timezone.as_ref().map_or(0, String::capacity),
            Self::List(values) | Self::FixedSizeList(values) => values.retained_payload_bytes()?,
            Self::Set(values) => {
                let inline = values.capacity().checked_mul(std::mem::size_of::<Self>()).ok_or_else(size_overflow)?;
                values.iter().try_fold(inline, |bytes, value| bytes.checked_add(value.heap_bytes()?).ok_or_else(size_overflow))?
            }
            Self::Map { entries, .. } => {
                let inline = entries.capacity().checked_mul(std::mem::size_of::<(Self, Self)>()).ok_or_else(size_overflow)?;
                entries.iter().try_fold(inline, |bytes, (key, value)| {
                    let key_heap = key.heap_bytes()?;
                    let value_heap = value.heap_bytes()?;
                    bytes.checked_add(key_heap).and_then(|bytes| bytes.checked_add(value_heap)).ok_or_else(size_overflow)
                })?
            }
            Self::Struct(fields) => Self::document_retained_bytes(fields)?,
            _ => 0,
        };
        Ok(total)
    }

    pub fn document_retained_bytes(fields: &StructFields<Self>) -> Result<usize> {
        let table = fields.table_bytes_bound().and_then(|bytes| bytes.checked_add(std::mem::size_of::<StructFields<Self>>())).ok_or_else(size_overflow)?;
        fields.iter().try_fold(table, |bytes, (name, value)| {
            let heap = value.heap_bytes()?;
            bytes.checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(heap)).ok_or_else(size_overflow)
        })
    }

    pub fn entity_id(&self) -> Result<EntityId> {
        match self {
            Self::Utf8(value) => Ok(EntityId::String(value.clone())),
            Self::Keyword(value) => Ok(EntityId::Keyword(value.clone())),
            Self::I8(value) => Ok(EntityId::Integer(i64::from(*value))),
            Self::I16(value) => Ok(EntityId::Integer(i64::from(*value))),
            Self::I32(value) => Ok(EntityId::Integer(i64::from(*value))),
            Self::I64(value) => Ok(EntityId::Integer(*value)),
            Self::Uuid(value) => Ok(EntityId::Uuid(*value)),
            Self::Date(_) | Self::DateDay(_) | Self::DateMilli(_) => Err(Anomaly::incorrect("xtdb/invalid-id", "Invalid ID type: java.time.LocalDate")
                .with_context("type", "java.time.LocalDate".into())),
            _ => Err(Anomaly::incorrect("xtdb/invalid-id", "An ID must be a string, keyword, signed integer, or UUID")),
        }
    }

    pub fn logical_type(&self) -> LogicalType {
        use ScalarType as S;
        let scalar = match self {
            Self::Undefined => return LogicalType::Nothing,
            Self::Null => return LogicalType::Null,
            Self::Bool(_) => S::Bool,
            Self::I8(_) => S::I8, Self::I16(_) => S::I16, Self::I32(_) => S::I32, Self::I64(_) => S::I64,
            Self::U8(_) => S::U8, Self::U16(_) => S::U16, Self::U32(_) => S::U32, Self::U64(_) => S::U64,
            Self::F16(_) => S::F16, Self::F32(_) => S::F32, Self::F64(_) => S::F64,
            Self::Utf8(_) => S::Utf8, Self::Binary(_) => S::Binary,
            Self::FixedSizeBinary(bytes) => S::FixedSizeBinary(i32::try_from(bytes.len()).unwrap_or(i32::MAX)),
            Self::Decimal(decimal) => S::Decimal { precision: decimal.precision, scale: decimal.scale, bit_width: decimal.bit_width },
            Self::Date(value) => match value.unit() { DateUnit::Day => S::DateDay, DateUnit::Millisecond => S::DateMilli },
            Self::DateDay(_) => S::DateDay, Self::DateMilli(_) => S::DateMilli,
            Self::Time { unit, bit_width, .. } => S::Time { unit: *unit, bit_width: *bit_width },
            Self::Timestamp { unit, timezone, .. } => S::Timestamp { unit: *unit, timezone: timezone.clone() },
            Self::Duration { unit, .. } => S::Duration(*unit),
            Self::IntervalYearMonth(_) => S::Interval(IntervalUnit::YearMonth),
            Self::IntervalDayTime { .. } => S::Interval(IntervalUnit::DayTime),
            Self::IntervalMonthDayNano { .. } => S::Interval(IntervalUnit::MonthDayNano),
            Self::IntervalMonthDayMicro { .. } => S::IntervalMonthDayMicro,
            Self::Keyword(_) => S::Keyword, Self::Uuid(_) => S::Uuid, Self::Uri(_) => S::Uri,
            Self::Transit(_) => S::Transit, Self::Oid(_) => S::Oid, Self::RegClass(_) => S::RegClass, Self::RegProc(_) => S::RegProc,
            Self::List(values) | Self::FixedSizeList(values) => return LogicalType::List {
                kind: match self {
                    Self::FixedSizeList(_) => ListKind::FixedSizeList(i32::try_from(values.len()).unwrap_or(i32::MAX)),
                    _ => ListKind::List,
                },
                element: Box::new(values.element_type()),
            },
            Self::Set(values) => {
                let types: Vec<_> = values.iter().map(Self::logical_type).collect();
                return LogicalType::List { kind: ListKind::Set, element: Box::new(LogicalType::join_all(&types)) };
            }
            Self::Map { entries, sorted } => {
                let keys: Vec<_> = entries.iter().map(|(key, _)| key.logical_type()).collect();
                let values: Vec<_> = entries.iter().map(|(_, value)| value.logical_type()).collect();
                let element = LogicalType::Struct(StructFields::from([
                    ("key".to_owned(), LogicalType::join_all(&keys)),
                    ("value".to_owned(), LogicalType::join_all(&values)),
                ]));
                return LogicalType::List { kind: ListKind::Map { sorted: *sorted }, element: Box::new(element) };
            }
            Self::TimestampTzRange { .. } => {
                return LogicalType::List { kind: ListKind::TimestampTzRange,
                    element: Box::new(LogicalType::Scalar(S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".to_owned()) })) };
            }
            Self::Struct(fields) => return LogicalType::Struct(fields.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect()),
        };
        LogicalType::Scalar(scalar)
    }

    pub fn list(values: Vec<Value>) -> Self { Self::List(values.into()) }

    pub fn list_view(&self) -> Option<ListView<'_>> {
        match self {
            Self::Set(values) => Some(ListView::Values(values)),
            Self::List(values) | Self::FixedSizeList(values) => Some(values.view()),
            _ => None,
        }
    }

    pub fn field(&self, name: &str) -> Option<&Self> {
        match self { Self::Struct(fields) => fields.get(name), _ => None }
    }

    pub fn is_null(&self) -> bool { matches!(self, Self::Null) }
}

fn size_overflow() -> Anomaly { Anomaly::incorrect("rtdb/value-size-overflow", "Retained value memory exceeds usize") }
