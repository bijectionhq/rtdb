use crate::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use crate::{StructFields, OrderedSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TimeUnit { Second, Millisecond, Microsecond, Nanosecond }

impl TimeUnit {
    pub fn units_per_second(self) -> i64 {
        match self { Self::Second => 1, Self::Millisecond => 1_000, Self::Microsecond => 1_000_000, Self::Nanosecond => 1_000_000_000 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IntervalUnit { YearMonth, DayTime, MonthDayNano }

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RunEndWidth { I16, I32, I64 }

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ScalarType {
    Bool,
    I8, I16, I32, I64,
    U8, U16, U32, U64,
    F16, F32, F64,
    Utf8, Binary, FixedSizeBinary(i32),
    Decimal { precision: i32, scale: i32, bit_width: u16 },
    DateDay, DateMilli,
    Time { unit: TimeUnit, bit_width: u8 },
    Timestamp { unit: TimeUnit, timezone: Option<String> },
    Duration(TimeUnit),
    Interval(IntervalUnit),
    IntervalMonthDayMicro,
    Keyword, Uuid, Uri, Transit, Oid, RegClass, RegProc,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ListKind {
    List,
    Set,
    Map { sorted: bool },
    FixedSizeList(i32),
    TimestampTzRange,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum LogicalType {
    Nothing,
    Null,
    Scalar(ScalarType),
    List { kind: ListKind, element: Box<LogicalType> },
    Struct(StructFields<LogicalType>),
    Maybe(Box<LogicalType>),
    Union(OrderedSet<LogicalType>),
    RunEndEncoded { width: RunEndWidth, value: Box<LogicalType> },
}

pub const OWNED_FACT_TEXT_VALUE: &str = "v2_value";

impl LogicalType {
    /// Classifies an owned search column, including null-only pages of a fact text wrapper.
    pub fn owned_search_text_presence(&self) -> Option<bool> {
        let mut has_text = false;
        for leg in self.legs() {
            match leg {
                Self::Null => {}
                Self::Scalar(ScalarType::Utf8) => has_text = true,
                Self::Struct(fields) if fields.len() == 1 => {
                    for value in fields.get(OWNED_FACT_TEXT_VALUE)?.legs() {
                        match value {
                            Self::Null => {}
                            Self::Scalar(ScalarType::Utf8) => has_text = true,
                            _ => return None,
                        }
                    }
                }
                _ => return None,
            }
        }
        Some(has_text)
    }

    /// Whether this type admits text and no non-text values besides null.
    pub fn is_clear_search_text(&self) -> bool {
        let legs = self.legs();
        legs.contains(&Self::Scalar(ScalarType::Utf8))
            && legs.iter().all(|leg| matches!(leg, Self::Scalar(ScalarType::Utf8) | Self::Null))
    }

    /// Includes inline storage and owned capacities, with conservative tree-node overhead.
    pub fn retained_bytes(&self) -> Result<usize> {
        let mut bytes = std::mem::size_of::<Self>();
        let overflow = || Anomaly::incorrect("rtdb/type-size", "Logical type size exceeds usize").with_kind(crate::AnomalyKind::ProgramLimitExceeded);
        match self {
            Self::Scalar(ScalarType::Timestamp { timezone: Some(timezone), .. }) => bytes = bytes.checked_add(timezone.capacity()).ok_or_else(overflow)?,
            Self::List { element, .. } | Self::Maybe(element) | Self::RunEndEncoded { value: element, .. } => bytes = bytes.checked_add(element.retained_bytes()?).ok_or_else(overflow)?,
            Self::Struct(fields) => for (name, ty) in fields {
                bytes = bytes.checked_add(256).and_then(|n| n.checked_add(name.capacity())).and_then(|n| n.checked_add(std::mem::size_of::<String>())).ok_or_else(overflow)?;
                bytes = bytes.checked_add(ty.retained_bytes()?).ok_or_else(overflow)?;
            },
            Self::Union(legs) => for leg in legs { bytes = bytes.checked_add(256).and_then(|n| n.checked_add(leg.retained_bytes().ok()?)).ok_or_else(overflow)?; },
            _ => {}
        }
        Ok(bytes)
    }

    pub fn list(element: Self) -> Self { Self::List { kind: ListKind::List, element: Box::new(element) } }

    /// Returns value kinds with physical encodings removed at every depth, in the order they were first seen.
    pub fn legs(&self) -> OrderedSet<Self> {
        match self {
            Self::Nothing => OrderedSet::new(),
            Self::Maybe(value) => { let mut legs = value.legs(); legs.insert(Self::Null); legs }
            Self::Union(values) => values.iter().flat_map(Self::legs).collect(),
            Self::RunEndEncoded { value, .. } => value.legs(),
            value => OrderedSet::from([value.unencoded()]),
        }
    }

    /// An empty leg set denotes an unknown prepared parameter and yields `Null`.
    /// The lattice join retains `Nothing` when no input contributes a leg.
    pub fn from_legs(legs: impl IntoIterator<Item = Self>) -> Self {
        Self::from_legs_set(legs.into_iter().flat_map(|leg| leg.legs()).collect())
    }

    /// `from_legs` for inputs whose leaves are already unencoded, so nested types are not rebuilt again.
    fn from_unencoded_legs(legs: impl IntoIterator<Item = Self>) -> Self {
        fn flatten(leg: LogicalType, out: &mut OrderedSet<LogicalType>) {
            match leg {
                LogicalType::Nothing => {}
                LogicalType::Maybe(value) => { flatten(*value, out); out.insert(LogicalType::Null); }
                LogicalType::Union(values) => for value in values { flatten(value, out); },
                value => { out.insert(value); }
            }
        }
        let mut flat = OrderedSet::new();
        for leg in legs { flatten(leg, &mut flat); }
        Self::from_legs_set(flat)
    }

    /// A union keeps its null leg where it was first seen.
    fn from_legs_set(legs: OrderedSet<Self>) -> Self {
        let nullable = legs.contains(&Self::Null);
        let values = legs.len() - usize::from(nullable);
        if values == 0 { return Self::Null; }
        if values == 1 {
            let value = legs.into_iter().find(|leg| *leg != Self::Null).unwrap_or(Self::Nothing);
            return if nullable { Self::Maybe(Box::new(value)) } else { value };
        }
        Self::Union(legs)
    }

    pub fn maybe(self) -> Self {
        match self {
            Self::Nothing | Self::Null => Self::Null,
            value @ Self::Maybe(_) => value,
            Self::RunEndEncoded { width, value } => Self::RunEndEncoded { width, value: Box::new((*value).maybe()) },
            Self::Union(mut legs) => { legs.insert(Self::Null); Self::from_legs(legs) }
            value => Self::Maybe(Box::new(value)),
        }
    }

    pub fn nullable(&self) -> bool { self.legs().contains(&Self::Null) }

    pub fn unencoded(&self) -> Self {
        match self {
            Self::RunEndEncoded { value, .. } => value.unencoded(),
            Self::Maybe(value) => match value.unencoded() {
                Self::Union(mut legs) => { legs.insert(Self::Null); Self::from_unencoded_legs(legs) }
                value => value.maybe(),
            },
            Self::Union(legs) => Self::from_unencoded_legs(legs.iter().map(Self::unencoded)),
            Self::List { kind, element } => Self::List { kind: kind.clone(), element: Box::new(element.unencoded()) },
            Self::Struct(children) => Self::Struct(children.iter().map(|(name, ty)| (name.clone(), ty.unencoded())).collect()),
            value => value.clone(),
        }
    }

    /// Replaces the outer layout while retaining child encodings and nullability.
    /// Null and bottom return an unsupported-run-end-type error.
    pub fn run_end_encoded(self, width: RunEndWidth) -> Result<Self> {
        let value = match self { Self::RunEndEncoded { value, .. } => *value, value => value };
        if matches!(value, Self::Nothing | Self::Null) {
            return Err(Anomaly::unsupported("xtdb/unsupported-run-end-type", "Null and bottom have no run-end encoding"));
        }
        Ok(Self::RunEndEncoded { width, value: Box::new(value) })
    }

    pub fn absent_contribution(has_put_rows: bool) -> Self {
        if has_put_rows { Self::Null } else { Self::Nothing }
    }

    pub fn join(&self, other: &Self) -> Self { Self::join_all([self, other]) }

    pub fn join_all<'a>(types: impl IntoIterator<Item = &'a Self>) -> Self {
        Self::join_iter(&mut types.into_iter())
    }

    /// Legs keep the order they were first seen in; lists of one kind, and all structs, merge into the slot of the first.
    fn join_iter<'a>(types: &mut dyn Iterator<Item = &'a Self>) -> Self {
        enum Slot { Leg(LogicalType), List(ListKind, LogicalType), Struct }
        let mut slots: Vec<Slot> = Vec::new();
        let mut structs: Vec<StructFields<Self>> = Vec::new();
        for ty in types {
            for leg in ty.legs() {
                match leg {
                    Self::List { kind, element } => match slots.iter_mut().find_map(|slot| match slot { Slot::List(existing, old) if *existing == kind => Some(old), _ => None }) {
                        Some(old) => *old = old.join(&element),
                        None => slots.push(Slot::List(kind, *element)),
                    },
                    Self::Struct(children) => {
                        if structs.is_empty() { slots.push(Slot::Struct); }
                        structs.push(children);
                    }
                    other => if !slots.iter().any(|slot| matches!(slot, Slot::Leg(existing) if *existing == other)) { slots.push(Slot::Leg(other)); },
                }
            }
        }
        if slots.is_empty() { return Self::Nothing; }
        let mut merged_struct = (!structs.is_empty()).then(|| {
            let names: OrderedSet<&String> = structs.iter().flat_map(|fields| fields.keys()).collect();
            names.into_iter().map(|name| (name.clone(), Self::join_iter(&mut structs.iter().map(|fields| fields.get(name).unwrap_or(&Self::Null))))).collect::<StructFields<_>>()
        });
        Self::from_legs(slots.into_iter().map(|slot| match slot {
            Slot::Leg(leg) => leg,
            Slot::List(kind, element) => Self::List { kind, element: Box::new(element) },
            Slot::Struct => Self::Struct(merged_struct.take().unwrap_or_default()),
        }).collect::<Vec<_>>())
    }

    /// Returns the computation type in the numeric/temporal promotion order.
    /// This is separate from the lattice join: joining `I32` and `I64` retains two legs.
    pub fn least_upper_bound(types: &[Self]) -> Option<Self> {
        let legs: Vec<_> = types.iter().flat_map(Self::legs).filter(|ty| *ty != Self::Null).collect();
        let mut iter = legs.into_iter();
        let Some(mut result) = iter.next() else {
            return Some(if types.iter().all(|ty| ty.legs().is_empty()) { Self::Nothing } else { Self::Null });
        };
        for next in iter {
            let (Self::Scalar(left), Self::Scalar(right)) = (result, next) else { return None; };
            result = Self::Scalar(left.promote(&right)?);
        }
        Some(result)
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Maybe(value) if !matches!(value.as_ref(), Self::Scalar(_) | Self::List { .. } | Self::Struct(_)) => {
                return Err(Anomaly::incorrect("xtdb/invalid-type", "Maybe requires a non-null monomorphic value type"));
            }
            Self::Maybe(value) => value.validate()?,
            Self::Union(legs) => {
                if legs.iter().filter(|leg| **leg != Self::Null).count() < 2 {
                    return Err(Anomaly::incorrect("xtdb/invalid-type", "A union requires at least two non-null legs"));
                }
                for leg in legs {
                    if !matches!(leg, Self::Null | Self::Scalar(_) | Self::List { .. } | Self::Struct(_)) || leg.unencoded() != *leg {
                        return Err(Anomaly::incorrect("xtdb/invalid-type", "Union legs must be plain monomorphic types"));
                    }
                    leg.validate()?;
                }
            }
            Self::RunEndEncoded { value, .. } => {
                if matches!(value.as_ref(), Self::Nothing | Self::Null | Self::RunEndEncoded { .. }) {
                    return Err(Anomaly::incorrect("xtdb/invalid-type", "Invalid run-end value type"));
                }
                value.validate()?;
            }
            Self::List { kind, element } => {
                if matches!(kind, ListKind::FixedSizeList(size) if *size < 0) {
                    return Err(Anomaly::incorrect("xtdb/invalid-type", "Negative fixed-size list width"));
                }
                element.validate()?;
            }
            Self::Struct(children) => { for child in children.values() { child.validate()?; } }
            Self::Scalar(scalar) => scalar.validate()?,
            Self::Nothing | Self::Null => {}
        }
        Ok(())
    }
}

impl ScalarType {
    fn signed_width(&self) -> Option<u8> {
        match self { Self::I8 => Some(8), Self::I16 => Some(16), Self::I32 => Some(32), Self::I64 => Some(64), _ => None }
    }
    fn float_width(&self) -> Option<u8> {
        match self { Self::F16 => Some(16), Self::F32 => Some(32), Self::F64 => Some(64), _ => None }
    }
    fn is_integer(&self) -> bool { matches!(self, Self::I8 | Self::I16 | Self::I32 | Self::I64 | Self::U8 | Self::U16 | Self::U32 | Self::U64) }
    pub fn promote(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (left, right) if left.signed_width().is_some() && right.signed_width().is_some() => {
                Some(if left.signed_width()? >= right.signed_width()? { left.clone() } else { right.clone() })
            }
            (left, right) if left.float_width().is_some() && right.float_width().is_some() => {
                Some(if left.float_width()? >= right.float_width()? { left.clone() } else { right.clone() })
            }
            (left, right) if left.is_integer() && right.float_width().is_some() => Some(right.clone()),
            (left, right) if left.float_width().is_some() && right.is_integer() => Some(left.clone()),
            (left @ Self::Decimal { .. }, right) if right.is_integer() => Some(left.clone()),
            (left, right @ Self::Decimal { .. }) if left.is_integer() => Some(right.clone()),
            (Self::Decimal { .. }, right) if right.float_width().is_some() => Some(right.clone()),
            (left, Self::Decimal { .. }) if left.float_width().is_some() => Some(left.clone()),
            (Self::Decimal { precision: lp, scale: ls, .. }, Self::Decimal { precision: rp, scale: rs, .. }) => {
                let scale = (*ls).max(*rs);
                let integer_digits = lp.wrapping_sub(*ls).max(rp.wrapping_sub(*rs));
                let precision = integer_digits.wrapping_add(scale);
                let bit_width = if precision <= 9 { 32 } else if precision <= 18 { 64 } else if precision <= 38 { 128 } else { 256 };
                Some(Self::Decimal { precision, scale, bit_width })
            }
            (Self::Duration(left), Self::Duration(right)) => Some(Self::Duration((*left).max(*right))),
            (Self::Time { unit: left, .. }, Self::Time { unit: right, .. }) => {
                let unit = (*left).max(*right);
                let bit_width = if unit >= TimeUnit::Microsecond { 64 } else { 32 };
                Some(Self::Time { unit, bit_width })
            }
            (Self::Timestamp { unit: left, timezone: lz }, Self::Timestamp { unit: right, timezone: rz }) => {
                let timezone = match (lz, rz) {
                    (None, None) => None,
                    (Some(lz), Some(rz)) => Some(if lz == rz { lz.clone() } else { "Z".to_owned() }),
                    _ => return None,
                };
                Some(Self::Timestamp { unit: (*left).max(*right), timezone })
            }
            _ => None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        let invalid = match self {
            Self::FixedSizeBinary(width) => *width < 0,
            Self::Decimal { bit_width, .. } => !matches!(bit_width, 32 | 64 | 128 | 256),
            Self::Time { unit, bit_width } => *bit_width != if *unit >= TimeUnit::Microsecond { 64 } else { 32 },
            _ => false,
        };
        if invalid { Err(Anomaly::incorrect("xtdb/invalid-type", format!("Invalid scalar type {self:?}"))) } else { Ok(()) }
    }
}
