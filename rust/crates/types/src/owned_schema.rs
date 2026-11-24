use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{Anomaly, ListKind, LogicalType, Result, ScalarType, StructFields, TimeUnit, Value};

/// Durable expiry frontier for one immutable creation-lifetime relation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedLifetimeFloor {
    pub duration: i64,
    pub expired_created_through: crate::Timestamp,
    pub swept_present_through: crate::Timestamp,
}

impl OwnedLifetimeFloor {
    /// The immutable hosted expiry replay policy.
    const REPLAY_WINDOW_MICROS: i64 = 24 * 60 * 60 * 1_000_000;

    pub fn expiry_replay_cutoff(ordered_present: crate::Timestamp) -> crate::Timestamp {
        crate::Timestamp(ordered_present.0.saturating_sub(Self::REPLAY_WINDOW_MICROS))
    }

    pub fn expiry_replay_eligible_present(expiry: crate::Timestamp) -> Option<crate::Timestamp> {
        expiry.0.checked_add(Self::REPLAY_WINDOW_MICROS).map(crate::Timestamp)
    }

    pub fn at(duration: i64, present: crate::Timestamp) -> Result<Self> {
        if !(1..=3_155_760_000_000_000).contains(&duration) {
            return Err(invalid("Invalid owned creation lifetime"));
        }
        Ok(Self { duration, expired_created_through: crate::Timestamp(present.0.saturating_sub(duration)), swept_present_through: crate::Timestamp::MIN })
    }

    pub fn record(self, relation: &crate::OwnedRelation) -> StructFields<Value> {
        StructFields::from([
            ("_id".into(), Value::Utf8(relation.name().into())),
            ("duration".into(), Value::I64(self.duration)),
            ("expired_created_through".into(), Value::I64(self.expired_created_through.0)),
            ("swept_present_through".into(), Value::I64(self.swept_present_through.0)),
        ])
    }

    pub fn decode(record: &StructFields<Value>, relation: &crate::OwnedRelation) -> Result<Self> {
        let (Some(Value::Utf8(id)), Some(Value::I64(duration)), Some(Value::I64(floor)), Some(Value::I64(swept))) =
            (record.get("_id"), record.get("duration"), record.get("expired_created_through"), record.get("swept_present_through"))
        else { return Err(invalid("Owned creation lifetime has no exact expiry floor")); };
        if record.len() != 4 || id != relation.name() || !(1..=3_155_760_000_000_000).contains(duration) {
            return Err(invalid("Invalid owned creation-lifetime floor"));
        }
        Ok(Self { duration: *duration, expired_created_through: crate::Timestamp(*floor), swept_present_through: crate::Timestamp(*swept) })
    }

    pub fn admit_recorded_present(self, present: crate::Timestamp) -> Result<()> {
        if present < self.swept_present_through {
            return Err(Anomaly::new(crate::AnomalyCategory::Unavailable, "rtdb.owned/lifetime-collected",
                "The recorded present is outside retained lifetime replay"));
        }
        Ok(())
    }

    pub fn check(self, duration: i64, present: crate::Timestamp) -> Result<()> {
        let allowed = Self::at(duration, present)?;
        if self.duration != duration || self.expired_created_through > allowed.expired_created_through || self.swept_present_through > present {
            return Err(invalid("Owned creation-lifetime floor differs from the declared policy and present"));
        }
        Ok(())
    }
}

/// A Root relation's retirement: the transaction that fenced it and how long its history stays readable after it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnedRetirement {
    pub retired_at: crate::Timestamp,
    pub retain_micros: i64,
}

impl OwnedRetirement {
    /// At most one hundred years, the bound the creation lifetime also takes.
    pub const MAX_RETAIN_MICROS: i64 = 3_155_760_000_000_000;

    pub fn at(retired_at: crate::Timestamp, retain_micros: i64) -> Result<Self> {
        if !(0..=Self::MAX_RETAIN_MICROS).contains(&retain_micros) {
            return Err(invalid("Invalid owned retirement retention"));
        }
        Ok(Self { retired_at, retain_micros })
    }

    /// The first system time at which no reader may still resolve the relation's history.
    pub fn reclaimable_at(self) -> crate::Timestamp {
        crate::Timestamp(self.retired_at.0.saturating_add(self.retain_micros))
    }

    pub fn record(self, relation: &crate::OwnedRelation) -> StructFields<Value> {
        StructFields::from([
            ("_id".into(), Value::Utf8(relation.name().into())),
            ("retired_at".into(), Value::I64(self.retired_at.0)),
            ("retain_micros".into(), Value::I64(self.retain_micros)),
        ])
    }

    pub fn decode(record: &StructFields<Value>) -> Result<(crate::OwnedRelation, Self)> {
        let (Some(Value::Utf8(id)), Some(Value::I64(retired_at)), Some(Value::I64(retain))) =
            (record.get("_id"), record.get("retired_at"), record.get("retain_micros"))
        else { return Err(invalid("Owned retirement has no exact retirement point")); };
        let relation = crate::OwnedRelation::new(id.as_str())?;
        if record.len() != 3 || !relation.is_root_data() { return Err(invalid("Invalid owned retirement record")); }
        Ok((relation, Self::at(crate::Timestamp(*retired_at), *retain)?))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedSchema(OwnedSchemaWire);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedSchemaWire {
    #[serde(default, deserialize_with = "decode_version", skip_serializing_if = "Option::is_none")]
    version: Option<u8>,
    fields: Vec<Field>,
    #[serde(default, skip_serializing_if = "Validity::is_explicit")]
    validity: Validity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lifetime_micros: Option<i64>,
}

fn decode_version<'de, D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Option<u8>, D::Error> {
    match u8::deserialize(deserializer)? {
        2 => Ok(Some(2)),
        _ => Err(serde::de::Error::custom("Unsupported owned declaration version")),
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Validity {
    #[default]
    Explicit,
    Acceptance,
    BoundedPeriod,
}

impl Validity {
    fn is_explicit(&self) -> bool {
        *self == Self::Explicit
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Field {
    name: String,
    #[serde(rename = "type")]
    kind: Kind,
    optional: bool,
    nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Kind {
    Bool {},
    I8 {},
    I16 {},
    I32 {},
    I64 {},
    U8 {},
    U16 {},
    U32 {},
    U64 {},
    F16 {},
    F32 {},
    F64 {},
    Utf8 {},
    Binary {},
    OpaqueBinary {},
    DateDay {},
    Instant {},
    Uuid {},
    Decimal {
        precision: i32,
        scale: i32,
        #[serde(rename = "bitWidth")]
        bit_width: u16,
    },
    Null {},
    Label {},
    Sealed {},
    CanonicalBinary {},
    Struct { fields: Vec<Field> },
    List { element: Box<Kind> },
    Union { legs: Vec<Kind> },
    Fallback { retained: Box<Kind> },
}

impl Kind {
    fn logical(&self) -> LogicalType {
        LogicalType::Scalar(match self {
            Self::Bool {} => ScalarType::Bool,
            Self::I8 {} => ScalarType::I8,
            Self::I16 {} => ScalarType::I16,
            Self::I32 {} => ScalarType::I32,
            Self::I64 {} => ScalarType::I64,
            Self::U8 {} => ScalarType::U8,
            Self::U16 {} => ScalarType::U16,
            Self::U32 {} => ScalarType::U32,
            Self::U64 {} => ScalarType::U64,
            Self::F16 {} => ScalarType::F16,
            Self::F32 {} => ScalarType::F32,
            Self::F64 {} => ScalarType::F64,
            Self::Utf8 {} | Self::Label {} => ScalarType::Utf8,
            Self::Binary {} | Self::OpaqueBinary {} | Self::Sealed {} | Self::CanonicalBinary {} => ScalarType::Binary,
            Self::DateDay {} => ScalarType::DateDay,
            Self::Uuid {} => ScalarType::Uuid,
            Self::Instant {} => ScalarType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            },
            Self::Decimal {
                precision,
                scale,
                bit_width,
            } => ScalarType::Decimal {
                precision: *precision,
                scale: *scale,
                bit_width: *bit_width,
            },
            Self::Null {} => return LogicalType::Null,
            Self::Struct { fields } => return LogicalType::Struct(fields.iter().map(|field| (field.name.clone(), field.logical())).collect()),
            Self::List { element } => return LogicalType::list(element.logical()),
            Self::Union { legs } => return LogicalType::from_legs(legs.iter().map(Self::logical)),
            Self::Fallback { retained } => return retained.logical().join(&Self::Binary {}.logical()),
        })
    }

    fn is_opaque(&self) -> bool {
        matches!(self, Self::OpaqueBinary {} | Self::Sealed {})
    }

    fn path_logical(&self) -> Option<LogicalType> {
        match self {
            Self::OpaqueBinary {} | Self::Sealed {} | Self::CanonicalBinary {} | Self::Fallback { .. }
            | Self::Null {} | Self::Union { .. } | Self::Struct { .. } | Self::List { .. } => None,
            _ => Some(self.logical()),
        }
    }

    fn accepts_type(&self, ty: &LogicalType) -> bool {
        match ty {
            LogicalType::Union(legs) => return legs.iter().all(|leg| self.accepts_type(leg)),
            LogicalType::Maybe(inner) => return self.accepts_type(inner) && self.accepts_type(&LogicalType::Null),
            _ => {}
        }
        match (self, ty) {
            (Self::Instant {}, LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some(_) })) => true,
            (Self::Union { legs }, ty) => legs.iter().any(|leg| leg.accepts_type(ty)),
            (Self::Fallback { retained }, ty) => Self::CanonicalBinary {}.accepts_type(ty) || retained.accepts_type(ty),
            (Self::List { element }, LogicalType::List { kind: ListKind::List, element: actual }) => {
                **actual == LogicalType::Nothing || element.accepts_type(actual)
            }
            (Self::Struct { fields }, LogicalType::Struct(actual)) => {
                fields.iter().all(|field| field.accepts_type(actual.get(&field.name)))
                    && actual.keys().all(|name| fields.iter().any(|field| &field.name == name))
            }
            (kind, ty) => kind.logical() == *ty,
        }
    }

    fn accepts(&self, value: &Value) -> bool {
        match (self, value) {
            (Self::Struct { fields }, Value::Struct(actual)) => {
                fields.iter().all(|field| field.accepts(actual.get(&field.name)))
                    && actual.iter().all(|(name, value)| matches!(value, Value::Undefined) || fields.iter().any(|field| &field.name == name))
            }
            (Self::List { element }, Value::List(actual)) => match actual.view() {
                crate::ListView::Values(values) => values.iter().all(|value| element.accepts(value)),
                _ => self.accepts_type(&value.logical_type()),
            },
            (Self::Union { legs }, value) => legs.iter().any(|leg| leg.accepts(value)),
            (Self::Fallback { retained }, value) => matches!(value, Value::Binary(_)) || retained.accepts(value),
            (_, Value::List(_) | Value::Set(_) | Value::Map { .. } | Value::FixedSizeList(_) | Value::TimestampTzRange { .. } | Value::Struct(_)) => false,
            (kind, value) => kind.accepts_type(&value.logical_type()),
        }
    }

    fn validate(&self, v2: bool, depth: usize) -> Result<()> {
        if depth > 16 {
            return Err(invalid("Owned field exceeds its nesting limit"));
        }
        match self {
            Self::Decimal { precision, scale, bit_width } => {
                let maximum = match bit_width { 128 => 38, 256 => 76, _ => 0 };
                if !(1..=maximum).contains(precision) || !(-128..=*precision).contains(scale) {
                    return Err(invalid("Invalid owned decimal precision, scale or width"));
                }
            }
            Self::List { element } if v2 => element.validate(v2, depth + 1)?,
            Self::Struct { fields } if v2 => validate_fields(fields, v2, depth + 1)?,
            Self::Union { legs } if v2 => {
                if !(2..=OwnedSchema::MAX_UNION_LEGS).contains(&legs.len()) {
                    return Err(invalid("Invalid owned union leg count"));
                }
                for (index, leg) in legs.iter().enumerate() {
                    if matches!(leg, Self::Union { .. } | Self::Fallback { .. }) || leg.is_opaque()
                        || legs[..index].iter().any(|other| leg.same_slot(other))
                    {
                        return Err(invalid("Owned union legs must have distinct physical kinds"));
                    }
                    leg.validate(v2, depth + 1)?;
                }
            }
            Self::Fallback { retained } if v2 && depth == 0 && !retained.is_opaque()
                && !matches!(**retained, Self::Fallback { .. } | Self::CanonicalBinary {}) => retained.validate(v2, depth)?,
            Self::Sealed {} | Self::CanonicalBinary {} | Self::Label {} if v2 && depth == 0 => {},
            Self::Null {} if v2 && depth > 0 => {},
            Self::Null {} | Self::Label {} | Self::Sealed {} | Self::CanonicalBinary {}
            | Self::List { .. } | Self::Struct { .. } | Self::Union { .. } | Self::Fallback { .. } => {
                return Err(invalid("Owned field kind requires a compatible version 2 layout"));
            }
            Self::OpaqueBinary {} if depth > 0 => return Err(invalid("Opaque fields must be top-level columns")),
            _ => {}
        }
        Ok(())
    }

    fn same_slot(&self, other: &Self) -> bool {
        matches!((self, other), (Self::Struct { .. }, Self::Struct { .. }) | (Self::List { .. }, Self::List { .. }))
            || self.logical() == other.logical()
    }

    fn widened(&self, additional: &Self) -> Self {
        match (self, additional) {
            (a, b) if a == b => a.clone(),
            (a @ (Self::CanonicalBinary {} | Self::Fallback { .. }), _) => a.clone(),
            (a, Self::CanonicalBinary {}) => Self::Fallback { retained: Box::new(a.clone()) },
            (Self::Struct { fields }, Self::Struct { fields: next }) => Self::Struct { fields: widen_fields(fields, next) },
            (Self::List { element }, Self::List { element: next }) => Self::List { element: Box::new(element.widened(next)) },
            (a, b) => {
                let mut legs = match a { Self::Union { legs } => legs.clone(), _ => vec![a.clone()] };
                let incoming = match b { Self::Union { legs } => legs.as_slice(), _ => std::slice::from_ref(b) };
                for leg in incoming {
                    match legs.iter_mut().find(|old| old.same_slot(leg)) {
                        Some(old @ (Self::Struct { .. } | Self::List { .. })) if old != leg => *old = old.widened(leg),
                        Some(_) => {},
                        None => legs.push(leg.clone()),
                    }
                }
                Self::Union { legs }
            }
        }
    }

    fn retained_bytes(&self) -> usize {
        match self {
            Self::Struct { fields } => retained_fields(fields),
            Self::List { element } | Self::Fallback { retained: element } => std::mem::size_of::<Self>() + element.retained_bytes(),
            Self::Union { legs } => legs.capacity() * std::mem::size_of::<Self>() + legs.iter().map(Self::retained_bytes).sum::<usize>(),
            _ => 0,
        }
    }
}

impl Field {
    fn logical(&self) -> LogicalType {
        let ty = self.kind.logical();
        if self.optional || self.nullable { ty.maybe() } else { ty }
    }

    fn accepts_type(&self, ty: Option<&LogicalType>) -> bool {
        match ty {
            None | Some(LogicalType::Nothing) => self.optional,
            Some(LogicalType::Null) => self.nullable,
            Some(LogicalType::Maybe(ty)) if self.nullable => self.kind.accepts_type(ty),
            Some(LogicalType::Union(legs)) => legs.iter().all(|leg| match leg {
                LogicalType::Null => self.nullable,
                other => self.kind.accepts_type(other),
            }),
            Some(ty) => self.kind.accepts_type(ty),
        }
    }

    fn accepts(&self, value: Option<&Value>) -> bool {
        match value {
            None | Some(Value::Undefined) => self.optional,
            Some(Value::Null) => self.nullable,
            Some(value) => self.kind.accepts(value),
        }
    }
}

fn retained_fields(fields: &Vec<Field>) -> usize {
    fields.capacity() * (std::mem::size_of::<Field>() + 64)
        + fields.iter().map(|field| field.name.capacity() + field.kind.retained_bytes()).sum::<usize>()
}

fn validate_fields(fields: &[Field], v2: bool, depth: usize) -> Result<()> {
    if fields.len() > 128 {
        return Err(invalid("An owned declaration accepts at most 128 fields"));
    }
    let mut names = BTreeSet::new();
    for field in fields {
        if field.name.is_empty() || field.name.len() > 128 || field.name.starts_with('_')
            || field.name.chars().any(char::is_control) || !names.insert(&field.name)
        {
            return Err(invalid("Owned declaration field names must be distinct, nonempty and unreserved"));
        }
        field.kind.validate(v2, depth)?;
    }
    Ok(())
}

fn widen_fields(existing: &[Field], additional: &[Field]) -> Vec<Field> {
    let mut fields: Vec<_> = existing.iter().map(|field| {
        if let Some(next) = additional.iter().find(|next| next.name == field.name) {
            Field {
                name: field.name.clone(),
                kind: field.kind.widened(&next.kind),
                optional: field.optional || next.optional,
                nullable: field.nullable || next.nullable,
            }
        } else {
            Field { optional: true, ..field.clone() }
        }
    }).collect();
    fields.extend(additional.iter().filter(|next| !existing.iter().any(|old| old.name == next.name))
        .map(|field| Field { optional: true, ..field.clone() }));
    fields
}

impl OwnedSchema {
    pub const MAX_JSON_BYTES: usize = 64 * 1024;
    pub const MAX_UNION_LEGS: usize = 16;
    pub const MAX_FIELD_KIND_BYTES: usize = 4096;

    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > Self::MAX_JSON_BYTES {
            return Err(invalid("Owned declaration exceeds its byte limit"));
        }
        let schema: OwnedSchemaWire = serde_json::from_slice(bytes)
            .map_err(|_| invalid("Invalid owned declaration fields or type tags"))?;
        if schema.lifetime_micros.is_some_and(|lifetime|
            !(1..=3_155_760_000_000_000).contains(&lifetime) || schema.validity != Validity::Acceptance)
        {
            return Err(invalid("Owned creation lifetime requires acceptance validity and a positive bounded duration"));
        }
        let schema = Self(schema);
        validate_fields(&schema.0.fields, schema.is_v2(), 0)?;
        if schema.is_v2() && schema.0.fields.iter().any(|field| !kind_fits(&field.kind)) {
            return Err(invalid("Owned field exceeds its layout size limit"));
        }
        Ok(schema)
    }

    pub fn is_v2(&self) -> bool { self.0.version == Some(2) }

    pub fn widened(&self, additional: &Self) -> Result<Option<Self>> {
        if !self.is_v2() || !additional.is_v2()
            || self.0.validity != additional.0.validity || self.lifetime_micros() != additional.lifetime_micros()
            || self.0.fields.len() != additional.0.fields.len()
        {
            return Ok(None);
        }
        let mut next = self.clone();
        for (old, target) in self.0.fields.iter().zip(&mut next.0.fields) {
            let Some(incoming) = additional.0.fields.iter().find(|field| field.name == old.name) else { return Ok(None); };
            if incoming.kind != old.kind && (old.kind.is_opaque() || incoming.kind.is_opaque()
                || matches!(incoming.kind, Kind::Fallback { .. } | Kind::Label {}) || matches!(old.kind, Kind::Label {}))
            {
                return Ok(None);
            }
            target.optional |= incoming.optional;
            target.nullable |= incoming.nullable;
            let kind = old.kind.widened(&incoming.kind);
            target.kind = if kind.validate(true, 0).is_ok() && kind_fits(&kind) {
                kind
            } else {
                Kind::Fallback { retained: Box::new(old.kind.clone()) }
            };
        }
        if next.to_json()?.len() > Self::MAX_JSON_BYTES {
            return Err(invalid("Owned declaration exceeds its byte limit"));
        }
        Ok(Some(next))
    }

    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.0).map_err(|_| {
            Anomaly::fault(
                "rtdb.owned/schema-encode",
                "Cannot encode owned declaration",
            )
        })
    }

    pub fn retained_bytes(&self) -> usize {
        retained_fields(&self.0.fields)
    }

    /// Only declared clear fields may contribute to an ordered or derived path.
    pub fn has_opaque_columns(&self) -> bool {
        self.0.fields.iter().any(|field| field.kind.is_opaque())
    }

    pub(crate) fn has_only_opaque_column(&self, name: &str) -> bool {
        let mut opaque = self.0.fields.iter().filter(|field| field.kind.is_opaque());
        opaque.next().is_some_and(|field| field.name == name) && opaque.next().is_none()
    }

    pub fn path_column_type(&self, name: &str) -> Option<ScalarType> {
        match self.path_field(name)?.kind.path_logical()? {
            LogicalType::Scalar(ty) => Some(ty),
            _ => None,
        }
    }

    pub fn search_column_logical_type(&self, name: &str) -> Option<LogicalType> {
        if self.path_column_type(name) == Some(ScalarType::Utf8) {
            return self.path_column_logical_type(name);
        }
        let field = self.path_field(name)?;
        let Kind::Struct { fields } = &field.kind else { return None; };
        let [value] = fields.as_slice() else { return None; };
        (self.is_v2() && field.optional && !field.nullable
            && value.name == crate::OWNED_FACT_TEXT_VALUE && !value.optional && value.nullable
            && matches!(&value.kind, Kind::Utf8 {}))
            .then(|| field.logical())
    }

    pub fn path_column_logical_type(&self, name: &str) -> Option<LogicalType> {
        self.path_field(name).and_then(|field| {
            let ty = field.kind.path_logical()?;
            if field.optional || field.nullable {
                Some(ty.maybe())
            } else {
                Some(ty)
            }
        })
    }

    pub fn lifetime_micros(&self) -> Option<i64> { self.0.lifetime_micros }

    pub fn has_acceptance_validity(&self) -> bool {
        self.0.validity == Validity::Acceptance
    }

    pub fn requires_bounded_period(&self) -> bool {
        self.0.validity == Validity::BoundedPeriod
    }

    pub fn require_root_write_period(
        &self, from: Option<crate::Timestamp>, to: Option<crate::Timestamp>,
    ) -> Result<()> {
        if self.requires_bounded_period() {
            if !matches!((from, to), (Some(from), Some(to))
                if crate::Timestamp::MIN < from && from < to && to < crate::Timestamp::MAX)
            {
                return Err(Anomaly::incorrect(
                    "rtdb.owned/validity-policy",
                    "This root relation requires an explicit finite nonempty valid period",
                ));
            }
            Ok(())
        } else {
            self.require_write_validity(from.is_some() || to.is_some())
        }
    }

    pub fn require_write_validity(&self, explicit: bool) -> Result<()> {
        if explicit && self.0.validity == Validity::Acceptance {
            return Err(Anomaly::incorrect(
                "rtdb.owned/validity-policy",
                "This root relation requires validity from acceptance with an open end",
            ));
        }
        Ok(())
    }

    /// Validate an interval already accepted by the original held installation.
    /// The destination ordering time does not extend the original lifetime.
    pub fn require_replayed_period(
        &self, valid: crate::Interval, source: crate::Timestamp, operation: crate::EventOp,
    ) -> Result<()> {
        use crate::{EventOp, Timestamp};
        let from = valid.lower();
        let to = valid.upper();
        let accepted = !valid.is_empty() && match (self.0.validity, self.0.lifetime_micros, operation) {
            (_, _, EventOp::Erase | EventOp::Redacted) => false,
            (Validity::Explicit, _, _) => true,
            (Validity::BoundedPeriod, _, _) => from > Timestamp::MIN && to < Timestamp::MAX,
            (Validity::Acceptance, None, _) => from > Timestamp::MIN && from <= source && to == Timestamp::MAX,
            (Validity::Acceptance, Some(duration), EventOp::Put) => from.0 > 0 && from <= source
                && from.0.checked_add(duration) == Some(to.0),
            (Validity::Acceptance, Some(_), EventOp::Delete) => false,
        };
        if !accepted {
            return Err(Anomaly::incorrect("rtdb.owned/restore-validity", "The copied fact interval disagrees with its original declaration"));
        }
        Ok(())
    }

    fn path_field(&self, name: &str) -> Option<&Field> {
        self.0.fields.iter().find(|field| field.name == name)
    }

    pub fn validate_types(&self, types: &StructFields<LogicalType>) -> Result<()> {
        for field in &self.0.fields {
            let ty = types.get(field.name.as_str());
            let accepts = (self.is_v2() || !matches!(ty, Some(LogicalType::Union(_) | LogicalType::Maybe(_))))
                && field.accepts_type(ty);
            if !accepts { return Err(nonconforming(&field.name)); }
        }
        for (name, ty) in types {
            if !matches!(name.as_str(), "_id" | "_valid_from" | "_valid_to")
                && ty != &LogicalType::Nothing && !self.0.fields.iter().any(|field| &field.name == name)
            { return Err(nonconforming(name)); }
        }
        Ok(())
    }

    pub fn validate_row(&self, row: &StructFields<Value>) -> Result<()> {
        for field in &self.0.fields {
            if !field.accepts(row.get(field.name.as_str())) {
                return Err(nonconforming(&field.name));
            }
        }
        for (name, value) in row {
            if !matches!(name.as_str(), "_id" | "_valid_from" | "_valid_to")
                && !matches!(value, Value::Undefined)
                && !self.0.fields.iter().any(|field| &field.name == name)
            {
                return Err(nonconforming(name));
            }
        }
        Ok(())
    }
}

fn kind_fits(kind: &Kind) -> bool {
    let bounded = match kind { Kind::Fallback { retained } => retained.as_ref(), kind => kind };
    serde_json::to_vec(bounded).is_ok_and(|bytes| bytes.len() <= OwnedSchema::MAX_FIELD_KIND_BYTES)
}

fn invalid(message: &'static str) -> Anomaly {
    Anomaly::incorrect("rtdb.owned/invalid-declaration", message)
}

fn nonconforming(field: &str) -> Anomaly {
    Anomaly::incorrect(
        "rtdb.owned/nonconforming",
        "Owned row differs from its declaration",
    )
    .with_context("field", field.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versioned_field(kind: &str) -> OwnedSchema {
        OwnedSchema::from_json(format!(r#"{{"version":2,"fields":[{{"name":"value","type":{kind},"optional":true,"nullable":false}}]}}"#).as_bytes()).unwrap()
    }

    #[test]
    fn v2_owned_shape_widens_without_changing_v1_bytes() -> Result<()> {
        let original = br#"{"fields":[{"name":"value","type":{"kind":"i64"},"optional":true,"nullable":false}]}"#;
        let v1 = OwnedSchema::from_json(original)?;
        let integers = versioned_field(r#"{"kind":"i64"}"#);
        let objects = versioned_field(r#"{"kind":"struct","fields":[{"name":"list","type":{"kind":"list","element":{"kind":"union","legs":[{"kind":"bool"},{"kind":"null"}]}},"optional":true,"nullable":false}]}"#);
        let joined = integers.widened(&objects)?.unwrap();
        let row = |value| StructFields::from([("value".to_owned(), value)]);
        let object = |list| Value::Struct(StructFields::from([("list".to_owned(), list)]));
        assert!(joined.validate_row(&row(Value::I64(9))).is_ok());
        assert!(joined.validate_row(&row(object(Value::list(vec![Value::Bool(true), Value::Null])))).is_ok());
        assert!(joined.validate_row(&row(object(Value::Null))).is_err());
        assert!(joined.validate_row(&row(Value::Null)).is_err());
        assert!(joined.validate_row(&StructFields::new()).is_ok());
        assert_eq!(joined, OwnedSchema::from_json(&joined.to_json()?)?);
        assert!(v1.widened(&integers)?.is_none());
        assert_eq!(v1.to_json()?, original);
        Ok(())
    }

    #[test]
    fn v2_owned_shape_caps_widening_per_field() -> Result<()> {
        let decimal = |scale| versioned_field(&format!(r#"{{"kind":"decimal","precision":38,"scale":{scale},"bitWidth":128}}"#));
        let start = decimal(0);
        let mut schema = start.clone();
        for scale in 1..OwnedSchema::MAX_UNION_LEGS {
            schema = schema.widened(&decimal(scale))?.unwrap();
        }
        let fallback = schema.widened(&decimal(OwnedSchema::MAX_UNION_LEGS))?.unwrap();
        let ty = |ty| StructFields::from([("value".to_owned(), LogicalType::Scalar(ty))]);
        assert!(fallback.validate_types(&ty(ScalarType::Binary)).is_ok());
        assert!(fallback.validate_types(&ty(ScalarType::Decimal { precision: 38, scale: 0, bit_width: 128 })).is_ok());
        assert!(fallback.validate_types(&ty(ScalarType::Decimal { precision: 38, scale: 16, bit_width: 128 })).is_err());
        assert_eq!(fallback.widened(&start)?, Some(fallback.clone()));
        assert_eq!(fallback, OwnedSchema::from_json(&fallback.to_json()?)?);
        Ok(())
    }

    #[test]
    fn replay_keeps_the_first_creation_and_its_original_expiry() -> Result<()> {
        use crate::{EventOp, Interval, Timestamp};
        let schema = OwnedSchema::from_json(br#"{"fields":[],"validity":"acceptance","lifetime_micros":10}"#)?;
        let original = Interval::from_micros(20, 30)?;
        assert!(schema.require_replayed_period(original, Timestamp(40), EventOp::Put).is_ok());
        assert!(schema.require_replayed_period(Interval::from_micros(40, 50)?, Timestamp(20), EventOp::Put).is_err());
        assert!(schema.require_replayed_period(Interval::from_micros(20, 50)?, Timestamp(40), EventOp::Put).is_err());
        assert!(schema.require_replayed_period(original, Timestamp(40), EventOp::Delete).is_err());
        Ok(())
    }

    #[test]
    fn declarations_are_closed_over_names_and_types() {
        for payload in [
            r#"{"fields":[{"name":"value","type":{"kind":"i64","extra":1},"optional":false,"nullable":false}]}"#,
            r#"{"fields":[{"name":"value","type":{"kind":"integer"},"optional":false,"nullable":false}]}"#,
            r#"{"fields":[{"name":"_id","type":{"kind":"i64"},"optional":false,"nullable":false}]}"#,
            r#"{"fields":[{"name":"value","type":{"kind":"decimal","precision":39,"scale":2,"bitWidth":128},"optional":false,"nullable":false}]}"#,
        ] {
            assert!(OwnedSchema::from_json(payload.as_bytes()).is_err());
        }
    }

    #[test]
    fn optional_and_nullable_are_independent() {
        let schema = OwnedSchema::from_json(
            br#"{"fields":[
            {"name":"optional","type":{"kind":"i64"},"optional":true,"nullable":false},
            {"name":"nullable","type":{"kind":"u64"},"optional":false,"nullable":true}
        ]}"#,
        )
        .unwrap();
        let mut row = StructFields::new();
        assert!(schema.validate_row(&row).is_err());
        row.insert("nullable".into(), Value::Null);
        assert!(schema.validate_row(&row).is_ok());
        row.insert("optional".into(), Value::Null);
        assert!(schema.validate_row(&row).is_err());
        row.insert("optional".into(), Value::Undefined);
        row.insert("nullable".into(), Value::I64(1));
        assert!(schema.validate_row(&row).is_err());
        row.insert("nullable".into(), Value::U64(u64::MAX));
        assert!(schema.validate_row(&row).is_ok());
    }
}
