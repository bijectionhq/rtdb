use std::num::NonZeroU32;

use crate::{Anomaly, EntityId, Interval, Result, StructFields, Timestamp, Value};

pub const OWNED_CHANGE_GROUP_BYTES: usize = 1024 * 1024;
pub const OWNED_CHANGE_PAGE_BYTES: usize = 8 * OWNED_CHANGE_GROUP_BYTES;
pub const OWNED_CHANGE_PAGE_GROUPS: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedChangeKind {
    Put,
    Delete,
    Redacted,
    Erase,
    Expired(Interval),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedChange {
    pub id: EntityId,
    pub kind: OwnedChangeKind,
    pub valid: Interval,
    pub assertion_ordinal: Option<NonZeroU32>,
}

impl OwnedChange {
    pub fn to_value(&self) -> Value {
        let mut fields = StructFields::from([
            ("_id".into(), self.id.as_value()),
            (
                "op".into(),
                Value::Utf8(
                    match self.kind {
                        OwnedChangeKind::Put => "put",
                        OwnedChangeKind::Delete => "delete",
                        OwnedChangeKind::Redacted => "redacted",
                        OwnedChangeKind::Erase => "erase",
                        OwnedChangeKind::Expired(_) => "expired",
                    }
                    .into(),
                ),
            ),
            ("valid_from_micros".into(), Value::I64(self.valid.lower().0)),
            (
                "valid_to_micros".into(),
                if self.valid.upper() == Timestamp::MAX {
                    Value::Null
                } else {
                    Value::I64(self.valid.upper().0)
                },
            ),
        ]);
        if let OwnedChangeKind::Expired(lifetime) = self.kind {
            fields.insert("created_micros".into(), Value::I64(lifetime.lower().0));
            fields.insert("expiry_micros".into(), Value::I64(lifetime.upper().0));
        }
        if let Some(ordinal) = self.assertion_ordinal {
            fields.insert("assertion_ordinal".into(), Value::I64(i64::from(ordinal.get())));
        }
        Value::Struct(fields)
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let Value::Struct(fields) = value else {
            return Err(change_feed_uncovered());
        };
        let id = fields
            .get("_id")
            .ok_or_else(change_feed_uncovered)?
            .owned_entity_id()
            .map_err(|_| change_feed_uncovered())?;
        let kind = match fields.get("op") {
            Some(Value::Utf8(op)) if op == "put" => OwnedChangeKind::Put,
            Some(Value::Utf8(op)) if op == "delete" => OwnedChangeKind::Delete,
            Some(Value::Utf8(op)) if op == "redacted" => OwnedChangeKind::Redacted,
            Some(Value::Utf8(op)) if op == "erase" => OwnedChangeKind::Erase,
            Some(Value::Utf8(op)) if op == "expired" => {
                let (Some(Value::I64(created)), Some(Value::I64(expiry))) =
                    (fields.get("created_micros"), fields.get("expiry_micros"))
                else { return Err(change_feed_uncovered()); };
                OwnedChangeKind::Expired(expired_lifetime_interval(Timestamp(*created), Timestamp(*expiry))?)
            },
            _ => return Err(change_feed_uncovered()),
        };
        let Some(Value::I64(from)) = fields.get("valid_from_micros") else {
            return Err(change_feed_uncovered());
        };
        let to = match fields.get("valid_to_micros") {
            Some(Value::I64(to)) if *to != Timestamp::MAX.0 => Timestamp(*to),
            Some(Value::Null) => Timestamp::MAX,
            _ => return Err(change_feed_uncovered()),
        };
        let valid = Interval::new(Timestamp(*from), to).map_err(|_| change_feed_uncovered())?;
        let ordinal = fields.get("assertion_ordinal");
        let assertion_ordinal = match ordinal {
            None | Some(Value::Null | Value::Undefined) => None,
            Some(Value::I64(ordinal)) => Some(
                u32::try_from(*ordinal).ok().and_then(NonZeroU32::new).ok_or_else(change_feed_uncovered)?,
            ),
            _ => return Err(change_feed_uncovered()),
        };
        let fields_len = fields.len() - usize::from(ordinal.is_some());
        let shape = if matches!(kind, OwnedChangeKind::Expired(_)) {
            fields_len == 6
        } else {
            fields_len == 4 || (fields_len == 6
                && ["created_micros", "expiry_micros"].iter().all(|name|
                    matches!(fields.get(*name), Some(Value::Null | Value::Undefined))))
        };
        if !shape || valid.is_empty()
            || (matches!(kind, OwnedChangeKind::Erase | OwnedChangeKind::Expired(_)) && valid != Interval::ALL)
        {
            return Err(change_feed_uncovered());
        }
        Ok(Self { id, kind, valid, assertion_ordinal })
    }
}

pub fn expired_lifetime_interval(created: Timestamp, expiry: Timestamp) -> Result<Interval> {
    if created.0 <= 0 || expiry <= created || expiry == Timestamp::MAX {
        return Err(change_feed_uncovered());
    }
    Interval::new(created, expiry).map_err(|_| change_feed_uncovered())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedEntityRegion {
    pub id: EntityId,
    pub valid: Interval,
}

impl OwnedEntityRegion {
    pub fn intersects(&self, touch: &OwnedChange) -> bool {
        self.id == touch.id && self.valid.intersects(touch.valid)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedRangeEvidence {
    FixedPath(crate::FixedRangeEvidence),
    Entity { anchor: i64, region: OwnedEntityRegion },
}

impl OwnedRangeEvidence {
    pub fn retained_bytes(&self) -> usize {
        match self {
            Self::FixedPath(fixed) => fixed.retained_bytes(),
            Self::Entity { region, .. } => 1024 + region.id.retained_bytes(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedChangeGroup {
    pub revision: u64,
    pub system_time: Timestamp,
    pub touches: Vec<OwnedChange>,
}

pub fn change_feed_uncovered() -> Anomaly {
    Anomaly::new(
        crate::AnomalyCategory::Unavailable,
        "rtdb.owned/change-feed-uncovered",
        "The held cut does not cover this owned change feed",
    )
}
