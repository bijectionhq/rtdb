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
        Self::decode_fields(fields, id)
    }

    /// Decodes a retained journal touch and takes ownership of its identity payload.
    pub fn from_owned_value(value: Value) -> Result<Self> {
        let Value::Struct(mut fields) = value else {
            return Err(change_feed_uncovered());
        };
        let id = fields.get_mut("_id").ok_or_else(change_feed_uncovered)?;
        let id = match std::mem::replace(id, Value::Undefined) {
            Value::Utf8(value) => EntityId::String(value),
            Value::Keyword(value) => EntityId::Keyword(value),
            Value::Binary(value) => EntityId::Binary(value),
            value => value.owned_entity_id().map_err(|_| change_feed_uncovered())?,
        };
        Self::decode_fields(&fields, id)
    }

    fn decode_fields(fields: &StructFields<Value>, id: EntityId) -> Result<Self> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_touch_takes_the_decoded_identity_allocation() {
        for value in [
            Value::Binary(vec![0, 255, 0, 1]),
            Value::Utf8("text/λ".into()),
            Value::Keyword(crate::Keyword::from_name(":stored/name")),
        ] {
            let address = match &value {
                Value::Binary(value) => value.as_ptr(),
                Value::Utf8(value) => value.as_ptr(),
                Value::Keyword(value) => value.as_str().as_ptr(),
                _ => unreachable!(),
            };
            let touch = Value::Struct(StructFields::from([
                ("_id".into(), value),
                ("op".into(), Value::Utf8("put".into())),
                ("valid_from_micros".into(), Value::I64(20)),
                ("valid_to_micros".into(), Value::Null),
                ("assertion_ordinal".into(), Value::I64(7)),
            ]));
            let expected = OwnedChange::from_value(&touch).unwrap();
            let decoded = OwnedChange::from_owned_value(touch).unwrap();
            assert_eq!(decoded, expected);
            let actual = match &decoded.id {
                EntityId::Binary(value) => value.as_ptr(),
                EntityId::String(value) => value.as_ptr(),
                EntityId::Keyword(value) => value.as_str().as_ptr(),
                _ => unreachable!(),
            };
            assert_eq!(actual, address);
        }
    }

    #[test]
    fn retained_touch_matches_checked_identity_and_shape_admission() {
        let touch = |id, ordinal| Value::Struct(StructFields::from([
            ("_id".into(), id),
            ("op".into(), Value::Utf8("put".into())),
            ("valid_from_micros".into(), Value::I64(20)),
            ("valid_to_micros".into(), Value::I64(21)),
            ("created_micros".into(), Value::Undefined),
            ("expiry_micros".into(), Value::Null),
            ("assertion_ordinal".into(), ordinal),
        ]));
        for value in [
            touch(Value::I8(-1), Value::Undefined),
            touch(Value::I16(-257), Value::Null),
            touch(Value::I32(-65_537), Value::I64(2)),
            touch(Value::I64(i64::MIN), Value::I64(3)),
            touch(Value::Uuid([0xa3; 16]), Value::I64(1)),
        ] {
            assert_eq!(OwnedChange::from_owned_value(value.clone()).unwrap(), OwnedChange::from_value(&value).unwrap());
        }
        for value in [
            touch(Value::U8(1), Value::Null),
            touch(Value::FixedSizeBinary(vec![0; 16]), Value::I64(1)),
            touch(Value::Binary(vec![0xff]), Value::I64(0)),
            {
                let Value::Struct(mut fields) = touch(Value::Uuid([0xa3; 16]), Value::Null) else {
                    unreachable!();
                };
                fields.insert("extra".into(), Value::Null);
                Value::Struct(fields)
            },
        ] {
            assert_eq!(OwnedChange::from_owned_value(value.clone()).unwrap_err().code, OwnedChange::from_value(&value).unwrap_err().code);
        }
    }
}
