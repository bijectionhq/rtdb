use std::ops::Range;

use prost::Message;
use rtdb_types::{Anomaly, EventHeader, EventOp, EventOrigin, EventPosition, Iid, Interval, Result, Timestamp};
use sha2::{Digest, Sha256};

use crate::{
    cow_tree::{PageDigest, RootRef, Row},
    keys::{TableSlug, TrieKey},
    log::proto::{ArtefactRecord, EntityDirectoryChange, EntityDirectoryRoot},
};

pub const RECORD_KIND: &str = "entity-directory";
const VERSION: i64 = 1;
const KEY_PREFIX: usize = 19;
const POSITION: usize = 20;
const FRONTIER: u8 = 255;

pub fn prefix(slug: &TableSlug) -> String {
    format!("{}/{RECORD_KIND}", slug.table_prefix())
}

pub fn page_digest(key: &str) -> Option<PageDigest> { crate::cow_tree::table_page_digest(key, RECORD_KIND) }

impl EntityDirectoryRoot {
    pub fn encoded_size(&self) -> usize {
        self.encoded_len()
    }

    pub fn reference(&self) -> Result<RootRef> {
        if self.through_block.is_none()
            || self.through_block() < 0
            || self.source_rows() <= 0
            || self.allocation().len() != 16
            || self.writer_fence.is_none()
            || self.erase_frontier_digest().len() != 32
        {
            return Err(invalid("Invalid directory receipt"));
        }
        let root = RootRef::decode(self.root())?;
        if !root.summary.is_empty() {
            return Err(invalid("Unexpected entity directory summary"));
        }
        Ok(root)
    }
}

impl EntityDirectoryChange {
    pub fn from_artefact(record: &ArtefactRecord) -> Result<Self> {
        if record.kind() != RECORD_KIND
            || record.definition_id() != RECORD_KIND
            || record.definition_version() != VERSION
        {
            return Err(invalid("Invalid directory record kind"));
        }
        let change = Self::decode(record.stats()).map_err(invalid)?;
        change
            .root
            .as_ref()
            .ok_or_else(|| invalid("Missing directory root"))?
            .reference()?;
        if !change.replaces_root().is_empty() {
            RootRef::decode(change.replaces_root())?;
        }
        Ok(change)
    }

    pub fn to_artefact(&self) -> Result<ArtefactRecord> {
        self.root
            .as_ref()
            .ok_or_else(|| invalid("Missing directory root"))?
            .reference()?;
        Ok(ArtefactRecord {
            kind: Some(RECORD_KIND.into()),
            definition_id: Some(RECORD_KIND.into()),
            definition_version: Some(VERSION),
            stats: Some(self.encode_to_vec()),
            ..Default::default()
        })
    }
}

#[derive(Clone, Debug)]
pub struct LogicalEvent {
    pub header: EventHeader,
    pub origin: EventOrigin,
    pub entity_key: Option<Vec<u8>>,
    pub payload_hint: Option<(TrieKey, u32, u32)>,
}

pub fn all_rows() -> Range<Vec<u8>> {
    vec![]..vec![255; KEY_PREFIX + u16::MAX as usize + POSITION + 1]
}

pub fn iid_rows(iid: Iid) -> Range<Vec<u8>> {
    let first = iid_prefix(iid);
    let mut last = first.clone();
    if let Some(i) = last.iter().rposition(|byte| *byte != 255) {
        last[i] += 1;
        last.truncate(i + 1);
    } else {
        last = all_rows().end;
    }
    first..last
}

fn iid_prefix(iid: Iid) -> Vec<u8> {
    let mut key = Vec::with_capacity(KEY_PREFIX);
    key.push(iid.0[0]);
    key.extend(iid.0);
    key
}

fn entity_prefix(iid: Iid, identity: &[u8]) -> Result<Vec<u8>> {
    let mut key = iid_prefix(iid);
    key.extend(
        u16::try_from(identity.len())
            .map_err(invalid)?
            .to_be_bytes(),
    );
    key.extend(identity);
    Ok(key)
}

pub fn entity_rows(iid: Iid, identity: Option<&[u8]>) -> Result<Range<Vec<u8>>> {
    let first = entity_prefix(iid, identity.unwrap_or_default())?;
    let mut last = first.clone();
    last.extend([255; POSITION + EventPosition::WIDTH as usize + 1]);
    Ok(first..last)
}

impl LogicalEvent {
    pub fn position(&self) -> ((Timestamp, i128), u64) {
        (self.header.precedence(), self.origin.ordinal())
    }

    pub fn is_frontier(row: &Row) -> bool {
        row.key.get(17..19).is_some_and(|len| {
            let len = u16::from_be_bytes(len.try_into().expect("key length width")) as usize;
            row.key.get(KEY_PREFIX + len..) == Some(&[FRONTIER][..])
        })
    }

    pub fn encode(&self) -> Result<Row> {
        if self.entity_key.as_ref().is_some_and(Vec::is_empty) {
            return Err(invalid("Empty recorded entity key"));
        }
        let mut key = entity_prefix(
            self.header.iid,
            self.entity_key.as_deref().unwrap_or_default(),
        )?;
        let mut value = Vec::new();
        let frontier = self.header.op == EventOp::Erase;
        if frontier {
            key.push(FRONTIER);
        }
        let position = [
            ((self.header.system_from.0 as u64) ^ (1 << 63))
                .to_be_bytes()
                .as_slice(),
            &self.header.commit_position.to_be_bytes(),
            &self.origin.ordinal().to_be_bytes(),
        ]
        .concat();
        if frontier {
            value.extend(position);
        } else {
            key.extend(position);
            if let Some(position) = self.header.position { key.extend(position.to_bytes()); }
        }
        value.extend(self.origin.0);
        value.extend(self.header.valid_time.lower().0.to_be_bytes());
        value.extend(self.header.valid_time.upper().0.to_be_bytes());
        let op = match self.header.op {
            EventOp::Put => 0,
            EventOp::Delete => 1,
            EventOp::Erase => 2,
            EventOp::Redacted => 3,
        };
        value.push(op | if self.header.position.is_some() { 0x80 } else { 0 });
        if let Some(position) = self.header.position { value.extend(position.to_bytes()); }
        if let Some((trie, page, row)) = &self.payload_hint {
            value.extend(page.to_be_bytes());
            value.extend(row.to_be_bytes());
            value.extend(trie.to_string().as_bytes());
        }
        Ok(Row { key, value })
    }

    pub fn decode(row: &Row) -> Result<Self> {
        let (key, value) = (&row.key, &row.value);
        if key.len() < KEY_PREFIX || key[0] != key[1] {
            return Err(invalid("Invalid directory key"));
        }
        let iid = Iid(key[1..17].try_into().expect("IID width"));
        let len = u16::from_be_bytes(key[17..19].try_into().expect("key length width")) as usize;
        let identity = key
            .get(KEY_PREFIX..KEY_PREFIX + len)
            .ok_or_else(|| invalid("Invalid directory entity key length"))?;
        let suffix = &key[KEY_PREFIX + len..];
        let frontier = suffix == [FRONTIER];
        let (position, value) = if frontier {
            value
                .split_at_checked(POSITION)
                .ok_or_else(|| invalid("Missing frontier position"))?
        } else {
            (suffix, value.as_slice())
        };
        if position.len() < POSITION || value.len() < 49 {
            return Err(invalid("Invalid directory event"));
        }
        let system = u64::from_be_bytes(position[..8].try_into().expect("system position width"));
        let commit_position =
            u32::from_be_bytes(position[8..12].try_into().expect("commit position width"));
        let origin = EventOrigin(value[..32].try_into().expect("event origin width"));
        if origin.ordinal().to_be_bytes() != position[12..POSITION] {
            return Err(invalid("Event ordinal differs from directory key"));
        }
        let valid_time = Interval::from_micros(
            i64::from_be_bytes(value[32..40].try_into().expect("valid from width")),
            i64::from_be_bytes(value[40..48].try_into().expect("valid to width")),
        )?;
        let op = match (frontier, value[48] & 0x7f) {
            (false, 0) => EventOp::Put,
            (false, 1) => EventOp::Delete,
            (false, 3) => EventOp::Redacted,
            (true, 2) => EventOp::Erase,
            _ => return Err(invalid("Unknown directory event operation")),
        };
        let entity_key = (!identity.is_empty()).then(|| identity.to_vec());
        let (event_position, hint) = if value[48] & 0x80 != 0 {
            let (encoded, hint) = value[49..].split_first_chunk::<{ EventPosition::WIDTH as usize }>()
                .ok_or_else(|| invalid("Truncated original event position"))?;
            if !frontier && &position[POSITION..] != encoded {
                return Err(invalid("Original event position differs from directory key"));
            }
            (Some(EventPosition::from_bytes(*encoded)), hint)
        } else {
            if position.len() != POSITION { return Err(invalid("Invalid directory position")); }
            (None, &value[49..])
        };
        let payload_hint = match hint.len() {
            0 => None,
            9.. => Some((
                std::str::from_utf8(&hint[8..]).map_err(invalid)?.parse()?,
                u32::from_be_bytes(hint[..4].try_into().expect("page number width")),
                u32::from_be_bytes(hint[4..8].try_into().expect("row number width")),
            )),
            _ => return Err(invalid("Truncated directory payload hint")),
        };
        Ok(Self {
            header: EventHeader {
                position: event_position,
                iid,
                system_from: Timestamp((system ^ (1 << 63)) as i64),
                commit_position,
                valid_time,
                op,
            },
            origin,
            entity_key,
            payload_hint,
        })
    }
}

pub fn add_frontier(hash: &mut Sha256, event: &LogicalEvent) -> Result<()> {
    hash.update(
        &entity_prefix(
            event.header.iid,
            event.entity_key.as_deref().unwrap_or_default(),
        )?[1..],
    );
    hash.update(event.header.system_from.0.to_be_bytes());
    hash.update(event.header.commit_position.to_be_bytes());
    if let Some(position) = event.header.position { hash.update(position.to_bytes()); }
    Ok(())
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.format/entity-directory", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{BlockIndex, EntityId};

    #[test]
    fn a_frontier_key_covers_its_exact_typed_identity() {
        let int = EntityId::Integer(42);
        let uuid = EntityId::Uuid(int.iid().0);
        assert_eq!(int.iid(), uuid.iid());
        let make = |entity_key: Vec<u8>, op| LogicalEvent {
            header: EventHeader {
                position: Some(EventPosition { transaction: rtdb_types::TxId(9), ordinal: 7 }),
                iid: int.iid(),
                system_from: Timestamp(-10),
                commit_position: 4,
                valid_time: Interval::ALL,
                op,
            },
            origin: EventOrigin::at([1; 16], 7, BlockIndex(12)),
            entity_key: Some(entity_key),
            payload_hint: None,
        };
        let erase = make(int.handle_bytes(), EventOp::Erase);
        let other = make(uuid.handle_bytes(), EventOp::Put);
        let range = entity_rows(int.iid(), erase.entity_key.as_deref()).unwrap();
        let frontier = erase.encode().unwrap();
        let put = other.encode().unwrap();
        assert_ne!(frontier.key, put.key);
        assert!(range.contains(&frontier.key));
        assert!(!range.contains(&put.key));
        assert!(iid_rows(int.iid()).contains(&frontier.key));
        assert!(iid_rows(int.iid()).contains(&put.key));
        assert_eq!(
            LogicalEvent::decode(&frontier).unwrap().header,
            erase.header
        );
        assert_eq!(
            LogicalEvent::decode(&put).unwrap().entity_key,
            other.entity_key
        );
    }
}
