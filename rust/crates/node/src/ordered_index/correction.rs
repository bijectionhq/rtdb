use std::sync::Arc;

use arrow_array::Int64Array;
use arrow_buffer::Buffer;
use bytes::Bytes;
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Iid, Interval, Result, Timestamp};

use super::{
    PAGE_LAYOUT_VERSION, format_error,
    key::arrow,
    postings::{BatchKind, KeyBatch, KeyBatchEncoding, ProjectedKeys},
    tree::{Address, Entry, Item, Key, Span},
};

pub(super) const HEADER: [u8; 16] = {
    let mut header = [0; 16];
    header[0] = PAGE_LAYOUT_VERSION;
    header[1] = 1;
    header
};

const GROUP_HEADER_BYTES: usize = 24;

pub(super) struct CorrectionPage<'a> {
    entries: &'a [Entry],
    batch: KeyBatch,
    groups_len: usize,
}

impl<'a> CorrectionPage<'a> {
    pub fn prepare(entries: &'a [Entry]) -> Result<Option<Self>> {
        if entries.is_empty() {
            return Ok(None);
        }
        let mut groups_len = 0usize;
        let mut from = Vec::new();
        let mut to = Vec::new();
        let mut tuples = Vec::new();
        for entry in entries {
            let (Key::Row(address), Item::Row(spans)) = (&entry.key, &entry.item) else {
                return Err(invalid());
            };
            if spans.is_empty() {
                return Err(invalid());
            }
            u32::try_from(spans.len()).map_err(format_error)?;
            let key_len = address.view().key.len();
            u32::try_from(key_len).map_err(format_error)?;
            groups_len = groups_len
                .checked_add(GROUP_HEADER_BYTES)
                .and_then(|len| len.checked_add(key_len))
                .ok_or_else(invalid)?;
            for span in spans.iter() {
                from.push(span.valid.lower().0);
                to.push(span.valid.upper().0);
                tuples.push(span.tuple.view());
            }
        }
        let batch = KeyBatch::gather(
            &tuples,
            BatchKind::Correction,
            vec![
                Arc::new(Int64Array::from(from)),
                Arc::new(Int64Array::from(to)),
            ],
        )?;
        Ok(Some(Self {
            entries,
            batch,
            groups_len,
        }))
    }

    pub fn stage(&self, grant: &Arc<MemoryReservation>) -> Result<KeyBatchEncoding<'_>> {
        self.batch.stage(&HEADER, self.groups_len, grant)
    }

    pub fn append_groups(&self, bytes: &mut Vec<u8>) {
        for entry in self.entries {
            let (Key::Row(address), Item::Row(spans)) = (&entry.key, &entry.item) else {
                unreachable!("validated correction entry");
            };
            let address = address.view();
            bytes.extend_from_slice(address.iid.as_bytes());
            bytes.extend_from_slice(&(spans.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&(address.key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(address.key);
        }
    }
}

pub(super) fn decode(
    bytes: Buffer,
    max_entries: usize,
    opening: &mut MemoryReservation,
) -> Result<Vec<Entry>> {
    let max_rows = bytes.len() / (2 * size_of::<i64>());
    if !bytes.starts_with(&HEADER) {
        return Err(invalid());
    }
    let (batch, mut groups) =
        KeyBatch::decode(bytes.slice(HEADER.len()), max_rows, BatchKind::Correction)?;
    if groups.is_empty() {
        return Err(invalid());
    }
    let rows = batch.batch().num_rows();
    let keys_bytes = batch.bytes_excluding(&bytes)?
        .saturating_add(ProjectedKeys::allocation_bytes())
        .saturating_add(rows.saturating_mul(size_of::<Span>()));
    if keys_bytes
        .saturating_add(max_entries.saturating_mul(size_of::<Entry>()))
        > opening.bytes()
    {
        return Err(invalid());
    }
    let from = arrow::<Int64Array>(batch.batch().column(0).as_ref())?.clone();
    let to = arrow::<Int64Array>(batch.batch().column(1).as_ref())?.clone();
    let mut entries = Vec::new();
    let mut offset = 0usize;
    let mut retained_addresses = None;
    while !groups.is_empty() {
        if entries.len() == max_entries || groups.len() < GROUP_HEADER_BYTES { return Err(invalid()); }
        let group = &groups[..GROUP_HEADER_BYTES];
        let iid = Iid::from_slice(&group[..16])?;
        let length = u32::from_le_bytes(group[16..20].try_into().map_err(format_error)?) as usize;
        let key_len = u32::from_le_bytes(group[20..24].try_into().map_err(format_error)?) as usize;
        groups.advance(GROUP_HEADER_BYTES);
        let key = groups.get(..key_len).ok_or_else(invalid)?;
        let key = if key.is_empty() {
            Bytes::new()
        } else {
            rtdb_columnar::EntityIdRef::from_handle_bytes(key).map_err(format_error)?;
            if retained_addresses.is_none() {
                retained_addresses = Some(Address::retain_bytes(bytes.clone(), opening.split_off(128)?));
            }
            retained_addresses.as_ref().expect("group backing is retained").slice_ref(key)
        };
        let end = offset
            .checked_add(length)
            .filter(|&end| end <= rows)
            .ok_or_else(invalid)?;
        if length == 0 {
            return Err(invalid());
        }
        entries.push((Address::from_retained(iid, key), offset..end));
        groups.advance(key_len);
        offset = end;
    }
    if offset != rows {
        return Err(invalid());
    }
    let bytes = keys_bytes.saturating_add(entries.len().saturating_mul(2 * size_of::<usize>()));
    let keys = ProjectedKeys::new(batch, opening.split_off(bytes)?);
    entries.into_iter().map(|(address, range)| {
        let mut spans = Vec::with_capacity(range.len());
        let mut prior_end = Timestamp::MIN;
        for row in range {
            let valid = Interval::new(Timestamp(from.value(row)), Timestamp(to.value(row)))?;
            if valid.is_empty() || valid.lower() < prior_end {
                return Err(invalid());
            }
            prior_end = valid.upper();
            spans.push(Span { valid, tuple: keys.row(row) });
        }
        Ok(Entry { key: Key::Row(address), item: Item::Row(spans.into()) })
    }).collect()
}

fn invalid() -> rtdb_types::Anomaly {
    format_error("Invalid ordered index correction page")
}

#[cfg(test)]
mod tests {
    use rtdb_columnar::MemoryBudget;

    use super::*;
    use crate::ordered_index::{
        key::{Atom, Bucket},
        postings::{Postings, Tuple},
        tree::{AddressRef, encode_leaf},
    };

    #[test]
    fn a_correction_page_persists_only_the_spans_it_references() {
        let budget = MemoryBudget::new(1 << 20);
        let retained = [Atom::Text("retained".into())];
        let omitted = [Atom::Text("omitted-key".into())];
        let cells = [Tuple::Owned(&retained), Tuple::Owned(&omitted)];
        let batch = KeyBatch::build(&cells, BatchKind::Keys, Vec::new()).unwrap();
        let reservation = budget.reserve(16 << 10).unwrap();
        let keys = ProjectedKeys::new(batch, reservation.clone());
        let span = Span {
            valid: Interval::from_micros(12, 40).unwrap(),
            tuple: keys.row(0),
        };
        let id = rtdb_types::EntityId::String("complete-identity".into());
        let handle = id.handle_bytes();
        let address = AddressRef {
            iid: id.iid(),
            key: &handle,
        }.owned();
        let entries = [Entry {
            key: Key::Row(address.clone()),
            item: Item::Row(Arc::from([span.clone()])),
        }];
        let encoding = budget.reserve(32 << 10).unwrap();
        let bytes = encode_leaf(None, &entries, &encoding).unwrap();
        assert!(
            !bytes
                .windows(b"omitted-key".len())
                .any(|slice| slice == b"omitted-key")
        );
        let group_start = bytes.len() - GROUP_HEADER_BYTES - handle.len();
        assert_eq!(&bytes[group_start..group_start + 16], id.iid().as_bytes());
        assert_eq!(&bytes[group_start + 16..group_start + 20], &1u32.to_le_bytes());
        assert_eq!(
            &bytes[group_start + 20..group_start + GROUP_HEADER_BYTES],
            &(handle.len() as u32).to_le_bytes()
        );
        assert_eq!(&bytes[group_start + GROUP_HEADER_BYTES..], handle.as_slice());

        let postings = Postings::build(&[(
            Bucket { level: 64, start: 0 },
            cells[0],
            address.view(),
        )]).unwrap();
        let leaf = encode_leaf(Some(&postings), &entries, &encoding).unwrap();
        let (_, suffix) = Postings::decode(Buffer::from_vec(leaf).slice(16), 64).unwrap();
        assert_eq!(suffix.as_ref(), bytes.as_slice());

        let mut opening = budget.reserve(16 << 10).unwrap();
        let opening = Arc::get_mut(&mut opening).unwrap();
        let buffer = Buffer::from_vec(bytes);
        let source = buffer[group_start + GROUP_HEADER_BYTES..].as_ptr();
        let backing = rtdb_columnar::retain_buffer(&buffer, &opening.split_off(buffer.capacity()).unwrap());
        let entries = decode(backing, 64, opening).unwrap();
        assert_eq!(entries[0].key, Key::Row(address));
        let Key::Row(address) = &entries[0].key else { unreachable!() };
        assert_eq!(address.view().key.as_ptr(), source);
        let Item::Row(decoded) = &entries[0].item else {
            panic!("Expected correction spans");
        };
        assert_eq!(decoded.as_ref(), [span]);
    }
}
