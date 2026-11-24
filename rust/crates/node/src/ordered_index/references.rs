use std::{cell::OnceCell, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    UInt8Array, UInt64Array,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder},
};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Iid, Result, Timestamp};

use super::{
    format_error,
    key::{AtomRef, Bucket, arrow},
    postings::{KeyBatch, ProjectedKeys, Tuple},
    tree::{Address, AddressRef, IndexRevision, Key, KeyRef, Measure, MeasureDomain, Pointer},
};

type Reference = (Option<IndexRevision>, Option<Arc<Pointer>>);

struct FenceAddressBuffer {
    values: Buffer,
    _memory: Arc<MemoryReservation>,
}

impl AsRef<[u8]> for FenceAddressBuffer {
    fn as_ref(&self) -> &[u8] {
        self.values.as_slice()
    }
}

fn fields() -> Vec<Field> {
    vec![
        Field::new("revision", DataType::Int64, true),
        Field::new("digest", DataType::Utf8, true),
        Field::new("generation", DataType::Int64, true),
        Field::new("size", DataType::UInt64, true),
        Field::new("entries", DataType::UInt64, true),
        Field::new("key_kind", DataType::UInt8, false),
        Field::new("bucket_level", DataType::UInt8, true),
        Field::new("bucket_start", DataType::UInt64, true),
        Field::new("iid", DataType::FixedSizeBinary(16), true),
        Field::new("entity_key", DataType::Binary, true),
        Field::new("system_time", DataType::Int64, true),
        Field::new("measure_count", DataType::UInt64, true),
        Field::new("measure_domain", DataType::FixedSizeBinary(3), true),
        Field::new("measure_sum", DataType::FixedSizeBinary(32), true),
        Field::new("revision_transaction", DataType::Int64, true),
        Field::new("fence_transaction", DataType::Int64, true),
    ]
}

pub(super) fn encode(references: &[(Option<IndexRevision>, Option<&Pointer>)]) -> Result<RecordBatch> {
    let width = references
        .iter()
        .filter_map(|(_, pointer)| *pointer)
        .flat_map(|pointer| [&pointer.first, &pointer.last])
        .filter_map(|key| match key.view() {
            KeyRef::Posting(_, tuple, _) => Some(tuple.len()),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    let rows = references.len().saturating_mul(2);
    let mut revisions = Vec::with_capacity(rows);
    let mut revision_transactions = Vec::with_capacity(rows);
    let mut fence_transactions = Vec::with_capacity(rows);
    let mut digests = StringBuilder::with_capacity(rows, 64 * rows);
    let mut generations = Vec::with_capacity(rows);
    let mut sizes = Vec::with_capacity(rows);
    let mut counts = Vec::with_capacity(rows);
    let mut kinds = Vec::with_capacity(rows);
    let mut levels = Vec::with_capacity(rows);
    let mut starts = Vec::with_capacity(rows);
    let mut iids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut addresses = BinaryBuilder::new();
    let mut times = Vec::with_capacity(rows);
    let mut measure_counts = Vec::with_capacity(rows);
    let mut measure_domains = FixedSizeBinaryBuilder::with_capacity(rows, 3);
    let mut measure_sums = FixedSizeBinaryBuilder::with_capacity(rows, 32);
    let mut keys = Vec::with_capacity(rows);
    for &(revision, pointer) in references {
        let fences = pointer.map(|pointer| [&pointer.first, &pointer.last]);
        for side in 0..2 {
            let (kind, level, start, address, time, tuple) =
                match fences.map(|fences| fences[side].view()) {
                    Some(KeyRef::Posting(bucket, tuple, iid)) => (
                        0,
                        Some(u8::try_from(bucket.level).map_err(format_error)?),
                        Some(bucket.start),
                        Some(iid),
                        None,
                        tuple,
                    ),
                    Some(KeyRef::Row(iid)) => (1, None, None, Some(iid), None, Tuple::Nulls(width)),
                    Some(KeyRef::Revision(time)) => {
                        (2, None, None, None, Some(time), Tuple::Nulls(width))
                    }
                    None => (3, None, None, None, None, Tuple::Nulls(width)),
                };
            revisions.push(revision.map(|revision| revision.time.0));
            revision_transactions.push(revision.map(|revision| revision.transaction));
            digests.append_option(pointer.map(|pointer| pointer.digest.as_str()));
            generations.push(pointer.map(|pointer| pointer.generation));
            sizes.push(pointer.map(|pointer| pointer.size as u64));
            counts.push(pointer.map(|pointer| pointer.entries));
            kinds.push(kind);
            levels.push(level);
            starts.push(start);
            if let Some(address) = address {
                iids.append_value(address.iid.as_bytes())
                    .map_err(format_error)?;
            } else {
                iids.append_null();
            }
            addresses.append_option(address.map(|address| address.key));
            times.push(time.map(|revision| revision.time.0));
            fence_transactions.push(time.map(|revision| revision.transaction));
            measure_counts.push(pointer.map(|pointer| pointer.measure.count));
            if let Some(pointer) = pointer {
                measure_domains.append_value(measure_domain_bytes(pointer.measure.domain)).map_err(format_error)?;
                measure_sums.append_value(pointer.measure.sum).map_err(format_error)?;
            } else {
                measure_domains.append_null();
                measure_sums.append_null();
            }
            keys.push(tuple);
        }
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(revisions)),
        Arc::new(digests.finish()),
        Arc::new(Int64Array::from(generations)),
        Arc::new(UInt64Array::from(sizes)),
        Arc::new(UInt64Array::from(counts)),
        Arc::new(UInt8Array::from(kinds)),
        Arc::new(UInt8Array::from(levels)),
        Arc::new(UInt64Array::from(starts)),
        Arc::new(iids.finish()),
        Arc::new(addresses.finish()),
        Arc::new(Int64Array::from(times)),
        Arc::new(UInt64Array::from(measure_counts)),
        Arc::new(measure_domains.finish()),
        Arc::new(measure_sums.finish()),
        Arc::new(Int64Array::from(revision_transactions)),
        Arc::new(Int64Array::from(fence_transactions)),
    ];
    let keys = KeyBatch::build_with_prefix(&keys, fields(), arrays)?;
    Ok(keys.batch().clone())
}

pub(super) fn decode(
    batch: RecordBatch,
    max: usize,
    memory: &Arc<MemoryReservation>,
) -> Result<Vec<Reference>> {
    decode_with_memory(batch, max, |keys, _| {
        if keys.retained_bytes() > memory.bytes() {
            return Err(invalid());
        }
        Ok(memory.clone())
    })
}

pub(super) fn decode_page(
    batch: RecordBatch,
    max: usize,
    backing: &Buffer,
    opening: &mut MemoryReservation,
) -> Result<Vec<Reference>> {
    let mut references = decode_with_memory(batch, max, |keys, retains_batch| {
        let bytes = keys.bytes_excluding(backing)?;
        if bytes > opening.bytes() {
            return Err(invalid());
        }
        let retained = if retains_batch {
            bytes.saturating_add(ProjectedKeys::allocation_bytes())
                .saturating_add(size_of::<FenceAddressBuffer>() + 2 * size_of::<usize>())
        } else {
            0
        };
        opening.split_off(retained)
    })?;
    for pointer in references.iter_mut().filter_map(|(_, pointer)| pointer.as_mut()) {
        let pointer = Arc::get_mut(pointer).expect("newly decoded pointers are unpublished");
        pointer._memory = Some(opening.split_off(pointer.owned_backing_bytes())?);
    }
    Ok(references)
}

fn decode_with_memory(
    batch: RecordBatch,
    max: usize,
    reserve: impl FnOnce(&KeyBatch, bool) -> Result<Arc<MemoryReservation>>,
) -> Result<Vec<Reference>> {
    let rows = batch.num_rows();
    if rows % 2 != 0 || rows / 2 > max {
        return Err(invalid());
    }
    let keys = KeyBatch::from_batch(batch, &fields())?;
    let columns = keys.batch().columns().to_vec();
    let kinds = arrow::<UInt8Array>(columns[5].as_ref())?;
    let memory = reserve(&keys, kinds.values().contains(&0))?;
    let keys = ProjectedKeys::new(keys, memory.clone());
    let revisions = arrow::<Int64Array>(columns[0].as_ref())?;
    let digests = arrow::<StringArray>(columns[1].as_ref())?;
    let generations = arrow::<Int64Array>(columns[2].as_ref())?;
    let sizes = arrow::<UInt64Array>(columns[3].as_ref())?;
    let counts = arrow::<UInt64Array>(columns[4].as_ref())?;
    let levels = arrow::<UInt8Array>(columns[6].as_ref())?;
    let starts = arrow::<UInt64Array>(columns[7].as_ref())?;
    let iids = arrow::<FixedSizeBinaryArray>(columns[8].as_ref())?;
    let addresses = arrow::<BinaryArray>(columns[9].as_ref())?;
    let retained_addresses = OnceCell::new();
    let times = arrow::<Int64Array>(columns[10].as_ref())?;
    let measure_counts = arrow::<UInt64Array>(columns[11].as_ref())?;
    let measure_domains = arrow::<FixedSizeBinaryArray>(columns[12].as_ref())?;
    let measure_sums = arrow::<FixedSizeBinaryArray>(columns[13].as_ref())?;
    let revision_transactions = arrow::<Int64Array>(columns[14].as_ref())?;
    let fence_transactions = arrow::<Int64Array>(columns[15].as_ref())?;
    let revision_key = |time: &Int64Array, transaction: &Int64Array, row| -> Result<_> {
        match (time.is_null(row), transaction.is_null(row)) {
            (true, true) => Ok(None),
            (false, false) => Ok(Some(IndexRevision {
                time: Timestamp(time.value(row)),
                transaction: transaction.value(row),
            })),
            _ => Err(invalid()),
        }
    };
    let measure = |row| -> Result<_> {
        match (measure_counts.is_null(row), measure_domains.is_null(row), measure_sums.is_null(row)) {
            (true, true, true) => Ok(None),
            (false, false, false) => {
                let count = measure_counts.value(row);
                let domain = measure_domain(measure_domains.value(row))?;
                let sum = measure_sums.value(row).try_into().map_err(format_error)?;
                if (count == 0) != (domain == MeasureDomain::Empty)
                    || matches!(domain, MeasureDomain::Empty | MeasureDomain::Mixed) && sum != [0; 32]
                {
                    return Err(invalid());
                }
                Ok(Some(Measure { count, domain, sum }))
            }
            _ => Err(invalid()),
        }
    };
    let revision = |row| revision_key(revisions, revision_transactions, row);
    let identity = |row| -> Result<_> {
        Ok((
            (!digests.is_null(row)).then(|| digests.value(row)),
            (!generations.is_null(row)).then(|| generations.value(row)),
            (!sizes.is_null(row)).then(|| sizes.value(row)),
            (!counts.is_null(row)).then(|| counts.value(row)),
            measure(row)?,
        ))
    };
    let key = |row| -> Result<_> {
        let tuple = keys.row(row);
        let bucket = match (levels.is_null(row), starts.is_null(row)) {
            (true, true) => None,
            (false, false) => {
                let level = u32::from(levels.value(row));
                let start = starts.value(row);
                if level > 64 || super::domain::bucket_start(start.into(), level) != start {
                    return Err(invalid());
                }
                Some(Bucket { level, start })
            }
            _ => return Err(invalid()),
        };
        let iid = (!iids.is_null(row))
            .then(|| Iid::from_slice(iids.value(row)))
            .transpose()?;
        let address = match (iid, addresses.is_null(row)) {
            (Some(iid), false) => Some(AddressRef {
                iid,
                key: addresses.value(row),
            }),
            (None, true) => None,
            _ => return Err(invalid()),
        };
        let time = revision_key(times, fence_transactions, row)?;
        let empty = || tuple.view().iter().all(|cell| cell == AtomRef::Null);
        match (kinds.value(row), bucket, address, time) {
            (0, Some(bucket), Some(address), None) if tuple.view().len() != 0 => {
                // A posting fence already retains this batch through its tuple.
                let key = if address.key.is_empty() {
                    Bytes::new()
                } else {
                    retained_addresses
                        .get_or_init(|| {
                            Bytes::from_owner(FenceAddressBuffer {
                                values: addresses.values().clone(),
                                _memory: memory.clone(),
                            })
                        })
                        .slice_ref(address.key)
                };
                Ok(Some(Key::Posting {
                    bucket,
                    tuple,
                    address: Address::new(address.iid, key),
                }))
            }
            (1, None, Some(address), None) if empty() => Ok(Some(Key::Row(address.owned()))),
            (2, None, None, Some(time)) if empty() => Ok(Some(Key::Revision(time))),
            (3, None, None, None) if empty() => Ok(None),
            _ => Err(invalid()),
        }
    };
    (0..rows)
        .step_by(2)
        .map(|row| {
            let time = revision(row)?;
            let address = identity(row)?;
            if time != revision(row + 1)? || address != identity(row + 1)? {
                return Err(invalid());
            }
            match (address, key(row)?, key(row + 1)?) {
                (
                    (Some(digest), Some(generation), Some(size), Some(entries), Some(measure)),
                    Some(first),
                    Some(last),
                ) if generation >= 0
                    && size > 0
                    && entries > 0
                    && measure.count <= entries
                    && first <= last
                    && digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
                {
                    Ok((
                        time,
                        Some(Arc::new(Pointer {
                            digest: digest.to_owned(),
                            generation,
                            size: usize::try_from(size).map_err(format_error)?,
                            entries,
                            measure,
                            first,
                            last,
                            _memory: None,
                        })),
                    ))
                }
                ((None, None, None, None, None), None, None) => Ok((time, None)),
                _ => Err(invalid()),
            }
        })
        .collect()
}

pub(super) fn measure_domain_bytes(domain: MeasureDomain) -> [u8; 3] {
    match domain {
        MeasureDomain::Empty => [0, 0, 0],
        MeasureDomain::Int64 => [1, 0, 0],
        MeasureDomain::Decimal128(precision, scale) => [2, precision, scale as u8],
        MeasureDomain::Mixed => [3, 0, 0],
    }
}

fn measure_domain(bytes: &[u8]) -> Result<MeasureDomain> {
    match bytes {
        [0, 0, 0] => Ok(MeasureDomain::Empty),
        [1, 0, 0] => Ok(MeasureDomain::Int64),
        [2, precision @ 1..=38, scale] => Ok(MeasureDomain::Decimal128(*precision, *scale as i8)),
        [3, 0, 0] => Ok(MeasureDomain::Mixed),
        _ => Err(invalid()),
    }
}

fn invalid() -> rtdb_types::Anomaly {
    format_error("Invalid ordered index page reference")
}

#[cfg(test)]
mod tests {
    use rtdb_columnar::MemoryBudget;
    use rtdb_types::EntityId;

    use super::*;
    use crate::ordered_index::{ipc, key::Atom, postings::KeyRow};

    #[test]
    fn arrow_posting_fences_share_reserved_bytes_without_changing_full_identities() {
        let first = EntityId::String("entity".into());
        let iid = first.iid();
        let last = EntityId::Uuid(iid.0);
        let first = Address::new(iid, first.handle_bytes().into());
        let last = Address::new(iid, last.handle_bytes().into());
        let fence = |address| Key::Posting {
            bucket: Bucket { level: 0, start: 0 },
            tuple: KeyRow::Fence(vec![Atom::Text("value".into())].into()),
            address,
        };
        let pointer = Pointer {
            digest: "01".repeat(32),
            generation: 3,
            size: 4096,
            entries: 2,
            measure: Measure {
                count: 2,
                domain: MeasureDomain::Decimal128(25, -3),
                sum: arrow_buffer::i256::from_i128(-123_456).to_be_bytes(),
            },
            first: fence(first.clone()),
            last: fence(last.clone()),
            _memory: None,
        };
        let rows = Pointer {
            digest: "02".repeat(32),
            measure: Measure::default(),
            first: Key::Row(first),
            last: Key::Row(last),
            ..pointer.clone()
        };
        let encode_both = |posting: &Pointer, rows: &Pointer| {
            ipc::encode(&encode(&[(None, Some(posting)), (None, Some(rows))]).unwrap()).unwrap()
        };
        let encoded = Bytes::from(encode_both(&pointer, &rows));
        let batch = ipc::decode(encoded.clone(), 4).unwrap();
        let addresses = arrow::<BinaryArray>(batch.column(9).as_ref()).unwrap().clone();
        let budget = MemoryBudget::new(1 << 20);
        let reservation = budget.reserve(64 << 10).unwrap();
        let mut decoded = decode(batch, 2, &reservation).unwrap();
        let posting = decoded[0].1.as_deref().unwrap();
        let row = decoded[1].1.as_deref().unwrap();
        assert_eq!(posting, &pointer);
        assert_eq!(row, &rows);
        for (offset, key) in [&posting.first, &posting.last].into_iter().enumerate() {
            let KeyRef::Posting(_, _, address) = key.view() else {
                panic!("Expected a posting fence");
            };
            assert_eq!(address.iid, iid);
            assert_eq!(address.key.as_ptr(), addresses.value(offset).as_ptr());
        }
        let KeyRef::Row(address) = row.first.view() else {
            panic!("Expected a row fence");
        };
        assert_ne!(address.key.as_ptr(), addresses.value(2).as_ptr());
        assert_eq!(encode_both(posting, row).as_slice(), encoded.as_ref());
        let legacy = serde_json::to_vec(&pointer).unwrap();
        assert_eq!(serde_json::to_vec(posting).unwrap(), legacy);
        assert_eq!(&serde_json::from_slice::<Pointer>(&legacy).unwrap(), posting);

        drop(addresses);
        drop(reservation);
        assert_eq!(budget.used_bytes(), 64 << 10);
        drop(decoded.remove(0));
        assert_eq!(budget.used_bytes(), 0);
    }
}
