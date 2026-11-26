use std::io::{self, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_schema::Schema;
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::ipc::OwnedStream;
use rtdb_types::{Anomaly, Result};
use sha2::{Digest, Sha256};

use crate::RetainedBytes;

pub(crate) struct Output {
    data: Vec<u8>,
    reservation: Arc<MemoryReservation>,
}

impl Output {
    pub(crate) fn new(memory: &Arc<MemoryBudget>, prefix_len: usize) -> Result<Self> {
        let reservation = memory.reserve(prefix_len.checked_add(128).ok_or_else(overflow)?)?;
        Ok(Self { data: vec![0; prefix_len], reservation })
    }

    pub(crate) fn len(&self) -> usize { self.data.len() }

    pub(crate) fn reserve(&mut self, additional: usize) -> Result<()> {
        let required = self.len().checked_add(additional).ok_or_else(overflow)?;
        let capacity = self.data.capacity();
        if required <= capacity { return Ok(()); }

        let (capacity, reservation) = reserve_growth(&self.reservation, capacity, required)?;
        let additional = capacity - self.len();
        self.data.try_reserve_exact(additional)
            .map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()).with_kind(rtdb_types::AnomalyKind::ResourceExhausted))?;
        self.reservation = reservation;
        Ok(())
    }

    pub(crate) fn append_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.reserve(bytes.len())?;
        self.data.extend_from_slice(bytes);
        Ok(())
    }

    pub(crate) fn write_stream<'a>(&mut self, schema: &Schema, pages: impl IntoIterator<Item = &'a RecordBatch>) -> Result<()> {
        let _schema = self.reservation.budget().reserve(rtdb_format::ipc::schema_translation_bound(schema)?)?;
        rtdb_format::ipc::write_stream_to(schema, pages, self)?;
        Ok(())
    }

    pub(crate) fn into_parts(self) -> (Vec<u8>, Arc<MemoryReservation>) {
        (self.data, self.reservation)
    }
}

fn reserve_growth(
    reservation: &MemoryReservation, capacity: usize, required: usize,
) -> Result<(usize, Arc<MemoryReservation>)> {
    let preferred = required.saturating_add((capacity / 2).max(256));
    let reserve = |capacity: usize| {
        reservation.budget().reserve(capacity.checked_add(128).ok_or_else(overflow)?)
    };
    // A reallocating growth can briefly keep the old allocation live.
    Ok(match reserve(preferred) {
        Err(_) if preferred > required => (required, reserve(required)?),
        result => (preferred, result?),
    })
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.append_bytes(bytes).map_err(io::Error::other)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

pub(crate) fn encode<'a>(schema: &Schema, pages: impl IntoIterator<Item = &'a RecordBatch>, memory: &Arc<MemoryBudget>) -> Result<RetainedBytes> {
    let mut output = Output::new(memory, 0)?;
    output.write_stream(schema, pages)?;
    let (data, reservation) = output.into_parts();
    RetainedBytes::with_reservation(data, reservation)
}

pub(crate) fn hash_batch(batch: &RecordBatch, memory: &Arc<MemoryBudget>, hash: &mut Sha256) -> Result<()> {
    let mut output = DigestOutput {
        hash,
        prefix: Vec::new(),
        framing: DigestFraming::Prefix { offset: 0 },
        len: 0,
        capacity: 0,
        reservation: memory.reserve(128)?,
    };
    let _schema = memory.reserve(rtdb_format::ipc::schema_translation_bound(batch.schema_ref())?)?;
    rtdb_format::ipc::write_stream_to(batch.schema_ref(), [batch], &mut output)?;
    match output.framing {
        DigestFraming::Body { stream_len } if stream_len == output.len => Ok(()),
        _ => Err(invalid("Receipt IPC stream length differs from its framing")),
    }
}

struct DigestOutput<'a> {
    hash: &'a mut Sha256,
    prefix: Vec<u8>,
    framing: DigestFraming,
    len: usize,
    capacity: usize,
    reservation: Arc<MemoryReservation>,
}

#[derive(Clone, Copy)]
enum DigestFraming {
    Prefix { offset: usize },
    Metadata { start: usize, end: usize },
    Body { stream_len: usize },
}

impl Write for DigestOutput<'_> {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        let written = bytes.len();
        let len = self.len.checked_add(written).ok_or_else(|| io::Error::other(overflow()))?;
        // Receipt admission keeps the buffered writer's growth checkpoints.
        if len > self.capacity {
            (self.capacity, self.reservation) = reserve_growth(&self.reservation, self.capacity, len)
                .map_err(io::Error::other)?;
        }
        self.len = len;

        while !bytes.is_empty() {
            let end = match self.framing {
                DigestFraming::Prefix { offset } => offset.checked_add(8).ok_or_else(|| io::Error::other(overflow()))?,
                DigestFraming::Metadata { end, .. } => end,
                DigestFraming::Body { .. } => {
                    self.hash.update(bytes);
                    break;
                }
            };
            let count = (end - self.prefix.len()).min(bytes.len());
            self.prefix.try_reserve_exact(count).map_err(|error| {
                io::Error::other(Anomaly::incorrect("rtdb/allocation-failed", error.to_string()).with_kind(rtdb_types::AnomalyKind::ResourceExhausted))
            })?;
            let (prefix, rest) = bytes.split_at(count);
            self.prefix.extend_from_slice(prefix);
            bytes = rest;
            if self.prefix.len() < end { break; }

            self.framing = match self.framing {
                DigestFraming::Prefix { offset } => {
                    let (marker, size) = self.prefix[offset..end].split_at(4);
                    let length = i32::from_le_bytes(size.try_into().expect("four length bytes"));
                    if marker != [0xff; 4] || length <= 0 {
                        return Err(io::Error::other(invalid("Invalid receipt IPC frame prefix")));
                    }
                    let metadata_end = end.checked_add(length as usize)
                        .ok_or_else(|| io::Error::other(overflow()))?;
                    DigestFraming::Metadata { start: end, end: metadata_end }
                }
                DigestFraming::Metadata { start, end } => {
                    let message = arrow_ipc::root_as_message(&self.prefix[start..end])
                        .map_err(|error| io::Error::other(invalid(error.to_string())))?;
                    let body_len = usize::try_from(message.bodyLength()).map_err(io::Error::other)?;
                    let next = end.checked_add(body_len).ok_or_else(|| io::Error::other(overflow()))?;
                    if message.header_type() == arrow_ipc::MessageHeader::RecordBatch {
                        // The one record batch's Java-layout body is followed by a single zero i32.
                        let stream_len = next.checked_add(4).ok_or_else(|| io::Error::other(overflow()))?;
                        self.hash.update((stream_len as u64).to_be_bytes());
                        self.hash.update(std::mem::take(&mut self.prefix));
                        DigestFraming::Body { stream_len }
                    } else {
                        DigestFraming::Prefix { offset: next }
                    }
                }
                DigestFraming::Body { .. } => unreachable!("the receipt is already hashing body bytes"),
            };
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

pub(crate) fn decode(bytes: &Bytes, memory: &Arc<MemoryBudget>) -> Result<Vec<OwnedBatch>> {
    Ok(OwnedStream::read_buffer(Buffer::from(bytes.clone()), memory)?.into_pages())
}

pub(crate) fn allocation_bound(bytes: &[u8]) -> Result<usize> {
    let mut offset = 0usize;
    let mut allocation = bytes.len().checked_mul(4).and_then(|size| size.checked_add(65_536)).ok_or_else(overflow)?;
    while offset < bytes.len() {
        let read_i32 = |at: usize| -> Result<i32> {
            let word = bytes.get(at..at.checked_add(4).ok_or_else(overflow)?).ok_or_else(|| invalid("Truncated IPC message prefix"))?;
            Ok(i32::from_le_bytes(word.try_into().map_err(|_| invalid("Invalid IPC prefix"))?))
        };
        let mut metadata_len = read_i32(offset)?;
        offset += 4;
        if metadata_len == -1 { metadata_len = read_i32(offset)?; offset += 4; }
        if metadata_len == 0 {
            if offset != bytes.len() { return Err(invalid("Bytes follow IPC end marker")); }
            return Ok(allocation);
        }
        let metadata_len = usize::try_from(metadata_len).map_err(|_| invalid("Negative IPC metadata length"))?;
        let metadata_end = offset.checked_add(metadata_len).ok_or_else(overflow)?;
        let message = arrow_ipc::root_as_message(bytes.get(offset..metadata_end).ok_or_else(|| invalid("IPC metadata exceeds payload"))?).map_err(|error| invalid(error.to_string()))?;
        let body_len = usize::try_from(message.bodyLength()).map_err(|_| invalid("Negative IPC body length"))?;
        let body_end = metadata_end.checked_add(body_len).ok_or_else(overflow)?;
        let body = bytes.get(metadata_end..body_end).ok_or_else(|| invalid("IPC body exceeds payload"))?;
        allocation = allocation.checked_add(metadata_len.checked_mul(64).ok_or_else(overflow)?).ok_or_else(overflow)?;
        let batch = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|dictionary| dictionary.data()));
        if let Some(batch) = batch {
            if let Some(nodes) = batch.nodes() {
                for node in nodes {
                    let rows = usize::try_from(node.length()).map_err(|_| invalid("Negative Arrow array length"))?;
                    allocation = allocation.checked_add(rows.checked_add(1).and_then(|rows| rows.checked_mul(4096)).ok_or_else(overflow)?).ok_or_else(overflow)?;
                }
            }
            if let Some(buffers) = batch.buffers() {
                for buffer in buffers {
                    let start = usize::try_from(buffer.offset()).map_err(|_| invalid("Negative Arrow buffer offset"))?;
                    let length = usize::try_from(buffer.length()).map_err(|_| invalid("Negative Arrow buffer length"))?;
                    let buffer = body.get(start..start.checked_add(length).ok_or_else(overflow)?).ok_or_else(|| invalid("Arrow buffer exceeds message body"))?;
                    let decoded = if batch.compression().is_some() && !buffer.is_empty() {
                        let word = buffer.get(..8).ok_or_else(|| invalid("Compressed Arrow buffer has no size"))?;
                        let size = i64::from_le_bytes(word.try_into().map_err(|_| invalid("Invalid compression size"))?);
                        if size == -1 { buffer.len() - 8 } else { usize::try_from(size).map_err(|_| invalid("Negative decoded buffer size"))? }
                    } else { buffer.len() };
                    allocation = allocation.checked_add(decoded.checked_mul(4).and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)?).ok_or_else(overflow)?;
                }
            }
        }
        offset = body_end;
    }
    Err(invalid("IPC stream has no end marker"))
}

fn overflow() -> Anomaly { invalid("IPC allocation bound exceeds address space") }
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.tx/invalid-ipc", message) }

#[test]
fn ipc_output_falls_back_to_exact_growth_and_releases_failed_growth() {
    let memory = MemoryBudget::new((64 + 128) + (65 + 128));
    let mut output = Output::new(&memory, 64).unwrap();
    output.write_all(&[7]).unwrap();
    assert_eq!(memory.used_bytes(), 65 + 128);

    assert!(output.write_all(&[8]).is_err());
    assert_eq!(output.len(), 65);
    assert_eq!(output.data[64], 7);
    assert_eq!(memory.used_bytes(), 65 + 128);

    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn batch_digest_matches_buffered_ipc_bytes_and_admission() {
    use arrow_array::{ArrayRef, DictionaryArray, Int8Array, StringArray, types::Int8Type};

    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), None, Some(1)]),
        Arc::new(StringArray::from(vec!["first", "second"])),
    ).unwrap();
    let payload = "x".repeat(16_384);
    let page = RecordBatch::try_from_iter([
        ("labels", Arc::new(dictionary) as ArrayRef),
        ("payload", Arc::new(StringArray::from(vec![Some(payload.as_str()), None, Some("tail")]))),
    ]).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let buffered = encode(page.schema_ref(), [&page], &memory).unwrap();
    let mut expected = Sha256::new();
    expected.update((buffered.as_slice().len() as u64).to_be_bytes());
    expected.update(buffered.as_slice());
    let expected = expected.finalize();

    let peak = memory.peak_bytes();
    let schema_bytes = rtdb_format::ipc::schema_translation_bound(page.schema_ref()).unwrap();
    for limit in [127, 128 + schema_bytes, peak - 1, peak] {
        let memory = MemoryBudget::new(limit);
        let buffered = encode(page.schema_ref(), [&page], &memory).map(drop);
        let mut actual = Sha256::new();
        match (buffered, hash_batch(&page, &memory, &mut actual)) {
            (Ok(()), Ok(())) => assert_eq!(actual.finalize(), expected),
            (Err(buffered), Err(streamed)) => {
                assert_eq!(buffered.code, streamed.code);
                assert_eq!(buffered.message, streamed.message);
            }
            (buffered, streamed) => panic!("budget {limit}: buffered {buffered:?}, streamed {streamed:?}"),
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn replay_ipc_buffers_keep_the_source_allocation_and_its_charge() {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};

    let memory = MemoryBudget::new(1 << 20);
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7, -4]))]).unwrap();
    let bytes = encode(batch.schema_ref(), [&batch], &memory).unwrap();
    let input_charge = memory.used_bytes();
    let inside = bytes.as_slice().as_ptr() as usize..bytes.as_slice().as_ptr() as usize + bytes.as_slice().len();
    let bytes = bytes.into_bytes();
    let pages = decode(&bytes, &memory).unwrap();
    let reencoded = rtdb_format::ipc::write_stream_to(batch.schema_ref(), pages.iter().map(OwnedBatch::batch), Vec::new()).unwrap();
    assert_eq!(bytes.as_ref(), reencoded.as_slice());

    let values = Arc::clone(pages[0].batch().column(0));
    let both_charges = memory.used_bytes();

    drop((pages, bytes));
    let values_view = values.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(inside.contains(&(values_view.values().as_ptr() as usize)));
    assert_eq!(values_view.values().as_ref(), &[7, -4]);
    assert!(both_charges > input_charge);
    assert_eq!(memory.used_bytes(), both_charges);
    drop(values);
    assert_eq!(memory.used_bytes(), 0);
}
