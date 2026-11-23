use super::Record;
use bytes::BufMut;
pub(super) use bytes::{Bytes, BytesMut};
use prost::Message;
pub(super) use std::io::{Read, Result as IoResult};

pub(super) fn read_header(reader: &mut impl Read) -> IoResult<[u8; 8]> {
    let mut header = [0; 8];
    reader.read_exact(&mut header)?;
    Ok(header)
}

pub(super) fn read_body(reader: &mut impl Read, length: usize) -> IoResult<Bytes> {
    let mut body = Vec::new();
    body.try_reserve_exact(length).map_err(std::io::Error::other)?;
    body.resize(length, 0);
    reader.read_exact(&mut body)?;
    Ok(Bytes::from(body))
}

pub(super) fn split_frame(frame: &Bytes) -> Option<([u8; 8], Bytes)> {
    let header = frame.get(..8)?.try_into().ok()?;
    Some((header, frame.slice(8..)))
}

pub(super) fn empty_bytes() -> Bytes { Bytes::new() }

pub(super) fn body_len(body: &Bytes) -> usize { body.len() }

pub(super) fn header_fields(header: [u8; 8]) -> (u32, u32) {
    let [a, b, c, d, e, f, g, h] = header;
    (u32::from_be_bytes([a, b, c, d]), u32::from_be_bytes([e, f, g, h]))
}

pub(super) fn checksum(body: &Bytes) -> u32 { crc32fast::hash(body) }

pub(super) fn encode_record(record: &Record) -> Bytes { Bytes::from(record.encode_to_vec()) }

pub(super) fn decode_record(body: Bytes) -> Option<Record> { Record::decode(body).ok() }

pub(super) fn append_frame(buffer: &mut BytesMut, length: u32, checksum: u32, body: &Bytes) {
    buffer.put_u32(length);
    buffer.put_u32(checksum);
    buffer.put_slice(body);
}
