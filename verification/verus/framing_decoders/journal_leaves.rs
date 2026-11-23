use crate::Record;
use vstd::prelude::*;

verus! {

pub struct Bytes { pub contents: Ghost<Seq<u8>> }
pub struct BytesMut { pub contents: Ghost<Seq<u8>> }
pub struct IoError;
pub type IoResult<T> = Result<T, IoError>;

impl View for Bytes {
    type V = Seq<u8>;
    open spec fn view(&self) -> Seq<u8> { self.contents@ }
}
impl View for BytesMut {
    type V = Seq<u8>;
    open spec fn view(&self) -> Seq<u8> { self.contents@ }
}

pub trait Read {
    spec fn remaining(&self) -> Seq<u8>;
}

pub uninterp spec fn empty() -> Bytes;
pub uninterp spec fn crc(bytes: Seq<u8>) -> u32;
pub uninterp spec fn encode(record: Record) -> Seq<u8>;
pub uninterp spec fn decode(bytes: Seq<u8>) -> Option<Record>;
pub uninterp spec fn be32(bytes: Seq<u8>) -> u32;
pub uninterp spec fn encode_be32(value: u32) -> Seq<u8>;

pub open spec fn frame_length(bytes: Seq<u8>) -> u32 { be32(bytes.take(4)) }

pub open spec fn frame_bytes(body: Seq<u8>) -> Seq<u8> {
    encode_be32(body.len() as u32) + encode_be32(crc(body)) + body
}

pub open spec fn frame_matches(bytes: Seq<u8>, record: Record) -> bool {
    bytes.len() >= 8
    && bytes.len() == 8 + frame_length(bytes)
    && be32(bytes.subrange(4, 8)) == crc(bytes.skip(8))
    && decode(bytes.skip(8)) == Some(record)
}

#[verifier::external_body]
pub fn read_header(reader: &mut impl Read) -> (out: IoResult<[u8; 8]>)
    ensures out matches Ok(header) ==> old(reader).remaining().len() >= 8
        && header@ == old(reader).remaining().take(8)
        && final(reader).remaining() == old(reader).remaining().skip(8),
{ unimplemented!() }

#[verifier::external_body]
pub fn read_body(reader: &mut impl Read, len: usize) -> (out: IoResult<Bytes>)
    ensures out matches Ok(body) ==> old(reader).remaining().len() >= len
        && body@ == old(reader).remaining().take(len as int)
        && final(reader).remaining() == old(reader).remaining().skip(len as int),
{ unimplemented!() }

#[verifier::external_body]
pub fn split_frame(frame: &Bytes) -> (out: Option<([u8; 8], Bytes)>)
    ensures (out is Some) == (frame@.len() >= 8),
        out matches Some((header, body)) ==> header@ == frame@.take(8) && body@ == frame@.skip(8),
{ unimplemented!() }

#[verifier::external_body]
pub fn empty_bytes() -> (out: Bytes)
    ensures out == empty(), out@.len() == 0,
{ unimplemented!() }

#[verifier::external_body]
pub fn body_len(body: &Bytes) -> (out: usize)
    ensures out == body@.len(),
{ unimplemented!() }

#[verifier::external_body]
pub fn header_fields(header: [u8; 8]) -> (out: (u32, u32))
    ensures out.0 == be32(header@.take(4)) && out.1 == be32(header@.skip(4)),
{ unimplemented!() }

#[verifier::external_body]
pub fn checksum(body: &Bytes) -> (out: u32)
    ensures out == crc(body@),
{ unimplemented!() }

#[verifier::external_body]
pub fn encode_record(record: &Record) -> (out: Bytes)
    ensures out@ == encode(*record),
{ unimplemented!() }

#[verifier::external_body]
pub fn decode_record(body: Bytes) -> (out: Option<Record>)
    ensures out == decode(body@),
{ unimplemented!() }

#[verifier::external_body]
pub fn append_frame(buffer: &mut BytesMut, length: u32, checksum: u32, body: &Bytes)
    ensures final(buffer)@ == old(buffer)@ + (encode_be32(length) + encode_be32(checksum) + body@),
{ unimplemented!() }

#[verifier::external_body]
pub proof fn be32_round_trips(value: u32)
    ensures encode_be32(value).len() == 4, be32(encode_be32(value)) == value,
{ unimplemented!() }

#[verifier::external_body]
pub proof fn prost_record_round_trips(record: Record)
    ensures decode(encode(record)) == Some(record),
{ unimplemented!() }

}
