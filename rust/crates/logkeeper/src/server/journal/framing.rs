use super::{Kind, Record, journal_codec_leaves::{
    Bytes, BytesMut, IoResult, Read, append_frame, body_len, checksum, decode_record, empty_bytes, encode_record,
    header_fields, read_body, read_header, split_frame,
}};

#[cfg(verus_keep_ghost)]
use super::journal_codec_leaves::{
    empty, encode, frame_bytes, frame_matches,
};

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (remaining >= 8),
))]
pub(super) fn has_header(remaining: u64) -> bool {
    remaining >= 8
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (out is Some) <==> 8int + length <= remaining && 8int + length <= usize::MAX,
        out matches Some(end) ==> end == 8int + length,
))]
pub(super) fn frame_size(remaining: u64, length: u32) -> Option<usize> {
    let end = 8 + length as u64;
    if end > remaining || end > usize::MAX as u64 { return None; }
    Some(end as usize)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (available == declared && actual_checksum == expected_checksum),
))]
pub(super) fn matches_body(available: usize, declared: usize, actual_checksum: u32, expected_checksum: u32) -> bool {
    available == declared && actual_checksum == expected_checksum
}

#[cfg_attr(verus_keep_ghost, verus_spec(
    requires encode(Record { stream, kind: Some(kind), active_ensemble: empty(), original_append: None }).len() <= u32::MAX,
    ensures final(buffer)@ == old(buffer)@ + frame_bytes(encode(
        Record { stream, kind: Some(kind), active_ensemble: empty(), original_append: None })),
))]
pub(super) fn frame(stream: u64, kind: Kind, buffer: &mut BytesMut) {
    frame_record(Record { stream, kind: Some(kind), active_ensemble: empty_bytes(), original_append: None }, buffer)
}

#[cfg_attr(verus_keep_ghost, verus_spec(
    requires encode(record).len() <= u32::MAX,
    ensures final(buffer)@ == old(buffer)@ + frame_bytes(encode(record)),
))]
pub(super) fn frame_record(record: Record, buffer: &mut BytesMut) {
    let body = encode_record(&record);
    append_frame(buffer, body_len(&body) as u32, checksum(&body), &body);
}

/// Reads the next whole record of a segment of `remaining` bytes from here, with its framed length.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out matches Ok(Some((record, end))) ==>
        8 <= end <= remaining && end <= old(reader).remaining().len()
        && frame_matches(old(reader).remaining().take(end as int), record)
        && final(reader).remaining() == old(reader).remaining().skip(end as int),
))]
pub(super) fn read_record(reader: &mut impl Read, remaining: u64) -> IoResult<Option<(Record, u64)>> {
    if !has_header(remaining) { return Ok(None); }
    let header = read_header(reader)?;
    let (length, expected_checksum) = header_fields(header);
    let Some(end) = frame_size(remaining, length) else { return Ok(None); };
    let body = read_body(reader, end - 8)?;
    if !matches_body(body_len(&body), length as usize, checksum(&body), expected_checksum) { return Ok(None); }
    let Some(record) = decode_record(body) else { return Ok(None); };
    #[cfg(verus_keep_ghost)]
    proof! {
        let input = old(reader).remaining();
        assert(input.take(end as int).take(8) =~= input.take(8));
        assert(input.take(end as int).skip(8) =~= input.skip(8).take(end - 8));
        assert(input.take(8).take(4) =~= input.take(end as int).take(4));
        assert(input.take(8).skip(4) =~= input.take(end as int).subrange(4, 8));
        assert(input.skip(8).skip(end - 8) =~= input.skip(end as int));
    }
    Ok(Some((record, end as u64)))
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures match out {
        Some(record) => frame_matches(frame@, record),
        None => !(exists|record: Record| #[trigger] frame_matches(frame@, record)),
    },
))]
pub(super) fn decode_located_record(frame: &Bytes) -> Option<Record> {
    let (header, body) = split_frame(frame)?;
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(header@.take(4) =~= frame@.take(4));
        assert(header@.skip(4) =~= frame@.subrange(4, 8));
    }
    let (length, expected_checksum) = header_fields(header);
    if !matches_body(body_len(&body), length as usize, checksum(&body), expected_checksum) { return None; }
    decode_record(body)
}
