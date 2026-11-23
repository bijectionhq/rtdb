#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {

pub open spec fn varint_ends(bytes: Seq<u8>, start: int, end: int) -> bool {
    0 <= start < end <= bytes.len() && end - start <= 10
    && (if end - start == 10 { bytes[end - 1] <= 1 } else { bytes[end - 1] < 128 })
    && (forall|pos: int| start <= pos < end - 1 ==> #[trigger] bytes[pos] >= 128)
}

pub open spec fn varint_value(bytes: Seq<u8>, start: int, width: nat) -> u64
    decreases width,
{
    if width == 0 { 0 }
    else {
        varint_value(bytes, start, (width - 1) as nat)
            | ((bytes[start + width - 1] & 127) as u64) << ((width - 1) * 7) as u64
    }
}

pub open spec fn key_at(bytes: Seq<u8>, start: int, end: int, field: u32, wire: u8) -> bool {
    varint_ends(bytes, start, end)
        && 0 < field <= 0x1fff_ffff && wire <= 7
        && varint_value(bytes, start, (end - start) as nat) == 8 * field + wire
}


pub open spec fn skipped_field(bytes: Seq<u8>, field: u32, wire: u8, start: int, depth: int, end: int) -> bool {
    start <= end <= bytes.len()
        && match wire {
            0 => varint_ends(bytes, start, end),
            1 => end == start + 8,
            2 => start < end && (exists|body: int| #[trigger] varint_ends(bytes, start, body)
                && body <= end && end == body + varint_value(bytes, start, (body - start) as nat)),
            3 => depth < 100 && start < end
                && (exists|at: int| start <= at < end && #[trigger] key_at(bytes, at, end, field, 4)),
            5 => end == start + 4,
            _ => false,
        }
}

pub ghost struct FieldSpan {
    pub start: int,
    pub body: int,
    pub end: int,
    pub field: u32,
    pub wire: u8,
}

pub closed spec fn scan_spine(bytes: Seq<u8>, start: int, end: int, spans: Seq<FieldSpan>, found: bool) -> bool {
    (spans.len() == 0 ==> end == start)
        && (spans.len() > 0 ==> spans[0].start == start && spans.last().end == end)
        && (forall|i: int| 0 <= i < spans.len() ==> {
            let span = #[trigger] spans[i];
            key_at(bytes, span.start, span.body, span.field, span.wire)
                && skipped_field(bytes, span.field, span.wire, span.body, 0, span.end)
                && (i > 0 ==> spans[i - 1].end == span.start)
        })
        && found == (exists|i: int| 0 <= i < spans.len() && (#[trigger] spans[i]).field == 8 && spans[i].wire == 2)
}


pub closed spec fn scan_complete(bytes: Seq<u8>, start: int, found: bool) -> bool {
    exists|spans: Seq<FieldSpan>| #[trigger] scan_spine(bytes, start, bytes.len() as int, spans, found)
}


}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures match out {
        Ok((value, end)) => varint_ends(bytes@, start as int, end as int)
            && value == varint_value(bytes@, start as int, (end - start) as nat),
        Err(_) => !(exists|end: int| #[trigger] varint_ends(bytes@, start as int, end)),
    },
))]
pub(super) fn wire_varint(bytes: &[u8], start: usize) -> Result<(u64, usize), &'static str> {
    let mut value = 0;
    let mut index = 0usize;
    let mut cursor = start;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            index <= 9, cursor == start + index,
            forall|pos: int| start <= pos < cursor ==> #[trigger] bytes@[pos] >= 128,
            index > 0 ==> cursor <= bytes@.len(),
            value == varint_value(bytes@, start as int, index as nat),
        decreases 10 - index,
    ))]
    while index < 10 {
        if cursor >= bytes.len() {
            #[cfg(verus_keep_ghost)]
            proof! {
                assert forall|end: int| #[trigger] varint_ends(bytes@, start as int, end)
                    implies false by {
                    assert(start <= end - 1 < cursor);
                }
            }
            return Err("Truncated protobuf varint");
        }
        let byte = bytes[cursor];
        cursor += 1;
        if index == 9 && byte > 1 {
            #[cfg(verus_keep_ghost)]
            proof! {
                assert forall|end: int| #[trigger] varint_ends(bytes@, start as int, end)
                    implies false by {
                    if end < cursor { assert(start <= end - 1 < cursor - 1); }
                }
            }
            return Err("Protobuf varint exceeds 64 bits");
        }
        value |= ((byte & 127) as u64) << ((index * 7) as u64);
        #[cfg(verus_keep_ghost)]
        proof! {
            reveal_with_fuel(varint_value, 2);
            assert(value == varint_value(bytes@, start as int, (index + 1) as nat));
        }
        if byte < 128 { return Ok((value, cursor)); }
        index += 1;
    }
    Err("Unterminated protobuf varint")
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out matches Ok((field, wire, end)) ==>
        key_at(bytes@, start as int, end as int, field, wire),
))]
pub(super) fn wire_key(bytes: &[u8], start: usize) -> Result<(u32, u8, usize), &'static str> {
    let (key, end) = wire_varint(bytes, start)?;
    let field = key / 8;
    if field == 0 || field > 0x1fff_ffff { return Err("Invalid protobuf field number"); }
    Ok((field as u32, (key % 8) as u8, end))
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures match out {
        Some(end) => end == body + length && end <= available,
        None => body + length > available,
    },
))]
pub(super) fn field_end(available: usize, body: usize, length: u64) -> Option<usize> {
    if body > available || length > (available - body) as u64 { return None; }
    Some(body + length as usize)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out matches Ok(end) ==> skipped_field(bytes@, field, wire, start as int, depth as int, end as int),
    decreases usize::MAX - depth,
))]
pub(super) fn skip_wire(bytes: &[u8], field: u32, wire: u8, start: usize, depth: usize)
    -> Result<usize, &'static str>
{
    let (body, length) = match wire {
        0 => { let (_, end) = wire_varint(bytes, start)?; return Ok(end); }
        1 => (start, 8),
        2 => { let (length, body) = wire_varint(bytes, start)?; (body, length) }
        3 => {
            if depth >= 100 { return Err("Protobuf group nesting exceeds limit"); }
            let mut cursor = start;
            #[cfg_attr(verus_keep_ghost, verus_spec(
                invariant start <= cursor, depth < 100, wire == 3,
                decreases if cursor < bytes@.len() { bytes@.len() as int - cursor } else { 0int },
            ))]
            loop {
                let (nested_field, nested_wire, next) = wire_key(bytes, cursor)?;
                if nested_wire == 4 {
                    #[cfg(verus_keep_ghost)]
                    proof! {
                        if nested_field == field {
                            assert(key_at(bytes@, cursor as int, next as int, field, 4));
                            assert(exists|at: int| start <= at < next && #[trigger] key_at(bytes@, at, next as int, field, 4));
                        }
                    }
                    return if nested_field == field { Ok(next) } else { Err("Mismatched protobuf end group") };
                }
                cursor = skip_wire(bytes, nested_field, nested_wire, next, depth + 1)?;
            }
        }
        5 => (start, 4),
        _ => return Err("Invalid protobuf wire type"),
    };
    let end = field_end(bytes.len(), body, length).ok_or("Truncated protobuf field")?;
    #[cfg(verus_keep_ghost)]
    proof! {
        if wire == 2 { assert(varint_ends(bytes@, start as int, body as int)); }
    }
    Ok(end)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out matches Ok((found, end)) ==> start <= end == bytes@.len()
        && scan_complete(bytes@, start as int, found),
))]
pub(super) fn scan_external_payload(bytes: &[u8], start: usize) -> Result<(bool, usize), &'static str> {
    let mut cursor = start;
    let mut found = false;
    #[cfg(verus_keep_ghost)]
    proof_decl! { let ghost mut spans = Seq::<FieldSpan>::empty(); }
    if cursor > bytes.len() { return Err("Truncated protobuf message"); }
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            start <= cursor <= bytes@.len(),
            scan_spine(bytes@, start as int, cursor as int, spans, found),
        decreases bytes@.len() - cursor,
    ))]
    while cursor < bytes.len() {
        #[cfg(verus_keep_ghost)]
        proof_decl! { let ghost previous = cursor; }
        let (field, wire, body) = wire_key(bytes, cursor)?;
        cursor = skip_wire(bytes, field, wire, body, 0)?;
        #[cfg(verus_keep_ghost)]
        proof_decl! { let ghost old_found = found; }
        if field == 8 && wire == 2 { found = true; }
        #[cfg(verus_keep_ghost)]
        proof! {
            let state = spans;
            let span = FieldSpan { start: previous as int, body: body as int, end: cursor as int, field, wire };
            spans = state.push(span);
            assert forall|i: int| 0 <= i < spans.len() implies ({
                let span = #[trigger] spans[i];
                key_at(bytes@, span.start, span.body, span.field, span.wire)
                    && skipped_field(bytes@, span.field, span.wire, span.body, 0, span.end)
                    && (i > 0 ==> spans[i - 1].end == span.start)
            }) by {
                if i < state.len() { assert(spans[i] == state[i]); }
            }
            if old_found {
                let i = choose|i: int| 0 <= i < state.len() && (#[trigger] state[i]).field == 8 && state[i].wire == 2;
                assert(spans[i] == state[i]);
            } else if found {
                assert(spans[state.len() as int] == span);
            } else {
                assert forall|i: int| 0 <= i < spans.len() implies
                    (#[trigger] spans[i]).field != 8 || spans[i].wire != 2 by {
                    if i < state.len() { assert(spans[i] == state[i]); }
                }
            }
            assert(scan_spine(bytes@, start as int, cursor as int, spans, found));
        }
    }
    let result = Ok((found, cursor));
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(result matches Ok((present, end)) && present == found && end == cursor);
        assert((exists|spans: Seq<FieldSpan>| #[trigger] scan_spine(bytes@, start as int, bytes@.len() as int, spans, found)));
    }
    result
}
