#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

const HEX: &[u8; 16] = b"0123456789abcdef";

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn writer_byte_matches(grant: Seq<u8>, text: Seq<u8>, i: int) -> bool {
        &&& text[2 * i] == b"0123456789abcdef"@[(grant[i] as int) / 16]
        &&& text[2 * i + 1] == b"0123456789abcdef"@[(grant[i] as int) % 16]
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(fields => ensures
    fields.0@ == bytes@.subrange(0, 8),
    fields.1@ == bytes@.subrange(8, 24),
    fields.2@ == bytes@.subrange(24, 40),
))]
pub(super) fn split_fields(bytes: &[u8; 40]) -> ([u8; 8], [u8; 16], [u8; 16]) {
    let mut revision = [0; 8];
    let mut i = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= 8,
        forall|j: int| 0 <= j < i ==> revision@[j] == bytes@[j],
        decreases 8 - i,
    ))]
    while i < 8 {
        revision[i] = bytes[i];
        i += 1;
    }
    let mut incarnation = [0; 16];
    let mut writer = [0; 16];
    let mut i = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= 16,
        forall|j: int| 0 <= j < i ==> incarnation@[j] == bytes@[8 + j],
        forall|j: int| 0 <= j < i ==> writer@[j] == bytes@[24 + j],
        decreases 16 - i,
    ))]
    while i < 16 {
        incarnation[i] = bytes[8 + i];
        writer[i] = bytes[24 + i];
        i += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(revision@ =~= bytes@.subrange(0, 8));
        assert(incarnation@ =~= bytes@.subrange(8, 24));
        assert(writer@ =~= bytes@.subrange(24, 40));
    }
    (revision, incarnation, writer)
}

#[cfg_attr(verus_keep_ghost, verus_spec(matches => ensures matches == (expected == Some(visible))))]
pub(super) fn revision_matches(expected: Option<i64>, visible: i64) -> bool {
    expected == Some(visible)
}

#[cfg_attr(verus_keep_ghost, verus_spec(matches => ensures
    matches == (observed@ == incarnation@),
))]
fn incarnation_matches(incarnation: &[u8; 16], observed: &[u8]) -> bool {
    if observed.len() != 16 {
        return false;
    }
    let mut i = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= 16, observed@.len() == 16,
        forall|j: int| 0 <= j < i ==> observed@[j] == incarnation@[j],
        decreases 16 - i,
    ))]
    while i < 16 {
        if observed[i] != incarnation[i] {
            return false;
        }
        i += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! { assert(observed@ =~= incarnation@); }
    true
}

#[cfg_attr(verus_keep_ghost, verus_spec(matches => ensures
    matches == (observed@.len() == 32
        && forall|j: int| 0 <= j < 16 ==> #[trigger] writer_byte_matches(writer@, observed@, j)),
))]
fn writer_matches(writer: &[u8; 16], observed: &[u8]) -> bool {
    if observed.len() != 32 {
        return false;
    }
    let mut i = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= 16, observed@.len() == 32,
        forall|j: int| 0 <= j < i ==> #[trigger] writer_byte_matches(writer@, observed@, j),
        decreases 16 - i,
    ))]
    while i < 16 {
        let byte = writer[i];
        if observed[2 * i] != HEX[(byte / 16) as usize]
            || observed[2 * i + 1] != HEX[(byte % 16) as usize]
        {
            #[cfg(verus_keep_ghost)]
            proof! { assert(!writer_byte_matches(writer@, observed@, i as int)); }
            return false;
        }
        #[cfg(verus_keep_ghost)]
        proof! { assert(writer_byte_matches(writer@, observed@, i as int)); }
        i += 1;
    }
    true
}

#[cfg_attr(verus_keep_ghost, verus_spec(matches => ensures
    matches == ((kind@ == "hosted"@ || kind@ == "standalone"@)
        && observed_incarnation@ == incarnation@ && observed_writer@.len() == 32
        && forall|i: int| 0 <= i < 16 ==> #[trigger] writer_byte_matches(writer@, observed_writer@, i)),
))]
pub(super) fn identity_matches(
    incarnation: &[u8; 16],
    writer: &[u8; 16],
    kind: &str,
    observed_incarnation: &[u8],
    observed_writer: &[u8],
) -> bool {
    (kind == "hosted" || kind == "standalone")
        && incarnation_matches(incarnation, observed_incarnation)
        && writer_matches(writer, observed_writer)
}
