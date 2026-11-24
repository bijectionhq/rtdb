#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
    pub(crate) open spec fn ordinal(bytes: Seq<u8>, start: int) -> u32 {
        ((bytes[start] as u32) << 24) | ((bytes[start + 1] as u32) << 16)
            | ((bytes[start + 2] as u32) << 8) | (bytes[start + 3] as u32)
    }

    pub(crate) open spec fn admitted(bytes: Seq<u8>, inventory: Seq<u32>, positions: Seq<u32>) -> bool {
        &&& positions.len() * 4 == bytes.len()
        &&& forall|i: int| 0 <= i < positions.len() ==>
            (#[trigger] positions[i]) == ordinal(bytes, i * 4)
            && (positions[i] != 0 ==> inventory.contains(positions[i]))
        &&& forall|i: int, j: int| 0 <= i < j < positions.len()
            && (#[trigger] positions[i]) != 0 && (#[trigger] positions[j]) != 0
            ==> positions[i] < positions[j]
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(plan => ensures
    plan is Some ==> admitted(body@, inventory@, plan.unwrap()@),
    body@.len() % 4 != 0 ==> plan is None,
))]
pub(super) fn decode(body: &[u8], inventory: &[u32]) -> Option<Vec<u32>> {
    if body.len() % 4 != 0 {
        return None;
    }
    let mut positions: Vec<u32> = Vec::with_capacity(body.len() / 4);
    let mut previous = 0;
    let mut offset = 0;
    let mut member = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            offset <= body@.len(),
            offset % 4 == 0,
            body@.len() % 4 == 0,
            positions@.len() * 4 == offset,
            member <= inventory@.len(),
            forall|i: int| 0 <= i < positions@.len() ==>
                (#[trigger] positions@[i]) == ordinal(body@, i * 4)
                && positions@[i] <= previous
                && (positions@[i] != 0 ==> inventory@.contains(positions@[i])),
            forall|i: int, j: int| 0 <= i < j < positions@.len()
                && (#[trigger] positions@[i]) != 0 && (#[trigger] positions@[j]) != 0
                ==> positions@[i] < positions@[j],
        decreases body@.len() - offset,
    ))]
    while offset < body.len() {
        let position = (u32::from(body[offset]) << 24) | (u32::from(body[offset + 1]) << 16)
            | (u32::from(body[offset + 2]) << 8) | u32::from(body[offset + 3]);
        if position != 0 {
            if position <= previous {
                return None;
            }
            #[cfg_attr(verus_keep_ghost, verus_spec(
                invariant member <= inventory@.len(),
                decreases inventory@.len() - member,
            ))]
            while member < inventory.len() && inventory[member] < position {
                member += 1;
            }
            if member == inventory.len() || inventory[member] != position {
                return None;
            }
            #[cfg(verus_keep_ghost)]
            proof! { assert(inventory@.contains(position)); }
            previous = position;
            member += 1;
        }
        positions.push(position);
        offset += 4;
    }
    Some(positions)
}
