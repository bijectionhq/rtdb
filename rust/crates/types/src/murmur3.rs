/// MurmurHash3 x86 32-bit, matching `xtdb.util.Hasher.Murmur3` when `seed` is zero.
pub fn murmur3_32(bytes: &[u8], seed: u32) -> u32 {
    let mix = |lane: u32| lane.wrapping_mul(0xcc9e_2d51).rotate_left(15).wrapping_mul(0x1b87_3593);
    let mut hash = seed;
    let mut chunks = bytes.chunks_exact(4);
    for chunk in &mut chunks {
        hash ^= mix(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        hash = hash.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    let tail = chunks.remainder().iter().enumerate().fold(0, |lane, (index, byte)| lane | (u32::from(*byte) << (index * 8)));
    if !chunks.remainder().is_empty() { hash ^= mix(tail); }
    hash ^= bytes.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85eb_ca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2_ae35);
    hash ^ (hash >> 16)
}
