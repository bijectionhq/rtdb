#![allow(unexpected_cfgs)]
#![cfg(kani)]

use rtdb_trie::Bucketer;
use rtdb_types::Iid;

#[kani::proof]
fn bucket_reads_the_designated_iid_bits_at_every_supported_width() {
    let iid = Iid(kani::any());
    let kind: u8 = kani::any();
    kani::assume(kind < 3);
    let width = [2, 4, 8][usize::from(kind)];
    let bucketer = Bucketer::new(width).unwrap();
    let depth: usize = kani::any();
    kani::assume(depth < 128 / usize::from(width));
    let packed = u128::from_be_bytes(iid.0);
    let shift = 128 - (depth + 1) * usize::from(width);
    let expected = ((packed >> shift) & ((1u128 << width) - 1)) as u8;
    assert_eq!(bucketer.bucket(iid, depth).unwrap(), expected);
    if width == 2 {
        assert_eq!(iid.bucket(depth).unwrap(), expected);
    }
}
