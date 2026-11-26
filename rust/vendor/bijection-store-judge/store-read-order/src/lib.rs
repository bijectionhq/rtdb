//! The one item of Bijection's `store_read` crate that the vendored
//! `judge.rs` calls outside its Verus annotations: `order::le_bytes`, which
//! Bijection defines and proves as `[u8]`'s `<=` (`store_read::order`).

pub mod order {
    /// Whether `a` sorts at or before `b` in unsigned lexicographic byte
    /// order: `[u8]`'s `<=`.
    pub fn le_bytes(a: &[u8], b: &[u8]) -> bool {
        a <= b
    }
}
