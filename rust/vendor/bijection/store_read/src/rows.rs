//! Row keys at field and presence grain (RTDB-PLAN §3, owner decision 2):
//!
//! ```text
//! row ‖ 0x00          presence: the row exists
//! row ‖ 0x01 ‖ field  one field's value
//! ```
//!
//! `row` must be prefix-free among rows (the kernel's escaped, terminated
//! segments are), so one row's keys never fall inside another row's. The
//! read sets and writes over these keys are `fact_store::persistence::rows`.
use crate::store::KeyRange;

pub const PRESENCE: u8 = 0x00;
pub const FIELD: u8 = 0x01;

/// The row's presence key.
pub fn presence(row: &[u8]) -> Vec<u8> {
    [row, &[PRESENCE]].concat()
}

/// The key of one field of the row.
pub fn field(row: &[u8], name: &[u8]) -> Vec<u8> {
    [row, &[FIELD], name].concat()
}

/// Every key of the row: presence and all fields.
pub fn whole(row: &[u8]) -> KeyRange {
    KeyRange::prefix(row)
}

/// Every field key of the row, without its presence key.
pub fn fields(row: &[u8]) -> KeyRange {
    KeyRange::prefix(&[row, &[FIELD]].concat())
}
