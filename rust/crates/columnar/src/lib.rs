mod allocation;
mod iid;
mod hash;
mod memory;
mod schema;
mod values;

pub use allocation::{estimate_builder_bytes, estimate_builder_bytes_refs};
pub use iid::{IidView, build_iids};
pub use hash::{hash_cell, hash_value, murmur3_32, xxhash32};
pub use memory::{MemoryBudget, MemoryReservation, OwnedBatch, OwnedColumn, Selection};
pub use schema::{EXTENSION_METADATA_KEY, EXTENSION_NAME_KEY, from_field, leg_name, to_field};
pub use values::{build_array, read_value};

use rtdb_types::Anomaly;

fn arrow_error(error: arrow_schema::ArrowError) -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-arrow", error.to_string())
}
