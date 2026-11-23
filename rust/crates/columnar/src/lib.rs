mod allocation;
mod conform;
mod decimal;
mod execution;
mod iid;
mod hash;
mod memory;
mod read_allocation;
mod schema;
mod values;

pub use allocation::{estimate_builder_bytes, estimate_builder_bytes_refs};
pub use conform::{DeclaredBatch, conform_batch, conform_float_list};
pub use decimal::{DecimalParameters, INTERNAL_DECIMAL_PRECISION_KEY, INTERNAL_DECIMAL_SCALE_KEY, decimal_field_parameters, decimal_parameters, decimal_scale, decimal_storage_precision, decimal_storage_scale, map_schema_fields, with_decimal_parameters, with_decimal_scale};
pub use execution::{admit_execution_value, execution_field_type, execution_type, read_execution_value, read_execution_value_as};
pub use read_allocation::{estimate_read_cell_bytes, estimate_read_value_bytes};
pub use iid::{IidView, build_iids};
pub use hash::{hash_cell, hash_value, hash_value_murmur3, murmur3_32, xxhash32};
pub use memory::{BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, OwnedColumn, Selection, shared_allocation_size};
pub use schema::{EXTENSION_METADATA_KEY, EXTENSION_NAME_KEY, from_field, leg_name, to_field};
pub use values::{ColumnReader, build_array, build_struct_array, copy_array, read_value, read_value_as, struct_field_cell};

use rtdb_types::Anomaly;

fn arrow_error(error: arrow_schema::ArrowError) -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-arrow", error.to_string())
}
