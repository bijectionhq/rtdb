mod allocation;
mod cells;
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
pub use cells::{CellRef, ContainerKind, DeclarationRef, ExecutionVector, NullChecks, OwnedCell, OwnedValue, PhysicalLayout, RetainedArrowField, RetainedBuffer, UnionEncoding, VectorDescriptor, VectorReadError, VectorReadResult};
pub use conform::{DeclaredBatch, conform_batch};
pub use decimal::{DecimalParameters, INTERNAL_DECIMAL_PRECISION_KEY, INTERNAL_DECIMAL_SCALE_KEY, decimal_field_parameters, decimal_parameters, decimal_scale, decimal_storage_precision, decimal_storage_scale, map_schema_fields, with_decimal_parameters, with_decimal_scale};
pub use execution::{admit_execution_value, execution_field_type, execution_type, read_execution_value};
pub use read_allocation::estimate_read_value_bytes;
pub use iid::{IidView, build_iids};
pub use hash::{hash_cell, hash_value, hash_value_murmur3, murmur3_32, xxhash32};
pub use memory::{MemoryBudget, MemoryReservation, OwnedBatch, OwnedColumn, Selection};
pub use schema::{EXTENSION_METADATA_KEY, EXTENSION_NAME_KEY, from_field, leg_name, to_field};
pub use values::{build_array, copy_array, read_value};

use rtdb_types::Anomaly;

fn arrow_error(error: arrow_schema::ArrowError) -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-arrow", error.to_string())
}
