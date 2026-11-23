mod allocation;
mod cell;
mod conform;
mod decimal;
mod entity_id;
mod execution;
mod iid;
mod interleave;
mod hash;
mod memory;
mod read_allocation;
mod read_check;
mod schema;
mod text;
mod values;

pub use allocation::{estimate_builder_bytes, estimate_builder_bytes_refs};
pub use cell::ArrowCell;
pub use conform::{DeclaredBatch, arrow_relayout_bound, conform_batch, event_relayout_bound, relayout_arrow_column, relayout_event_column};
pub use decimal::{DecimalParameters, INTERNAL_DECIMAL_PRECISION_KEY, INTERNAL_DECIMAL_SCALE_KEY, decimal_field_parameters, decimal_parameters, decimal_scale, decimal_storage_precision, decimal_storage_scale, map_schema_fields, validate_projected_decimals, with_decimal_parameters, with_decimal_scale};
pub use entity_id::{EntityIdRef, entity_id_array, entity_id_type, read_entity_id};
pub use execution::{admit_execution_value, execution_field_type, execution_type, read_execution_value, read_execution_value_as};
pub use read_allocation::{estimate_copy_cell_bytes, estimate_event_copy_cell_bytes, estimate_event_null_cell_bytes, estimate_null_copy_cell_bytes,
    estimate_read_cell_bytes, estimate_read_value_bytes};
pub use read_check::{ColumnPresence, validate_readable_cell, validate_readable_column};
pub use iid::{IidView, build_iids};
pub use interleave::{copy_event_cells, copy_nullable_cells, copy_selected_cells, interleave};
pub use hash::{decimal_cell_as_f64, hash_cell, hash_value, hash_value_murmur3, murmur3_32, xxhash32};
pub use memory::{BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, OwnedColumn, Selection, retain_array, retain_buffer, retain_buffer_with_owner, shared_allocation_size};
pub use schema::{EXTENSION_METADATA_KEY, EXTENSION_NAME_KEY, from_field, leg_name, to_field};
pub use text::{TextCell, binary_cell, selected_cell, text_cell, utf8_list};
pub use values::{ColumnReader, build_array, build_struct_array, copy_array, copy_evaluated_cells, read_value, read_value_as, repack_numeric_list, struct_field_cell};

use rtdb_types::Anomaly;

fn arrow_error(error: arrow_schema::ArrowError) -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-arrow", error.to_string())
}
