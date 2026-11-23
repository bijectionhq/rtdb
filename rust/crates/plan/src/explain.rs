use rtdb_types::StructFields;
use serde::{Deserialize, Serialize};
use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit};
use crate::{ColumnId, Field, Schema};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExplainMode { Plan, Analyze }

/// Result fields for measured execution; durations use microseconds and pushdowns retain Transit structure.
pub fn explain_analyze_schema() -> Result<Schema> {
    let duration = LogicalType::Scalar(ScalarType::Duration(TimeUnit::Microsecond));
    let int = LogicalType::Scalar(ScalarType::I64);
    let string = LogicalType::Scalar(ScalarType::Utf8);
    let attributes = LogicalType::Struct(StructFields::from([
        ("scan_db".into(), string.clone()), ("scan_source".into(), string.clone()),
        ("scan_files_pruned".into(), int.clone()), ("scan_files_used".into(), int.clone()),
        ("scan_pages_pruned".into(), int.clone()), ("scan_pages_used".into(), int.clone()), ("scan_rows_read".into(), int.clone()),
    ])).maybe();
    Schema::new(vec![
        ("depth", string), ("op", LogicalType::Scalar(ScalarType::Keyword)), ("attributes", attributes),
        ("total_time", duration.clone()), ("self_time", duration.clone()), ("time_to_first_page", duration.maybe()),
        ("page_count", int.clone()), ("row_count", int.clone()), ("pushdowns", LogicalType::Scalar(ScalarType::Transit).maybe()),
        ("peak_memory_bytes", int.clone()), ("query_peak_memory_bytes", int),
    ].into_iter().enumerate().map(|(index, (name, data_type))| Field { id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type }).collect())
}
