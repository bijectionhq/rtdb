use rtdb_plan::explain_analyze_schema;
use rtdb_types::{LogicalType, ScalarType, TimeUnit};

#[test]
fn explain_analyze_fields_preserve_duration_and_nested_attribute_types() {
    let schema = explain_analyze_schema().unwrap();
    assert_eq!(schema.fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["depth", "op", "attributes", "total_time", "self_time", "time_to_first_page", "page_count", "row_count", "pushdowns", "peak_memory_bytes", "query_peak_memory_bytes"]);
    assert_eq!(schema.fields()[1].data_type, LogicalType::Scalar(ScalarType::Keyword));
    assert_eq!(schema.fields()[3].data_type, LogicalType::Scalar(ScalarType::Duration(TimeUnit::Microsecond)));
    assert!(schema.fields()[5].data_type.nullable());
    let LogicalType::Maybe(attributes) = &schema.fields()[2].data_type else { panic!("Expected nullable attributes") };
    let LogicalType::Struct(attributes) = attributes.as_ref() else { panic!("Expected nested attributes") };
    assert_eq!(attributes.len(), 7);
    assert_eq!(attributes["scan_db"], LogicalType::Scalar(ScalarType::Utf8));
    assert_eq!(attributes["scan_rows_read"], LogicalType::Scalar(ScalarType::I64));
}
