use std::{io::Cursor, sync::Arc};
use arrow_schema::{DataType, Field, Schema};
use rtdb_format::ipc;

fn schema() -> Schema {
    Schema::new(vec![Field::new("items",DataType::List(Arc::new(Field::new("item",DataType::Struct(vec![Field::new("value",DataType::Decimal256(2,1),true)].into()),true))),true)])
}

fn scalar_positions(schema: arrow_ipc::Schema<'_>, base: usize) -> [usize;3] {
    let field = schema.fields().unwrap().get(0).children().unwrap().get(0).children().unwrap().get(0);
    let decimal = field.type_as_decimal().unwrap();
    [arrow_ipc::Decimal::VT_PRECISION,arrow_ipc::Decimal::VT_SCALE,arrow_ipc::Decimal::VT_BITWIDTH].map(|slot| {
        let offset = decimal._tab.vtable().get(slot);
        assert_ne!(offset,0);
        base+decimal._tab.loc()+usize::from(offset)
    })
}

fn stream_positions(bytes: &[u8]) -> [usize;3] {
    let base = if bytes.starts_with(&[0xff;4]) { 8 } else { 4 };
    scalar_positions(arrow_ipc::root_as_message(&bytes[base..]).unwrap().header_as_schema().unwrap(),base)
}

fn file_positions(bytes: &[u8]) -> [usize;3] {
    let length = arrow_ipc::reader::read_footer_length(bytes[bytes.len()-10..].try_into().unwrap()).unwrap();
    let base = bytes.len()-10-length;
    scalar_positions(arrow_ipc::root_as_footer(&bytes[base..bytes.len()-10]).unwrap().schema().unwrap(),base)
}

#[test]
fn nested_invalid_decimal_width_is_rejected_before_native_unchecked_narrowing() {
    for (field,value) in [(2,0i32),(2,512)] {
        let mut stream = ipc::write_stream(&schema(),&[]).unwrap();
        let position = stream_positions(&stream)[field];
        stream[position..position+4].copy_from_slice(&value.to_le_bytes());
        assert_eq!(ipc::read_schema_message(&stream).unwrap_err().code,"rtdb.format/decimal-metadata");
        assert_eq!(ipc::stream_allocation_bound(&stream).unwrap_err().code,"rtdb.format/decimal-metadata");
        assert_eq!(ipc::ArrowStream::open(stream.into(),None).err().unwrap().code,"rtdb.format/decimal-metadata");

        let mut file = ipc::write_file(&schema(),&[]).unwrap();
        let position = file_positions(&file)[field];
        file[position..position+4].copy_from_slice(&value.to_le_bytes());
        assert_eq!(ipc::file_allocation_bound(&file).unwrap_err().code,"rtdb.format/decimal-metadata");
        assert_eq!(ipc::inspect_file(&mut Cursor::new(&file)).unwrap_err().code,"rtdb.format/decimal-metadata");
        assert_eq!(ipc::ArrowFile::open(file.into(),None).err().unwrap().code,"rtdb.format/decimal-metadata");
    }
}

#[test]
fn nested_external_decimal_scales_are_derived_from_real_wire_metadata() {
    for scale in [i32::MIN, -129, 128, i32::MAX] {
        let mut message = ipc::write_schema_message(&schema()).unwrap();
        let position = stream_positions(&message)[1];
        message[position..position+4].copy_from_slice(&scale.to_le_bytes());
        let decoded = ipc::read_schema_message(&message).unwrap();
        let DataType::List(item) = decoded.field(0).data_type() else { panic!("Expected list") };
        let DataType::Struct(children) = item.data_type() else { panic!("Expected struct") };
        assert_eq!(rtdb_columnar::decimal_scale(&children[0]).unwrap(), Some(scale));
        assert_eq!(children[0].data_type(), &DataType::Decimal256(2, 1));
        assert_eq!(ipc::write_schema_message(&decoded).unwrap(), message);
    }
}

#[test]
fn reserved_scale_metadata_cannot_override_external_decimal_metadata() {
    for key in [rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY, rtdb_columnar::INTERNAL_DECIMAL_PRECISION_KEY] {
    for schema_level in [false, true] {
        let metadata = std::collections::HashMap::from([(key.to_owned(), i32::MAX.to_string())]);
        let field = Field::new("value", DataType::Decimal128(3, 1), true);
        let schema = if schema_level { Schema::new_with_metadata(vec![field], metadata) } else { Schema::new(vec![field.with_metadata(metadata)]) };
        let writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema).unwrap();
        let bytes = writer.into_inner().unwrap();
        assert!(ipc::read_schema_message(&bytes).is_err());
        assert!(ipc::stream_allocation_bound(&bytes).is_err());
        let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), &schema).unwrap();
        writer.finish().unwrap();
        assert!(ipc::ArrowFile::open(writer.into_inner().unwrap().into(), None).is_err());
    }
    }
}

#[test]
fn nested_external_signed_precision_is_derived_from_real_wire_metadata() {
    for precision in [i32::MIN, -1, 0, 77, 255, 256, i32::MAX] {
        let mut message = ipc::write_schema_message(&schema()).unwrap();
        let position = stream_positions(&message)[0];
        message[position..position+4].copy_from_slice(&precision.to_le_bytes());
        let decoded = ipc::read_schema_message(&message).unwrap();
        let DataType::List(item) = decoded.field(0).data_type() else { panic!("Expected list") };
        let DataType::Struct(children) = item.data_type() else { panic!("Expected struct") };
        assert_eq!(rtdb_columnar::decimal_field_parameters(&children[0]).unwrap().unwrap().precision, precision);
        assert_eq!(children[0].data_type(), &DataType::Decimal256(rtdb_columnar::decimal_storage_precision(precision), 1));
        if precision != 0 { assert_eq!(ipc::write_schema_message(&decoded).unwrap(), message); }
    }
}

#[test]
fn zero_precision_and_scale_larger_than_precision_survive_metadata_preflight() {
    for (precision,scale) in [(0,-128),(0,127),(1,3)] {
        let mut message = ipc::write_schema_message(&schema()).unwrap();
        let [precision_position,scale_position,_] = stream_positions(&message);
        message[precision_position..precision_position+4].copy_from_slice(&i32::to_le_bytes(precision));
        message[scale_position..scale_position+4].copy_from_slice(&i32::to_le_bytes(scale));
        let decoded = ipc::read_schema_message(&message).unwrap();
        let DataType::List(item) = decoded.field(0).data_type() else { panic!("Expected list") };
        let DataType::Struct(children) = item.data_type() else { panic!("Expected struct") };
        assert_eq!(children[0].data_type(),&DataType::Decimal256(precision as u8,scale as i8));
    }
}
