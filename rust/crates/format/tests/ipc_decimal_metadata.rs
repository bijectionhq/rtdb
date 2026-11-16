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
fn nested_decimal_wire_metadata_is_rejected_before_native_unchecked_narrowing() {
    for (field,value) in [(0,-1),(0,256),(1,-129),(1,128),(1,i32::MAX),(2,512)] {
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
