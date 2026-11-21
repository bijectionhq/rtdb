use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rtdb_format::ipc::{ArrowFile, SparseFile, inspect_file_reserved, write_file};

#[test]
fn a_file_opened_from_its_footer_reads_a_page_from_that_page_s_bytes_alone() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let pages = (0..3i64).map(|page| RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from_iter_values(page * 10..page * 10 + 10)) as ArrayRef]).unwrap()).collect::<Vec<_>>();
    let bytes: Arc<Vec<u8>> = Arc::new(write_file(&schema, &pages).unwrap());
    let layout = inspect_file_reserved(&mut std::io::Cursor::new(bytes.as_slice()), |bytes| rtdb_columnar::MemoryBudget::new(1 << 30).reserve(bytes)).unwrap();
    let length = bytes.len() as u64;
    let footer = length - layout.footer_encoded_bytes as u64;
    let file = SparseFile::new(length);
    file.add(footer, Arc::new(bytes[footer as usize..].to_vec()));
    let mut reader = ArrowFile::from_inspected_reader(file.clone(), None, layout).unwrap();
    let range = reader.page_range(1).unwrap();
    file.retain_from(0);
    file.add(range.start, Arc::new(bytes[range.start as usize..range.end as usize].to_vec()));
    assert_eq!(reader.read_page(1).unwrap(), pages[1]);
    assert!(reader.read_page(0).is_err(), "a page whose bytes were not fetched cannot be read");
}

#[test]
fn a_selected_batch_can_read_its_dictionary_without_other_batch_bytes() {
    use arrow_array::{builder::StringDictionaryBuilder, types::Int32Type};
    use arrow_ipc::writer::FileWriter;
    use rtdb_format::ipc::{ArrowFileFooter, ArrowFilePages, file_footer_range};

    let mut values = StringDictionaryBuilder::<Int32Type>::new();
    values.append("often").unwrap();
    values.append("rarely").unwrap();
    values.append("often").unwrap();
    let array = Arc::new(values.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("status", array.data_type().clone(), false)]));
    let page = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
    let mut writer = FileWriter::try_new(Vec::new(), &schema).unwrap();
    writer.write(&page).unwrap();
    writer.write(&page).unwrap();
    let bytes = writer.into_inner().unwrap();

    let trailer = bytes[bytes.len() - 10..].try_into().unwrap();
    let footer_range = file_footer_range(bytes.len() as u64, trailer).unwrap();
    let footer = ArrowFileFooter::parse(&bytes[footer_range.start as usize..footer_range.end as usize], footer_range.start).unwrap();
    let mut reader = ArrowFilePages::new(&footer).unwrap();
    for (index, range) in footer.dictionary_ranges().enumerate() {
        let range = range.unwrap();
        let bytes = bytes[range.start as usize..range.end as usize].to_vec();
        assert!(footer.dictionary_allocation_bound(index, &bytes).unwrap() >= bytes.len());
        reader.read_dictionary(&footer, index, bytes).unwrap();
    }

    let range = reader.page_range(1).unwrap();
    let bytes = bytes[range.start as usize..range.end as usize].to_vec();
    assert!(reader.page_allocation_bound(1, &bytes).unwrap() >= bytes.len());
    assert!(reader.page_allocation_bound(1, &bytes[..bytes.len() - 1]).is_err());
    let decoded = reader.read_page(1, bytes).unwrap();
    drop(reader);
    assert_eq!(decoded, page);
}

#[test]
fn native_page_arrays_keep_the_fetched_bytes_without_copying_them() {
    use rtdb_format::ipc::{ArrowFileFooter, ArrowFilePages, file_footer_range};

    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let page = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vec![100, 200]))]).unwrap();
    let bytes = write_file(&schema, std::slice::from_ref(&page)).unwrap();
    let trailer = bytes[bytes.len() - 10..].try_into().unwrap();
    let footer_range = file_footer_range(bytes.len() as u64, trailer).unwrap();
    let footer = ArrowFileFooter::parse(&bytes[footer_range.start as usize..footer_range.end as usize], footer_range.start).unwrap();
    let reader = ArrowFilePages::new(&footer).unwrap();
    let range = reader.page_range(0).unwrap();
    let range = bytes[range.start as usize..range.end as usize].to_vec();
    let address = range.as_ptr_range();
    let decoded = reader.read_page(0, range).unwrap();
    let column = decoded.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(address.contains(&column.values().as_ptr().cast()));
    assert_eq!(decoded, page);
}
