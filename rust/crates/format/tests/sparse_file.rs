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
