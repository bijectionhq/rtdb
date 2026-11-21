use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, StringArray};
use arrow_schema::Field;
use rtdb_format::bloom::BloomFilter;

#[test]
fn byte_cells_entered_in_bulk_set_the_bits_each_cell_sets_alone() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("a"), None, Some("sku-1"), Some(""), Some("a")])),
        Arc::new(BinaryArray::from(vec![Some(&b"\x00\x01"[..]), None, Some(&b"xyz"[..])])),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(vec![Some([1u8; 16]), None, Some([7u8; 16])].into_iter(), 16).unwrap()),
    ];
    for array in arrays {
        let field = Field::new("v", array.data_type().clone(), true);
        let rows = (0..array.len()).filter(|row| *row != 2).collect::<Vec<_>>();
        let mut single = BloomFilter::default();
        for &row in &rows { single.insert_cell(&field, array.as_ref(), row).unwrap(); }
        let mut bulk = BloomFilter::default();
        assert!(bulk.insert_byte_cells(array.as_ref(), rows.iter().copied()));
        assert_eq!(bulk.to_bytes().unwrap(), single.to_bytes().unwrap(), "{:?}", array.data_type());
    }
}
