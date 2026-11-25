use std::io::{self, Read, Seek, Write};

use super::JavaLayout;

#[derive(Default)]
struct ShortWrites(Vec<u8>);

impl Write for ShortWrites {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(&bytes[..bytes.len().min(13)])
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

/// The reference's source-log tx-ops stream for `indexer-test`'s first transaction, as its Java writer produced it.
const REFERENCE_TX_OPS: &[u8] = include_bytes!("java_layout_tx.ipc");

#[test]
fn rewriting_the_reference_tx_ops_stream_reproduces_its_bytes() {
    for chunk in [1, 7, REFERENCE_TX_OPS.len()] {
        let mut writer = JavaLayout::new_stream(ShortWrites::default());
        for piece in REFERENCE_TX_OPS.chunks(chunk) { writer.write_all(piece).unwrap(); }
        assert!(writer.footer.is_none());
        let written = writer.inner.0;
        assert_eq!(written.len(), REFERENCE_TX_OPS.len(), "chunk {chunk}");
        assert!(written == REFERENCE_TX_OPS, "chunk {chunk}: first difference at {:?}", written.iter().zip(REFERENCE_TX_OPS).position(|(a, b)| a != b));
    }
}

/// L0 files the reference wrote for `indexer-test/can-handle-dynamic-cols-in-same-block`:
/// unions with empty legs, an empty list's offsets, nested structs, transit and HLL columns, and the data file whose
/// null `struct` rows leave their dense-union child `a` undefined: type id -1, offset 0, no child slot.
const REFERENCE_FILES: [(&str, &[u8]); 4] = [
    ("xt$txs data", include_bytes!("java_layout_txs_data.arrow")),
    ("xt$txs meta", include_bytes!("java_layout_txs_meta.arrow")),
    ("public$xt_docs meta", include_bytes!("java_layout_docs_meta.arrow")),
    ("public$xt_docs data", include_bytes!("java_layout_docs_data.arrow")),
];

#[test]
fn a_reference_undefined_union_cell_reads_as_its_absent_field() {
    let bytes: &[u8] = include_bytes!("java_layout_docs_data.arrow");
    let mut file = crate::ipc::ArrowFile::open(bytes.into(), None).unwrap();
    let page = file.read_page(0).unwrap();
    let schema = page.schema();
    let index = schema.index_of("op").unwrap();
    let values = (0..page.num_rows()).map(|row| rtdb_columnar::read_value(schema.field(index), page.column(index).as_ref(), row).unwrap()).collect::<Vec<_>>();
    let structs = values.iter().filter_map(|value| value.field("struct")).collect::<Vec<_>>();
    assert_eq!(structs.iter().filter(|value| matches!(value, rtdb_types::Value::Null)).count(), 4, "{values:?}");
    let present = structs.iter().filter_map(|value| value.field("a")).collect::<Vec<_>>();
    assert_eq!(present, [&rtdb_types::Value::I64(1), &rtdb_types::Value::Bool(true)]);
}

#[test]
fn rewriting_reference_files_reproduces_their_bytes() {
    for (name, bytes) in REFERENCE_FILES {
        let mut file = crate::ipc::ArrowFile::open(bytes.into(), None).unwrap();
        let pages = (0..file.page_count()).map(|page| file.read_page(page)).collect::<rtdb_types::Result<Vec<_>>>().unwrap();
        let written = crate::ipc::write_file(file.schema().as_ref(), &pages).unwrap();
        assert!(written == bytes, "{name}: {} bytes against {}, first difference at {:?}", written.len(), bytes.len(), written.iter().zip(bytes).position(|(a, b)| a != b));
        let mut sink = crate::ipc::ArrowFileWriter::new(tempfile::tempfile().unwrap(), file.schema().as_ref()).unwrap();
        for page in &pages { sink.write(page).unwrap(); }
        let mut output = sink.finish().unwrap();
        let mut copied = Vec::new();
        output.rewind().unwrap();
        output.read_to_end(&mut copied).unwrap();
        assert!(copied == bytes, "{name}: the incremental writer's file differs");

        let mut sink = crate::ipc::JavaFile::new(ShortWrites::default(), file.schema().as_ref()).unwrap();
        for page in &pages { sink.write(page).unwrap(); }
        let written = sink.finish().unwrap().0;
        assert!(written == bytes, "{name}: the short-writing sink's file differs");
    }
}
