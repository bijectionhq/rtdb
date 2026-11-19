use std::io::{Read, Seek, Write};

use super::JavaLayout;

/// The reference's source-log tx-ops stream for `indexer-test`'s first transaction, as its Java writer produced it.
const REFERENCE_TX_OPS: &[u8] = include_bytes!("java_layout_tx.ipc");

#[test]
fn rewriting_the_reference_tx_ops_stream_reproduces_its_bytes() {
    for chunk in [1, 7, REFERENCE_TX_OPS.len()] {
        let mut writer = JavaLayout::new(Vec::new());
        for piece in REFERENCE_TX_OPS.chunks(chunk) { writer.write_all(piece).unwrap(); }
        assert_eq!(writer.inner.len(), REFERENCE_TX_OPS.len(), "chunk {chunk}");
        assert!(writer.inner == REFERENCE_TX_OPS, "chunk {chunk}: first difference at {:?}", writer.inner.iter().zip(REFERENCE_TX_OPS).position(|(a, b)| a != b));
    }
}

/// L0 files the reference wrote for `indexer-test/can-handle-dynamic-cols-in-same-block`:
/// unions with empty legs, an empty list's offsets, nested structs, transit and HLL columns.
const REFERENCE_FILES: [(&str, &[u8]); 3] = [
    ("xt$txs data", include_bytes!("java_layout_txs_data.arrow")),
    ("xt$txs meta", include_bytes!("java_layout_txs_meta.arrow")),
    ("public$xt_docs meta", include_bytes!("java_layout_docs_meta.arrow")),
];

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
    }
}
