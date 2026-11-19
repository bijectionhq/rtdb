use std::io::Write;

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
