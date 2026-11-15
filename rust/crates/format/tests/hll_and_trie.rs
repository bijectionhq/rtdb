use rtdb_format::{hll::{self, Hll}, memory_trie};
use rtdb_trie::{PersistedNode, PersistedTrie};

#[test]
fn hll_registers_use_big_endian_maxima_and_null_free_cardinality() {
    let mut left = Hll::default();
    assert_eq!(left.estimate(), 0.0);
    left.insert_hash(0);
    let mut right = Hll::default();
    right.insert_hash(1 << 22);
    let merged = left.combined(&right).unwrap();
    assert_eq!(&merged.as_bytes()[..8], &[0, 0, 0, 23, 0, 0, 0, 23]);
    assert!((merged.estimate() - 2.001955671862574).abs() < 1e-12);
    assert_eq!(hll::combine(left.as_bytes(), right.as_bytes()).unwrap(), hll::combine(right.as_bytes(), left.as_bytes()).unwrap());
    assert_eq!(merged.combined(&merged).unwrap(), merged);
    assert!(hll::combine(&[0; 4], &[0; 8]).is_err());
}

#[test]
fn memory_trie_codec_preserves_empty_buckets_and_descending_row_ties() {
    let trie = PersistedTrie {
        log_limit: 64, page_limit: 1024, root_path: Vec::new(),
        root: PersistedNode::Branch(vec![Some(PersistedNode::Leaf(vec![8, 3, 1])), None, Some(PersistedNode::Leaf(vec![2])), None]),
    };
    let bytes = memory_trie::encode(&trie).unwrap();
    let decoded = memory_trie::decode(&bytes).unwrap();
    assert_eq!(decoded.log_limit, 64);
    assert_eq!(decoded.page_limit, 1024);
    assert_eq!(decoded.root_path, Vec::<u8>::new());
    let PersistedNode::Branch(children) = decoded.root else { panic!("expected branch"); };
    assert!(children[1].is_none());
    assert!(children[3].is_none());
    let Some(PersistedNode::Leaf(rows)) = &children[0] else { panic!("expected leaf"); };
    assert_eq!(rows, &[8, 3, 1]);
}
