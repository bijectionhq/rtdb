use std::collections::BTreeSet;

use arrow_buffer::Buffer;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{
    data::{EventBatch, StoredEvent, StoredOperation, build_event_batch_with_recency, copy_encoded_batch_with_recency},
    entity_history::{HistoryRoot, HistoryWriter, ranges},
};
use rtdb_types::{BlockIndex, EventOrigin, Iid, Interval, Timestamp};

#[test]
fn history_ranges_keep_mixed_event_identities_and_reject_unbound_bytes() {
    let stable = EventOrigin::at([3; 16], 11, BlockIndex(8));
    let events = [None, Some(stable)].into_iter().enumerate().map(|(row, origin)| StoredEvent { position: None,
        iid: Iid([row as u8; 16]), system_from: Timestamp(40), valid_time: Interval::ALL,
        operation: StoredOperation::Delete, recency: None, entity_key: None, commit_position: 0, origin,
    }).collect::<Vec<_>>();
    let page = EventBatch::new(build_event_batch_with_recency(&events, None, None, false).unwrap()).unwrap();
    let rewritten = EventBatch::new(copy_encoded_batch_with_recency(&page.page_events().unwrap(), None, None, false).unwrap()).unwrap();
    assert_eq!(rewritten.origin(0).unwrap(), None);
    assert_eq!(rewritten.origin(1).unwrap(), Some(stable));
    assert_eq!(stable.source_block(), BlockIndex(8));

    let owner = [42; 32];
    let mut writer = HistoryWriter::new(Vec::new(), owner, &MemoryBudget::new(1 << 20)).unwrap();
    writer.append(0, &rewritten).unwrap();
    let (bytes, root) = writer.finish().unwrap();
    let root = HistoryRoot::decode(&root.encode(), bytes.len() as i64).unwrap();
    let directory = &bytes[root.range().start as usize..];
    let keys = BTreeSet::from([Iid([1; 16])]);
    assert!(ranges(directory, root, [43; 32], Some(&keys)).is_err());
    let selected = ranges(directory, root, owner, Some(&keys)).unwrap();
    assert_eq!(selected.len(), 1);
    let range = selected[0];
    let raw = &bytes[range.range().start as usize..range.range().end as usize];
    let table = range.read(Buffer::from(raw)).unwrap();
    assert_eq!(table.rows(&keys).collect::<Vec<_>>(), [1]);
    assert_eq!(table.event(1).unwrap(), (events[1].header(), Some(stable)));
    let mut corrupt = raw.to_vec();
    corrupt[0] ^= 1;
    assert!(range.read(Buffer::from(corrupt)).is_err());
}

#[test]
fn forward_witness_requires_the_same_logical_event() {
    let origin = EventOrigin::at([3; 16], 11, BlockIndex(8));
    let iid = Iid([1; 16]);
    let event = StoredEvent { position: None,
        iid, system_from: Timestamp(40), valid_time: Interval::ALL, operation: StoredOperation::Delete,
        recency: None, entity_key: None, commit_position: 0, origin: Some(origin),
    };
    let table = EventBatch::new(build_event_batch_with_recency(&[event], None, None, false).unwrap()).unwrap();
    let mut writer = HistoryWriter::new(Vec::new(), [42; 32], &MemoryBudget::new(1 << 20)).unwrap();
    writer.append_forward(0, &table, &["body".into()], |_| Ok(Some([25; 32]))).unwrap();
    let (bytes, root) = writer.finish().unwrap();
    let range = ranges(&bytes[root.range().start as usize..], root, [42; 32], None).unwrap()[0];
    let witness = range.read_forward(Buffer::from(&bytes[..range.range().end as usize]), 1).unwrap();
    assert_eq!(witness.revision(0, iid, origin, 0).unwrap(), Some([25; 32]));
    assert!(witness.revision(0, iid, EventOrigin::at([3; 16], 12, BlockIndex(8)), 0).is_err());
}
