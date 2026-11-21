#![allow(unexpected_cfgs)]
#![cfg(kani)]

use rtdb_types::{ReplicaPosition, SourcePosition, StorageEpoch};

fn nonnegative_position() -> (StorageEpoch, i64) {
    let epoch: u16 = kani::any();
    let offset: u64 = kani::any();
    kani::assume(epoch <= i16::MAX as u16);
    kani::assume(offset < (1u64 << 48));
    (StorageEpoch(i32::from(epoch)), offset as i64)
}

#[kani::proof]
fn position_packing_preserves_order_and_resumption_in_the_nonnegative_domain() {
    let (epoch, offset) = nonnegative_position();
    let (other_epoch, other_offset) = nonnegative_position();
    let source = SourcePosition::from_epoch_offset(epoch, offset).unwrap();
    let replica = ReplicaPosition::from_epoch_offset(epoch, offset).unwrap();
    let other_source = SourcePosition::from_epoch_offset(other_epoch, other_offset).unwrap();
    let other_replica = ReplicaPosition::from_epoch_offset(other_epoch, other_offset).unwrap();
    assert_eq!((source.epoch(), source.offset()), (epoch, offset));
    assert_eq!((replica.epoch(), replica.offset()), (epoch, offset));
    assert_eq!(replica.0, source.0);
    let order = (epoch, offset).cmp(&(other_epoch, other_offset));
    assert_eq!(source.cmp(&other_source), order);
    assert_eq!(replica.cmp(&other_replica), order);
    let expected = if epoch == other_epoch { offset } else { -1 };
    assert_eq!(source.after_offset(other_epoch), expected);
    assert_eq!(replica.after_offset(other_epoch), expected);
}

#[kani::proof]
fn negative_message_ids_and_start_sentinels_resume_from_the_start() {
    let raw: i64 = kani::any();
    let requested = StorageEpoch(kani::any());
    kani::assume(raw < 0);
    assert_eq!(SourcePosition(raw).after_offset(requested), -1);
    assert_eq!(ReplicaPosition(raw).after_offset(requested), -1);

    let (epoch, _) = nonnegative_position();
    let source = SourcePosition::from_epoch_offset(epoch, -1).unwrap();
    let replica = ReplicaPosition::from_epoch_offset(epoch, -1).unwrap();
    assert_eq!(source.after_offset(epoch), -1);
    assert_eq!(replica.after_offset(epoch), -1);
}
