#![allow(unexpected_cfgs)]
#![cfg(all(kani, feature = "simulation"))]

use bytes::Bytes;
use rtdb_logkeeper::simulation::{Event, ResponseBody, Sealed, StreamState, winner};

#[kani::proof]
fn a_commit_requires_the_live_promised_and_synced_epoch() {
    let mut state = StreamState::<Bytes>::default();
    state.promised = kani::any();
    state.synced = if kani::any() { Some(kani::any()) } else { None };
    state.dropped = kani::any();
    let writer: u64 = kani::any();

    let should_accept = !state.dropped && state.promised == writer && state.synced == Some(writer);
    let commit = state.commit(writer, -1);
    assert_eq!(matches!(commit.reply, ResponseBody::Accepted(_)), should_accept);
    assert!(commit.events.is_empty());
}

#[kani::proof]
fn replaying_a_promise_preserves_the_greatest_epoch() {
    let mut state = StreamState::<Bytes>::default();
    let prior: u64 = kani::any();
    let promised: u64 = kani::any();
    let synced = if kani::any() { Some(kani::any()) } else { None };
    state.promised = prior;
    state.synced = synced;
    state.apply(&Event::Promise(promised));
    assert_eq!(state.promised, prior.max(promised));
    assert_eq!(state.synced, synced);
}

#[kani::proof]
fn replaying_retirement_closes_commit_admission_at_every_epoch() {
    let mut state = StreamState::<Bytes>::default();
    let prior: u64 = kani::any();
    let deletion_epoch: u64 = kani::any();
    let writer_epoch: u64 = kani::any();
    state.promised = prior;
    state.synced = Some(writer_epoch);
    state.apply(&Event::Drop(deletion_epoch));
    assert!(state.dropped);
    assert_eq!(state.promised, prior.max(deletion_epoch));
    assert_eq!(state.synced, None);

    let commit = state.commit(writer_epoch, -1);
    assert!(matches!(commit.reply, ResponseBody::Refused(_)));
    assert!(commit.events.is_empty());
}

#[kani::proof]
#[kani::unwind(6)]
fn up_to_three_replies_choose_the_last_greatest_synced_epoch_then_lsn() {
    let replies: [Sealed; 3] = std::array::from_fn(|_| Sealed {
        synced: kani::any(),
        last_lsn: kani::any(),
        ..Sealed::default()
    });
    let count: usize = kani::any();
    kani::assume((1..=replies.len()).contains(&count));
    let replies = &replies[..count];
    let chosen = winner(replies);
    let best = &replies[chosen];
    for (index, reply) in replies.iter().enumerate() {
        assert!(
            best.synced > reply.synced
                || (best.synced == reply.synced && best.last_lsn > reply.last_lsn)
                || (best.synced == reply.synced && best.last_lsn == reply.last_lsn && chosen >= index)
        );
    }
}
