use super::winner;
use crate::protocol::Sealed;

#[kani::proof]
#[kani::unwind(5)]
fn three_reply_winner_is_the_latest_synced_then_longest_history() {
    let replies: [Sealed; 3] = std::array::from_fn(|_| Sealed {
        synced: kani::any(),
        last_lsn: kani::any(),
        ..Sealed::default()
    });
    let chosen = winner(&replies);
    let best = &replies[chosen];
    for (index, reply) in replies.iter().enumerate() {
        assert!(
            best.synced > reply.synced
                || (best.synced == reply.synced && best.last_lsn > reply.last_lsn)
                || (best.synced == reply.synced && best.last_lsn == reply.last_lsn && chosen >= index)
        );
    }
}
