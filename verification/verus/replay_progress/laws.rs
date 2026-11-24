use crate::replay_progress::{AppliedUpdate, Regression, applied_update, replayed_source};
use vstd::prelude::*;

verus! {

pub(super) fn replay_preserves_original_watermark_and_receipt<T>(
    previous_source: i64,
    previous_tx: i64,
    recorded_source: i64,
    transaction: i64,
    external_source: bool,
    receipt: T,
) -> (update: Result<AppliedUpdate<T>, Regression>)
    ensures
        update is Ok <==> previous_source <= recorded_source && previous_tx < transaction,
        update matches Ok(applied) ==> applied.source == recorded_source
            && applied.completed == Some((transaction, receipt)),
{
    let source = replayed_source(Some(recorded_source), external_source, previous_source, transaction);
    applied_update(previous_source, previous_tx, Some(source), Some((transaction, receipt)))
}

}
