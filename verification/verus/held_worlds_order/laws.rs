use crate::control_order::{SourceOrder, fingerprint_bytes, fingerprint_matches, hosted_check_for_refusal, revision_matches, source_order, staging_clock_upper};
use crate::control_order::{HostedStamp, LandingMarker, hosted_stamps_current, landing_marker_matches};
use crate::event_revision::matches_transaction;
use crate::prepared_landing;
use crate::system_time::{advances, owned_deadline_is_live};
use vstd::prelude::*;

verus! {

pub fn kind_six_requires_the_prepared_action_and_clock_slot(prepared_action: bool, slot: bool) {
    let admitted = prepared_landing::admits(true, prepared_action, slot);
    assert(admitted == (prepared_action && slot));
}

pub fn ordered_writer_stamp_is_exact(predecessor: Option<i64>, submitted: i64) {
    let admitted = revision_matches(predecessor, Some(submitted));
    assert(admitted == (predecessor == Some(submitted)));
}

pub(crate) fn ordered_hosted_inventory_is_current(stamps: &[HostedStamp], ordered_at: i64, until: i64)
    -> (admitted: bool)
    ensures admitted == (ordered_at < until && forall|i: int| 0 <= i < stamps@.len() ==>
        (#[trigger] stamps@[i]).observed == stamps@[i].expected),
{
    owned_deadline_is_live(ordered_at, until) && hosted_stamps_current(stamps)
}

pub(crate) fn chosen_landing_marker_has_original_identity(
    marker: LandingMarker<'_>, context: &str, submitted: &[u8; 32], source: i64, revision: i64,
) -> (admitted: bool)
    ensures admitted == (marker.context@ == context@ && marker.fingerprint@ == submitted@
        && marker.source == Some(source) && marker.revision == revision),
{
    landing_marker_matches(marker, context, submitted, source, revision)
}

pub fn hosted_staging_cannot_repair_a_stale_writer(
    predecessor: Option<i64>, submitted: i64, sample: i64, until: i64,
) {
    let admitted = revision_matches(predecessor, Some(submitted));
    let upper = staging_clock_upper(sample, until);
    if admitted {
        if let Some(staged) = upper {
            assert(predecessor == Some(submitted) && sample < staged < until);
        }
    }
}

pub fn refusal_rechecks_original_writer(predecessor: Option<i64>, submitted: i64) {
    let original = Some(submitted);
    let retained = hosted_check_for_refusal(&original, true);
    assert(retained.is_some());
    if let Some(expected) = retained {
        let admitted = revision_matches(predecessor, *expected);
        assert(admitted == (predecessor == Some(submitted)));
    }
}

pub fn staged_live_marker_has_submitted_fingerprint_and_ordered_revision(
    submitted: &[u8; 32], system_from: i64, accepted_revision: i64,
) {
    let payload = fingerprint_bytes(submitted);
    let staged = matches_transaction(system_from, accepted_revision);
    if staged {
        assert(payload@ == submitted@ && system_from == accepted_revision);
    }
}

pub fn refusal_marker_preserves_submitted_fingerprint(candidate: &[u8], submitted: &[u8; 32]) {
    let admitted = fingerprint_matches(candidate, submitted);
    if admitted {
        assert(candidate@ == submitted@);
    }
}

pub fn placed_publication_advances_reopened_claim(
    claim_position: i64, claim_revision: i64,
    predecessor_position: i64, predecessor_revision: i64,
    source: i64, transaction: i64, revision: i64, observed: Option<i64>,
)
    requires claim_position <= predecessor_position, claim_revision <= predecessor_revision,
{
    let order = source_order(true, source, Some(predecessor_position), transaction);
    let advancing_time = advances(Some(predecessor_revision), revision);
    let writer = revision_matches(observed, Some(claim_revision));
    if advancing_time && writer && matches!(order, SourceOrder::Admitted) {
        assert(observed == Some(claim_revision));
        assert(source == transaction && claim_position < source && claim_revision < revision);
    }
}

}
