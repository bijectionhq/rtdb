use crate::{production::*, provider_decision::*};
use vstd::prelude::*;

verus! {

spec fn MatchingObjectFence(fenced_name: Seq<u8>, name: Seq<u8>, fenced_version: Seq<u8>, version: Seq<u8>) -> bool {
    fenced_name == name && fenced_version == version
}

spec fn ReleaseIsCovered(through: int, trim_floor: int, selected_cut: int) -> bool {
    0 <= through < trim_floor <= selected_cut
}

spec fn ErasureRequiresCustodyJoin(erasure_floor: int) -> bool {
    erasure_floor != 0
}

spec fn ExactVersionRemoval(listed_as_current: Option<bool>, other_data: bool) -> bool {
    listed_as_current == Some(false) || (listed_as_current == Some(true) && !other_data)
}

spec fn TerminalTargetMatches(same_address: bool, same_condition: bool) -> bool {
    same_address && same_condition
}

spec fn RetainsServingSlot(same_origin: bool, same_attempt: bool) -> bool {
    same_origin && same_attempt
}

spec fn RetainsPublicationScope(deleted: bool, disposed: bool, regions_empty: bool) -> bool {
    !deleted || !disposed || !regions_empty
}

fn retirement_requires_the_exact_logkeeper_fence(
    fenced_name: [u8; 32], fenced_version: [u8; 32], name: [u8; 32], version: [u8; 32],
) {
    let admitted = matches_object_fence(fenced_name, fenced_version, name, version);
    assert(admitted == MatchingObjectFence(fenced_name@, name@, fenced_version@, version@));
}

fn release_is_strictly_below_the_trim_floor(through: i64, trim_floor: u64, selected_cut: u64) {
    let covered = release_is_covered(through, trim_floor, selected_cut);
    assert(covered == ReleaseIsCovered(through as int, trim_floor as int, selected_cut as int));
}

fn retirement_requires_custody_join_after_erasure(erasure_floor: u64) {
    let needs_join = erasure_requires_custody_join(erasure_floor);
    assert(needs_join == ErasureRequiresCustodyJoin(erasure_floor as int));
}

fn version_delete_requires_the_exact_listing(target_latest: Option<bool>, other_data: bool) {
    let status = exact_version_status(target_latest, other_data);
    assert((status is SafeToRemove) == ExactVersionRemoval(target_latest, other_data)) by {
        match target_latest {
            None => {},
            Some(latest) => if latest {} else {},
        }
    };
}

fn retirement_settles_from_exact_absence(target_latest: Option<bool>, other_data: bool) {
    let status = exact_version_status(target_latest, other_data);
    assert((status is Absent) == (target_latest is None));
}

fn terminal_replacement_requires_the_exact_condition(same_address: bool, same_condition: bool) {
    let admitted = matches_terminal_target(same_address, same_condition);
    assert(admitted == TerminalTargetMatches(same_address, same_condition));
}

fn an_exact_alias_retains_its_serving_slot(same_origin: bool, same_attempt: bool) {
    let retains = retains_serving_slot(same_origin, same_attempt);
    assert(retains == RetainsServingSlot(same_origin, same_attempt));
}

fn a_peer_with_custody_retains_the_publication_scope(deleted: bool, disposed: bool, regions_empty: bool) {
    let retains = retains_publication_scope(deleted, disposed, regions_empty);
    assert(retains == RetainsPublicationScope(deleted, disposed, regions_empty));
}

}
