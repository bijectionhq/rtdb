//! The pure placement rules of `rtdb-multi.allium` (`Claim`, `Park`, `effective_holder`).
//!
//! Each function computes the record a caller then stores with a conditional replace; the directory's version check is what makes two concurrent claims resolve to one.

use std::collections::BTreeMap;

use crate::{DeploymentRecord, Lifecycle, MemberId, MemberRecord};

/// The holder that counts: the recorded holder, only while its member may hold.
pub fn effective_holder<'a>(record: &'a DeploymentRecord, members: &BTreeMap<MemberId, MemberRecord>, now_ms: i64) -> Option<&'a MemberId> {
    record.holder.as_ref().filter(|holder| members.get(*holder).is_some_and(|member| member.may_hold(now_ms)))
}

/// The record that makes `claimant` the holder under the next epoch, or `None` if the deployment may not be claimed by it now.
/// A live holder other than the claimant blocks the claim; the claimant's own stale holding does not, so a restarted member's successor process can reclaim.
pub fn claim(record: &DeploymentRecord, claimant: &MemberId, members: &BTreeMap<MemberId, MemberRecord>, now_ms: i64) -> Option<DeploymentRecord> {
    if record.lifecycle != Lifecycle::Live { return None; }
    if !members.get(claimant).is_some_and(|member| member.may_receive_placements(now_ms)) { return None; }
    if effective_holder(record, members, now_ms).is_some_and(|holder| holder != claimant) { return None; }
    let mut next = record.clone();
    next.epoch += 1;
    next.holder = Some(claimant.clone());
    Some(next)
}

/// The record that releases the deployment from `holder` at `epoch`: no holder, and the epoch bumped so the released holder's writes are fenced.
pub fn park(record: &DeploymentRecord, holder: &MemberId, epoch: u64) -> Option<DeploymentRecord> {
    if record.holder.as_ref() != Some(holder) || record.epoch != epoch { return None; }
    let mut next = record.clone();
    next.epoch += 1;
    next.holder = None;
    Some(next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemberRole, MemberState};

    fn node(id: &str, state: MemberState, lease: i64) -> MemberRecord {
        MemberRecord { id: MemberId(id.into()), role: MemberRole::Node, address: String::new(), control_address: None, availability_zone: "a".into(),
            engine_version: "0".into(), lease_expires_at_ms: lease, state, version: 1 }
    }

    fn members(list: &[MemberRecord]) -> BTreeMap<MemberId, MemberRecord> { list.iter().map(|member| (member.id.clone(), member.clone())).collect() }

    #[test]
    fn a_live_holder_blocks_another_claimant_and_a_dead_one_does_not() {
        let members = members(&[node("a", MemberState::Eligible, 100), node("b", MemberState::Eligible, 100), node("c", MemberState::Dead, 100)]);
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let held = claim(&record, &MemberId("a".into()), &members, 10).unwrap();
        assert_eq!((held.epoch, held.holder.clone()), (1, Some(MemberId("a".into()))));
        assert!(claim(&held, &MemberId("b".into()), &members, 10).is_none(), "a live holder blocks");
        assert!(claim(&held, &MemberId("b".into()), &members, 200).is_none(), "an expired claimant may not claim");
        record.holder = Some(MemberId("c".into()));
        record.epoch = 7;
        let taken = claim(&record, &MemberId("b".into()), &members, 10).unwrap();
        assert_eq!(taken.epoch, 8, "taking over from a dead holder bumps the epoch");
    }

    #[test]
    fn parking_fences_the_released_holder() {
        let members = members(&[node("a", MemberState::Eligible, 100)]);
        let held = claim(&DeploymentRecord::new("shop", "acme", vec![], 0), &MemberId("a".into()), &members, 10).unwrap();
        assert!(park(&held, &MemberId("a".into()), 0).is_none(), "only the current epoch parks");
        let parked = park(&held, &MemberId("a".into()), held.epoch).unwrap();
        assert_eq!((parked.epoch, parked.holder), (held.epoch + 1, None));
    }
}
