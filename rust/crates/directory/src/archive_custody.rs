#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(held =>
    ensures held <==> (destination.is_some() && destination.unwrap() == capture)
                   || (repair.is_some() && repair.unwrap() == capture),
))]
pub(crate) fn covers_archive(destination: Option<u128>, repair: Option<u128>, capture: u128) -> bool {
    matches!(destination, Some(id) if id == capture) || matches!(repair, Some(id) if id == capture)
}

#[cfg_attr(verus_keep_ghost, verus_spec(admitted =>
    ensures admitted <==> covered && !closed,
))]
pub(crate) fn may_read_archive(covered: bool, closed: bool) -> bool {
    covered && !closed
}

#[cfg(test)]
mod tests {
    use crate::StorageRegion;
    use uuid::Uuid;

    #[test]
    fn source_repair_and_capture_destination_have_independent_archive_custody() {
        let destination = Uuid::new_v4();
        let repair = Uuid::new_v4();
        let unrelated = Uuid::new_v4();
        let local = StorageRegion::Local {
            archive: Some(destination), checkpoint_suffix: None, repair_archive: Some(repair),
            tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None,
        };
        let wire = serde_json::to_vec(&local).unwrap();
        let local = serde_json::from_slice::<StorageRegion>(&wire).unwrap();
        let remote = StorageRegion::Remote {
            archive_source: None,
            archive: Some(destination), checkpoint_suffix: None, repair_archive: Some(repair), owner: None, closed: false,
            tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None,
            publications: Default::default(),
        };
        for region in [&local, &remote] {
            assert!(region.holds_archive(destination) && region.holds_archive(repair),
                "the repair source must be kept independently of the capture destination");
            assert!(region.admits_archive_read(repair));
            assert!(!region.holds_archive(unrelated) && !region.admits_archive_read(unrelated));
        }
        let mut remote = remote;
        if let StorageRegion::Remote { closed, .. } = &mut remote { *closed = true; }
        assert!(remote.holds_archive(destination) && remote.holds_archive(repair));
        assert!(!remote.admits_archive_read(repair), "a closed repair region cannot issue provider reads");

        let legacy = serde_json::json!({"state": "local", "archive": destination});
        let legacy = serde_json::from_value::<StorageRegion>(legacy).unwrap();
        assert!(legacy.admits_archive_read(destination));
        assert!(!legacy.holds_archive(repair));
    }
}
