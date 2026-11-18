use rdkafka as _;

unsafe extern "C" {
    fn unittest_metadata_cache_ownership() -> std::ffi::c_int;
}

#[test]
fn native_cache_owns_retained_pointees_across_full_and_merged_metadata() {
    assert_eq!(unsafe { unittest_metadata_cache_ownership() }, 0);
}

unsafe extern "C" {
    fn unittest_metadata_cache_topic_borrow() -> std::ffi::c_int;
}

#[test]
fn native_lightweight_topic_creation_observes_client_lock_and_inherits_cached_metadata() {
    assert_eq!(unsafe { unittest_metadata_cache_topic_borrow() }, 0);
}

unsafe extern "C" {
    fn unittest_metadata_tag_update_lock() -> std::ffi::c_int;
}

#[test]
fn native_metadata_tag_update_protects_cache_borrows_and_notifies_after_changes() {
    assert_eq!(unsafe { unittest_metadata_tag_update_lock() }, 0);
}
