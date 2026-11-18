use rdkafka as _;

unsafe extern "C" {
    fn unittest_metadata_cache_ownership() -> std::ffi::c_int;
}

#[test]
fn native_cache_owns_retained_pointees_across_full_and_merged_metadata() {
    assert_eq!(unsafe { unittest_metadata_cache_ownership() }, 0);
}
