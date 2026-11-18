use rdkafka as _;

unsafe extern "C" {
    fn unittest_metadata_cache_admission() -> std::ffi::c_int;
}

#[test]
fn native_cache_arenas_admit_overlap_preserve_denied_entries_and_release_after_response_errors() {
    assert_eq!(unsafe { unittest_metadata_cache_admission() }, 0);
}
