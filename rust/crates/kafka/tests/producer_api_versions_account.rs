extern crate rdkafka as _;

#[test]
fn api_version_ranges_keep_their_native_allowance_through_replacement() {
    assert_eq!(unsafe { unittest_producer_api_versions_account() }, 0);
}

unsafe extern "C" {
    fn unittest_producer_api_versions_account() -> std::ffi::c_int;
}
