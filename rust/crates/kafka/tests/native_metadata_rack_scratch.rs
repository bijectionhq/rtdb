use rdkafka as _;

unsafe extern "C" {
    fn unittest_metadata_rack_scratch() -> std::ffi::c_int;
}

#[test]
fn native_metadata_rack_scratch_admits_overlap_and_preserves_parser_failure_prefixes() {
    assert_eq!(unsafe { unittest_metadata_rack_scratch() }, 0);
}
