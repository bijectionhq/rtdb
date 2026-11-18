use rdkafka as _;

unsafe extern "C" {
    fn unittest_topic_info_admission() -> std::ffi::c_int;
}

#[test]
fn native_topic_info_admits_independent_copies_and_discards_incomplete_group_projections() {
    assert_eq!(unsafe { unittest_topic_info_admission() }, 0);
}
