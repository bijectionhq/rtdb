use rdkafka as _;

unsafe extern "C" {
    fn unittest_cgrp_metadata_hint_lock() -> std::ffi::c_int;
}

#[test]
fn native_consumer_group_metadata_fallback_holds_the_client_lock() {
    assert_eq!(unsafe { unittest_cgrp_metadata_hint_lock() }, 0);
}
