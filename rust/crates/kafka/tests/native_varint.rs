use rdkafka as _;

#[test]
fn native_slice_varints_preserve_cursor_and_output_on_overflow_or_underflow() {
    unsafe extern "C" {
        fn unittest_rdbuf() -> std::ffi::c_int;
    }
    assert_eq!(unsafe { unittest_rdbuf() }, 0);
}
