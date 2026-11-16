#[test]
fn compiled_build_info_needs_no_configuration_and_ignores_runtime_version_overrides() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rtdb-config")).arg("--build-info").env("XTDB_VERSION", "runtime-version").env("GIT_SHA", "runtime-revision").output().unwrap();
    assert!(output.status.success());
    let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual, serde_json::to_value(rtdb_types::compiled_build_info()).unwrap());
    let rejected = std::process::Command::new(env!("CARGO_BIN_EXE_rtdb-config")).args(["--build-info", "extra"]).output().unwrap();
    assert!(!rejected.status.success());
}
