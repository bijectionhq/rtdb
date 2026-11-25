fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = "../../proto";
    let schemas = [
        "../../proto/rtdb.v1.proto",
        "../../proto/rtdb.admin.v1.proto",
        "../../proto/rtdb.store.v1.proto",
    ];
    for schema in schemas {
        println!("cargo:rerun-if-changed={schema}");
    }
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.enable_type_names();
    config.type_name_domain(["."], "type.googleapis.com");
    config.compile_protos(
        &schemas,
        &[
            std::path::PathBuf::from(root),
            protoc_bin_vendored::include_path()?,
        ],
    )?;
    Ok(())
}
