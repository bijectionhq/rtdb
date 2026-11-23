use std::path::PathBuf;

use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::DescriptorProto;

fn normalize_edition_message(message: &mut DescriptorProto) {
    for field in &mut message.field {
        if field.label() == Label::Repeated
            && !matches!(field.r#type(), Type::String | Type::Bytes | Type::Message | Type::Group)
        {
            field.options.get_or_insert_with(Default::default).packed = Some(true);
        }
    }
    for nested in &mut message.nested_type {
        normalize_edition_message(nested);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("../../..");
    let core = root.join("core/src/main/proto");
    let api = root.join("api/src/main/proto");
    let files = [
        core.join("xtdb/database/proto/db.proto"),
        core.join("xtdb/log/proto/log.proto"),
        core.join("xtdb/log/proto/replica-log.proto"),
        core.join("xtdb/meta/block/proto/block.proto"),
        core.join("xtdb/cols/proto/cols.proto"),
        core.join("xtdb/trie.proto"),
        api.join("xtdb/proto/basis.proto"),
        root.join("modules/aws/src/main/proto/s3.proto"),
        root.join("modules/azure/src/main/proto/azure.proto"),
        root.join("modules/google-cloud/src/main/proto/gcs.proto"),
        root.join("modules/kafka/src/main/proto/kafka.proto"),
        root.join("modules/kafka/src/main/proto/kafka_connect_source.proto"),
        root.join("modules/postgres-source/src/main/proto/postgres_source.proto"),
    ];
    for path in &files {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.btree_map(["."]);
    config.bytes([
        ".xtdb.log.proto.Tx.tx_ops", ".xtdb.log.proto.Tx.user_metadata",
        ".xtdb.log.proto.Tx.external_source_token", ".xtdb.log.proto.Tx.placed_attempt_seed",
        // Prost resolves a map's synthetic value field under the containing message.
        ".xtdb.log.proto.ResolvedTx.value", ".xtdb.log.proto.ResolvedTx.error",
        ".xtdb.log.proto.ResolvedTx.placed_attempt", ".xtdb.log.proto.ResolvedTx.search_delta",
    ]);
    config.include_file("xtdb.rs");
    for field in ["Listy.el", "FixedSizeList.el", "MapType.el", "Maybe.mono", "ColumnMeta.type",
        "VecType.type.decimal", "VecType.type.timestamp", "VecType.type.date", "VecType.type.time",
        "VecType.type.interval", "VecType.type.list", "VecType.type.set", "VecType.type.tstz_range",
        "VecType.type.fixed_size_list", "VecType.type.map", "VecType.type.struct", "VecType.type.maybe", "VecType.type.poly"] {
        config.boxed(format!(".xtdb.cols.proto.{field}"));
    }
    let includes = [core, api, root, protoc_bin_vendored::include_path()?];
    let mut descriptors = config.load_fds(&files, &includes)?;
    for file in &mut descriptors.file {
        if file.syntax.as_deref() == Some("editions") {
            file.syntax = Some("proto2".into());
            for message in &mut file.message_type {
                normalize_edition_message(message);
            }
        }
    }
    config.compile_fds(descriptors)?;
    Ok(())
}
