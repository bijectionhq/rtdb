use std::{env, fs, io::Read, path::PathBuf};
use sha2::{Digest, Sha256};

#[path = "build_support/locale.rs"]
mod locale;

fn main() {
    println!("cargo:rerun-if-changed=build_support/locale.rs");
    println!("cargo:rerun-if-changed=build_support/locale_class.rs");
    println!("cargo:rerun-if-changed=data/locale/temurin-25.0.1+8.tar");
    let path = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory"))
        .join("data/locale/temurin-25.0.1+8.tar");
    let mut bytes = Vec::new();
    fs::File::open(path).expect("locale bundle file").take(locale::MAX_ARCHIVE_BYTES as u64 + 1).read_to_end(&mut bytes).expect("locale bundle bytes");
    assert!(bytes.len() <= locale::MAX_ARCHIVE_BYTES, "locale bundle exceeds 32 MiB");
    assert_eq!(format!("{:x}", Sha256::digest(&bytes)), locale::ARCHIVE_SHA256, "locale bundle provenance mismatch");
    let source = locale::source(&bytes).expect("bounded locale resource decoding");
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo build output directory")).join("legacy_locale_data.rs");
    fs::write(output, source).expect("generated locale data");
}
