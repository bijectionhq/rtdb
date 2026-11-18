#[path = "build_support/tzdb.rs"]
mod tzdb;

use sha2::{Digest, Sha256};
use std::{env, fs::File, io::Read, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=RTDB_TZDB_PATH");
    println!("cargo:rerun-if-changed=build_support/tzdb.rs");
    println!("cargo:rerun-if-changed=build_support/legacy_tzdb.rs");
    let path = env::var_os("RTDB_TZDB_PATH").map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory")).join("data/tzdb/2026e.dat"));
    println!("cargo:rerun-if-changed={}", path.display());
    let mut bytes = Vec::new();
    File::open(&path).expect("Open selected OpenJDK TZDB data").take(tzdb::MAX_BYTES as u64 + 1).read_to_end(&mut bytes).expect("Read selected OpenJDK TZDB data");
    let database = tzdb::decode(&bytes).expect("Validate selected OpenJDK TZDB data");
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let destination = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo output directory")).join("tzdb.rs");
    std::fs::write(destination, database.rust_source(&digest)).expect("Write native timezone tables");
}
