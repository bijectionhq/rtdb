#[allow(dead_code)]
#[path = "src/tzdb.rs"]
mod tzdb;

use sha2::{Digest, Sha256};
use std::{env, fs::File, io::Read, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=RTDB_TZDB_PATH");
    println!("cargo:rerun-if-changed=src/tzdb.rs");
    println!("cargo:rerun-if-changed=src/tzdb/legacy.rs");
    let path = env::var_os("RTDB_TZDB_PATH").map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory")).join("data/tzdb/2026e.dat"));
    println!("cargo:rerun-if-changed={}", path.display());
    let mut bytes = Vec::new();
    File::open(&path).expect("Open selected OpenJDK TZDB data").take(tzdb::MAX_BYTES as u64 + 1).read_to_end(&mut bytes).expect("Read selected OpenJDK TZDB data");
    let database = tzdb::decode(&bytes).expect("Validate selected OpenJDK TZDB data");
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo output directory"));
    std::fs::write(out.join("tzdb.dat"), &bytes).expect("Write bundled timezone data");
    std::fs::write(out.join("tzdb.rs"), format!("pub const TZDB_VERSION: &str = {:?};\npub const TZDB_SHA256: &str = {digest:?};\n", database.version)).expect("Write bundled timezone identity");
}
