//! `RANK_DIGEST`: SHA-256 over the ranking code actually compiled. It hashes,
//! in sorted order, every file under `src/` of this crate and of its sibling
//! crates `kernel_text` and `geometry` (each as `crate/relative path` and
//! contents, both length-prefixed; hidden files skipped), then `pins.txt`. Bijection keeps the
//! three crates side by side in `crates/`; RTDB vendors them side by side in
//! `rust/vendor/bijection/`, so the same files give the same digest there.
use std::{
    fs,
    path::{
        Path,
        PathBuf,
    },
};

use sha2::{
    Digest,
    Sha256,
};

fn files(root: &Path, at: &Path, out: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(at).unwrap_or_else(|error| panic!("{}: {error}", at.display())).map(|entry| entry.expect("a directory entry").path()).collect::<Vec<_>>();
    entries.sort();
    for path in entries {
        // Hidden files (editor and Finder droppings) are never sources.
        if path.file_name().is_some_and(|name| name.to_string_lossy().starts_with('.')) {
            continue;
        }
        if path.is_dir() {
            files(root, &path, out);
        } else {
            out.push(path.strip_prefix(root).expect("under the root").to_path_buf());
        }
    }
}

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets the manifest directory"));
    let siblings = manifest.parent().expect("a crate has a parent directory").to_path_buf();
    let mut hasher = Sha256::new();
    for (name, dir) in [("store_read", manifest.clone()), ("kernel_text", siblings.join("kernel_text")), ("geometry", siblings.join("geometry"))] {
        let src = dir.join("src");
        println!("cargo:rerun-if-changed={}", src.display());
        let mut found = Vec::new();
        files(&src, &src, &mut found);
        found.sort();
        for relative in found {
            let label = format!("{name}/{}", relative.to_string_lossy().replace('\\', "/"));
            let bytes = fs::read(src.join(&relative)).expect("a source file reads");
            hasher.update((label.len() as u64).to_be_bytes());
            hasher.update(label.as_bytes());
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(&bytes);
        }
    }
    let pins = manifest.join("pins.txt");
    println!("cargo:rerun-if-changed={}", pins.display());
    hasher.update(fs::read(&pins).expect("pins.txt reads"));
    let digest = hasher.finalize().iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    println!("cargo:rustc-env=STORE_READ_RANK_DIGEST={digest}");
}
