use std::{env, path::PathBuf, process::Command};

fn git(root: &std::path::Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git").arg("-C").arg(root).args(arguments).output().ok()?;
    output.status.success().then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn main() {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory"));
    let root = manifest.ancestors().nth(3).expect("conformance crate belongs to the RTDB workspace");
    let revision = git(root, &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty = git(root, &["status", "--porcelain", "--untracked-files=all"]).is_none_or(|status| !status.is_empty());
    println!("cargo:rustc-env=RTDB_CONFORMANCE_REVISION={revision}");
    println!("cargo:rustc-env=RTDB_CONFORMANCE_DIRTY={dirty}");
    for path in ["rust/crates", "rust/Cargo.toml", "rust/Cargo.lock", "core/src/main/proto", "rust/conformance"] {
        println!("cargo:rerun-if-changed={}", root.join(path).display());
    }
    for name in ["HEAD", "index", "packed-refs"] {
        if let Some(path) = git(root, &["rev-parse", "--path-format=absolute", "--git-path", name]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Some(reference) = git(root, &["symbolic-ref", "HEAD"]) {
        if let Some(path) = git(root, &["rev-parse", "--path-format=absolute", "--git-path", &reference]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}
