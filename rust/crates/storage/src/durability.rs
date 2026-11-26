use std::fs::File;
use std::path::Path;

/// How a durable write reaches storage on Apple platforms, where `fsync` stops at the drive's volatile write cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppleSync {
    /// `F_FULLFSYNC`: the drive flushes its cache, so an acknowledged write survives power loss, as with the JDK's `force`.
    Full,
    /// `fsync` alone: faster, but the last acknowledged writes can be lost or reordered on power loss.
    Fsync,
}

impl AppleSync {
    /// `RTDB_APPLE_SYNC=fsync` selects `Fsync`; anything else, or nothing, keeps `Full`.
    pub fn from_setting(setting: Option<&str>) -> Self {
        match setting.map(str::trim) {
            Some(value) if value.eq_ignore_ascii_case("fsync") => Self::Fsync,
            _ => Self::Full,
        }
    }

    pub fn configured() -> Self {
        static CONFIGURED: std::sync::LazyLock<AppleSync> =
            std::sync::LazyLock::new(|| AppleSync::from_setting(std::env::var("RTDB_APPLE_SYNC").ok().as_deref()));
        *CONFIGURED
    }
}

/// Flushes a file's data and metadata to stable storage, as the reference's `FileChannel.force(true)` does.
/// On Apple platforms that is `F_FULLFSYNC`, as the JDK issues there, unless `RTDB_APPLE_SYNC=fsync` chose `fsync`;
/// where the filesystem or file type does not support `F_FULLFSYNC`, it falls back to `fsync`.
pub fn sync_file(file: &File) -> std::io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor is owned by `file`, which outlives both calls.
        if AppleSync::configured() == AppleSync::Full {
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } != -1 { return Ok(()); }
            let error = std::io::Error::last_os_error();
            if !matches!(error.raw_os_error(), Some(libc::ENOTSUP | libc::EINVAL | libc::ENOTTY)) { return Err(error); }
        }
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
    }
    #[cfg(not(target_vendor = "apple"))]
    { file.sync_all() }
}


/// Flushes the directory entries that publish or remove durable files.
pub fn sync_directory(path: &Path) -> std::io::Result<()> {
    sync_file(&rtdb_types::directory_io::open_for_sync(path)?)
}

#[cfg(test)]
mod tests {
    use super::AppleSync;
    use std::io::Write;

    #[test]
    fn files_and_directories_reach_stable_storage() {
        let directory = tempfile::tempdir().unwrap();
        let mut file = std::fs::File::create(directory.path().join("journal")).unwrap();
        file.write_all(b"durable").unwrap();
        super::sync_file(&file).unwrap();
        super::sync_directory(directory.path()).unwrap();
    }

    #[test]
    fn only_an_explicit_fsync_setting_weakens_apple_durability() {
        assert_eq!(AppleSync::from_setting(None), AppleSync::Full);
        assert_eq!(AppleSync::from_setting(Some("full")), AppleSync::Full);
        assert_eq!(AppleSync::from_setting(Some("fsnyc")), AppleSync::Full);
        assert_eq!(AppleSync::from_setting(Some(" FSYNC ")), AppleSync::Fsync);
    }
}
