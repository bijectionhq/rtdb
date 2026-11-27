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

static INSTALLED: std::sync::OnceLock<AppleSync> = std::sync::OnceLock::new();

impl AppleSync {
    /// Absent or blank keeps `Full`; trimmed, case-insensitive `full` or `fsync` selects a policy.
    /// - Errors: any other value, so that a typo cannot choose a durability policy.
    pub fn from_setting(setting: Option<&str>) -> rtdb_types::Result<Self> {
        match setting.map(str::trim) {
            None | Some("") => Ok(Self::Full),
            Some(value) if value.eq_ignore_ascii_case("full") => Ok(Self::Full),
            Some(value) if value.eq_ignore_ascii_case("fsync") => Ok(Self::Fsync),
            Some(_) => Err(rtdb_types::Anomaly::incorrect("rtdb.storage/apple-sync", "The Apple sync policy must be 'full' or 'fsync'")),
        }
    }

    /// Installs the process's policy; a binary calls this from its validated startup configuration before any durable write.
    /// - Errors: a policy different from one already installed. Reinstalling the same policy succeeds.
    pub fn install(self) -> rtdb_types::Result<()> {
        if *INSTALLED.get_or_init(|| self) == self { return Ok(()); }
        Err(rtdb_types::Anomaly::incorrect("rtdb.storage/apple-sync", "A different Apple sync policy is already installed"))
    }

    /// The installed policy, or `Full` when the process installed none.
    pub fn installed() -> Self { INSTALLED.get().copied().unwrap_or(Self::Full) }
}

/// Flushes a file's data and metadata to stable storage, as the reference's `FileChannel.force(true)` does.
/// On Apple platforms that is `F_FULLFSYNC`, as the JDK issues there, unless the installed [`AppleSync`] is `Fsync`;
/// where the filesystem or file type does not support `F_FULLFSYNC`, it falls back to `fsync` and reports that once per process.
pub fn sync_file(file: &File) -> std::io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor is owned by `file`, which outlives both calls.
        if AppleSync::installed() == AppleSync::Full {
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } != -1 { return Ok(()); }
            let error = std::io::Error::last_os_error();
            if !matches!(error.raw_os_error(), Some(libc::ENOTSUP | libc::EINVAL | libc::ENOTTY)) { return Err(error); }
            static REPORTED: std::sync::Once = std::sync::Once::new();
            REPORTED.call_once(|| tracing::warn!("RTDB durable writes fall back from F_FULLFSYNC to fsync where it is unsupported: {error}"));
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
    fn only_a_known_setting_selects_apple_durability() {
        assert_eq!(AppleSync::from_setting(None).unwrap(), AppleSync::Full);
        assert_eq!(AppleSync::from_setting(Some(" ")).unwrap(), AppleSync::Full);
        assert_eq!(AppleSync::from_setting(Some("Full")).unwrap(), AppleSync::Full);
        assert_eq!(AppleSync::from_setting(Some(" FSYNC ")).unwrap(), AppleSync::Fsync);
        assert_eq!(AppleSync::from_setting(Some("fsnyc")).unwrap_err().code, "rtdb.storage/apple-sync");
    }

    #[test]
    fn an_installed_policy_is_fixed_for_the_process() {
        AppleSync::Full.install().unwrap();
        AppleSync::Full.install().unwrap();
        assert_eq!(AppleSync::installed(), AppleSync::Full);
        assert_eq!(AppleSync::Fsync.install().unwrap_err().code, "rtdb.storage/apple-sync");
        assert_eq!(AppleSync::installed(), AppleSync::Full);
    }
}
