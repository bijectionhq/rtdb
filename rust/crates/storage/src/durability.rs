use std::fs::File;

/// Flushes a file's data and metadata to the device, as the reference's `FileChannel.force(true)` does.
/// On Apple platforms that is `fsync`; the standard library's `sync_all` there issues `F_FULLFSYNC`, which also drains the drive's write cache.
pub fn sync_file(file: &File) -> std::io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor is owned by `file`, which outlives the call.
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
    }
    #[cfg(not(target_vendor = "apple"))]
    { file.sync_all() }
}
