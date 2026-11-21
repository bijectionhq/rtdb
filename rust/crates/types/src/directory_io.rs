use std::fs::File;
use std::io;
use std::path::Path;

/// Opens a directory for flushing its namespace updates.
pub fn open_for_sync(path: &Path) -> io::Result<File> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;

        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        // A Windows directory handle requires backup semantics to open.
        // Flushing the handle requires write access.
        let directory = File::options()
            .write(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)?;
        let kind = directory.metadata()?.file_type();
        if !kind.is_dir() || kind.is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "expected a directory for flushing",
            ));
        }
        Ok(directory)
    }
    #[cfg(not(windows))]
    {
        File::open(path)
    }
}
