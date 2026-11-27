use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use fs2::FileExt;
use uuid::Uuid;

use super::local::corrupt;
use crate::{AppleSync, Error, Result, sync_directory, sync_file};

const MAGIC: [u8; 8] = *b"\x1aRTDBJ01";
const LINEAGE_HEADER_BYTES: u64 = 24;

fn migration_name(destination: &Path) -> OsString {
    let mut name = OsString::from(".");
    name.push(destination.file_name().expect("canonical journal file name"));
    name.push(".rtdb-lineage-migration");
    name
}

fn discard_interrupted_migration(destination: &Path, apple_sync: AppleSync) -> Result<()> {
    let staged = destination.with_file_name(migration_name(destination));
    match fs::remove_file(&staged) {
        Ok(()) => {
            let directory = destination.parent().expect("canonical journal directory");
            sync_directory(directory, apple_sync).map_err(|error| Error::io(directory, error))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(staged, error)),
    }
}

fn sync_parent_directory(path: &Path, apple_sync: AppleSync) -> Result<()> {
    let directory = path.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or(Path::new("."));
    sync_directory(directory, apple_sync).map_err(|error| Error::io(directory, error))
}

pub(crate) struct JournalFile {
    file: File,
    lineage: Option<Uuid>,
    apple_sync: AppleSync,
}

impl JournalFile {
    fn inspect_header(mut file: File, path: &Path, apple_sync: AppleSync) -> Result<Self> {
        let mut magic = [0; MAGIC.len()];
        let lineage = if file.read(&mut magic[..1]).map_err(|error| Error::io(path, error))? == 1 && magic[0] == MAGIC[0] {
            let invalid = || corrupt(path, 0, "invalid replica journal lineage header");
            file.read_exact(&mut magic[1..]).map_err(|_| invalid())?;
            if magic != MAGIC { return Err(invalid()); }
            let mut id = [0; 16];
            file.read_exact(&mut id).map_err(|_| invalid())?;
            let lineage = Uuid::from_bytes(id);
            if lineage.is_nil() { return Err(invalid()); }
            Some(lineage)
        } else {
            file.rewind().map_err(|error| Error::io(path, error))?;
            None
        };
        Ok(Self { file, lineage, apple_sync })
    }

    /// Whether `path` still names this open file, so that reading through it reads the journal at that path.
    pub(super) fn is_at(&self, path: &Path) -> Result<bool> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let current = match fs::metadata(path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(Error::io(path, error)),
            };
            let held = self.file.metadata().map_err(|error| Error::io(path, error))?;
            Ok((held.dev(), held.ino()) == (current.dev(), current.ino()))
        }
        #[cfg(not(unix))]
        { let _ = path; Ok(false) }
    }

    pub(super) fn open(path: &Path, expected: Option<Uuid>) -> Result<Option<Self>> {
        let journal = match File::open(path) {
            Ok(file) => Some(Self::inspect_header(file, path, AppleSync::Full)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(Error::io(path, error)),
        };
        if expected.is_some() && journal.as_ref().and_then(Self::lineage) != expected {
            return Err(corrupt(path, 0, "replica journal lineage changed after open"));
        }
        Ok(journal)
    }

    pub(crate) fn lock(path: &Path, create: bool, apple_sync: AppleSync) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).create(create).truncate(false).open(path)
            .map_err(|error| Error::io(path, error))?;
        FileExt::try_lock_exclusive(&file).map_err(|error| Error::io(path, error))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let held = file.metadata().map_err(|error| Error::io(path, error))?;
            let current = fs::metadata(path).map_err(|error| Error::io(path, error))?;
            if (held.dev(), held.ino()) != (current.dev(), current.ino()) {
                return Err(corrupt(path, 0, "journal file was replaced while acquiring its writer lock"));
            }
        }
        let journal = Self::inspect_header(file, path, apple_sync)?;
        if journal.lineage.is_some() || path.file_name().is_some_and(|name| name == "REPLICA_LOG") {
            let destination = fs::canonicalize(path).map_err(|error| Error::io(path, error))?;
            discard_interrupted_migration(&destination, apple_sync)?;
        }
        Ok(journal)
    }

    pub(crate) fn cached_lineage(&self, path: &Path) -> Result<Option<Uuid>> {
        if self.lineage.is_none() && path.file_name().is_none_or(|name| name != "REPLICA_LOG") {
            return Ok(None);
        }
        let cache_path = path.with_extension("lineage");
        let bytes = match fs::read(&cache_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Error::io(cache_path, error)),
        };
        let cached = Uuid::from_slice(&bytes).ok().filter(|id| !id.is_nil())
            .ok_or_else(|| corrupt(path, 0, "invalid replica journal lineage sidecar"))?;
        if self.lineage.is_some_and(|lineage| lineage != cached) {
            return Err(corrupt(path, 0, "replica journal lineage disagrees with its sidecar"));
        }
        Ok(Some(cached))
    }

    pub(super) fn remove_lineage_sidecar(path: &Path, apple_sync: AppleSync) -> Result<()> {
        let sidecar = path.with_extension("lineage");
        match fs::remove_file(&sidecar) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::io(sidecar, error)),
        }
        sync_parent_directory(&sidecar, apple_sync)
    }

    pub(super) fn bind_lineage(mut self, path: &Path, lineage: Uuid) -> Result<Self> {
        let apple_sync = self.apple_sync;
        let io = |error| Error::io(path, error);
        let destination = fs::canonicalize(path).map_err(io)?;
        let directory = destination.parent().ok_or_else(|| corrupt(path, 0, "replica journal has no owner directory"))?;
        discard_interrupted_migration(&destination, apple_sync)?;
        let mut staged = tempfile::Builder::new().prefix(&migration_name(&destination)).rand_bytes(0).tempfile_in(directory).map_err(io)?;
        let permissions = self.file.metadata().map_err(io)?.permissions();
        staged.write_all(&MAGIC).map_err(io)?;
        staged.write_all(lineage.as_bytes()).map_err(io)?;
        self.rewind().map_err(io)?;
        io::copy(&mut self, &mut staged).map_err(io)?;
        staged.as_file().set_permissions(permissions).map_err(io)?;
        // Hold both inodes locked across publication so the replacement has no unlocked interval.
        FileExt::try_lock_exclusive(staged.as_file()).map_err(io)?;
        sync_file(staged.as_file(), apple_sync).map_err(io)?;
        let file = staged.persist(&destination).map_err(|error| io(error.error))?;
        sync_directory(directory, apple_sync).map_err(io)?;
        Ok(Self { file, lineage: Some(lineage), apple_sync })
    }

    pub(super) fn lineage(&self) -> Option<Uuid> { self.lineage }

    fn records_start(&self) -> u64 { if self.lineage.is_some() { LINEAGE_HEADER_BYTES } else { 0 } }

    pub(super) fn records_len(&self) -> io::Result<u64> {
        let metadata = self.file.metadata()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.nlink() == 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "journal file was unlinked"));
            }
        }
        metadata.len().checked_sub(self.records_start())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "replica journal lineage header was truncated"))
    }

    pub(super) fn truncate_records(&self, length: u64) -> io::Result<()> {
        self.file.set_len(self.physical_offset(length)?)
    }

    pub(super) fn sync(&self) -> io::Result<()> { sync_file(&self.file, self.apple_sync) }

    pub(super) fn sync_open(&self, path: &Path) -> Result<()> {
        let io = |error| Error::io(path, error);
        self.sync().map_err(io)?;
        let destination = if self.lineage.is_some() { fs::canonicalize(path).map_err(io)? } else { path.to_path_buf() };
        sync_parent_directory(&destination, self.apple_sync)
    }

    fn physical_offset(&self, offset: u64) -> io::Result<u64> {
        offset.checked_add(self.records_start()).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "journal offset overflow"))
    }
}

impl Read for JournalFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> { self.file.read(buffer) }
}

impl Write for JournalFile {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> { self.file.write(buffer) }
    fn write_vectored(&mut self, buffers: &[io::IoSlice<'_>]) -> io::Result<usize> { self.file.write_vectored(buffers) }
    fn flush(&mut self) -> io::Result<()> { self.file.flush() }
}

impl Seek for JournalFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let position = match position {
            SeekFrom::Start(offset) => SeekFrom::Start(self.physical_offset(offset)?),
            position => position,
        };
        self.file.seek(position)?.checked_sub(self.records_start())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek into replica journal lineage header"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_journal_lineage_sync_accepts_a_bare_relative_path() -> Result<()> {
        sync_parent_directory(Path::new("REPLICA_LOG.lineage"), AppleSync::Full)
    }
}
