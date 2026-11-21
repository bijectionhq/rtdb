use std::{
    fs::{self, File, TryLockError},
    io::{BufReader, BufWriter, ErrorKind, Write},
    path::{Path, PathBuf},
};

use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};

use super::{MemoryState, exists};

const FORMAT: u32 = 1;

pub(super) struct StateFile {
    root: PathBuf,
    _lock: File,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    format: u32,
    state: MemoryState,
}

#[derive(Serialize)]
struct SnapshotRef<'a> {
    format: u32,
    state: &'a MemoryState,
}

impl StateFile {
    pub(super) fn lock(root: &Path) -> Result<Self> {
        let lock = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("directory.lock"))
            .map_err(io)?;
        lock.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.directory/locked",
                "The local directory already has a serving process",
            ),
            TryLockError::Error(error) => io(error),
        })?;
        Ok(Self { root: root.to_owned(), _lock: lock })
    }

    pub(super) fn initialize(&self) -> Result<()> {
        if self.state_path().try_exists().map_err(io)? {
            return Err(exists("Local directory", "directory.json"));
        }
        self.save(&MemoryState::default())
    }

    pub(super) fn load(&self) -> Result<MemoryState> {
        let source = File::open(self.state_path()).map_err(io)?;
        let snapshot: Snapshot = serde_json::from_reader(BufReader::new(source)).map_err(codec)?;
        if snapshot.format != FORMAT {
            return Err(Anomaly::incorrect("rtdb.directory/state-format", "Unsupported local directory format"));
        }
        Ok(snapshot.state)
    }

    pub(super) fn save(&self, state: &MemoryState) -> Result<()> {
        let next = self.root.join("directory.pending");
        match fs::remove_file(&next) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(io(error)),
        }
        let mut file = BufWriter::new(File::create_new(&next).map_err(io)?);
        serde_json::to_writer(&mut file, &SnapshotRef { format: FORMAT, state }).map_err(codec)?;
        file.flush().map_err(io)?;
        file.get_ref().sync_all().map_err(io)?;
        drop(file);
        fs::rename(next, self.state_path()).map_err(io)?;
        rtdb_types::directory_io::open_for_sync(&self.root).and_then(|root| root.sync_all()).map_err(io)
    }

    fn state_path(&self) -> PathBuf { self.root.join("directory.json") }
}

fn io(error: std::io::Error) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.directory/state", error.to_string())
}

fn codec(error: serde_json::Error) -> Anomaly {
    Anomaly::fault("rtdb.directory/state-codec", error.to_string())
}
