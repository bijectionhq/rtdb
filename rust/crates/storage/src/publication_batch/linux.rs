use std::{fs::File, path::Path, thread};

use crate::{AppleSync, Error, Result, sync_file};

use super::{Directory, StagedObject};

const MAX_PAYLOAD_SYNC_WORKERS: usize = 32;
const MAX_DIRECTORY_SYNC_WORKERS: usize = 8;

fn sync_descriptors(descriptors: &[(&File, &Path)], policy: AppleSync, max_workers: usize) -> Result<()> {
    if descriptors.is_empty() { return Ok(()); }
    let worker_count = descriptors.len().min(max_workers);
    let sync_assigned = |worker| {
        for &(file, path) in descriptors.iter().skip(worker).step_by(worker_count) {
            sync_file(file, policy).map_err(|error| Error::io(path, error))?;
        }
        Ok(())
    };
    thread::scope(|scope| {
        let mut workers = Vec::new();
        let mut result = Ok(());
        for worker in 1..worker_count {
            match thread::Builder::new().spawn_scoped(scope, move || sync_assigned(worker)) {
                Ok(handle) => workers.push(handle),
                Err(_) => result = result.and(sync_assigned(worker)),
            }
        }
        result = result.and(sync_assigned(0));
        for worker in workers {
            let synced = worker.join().unwrap_or_else(|_| {
                Err(Error::Task("Checkpoint publication sync worker panicked".into()))
            });
            result = result.and(synced);
        }
        result
    })
}

pub(super) fn sync_payloads(files: &[StagedObject], policy: AppleSync) -> Result<()> {
    let descriptors: Vec<_> = files.iter()
        .map(|object| (object.upload.as_file(), object.upload.path()))
        .collect();
    sync_descriptors(&descriptors, policy, MAX_PAYLOAD_SYNC_WORKERS)
}

pub(super) fn sync_directories(directories: &[Directory], policy: AppleSync) -> Result<()> {
    for level in directories.chunk_by(|left, right| {
        left.path.components().count() == right.path.components().count()
    }) {
        let descriptors: Vec<_> = level.iter()
            .map(|directory| (&directory.file, directory.path.as_path()))
            .collect();
        sync_descriptors(&descriptors, policy, MAX_DIRECTORY_SYNC_WORKERS)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write, path::Path};

    use crate::{AppleSync, Error};

    use super::sync_descriptors;

    #[test]
    fn local_checkpoint_publication_sync_reports_worker_file_error() {
        let mut staged = tempfile::NamedTempFile::new().unwrap();
        staged.write_all(b"payload").unwrap();
        let unsyncable_path = Path::new("/dev/null");
        let unsyncable = File::open(unsyncable_path).unwrap();
        let valid = (staged.as_file(), staged.path());
        let result = sync_descriptors(
            &[valid, valid, valid, (&unsyncable, unsyncable_path)],
            AppleSync::Full,
            2,
        );
        assert!(matches!(result, Err(Error::Io { path, .. }) if path == unsyncable_path));
    }
}
