use std::{path::Path, sync::Arc};
#[cfg(feature = "remote")]
use std::{fs::{self, File, OpenOptions}, path::PathBuf};

#[cfg(feature = "remote")]
use fs2::FileExt;
#[cfg(feature = "remote")]
use uuid::Uuid;

#[cfg(feature = "remote")]
use crate::{Error, Result};

tokio::task_local! {
    static SELECTED_VAULT: Arc<Path>;
}

#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ClassicCatalogDrain {
    device: u64,
    inode: u64,
}

pub(crate) fn within_vault<F: std::future::Future>(vault: Option<Arc<Path>>, work: F) -> impl std::future::Future<Output = F::Output> {
    async move {
        match vault {
            Some(vault) => SELECTED_VAULT.scope(vault, work).await,
            None => work.await,
        }
    }
}

#[cfg(feature = "remote")]
pub(crate) fn selected_vault() -> Option<Arc<Path>> {
    SELECTED_VAULT.try_with(Arc::clone).ok()
}

#[cfg(feature = "remote")]
fn witness_path(vault: &Path, ticket: Uuid) -> PathBuf {
    vault.join("catalog-readers").join(ticket.simple().to_string())
}

#[cfg(all(feature = "remote", unix))]
fn identity(file: &File, path: &Path) -> Result<Option<ClassicCatalogDrain>> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata().map_err(|error| Error::io(path, error))?;
    Ok(Some(ClassicCatalogDrain { device: metadata.dev(), inode: metadata.ino() }))
}

#[cfg(all(feature = "remote", not(unix)))]
fn identity(_: &File, _: &Path) -> Result<Option<ClassicCatalogDrain>> { Ok(None) }

/// Its file stays locked from before provider registration through the last
/// admitted reader. A crash leaves the named witness for the vault owner.
#[cfg(feature = "remote")]
pub(crate) struct CatalogReadWitness {
    path: PathBuf,
    _locked: File,
    pub(crate) identity: ClassicCatalogDrain,
}

#[cfg(feature = "remote")]
impl CatalogReadWitness {
    pub(crate) fn reserve(vault: &Path, ticket: Uuid) -> Result<Option<Self>> {
        let path = witness_path(vault, ticket);
        let directory = vault.join("catalog-readers");
        fs::create_dir_all(&directory).map_err(|error| Error::io(&directory, error))?;
        let file = OpenOptions::new().read(true).write(true).create_new(true).open(&path)
            .map_err(|error| Error::io(&path, error))?;
        FileExt::lock_exclusive(&file).map_err(|error| Error::io(&path, error))?;
        let Some(identity) = identity(&file, &path)? else {
            fs::remove_file(&path).map_err(|error| Error::io(&path, error))?;
            return Ok(None);
        };
        crate::sync_file(&file).map_err(|error| Error::io(&path, error))?;
        crate::sync_directory(&directory).map_err(|error| Error::io(&directory, error))?;
        crate::sync_directory(vault).map_err(|error| Error::io(vault, error))?;
        Ok(Some(Self { path, _locked: file, identity }))
    }

    pub(crate) fn drained(vault: &Path, ticket: Uuid, expected: ClassicCatalogDrain) -> Result<bool> {
        let path = witness_path(vault, ticket);
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(Error::io(&path, error)),
        };
        if identity(&file, &path)? != Some(expected) { return Ok(false); }
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == fs2::lock_contended_error().kind() => Ok(false),
            Err(error) => Err(Error::io(path, error)),
        }
    }

    pub(crate) fn forget(&self) -> Result<()> {
        fs::remove_file(&self.path).map_err(|error| Error::io(&self.path, error))
    }
}

#[cfg(all(test, feature = "remote", unix))]
mod tests {
    use super::*;

    #[test]
    fn the_exact_witness_must_stay_locked_until_the_last_provider_owner_drains() {
        let vault = tempfile::tempdir().unwrap();
        let ticket = Uuid::new_v4();
        let registered = Arc::new(CatalogReadWitness::reserve(vault.path(), ticket).unwrap().unwrap());
        let identity = registered.identity;
        let admitted_io = Arc::clone(&registered);
        drop(registered);
        assert!(!CatalogReadWitness::drained(vault.path(), ticket, identity).unwrap());
        drop(admitted_io);
        assert!(CatalogReadWitness::drained(vault.path(), ticket, identity).unwrap());
        assert!(!CatalogReadWitness::drained(vault.path(), Uuid::new_v4(), identity).unwrap());
    }
}
