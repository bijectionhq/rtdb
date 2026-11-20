//! Forks by reference (`ForkByReference`): a fork's store names, in a `~base` manifest, the objects it shares with its parent and where each one physically lives, instead of copying them.
//!
//! - `ReferencingStore` reads a store's own object first and its referenced one otherwise, and lists both.
//! - Whatever erases a physical object (the reaper, a deletion) first copies it into every store that still references it, so a reference never dangles.
//! - A fork releasing an object it only references drops the reference; the object itself is its owner's.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rtdb_directory::{DeploymentRecord, Lifecycle};
use rtdb_storage::{Error, IoRegion, LocalStore, ObjectKey, ObjectStore, object_store::StoredObject};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};

pub const BASE_MANIFEST: &str = "~base";

/// A store's references: for each object it shares, the directory, relative to the cell's storage root, that physically holds it under the same key.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Base { pub objects: BTreeMap<String, String> }

impl Base {
    pub fn load(store: &Path) -> Result<Base> {
        match std::fs::read(store.join(BASE_MANIFEST)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| Anomaly::fault("rtdb.multi/base-manifest", format!("{}: {error}", store.display()))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Base::default()),
            Err(error) => Err(io(error)),
        }
    }
    pub fn save(&self, store: &Path) -> Result<()> {
        std::fs::create_dir_all(store).map_err(io)?;
        let temporary = store.join(format!("{BASE_MANIFEST}.{}.tmp", uuid::Uuid::new_v4().simple()));
        std::fs::write(&temporary, serde_json::to_vec(self).expect("a manifest serializes")).map_err(io)?;
        std::fs::rename(&temporary, store.join(BASE_MANIFEST)).map_err(io)
    }
}

/// A store's own objects and the objects its manifest references, read own-first.
pub struct ReferencingStore { own: LocalStore, root: PathBuf, base: Base, io: Arc<IoRegion> }

impl ReferencingStore {
    /// Opens `own`, a store under the cell's `root` whose manifest is `base`.
    pub fn new(own: LocalStore, root: PathBuf, base: Base, io: Arc<IoRegion>) -> Self { Self { own, root, base, io } }

    fn referenced(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<LocalStore>> {
        let Some(directory) = self.base.objects.get(key.as_str()) else { return Ok(None) };
        Ok(Some(LocalStore::open_in(self.root.join(directory), Arc::clone(&self.io)).map_err(|error| Error::Task(error.to_string()))?))
    }

    /// The store that holds `key`: this one, or the one it references the object in.
    async fn locate(&self, key: &ObjectKey) -> rtdb_storage::Result<Located<'_>> {
        match self.own.head(key).await {
            Err(Error::MissingObject(_)) => Ok(self.referenced(key)?.map_or(Located::Own(&self.own), Located::Referenced)),
            _ => Ok(Located::Own(&self.own)),
        }
    }
}

enum Located<'a> { Own(&'a LocalStore), Referenced(LocalStore) }

impl Located<'_> {
    fn store(&self) -> &LocalStore { match self { Located::Own(store) => store, Located::Referenced(store) => store } }
}

#[async_trait]
impl ObjectStore for ReferencingStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.locate(key).await?.store().head(key).await }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.locate(key).await?.store().get(key).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.locate(key).await?.store().get_range(key, range).await }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        // Put-if-absent holds across the references: a shared object is already present.
        if let (Err(Error::MissingObject(_)), Some(referenced)) = (self.own.head(key).await, self.referenced(key)?) {
            return if referenced.get(key).await? == bytes { Ok(()) } else { Err(Error::ConflictingObject(key.to_string())) };
        }
        self.own.put(key, bytes).await
    }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> {
        if self.base.objects.contains_key(key.as_str()) { return self.put(key, Bytes::from(std::fs::read(source).map_err(|error| Error::Task(format!("{}: {error}", source.display())))?)).await; }
        self.own.put_file(key, source).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        let mut listed: BTreeMap<String, StoredObject> = self.own.list(directory).await?.into_iter()
            .filter(|object| object.key.as_str() != BASE_MANIFEST).map(|object| (object.key.as_str().to_owned(), object)).collect();
        for name in self.base.objects.keys() {
            let key = ObjectKey::new(name.clone()).map_err(|error| Error::InvalidKey(error.to_string()))?;
            if listed.contains_key(name) || directory.is_some_and(|directory| !key.is_under(directory)) { continue; }
            // A reference whose object has since been copied in and erased at its source is listed from the copy.
            if let Ok(object) = self.head(&key).await { listed.insert(name.clone(), object); }
        }
        Ok(listed.into_values().collect())
    }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.own.delete(key).await }
}

/// The physical files of one store directory, as keys: everything but its bookkeeping.
pub fn physical_objects(store: &Path) -> Result<Vec<String>> {
    fn visit(root: &Path, directory: &Path, found: &mut Vec<String>) -> Result<()> {
        let Ok(entries) = std::fs::read_dir(directory) else { return Ok(()) };
        for entry in entries {
            let entry = entry.map_err(io)?;
            let path = entry.path();
            if entry.file_type().map_err(io)?.is_dir() {
                if entry.file_name() != ".tmp" { visit(root, &path, found)?; }
                continue;
            }
            let key = path.strip_prefix(root).expect("under the root").to_string_lossy().replace('\\', "/");
            if key == crate::store::ROOT_MARKER || key == BASE_MANIFEST || key.ends_with(".tmp") { continue; }
            found.push(key);
        }
        Ok(())
    }
    let mut found = Vec::new();
    visit(store, store, &mut found)?;
    Ok(found)
}

/// The store directories (one per storage version) under a deployment's prefix.
pub fn stores(prefix: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(prefix).map(|entries| entries.flatten().filter(|entry| entry.path().is_dir()).map(|entry| entry.path()).collect()).unwrap_or_default()
}

/// Forks the deployment at `parent` (a prefix under `root`) into `child`, referencing every object the parent's stores hold or reference.
pub fn fork(root: &Path, parent: &str, child: &str) -> Result<()> {
    for store in stores(&root.join(parent)) {
        let version = store.file_name().expect("a store directory has a name").to_string_lossy().into_owned();
        let mut base = Base::load(&store)?;
        let relative = format!("{parent}/{version}");
        for key in physical_objects(&store)? { base.objects.insert(key, relative.clone()); }
        base.save(&root.join(child).join(&version))?;
    }
    Ok(())
}

/// Every reference in a cell: for each physical object, the store directories that reference it.
#[derive(Default)]
pub struct References { by_object: BTreeMap<PathBuf, BTreeSet<PathBuf>> }

impl References {
    /// The references of every deployment in `records` that is not deleted, under `root`.
    pub fn scan(root: &Path, records: &[DeploymentRecord]) -> Result<Self> {
        let mut references = Self::default();
        for record in records.iter().filter(|record| record.lifecycle != Lifecycle::Deleted) {
            for store in stores(&root.join(record.storage_prefix())) {
                for (key, directory) in Base::load(&store)?.objects {
                    if store.join(&key).exists() { continue; }
                    references.by_object.entry(root.join(directory).join(&key)).or_default().insert(store.clone());
                }
            }
        }
        Ok(references)
    }

    /// Erases the physical object `path` (stored as `key`), first copying it into every store that still references it.
    pub fn erase(&self, path: &Path, key: &str) -> Result<()> {
        for store in self.by_object.get(path).into_iter().flatten() { copy_in(path, &store.join(key))?; }
        std::fs::remove_file(path).map_err(io)
    }

    /// Erases every physical object under a store directory, copying the referenced ones out first.
    pub fn erase_store(&self, store: &Path) -> Result<()> {
        for key in physical_objects(store)? { self.erase(&store.join(&key), &key)?; }
        Ok(())
    }
}

fn copy_in(from: &Path, to: &Path) -> Result<()> {
    if to.exists() { return Ok(()); }
    std::fs::create_dir_all(to.parent().expect("an object has a directory")).map_err(io)?;
    let temporary = to.with_extension(format!("{}.tmp", uuid::Uuid::new_v4().simple()));
    std::fs::copy(from, &temporary).map_err(io)?;
    std::fs::rename(&temporary, to).map_err(io)
}

/// Copies a deployment's stores from `from` to `to` (prefixes under their cells' roots), resolving every reference into an object of its own.
pub fn materialize(from_root: &Path, from: &str, to_root: &Path, to: &str) -> Result<()> {
    for store in stores(&from_root.join(from)) {
        let version = store.file_name().expect("a store directory has a name").to_owned();
        let target = to_root.join(to).join(&version);
        for key in physical_objects(&store)? { copy_in(&store.join(&key), &target.join(&key))?; }
        for (key, directory) in Base::load(&store)?.objects {
            let source = from_root.join(directory).join(&key);
            if source.exists() { copy_in(&source, &target.join(&key))?; }
        }
        std::fs::create_dir_all(&target).map_err(io)?;
    }
    Ok(())
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/references", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fork_moved_to_another_cell_owns_everything_it_referenced() {
        let (here, there) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let parent = here.path().join("p/v06");
        std::fs::create_dir_all(parent.join("blocks")).unwrap();
        std::fs::write(parent.join("blocks/b00"), b"parent").unwrap();
        std::fs::write(parent.join(crate::store::ROOT_MARKER), b"p").unwrap();
        fork(here.path(), "p", "c").unwrap();
        std::fs::create_dir_all(here.path().join("c/v06/blocks")).unwrap();
        std::fs::write(here.path().join("c/v06/blocks/b01"), b"child").unwrap();
        materialize(here.path(), "c", there.path(), "c").unwrap();
        let mut moved = physical_objects(&there.path().join("c/v06")).unwrap();
        moved.sort();
        assert_eq!(moved, ["blocks/b00", "blocks/b01"]);
        assert!(!there.path().join("c/v06").join(BASE_MANIFEST).exists(), "the moved store references nothing");
        assert_eq!(std::fs::read(there.path().join("c/v06/blocks/b00")).unwrap(), b"parent");
    }
}
