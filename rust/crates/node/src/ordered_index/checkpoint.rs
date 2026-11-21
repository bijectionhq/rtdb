use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_storage::{Error, ObjectKey, cache::BufferPool};
use rtdb_types::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    format_error,
    maintenance::cancelled,
    projection::RevisionRoots,
    tree::{Item, Key, Link, Pointer, Root, Tree},
};

const MANIFESTS: &str = "manifests/scoped";

#[derive(Serialize, Deserialize)]
pub(super) struct Checkpoint {
    pub through: i64,
    pub scope: u64,
    pub source_rows: i64,
    revisions: Option<Arc<Pointer>>,
    #[serde(skip)]
    manifest_bytes: u64,
    #[serde(skip)]
    _allocation: Option<Arc<MemoryReservation>>,
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    checksum: String,
    layout: Checkpoint,
}

impl Checkpoint {
    pub fn roots(&self) -> RevisionRoots {
        RevisionRoots {
            archived: self.revisions.clone().map(Link::Stored),
            changes: BTreeMap::new(),
        }
    }

    async fn reachable(
        &self,
        tree: &Tree<'_>,
        cancellation: &CancellationToken,
    ) -> Result<BTreeMap<ObjectKey, u64>> {
        let mut pages = BTreeMap::new();
        if let Some(pointer) = &self.revisions {
            tree.mark(Arc::clone(pointer), &mut pages, cancellation).await?;
        }
        Ok(pages)
    }

    pub async fn file_bytes(&self, tree: &Tree<'_>, cancellation: &CancellationToken) -> Result<u64> {
        self.reachable(tree, cancellation).await?.values().try_fold(
            self.manifest_bytes,
            |total, bytes| total.checked_add(*bytes)
                .ok_or_else(|| format_error("Ordered index file bytes overflow")),
        )
    }

    pub async fn finish(&self, tree: &Tree<'_>, cancellation: &CancellationToken) -> Result<()> {
        let pending = self.pending_collection(tree).await?;
        if pending.is_empty() {
            self.retire_manifests(tree, cancellation).await
        } else {
            self.sweep(tree, pending, cancellation).await
        }
    }

    pub async fn collect(&self, tree: &Tree<'_>, cancellation: &CancellationToken) -> Result<()> {
        let pending = self.pending_collection(tree).await?;
        self.sweep(tree, pending, cancellation).await
    }

    async fn pending_collection(&self, tree: &Tree<'_>) -> Result<Vec<ObjectKey>> {
        let root = key(tree.directory, "cleanup")?;
        Ok(tree
            .storage
            .object_store()
            .list(Some(&root))
            .await?
            .into_iter()
            .filter_map(|object| {
                let through = frontier(&object.key)?;
                (through <= self.through).then_some(object.key)
            })
            .collect())
    }

    async fn sweep(
        &self,
        tree: &Tree<'_>,
        pending: Vec<ObjectKey>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let reachable = self.reachable(tree, cancellation).await?;
        let store = tree.storage.object_store();
        let prefix = tree.pages()?;
        let generations = key(tree.directory, "generations")?;
        let objects = store.list(Some(&generations)).await?;
        let present: BTreeSet<_> = objects.iter().map(|object| &object.key).collect();
        if !reachable.keys().all(|key| present.contains(key)) {
            return Err(format_error(
                "An ordered index checkpoint references a missing page",
            ));
        }
        self.retire_manifests(tree, cancellation).await?;
        let managed = store.index_page_lifecycle();
        if let Some(lifecycle) = managed {
            lifecycle.retire_before(tree.page_scope()).await?;
        }
        for object in objects {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let Some((scope, _)) = object.key.as_str()
                .strip_prefix(generations.as_str())
                .and_then(|suffix| suffix.strip_prefix('/'))
                .and_then(|suffix| suffix.split_once('/'))
            else {
                continue;
            };
            let Ok(scope) = u64::from_str_radix(scope, 16) else {
                continue;
            };
            let block = page_block(&object.key, &prefix);
            if (managed.is_none() && scope < self.scope)
                || (block.is_some_and(|block| block <= self.through)
                    && !reachable.contains_key(&object.key))
            {
                store.delete(&object.key).await?;
                tree.storage.invalidate(&object.key).await?;
            }
        }
        if managed.is_none() {
            let legacy = key(tree.directory, "pages")?;
            for object in store.list(Some(&legacy)).await? {
                if cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                if page_block(&object.key, &legacy).is_some_and(|block| block <= self.through) {
                    store.delete(&object.key).await?;
                    tree.storage.invalidate(&object.key).await?;
                }
            }
        }
        for debt in pending {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            store.delete(&debt).await?;
        }
        Ok(())
    }

    async fn retire_manifests(
        &self,
        tree: &Tree<'_>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let store = tree.storage.object_store();
        let published = key(tree.directory, MANIFESTS)?;
        for object in store.list(Some(&key(tree.directory, "manifests")?)).await? {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            if frontier(&object.key).is_some_and(|block| {
                block < self.through || (block == self.through && !object.key.is_under(&published))
            }) {
                store.delete(&object.key).await?;
                tree.storage.invalidate(&object.key).await?;
            }
        }
        Ok(())
    }
}

/// Recovery may advance the manifest before the sweep finishes.
pub(super) async fn request_collection(tree: &Tree<'_>, through: i64) -> Result<()> {
    let debt = key(tree.directory, format_args!("cleanup/{through:016x}"))?;
    tree.storage
        .object_store()
        .put(&debt, Vec::<u8>::new().into())
        .await?;
    Ok(())
}

fn key(directory: &ObjectKey, suffix: impl std::fmt::Display) -> Result<ObjectKey> {
    Ok(ObjectKey::new(format!("{directory}/{suffix}"))?)
}

fn page_block(object: &ObjectKey, pages: &ObjectKey) -> Option<i64> {
    let path = object.as_str().strip_prefix(pages.as_str())?.strip_prefix('/')?;
    let (block, _) = path.split_once('/')?;
    i64::from_str_radix(block, 16).ok()
}

fn frontier(key: &ObjectKey) -> Option<i64> {
    i64::from_str_radix(key.as_str().rsplit('/').next()?, 16).ok()
}

pub(super) async fn latest(
    storage: &BufferPool,
    directory: &ObjectKey,
    memory: &Arc<MemoryBudget>,
    through: i64,
) -> Result<Option<Checkpoint>> {
    if through < 0 {
        return Ok(None);
    }
    let store = storage.object_store();
    let head = key(directory, format_args!("{MANIFESTS}/{through:016x}"))?;
    let object = match store.head_fresh(&head).await {
        Ok(object) => Some(object),
        Err(Error::MissingObject(_)) => store
            .list(Some(&key(directory, MANIFESTS)?))
            .await?
            .into_iter()
            .filter(|object| frontier(&object.key).is_some_and(|block| block <= through))
            .max_by_key(|object| frontier(&object.key)),
        Err(error) => return Err(error.into()),
    };
    let Some(object) = object else {
        return Ok(None);
    };
    let size = usize::try_from(object.size).map_err(format_error)?;
    let allocation = memory.reserve(size.saturating_mul(8).saturating_add(4096))?;
    let bytes = store.get_fresh(&object.key).await?;
    if bytes.len() != size {
        return Err(format_error("Ordered index manifest length changed"));
    }
    let Manifest {
        checksum,
        layout: mut checkpoint,
    } = serde_json::from_slice(&bytes).map_err(format_error)?;
    if checksum
        != format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&checkpoint).map_err(format_error)?)
        )
        || frontier(&object.key) != Some(checkpoint.through)
        || checkpoint.through < 0
        || checkpoint.scope > checkpoint.through as u64 + 1
        || checkpoint.source_rows < 0
        || checkpoint
            .revisions
            .as_ref()
            .is_some_and(|root| !root.exists_by(checkpoint.through))
    {
        return Err(format_error("Invalid ordered index checkpoint"));
    }
    checkpoint.manifest_bytes = object.size;
    checkpoint._allocation = Some(allocation);
    Ok(Some(checkpoint))
}

pub(super) async fn store_roots(
    tree: &Tree<'_>,
    block: i64,
    roots: &RevisionRoots,
    cancellation: &CancellationToken,
) -> Result<Root> {
    let mut edits = Vec::with_capacity(roots.changes.len());
    let mut written = BTreeMap::new();
    for (&time, root) in &roots.changes {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let pointer = match root {
            Some(root) => Some(
                tree.persist(root, block, &mut written, cancellation)
                    .await?,
            ),
            None => None,
        };
        edits.push((Key::Revision(time), Some(Item::Revision(pointer))));
    }
    let revisions = tree.edit(roots.archived.clone(), edits).await?;
    let revisions = match revisions {
        Some(root) => {
            // This directory has a different lifetime from pages held in roots.changes.
            let mut directory = BTreeMap::new();
            Some(
                tree.persist(&root, block, &mut directory, cancellation)
                    .await?,
            )
        }
        None => None,
    };
    Ok(revisions.map(Link::Stored))
}

pub(super) async fn publish(
    tree: &Tree<'_>,
    through: i64,
    source_rows: i64,
    archive: Root,
    cancellation: &CancellationToken,
) -> Result<Checkpoint> {
    let revisions = match archive {
        Some(Link::Stored(pointer)) if pointer.exists_by(through) => Some(pointer),
        Some(Link::Stored(_)) => {
            return Err(format_error(
                "Ordered index checkpoint refers to a later block",
            ));
        }
        Some(Link::New(_)) => {
            return Err(format_error(
                "An ordered index checkpoint references unpublished pages",
            ));
        }
        None => None,
    };
    let checkpoint = Checkpoint {
        through,
        scope: tree.scope,
        source_rows,
        revisions,
        manifest_bytes: 0,
        _allocation: None,
    };
    let encoded = serde_json::to_vec(&checkpoint).map_err(format_error)?;
    let checksum = format!("{:x}", Sha256::digest(&encoded));
    let encoded = serde_json::to_vec(&Manifest {
        checksum,
        layout: checkpoint,
    })
    .map_err(format_error)?;
    if cancellation.is_cancelled() {
        return Err(cancelled());
    }
    let manifest = key(tree.directory, format_args!("{MANIFESTS}/{through:016x}"))?;
    tree.storage
        .object_store()
        .put(&manifest, encoded.into())
        .await?;
    latest(tree.storage, tree.directory, tree.memory, through)
        .await?
        .ok_or_else(|| format_error("Published checkpoint is unavailable"))
}

#[cfg(test)]
mod tests {
    use rtdb_columnar::MemoryBudget;
    use rtdb_storage::{MemoryStore, cache::BufferPool};

    use super::*;

    #[tokio::test]
    async fn advancing_reopens_collection_abandoned_after_manifest_publication() {
        let storage = BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20);
        let memory = MemoryBudget::new(1 << 20);
        let directory = ObjectKey::new("index").unwrap();
        let tree = Tree::new(&storage, &directory, &memory, 0).await.unwrap();
        let token = CancellationToken::new();
        let store = storage.object_store();
        let erased = key(&tree.pages().unwrap(), "0000000000000000/erased").unwrap();
        let later = key(&tree.pages().unwrap(), "0000000000000003/later").unwrap();
        for page in [&erased, &later] {
            store.put(page, Vec::<u8>::new().into()).await.unwrap();
        }

        request_collection(&tree, 1).await.unwrap();
        publish(&tree, 1, 2, None, &token).await.unwrap();

        let reopened = latest(&storage, &directory, &memory, 1).await.unwrap().unwrap();
        assert_eq!(reopened.through, 1);
        let advanced = publish(&tree, 2, 3, None, &token).await.unwrap();
        advanced.finish(&tree, &token).await.unwrap();

        assert!(matches!(
            store.head(&erased).await,
            Err(Error::MissingObject(_))
        ));
        assert!(store.head(&later).await.is_ok());
        assert!(advanced.pending_collection(&tree).await.unwrap().is_empty());
        assert_eq!(latest(&storage, &directory, &memory, 2).await.unwrap().unwrap().through, 2);
    }

    #[tokio::test]
    async fn sweeping_a_scope_preserves_pages_staged_in_a_later_scope() {
        let storage = BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20);
        let memory = MemoryBudget::new(1 << 20);
        let directory = ObjectKey::new("index").unwrap();
        let tree = Tree::new(&storage, &directory, &memory, 2).await.unwrap();
        let token = CancellationToken::new();
        let store = storage.object_store();
        let older = key(&directory, "generations/0000000000000000/pages/0000000000000000/page").unwrap();
        let newer = key(&directory, "generations/0000000000000003/pages/0000000000000000/page").unwrap();
        for page in [&older, &newer] {
            store.put(page, Vec::<u8>::new().into()).await.unwrap();
        }

        request_collection(&tree, 1).await.unwrap();
        let checkpoint = publish(&tree, 1, 2, None, &token).await.unwrap();
        checkpoint.finish(&tree, &token).await.unwrap();

        assert!(matches!(store.head(&older).await, Err(Error::MissingObject(_))));
        assert!(store.head(&newer).await.is_ok());
    }
}
