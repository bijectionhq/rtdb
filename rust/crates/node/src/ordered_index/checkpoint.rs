use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int64Array, NullArray, RecordBatch, UInt64Array,
    builder::FixedSizeBinaryBuilder,
};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_storage::{Error, ObjectKey, cache::BufferPool};
use rtdb_types::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    format_error, ipc,
    key::arrow,
    maintenance::cancelled,
    projection::RevisionRoots,
    references,
    tree::{Item, Key, Link, Pointer, ReachablePages, Root, Tree},
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
        tree: &Tree,
        cancellation: &CancellationToken,
    ) -> Result<ReachablePages> {
        let mut pages = ReachablePages::default();
        if let Some(pointer) = &self.revisions {
            tree.mark(Arc::clone(pointer), &mut pages, cancellation)
                .await?;
        }
        Ok(pages)
    }

    pub async fn file_bytes(&self, tree: &Tree, cancellation: &CancellationToken) -> Result<u64> {
        self.reachable(tree, cancellation)
            .await?
            .bytes
            .checked_add(self.manifest_bytes)
            .ok_or_else(|| format_error("Ordered index file bytes overflow"))
    }

    pub async fn finish(&self, tree: &Tree, cancellation: &CancellationToken) -> Result<()> {
        let pending = self.pending_collection(tree).await?;
        if pending.is_empty() {
            self.retire_manifests(tree, cancellation).await
        } else {
            self.sweep(tree, pending, cancellation).await
        }
    }

    pub async fn collect(&self, tree: &Tree, cancellation: &CancellationToken) -> Result<()> {
        let pending = self.pending_collection(tree).await?;
        self.sweep(tree, pending, cancellation).await
    }

    async fn pending_collection(&self, tree: &Tree) -> Result<Vec<ObjectKey>> {
        let root = key(&tree.directory, "cleanup")?;
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
        tree: &Tree,
        pending: Vec<ObjectKey>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let reachable = self.reachable(tree, cancellation).await?;
        let store = tree.storage.object_store();
        let prefix = tree.pages()?;
        let generations = key(&tree.directory, "generations")?;
        let objects = store.list(Some(&generations)).await?;
        let present: BTreeSet<_> = objects.iter().map(|object| &object.key).collect();
        if !reachable.keys.iter().all(|key| present.contains(key)) {
            return Err(format_error(
                "An ordered index checkpoint references a missing page",
            ));
        }
        self.retire_manifests(tree, cancellation).await?;
        let managed = store.index_page_lifecycle();
        if let Some(lifecycle) = managed {
            lifecycle.retire_before(tree.page_scope()).await?;
        } else {
            for object in objects {
                if cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                let Some((scope, _)) = object
                    .key
                    .as_str()
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
                if scope < self.scope
                    || (block.is_some_and(|block| block <= self.through)
                        && !reachable.keys.contains(&object.key))
                {
                    store.delete(&object.key).await?;
                    tree.storage.invalidate(&object.key).await?;
                }
            }
            let legacy = key(&tree.directory, "pages")?;
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

    async fn retire_manifests(&self, tree: &Tree, cancellation: &CancellationToken) -> Result<()> {
        let store = tree.storage.object_store();
        let published = key(&tree.directory, MANIFESTS)?;
        for object in store
            .list(Some(&key(&tree.directory, "manifests")?))
            .await?
        {
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
pub(super) async fn request_collection(tree: &Tree, through: i64) -> Result<()> {
    let debt = key(&tree.directory, format_args!("cleanup/{through:016x}"))?;
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
    let path = object
        .as_str()
        .strip_prefix(pages.as_str())?
        .strip_prefix('/')?;
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
    let mut checkpoint = if bytes.starts_with(&ipc::CONTINUATION) {
        decode_manifest(ipc::decode(bytes, 1)?, &allocation)?
    } else {
        let Manifest { checksum, layout } = serde_json::from_slice(&bytes).map_err(format_error)?;
        let encoded = serde_json::to_vec(&layout).map_err(format_error)?;
        if checksum != format!("{:x}", Sha256::digest(&encoded)) {
            return Err(format_error("Invalid ordered index checkpoint checksum"));
        }
        layout
    };
    if frontier(&object.key) != Some(checkpoint.through)
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
    tree: &Tree,
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
    tree: &Tree,
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
    let _scratch = tree.arrow_references.then(|| tree.memory.reserve(32 << 10)).transpose()?;
    let encoded = if tree.arrow_references {
        ipc::encode(&encode_manifest(&checkpoint)?)?
    } else {
        let encoded = serde_json::to_vec(&checkpoint).map_err(format_error)?;
        let checksum = format!("{:x}", Sha256::digest(&encoded));
        serde_json::to_vec(&Manifest { checksum, layout: checkpoint }).map_err(format_error)?
    };
    if cancellation.is_cancelled() {
        return Err(cancelled());
    }
    let manifest = key(&tree.directory, format_args!("{MANIFESTS}/{through:016x}"))?;
    tree.storage
        .object_store()
        .put(&manifest, encoded.into())
        .await?;
    latest(&tree.storage, &tree.directory, &tree.memory, through)
        .await?
        .ok_or_else(|| format_error("Published checkpoint is unavailable"))
}

fn manifest_fields(revisions: DataType) -> [Field; 5] {
    [
        Field::new("through", DataType::Int64, false),
        Field::new("scope", DataType::UInt64, false),
        Field::new("source_rows", DataType::Int64, false),
        Field::new("checksum", DataType::FixedSizeBinary(32), false),
        Field::new("revisions", revisions, true),
    ]
}

fn manifest_checksum(checkpoint: &Checkpoint) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    digest.update(b"rtdb-ordered-index-checkpoint");
    digest.update(checkpoint.through.to_le_bytes());
    digest.update(checkpoint.scope.to_le_bytes());
    digest.update(checkpoint.source_rows.to_le_bytes());
    if let Some(root) = &checkpoint.revisions {
        let (Key::Revision(first), Key::Revision(last)) = (&root.first, &root.last) else {
            return Err(format_error(
                "The checkpoint root must be a revision directory",
            ));
        };
        digest.update([1]);
        digest.update(root.digest.as_bytes());
        digest.update(root.generation.to_le_bytes());
        digest.update((root.size as u64).to_le_bytes());
        digest.update(root.entries.to_le_bytes());
        digest.update(root.measure.count.to_le_bytes());
        digest.update(references::measure_domain_bytes(root.measure.domain));
        digest.update(root.measure.sum);
        for revision in [first, last] {
            digest.update(revision.time.0.to_le_bytes());
            digest.update(revision.transaction.to_le_bytes());
        }
    } else {
        digest.update([0]);
    }
    Ok(digest.finalize().into())
}

fn encode_manifest(checkpoint: &Checkpoint) -> Result<RecordBatch> {
    let mut checksum = FixedSizeBinaryBuilder::with_capacity(1, 32);
    checksum
        .append_value(manifest_checksum(checkpoint)?)
        .map_err(format_error)?;
    let revisions = match checkpoint.revisions.as_deref() {
        Some(pointer) => ipc::list(references::encode(&[(None, Some(pointer))])?)?,
        None => Arc::new(NullArray::new(1)) as ArrayRef,
    };
    let schema = Schema::new(manifest_fields(revisions.data_type().clone()).to_vec());
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![checkpoint.through])),
            Arc::new(UInt64Array::from(vec![checkpoint.scope])),
            Arc::new(Int64Array::from(vec![checkpoint.source_rows])),
            Arc::new(checksum.finish()),
            revisions,
        ],
    )
    .map_err(format_error)
}

fn decode_manifest(batch: RecordBatch, memory: &Arc<MemoryReservation>) -> Result<Checkpoint> {
    let invalid = || format_error("Invalid ordered index checkpoint");
    let Some(revisions) = batch.columns().get(4) else {
        return Err(invalid());
    };
    let fields = manifest_fields(revisions.data_type().clone());
    if batch.num_rows() != 1
        || batch.num_columns() != fields.len()
        || batch
            .schema_ref()
            .fields()
            .iter()
            .zip(fields)
            .any(|(actual, expected)| actual.as_ref() != &expected)
        || batch.columns()[..4]
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid());
    }
    let root = if revisions.data_type() == &DataType::Null {
        None
    } else {
        let mut roots = references::decode(ipc::unlist(revisions.as_ref())?, 1, memory)?;
        let Some((None, Some(root))) = roots.pop() else {
            return Err(invalid());
        };
        Some(root)
    };
    let checkpoint = Checkpoint {
        through: arrow::<Int64Array>(batch.column(0).as_ref())?.value(0),
        scope: arrow::<UInt64Array>(batch.column(1).as_ref())?.value(0),
        source_rows: arrow::<Int64Array>(batch.column(2).as_ref())?.value(0),
        revisions: root,
        manifest_bytes: 0,
        _allocation: None,
    };
    let checksum = arrow::<FixedSizeBinaryArray>(batch.column(3).as_ref())?.value(0);
    if checksum != manifest_checksum(&checkpoint)? {
        return Err(invalid());
    }
    Ok(checkpoint)
}

#[cfg(test)]
mod tests {
    use rtdb_columnar::MemoryBudget;
    use rtdb_storage::{MemoryStore, cache::BufferPool};

    use super::*;

    #[tokio::test]
    async fn arrow_checkpoint_reopens_the_json_subtrees_it_keeps() -> Result<()> {
        use rtdb_types::Timestamp;
        use crate::ordered_index::tree::{IndexRevision, KeyRef};

        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let memory = MemoryBudget::new(32 << 20);
        let directory = ObjectKey::new("index")?;
        let mut tree = Tree::new(&storage, &directory, &memory, 0).await?;
        let token = CancellationToken::new();
        let roots = RevisionRoots {
            archived: None,
            changes: (0..128).map(|instant| (IndexRevision { time: Timestamp(10), transaction: instant }, None)).collect(),
        };
        let root = store_roots(&tree, 0, &roots, &token).await?;
        publish(&tree, 0, 128, root, &token).await?;

        tree.arrow_references = true;
        let legacy = latest(&storage, &directory, &memory, 0).await?.unwrap();
        let mut roots = legacy.roots();
        roots.changes.insert(IndexRevision { time: Timestamp(10), transaction: 128 }, None);
        let root = store_roots(&tree, 1, &roots, &token).await?;
        publish(&tree, 1, 129, root, &token).await?;

        let key = key(&directory, format_args!("{MANIFESTS}/{:016x}", 1))?;
        assert!(storage.object_store().get_fresh(&key).await?.starts_with(&ipc::CONTINUATION));
        let reopened = latest(&storage, &directory, &memory, 1).await?.unwrap();
        let reader = Tree::new(&storage, &directory, &memory, reopened.scope).await?;
        let root = reopened.roots().archived;
        let mut cursor = crate::ordered_index::tree::Cursor::new(root, false);
        let mut observed = Vec::new();
        while let Some(hit) = cursor.next(&reader, |_| std::cmp::Ordering::Equal).await? {
            let KeyRef::Revision(time) = hit.key() else {
                panic!("Expected a revision at every key");
            };
            assert_eq!(time.time, Timestamp(10));
            observed.push(time.transaction);
        }
        assert_eq!(observed, (0..129).collect::<Vec<_>>());
        Ok(())
    }

    #[tokio::test]
    async fn advancing_reopens_collection_abandoned_after_manifest_publication() {
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
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

        let reopened = latest(&storage, &directory, &memory, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reopened.through, 1);
        let advanced = publish(&tree, 2, 3, None, &token).await.unwrap();
        advanced.finish(&tree, &token).await.unwrap();

        assert!(matches!(
            store.head(&erased).await,
            Err(Error::MissingObject(_))
        ));
        assert!(store.head(&later).await.is_ok());
        assert!(advanced.pending_collection(&tree).await.unwrap().is_empty());
        assert_eq!(
            latest(&storage, &directory, &memory, 2)
                .await
                .unwrap()
                .unwrap()
                .through,
            2
        );
    }

    #[tokio::test]
    async fn sweeping_a_scope_preserves_pages_staged_in_a_later_scope() {
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let memory = MemoryBudget::new(1 << 20);
        let directory = ObjectKey::new("index").unwrap();
        let tree = Tree::new(&storage, &directory, &memory, 2).await.unwrap();
        let token = CancellationToken::new();
        let store = storage.object_store();
        let older = key(
            &directory,
            "generations/0000000000000000/pages/0000000000000000/page",
        )
        .unwrap();
        let newer = key(
            &directory,
            "generations/0000000000000003/pages/0000000000000000/page",
        )
        .unwrap();
        for page in [&older, &newer] {
            store.put(page, Vec::<u8>::new().into()).await.unwrap();
        }

        request_collection(&tree, 1).await.unwrap();
        let checkpoint = publish(&tree, 1, 2, None, &token).await.unwrap();
        checkpoint.finish(&tree, &token).await.unwrap();

        assert!(matches!(
            store.head(&older).await,
            Err(Error::MissingObject(_))
        ));
        assert!(store.head(&newer).await.is_ok());
    }
}
