use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int64Array, NullArray, RecordBatch, UInt64Array,
    builder::FixedSizeBinaryBuilder,
};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_storage::{Error, IoRegion, ObjectKey, cache::BufferPool};
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
}

#[derive(Deserialize)]
struct Manifest<'a> {
    checksum: String,
    #[serde(borrow)]
    layout: &'a serde_json::value::RawValue,
}

fn encode_legacy_manifest(checkpoint: &Checkpoint) -> Result<Vec<u8>> {
    const PREFIX: &[u8] = br#"{"checksum":""#;
    const SEPARATOR: &[u8] = br#"","layout":"#;
    const CHECKSUM_BYTES: usize = 64;
    let end_checksum = PREFIX.len() + CHECKSUM_BYTES;
    let mut bytes = PREFIX.to_vec();
    bytes.resize(end_checksum, b'0');
    bytes.extend_from_slice(SEPARATOR);
    let layout = bytes.len();
    serde_json::to_writer(&mut bytes, checkpoint).map_err(format_error)?;
    let checksum = format!("{:x}", Sha256::digest(&bytes[layout..]));
    bytes[PREFIX.len()..end_checksum].copy_from_slice(checksum.as_bytes());
    bytes.push(b'}');
    Ok(bytes)
}

fn decode_legacy_manifest(bytes: &[u8]) -> Result<Checkpoint> {
    let Manifest { checksum, layout } = serde_json::from_slice(bytes).map_err(format_error)?;
    let checkpoint = match serde_json::from_str(layout.get()) {
        Ok(checkpoint) => checkpoint,
        Err(_) => {
            #[derive(Deserialize)]
            struct ParsedManifest {
                #[serde(rename = "checksum")]
                _checksum: String,
                layout: Checkpoint,
            }
            serde_json::from_slice::<ParsedManifest>(bytes).map_err(format_error)?.layout
        }
    };
    if checksum != format!("{:x}", Sha256::digest(layout.get().as_bytes())) {
        // Legacy verification also accepted whitespace or field-order changes to the envelope's layout.
        struct HashWriter(Sha256);
        impl std::io::Write for HashWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let mut digest = HashWriter(Sha256::new());
        serde_json::to_writer(&mut digest, &checkpoint).map_err(format_error)?;
        if checksum != format!("{:x}", digest.0.finalize()) {
            return Err(format_error("Invalid ordered index checkpoint checksum"));
        }
    }
    Ok(checkpoint)
}

impl Checkpoint {
    fn validate(&self, object: &ObjectKey) -> Result<()> {
        if frontier(object) != Some(self.through)
            || self.through < 0
            || self.scope > self.through as u64 + 1
            || self.source_rows < 0
            || self.revisions.as_ref().is_some_and(|root| !root.exists_by(self.through))
        {
            return Err(format_error("Invalid ordered index checkpoint"));
        }
        Ok(())
    }

    pub fn roots(&self) -> RevisionRoots {
        RevisionRoots {
            archived: self.revisions.clone().map(Link::Stored),
            changes: BTreeMap::new(),
            allocation: None,
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
    let observed = match store.observe_head(&head).await {
        Ok(object) => object,
        Err(Error::MissingObject(_)) => {
            let listed = store
                .list(Some(&key(directory, MANIFESTS)?))
                .await?
                .into_iter()
                .filter(|object| frontier(&object.key).is_some_and(|block| block <= through))
                .max_by_key(|object| frontier(&object.key));
            let Some(listed) = listed else { return Ok(None); };
            let observed = store.observe_head(&listed.key).await?;
            if observed.value != listed {
                return Err(format_error("Ordered index manifest length changed"));
            }
            observed
        }
        Err(error) => return Err(error.into()),
    };
    let object = observed.value;
    let size = usize::try_from(object.size).map_err(format_error)?;
    let decode_bound = size.saturating_mul(8).saturating_add(4096);
    let input = store.exact_object_allocation(&object.key, size)?;
    let read_bound = input.retained_bytes.saturating_add(input.scratch_bytes);
    let allocation = memory.reserve(decode_bound.max(read_bound))?;
    let read = IoRegion::retaining(
        Arc::clone(&allocation),
        store.observe_fresh_exact(&object.key, size),
    )
    .await?;
    if read.value.len() != size {
        return Err(format_error("Ordered index manifest length changed"));
    }
    if observed.version.is_some() && observed.version != read.version {
        return Err(format_error("Ordered index manifest version changed"));
    }
    let mut opening = Arc::try_unwrap(allocation)
        .map_err(|_| format_error("Ordered index manifest read still owns its opening grant"))?;
    let mut checkpoint = if read.value.starts_with(&ipc::CONTINUATION) {
        let backing = Buffer::from(read.value);
        decode_manifest(ipc::decode_buffer(backing.clone(), 1)?, &backing, &mut opening)?
    } else if store.ordered_index_arrow_only() {
        return Err(format_error("The installed ordered index checkpoint requires Arrow IPC"));
    } else {
        let mut checkpoint = decode_legacy_manifest(&read.value)?;
        if let Some(root) = &mut checkpoint.revisions {
            let root = Arc::get_mut(root).expect("decoded manifest root is unpublished");
            root._memory = Some(opening.split_off(root.owned_backing_bytes())?);
        }
        checkpoint
    };
    checkpoint.validate(&object.key)?;
    checkpoint.manifest_bytes = object.size;
    Ok(Some(checkpoint))
}

pub(super) async fn store_roots(
    tree: &Tree,
    block: i64,
    roots: &RevisionRoots,
    cancellation: &CancellationToken,
) -> Result<Root> {
    let _edits = tree.memory.reserve(roots.changes.len().saturating_mul(size_of::<(Key, Option<Item>)>()))?;
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
    let mut checkpoint = Checkpoint {
        through,
        scope: tree.scope,
        source_rows,
        revisions,
        manifest_bytes: 0,
    };
    let arrow = tree.writes_arrow_references();
    let scratch = tree.memory.reserve(if arrow { 32 << 10 } else { 4096 })?;
    let encoded = if arrow {
        ipc::encode(&encode_manifest(&checkpoint)?)?
    } else {
        encode_legacy_manifest(&checkpoint)?
    };
    if cancellation.is_cancelled() {
        return Err(cancelled());
    }
    let manifest = key(&tree.directory, format_args!("{MANIFESTS}/{through:016x}"))?;
    checkpoint.manifest_bytes = encoded.len() as u64;
    let store = tree.storage.object_store();
    let publication = IoRegion::retaining(scratch, store.put_with_publication(&manifest, encoded.into())).await?;
    if publication.is_some() {
        checkpoint.validate(&manifest)?;
        return Ok(checkpoint);
    }
    let reopened = latest(&tree.storage, &tree.directory, &tree.memory, through)
        .await?
        .filter(|reopened| reopened.through == through)
        .ok_or_else(|| format_error("Published checkpoint is unavailable"))?;
    Ok(reopened)
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

fn decode_manifest(batch: RecordBatch, backing: &Buffer, opening: &mut MemoryReservation) -> Result<Checkpoint> {
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
        let mut roots = references::decode_page(ipc::unlist(revisions.as_ref())?, 1, backing, opening)?;
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
    };
    let checksum = arrow::<FixedSizeBinaryArray>(batch.column(3).as_ref())?.value(0);
    if checksum != manifest_checksum(&checkpoint)? {
        return Err(invalid());
    }
    Ok(checkpoint)
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, path::Path};

    use arrow_array::StringArray;
    use bytes::Bytes;
    use rtdb_columnar::MemoryBudget;
    use rtdb_storage::{MemoryStore, ObjectStore, StoredObject, cache::BufferPool};
    use rtdb_types::{Iid, Interval, TableRef, Timestamp};

    use crate::ordered_index::{
        Definition,
        key::{Atom, Bucket},
        postings::KeyRow,
        tree::{Address, Cursor, IndexRevision, KeyRef, Span, Spans},
    };

    use super::*;

    struct InstalledStore(Arc<MemoryStore>);

    #[async_trait::async_trait]
    impl ObjectStore for InstalledStore {
        fn ordered_index_arrow_only(&self) -> bool {
            true
        }
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
            self.0.head(key).await
        }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
            self.0.get(key).await
        }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
            self.0.get_range(key, range).await
        }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
            self.0.put(key, bytes).await
        }
        async fn put_file(&self, key: &ObjectKey, path: &Path) -> rtdb_storage::Result<()> {
            self.0.put_file(key, path).await
        }
        async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
            self.0.list(directory).await
        }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
            self.0.delete(key).await
        }
    }

    #[test]
    fn legacy_manifest_accepts_published_bytes_and_preserves_the_checksum() {
        let layout = r#"{"through":7,"scope":0,"source_rows":9,"revisions":null}"#;
        let checksum = format!("{:x}", Sha256::digest(layout));
        let manifest = format!(r#"{{"checksum":"{checksum}","layout":{layout}}}"#);
        let decoded = decode_legacy_manifest(manifest.as_bytes()).unwrap();
        assert_eq!(decoded.through, 7);
        assert_eq!(decoded.source_rows, 9);
        assert_eq!(encode_legacy_manifest(&decoded).unwrap(), manifest.as_bytes());
        let spaced = manifest.replace(r#""scope":0"#, r#""scope": 0"#);
        assert_eq!(decode_legacy_manifest(spaced.as_bytes()).unwrap().scope, 0);
        let corrupt = manifest.replace(r#""scope":0"#, r#""scope":1"#);
        assert_eq!(decode_legacy_manifest(corrupt.as_bytes()).err().unwrap().message.as_deref(),
            Some("Invalid ordered index checkpoint checksum"));
    }

    #[tokio::test]
    async fn installed_checkpoint_refuses_shared_json_manifests_and_subtrees() -> Result<()> {
        let store = Arc::new(MemoryStore::default());
        let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let installed = Arc::new(BufferPool::new(Arc::new(InstalledStore(store.clone())), 1 << 20));
        let memory = MemoryBudget::new(32 << 20);
        let directory = ObjectKey::new("index")?;
        let mut tree = Tree::new(&storage, &directory, &memory, 0).await?;
        let token = CancellationToken::new();
        let roots = RevisionRoots {
            archived: None,
            changes: (0..128).map(|instant| (IndexRevision { time: Timestamp(10), transaction: instant }, None)).collect(),
            allocation: None,
        };
        let root = store_roots(&tree, 0, &roots, &token).await?;
        publish(&tree, 0, 128, root, &token).await?;

        tree.arrow_references = true;
        let legacy = latest(&storage, &directory, &memory, 0).await?.unwrap();
        let mut roots = legacy.roots();
        roots.changes.insert(IndexRevision { time: Timestamp(10), transaction: 128 }, None);
        let root = store_roots(&tree, 1, &roots, &token).await?;
        publish(&tree, 1, 129, root, &token).await?;

        let manifest = key(&directory, format_args!("{MANIFESTS}/{:016x}", 1))?;
        assert!(store.get(&manifest).await?.starts_with(&ipc::CONTINUATION));
        let reopened = latest(&storage, &directory, &memory, 1).await?.unwrap();
        let reader = Tree::new(&storage, &directory, &memory, reopened.scope).await?;
        let root = reopened.roots().archived;
        let mut cursor = Cursor::new(root, false);
        let mut observed = Vec::new();
        while let Some(hit) = cursor.next(&reader, |_| std::cmp::Ordering::Equal).await? {
            let KeyRef::Revision(time) = hit.key() else {
                panic!("Expected a revision at every key");
            };
            assert_eq!(time.time, Timestamp(10));
            observed.push(time.transaction);
        }
        assert_eq!(observed, (0..129).collect::<Vec<_>>());

        tree.arrow_references = false;
        publish(&tree, 2, 129, None, &token).await?;
        let error = latest(&installed, &directory, &memory, 2).await.err().unwrap();
        assert_eq!(error.message.as_deref(), Some("The installed ordered index checkpoint requires Arrow IPC"));
        let unfunded = MemoryBudget::new(1);
        let error = latest(&installed, &directory, &unfunded, 2).await.err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");

        let mixed = latest(&installed, &directory, &memory, 1).await?.unwrap();
        let reader = Tree::new(&installed, &directory, &memory, mixed.scope).await?;
        let mut cursor = Cursor::new(mixed.roots().archived, false);
        let error = cursor.next(&reader, |_| std::cmp::Ordering::Equal).await.err().unwrap();
        assert_eq!(error.message.as_deref(), Some("An installed ordered index page requires Arrow IPC"));

        let pointer = legacy.revisions.as_ref().unwrap();
        let page = key(&reader.pages()?, format_args!("{:016x}/{}", pointer.generation, pointer.digest))?;
        store.delete(&page).await?;
        store.put(&page, vec![0; pointer.size].into()).await?;
        let mut cursor = Cursor::new(legacy.roots().archived, false);
        let error = cursor.next(&reader, |_| std::cmp::Ordering::Equal).await.err().unwrap();
        assert_eq!(error.message.as_deref(), Some("Ordered index page checksum mismatch"));
        Ok(())
    }

    #[tokio::test]
    async fn installed_arrow_checkpoint_rebuilds_at_the_same_frontier_and_preserves_page_reads() -> Result<()> {
        let store = Arc::new(MemoryStore::default());
        let shared = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let installed = Arc::new(BufferPool::new(Arc::new(InstalledStore(store.clone())), 1 << 20));
        let memory = MemoryBudget::new(32 << 20);
        let token = CancellationToken::new();
        let definition = Definition {
            name: "by_key".into(),
            table: TableRef::new("public", "events"),
            columns: vec!["key".into()],
            version: 6,
        };
        let shared_directory = definition.directory(&shared)?;
        let installed_directory = definition.directory(&installed)?;
        assert_ne!(shared_directory, installed_directory);
        let address = Address::new(Iid([1; 16]), Bytes::new());
        let tuple = KeyRow::Fence(Arc::from([Atom::Text("key".into())]));
        let posting = Key::Posting {
            bucket: Bucket { level: 64, start: 0 },
            tuple: tuple.clone(),
            address: address.clone(),
        };
        let span = Span { valid: Interval::ALL, tuple };
        for (storage, directory, arrow_only) in [
            (&shared, &shared_directory, false),
            (&installed, &installed_directory, true),
        ] {
            assert!(latest(storage, directory, &memory, 7).await?.is_none());
            let tree = Tree::new(storage, directory, &memory, 0).await?;
            let root = tree.edit(
                None,
                vec![
                    (posting.clone(), Some(Item::Posting)),
                    (Key::Row(address.clone()), Some(Item::Row(Spans::new(
                        vec![span.clone()], memory.reserve(Spans::allocation_bytes(1))?,
                    )))),
                ],
            ).await?;
            let revisions = RevisionRoots {
                archived: None,
                changes: (0..129)
                    .map(|transaction| (IndexRevision { time: Timestamp(10), transaction }, root.clone()))
                    .collect(),
                allocation: None,
            };
            let root = store_roots(&tree, 7, &revisions, &token).await?;
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            let error = publish(&tree, 6, 129, root.clone(), &cancelled).await.err().unwrap();
            assert_eq!(error.message.as_deref(), Some("Ordered index checkpoint refers to a later block"));
            publish(&tree, 7, 129, root.clone(), &token).await?;
            publish(&tree, 7, 129, root, &token).await?;
            let manifest = key(directory, format_args!("{MANIFESTS}/{:016x}", 7))?;
            assert_eq!(store.get(&manifest).await?.starts_with(&ipc::CONTINUATION), arrow_only);

            let reopened = latest(storage, directory, &memory, 8).await?.unwrap();
            assert_eq!((reopened.through, reopened.source_rows), (7, 129));
            let reader = Tree::new(storage, directory, &memory, reopened.scope).await?;
            assert!(reopened.file_bytes(&reader, &token).await? > reopened.manifest_bytes);
            let wanted = IndexRevision { time: Timestamp(10), transaction: 64 };
            let (revision, root) = reopened.roots().at(&reader, wanted).await?.unwrap();
            assert_eq!(revision, wanted);
            let mut cursor = Cursor::new(root, false);
            let hit = cursor.next(&reader, |_| std::cmp::Ordering::Equal).await?.unwrap();
            let (column, row, _) = hit.plain_column(0, 1).unwrap();
            assert_eq!(arrow::<StringArray>(column.as_ref())?.value(row), "key");
            assert_eq!(hit.into_entry().key, posting);
            let correction = cursor.next(&reader, |_| std::cmp::Ordering::Equal).await?.unwrap().into_entry();
            assert_eq!(correction.key, Key::Row(address.clone()));
            let Item::Row(spans) = correction.item else { panic!("Expected correction spans"); };
            assert_eq!(spans.as_ref(), std::slice::from_ref(&span));
        }
        Ok(())
    }

    #[tokio::test]
    async fn reopened_root_retains_only_its_pointer_after_checkpoint_drop() -> Result<()> {
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let directory = ObjectKey::new("index")?;
        let writer_memory = MemoryBudget::new(8 << 20);
        let tree = Tree::new(&storage, &directory, &writer_memory, 0).await?;
        let token = CancellationToken::new();
        let roots = RevisionRoots {
            archived: None,
            changes: BTreeMap::from([(IndexRevision::through_time(Timestamp(10)), None)]),
            allocation: None,
        };
        let root = store_roots(&tree, 0, &roots, &token).await?;
        publish(&tree, 0, 1, root, &token).await?;

        let reader_memory = MemoryBudget::new(8 << 20);
        let checkpoint = latest(&storage, &directory, &reader_memory, 0).await?.unwrap();
        let roots = checkpoint.roots();
        drop(checkpoint);
        let Some(Link::Stored(pointer)) = &roots.archived else { panic!("Missing archived root") };
        assert_eq!(reader_memory.used_bytes(), pointer.owned_backing_bytes());
        drop(roots);
        assert_eq!(reader_memory.used_bytes(), 0);
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
