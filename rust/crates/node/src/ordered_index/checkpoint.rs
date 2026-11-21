use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use rtdb_columnar::MemoryReservation;
use rtdb_storage::{Error, ObjectKey};
use rtdb_types::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    format_error,
    maintenance::{cancelled, tip_key},
    projection::RevisionRoots,
    tree::{Item, Key, Link, Pointer, Root, Tree},
};

#[derive(Serialize, Deserialize)]
pub(super) struct Checkpoint {
    pub through: i64,
    pub source_rows: i64,
    revisions: Option<Arc<Pointer>>,
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

    pub async fn collect(&self, tree: &Tree<'_>, cancellation: &CancellationToken) -> Result<()> {
        let mut reachable = BTreeSet::new();
        if let Some(pointer) = &self.revisions {
            tree.mark(Arc::clone(pointer), &mut reachable).await?;
        }
        let store = tree.storage.object_store();
        let prefix = key(tree, "pages")?;
        let objects = store.list(Some(&prefix)).await?;
        let present: BTreeSet<_> = objects.iter().map(|object| &object.key).collect();
        if !reachable.iter().all(|key| present.contains(key)) {
            return Err(format_error(
                "An ordered index checkpoint references a missing page",
            ));
        }
        tree.storage.invalidate(&tip_key(tree.directory)?).await?;
        self.retire_manifests(tree, cancellation).await?;
        for object in objects {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let generation = object
                .key
                .as_str()
                .strip_prefix(prefix.as_str())
                .and_then(|suffix| suffix.trim_start_matches('/').split_once('/'))
                .and_then(|(generation, _)| i64::from_str_radix(generation, 16).ok());
            if generation.is_some_and(|generation| generation <= self.through)
                && !reachable.contains(&object.key)
            {
                store.delete(&object.key).await?;
                tree.storage.invalidate(&object.key).await?;
            }
        }
        Ok(())
    }

    pub async fn retire_manifests(
        &self,
        tree: &Tree<'_>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let store = tree.storage.object_store();
        for object in store.list(Some(&key(tree, "manifests")?)).await? {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            if frontier(&object.key).is_some_and(|block| block < self.through) {
                store.delete(&object.key).await?;
                tree.storage.invalidate(&object.key).await?;
            }
        }
        Ok(())
    }
}

fn key(tree: &Tree<'_>, suffix: impl std::fmt::Display) -> Result<ObjectKey> {
    Ok(ObjectKey::new(format!("{}/{suffix}", tree.directory))?)
}

fn frontier(key: &ObjectKey) -> Option<i64> {
    i64::from_str_radix(key.as_str().rsplit('/').next()?, 16).ok()
}

pub(super) async fn latest(tree: &Tree<'_>, through: i64) -> Result<Option<Checkpoint>> {
    if through < 0 {
        return Ok(None);
    }
    let store = tree.storage.object_store();
    let head = key(tree, format_args!("manifests/{through:016x}"))?;
    let object = match store.head(&head).await {
        Ok(object) => Some(object),
        Err(Error::MissingObject(_)) => store
            .list(Some(&key(tree, "manifests")?))
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
    let allocation = tree
        .memory
        .reserve(size.saturating_mul(8).saturating_add(4096))?;
    let bytes = store.get(&object.key).await?;
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
        || checkpoint.source_rows < 0
        || checkpoint
            .revisions
            .as_ref()
            .is_some_and(|root| !root.exists_by(checkpoint.through))
    {
        return Err(format_error("Invalid ordered index checkpoint"));
    }
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
        source_rows,
        revisions,
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
    tree.storage
        .object_store()
        .put(
            &key(tree, format_args!("manifests/{through:016x}"))?,
            encoded.into(),
        )
        .await?;
    latest(tree, through)
        .await?
        .ok_or_else(|| format_error("Published checkpoint is unavailable"))
}
