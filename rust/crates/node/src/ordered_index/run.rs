use std::{
    cmp::{Ordering, Reverse},
    sync::Arc,
};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_storage::{ObjectKey, ObjectStore, StoredObject};
use rtdb_types::{Iid, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{format_error, key::Atom};

const PAGE_ROWS: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Entry {
    pub key: Vec<Atom>,
    pub iid: Iid,
    pub system_from: i64,
    pub source_row: u64,
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            &self.key,
            self.iid,
            Reverse(self.system_from),
            self.source_row,
        )
            .cmp(&(
                &other.key,
                other.iid,
                Reverse(other.system_from),
                other.source_row,
            ))
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Entry {
    fn same_key(&self, other: &Self) -> bool {
        self.iid == other.iid && self.key == other.key
    }

    pub fn bytes(&self) -> usize {
        size_of::<Self>().saturating_add(self.key.iter().fold(0usize, |total, key| {
            total
                .saturating_add(size_of::<Atom>())
                .saturating_add(key.retained_bytes())
        }))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Page {
    pub generation: i64,
    pub digest: String,
    pub size: u64,
    pub first: Vec<Atom>,
    pub last: Vec<Atom>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct Run {
    pub level: u32,
    pub pages: Vec<Page>,
    #[serde(skip)]
    _charges: Vec<Arc<MemoryReservation>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Manifest {
    pub through: i64,
    pub runs: Vec<Run>,
    #[serde(skip)]
    _charge: Option<Arc<MemoryReservation>>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            through: -1,
            runs: Vec::new(),
            _charge: None,
        }
    }
}

#[derive(Deserialize)]
struct PublishedManifest {
    digest: String,
    layout: Manifest,
}

fn key(directory: &ObjectKey, suffix: impl std::fmt::Display) -> Result<ObjectKey> {
    Ok(ObjectKey::new(format!("{directory}/{suffix}"))?)
}

fn page_key(directory: &ObjectKey, generation: i64, digest: &str) -> Result<ObjectKey> {
    key(directory, format_args!("pages/{generation:016x}/{digest}"))
}

pub(super) fn manifest_block(object: &ObjectKey) -> Option<i64> {
    let name = object.as_str().rsplit('/').next()?;
    (name.len() == 16)
        .then(|| i64::from_str_radix(name, 16).ok())
        .flatten()
}

pub(super) async fn manifest_objects(
    store: &dyn ObjectStore,
    directory: &ObjectKey,
) -> Result<Vec<StoredObject>> {
    Ok(store.list(Some(&key(directory, "manifests")?)).await?)
}

pub(super) async fn latest(
    store: &dyn ObjectStore,
    directory: &ObjectKey,
    through: i64,
    memory: &Arc<MemoryBudget>,
) -> Result<Manifest> {
    if through < 0 {
        return Ok(Manifest::default());
    }
    let frontier = key(directory, format_args!("manifests/{through:016x}"))?;
    let object = match store.head(&frontier).await {
        Ok(object) => Some(object),
        Err(rtdb_storage::Error::MissingObject(_)) => manifest_objects(store, directory)
            .await?
            .into_iter()
            .rev()
            .find(|object| manifest_block(&object.key).is_some_and(|block| block <= through)),
        Err(error) => return Err(error.into()),
    };
    let Some(object) = object else {
        return Ok(Manifest::default());
    };
    let charge = read_charge(memory, object.size)?;
    let bytes = store.get(&object.key).await?;
    if bytes.len() as u64 != object.size {
        return Err(format_error("Index manifest length changed"));
    }
    let PublishedManifest {
        digest,
        layout: mut manifest,
    } = serde_json::from_slice(&bytes).map_err(format_error)?;
    if format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&manifest).map_err(format_error)?)
    ) != digest
    {
        return Err(format_error("Index manifest checksum mismatch"));
    }
    manifest._charge = Some(charge);
    if Some(manifest.through) != manifest_block(&object.key) {
        return Err(format_error("Index manifest frontier changed"));
    }
    if manifest
        .runs
        .windows(2)
        .any(|pair| pair[0].level <= pair[1].level)
        || manifest.runs.iter().any(|run| {
            run.pages
                .iter()
                .any(|page| page.first > page.last || page.generation > manifest.through)
                || run
                    .pages
                    .windows(2)
                    .any(|pair| pair[0].last > pair[1].first)
        })
    {
        return Err(format_error(
            "Index manifest pages or runs are out of order",
        ));
    }
    Ok(manifest)
}

pub(super) async fn publish(
    store: &dyn ObjectStore,
    directory: &ObjectKey,
    manifest: &Manifest,
) -> Result<()> {
    #[derive(Serialize)]
    struct Published<'a> {
        digest: String,
        layout: &'a Manifest,
    }
    let key = key(
        directory,
        format_args!("manifests/{:016x}", manifest.through),
    )?;
    let layout = serde_json::to_vec(manifest).map_err(format_error)?;
    let checksum = format!("{:x}", Sha256::digest(layout));
    let encoded = serde_json::to_vec(&Published {
        digest: checksum,
        layout: manifest,
    })
    .map_err(format_error)?;
    Ok(store.put(&key, encoded.into()).await?)
}

fn read_charge(memory: &Arc<MemoryBudget>, size: u64) -> Result<Arc<MemoryReservation>> {
    let bytes = usize::try_from(size).map_err(format_error)?;
    memory.reserve(bytes.saturating_mul(8).saturating_add(4096))
}

async fn read_page(
    store: &dyn ObjectStore,
    directory: &ObjectKey,
    page: &Page,
    memory: &Arc<MemoryBudget>,
) -> Result<(Vec<Entry>, Arc<MemoryReservation>)> {
    let lease = read_charge(memory, page.size)?;
    let key = page_key(directory, page.generation, &page.digest)?;
    let bytes = store.get(&key).await?;
    if bytes.len() as u64 != page.size || format!("{:x}", Sha256::digest(&bytes)) != page.digest {
        return Err(format_error("Ordered index page checksum mismatch"));
    }
    let entries: Vec<Entry> = serde_json::from_slice(&bytes).map_err(format_error)?;
    if entries.first().map(|entry| &entry.key) != Some(&page.first)
        || entries.last().map(|entry| &entry.key) != Some(&page.last)
        || entries.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(format_error("Ordered index page is out of order"));
    }
    Ok((entries, lease))
}

pub(super) struct RunReader<'a> {
    pub run: &'a Run,
    pub start: usize,
    pub end: usize,
    pub reverse: bool,
    entries: std::collections::VecDeque<Entry>,
    reservation: Option<Arc<MemoryReservation>>,
}

impl<'a> RunReader<'a> {
    pub fn new(run: &'a Run, start: usize, end: usize, reverse: bool) -> Self {
        Self {
            run,
            start,
            end,
            reverse,
            entries: Default::default(),
            reservation: None,
        }
    }

    pub async fn next(
        &mut self,
        store: &dyn ObjectStore,
        directory: &ObjectKey,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Option<Entry>> {
        if let Some(entry) = self.entries.pop_front() {
            return Ok(Some(entry));
        }
        self.reservation = None;
        if self.start == self.end {
            return Ok(None);
        }
        let index = if self.reverse {
            self.end -= 1;
            self.end
        } else {
            let next = self.start;
            self.start += 1;
            next
        };
        let (mut entries, reservation) =
            read_page(store, directory, &self.run.pages[index], memory).await?;
        if self.reverse {
            entries.reverse();
        }
        self.entries = entries.into();
        self.reservation = Some(reservation);
        Ok(self.entries.pop_front())
    }
}

struct RunWriter<'a> {
    store: &'a dyn ObjectStore,
    directory: &'a ObjectKey,
    memory: &'a Arc<MemoryBudget>,
    run: Run,
    generation: i64,
    pending: Vec<Entry>,
    charges: Vec<Arc<MemoryReservation>>,
}

impl<'a> RunWriter<'a> {
    fn new(
        store: &'a dyn ObjectStore,
        directory: &'a ObjectKey,
        memory: &'a Arc<MemoryBudget>,
        level: u32,
        generation: i64,
    ) -> Self {
        Self {
            store,
            directory,
            memory,
            run: Run {
                level,
                ..Run::default()
            },
            generation,
            pending: Vec::new(),
            charges: Vec::new(),
        }
    }

    async fn push(&mut self, entry: Entry) -> Result<()> {
        if self
            .pending
            .last()
            .is_some_and(|last| last.same_key(&entry))
        {
            return Ok(());
        }
        if self.pending.len() == PAGE_ROWS {
            self.flush().await?;
        }
        self.charges
            .push(self.memory.reserve(entry.bytes().saturating_mul(4))?);
        self.pending.push(entry);
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let Some(first) = self.pending.first() else {
            return Ok(());
        };
        let last = self
            .pending
            .last()
            .expect("a page with a first entry also has a last");
        let bytes = serde_json::to_vec(&self.pending).map_err(format_error)?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let charge = self.memory.reserve(
            first
                .bytes()
                .saturating_add(last.bytes())
                .saturating_add(128),
        )?;
        let page = Page {
            generation: self.generation,
            digest,
            size: bytes.len() as u64,
            first: first.key.clone(),
            last: last.key.clone(),
        };
        self.store
            .put(
                &page_key(self.directory, page.generation, &page.digest)?,
                bytes.into(),
            )
            .await?;
        self.run.pages.push(page);
        self.run._charges.push(charge);
        self.pending.clear();
        self.charges.clear();
        Ok(())
    }

    async fn finish(mut self) -> Result<Run> {
        self.flush().await?;
        Ok(self.run)
    }
}

pub(super) async fn append(
    manifest: &mut Manifest,
    through: i64,
    entries: Vec<Entry>,
    erased: &super::maintenance::Erased,
    store: &dyn ObjectStore,
    directory: &ObjectKey,
    memory: &Arc<MemoryBudget>,
) -> Result<()> {
    if !erased.is_empty() {
        for input in &mut manifest.runs {
            let mut output = RunWriter::new(store, directory, memory, input.level, through);
            let mut reader = RunReader::new(input, 0, input.pages.len(), false);
            while let Some(entry) = reader.next(store, directory, memory).await? {
                if !super::maintenance::was_erased(&entry, erased) {
                    output.push(entry).await?;
                }
            }
            *input = output.finish().await?;
        }
    }
    let mut writer = RunWriter::new(store, directory, memory, 0, through);
    for entry in entries {
        if !super::maintenance::was_erased(&entry, erased) {
            writer.push(entry).await?;
        }
    }
    let mut carry = writer.finish().await?;
    while manifest
        .runs
        .last()
        .is_some_and(|previous| previous.level == carry.level)
    {
        let previous = manifest.runs.pop().expect("last run was checked");
        carry = merge(&previous, &carry, store, directory, memory, through).await?;
    }
    manifest.runs.push(carry);
    manifest.through = through;
    Ok(())
}

async fn merge(
    left: &Run,
    right: &Run,
    store: &dyn ObjectStore,
    directory: &ObjectKey,
    memory: &Arc<MemoryBudget>,
    generation: i64,
) -> Result<Run> {
    let mut writer = RunWriter::new(store, directory, memory, left.level + 1, generation);
    let (mut lhs, mut rhs) = (
        RunReader::new(left, 0, left.pages.len(), false),
        RunReader::new(right, 0, right.pages.len(), false),
    );
    let (mut a, mut b) = (
        lhs.next(store, directory, memory).await?,
        rhs.next(store, directory, memory).await?,
    );
    loop {
        match (a.as_ref(), b.as_ref()) {
            (None, None) => break,
            (Some(x), Some(y)) if x == y => {
                writer.push(a.take().expect("left entry exists")).await?;
                a = lhs.next(store, directory, memory).await?;
                b = rhs.next(store, directory, memory).await?;
            }
            (Some(x), Some(y)) if x < y => {
                writer.push(a.take().expect("left entry exists")).await?;
                a = lhs.next(store, directory, memory).await?;
            }
            (Some(_), None) => {
                writer.push(a.take().expect("left entry exists")).await?;
                a = lhs.next(store, directory, memory).await?;
            }
            _ => {
                writer.push(b.take().expect("right entry exists")).await?;
                b = rhs.next(store, directory, memory).await?;
            }
        }
    }
    writer.finish().await
}

pub(super) async fn prune(
    store: &dyn ObjectStore,
    directory: &ObjectKey,
    manifest: &Manifest,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<bool> {
    let manifests = manifest_objects(store, directory).await?;
    if manifests
        .iter()
        .any(|object| manifest_block(&object.key).is_some_and(|block| block > manifest.through))
    {
        return Ok(true);
    }
    let mut missing: std::collections::BTreeMap<_, _> = manifest
        .runs
        .iter()
        .flat_map(|run| run.pages.iter())
        .map(|page| {
            Ok((
                page_key(directory, page.generation, &page.digest)?,
                page.size,
            ))
        })
        .collect::<Result<_>>()?;
    for object in manifests {
        if cancellation.is_cancelled() {
            return Err(super::maintenance::cancelled());
        }
        if manifest_block(&object.key).is_some_and(|block| block < manifest.through) {
            store.delete(&object.key).await?;
        }
    }
    let prefix = key(directory, "pages")?;
    for object in store.list(Some(&prefix)).await? {
        if cancellation.is_cancelled() {
            return Err(super::maintenance::cancelled());
        }
        let generation = object
            .key
            .as_str()
            .rsplit('/')
            .nth(1)
            .and_then(|part| i64::from_str_radix(part, 16).ok());
        if let Some(size) = missing.get(&object.key) {
            if *size == object.size {
                missing.remove(&object.key);
                continue;
            }
        }
        if generation.is_some_and(|generation| generation <= manifest.through) {
            store.delete(&object.key).await?;
        }
    }
    Ok(missing.is_empty())
}
