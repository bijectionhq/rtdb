use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Cursor, Read, Seek},
    ops::Range,
    path::Path,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message;
use rtdb_db::{RankedBuild, RankedFiles, RankedPublication};
use rtdb_format::{
    catalog::TableBlock,
    ipc::{ArrowFile, inspect_file_reserved},
    keys::{CURRENT_STORAGE_VERSION, TrieKey},
    log::proto::TrieDetails,
    metadata::MetadataFile,
};
use rtdb_index::{
    CatalogTrie, LiveIndex, Snapshot, TableCatalog, TableMeta, TrieCatalog, TrieState,
    branches::{BranchCut, BranchView, RankedIndexDeclaration, RankedIndexKind},
};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
use rtdb_storage::{
    Error as StorageError, ObjectKey, ObjectObservation, ObjectStore, StoredObject,
    cache::{BufferPool, CacheBudget},
};
use rtdb_trie::TrieConfig;
use rtdb_types::{
    Anomaly, BlockIndex, CatalogCut, MemoryBudget, MemoryReservation, PhysicalObjectVersion,
    ReplicaPosition, Result, StorageEpoch, TableRef, Timestamp, TransactionKey, TxId,
    rtdb_v1::{At, Layer, MAIN_BRANCH},
};
use rtdb_vector::{mounted_native_definition, stage_native_generation};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{MultiServing, QuerySource, QueryView, interrupted};
use crate::serving::ranked_ivf::{self, Catalog, Target};

const PUBLISH_INTERVAL: Duration = Duration::from_secs(5);
const MAX_FILE_BYTES: u64 = 256 << 20;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Primary {
    table: TableRef,
    metadata: Vec<u8>,
    trie: Vec<u8>,
    definitions: BTreeMap<String, RankedIndexDeclaration>,
}

struct Mounted {
    snapshot: Snapshot,
    physical: CatalogCut,
    storage: Arc<BufferPool>,
    definitions: BTreeMap<TableRef, BTreeMap<String, RankedIndexDeclaration>>,
}

type Object = (Bytes, Option<PhysicalObjectVersion>);

struct MountedStore {
    files: BTreeMap<ObjectKey, Object>,
    _catalogue: Arc<MemoryReservation>,
}

struct ChargedBytes {
    bytes: Vec<u8>,
    _memory: Arc<MemoryReservation>,
}

impl AsRef<[u8]> for ChargedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl MultiServing {
    pub async fn maintain_ranked_catalog(&self, cancellation: &CancellationToken) -> Result<()> {
        let Some(build) = self.database.lock().await.ranked_build()? else {
            return Ok(());
        };
        let basis = build.index.head().basis;
        {
            let mut schedule = self.ranked_schedule.lock().await;
            let now = Instant::now();
            match *schedule {
                Some((previous, allowed)) if previous == basis && now >= allowed => (),
                Some((previous, _)) if previous == basis => return Ok(()),
                _ => {
                    *schedule = Some((basis, now + PUBLISH_INTERVAL));
                    return Ok(());
                }
            }
            *schedule = Some((basis, now + PUBLISH_INTERVAL));
        }
        let Some((inventory, files)) = stage(&build, &self.memory, cancellation).await? else {
            return Ok(());
        };
        if cancellation.is_cancelled() {
            return Err(interrupted());
        }
        self.writes
            .publish_ranked_catalog(build, inventory, files, Arc::clone(&self.memory))
            .await
    }

    async fn capture_ranked(
        &self,
        source: &QuerySource,
        now: Timestamp,
        follower: Option<&Arc<()>>,
        cancellation: &CancellationToken,
    ) -> Result<(
        rtdb_index::branches::BranchIndex,
        u64,
        Option<RankedPublication>,
    )> {
        cancellation
            .run_until_cancelled(async {
                let mut database = self.database.lock().await;
                let (index, generation) = match follower {
                    Some(owner) => {
                        let (index, generation) = database.follower_view(owner)?;
                        super::require_plain_follower(&index)?;
                        if let At::Exactly(basis) = source.at {
                            index.cut(At::AtLeast(basis))?;
                        }
                        (index, generation)
                    }
                    None => database.fresh_view().await?,
                };
                index.require_local_custody()?;
                let cut = index
                    .view(&source.branch, source.at, source.layer, now)
                    .map_err(|error| {
                        if follower.is_some() && error.code == "rtdb/unknown-basis" {
                            Anomaly::new(
                                rtdb_types::AnomalyCategory::NotFound,
                                "rtdb/cursor-expired",
                                "The replica has no retained exact cut at this basis",
                            )
                        } else {
                            error
                        }
                    })?
                    .cut();
                let publication = if ranked_source(source)
                    && !index.has_custody_groups()
                    && !index.has_visibility_history()
                {
                    database.ranked_publication(cut)
                } else {
                    None
                };
                Ok((index, generation, publication))
            })
            .await
            .ok_or_else(interrupted)?
    }

    pub(super) async fn ranked_query(
        &self,
        source: QuerySource,
        target: &Target,
        now: Timestamp,
        follower: Option<&Arc<()>>,
        cancellation: &CancellationToken,
    ) -> Result<(QueryView, Catalog)> {
        if !ranked_source(&source) {
            return Err(ranked_ivf::unsupported());
        }
        let (index, generation, publication) = self
            .capture_ranked(&source, now, follower, cancellation)
            .await?;
        let branch = index.view(&source.branch, source.at, source.layer, now)?;
        let mounted = open(
            publication.as_ref().ok_or_else(ranked_ivf::uncovered)?,
            &branch,
            generation,
            &self.memory,
            cancellation,
        )
        .await?;
        let declaration = mounted
            .declaration(&target.table, &target.index)
            .filter(|declaration| declaration.position == target.generation)
            .ok_or_else(ranked_ivf::uncovered)?
            .clone();
        let catalog = Catalog {
            target: target.clone(),
            declaration,
            physical: mounted.physical,
            storage: mounted.storage,
            mounted: true,
        };
        Ok((
            query_view(mounted.snapshot, branch.cut(), generation),
            catalog,
        ))
    }

    pub(super) async fn ranked_artefacts(
        &self,
        database: &str,
        source: QuerySource,
        now: Timestamp,
        follower: Option<&Arc<()>>,
        cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        if !ranked_source(&source) {
            if follower.is_some() {
                return Err(ranked_ivf::unsupported());
            }
            return super::ServingDriver::query(self, source, false, now, cancellation).await;
        }
        let (index, generation, publication) = self
            .capture_ranked(&source, now, follower, cancellation)
            .await?;
        if index.has_custody_groups() || index.has_visibility_history() {
            return super::ServingDriver::query(self, source, false, now, cancellation).await;
        }
        let branch = index.view(&source.branch, source.at, source.layer, now)?;
        let mut ready = BTreeSet::new();
        let mut retained = None;
        if let Some(publication) = publication {
            match open(
                &publication,
                &branch,
                generation,
                &self.memory,
                cancellation,
            )
            .await
            {
                Ok(mounted) => {
                    let context =
                        mounted.context(database, branch.cut(), &self.memory, cancellation)?;
                    for (table, definitions) in &mounted.definitions {
                        for (name, declaration) in definitions {
                            let definition = mounted_native_definition(table, name, declaration)?;
                            match rtdb_vector::mounted_native_ready(&context, &definition).await {
                                Ok(true) => {
                                    ready.insert(name.clone());
                                }
                                Ok(false) => (),
                                Err(error)
                                    if error.category
                                        == rtdb_types::AnomalyCategory::Interrupted =>
                                {
                                    return Err(error);
                                }
                                Err(_) => (),
                            }
                        }
                    }
                    retained = Some(mounted.storage);
                }
                Err(error) if error.category == rtdb_types::AnomalyCategory::Interrupted => {
                    return Err(error);
                }
                Err(_) => (),
            }
        }
        let names = ready.iter().map(String::as_str).collect();
        let mut snapshot =
            rtdb_query::branch_snapshot_with_ranked_artefacts(&branch, &self.memory, &names)?;
        if let Some(retained) = retained {
            snapshot = snapshot.with_catalog_read(retained);
        }
        Ok(query_view(snapshot, branch.cut(), generation))
    }
}

fn ranked_source(source: &QuerySource) -> bool {
    source.branch == MAIN_BRANCH
        && source.layer == Layer::Merged
        && matches!(source.at, At::Exactly(_))
}

fn query_view(snapshot: Snapshot, cut: BranchCut, generation: u64) -> QueryView {
    QueryView {
        snapshot,
        cut,
        claim: None,
        custody_groups: BTreeSet::new(),
        custody_contributors: Some(BTreeSet::new()),
        key_generation: Some(generation),
    }
}

fn ivf_definitions(
    branch: &BranchView<'_>,
) -> BTreeMap<TableRef, BTreeMap<String, RankedIndexDeclaration>> {
    branch
        .tables()
        .filter_map(|(table, definition)| {
            let indexes: BTreeMap<_, _> = definition
                .ranked_indexes
                .iter()
                .filter(|(_, declaration)| {
                    matches!(declaration.definition.kind, RankedIndexKind::Ivf { .. })
                })
                .map(|(name, declaration)| (name.clone(), declaration.clone()))
                .collect();
            (!indexes.is_empty()).then(|| (table.clone(), indexes))
        })
        .collect()
}

async fn stage(
    build: &RankedBuild,
    memory: &Arc<MemoryBudget>,
    cancellation: &CancellationToken,
) -> Result<Option<(Vec<u8>, Vec<Bytes>)>> {
    let index = &build.index;
    let head = index.head();
    let branch = index.view(
        MAIN_BRANCH,
        At::Exactly(head.basis),
        Layer::Merged,
        head.system_time,
    )?;
    let definitions = ivf_definitions(&branch);
    if definitions.is_empty() {
        return Ok(None);
    }
    let snapshot = rtdb_query::branch_primary_snapshot(
        &branch,
        memory,
        &definitions.keys().cloned().collect(),
    )?;
    let mut inventory = Vec::new();
    let mut files = Vec::new();
    for (table, definitions) in definitions {
        let Some(live) = snapshot
            .iter_segments(&table)
            .find(|segment| segment.row_count() > 0)
        else {
            continue;
        };
        let (mut trie, mut primary, metadata) =
            rtdb_replication::stage_live_primary(live, memory, cancellation).await?;
        files.push(file_bytes(&mut primary, memory)?);
        files.push(metadata);
        for (name, declaration) in &definitions {
            let definition = mounted_native_definition(&table, name, declaration)?;
            let source = primary.try_clone().map_err(invalid)?;
            let memory = Arc::clone(memory);
            let cancellation = cancellation.clone();
            let (generation, record) = tokio::task::spawn_blocking(move || {
                let (mut file, record) =
                    stage_native_generation(source, &definition, &memory, &cancellation)?;
                Ok::<_, Anomaly>((file_bytes(&mut file, &memory)?, record))
            })
            .await
            .map_err(invalid)??;
            files.push(generation);
            trie.artefacts.push(record);
        }
        inventory.push(Primary {
            metadata: live.block_metadata()?.to_block(Vec::new())?.encode(),
            trie: trie.to_proto(&table).encode_to_vec(),
            table,
            definitions,
        });
    }
    if inventory.is_empty() {
        return Ok(None);
    }
    Ok(Some((
        serde_json::to_vec(&inventory).map_err(invalid)?,
        files,
    )))
}

fn file_bytes(file: &mut File, memory: &Arc<MemoryBudget>) -> Result<Bytes> {
    let size = file.metadata().map_err(invalid)?.len();
    if size == 0 || size > MAX_FILE_BYTES {
        return Err(ranked_ivf::uncovered());
    }
    let held = memory.reserve(size as usize)?;
    file.rewind().map_err(invalid)?;
    let mut bytes = vec![0; size as usize];
    file.read_exact(&mut bytes).map_err(invalid)?;
    Ok(Bytes::from_owner(ChargedBytes {
        bytes,
        _memory: held,
    }))
}

async fn open(
    publication: &RankedPublication,
    branch: &BranchView<'_>,
    generation: u64,
    memory: &Arc<MemoryBudget>,
    cancellation: &CancellationToken,
) -> Result<Mounted> {
    cancellation
        .run_until_cancelled(async {
            let files = publication.open(memory).await?;
            if files.cut != branch.cut() || files.generation != generation {
                return Err(ranked_ivf::uncovered());
            }
            mount(files, branch, memory)
        })
        .await
        .ok_or_else(interrupted)?
}

fn mount(
    files: RankedFiles,
    branch: &BranchView<'_>,
    memory: &Arc<MemoryBudget>,
) -> Result<Mounted> {
    let _decode = memory.reserve(files.inventory.len().saturating_mul(8).saturating_add(4096))?;
    let inventory: Vec<Primary> = serde_json::from_slice(&files.inventory).map_err(invalid)?;
    let cut = files.cut;
    let mut objects = files.files.into_iter();
    let mut actual = BTreeMap::new();
    let mut tries = TrieCatalog::default();
    let mut definitions = BTreeMap::new();
    let mut schemas = BTreeMap::new();
    let expected = ivf_definitions(branch);
    for primary in &inventory {
        if expected.get(&primary.table) != Some(&primary.definitions)
            || definitions
                .insert(primary.table.clone(), primary.definitions.clone())
                .is_some()
        {
            return Err(ranked_ivf::uncovered());
        }
        let metadata = TableMeta::from_block(&TableBlock::decode(&primary.metadata)?)?;
        schemas.insert(primary.table.clone(), metadata);
    }
    let catalog = TableCatalog::default().fold(&schemas)?;
    for primary in inventory {
        let entry = catalog
            .entry(&primary.table)
            .ok_or_else(ranked_ivf::uncovered)?;
        let detail = TrieDetails::decode(primary.trie.as_slice()).map_err(invalid)?;
        let mut trie = CatalogTrie::from_proto(&detail)?;
        if detail.table_name.as_deref() != Some(primary.table.schema_and_table().as_str())
            || trie.key != TrieKey::l0(BlockIndex(0))?
            || trie.state != TrieState::Nascent
            || trie.artefacts.len() != primary.definitions.len()
        {
            return Err(ranked_ivf::uncovered());
        }
        let data = objects.next().ok_or_else(ranked_ivf::uncovered)?;
        let metadata = objects.next().ok_or_else(ranked_ivf::uncovered)?;
        if data.0.len() as i64 != trie.data_file_size {
            return Err(ranked_ivf::uncovered());
        }
        verify_primary(&data.0, &metadata.0, memory)?;
        actual.insert(ObjectKey::new(entry.slug.data_key(&trie.key))?, data);
        actual.insert(
            ObjectKey::new(entry.slug.metadata_key(&trie.key))?,
            metadata,
        );
        for ((name, declaration), record) in primary.definitions.iter().zip(&mut trie.artefacts) {
            let definition =
                mounted_native_definition(&primary.table, name, declaration)?.artefact();
            let object = objects.next().ok_or_else(ranked_ivf::uncovered)?;
            if record.kind() != definition.kind
                || record.definition_id() != definition.id
                || record.definition_version() != definition.version
                || record.file_size != Some(object.0.len() as i64)
            {
                return Err(ranked_ivf::uncovered());
            }
            record.published_version = object.1.clone().map(Into::into);
            actual.insert(
                ObjectKey::new(entry.slug.recorded_artefact_key(&trie.key, record)?)?,
                object,
            );
        }
        tries = tries.with_tries(primary.table, [trie], cut.system_time)?;
    }
    if actual.is_empty() || objects.next().is_some() {
        return Err(ranked_ivf::uncovered());
    }
    let position = i64::try_from(cut.basis.position()).map_err(invalid)?;
    let epoch = i32::try_from(files.epoch).map_err(invalid)?;
    let storage_epoch = i32::try_from(files.generation).map_err(invalid)?;
    let physical = CatalogCut {
        storage_version: CURRENT_STORAGE_VERSION,
        storage_epoch: StorageEpoch(storage_epoch),
        replica_epoch: StorageEpoch(epoch),
        through: ReplicaPosition(position),
    };
    let tx = TransactionKey {
        tx_id: TxId(position),
        system_time: cut.system_time,
    };
    let snapshot = Snapshot::new(
        Arc::new(tries),
        Arc::new(catalog),
        Arc::new(LiveIndex::new(
            BlockIndex(1),
            Some(tx),
            TrieConfig::default(),
        )),
    )
    .with_catalog_cut(Some(physical));
    let store = Arc::new(MountedStore {
        files: actual,
        _catalogue: _decode,
    });
    let storage = Arc::new(BufferPool::with_budget(
        store,
        CacheBudget::with_memory(Arc::clone(memory)),
    ));
    Ok(Mounted {
        snapshot,
        physical,
        storage,
        definitions,
    })
}

fn verify_primary(data: &Bytes, metadata: &Bytes, memory: &Arc<MemoryBudget>) -> Result<()> {
    let mut metadata = Cursor::new(metadata.clone());
    let layout = inspect_file_reserved(&mut metadata, |bytes| memory.reserve(bytes))?;
    let _metadata = memory.reserve(layout.full_allocation_bound()?)?;
    let tree = MetadataFile::read_file(ArrowFile::from_inspected_reader(metadata, None, layout)?)?;
    let mut data = Cursor::new(data.clone());
    let layout = inspect_file_reserved(&mut data, |bytes| memory.reserve(bytes))?;
    let _data = memory.reserve(layout.opening_allocation_bound()?)?;
    let events = ArrowFile::from_inspected_reader(data, None, layout)?;
    let pages = events.page_count();
    let _inventory = memory.reserve(tree.nodes().len().saturating_mul(160))?;
    let mut leaves: Vec<_> = tree.leaves().into_iter().map(|(_, page)| page).collect();
    leaves.sort_unstable();
    if pages == 0 || leaves.into_iter().ne(0..pages) {
        return Err(ranked_ivf::uncovered());
    }
    Ok(())
}

impl Mounted {
    fn declaration(&self, table: &TableRef, name: &str) -> Option<&RankedIndexDeclaration> {
        self.definitions.get(table)?.get(name)
    }

    fn context(
        &self,
        database: &str,
        cut: BranchCut,
        memory: &Arc<MemoryBudget>,
        cancellation: &CancellationToken,
    ) -> Result<Arc<QueryContext>> {
        let catalog = CatalogSnapshot::new(
            database,
            BTreeMap::from([(
                database.to_string(),
                DatabaseSnapshot {
                    partitions: vec![self.snapshot.clone()],
                    storage: Arc::clone(&self.storage),
                },
            )]),
            false,
        )?;
        QueryContext::new(
            catalog,
            Arc::clone(memory),
            cut.system_time,
            "UTC",
            QueryOptions::default(),
            cancellation,
        )
    }
}

impl MountedStore {
    fn file(&self, key: &ObjectKey) -> rtdb_storage::Result<&Object> {
        self.files
            .get(key)
            .ok_or_else(|| StorageError::MissingObject(key.to_string()))
    }
}

#[async_trait]
impl ObjectStore for MountedStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        Ok(StoredObject {
            key: key.clone(),
            size: self.file(key)?.0.len() as u64,
        })
    }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        Ok(self.file(key)?.0.clone())
    }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        let bytes = &self.file(key)?.0;
        if range.start > range.end || range.end > bytes.len() as u64 {
            return Err(StorageError::InvalidRange {
                key: key.to_string(),
                start: range.start,
                end: range.end,
            });
        }
        Ok(bytes.slice(range.start as usize..range.end as usize))
    }
    async fn observe_fresh_exact(
        &self,
        key: &ObjectKey,
        length: usize,
    ) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        let (bytes, version) = self.file(key)?;
        if length != bytes.len() {
            return Err(StorageError::InvalidRange {
                key: key.to_string(),
                start: 0,
                end: length as u64,
            });
        }
        Ok(ObjectObservation {
            value: bytes.clone(),
            version: version.clone(),
        })
    }
    async fn put(&self, _: &ObjectKey, _: Bytes) -> rtdb_storage::Result<()> {
        Err(read_only())
    }
    async fn put_file(&self, _: &ObjectKey, _: &Path) -> rtdb_storage::Result<()> {
        Err(read_only())
    }
    async fn list(&self, _: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        Err(read_only())
    }
    async fn delete(&self, _: &ObjectKey) -> rtdb_storage::Result<()> {
        Err(read_only())
    }
}

fn read_only() -> StorageError {
    StorageError::Closed("The mounted ranked catalogue is immutable".into())
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.node/ranked-catalog", error.to_string())
}
