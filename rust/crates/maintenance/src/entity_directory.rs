use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{
    cow_tree::{self, Edit, NoSummary, PageGeneration},
    data::EventBatch,
    entity_directory::{self, LogicalEvent},
    keys::TrieKey,
    log::proto::{ArtefactRecord, EntityDirectoryChange, EntityDirectoryRoot},
};
use rtdb_index::PartitionValue;
use rtdb_storage::{ObjectKey, cow_pages::ObjectPageStore};
use rtdb_types::{BlockIndex, EventOp, Iid, Result, TableRef};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    ArtefactBuilder, ArtefactDatabase, ArtefactDefinition, ArtefactJob, ArtefactSource,
    input::read_page,
    merge::{PrimaryData, primary_data_budget},
};

pub(crate) struct EntityDirectoryBuilder {
    store: Arc<dyn rtdb_storage::ObjectStore>,
    storage: Arc<rtdb_storage::cache::BufferPool>,
    memory: Arc<MemoryBudget>,
}

impl EntityDirectoryBuilder {
    pub fn new(database: &ArtefactDatabase) -> Self {
        Self {
            store: database.store.clone(),
            storage: database.storage.clone(),
            memory: database.memory.clone(),
        }
    }

    async fn advance(
        &self,
        value: &PartitionValue,
        table: &TableRef,
        trie: &TrieKey,
        path: &Path,
        cancellation: &CancellationToken,
    ) -> Result<ArtefactRecord> {
        let catalog = value.trie_catalog();
        let entry = value
            .table_catalog()
            .entry(table)
            .ok_or_else(|| crate::invalid("Directory table has no slug"))?;
        let previous = catalog.entity_directory(table);
        let base = previous.map(EntityDirectoryRoot::reference).transpose()?;
        let through = trie.block_index();
        let after = previous.map_or(BlockIndex(-1), |root| BlockIndex(root.through_block()));
        let durable_count = value
            .table_catalog()
            .metadata(table)
            .map_or(0, |metadata| metadata.row_count);
        let sources = catalog.l0_source(table, after, BlockIndex(through.0 - 1));
        let mut read = 0i64;
        let mut input = Vec::new();
        let retained = primary_data_budget(&self.memory);
        for source in sources {
            let temp = tempfile::tempdir().map_err(crate::invalid)?;
            let key = ObjectKey::new(entry.slug.data_key(&source.key))?;
            let size = self.store.head(&key).await?.size;
            let data = PrimaryData::stored(
                self.store.as_ref(),
                &key,
                temp.path().join("directory-source.arrow"),
                size,
                &self.memory,
                &retained,
                cancellation,
            )
            .await?;
            let rows = self.read(data, Some(temp), source.key.clone(), cancellation).await?;
            read = read
                .checked_add(i64::try_from(rows.0.len()).map_err(crate::invalid)?)
                .ok_or_else(overflow)?;
            input.push(rows);
        }
        if previous.is_none() && read != durable_count {
            return Err(crate::invalid(
                "Directory bootstrap requires every accepted source event",
            ));
        }
        input.push(
            self.read(
                PrimaryData::File(File::open(path).map_err(crate::invalid)?),
                None,
                trie.clone(),
                cancellation,
            )
            .await?,
        );
        let pages = ObjectPageStore {
            store: self.store.clone(),
            storage: self.storage.clone(),
            prefix: entity_directory::prefix(&entry.slug),
        };
        let mut by_entity = BTreeMap::<(Iid, Option<Vec<u8>>), Vec<&LogicalEvent>>::new();
        let _input_charge =
            self.memory
                .reserve(input.iter().flat_map(|(events, _)| events).fold(
                    4096usize,
                    |bytes, event| {
                        bytes
                            .saturating_add(128)
                            .saturating_add(event.entity_key.as_ref().map_or(0, Vec::len))
                    },
                ))?;
        for event in input.iter().flat_map(|(events, _)| events) {
            by_entity
                .entry((event.header.iid, event.entity_key.clone()))
                .or_default()
                .push(event);
        }
        let mut edits = BTreeMap::new();
        let mut edit_charge = self.memory.reserve(4096)?;
        let mut source_rows = previous.map_or(0, EntityDirectoryRoot::source_rows);
        let mut erased = false;
        for ((iid, identity), events) in by_entity {
            if cancellation.is_cancelled() {
                return Err(crate::closed());
            }
            let frontier = events
                .iter()
                .filter(|event| event.header.op == EventOp::Erase)
                .max_by_key(|event| event.position())
                .copied();
            if let Some(erase) = frontier {
                erased = true;
                if let Some(base) = &base {
                    cow_tree::visit(
                        &pages,
                        base,
                        entity_directory::entity_rows(iid, identity.as_deref())?,
                        &|_| true,
                        &self.memory,
                        |row| {
                            let old = LogicalEvent::decode(&row)?;
                            if old.header.op == EventOp::Erase && old.position() >= erase.position()
                            {
                                return Err(crate::invalid(
                                    "Directory erase frontier must advance",
                                ));
                            }
                            if old.header.op == EventOp::Erase || old.position() <= erase.position()
                            {
                                charge(&mut edit_charge, row.key.len() + 128)?;
                                edits.insert(row.key.clone(), Edit::Delete(row.key));
                                source_rows -= 1;
                            }
                            Ok(true)
                        },
                    )
                    .await?;
                }
            }
            for event in events {
                if frontier.is_some_and(|erase| {
                    (event.header.op == EventOp::Erase && event.origin != erase.origin)
                        || (event.header.op != EventOp::Erase
                            && event.position() <= erase.position())
                }) {
                    continue;
                }
                let row = event.encode()?;
                charge(
                    &mut edit_charge,
                    row.key.len().saturating_mul(2) + row.value.len() + 128,
                )?;
                if matches!(
                    edits.insert(row.key.clone(), Edit::Put(row)),
                    Some(Edit::Put(_))
                ) {
                    return Err(crate::invalid("Two events have the same directory key"));
                }
                source_rows = source_rows.checked_add(1).ok_or_else(overflow)?;
            }
        }
        let allocation = uuid::Uuid::new_v4();
        let (root, _) = cow_tree::apply_with_generation(
            &pages,
            base.as_ref(),
            edits.into_values().collect(),
            &NoSummary,
            &self.memory,
            Some(PageGeneration::Block(through)),
        )
        .await?;
        let root =
            root.ok_or_else(|| crate::invalid("A sealed table directory must have a root"))?;
        let (has_erases, erase_frontier_digest) = if erased {
            let mut hash = Sha256::new();
            cow_tree::visit(
                &pages,
                &root,
                entity_directory::all_rows(),
                &|_| true,
                &self.memory,
                |row| {
                    if cancellation.is_cancelled() {
                        return Err(crate::closed());
                    }
                    if LogicalEvent::is_frontier(&row) {
                        entity_directory::add_frontier(&mut hash, &LogicalEvent::decode(&row)?)?;
                    }
                    Ok(true)
                },
            )
            .await?;
            (true, hash.finalize().to_vec())
        } else {
            previous.map_or_else(
                || (false, Sha256::digest([]).to_vec()),
                |root| (root.has_erases(), root.erase_frontier_digest().to_vec()),
            )
        };
        EntityDirectoryChange {
            root: Some(EntityDirectoryRoot {
                root: Some(root.encode()),
                through_block: Some(through.0),
                source_rows: Some(source_rows),
                has_erases: Some(has_erases),
                allocation: Some(allocation.as_bytes().to_vec()),
                writer_fence: Some(match self.store.search_staging_owner().await? {
                    Some(rtdb_format::search_manifest::SearchStagingOwner::Directory { writer_epoch, .. }) => writer_epoch,
                    _ => 0,
                }),
                erase_frontier_digest: Some(erase_frontier_digest),
            }),
            replaces_root: previous.and_then(|root| root.root.clone()),
        }
        .to_artefact()
    }

    async fn read(
        &self,
        input: PrimaryData,
        temporary: Option<tempfile::TempDir>,
        trie: TrieKey,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<LogicalEvent>, Arc<MemoryReservation>)> {
        let memory = self.memory.clone();
        let cancellation = cancellation.clone();
        tokio::task::spawn_blocking(move || {
            let _temporary = temporary;
            input.read(&memory, &cancellation, |mut input| {
                let mut reservation = memory.reserve(4096)?;
                let mut events = Vec::new();
                for page in 0..input.reader.page_count() {
                    if cancellation.is_cancelled() {
                        return Err(crate::closed());
                    }
                    let batch = read_page(&mut input.reader, page, &memory)?;
                    let batch = EventBatch::new(batch.batch().clone())?;
                    for row in 0..batch.len() {
                        let origin = batch
                            .origin(row)?
                            .filter(|origin| origin.source_block() == trie.block_index())
                            .ok_or_else(|| {
                                crate::invalid(
                                    "Directory requires a stable origin for each accepted event",
                                )
                            })?;
                        let key = batch.entity_key(row)?;
                        charge(&mut reservation, 256 + key.map_or(0, <[u8]>::len))?;
                        let hint = (
                            trie.clone(),
                            u32::try_from(page).map_err(crate::invalid)?,
                            u32::try_from(row).map_err(crate::invalid)?,
                        );
                        events.push(LogicalEvent {
                            header: batch.header(row)?,
                            origin,
                            entity_key: key.map(<[u8]>::to_vec),
                            payload_hint: Some(hint),
                        });
                    }
                }
                Ok((events, reservation))
            })
        })
        .await
        .map_err(crate::invalid)?
    }
}

fn overflow() -> rtdb_types::Anomaly {
    crate::invalid("Directory row count overflow")
}

fn charge(reservation: &mut Arc<MemoryReservation>, additional: usize) -> Result<()> {
    let held = Arc::get_mut(reservation).expect("unpublished directory rows own their reservation");
    held.grow_to(held.bytes().saturating_add(additional))
}

#[async_trait]
impl ArtefactBuilder for EntityDirectoryBuilder {
    async fn definitions(
        &self,
        _: &Arc<PartitionValue>,
        _: &CancellationToken,
    ) -> Result<Vec<ArtefactDefinition>> {
        Ok(Vec::new())
    }

    async fn build(
        &self,
        _: &ArtefactJob,
        _: ArtefactSource<'_>,
        _: &Arc<PartitionValue>,
        _: &CancellationToken,
    ) -> Result<ArtefactRecord> {
        Err(rtdb_types::Anomaly::unsupported(
            "rtdb.maintenance/entity-directory",
            "An entity directory advances with a sealed table",
        ))
    }

    async fn seal_serving_search(
        &self,
        value: &Arc<PartitionValue>,
        table: &TableRef,
        trie: &TrieKey,
        path: &Path,
        _: u64,
        cancellation: &CancellationToken,
    ) -> Result<Vec<ArtefactRecord>> {
        if !self.store.supports_directory_page_release() || table.is_owned_event_data() || TableRef::owned_schema(&table.schema) {
            return Ok(Vec::new());
        }
        match self.advance(value, table, trie, path, cancellation).await {
            Ok(record) => Ok(vec![record]),
            Err(error) if cancellation.is_cancelled() => Err(error),
            Err(_) => Ok(Vec::new()),
        }
    }
}
