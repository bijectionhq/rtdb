use std::{collections::{BTreeMap, BTreeSet}, sync::Arc, time::Duration};

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_crypt::tokens::VerifiedClaims;
use rtdb_index::{Snapshot, branches::{BranchIndex, BranchView, RankedIndexKind, store::{StorePage, StoreRead, sizes::StorageSizes}}};
use rtdb_replication::{NATIVE_READ_FEATURES, NativeServingHandle, NativeServingScope, NativeServingView};
use rtdb_tx::transact::{CustodyGroupId, CustodyWriterFence, ROW_CUSTODY, Receipt, Transact, TrustedCustodyCommand};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::{At, Basis, ClaimEvidence, ClaimScope, Layer}};
use tokio_util::sync::CancellationToken;

use super::{ChangeGroup, ChangeOrder, ChangePage, Changes, QuerySource, QueryView, ServingDriver,
    ServingScope, append_change_rows, ranked_ivf};
use crate::Clock;

pub(crate) struct DefaultServing {
    pub(crate) handle: NativeServingHandle,
    pub(crate) memory: Arc<MemoryBudget>,
    pub(crate) storage: Arc<rtdb_storage::cache::BufferPool>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) tokens: Arc<crate::database::tokens::DatabaseTokens>,
    pub(crate) writable: bool,
    pub(crate) write_features: Vec<&'static str>,
}

impl DefaultServing {
    async fn store_snapshot(&self, at: At, cancellation: &CancellationToken) -> Result<NativeServingView> {
        if self.handle.has_classic_read() || !self.handle.is_compatible() {
            return Err(Anomaly::unsupported("rtdb.store/unsupported", "This database does not serve the ordered-bytes store"));
        }
        let snapshot = self.handle.view(at, cancellation).await?;
        if snapshot.index.has_custody_groups() || snapshot.index.has_visibility_history() {
            return Err(Anomaly::unsupported("rtdb.store/unsupported", "The ordered-bytes store serves only an unlabelled database"));
        }
        Ok(snapshot)
    }

    async fn open_change_page(&self, view: &NativeServingView, mut page: ChangePage) -> Result<ChangePage> {
        page.key_generation = view.key_generation;
        page.read_custody = view.read_custody.clone();
        if !view.index.has_custody_groups() {
            return Ok(page);
        }
        let vault = self.tokens.authority().await?.custody_vault();
        super::custody::open_changes(&view.index, &vault, page).await
    }

    async fn ranked_snapshot(
        &self, database: &str, branch: &BranchView<'_>, physical: Snapshot, cancellation: &CancellationToken,
    ) -> Result<Snapshot> {
        use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};

        // Collected so the awaiting loop below holds plain references, not a lifetime-generic closure
        // that the `Send` check of the async-trait query cannot prove general enough.
        let indexes = branch.tables().flat_map(|(table, definition)| definition.ranked_indexes.iter()
            .map(move |(name, declaration)| (table, name, declaration))).collect::<Vec<_>>();
        let count = indexes.len();
        if count == 0 {
            return rtdb_query::branch_snapshot(branch, &self.memory);
        }
        let catalog = CatalogSnapshot::new(database, BTreeMap::from([(database.into(), DatabaseSnapshot {
            partitions: vec![physical], storage: Arc::clone(&self.storage),
        })]), false)?;
        let context = QueryContext::new(catalog, Arc::clone(&self.memory), branch.cut().system_time,
            "UTC", QueryOptions::default(), cancellation)?;
        let _ready = self.memory.reserve(count.saturating_mul(128))?;
        let mut ready = BTreeSet::new();
        for (table, name, declaration) in indexes {
            let verified = match &declaration.definition.kind {
                RankedIndexKind::Spatial { .. } => rtdb_spatial::committed_native_ready(Arc::clone(&context), table, name, declaration).await,
                RankedIndexKind::Ivf { .. } => rtdb_vector::committed_native_ready(Arc::clone(&context), table, name, declaration).await,
                RankedIndexKind::Search { .. } => {
                    #[cfg(feature = "search")]
                    { rtdb_search::committed_native_ready(Arc::clone(&context), table, name, declaration).await }
                    #[cfg(not(feature = "search"))]
                    { continue; }
                }
            };
            match verified {
                Ok(true) => { ready.insert(name.as_str()); }
                Ok(false) => {}
                Err(error) if error.category == AnomalyCategory::Interrupted => return Err(error),
                Err(_) => {}
            }
        }
        rtdb_query::branch_snapshot_with_ranked_artefacts(branch, &self.memory, &ready)
    }

    async fn capture_query(
        &self, source: QuerySource, track: bool, now: Timestamp, cancellation: &CancellationToken,
        ranked_database: Option<&str>,
    ) -> Result<QueryView> {
        if self.handle.has_classic_read() {
            if self.handle.has_local_erasure_fence() { return Err(rtdb_crypt::erased_under_view()); }
            if source.branch != rtdb_types::rtdb_v1::MAIN_BRANCH || source.layer != Layer::Merged || track {
                return Err(Anomaly::unsupported("rtdb/classic-query-scope", "The Classic catalog only serves untracked queries on merged main"));
            }
            let (snapshot, cut) = self.handle.classic_read(source.at, cancellation).await?;
            return Ok(QueryView { snapshot, cut, claim: None, custody_groups: BTreeSet::new(), custody_contributors: None, scoped_source_basis: None, key_generation: None });
        }
        let snapshot = self.handle.view(source.at, cancellation).await?;
        let at = if source.at == At::Linearizable { At::Latest } else { source.at };
        let branch = snapshot.index.view(&source.branch, at, source.layer, now)?;
        let claim = if track {
            Some(self.tokens.authority().await?.seal_claim(&ClaimEvidence {
                scope: ClaimScope { branch: branch.branch().into(), basis: branch.cut().basis, layer: source.layer },
                footprint: Default::default(),
            }).await?)
        } else {
            None
        };
        let cut = branch.cut();
        let key_generation = snapshot.key_generation;
        let snapshot_read_custody = snapshot.read_custody.clone();
        let (snapshot, custody_groups, custody_contributors) = if snapshot.index.has_custody_groups() {
            let vault = self.tokens.authority().await?.custody_vault();
            super::custody::snapshot(&branch, &self.memory, &vault).await?
        } else {
            let physical = ranked_database.filter(|_| branch.branch() == rtdb_types::rtdb_v1::MAIN_BRANCH
                && branch.layer() == Layer::Merged && matches!(source.at, At::Exactly(_)))
                .and_then(|database| self.handle.ranked_catalog(&snapshot, cut).map(|physical| (database, physical)));
            let rows = match physical {
                Some((database, physical)) => self.ranked_snapshot(database, &branch, physical, cancellation).await?,
                None => rtdb_query::branch_snapshot(&branch, &self.memory)?,
            };
            (rows, BTreeSet::new(), Some(BTreeSet::new()))
        };
        let snapshot = match snapshot_read_custody {
            Some(custody) => {
                let deadline = custody.deadline();
                snapshot.with_read_custody(custody, deadline)
            }
            None => snapshot,
        };
        Ok(QueryView { snapshot, cut, claim, custody_groups, custody_contributors, scoped_source_basis: None, key_generation })
    }
}

#[async_trait]
impl ServingDriver for DefaultServing {
    fn incarnation(&self) -> [u8; 16] { self.handle.authority().incarnation }
    fn uses_database_tokens(&self) -> bool { true }
    fn supports_query_tokens(&self) -> bool { self.handle.is_compatible() }
    fn supports_row_visibility(&self) -> bool { self.handle.is_compatible() && self.tokens.supported() }

    fn features(&self) -> &[&str] {
        if self.handle.has_classic_read() {
            return &["query", "query.linearizable", "sql.history", "sql.search", "sql.search-table", "sql.nearest-table", "sql.cosine-distance"];
        }
        if !self.handle.is_compatible() { return &[]; }
        if self.writable { &self.write_features } else { NATIVE_READ_FEATURES }
    }

    async fn query(
        &self, source: QuerySource, track: bool, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        self.capture_query(source, track, now, cancellation, None).await
    }

    async fn read_store(
        &self, read: StoreRead, at: At, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<StorePage> {
        let snapshot = self.store_snapshot(at, cancellation).await?;
        let at = if at == At::Linearizable { At::Latest } else { at };
        snapshot.index.view(rtdb_types::rtdb_v1::MAIN_BRANCH, at, Layer::Merged, now)?.store_page(&read)
    }

    async fn read_store_batch(
        &self, reads: &mut (dyn ExactSizeIterator<Item = StoreRead> + Send),
        at: At, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<Vec<StorePage>> {
        if reads.len() == 0 { return Ok(Vec::new()); }
        let snapshot = self.store_snapshot(at, cancellation).await?;
        let at = if at == At::Linearizable { At::Latest } else { at };
        let view = snapshot.index.view(rtdb_types::rtdb_v1::MAIN_BRANCH, at, Layer::Merged, now)?;
        reads.map(|read| view.store_page(&read)).collect()
    }

    async fn store_sizes(&self, epoch: Option<u64>, now: Timestamp, cancellation: &CancellationToken) -> Result<StorageSizes> {
        if self.handle.has_classic_read() || !self.handle.is_compatible() {
            return Err(Anomaly::unsupported("rtdb.store/unsupported", "This database does not serve the ordered-bytes store"));
        }
        let snapshot = self.handle.view(At::Latest, cancellation).await?;
        super::store_size_snapshot(snapshot.index, epoch, now, cancellation).await
    }

    async fn query_ranked_artefacts(
        &self, database: &str, source: QuerySource, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        self.capture_query(source, false, now, cancellation, Some(database)).await
    }

    fn supports_ranked_ivf(&self) -> bool {
        cfg!(feature = "search") && self.handle.supports_ranked_catalog()
    }

    async fn query_ranked_ivf(
        &self, source: QuerySource, target: &ranked_ivf::Target, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<(QueryView, ranked_ivf::Catalog)> {
        if !self.supports_ranked_ivf() || source.branch != rtdb_types::rtdb_v1::MAIN_BRANCH
            || source.layer != Layer::Merged || !matches!(source.at, At::Exactly(_))
        {
            return Err(ranked_ivf::unsupported());
        }
        let selected = self.handle.view(source.at, cancellation).await?;
        if selected.index.has_custody_groups() || selected.index.has_visibility_history() {
            return Err(ranked_ivf::unsupported());
        }
        let branch = selected.index.view(&source.branch, source.at, source.layer, now)?;
        let cut = branch.cut();
        let declaration = branch.table(&target.table).and_then(|table| table.ranked_indexes.get(&target.index))
            .filter(|index| index.position == target.generation && matches!(index.definition.kind, RankedIndexKind::Ivf { .. }))
            .ok_or_else(ranked_ivf::uncovered)?;
        let physical = self.handle.ranked_catalog(&selected, cut).ok_or_else(ranked_ivf::uncovered)?;
        let catalog_cut = physical.catalog_cut().ok_or_else(ranked_ivf::uncovered)?;
        let snapshot = physical;
        let view = QueryView { snapshot, cut, claim: None, custody_groups: BTreeSet::new(),
            custody_contributors: Some(BTreeSet::new()), scoped_source_basis: None, key_generation: selected.key_generation };
        let catalog = ranked_ivf::Catalog { target: target.clone(), declaration: declaration.clone(),
            physical: catalog_cut, storage: Arc::clone(&self.storage), mounted: false };
        Ok((view, catalog))
    }

    async fn transact(&self, mut request: Transact, cancellation: &CancellationToken) -> Result<Receipt> {
        if !self.writable {
            return Err(Anomaly::unsupported("rtdb/default-serving-read-only", "The default serving attachment is read-only"));
        }
        if request.uses_custody() && self.features().contains(&ROW_CUSTODY) {
            let vault = self.tokens.authority().await?.custody_vault();
            super::custody::prepare(&mut request, &vault).await?;
        }
        let claims = if request.claims().next().is_some() {
            self.tokens.authority().await?.verify_claims(request.claims()).await
        } else {
            VerifiedClaims::default()
        };
        self.handle.transact(request, claims, None, cancellation).await
    }

    async fn check_read_delivery(&self, generation: Option<u64>, groups: &BTreeSet<CustodyGroupId>, cancellation: &CancellationToken) -> Result<()> {
        let Some(generation) = generation else {
            return if groups.is_empty() { Ok(()) } else { Err(super::custody_release::release_unavailable()) };
        };
        if !groups.is_empty() {
            let index = self.current_custody(cancellation).await?;
            let vault = self.tokens.authority().await?.custody_vault();
            super::custody::check_delivery(&index, &vault, groups).await?;
        }
        self.handle.check_read_generation(generation).await
    }

    async fn current_custody(&self, cancellation: &CancellationToken) -> Result<BranchIndex> {
        if self.handle.has_classic_read() || self.handle.has_local_erasure_fence() { return Err(super::custody_release::release_unavailable()); }
        Ok(self.handle.view(At::Linearizable, cancellation).await?.index)
    }

    async fn original_custody_receipt(&self, key: &[u8], basis: Basis, cancellation: &CancellationToken) -> Result<Receipt> {
        self.handle.original_custody_receipt(key.to_vec(), basis, cancellation).await
    }

    async fn submit_custody_release(&self, command: TrustedCustodyCommand, cancellation: &CancellationToken) -> Result<Receipt> {
        if !self.writable || self.handle.has_classic_read() || self.handle.has_local_erasure_fence() { return Err(super::custody_release::release_unavailable()); }
        self.handle.submit_custody_release(command, cancellation).await
    }

    async fn check_custody_release_delivery(&self, groups: &BTreeSet<CustodyGroupId>, writer: &CustodyWriterFence,
                                            generation: Option<u64>, cancellation: &CancellationToken) -> Result<()> {
        if !groups.is_empty() {
            let vault = self.tokens.authority().await?.custody_vault();
            for &group in groups { vault.check_readable(group).await?; }
        }
        let index = self.current_custody(cancellation).await?;
        super::custody_release::check_release_cut(&index, writer, groups, self.clock.now())?;
        if let Some(generation) = generation {
            self.handle.check_read_generation(generation).await?;
        }
        Ok(())
    }

    async fn reconcile_custody(&self, cancellation: &CancellationToken) -> Result<()> {
        if !self.writable { return Ok(()); }
        let view = self.handle.view(At::Linearizable, cancellation).await?;
        if !view.index.has_custody_groups() { return Ok(()); }
        let vault = self.tokens.authority().await?.custody_vault();
        super::custody::reconcile_revocations(&view.index, &vault, self.clock.now()).await
    }

    async fn tx_status(&self, key: &[u8], cancellation: &CancellationToken) -> Result<Receipt> {
        self.handle.tx_status(key.to_vec(), None, cancellation).await
    }

    async fn query_scoped(
        &self, source: QuerySource, track: bool, now: Timestamp, scope: &ServingScope,
        cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let at = if source.at == At::Linearizable { At::Linearizable } else { At::Latest };
        let mut snapshot = self.handle.view(at, cancellation).await?;
        loop {
            let vault = if snapshot.index.has_custody_groups() {
                Some(self.tokens.authority().await?.custody_vault())
            } else {
                None
            };
            match scope.query(&snapshot.index, source.clone(), now, &self.memory, vault.as_ref()).await {
                Ok((mut view, raw)) => {
                    view.key_generation = snapshot.key_generation;
                    if let Some(custody) = snapshot.read_custody.clone() {
                        let deadline = custody.deadline();
                        view.snapshot = view.snapshot.with_read_custody(custody, deadline);
                    }
                    if track {
                        view.claim = Some(self.tokens.authority().await?.issuer().await?
                            .seal_scoped_claim(raw, &scope.labels).await?);
                    }
                    return Ok(view);
                }
                Err(error) if matches!(source.at, At::AtLeast(_)) && error.is(rtdb_types::AnomalyKind::BasisAhead) => {
                    let wait = self.handle.after(snapshot.index.head().basis, cancellation);
                    tokio::time::timeout_at(deadline, wait).await.map_err(|_| error)??;
                    snapshot = self.handle.view(At::Latest, cancellation).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn transact_scoped(
        &self, request: Transact, scope: ServingScope, cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        if !self.writable {
            return Err(Anomaly::unsupported("rtdb/default-serving-read-only", "The default serving attachment is read-only"));
        }
        let claims = if request.claims().next().is_some() {
            self.tokens.authority().await?
                .verify_claims_in(request.claims(), Some(&scope.labels)).await
        } else {
            VerifiedClaims::default()
        };
        let scope = NativeServingScope { labels: scope.labels, receipt_namespace: scope.receipt_namespace };
        self.handle.transact(request, claims, Some(scope), cancellation).await
    }

    async fn tx_status_scoped(
        &self, key: &[u8], scope: &ServingScope, cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        let scope = NativeServingScope { labels: scope.labels.clone(), receipt_namespace: scope.receipt_namespace };
        self.handle.tx_status(key.to_vec(), Some(scope), cancellation).await
    }

    async fn changes_scoped(
        &self, request: Changes, scope: &ServingScope, cancellation: &CancellationToken,
    ) -> Result<ChangePage> {
        let deadline = tokio::time::Instant::now() + request.wait;
        loop {
            let snapshot = self.handle.view(At::Latest, cancellation).await?;
            let page = scope.changes(&snapshot.index, &request)?;
            if !page.groups.is_empty() || request.until.is_some() && page.complete || tokio::time::Instant::now() >= deadline {
                return self.open_change_page(&snapshot, page).await;
            }
            match tokio::time::timeout_at(deadline, self.handle.after(snapshot.index.head().basis, cancellation)).await {
                Ok(result) => result?,
                Err(_) => return self.open_change_page(&snapshot, page).await,
            }
        }
    }

    async fn changes(&self, request: Changes, cancellation: &CancellationToken) -> Result<ChangePage> {
        let after = request.after.unwrap_or_else(|| Basis::new(self.incarnation(), 0));
        let deadline = tokio::time::Instant::now() + request.wait;
        loop {
            let view = self.handle.view(At::Latest, cancellation).await?;
            let head = view.index.head().basis;
            let page = change_page(&view, &request, after, self.clock.now())?;
            if !page.groups.is_empty()
                || request.until.is_some_and(|until| until <= head)
                || tokio::time::Instant::now() >= deadline
            {
                return self.open_change_page(&view, page).await;
            }
            match tokio::time::timeout_at(deadline, self.handle.after(head, cancellation)).await {
                Ok(result) => result?,
                Err(_) => return self.open_change_page(&view, page).await,
            }
        }
    }
}

fn change_page(view: &NativeServingView, request: &Changes, after: Basis, now: Timestamp) -> Result<ChangePage> {
    let head = view.index.head().basis;
    if after.incarnation() != head.incarnation()
        || request.until.is_some_and(|until| until.incarnation() != head.incarnation())
        || after < view.floor
    {
        return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired", "Resume from a fresh query"));
    }
    if request.limit == 0 || request.until.is_some_and(|until| until < after) {
        return Err(Anomaly::incorrect("rtdb/change-range", "Invalid Changes interval or limit"));
    }
    if after > head {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/change-basis-ahead", "The requested basis has not applied"));
    }
    view.index.view(&request.branch, At::Latest, Layer::Merged, now)?;
    let upper = request.until.unwrap_or(head).min(head);
    let requested = |table| request.tables.is_empty() || request.tables.contains(table);
    let entries = view.changes.iter().filter(|(cut, writes)| {
        cut.basis > after && cut.basis <= upper &&
            writes.iter().any(|write| write.branch == request.branch && requested(&write.table))
    });
    let fetch = request.limit.saturating_add(1);
    let mut entries = match request.order {
        ChangeOrder::Ascending => entries.take(fetch).collect::<Vec<_>>(),
        ChangeOrder::Descending => entries.rev().take(fetch).collect(),
    };
    let more = entries.len() > request.limit;
    let next = more.then(|| entries.pop().expect("one excess group").0.basis);
    let through = match (request.order, next) {
        (ChangeOrder::Ascending, Some(_)) => entries.last().expect("positive page limit").0.basis,
        (ChangeOrder::Ascending, None) => upper,
        (ChangeOrder::Descending, Some(next)) => next,
        (ChangeOrder::Descending, None) => after,
    };
    let groups = entries.into_iter().map(|(cut, writes)| {
        let before = view.index.before(cut.basis)?.and_then(|previous| {
            view.index.view(&request.branch, At::Exactly(previous.basis), Layer::Merged, cut.system_time).ok()
        });
        let after = view.index.view(&request.branch, At::Exactly(cut.basis), Layer::Merged, cut.system_time).ok();
        let mut rows = Vec::new();
        for write in writes.iter().filter(|write| write.branch == request.branch && requested(&write.table)) {
            append_change_rows(&mut rows, write, before.as_ref(), after.as_ref(), request.images)?;
        }
        Ok(ChangeGroup { cut: *cut, rows })
    }).collect::<Result<_>>()?;
    Ok(ChangePage { read_custody: None, scoped_custody: None, groups, through, complete: !more && request.until.is_none_or(|until| until <= head), custody_groups: BTreeSet::new(), key_generation: None })
}
