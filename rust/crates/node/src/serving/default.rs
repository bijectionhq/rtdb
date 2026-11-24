use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_crypt::tokens::VerifiedClaims;
use rtdb_index::branches::{BranchView, BranchWrite};
use rtdb_replication::{NATIVE_READ_FEATURES, NATIVE_WRITE_FEATURES, NativeServingHandle, NativeServingScope, NativeServingView};
use rtdb_tx::transact::{CustodyGroupId, ROW_CUSTODY, Receipt, Transact};
use rtdb_types::{Anomaly, AnomalyCategory, Interval, Result, Timestamp, rtdb_v1::{At, Basis, ClaimEvidence, ClaimScope, Layer}};
use tokio_util::sync::CancellationToken;

use super::{ChangeGroup, ChangeImages, ChangeKind, ChangeOrder, ChangePage, ChangeRow, Changes,
    QuerySource, QueryView, ServingDriver, ServingScope};
use crate::Clock;

pub(crate) struct DefaultServing {
    pub(crate) handle: NativeServingHandle,
    pub(crate) memory: Arc<MemoryBudget>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) tokens: Arc<crate::database::tokens::DatabaseTokens>,
    pub(crate) writable: bool,
}

impl DefaultServing {
    async fn open_change_page(&self, view: &NativeServingView, page: ChangePage) -> Result<ChangePage> {
        if !view.index.has_custody_groups() {
            return Ok(page);
        }
        let vault = self.tokens.authority().await?.custody_vault();
        super::custody::open_changes(&view.index, &vault, page).await
    }
}

#[async_trait]
impl ServingDriver for DefaultServing {
    fn incarnation(&self) -> [u8; 16] { self.handle.authority().incarnation }
    fn uses_database_tokens(&self) -> bool { true }
    fn supports_row_visibility(&self) -> bool { self.handle.is_compatible() && self.tokens.supported() }

    fn features(&self) -> &[&str] {
        if !self.handle.is_compatible() { return &[]; }
        if self.writable { NATIVE_WRITE_FEATURES } else { NATIVE_READ_FEATURES }
    }

    async fn query(
        &self, source: QuerySource, track: bool, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<QueryView> {
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
        let (snapshot, custody_groups) = if snapshot.index.has_custody_groups() {
            let vault = self.tokens.authority().await?.custody_vault();
            super::custody::snapshot(&branch, &self.memory, &vault).await?
        } else { (rtdb_query::branch_snapshot(&branch, &self.memory)?, BTreeSet::new()) };
        Ok(QueryView { snapshot, cut, claim, custody_groups })
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

    async fn check_custody_delivery(&self, groups: &BTreeSet<CustodyGroupId>, cancellation: &CancellationToken) -> Result<()> {
        if groups.is_empty() { return Ok(()); }
        let view = self.handle.view(At::Linearizable, cancellation).await?;
        let vault = self.tokens.authority().await?.custody_vault();
        super::custody::check_delivery(&view.index, &vault, groups).await
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
                    if track {
                        view.claim = Some(self.tokens.authority().await?.issuer().await?
                            .seal_scoped_claim(raw, &scope.labels).await?);
                    }
                    return Ok(view);
                }
                Err(error) if matches!(source.at, At::AtLeast(_)) && error.code == "rtdb/basis-ahead" => {
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
    let entries = view.changes.iter().filter(|(cut, writes)| {
        cut.basis > after && cut.basis <= upper &&
            writes.iter().any(|write| write.branch == request.branch && request.tables.contains(&write.table))
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
            view.index.view(&request.branch, At::Exactly(previous.basis), Layer::Merged, previous.system_time).ok()
        });
        let after = view.index.view(&request.branch, At::Exactly(cut.basis), Layer::Merged, cut.system_time).ok();
        let mut rows = Vec::new();
        for write in writes.iter().filter(|write| write.branch == request.branch && request.tables.contains(&write.table)) {
            append_images(&mut rows, write, before.as_ref(), after.as_ref(), request.images)?;
        }
        Ok(ChangeGroup { cut: *cut, rows })
    }).collect::<Result<_>>()?;
    Ok(ChangePage { groups, through, complete: !more && request.until.is_none_or(|until| until <= head), custody_groups: BTreeSet::new() })
}

fn append_images(
    output: &mut Vec<ChangeRow>, write: &BranchWrite, before: Option<&BranchView<'_>>,
    after: Option<&BranchView<'_>>, images: ChangeImages,
) -> Result<()> {
    let make_row = |valid, kind, before, after| ChangeRow {
        table: write.table.clone(), id: write.id.clone(), valid, kind, before, after,
        erased_written_basis: write.erased_written_basis,
    };
    if write.erased_written_basis.is_some() {
        output.push(make_row(write.valid, ChangeKind::Erased, None, None));
        return Ok(());
    }
    let Some(valid) = write.valid else {
        output.push(make_row(None, ChangeKind::Erased, None, None));
        return Ok(());
    };
    let mut boundaries = BTreeSet::from([valid.lower(), valid.upper()]);
    for view in before.into_iter().chain(after) {
        for event in view.history(&write.table, &write.id) {
            for boundary in [Some(event.valid.lower()), Some(event.valid.upper()), event.image.as_ref().and_then(|image| image.expires_at)].into_iter().flatten() {
                if valid.contains(boundary) { boundaries.insert(boundary); }
            }
        }
    }
    let starts = boundaries.iter().copied();
    for (from, to) in starts.clone().zip(starts.skip(1)) {
        let valid = Some(Interval::new(from, to)?);
        let previous = before.and_then(|view| view.row(&write.table, &write.id, from)).and_then(|event| event.image_at(from));
        let current = after.and_then(|view| view.row(&write.table, &write.id, from)).and_then(|event| event.image_at(from));
        if let Some(previous) = previous {
            let image = matches!(images, ChangeImages::Before | ChangeImages::Both).then(|| previous.clone());
            output.push(make_row(valid, ChangeKind::Closed, image, None));
        }
        if let Some(current) = current {
            let image = matches!(images, ChangeImages::After | ChangeImages::Both).then(|| current.clone());
            output.push(make_row(valid, ChangeKind::Opened, None, image));
        } else if previous.is_none() {
            output.push(make_row(valid, ChangeKind::Closed, None, None));
        }
    }
    Ok(())
}
