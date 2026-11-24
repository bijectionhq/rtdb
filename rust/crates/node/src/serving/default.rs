use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_index::branches::{BranchCut, BranchView, BranchWrite};
use rtdb_replication::{NATIVE_READ_FEATURES, NATIVE_WRITE_FEATURES, NativeServingHandle, NativeServingView};
use rtdb_tx::transact::Receipt;
use rtdb_types::{Anomaly, AnomalyCategory, Interval, Result, Timestamp, rtdb_v1::{At, Basis, Layer}};
use tokio_util::sync::CancellationToken;

use super::{ChangeGroup, ChangeImages, ChangeKind, ChangeOrder, ChangePage, ChangeRow, Changes,
    QuerySource, QueryView, ServingDriver, TransactSubmission};
use crate::Clock;

pub(crate) struct DefaultServing {
    pub(crate) handle: NativeServingHandle,
    pub(crate) memory: Arc<MemoryBudget>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) writable: bool,
}

#[async_trait]
impl ServingDriver for DefaultServing {
    fn incarnation(&self) -> [u8; 16] { self.handle.authority().incarnation }

    fn features(&self) -> &[&str] {
        if self.writable { NATIVE_WRITE_FEATURES } else { NATIVE_READ_FEATURES }
    }

    async fn query(
        &self, source: QuerySource, track: bool, now: Timestamp, cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        let snapshot = self.handle.view(source.at, cancellation).await?;
        let at = if source.at == At::Linearizable { At::Latest } else { source.at };
        let branch = snapshot.index.view(&source.branch, at, source.layer, now)?;
        let claim = track.then(|| rtdb_query::seal_branch_claim(&branch, &self.handle.authority().claims)).transpose()?;
        let snapshot = rtdb_query::branch_snapshot(&branch, &self.memory)?;
        Ok(QueryView { snapshot, cut: branch.cut(), claim })
    }

    async fn transact(&self, request: TransactSubmission, cancellation: &CancellationToken) -> Result<Receipt> {
        if !self.writable {
            return Err(Anomaly::unsupported("rtdb/default-serving-read-only", "The default serving attachment is read-only"));
        }
        self.handle.transact(request, cancellation).await
    }

    async fn tx_status(&self, key: &[u8], cancellation: &CancellationToken) -> Result<Receipt> {
        self.handle.tx_status(key.to_vec(), cancellation).await
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
                return Ok(page);
            }
            match tokio::time::timeout_at(deadline, self.handle.after(head, cancellation)).await {
                Ok(result) => result?,
                Err(_) => return Ok(page),
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
    Ok(ChangePage { groups, through, complete: !more && request.until.is_none_or(|until| until <= head) })
}

fn append_images(
    output: &mut Vec<ChangeRow>, write: &BranchWrite, before: Option<&BranchView<'_>>,
    after: Option<&BranchView<'_>>, images: ChangeImages,
) -> Result<()> {
    let make_row = |valid, kind, before, after| ChangeRow {
        table: write.table.clone(), id: write.id.clone(), valid, kind, before, after,
    };
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
            let image = matches!(images, ChangeImages::Before | ChangeImages::Both).then(|| previous.cells.clone());
            output.push(make_row(valid, ChangeKind::Closed, image, None));
        }
        if let Some(current) = current {
            let image = matches!(images, ChangeImages::After | ChangeImages::Both).then(|| current.cells.clone());
            output.push(make_row(valid, ChangeKind::Opened, None, image));
        } else if previous.is_none() {
            output.push(make_row(valid, ChangeKind::Closed, None, None));
        }
    }
    Ok(())
}
