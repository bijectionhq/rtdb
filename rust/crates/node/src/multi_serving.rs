use std::{collections::BTreeSet, net::IpAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use rtdb_crypt::tokens::TokenAuthority;
use rtdb_db::{ChangeHistory, MultiDatabase, Role, WritePipeline};
use rtdb_index::branches::{BranchCut, BranchEffect, BranchEvent, BranchView};
use rtdb_tx::transact::{LINEARIZABLE, Receipt, TIMED_TRANSACT};
use rtdb_types::{
    Anomaly, AnomalyCategory, Interval, MemoryBudget, Result, TableRef, Timestamp,
    rtdb_v1::{At, Basis, ClaimEvidence, ClaimScope, Layer},
};
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::serving::{
    ChangeGroup, ChangeImages, ChangeKind, ChangeOrder, ChangePage, ChangeRow, Changes,
    QuerySource, QueryView, ServingDriver, TransactSubmission,
};

pub struct MultiServing {
    database: Arc<Mutex<MultiDatabase>>,
    writes: WritePipeline,
    incarnation: [u8; 16],
    tokens: TokenAuthority,
    memory: Arc<MemoryBudget>,
}

impl MultiServing {
    pub fn new(database: MultiDatabase, memory: Arc<MemoryBudget>) -> Self {
        let incarnation = database.incarnation();
        let tokens = database.token_authority();
        let database = Arc::new(Mutex::new(database));
        let writes = WritePipeline::start(Arc::clone(&database));
        Self {
            incarnation,
            tokens,
            database,
            writes,
            memory,
        }
    }

    pub async fn tick(&self, live_nodes: &BTreeSet<IpAddr>) -> Result<Role> {
        self.writes.tick(live_nodes.clone()).await
    }
}

#[async_trait]
impl ServingDriver for MultiServing {
    fn incarnation(&self) -> [u8; 16] {
        self.incarnation
    }

    fn token_authority(&self) -> Option<TokenAuthority> { Some(self.tokens.clone()) }

    async fn apply_token_key_rotation(&self, generation: u64, unreadable_by: i64) -> Result<()> {
        self.database.lock().await.finish_token_rotation(generation, unreadable_by).await
    }

    async fn destroy_retired_token_key(&self, generation: u64) -> Result<()> {
        self.database.lock().await.destroy_retired_token_key(generation).await
    }

    fn features(&self) -> &[&str] {
        &[LINEARIZABLE, TIMED_TRANSACT]
    }

    async fn query(
        &self,
        source: QuerySource,
        track: bool,
        now: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<QueryView> {
        cancellation
            .run_until_cancelled(async {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                let at = if source.at == At::Linearizable {
                    At::Latest
                } else {
                    source.at
                };
                let view = index.view(&source.branch, at, source.layer, now)?;
                let cut = view.cut();
                let snapshot = rtdb_query::branch_snapshot(&view, &self.memory)?;
                let claim = if track {
                    Some(self.tokens.seal_claim(&ClaimEvidence {
                        scope: ClaimScope { branch: source.branch, basis: cut.basis, layer: source.layer },
                        footprint: Default::default(),
                    }).await?)
                } else {
                    None
                };
                self.database
                    .lock()
                    .await
                    .check_generation(generation)
                    .await?;
                Ok(QueryView {
                    snapshot,
                    cut,
                    claim,
                })
            })
            .await
            .ok_or_else(interrupted)?
    }

    async fn transact(
        &self,
        request: TransactSubmission,
        cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        cancellation
            .run_until_cancelled(self.writes.transact(request.transaction))
            .await
            .ok_or_else(interrupted)?
    }

    async fn tx_status(&self, key: &[u8], cancellation: &CancellationToken) -> Result<Receipt> {
        cancellation
            .run_until_cancelled(async { self.database.lock().await.tx_status(key).await })
            .await
            .ok_or_else(interrupted)?
    }

    async fn changes(
        &self,
        request: Changes,
        cancellation: &CancellationToken,
    ) -> Result<ChangePage> {
        if request.limit == 0 {
            return Err(Anomaly::incorrect(
                "rtdb/change-limit",
                "Changes requires a positive group limit",
            ));
        }
        cancellation
            .run_until_cancelled(async {
                let start = Instant::now();
                loop {
                    let page = {
                        let mut database = self.database.lock().await;
                        let history = database.fresh_history().await?;
                        changes_page(&history, &request, self.incarnation)?
                    };
                    let remaining = request.wait.saturating_sub(start.elapsed());
                    if !page.groups.is_empty()
                        || request.until.is_some() && page.complete
                        || remaining.is_zero()
                    {
                        return Ok(page);
                    }
                    tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
                }
            })
            .await
            .ok_or_else(interrupted)?
    }
}

fn changes_page(
    history: &ChangeHistory<'_>,
    request: &Changes,
    incarnation: [u8; 16],
) -> Result<ChangePage> {
    let position = |basis: Basis| {
        if basis.incarnation() != incarnation {
            return Err(Anomaly::incorrect(
                "rtdb/foreign-basis",
                "The basis belongs to another database incarnation",
            ));
        }
        Ok(basis.position())
    };
    let floor = history.floor.0 - 1;
    let after = request.after.map(position).transpose()?.unwrap_or(floor);
    if after < floor {
        return Err(Anomaly::new(
            AnomalyCategory::NotFound,
            "rtdb/cursor-expired",
            "The changes cursor is below the retained floor",
        ));
    }
    let until = request.until.map(position).transpose()?;
    if until.is_some_and(|until| until < after) {
        return Err(Anomaly::incorrect(
            "rtdb/change-range",
            "The change interval is inverted",
        ));
    }
    let upper = until.unwrap_or(u64::MAX).min(history.head.0);
    let index = history.index;
    index.view(&request.branch, At::Latest, Layer::Merged, history.now)?;
    let descending = request.order == ChangeOrder::Descending;
    let groups = history.groups.iter();
    let order: Box<dyn Iterator<Item = _>> = if descending {
        Box::new(groups.rev())
    } else {
        Box::new(groups)
    };
    let mut page = Vec::new();
    let mut has_more = false;
    for group in order
        .filter(|group| group.cut.basis.position() > after && group.cut.basis.position() <= upper)
    {
        let before = index
            .view(
                &request.branch,
                At::Exactly(group.previous.basis),
                Layer::Merged,
                group.cut.system_time,
            )
            .ok();
        let mut rows = Vec::new();
        for effect in &group.effects {
            match effect {
                BranchEffect::Write {
                    branch,
                    table,
                    event,
                } if branch == &request.branch && matches_table(request, table) => {
                    rows.extend(change_rows(table, event, before.as_ref(), request.images)?);
                }
                BranchEffect::Erase { branch, table, id }
                    if branch
                        .as_ref()
                        .is_none_or(|branch| branch == &request.branch)
                        && matches_table(request, table) =>
                {
                    rows.push(ChangeRow {
                        table: table.clone(),
                        id: id.clone(),
                        valid: None,
                        kind: ChangeKind::Erased,
                        before: None,
                        after: None,
                    });
                }
                _ => {}
            }
        }
        if rows.is_empty() {
            continue;
        }
        if page.len() == request.limit {
            has_more = true;
            break;
        }
        page.push(ChangeGroup {
            cut: group.cut,
            rows,
        });
    }
    let through = match (descending, has_more, page.last()) {
        (true, true, Some(group)) => group.cut.basis.position().saturating_sub(1),
        (false, true, Some(group)) => group.cut.basis.position(),
        (true, _, _) => after,
        _ => upper.max(after),
    };
    Ok(ChangePage {
        groups: page,
        through: Basis::new(incarnation, through),
        complete: !has_more && until.is_none_or(|until| until <= history.head.0),
    })
}

fn matches_table(request: &Changes, table: &TableRef) -> bool {
    request.tables.is_empty() || request.tables.contains(table)
}

fn change_rows(
    table: &TableRef,
    event: &BranchEvent,
    before: Option<&BranchView<'_>>,
    images: ChangeImages,
) -> Result<Vec<ChangeRow>> {
    let lower = event.valid.lower();
    let upper = event.valid.upper();
    let mut boundaries = BTreeSet::from([lower, upper]);
    let mut insert = |timestamp| {
        if lower < timestamp && timestamp < upper {
            boundaries.insert(timestamp);
        }
    };
    for previous in before
        .into_iter()
        .flat_map(|view| view.history(table, &event.id))
    {
        insert(previous.valid.lower());
        insert(previous.valid.upper());
        if let Some(until) = previous.image.as_ref().and_then(|image| image.expires_at) {
            insert(until);
        }
    }
    if let Some(until) = event.image.as_ref().and_then(|image| image.expires_at) {
        insert(until);
    }
    boundaries
        .iter()
        .zip(boundaries.iter().skip(1))
        .map(|(&from, &to)| {
            let prior = if matches!(images, ChangeImages::Before | ChangeImages::Both) {
                before
                    .and_then(|view| view.row(table, &event.id, from))
                    .and_then(|event| event.image_at(from))
                    .map(|image| image.cells.clone())
            } else {
                None
            };
            let after = if matches!(images, ChangeImages::After | ChangeImages::Both) {
                event.image_at(from).map(|image| image.cells.clone())
            } else {
                None
            };
            Ok(ChangeRow {
                table: table.clone(),
                id: event.id.clone(),
                valid: Some(Interval::new(from, to)?),
                kind: if event.image.is_some() {
                    ChangeKind::Opened
                } else {
                    ChangeKind::Closed
                },
                before: prior,
                after,
            })
        })
        .collect()
}

fn interrupted() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Interrupted,
        "rtdb/request-cancelled",
        "The request was cancelled",
    )
}

#[cfg(test)]
mod tests {
    use journal_contract::{Identity, MemoryJournal};
    use rtdb_crypt::LocalKms;
    use rtdb_db::MultiOptions;
    use rtdb_query::{
        CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions,
    };
    use rtdb_storage::cache::BufferPool;
    use rtdb_store::MemoryStore;
    use rtdb_tx::transact::{Cell, Op, Outcome, Row, Transact, Window};
    use rtdb_types::{
        EntityId, StructFields, Value,
        rtdb_v1::MAIN_BRANCH,
    };
    use uuid::Uuid;

    use super::*;

    fn write(key: u8, title: &str, before: Option<Timestamp>) -> TransactSubmission {
        let transaction = Transact {
            key: vec![key; 16],
            require: vec![],
            window: Window {
                not_before: None,
                before,
            },
            guards: vec![],
            ops: vec![Op::Put {
                branch: MAIN_BRANCH.into(),
                table: TableRef::new("public", "notes"),
                rows: vec![Row {
                    id: EntityId::Integer(1),
                    valid: None,
                    cells: StructFields::from([(
                        "title".into(),
                        Cell::Value(Value::Utf8(title.into())),
                    )]),
                }],
                if_absent: false,
            }],
            otherwise: vec![],
        };
        TransactSubmission { transaction, attempt: Uuid::from_u128(u128::from(key)), submitted_at: Timestamp(10), source: Default::default() }
    }

    #[tokio::test]
    async fn multi_serving_uses_the_settled_cut_for_queries_and_change_pages() {
        let journal = MemoryJournal::default();
        let id = Uuid::from_u128(1);
        let node = IpAddr::from([127, 0, 0, 1]);
        let database = MultiDatabase::new(
            MultiOptions {
                database: id,
                node,
                lease_micros: 100,
                drift_micros: 1,
            },
            Arc::new(journal.handle(Identity(1))),
            Arc::new(MemoryStore::default()),
            Arc::new(LocalKms::new(|| 10)),
            Arc::new(|| Timestamp(10)),
        )
        .unwrap();
        database.create().await.unwrap();
        let driver = MultiServing::new(database, MemoryBudget::new(64 * 1024 * 1024));
        driver.tick(&BTreeSet::from([node])).await.unwrap();
        let cancel = CancellationToken::new();
        let first = driver
            .transact(write(1, "one", None), &cancel)
            .await
            .unwrap();
        let second = driver
            .transact(write(2, "two", Some(Timestamp(20))), &cancel)
            .await
            .unwrap();
        assert_eq!(second.outcome, Outcome::Then);
        assert_eq!(second.basis.position(), first.basis.position() + 2);
        assert!(driver.tx_status(&[2; 16], &cancel).await.unwrap().replayed);

        let view = driver
            .query(
                QuerySource {
                    branch: MAIN_BRANCH.into(),
                    at: At::Linearizable,
                    layer: Layer::Merged,
                },
                true,
                Timestamp(11),
                &cancel,
            )
            .await
            .unwrap();
        assert_eq!(view.cut.basis, second.basis);
        assert!(view.claim.is_some());
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let catalog = CatalogSnapshot::new(
            "xtdb",
            std::collections::BTreeMap::from([(
                "xtdb".into(),
                DatabaseSnapshot {
                    partitions: vec![view.snapshot],
                    storage,
                },
            )]),
            false,
        )
        .unwrap();
        let prepared = rtdb_sql::prepare_in(
            "SELECT title FROM notes",
            &catalog,
            &Default::default(),
            &driver.memory,
        )
        .unwrap();
        let context = QueryContext::new(
            catalog,
            Arc::clone(&driver.memory),
            Timestamp(11),
            "UTC",
            QueryOptions::default(),
            &cancel,
        )
        .unwrap();
        let mut cursor = PreparedQuery::from_prepared(prepared, &driver.memory)
            .unwrap()
            .execute(context, Vec::new())
            .await
            .unwrap();
        let result = cursor.next_batch().await.unwrap().unwrap();
        let batch = result.batch();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            rtdb_columnar::read_value(batch.schema().field(0), batch.column(0).as_ref(), 0)
                .unwrap(),
            Value::Utf8("two".into())
        );

        let changes = || Changes {
            branch: MAIN_BRANCH.into(),
            tables: BTreeSet::new(),
            after: Some(first.basis),
            until: Some(second.basis),
            order: ChangeOrder::Ascending,
            images: ChangeImages::Both,
            limit: 1,
            wait: Duration::ZERO,
        };
        let page = driver.changes(changes(), &cancel).await.unwrap();
        assert!(page.complete);
        assert_eq!(page.through, second.basis);
        assert_eq!(page.groups.len(), 1);
        let row = &page.groups[0].rows[0];
        assert_eq!(
            row.before.as_ref().unwrap()["title"],
            Value::Utf8("one".into())
        );
        assert_eq!(
            row.after.as_ref().unwrap()["title"],
            Value::Utf8("two".into())
        );

        driver
            .database
            .lock()
            .await
            .checkpoint(vec![])
            .await
            .unwrap();
        let error = driver.changes(changes(), &cancel).await.err().unwrap();
        assert_eq!(error.code, "rtdb/cursor-expired");
    }
}
