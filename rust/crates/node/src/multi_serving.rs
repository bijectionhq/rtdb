use std::{collections::BTreeSet, net::IpAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use rtdb_crypt::tokens::TokenAuthority;
use rtdb_db::{ChangeHistory, MultiDatabase, Role, WritePipeline};
use rtdb_index::branches::{BranchCut, BranchEffect, BranchIndex, BranchWrite};
use rtdb_tx::transact::{CustodyGroupId, LINEARIZABLE, PHYSICAL_ERASURE, RELATIVE_VALID_TIME, ROW_CUSTODY, Receipt, TIMED_TRANSACT, TRANSACT_REQUIRES, Transact};
use rtdb_types::{
    Anomaly, AnomalyCategory, MemoryBudget, Result, TableRef, Timestamp,
    rtdb_v1::{At, Basis, ClaimEvidence, ClaimScope, Layer},
};
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::serving::{
    ChangeGroup, ChangeOrder, ChangePage, Changes, QuerySource, QueryView, ROW_VISIBILITY,
    ServingDriver, ServingScope, append_change_rows,
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
        &[LINEARIZABLE, TIMED_TRANSACT, TRANSACT_REQUIRES, PHYSICAL_ERASURE, RELATIVE_VALID_TIME, ROW_VISIBILITY, "query", "query.track", "query.page", rtdb_types::rtdb_v1::QUERY_WRITTEN_BASIS,
          "query.linearizable", "branches", "branch.local-layer", "changes", "changes.progress-only", "transact", "tx-status", "sql.history"]
    }

    fn supports_row_visibility(&self) -> bool { true }

    async fn query_scoped(&self, source: QuerySource, track: bool, now: Timestamp,
                          scope: &ServingScope, cancellation: &CancellationToken) -> Result<QueryView> {
        cancellation.run_until_cancelled(async {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                let vault = index.has_custody_groups().then(|| self.tokens.custody_vault());
                match scope.query(&index, source.clone(), now, &self.memory, vault.as_ref()).await {
                    Ok((mut view, raw)) => {
                        if track {
                            view.claim = Some(self.tokens.issuer().await?.seal_scoped_claim(raw, &scope.labels).await?);
                        }
                        self.database.lock().await.check_generation(generation).await?;
                        view.key_generation = Some(generation);
                        return Ok(view);
                    }
                    Err(error) if error.code == "rtdb/basis-ahead" && Instant::now() < deadline => {
                        tokio::time::sleep_until((Instant::now() + Duration::from_millis(50)).min(deadline)).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        }).await.ok_or_else(interrupted)?
    }

    async fn transact_scoped(&self, request: Transact, scope: ServingScope,
                             cancellation: &CancellationToken) -> Result<Receipt> {
        cancellation.run_until_cancelled(self.writes.transact_scoped(
            request, scope.labels, scope.receipt_namespace,
        )).await.ok_or_else(interrupted)?
    }

    async fn tx_status_scoped(&self, key: &[u8], scope: &ServingScope,
                              cancellation: &CancellationToken) -> Result<Receipt> {
        cancellation.run_until_cancelled(async {
            self.database.lock().await.tx_status_scoped(key, scope.transaction_scope()).await
        }).await.ok_or_else(interrupted)?
    }

    async fn changes_scoped(&self, request: Changes, scope: &ServingScope,
                            cancellation: &CancellationToken) -> Result<ChangePage> {
        cancellation.run_until_cancelled(async {
            let start = Instant::now();
            loop {
                let (index, generation) = self.database.lock().await.fresh_view().await?;
                let mut page = scope.changes(&index, &request)?;
                let remaining = request.wait.saturating_sub(start.elapsed());
                if !page.groups.is_empty() || request.until.is_some() && page.complete || remaining.is_zero() {
                    self.database.lock().await.check_generation(generation).await?;
                    page.key_generation = Some(generation);
                    if index.has_custody_groups() {
                        return crate::serving::custody::open_changes(&index, &self.tokens.custody_vault(), page).await;
                    }
                    return Ok(page);
                }
                tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
            }
        }).await.ok_or_else(interrupted)?
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
                let (snapshot, custody_groups) = if index.has_custody_groups() {
                    crate::serving::custody::snapshot(&view, &self.memory, &self.tokens.custody_vault()).await?
                } else { (rtdb_query::branch_snapshot(&view, &self.memory)?, BTreeSet::new()) };
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
                    custody_groups,
                    key_generation: Some(generation),
                })
            })
            .await
            .ok_or_else(interrupted)?
    }

    async fn transact(
        &self,
        mut request: Transact,
        cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        cancellation
            .run_until_cancelled(async {
                if request.uses_custody() && self.features().contains(&ROW_CUSTODY) {
                    crate::serving::custody::prepare(&mut request, &self.tokens.custody_vault()).await?;
                }
                self.writes.transact(request).await
            })
            .await
            .ok_or_else(interrupted)?
    }

    async fn check_custody_delivery(&self, groups: &BTreeSet<CustodyGroupId>, cancellation: &CancellationToken) -> Result<()> {
        self.check_read_delivery(None, groups, cancellation).await
    }

    async fn check_read_delivery(&self, generation: Option<u64>, groups: &BTreeSet<CustodyGroupId>, cancellation: &CancellationToken) -> Result<()> {
        if generation.is_none() && groups.is_empty() { return Ok(()); }
        cancellation.run_until_cancelled(async {
            let mut database = self.database.lock().await;
            if !groups.is_empty() {
                let index = database.fresh_index().await?;
                crate::serving::custody::check_delivery(index, &self.tokens.custody_vault(), groups).await?;
            }
            if let Some(generation) = generation { database.check_generation(generation).await?; }
            Ok(())
        }).await.ok_or_else(interrupted)?
    }

    async fn current_custody(&self, cancellation: &CancellationToken) -> Result<BranchIndex> {
        cancellation.run_until_cancelled(async {
            let (index, generation) = self.database.lock().await.fresh_view().await?;
            self.database.lock().await.check_generation(generation).await?;
            Ok(index)
        }).await.ok_or_else(interrupted)?
    }

    async fn reconcile_custody(&self, cancellation: &CancellationToken) -> Result<()> {
        cancellation.run_until_cancelled(async {
            let (index, _) = self.database.lock().await.fresh_view().await?;
            crate::serving::custody::reconcile_revocations(&index, &self.tokens.custody_vault(), crate::Clock::now(&crate::SystemClock)).await
        }).await.ok_or_else(interrupted)?
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
                    let mut database = self.database.lock().await;
                    let history = database.fresh_history().await?;
                    let page = changes_page(&history, &request, self.incarnation)?;
                    let remaining = request.wait.saturating_sub(start.elapsed());
                    if !page.groups.is_empty() || request.until.is_some() && page.complete || remaining.is_zero() {
                        let index = history.index.has_custody_groups().then(|| history.index.clone());
                        drop(database);
                        return match index {
                            Some(index) => crate::serving::custody::open_changes(&index, &self.tokens.custody_vault(), page).await,
                            None => Ok(page),
                        };
                    }
                    drop(database);
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
    let floor = history.after_floor;
    let after = request.after.map(position).transpose()?.unwrap_or(0);
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
    if after > history.through {
        return Err(Anomaly::new(
            AnomalyCategory::Unavailable,
            "rtdb/change-basis-ahead",
            "The requested basis has not applied",
        ));
    }
    let upper = until.unwrap_or(u64::MAX).min(history.through);
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
    let mut next = None;
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
        let after = index.view(&request.branch, At::Exactly(group.cut.basis), Layer::Merged, group.cut.system_time).ok();
        let mut rows = Vec::new();
        for effect in &group.effects {
            match effect {
                BranchEffect::Write {
                    branch,
                    table,
                    event,
                } if branch == &request.branch && matches_table(request, table) => {
                    let write = BranchWrite {
                        branch: branch.clone(), table: table.clone(), id: event.id.clone(),
                        valid: Some(event.valid), erased_written_basis: None,
                    };
                    append_change_rows(&mut rows, &write, before.as_ref(), after.as_ref(), request.images)?;
                }
                BranchEffect::Erase { branch, table, id }
                    if branch
                        .as_ref()
                        .is_none_or(|branch| branch == &request.branch)
                        && matches_table(request, table) =>
                {
                    let write = BranchWrite {
                        branch: request.branch.clone(),
                        table: table.clone(),
                        id: id.clone(),
                        valid: None,
                        erased_written_basis: None,
                    };
                    append_change_rows(&mut rows, &write, None, None, request.images)?;
                }
                BranchEffect::RevokeCustodyGroup { group: id } => {
                    for write in index.custody_revocation_writes(*id, group.cut)?.into_iter().filter(|write| {
                        write.branch == request.branch && matches_table(request, &write.table)
                    }) {
                        append_change_rows(&mut rows, &write, None, None, request.images)?;
                    }
                }
                _ => {}
            }
        }
        if rows.is_empty() {
            continue;
        }
        if page.len() == request.limit {
            next = Some(group.cut.basis.position());
            break;
        }
        page.push(ChangeGroup {
            cut: group.cut,
            rows,
        });
    }
    let through = match (descending, next, page.last()) {
        (true, Some(next), _) => next,
        (false, Some(_), Some(group)) => group.cut.basis.position(),
        (true, None, _) => after,
        _ => upper.max(after),
    };
    Ok(ChangePage {
        groups: page,
        through: Basis::new(incarnation, through),
        complete: next.is_none() && until.is_none_or(|until| until <= history.through),
        custody_groups: BTreeSet::new(),
        key_generation: Some(history.generation),
    })
}

fn matches_table(request: &Changes, table: &TableRef) -> bool {
    request.tables.is_empty() || request.tables.contains(table)
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
    use rtdb_tx::transact::{Cell, Op, Outcome, Row, Window};
    use rtdb_types::{
        EntityId, Interval, StructFields, Value,
        rtdb_v1::MAIN_BRANCH,
    };
    use uuid::Uuid;

    use super::*;
    use crate::serving::{ChangeImages, ChangeKind};

    fn write(key: u8, title: &str, before: Option<Timestamp>) -> Transact {
        Transact {
            custody_request_digest: None,
            request_memory: None,
            key: vec![key; 16],
            require: vec![],
            window: Window {
                not_before: None,
                before,
            },
            guards: vec![],
            requires: vec![],
            ops: vec![Op::Put {
                branch: MAIN_BRANCH.into(),
                table: TableRef::new("public", "notes"),
                rows: vec![Row {
                    id: EntityId::Integer(1),
                    valid: None,
                    custody_group: None,
                    protected_columns: Vec::new(),
                    custody_payload: None,
                    cells: StructFields::from([(
                        "title".into(),
                        Cell::Value(Value::Utf8(title.into())),
                    )]),
                }],
                if_absent: false,
            }],
            otherwise: vec![],
        }
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
        let [closed, opened] = page.groups[0].rows.as_slice() else { panic!("expected a closure and an opening"); };
        assert!(matches!(closed.kind, ChangeKind::Closed));
        assert!(matches!(opened.kind, ChangeKind::Opened));
        assert_eq!(
            closed.before.as_ref().unwrap().cells["title"],
            Value::Utf8("one".into())
        );
        assert_eq!(
            opened.after.as_ref().unwrap().cells["title"],
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
        let from_beginning = Changes { after: None, ..changes() };
        let error = driver.changes(from_beginning, &cancel).await.err().unwrap();
        assert_eq!(error.code, "rtdb/cursor-expired");
    }

    #[tokio::test]
    async fn multi_serving_scoped_requires_preserves_projected_coordinates() {
        use rtdb_tx::transact::{Declaration, Guard, REQUIREMENT_FAILED, Version, Visibility};
        use rtdb_types::rtdb_v1::LabelScope;

        let journal = MemoryJournal::default();
        let id = Uuid::from_u128(1);
        let node = IpAddr::from([127, 0, 0, 1]);
        let database = MultiDatabase::new(
            MultiOptions { database: id, node, lease_micros: 100, drift_micros: 1 },
            Arc::new(journal.handle(Identity(1))), Arc::new(MemoryStore::default()),
            Arc::new(LocalKms::new(|| 10)), Arc::new(|| Timestamp(10)),
        ).unwrap();
        database.create().await.unwrap();
        let driver = MultiServing::new(database, MemoryBudget::new(64 * 1024 * 1024));
        driver.tick(&BTreeSet::from([node])).await.unwrap();
        assert!(driver.features().contains(&TRANSACT_REQUIRES));
        let cancel = CancellationToken::new();
        let mut declared = write(1, "declaration", None);
        declared.ops = vec![Op::Declare {
            table: TableRef::new("public", "notes"), change: Declaration::Visibility(Visibility { column: "label".into() }),
        }];
        assert_eq!(driver.transact(declared, &cancel).await.unwrap().outcome, Outcome::Then);
        let labelled = |key, label, id| {
            let mut submission = write(key, "row", None);
            let Op::Put { rows, .. } = &mut submission.ops[0] else { unreachable!() };
            rows[0].id = EntityId::Integer(id);
            rows[0].valid = Some(Interval::ALL.into());
            rows[0].cells.insert("label".into(), Cell::Value(Value::Binary(vec![label])));
            submission
        };
        let scope = ServingScope {
            labels: LabelScope { read: BTreeSet::from([vec![1]]), write: BTreeSet::from([vec![1]]) },
            receipt_namespace: [7; 32],
        };
        let receipt = driver.transact_scoped(labelled(2, 1, 1), scope.clone(), &cancel).await.unwrap();
        assert_eq!(receipt.outcome, Outcome::Then);
        let raw = driver.transact(labelled(3, 2, 2), &cancel).await.unwrap();
        let source = QuerySource { branch: MAIN_BRANCH.into(), at: At::Linearizable, layer: Layer::Merged };
        let view = driver.query_scoped(source, true, Timestamp(11), &scope, &cancel).await.unwrap();
        assert_eq!(view.cut.basis, receipt.basis);
        assert_ne!(view.cut.basis.incarnation(), raw.basis.incarnation());
        let claim = view.claim.unwrap();
        let changes = || Changes {
            branch: MAIN_BRANCH.into(), tables: BTreeSet::new(), after: Some(receipt.basis), until: None,
            order: ChangeOrder::Ascending, images: ChangeImages::Both, limit: 1, wait: Duration::ZERO,
        };
        let page = driver.changes_scoped(changes(), &scope, &cancel).await.unwrap();
        assert_eq!(page.through, receipt.basis);
        assert!(page.groups.is_empty());
        let status = driver.tx_status_scoped(&[2; 16], &scope, &cancel).await.unwrap();
        assert_eq!(status.basis, receipt.basis);
        assert!(status.replayed);
        assert!(driver.tx_status(&[2; 16], &cancel).await.is_err());

        let version = |id, expect| Guard::Version {
            branch: MAIN_BRANCH.into(), table: TableRef::new("public", "notes"),
            id: EntityId::Integer(id), valid_at: None, expect,
        };
        let mut transaction = labelled(4, 1, 3);
        transaction.require.push(TRANSACT_REQUIRES.into());
        transaction.requires = vec![
            Guard::Unchanged(claim),
            version(1, Version::Written(receipt.basis)),
            version(2, Version::Absent),
        ];
        transaction.guards = vec![version(3, Version::Present)];
        transaction.otherwise = labelled(5, 1, 4).ops;

        let refused = driver.transact_scoped(transaction.clone(), scope.clone(), &cancel).await.unwrap();
        assert_eq!(refused.outcome, Outcome::Refused {
            error: Anomaly::new(AnomalyCategory::Conflict, REQUIREMENT_FAILED, "Requirement 2 failed"),
        });
        assert_eq!(refused.basis, receipt.basis);
        assert_eq!(refused.system_time, receipt.system_time);
        assert!(driver.changes_scoped(changes(), &scope, &cancel).await.unwrap().groups.is_empty());
        let replayed = Receipt { replayed: true, ..refused };
        let retry = driver.transact_scoped(transaction.clone(), scope.clone(), &cancel).await.unwrap();
        assert_eq!(retry, replayed);
        assert_eq!(driver.tx_status_scoped(&transaction.key, &scope, &cancel).await.unwrap(), replayed);
        assert_eq!(driver.tx_status(&transaction.key, &cancel).await.unwrap_err().code, "rtdb/unknown-transaction");

        transaction.key = vec![5; 16];
        transaction.requires.truncate(2);
        let selected = driver.transact_scoped(transaction, scope.clone(), &cancel).await.unwrap();
        assert_eq!(selected.outcome, Outcome::Otherwise { failed: vec![0] });
        let page = driver.changes_scoped(changes(), &scope, &cancel).await.unwrap();
        assert_eq!(page.through, selected.basis);
        let [group] = page.groups.as_slice() else { panic!("expected one change group"); };
        let [row] = group.rows.as_slice() else { panic!("expected one changed row"); };
        assert_eq!(row.id, EntityId::Integer(4));
    }
}
