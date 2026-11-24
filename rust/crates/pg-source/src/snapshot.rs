use std::{collections::{BTreeSet, VecDeque}, sync::Arc};
use rtdb_columnar::MemoryBudget;
use rtdb_types::{Anomaly, AnomalyCategory, Result, TableRef};
use crate::{decoder::DecodedTransaction, Lsn, Remote, SourceConfig, config::{identifier, literal}, decoder::snapshot_rows, vector_types::VectorTypes, wire::{Connection, QueryResult, SnapshotFetch, protocol}};

pub(crate) async fn publication_exists(remote: &Remote, config: &SourceConfig, memory: Arc<MemoryBudget>) -> Result<bool> {
    let mut connection = Connection::open(remote, false, memory).await?;
    let result = connection.query(&format!("SELECT 1 FROM pg_publication WHERE pubname = {}", literal(&config.publication_name)?)).await?;
    Ok(!result.rows.is_empty())
}
struct SnapshotTable { table: TableRef, fetch: SnapshotFetch }

pub(crate) struct SnapshotReader {
    _slot: Connection, query: Connection, vectors: VectorTypes, pub lsn: Lsn, ignored: BTreeSet<TableRef>, tables: VecDeque<TableRef>, table_page: Option<QueryResult>, table: Option<SnapshotTable>, tables_finished: bool, memory: Arc<MemoryBudget>,
}
impl SnapshotReader {
    pub async fn open(remote: &Remote, config: &SourceConfig, memory: Arc<MemoryBudget>) -> Result<Self> {
        let mut slot = Connection::open(remote, true, Arc::clone(&memory)).await?;
        let result = slot.query(&format!("CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER)", identifier(&config.slot_name)?)).await.map_err(|error| {
            let duplicate = error.context.get("sql-state").and_then(serde_json::Value::as_str) == Some("42710");
            Anomaly::new_nullable(if duplicate { AnomalyCategory::Conflict } else { AnomalyCategory::Incorrect }, if duplicate { "xtdb.postgres/slot-exists" } else { "xtdb.postgres/slot-creation-failed" }, error.message).with_context("slot-name", config.slot_name.clone().into())
        })?;
        let row = result.rows.first().ok_or_else(|| Anomaly::fault("xtdb.postgres/slot-creation-no-result", "CREATE_REPLICATION_SLOT returned no slot"))?;
        let lsn = row.cell(1)?.ok_or_else(|| protocol("Slot has no consistent point"))?.parse()?;
        let snapshot = row.cell(2)?.ok_or_else(|| protocol("Slot has no exported snapshot"))?.to_owned();
        let mut query = Connection::open(remote, false, Arc::clone(&memory)).await?;
        query.query("BEGIN ISOLATION LEVEL REPEATABLE READ").await?;
        query.query(&format!("SET TRANSACTION SNAPSHOT {}", literal(&snapshot)?)).await?;
        query.query("SET LOCAL IntervalStyle = 'iso_8601'").await?;
        query.query(&format!("DECLARE rtdb_tables NO SCROLL CURSOR FOR SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = {} ORDER BY schemaname, tablename", literal(&config.publication_name)?)).await?;
        let vectors = VectorTypes::load(&mut query, &memory).await?;
        Ok(Self { _slot: slot, query, vectors, lsn, ignored: config.ignore_tables.clone(), tables: VecDeque::new(), table_page: None, table: None, tables_finished: false, memory })
    }
    pub async fn next_batch(&mut self) -> Result<Option<DecodedTransaction>> {
        loop {
            if let Some(SnapshotTable { table, fetch }) = &self.table {
                let result = self.query.snapshot_fetch(fetch).await?;
                if result.rows.is_empty() { self.query.query("CLOSE rtdb_rows").await?; self.table = None; continue; }
                let changes = snapshot_rows(table, &result.columns, &result.rows, &self.memory, &self.vectors)?;
                return Ok(Some(DecodedTransaction::new(self.lsn, None, vec![changes])));
            }
            if self.tables.is_empty() && !self.tables_finished {
                let result = self.query.query("FETCH FORWARD 1000 FROM rtdb_tables").await?;
                self.tables_finished = result.rows.is_empty();
                for row in &result.rows { let table = TableRef::new(row.cell(0)?.ok_or_else(|| protocol("Publication schema is null"))?, row.cell(1)?.ok_or_else(|| protocol("Publication table is null"))?); if !self.ignored.contains(&table) { self.tables.push_back(table); } }
                self.table_page = Some(result);
            }
            match self.tables.pop_front() {
                Some(table) => { self.query.query(&format!("DECLARE rtdb_rows NO SCROLL CURSOR FOR SELECT * FROM {}.{}", identifier(&table.schema)?, identifier(&table.table)?)).await?; self.table = Some(SnapshotTable { table, fetch: self.query.prepare_snapshot_fetch().await? }); }
                None if self.tables_finished => return Ok(None),
                None => continue,
            }
        }
    }
    pub async fn finish(mut self) -> Result<()> { self.query.query("ROLLBACK").await?; Ok(()) }
}
