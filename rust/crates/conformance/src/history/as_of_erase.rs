use super::{Error, combine, flush, instant, store::Reads, support::{FixtureClock, Identity, RunningNode}};
use crate::{ObservationReceipt, Outcome};
use rtdb_columnar::read_value;
use rtdb_node::{AutonomousTxOptions, NativeSession, Node};
use rtdb_tx::{Documents, EntityIds, TxOp};
use rtdb_types::{BlockIndex, EntityId, Keyword, TableRef, Timestamp, TransactionKey, Value};
use serde::Deserialize;
use serde_json::json;
use rtdb_types::StructFields;
use std::{collections::BTreeSet, sync::{Arc, atomic::{AtomicI64, Ordering}}, time::Duration};

#[derive(Deserialize)]
pub(crate) struct Fixture {
    schema_version: u32,
    timezone: String,
    put_system_time: String,
    put_keyword_ids: [String; 2],
    erase_system_time: String,
    erase_keyword_id: String,
    query_time: String,
    queries: [Query; 2],
}

#[derive(Deserialize)]
struct Query {
    sql: String,
    live_observation: String,
    flushed_observation: String,
}

async fn require_layout(node: &Node, session: &mut NativeSession, table: &TableRef, tx: TransactionKey, live: bool) -> Result<BlockIndex, Error> {
    let database = node.catalog().database("xtdb")?;
    let status = database.status();
    let block = status.adopted_block.as_ref().ok_or("erase fixture has no adopted block")?;
    let context = session.query_context().await?;
    let snapshot = &context.catalog.database("xtdb")?.partitions[0];
    let has_live_docs = !snapshot.segments(table).is_empty();
    let l0_matches = snapshot.trie_entries(table).iter()
        .any(|trie| trie.key.level() == 0 && trie.key.block_index() == block.block_index());
    if database.progress().current().tx_key != Some(tx) || has_live_docs != live
        || !l0_matches || (!live && block.latest_completed_tx() != Some(tx))
    {
        return Err("erase fixture is missing its applied transaction or required docs layout".into());
    }
    Ok(block.block_index())
}

async fn query(session: &mut NativeSession, query: &Query, live: bool) -> Result<ObservationReceipt, Error> {
    let mut cursor = session.query_sql(&query.sql, vec![]).await?;
    let fields = cursor.schema().fields();
    if fields.len() != 1 || fields[0].name != "_id" {
        return Err("erase fixture query must project only _id".into());
    }
    let mut ids = BTreeSet::new();
    while let Some(batch) = cursor.next_batch().await? {
        let schema = batch.batch().schema();
        for row in 0..batch.batch().num_rows() {
            let value = read_value(schema.field(0), batch.batch().column(0).as_ref(), row)?;
            let Value::Keyword(id) = value else {
                return Err("erase fixture query returned a non-keyword _id".into());
            };
            ids.insert(id);
        }
    }
    let values: Vec<_> = ids.into_iter().map(|id| {
        let (namespace, name) = id.as_str().split_once('/').map_or((None, id.as_str()), |(ns, name)| (Some(ns), name));
        json!({"kind":"keyword","namespace":namespace,"name":name})
    }).collect();
    let id = if live { &query.live_observation } else { &query.flushed_observation };
    Ok(ObservationReceipt { id: id.clone(), executed: true, outcome: Outcome::Returned { typed_value: json!(values) } })
}

async fn observe_node(node: &Node, clock: &FixtureClock, fixture: &Fixture, put_time: Timestamp, erase_time: Timestamp) -> Result<Vec<ObservationReceipt>, Error> {
    let database = node.catalog().database("xtdb")?;
    let compactor = database.compaction().ok_or("erase fixture compactor is missing")?;
    tokio::time::timeout(Duration::from_secs(60), compactor.compact_all()).await??;

    let table = TableRef::new("public", "docs");
    let documents = fixture.put_keyword_ids.iter().map(|id| {
        Ok(StructFields::from([("_id".into(), Value::Keyword(Keyword::parse(id)?))]))
    }).collect::<Result<Vec<_>, Error>>()?;
    let erased = EntityId::Keyword(Keyword::parse(&fixture.erase_keyword_id)?);
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let put = TxOp::PutDocs { table: table.clone(), documents: Documents::new(documents, node.memory())?,
        valid_from: None, valid_to: None };
    let put = session.state_mut().execute_tx(vec![put], AutonomousTxOptions::default()).await?;
    if !put.committed() || put.tx_key().system_time != put_time {
        return Err("erase fixture put did not commit at its fixture clock reading".into());
    }
    flush(node).await?;
    let put_block = require_layout(node, &mut session, &table, put.tx_key(), false).await?;

    clock.0.store(erase_time.0, Ordering::SeqCst);
    let erase = TxOp::EraseDocs { table: table.clone(), ids: EntityIds::new(vec![erased], node.memory())? };
    let erase = session.state_mut().execute_tx(vec![erase], AutonomousTxOptions::default()).await?;
    if !erase.committed() || erase.tx_key().system_time != erase_time {
        return Err("erase fixture erase did not commit at its fixture clock reading".into());
    }
    if require_layout(node, &mut session, &table, erase.tx_key(), true).await? != put_block {
        return Err("erase fixture moved to a later block before its live queries".into());
    }
    let mut observations = Vec::with_capacity(4);
    for sql in &fixture.queries {
        observations.push(query(&mut session, sql, true).await?);
    }

    flush(node).await?;
    if require_layout(node, &mut session, &table, erase.tx_key(), false).await? <= put_block {
        return Err("erase fixture did not adopt a block after flushing the erase".into());
    }
    for sql in &fixture.queries {
        observations.push(query(&mut session, sql, false).await?);
    }
    Ok(observations)
}

pub(crate) async fn observe(fixture: &Fixture) -> Result<Vec<ObservationReceipt>, Error> {
    let put_time = instant(&fixture.put_system_time)?;
    let erase_time = instant(&fixture.erase_system_time)?;
    let query_time = instant(&fixture.query_time)?;
    if fixture.schema_version != 1 || fixture.timezone != "UTC" || put_time >= erase_time || query_time != erase_time
        || fixture.put_keyword_ids[0] == fixture.put_keyword_ids[1] || !fixture.put_keyword_ids.contains(&fixture.erase_keyword_id)
    {
        return Err("invalid AS OF erase fixture".into());
    }
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(FixtureClock(AtomicI64::new(put_time.0)));
    let running = RunningNode::open(directory.path(), Arc::clone(&clock), Arc::new(Reads::default())).await?;
    let work = observe_node(&running.node, &clock, fixture, put_time, erase_time).await;
    let result = combine(work, running.close().await);
    combine(result, directory.close().map_err(Error::from))
}

#[cfg(test)]
#[tokio::test]
async fn historical_window_after_erase() {
    let fixture = include_bytes!("../../../../conformance/fixtures/as-of-erase.json");
    let observations = crate::controls::observe(fixture).await.unwrap();
    let ids: Vec<_> = observations.iter().map(|observation| observation.id.as_str()).collect();
    assert_eq!(ids, [
        "as_of_before_erase_with_live_erase",
        "system_time_range_before_erase_with_live_erase",
        "as_of_before_erase_with_flushed_erase",
        "system_time_range_before_erase_with_flushed_erase",
    ]);
    let survivor = json!([{"kind":"keyword","namespace":null,"name":"other-doc"}]);
    for observation in observations {
        assert!(observation.executed);
        match observation.outcome {
            Outcome::Returned { typed_value } => assert_eq!(typed_value, survivor),
            other => panic!("expected typed erase observation, got {other:?}"),
        }
    }
}
