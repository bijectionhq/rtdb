//! A cell under faults while a client writes: nodes die and are replaced, deployments move, park and rebalance, a logkeeper dies.
//! Every acknowledged write must survive, and nothing never attempted may appear.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::*;
use rtdb_multi::admin;
use tokio_postgres::NoTls;

/// A small deterministic generator, so a failing run's sequence of faults can be replayed by its seed.
struct Faults(u64);
impl Faults {
    fn next(&mut self, bound: usize) -> usize { self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); ((self.0 >> 33) as usize) % bound }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn acknowledged_writes_survive_a_cell_under_faults() {
    let seed: u64 = std::env::var("RTDB_CHAOS_SEED").ok().and_then(|seed| seed.parse().ok()).unwrap_or(7);
    let rounds: usize = std::env::var("RTDB_CHAOS_ROUNDS").ok().and_then(|rounds| rounds.parse().ok()).unwrap_or(12);
    println!("chaos seed {seed}, {rounds} rounds");
    let mut cell = Cell::start(3, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let (first_acknowledgement, started) = tokio::sync::oneshot::channel();
    let writer = {
        let (stop, port) = (Arc::clone(&stop), cell.gateway.port());
        tokio::spawn(async move {
            let (mut acknowledged, mut unknown) = (BTreeSet::new(), BTreeSet::new());
            let mut first_acknowledgement = Some(first_acknowledgement);
            let mut client = None;
            let mut id = 0i64;
            while !stop.load(Ordering::Relaxed) {
                if client.is_none() {
                    match tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=xtdb password=cell-test dbname=shop"), NoTls).await {
                        Ok((connected, connection)) => { tokio::spawn(connection); client = Some(connected); }
                        Err(_) => { tokio::time::sleep(Duration::from_millis(50)).await; continue; }
                    }
                }
                let insert = format!("INSERT INTO orders (_id, total) VALUES ({id}, {id})");
                match client.as_ref().expect("connected").simple_query(&insert).await {
                    Ok(_) => {
                        acknowledged.insert(id);
                        if let Some(started) = first_acknowledgement.take() { let _ = started.send(()); }
                    }
                    Err(_) => { unknown.insert(id); client = None; }
                }
                id += 1;
            }
            (acknowledged, unknown, id)
        })
    };
    tokio::time::timeout(Duration::from_secs(10), started).await
        .expect("the writer acknowledged a transaction before the faults").unwrap();

    let mut faults = Faults(seed);
    let mut logkeeper_killed = false;
    for round in 0..rounds {
        tokio::time::sleep(Duration::from_millis(300 + faults.next(400) as u64)).await;
        let live: Vec<usize> = (0..cell.nodes.len()).filter(|&index| cell.nodes[index].is_some()).collect();
        match faults.next(5) {
            0 if live.len() > 1 => {
                let victim = live[faults.next(live.len())];
                println!("round {round}: kill node {victim}, start a replacement");
                cell.nodes[victim].take().unwrap().kill().await.unwrap();
                cell.add_node().await;
            }
            1 => {
                let target = live[faults.next(live.len())];
                println!("round {round}: move to node {target}");
                let _ = admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(target).lease.id(), &control()).await;
            }
            2 => {
                if let Some(holder) = cell.holder("shop").await {
                    println!("round {round}: park on node {holder}");
                    let _ = cell.node(holder).agent.park("shop").await;
                }
            }
            3 if !logkeeper_killed => {
                println!("round {round}: kill a logkeeper");
                cell.logkeepers.remove(faults.next(cell.logkeepers.len())).stop().await.unwrap();
                logkeeper_killed = true;
            }
            _ => {
                println!("round {round}: rebalance");
                let _ = admin::rebalance(cell.directory.as_ref(), 2, &control()).await;
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    let (acknowledged, unknown, attempted) = writer.await.unwrap();
    println!("{} acknowledged, {} unknown of {attempted}", acknowledged.len(), unknown.len());

    let record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    println!("after the faults: lifecycle {:?}, epoch {}, holder {:?}, crashes {}", record.lifecycle, record.epoch, record.holder, record.activation_crashes);
    let client = connect_eventually(&cell, "shop").await;
    let stored: BTreeSet<i64> = values(&client, "SELECT _id FROM orders").await.into_iter().map(|id| id.parse().unwrap()).collect();
    let lost: Vec<_> = acknowledged.difference(&stored).collect();
    assert!(lost.is_empty(), "acknowledged writes were lost: {lost:?}");
    let invented: Vec<_> = stored.iter().filter(|id| !acknowledged.contains(id) && !unknown.contains(id)).collect();
    assert!(invented.is_empty(), "rows appeared that were never attempted: {invented:?}");
    client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({attempted}, {attempted})")).await.unwrap();
    assert_eq!(values(&client, &format!("SELECT _id FROM orders WHERE _id = {attempted}")).await, [attempted.to_string()]);
    drop(client);
    let report = admin::audit(cell.directory.as_ref(), &cell.keys(), "shop", 4 << 20).await.unwrap();
    println!("audit: {} records, {} transactions, highest term {}", report.records, report.transactions, report.highest_term);
    assert!(report.violations.is_empty(), "the history is one chain: {:?}", report.violations);
    cell.stop().await;
}
