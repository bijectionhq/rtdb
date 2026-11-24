//! How a MATCH's cost grows with the number of search files a table has, read through the buffer pool or mapped from this node's disk.
//! Run with `cargo test --release -p rtdb-search --test fanout -- --ignored --nocapture`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rtdb_search::{SearchRow, SearchRuntime, build_search_file};
use rtdb_storage::{LocalStore, MemoryStore, ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_text::Query;

const DOCUMENTS: usize = 2_000;
const VOCABULARY: usize = 2_000;

fn rows(seed: u64) -> Vec<SearchRow> {
    let mut state = seed;
    let mut next = || { state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (state >> 33) as usize };
    (0..DOCUMENTS).map(|document| {
        let tokens = (0..20).map(|_| format!("w{}", next() % VOCABULARY)).collect();
        SearchRow { position: None, identity: None, iid: [(document / 8) as u8; 16], system_from: -(document as i64), valid_from: 0, valid_to: i64::MAX, op: rtdb_types::EventOp::Put, values: vec![Some(tokens)] }
    }).collect()
}

/// The first query over every file, then the next, with `kept` bytes of files kept in memory (and 32 times that mapped).
async fn time_queries(store: Arc<dyn ObjectStore>, keys: &[ObjectKey], columns: &[String], kept: usize) -> (Duration, Duration, usize) {
    let pool = Arc::new(BufferPool::new(store, kept));
    let runtime = SearchRuntime::new(kept);
    let query = Query::parse("w7");
    let mut found = 0;
    let mut pass = || async {
        let start = Instant::now();
        let mut matches = 0;
        for key in keys { matches += runtime.file("bench", &pool, key, columns).await.unwrap().matches("body", &query, rtdb_types::TemporalBounds { valid_time: rtdb_types::Interval::ALL, system_time: rtdb_types::Interval::ALL }).unwrap().len(); }
        (start.elapsed(), matches)
    };
    let (cold, matches) = pass().await;
    found += matches;
    let (warm, _) = pass().await;
    (cold, warm, found)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a benchmark: run explicitly to see its numbers"]
async fn match_cost_grows_with_the_number_of_search_files() {
    let columns = vec!["body".to_string()];
    let file = build_search_file(&columns, &rows(1), "bench").unwrap();
    println!("one search file of {DOCUMENTS} documents: {} KiB", file.len() / 1024);
    println!("{:>6} {:>8} {:>14} {:>14} {:>14} {:>14}", "files", "kept", "pool cold", "pool warm", "mapped cold", "mapped warm");
    for count in [10, 100, 1_000] {
        let keys: Vec<_> = (0..count).map(|index| ObjectKey::new(format!("tables/docs/artefacts/l02-rc-p{index}-b00/search/docs~a2f3-v1.bin")).unwrap()).collect();
        let memory = Arc::new(MemoryStore::default());
        let directory = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalStore::open(directory.path()).unwrap());
        for (index, key) in keys.iter().enumerate() {
            let bytes = build_search_file(&columns, &rows(index as u64), "bench").unwrap();
            memory.put(key, bytes.clone().into()).await.unwrap();
            local.put(key, bytes.into()).await.unwrap();
        }
        for kept in [256 << 20, 16 << 20] {
            let (pool_cold, pool_warm, pool_found) = time_queries(memory.clone(), &keys, &columns, kept).await;
            let (mapped_cold, mapped_warm, mapped_found) = time_queries(local.clone(), &keys, &columns, kept).await;
            assert_eq!(pool_found, mapped_found);
            println!("{count:>6} {:>7}M {pool_cold:>14.2?} {pool_warm:>14.2?} {mapped_cold:>14.2?} {mapped_warm:>14.2?}", kept >> 20);
        }
        local.close_and_wait().await;
    }
}
