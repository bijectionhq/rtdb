use std::sync::Arc;
use rtdb_columnar::MemoryBudget;
use rtdb_server::{ServerDriver, config::NodeConfiguration};

#[tokio::test]
async fn embedded_startup_keeps_the_supplied_execution_budget() {
    let config = NodeConfiguration::from_yaml("server: null\nflightSql: null\nnodeId: shared-memory-test\n").unwrap();
    let total = 128 * 1024 * 1024;
    let memory = MemoryBudget::new(config.node_options(total).unwrap().execution_memory_bytes);
    let retained_config = memory.reserve(65_536).unwrap();
    let (handle, driver) = ServerDriver::open_with_memory(config, total, Arc::clone(&memory)).await.unwrap();
    assert!(Arc::ptr_eq(handle.node().memory(), &memory));
    handle.shutdown();
    driver.run().await.unwrap();
    drop(handle); drop(retained_config);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn supplied_execution_budget_must_match_the_process_cache_split() {
    let config = NodeConfiguration::from_yaml("server: null\nflightSql: null\n").unwrap();
    let result = ServerDriver::open_with_memory(config, 128 * 1024 * 1024, MemoryBudget::new(128 * 1024 * 1024)).await;
    assert!(matches!(result, Err(error) if error.code == "rtdb.server/invalid-memory-budget"));
}
