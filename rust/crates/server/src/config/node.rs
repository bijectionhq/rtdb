use rtdb_node::{Membership, NodeOptions};
use rtdb_types::Result;

use super::{NodeConfiguration, invalid};

impl NodeConfiguration {
    /// Splits the process allocation ceiling between the shared object cache and execution region.
    /// Each cache and execution reservation is shared across every database on the node.
    pub fn node_options(&self, allocation_limit_bytes: usize) -> Result<NodeOptions> {
        let cache_bytes = self.memory_cache.maximum_bytes.unwrap_or_else(|| (allocation_limit_bytes as f64 * self.memory_cache.maximum_ratio) as usize);
        let execution_memory_bytes = allocation_limit_bytes.checked_sub(cache_bytes).filter(|bytes| *bytes > 0)
            .ok_or_else(|| invalid("Memory cache must leave a positive execution allocation budget"))?;
        if cache_bytes == 0 { return Err(invalid("Memory cache allocation budget must be positive")); }
        let mut options = NodeOptions::default();
        options.membership = match &self.fixed_databases {
            Some(databases) => Membership::Fixed(databases.clone()),
            None => Membership::Durable { primary: self.primary.clone(), skipped: self.skipped_databases.clone() },
        };
        options.read_only = self.read_only_databases;
        options.default_timezone = self.default_timezone.clone();
        options.query.join_build_rows = self.join_build_rows;
        options.query.generate_series_end_exclusive = self.generate_series_end_exclusive;
        options.cache_bytes = cache_bytes;
        options.execution_memory_bytes = execution_memory_bytes;
        options.trie = rtdb_trie::TrieConfig::new(self.indexer.log_limit, self.indexer.page_limit)?;
        options.replication.enabled = self.indexer.enabled;
        options.replication.rows_per_block = self.indexer.rows_per_block;
        options.replication.flush_timeout = self.indexer.flush_duration;
        options.replication.skipped_transactions = self.indexer.skip_transactions.clone();
        options.compaction.page_size = self.indexer.page_limit;
        options.compaction.run_end_encoded_iids = self.compactor.run_end_encoded_iids;
        options.compaction.within_file_recency_branching = self.compactor.within_file_recency_branching;
        options.compaction_threads = if self.fixed_databases.is_some() { 0 } else { self.compactor.threads };
        options.garbage.blocks_to_keep = self.garbage.blocks_to_keep;
        options.garbage.garbage_lifetime = self.garbage.garbage_lifetime;
        options.automatic_gc = self.garbage.enabled;
        Ok(options)
    }
}
