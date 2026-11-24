use rtdb_node::{Membership, NodeOptions};
use rtdb_types::Result;

use super::{NodeConfiguration, invalid};

impl NodeConfiguration {
    /// Returns the effective node time zone, including the in-process default.
    pub fn node_timezone(&self) -> &str {
        self.default_timezone.as_deref().unwrap_or(NodeOptions::DEFAULT_TIMEZONE)
    }

    /// Funds the installed audience before splitting the ordinary allocation between cache and execution.
    pub fn node_options(&self, allocation_limit_bytes: usize) -> Result<NodeOptions> {
        let prepaid = self.flight_sql.as_ref().and_then(|settings| settings.audience)
            .map_or(0, |(_, audience)| audience.prepaid_bytes());
        let ordinary = allocation_limit_bytes.checked_sub(prepaid)
            .ok_or_else(|| invalid("Memory limit cannot fund the installed Flight audience"))?;
        let cache_bytes = self.memory_cache.maximum_bytes.unwrap_or_else(|| (ordinary as f64 * self.memory_cache.maximum_ratio) as usize);
        if cache_bytes == 0 { return Err(invalid("Memory cache allocation budget must be positive")); }
        if cache_bytes >= ordinary { return Err(invalid("Memory cache must leave a positive ordinary execution allocation budget")); }
        let execution_memory_bytes = allocation_limit_bytes - cache_bytes;
        let mut options = NodeOptions::default();
        options.membership = match &self.fixed_databases {
            Some(databases) => Membership::Fixed(databases.clone()),
            None => Membership::Durable { primary: self.primary.clone(), skipped: self.skipped_databases.clone() },
        };
        options.read_only = self.read_only_databases;
        options.default_v1_serving = self.default_v1_serving;
        options.ordered_index_arrow_references = self.ordered_index_arrow_references;
        options.ordered_index_arrow_references_on_private_storage = self.ordered_index_arrow_references_on_private_storage;
        options.default_timezone = self.node_timezone().to_owned();
        options.query.join_build_rows = self.join_build_rows;
        options.query.generate_series_end_exclusive = self.generate_series_end_exclusive;
        options.query.object_sort = self.object_sort;
        // Queries a network listener serves run many at once and contend on the node's budget, so each admits its many
        // small reservations a quantum at a time; an embedded node keeps exact admission.
        if self.server.is_some() || self.flight_sql.is_some() { options.query.memory_quantum = 256 << 10; }
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
        options.compaction.scan_tie_order_from_naming_epoch = self.compactor.scan_tie_order_from_naming_epoch;
        options.compaction_threads = if self.fixed_databases.is_some() { 0 } else { self.compactor.threads };
        options.garbage.blocks_to_keep = self.garbage.blocks_to_keep;
        options.garbage.garbage_lifetime = self.garbage.garbage_lifetime;
        options.garbage.reader_lease = self.garbage.reader_lease;
        options.garbage.reader_lease_margin = self.garbage.reader_lease_margin;
        options.automatic_gc = self.garbage.enabled;
        options.artefact_threads = if self.fixed_databases.is_some() || !self.search.enabled { 0 } else { self.search.threads };
        Ok(options)
    }
}
