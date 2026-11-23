//! Scan fragments for deployments larger than one node's cache (`RefineKeyForLargeDeployments`).
//!
//! A scan of such a deployment splits by trie prefix, and each fragment is read by the node that ranks first for
//! `{storage identity}/{table}/{prefix}` among the cell's readers, so every node keeps the same slice of the deployment warm.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rtdb_columnar::MemoryBudget;
use rtdb_directory::{Directory, MemberId, MemberPool, MemberRecord, now_ms};
use rtdb_query::{FragmentDepth, FragmentReply, ScanFragment, ScanFragmenter, TriePrefix};
use rtdb_types::{Anomaly, AnomalyCategory, Result, TableRef};

use crate::control::{self, Request};

#[derive(Clone, Debug)]
pub struct FragmentOptions {
    /// Split the scans of a deployment holding more persisted bytes than this; by default, the node's disk cache size.
    pub above_bytes: Option<u64>,
    pub depth: FragmentDepth,
    /// How long a reader may take to answer a fragment, catching up to its basis included.
    pub timeout: Duration,
    /// How long a reader waits for its copy of the deployment to reach a fragment's basis.
    pub catch_up: Duration,
}

impl Default for FragmentOptions {
    fn default() -> Self {
        Self { above_bytes: None, depth: FragmentDepth::new(2).expect("a valid depth"), timeout: Duration::from_secs(30), catch_up: Duration::from_secs(5) }
    }
}

pub(crate) fn storage_identity(namespace: uuid::Uuid, record: &rtdb_directory::DeploymentRecord) -> Arc<str> {
    format!("{namespace}/{}", record.storage_prefix()).into()
}

/// The affinity key a fragment is placed by.
pub fn fragment_key(storage_identity: &str, table: &TableRef, prefix: &TriePrefix) -> String {
    format!("{storage_identity}/{}.{}/{prefix}", table.schema, table.table)
}

/// The nodes that may read a fragment: every node that may take a placement, compaction workers aside.
pub fn readers(members: &[MemberRecord]) -> Vec<&MemberRecord> {
    let now = now_ms();
    members.iter().filter(|member| member.may_receive_placements(now) && member.pool == MemberPool::General && member.control_address.is_some()).collect()
}

/// A node's fragmenter: places each fragment of its sessions' scans on the reader it ranks first on.
pub struct CellFragmenter {
    directory: Arc<dyn Directory>,
    object_namespace: uuid::Uuid,
    secret: Option<Arc<crate::capability::CellSecret>>,
    member: OnceLock<MemberId>,
    above_bytes: AtomicU64,
    depth: FragmentDepth,
    timeout: Duration,
    failures: Arc<AtomicU64>,
}

impl CellFragmenter {
    pub fn new(directory: Arc<dyn Directory>, object_namespace: uuid::Uuid, options: &FragmentOptions, cache_bytes: u64, secret: Option<Arc<crate::capability::CellSecret>>) -> Arc<Self> {
        Arc::new(Self { directory, object_namespace, secret, member: OnceLock::new(), above_bytes: AtomicU64::new(options.above_bytes.unwrap_or(cache_bytes)), depth: options.depth, timeout: options.timeout, failures: Arc::new(AtomicU64::new(0)) })
    }

    /// Names this node once its lease is registered; until then, every fragment is read here.
    pub fn set_member(&self, member: MemberId) { let _ = self.member.set(member); }

    /// Splits scans of deployments holding more than `bytes` of persisted tries from now on; `u64::MAX` splits none.
    pub fn split_above(&self, bytes: u64) { self.above_bytes.store(bytes, Ordering::Relaxed); }

    /// How many fragments a reader failed to answer, each of which the coordinator then read itself.
    pub fn failures(&self) -> u64 { self.failures.load(Ordering::Relaxed) }

    async fn place(&self, fragment: &ScanFragment) -> Result<Option<String>> {
        let Some(me) = self.member.get() else { return Ok(None) };
        let record = self.directory.deployment_by_name(fragment.database()).await?
            .ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment is named {}", fragment.database())))?;
        let namespace = record.storage_namespaces.map_or(self.object_namespace, |storage| storage.objects);
        let current = rtdb_query::FragmentResidence { storage: storage_identity(namespace, &record), replica_epoch: record.log_epoch };
        if fragment.residence.as_ref() != Some(&current) { return Ok(None); }
        let members = self.directory.members().await?;
        let readers = readers(&members);
        let key = fragment_key(&record.storage_identity(), fragment.table(), &fragment.prefix);
        let first = rtdb_affinity::rank(&key, &readers, |member| member.id.as_str()).into_iter().next().copied();
        Ok(first.filter(|first| first.id != *me).and_then(|first| first.control_address.clone()))
    }

}

#[async_trait::async_trait]
impl ScanFragmenter for CellFragmenter {
    fn depth(&self, _database: &str, _table: &TableRef, persisted_bytes: u64) -> Option<FragmentDepth> {
        (persisted_bytes > self.above_bytes.load(Ordering::Relaxed)).then_some(self.depth)
    }

    async fn dispatch(&self, fragment: &ScanFragment) -> Result<Option<Vec<u8>>> {
        let memory = MemoryBudget::new(control::MAXIMUM_FRAGMENT_BYTES as usize + 8);
        let Some(reply) = self.dispatch_reply(fragment, &memory).await? else { return Ok(None) };
        Ok(Some(reply.await?.into_bytes()))
    }

    async fn dispatch_reply(&self, fragment: &ScanFragment, memory: &Arc<MemoryBudget>) -> Result<Option<FragmentReply>> {
        let Some(address) = self.place(fragment).await? else { return Ok(None) };
        let secret = self.secret.as_ref().ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-cell-secret", "Fragments are read elsewhere only in a cell with a secret"))?;
        let request = Request::ScanFragment { name: fragment.database().to_owned(), fragment: fragment.clone(), grant: secret.node_grant(fragment.database()) };
        let deadline = tokio::time::Instant::now() + self.timeout;
        let stream = control::start_fragment(&address, request, deadline, &control::ControlKey(self.secret.clone())).await.inspect_err(|_| {
            self.failures.fetch_add(1, Ordering::Relaxed);
        })?;
        let memory = Arc::clone(memory);
        let failures = Arc::clone(&self.failures);
        Ok(Some(Box::pin(async move {
            control::receive_fragment(stream, &address, deadline, &memory).await.inspect_err(|_| {
                failures.fetch_add(1, Ordering::Relaxed);
            })
        })))
    }
}
