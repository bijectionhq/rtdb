//! Stateless SQL snapshot tokens.
//!
//! As in XTDB, a snapshot token carries its read basis itself: the transaction head and system-time cap it was issued
//! at, with the evaluation clock and time zone. Redeeming it reads the database's current catalog capped at that basis,
//! so no server-side table, catalog hold or expiry is involved; the token stays valid for as long as the history it
//! names exists. Where the database has a placed-receipt codec the basis is sealed, binding it to the issuing principal
//! and history; otherwise it is plain, and grants nothing a `SNAPSHOT_TIME` setting would not.

use std::sync::{Arc, Weak};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rtdb_query::QueryContext;
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, Instant, Result, Timestamp, TransactionKey};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

const SEALED_PREFIX: &str = "rtdb-sealed-snapshot-v3:";
const PLAIN_PREFIX: &str = "rtdb-snapshot-v3:";
const MAX_ENVELOPE_BYTES: usize = 2048;

/// The read basis a token names.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotBasis {
    /// The issuing principal and history fingerprint; present only in a sealed token.
    owner: Option<String>,
    history: Option<[u8; 32]>,
    database: String,
    /// The storage generation the basis was read from, which a redeeming catalog must share.
    cut: Option<CatalogCut>,
    head: Option<TransactionKey>,
    cap: Option<Timestamp>,
    clock: Instant,
    timezone: String,
}

impl SnapshotBasis {
    fn capture(view: &rtdb_query::SqlSnapshotView) -> Result<Self> {
        if view.catalog.database_names().count() != 1 { return Err(unretained()); }
        let captured = view.catalog.database(view.database())?;
        let [snapshot] = captured.partitions.as_slice() else { return Err(unretained()); };
        if snapshot.has_overlays() { return Err(unretained()); }
        Ok(Self {
            owner: None, history: None, database: view.database().to_owned(), cut: snapshot.catalog_cut(),
            head: snapshot.tx_basis(), cap: snapshot.system_time_cap(), clock: view.current_time,
            timezone: view.timezone.clone(),
        })
    }

    /// The system time the basis reads at: its cap, or its head when it carries no lower cap.
    fn system_time(&self) -> Option<Timestamp> {
        self.cap.or(self.head.map(|head| head.system_time))
    }

    fn encode(mut self, owner: Option<&str>, codec: Option<&dyn crate::placed_receipts::PlacedReceiptCodec>) -> Result<String> {
        let token = match codec {
            Some(codec) => {
                self.owner = owner.map(str::to_owned);
                self.history = Some(codec.scope().history_fingerprint());
                let sealed = codec.seal_snapshot(&serde_json::to_vec(&self).map_err(|_| unretained())?)?;
                format!("{SEALED_PREFIX}{sealed}")
            }
            None => format!("{PLAIN_PREFIX}{}", URL_SAFE_NO_PAD.encode(serde_json::to_vec(&self).map_err(|_| unretained())?)),
        };
        if token.len() > MAX_ENVELOPE_BYTES { return Err(unretained()); }
        Ok(token)
    }

    /// Decodes and authenticates a token for `owner`; a sealed token needs the codec that sealed it.
    fn decode(token: &str, owner: Option<&str>, codec: Option<&dyn crate::placed_receipts::PlacedReceiptCodec>) -> Result<Self> {
        if token.len() > MAX_ENVELOPE_BYTES { return Err(unretained()); }
        if let Some(sealed) = token.strip_prefix(SEALED_PREFIX) {
            let codec = codec.ok_or_else(unretained)?;
            let basis: Self = serde_json::from_slice(&codec.open_snapshot(sealed).map_err(|_| unretained())?)
                .map_err(|_| unretained())?;
            if basis.owner.as_deref() != owner || basis.history != Some(codec.scope().history_fingerprint()) {
                return Err(unretained());
            }
            return Ok(basis);
        }
        let plain = token.strip_prefix(PLAIN_PREFIX).filter(|plain| !plain.is_empty()).ok_or_else(unretained)?;
        let basis: Self = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(plain).map_err(|_| unretained())?)
            .map_err(|_| unretained())?;
        if basis.owner.is_some() || basis.history.is_some() { return Err(unretained()); }
        Ok(basis)
    }

    /// Caps `current` at this basis, as XTDB reads a snapshot token against the snapshot it opens.
    /// A basis after the current head refuses with `xtdb/unindexed-tx`, as in XTDB.
    fn read_at(&self, current: &rtdb_index::Snapshot) -> Result<rtdb_index::Snapshot> {
        if current.has_overlays() { return Err(unretained()); }
        if let (Some(issued), Some(redeemed)) = (self.cut, current.catalog_cut())
            && (issued.storage_version != redeemed.storage_version || issued.storage_epoch != redeemed.storage_epoch)
        {
            return Err(unretained());
        }
        let Some(head) = self.head else {
            // An empty basis has no system time to cap a later transaction below.
            if current.tx_basis().is_some() { return Err(unretained()); }
            return Ok(current.clone());
        };
        if current.tx_basis().is_none_or(|present| present.tx_id < head.tx_id || present.system_time < head.system_time) {
            return Err(Anomaly::incorrect("xtdb/unindexed-tx",
                format!("system-time ({}) is after the latest completed tx", head.system_time.0)));
        }
        current.clone().with_system_time_cap(self.system_time()).map_err(|_| unretained())
    }
}

/// Lets `context` issue tokens for the views it executes.
pub(crate) fn with_issuer(context: Arc<QueryContext>, owner: Option<&str>, database: Option<Weak<crate::Database>>) -> Arc<QueryContext> {
    let owner = owner.map(Arc::<str>::from);
    context.with_snapshot_token_issuer(Arc::new(move |view| {
        let database = database.as_ref().and_then(Weak::upgrade);
        let codec = database.as_deref().filter(|database| database.name() == view.database())
            .and_then(|database| database.await_authority.codec());
        SnapshotBasis::capture(&view)?.encode(owner.as_deref(), codec)
    }))
}

pub(crate) fn unretained() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/snapshot-unretained", "The SQL snapshot token cannot be redeemed here")
}

pub(crate) struct SqlSnapshotAdmission {
    pub(crate) node: crate::Node,
    pub(crate) identity: Arc<dyn crate::SessionIdentity>,
}

impl SqlSnapshotAdmission {
    /// Redeems a token against the current context's catalog, with no server-side state.
    pub(crate) async fn restore(
        &self, token: &str, current: &QueryContext, cancellation: &CancellationToken,
    ) -> Result<rtdb_query::SqlSnapshotView> {
        use rtdb_plan::Catalog;

        let name = current.catalog.default_database();
        self.node.admit_retained_catalog(&current.catalog, self.identity.as_ref(), cancellation).await?;
        let database = self.node.catalog().database(name).map_err(|_| unretained())?;
        let current_database = current.catalog.database(name)?;
        if !Arc::ptr_eq(&database.storage, &current_database.storage) || current.catalog.database_names().count() != 1 {
            return Err(unretained());
        }
        let basis = SnapshotBasis::decode(token, self.identity.user(), database.await_authority.codec())?;
        if basis.database != name || basis.timezone != current.timezone { return Err(unretained()); }
        let [partition] = current_database.partitions.as_slice() else { return Err(unretained()); };
        let snapshot = basis.read_at(partition)?;
        let view = rtdb_query::SqlSnapshotView::from_exact_partition(current, snapshot, basis.clock, None, None)?;
        self.node.admit_retained_catalog(&view.catalog, self.identity.as_ref(), cancellation).await?;
        Ok(view)
    }
}

#[async_trait::async_trait]
impl rtdb_query::SnapshotAdmission for SqlSnapshotAdmission {
    async fn reopen(&self, token: &str, current: &QueryContext) -> Result<rtdb_query::SqlSnapshotView> {
        self.restore(token, current, &current.cancellation).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_types::{ReplicaPosition, StorageEpoch, StorageVersion, TxId};

    fn basis(head: Option<TransactionKey>, cap: Option<Timestamp>, cut: CatalogCut) -> SnapshotBasis {
        SnapshotBasis {
            owner: None, history: None, database: "xtdb".into(), cut: Some(cut), head, cap,
            clock: Instant::new(60, 123).unwrap(), timezone: "UTC".into(),
        }
    }

    fn cut() -> CatalogCut {
        CatalogCut { storage_version: StorageVersion(2), storage_epoch: StorageEpoch(7), replica_epoch: StorageEpoch(1), through: ReplicaPosition(20) }
    }

    #[test]
    fn a_plain_token_carries_its_whole_basis_and_needs_no_server_state() {
        let issued = basis(Some(TransactionKey { tx_id: TxId(5), system_time: Timestamp(22) }), Some(Timestamp(19)), cut());
        let token = basis(issued.head, issued.cap, cut()).encode(Some("alice"), None).unwrap();
        assert!(token.starts_with(PLAIN_PREFIX));
        let redeemed = SnapshotBasis::decode(&token, Some("someone else"), None).unwrap();
        assert_eq!(redeemed, issued, "a plain token names only its basis, like an XTDB time basis");
        assert_eq!(redeemed.system_time(), Some(Timestamp(19)));
        assert!(SnapshotBasis::decode("rtdb-snapshot:6f9619ff-8b86-d011-b42d-00c04fc964ff", None, None).is_err(),
            "a token of the retired server-side registry is refused");
        assert!(SnapshotBasis::decode(&format!("{SEALED_PREFIX}abc"), None, None).is_err(),
            "a sealed token needs the database's codec");
        let mut claimed = basis(None, None, cut());
        claimed.owner = Some("alice".into());
        let forged = format!("{PLAIN_PREFIX}{}", URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claimed).unwrap()));
        assert!(SnapshotBasis::decode(&forged, Some("alice"), None).is_err(), "a plain token cannot claim an owner");
    }

    #[test]
    fn redeeming_caps_the_current_catalog_at_the_issued_basis_whenever_it_is_redeemed() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let partition = PartitionState::empty(rtdb_trie::TrieConfig::default()).unwrap();
        let apply = |key| {
            let write = TransactionBuffer::new(partition.snapshot(), key, Arc::clone(&memory)).unwrap().stage().unwrap();
            partition.apply(&write).unwrap();
        };
        let head = TransactionKey { tx_id: TxId(5), system_time: Timestamp(22) };
        apply(head);
        partition.publish_catalog_cut(cut()).unwrap();
        let issued = basis(Some(head), Some(Timestamp(22)), cut());

        // Later transactions and catalog generations do not invalidate the token: there is no expiry or hold.
        apply(TransactionKey { tx_id: TxId(6), system_time: Timestamp(30) });
        partition.publish_catalog_cut(CatalogCut { through: ReplicaPosition(40), ..cut() }).unwrap();
        let redeemed = issued.read_at(&partition.snapshot()).unwrap();
        assert_eq!(redeemed.system_time_cap(), Some(Timestamp(22)));
        assert_eq!(redeemed.tx_basis().map(|key| key.tx_id), Some(TxId(6)));

        let ahead = basis(Some(TransactionKey { tx_id: TxId(9), system_time: Timestamp(90) }), None, cut());
        assert_eq!(ahead.read_at(&partition.snapshot()).unwrap_err().code, "xtdb/unindexed-tx");
        let other_generation = basis(Some(head), None, CatalogCut { storage_epoch: StorageEpoch(8), ..cut() });
        assert_eq!(other_generation.read_at(&partition.snapshot()).unwrap_err().code, "rtdb/snapshot-unretained");
        assert!(basis(None, None, cut()).read_at(&partition.snapshot()).is_err(), "an empty basis cannot cap a later transaction");
    }
}
