//! SQL snapshot tokens, as XTDB's.
//!
//! A token is XTDB's `TimeBasis` (`xtdb.basis/->time-basis-str`): base64 protobuf naming, for each database the
//! query read, the system time each partition was read at. Redeeming it reads the current catalog capped at those
//! times, with the redeeming session's clock and zone, so no server-side table, catalog hold or expiry is involved and
//! a token issued by an XTDB node redeems here, and the reverse.
//!
//! RTDB-only: where the default database has a placed-receipt codec, the token is sealed instead, binding it to the
//! issuing principal, history and storage generation. An XTDB node refuses a sealed token as an invalid basis.

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

use rtdb_query::QueryContext;
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, Instant, Result, TimeBasis};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

const SEALED_PREFIX: &str = "rtdb-sealed-snapshot-v4:";

/// The RTDB-only envelope a sealed token carries.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SealedBasis {
    owner: Option<String>,
    history: [u8; 32],
    /// The storage generation the default database was read from, which a redeeming catalog must share.
    cut: Option<CatalogCut>,
    times: TimeBasis,
}

fn capture(view: &rtdb_query::SqlSnapshotView, include_empty: bool) -> Result<TimeBasis> {
    if let Some(basis) = &view.token_basis {
        for name in view.catalog.database_names() {
            if view.catalog.database(name)?.partitions.iter().any(rtdb_index::Snapshot::has_overlays) { return Err(overlaid()); }
        }
        return Ok(basis.clone());
    }
    let mut databases = BTreeMap::new();
    for name in view.catalog.database_names() {
        let mut times = Vec::new();
        for snapshot in &view.catalog.database(name)?.partitions {
            if snapshot.has_overlays() { return Err(overlaid()); }
            times.push(snapshot.system_time_cap().or(snapshot.tx_basis().map(|head| head.system_time)).map(Instant::from));
        }
        if include_empty || times.iter().any(Option::is_some) { databases.insert(name.to_owned(), times); }
    }
    Ok(TimeBasis { databases })
}

fn encode(view: &rtdb_query::SqlSnapshotView, owner: Option<&str>, codec: Option<&dyn crate::placed_receipts::PlacedReceiptCodec>, include_empty: bool) -> Result<String> {
    let times = capture(view, include_empty)?;
    let Some(codec) = codec else { return Ok(rtdb_format::basis::encode_time_basis(&times)); };
    let cut = view.catalog.database(view.database())?.partitions.first().and_then(|snapshot| snapshot.catalog_cut());
    let sealed = SealedBasis { owner: owner.map(str::to_owned), history: codec.scope().history_fingerprint(), cut, times };
    Ok(format!("{SEALED_PREFIX}{}", codec.seal_snapshot(&serde_json::to_vec(&sealed).map_err(|error| invalid_basis(error.to_string()))?)?))
}

fn decode(token: &str, owner: Option<&str>, codec: Option<&dyn crate::placed_receipts::PlacedReceiptCodec>) -> Result<(TimeBasis, Option<CatalogCut>)> {
    let Some(sealed) = token.strip_prefix(SEALED_PREFIX) else {
        return Ok((rtdb_format::basis::decode_time_basis(token)?, None));
    };
    let codec = codec.ok_or_else(|| invalid_basis("a sealed token needs its database's codec"))?;
    let opened = codec.open_snapshot(sealed).map_err(|_| invalid_basis("the sealed token does not open here"))?;
    let basis: SealedBasis = serde_json::from_slice(&opened).map_err(|error| invalid_basis(error.to_string()))?;
    if basis.owner.as_deref() != owner || basis.history != codec.scope().history_fingerprint() {
        return Err(invalid_basis("the sealed token belongs to another principal or history"));
    }
    Ok((basis.times, basis.cut))
}

/// Caps each partition at its time in the token, as XTDB reads a snapshot token against the snapshot it opens.
/// A partition the token gives no time keeps its current read, through its head, as in XTDB. A time after the
/// partition's latest completed transaction refuses with `xtdb/unindexed-tx`, as `validate-basis-not-before` does.
fn read_at(times: Option<&[Option<Instant>]>, current: &[rtdb_index::Snapshot]) -> Result<Vec<rtdb_index::Snapshot>> {
    current.iter().enumerate().map(|(index, snapshot)| {
        if snapshot.has_overlays() { return Err(overlaid()); }
        let Some(time) = times.and_then(|times| times.get(index)).copied().flatten() else {
            return snapshot.clone().with_system_time_cap(snapshot.tx_basis().map(|head| head.system_time));
        };
        if snapshot.tx_basis().is_none_or(|head| Instant::from(head.system_time) < time) {
            return Err(rtdb_query::unindexed_tx(time, snapshot.tx_basis()));
        }
        snapshot.clone().with_system_time_cap(Some(time.as_micros()?))
    }).collect()
}

/// Lets `context` issue tokens for the views it executes.
pub(crate) fn with_issuer(context: Arc<QueryContext>, owner: Option<&str>, database: Option<Weak<crate::Database>>) -> Arc<QueryContext> {
    let owner = owner.map(Arc::<str>::from);
    let include_empty = database.is_none();
    context.with_snapshot_token_issuer(Arc::new(move |view| {
        let database = database.as_ref().and_then(Weak::upgrade);
        let codec = database.as_deref().filter(|database| database.name() == view.database())
            .and_then(|database| database.await_authority.codec());
        encode(&view, owner.as_deref(), codec, include_empty)
    }))
}

fn invalid_basis(detail: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("xtdb.basis/invalid-basis", format!("Invalid basis: {detail}"))
}

fn overlaid() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unsupported, "rtdb/snapshot-token-overlay", "A SQL snapshot token cannot name a read with world overlays")
}

/// The token cannot be read on this node: another storage generation, or a fixed world clock (`bijection.at`).
pub(crate) fn unretained() -> Anomaly {
    Anomaly::incorrect("rtdb/snapshot-unretained", "The SQL snapshot token cannot be redeemed here")
}

pub(crate) struct SqlSnapshotAdmission {
    pub(crate) node: crate::Node,
    pub(crate) identity: Arc<dyn crate::SessionIdentity>,
}

impl SqlSnapshotAdmission {
    /// Redeems a token against the current context's catalog, clock and zone, with no server-side state.
    /// As in XTDB, a database is read by its name at the token's times: one replaced since the token was issued is read
    /// as it is now, and refuses with `xtdb/unindexed-tx` when it has not reached those times.
    pub(crate) async fn restore(
        &self, token: &str, current: &QueryContext, cancellation: &CancellationToken,
    ) -> Result<rtdb_query::SqlSnapshotView> {
        use rtdb_plan::Catalog;

        let name = current.catalog.default_database();
        self.node.admit_retained_catalog(&current.catalog, self.identity.as_ref(), cancellation).await?;
        let database = self.node.catalog().database(name)?;
        let current_database = current.catalog.database(name)?;
        let (times, cut) = decode(token, self.identity.user(), database.await_authority.codec())?;
        // A sealed token also names the storage generation it was read from, which XTDB's basis cannot express.
        if let (Some(issued), Some(redeemed)) = (cut, current_database.partitions.first().and_then(|snapshot| snapshot.catalog_cut()))
            && (issued.storage_version != redeemed.storage_version || issued.storage_epoch != redeemed.storage_epoch)
        {
            return Err(invalid_basis("the sealed token was read from another storage generation"));
        }
        let partitions = current.catalog.database_names().map(|name| {
            Ok((name.to_owned(), read_at(times.databases.get(name).map(Vec::as_slice), &current.catalog.database(name)?.partitions)?))
        }).collect::<Result<BTreeMap<_, _>>>()?;
        let mut view = rtdb_query::SqlSnapshotView::from_partitions(current, partitions)?;
        if !self.node.catalog().is_placed() { view.token_basis = Some(times); }
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
    use rtdb_types::{ReplicaPosition, StorageEpoch, StorageVersion, Timestamp, TransactionKey, TxId};

    fn cut() -> CatalogCut {
        CatalogCut { storage_version: StorageVersion(2), storage_epoch: StorageEpoch(7), replica_epoch: StorageEpoch(1), through: ReplicaPosition(20) }
    }

    #[test]
    fn a_plain_token_is_xtdbs_time_basis_and_needs_no_server_state() {
        // The token `xtdb.api/q` documents: database "xtdb", one partition at 2020-01-01T00:00:00Z.
        let (times, cut) = decode("ChAKBHh0ZGISCAoGCIDCr/AF", Some("anyone"), None).unwrap();
        assert_eq!(times.databases, BTreeMap::from([("xtdb".to_owned(), vec![Some(Instant::new(1_577_836_800, 0).unwrap())])]));
        assert_eq!(cut, None, "an XTDB token names no storage generation");
        for token in ["rtdb-snapshot:6f9619ff-8b86-d011-b42d-00c04fc964ff", "rtdb-snapshot-v3:e30", "not a token", &format!("{SEALED_PREFIX}abc")] {
            let error = decode(token, None, None).unwrap_err();
            assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb.basis/invalid-basis"), "{token}");
        }
    }

    #[test]
    fn redeeming_caps_the_current_catalog_at_the_issued_times_whenever_it_is_redeemed() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let partition = PartitionState::empty(rtdb_trie::TrieConfig::default()).unwrap();
        let apply = |key| {
            let write = TransactionBuffer::new(partition.snapshot(), key, Arc::clone(&memory)).unwrap().stage().unwrap();
            partition.apply(&write).unwrap();
        };
        let head = TransactionKey { tx_id: TxId(5), system_time: Timestamp(22) };
        apply(head);
        partition.publish_catalog_cut(cut()).unwrap();
        let issued = vec![Some(Instant::from(Timestamp(22)))];

        // Later transactions and catalog generations do not invalidate the token: there is no expiry or hold.
        apply(TransactionKey { tx_id: TxId(6), system_time: Timestamp(30) });
        partition.publish_catalog_cut(CatalogCut { through: ReplicaPosition(40), ..cut() }).unwrap();
        let redeemed = read_at(Some(&issued), &[partition.snapshot()]).unwrap().pop().unwrap();
        assert_eq!(redeemed.system_time_cap(), Some(Timestamp(22)));
        assert_eq!(redeemed.tx_basis().map(|key| key.tx_id), Some(TxId(6)));

        let ahead = vec![Some(Instant::from(Timestamp(90)))];
        assert_eq!(read_at(Some(&ahead), &[partition.snapshot()]).unwrap_err().code, "xtdb/unindexed-tx");
        let within_head_microsecond = [Some(Instant::new(0, 30_001).unwrap())];
        assert_eq!(read_at(Some(&within_head_microsecond), &[partition.snapshot()]).unwrap_err().code, "xtdb/unindexed-tx");
        // XTDB bounds such a scan by nothing but the snapshot it opened, so it sees through the head; an RTDB
        // snapshot already carries its head as its cap, which redeeming leaves alone.
        let uncapped = read_at(None, &[partition.snapshot()]).unwrap().pop().unwrap();
        assert_eq!(uncapped.system_time_cap(), Some(Timestamp(30)), "a database the token leaves out reads through its head, as in XTDB");
        let absent = read_at(Some(&vec![None]), &[partition.snapshot()]).unwrap().pop().unwrap();
        assert_eq!(absent.system_time_cap(), Some(Timestamp(30)), "a partition the token gives no time reads through its head");
        let recapped_to_head = read_at(None, &[redeemed]).unwrap().pop().unwrap();
        assert_eq!(recapped_to_head.system_time_cap(), Some(Timestamp(30)), "overriding a capped snapshot with a token omitting the database restores reading through its head");
    }
}
