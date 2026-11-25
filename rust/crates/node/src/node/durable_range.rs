//! Portable range observations authenticated by the placed history's receipt owner.

use super::*;
use rtdb_types::{EntityId, FixedRangeEvidence, OwnedEntityRegion, OwnedRangeEvidence, OwnedRelation, PlacedAttemptScope};
use serde::{Deserialize, Serialize};

pub const MAX_DURABLE_RANGE_BYTES: usize = 64 << 10;
const DOMAIN: &[u8] = b"rtdb.owned/durable-range/v1\0";

#[derive(Serialize, Deserialize)]
struct Evidence {
    owner: String,
    scope: Vec<u8>,
    relation: String,
    basis: TransactionKey,
    frontier: i64,
    until: Timestamp,
    scoped: bool,
    region: Option<Region>,
}

#[derive(Serialize, Deserialize)]
enum Region {
    Fixed(rtdb_types::Value),
    Entity { anchor: i64, id: rtdb_types::Value, from: Timestamp, to: Timestamp },
}

pub struct DurableRange {
    evidence: OwnedRangeWitness,
    scoped: bool,
}

impl DurableRange {
    pub fn requires_live_root(&self) -> bool { self.scoped }
    pub fn operation(self) -> TxOp { self.evidence.operation() }
    pub(super) fn evidence(&self) -> &OwnedRangeWitness { &self.evidence }
}

fn invalid() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/durable-range", "The original range evidence is unavailable")
}

impl OwnedReadSession {
    /// Called only after the native receiver completed an admitted range.
    pub fn seal_durable_range(&self, range: &OwnedRangeWitness) -> Result<String> {
        self.check_witness_relation(&range.relation, range)?;
        let database = self.node.attached_database(&self.database, Some(&self.attachment))?;
        let codec = database.await_authority.codec().filter(|codec| codec.scope() == range.scope).ok_or_else(invalid)?;
        let region = range.region.as_deref().map(|region| match region {
            OwnedRangeEvidence::FixedPath(fixed) => Region::Fixed(fixed.to_value()),
            OwnedRangeEvidence::Entity { anchor, region } => Region::Entity {
                anchor: *anchor, id: region.id.as_value(), from: region.valid.lower(), to: region.valid.upper(),
            },
        });
        let evidence = Evidence {
            owner: self.owner.clone(), scope: range.scope.encode().to_vec(), relation: range.relation.name().into(),
            basis: range.basis, frontier: range.frontier, until: range.until, scoped: self.is_scoped_root(), region,
        };
        let mut bytes = DOMAIN.to_vec();
        bytes.extend(serde_json::to_vec(&evidence).map_err(|_| invalid())?);
        if bytes.len() > MAX_DURABLE_RANGE_BYTES { return Err(invalid()); }
        codec.seal_range(&bytes)
    }
}

impl NativeSession {
    pub fn open_durable_range(&self, token: &str) -> Result<DurableRange> {
        let database = self.service.database()?;
        let codec = database.await_authority.codec().ok_or_else(invalid)?;
        let bytes = codec.open_range(token)?;
        let body = bytes.strip_prefix(DOMAIN).ok_or_else(invalid)?;
        let evidence: Evidence = serde_json::from_slice(body).map_err(|_| invalid())?;
        let source = PlacedAttemptScope::decode(&evidence.scope)?;
        let scope = codec.scope();
        if self.state.user() != Some(evidence.owner.as_str())
            || !scope.same_history(source) || source.writer_epoch() > scope.writer_epoch()
            || evidence.frontier < 0 || evidence.basis.tx_id.0 < 0
        {
            return Err(invalid());
        }
        let relation = OwnedRelation::new(&evidence.relation)?;
        if relation.world().is_some() || relation.is_world_catalog() { return Err(invalid()); }
        let region = match evidence.region {
            None => None,
            Some(Region::Fixed(fixed)) => Some(Arc::new(OwnedRangeEvidence::FixedPath(FixedRangeEvidence::from_value(&fixed)?))),
            Some(Region::Entity { anchor, id, from, to }) => {
                let id = match id {
                    rtdb_types::Value::Utf8(id) => EntityId::String(id),
                    rtdb_types::Value::Keyword(id) => EntityId::Keyword(id),
                    rtdb_types::Value::I64(id) => EntityId::Integer(id),
                    rtdb_types::Value::Uuid(id) => EntityId::Uuid(id),
                    rtdb_types::Value::Binary(id) => EntityId::Binary(id),
                    _ => return Err(invalid()),
                };
                Some(Arc::new(OwnedRangeEvidence::Entity {
                    anchor, region: OwnedEntityRegion { id, valid: rtdb_types::Interval::new(from, to)? },
                }))
            }
        };
        Ok(DurableRange {
            scoped: evidence.scoped,
            evidence: OwnedRangeWitness { relation, scope, basis: evidence.basis,
                frontier: evidence.frontier, until: evidence.until, region, revealed: None },
        })
    }
}
