//! Bounded derived equality postings for one installed protected partition.
//! Postings live only in the original held reader's memory. Neither the
//! journal nor checkpoint objects receive token bytes.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use rtdb_columnar::{MemoryReservation, read_execution_value};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result, TransactionKey, Value,
};

use super::{OwnedRangeWitness, OwnedReadSession};

const MAX_ROWS: usize = 512;
const PAGE_ROWS: usize = 64;
const MAX_KEY_BYTES: usize = 4096;
const MAX_RESULTS: usize = 32;
const MAX_ENVELOPE_BYTES: usize = 8 << 20;
const COLUMN: &str = "sealed_payload";
const CARRIER_PREFIX_BYTES: usize = 5 + 16 + 32 + 16 + 1 + 16 + 16 + 4;
const CARRIER_IMAGE_BYTES: usize = 4096;

/// Trusted installation owner for protected equality. There is no default
/// projector and the node has no transport for supplying one as query input.
#[async_trait]
pub trait ProtectedEqualityProjector: Send + Sync {
    /// Admit the exact installed label, one shared unit, and field/path
    /// generation for this source and held cut. Refuse scopes outside this
    /// family's complete, acceptance-valid carrier relation.
    async fn bind(
        &self,
        database: &str,
        owner: &str,
        relation: &OwnedRelation,
        path: &OwnedRelation,
        basis: TransactionKey,
    ) -> Result<Arc<dyn ProtectedEqualityPartition>>;
}

/// A live original key-service lease for one partition. Every method belongs
/// to the same lease. Invalid bindings, unknown coverage, and erased units
/// refuse the entire partition, before any result can be interpreted.
#[async_trait]
pub trait ProtectedEqualityPartition: Send + Sync {
    /// The exact canonical carrier prefix for this installed field and unit.
    fn carrier_prefix(&self) -> &str;
    /// Confirm the live ordered unit exclusion and original label/key standing.
    /// A held historical image or cached key alone cannot authorize this check.
    async fn require_current(&self) -> Result<()>;

    /// Verify this BJS2 envelope's exact field, carrier, label, unit and
    /// generation, then derive its opaque token inside the custody service.
    async fn project(&self, id: &str, envelope: &[u8]) -> Result<[u8; 16]>;

    /// Canonicalize a complete equality operand under the same declaration.
    async fn operand(&self, canonical: &[u8]) -> Result<[u8; 16]>;

    /// Reconcile a token hit with the actual value to exclude collisions.
    async fn matches(&self, id: &str, envelope: &[u8], canonical: &[u8]) -> Result<bool>;
}

pub struct HeldProtectedEquality<'a> {
    held: &'a OwnedReadSession,
    relation: OwnedRelation,
    custody: Arc<dyn ProtectedEqualityPartition>,
    witness: OwnedRangeWitness,
    postings: BTreeMap<[u8; 16], Vec<String>>,
    _charges: Vec<Arc<MemoryReservation>>,
}

impl OwnedReadSession {
    /// Build a small complete derived index from the admitted live root cut.
    /// The owned relation is private to the admitted installation; this path
    /// does not read a historical restore, world overlay, or lifetime table.
    pub async fn build_protected_equality(
        &self,
        relation: &OwnedRelation,
        path: &OwnedRelation,
    ) -> Result<HeldProtectedEquality<'_>> {
        self.admit_read().await?;
        if relation.world().is_some() || relation.is_world_catalog() {
            return Err(unavailable());
        }
        let witness = self.relation_witness(relation)?.ok_or_else(unavailable)?;
        let projector = self
            .node
            .factory
            .resources
            .protected_equality()
            .ok_or_else(unavailable)?;
        let custody = projector
            .bind(&self.database, &self.owner, relation, path, self.present)
            .await?;
        let mut index = HeldProtectedEquality {
            held: self,
            relation: relation.clone(),
            custody,
            witness,
            postings: BTreeMap::new(),
            _charges: Vec::new(),
        };
        let prefix = index.custody.carrier_prefix();
        if prefix.len() != 2 * CARRIER_PREFIX_BYTES || !prefix.starts_with("4246435032") {
            return Err(unavailable());
        }
        let mut after = None;
        let mut scanned = 0;
        loop {
            index.require_current().await?;
            let at = self.present.system_time;
            let page = self
                .identity_page_in_prefix(
                    relation,
                    at,
                    at,
                    after.as_deref(),
                    PAGE_ROWS,
                    Some(prefix),
                )
                .await?;
            for id in &page.keys {
                scanned += 1;
                if scanned > MAX_ROWS {
                    return Err(bound());
                }
                if id.len() != 2 * (CARRIER_PREFIX_BYTES + CARRIER_IMAGE_BYTES)
                    || !id
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                {
                    return Err(unavailable());
                }
                let (envelope, _charge) = envelope(self, relation, id).await?;
                let token = index.custody.project(id, &envelope).await?;
                let charge = self.context.memory.reserve(id.len().saturating_add(256))?;
                index._charges.push(charge);
                index.postings.entry(token).or_default().push(id.clone());
            }
            if !page.more {
                break;
            }
            after = Some(page.keys.last().cloned().ok_or_else(unavailable)?);
        }
        index.require_current().await?;
        Ok(index)
    }
}

impl HeldProtectedEquality<'_> {
    async fn require_current(&self) -> Result<()> {
        self.held.admit_read().await?;
        self.held.check_relation(&self.relation)?;
        self.custody.require_current().await?;
        self.held.check_relation(&self.relation)
    }

    /// Refine only one posting bucket on the same hold and release with its
    /// original relation witness. The consumer retains this index and must
    /// revalidate again at final delivery if values outlive this callback.
    pub async fn lookup<R>(
        &self,
        canonical: &[u8],
        receive: impl FnOnce(&[&str], OwnedRangeWitness) -> R,
    ) -> Result<R> {
        if canonical.is_empty() || canonical.len() > MAX_KEY_BYTES {
            return Err(bound());
        }
        self.require_current().await?;
        let token = self.custody.operand(canonical).await?;
        let mut matches = Vec::new();
        for id in self.postings.get(&token).into_iter().flatten() {
            let (envelope, _charge) = envelope(self.held, &self.relation, id).await?;
            if self.custody.matches(id, &envelope, canonical).await? {
                if matches.len() >= MAX_RESULTS {
                    return Err(bound());
                }
                matches.push(id.as_str());
            }
        }
        self.require_current().await?;
        Ok(receive(&matches, self.witness.clone()))
    }

    pub async fn check_release(&self) -> Result<()> {
        self.require_current().await
    }
}

async fn envelope(
    held: &OwnedReadSession,
    relation: &OwnedRelation,
    id: &str,
) -> Result<(Vec<u8>, Arc<MemoryReservation>)> {
    let entity = EntityId::String(id.to_owned());
    let Some(mut cursor) = held
        .open_point_at(relation, &entity, Some(held.present.system_time))
        .await?
    else {
        return Err(unavailable());
    };
    let mut output = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        if batch.num_rows() == 0 {
            continue;
        }
        if output.is_some() || batch.num_rows() != 1 {
            return Err(unavailable());
        }
        let read = |name: &str| {
            let (index, field) = batch
                .schema_ref()
                .column_with_name(name)
                .ok_or_else(unavailable)?;
            read_execution_value(field, batch.column(index).as_ref(), 0)
        };
        let Value::Utf8(actual) = read("_id")? else {
            return Err(unavailable());
        };
        if actual != id {
            return Err(unavailable());
        }
        let (index, field) = batch
            .schema_ref()
            .column_with_name(COLUMN)
            .ok_or_else(unavailable)?;
        let column = batch.column(index).as_ref();
        let bytes = rtdb_columnar::estimate_read_value_bytes(field, column, 0)?;
        if bytes > MAX_ENVELOPE_BYTES {
            return Err(bound());
        }
        let charge = held.context.memory.reserve(bytes)?;
        let Value::Binary(envelope) = read_execution_value(field, column, 0)? else {
            return Err(unavailable());
        };
        if !envelope.starts_with(b"BJS2") {
            return Err(unavailable());
        }
        output = Some((envelope, charge));
    }
    output.ok_or_else(unavailable)
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/protected-equality",
        "The installed protected equality partition is unavailable",
    )
}

fn bound() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Busy,
        "rtdb.owned/protected-equality-bound",
        "The protected equality work exceeds its bound",
    )
}
