use std::sync::Arc;

use rtdb_columnar::{MemoryReservation, estimate_read_value_bytes, read_execution_value};
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result, Timestamp, Value};

use super::{OwnedRangeWitness, OwnedReadSession};

const MAX_PAGE_ROWS: usize = 128;
const MAX_PAGE_BYTES: usize = 8 << 20;
const PROTECTED_PAGE_ROWS: usize = 64;
const CARRIER_PREFIX_BYTES: usize = 5 + 16 + 32 + 16 + 1 + 16 + 16 + 4;
const CARRIER_IMAGE_BYTES: usize = 4096;

/// A bounded key page with progress through its last returned identity.
pub struct OwnedIdentityPage {
    pub keys: Vec<String>,
    pub more: bool,
    _charges: Vec<Arc<MemoryReservation>>,
}

impl OwnedReadSession {
    /// Observe existence over the complete valid extent without opening ciphertext.
    pub async fn protected_row_carrier_exists(
        &self, relation: &OwnedRelation, id: &EntityId, scope: rtdb_types::PlacedAttemptScope,
    ) -> Result<bool> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        if self.witness_scope != Some(scope) || !is_protected_row_carrier(relation, id) {
            return Err(protected_page_unavailable());
        }
        let context = self.relation_context(relation)?;
        let mut present = false;
        if let Some(mut rows) = rtdb_query::open_owned_point(
            context, &self.owner, relation, id, Some(rtdb_types::Interval::ALL), Some(&["_id"]),
        ).await? {
            while let Some(batch) = rows.next_batch().await? {
                if batch.batch().num_rows() != 0 {
                    present = true;
                    break;
                }
            }
        }
        self.check_live_point_relation(relation)?;
        Ok(present)
    }

    /// Give the installed hosted projector a bounded page of ciphertext
    /// carrier identities and a witness only on completion of its prefix.
    pub async fn protected_carrier_page(
        &self,
        relation: &OwnedRelation,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
        scope: rtdb_types::PlacedAttemptScope,
    ) -> Result<(OwnedIdentityPage, Option<OwnedRangeWitness>)> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        if self.witness_scope != Some(scope)
            || relation.name() != "fact_unit_column_payloads"
            || !valid_carrier_prefix(prefix)
            || !(1..=PROTECTED_PAGE_ROWS).contains(&limit)
            || after.is_some_and(|id| !valid_carrier(id, prefix))
        {
            return Err(protected_page_unavailable());
        }
        let at = self.present.system_time;
        let page = self
            .identity_page_in_prefix(relation, at, at, after, limit, Some(prefix))
            .await?;
        if page.keys.iter().any(|id| !valid_carrier(id, prefix)) {
            return Err(protected_page_unavailable());
        }
        let witness = if page.more {
            None
        } else {
            Some(self.relation_witness(relation)?.ok_or_else(protected_page_unavailable)?)
        };
        Ok((page, witness))
    }

    /// Compare resident derived coverage with the exact carrier events in
    /// this originating hold. Coverage is comparable only in one live
    /// database attachment and placed writer history.
    pub async fn protected_carrier_coverage(
        &self,
        relation: &OwnedRelation,
        prefix: &str,
        scope: rtdb_types::PlacedAttemptScope,
    ) -> Result<([u8; 72], OwnedRangeWitness)> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        if self.witness_scope != Some(scope) || relation.name() != "fact_unit_column_payloads"
            || !valid_carrier_prefix(prefix)
        {
            return Err(protected_page_unavailable());
        }
        let witness = self.relation_witness(relation)?.ok_or_else(protected_page_unavailable)?;
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(protected_page_unavailable());
        };
        let purge = snapshot.relation_event_frontier(&relation.purge_table(&self.owner))?;
        let current = self.node.attached_database(&self.database, Some(&self.attachment))?;
        let mut coverage = [0; 72];
        coverage[..40].copy_from_slice(&scope.encode());
        coverage[40..56].copy_from_slice(current.protected_equality_epoch.as_bytes());
        coverage[56..64].copy_from_slice(&witness.frontier.to_be_bytes());
        coverage[64..].copy_from_slice(&purge.to_be_bytes());
        self.check_relation(relation)?;
        Ok((coverage, witness))
    }

    /// Reads live local shadow keys, including tombstones, for one admitted world.
    pub async fn world_identity_candidates(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, valid: Timestamp,
        after: Option<&str>, fetch: usize,
    ) -> Result<rtdb_query::OwnedIdentityCandidates> {
        self.world_identity_candidates_at(world, relation, self.present.system_time, valid, after, fetch).await
    }

    pub(super) async fn world_identity_candidates_at(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, system: Timestamp, valid: Timestamp,
        after: Option<&str>, fetch: usize,
    ) -> Result<rtdb_query::OwnedIdentityCandidates> {
        let (admitted, _charge) = self.world_registration(world).await?
            .filter(|(world, _)| !world.dropped && world.until > self.present.system_time)
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        let context = self.relation_context(relation)?;
        let relation = relation.in_world(admitted.id)?;
        let page = rtdb_query::owned_identity_candidates(
            Arc::clone(&context), &self.owner, &relation, system, valid, after, fetch,
        ).await?;
        context.check_cancelled()?;
        Ok(page)
    }

    pub async fn identity_page(
        &self,
        relation: &OwnedRelation,
        system: Timestamp,
        valid: Timestamp,
        after: Option<&str>,
        limit: usize,
    ) -> Result<OwnedIdentityPage> {
        self.identity_page_in_prefix(relation, system, valid, after, limit, None).await
    }

    /// Root identities with any assertion current at `system`, across all
    /// valid time, in key order. Adjacent valid slices of one key collapse;
    /// a saturated fetch reports more, since repeats consumed it.
    pub async fn identity_page_all_valid(
        &self, relation: &OwnedRelation, system: Timestamp, after: Option<&str>, limit: usize,
    ) -> Result<OwnedIdentityPage> {
        if !(1..=MAX_PAGE_ROWS).contains(&limit) {
            return Err(Anomaly::incorrect(
                "rtdb.owned/identity-page",
                "An identity page must request between 1 and 128 keys",
            ));
        }
        let at_head = self.admit_root_system(relation, system).await?;
        let context = self.relation_context(relation)?;
        let fetch = (limit + 1) * 4;
        let mut charges = vec![self.context.memory.reserve(fetch * 128)?];
        let mut keys: Vec<String> = Vec::with_capacity(limit + 1);
        let mut remaining = MAX_PAGE_BYTES;
        let mut more = false;
        let mut fetched = 0usize;
        if let Some(mut cursor) = rtdb_query::open_owned_identity_page_all_valid(
            Arc::clone(&context), &self.owner, relation, system, after, fetch as i64,
        ).await? {
            'page: while let Some(batch) = cursor.next_batch().await? {
                let batch = batch.batch();
                let (column, field) = batch.schema_ref().column_with_name("_id").ok_or_else(invalid_page)?;
                let array = batch.column(column).as_ref();
                for row in 0..batch.num_rows() {
                    fetched += 1;
                    let Value::Utf8(key) = read_execution_value(field, array, row)? else {
                        return Err(invalid_page());
                    };
                    let previous = keys.last().map(String::as_str).or(after);
                    if previous.is_some_and(|previous| previous == key.as_str()) && !keys.is_empty() {
                        continue;
                    }
                    if previous.is_some_and(|previous| previous >= key.as_str()) {
                        return Err(invalid_page());
                    }
                    if keys.len() == limit {
                        more = true;
                        break 'page;
                    }
                    let bytes = estimate_read_value_bytes(field, array, row)?;
                    let Some(rest) = remaining.checked_sub(bytes) else {
                        if keys.is_empty() {
                            return Err(Anomaly::new(
                                AnomalyCategory::Busy,
                                "rtdb.owned/identity-size",
                                "An identity exceeds the page's retained byte bound",
                            ));
                        }
                        more = true;
                        break 'page;
                    };
                    remaining = rest;
                    charges.push(self.context.memory.reserve(bytes)?);
                    if !at_head {
                        self.check_point_purge_floor(relation, &EntityId::String(key.clone()), system).await?;
                    }
                    keys.push(key);
                }
            }
        }
        if fetched >= fetch {
            more = true;
        }
        context.check_cancelled()?;
        Ok(OwnedIdentityPage { keys, more, _charges: charges })
    }

    async fn identity_page_in_prefix(
        &self, relation: &OwnedRelation, system: Timestamp, valid: Timestamp, after: Option<&str>,
        limit: usize, prefix: Option<&str>,
    ) -> Result<OwnedIdentityPage> {
        if !(1..=MAX_PAGE_ROWS).contains(&limit) {
            return Err(Anomaly::incorrect(
                "rtdb.owned/identity-page",
                "An identity page must request between 1 and 128 keys",
            ));
        }
        let at_head = self.admit_root_system(relation, system).await?;
        let context = self.relation_context(relation)?;
        let count = limit + 1;
        let mut charges = vec![self.context.memory.reserve(count * 128)?];
        let mut keys: Vec<String> = Vec::with_capacity(count);
        let mut remaining = MAX_PAGE_BYTES;
        let mut more = false;
        let cursor = match prefix {
            Some(prefix) if at_head && valid == system => rtdb_query::open_owned_identity_prefix(
                Arc::clone(&context), &self.owner, relation, system, after, prefix, count as i64,
            ).await?,
            Some(_) => return Err(invalid_page()),
            None => rtdb_query::open_owned_identity_page(
                Arc::clone(&context), &self.owner, relation, system, valid, after, count as i64,
            ).await?,
        };
        if let Some(mut cursor) = cursor {
            'page: while let Some(batch) = cursor.next_batch().await? {
                let batch = batch.batch();
                let (column, field) = batch
                    .schema_ref()
                    .column_with_name("_id")
                    .ok_or_else(invalid_page)?;
                let array = batch.column(column).as_ref();
                for row in 0..batch.num_rows() {
                    if keys.len() == limit {
                        more = true;
                        break 'page;
                    }
                    let bytes = estimate_read_value_bytes(field, array, row)?;
                    let Some(rest) = remaining.checked_sub(bytes) else {
                        if keys.is_empty() {
                            return Err(Anomaly::new(
                                AnomalyCategory::Busy,
                                "rtdb.owned/identity-size",
                                "An identity exceeds the page's retained byte bound",
                            ));
                        }
                        more = true;
                        break 'page;
                    };
                    remaining = rest;
                    charges.push(self.context.memory.reserve(bytes)?);
                    let Value::Utf8(key) = read_execution_value(field, array, row)? else {
                        return Err(invalid_page());
                    };
                    let previous = keys.last().map(String::as_str).or(after);
                    if previous.is_some_and(|previous| previous >= key.as_str())
                        || prefix.is_some_and(|prefix| !key.starts_with(prefix))
                    {
                        return Err(invalid_page());
                    }
                    if !at_head {
                        self.check_point_purge_floor(
                            relation,
                            &EntityId::String(key.clone()),
                            system,
                        )
                        .await?;
                    }
                    keys.push(key);
                }
            }
        }
        context.check_cancelled()?;
        Ok(OwnedIdentityPage {
            keys,
            more,
            _charges: charges,
        })
    }
}

fn invalid_page() -> Anomaly {
    Anomaly::fault(
        "rtdb.owned/identity-order",
        "The owned identity query returned invalid keys",
    )
}

fn lower_hex(text: &str) -> bool {
    text.bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_protected_row_carrier(relation: &OwnedRelation, id: &EntityId) -> bool {
    relation.name() == "fact_unit_column_payloads"
        && matches!(id, EntityId::String(id) if id.starts_with("row/") && id.len() > 4)
}

#[cfg(test)]
#[test]
fn held_carrier_existence_has_only_the_row_payload_target() -> Result<()> {
    let payloads = OwnedRelation::new("fact_unit_column_payloads")?;
    let row = EntityId::String("row/opaque".to_owned());
    assert!(is_protected_row_carrier(&payloads, &row));
    assert!(!is_protected_row_carrier(&payloads, &EntityId::String("row/".to_owned())));
    assert!(!is_protected_row_carrier(&payloads, &EntityId::String("column/opaque".to_owned())));
    assert!(!is_protected_row_carrier(&OwnedRelation::new("fact_unit_fences")?, &row));
    Ok(())
}

fn valid_carrier_prefix(prefix: &str) -> bool {
    prefix.len() == 2 * CARRIER_PREFIX_BYTES
        && prefix.starts_with("4246435032")
        && lower_hex(prefix)
}

fn valid_carrier(id: &str, prefix: &str) -> bool {
    id.len() == 2 * (CARRIER_PREFIX_BYTES + CARRIER_IMAGE_BYTES)
        && id.starts_with(prefix)
        && lower_hex(id)
}

fn protected_page_unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/protected-equality",
        "The installed protected equality partition is unavailable",
    )
}
