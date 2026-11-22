use std::sync::Arc;

use rtdb_columnar::{MemoryReservation, estimate_read_value_bytes, read_execution_value};
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result, Timestamp, Value};

use super::OwnedReadSession;

const MAX_PAGE_ROWS: usize = 128;
const MAX_PAGE_BYTES: usize = 8 << 20;

/// A bounded key page with progress through its last returned identity.
pub struct OwnedIdentityPage {
    pub keys: Vec<String>,
    pub more: bool,
    _charges: Vec<Arc<MemoryReservation>>,
}

impl OwnedReadSession {
    /// Reads live local shadow keys, including tombstones, for one admitted world.
    pub async fn world_identity_candidates(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, valid: Timestamp,
        after: Option<&str>, fetch: usize,
    ) -> Result<rtdb_query::OwnedIdentityCandidates> {
        let (admitted, _charge) = self.world_registration(world).await?
            .filter(|(world, _)| !world.dropped && world.until > self.present.system_time)
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        let context = self.relation_context(relation)?;
        let relation = relation.in_world(admitted.id)?;
        let page = rtdb_query::owned_identity_candidates(
            Arc::clone(&context), &self.owner, &relation, self.present.system_time, valid, after, fetch,
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
        if let Some(mut cursor) = rtdb_query::open_owned_identity_page(
            Arc::clone(&context),
            &self.owner,
            relation,
            system,
            valid,
            after,
            count as i64,
        )
        .await?
        {
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
                    if previous.is_some_and(|previous| previous >= key.as_str()) {
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
