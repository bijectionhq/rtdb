use std::{cmp::Ordering, sync::Arc};

use arrow_array::{ArrayRef, BinaryArray, builder::{BinaryBuilder, StringBuilder}};
use rtdb_columnar::{EntityIdRef, MemoryReservation, estimate_read_value_bytes, read_owned_entity_id, retain_array};
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, OwnedIdentityScan, OwnedRelation, OwnedSchema, PlacedAttemptScope, Result,
    SEALED_SEARCH_UNIT_ROWS, SHARED_COLUMN_CARRIERS, SharedColumnCarrierPrefix, Timestamp, sealed_search_inventory_row};

use super::{OwnedRangeWitness, OwnedReadSession};

const MAX_PAGE_ROWS: usize = 128;
const MAX_PAGE_BYTES: usize = 8 << 20;
const PROTECTED_PAGE_ROWS: usize = 64;

/// A bounded key page with progress through its last returned identity.
pub struct OwnedIdentityPage {
    pub keys: Vec<EntityId>,
    pub more: bool,
    _charge: Arc<MemoryReservation>,
}

/// A validated Root key page for a consumer that can render Arrow identities.
pub enum OwnedIdentityPageOutput {
    Rows(OwnedIdentityPage),
    Arrow(OwnedIdentityArrowPage),
}

/// The page keeps the reservations for its compact identity column alive.
pub struct OwnedIdentityArrowPage {
    ids: ArrayRef,
    pub more: bool,
}

impl OwnedIdentityArrowPage {
    pub fn ids(&self) -> &ArrayRef { &self.ids }
}

enum PageKeys {
    Rows(Vec<EntityId>),
    Text(StringBuilder),
    Binary(BinaryBuilder),
}

impl PageKeys {
    fn new(scan: Option<&OwnedIdentityScan<'_>>, count: usize) -> Self {
        match scan {
            Some(OwnedIdentityScan::Text(_)) => Self::Text(StringBuilder::with_capacity(count, 0)),
            Some(OwnedIdentityScan::Binary(_)) => Self::Binary(BinaryBuilder::with_capacity(count, 0)),
            _ => Self::Rows(Vec::with_capacity(count)),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Rows(keys) => keys.len(),
            Self::Text(keys) => keys.offsets_slice().len() - 1,
            Self::Binary(keys) => keys.offsets_slice().len() - 1,
        }
    }

    fn compare_last(&self, key: EntityIdRef<'_>) -> Option<Ordering> {
        let mut integer = [0; 8];
        let current = key.handle_parts(&mut integer);
        let previous = match self {
            Self::Rows(keys) => return keys.last().map(|previous| EntityIdRef::from(previous).cmp(&key)),
            Self::Text(keys) => (0, last_key_bytes(keys.values_slice(), keys.offsets_slice())?),
            Self::Binary(keys) => (4, last_key_bytes(keys.values_slice(), keys.offsets_slice())?),
        };
        Some(previous.cmp(&current))
    }

    fn follows_last(&self, key: EntityIdRef<'_>, bound: Option<EntityIdRef<'_>>) -> bool {
        self.compare_last(key).or_else(|| bound.map(|previous| previous.cmp(&key))).is_none_or(Ordering::is_lt)
    }

    fn seek_after_last(&self) -> Result<OwnedIdentityScan<'_>> {
        match self {
            Self::Rows(keys) => OwnedIdentityScan::seek(keys.last().ok_or_else(invalid_page)?),
            Self::Text(keys) => {
                let key = last_key_bytes(keys.values_slice(), keys.offsets_slice()).ok_or_else(invalid_page)?;
                let key = std::str::from_utf8(key).map_err(|_| invalid_page())?;
                Ok(OwnedIdentityScan::text(Some(key)))
            }
            Self::Binary(keys) => {
                let key = last_key_bytes(keys.values_slice(), keys.offsets_slice()).ok_or_else(invalid_page)?;
                Ok(OwnedIdentityScan::binary(Some(key)))
            }
        }
    }

    fn push(&mut self, key: EntityIdRef<'_>, capacity: usize) {
        match (self, key) {
            (Self::Rows(keys), key) => keys.push(key.into_owned()),
            (Self::Text(keys), EntityIdRef::String(key)) => keys.append_value(key),
            (Self::Binary(keys), EntityIdRef::Binary(key)) => keys.append_value(key),
            (output, key) => {
                let mut rows = Vec::with_capacity(capacity);
                match output {
                    Self::Text(keys) => rows.extend(keys.finish().iter().flatten().map(|key| EntityIdRef::String(key).into_owned())),
                    Self::Binary(keys) => rows.extend(keys.finish().iter().flatten().map(|key| EntityIdRef::Binary(key).into_owned())),
                    Self::Rows(_) => unreachable!(),
                }
                rows.push(key.into_owned());
                *output = Self::Rows(rows);
            }
        }
    }

    fn finish(self, more: bool, charge: Arc<MemoryReservation>) -> Result<OwnedIdentityPageOutput> {
        let ids: ArrayRef = match self {
            Self::Rows(keys) => return Ok(OwnedIdentityPageOutput::Rows(OwnedIdentityPage { keys, more, _charge: charge })),
            Self::Text(mut keys) => Arc::new(keys.finish()),
            Self::Binary(mut keys) => Arc::new(keys.finish()),
        };
        let ids = retain_array(ids, &charge)?;
        Ok(OwnedIdentityPageOutput::Arrow(OwnedIdentityArrowPage { ids, more }))
    }
}

fn last_key_bytes<'a>(bytes: &'a [u8], offsets: &[i32]) -> Option<&'a [u8]> {
    let [.., start, end] = offsets else { return None; };
    Some(&bytes[*start as usize..*end as usize])
}

fn identity_bound<'a>(scan: &'a OwnedIdentityScan<'_>) -> Option<EntityIdRef<'a>> {
    match scan {
        OwnedIdentityScan::Text(bound) => bound.as_deref().map(EntityIdRef::String),
        OwnedIdentityScan::Binary(bound) => bound.as_deref().map(EntityIdRef::Binary),
        OwnedIdentityScan::Unbounded => None,
    }
}

fn checked_binary_prefix(
    page: OwnedIdentityPageOutput, contains: impl Fn(&[u8]) -> bool, unavailable: fn() -> Anomaly,
) -> Result<OwnedIdentityArrowPage> {
    let OwnedIdentityPageOutput::Arrow(page) = page else { return Err(unavailable()); };
    let keys = page.ids().as_any().downcast_ref::<BinaryArray>().ok_or_else(unavailable)?;
    if keys.iter().any(|key| !key.is_some_and(&contains)) {
        return Err(unavailable());
    }
    Ok(page)
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

    /// Gives the installed hosted projector a bounded Binary page of ciphertext carrier identities.
    /// The relation witness is available on the terminal page of the prefix.
    pub async fn protected_carrier_page(
        &self,
        relation: &OwnedRelation,
        prefix: SharedColumnCarrierPrefix<'_>,
        after: Option<&[u8]>,
        limit: usize,
        scope: rtdb_types::PlacedAttemptScope,
    ) -> Result<(OwnedIdentityArrowPage, Option<OwnedRangeWitness>)> {
        if !(1..=PROTECTED_PAGE_ROWS).contains(&limit) || after.is_some_and(|id| !prefix.contains(id)) {
            return Err(protected_page_unavailable());
        }
        self.admit_shared_column_carriers(relation, scope).await?;
        let page = self.read_identity_prefix(relation, prefix.as_bytes(), after, limit).await?;
        let page = checked_binary_prefix(page, |id| prefix.contains(id), protected_page_unavailable)?;
        let witness = if page.more {
            None
        } else {
            Some(self.relation_witness(relation)?.ok_or_else(protected_page_unavailable)?)
        };
        Ok((page, witness))
    }

    pub async fn sealed_search_unit_rows_page(
        &self, relation: &OwnedRelation, prefix: &[u8; 32], after: Option<&[u8]>, limit: usize, scope: PlacedAttemptScope,
    ) -> Result<(OwnedIdentityArrowPage, Option<OwnedRangeWitness>, impl Fn() -> Result<()> + Send + Sync + use<>)> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        if self.witness_scope != Some(scope) || relation.name() != SEALED_SEARCH_UNIT_ROWS {
            return Err(sealed_search_unit_unavailable());
        }
        if !(1..=PROTECTED_PAGE_ROWS).contains(&limit)
            || after.is_some_and(|id| sealed_search_inventory_row(prefix, id).is_none())
        {
            return Err(Anomaly::incorrect("rtdb.owned/sealed-search-unit", "Invalid sealed search unit prefix, continuation or limit"));
        }
        let context = self.relation_context(relation)?;
        let expected = OwnedSchema::from_json(br#"{"fields":[]}"#)?;
        if !rtdb_query::read_owned_relation_schema(context, &self.owner, relation).await?
            .is_some_and(|(schema, _)| schema == expected)
        {
            return Err(sealed_search_unit_unavailable());
        }
        let original = self.original_inventory_provider_guard(scope).await?;
        let page = self.read_identity_prefix(relation, prefix, after, limit).await?;
        let page = checked_binary_prefix(page, |id| sealed_search_inventory_row(prefix, id).is_some(), sealed_search_unit_unavailable)?;
        let witness = if page.more { None } else {
            Some(self.relation_witness(relation)?.ok_or_else(sealed_search_unit_unavailable)?)
        };
        original()?;
        Ok((page, witness, original))
    }

    /// Compare resident derived coverage with the exact carrier events in
    /// this originating hold. Coverage is comparable only in one live
    /// database attachment and placed writer history.
    pub async fn protected_carrier_coverage(
        &self,
        relation: &OwnedRelation,
        _prefix: SharedColumnCarrierPrefix<'_>,
        scope: rtdb_types::PlacedAttemptScope,
    ) -> Result<([u8; 72], OwnedRangeWitness)> {
        self.admit_shared_column_carriers(relation, scope).await?;
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

    async fn admit_shared_column_carriers(&self, relation: &OwnedRelation, scope: PlacedAttemptScope) -> Result<()> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        if self.witness_scope != Some(scope) || relation.name() != SHARED_COLUMN_CARRIERS {
            return Err(protected_page_unavailable());
        }
        let context = self.relation_context(relation)?;
        if !rtdb_query::read_owned_relation_schema(context, &self.owner, relation).await?
            .is_some_and(|(schema, _)| schema.is_opaque_root_carrier())
        {
            return Err(protected_page_unavailable());
        }
        Ok(())
    }

    /// Reads live local shadow keys, including tombstones, for one admitted world.
    pub async fn world_identity_candidates(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, valid: Timestamp,
        after: &OwnedIdentityScan<'_>, fetch: usize,
    ) -> Result<rtdb_query::OwnedIdentityCandidates> {
        self.world_identity_candidates_at(world, relation, self.present.system_time, valid, after, fetch).await
    }

    pub async fn world_identity_candidates_in_prefix(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, valid: Timestamp,
        after: Option<&str>, fetch: usize, prefix: &str,
    ) -> Result<rtdb_query::OwnedIdentityCandidates> {
        self.world_identity_candidates_on(world, relation, self.present.system_time, valid, &OwnedIdentityScan::text(after), fetch, Some(prefix)).await
    }

    pub(super) async fn world_identity_candidates_at(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, system: Timestamp, valid: Timestamp,
        after: &OwnedIdentityScan<'_>, fetch: usize,
    ) -> Result<rtdb_query::OwnedIdentityCandidates> {
        self.world_identity_candidates_on(world, relation, system, valid, after, fetch, None).await
    }

    async fn world_identity_candidates_on(
        &self, world: rtdb_types::Iid, relation: &OwnedRelation, system: Timestamp, valid: Timestamp,
        after: &OwnedIdentityScan<'_>, fetch: usize, prefix: Option<&str>,
    ) -> Result<rtdb_query::OwnedIdentityCandidates> {
        let (admitted, _charge) = self.world_registration(world).await?
            .filter(|(world, _)| !world.dropped && world.until > self.present.system_time)
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        let context = self.relation_context(relation)?;
        let relation = relation.in_world(admitted.id)?;
        let page = rtdb_query::owned_identity_candidates_in_prefix(
            Arc::clone(&context), &self.owner, &relation, system, valid, after, fetch, prefix.map(EntityIdRef::String),
        ).await?;
        context.check_cancelled()?;
        Ok(page)
    }

    pub async fn identity_page(
        &self,
        relation: &OwnedRelation,
        system: Timestamp,
        valid: Timestamp,
        after: &OwnedIdentityScan<'_>,
        limit: usize,
    ) -> Result<OwnedIdentityPage> {
        match self.read_identity_page(relation, system, valid, after, limit, None, false).await? {
            OwnedIdentityPageOutput::Rows(page) => Ok(page),
            OwnedIdentityPageOutput::Arrow(_) => unreachable!("Requested owned keys"),
        }
    }

    /// Reads a typed Root page, using compact funded Arrow keys at the held head.
    pub async fn identity_page_output(
        &self, relation: &OwnedRelation, system: Timestamp, valid: Timestamp, after: &OwnedIdentityScan<'_>,
        limit: usize,
    ) -> Result<OwnedIdentityPageOutput> {
        self.read_identity_page(relation, system, valid, after, limit, None, true).await
    }

    /// Returns Root identities with any assertion current at `system`, across all valid time.
    /// Each key appears once in ascending order after the exclusive bound.
    /// The page stops at the row or byte bound and reports whether another key remains.
    pub async fn identity_page_all_valid(
        &self, relation: &OwnedRelation, system: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize,
    ) -> Result<OwnedIdentityPage> {
        match self.read_identity_page_all_valid(relation, system, after, limit, false).await? {
            OwnedIdentityPageOutput::Rows(page) => Ok(page),
            OwnedIdentityPageOutput::Arrow(_) => unreachable!("Requested owned keys"),
        }
    }

    /// Returns the same keys and continuation as `Self::identity_page_all_valid`.
    /// At the held head a typed scan can return funded Arrow keys.
    pub async fn identity_page_all_valid_output(
        &self, relation: &OwnedRelation, system: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize,
    ) -> Result<OwnedIdentityPageOutput> {
        self.read_identity_page_all_valid(relation, system, after, limit, true).await
    }

    async fn read_identity_page_all_valid(
        &self, relation: &OwnedRelation, system: Timestamp, after: &OwnedIdentityScan<'_>, limit: usize, arrow: bool,
    ) -> Result<OwnedIdentityPageOutput> {
        const FETCH: usize = 256;
        if !(1..=MAX_PAGE_ROWS).contains(&limit) {
            return Err(Anomaly::incorrect(
                "rtdb.owned/identity-page",
                "An identity page must request between 1 and 128 keys",
            ));
        }
        let at_head = self.admit_root_system(relation, system).await?;
        let context = self.relation_context(relation)?;
        let mut charge = self.context.memory.reserve(FETCH * 128)?;
        let count = limit + 1;
        let mut keys = PageKeys::new((at_head && arrow).then_some(after), count);
        let mut remaining = MAX_PAGE_BYTES;
        let mut more = false;
        let bound = identity_bound(after);
        let mut reseek = false;
        'fetch: loop {
            let cursor = {
                let seek = reseek.then(|| keys.seek_after_last()).transpose()?;
                rtdb_query::open_owned_identity_page_all_valid(
                    Arc::clone(&context), &self.owner, relation, system, seek.as_ref().unwrap_or(after), FETCH as i64,
                ).await?
            };
            let Some(mut cursor) = cursor else { break; };
            let mut fetched = 0usize;
            while let Some(batch) = cursor.next_batch().await? {
                let batch = batch.batch();
                let (column, field) = batch.schema_ref().column_with_name("_id").ok_or_else(invalid_page)?;
                let array = batch.column(column).as_ref();
                for row in 0..batch.num_rows() {
                    fetched += 1;
                    let key = read_owned_entity_id(field, array, row).map_err(|_| invalid_page())?;
                    match keys.compare_last(key) {
                        Some(Ordering::Equal) => continue,
                        Some(Ordering::Greater) => return Err(invalid_page()),
                        None if bound.is_some_and(|previous| previous >= key) => return Err(invalid_page()),
                        _ => (),
                    }
                    if keys.len() == limit {
                        more = true;
                        break 'fetch;
                    }
                    let bytes = estimate_read_value_bytes(field, array, row)?;
                    let Some(rest) = remaining_page_bytes(remaining, bytes, keys.len())? else {
                        more = true;
                        break 'fetch;
                    };
                    remaining = rest;
                    Arc::get_mut(&mut charge).expect("Identity page owns its reservation")
                        .grow_to(FETCH * 128 + MAX_PAGE_BYTES - remaining)?;
                    if !at_head {
                        self.point_purge_guard(relation, key, system, false).await?;
                    }
                    keys.push(key, count);
                }
            }
            if fetched < FETCH {
                break;
            }
            reseek = true;
        }
        context.check_cancelled()?;
        keys.finish(more, charge)
    }

    async fn read_identity_prefix(
        &self, relation: &OwnedRelation, prefix: &[u8], after: Option<&[u8]>, limit: usize,
    ) -> Result<OwnedIdentityPageOutput> {
        let at = self.present.system_time;
        self.read_identity_page(relation, at, at, &OwnedIdentityScan::binary(after), limit,
            Some(EntityIdRef::Binary(prefix)), true).await
    }

    async fn read_identity_page(
        &self, relation: &OwnedRelation, system: Timestamp, valid: Timestamp, after: &OwnedIdentityScan<'_>,
        limit: usize, prefix: Option<EntityIdRef<'_>>, arrow: bool,
    ) -> Result<OwnedIdentityPageOutput> {
        if !(1..=MAX_PAGE_ROWS).contains(&limit) {
            return Err(Anomaly::incorrect(
                "rtdb.owned/identity-page",
                "An identity page must request between 1 and 128 keys",
            ));
        }
        let at_head = self.admit_root_system(relation, system).await?;
        let context = self.relation_context(relation)?;
        let count = limit + 1;
        let mut charge = self.context.memory.reserve(count * 128)?;
        let mut keys = PageKeys::new((at_head && arrow).then_some(after), count);
        let mut remaining = MAX_PAGE_BYTES;
        let mut more = false;
        let bound = identity_bound(after);
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
                    let Some(rest) = remaining_page_bytes(remaining, bytes, keys.len())? else {
                        more = true;
                        break 'page;
                    };
                    remaining = rest;
                    Arc::get_mut(&mut charge).expect("Identity page owns its reservation")
                        .grow_to(count * 128 + MAX_PAGE_BYTES - remaining)?;
                    let key = read_owned_entity_id(field, array, row).map_err(|_| invalid_page())?;
                    if !keys.follows_last(key, bound) {
                        return Err(invalid_page());
                    }
                    if !at_head {
                        self.point_purge_guard(relation, key, system, false).await?;
                    }
                    keys.push(key, count);
                }
            }
        }
        context.check_cancelled()?;
        keys.finish(more, charge)
    }
}

fn remaining_page_bytes(remaining: usize, bytes: usize, rows: usize) -> Result<Option<usize>> {
    match remaining.checked_sub(bytes) {
        None if rows == 0 => Err(Anomaly::new(
            AnomalyCategory::Busy,
            "rtdb.owned/identity-size",
            "An identity exceeds the page's retained byte bound",
        )),
        rest => Ok(rest),
    }
}

fn sealed_search_unit_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/sealed-search-unit",
        "The Original sealed search unit inventory is unavailable")
}

fn invalid_page() -> Anomaly {
    Anomaly::fault(
        "rtdb.owned/identity-order",
        "The owned identity query returned invalid keys",
    )
}

fn is_protected_row_carrier(relation: &OwnedRelation, id: &EntityId) -> bool {
    use rtdb_types::{ROOT_ROW_CARRIERS, RootRowCarrier};

    relation.name() == ROOT_ROW_CARRIERS
        && matches!(id, EntityId::Binary(id) if RootRowCarrier::parse(id).is_some())
}

#[cfg(test)]
#[test]
fn held_carrier_existence_has_only_the_row_payload_target() -> Result<()> {
    use rtdb_types::{ROOT_ROW_CARRIERS, RootRowCarrier};

    let payloads = OwnedRelation::new(ROOT_ROW_CARRIERS)?;
    let mut image = RootRowCarrier::table_prefix(&[17; 16]).to_vec();
    image.extend_from_slice(&[0, 255, 0, 255]);
    image.extend_from_slice(&[1; 64]);
    image.extend_from_slice(&[0, 1]);
    let row = EntityId::Binary(image);
    assert!(is_protected_row_carrier(&payloads, &row));
    let EntityId::Binary(image) = &row else { unreachable!() };
    let scope = RootRowCarrier::parse(image).unwrap();
    assert_eq!(scope.assertion_key()[1..], image[..]);
    assert_eq!(scope.retirement_key()[1..], image[..]);
    assert!(RootRowCarrier::is_assertion_key(&scope.assertion_key()));
    assert!(!RootRowCarrier::is_assertion_key(&scope.retirement_key()));
    let fence = scope.fence_name();
    assert_eq!(RootRowCarrier::decode_named_address(fence.strip_prefix("row/").unwrap()).as_deref(), Some(image.as_slice()));
    let mut invalid = image.clone();
    invalid[18] = 1;
    assert!(RootRowCarrier::parse(&invalid).is_none());
    assert!(!is_protected_row_carrier(&payloads, &EntityId::String("row/opaque".to_owned())));
    assert!(!is_protected_row_carrier(&payloads, &EntityId::Binary(vec![11; 18])));
    assert!(!is_protected_row_carrier(&OwnedRelation::new("fact_unit_column_payloads")?, &row));
    Ok(())
}

fn protected_page_unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/protected-equality",
        "The installed protected equality partition is unavailable",
    )
}

#[cfg(test)]
#[test]
fn held_identity_page_refuses_its_first_oversize_key_and_truncates_later_keys() {
    use arrow_array::StringArray;

    let values = StringArray::from(vec!["a".repeat(MAX_PAGE_BYTES / 2), "b".into()]);
    let field = arrow_schema::Field::new("_id", arrow_schema::DataType::Utf8, false);
    let large = estimate_read_value_bytes(&field, &values, 0).unwrap();
    let small = estimate_read_value_bytes(&field, &values, 1).unwrap();
    let error = remaining_page_bytes(MAX_PAGE_BYTES, large, 0).unwrap_err();
    assert_eq!(error.category, AnomalyCategory::Busy);
    assert_eq!(error.code, "rtdb.owned/identity-size");
    let remaining = remaining_page_bytes(MAX_PAGE_BYTES, small, 0).unwrap().unwrap();
    assert_eq!(remaining_page_bytes(remaining, large, 1).unwrap(), None);
    assert_eq!(remaining_page_bytes(remaining, remaining, 1).unwrap(), Some(0));
}
