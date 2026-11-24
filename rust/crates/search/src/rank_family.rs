use std::{num::NonZeroU64, sync::Arc};

use rtdb_columnar::{ArrowCell, EntityIdRef, OwnedBatch};
use rtdb_query::{QueryContext, owned_row_revision, owned_row_revision_for};
use rtdb_storage::sealing::{PlaintextView, RootRankSelection};
use rtdb_types::{Anomaly, AnomalyCategory, AssertionId, EntityId, Interval, OwnedRelation, Result, RootFamilyDerivativeId, RootLandingTarget, Timestamp};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(crate) async fn needs_rank_family(
    context: &Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
) -> Result<bool> {
    if !relation.name().ends_with("_search") {
        return Ok(false);
    }
    let (schema, _) = rtdb_query::read_owned_relation_schema(Arc::clone(context), owner, relation).await?
        .ok_or_else(unavailable)?;
    if relation.admits_world_ciphertext_points(&schema) {
        return if PlaintextView::has_root_rank_authority() { Ok(true) } else { Err(unavailable()) };
    }
    if schema.has_opaque_columns() { return Err(unavailable()); }
    Ok(false)
}

pub(crate) struct RootProjectionId<'a> {
    physical: &'a str,
    landing: Option<RootFamilyDerivativeId<'a>>,
}

impl<'a> RootProjectionId<'a> {
    pub(crate) fn parse(id: EntityIdRef<'a>) -> Result<Self> {
        let EntityIdRef::String(physical) = id else { return Err(unavailable()); };
        if physical.is_empty() || physical.len() > RootFamilyDerivativeId::MAX_KEY_BYTES {
            return Err(unavailable());
        }
        let landing = RootFamilyDerivativeId::parse(physical).map_err(|_| unavailable())?;
        Ok(Self { physical, landing })
    }

    pub(crate) fn canonical(&self) -> EntityIdRef<'a> {
        EntityIdRef::String(self.address())
    }

    fn address(&self) -> &'a str {
        self.landing.map_or(self.physical, |landing| landing.address)
    }

    pub(crate) async fn require(
        &self, context: &Arc<QueryContext>, owner: &str, relation: &OwnedRelation, valid: Timestamp,
        projection: &arrow_array::RecordBatch, row: usize,
    ) -> Result<()> {
        PlaintextView::charge_rank_carrier()?;
        let carrier_relation = OwnedRelation::new("fact_unit_column_payloads")?;
        let carrier_key = format!("row/{}", self.address());
        let journal_key = self.landing.map(|_| format!("row-position/{carrier_key}"));
        let carrier = EntityId::String(carrier_key);
        let page = point(context, owner, &carrier_relation, &carrier, valid).await?;
        let body = ArrowCell::named(page.batch(), "sealed_payload", 0)
            .and_then(ArrowCell::binary).ok_or_else(unavailable)?;
        let Some(landing) = self.landing else {
            return if body.starts_with(b"BJRP1") { Ok(()) } else { Err(unavailable()) };
        };

        let body = body.strip_prefix(b"BJRL1").ok_or_else(unavailable)?;
        let (family, body) = body.split_first_chunk::<16>().ok_or_else(unavailable)?;
        let (stage, body) = body.split_first_chunk::<40>().ok_or_else(unavailable)?;
        let (member, body) = body.split_first_chunk::<32>().ok_or_else(unavailable)?;
        let (target, body) = body.split_first_chunk::<{ RootLandingTarget::WIDTH }>().ok_or_else(unavailable)?;
        let (link_digest, envelope) = body.split_first_chunk::<32>().ok_or_else(unavailable)?;
        if *family != landing.family || *member != landing.member.to_bytes()
            || *link_digest == [0; 32] || envelope.is_empty()
        {
            return Err(unavailable());
        }

        let journal = OwnedRelation::new(relation.name().strip_suffix("_search").ok_or_else(unavailable)?)?;
        let journal_id = EntityId::String(journal_key.ok_or_else(unavailable)?);
        let position = point(context, owner, &journal, &journal_id, valid).await?;
        ArrowCell::named(position.batch(), "membership_until", 0).and_then(ArrowCell::binary)
            .filter(|bytes| bytes.len() == size_of::<i64>()).ok_or_else(unavailable)?;
        let publication = owned_row_revision(position.batch(), 0, &journal_id, &context.memory)?;
        let carrier_revision = owned_row_revision(page.batch(), 0, &carrier, &context.memory)?;
        let derivative_revision = owned_row_revision_for(projection, row, EntityIdRef::String(self.physical), &context.memory)?;
        if publication != carrier_revision || derivative_revision < publication || publication.0 <= 0 {
            return Err(unavailable());
        }
        let publication = publication.0 as u64;
        let ordinal = NonZeroU64::from(rtdb_query::owned_row_assertion_ordinal(position.batch(), 0)?.ok_or_else(unavailable)?);
        let target = RootLandingTarget::from_bytes(*target).map_err(|_| unavailable())?;
        PlaintextView::require_root_rank_derivative(RootRankSelection {
            family: Uuid::from_bytes(*family), assertion: AssertionId::from_bytes(*member), stage: *stage,
            target, link_digest: *link_digest, payload_digest: Sha256::digest(envelope).into(),
            publication, ordinal, relation: relation.name(), selector: self.physical,
        }).await?;

        let mut entry = Vec::with_capacity(109 + relation.name().len() + self.physical.len());
        entry.push(0);
        entry.extend_from_slice(family);
        entry.extend_from_slice(member);
        entry.extend_from_slice(stage);
        entry.extend_from_slice(&publication.to_be_bytes());
        entry.extend_from_slice(&ordinal.get().to_be_bytes());
        for field in [relation.name().as_bytes(), self.physical.as_bytes()] {
            entry.extend_from_slice(&(field.len() as u16).to_be_bytes());
            entry.extend_from_slice(field);
        }
        PlaintextView::track_rank_contributor(&entry)
    }
}

async fn point(
    context: &Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, valid: Timestamp,
) -> Result<OwnedBatch> {
    let mut cursor = rtdb_query::open_owned_assertion_point(
        Arc::clone(context), owner, relation, id, Some(Interval::at(valid)?), false,
    ).await?.ok_or_else(unavailable)?;
    let mut selected = None;
    while let Some(page) = cursor.next_batch().await? {
        if page.batch().num_rows() == 0 { continue; }
        if page.batch().num_rows() != 1 || selected.replace(page).is_some() { return Err(unavailable()); }
    }
    selected.ok_or_else(unavailable)
}

pub(crate) fn unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/rank-custody",
        "The held rank cannot authenticate its original Root carrier")
}
