use std::{num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_node::nearest::{HeldVectorOverlay, NearestPartition, NearestQuery};
use rtdb_storage::IoRegion;
use rtdb_types::{AssertionId, EntityId, OwnedRelation, OwnedWorld, Result};
use serde::Deserialize;
use uuid::Uuid;

use super::{WorldFact, search_text};
use crate::keys::{
    AssertionAuthoring, AssertionEnvelope, AssertionStage, KeyService, WorldRowSlot,
    family_vault_address, world_row_scope,
};

const FIELD: &str = "sealed_world_projection";
const CARRIER: &[u8] = b"BJWP1";
const CODEC: u32 = u32::from_be_bytes(*b"BJWR");

pub(super) struct WorldProjectionCustody {
    keys: Arc<dyn KeyService>,
    deployment: Uuid,
    incarnation: u64,
    region: Arc<IoRegion>,
}

struct ProjectionCarrier<'a> {
    family: Uuid,
    deployment: Uuid,
    incarnation: u64,
    stage: Uuid,
    member: AssertionId,
    disclosure: [u8; 32],
    envelope: &'a [u8],
}

impl<'a> ProjectionCarrier<'a> {
    fn decode(bytes: &'a [u8]) -> Result<Self> {
        let invalid = OwnedWorld::unavailable;
        let body = bytes.strip_prefix(CARRIER).ok_or_else(invalid)?;
        let (family, body) = body.split_first_chunk::<16>().ok_or_else(invalid)?;
        let (deployment, body) = body.split_first_chunk::<16>().ok_or_else(invalid)?;
        let (incarnation, body) = body.split_first_chunk::<8>().ok_or_else(invalid)?;
        let (stage, body) = body.split_first_chunk::<16>().ok_or_else(invalid)?;
        let (assertion, body) = body.split_first_chunk::<32>().ok_or_else(invalid)?;
        let (member, body) = body.split_first_chunk::<32>().ok_or_else(invalid)?;
        let (disclosure, envelope) = body.split_first_chunk::<32>().ok_or_else(invalid)?;
        let family = Uuid::from_bytes(*family);
        if family.is_nil() || assertion != member {
            return Err(invalid());
        }
        Ok(Self {
            family, deployment: Uuid::from_bytes(*deployment), incarnation: u64::from_be_bytes(*incarnation),
            stage: Uuid::from_bytes(*stage), member: AssertionId::from_bytes(*member), disclosure: *disclosure, envelope,
        })
    }
}

#[derive(Deserialize)]
struct TextPath(String, String, String);

#[derive(Deserialize)]
struct VectorPath(String, Vec<f32>, Vec<(String, String)>);

#[derive(Deserialize)]
struct Projection(Vec<TextPath>, Vec<VectorPath>);

enum SearchRow<'a> {
    Clear(Option<&'a WorldFact>),
    Sealed(Projection, Arc<MemoryReservation>),
    Erased,
}

impl WorldProjectionCustody {
    pub(super) fn new(keys: Arc<dyn KeyService>, deployment: Uuid, incarnation: u64) -> Self {
        Self { keys, deployment, incarnation, region: IoRegion::new() }
    }

    async fn row<'a>(
        &self, relation: &OwnedRelation, id: &EntityId, fact: Option<&'a WorldFact>, memory: &Arc<MemoryBudget>,
    ) -> Result<SearchRow<'a>> {
        let Some(fact) = fact else { return Ok(SearchRow::Clear(None)); };
        let Some(cell) = ArrowCell::named(&fact.row, FIELD, 0) else { return Ok(SearchRow::Clear(Some(fact))); };
        let invalid = OwnedWorld::unavailable;
        let bytes = rtdb_columnar::binary_cell(cell.field, cell.array, cell.row)?.ok_or_else(invalid)?;
        let carrier = ProjectionCarrier::decode(bytes)?;
        if bytes.len() > crate::process::MAXIMUM_LOG_MESSAGE_BYTES
            || carrier.deployment != self.deployment || carrier.incarnation != self.incarnation
        {
            return Err(invalid());
        }
        let world = fact.assertion.and_then(|assertion| assertion.line).ok_or_else(invalid)?;
        let EntityId::String(key) = id else { return Err(invalid()); };
        if key.is_empty() || key.len() > 512 {
            return Err(invalid());
        }

        let stage = AssertionStage::new(carrier.deployment, NonZeroU64::new(carrier.incarnation).ok_or_else(invalid)?, carrier.stage)?;
        let family = family_vault_address(self.deployment, carrier.family);
        let slot = WorldRowSlot { world: Uuid::from_bytes(world.0), relation: relation.name(), key: key.as_bytes() };
        let families = self.keys.assertion_families(Arc::clone(&self.region)).ok_or_else(invalid)?;
        let vault = self.keys.assertion_keys().ok_or_else(invalid)?;
        let opened = async {
            families.require_derived_capability(family, stage, carrier.member, slot).await?;
            let authoring = AssertionAuthoring {
                assertion_id: carrier.member, scope_commitment: world_row_scope(slot, &carrier.disclosure), codec: CODEC,
            };
            let envelope = AssertionEnvelope::try_from(Bytes::copy_from_slice(carrier.envelope))?;
            vault.open_assertion_for_resolution(stage, authoring, &self.region, envelope).await
        };
        let charge = memory.reserve(bytes.len().saturating_mul(16))?;
        match opened.await {
            Ok(bytes) => {
                let projection = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
                Ok(SearchRow::Sealed(projection, charge))
            }
            Err(error) if error.code == "rtdb/erased-under-view" => Ok(SearchRow::Erased),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn shadow_text(
        &self, relation: &OwnedRelation, column: &str, fact: Option<&WorldFact>, id: EntityId,
        partition: usize, corpus: &mut rtdb_node::HeldTextCorpus, memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        match self.row(relation, &id, fact, memory).await? {
            SearchRow::Clear(fact) => corpus.shadow(partition, id, fact.map(|fact| search_text(fact, column)).transpose()?.flatten()),
            SearchRow::Erased => corpus.shadow(partition, id, None),
            SearchRow::Sealed(Projection(paths, _), _charge) => {
                let mut selected = paths.iter().filter(|path| path.0 == column);
                let text = selected.next().map(|TextPath(_, field, value)| {
                    if field.is_empty() { return Err(OwnedWorld::unavailable()); }
                    canonical_text(value)
                }).transpose()?;
                if selected.next().is_some() { return Err(OwnedWorld::unavailable()); }
                corpus.shadow(partition, id, text.as_deref())
            }
        }
    }

    pub(super) async fn shadow_nearest(
        &self, selected: &NearestPartition, query: &NearestQuery, fact: Option<&WorldFact>,
        id: EntityId, partition: usize, shadows: &mut HeldVectorOverlay, memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        match self.row(&selected.relation, &id, fact, memory).await? {
            SearchRow::Clear(fact) => shadows.shadow_row(partition, selected, id, fact.map(|fact| &fact.row), query),
            SearchRow::Erased => shadows.shadow_row(partition, selected, id, None, query),
            SearchRow::Sealed(Projection(_, paths), _charge) => {
                let invalid = OwnedWorld::unavailable;
                let mut paths = paths.iter().filter(|path| path.0 == selected.vector);
                let Some(VectorPath(_, vector, filter_paths)) = paths.next() else {
                    return shadows.shadow_row(partition, selected, id, None, query);
                };
                if paths.next().is_some() { return Err(invalid()); }
                let filters = selected.filters.iter().map(|column| {
                    let mut keys = filter_paths.iter().filter(|(name, _)| name == column);
                    let selected = keys.next().map(|(_, value)| hex_bytes(value)).transpose()?;
                    if keys.next().is_some() { return Err(invalid()); }
                    Ok(selected)
                }).collect::<Result<Vec<_>>>()?;
                shadows.shadow_opened_projection(partition, id, vector, &filters, query)
            }
        }
    }
}

fn hex_bytes(value: &str) -> Result<Vec<u8>> {
    let invalid = OwnedWorld::unavailable;
    let (pairs, rest) = value.as_bytes().as_chunks::<2>();
    if !rest.is_empty() { return Err(invalid()); }
    let digit = |byte| match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(invalid()),
    };
    pairs.iter().map(|&[high, low]| Ok(digit(high)? << 4 | digit(low)?)).collect()
}

fn canonical_text(encoded: &str) -> Result<String> {
    let invalid = OwnedWorld::unavailable;
    let bytes = hex_bytes(encoded)?;
    let mut body = bytes.strip_prefix(&[9]).and_then(|bytes| bytes.strip_suffix(&[0, 1])).ok_or_else(invalid)?;
    let mut text = Vec::with_capacity(body.len());
    while let Some((&byte, rest)) = body.split_first() {
        text.push(byte);
        body = if byte == 0 { rest.strip_prefix(&[0xff]).ok_or_else(invalid)? } else { rest };
    }
    String::from_utf8(text).map_err(|_| invalid())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn world_projection_text_decodes_canonical_utf8_and_escaped_nulls() {
        assert_eq!(canonical_text("096100ffc3a90001").unwrap(), "a\0é");
        assert_eq!(canonical_text("090001").unwrap(), "");
        assert!(canonical_text("09610001620001").is_err());
    }
}
