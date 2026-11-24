use std::{num::NonZeroU64, sync::Arc};

use bytes::{Bytes, BytesMut};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_node::{OriginalAssertionFamilyReader, nearest::{HeldVectorOverlay, NearestPartition, NearestQuery}};
use tokio_util::sync::CancellationToken;
use rtdb_storage::IoRegion;
use rtdb_types::{AssertionId, EntityId, OwnedRelation, OwnedWorld, Result};
use uuid::Uuid;
use zeroize::Zeroize;

use super::{WorldFact, search_text};
use crate::keys::{
    AssertionAuthoring, AssertionEnvelope, AssertionStage, KeyService, WorldRowSlot,
    family_vault_address, world_row_scope,
};

const FIELD: &str = "sealed_world_projection";
const CARRIER: &[u8] = b"BJWP1";
const CODEC: u32 = u32::from_be_bytes(*b"BJWR");
const BINARY_BODY: &[u8] = b"BJPS\x01";
const COUNT_WIDTH: usize = size_of::<u32>();
const SOURCE_COLUMN_WIDTH: usize = 16;

pub(super) struct WorldProjectionCustody {
    keys: Arc<dyn KeyService>,
    deployment: Uuid,
    incarnation: u64,
    region: Arc<IoRegion>,
    original: OriginalAssertionFamilyReader,
    cancellation: CancellationToken,
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

struct OpenedProjection(BytesMut);

impl OpenedProjection {
    fn new(bytes: Bytes) -> Result<Self> {
        // The assertion vault returns an exclusively owned buffer.
        bytes.try_into_mut().map(Self).map_err(|_| OwnedWorld::unavailable())
    }

    fn decode(&self) -> Result<BinaryProjection<'_>> {
        let body = self.0.strip_prefix(BINARY_BODY).ok_or_else(OwnedWorld::unavailable)?;
        BinaryProjection::decode(body)
    }
}

impl Drop for OpenedProjection {
    fn drop(&mut self) {
        self.0.as_mut().zeroize();
    }
}

struct BinaryProjection<'a> {
    texts: Vec<(&'a str, &'a str)>,
    vectors: Vec<BinaryVectorPath<'a>>,
}

struct BinaryVectorPath<'a> {
    column: &'a str,
    components: &'a [[u8; 4]],
    filters: Vec<(&'a str, &'a [u8])>,
}

impl<'a> BinaryProjection<'a> {
    fn decode(body: &'a [u8]) -> Result<Self> {
        let mut input = ProjectionReader(body);
        let texts = input.list(2 * COUNT_WIDTH + SOURCE_COLUMN_WIDTH, |input| {
            let alias = input.text()?;
            input.take(SOURCE_COLUMN_WIDTH)?;
            Ok((alias, input.text()?))
        })?;
        let vectors = input.list(3 * COUNT_WIDTH, |input| {
            let column = input.text()?;
            let bytes = input.count()?.checked_mul(size_of::<f32>()).ok_or_else(OwnedWorld::unavailable)?;
            let (components, _) = input.take(bytes)?.as_chunks::<4>();
            if components.iter().any(|bytes| !f32::from_le_bytes(*bytes).is_finite()) {
                return Err(OwnedWorld::unavailable());
            }
            let filters = input.list(2 * COUNT_WIDTH, |input| Ok((input.text()?, input.bytes()?)))?;
            Ok(BinaryVectorPath { column, components, filters })
        })?;
        if !input.0.is_empty() {
            return Err(OwnedWorld::unavailable());
        }
        Ok(Self { texts, vectors })
    }
}

struct ProjectionReader<'a>(&'a [u8]);

impl<'a> ProjectionReader<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let (part, rest) = self.0.split_at_checked(size).ok_or_else(OwnedWorld::unavailable)?;
        self.0 = rest;
        Ok(part)
    }

    fn count(&mut self) -> Result<usize> {
        let (width, rest) = self.0.split_first_chunk::<COUNT_WIDTH>().ok_or_else(OwnedWorld::unavailable)?;
        self.0 = rest;
        Ok(u32::from_le_bytes(*width) as usize)
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let size = self.count()?;
        self.take(size)
    }

    fn text(&mut self) -> Result<&'a str> {
        std::str::from_utf8(self.bytes()?).map_err(|_| OwnedWorld::unavailable())
    }

    fn list<T>(&mut self, min_item_bytes: usize, mut read: impl FnMut(&mut Self) -> Result<T>) -> Result<Vec<T>> {
        let count = self.count()?;
        if count > self.0.len() / min_item_bytes {
            return Err(OwnedWorld::unavailable());
        }
        (0..count).map(|_| read(self)).collect()
    }
}

enum SearchRow<'a> {
    Clear(Option<&'a WorldFact>),
    Sealed(OpenedProjection, Arc<MemoryReservation>),
    Erased,
}

impl WorldProjectionCustody {
    pub(super) fn new(
        keys: Arc<dyn KeyService>, deployment: Uuid, incarnation: u64,
        original: OriginalAssertionFamilyReader, cancellation: CancellationToken,
    ) -> Self {
        Self { keys, deployment, incarnation, region: IoRegion::new(), original, cancellation }
    }

    async fn row<'a>(
        &self, relation: &OwnedRelation, id: &EntityId, fact: Option<&'a WorldFact>, memory: &Arc<MemoryBudget>, historical: bool,
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
        let world = fact.authoring_world.ok_or_else(invalid)?;
        let EntityId::String(key) = id else { return Err(invalid()); };
        if key.is_empty() || key.len() > 512 {
            return Err(invalid());
        }

        let stage = AssertionStage::new(carrier.deployment, NonZeroU64::new(carrier.incarnation).ok_or_else(invalid)?, carrier.stage)?;
        let family = family_vault_address(self.deployment, carrier.family);
        let slot = WorldRowSlot { world: Uuid::from_bytes(world.0), relation: relation.name(), key: key.as_bytes() };
        let families = self.keys.assertion_families(Arc::clone(&self.region)).ok_or_else(invalid)?
            .with_original_reader(&self.original, self.deployment, &self.cancellation);
        let vault = self.keys.assertion_keys().ok_or_else(invalid)?;
        let opened = async {
            families.require_derived_capability(family, stage, carrier.member, slot).await?;
            let authoring = AssertionAuthoring {
                assertion_id: carrier.member, scope_commitment: world_row_scope(slot, &carrier.disclosure), codec: CODEC,
            };
            let envelope = AssertionEnvelope::try_from(Bytes::copy_from_slice(carrier.envelope))?;
            let body = OpenedProjection::new(
                vault.open_assertion_for_resolution(stage, authoring, &self.region, envelope).await?,
            );
            families.require_derived_capability(family, stage, carrier.member, slot).await?;
            body
        };
        let charge = memory.reserve(bytes.len().saturating_mul(16))?;
        match opened.await {
            Ok(body) => {
                families.track_family_delivery(carrier.family)?;
                if rtdb_storage::sealing::PlaintextView::tracks_rank_inventory() {
                    let mut input = Vec::with_capacity(109 + relation.name().len() + key.len());
                    input.push(1);
                    input.extend_from_slice(carrier.family.as_bytes());
                    input.extend_from_slice(&carrier.member.to_bytes());
                    input.extend_from_slice(carrier.deployment.as_bytes());
                    input.extend_from_slice(&carrier.incarnation.to_be_bytes());
                    input.extend_from_slice(carrier.stage.as_bytes());
                    input.extend_from_slice(&world.0);
                    for field in [relation.name().as_bytes(), key.as_bytes()] {
                        input.extend_from_slice(&(field.len() as u16).to_be_bytes());
                        input.extend_from_slice(field);
                    }
                    rtdb_storage::sealing::PlaintextView::track_rank_contributor(&input)?;
                }
                Ok(SearchRow::Sealed(body, charge))
            }
            Err(error) if !historical && error.code == "rtdb/erased-under-view" => Ok(SearchRow::Erased),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn shadow_text(
        &self, relation: &OwnedRelation, column: &str, fact: Option<&WorldFact>, id: EntityId,
        partition: usize, corpus: &mut rtdb_node::HeldTextCorpus, memory: &Arc<MemoryBudget>, historical: bool,
    ) -> Result<()> {
        match self.row(relation, &id, fact, memory, historical).await? {
            SearchRow::Clear(fact) => corpus.shadow(partition, id, fact.map(|fact| search_text(fact, column)).transpose()?.flatten()),
            SearchRow::Erased => corpus.shadow(partition, id, None),
            SearchRow::Sealed(body, _charge) => {
                let projection = body.decode()?;
                let text = unique(projection.texts.iter().filter(|(alias, _)| *alias == column))?
                    .map(|(_, text)| *text);
                corpus.shadow(partition, id, text)
            },
        }
    }

    pub(super) async fn shadow_nearest(
        &self, selected: &NearestPartition, query: &NearestQuery, fact: Option<&WorldFact>,
        id: EntityId, partition: usize, shadows: &mut HeldVectorOverlay, memory: &Arc<MemoryBudget>, historical: bool,
    ) -> Result<()> {
        match self.row(&selected.relation, &id, fact, memory, historical).await? {
            SearchRow::Clear(fact) => shadows.shadow_row(partition, selected, id, fact.map(|fact| &fact.row), query),
            SearchRow::Erased => shadows.shadow_row(partition, selected, id, None, query),
            SearchRow::Sealed(body, _charge) => {
                let projection = body.decode()?;
                let paths = projection.vectors.iter().filter(|path| path.column == selected.vector.as_str());
                let Some(path) = unique(paths)? else {
                    return shadows.shadow_row(partition, selected, id, None, query);
                };
                let filters = selected.filters.iter().map(|column| {
                    Ok(unique(path.filters.iter().filter(|(name, _)| *name == column.as_str()))?
                        .map(|(_, key)| *key))
                }).collect::<Result<Vec<_>>>()?;
                let vector = path.components.iter().map(|bytes| f32::from_le_bytes(*bytes));
                shadows.shadow_opened_projection(partition, id, vector, &filters, query)
            },
        }
    }
}

fn unique<T>(mut matching: impl Iterator<Item = T>) -> Result<Option<T>> {
    let first = matching.next();
    if matching.next().is_some() {
        return Err(OwnedWorld::unavailable());
    }
    Ok(first)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BINARY_FIXTURE: &[u8] = b"BJPS\x01\
        \x01\0\0\0\x01\0\0\0s\
        \x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\
        \x04\0\0\0a\0\xc3\xa9\
        \x01\0\0\0\x01\0\0\0v\x02\0\0\0\0\0\xc0\x3f\0\0\0\x80\
        \x01\0\0\0\x01\0\0\0f\x03\0\0\0\0\xff\x05";

    #[test]
    fn world_projection_decodes_binary_plaintext() {
        let opened = OpenedProjection::new(Bytes::copy_from_slice(BINARY_FIXTURE)).unwrap();
        let binary = opened.decode().unwrap();
        assert_eq!(binary.texts, [("s", "a\0é")]);
        let [vector] = binary.vectors.as_slice() else { panic!("Expected one vector path"); };
        assert_eq!(vector.column, "v");
        assert_eq!(vector.components, &[1.5f32.to_le_bytes(), (-0.0f32).to_le_bytes()]);
        assert_eq!(vector.filters, [("f", &[0u8, 255, 5][..])]);
    }

    #[test]
    fn binary_world_projection_refuses_malformed_framing() {
        assert!(OpenedProjection(BytesMut::new()).decode().is_err());
        for end in 1..BINARY_FIXTURE.len() {
            let opened = OpenedProjection::new(Bytes::copy_from_slice(&BINARY_FIXTURE[..end])).unwrap();
            assert!(opened.decode().is_err());
        }
        let mut trailing = BINARY_FIXTURE.to_vec();
        trailing.push(0);
        assert!(OpenedProjection::new(trailing.into()).unwrap().decode().is_err());

        let mut impossible_count = BINARY_FIXTURE.to_vec();
        let start = BINARY_BODY.len();
        impossible_count[start..start + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(OpenedProjection::new(impossible_count.into()).unwrap().decode().is_err());
    }
}
