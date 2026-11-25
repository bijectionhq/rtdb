//! The hosted shared-field server census. Only opaque carrier addresses cross
//! the Flight boundary. The installed field owner opens and compares values.

use rtdb_columnar::EntityIdRef;
use rtdb_node::OwnedWorldWitness;
use rtdb_types::{EntityId, OwnedIdentityScan, OwnedRelation, OwnedSchema, OwnedWorld, PlacedAttemptScope, Result,
    SHARED_COLUMN_CARRIERS, SharedColumnCarrierPrefix, Timestamp, WorldClockMode};

use super::{HeldCheckpointWorld, WorldIdentityPage};

const MAX_CARRIERS: usize = 512;
const PAGE_ROWS: usize = 64;
const WORLD_SCOPE: &str = "world-column/";
const WORLD_ADDRESS_BYTES: usize = 64;

fn world_component(component: &str) -> bool {
    component.len() == WORLD_ADDRESS_BYTES
        && component.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl HeldCheckpointWorld {
    pub(super) fn shared_root_schema(&self, relation: &OwnedRelation) -> Result<&OwnedSchema> {
        self.open.record.schemas.relation(relation)
            .filter(|schema| relation.name() == SHARED_COLUMN_CARRIERS && schema.is_opaque_root_carrier())
            .ok_or_else(OwnedWorld::unavailable)
    }

    pub(crate) async fn shared_column_census(
        &self,
        scope: PlacedAttemptScope,
        valid: Timestamp,
        root_prefix: SharedColumnCarrierPrefix<'_>,
        world_prefix: &str,
    ) -> Result<(WorldIdentityPage, [OwnedWorldWitness; 2])> {
        let unavailable = OwnedWorld::unavailable;
        let root = OwnedRelation::new(SHARED_COLUMN_CARRIERS)?;
        let overlay = OwnedRelation::new("fact_unit_column_payloads")?;
        let world_scope = world_prefix.strip_prefix(WORLD_SCOPE).and_then(|scope| scope.strip_suffix('/'));
        let expected_valid = match self.clock_mode() {
            WorldClockMode::Proposal => self.present().system_time,
            WorldClockMode::Held { .. } => self.application_publication().map_or(self.initial_clock(), |(clock, _)| clock),
            _ => return Err(unavailable()),
        };
        if valid != expected_valid || !world_scope.is_some_and(world_component) {
            return Err(unavailable());
        }
        self.shared_root_schema(&root)?;
        if !overlay.admits_world_ciphertext_points(self.world_point_schema(&overlay)?) {
            return Err(unavailable());
        }
        let root_witness = self.open.snapshot.world_parent_witness(self.open.record.id, &root).await?;
        self.require_original_carrier_scope(&root_witness, Some(scope)).await?;
        let overlay_witness = self.open.snapshot.world_relation_witnesses(self.open.record.id, [&overlay])
            .await?.pop().filter(|witness| witness.matches_scope(scope)).ok_or_else(unavailable)?;
        let guard = self.open.snapshot.relation_release_guard([&root, &overlay])?;
        self.require_archive_coverage(vec![
            (root.clone(), root_witness.purge_frontier()),
            (overlay.clone(), overlay_witness.purge_frontier()),
        ], guard).await?;

        let mut census = WorldIdentityPage { keys: Vec::new(), more: false, reservations: Vec::new() };
        for prefix in [EntityIdRef::Binary(root_prefix.as_bytes()), EntityIdRef::String(world_prefix)] {
            let start = census.keys.len();
            loop {
                let previous = census.keys.get(start..).and_then(|leg| leg.last());
                let page = match prefix {
                    EntityIdRef::Binary(_) => {
                        let after = previous.map(OwnedIdentityScan::seek).transpose()?
                            .unwrap_or_else(|| OwnedIdentityScan::binary(None));
                        let candidates = self.archive_identity_candidates(
                            &root, valid, &after, PAGE_ROWS + 1, None, Some(root_prefix),
                        ).await?;
                        self.finish_overlay_identities(&root, candidates, PAGE_ROWS)?
                    }
                    EntityIdRef::String(prefix) => {
                        let after = match previous {
                            Some(EntityId::String(key)) => Some(key.as_str()),
                            Some(_) => return Err(unavailable()),
                            None => None,
                        };
                        let candidates = self.open.snapshot.world_identity_candidates_in_prefix(
                            self.open.record.id, &overlay, valid, after, PAGE_ROWS + 1, prefix,
                        ).await?;
                        self.merge_overlay_identities(
                            &overlay, valid, &OwnedIdentityScan::text(after), PAGE_ROWS, candidates, Some(prefix),
                        ).await?
                    }
                    _ => unreachable!(),
                };
                for key in page.keys {
                    let valid_key = match (prefix, EntityIdRef::from(&key)) {
                        (EntityIdRef::Binary(_), EntityIdRef::Binary(id)) => root_prefix.contains(id),
                        (EntityIdRef::String(prefix), EntityIdRef::String(id)) =>
                            id.strip_prefix(prefix).is_some_and(world_component),
                        _ => false,
                    };
                    if !valid_key || census.keys[start..].last().is_some_and(|previous| previous >= &key) {
                        return Err(unavailable());
                    }
                    if census.keys.len() == MAX_CARRIERS {
                        return Err(rtdb_types::Anomaly::new(
                            rtdb_types::AnomalyCategory::Unavailable,
                            "rtdb.owned/shared-field-census-bound",
                            "The protected shared-field census exceeds its ordered carrier bound",
                        ));
                    }
                    if matches!(prefix, EntityIdRef::Binary(_)) {
                        self.open.snapshot.check_point_purge_floor(
                            &root, &key, self.parent_basis().system_time,
                        ).await?;
                    }
                    census.keys.push(key);
                }
                census.reservations.extend(page.reservations);
                if !page.more {
                    break;
                }
            }
        }
        self.check_relation(&root)?;
        self.check_relation(&overlay)?;
        Ok((census, [root_witness, overlay_witness]))
    }
}
