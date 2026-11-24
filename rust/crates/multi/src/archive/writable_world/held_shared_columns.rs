//! The hosted shared-field server census. Only opaque carrier addresses cross
//! the Flight boundary. The installed field owner opens and compares values.

use super::*;

const MAX_CARRIERS: usize = 512;
const PAGE_ROWS: usize = 64;
const ROOT_PREFIX_BYTES: usize = 2 * (5 + 16 + 32 + 16 + 1 + 16 + 16 + 4);
const ROOT_ADDRESS_BYTES: usize = 8192;
const WORLD_SCOPE: &str = "world-column/";
const WORLD_ADDRESS_BYTES: usize = 64;

fn lower_hex(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

impl HeldCheckpointWorld {
    /// The frozen parent and the selected mutable overlay share one placed
    /// scope. A completed census retains the local relation frontier and the
    /// inherited purge frontier required by the next ordered mutation.
    pub(crate) async fn shared_column_census(
        &self,
        scope: PlacedAttemptScope,
        valid: Timestamp,
        root_prefix: &str,
        world_prefix: &str,
    ) -> Result<(WorldIdentityPage, OwnedWorldWitness)> {
        let unavailable = OwnedWorld::unavailable;
        let relation = OwnedRelation::new("fact_unit_column_payloads")?;
        let world_scope = world_prefix.strip_prefix(WORLD_SCOPE).and_then(|scope| scope.strip_suffix('/'));
        let expected_valid = match self.clock_mode() {
            WorldClockMode::Proposal => self.present().system_time,
            WorldClockMode::Held { .. } => self.application_publication().map_or(self.initial_clock(), |(clock, _)| clock),
            _ => return Err(unavailable()),
        };
        if valid != expected_valid
            || root_prefix.len() != ROOT_PREFIX_BYTES
            || !root_prefix.starts_with("4246435032")
            || !lower_hex(root_prefix.as_bytes())
            || world_scope.is_none_or(|scope| scope.len() != WORLD_ADDRESS_BYTES || !lower_hex(scope.as_bytes()))
        {
            return Err(unavailable());
        }
        self.world_point_schema(&relation)?;
        let witness = self.open.snapshot.world_relation_witnesses(self.open.record.id, [&relation])
            .await?.pop().filter(|witness| witness.matches_scope(scope)).ok_or_else(unavailable)?;
        self.require_overlay_coverage(&relation, &witness).await?;

        let mut census = WorldIdentityPage { keys: Vec::new(), more: false, reservations: Vec::new() };
        for (prefix, width, overlay) in [
            (root_prefix, ROOT_ADDRESS_BYTES, false),
            (world_prefix, WORLD_ADDRESS_BYTES, true),
        ] {
            // Root holds separate exact winner selectors in the world slot
            // namespace. Count only the overlay carriers in that namespace.
            let mut after = None;
            loop {
                let candidates = if overlay {
                    self.open.snapshot.world_identity_candidates_in_prefix(
                        self.open.record.id, &relation, valid, after, PAGE_ROWS + 1, prefix,
                    ).await?
                } else {
                    self.archive_identity_candidates(&relation, valid, after, PAGE_ROWS + 1, None, Some(prefix)).await?
                };
                let page = if overlay {
                    self.merge_overlay_identities(
                        &relation, valid, after, PAGE_ROWS, candidates, Some(prefix),
                    ).await?
                } else {
                    self.finish_overlay_identities(&relation, candidates, PAGE_ROWS)?
                };
                for key in page.keys {
                    let suffix = key.strip_prefix(prefix).ok_or_else(unavailable)?;
                    if suffix.len() != width || !lower_hex(suffix.as_bytes())
                        || census.keys.last().is_some_and(|previous| previous >= &key)
                    {
                        return Err(unavailable());
                    }
                    if census.keys.len() == MAX_CARRIERS {
                        return Err(rtdb_types::Anomaly::new(
                            rtdb_types::AnomalyCategory::Unavailable,
                            "rtdb.owned/shared-field-census-bound",
                            "The protected shared-field census exceeds its ordered carrier bound",
                        ));
                    }
                    if !overlay {
                        self.open.snapshot.check_point_purge_floor(
                            &relation, &EntityId::String(key.clone()), self.parent_basis().system_time,
                        ).await?;
                    }
                    census.keys.push(key);
                }
                census.reservations.extend(page.reservations);
                if !page.more {
                    break;
                }
                after = Some(census.keys.last().ok_or_else(unavailable)?.as_str());
            }
        }
        self.check_relation(&relation)?;
        Ok((census, witness))
    }
}
