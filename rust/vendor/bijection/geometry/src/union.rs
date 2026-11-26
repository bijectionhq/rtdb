//! Exact set union without introducing intersection coordinates. Collections
//! already denote unions of their members. Canonical leaf order makes reduction
//! independent of row order, partitioning and nested collection structure.
use std::collections::BTreeMap;

use crate::{
    Coords,
    Geometry,
    GeometryDeclaration,
    GeometryRefusal,
    GeometrySpace,
    GeometryWork,
    Gv,
    Kind,
    Size,
    TierBound,
    LARGE,
};

fn insert(
    value: Geometry,
    space: &GeometrySpace,
    members: &mut BTreeMap<Vec<u8>, Geometry>,
    work: &mut GeometryWork,
) -> Result<(), GeometryRefusal> {
    work.charge(1)?;
    if value.space != *space {
        return Err(crate::unsupported_space(
            "Union members require the same coordinate space",
        ));
    }
    if let Coords::Collection(children) = value.coords {
        for child in children {
            insert(child, space, members, work)?;
        }
        return Ok(());
    }
    if value.is_empty() {
        return Ok(());
    }
    let value = crate::standardize::standardize(value, work)?;
    let bytes = value.wkb_len();
    work.charge(bytes.saturating_mul(members.len().max(1).ilog2() as usize + 2))?;
    work.allocate(bytes)?;
    let key = value.wkb();
    if !members.contains_key(&key) {
        if members.len() >= LARGE.ring_coordinates.saturating_sub(1) {
            return Err(LARGE.over("union component bound".into(), TierBound::Components));
        }
        work.allocate_retained(value.retained_bytes().saturating_add(96))?;
        members.insert(key, value);
    }
    Ok(())
}

/// Private reduction state, independent of row, checkpoint and storage owners.
/// Multiplicity belongs to contributions, not the represented geometric set:
/// retracting one duplicate must not remove another row's contribution.
pub struct State {
    space: GeometrySpace,
    members: BTreeMap<Vec<u8>, (Geometry, u64)>,
}

impl State {
    pub fn new(space: GeometrySpace) -> Result<Self, GeometryRefusal> {
        space.validate_kernel()?;
        Ok(Self {
            space,
            members: BTreeMap::new(),
        })
    }

    pub fn space(&self) -> &GeometrySpace {
        &self.space
    }

    /// Normalize a whole contribution before touching retained state. A refusal
    /// spends work but leaves the previous state intact.
    pub fn add(&mut self, value: Geometry, work: &mut GeometryWork) -> Result<(), GeometryRefusal> {
        let mut leaves = BTreeMap::new();
        insert(value, &self.space, &mut leaves, work)?;
        self.add_members(leaves.into_iter().map(|(key, value)| (key, value, 1)), work)
    }

    pub fn retract(
        &mut self,
        value: Geometry,
        work: &mut GeometryWork,
    ) -> Result<(), GeometryRefusal> {
        work.scoped(|work| {
            let mut leaves = BTreeMap::new();
            insert(value, &self.space, &mut leaves, work)?;
            for key in leaves.keys() {
                work.charge(
                    key.len()
                        .saturating_mul(self.members.len().max(1).ilog2() as usize + 2),
                )?;
                if !self.members.contains_key(key) {
                    return Err(crate::invalid(
                        "Union retraction has no retained contribution",
                    ));
                }
            }
            for key in leaves.into_keys() {
                let (_, count) = self.members.get_mut(&key).expect("checked contribution");
                *count -= 1;
                if *count == 0 {
                    self.members.remove(&key);
                }
            }
            Ok(())
        })
    }

    pub fn merge(&mut self, other: Self, work: &mut GeometryWork) -> Result<(), GeometryRefusal> {
        if self.space != other.space {
            return Err(crate::unsupported_space(
                "Union states require the same coordinate space",
            ));
        }
        self.add_members(
            other
                .members
                .into_iter()
                .map(|(key, (value, count))| (key, value, count)),
            work,
        )
    }

    fn add_members(
        &mut self,
        members: impl ExactSizeIterator<Item = (Vec<u8>, Geometry, u64)>,
        work: &mut GeometryWork,
    ) -> Result<(), GeometryRefusal> {
        // Reserve before collecting, including transient duplicate keys. The
        // input iterator is an already bounded canonical map, never host rows.
        work.allocate(
            members
                .len()
                .saturating_mul(std::mem::size_of::<(Vec<u8>, Geometry, u64)>()),
        )?;
        let members = members.collect::<Vec<_>>();
        let mut added = 0usize;
        for (key, value, count) in &members {
            work.charge(
                key.len()
                    .saturating_mul(self.members.len().max(1).ilog2() as usize + 2),
            )?;
            if let Some((_, retained)) = self.members.get(key) {
                if retained.checked_add(*count).is_none() {
                    return Err(crate::invalid("Union contribution count exceeds u64"));
                }
            } else {
                added += 1;
                work.allocate_retained(
                    key.len()
                        .saturating_add(value.retained_bytes())
                        .saturating_add(96),
                )?;
            }
        }
        if added
            > LARGE
                .ring_coordinates
                .saturating_sub(1)
                .saturating_sub(self.members.len())
        {
            return Err(LARGE.over("union component bound".into(), TierBound::Components));
        }
        for (key, value, count) in members {
            self.members
                .entry(key)
                .and_modify(|(_, retained)| *retained += count)
                .or_insert((value, count));
        }
        Ok(())
    }

    pub fn finish(self, work: &mut GeometryWork) -> Result<Geometry, GeometryRefusal> {
        finish(
            self.members.into_values().map(|(value, _)| value),
            &self.space,
            work,
        )
    }
}

/// Missing/null handling belongs to the aggregate contract. This kernel takes
/// admitted values in one space, removes empty/duplicate leaves and returns an
/// admitted GeometryCollection. It does not snap, repair or dissolve
/// boundaries.
pub fn reduce(
    values: impl IntoIterator<Item = Geometry>,
    space: &GeometrySpace,
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    work.scoped(|work| {
        let mut state = State::new(space.clone())?;
        for value in values {
            state.add(value, work)?;
        }
        state.finish(work)
    })
}

fn finish(
    members: impl ExactSizeIterator<Item = Geometry>,
    space: &GeometrySpace,
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    work.scoped(|work| {
        work.allocate(
            members
                .len()
                .saturating_mul(std::mem::size_of::<Geometry>()),
        )?;
        let value = Geometry {
            space: space.clone(),
            kind: Kind::GeometryCollection,
            coords: Coords::Collection(members.collect()),
            heights: (!space.is_ground()).then(Vec::new),
        };
        // Re-admission enforces the complete output's coordinate, ring, byte
        // and component bounds, including nested multipart members.
        work.allocate(value.retained_bytes().saturating_mul(16))?;
        crate::admit_value_with_work(
            &Gv::from(&value),
            &GeometryDeclaration {
                space: space.clone(),
                size: Size::Large,
            },
            work,
        )
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn union_state_retracts_duplicate_contributions_and_refuses_atomically() -> anyhow::Result<()> {
        let space = GeometrySpace::wgs84_xy();
        let point = |x| Geometry::new(Kind::Point, Coords::Point([x, 0.]), space.clone());
        let mut state = State::new(space.clone())?;
        let a = point(1.);
        let b = point(2.);
        let collection = Geometry::new(
            Kind::GeometryCollection,
            Coords::Collection(vec![a.clone(), a.clone(), b.clone()]),
            space.clone(),
        );
        let mut work = GeometryWork::default();
        state.add(collection.clone(), &mut work)?;
        let mut partition = State::new(space.clone())?;
        partition.add(a.clone(), &mut work)?;
        state.merge(partition, &mut work)?;
        state.retract(collection, &mut work)?;
        assert!(state.retract(b.clone(), &mut work).is_err());
        assert!(state
            .add(b, &mut GeometryWork::with_limits(0, 1_000_000))
            .is_err());
        let mut overflowing = State::new(space.clone())?;
        overflowing.members.insert(a.wkb(), (a.clone(), u64::MAX));
        assert!(state.merge(overflowing, &mut work).is_err());
        // Neither the absent retraction, refused addition nor overflowing
        // partition merge removed or incremented the surviving contribution.
        state.retract(a, &mut work)?;
        assert!(state.finish(&mut work)?.is_empty());
        Ok(())
    }

    #[test]
    fn union_is_exact_associative_and_order_independent() -> anyhow::Result<()> {
        let space = GeometrySpace::wgs84_xy();
        let a = crate::admit_value(
            &Gv::from(&json!({"type":"LineString","coordinates":[[0.,0.],[3.,2.]]})),
            &GeometryDeclaration {
                space: space.clone(),
                size: Size::Large,
            },
        )?;
        let b = crate::admit_value(
            &Gv::from(&json!({"type":"LineString","coordinates":[[0.,1.],[3.,0.]]})),
            &GeometryDeclaration {
                space: space.clone(),
                size: Size::Large,
            },
        )?;
        let union = |values| reduce(values, &space, &mut GeometryWork::default());
        let expected = union(vec![a.clone(), b.clone()])?;
        assert_eq!(expected, union(vec![b.clone(), a.clone(), b.clone()])?);
        assert_eq!(
            expected,
            union(vec![
                union(vec![a.clone()])?,
                union(vec![b.clone(), a.clone()])?
            ])?
        );
        assert_eq!(expected.position_count(), 4); // no rounded intersection is introduced
        assert!(union(vec![])?.is_empty());
        assert!(reduce(
            vec![a],
            &space,
            &mut GeometryWork::with_limits(0, 1_000_000)
        )
        .is_err());
        Ok(())
    }
}
