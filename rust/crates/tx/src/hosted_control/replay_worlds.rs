//! Bounded private world effects carried by one held native replay receipt.

use rtdb_types::{
    Anomaly, EntityId, Iid, Interval, OwnedRelation, OwnedWorld, OwnedWorldAncestor, Result, StructFields, Timestamp,
    TransactionKey, TxId, Value, WorldClockMode, WorldSchemas,
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use super::WorldReadmission;

pub(super) const CHUNK_BYTES: usize = 256 * 1024;
// Match the owned Flight request envelope. Each control cell is bounded separately.
pub(super) const MAX_BYTES: usize = (64 << 20) - 1024;
const MAGIC: &[u8] = b"BJWP7";
const WITHOUT_TOPOLOGY_POSITIONS: &[u8] = b"BJWP6";
const MAX_EFFECTS: usize = 4096;

#[derive(Default)]
pub(crate) struct ReplayWorlds {
    pub(crate) registrations: Vec<ReplayRegistration>,
    pub(crate) clocks: Vec<ReplayClockPublication>,
    pub(crate) shadows: Vec<ReplayShadow>,
    pub(crate) omitted_shadows: BTreeMap<NonZeroU32, Iid>,
}

pub(crate) struct ReplayRegistration {
    pub(crate) position: Option<NonZeroU32>,
    pub(crate) world: OwnedWorld,
    pub(crate) bind_parent: bool,
    pub(crate) readmission: Option<WorldReadmission>,
}

pub(crate) struct ReplayClockPublication {
    pub(crate) position: Option<NonZeroU32>,
    pub(crate) world: Iid,
    pub(crate) reading: Timestamp,
}

pub(crate) struct ReplayShadow {
    pub(crate) position: Option<NonZeroU32>,
    pub(crate) world: Iid,
    pub(crate) relation: OwnedRelation,
    pub(crate) id: EntityId,
    pub(crate) valid: Interval,
    pub(crate) fields: Option<StructFields<Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchivedWorld {
    id: [u8; 16],
    bind_parent: bool,
    readmission: Option<WorldReadmission>,
    pin: [u8; 16],
    mode: ArchivedMode,
    parent: ArchivedParent,
    ancestors: Vec<OwnedWorldAncestor>,
    fork_lineage: Vec<rtdb_types::OwnedWorldForkLink>,
    until: i64,
    dropped: bool,
}

#[derive(Deserialize)]
enum ArchivedMode {
    Retained,
    Held(i64),
    Proposal,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchivedParent {
    capture: [u8; 16],
    suffix: Option<[u8; 16]>,
    manifest: String,
    basis_id: i64,
    basis_time: i64,
    publication: Option<i64>,
    schemas: BTreeMap<String, String>,
}

impl ArchivedWorld {
    fn into_registration(self, position: Option<NonZeroU32>) -> Result<ReplayRegistration> {
        let mode = match self.mode {
            ArchivedMode::Retained => WorldClockMode::Retained,
            ArchivedMode::Held(start) => WorldClockMode::Held {
                start: Timestamp(start),
            },
            ArchivedMode::Proposal => WorldClockMode::Proposal,
        };
        let schemas = serde_json::to_vec(&self.parent.schemas).map_err(|_| invalid())?;
        let world = OwnedWorld {
            ancestors: self.ancestors,
            fork_lineage: self.fork_lineage,
            id: Iid(self.id),
            pin: Iid(self.pin),
            capture: Iid(self.parent.capture),
            suffix: self.parent.suffix.map(Iid),
            manifest: self.parent.manifest,
            basis: TransactionKey {
                tx_id: TxId(self.parent.basis_id),
                system_time: Timestamp(self.parent.basis_time),
            },
            parent_publication: self.parent.publication.map(Timestamp),
            mode,
            until: Timestamp(self.until),
            schemas: WorldSchemas::from_bytes(&schemas)?,
            dropped: self.dropped,
        };
        OwnedWorld::from_record(&world.record())?;
        if world.basis.tx_id.0 < 0 || world.basis.system_time.0 <= 0 {
            return Err(invalid());
        }
        if position.is_some() && self.readmission.is_some() {
            return Err(invalid());
        }
        Ok(ReplayRegistration {
            position,
            world,
            bind_parent: self.bind_parent,
            readmission: self.readmission,
        })
    }
}

impl ReplayWorlds {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let (body, topology_positions) = if let Some(body) = bytes.strip_prefix(MAGIC) {
            (body, true)
        } else {
            (bytes.strip_prefix(WITHOUT_TOPOLOGY_POSITIONS).ok_or_else(invalid)?, false)
        };
        let mut input = Input(body);
        let registrations = input.count()?;
        let mut worlds = Self::default();
        let mut ids = BTreeSet::new();
        let mut positions = BTreeSet::new();
        for _ in 0..registrations {
            let position = if topology_positions { input.position(&mut positions)? } else { None };
            let world: ArchivedWorld =
                serde_json::from_slice(input.frame()?).map_err(|_| invalid())?;
            let registration = world.into_registration(position)?;
            if !ids.insert(registration.world.id) {
                return Err(invalid());
            }
            worlds.registrations.push(registration);
        }
        ids.clear();
        let clocks = input.count()?;
        if clocks + registrations > MAX_EFFECTS {
            return Err(invalid());
        }
        for _ in 0..clocks {
            let position = if topology_positions { input.position(&mut positions)? } else { None };
            let id = Iid(input.fixed()?);
            if id.0 == [0; 16] || !ids.insert(id) {
                return Err(invalid());
            }
            worlds.clocks.push(ReplayClockPublication { position, world: id, reading: Timestamp(input.number()?) });
        }

        let shadows = input.count()?;
        if shadows + clocks + registrations > MAX_EFFECTS {
            return Err(invalid());
        }
        for _ in 0..shadows {
            let position = input.position(&mut positions)?;
            let world = Iid(input.fixed()?);
            let relation = OwnedRelation::new(input.text()?)?;
            let id = input.text()?;
            let valid = Interval::new(Timestamp(input.number()?), Timestamp(input.number()?))?;
            if world.0 == [0; 16] || !relation.is_root_data() || id.is_empty() {
                return Err(invalid());
            }
            let size = u16::from_be_bytes(input.fixed()?);
            let fields = if size == u16::MAX {
                None
            } else {
                if size > 128 {
                    return Err(invalid());
                }
                let mut fields = StructFields::new();
                for _ in 0..size {
                    let name = input.text()?;
                    if name.is_empty() || name.starts_with('_') {
                        return Err(invalid());
                    }
                    let value = match input.fixed::<1>()?[0] {
                        0 => Value::Null,
                        1 => Value::Utf8(input.text()?.into()),
                        2 => Value::Binary(input.frame()?.into()),
                        3 => Value::I64(input.number()?),
                        _ => return Err(invalid()),
                    };
                    if fields.insert(name.into(), value).is_some() {
                        return Err(invalid());
                    }
                }
                Some(fields)
            };
            worlds.shadows.push(ReplayShadow {
                position,
                world,
                relation,
                id: EntityId::String(id.into()),
                valid,
                fields,
            });
        }
        let omitted = input.count()?;
        let total = registrations + clocks + shadows + omitted;
        if total == 0 || total > MAX_EFFECTS {
            return Err(invalid());
        }
        for _ in 0..omitted {
            let position = input.position(&mut positions)?.ok_or_else(invalid)?;
            let world = Iid(input.fixed()?);
            if world.0 == [0; 16] {
                return Err(invalid());
            }
            worlds.omitted_shadows.insert(position, world);
        }
        if !input.0.is_empty() {
            return Err(invalid());
        }
        Ok(worlds)
    }
}

struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (part, rest) = self.0.split_first_chunk::<N>().ok_or_else(invalid)?;
        self.0 = rest;
        Ok(*part)
    }

    fn position(&mut self, positions: &mut BTreeSet<NonZeroU32>) -> Result<Option<NonZeroU32>> {
        let position = NonZeroU32::new(u32::from_be_bytes(self.fixed()?));
        if position.is_some_and(|position| !positions.insert(position)) {
            return Err(invalid());
        }
        Ok(position)
    }

    fn number(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.fixed()?))
    }

    fn count(&mut self) -> Result<usize> {
        let count = usize::try_from(u32::from_be_bytes(self.fixed()?)).map_err(|_| invalid())?;
        if count > MAX_EFFECTS {
            return Err(invalid());
        }
        Ok(count)
    }

    fn frame(&mut self) -> Result<&'a [u8]> {
        let bytes = usize::try_from(u32::from_be_bytes(self.fixed()?)).map_err(|_| invalid())?;
        let (part, rest) = self.0.split_at_checked(bytes).ok_or_else(invalid)?;
        self.0 = rest;
        Ok(part)
    }

    fn text(&mut self) -> Result<&'a str> {
        std::str::from_utf8(self.frame()?).map_err(|_| invalid())
    }
}

pub(crate) fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.hosted/world-replay",
        "The held replay has invalid Original world effects",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_world_clock_and_omission_cannot_share_a_position() {
        let mut body = MAGIC.to_vec();
        body.extend_from_slice(&0u32.to_be_bytes()); // Registration count.
        body.extend_from_slice(&1u32.to_be_bytes()); // Clock count.
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(&[1; 16]);
        body.extend_from_slice(&12i64.to_be_bytes());
        body.extend_from_slice(&0u32.to_be_bytes()); // Overlay count.
        body.extend_from_slice(&1u32.to_be_bytes()); // Omission count.
        body.extend_from_slice(&4u32.to_be_bytes());
        body.extend_from_slice(&[2; 16]);

        let replay = ReplayWorlds::decode(&body).unwrap();
        let [clock] = replay.clocks.as_slice() else { panic!("Expected one clock"); };
        assert_eq!(clock.position, NonZeroU32::new(3));
        assert_eq!(clock.world, Iid([1; 16]));
        assert_eq!(clock.reading, Timestamp(12));
        let omission = body.len() - 20;
        body[omission..omission + 4].copy_from_slice(&3u32.to_be_bytes());
        assert!(ReplayWorlds::decode(&body).is_err());
    }

    #[test]
    fn replay_world_inventory_refuses_duplicate_or_unqualified_omissions() {
        let mut body = MAGIC.to_vec();
        for count in [0u32, 0, 0, 1] {
            body.extend_from_slice(&count.to_be_bytes());
        }
        let ordinal = NonZeroU32::new(3).unwrap();
        body.extend_from_slice(&ordinal.get().to_be_bytes());
        body.extend_from_slice(&[1; 16]);
        let replay = ReplayWorlds::decode(&body).unwrap();
        assert_eq!(replay.omitted_shadows.get(&ordinal), Some(&Iid([1; 16])));

        let omitted_count = MAGIC.len() + 3 * 4;
        body[omitted_count..omitted_count + 4].copy_from_slice(&2u32.to_be_bytes());
        body.extend_from_slice(&ordinal.get().to_be_bytes());
        body.extend_from_slice(&[2; 16]);
        assert!(ReplayWorlds::decode(&body).is_err());
        let second_ordinal = body.len() - 20;
        body[second_ordinal..second_ordinal + 4].copy_from_slice(&0u32.to_be_bytes());
        assert!(ReplayWorlds::decode(&body).is_err());
    }
}
