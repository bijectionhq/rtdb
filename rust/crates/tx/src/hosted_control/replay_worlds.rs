//! Bounded private world effects carried by one held native replay receipt.

use rtdb_types::{
    Anomaly, EntityId, Iid, Interval, OwnedRelation, OwnedWorld, OwnedWorldAncestor, Result, StructFields, Timestamp,
    TransactionKey, TxId, Value, WorldClockMode, WorldSchemas,
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

use super::WorldReadmission;

pub(super) const CHUNK_BYTES: usize = 256 * 1024;
// Match the owned Flight request envelope. Each control cell is bounded separately.
pub(super) const MAX_BYTES: usize = (64 << 20) - 1024;
const MAGIC: &[u8] = b"BJWP3";
const MAX_EFFECTS: usize = 4096;

#[derive(Default)]
pub(crate) struct ReplayWorlds {
    pub(crate) registrations: Vec<ReplayRegistration>,
    pub(crate) clocks: Vec<(Iid, Timestamp)>,
    pub(crate) shadows: Vec<ReplayShadow>,
}

pub(crate) struct ReplayRegistration {
    pub(crate) world: OwnedWorld,
    pub(crate) bind_parent: bool,
    pub(crate) readmission: Option<WorldReadmission>,
}

pub(crate) struct ReplayShadow {
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
    ancestor: Option<OwnedWorldAncestor>,
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
    fn into_registration(self) -> Result<ReplayRegistration> {
        let mode = match self.mode {
            ArchivedMode::Retained => WorldClockMode::Retained,
            ArchivedMode::Held(start) => WorldClockMode::Held {
                start: Timestamp(start),
            },
            ArchivedMode::Proposal => WorldClockMode::Proposal,
        };
        let schemas = serde_json::to_vec(&self.parent.schemas).map_err(|_| invalid())?;
        let world = OwnedWorld {
            ancestor: self.ancestor,
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
        Ok(ReplayRegistration {
            world,
            bind_parent: self.bind_parent,
            readmission: self.readmission,
        })
    }
}

impl ReplayWorlds {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let mut input = Input(bytes.strip_prefix(MAGIC).ok_or_else(invalid)?);
        let registrations = input.count()?;
        let mut worlds = Self::default();
        let mut ids = BTreeSet::new();
        for _ in 0..registrations {
            let world: ArchivedWorld =
                serde_json::from_slice(input.frame()?).map_err(|_| invalid())?;
            let registration = world.into_registration()?;
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
            let id = Iid(input.fixed()?);
            if id.0 == [0; 16] || !ids.insert(id) {
                return Err(invalid());
            }
            worlds.clocks.push((id, Timestamp(input.number()?)));
        }

        let shadows = input.count()?;
        if shadows + clocks + registrations > MAX_EFFECTS || shadows + clocks + registrations == 0 {
            return Err(invalid());
        }
        for _ in 0..shadows {
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
                world,
                relation,
                id: EntityId::String(id.into()),
                valid,
                fields,
            });
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
