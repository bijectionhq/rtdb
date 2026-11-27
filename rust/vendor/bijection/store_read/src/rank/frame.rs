//! What a ranked read reads: frames (a label partition's hold, or a
//! world's selected levels over it) and the views ranking resolves rows
//! through.
use std::collections::BTreeSet;

use anyhow::ensure;

use crate::{
    event::{
        self,
        Cut,
        Event,
        Image,
    },
    keys::{
        self,
        RowId,
    },
    layout::Place,
    source::{
        held_events,
        resolve_levels,
        Level,
        RangeSource,
    },
    store::{
        KeyRange,
        Version,
    },
};

/// One label partition's hold: where its keys live, the store version and
/// the cut it reads, and (for a world read) the world's selected levels.
/// The partition is a key prefix; a frame never names a label.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Frame {
    /// The partition's Root place.
    pub place: Place,
    pub version: Version,
    /// The hold's system time.
    pub present_micros: i64,
    pub cut: Cut,
    /// A scoped hold reads only these relations.
    pub scope: Option<BTreeSet<String>>,
    /// Family-held: the store holds this partition's values sealed, so only
    /// Bijection, opening them through custody, can rank it. Decided by the
    /// partition's class, never at runtime.
    pub sealed: bool,
    pub world: Option<WorldFrame>,
}

/// A world read over a frame: the world, its selected levels (nearest
/// first, the Root parent last) and what its hold decided at its version.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorldFrame {
    pub world: [u8; 16],
    /// Whether the hold reads the world's own overlay (`levels[0]` may be it).
    pub own: bool,
    pub levels: Vec<Level>,
    /// The system time the world hold reads at (its accepted publication,
    /// else the Root hold's present).
    pub present_micros: i64,
    /// Whether every non-Root level's counter deltas were taken over the
    /// levels below it (each level's world record at the hold names the
    /// levels under it). When not, counted text reads recount.
    pub stacked: bool,
}

impl Frame {
    /// Refuse a relation outside a scoped hold.
    pub fn admit(&self, relation: &str) -> anyhow::Result<()> {
        if let Some(scope) = &self.scope {
            ensure!(scope.contains(relation), "rtdb.owned/scoped-root-denied: {relation} is outside the scoped hold");
        }
        Ok(())
    }

    /// The system time a page of this frame reports.
    pub fn page_present(&self) -> i64 {
        self.world.as_ref().map_or(self.present_micros, |world| world.present_micros)
    }

    /// The place of one world of the partition.
    pub fn world_place(&self, world: [u8; 16]) -> Place {
        Place { world, ..self.place }
    }

    /// Every place whose path entries the frame reads, nearest first.
    pub fn places(&self) -> Vec<Place> {
        match &self.world {
            None => vec![self.place],
            Some(world) => world.levels.iter().map(|level| self.world_place(level.world)).collect(),
        }
    }

    /// The image a row presents at the frame's cut and `valid`, through its
    /// purge floor. A world resolves the row through its levels and records
    /// the floors it consulted.
    pub async fn image<S: RangeSource + ?Sized>(&self, source: &S, relation: &str, id: &RowId, valid: i64, reads: &mut Vec<KeyRange>) -> anyhow::Result<Option<Image>> {
        Ok(self.winner(source, relation, id, valid, reads).await?.and_then(|event| event.image))
    }

    /// The winning present event of a row at the frame's cut and `valid`.
    pub async fn winner<S: RangeSource + ?Sized>(&self, source: &S, relation: &str, id: &RowId, valid: i64, reads: &mut Vec<KeyRange>) -> anyhow::Result<Option<Event>> {
        match &self.world {
            None => {
                let events = held_events(source, &keys::row(self.place, relation, id), &self.cut, self.version).await?;
                Ok(event::point(&events, &self.cut, valid).present().cloned())
            },
            Some(world) => Ok(resolve_levels(source, self.place, &world.levels, relation, id, valid, self.version, true, reads).await?.1),
        }
    }

    /// The partition's read set before any row floor: the world record and
    /// the world's own path tablet; a Root read names its own ranges.
    pub fn world_reads(&self, world: &WorldFrame, relation: &str, path: &str) -> Vec<KeyRange> {
        let mut reads = vec![KeyRange::point(&keys::world_record(self.place, world.world))];
        if world.own && world.levels.first().is_some_and(|level| level.world == world.world) {
            reads.push(keys::path_all(self.world_place(world.world), relation, path));
        }
        reads
    }

    /// Whether the frame reads the Root world of its partition.
    pub fn is_root(&self) -> bool {
        self.world.is_none()
    }
}
