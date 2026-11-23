//! Cube-face cells on the auxiliary sphere: the spatial index's cells.
//!
//! The auxiliary sphere is the one `arcs` works on, where every WGS84 edge is
//! a great circle. A cell is a quadrilateral of that sphere bounded by four
//! great circles, so on the ellipsoid its sides are great elliptic arcs, the
//! profile's own edges. Cells follow S2's layout: six cube faces, the
//! quadratic projection from face coordinates (u, v) to (s, t), a quadtree of
//! 27 levels and a Hilbert curve that orders each face. A cell id holds the
//! face (3 bits), two bits of Hilbert position per level and a sentinel bit
//! marking the level, so a cell's descendants are one contiguous id range.
//!
//! A direction's leaf is computed in binary64 with IEEE operations only, so
//! every host assigns the same leaf. Cell tests use the cell padded by `PAD`
//! in s and t, far wider than a direction's rounding there, so a cover that
//! keeps every cell a target meets also keeps every cell a matching
//! direction was keyed into.
use crate::{
    arcs::{
        self,
        Vector,
    },
    predicates::segment_distance,
    GeometryRefusal,
    GeometryWork,
    Pos,
};

/// The cell recipe's identity: a change to how directions map to cells or
/// how cells are tested changes it, and every index keyed by it rebuilds.
pub const RECIPE: &str =
    "aux-sphere cube faces, quadratic st, Hilbert, 27 levels, pad 2^-40, selective cover v2";
/// Quadtree levels below a face.
pub const MAX_LEVEL: u32 = 27;
/// Bits of Hilbert position and the sentinel.
const POS_BITS: u32 = 2 * MAX_LEVEL + 1;
/// Bits of a cell id: face, Hilbert position and sentinel.
pub const ID_BITS: u32 = 3 + POS_BITS;

const SWAP: usize = 1;
const INVERT: usize = 2;
/// S2's Hilbert tables: the position of a child (i, j) quadrant under each
/// orientation, the quadrant of each position, and how a position turns the
/// orientation of its children.
const IJ_TO_POS: [[u64; 4]; 4] = [[0, 1, 3, 2], [0, 3, 1, 2], [2, 3, 1, 0], [2, 1, 3, 0]];
const POS_TO_IJ: [[u64; 4]; 4] = [[0, 1, 3, 2], [0, 2, 3, 1], [3, 2, 0, 1], [3, 1, 0, 2]];
const POS_TO_ORIENTATION: [usize; 4] = [SWAP, 0, 0, SWAP | INVERT];

/// Padding of every cell test in face coordinates s and t: 2^-40, against a
/// direction's rounding in s of about 1e-15 and a leaf's width of 2^-27.
const PAD: f64 = 1.0 / (1u64 << 40) as f64;
/// Within this many radians of a padded cell's edge a direction counts as in
/// the cell for distance bounds: the padding is narrower than this.
const NEAR: f64 = 1e-9;

/// A cell: face, Hilbert position and sentinel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CellId(u64);

impl CellId {
    /// A cell id read back from storage, or None when it names no cell.
    pub fn from_raw(raw: u64) -> Option<Self> {
        let lsb = raw.isolate_lowest_one();
        (raw != 0
            && raw >> POS_BITS < 6
            && raw >> ID_BITS == 0
            && lsb.trailing_zeros() % 2 == 0
            && lsb.trailing_zeros() <= 2 * MAX_LEVEL)
            .then_some(Self(raw))
    }

    pub fn raw(self) -> u64 {
        self.0
    }

    /// The six face cells, in id order.
    pub fn faces() -> [Self; 6] {
        [0, 1, 2, 3, 4, 5].map(|face: u64| Self((face << POS_BITS) | (1 << (POS_BITS - 1))))
    }

    pub fn face(self) -> u8 {
        (self.0 >> POS_BITS) as u8
    }

    fn lsb(self) -> u64 {
        self.0 & self.0.wrapping_neg()
    }

    fn lsb_for(level: u32) -> u64 {
        1 << (2 * (MAX_LEVEL - level))
    }

    pub fn level(self) -> u32 {
        MAX_LEVEL - self.lsb().trailing_zeros() / 2
    }

    /// The first and last leaf ids under the cell, the cell included.
    pub fn range(self) -> (u64, u64) {
        (self.0 - (self.lsb() - 1), self.0 + (self.lsb() - 1))
    }

    pub fn contains(self, other: Self) -> bool {
        let (first, last) = self.range();
        first <= other.0 && other.0 <= last
    }

    /// The ancestor at `level`, which must not be finer than the cell.
    pub fn parent(self, level: u32) -> Self {
        let lsb = Self::lsb_for(level);
        Self((self.0 & lsb.wrapping_neg()) | lsb)
    }

    /// The four children in Hilbert order; a leaf has none.
    pub fn children(self) -> Option<[Self; 4]> {
        (self.level() < MAX_LEVEL).then(|| {
            let step = self.lsb() >> 2;
            let first = self.0 - self.lsb() + step;
            [0, 1, 2, 3].map(|k| Self(first + 2 * k * step))
        })
    }

    /// The leaf at (i, j) on a face, by the Hilbert curve.
    pub fn from_face_ij(face: u8, i: u32, j: u32) -> Self {
        let mut orientation = usize::from(face) & SWAP;
        let mut position = 0u64;
        for k in (0..MAX_LEVEL).rev() {
            let quadrant = (((i >> k) & 1) << 1 | ((j >> k) & 1)) as usize;
            let p = IJ_TO_POS[orientation][quadrant];
            position = (position << 2) | p;
            orientation ^= POS_TO_ORIENTATION[p as usize];
        }
        Self((u64::from(face) << POS_BITS) | (position << 1) | 1)
    }

    /// The face, level and (i, j) of the cell at its own level.
    pub fn face_ij(self) -> (u8, u32, u32, u32) {
        let (face, level) = (self.face(), self.level());
        let position = (self.0 >> (POS_BITS - 2 * level)) & ((1u64 << (2 * level)) - 1);
        let mut orientation = usize::from(face) & SWAP;
        let (mut i, mut j) = (0u32, 0u32);
        for k in (0..level).rev() {
            let p = ((position >> (2 * k)) & 3) as usize;
            let quadrant = POS_TO_IJ[orientation][p];
            i = (i << 1) | (quadrant >> 1) as u32;
            j = (j << 1) | (quadrant & 1) as u32;
            orientation ^= POS_TO_ORIENTATION[p];
        }
        (face, level, i, j)
    }
}

/// The face of a direction: its largest component's axis, 3 more when
/// negative (S2's numbering).
fn face_of(v: Vector) -> u8 {
    let a = v.map(f64::abs);
    let axis = if a[0] > a[1] {
        if a[0] > a[2] { 0 } else { 2 }
    } else if a[1] > a[2] {
        1
    } else {
        2
    };
    if v[axis] < 0.0 {
        axis as u8 + 3
    } else {
        axis as u8
    }
}

/// A direction's (u, v) on a face; S2's `ValidFaceXYZtoUV`.
fn face_uv(face: u8, p: Vector) -> (f64, f64) {
    match face {
        0 => (p[1] / p[0], p[2] / p[0]),
        1 => (-p[0] / p[1], p[2] / p[1]),
        2 => (-p[0] / p[2], -p[1] / p[2]),
        3 => (p[2] / p[0], p[1] / p[0]),
        4 => (p[2] / p[1], -p[0] / p[1]),
        _ => (-p[1] / p[2], -p[0] / p[2]),
    }
}

/// The direction of (u, v) on a face; S2's `FaceUVtoXYZ`. Each face's (u, v)
/// frame turns counterclockwise about its outward normal.
fn face_xyz(face: u8, u: f64, v: f64) -> Vector {
    match face {
        0 => [1.0, u, v],
        1 => [-u, 1.0, v],
        2 => [-u, -v, 1.0],
        3 => [-1.0, -v, -u],
        4 => [v, -1.0, -u],
        _ => [v, u, -1.0],
    }
}

/// S2's quadratic projection: cells near face corners are at most about
/// 2.08 times the area of those at face centres.
fn uv_to_st(u: f64) -> f64 {
    if u >= 0.0 {
        0.5 * (1.0 + 3.0 * u).sqrt()
    } else {
        1.0 - 0.5 * (1.0 - 3.0 * u).sqrt()
    }
}

fn st_to_uv(s: f64) -> f64 {
    if s >= 0.5 {
        (4.0 * s * s - 1.0) / 3.0
    } else {
        (1.0 - 4.0 * (1.0 - s) * (1.0 - s)) / 3.0
    }
}

/// The leaf holding a direction of the auxiliary sphere.
pub fn leaf(p: Vector) -> CellId {
    let face = face_of(p);
    let (u, v) = face_uv(face, p);
    let side = (1u64 << MAX_LEVEL) as f64;
    let index = |x: f64| (uv_to_st(x) * side).floor().clamp(0.0, side - 1.0) as u32;
    CellId::from_face_ij(face, index(u), index(v))
}

/// The leaf holding a WGS84 position. Positions at ±180 and every longitude
/// of a pole name one direction, so they share one leaf.
pub fn leaf_of(p: Pos) -> CellId {
    leaf(arcs::direction(p))
}

/// The padded corners of a cell, counterclockwise about its outward normal.
fn corners(id: CellId) -> [Vector; 4] {
    let (face, level, i, j) = id.face_ij();
    let size = 1.0 / (1u64 << level) as f64;
    let low = |k: u32| st_to_uv(f64::from(k) * size - PAD);
    let high = |k: u32| st_to_uv(f64::from(k + 1) * size + PAD);
    let (u0, u1, v0, v1) = (low(i), high(i), low(j), high(j));
    [
        face_xyz(face, u0, v0),
        face_xyz(face, u1, v0),
        face_xyz(face, u1, v1),
        face_xyz(face, u0, v1),
    ]
}

/// A lower bound on the auxiliary-sphere angle from a direction to any
/// direction keyed into `id`: 0 when the direction's own leaf is in the cell
/// or it lies within `NEAR` of the padded cell, else its angle to the padded
/// cell's nearest edge.
fn angle_to(id: CellId, corners: &[Vector; 4], p: Vector) -> f64 {
    if id.contains(leaf(p)) {
        return 0.0;
    }
    let angle = (0..4)
        .map(|k| arcs::nearest_on_arc(p, corners[k], corners[(k + 1) % 4]).0)
        .fold(f64::INFINITY, f64::min);
    if angle < NEAR {
        0.0
    } else {
        angle
    }
}

/// A lower bound, in metres, on the WGS84 distance from `origin` to any
/// position keyed into `id`.
pub fn distance_lower_bound(origin: Pos, id: CellId) -> f64 {
    let angle = angle_to(id, &corners(id), arcs::direction(origin));
    if angle == 0.0 {
        0.0
    } else {
        segment_distance::lower_bound(angle)
    }
}

/// A direction's (s, t) in a face's frame, when it lies in the open
/// hemisphere around that face's axis. Lines of constant s or t there are
/// great circles, so a cell is a range of each.
fn face_st(face: u8, p: Vector) -> Option<(f64, f64)> {
    let axis = match face {
        0 => p[0],
        1 => p[1],
        2 => p[2],
        3 => -p[0],
        4 => -p[1],
        _ => -p[2],
    };
    (axis > 0.0).then(|| {
        let (u, v) = face_uv(face, p);
        (uv_to_st(u), uv_to_st(v))
    })
}

/// A cell prepared for tests: its face, its padded ranges of s and t, the
/// corners they meet at and a longitude/latitude box holding it.
pub struct Cell {
    id: CellId,
    face: u8,
    s: [f64; 2],
    t: [f64; 2],
    corners: [Vector; 4],
    bounds: [f64; 4],
}

impl Cell {
    pub fn new(id: CellId, work: &mut GeometryWork) -> Result<Self, GeometryRefusal> {
        work.charge(16)?;
        let (face, level, i, j) = id.face_ij();
        let size = 1.0 / (1u64 << level) as f64;
        let range = |k: u32| [f64::from(k) * size - PAD, f64::from(k + 1) * size + PAD];
        let corners = corners(id);
        let positions = corners.map(arcs::position);
        let boxes: Vec<[f64; 4]> = (0..4)
            .map(|k| arcs::arc_bounds(positions[k], positions[(k + 1) % 4]))
            .collect();
        let south = boxes.iter().map(|b| b[1]).fold(f64::INFINITY, f64::min);
        let north = boxes.iter().map(|b| b[3]).fold(f64::NEG_INFINITY, f64::max);
        let (west, east) = arcs::covering(boxes.iter().map(|b| (b[0], b[2])));
        let mut cell = Self {
            id,
            face,
            s: range(i),
            t: range(j),
            corners,
            // Margins for the corners' rounding to positions.
            bounds: [west - 1e-9, south - 1e-9, east + 1e-9, north + 1e-9],
        };
        if cell.holds_direction([0.0, 0.0, 1.0]) {
            cell.bounds = [-180.0, cell.bounds[1], 180.0, 90.0];
        }
        if cell.holds_direction([0.0, 0.0, -1.0]) {
            cell.bounds = [-180.0, -90.0, 180.0, cell.bounds[3]];
        }
        Ok(cell)
    }

    fn holds_direction(&self, p: Vector) -> bool {
        face_st(self.face, p).is_some_and(|(s, t)| {
            self.s[0] <= s && s <= self.s[1] && self.t[0] <= t && t <= self.t[1]
        })
    }

    pub fn id(&self) -> CellId {
        self.id
    }

    pub fn corners(&self) -> &[Vector; 4] {
        &self.corners
    }

    /// A box holding the padded cell: east may pass 180 by up to a turn, and
    /// a cell around a pole spans every longitude.
    pub fn bounds(&self) -> [f64; 4] {
        self.bounds
    }

    /// Whether a direction lies in the padded cell, its edges included.
    pub fn holds(&self, p: Vector, work: &mut GeometryWork) -> Result<bool, GeometryRefusal> {
        work.charge(1)?;
        Ok(self.holds_direction(p))
    }

    /// Whether the arc a-b meets the padded cell: an end in it, or a crossing
    /// or touch with one of its edges.
    pub fn meets_arc(
        &self,
        a: Vector,
        b: Vector,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        if self.holds(a, work)? || self.holds(b, work)? {
            return Ok(true);
        }
        if a == b {
            return Ok(false);
        }
        for k in 0..4 {
            let (c, d) = (self.corners[k], self.corners[(k + 1) % 4]);
            if arcs::crossing_sign(a, b, c, d, work)? >= 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A lower bound on the auxiliary angle from a direction to the cell.
    pub fn angle_to(&self, p: Vector) -> f64 {
        angle_to(self.id, &self.corners, p)
    }

    /// A lower bound on the auxiliary angle from the arc a-b to the cell,
    /// once the arc is known not to meet it: two disjoint minor arcs are
    /// nearest at an end of one of them.
    pub fn angle_to_arc(&self, a: Vector, b: Vector) -> f64 {
        let mut angle = self.angle_to(a).min(self.angle_to(b));
        if a != b {
            for corner in self.corners {
                angle = angle.min(arcs::nearest_on_arc(corner, a, b).0);
            }
        }
        angle
    }
}

/// How a cell stands to a cover's target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellRelation {
    /// Nothing of the target is in the cell.
    Misses,
    /// The target is in part of the cell, or may be.
    Meets,
    /// The whole cell is in the target.
    Inside,
}

/// What a cover refines against.
pub trait CellTarget {
    fn relate(&self, cell: &Cell, work: &mut GeometryWork) -> Result<CellRelation, GeometryRefusal>;
}

/// A bounded cover of a target: from the six faces, every cell the target
/// meets may be split and every cell inside it kept whole. Refine coarse
/// boundary cells first, breaking ties by ID. A split that does not fit keeps
/// its parent while other cells can still refine. Only the target and the
/// budget determine the sorted cover; population statistics never change it.
pub fn cover(
    target: &dyn CellTarget,
    max_cells: usize,
    work: &mut GeometryWork,
) -> Result<Vec<CellId>, GeometryRefusal> {
    let classify = |id: CellId,
                    kept: &mut Vec<CellId>,
                    meets: &mut Vec<CellId>,
                    work: &mut GeometryWork|
     -> Result<(), GeometryRefusal> {
        let cell = work.scoped(|work| Cell::new(id, work))?;
        match work.scoped(|work| target.relate(&cell, work))? {
            CellRelation::Misses => {},
            CellRelation::Meets => meets.push(id),
            CellRelation::Inside => kept.push(id),
        }
        Ok(())
    };
    if max_cells < 6 {
        return Err(crate::invalid(
            "A global cell cover needs room for six faces",
        ));
    }
    work.allocate(max_cells.saturating_mul(128))?;
    let (mut kept, mut meets) = (Vec::new(), Vec::new());
    for face in CellId::faces() {
        classify(face, &mut kept, &mut meets, work)?;
    }
    while !meets.is_empty() {
        work.charge(meets.len())?;
        let position = (0..meets.len())
            .min_by_key(|position| (meets[*position].level(), meets[*position]))
            .expect("nonempty frontier");
        let id = meets.swap_remove(position);
        let Some(children) = id.children() else {
            kept.push(id);
            continue;
        };
        let (mut inside, mut boundary) = (Vec::with_capacity(4), Vec::with_capacity(4));
        for child in children {
            classify(child, &mut inside, &mut boundary, work)?;
        }
        if kept.len() + meets.len() + inside.len() + boundary.len() <= max_cells {
            kept.extend(inside);
            meets.extend(boundary);
        } else {
            // Keeping the unsplit parent preserves complete coverage.
            kept.push(id);
        }
    }
    kept.extend(meets);
    kept.sort_unstable();
    Ok(kept)
}

/// The cells within an angle of a centre: a cap on the auxiliary sphere.
pub struct Cap {
    pub centre: Vector,
    /// Radians of auxiliary angle, under a quarter turn.
    pub angle: f64,
}

impl CellTarget for Cap {
    fn relate(&self, cell: &Cell, _: &mut GeometryWork) -> Result<CellRelation, GeometryRefusal> {
        if self.angle >= std::f64::consts::FRAC_PI_2 {
            return Ok(CellRelation::Meets);
        }
        if cell.angle_to(self.centre) * (1.0 - 1e-12) > self.angle {
            return Ok(CellRelation::Misses);
        }
        let within = |corner: &Vector| arcs::angle(self.centre, *corner) * (1.0 + 1e-12) < self.angle;
        Ok(if cell.corners().iter().all(within) {
            CellRelation::Inside
        } else {
            CellRelation::Meets
        })
    }
}

/// The cells a great elliptic arc between two positions meets.
pub struct ArcTarget {
    pub a: Vector,
    pub b: Vector,
}

impl ArcTarget {
    pub fn new(a: Pos, b: Pos) -> Self {
        Self {
            a: arcs::direction(a),
            b: arcs::direction(b),
        }
    }
}

impl CellTarget for ArcTarget {
    fn relate(&self, cell: &Cell, work: &mut GeometryWork) -> Result<CellRelation, GeometryRefusal> {
        Ok(if cell.meets_arc(self.a, self.b, work)? {
            CellRelation::Meets
        } else {
            CellRelation::Misses
        })
    }
}

#[cfg(test)]
#[path = "cells_tests.rs"]
mod tests;
