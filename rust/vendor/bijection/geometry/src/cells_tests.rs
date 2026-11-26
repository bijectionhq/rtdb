use super::*;
use crate::{
    predicates::{
        point_distance,
        prepared::{
            Near,
            Preparation,
            PreparedGeometry,
        },
    },
    Coords,
    Geometry,
    GeometrySpace,
    Kind,
};

/// A deterministic generator for reproducible positions.
struct Random(u64);

impl Random {
    fn next(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 11) as f64) / ((1_u64 << 53) as f64)
    }

    fn below(&mut self, n: u32) -> u32 {
        (self.next() * f64::from(n)) as u32
    }

    fn position(&mut self) -> Pos {
        [self.next() * 360.0 - 180.0, self.next() * 180.0 - 90.0]
    }
}

fn work() -> GeometryWork {
    GeometryWork::default()
}

#[test]
fn ids_round_trip_and_nest() {
    let mut random = Random(7);
    let side = 1u32 << MAX_LEVEL;
    for _ in 0..2000 {
        let face = random.below(6) as u8;
        let (i, j) = (random.below(side), random.below(side));
        let leaf = CellId::from_face_ij(face, i, j);
        assert_eq!(leaf.face_ij(), (face, MAX_LEVEL, i, j));
        assert_eq!(CellId::from_raw(leaf.raw()), Some(leaf));
        for level in [0, 1, 5, 13, 26] {
            let parent = leaf.parent(level);
            assert_eq!(CellId::from_raw(parent.raw()), Some(parent));
            assert_eq!(parent.level(), level);
            assert!(parent.contains(leaf));
            let shift = MAX_LEVEL - level;
            assert_eq!(parent.face_ij(), (face, level, i >> shift, j >> shift));
            let children = parent.children().expect("children");
            assert_eq!(children.iter().filter(|child| child.contains(leaf)).count(), 1);
            let (first, last) = parent.range();
            assert_eq!(first, children[0].range().0);
            assert_eq!(last, children[3].range().1);
        }
    }
    assert!(CellId::from_raw(0).is_none());
    assert!(CellId::from_raw(2).is_none());
    assert!(CellId::from_raw(6 << POS_BITS | 1).is_none());
    // Seven faces of ids fit under the class bits of a non-negative key.
    assert!(CellId::faces()[5].range().1 < 1 << ID_BITS);
}

#[test]
fn hilbert_neighbours_share_an_edge() {
    for face in CellId::faces() {
        let level = 5;
        let (first, last) = face.range();
        let step = 2 * (1u64 << (2 * (MAX_LEVEL - level)));
        let mut id = CellId(first - 1 + step / 2);
        let mut previous: Option<(u32, u32)> = None;
        while id.0 <= last {
            let (_, found_level, i, j) = id.face_ij();
            assert_eq!(found_level, level);
            if let Some((pi, pj)) = previous {
                assert_eq!(pi.abs_diff(i) + pj.abs_diff(j), 1, "{id:?}");
            }
            previous = Some((i, j));
            id = CellId(id.0 + step);
        }
    }
}

#[test]
fn aliases_share_a_leaf_and_padded_cells_hold_their_directions() -> anyhow::Result<()> {
    assert_eq!(leaf_of([0.0, 90.0]), leaf_of([123.0, 90.0]));
    assert_eq!(leaf_of([180.0, 12.5]), leaf_of([-180.0, 12.5]));
    let pole = leaf_of([0.0, 90.0]).face_ij();
    assert_eq!(pole, (2, MAX_LEVEL, 1 << (MAX_LEVEL - 1), 1 << (MAX_LEVEL - 1)));
    let mut random = Random(11);
    for _ in 0..500 {
        let p = random.position();
        let v = arcs::direction(p);
        let leaf = leaf(v);
        for level in [MAX_LEVEL, 20, 9, 0] {
            let cell = Cell::new(leaf.parent(level), &mut work())?;
            assert!(cell.holds(v, &mut work())?, "{p:?} at {level}");
            assert_eq!(cell.angle_to(v), 0.0);
        }
    }
    Ok(())
}

/// The solid angle of a quadrilateral of unit directions, as two triangles.
fn solid_angle(c: &[Vector; 4]) -> f64 {
    let unit = c.map(arcs::unit);
    let triangle = |a: Vector, b: Vector, d: Vector| {
        let cross = [
            b[1] * d[2] - b[2] * d[1],
            b[2] * d[0] - b[0] * d[2],
            b[0] * d[1] - b[1] * d[0],
        ];
        let dot = |x: Vector, y: Vector| x[0] * y[0] + x[1] * y[1] + x[2] * y[2];
        2.0 * libm::atan2(dot(a, cross).abs(), 1.0 + dot(a, b) + dot(b, d) + dot(d, a))
    };
    triangle(unit[0], unit[1], unit[2]) + triangle(unit[0], unit[2], unit[3])
}

#[test]
fn quadratic_cells_stay_within_a_small_area_ratio() {
    let level = 6;
    let face = CellId::faces()[0];
    let (first, last) = face.range();
    let step = 2 * (1u64 << (2 * (MAX_LEVEL - level)));
    let (mut smallest, mut largest) = (f64::INFINITY, 0.0_f64);
    let mut id = first - 1 + step / 2;
    while id <= last {
        let area = solid_angle(&corners(CellId(id)));
        smallest = smallest.min(area);
        largest = largest.max(area);
        id += step;
    }
    let ratio = largest / smallest;
    assert!((1.9..2.2).contains(&ratio), "{ratio}");
}

#[test]
fn distance_bounds_never_pass_true_distances() {
    let mut random = Random(13);
    for _ in 0..1000 {
        let origin = random.position();
        let point = random.position();
        let distance = point_distance(origin, point);
        for level in [0, 3, 8, 14, 20, MAX_LEVEL] {
            let cell = leaf_of(point).parent(level);
            assert!(
                distance_lower_bound(origin, cell) <= distance + 1e-6,
                "{origin:?} {point:?} {level}"
            );
        }
        assert_eq!(distance_lower_bound(point, leaf_of(point)), 0.0);
    }
}

fn covered(cover: &[CellId], p: Pos) -> bool {
    let leaf = leaf_of(p);
    cover.iter().any(|cell| cell.contains(leaf))
}

#[test]
fn cap_and_arc_covers_hold_every_member() -> anyhow::Result<()> {
    let mut random = Random(17);
    for _ in 0..40 {
        let centre = random.position();
        let angle = random.next() * 0.2;
        let cap = Cap {
            centre: arcs::unit(arcs::direction(centre)),
            angle,
        };
        let cells = cover(&cap, 32, &mut work())?;
        assert!(cells.len() <= 32);
        for _ in 0..50 {
            let p = random.position();
            if arcs::angle(cap.centre, arcs::direction(p)) <= angle {
                assert!(covered(&cells, p), "{centre:?} {angle} {p:?}");
            }
        }
        assert!(covered(&cells, centre));
    }
    for (a, b) in [
        ([179.0, 0.0], [-179.0, 1.0]),
        ([-80.0, -30.0], [80.0, 30.0]),
        ([0.0, 89.0], [180.0, 89.0]),
        ([10.0, 10.0], [10.0, 10.0]),
    ] {
        let cells = cover(&ArcTarget::new(a, b), 8, &mut work())?;
        assert!(!cells.is_empty() && cells.len() <= 8);
        for k in 0..=64 {
            let p = crate::trajectory::position_at(a, b, f64::from(k) / 64.0);
            assert!(covered(&cells, p), "{a:?} {b:?} {k}");
        }
    }
    Ok(())
}

fn polygon(ring: &[Pos]) -> anyhow::Result<PreparedGeometry> {
    let geometry = crate::admit_value(
        &crate::Gv::from(&serde_json::json!({"type":"Polygon","coordinates":[ring]})),
        &crate::GeometryDeclaration {
            space: GeometrySpace::wgs84_xy(),
            size: crate::Size::Large,
        },
    )?;
    Ok(PreparedGeometry::new(geometry, Preparation::Topology, &mut work())?)
}

#[test]
fn shape_covers_hold_members_and_keep_insides_whole() -> anyhow::Result<()> {
    let square = polygon(&[[2.0, 48.0], [3.0, 48.0], [3.0, 49.0], [2.0, 49.0], [2.0, 48.0]])?;
    let polar = polygon(&[
        [-180.0, 80.0],
        [-90.0, 80.0],
        [0.0, 80.0],
        [90.0, 80.0],
        [-180.0, 80.0],
    ])?;
    let seam = polygon(&[[170.0, -10.0], [-170.0, -10.0], [-170.0, 10.0], [170.0, 10.0], [170.0, -10.0]])?;
    let mut random = Random(19);
    for region in [&square, &polar, &seam] {
        let target = Near {
            geometry: region,
            reach_m: 0.0,
        };
        let cells = cover(&target, 32, &mut work())?;
        assert!(!cells.is_empty() && cells.len() <= 32);
        for _ in 0..3000 {
            let p = random.position();
            if region.point(p, &mut work())? != crate::Rel::Outside {
                assert!(covered(&cells, p), "{p:?}");
            }
        }
    }
    // The pole lies inside the polar cap.
    let cells = cover(
        &Near {
            geometry: &polar,
            reach_m: 0.0,
        },
        32,
        &mut work(),
    )?;
    assert!(covered(&cells, [0.0, 90.0]));
    // A reach widens the cover to every position within it.
    for reach_m in [10.0, 10_000.0, 1_000_000.0] {
        let cells = cover(
            &Near {
                geometry: &square,
                reach_m,
            },
            128,
            &mut work(),
        )?;
        for _ in 0..500 {
            // A position south of the square's southern edge, within the reach.
            let edge = [2.0 + random.next(), 48.0];
            let offset = crate::geodesy::direct(edge[1], edge[0], 180.0, reach_m * random.next());
            let near = [offset.lon2, offset.lat2];
            assert!(covered(&cells, near), "{reach_m} {near:?}");
        }
    }
    Ok(())
}

#[test]
fn an_expensive_split_does_not_block_other_faces() -> anyhow::Result<()> {
    struct Target;
    impl CellTarget for Target {
        fn relate(
            &self,
            cell: &Cell,
            _: &mut GeometryWork,
        ) -> Result<CellRelation, GeometryRefusal> {
            let (_, _, i, j) = cell.id.face_ij();
            Ok(
                if cell.id.face() == 0 || cell.id.level() == 0 || (i == 0 && j == 0) {
                    CellRelation::Meets
                } else {
                CellRelation::Misses
            },
            )
        }
    }
    let cover = cover(&Target, 6, &mut work())?;
    assert_eq!(cover.len(), 6);
    assert!(cover.contains(&CellId::faces()[0]));
    assert!(cover
        .iter()
        .filter(|cell| cell.face() != 0)
        .all(|cell| cell.level() == MAX_LEVEL));
    Ok(())
}

#[test]
fn point_and_line_geometries_cover_their_vertices() -> anyhow::Result<()> {
    let line = crate::admit_value(
        &crate::Gv::from(&serde_json::json!({"type":"LineString","coordinates":[[179,0],[-179,1],[-170,5]]})),
        &crate::GeometryDeclaration {
            space: GeometrySpace::wgs84_xy(),
            size: crate::Size::Large,
        },
    )?;
    let points = Geometry::new(
        Kind::MultiPoint,
        Coords::MultiPoint(vec![Some([0.0, 90.0]), Some([45.0, -10.0])]),
        GeometrySpace::wgs84_xy(),
    );
    for geometry in [line, points] {
        let prepared = PreparedGeometry::new(geometry.clone(), Preparation::Metric, &mut work())?;
        let cells = cover(
            &Near {
                geometry: &prepared,
                reach_m: 0.0,
            },
            32,
            &mut work(),
        )?;
        geometry.for_each(|p| assert!(covered(&cells, *p), "{p:?}"));
    }
    Ok(())
}

/// An edge whose box passes 180 holds longitudes west of the seam only one
/// turn east. Every cell such an edge meets, on either side of ±180, must
/// relate as met and stay in the cover: a line across the seam, a polygon
/// straddling it and a polygon around a pole with an edge across it.
#[test]
fn covers_keep_cells_met_on_both_sides_of_the_antimeridian() -> anyhow::Result<()> {
    let admit = |value: serde_json::Value| {
        crate::admit_value(
            &crate::Gv::from(&value),
            &crate::GeometryDeclaration {
                space: GeometrySpace::wgs84_xy(),
                size: crate::Size::Large,
            },
        )
    };
    let line = serde_json::json!({"type":"LineString","coordinates":[[179.5,-1],[-179.5,1]]});
    let line = admit(line)?;
    let shapes = [
        (line.clone(), Preparation::Metric),
        (line, Preparation::Topology),
        (
            admit(serde_json::json!({"type":"Polygon","coordinates":[[
                [179.0,-1.0],[-179.0,-1.0],[-179.0,1.0],[179.0,1.0],[179.0,-1.0]
            ]]}))?,
            Preparation::Topology,
        ),
        (
            admit(serde_json::json!({"type":"Polygon","coordinates":[[
                [-135.0,75.0],[-45.0,75.0],[45.0,75.0],[135.0,75.0],[-135.0,75.0]
            ]]}))?,
            Preparation::Topology,
        ),
    ];
    for (geometry, preparation) in shapes {
        let shape = PreparedGeometry::new(geometry.clone(), preparation, &mut work())?;
        let cells = cover(
            &Near {
                geometry: &shape,
                reach_m: 0.0,
            },
            64,
            &mut work(),
        )?;
        let rings = geometry.polygons().into_iter().flatten();
        let mut sides = [false; 2];
        for path in geometry.lines().into_iter().chain(rings) {
            for pair in path.windows(2) {
                for k in 0..=256 {
                    let p = crate::trajectory::position_at(pair[0], pair[1], f64::from(k) / 256.0);
                    sides[usize::from(p[0] < 0.0)] = true;
                    assert!(covered(&cells, p), "{:?} misses {p:?}", geometry.kind);
                    for level in [1, 2, 4, 8, 12, 16, 20] {
                        let id = leaf_of(p).parent(level);
                        let cell = Cell::new(id, &mut work())?;
                        assert!(
                            cell.meets_arc(
                                arcs::direction(pair[0]),
                                arcs::direction(pair[1]),
                                &mut work()
                            )?,
                            "{p:?} at {level}"
                        );
                        assert_ne!(
                            shape.relate_cell(&cell, &mut work())?,
                            CellRelation::Misses,
                            "{:?} {p:?} at {level}",
                            geometry.kind
                        );
                    }
                }
            }
        }
        assert_eq!(sides, [true, true], "{:?}", geometry.kind);
    }
    Ok(())
}

/// Step 2 of the index design: cover cost and precision at several shape
/// budgets, on Natural Earth's countries read through the planar import.
/// Every point a country holds must lie in one of its cells; the report
/// names keys per country and the share of candidate countries the exact
/// test rejects.
#[test]
fn country_covers_hold_their_points_at_every_budget() -> anyhow::Result<()> {
    let set: serde_json::Value = serde_json::from_str(include_str!(
        "../../../npm-packages/libraries/geo/boundaries/ne-50m-admin0.json"
    ))?;
    let declaration = crate::GeometryDeclaration {
        space: GeometrySpace::wgs84_xy(),
        size: crate::Size::Large,
    };
    let mut countries = Vec::new();
    for region in set["regions"].as_array().expect("regions") {
        let mut work = GeometryWork::with_limits(usize::MAX, usize::MAX);
        let Ok(geometry) = crate::planar::from_planar(
            &crate::Gv::from(&region["geometry"]),
            &declaration,
            10.0,
            &mut work,
        ) else {
            continue;
        };
        countries.push(PreparedGeometry::new(geometry, Preparation::Topology, &mut work)?);
    }
    assert!(countries.len() > 200, "{}", countries.len());
    let mut random = Random(23);
    let points: Vec<Pos> = (0..4000).map(|_| random.position()).collect();
    let holders: Vec<Vec<usize>> = points
        .iter()
        .map(|p| {
            (0..countries.len())
                .filter(|&k| {
                    countries[k].point(*p, &mut GeometryWork::with_limits(usize::MAX, usize::MAX))
                        .is_ok_and(|rel| rel == crate::Rel::Inside)
                })
                .collect()
        })
        .collect();
    for budget in [8, 16, 32, 64] {
        let mut keys = 0;
        let covers: Vec<Vec<CellId>> = countries
            .iter()
            .map(|country| {
                let cells = cover(
                    &Near {
                        geometry: country,
                        reach_m: 0.0,
                    },
                    budget,
                    &mut GeometryWork::with_limits(usize::MAX, usize::MAX),
                )
                .expect("cover");
                keys += cells.len();
                cells
            })
            .collect();
        let (mut candidates, mut rejected) = (0usize, 0usize);
        for (p, holders) in points.iter().zip(&holders) {
            for (k, cells) in covers.iter().enumerate() {
                let found = covered(cells, *p);
                if holders.contains(&k) {
                    assert!(found, "budget {budget}: country {k} misses {p:?}");
                }
                if found {
                    candidates += 1;
                    rejected += usize::from(!holders.contains(&k));
                }
            }
        }
        eprintln!(
            "BUDGET {budget}: {:.1} keys per country, {candidates} candidates, {:.1}% rejected",
            keys as f64 / countries.len() as f64,
            100.0 * rejected as f64 / candidates.max(1) as f64
        );
    }
    Ok(())
}
