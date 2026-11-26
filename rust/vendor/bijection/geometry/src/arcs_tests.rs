use super::*;

fn work() -> GeometryWork {
    GeometryWork::default()
}

/// A deterministic generator for reproducible random positions.
struct Random(u64);

impl Random {
    fn next(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 11) as f64) / ((1_u64 << 53) as f64)
    }

    fn position(&mut self) -> Pos {
        [self.next() * 360.0 - 180.0, self.next() * 180.0 - 90.0]
    }
}

fn exact_det(a: Vector, b: Vector, c: Vector) -> i8 {
    let [a, b, c]: [Exact; 3] = exact_vectors(&[a, b, c], &mut work())
        .unwrap()
        .try_into()
        .unwrap_or_else(|_| unreachable!());
    signum(&exact_dot(&a, &exact_cross(&b, &c)))
}

#[test]
fn directions_keep_meridians_the_equator_poles_and_the_seam_exact() {
    assert_eq!(direction([0.0, 90.0]), [0.0, 0.0, 1.0]);
    assert_eq!(direction([123.0, -90.0]), [0.0, 0.0, -1.0]);
    assert_eq!(direction([180.0, 0.0]), direction([-180.0, 0.0]));
    assert_eq!(direction([37.0, 0.0])[2], 0.0);
    let meridian = [
        direction([12.5, -40.0]),
        direction([12.5, 3.0]),
        direction([12.5, 71.0]),
    ];
    assert_eq!(&meridian[0][..2], &meridian[1][..2]);
    let mut w = work();
    // Exactly coplanar: three points on one meridian, three on the equator.
    assert_eq!(
        exact_sign(meridian[0], meridian[1], meridian[2], &mut w).unwrap(),
        0
    );
    let equator = [
        direction([-10.0, 0.0]),
        direction([20.0, 0.0]),
        direction([95.0, 0.0]),
    ];
    assert_eq!(
        exact_sign(equator[0], equator[1], equator[2], &mut w).unwrap(),
        0
    );
    // Reduced latitude: tan β = (1 − f) tan φ.
    let v = direction([0.0, 45.0]);
    assert!((v[2] - F1).abs() < 1e-15);
}

#[test]
fn arc_bounds_preserve_tiny_westward_longitude_differences() {
    // This difference disappears when added to 540 degrees.
    let (a, b) = ([-90.0, -70.0], [-90.00000000000001, -69.0]);
    for (p, q) in [(a, b), (b, a)] {
        let [west, south, east, north] = arc_bounds(p, q);
        assert_eq!((west, east), (b[0], a[0]));
        assert!(south <= a[1] && north >= b[1]);
    }
    for (p, q) in [([179.0, 0.0], [-179.0, 0.0]), ([-179.0, 0.0], [179.0, 0.0])] {
        assert_eq!(arc_bounds(p, q), [179.0, 0.0, 181.0, 0.0]);
    }
}

#[test]
fn positions_round_trip_through_directions() {
    let mut random = Random(7);
    for _ in 0..1000 {
        let p = random.position();
        let q = position(direction(p));
        assert!((q[1] - p[1]).abs() < 1e-12, "{p:?} {q:?}");
        let turn = (q[0] - p[0] + 540.0).rem_euclid(360.0) - 180.0;
        assert!(turn.abs() < 1e-12 || p[1].abs() > 89.9999, "{p:?} {q:?}");
    }
    assert_eq!(position([0.0, 0.0, 2.0]), [0.0, 90.0]);
}

#[test]
fn orientation_is_counterclockwise_with_the_interior_on_the_left() {
    let mut w = work();
    let a = direction([0.0, 0.0]);
    let b = direction([1.0, 0.0]);
    assert_eq!(sign(a, b, direction([0.5, 1.0]), &mut w).unwrap(), 1);
    assert_eq!(sign(a, b, direction([0.5, -1.0]), &mut w).unwrap(), -1);
    assert_eq!(sign(a, a, b, &mut w).unwrap(), 0);
}

#[test]
fn the_binary64_filter_agrees_with_exact_arithmetic() {
    let mut random = Random(11);
    let mut w = work();
    let mut decided = 0;
    for _ in 0..2000 {
        let a = direction(random.position());
        let b = direction(random.position());
        // Near-degenerate: c lies within rounding of the plane through a, b.
        let c = if random.next() < 0.5 {
            direction(random.position())
        } else {
            let t = random.next();
            let mid = [
                a[0] * t + b[0] * (1.0 - t),
                a[1] * t + b[1] * (1.0 - t),
                a[2] * t + b[2] * (1.0 - t),
            ];
            direction(position(mid))
        };
        let fast = triage(a, b, c, 8.0);
        let exact = exact_det(a, b, c);
        if fast != 0 {
            decided += 1;
            assert_eq!(fast, exact, "{a:?} {b:?} {c:?}");
        }
        assert_eq!(exact_sign(a, b, c, &mut w).unwrap(), exact);
        if exact != 0 {
            assert_eq!(sign(a, b, c, &mut w).unwrap(), exact);
        }
    }
    assert!(decided > 1000, "the filter decides most cases ({decided})");
}

/// Coplanar triples get a non-zero sign that flips with every transposition,
/// and the perturbed orientations satisfy the Grassmann–Plücker relation
/// [abc][ade] − [abd][ace] + [abe][acd] = 0 that real vectors obey.
#[test]
fn symbolic_perturbation_is_antisymmetric_and_realisable() {
    let mut w = work();
    let degenerate_sets: [[Pos; 5]; 4] = [
        [
            [-10.0, 0.0],
            [20.0, 0.0],
            [95.0, 0.0],
            [-120.0, 0.0],
            [170.0, 0.0],
        ],
        [
            [12.5, -40.0],
            [12.5, 3.0],
            [12.5, 71.0],
            [12.5, -89.0],
            [-167.5, 10.0],
        ],
        [
            [0.0, 0.0],
            [0.0, 30.0],
            [90.0, 0.0],
            [0.0, 90.0],
            [45.0, 0.0],
        ],
        [
            [0.0, 0.0],
            [10.0, 0.0],
            [0.0, 10.0],
            [10.0, 10.0],
            [-10.0, 0.0],
        ],
    ];
    for set in degenerate_sets {
        let v = set.map(direction);
        let s =
            |i: usize, j: usize, k: usize, w: &mut GeometryWork| sign(v[i], v[j], v[k], w).unwrap();
        for (i, j, k) in [(0, 1, 2), (0, 1, 3), (1, 2, 3), (0, 3, 4), (2, 3, 4)] {
            let abc = s(i, j, k, &mut w);
            assert_ne!(abc, 0);
            assert_eq!(s(j, i, k, &mut w), -abc);
            assert_eq!(s(i, k, j, &mut w), -abc);
            assert_eq!(s(j, k, i, &mut w), abc);
            assert_eq!(s(k, i, j, &mut w), abc);
        }
        for a in 0..5 {
            let others: Vec<usize> = (0..5).filter(|x| *x != a).collect();
            let [b, c, d, e] = [others[0], others[1], others[2], others[3]];
            let terms = [
                s(a, b, c, &mut w) * s(a, d, e, &mut w),
                -(s(a, b, d, &mut w) * s(a, c, e, &mut w)),
                s(a, b, e, &mut w) * s(a, c, d, &mut w),
            ];
            assert!(
                terms.contains(&1) && terms.contains(&-1),
                "set {set:?} at {a}: {terms:?}"
            );
        }
    }
}

#[test]
fn arcs_cross_only_at_interior_points_of_both() {
    let mut w = work();
    let d = |lon: f64, lat: f64| direction([lon, lat]);
    // Equator and a meridian through its middle.
    assert_eq!(
        crossing_sign(
            d(-10.0, 0.0),
            d(10.0, 0.0),
            d(0.0, -10.0),
            d(0.0, 10.0),
            &mut w
        )
        .unwrap(),
        1
    );
    // The great circles meet at (170, 0) and (-10, 0), neither inside both arcs.
    assert_eq!(
        crossing_sign(
            d(-10.0, 0.0),
            d(10.0, 0.0),
            d(170.0, -10.0),
            d(170.0, 10.0),
            &mut w
        )
        .unwrap(),
        -1
    );
    // Disjoint arcs of one great circle.
    assert_eq!(
        crossing_sign(
            d(0.0, 0.0),
            d(10.0, 0.0),
            d(20.0, 0.0),
            d(30.0, 0.0),
            &mut w
        )
        .unwrap(),
        -1
    );
    // A shared vertex.
    assert_eq!(
        crossing_sign(d(0.0, 0.0), d(10.0, 0.0), d(0.0, 0.0), d(0.0, 10.0), &mut w).unwrap(),
        0
    );
    // Across the antimeridian, with no cut.
    assert_eq!(
        crossing_sign(
            d(175.0, 0.0),
            d(-175.0, 0.0),
            d(180.0, -5.0),
            d(-180.0, 5.0),
            &mut w
        )
        .unwrap(),
        1
    );
    // Arcs are curved: the arc between two points at 60°N passes north of a
    // point on the parallel between them.
    let a = d(-30.0, 60.0);
    let b = d(30.0, 60.0);
    assert_eq!(sign(a, b, d(0.0, 60.0), &mut w).unwrap(), -1);
    assert!(on_arc(d(0.0, 60.0), a, b, &mut w).is_ok_and(|on| !on));
}

#[test]
fn rings_locate_points_inside_outside_and_on_the_boundary() {
    let mut w = work();
    let square = Ring::new(
        &[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]],
        &mut w,
    )
    .unwrap();
    assert_eq!(square.locate([0.5, 0.5], &mut w).unwrap(), Rel::Inside);
    assert_eq!(square.locate([2.0, 2.0], &mut w).unwrap(), Rel::Outside);
    assert_eq!(
        square.locate([-120.0, -45.0], &mut w).unwrap(),
        Rel::Outside
    );
    // The equator and meridian edges are exact.
    assert_eq!(square.locate([0.5, 0.0], &mut w).unwrap(), Rel::Boundary);
    assert_eq!(square.locate([1.0, 0.5], &mut w).unwrap(), Rel::Boundary);
    assert_eq!(square.locate([1.0, 1.0], &mut w).unwrap(), Rel::Boundary);
    // Clockwise, the same vertices bound the rest of the Earth.
    let complement = Ring::new(
        &[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]],
        &mut w,
    )
    .unwrap();
    assert_eq!(complement.locate([0.5, 0.5], &mut w).unwrap(), Rel::Outside);
    assert_eq!(
        complement.locate([50.0, 50.0], &mut w).unwrap(),
        Rel::Inside
    );
    // A ring around the north pole, and one across the antimeridian, uncut.
    let cap = Ring::new(
        &[[0.0, 80.0], [120.0, 80.0], [-120.0, 80.0], [0.0, 80.0]],
        &mut w,
    )
    .unwrap();
    assert_eq!(cap.locate([33.0, 90.0], &mut w).unwrap(), Rel::Inside);
    assert_eq!(cap.locate([33.0, 0.0], &mut w).unwrap(), Rel::Outside);
    let seam = Ring::new(
        &[
            [170.0, -5.0],
            [-170.0, -5.0],
            [-170.0, 5.0],
            [170.0, 5.0],
            [170.0, -5.0],
        ],
        &mut w,
    )
    .unwrap();
    assert_eq!(seam.locate([180.0, 0.0], &mut w).unwrap(), Rel::Inside);
    assert_eq!(seam.locate([-180.0, 0.0], &mut w).unwrap(), Rel::Inside);
    assert_eq!(seam.locate([0.0, 0.0], &mut w).unwrap(), Rel::Outside);
    // Its northern edge bulges poleward of the 5° parallel at the seam.
    assert_eq!(seam.locate([180.0, 5.0], &mut w).unwrap(), Rel::Inside);
}

/// Small convex rings have an independent exact answer: a point is inside
/// when it lies strictly left of every edge, and on the boundary when it lies
/// on an edge.
#[test]
fn containment_agrees_with_an_exact_convex_reference() {
    let mut random = Random(23);
    let mut w = work();
    for _ in 0..200 {
        let centre = [random.next() * 300.0 - 150.0, random.next() * 140.0 - 70.0];
        let size = 0.5 + random.next() * 5.0;
        let ring_positions = [
            [centre[0] - size, centre[1] - size],
            [centre[0] + size, centre[1] - size],
            [centre[0] + size, centre[1] + size],
            [centre[0] - size, centre[1] + size],
            [centre[0] - size, centre[1] - size],
        ];
        let ring = Ring::new(&ring_positions, &mut w).unwrap();
        let v = ring.vertices().to_vec();
        let mut points: Vec<Pos> = (0..20)
            .map(|_| {
                [
                    centre[0] + (random.next() * 3.0 - 1.5) * size,
                    centre[1] + (random.next() * 3.0 - 1.5) * size,
                ]
            })
            .collect();
        // Points on the meridian edges and at the vertices.
        points.push([centre[0] + size, centre[1]]);
        points.push([centre[0] - size, centre[1] + 0.25 * size]);
        points.extend(ring_positions.iter().take(4));
        for p in points {
            let q = direction(p);
            let signs: Vec<i8> = (0..4).map(|i| exact_det(v[i], v[(i + 1) % 4], q)).collect();
            let on_boundary = (0..4).any(|i| on_arc(q, v[i], v[(i + 1) % 4], &mut w).unwrap());
            let expected = if on_boundary {
                Rel::Boundary
            } else if signs.iter().all(|s| *s > 0) {
                Rel::Inside
            } else {
                Rel::Outside
            };
            assert_eq!(
                ring.locate(p, &mut w).unwrap(),
                expected,
                "{ring_positions:?} {p:?}"
            );
        }
    }
}

#[test]
fn ring_admission_refuses_invalid_rings() {
    let mut w = work();
    let refused = |positions: &[Pos], w: &mut GeometryWork| Ring::new(positions, w).is_err();
    assert!(refused(&[[0.0, 0.0], [1.0, 0.0], [0.0, 0.0]], &mut w));
    assert!(refused(&[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0]], &mut w));
    // A bow tie crosses itself.
    assert!(refused(
        &[[0.0, 0.0], [1.0, 1.0], [1.0, 0.0], [0.0, 1.0], [0.0, 0.0]],
        &mut w
    ));
    // A repeated vertex, including one across the seam.
    assert!(refused(
        &[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]],
        &mut w
    ));
    assert!(refused(
        &[
            [180.0, 0.0],
            [-179.0, 1.0],
            [-180.0, 0.0],
            [179.0, 1.0],
            [180.0, 0.0]
        ],
        &mut w
    ));
    // An antipodal edge fixes no unique arc.
    assert!(refused(
        &[[0.0, 0.0], [180.0, 0.0], [90.0, 45.0], [0.0, 0.0]],
        &mut w
    ));
    // A vertex on another edge.
    assert!(refused(
        &[[0.0, 0.0], [2.0, 0.0], [2.0, 2.0], [1.0, 0.0], [0.0, 0.0]],
        &mut w
    ));
    assert!(refused(
        &[[0.0, 0.0], [1.0, 0.0], [1.0, 91.0], [0.0, 0.0]],
        &mut w
    ));
    assert!(Ring::new(
        &[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]],
        &mut GeometryWork::with_limits(0, 1 << 20)
    )
    .is_err());
}

#[test]
fn geometry_bounds_reach_held_poles_and_wrap_the_seam() -> anyhow::Result<()> {
    let admit = |value: serde_json::Value| {
        crate::admit_value(
            &crate::Gv::from(&value),
            &crate::GeometryDeclaration {
                space: crate::GeometrySpace::wgs84_xy(),
                size: crate::Size::Large,
            },
        )
    };
    let bounds = |value| -> anyhow::Result<[f64; 4]> {
        Ok(geometry_bounds(&admit(value)?, &mut work())?.expect("bounds"))
    };
    // Across the antimeridian: one interval east of 180, and the arcs'
    // poleward bulge.
    let seam = bounds(serde_json::json!({"type":"Polygon","coordinates":[
        [[170,-10],[-170,-10],[-170,10],[170,10],[170,-10]]]}))?;
    assert_eq!((seam[0], seam[2]), (170.0, 190.0));
    assert!(seam[1] < -10.0 && seam[3] > 10.0);
    // A cap around the north pole holds it at every longitude.
    let mut cap: Vec<serde_json::Value> =
        (0..12).map(|k| serde_json::json!([-180 + 30 * k, 80])).collect();
    cap.push(cap[0].clone());
    let cap = bounds(serde_json::json!({"type":"Polygon","coordinates":[cap]}))?;
    assert_eq!((cap[0], cap[2], cap[3]), (-180.0, 180.0, 90.0));
    assert!(cap[1] == 80.0);
    // A clockwise ring bounds everything outside it.
    let complement = bounds(serde_json::json!({"type":"Polygon","coordinates":[
        [[0,0],[0,1],[1,1],[1,0],[0,0]]]}))?;
    assert_eq!(complement, [-180.0, -90.0, 180.0, 90.0]);
    Ok(())
}

#[test]
fn grouped_bounds_keep_gaps_until_every_member_has_contributed() -> anyhow::Result<()> {
    let values = [
        serde_json::json!({"type":"MultiPoint","coordinates":[[-170,0],[0,0]]}),
        serde_json::json!({"type":"Point","coordinates":[100,0]}),
        serde_json::json!({"type":"Point","coordinates":[]}),
    ];
    let declaration = crate::GeometryDeclaration {
        space: crate::GeometrySpace::wgs84_xy(),
        size: crate::Size::Large,
    };
    let geometries = values
        .iter()
        .map(|value| crate::admit_value(&crate::Gv::from(value), &declaration))
        .collect::<Result<Vec<_>, _>>()?;
    // Closing the first member's box before adding the second would fill
    // [-170, 0], the largest gap in the combined population.
    assert_eq!(
        geometries_bounds(&geometries, &mut work())?,
        Some([0.0, 0.0, 190.0, 0.0])
    );
    assert_eq!(geometries_bounds(&[], &mut work())?, None);
    let reply = crate::binding::invoke(serde_json::json!({
        "operation": "bounds", "values": values, "declaration": declaration,
    }));
    assert_eq!(reply["value"], serde_json::json!([0.0, 0.0, 190.0, 0.0]));
    Ok(())
}
