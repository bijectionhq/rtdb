use serde_json::json;

use super::*;

fn geometry(value: serde_json::Value) -> Geometry {
    crate::admit_value(
        &crate::Gv::from(&value),
        &crate::GeometryDeclaration {
            space: crate::GeometrySpace::wgs84_xy(),
            size: crate::Size::Large,
        },
    )
    .unwrap()
}

fn holds(a: &Geometry, b: &Geometry, predicate: Predicate) -> bool {
    evaluate(
        a,
        b,
        predicate,
        &mut GeometryWork::with_limits(100_000_000, 64 << 20),
    )
    .unwrap()
}

#[test]
fn all_topology_predicates_share_exact_boundaries_and_empty_rules() {
    let square =
        geometry(json!({"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}));
    // A subnormal longitude rounds onto its meridian's direction; 1e-300
    // degrees is the scale where directions still separate.
    for offset in [-5e-11, -1e-300, 0.0, 1e-300, 5e-11] {
        let point = geometry(json!({"type":"Point","coordinates":[offset,0.5]}));
        for (predicate, expected) in [
            (Predicate::Intersects, offset >= 0.0),
            (Predicate::Disjoint, offset < 0.0),
            (Predicate::Covers, offset >= 0.0),
            (Predicate::Contains, offset > 0.0),
            (Predicate::Touches, offset == 0.0),
            (Predicate::Crosses, false),
            (Predicate::Overlaps, false),
        ] {
            assert_eq!(
                evaluate(&square, &point, predicate, &mut GeometryWork::default()).unwrap(),
                expected,
                "{predicate:?} at {offset}"
            );
            assert_eq!(
                evaluate(
                    &point,
                    &square,
                    predicate.transposed(),
                    &mut GeometryWork::default()
                )
                .unwrap(),
                expected,
                "transposed {predicate:?} at {offset}"
            );
        }
    }
    let empty = geometry(json!({"type":"Point","coordinates":[]}));
    assert!(evaluate(
        &square,
        &empty,
        Predicate::Disjoint,
        &mut GeometryWork::default()
    )
    .unwrap());
    assert!(!evaluate(
        &square,
        &empty,
        Predicate::Intersects,
        &mut GeometryWork::default()
    )
    .unwrap());
    assert!(!evaluate(
        &square,
        &empty,
        Predicate::Covers,
        &mut GeometryWork::default()
    )
    .unwrap());
}

#[test]
fn point_sets_use_exact_locations_and_canonical_geographic_identity() {
    let square =
        geometry(json!({"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}));
    let points = |values| geometry(json!({"type":"MultiPoint","coordinates":values}));
    for (value, inside, outside, boundary) in [
        (json!([[0.5, 0.5]]), true, false, false),
        (json!([[0.5, 0.5], [2, 2]]), true, true, false),
        (json!([[0, 0.5], [2, 2]]), false, true, true),
        (json!([[0, 0.5], [0.5, 0.5]]), true, false, true),
    ] {
        let value = points(value);
        for (predicate, expected) in [
            (Predicate::Intersects, inside || boundary),
            (Predicate::Disjoint, !(inside || boundary)),
            (Predicate::CoveredBy, !outside),
            (Predicate::Within, inside && !outside),
            (Predicate::Touches, !inside && boundary),
            (Predicate::Crosses, inside && outside),
            (Predicate::Covers, false),
            (Predicate::Contains, false),
            (Predicate::Overlaps, false),
        ] {
            assert_eq!(
                evaluate(&value, &square, predicate, &mut GeometryWork::default()).unwrap(),
                expected
            );
            assert_eq!(
                evaluate(
                    &square,
                    &value,
                    predicate.transposed(),
                    &mut GeometryWork::default()
                )
                .unwrap(),
                expected
            );
        }
    }
    let a = points(json!([[180, 0], [90, 90], [-0.0, 0.0]]));
    let b = points(json!([[-180, 0], [-45, 90], [0.0, -0.0]]));
    for predicate in [
        Predicate::Covers,
        Predicate::CoveredBy,
        Predicate::Contains,
        Predicate::Within,
    ] {
        assert!(evaluate(&a, &b, predicate, &mut GeometryWork::default()).unwrap());
    }
    assert!(!evaluate(&a, &b, Predicate::Overlaps, &mut GeometryWork::default()).unwrap());
    let b = points(json!([[-180, 0], [1, 1]]));
    assert!(evaluate(&a, &b, Predicate::Overlaps, &mut GeometryWork::default()).unwrap());
    // A longitude beyond the chart names its direction one turn away.
    let west = geometry(
        json!({"type":"Polygon","coordinates":[[[-180,-1],[-179,-1],[-179,1],[-180,1],[-180,-1]]]}),
    );
    let mut work = GeometryWork::default();
    let west = PreparedTopology::new(&west, &mut work).unwrap();
    assert_eq!(west.relation([180.25, 0.], &mut work).unwrap(), Rel::Inside);
}

#[test]
fn named_topology_uses_interiors_boundaries_and_union() {
    let square =
        geometry(json!({"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}));
    let interior = geometry(json!({"type":"Point","coordinates":[1,1]}));
    let boundary = geometry(json!({"type":"Point","coordinates":[0,1]}));
    let line = geometry(json!({"type":"LineString","coordinates":[[-1,1],[3,1]]}));
    let crossing = geometry(json!({"type":"LineString","coordinates":[[1,0],[1,2]]}));
    let overlap =
        geometry(json!({"type":"Polygon","coordinates":[[[1,0],[3,0],[3,2],[1,2],[1,0]]]}));
    for (a, b, p, expected) in [
        (&square, &interior, Predicate::Contains, true),
        (&square, &boundary, Predicate::Contains, false),
        (&square, &boundary, Predicate::Touches, true),
        (&square, &square, Predicate::Contains, true),
        (&square, &square, Predicate::Touches, false),
        (&square, &square, Predicate::Equals, true),
        (&interior, &square, Predicate::Within, true),
        (&line, &square, Predicate::Crosses, true),
        (&line, &crossing, Predicate::Crosses, true),
        (&square, &overlap, Predicate::Overlaps, true),
        (&square, &square, Predicate::Overlaps, false),
    ] {
        assert_eq!(holds(a, b, p), expected, "{p:?}");
    }
    let adjacent = geometry(json!({"type":"GeometryCollection","geometries":[
        {"type":"Polygon","coordinates":[[[-2,-1],[0,-1],[0,1],[-2,1],[-2,-1]]]},
        {"type":"Polygon","coordinates":[[[0,-1],[2,-1],[2,1],[0,1],[0,-1]]]}
    ]}));
    let shared = geometry(json!({"type":"Point","coordinates":[0,0]}));
    assert!(holds(&adjacent, &shared, Predicate::Contains));
}

#[test]
fn named_topology_preserves_seam_and_pole_identity_and_work_refusals() {
    let west = geometry(
        json!({"type":"Polygon","coordinates":[[[-180,-2],[-178,-2],[-178,2],[-180,2],[-180,-2]]]}),
    );
    let seam = geometry(json!({"type":"Point","coordinates":[180,0]}));
    let line = geometry(json!({"type":"LineString","coordinates":[[0,80],[0,90]]}));
    let pole = geometry(json!({"type":"Point","coordinates":[120,90]}));
    for (a, b, p, expected) in [
        (&west, &seam, Predicate::Touches, true),
        (&west, &seam, Predicate::Contains, false),
        (&line, &pole, Predicate::Touches, true),
        (&line, &pole, Predicate::Contains, false),
    ] {
        assert_eq!(holds(a, b, p), expected);
        assert!(evaluate(a, b, p, &mut GeometryWork::with_limits(0, 64 << 20)).is_err());
    }
    // A polygon across the antimeridian holds the seam in its interior.
    let cut = geometry(json!({"type":"Polygon","coordinates":[
        [[170,-1],[-170,-1],[-170,1],[170,1],[170,-1]]
    ]}));
    assert!(holds(&cut, &seam, Predicate::Contains));
    let across = geometry(json!({"type":"LineString","coordinates":[[175,0],[180,0]]}));
    assert!(holds(&cut, &across, Predicate::Contains));
}

/// An edge is the great elliptic arc between its vertices: a long east-west
/// edge bulges poleward of its vertices' parallel.
#[test]
fn long_edges_bulge_poleward_of_their_parallel() {
    let band = geometry(
        json!({"type":"Polygon","coordinates":[[[0,60],[90,60],[90,70],[0,70],[0,60]]]}),
    );
    let point = |longitude: f64, latitude: f64| {
        geometry(json!({"type":"Point","coordinates":[longitude, latitude]}))
    };
    // At 45° the southern arc reaches about 67.8° and the northern 75.6°.
    for (latitude, inside) in [(61.0, false), (66.0, false), (69.0, true), (74.0, true), (77.0, false)] {
        assert_eq!(
            holds(&band, &point(45.0, latitude), Predicate::Contains),
            inside,
            "45, {latitude}"
        );
    }
    // A line along the 60th parallel's vertices meets the arc only at its ends.
    let parallel = geometry(json!({"type":"LineString","coordinates":[[0,60],[45,60],[90,60]]}));
    assert!(holds(&band, &parallel, Predicate::Touches));
    assert!(!holds(&band, &parallel, Predicate::Crosses));
    let meridian = geometry(json!({"type":"LineString","coordinates":[[45,65],[45,72]]}));
    assert!(holds(&meridian, &band, Predicate::Crosses));
}

/// Orientation fixes the interior: a clockwise ring bounds everything but
/// the small region it encircles, and a ring may reach a pole.
#[test]
fn orientation_fixes_complements_and_pole_vertices() {
    let point = |longitude: f64, latitude: f64| {
        geometry(json!({"type":"Point","coordinates":[longitude, latitude]}))
    };
    let square =
        geometry(json!({"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}));
    let rest =
        geometry(json!({"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}));
    for (p, in_square) in [
        (point(0.5, 0.5), true),
        (point(10.0, 10.0), false),
        (point(0.0, 90.0), false),
        (point(-170.0, -89.0), false),
    ] {
        assert_eq!(holds(&square, &p, Predicate::Contains), in_square);
        assert_eq!(holds(&rest, &p, Predicate::Contains), !in_square);
    }
    assert!(holds(&rest, &point(0.0, 0.5), Predicate::Touches));
    assert!(holds(&square, &rest, Predicate::Touches));
    assert!(!holds(&square, &rest, Predicate::Overlaps));
    let far = geometry(
        json!({"type":"Polygon","coordinates":[[[10,10],[11,10],[11,11],[10,11],[10,10]]]}),
    );
    assert!(holds(&rest, &far, Predicate::Contains));
    // Two of the pole's longitudes are one vertex: a triangle to the pole,
    // whose 80th-parallel edge bulges to about 82.9° halfway.
    let wedge = geometry(
        json!({"type":"Polygon","coordinates":[[[0,80],[90,80],[90,90],[0,90],[0,80]]]}),
    );
    assert!(holds(&wedge, &point(45.0, 84.0), Predicate::Contains));
    assert!(!holds(&wedge, &point(45.0, 81.0), Predicate::Intersects));
    assert!(holds(&wedge, &point(-123.0, 90.0), Predicate::Touches));
}

/// The same answers in a Cartesian plane, where arcs are segments.
#[test]
fn cartesian_frames_keep_straight_segments() {
    let mut space = crate::GeometrySpace::wgs84_xy();
    space.segments = crate::SegmentModel::CoordinateLinear;
    space.frame = CoordinateFrame::Cartesian {
        name: "local".into(),
        unit: crate::LinearUnit::metre(),
    };
    let planar = |value: serde_json::Value| {
        crate::admit_value(
            &crate::Gv::from(&value),
            &crate::GeometryDeclaration {
                space: space.clone(),
                size: crate::Size::Large,
            },
        )
        .unwrap()
    };
    // Clockwise rings are admitted in a plane; each bounds its finite side.
    let square = planar(
        json!({"type":"Polygon","coordinates":[[[0,60],[0,70],[90,70],[90,60],[0,60]]]}),
    );
    let point = |x: f64, y: f64| planar(json!({"type":"Point","coordinates":[x, y]}));
    assert!(holds(&square, &point(45.0, 61.0), Predicate::Contains));
    assert!(holds(&square, &point(45.0, 60.0), Predicate::Touches));
    assert!(!holds(&square, &point(45.0, 75.0), Predicate::Intersects));
    let huge = planar(json!({"type":"Polygon","coordinates":[[
        [-1e30,-1e30],[1e30,-1e30],[1e30,1e30],[-1e30,1e30],[-1e30,-1e30]
    ]]}));
    assert!(holds(&huge, &point(1e29, -1e29), Predicate::Contains));
    let tiny = planar(json!({"type":"LineString","coordinates":[[0,0],[2e-200,2e-200]]}));
    assert!(holds(&tiny, &point(1e-200, 1e-200), Predicate::Contains));
    assert!(!holds(&tiny, &point(1e-200, 2e-200), Predicate::Intersects));
}

#[test]
fn predicates_refine_only_edges_near_the_other_operand() {
    let pieces: Vec<_> = (0..128).map(|i| [[i, 0], [i, 1]]).collect();
    let pieces = geometry(json!({"type":"MultiLineString","coordinates":pieces}));
    let piece = geometry(json!({"type":"LineString","coordinates":[[3, 0.1], [3, 0.9]]}));
    for (predicate, expected) in [
        (Predicate::CoveredBy, true),
        (Predicate::Within, true),
        (Predicate::Intersects, true),
        (Predicate::Covers, false),
    ] {
        let mut work = GeometryWork::with_limits(200_000, 512 << 10);
        assert_eq!(
            evaluate(&piece, &pieces, predicate, &mut work).unwrap(),
            expected,
            "{predicate:?}"
        );
    }
}

#[test]
fn trajectory_steps_follow_arcs_and_report_rounded_crossings() {
    let band = geometry(
        json!({"type":"Polygon","coordinates":[[[0,60],[90,60],[90,70],[0,70],[0,60]]]}),
    );
    let mut work = GeometryWork::default();
    let band = PreparedTopology::new(&band, &mut work).unwrap();
    // Due north along the 45° meridian: inside between the two arcs.
    let intervals = band
        .trajectory_intervals([45.0, 50.0], [45.0, 80.0], &mut work)
        .unwrap();
    assert_eq!(intervals.len(), 1);
    let interval = &intervals[0];
    assert!(interval.from > 0.5 && interval.to < 0.9, "{interval:?}");
    assert!((interval.start[1] - 67.8).abs() < 0.2, "{interval:?}");
    assert!((interval.end[1] - 75.6).abs() < 0.2, "{interval:?}");
    assert!((interval.start[0] - 45.0).abs() < 1e-9, "{interval:?}");
    // A stationary fix inside.
    let still = band
        .trajectory_intervals([45.0, 72.0], [45.0, 72.0], &mut work)
        .unwrap();
    assert_eq!((still[0].from, still[0].to), (0.0, 1.0));
    assert!(band
        .trajectory_intervals([0.0, 0.0], [180.0, 0.0], &mut work)
        .is_err());
}
