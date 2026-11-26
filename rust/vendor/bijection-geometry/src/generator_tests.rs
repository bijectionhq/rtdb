use serde_json::json;

use super::invoke;

fn space() -> serde_json::Value {
    json!({"frame":{"kind":"wgs84"},"dimensions":{"kind":"xy"},"segments":"great_elliptic",
        "numerics":{"topology":"binary64_exact","distance_absolute_m":0.001,"distance_relative":1e-7}})
}

fn square(x: f64, y: f64) -> serde_json::Value {
    json!({"type":"Polygon","coordinates":[[[x,y],[x+1.,y],[x+1.,y+1.],[x,y+1.],[x,y]]]})
}

fn relate(a: serde_json::Value, b: serde_json::Value) -> serde_json::Value {
    invoke(json!({"operation":"relate","space":space(),
        "left":{"kind":"value","value":a},"right":{"kind":"value","value":b}}))["value"].clone()
}

#[test]
fn relate_answers_the_full_matrix() {
    assert_eq!(relate(square(0., 0.), square(0., 0.)), json!("2FFF1FFF2"));
    assert_eq!(relate(square(0., 0.), square(5., 5.)), json!("FF2FF1212"));
    assert_eq!(relate(square(0., 0.), json!({"type":"Point","coordinates":[0.5,0.5]})), json!("0F2FF1FF2"));
}

#[test]
fn a_split_at_the_antimeridian_is_admitted_and_relates_as_one_region() {
    let split = json!({"type":"MultiPolygon","coordinates":[
        [[[179.,0.],[180.,0.],[180.,1.],[179.,1.],[179.,0.]]],
        [[[-180.,0.],[-179.,0.],[-179.,1.],[-180.,1.],[-180.,0.]]]]});
    let across = json!({"type":"Point","coordinates":[180.,0.5]});
    // The cut meridian is interior to the union.
    assert_eq!(relate(split, across), json!("0F2FF1FF2"));
}

#[test]
fn inverse_and_direct_round_trip() {
    let inverse = invoke(json!({"operation":"inverse","space":space(),"from":[0.,0.],"to":[1.,0.]}))["value"].clone();
    let distance = inverse["distance_m"].as_f64().unwrap();
    assert!((distance - 111_319.490_793).abs() < 1e-3);
    assert_eq!(inverse["area_under_m2"], json!(0.0));
    let direct = invoke(json!({"operation":"direct","space":space(),"from":[0.,0.],"azimuth_degrees":90.,"distance_m":distance}))["value"].clone();
    assert!((direct[0].as_f64().unwrap() - 1.).abs() < 1e-9);
}

#[test]
fn cube_faces_see_their_own_side() {
    let answer = invoke(json!({"operation":"chart","space":space(),"chart":{"kind":"cube","face":0},
        "direction":"forward","positions":[[0.,0.],[180.,0.]]}))["value"].clone();
    assert_eq!(answer, json!([[0.0,0.0], null]));
}

#[test]
fn sep_answers_the_separation_and_a_witness() {
    let point = json!({"type":"Point","coordinates":[0.5,0.5]});
    let inside = invoke(json!({"operation":"sep","space":space(),
        "left":{"kind":"value","value":point},"right":{"kind":"value","value":square(0.,0.)}}))["value"].clone();
    assert_eq!(inside["distance_m"], json!(0.0));
    let boundary = invoke(json!({"operation":"sep","space":space(),
        "left":{"kind":"value","value":point},"right":{"kind":"boundary","of":{"kind":"value","value":square(0.,0.)}}}))["value"].clone();
    let distance = boundary["distance_m"].as_f64().unwrap();
    // Half a degree from the nearest side, about 55 km.
    assert!((55_000.0..56_000.0).contains(&distance), "{distance}");
    let witness = boundary["witness"][1].as_array().unwrap();
    let side = [witness[0].as_f64().unwrap(), witness[1].as_f64().unwrap()];
    assert!(side.iter().any(|x| (x - 0.0).abs() < 1e-6 || (x - 1.0).abs() < 1e-6), "{side:?}");
}

fn realize(term: serde_json::Value, max: f64, side: &str) -> serde_json::Value {
    invoke(json!({"operation":"realize","space":space(),"term":term,"size":"standard","max_deviation_m":max,"side":side}))
}

fn value(v: serde_json::Value) -> serde_json::Value {
    json!({"kind":"value","value":v})
}

#[test]
fn realize_answers_values_unions_and_boundaries() {
    // A value is itself, with no new coordinate.
    let answer = realize(value(square(0., 0.)), 0.0, "outer");
    assert_eq!(answer["value"]["value"]["type"], json!("Polygon"), "{answer}");
    assert_eq!(answer["value"]["deviation_m"], json!(0.0));
    assert_eq!(answer["value"]["side"], json!("outer"));
    // A union is the collection of its operands.
    let union = realize(json!({"kind":"union","left":value(square(0., 0.)),"right":value(square(5., 5.))}), 0.0, "inner");
    assert_eq!(union["value"]["value"]["type"], json!("GeometryCollection"), "{union}");
    assert_eq!(union["value"]["value"]["geometries"].as_array().map(Vec::len), Some(2));
    // A square's boundary is its ring, as it is.
    let ring = realize(json!({"kind":"boundary","of":value(square(0., 0.))}), 0.0, "nearest");
    assert_eq!(ring["value"]["value"]["type"], json!("MultiLineString"), "{ring}");
    assert_eq!(ring["value"]["deviation_m"], json!(0.0));
    assert_eq!(relate(ring["value"]["value"].clone(), square(0., 0.)), json!("F1FFFF2F2"));
    // A line's boundary is its ends.
    let line = json!({"type":"LineString","coordinates":[[0.,0.],[1.,1.],[2.,0.]]});
    let ends = realize(json!({"kind":"boundary","of":value(line)}), 0.0, "nearest");
    assert_eq!(ends["value"]["value"], json!({"type":"MultiPoint","coordinates":[[0,0],[2,0]]}));
    // Two overlapping squares: their union's boundary crosses at rounded
    // points, reported, and held to the nearest side only.
    let overlap = json!({"kind":"boundary","of":{"kind":"union","left":value(square(0., 0.)),"right":value(square(0.5, 0.5))}});
    let rounded = realize(overlap.clone(), 1.0, "nearest");
    let deviation = rounded["value"]["deviation_m"].as_f64().expect("a deviation");
    assert!(deviation < 1e-6, "{rounded}");
    if deviation > 0.0 {
        assert!(realize(overlap.clone(), 1.0, "outer").get("refusal").is_some());
        assert!(realize(overlap, 0.0, "nearest").get("refusal").is_some());
    }
}

fn planar() -> serde_json::Value {
    json!({"frame":{"kind":"cartesian","name":"site","unit":{"metres_per_unit":1.0}},"dimensions":{"kind":"xy"},
        "segments":"coordinate_linear","numerics":{"topology":"binary64_exact","distance_absolute_m":0.001,"distance_relative":1e-7}})
}

fn realize_in(space: serde_json::Value, term: serde_json::Value, max: f64, side: &str) -> serde_json::Value {
    invoke(json!({"operation":"realize","space":space,"term":term,"size":"standard","max_deviation_m":max,"side":side}))
}

fn relate_in(space: serde_json::Value, a: serde_json::Value, b: serde_json::Value) -> serde_json::Value {
    invoke(json!({"operation":"relate","space":space,"left":value(a),"right":value(b)}))["value"].clone()
}

fn square_of(x: f64, y: f64, side: f64) -> serde_json::Value {
    json!({"type":"Polygon","coordinates":[[[x,y],[x+side,y],[x+side,y+side],[x,y+side],[x,y]]]})
}

#[test]
fn realize_intersects_and_subtracts_through_the_gated_overlay() {
    let (a, b) = (square_of(0., 0., 2.), square_of(1., 1., 2.));
    // Planar: the crossings are exact binary64 points, so nothing rounds.
    let meet = realize_in(planar(), json!({"kind":"intersection","left":value(a.clone()),"right":value(b.clone())}), 0.0, "outer");
    assert_eq!(meet["value"]["deviation_m"], json!(0.0), "{meet}");
    assert_eq!(relate_in(planar(), meet["value"]["value"].clone(), square_of(1., 1., 1.)), json!("2FFF1FFF2"), "{meet}");
    let less = realize_in(planar(), json!({"kind":"difference","left":value(a.clone()),"right":value(b.clone())}), 0.0, "nearest");
    let expected = json!({"type":"Polygon","coordinates":[[[0.,0.],[2.,0.],[2.,1.],[1.,1.],[1.,2.],[0.,2.],[0.,0.]]]});
    assert_eq!(relate_in(planar(), less["value"]["value"].clone(), expected), json!("2FFF1FFF2"), "{less}");
    // A hole: a square less one inside it.
    let holed = realize_in(planar(), json!({"kind":"difference","left":value(square_of(0., 0., 4.)),"right":value(square_of(1., 1., 1.))}), 0.0, "inner");
    assert_eq!(holed["value"]["value"]["coordinates"].as_array().map(Vec::len), Some(2), "{holed}");
    assert_eq!(holed["value"]["deviation_m"], json!(0.0));
    // Disjoint operands meet in nothing; squares sharing an edge meet in it.
    let none = realize_in(planar(), json!({"kind":"intersection","left":value(square_of(0., 0., 1.)),"right":value(square_of(5., 5., 1.))}), 0.0, "nearest");
    assert_eq!(none["value"]["value"], json!({"type":"GeometryCollection","geometries":[]}), "{none}");
    let edge = realize_in(planar(), json!({"kind":"intersection","left":value(square_of(0., 0., 1.)),"right":value(square_of(1., 0., 1.))}), 0.0, "nearest");
    assert_eq!(edge["value"]["value"]["type"], json!("LineString"), "{edge}");
    let corner = realize_in(planar(), json!({"kind":"intersection","left":value(square_of(0., 0., 1.)),"right":value(square_of(1., 1., 1.))}), 0.0, "nearest");
    assert_eq!(corner["value"]["value"], json!({"type":"Point","coordinates":[1,1]}), "{corner}");
    // A line clipped to an area.
    let line = json!({"type":"LineString","coordinates":[[-1.,0.5],[3.,0.5]]});
    let clipped = realize_in(planar(), json!({"kind":"intersection","left":value(line),"right":value(square_of(0., 0., 1.))}), 0.0, "nearest");
    assert_eq!(clipped["value"]["value"], json!({"type":"LineString","coordinates":[[0,0.5],[1,0.5]]}), "{clipped}");
    // WGS84: crossings of great elliptic arcs round, and report it.
    let geo = realize(json!({"kind":"intersection","left":value(square(0., 0.)),"right":value(square(0.5, 0.5))}), 1.0, "nearest");
    let deviation = geo["value"]["deviation_m"].as_f64().expect("a deviation");
    assert!(deviation < 1e-6, "{geo}");
    assert_eq!(relate(geo["value"]["value"].clone(), square(0., 0.)).as_str().map(|m| &m[..1]), Some("2"), "{geo}");
    // A nested square needs no new coordinate, on WGS84 too.
    let nested = realize(json!({"kind":"intersection","left":value(square_of(0., 0., 3.)),"right":value(square(1., 1.))}), 0.0, "outer");
    assert_eq!(relate(nested["value"]["value"].clone(), square(1., 1.)), json!("2FFF1FFF2"), "{nested}");
    // relate reads a term that realizes exactly.
    let term = invoke(json!({"operation":"relate","space":planar(),
        "left":{"kind":"intersection","left":value(a),"right":value(b)},"right":value(square_of(1., 1., 1.))}))["value"].clone();
    assert_eq!(term, json!("2FFF1FFF2"));
}

#[test]
fn realize_buffers_on_the_side_asked() {
    let point = json!({"type":"Point","coordinates":[10.,20.]});
    let term = json!({"kind":"buffer","of":value(point.clone()),"metres":1000.0});
    for side in ["outer", "inner", "nearest"] {
        let answer = realize(term.clone(), 1.0, side);
        let deviation = answer["value"]["deviation_m"].as_f64().unwrap_or_else(|| panic!("{answer}"));
        assert!(deviation > 0.0 && deviation <= 1.0, "{answer}");
        assert_eq!(answer["value"]["side"], json!(side));
        // Every vertex is within the bound of the circle, on its side.
        for p in answer["value"]["value"]["coordinates"][0].as_array().expect("a ring") {
            let d = crate::predicates::point_distance([10., 20.], [p[0].as_f64().unwrap(), p[1].as_f64().unwrap()]);
            match side {
                "outer" => assert!((1000.0..=1001.0).contains(&d), "{d}"),
                "inner" => assert!((999.0..=1000.0).contains(&d), "{d}"),
                _ => assert!((999.0..=1001.0).contains(&d), "{d}"),
            }
        }
    }
    // The outer buffer covers the point and a position 999 m east of it.
    let outer = realize(term.clone(), 1.0, "outer")["value"]["value"].clone();
    let east = crate::geodesy::direct(20., 10., 90., 999.9);
    assert_eq!(relate(outer.clone(), json!({"type":"Point","coordinates":[east.lon2, east.lat2]})).as_str().map(|m| &m[..1]), Some("0"));
    // A zero bound cannot hold a curve.
    let refused = realize(term, 0.0, "outer");
    assert_eq!(refused["refusal"]["code"], json!("SpatialPrecisionLoss"), "{refused}");
    // A line's buffer and a polygon's buffer dissolve into one polygon.
    let line = json!({"type":"LineString","coordinates":[[0.,0.],[0.01,0.],[0.01,0.01]]});
    let lined = realize(json!({"kind":"buffer","of":value(line),"metres":100.0}), 0.5, "outer");
    assert_eq!(lined["value"]["value"]["type"], json!("Polygon"), "{lined}");
    let planar_square = realize_in(planar(), json!({"kind":"buffer","of":value(square_of(0., 0., 10.)),"metres":1.0}), 0.01, "outer");
    assert_eq!(planar_square["value"]["value"]["type"], json!("Polygon"), "{planar_square}");
    assert_eq!(relate_in(planar(), planar_square["value"]["value"].clone(), json!({"type":"Polygon","coordinates":[[[-0.99,0.],[10.99,0.],[10.99,10.],[-0.99,10.],[-0.99,0.]]]})).as_str().map(|m| &m[6..8]), Some("FF"), "{planar_square}");
    // A ring's buffer keeps its hole.
    let ring = json!({"type":"LineString","coordinates":[[0.,0.],[10.,0.],[10.,10.],[0.,10.],[0.,0.]]});
    let annulus = realize_in(planar(), json!({"kind":"buffer","of":value(ring),"metres":1.0}), 0.01, "inner");
    assert_eq!(annulus["value"]["value"]["coordinates"].as_array().map(Vec::len), Some(2), "{annulus}");
}

#[test]
fn realize_recasts_through_a_chart() {
    let utm = json!({"frame":{"kind":"cartesian","name":"utm-31n","unit":{"metres_per_unit":1.0}},"dimensions":{"kind":"xy"},
        "segments":"coordinate_linear","numerics":{"topology":"binary64_exact","distance_absolute_m":0.001,"distance_relative":1e-7}});
    let chart = json!({"kind":"utm","zone":31,"hemisphere":"north"});
    let term = json!({"kind":"recast","of":value(square(2., 45.)),"target":utm,"via":chart});
    let answer = realize(term.clone(), 0.5, "nearest");
    let deviation = answer["value"]["deviation_m"].as_f64().unwrap_or_else(|| panic!("{answer}"));
    assert!(deviation > 0.0 && deviation <= 0.5, "{answer}");
    // Edges curve in UTM, so the ring gains vertices.
    let ring = answer["value"]["value"]["coordinates"][0].as_array().expect("a ring").len();
    assert!(ring > 5, "{answer}");
    // A tighter bound needs more vertices; a zero bound refuses.
    let tight = realize(term.clone(), 0.01, "nearest");
    assert!(tight["value"]["value"]["coordinates"][0].as_array().expect("a ring").len() > ring, "{tight}");
    assert!(realize(term, 0.0, "nearest").get("refusal").is_some());
    // Without a chart, a recast into its own space is the value.
    let same = realize(json!({"kind":"recast","of":value(square(0., 0.)),"target":space(),"via":null}), 0.0, "outer");
    assert_eq!(same["value"]["deviation_m"], json!(0.0), "{same}");
    // Densifying into the frame's own edge model is the value; another is refused.
    let dense = realize(json!({"kind":"densify","of":value(square(0., 0.)),"segments":"great_elliptic"}), 0.0, "outer");
    assert_eq!(dense["value"]["deviation_m"], json!(0.0), "{dense}");
    assert!(realize(json!({"kind":"densify","of":value(square(0., 0.)),"segments":"coordinate_linear"}), 1.0, "nearest").get("refusal").is_some());
}

fn sphere() -> serde_json::Value {
    json!({"frame":{"kind":"wgs84_sphere","radius_m":6_371_008.8},"dimensions":{"kind":"xy"},"segments":"great_circle",
        "numerics":{"topology":"binary64_exact","distance_absolute_m":0.001,"distance_relative":1e-7}})
}

#[test]
fn a_sphere_space_measures_on_its_sphere_and_relates_as_wgs84() {
    let r = 6_371_008.8_f64;
    // Topology: the great circle is WGS84's great elliptic curve.
    assert_eq!(relate_in(sphere(), square(0., 0.), square(0.5, 0.5)), relate(square(0., 0.), square(0.5, 0.5)));
    // inverse and direct are the sphere's.
    let inverse = invoke(json!({"operation":"inverse","space":sphere(),"from":[0.,0.],"to":[1.,0.]}))["value"].clone();
    let distance = inverse["distance_m"].as_f64().unwrap_or_else(|| panic!("{inverse}"));
    assert!((distance - r * 1f64.to_radians()).abs() < 1e-6, "{inverse}");
    assert!((inverse["azimuth_from_degrees"].as_f64().unwrap() - 90.).abs() < 1e-9);
    assert_eq!(inverse["area_under_m2"], json!(0.0));
    let up = invoke(json!({"operation":"inverse","space":sphere(),"from":[0.,0.],"to":[90.,90.]}))["value"].clone();
    // From the equator to the pole: a quarter meridian, and the area under
    // it is that of the zone up to the pole along no longitude.
    assert!((up["distance_m"].as_f64().unwrap() - r * std::f64::consts::FRAC_PI_2).abs() < 1e-6, "{up}");
    let direct = invoke(json!({"operation":"direct","space":sphere(),"from":[0.,0.],"azimuth_degrees":90.,"distance_m":distance}))["value"].clone();
    assert!((direct[0].as_f64().unwrap() - 1.).abs() < 1e-12 && direct[1].as_f64().unwrap().abs() < 1e-12, "{direct}");
    // Areas are spherical excess: an octant is an eighth of the sphere.
    let space: crate::GeometrySpace = serde_json::from_value(sphere()).unwrap();
    let octant = crate::admit_geometry(
        &crate::Gv::from(&json!({"type":"Polygon","coordinates":[[[0.,0.],[90.,0.],[0.,90.],[0.,0.]]]})),
        &space,
        &crate::STANDARD,
    )
    .unwrap();
    let area = crate::measure::measure(&octant, crate::measure::Measurement::Area, &mut crate::GeometryWork::default()).unwrap();
    assert!((area / (4.0 * std::f64::consts::PI * r * r / 8.0) - 1.0).abs() < 1e-12, "{area}");
    let perimeter = crate::measure::measure(&octant, crate::measure::Measurement::Perimeter, &mut crate::GeometryWork::default()).unwrap();
    assert!((perimeter - 3.0 * r * std::f64::consts::FRAC_PI_2).abs() < 1e-6, "{perimeter}");
    // sep reads the sphere's metric.
    let apart = invoke(json!({"operation":"sep","space":sphere(),
        "left":value(json!({"type":"Point","coordinates":[0.,0.]})),"right":value(json!({"type":"Point","coordinates":[0.,1.]}))}))["value"].clone();
    assert!((apart["distance_m"].as_f64().unwrap() - r * 1f64.to_radians()).abs() < 1e-6, "{apart}");
    assert_eq!(apart["deviation_m"], json!(0.0));
    // A point to an edge's middle: the arc's nearest point, not a vertex.
    let off = invoke(json!({"operation":"sep","space":sphere(),
        "left":value(json!({"type":"Point","coordinates":[0.5,1.]})),
        "right":value(json!({"type":"LineString","coordinates":[[0.,0.],[1.,0.]]}))}))["value"].clone();
    assert!((off["distance_m"].as_f64().unwrap() - r * 1f64.to_radians()).abs() < 1e-3, "{off}");
    // A buffer is the sphere's disc.
    let disc = realize_in(sphere(), json!({"kind":"buffer","of":value(json!({"type":"Point","coordinates":[10.,20.]})),"metres":1000.0}), 1.0, "outer");
    for p in disc["value"]["value"]["coordinates"][0].as_array().unwrap_or_else(|| panic!("{disc}")) {
        let d = r * crate::sphere::angle([10., 20.], [p[0].as_f64().unwrap(), p[1].as_f64().unwrap()]);
        assert!((1000.0..=1001.0).contains(&d), "{d}");
    }
}

#[test]
fn recast_joins_wgs84_and_its_sphere_exactly() {
    let term = json!({"kind":"recast","of":value(square(0., 0.)),"target":sphere(),"via":null});
    let answer = realize(term, 0.0, "outer");
    assert_eq!(answer["value"]["deviation_m"], json!(0.0), "{answer}");
    assert_eq!(relate_in(sphere(), answer["value"]["value"].clone(), square(0., 0.)), json!("2FFF1FFF2"), "{answer}");
    // And back, into WGS84 from the sphere.
    let back = realize_in(sphere(), json!({"kind":"recast","of":value(square(0., 0.)),"target":space(),"via":null}), 0.0, "inner");
    assert_eq!(back["value"]["deviation_m"], json!(0.0), "{back}");
    // A change of edge model is a change of frame: densify refuses it.
    let dense = realize(json!({"kind":"densify","of":value(square(0., 0.)),"segments":"great_circle"}), 1.0, "nearest");
    assert_eq!(dense["refusal"]["code"], json!("UnsupportedCoordinateSpace"), "{dense}");
    let own = realize_in(sphere(), json!({"kind":"densify","of":value(square(0., 0.)),"segments":"great_circle"}), 0.0, "outer");
    assert_eq!(own["value"]["deviation_m"], json!(0.0), "{own}");
    // A sphere value reaches a projected frame through a chart.
    let utm = json!({"frame":{"kind":"cartesian","name":"utm-31n","unit":{"metres_per_unit":1.0}},"dimensions":{"kind":"xy"},
        "segments":"coordinate_linear","numerics":{"topology":"binary64_exact","distance_absolute_m":0.001,"distance_relative":1e-7}});
    let projected = realize_in(sphere(), json!({"kind":"recast","of":value(square(2., 45.)),"target":utm,"via":{"kind":"utm","zone":31,"hemisphere":"north"}}), 0.5, "nearest");
    assert!(projected["value"]["deviation_m"].as_f64().is_some_and(|d| d > 0.0 && d <= 0.5), "{projected}");
}

#[test]
fn relate_and_sep_read_terms_that_create_coordinates() {
    let overlap = json!({"kind":"intersection","left":value(square(0., 0.)),"right":value(square(0.5, 0.5))});
    let realized = realize(overlap.clone(), 1.0, "nearest");
    let deviation = realized["value"]["deviation_m"].as_f64().unwrap_or_else(|| panic!("{realized}"));
    let relate_term = |term: &serde_json::Value, other: serde_json::Value| {
        invoke(json!({"operation":"relate","space":space(),"left":term,"right":value(other)}))
    };
    // Far from the crossings, the realization's matrix is the term's.
    let inside = relate_term(&overlap, json!({"type":"Point","coordinates":[0.75,0.75]}));
    assert_eq!(inside["value"], json!("0F2FF1FF2"), "{inside}");
    let far = relate_term(&overlap, json!({"type":"Point","coordinates":[5.,5.]}));
    assert_eq!(far["value"], json!("FF2FF10F2"), "{far}");
    let neighbour = relate_term(&overlap, square(3., 3.));
    assert_eq!(neighbour["value"], json!("FF2FF1212"), "{neighbour}");
    if deviation > 0.0 {
        // At a crossing, the rounding could change the answer: refused.
        let crossing = realized["value"]["value"]["coordinates"][0]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p[0] == json!(1.0) && p[1] != json!(1.0) || p[1] == json!(1.0) && p[0] != json!(1.0))
            .cloned();
        if let Some(c) = crossing {
            let near = relate_term(&overlap, json!({"type":"Point","coordinates":c}));
            assert!(near.get("refusal").is_some(), "{near}");
        }
    }
    // sep reads the realization and reports its bound.
    let apart = invoke(json!({"operation":"sep","space":space(),"left":overlap,"right":value(json!({"type":"Point","coordinates":[3.,0.75]}))}))["value"].clone();
    assert_eq!(apart["deviation_m"].as_f64(), Some(deviation), "{apart}");
    assert!(apart["distance_m"].as_f64().is_some_and(|d| d > 200_000.0), "{apart}");
    // A buffer: decided away from its boundary, refused next to it.
    let point = json!({"type":"Point","coordinates":[10.,20.]});
    let buffer = json!({"kind":"buffer","of":value(point),"metres":1000.0});
    let east = |metres: f64| {
        let to = crate::geodesy::direct(20., 10., 90., metres);
        json!({"type":"Point","coordinates":[to.lon2, to.lat2]})
    };
    assert_eq!(relate_term(&buffer, east(500.))["value"], json!("0F2FF1FF2"));
    assert_eq!(relate_term(&buffer, east(1500.))["value"], json!("FF2FF10F2"));
    assert!(relate_term(&buffer, east(1000.)).get("refusal").is_some());
    let reach = invoke(json!({"operation":"sep","space":space(),"left":buffer,"right":value(east(1500.))}))["value"].clone();
    assert!(reach["distance_m"].as_f64().is_some_and(|d| (d - 500.0).abs() < 5e-3), "{reach}");
    assert!(reach["deviation_m"].as_f64().is_some_and(|d| d > 0.0 && d <= 4e-3), "{reach}");
}

#[test]
fn overlays_turn_around_a_pole() {
    // A cap north of 80°, cut along the antimeridian, and one north of 85°.
    let cap = |lat: f64| {
        json!({"type":"Polygon","coordinates":[[[-180.,lat],[-90.,lat],[0.,lat],[90.,lat],[180.,lat],[180.,90.],[-180.,90.],[-180.,lat]]]})
    };
    let notch = json!({"type":"Polygon","coordinates":[[[-10.,70.],[10.,70.],[10.,85.],[-10.,85.],[-10.,70.]]]});
    // The cap less a notch: one ring around the pole.
    let less = realize(json!({"kind":"difference","left":value(cap(80.)),"right":value(notch.clone())}), 1.0, "nearest");
    let ring = less["value"]["value"].clone();
    assert_eq!(ring["type"], json!("Polygon"), "{less}");
    let at = |lon: f64, lat: f64| json!({"type":"Point","coordinates":[lon, lat]});
    assert_eq!(relate(ring.clone(), at(100., 85.)), json!("0F2FF1FF2"), "{ring}");
    assert_eq!(relate(ring.clone(), at(0., 90.)), json!("0F2FF1FF2"), "{ring}");
    assert_eq!(relate(ring.clone(), at(0., 82.)), json!("FF2FF10F2"), "{ring}");
    // Nested caps: the inner one, with no new coordinate, and an annulus.
    let inner = realize(json!({"kind":"intersection","left":value(cap(80.)),"right":value(cap(85.))}), 0.0, "outer");
    assert_eq!(relate(inner["value"]["value"].clone(), cap(85.)), json!("2FFF1FFF2"), "{inner}");
    let annulus = realize(json!({"kind":"difference","left":value(cap(80.)),"right":value(cap(85.))}), 0.0, "inner");
    let band = annulus["value"]["value"].clone();
    assert_eq!(band["coordinates"].as_array().map(Vec::len), Some(2), "{annulus}");
    assert_eq!(relate(band.clone(), at(0., 82.)), json!("0F2FF1FF2"), "{band}");
    assert_eq!(relate(band, at(0., 88.)), json!("FF2FF10F2"));
    // A ring that turns at the pole between two longitudes.
    let lens = json!({"type":"Polygon","coordinates":[[[-90.,80.],[0.,80.],[90.,80.],[90.,90.],[-90.,90.],[-90.,80.]]]});
    let clipped = realize(json!({"kind":"intersection","left":value(lens),"right":value(notch)}), 1.0, "nearest");
    assert!(clipped.get("value").is_some(), "{clipped}");
    assert_eq!(relate(clipped["value"]["value"].clone(), at(0., 82.)).as_str().map(|m| &m[..1]), Some("0"), "{clipped}");
    // A buffer around the pole.
    let polar = realize(json!({"kind":"buffer","of":value(at(0., 89.99)),"metres":5000.0}), 1.0, "outer");
    assert_eq!(relate(polar["value"]["value"].clone(), at(0., 90.)), json!("0F2FF1FF2"), "{polar}");
}
