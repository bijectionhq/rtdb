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
    // What needs an overlay, an offset or a reprojection is refused.
    let meet = realize(json!({"kind":"intersection","left":value(square(0., 0.)),"right":value(square(0.5, 0.5))}), 1.0, "nearest");
    assert!(meet.get("refusal").is_some());
}
