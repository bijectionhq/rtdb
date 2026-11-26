use serde_json::{
    json,
    Value,
};

use anyhow::Context;

use super::*;
use crate::{
    measure::{
        measure,
        Measurement,
    },
    GeometrySpace,
    Size,
};

const TOLERANCE_M: f64 = 1.0;

fn declaration() -> GeometryDeclaration {
    GeometryDeclaration {
        space: GeometrySpace::wgs84_xy(),
        size: Size::Large,
    }
}

fn import(value: Value) -> Result<Geometry, GeometryRefusal> {
    from_planar(
        &Gv::from(&value),
        &declaration(),
        TOLERANCE_M,
        &mut GeometryWork::default(),
    )
}

fn native(value: Value) -> Result<Geometry, GeometryRefusal> {
    crate::admit_value(&Gv::from(&value), &declaration())
}

fn export(geometry: &Geometry) -> Result<Value, GeometryRefusal> {
    let value = to_planar(geometry, TOLERANCE_M, &mut GeometryWork::default())?;
    let mut text = String::new();
    crate::js_json(&value, &mut text)?;
    Ok(serde_json::from_str(&text).expect("planar JSON"))
}

fn covers(geometry: &Geometry, p: Pos) -> Result<bool, GeometryRefusal> {
    let point = Geometry::new(Kind::Point, Coords::Point(p), geometry.space.clone());
    crate::predicates::covered_by(&point, geometry, &mut GeometryWork::default())
}

fn area(geometry: &Geometry) -> f64 {
    measure(geometry, Measurement::Area, &mut GeometryWork::default()).expect("area")
}

/// The planar polygons of an exported value.
fn planar_polygons(value: &Value) -> Vec<Vec<Vec<Pos>>> {
    let ring = |ring: &Value| -> Vec<Pos> {
        ring.as_array()
            .expect("ring")
            .iter()
            .map(|p| [p[0].as_f64().expect("lon"), p[1].as_f64().expect("lat")])
            .collect()
    };
    let polygon = |polygon: &Value| -> Vec<Vec<Pos>> {
        polygon
            .as_array()
            .expect("polygon")
            .iter()
            .map(ring)
            .collect()
    };
    match value["type"].as_str() {
        Some("Polygon") => vec![polygon(&value["coordinates"])],
        Some("MultiPolygon") => value["coordinates"]
            .as_array()
            .expect("parts")
            .iter()
            .map(polygon)
            .collect(),
        other => panic!("not an area: {other:?}"),
    }
}

fn planar_covers(polygons: &[Vec<Vec<Pos>>], p: Pos) -> bool {
    polygons.iter().any(|polygon| {
        planar_contains(&polygon[0], p) && !polygon[1..].iter().any(|hole| planar_contains(hole, p))
    })
}

#[test]
fn import_reads_either_winding_as_the_bounded_region() -> anyhow::Result<()> {
    let counterclockwise =
        json!({"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]});
    let clockwise = json!({"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]});
    for value in [counterclockwise, clockwise] {
        let geometry = import(value)?;
        assert!(covers(&geometry, [0.5, 0.5])?);
        assert!(!covers(&geometry, [10.0, 10.0])?);
        assert!(area(&geometry) < 2e10);
    }
    // Natively the clockwise ring bounds everything outside it.
    let complement =
        native(json!({"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}))?;
    assert!(covers(&complement, [10.0, 10.0])?);
    Ok(())
}

#[test]
fn import_densifies_parallels_within_the_slack() -> anyhow::Result<()> {
    let geometry =
        import(json!({"type":"Polygon","coordinates":[[[0,60],[10,60],[10,61],[0,61],[0,60]]]}))?;
    let Coords::Lines(rings) = &geometry.coords else {
        panic!("a polygon");
    };
    assert!(rings[0].len() > 40, "{}", rings[0].len());
    // Every arc between consecutive vertices stays within the tolerance of
    // the parallel it replaces.
    for pair in rings[0].windows(2) {
        if pair[0][1] == pair[1][1] {
            assert!(fits(pair[0], pair[1], TOLERANCE_M));
        }
    }
    // A point just inside the parallel stays inside; natively the arc cuts
    // below it.
    assert!(covers(&geometry, [5.0, 60.0001])?);
    let arcs =
        native(json!({"type":"Polygon","coordinates":[[[0,60],[10,60],[10,61],[0,61],[0,60]]]}))?;
    assert!(!covers(&arcs, [5.0, 60.0001])?);
    Ok(())
}

#[test]
fn import_joins_parts_cut_at_the_antimeridian() -> anyhow::Result<()> {
    let geometry = import(json!({"type":"MultiPolygon","coordinates":[
        [[[170,-10],[180,-10],[180,0],[180,10],[170,10],[170,-10]]],
        [[[-180,-10],[-170,-10],[-170,10],[-180,10],[-180,-10]]]
    ]}))?;
    assert_eq!(geometry.kind, Kind::MultiPolygon);
    let Coords::Polygons(parts) = &geometry.coords else {
        panic!("parts");
    };
    assert_eq!(parts.len(), 1);
    for p in [[180.0, 0.0], [175.0, 5.0], [-175.0, -5.0], [180.0, 9.0]] {
        assert!(covers(&geometry, p)?, "{p:?}");
    }
    assert!(!covers(&geometry, [0.0, 0.0])?);
    // Natively two parts split at the antimeridian (+180 in one, -180 in
    // the other) are admitted as they are, and their seam is interior.
    let split = native(json!({"type":"MultiPolygon","coordinates":[
        [[[170,-10],[180,-10],[180,10],[170,10],[170,-10]]],
        [[[-180,-10],[-170,-10],[-170,10],[-180,10],[-180,-10]]]
    ]}))?;
    for p in [[180.0, 0.0], [175.0, 5.0], [-175.0, -5.0]] {
        assert!(covers(&split, p)?, "{p:?}");
    }
    // Two parts on one side of the seam still may not share it.
    assert!(native(json!({"type":"MultiPolygon","coordinates":[
        [[[170,-10],[180,-10],[180,10],[170,10],[170,-10]]],
        [[[175,-10],[180,-10],[180,10],[175,10],[175,-10]]]
    ]}))
    .is_err());
    Ok(())
}

#[test]
fn import_closes_a_polar_ring_along_the_cut_and_the_pole() -> anyhow::Result<()> {
    // Antarctica as files write it: the coast closed down the antimeridian,
    // along the pole's line and back up, in either winding.
    let coast: Vec<Value> = (0..=36)
        .map(|k| json!([180 - 10 * k, -70 - (k % 3)]))
        .collect();
    let mut ring = coast;
    ring.extend([json!([-180, -90]), json!([180, -90]), json!([180, -70])]);
    for ring in [ring.clone(), ring.into_iter().rev().collect()] {
        let geometry = import(json!({"type":"Polygon","coordinates":[ring]}))?;
        assert!(covers(&geometry, [0.0, -90.0])?);
        assert!(covers(&geometry, [45.0, -80.0])?);
        assert!(!covers(&geometry, [0.0, 0.0])?);
        assert!(!covers(&geometry, [0.0, 90.0])?);
    }
    Ok(())
}

#[test]
fn import_keeps_a_polar_line_short_of_the_pole_as_a_hole() -> anyhow::Result<()> {
    // Natural Earth closes Antarctica along -89.998926, not the pole: in the
    // plane that leaves a sliver south of it outside the region.
    let mut ring: Vec<Value> = (0..=36)
        .map(|k| json!([180 - 10 * k, -70 - (k % 3)]))
        .collect();
    ring.extend([
        json!([-180, -89.998926]),
        json!([180, -89.998926]),
        json!([180, -70]),
    ]);
    let geometry = import(json!({"type":"Polygon","coordinates":[ring]}))?;
    let Coords::Lines(rings) = &geometry.coords else {
        panic!("one polygon: {geometry:?}");
    };
    assert_eq!(rings.len(), 2);
    assert!(covers(&geometry, [45.0, -80.0])?);
    assert!(covers(&geometry, [45.0, -89.99])?);
    assert!(!covers(&geometry, [0.0, -90.0])?);
    assert!(!covers(&geometry, [0.0, 0.0])?);
    Ok(())
}

#[test]
fn export_cuts_areas_at_the_antimeridian() -> anyhow::Result<()> {
    let geometry = native(json!({"type":"Polygon","coordinates":[
        [[170,-10],[-170,-10],[-170,10],[170,10],[170,-10]]
    ]}))?;
    let value = export(&geometry)?;
    assert_eq!(value["type"], "MultiPolygon");
    let polygons = planar_polygons(&value);
    assert_eq!(polygons.len(), 2);
    for polygon in &polygons {
        for p in &polygon[0] {
            assert!(p[0] >= 170.0 || p[0] <= -170.0, "{p:?}");
        }
    }
    assert!(planar_covers(&polygons, [175.0, 0.0]));
    assert!(planar_covers(&polygons, [-175.0, 0.0]));
    assert!(!planar_covers(&polygons, [0.0, 0.0]));
    // Importing the export gives back one region.
    let back = import(value)?;
    assert!(covers(&back, [180.0, 0.0])?);
    assert!((area(&back) / area(&geometry) - 1.0).abs() < 1e-6);
    Ok(())
}

#[test]
fn export_draws_a_polar_cap_along_the_frame() -> anyhow::Result<()> {
    // A ring at 80 degrees north, counterclockwise about the pole.
    let mut ring: Vec<Value> = (0..12).map(|k| json!([-180 + 30 * k, 80])).collect();
    ring.push(ring[0].clone());
    let geometry = native(json!({"type":"Polygon","coordinates":[ring]})).context("native")?;
    assert!(covers(&geometry, [0.0, 90.0])?);
    let value = export(&geometry).context("export")?;
    let polygons = planar_polygons(&value);
    assert_eq!(polygons.len(), 1);
    assert!(planar_covers(&polygons, [0.0, 85.0]));
    assert!(planar_covers(&polygons, [179.0, 89.0]));
    assert!(!planar_covers(&polygons, [0.0, 70.0]));
    let back = import(value).context("import")?;
    assert!(covers(&back, [0.0, 90.0])?);
    assert!((area(&back) / area(&geometry) - 1.0).abs() < 1e-6);
    Ok(())
}

#[test]
fn export_keeps_holes_and_frames_complements() -> anyhow::Result<()> {
    let geometry = native(json!({"type":"Polygon","coordinates":[
        [[0,0],[10,0],[10,10],[0,10],[0,0]],
        [[2,2],[2,8],[8,8],[8,2],[2,2]]
    ]}))?;
    let polygons = planar_polygons(&export(&geometry)?);
    assert_eq!((polygons.len(), polygons[0].len()), (1, 2));
    assert!(planar_covers(&polygons, [1.0, 5.0]));
    assert!(!planar_covers(&polygons, [5.0, 5.0]));
    // A clockwise ring bounds everything outside it: the frame with a hole.
    let complement =
        native(json!({"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}))?;
    let polygons = planar_polygons(&export(&complement)?);
    assert!(planar_covers(&polygons, [100.0, 50.0]));
    assert!(!planar_covers(&polygons, [0.5, 0.5]));
    Ok(())
}

#[test]
fn export_cuts_lines_and_densifies_arcs() -> anyhow::Result<()> {
    let line = native(json!({"type":"LineString","coordinates":[[170,50],[-170,50]]}))?;
    let value = export(&line)?;
    assert_eq!(value["type"], "MultiLineString");
    let parts = value["coordinates"].as_array().expect("parts");
    assert_eq!(parts.len(), 2);
    // The arc bulges north of the parallel to about 50.43 degrees; its
    // planar pieces follow it.
    let north = parts
        .iter()
        .flat_map(|part| part.as_array().expect("part"))
        .map(|p| p[1].as_f64().expect("lat"))
        .fold(f64::NEG_INFINITY, f64::max);
    assert!(north > 50.43, "{north}");
    let first = parts[0].as_array().expect("first part");
    assert_eq!(first[first.len() - 1][0].as_f64(), Some(180.0));
    assert_eq!(parts[1][0][0].as_f64(), Some(-180.0));
    Ok(())
}

#[test]
fn a_polar_ring_cut_along_the_antimeridian_is_one_region() -> anyhow::Result<()> {
    // RFC 7946's one-ring cap: east along 70°N from -180 to +180, up the
    // +180 side of the cut to the pole, back down the -180 side.
    let cut = native(json!({"type":"Polygon","coordinates":[[
        [-180,70],[-90,70],[0,70],[90,70],[180,70],[180,90],[0,90],[-180,90],[-180,70]
    ]]}))?;
    // The same region held around its pole.
    let held = native(json!({"type":"Polygon","coordinates":[[
        [-90,70],[0,70],[90,70],[180,70],[-90,70]
    ]]}))?;
    for p in [[0.0, 89.0], [180.0, 80.0], [-180.0, 75.0], [45.0, 80.0]] {
        assert!(covers(&cut, p)?, "{p:?}");
    }
    assert!(!covers(&cut, [0.0, 60.0])?);
    let measured = |g: &Geometry, m| measure(g, m, &mut GeometryWork::default()).expect("measure");
    for m in [Measurement::Area, Measurement::Perimeter] {
        let (a, b) = (measured(&cut, m), measured(&held, m));
        assert!((a - b).abs() <= 1e-6 * b, "{a} {b}");
    }
    // The cut is interior: the two hold the same point set.
    let relation = crate::topology::relation::matrix(&cut, &held, &mut GeometryWork::default())?;
    assert_eq!(relation, "2FFF1FFF2");
    // A spur up and back down one side of the meridian is no cut.
    assert!(native(json!({"type":"Polygon","coordinates":[[
        [-180,70],[-90,70],[0,70],[90,70],[180,70],[180,90],[180,70],[-180,70]
    ]]}))
    .is_err());
    // Nor is a cut whose legs return to another latitude.
    assert!(native(json!({"type":"Polygon","coordinates":[[
        [-180,60],[-90,70],[0,70],[90,70],[180,70],[180,90],[-180,90],[-180,60]
    ]]}))
    .is_err());
    Ok(())
}
