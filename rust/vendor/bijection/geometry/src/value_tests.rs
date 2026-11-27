use serde_json::json;

use crate::{
    predicates,
    wkb,
    GeometryWork,
    Gv,
    STANDARD,
};

#[test]
fn edge_contracts_and_native_bounds_are_explicit() -> anyhow::Result<()> {
    let mut space = crate::GeometrySpace::wgs84_xy();
    assert_eq!(space.segments, crate::SegmentModel::GreatElliptic);
    assert_eq!(space.arrow_extension(), "bijection.geometry");
    space.segments = crate::SegmentModel::CoordinateLinear;
    assert!(space.validate().is_err());
    space.frame = crate::CoordinateFrame::Cartesian {
        name: "local".into(),
        unit: crate::LinearUnit::metre(),
    };
    assert!(space.validate().is_ok());
    assert_eq!(space.arrow_extension(), "geoarrow.wkb");
    let line = json!({"type":"LineString","coordinates":[[-60,60],[60,60]]});
    let native = crate::binding::invoke(json!({"operation":"bounds", "values":[line],
        "declaration":{"space":crate::GeometrySpace::wgs84_xy(),"size":"large"}}));
    assert!(native["value"][3].as_f64().unwrap() > 70.0);
    let planar = crate::binding::invoke(json!({"operation":"bounds", "values":[line],
        "declaration":{"space":space,"size":"large"}}));
    assert_eq!(planar["value"], json!([-60.0, 60.0, 60.0, 60.0]));
    Ok(())
}

#[test]
fn bindings_enforce_the_callers_work_allowance_and_declaration() {
    let request = json!({"operation":"admit", "value":{"type":"Point","coordinates":[0,0]}, "declaration":{"space":crate::GeometrySpace::wgs84_xy(),"size":"standard"},"allowance":{"steps":0,"allocated_bytes":1000000}});
    let reply = crate::binding::invoke(request);
    assert_eq!(reply["refusal"]["code"], "SpatialWorkLimit");
    assert_eq!(reply["refusal"]["bound"]["kind"], "geometry_work_steps");
    assert_eq!(reply["refusal"]["bound"]["limit"], 0);
    let invalid_work = crate::binding::invoke(
        json!({"operation":"admit", "value":{"type":"LineString","coordinates":[[0,0],[0,0]]}, "declaration":{"space":crate::GeometrySpace::wgs84_xy(),"size":"standard"}}),
    );
    assert!(invalid_work["usage"]["steps"].as_u64().unwrap() > 0);
    assert_eq!(invalid_work["refusal"]["code"], "InvalidGeometry");
    let mut unsupported = serde_json::to_value(crate::GeometrySpace::wgs84_xy()).unwrap();
    unsupported["numerics"]["predicates"] = json!("unsupported");
    let refusal = crate::binding::invoke(
        json!({"operation":"admit", "value":{"type":"Point","coordinates":[0,0]}, "declaration":{"space":unsupported,"size":"standard"}}),
    );
    // A malformed declaration is still a typed geometry refusal.
    assert_eq!(refusal["refusal"]["code"], "InvalidGeometry");
    let invalid = crate::binding::invoke(
        json!({"operation":"admit", "value":{"type":"Point","coordinates":[]}, "declaration":{"size":"large"}}),
    );
    assert!(invalid.get("refusal").is_some());
}

#[test]
fn empty_values_preserve_kind_dimension_and_codec() -> anyhow::Result<()> {
    for kind in [
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
    ] {
        let value = json!({"type": kind, "coordinates": []});
        let geometry = crate::admit_geometry(
            &Gv::from(&value),
            &crate::GeometrySpace::wgs84_xy(),
            &STANDARD,
        )?;
        assert!(geometry.is_empty());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&geometry.js_json())?,
            value
        );
        assert_eq!(
            wkb::decode(&geometry.wkb(), &geometry.space, &STANDARD)?,
            geometry
        );
        let height = crate::admit_geometry(
            &Gv::from(&value),
            &crate::GeometrySpace::wgs84_xyz(),
            &STANDARD,
        )?;
        assert!(height.is_empty());
        assert_eq!(height.heights.as_deref(), Some([].as_slice()));
        assert_eq!(
            wkb::decode(&height.wkb(), &height.space, &STANDARD)?,
            height
        );
    }
    Ok(())
}

#[test]
fn empty_topology_is_known_and_has_no_distance() -> anyhow::Result<()> {
    let point = crate::admit_geometry(
        &Gv::from(&json!({"type":"Point", "coordinates":[0,0]})),
        &crate::GeometrySpace::wgs84_xy(),
        &STANDARD,
    )?;
    for kind in [
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
    ] {
        let empty = crate::admit_geometry(
            &Gv::from(&json!({"type":kind,"coordinates":[]})),
            &crate::GeometrySpace::wgs84_xy(),
            &STANDARD,
        )?;
        let mut work = GeometryWork::default();
        assert!(!predicates::intersects(&point, &empty, &mut work)?);
        assert!(!predicates::covered_by(&empty, &point, &mut work)?);
        assert!(!predicates::covered_by(&point, &empty, &mut work)?);
        assert!(predicates::distance(&point, &empty, &mut work)?.is_infinite());
        let prepared = predicates::prepared::PreparedGeometry::new(
            empty,
            crate::predicates::prepared::Preparation::Topology,
            &mut work,
        )?;
        assert_eq!(
            prepared.classify([0.0, 0.0], Some(0.0), &mut work)?,
            "outside"
        );
        assert!(!prepared.intersects_box([-1.0, -1.0, 1.0, 1.0], &mut work)?);
    }
    assert!(crate::admit_geometry(
        &Gv::from(&json!({"type":"LineString","coordinates":[[0,0]]})),
        &crate::GeometrySpace::wgs84_xy(),
        &STANDARD
    )
    .is_err());
    assert!(crate::admit_geometry(
        &Gv::from(&json!({"type":"Polygon","coordinates":[[]]})),
        &crate::GeometrySpace::wgs84_xy(),
        &STANDARD
    )
    .is_err());
    Ok(())
}

#[test]
fn reference_and_prepared_topology_agree_at_the_meridian() -> anyhow::Result<()> {
    let area = crate::admit_geometry(
        &Gv::from(
            &json!({"type":"Polygon", "coordinates":[[[-180,0],[-178,0],[-178,2],[-180,2],[-180,0]]]}),
        ),
        &crate::GeometrySpace::wgs84_xy(),
        &STANDARD,
    )?;
    let mut work = GeometryWork::default();
    let prepared_area = predicates::prepared::PreparedGeometry::new(
        area.clone(),
        crate::predicates::prepared::Preparation::Topology,
        &mut work,
    )?;
    for (longitude, expected) in [
        (-180.0, true),
        (180.0, true),
        (-179.0, true),
        (179.0, false),
    ] {
        let point = crate::Geometry::new(
            crate::Kind::Point,
            crate::Coords::Point([longitude, 1.0]),
            crate::GeometrySpace::wgs84_xy(),
        );
        let prepared_point = predicates::prepared::PreparedGeometry::new(
            point.clone(),
            crate::predicates::prepared::Preparation::Topology,
            &mut work,
        )?;
        assert_eq!(predicates::intersects(&point, &area, &mut work)?, expected);
        assert_eq!(
            prepared_point.intersects(&prepared_area, &mut work)?,
            expected
        );
        assert_eq!(predicates::covered_by(&point, &area, &mut work)?, expected);
        assert_eq!(
            prepared_point.covered_by(&prepared_area, &mut work)?,
            expected
        );
    }
    Ok(())
}

#[test]
fn projected_values_keep_their_frame_and_use_its_linear_unit() -> anyhow::Result<()> {
    let mut space = crate::GeometrySpace::wgs84_xy();
    space.segments = crate::SegmentModel::CoordinateLinear;
    space.frame = crate::CoordinateFrame::Cartesian {
        name: "EPSG:3857".into(),
        unit: crate::LinearUnit {
            metres_per_unit: crate::Positive::new(0.3048).unwrap(),
        },
    };
    let admit = |value: serde_json::Value, space: &crate::GeometrySpace| {
        crate::admit_geometry(&Gv::from(&value), space, &STANDARD)
    };
    let left = admit(
        json!({"type":"Point","coordinates":[1000000,1000000]}),
        &space,
    )?;
    let right = admit(
        json!({"type":"Point","coordinates":[1000003,1000004]}),
        &space,
    )?;
    assert_eq!(wkb::decode(&left.wkb(), &space, &STANDARD)?, left);
    let mut work = GeometryWork::default();
    let left = predicates::prepared::PreparedGeometry::new(
        left,
        crate::predicates::prepared::Preparation::Topology,
        &mut work,
    )?;
    let right = predicates::prepared::PreparedGeometry::new(
        right,
        crate::predicates::prepared::Preparation::Topology,
        &mut work,
    )?;
    assert!((left.distance(&right, &mut work)? - 1.524).abs() < 1e-12);
    let seam = |x| {
        predicates::prepared::PreparedGeometry::new(
            admit(json!({"type":"Point","coordinates":[x,0]}), &space).unwrap(),
            crate::predicates::prepared::Preparation::Topology,
            &mut GeometryWork::default(),
        )
        .unwrap()
    };
    assert!(!seam(180).intersects(&seam(-180), &mut work)?);
    let foreign = predicates::prepared::PreparedGeometry::new(
        admit(
            json!({"type":"Point","coordinates":[0,0]}),
            &crate::GeometrySpace::wgs84_xy(),
        )?,
        crate::predicates::prepared::Preparation::Topology,
        &mut work,
    )?;
    assert!(left.intersects(&foreign, &mut work).is_err());
    assert!(admit(json!({"type":"Point","coordinates":[1e200,0]}), &space).is_err());
    Ok(())
}
#[test]
fn xyz_is_preserved_without_implying_ground_topology() -> anyhow::Result<()> {
    let mut space = crate::GeometrySpace::wgs84_xy();
    space.segments = crate::SegmentModel::CoordinateLinear;
    space.frame = crate::CoordinateFrame::Cartesian {
        name: "local".into(),
        unit: crate::LinearUnit::metre(),
    };
    space.dimensions = crate::Dimensions::Xyz {
        vertical: crate::VerticalReference::CartesianAxis,
        unit: crate::LinearUnit {
            metres_per_unit: crate::Positive::new(0.01).unwrap(),
        },
    };
    let admit =
        |value: serde_json::Value| crate::admit_geometry(&Gv::from(&value), &space, &STANDARD);
    let vertical = admit(json!({"type":"LineString","coordinates":[[0,0,0],[0,0,5]]}))?;
    assert_eq!(wkb::decode(&vertical.wkb(), &space, &STANDARD)?, vertical);
    let mut work = GeometryWork::default();
    let point = |z| {
        predicates::prepared::PreparedGeometry::new(
            admit(json!({"type":"Point","coordinates":[0,0,z]})).unwrap(),
            crate::predicates::prepared::Preparation::Topology,
            &mut GeometryWork::default(),
        )
        .unwrap()
    };
    assert_eq!(point(0).distance(&point(500), &mut work)?, 5.0);
    assert!(point(0).intersects(&point(5), &mut work).is_err());
    Ok(())
}

#[test]
fn projected_ring_validity_does_not_depend_on_json_number_spelling() -> anyhow::Result<()> {
    let mut space = crate::GeometrySpace::wgs84_xy();
    space.segments = crate::SegmentModel::CoordinateLinear;
    space.frame = crate::CoordinateFrame::Cartesian {
        name: "local".into(),
        unit: crate::LinearUnit::metre(),
    };
    let offset = 10_000_000_000_000_000_i64;
    let value = Gv::from(
        &json!({"type":"Polygon","coordinates":[[[offset,offset],[offset+8,offset],[offset+8,offset+8],[offset,offset+8],[offset,offset]]]}),
    );
    fn float_tokens(value: &Gv) -> Gv {
        match value {
            Gv::Int64(number) => Gv::Float(*number as f64),
            Gv::Array(values) => Gv::Array(values.iter().map(float_tokens).collect()),
            Gv::Object(fields) => Gv::Object(
                fields
                    .iter()
                    .map(|(name, value)| (name.clone(), float_tokens(value)))
                    .collect(),
            ),
            value => value.clone(),
        }
    }
    let integer = crate::admit_geometry(&value, &space, &STANDARD)?;
    assert_eq!(
        crate::admit_geometry(&float_tokens(&value), &space, &STANDARD)?,
        integer
    );
    let large = 9e18;
    assert!(crate::admit_geometry(
        &Gv::from(&json!({"type":"Polygon","coordinates":[[
            [-large,-large],[large,-large],[large,large],[-large,large],[-large,-large]
        ]]})),
        &space,
        &STANDARD
    )
    .is_ok());
    Ok(())
}

#[test]
fn collections_preserve_nested_kind_dimensions_empty_values_and_codecs() -> anyhow::Result<()> {
    let point = json!({"type":"Point","coordinates":[1,2]});
    for space in [
        crate::GeometrySpace::wgs84_xy(),
        crate::GeometrySpace::wgs84_xyz(),
    ] {
        let mut point = point.clone();
        if !space.is_ground() {
            point["coordinates"] = json!([1, 2, 3]);
        }
        let value = json!({"type":"GeometryCollection","geometries":[point,{"type":"GeometryCollection","geometries":[]},{"type":"LineString","coordinates":[]}]});
        let admitted = crate::admit_geometry(&Gv::from(&value), &space, &STANDARD)?;
        assert_eq!(admitted.position_count(), 1);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&admitted.js_json())?,
            value
        );
        assert_eq!(wkb::decode(&admitted.wkb(), &space, &STANDARD)?, admitted);
        let empty = crate::admit_geometry(
            &Gv::from(&json!({"type":"GeometryCollection","geometries":[]})),
            &space,
            &STANDARD,
        )?;
        assert!(empty.is_empty());
        assert_eq!(wkb::decode(&empty.wkb(), &space, &STANDARD)?, empty);
    }
    Ok(())
}

#[test]
fn collection_union_coverage_includes_shared_and_overlapping_area_boundaries() -> anyhow::Result<()>
{
    // Split across latitudes, so every part's east-west arcs span the same
    // longitudes as the covered rectangle's and bulge alike.
    let rectangle = |south: f64, north: f64| json!({"type":"Polygon","coordinates":[[[0,south],[2,south],[2,north],[0,north],[0,south]]]});
    let admit = |value: serde_json::Value| {
        crate::admit_geometry(
            &Gv::from(&value),
            &crate::GeometrySpace::wgs84_xy(),
            &STANDARD,
        )
    };
    let left = admit(rectangle(0., 2.))?;
    for (parts, expected) in [
        (vec![rectangle(0., 1.), rectangle(1., 2.)], true),
        (vec![rectangle(0., 1.5), rectangle(0.5, 2.)], true),
        (vec![rectangle(0., 0.9), rectangle(1.1, 2.)], false),
        (vec![rectangle(0., 0.5), rectangle(1.5, 2.)], false),
    ] {
        let right = admit(json!({"type":"GeometryCollection","geometries":parts}))?;
        assert_eq!(
            predicates::covered_by(&left, &right, &mut GeometryWork::default())?,
            expected
        );
        let mut work = GeometryWork::default();
        let a = predicates::prepared::PreparedGeometry::new(
            left.clone(),
            crate::predicates::prepared::Preparation::Topology,
            &mut work,
        )?;
        let b = predicates::prepared::PreparedGeometry::new(
            right,
            crate::predicates::prepared::Preparation::Topology,
            &mut work,
        )?;
        assert_eq!(a.covered_by(&b, &mut work)?, expected);
        assert!(a.intersects(&b, &mut work)?);
    }
    let mixed = admit(
        json!({"type":"GeometryCollection","geometries":[rectangle(0.,2.),{"type":"Point","coordinates":[5,5]}]}),
    )?;
    let point = admit(json!({"type":"Point","coordinates":[5,5]}))?;
    assert!(predicates::intersects(
        &point,
        &mixed,
        &mut GeometryWork::default()
    )?);
    assert!(predicates::covered_by(
        &point,
        &mixed,
        &mut GeometryWork::default()
    )?);
    assert!(!predicates::covered_by(
        &mixed,
        &left,
        &mut GeometryWork::default()
    )?);
    Ok(())
}

#[test]
fn collection_admission_enforces_whole_value_and_nesting_allowances() -> anyhow::Result<()> {
    let mut nested = json!({"type":"Point","coordinates":[]});
    for _ in 0..17 {
        nested = json!({"type":"GeometryCollection","geometries":[nested]});
    }
    let refusal = crate::admit_geometry(
        &Gv::from(&nested),
        &crate::GeometrySpace::wgs84_xy(),
        &STANDARD,
    )
    .unwrap_err();
    assert_eq!(refusal.bound.unwrap().kind, crate::TierBound::Depth);
    let points: Vec<_> = (0..300).map(|i| json!([i % 100, i % 50])).collect();
    let value = json!({"type":"GeometryCollection","geometries":[{"type":"MultiPoint","coordinates":points},{"type":"MultiPoint","coordinates":points}]});
    let refusal = crate::admit_geometry(
        &Gv::from(&value),
        &crate::GeometrySpace::wgs84_xy(),
        &STANDARD,
    )
    .unwrap_err();
    assert_eq!(refusal.bound.unwrap().kind, crate::TierBound::Positions);
    Ok(())
}

#[test]
fn ground_projection_retains_collapsed_components_as_geometry() -> anyhow::Result<()> {
    let admit = |value: serde_json::Value| {
        crate::admit_geometry(
            &Gv::from(&value),
            &crate::GeometrySpace::wgs84_xyz(),
            &STANDARD,
        )
    };
    let vertical = admit(json!({"type":"LineString","coordinates":[[1,2,0],[1,2,5]]}))?;
    let projected = vertical.ground_projection(&mut GeometryWork::default())?;
    assert_eq!(projected.kind, crate::Kind::Point);
    assert!(projected.space.is_ground());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&projected.js_json())?,
        json!({"type":"Point","coordinates":[1,2]})
    );
    let mixed = admit(
        json!({"type":"MultiLineString","coordinates":[[[1,2,0],[1,2,5]],[[1,2,0],[3,4,5]]]}),
    )?;
    let mixed = mixed.ground_projection(&mut GeometryWork::default())?;
    assert_eq!(mixed.kind, crate::Kind::GeometryCollection);
    let admitted = crate::admit_geometry(
        &Gv::from(&serde_json::from_str::<serde_json::Value>(
            &mixed.js_json(),
        )?),
        &mixed.space,
        &STANDARD,
    )?;
    assert_eq!(mixed, admitted);
    Ok(())
}

#[test]
fn multipoint_members_preserve_empty_values_and_height_order() -> anyhow::Result<()> {
    for (space, points) in [
        (
            crate::GeometrySpace::wgs84_xy(),
            json!([[], [1, 2], [], [3, 4], []]),
        ),
        (
            crate::GeometrySpace::wgs84_xyz(),
            json!([[], [1, 2, 7], [], [3, 4, 11], []]),
        ),
    ] {
        let value = json!({"type":"MultiPoint", "coordinates":points});
        let geometry = crate::admit_geometry(&Gv::from(&value), &space, &STANDARD)?;
        assert_eq!(geometry.position_count(), 2);
        assert!(!geometry.is_empty());
        assert_eq!(geometry.wkb_len(), geometry.wkb().len());
        assert_eq!(wkb::decode(&geometry.wkb(), &space, &STANDARD)?, geometry);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&geometry.js_json())?,
            value
        );
        if !space.is_ground() {
            assert_eq!(geometry.heights.as_deref(), Some([7.0, 11.0].as_slice()));
            let ground = geometry.ground_projection(&mut GeometryWork::default())?;
            assert_eq!(
                ground.js_json(),
                r#"{"type":"MultiPoint","coordinates":[[],[1,2],[],[3,4],[]]}"#
            );
        }
    }
    let empty = crate::admit_geometry(
        &Gv::from(&json!({"type":"MultiPoint", "coordinates":[[], []]})),
        &crate::GeometrySpace::wgs84_xy(),
        &STANDARD,
    )?;
    assert!(empty.is_empty());
    assert_eq!(wkb::decode(&empty.wkb(), &empty.space, &STANDARD)?, empty);
    Ok(())
}

#[test]
fn position_binding_retains_unknown_accuracy_and_refuses_invalid_centers() {
    let declaration = json!({"space":crate::GeometrySpace::wgs84_xy(),"size":"standard"});
    for accuracy in [json!(null), json!(0.0), json!(2.0)] {
        let value = json!({"point":{"type":"Point","coordinates":[0.0,0.0]},"accuracy_m":accuracy});
        let reply = crate::binding::invoke(
            json!({"operation":"admit_position","declaration":declaration,"value":value}),
        );
        // The point takes the canonical JSON spelling, so integral
        // coordinates come back as integers.
        assert_eq!(
            reply["value"],
            json!({"point":{"type":"Point","coordinates":[0,0]},"accuracy_m":accuracy})
        );
        assert!(reply["usage"]["steps"].as_u64().unwrap() > 0);
    }
    for value in [
        json!({"point":{"type":"Point","coordinates":[]},"accuracy_m":null}),
        json!({"point":{"type":"Point","coordinates":[0,0]},"accuracy_m":-1}),
        json!({"point":{"type":"Point","coordinates":[0,0]}}),
    ] {
        let reply = crate::binding::invoke(
            json!({"operation":"admit_position","declaration":declaration,"value":value}),
        );
        assert!(reply.get("refusal").is_some());
    }
    let reply = crate::binding::invoke(
        json!({"operation":"admit_position","declaration":declaration,
        "value":{"point":{"type":"Point","coordinates":[0,0]},"accuracy_m":null},
        "allowance":{"steps":0,"allocated_bytes":1000000}}),
    );
    assert_eq!(reply["refusal"]["bound"]["kind"], "geometry_work_steps");
}

#[test]
fn union_binding_reduces_admitted_values_to_one_canonical_collection() {
    let declaration = json!({"space":crate::GeometrySpace::wgs84_xy(),"size":"standard"});
    let a = json!({"type":"Point","coordinates":[1.0,2.0]});
    let b = json!({"type":"LineString","coordinates":[[0.0,0.0],[1.0,1.0]]});
    let union = |values: serde_json::Value| {
        crate::binding::invoke(
            json!({"operation":"union","declaration":declaration,"values":values}),
        )
    };
    let forward = union(json!([a, b, a]));
    assert_eq!(forward["value"]["type"], "GeometryCollection");
    assert_eq!(forward["value"]["geometries"].as_array().unwrap().len(), 2);
    assert_eq!(forward["value"], union(json!([b, a]))["value"]);
    assert_eq!(
        union(json!([]))["value"],
        json!({"type":"GeometryCollection","geometries":[]})
    );
    let invalid = union(json!([a, {"type":"LineString","coordinates":[[0,0],[0,0]]}]));
    assert_eq!(invalid["refusal"]["code"], "InvalidGeometry");
    let bounded = crate::binding::invoke(json!({"operation":"union","declaration":declaration,
        "values":[a],"allowance":{"steps":0,"allocated_bytes":1000000}}));
    assert_eq!(bounded["refusal"]["bound"]["kind"], "geometry_work_steps");
}

#[test]
fn admission_matches_exact_ring_topology() -> anyhow::Result<()> {
    let cases: Vec<serde_json::Value> = serde_json::from_str(include_str!("admission.cases.json"))?;
    assert!(!cases.is_empty());
    for case in cases {
        let admitted = crate::admit_geometry(
            &Gv::from(&case["value"]),
            &crate::GeometrySpace::wgs84_xy(),
            &STANDARD,
        );
        assert_eq!(
            admitted.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {admitted:?}",
            case["name"]
        );
    }
    Ok(())
}

/// A large part touching another at one vertex refines each contact edge
/// through the other part's packed edges, not its every ring vertex.
#[test]
fn touching_parts_locate_contact_samples_through_the_packed_edges() {
    let shell: Vec<[f64; 2]> = (0..=2000)
        .map(|i| [i as f64 / 200.0, if i % 2 == 1 { -0.01 } else { 0.0 }])
        .chain([[10.0, 10.0], [0.0, 10.0], [0.0, 0.0]])
        .collect();
    let island: Vec<[f64; 2]> = [[10.0, 10.0], [12.0, 11.0]]
        .into_iter()
        .chain((1..=400).map(|k| {
            [
                12.0 - k as f64 / 200.0,
                if k % 2 == 1 { 11.01 } else { 11.0 },
            ]
        }))
        .chain([[10.0, 10.0]])
        .collect();
    let admit = |coordinates: serde_json::Value| {
        crate::binding::invoke(json!({"operation":"admit",
            "value":{"type":"MultiPolygon","coordinates":coordinates},
            "declaration":{"space":crate::GeometrySpace::wgs84_xy(),"size":"large"},
            "allowance":{"steps":1000000,"allocated_bytes":16777216}}))
    };
    let reply = admit(json!([[shell], [island]]));
    assert!(reply.get("refusal").is_none(), "{reply}");
    assert!(reply["usage"]["steps"].as_u64().unwrap() < 1_000_000);
    let overlapping: Vec<[f64; 2]> = island.iter().map(|p| [p[0] - 1.0, p[1] - 1.0]).collect();
    let reply = admit(json!([[shell], [overlapping]]));
    assert_eq!(reply["refusal"]["code"], "InvalidGeometry", "{reply}");
}

/// Platform maths libraries differ in their last bits; every transcendental
/// call in the kernel goes through `libm`, so native and Wasm agree.
#[test]
fn kernel_sources_call_no_platform_maths() -> anyhow::Result<()> {
    let methods = [
        "sin", "cos", "tan", "sin_cos", "asin", "acos", "atan", "atan2", "sinh", "cosh", "tanh",
        "asinh", "acosh", "atanh", "hypot", "cbrt", "exp", "exp2", "exp_m1", "ln", "ln_1p", "log",
        "log2", "log10", "powf", "powi",
    ];
    let calls: Vec<String> = methods.iter().map(|name| format!(".{name}(")).collect();
    let mut pending = vec![std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];
    let mut found = Vec::new();
    let mut files = 0;
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path)? {
                pending.push(entry?.path());
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files += 1;
            for (number, line) in std::fs::read_to_string(&path)?.lines().enumerate() {
                let code = line.split("//").next().unwrap_or("");
                if calls.iter().any(|call| code.contains(call.as_str())) {
                    found.push(format!("{}:{}", path.display(), number + 1));
                }
            }
        }
    }
    assert!(files > 20, "{files} source files");
    assert!(found.is_empty(), "{found:?}");
    Ok(())
}
