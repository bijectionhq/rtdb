use geometry::{Coords, Geometry, GeometryDeclaration, GeometrySpace, GeometryWork, Gv, Kind, Size};
use rtdb_columnar::MemoryBudget;
use rtdb_types::{Iid, SpatialGeometryEncoding};

use crate::{
    Predicate, bijection_geometry,
    kernel::Location,
    postings::{self, Partition, Postings},
};

fn point(longitude: f64, latitude: f64) -> Vec<u8> {
    let geometry = Geometry::new(
        Kind::Point,
        Coords::Point([longitude, latitude]),
        GeometrySpace::wgs84_xy(),
    );
    [vec![0], geometry.wkb()].concat()
}

async fn checked_candidates(
    bytes: Vec<u8>,
    stats: &[u8],
    partition: Option<Partition>,
    ranges: &[geometry::KeyRange],
    memory: &std::sync::Arc<MemoryBudget>,
    check: impl Fn() -> rtdb_types::Result<()>,
    insert: impl FnMut(Iid) -> rtdb_types::Result<()>,
) -> rtdb_types::Result<()> {
    let bytes = bytes::Bytes::from(bytes);
    let root = postings::Root::decode(stats, bytes.len() as u64)?;
    postings::candidates(root, partition, ranges, memory, check, async |range| {
        Ok(bytes.slice(range.start as usize..range.end as usize))
    }, insert).await
}

fn canonical_float(value: f64) -> Vec<u8> {
    let bits = value.to_bits();
    let encoded = if value.is_sign_negative() {
        !bits
    } else {
        bits ^ (1 << 63)
    };
    [vec![5], encoded.to_be_bytes().to_vec()].concat()
}

fn canonical_text(value: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    for byte in value.bytes() {
        bytes.push(byte);
        if byte == 0 {
            bytes.push(0xff);
        }
    }
    bytes.extend([0, 1]);
    bytes
}

fn canonical_array(values: impl IntoIterator<Item = Vec<u8>>) -> Vec<u8> {
    let mut bytes = vec![12];
    bytes.extend(values.into_iter().flatten());
    bytes.extend([0, 0]);
    bytes
}

fn canonical_object(fields: impl IntoIterator<Item = (&'static str, Vec<u8>)>) -> Vec<u8> {
    let mut bytes = vec![13];
    for (key, value) in fields {
        bytes.extend(canonical_text(key));
        bytes.extend(value);
    }
    bytes.extend([0, 0]);
    bytes
}

fn canonical_shape(key: &'static str, contents: Vec<u8>, kind: &str) -> Vec<u8> {
    canonical_object([(key, contents), ("type", [vec![9], canonical_text(kind)].concat())])
}

fn canonical_point(x: f64, y: f64) -> Vec<u8> {
    canonical_shape(
        "coordinates",
        canonical_array([canonical_float(x), canonical_float(y)]),
        "Point",
    )
}

const DECLARATION: GeometryDeclaration = GeometryDeclaration {
    space: GeometrySpace::wgs84_xy(),
    size: Size::Large,
};

#[test]
fn canonical_geometry_preserves_work_and_tagged_wkb_results() {
    let subnormal = f64::from_bits(1);
    let image = canonical_point(-0.0, subnormal);
    let expected_bytes = 5 * std::mem::size_of::<Gv>()
        + 2 * std::mem::size_of::<String>()
        + 23
        + DECLARATION.space.retained_bytes()
        + 256;
    let mut admission = GeometryWork::with_limits(58, expected_bytes);
    let location = Location::decode_stored(
        &image,
        SpatialGeometryEncoding::BijectionGeometry,
        &mut admission,
    )
    .unwrap()
    .unwrap();
    assert_eq!(admission.usage().steps, 58);
    assert_eq!(admission.usage().peak_allocated_bytes, expected_bytes);
    for (steps, bytes) in [(57, expected_bytes), (58, expected_bytes - 1)] {
        let mut work = GeometryWork::with_limits(steps, bytes);
        let error = Location::decode_stored(
            &image,
            SpatialGeometryEncoding::BijectionGeometry,
            &mut work,
        )
        .err()
        .unwrap();
        assert_eq!(error.code, "rtdb.spatial/work-limit");
    }
    let mut work = GeometryWork::default();
    let value = bijection_geometry::decode(&image, &mut work).unwrap();
    let geometry = geometry::admit_view_with_work(value, &DECLARATION, &mut work).unwrap();
    let Coords::Point(coords) = geometry.coords else {
        panic!("expected a point");
    };
    assert_eq!(coords.map(f64::to_bits), [0, 1]);
    let wkb = Location::decode_stored(
        &point(0.0, subnormal),
        SpatialGeometryEncoding::TaggedWkb,
        &mut work,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        location.keys(&mut work).unwrap(),
        wkb.keys(&mut work).unwrap()
    );
    let exact = Predicate::WithinDistance(0.0);
    assert_eq!(
        exact.evaluate(&location, &wkb.geometry, None, &mut work).unwrap(),
        Some(None)
    );
}

#[test]
fn canonical_collections_and_position_accuracy_keep_their_typed_values() {
    let image = canonical_shape(
        "geometries",
        canonical_array([
            canonical_point(3.0, 4.0),
            canonical_shape(
                "coordinates",
                canonical_array([
                    canonical_array([]),
                    canonical_array([canonical_float(1.0), canonical_float(2.0)]),
                ]),
                "MultiPoint",
            ),
        ]),
        "GeometryCollection",
    );
    let mut work = GeometryWork::default();
    let value = bijection_geometry::decode(&image, &mut work).unwrap();
    let geometry = geometry::admit_view_with_work(value, &DECLARATION, &mut work).unwrap();
    let Coords::Collection(members) = geometry.coords else {
        panic!("expected a collection");
    };
    assert!(matches!(&members[0].coords, Coords::Point(point) if *point == [3.0, 4.0]));
    assert!(matches!(
        &members[1].coords,
        Coords::MultiPoint(points) if points.as_slice() == &[None, Some([1.0, 2.0])]
    ));

    for (accuracy, bits) in [
        (vec![2], None),
        (canonical_float(-0.0), Some((-0.0f64).to_bits())),
    ] {
        let image = canonical_object([
            ("accuracy_m", accuracy),
            ("point", canonical_point(1.0, 2.0)),
        ]);
        let value = bijection_geometry::decode(&image, &mut work).unwrap();
        let position = geometry::admit_position_view_with_work(value, &DECLARATION, &mut work).unwrap();
        assert_eq!(position.point, [1.0, 2.0]);
        assert_eq!(position.accuracy_m.map(f64::to_bits), bits);

        let location = Location::decode_stored(
            &image,
            SpatialGeometryEncoding::BijectionPosition,
            &mut work,
        )
        .unwrap()
        .unwrap();
        let mut tagged = if bits.is_some() {
            [vec![2], (-0.0f64).to_le_bytes().to_vec()].concat()
        } else {
            vec![1]
        };
        tagged.extend_from_slice(&point(1.0, 2.0)[1..]);
        let legacy = Location::decode(&tagged, &mut work).unwrap();
        assert_eq!(
            location.keys(&mut work).unwrap(),
            legacy.keys(&mut work).unwrap()
        );
    }
    for encoding in [
        SpatialGeometryEncoding::BijectionGeometry,
        SpatialGeometryEncoding::BijectionPosition,
    ] {
        assert!(Location::decode_stored(&[2], encoding, &mut work).unwrap().is_none());
    }
}

#[test]
fn canonical_geometry_validates_the_complete_image_before_shape_admission() {
    let too_deep = [vec![12; 65], vec![2], vec![0; 130]].concat();
    let cases = [
        (vec![3, 2], "Invalid Bijection geometry boolean"),
        (vec![5], "Bijection geometry is truncated"),
        (
            canonical_float(f64::from_bits(0x7ff8_0000_0000_0001)),
            "Invalid Bijection geometry float",
        ),
        (vec![2, 2], "Bijection geometry has trailing bytes"),
        (
            vec![9, 0xc3, 0, 0xff, 0xa9, 0, 1],
            "Bijection geometry text must be UTF-8",
        ),
        (
            vec![9, 0xff, 0, 0xff, 0, 2],
            "Invalid Bijection geometry text delimiter",
        ),
        (too_deep, "Bijection geometry is too deeply nested"),
        (
            canonical_object([("a\0", vec![2]), ("a", vec![2])]),
            "Bijection geometry fields must be strictly ordered",
        ),
        (
            canonical_object([("é", vec![2]), ("é", vec![2])]),
            "Bijection geometry fields must be strictly ordered",
        ),
        (
            canonical_object([("a", vec![2]), ("a\0", vec![2])]),
            "Invalid geometry: unsupported geometry type",
        ),
        (
            canonical_object([("ignored", vec![0xff])]),
            "Unsupported canonical Bijection geometry value",
        ),
        (
            canonical_float(f64::from_bits(0x7ff8_0000_0000_0000)),
            "Invalid geometry: expected geometry object",
        ),
    ];
    for (image, message) in cases {
        let error = Location::decode_stored(
            &image,
            SpatialGeometryEncoding::BijectionGeometry,
            &mut GeometryWork::default(),
        )
        .err()
        .unwrap();
        assert_eq!(error.code, "rtdb.spatial/invalid");
        assert_eq!(error.message.as_deref(), Some(message));
    }
}

#[tokio::test]
async fn spatial_arrow_posting_covers_keep_points_across_the_antimeridian_and_exclude_other_partitions() {
    let cases = [(179.999, 0.0, -179.999, 0.0), (34.0, 90.0, -31.0, 90.0)];
    for (row_lon, row_lat, query_lon, query_lat) in cases {
        let mut work = GeometryWork::default();
        let row = Location::decode(&point(row_lon, row_lat), &mut work).unwrap();
        let origin = Location::decode(&point(query_lon, query_lat), &mut work).unwrap();
        let radius = Predicate::WithinDistance(500.0);
        assert_eq!(
            radius.evaluate(&row, &origin.geometry, None, &mut work).unwrap(),
            Some(None)
        );
        let prepared = radius.prepare_geometry(&origin.geometry, &mut work).unwrap();
        let cover = radius.cover(&prepared, &mut work).unwrap().unwrap();
        let selected = Partition::of([b"east".as_slice()]);
        let elsewhere = Partition::of([b"west".as_slice()]);
        let wanted = Iid([1; 16]);
        let memory = MemoryBudget::new(4 << 20);
        let mut file = Postings::new(&memory).unwrap();
        file.append(selected, wanted, row.keys(&mut work).unwrap())
            .unwrap();
        file.append(elsewhere, Iid([2; 16]), row.keys(&mut work).unwrap())
            .unwrap();
        let output = tempfile::NamedTempFile::new().unwrap();
        let (length, stats) = file
            .write(
                output.reopen().unwrap(),
                &tokio_util::sync::CancellationToken::new(),
            )
            .unwrap();
        let mut bytes = std::fs::read(output.path()).unwrap();
        assert_eq!(length, bytes.len() as u64);
        assert!(bytes.starts_with(b"ARROW1"));
        let mut hits = Vec::new();
        checked_candidates(
            bytes.clone(),
            &stats,
            Some(selected),
            &cover,
            &memory,
            || Ok(()),
            |iid| {
                hits.push(iid);
                Ok(())
            },
        ).await
        .unwrap();
        assert_eq!(hits, [wanted]);
        hits.clear();
        checked_candidates(
            bytes.clone(),
            &stats,
            None,
            &cover,
            &memory,
            || Ok(()),
            |iid| {
                hits.push(iid);
                Ok(())
            },
        ).await
        .unwrap();
        hits.sort_unstable();
        assert_eq!(hits, [wanted, Iid([2; 16])]);
        let error = checked_candidates(
            bytes.clone(),
            &stats,
            None,
            &cover,
            &memory,
            || Err(postings::cancelled()),
            |_| panic!("cancelled posting traversal produced a candidate"),
        ).await
        .unwrap_err();
        assert_eq!(error.code, "rtdb.spatial/cancelled");
        *bytes.last_mut().unwrap() ^= 1;
        assert!(
            checked_candidates(
                bytes,
                &stats,
                Some(selected),
                &cover,
                &memory,
                || Ok(()),
                |_| Ok(())
            ).await
            .is_err()
        );
    }
}

#[tokio::test]
async fn spatial_arrow_postings_seek_across_page_boundaries() {
    let memory = MemoryBudget::new(32 << 20);
    let partition = Partition::of([b"area".as_slice()]);
    let iid = Iid([9; 16]);
    let boundary = 16_384;
    let write = |keys, duplicates| {
        let mut postings = Postings::new(&memory).unwrap();
        postings.append(partition, iid, keys).unwrap();
        if duplicates {
            postings.append(partition, iid, vec![boundary, boundary - 1]).unwrap();
        }
        let file = tempfile::NamedTempFile::new().unwrap();
        let (_, stats) = postings.write(
            file.reopen().unwrap(),
            &tokio_util::sync::CancellationToken::new(),
        ).unwrap();
        (std::fs::read(file.path()).unwrap(), stats)
    };

    let (bytes, stats) = write((0..=boundary + 1).collect(), false);
    let (reordered, reordered_stats) = write((0..=boundary + 1).rev().collect(), true);
    assert_eq!(bytes, reordered);
    assert_eq!(stats, reordered_stats);
    let mut selected = Vec::new();
    checked_candidates(
        bytes,
        &stats,
        Some(partition),
        &[geometry::KeyRange {
            start: boundary - 1,
            end: boundary,
        }],
        &memory,
        || Ok(()),
        |iid| {
            selected.push(iid);
            Ok(())
        },
    ).await
    .unwrap();
    assert_eq!(selected, [iid, iid]);
}


#[test]
fn spatial_arrow_bijection_images_borrow_fields_and_preserve_coordinate_bits() {
    use geometry::{Scalar, ValueView};
    use rtdb_types::SpatialGeometryEncoding;

    fn float(out: &mut Vec<u8>, value: f64) {
        out.push(5);
        let bits = value.to_bits();
        out.extend_from_slice(&(if bits >> 63 == 0 { bits ^ (1 << 63) } else { !bits }).to_be_bytes());
    }

    let longitude = 1.000_000_000_000_000_2;
    let mut image = b"\rcoordinates\0\x01\x0c".to_vec();
    float(&mut image, longitude);
    float(&mut image, -0.0);
    image.extend_from_slice(b"\0\0type\0\x01\x09Point\0\x01\0\0");
    let mut work = GeometryWork::default();
    let decoded = super::bijection_geometry::decode(&image, &mut work).unwrap();
    assert!(matches!(decoded.field("type").unwrap().text(), Some(std::borrow::Cow::Borrowed("Point"))));
    assert!(matches!(decoded.field("coordinates").unwrap().array().unwrap().next().unwrap().scalar(),
        Scalar::Float(value) if value.to_bits() == longitude.to_bits()));
    let loc = Location::decode_stored(&image, SpatialGeometryEncoding::BijectionGeometry, &mut work)
        .unwrap().unwrap();
    assert_eq!(loc.geometry.wkb(), point(longitude, 0.0)[1..]);

    image.push(0);
    let error = Location::decode_stored(&image, SpatialGeometryEncoding::BijectionGeometry, &mut work).err().unwrap();
    assert_eq!(error.message.as_deref(), Some("Bijection geometry has trailing bytes"));
}
