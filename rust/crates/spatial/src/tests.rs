use geometry::{Coords, Geometry, GeometrySpace, GeometryWork, Kind};
use rtdb_columnar::MemoryBudget;
use rtdb_types::Iid;

use crate::{
    Predicate,
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

fn checked_candidates(
    bytes: Vec<u8>,
    digest: &[u8],
    partition: Option<Partition>,
    ranges: &[geometry::KeyRange],
    memory: &std::sync::Arc<MemoryBudget>,
    check: impl Fn() -> rtdb_types::Result<()>,
    insert: impl FnMut(Iid) -> rtdb_types::Result<()>,
) -> rtdb_types::Result<()> {
    postings::verify_digest(postings::sha256(&bytes, &check)?, digest)?;
    postings::candidates(bytes, partition, ranges, memory, check, insert)
}

#[test]
fn spatial_arrow_posting_covers_keep_points_across_the_antimeridian_and_exclude_other_partitions() {
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
        let (length, digest) = file
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
            &digest,
            Some(selected),
            &cover,
            &memory,
            || Ok(()),
            |iid| {
                hits.push(iid);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(hits, [wanted]);
        hits.clear();
        checked_candidates(
            bytes.clone(),
            &digest,
            None,
            &cover,
            &memory,
            || Ok(()),
            |iid| {
                hits.push(iid);
                Ok(())
            },
        )
        .unwrap();
        hits.sort_unstable();
        assert_eq!(hits, [wanted, Iid([2; 16])]);
        let error = checked_candidates(
            bytes.clone(),
            &digest,
            None,
            &cover,
            &memory,
            || Err(postings::cancelled()),
            |_| panic!("cancelled posting traversal produced a candidate"),
        )
        .unwrap_err();
        assert_eq!(error.code, "rtdb.spatial/cancelled");
        bytes[0] ^= 1;
        assert!(
            checked_candidates(
                bytes,
                &digest,
                Some(selected),
                &cover,
                &memory,
                || Ok(()),
                |_| Ok(())
            )
            .is_err()
        );
    }
}

#[test]
fn spatial_arrow_postings_seek_across_page_boundaries() {
    let memory = MemoryBudget::new(32 << 20);
    let partition = Partition::of([b"area".as_slice()]);
    let iid = Iid([9; 16]);
    let mut postings = Postings::new(&memory).unwrap();
    let boundary = 16_384;
    postings
        .append(partition, iid, (0..=boundary + 1).rev().collect())
        .unwrap();
    postings
        .append(partition, iid, vec![boundary, boundary - 1])
        .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let (_, digest) = postings
        .write(
            file.reopen().unwrap(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .unwrap();
    let mut selected = Vec::new();
    checked_candidates(
        std::fs::read(file.path()).unwrap(),
        &digest,
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
    )
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
