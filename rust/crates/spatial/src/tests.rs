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

#[test]
fn posting_covers_keep_points_across_the_antimeridian_and_exclude_other_partitions() {
    let cases = [(179.999, 0.0, -179.999, 0.0), (34.0, 90.0, -31.0, 90.0)];
    for (row_lon, row_lat, query_lon, query_lat) in cases {
        let mut work = GeometryWork::default();
        let row = Location::decode(&point(row_lon, row_lat), &mut work).unwrap();
        let origin = Location::decode(&point(query_lon, query_lat), &mut work).unwrap();
        let radius = Predicate::WithinDistance(500.0);
        assert_eq!(
            radius.evaluate(&row, &origin, &mut work).unwrap(),
            Some(None)
        );
        let cover = radius.cover(&origin, &mut work).unwrap().unwrap();
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
        postings::candidates(
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
        postings::candidates(
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
        let error = postings::candidates(
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
            postings::candidates(
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
fn spatial_postings_seek_across_arrow_page_boundaries() {
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
    postings::candidates(
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
