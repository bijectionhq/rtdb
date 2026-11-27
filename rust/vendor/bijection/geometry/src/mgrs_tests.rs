use super::*;

const TOLERANCE: f64 = 1e-8;

fn spaced(position: Pos) -> String {
    reference(position, Digits::METRE, Spelling::Spaced).expect("a reference")
}

fn corner(text: &str) -> Pos {
    square(text)
        .unwrap_or_else(|refusal| panic!("{text}: {refusal:?}"))
        .at(0.0, 0.0)
}

fn close(actual: Pos, expected: Pos, tolerance: f64, context: &str) {
    for (a, e) in actual.iter().zip(expected) {
        assert!(
            (a - e).abs() <= tolerance,
            "{context}: {actual:?} {expected:?}"
        );
    }
}

/// Foundry's geospatial examples and formats.py's pinned values, which the
/// TypeScript library also passes. Position, reference and the reference's
/// south-west corner.
const FORMAT_CASES: [(Pos, &str, Pos); 18] = [
    (
        [0.9996456505181999, 88.99999659707431],
        "Z AF 01937 88989",
        [0.9996366474211815, 88.99998759202673],
    ),
    (
        [-157.91608117421092, 21.409796671597924],
        "4Q FJ 12345 67889",
        [-157.91608124083402, 21.409787638631528],
    ),
    (
        [-157.93921670599434, 21.338665624760598],
        "4Q FJ 10000 59999",
        [-157.9392167709575, 21.338656591657095],
    ),
    (
        [-157.91652127483704, 21.40898645576642],
        "4Q FJ 12300 67799",
        [-157.9165213414303, 21.408977422797914],
    ),
    (
        [2.3522, 48.8566],
        "31U DQ 52482 11717",
        [2.3521927587433393, 48.85659836821626],
    ),
    (
        [151.2153, -33.8568],
        "56H LH 34900 52288",
        [151.21529370353971, -33.8568066979483],
    ),
    // Norway: zone 32 widens over 3°E to 12°E in band V.
    (
        [5.0, 60.0],
        "32V KM 76979 58157",
        [4.999983650885302, 59.9999976844381],
    ),
    // Svalbard: odd zones 31 to 37 in band X.
    (
        [15.0, 78.0],
        "33X WG 00000 58369",
        [15.0, 77.99999475067479],
    ),
    (
        [8.9, 78.0],
        "31X FG 36716 65261",
        [8.89996136189154, 77.9999958621625],
    ),
    (
        [40.0, 83.9],
        "37X EP 11863 17033",
        [39.999977166508984, 83.89999917202113],
    ),
    (
        [-60.0, -79.99],
        "21C VM 41810 18029",
        [-60.00001506396336, -79.99000667374122],
    ),
    (
        [-45.0, -85.0],
        "A UR 07232 92767",
        [-45.00007293854445, -85.0000023917124],
    ),
    (
        [120.0, -89.5],
        "B AM 48075 72243",
        [120.00079177372126, -89.49999616841706],
    ),
    (
        [-100.0, 86.0],
        "Y TH 62481 77146",
        [-99.9999486544282, 86.00000057355344],
    ),
    (
        [0.0, 0.0],
        "31N AA 66021 00000",
        [-3.976359147284114e-06, 0.0],
    ),
    (
        [179.999999, -1e-06],
        "60M ZE 33978 99999",
        [179.99999500201073, -9.034831087119473e-06],
    ),
    // 84°N is UPS; 80°S is UTM.
    ([0.0, 84.0], "Z AA 00000 33272", [0.0, 83.99999733807428]),
    (
        [0.0, -80.0],
        "31C DM 41867 16915",
        [-4.054849112700154e-05, -80.00000003170109],
    ),
];

#[test]
fn shared_references_truncate_and_read_back_to_their_corner() {
    for (position, text, expected) in FORMAT_CASES {
        assert_eq!(spaced(position), text, "{position:?}");
        assert_eq!(
            reference(position, Digits::METRE, Spelling::Compact).expect("a reference"),
            text.replace(' ', ""),
        );
        close(corner(text), expected, TOLERANCE, text);
        close(corner(&text.replace(' ', "")), expected, TOLERANCE, text);
        // A 1 m square's corner is within a metre and a half of the point.
        let [lon, lat] = corner(text);
        let metres = crate::geodesy::inverse(lat, lon, position[1], position[0]).s12;
        assert!(metres < 1.5, "{text}: {metres} m");
    }
    let hawaii = [-157.91608117421092, 21.409796671597924];
    let digits = |count| Digits::new(count).expect("digits");
    assert_eq!(
        reference(hawaii, digits(3), Spelling::Spaced),
        Ok("4Q FJ 123 678".into())
    );
    assert_eq!(
        reference(hawaii, digits(1), Spelling::Compact),
        Ok("4QFJ16".into())
    );
    assert_eq!(
        reference(hawaii, digits(0), Spelling::Compact),
        Ok("4QFJ".into())
    );
    assert_eq!(Digits::new(6), None);
}

/// `mgrsParse` of the shared cases: the south-west corner, or the refusal.
#[test]
fn shared_references_parse_or_refuse() {
    for (text, expected) in [
        ("ZAF0193788990", [0.9996456505182001, 88.9999965970743]),
        (
            "4Q FJ 12345 67890",
            [-157.91608117420762, 21.409796672217205],
        ),
        ("4Q FJ 1 6", [-157.93921670599093, 21.3386656253879]),
        ("4Q FJ 123 678", [-157.9165212748337, 21.40898645638579]),
        ("4qfj1234567890", [-157.91608117420762, 21.409796672217205]),
        (
            "31U DQ 48251 11932",
            [2.2944892452301513, 48.85819383792174],
        ),
        ("32V KK 0 0", [3.9719250747292794, 57.642136785615264]),
        ("4Q FJ", [-158.03912886390302, 20.797188997794894]),
        (" 04Q\tFJ\n", [-158.03912886390302, 20.797188997794894]),
    ] {
        close(corner(text), expected, TOLERANCE, text);
    }
    for (text, refusal) in [
        ("4Q FJ 1234 567", MgrsRefusal::Syntax),
        ("4Q FI 12345 67890", MgrsRefusal::Syntax),
        ("4Q FJ 123456 654321", MgrsRefusal::Syntax),
        ("104Q FJ", MgrsRefusal::Syntax),
        ("hello", MgrsRefusal::Syntax),
        ("", MgrsRefusal::Syntax),
        ("61Q FJ 1 1", MgrsRefusal::Coordinate),
        ("0Q FJ", MgrsRefusal::Coordinate),
        ("4Q FW 1 1", MgrsRefusal::Coordinate),
        ("4Q SJ 1 1", MgrsRefusal::Coordinate),
        ("ZDF 1 1", MgrsRefusal::Coordinate),
        ("ZAZ 1 1", MgrsRefusal::Coordinate),
        ("CAA", MgrsRefusal::Coordinate),
        ("4A FJ 1 1", MgrsRefusal::Coordinate),
        // Row A of an even zone is 1,500 km north: outside band N.
        ("60N ZA", MgrsRefusal::Coordinate),
    ] {
        assert_eq!(square(text), Err(refusal), "{text}");
    }
    assert_eq!(
        square("4Q FJ 1 6").map(|square| square.size_metres),
        Ok(10_000.0)
    );
    assert_eq!(
        square("4QFJ").map(|square| square.size_metres),
        Ok(100_000.0)
    );
}

/// The equator three degrees from a central meridian: the UTM grid's least
/// easting, 166,021.443 m, and the inverse back to the point.
#[test]
fn utm_matches_the_transverse_mercator_reference() {
    let projected = utm_forward([0.0, 0.0], 31, Hemisphere::North);
    assert!(
        (projected.easting - 166_021.443_081).abs() < 1e-3,
        "{projected:?}"
    );
    assert_eq!(projected.northing, 0.0);
    close(
        utm_inverse(projected, 31, Hemisphere::North),
        [0.0, 0.0],
        1e-12,
        "equator",
    );
    // Krüger's series to n^6 holds to nanometres across a zone and its
    // MGRS overlap: forward and inverse agree far below a millimetre.
    for zone in [1, 17, 31, 32, 60] {
        let lon0 = central_meridian(zone);
        for lat in [-80.0, -45.5, -0.25, 0.0, 33.3, 64.0, 84.0] {
            for offset in [-9.0, -3.0, 0.0, 1.5, 3.0, 9.0] {
                let hemisphere = if lat < 0.0 {
                    Hemisphere::South
                } else {
                    Hemisphere::North
                };
                let position = [lon0 + offset, lat];
                let back = utm_inverse(utm_forward(position, zone, hemisphere), zone, hemisphere);
                close(back, position, 1e-10, &format!("{zone} {position:?}"));
            }
        }
    }
    for lat in [84.0, 87.5, 90.0, -80.5, -89.0, -90.0] {
        for lon in [-180.0, -135.0, -0.5, 0.0, 45.0, 179.0] {
            let hemisphere = if lat > 0.0 {
                Hemisphere::North
            } else {
                Hemisphere::South
            };
            let back = ups_inverse(ups_forward([lon, lat]), hemisphere);
            close([back[1], 0.0], [lat, 0.0], 1e-10, &format!("{lon} {lat}"));
            if lat.abs() < 90.0 {
                let wrapped = if back[0] - lon > 180.0 {
                    back[0] - 360.0
                } else {
                    back[0]
                };
                close([wrapped, 0.0], [lon, 0.0], 1e-9, &format!("{lon} {lat}"));
            }
        }
    }
}

#[test]
fn zones_follow_the_norway_and_svalbard_exceptions() {
    let zone = |lon: f64, lat: f64| {
        let text = spaced([lon, lat]);
        text.split(' ').next().expect("zone").to_owned()
    };
    for (lon, lat, expected) in [
        (2.999, 60.0, "31V"),
        (3.0, 60.0, "32V"),
        (11.999, 60.0, "32V"),
        (12.0, 60.0, "33V"),
        (5.0, 55.999, "31U"),
        (5.0, 64.0, "31W"),
        (10.0, 71.999, "32W"),
        (8.999, 72.0, "31X"),
        (9.0, 72.0, "33X"),
        (20.999, 78.0, "33X"),
        (21.0, 78.0, "35X"),
        (32.999, 78.0, "35X"),
        (33.0, 78.0, "37X"),
        (41.999, 83.999, "37X"),
        (42.0, 78.0, "38X"),
        (-0.001, 78.0, "30X"),
        (-180.0, 10.0, "1P"),
        (180.0, 10.0, "60P"),
        (0.0, -80.0, "31C"),
        (0.0, 83.999_999, "31X"),
        (0.0, -80.000_001, "B"),
        (-0.001, -80.000_001, "A"),
        (0.0, 84.0, "Z"),
        (-0.001, 84.0, "Y"),
    ] {
        assert_eq!(zone(lon, lat), expected, "{lon} {lat}");
    }
}

#[test]
fn poles_and_the_antimeridian() {
    // Each pole is a corner of four UPS squares at the zones' false origin.
    assert_eq!(spaced([0.0, 90.0]), "Z AH 00000 00000");
    assert_eq!(spaced([-120.0, 90.0]), "Z AH 00000 00000");
    assert_eq!(spaced([-45.0, -90.0]), "B AN 00000 00000");
    assert_eq!(corner("ZAH"), [0.0, 90.0]);
    assert_eq!(corner("BAN0000000000"), [0.0, -90.0]);
    let pole = square("ZAH").expect("a square").ring();
    close(pole[1], [90.0, 89.09931014957627], TOLERANCE, "ZAH");
    close(pole[2], [135.0, 88.72625736726883], TOLERANCE, "ZAH");
    close(pole[3], [180.0, 89.09931014957627], TOLERANCE, "ZAH");
    // 180°E is zone 60 and 180°W zone 1; zone 1's corners pass the
    // antimeridian in its own longitudes.
    assert_eq!(spaced([180.0, 0.0]), "60N ZF 33978 00000");
    assert_eq!(spaced([-180.0, 0.0]), "1N AA 66021 00000");
    let west = square("1NAA6602100000").expect("a square");
    close(
        west.at(0.0, 0.0),
        [-180.00000397635915, 0.0],
        TOLERANCE,
        "1N AA",
    );
    close(
        west.at(1.0, 0.0),
        [-179.99999500201068, 0.0],
        TOLERANCE,
        "1N AA",
    );
    close(
        square("1NAA").expect("a square").at(0.0, 0.0),
        [-180.5923272025667, 0.0],
        TOLERANCE,
        "1N AA",
    );
}

/// Over a grid of positions from pole to pole: the 1 m square a reference
/// names holds its position (its centre has the same reference), and a
/// coarser reference truncates the finer one's digits.
#[test]
fn references_name_the_square_holding_their_position() {
    let mut count = 0;
    for i in 0..=25 {
        let lat = -89.9 + 7.19 * f64::from(i);
        for j in 0..=31 {
            let lon = -179.95 + 11.6 * f64::from(j);
            let text =
                reference([lon, lat], Digits::METRE, Spelling::Compact).expect("a reference");
            let named = square(&text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
            let [x, y] = named.at(0.5, 0.5);
            let centre = [if x > 180.0 { x - 360.0 } else { x }, y];
            assert_eq!(
                reference(centre, Digits::METRE, Spelling::Compact).as_deref(),
                Ok(text.as_str()),
                "{lon} {lat}"
            );
            let (grid, digits) = text.split_at(text.len() - 10);
            let (east, north) = digits.split_at(5);
            for kept in 0..=5 {
                let coarse = reference([lon, lat], Digits(kept), Spelling::Compact);
                let width = usize::from(kept);
                assert_eq!(
                    coarse,
                    Ok(format!("{grid}{}{}", &east[..width], &north[..width])),
                    "{lon} {lat}"
                );
            }
            count += 1;
        }
    }
    assert_eq!(count, 26 * 32);
}

#[test]
fn usng_is_spaced_mgrs_without_the_polar_zones() {
    let monument = [-77.0352, 38.88949];
    let digits = |count| Digits::new(count).expect("digits");
    assert_eq!(
        usng(monument, Digits::METRE),
        Ok("18S UJ 23486 06481".into())
    );
    assert_eq!(usng(monument, digits(4)), Ok("18S UJ 2348 0648".into()));
    assert_eq!(usng(monument, digits(1)), Ok("18S UJ 2 0".into()));
    assert_eq!(
        reference(monument, Digits::METRE, Spelling::Compact),
        Ok("18SUJ2348606481".into())
    );
    for text in [
        "18S UJ 23486 06481",
        "18suj2348606481",
        " 18S  UJ 23486\t06481 ",
    ] {
        close(corner(text), monument, 1e-4, text);
    }
    assert_eq!(
        usng([10.0, 85.0], Digits::METRE),
        Err(MgrsRefusal::Coordinate)
    );
    assert_eq!(
        usng([10.0, -81.0], Digits::METRE),
        Err(MgrsRefusal::Coordinate)
    );
    assert_eq!(
        reference([181.0, 0.0], Digits::METRE, Spelling::Compact),
        Err(MgrsRefusal::Coordinate)
    );
    assert_eq!(
        reference([0.0, f64::NAN], Digits::METRE, Spelling::Compact),
        Err(MgrsRefusal::Coordinate)
    );
}
