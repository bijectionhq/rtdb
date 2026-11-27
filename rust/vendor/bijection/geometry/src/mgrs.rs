//! UTM and UPS on the WGS84 ellipsoid, and the Military Grid Reference
//! System (MGRS) and US National Grid (USNG) written on them.
//!
//! UTM is the transverse Mercator projection with Krüger's series to n^6
//! (Karney 2011, the order GeographicLib's `TransverseMercator` and PROJ's
//! exact transverse Mercator use): nanometre accuracy within a zone. UPS is
//! the polar stereographic projection with scale 0.994. Zones, bands and the
//! 100 km square letters follow GEOTRANS, with the Norway (32V) and Svalbard
//! (31X to 37X) zone exceptions; latitudes from 84°N and below 80°S are UPS
//! (zones A, B, Y and Z).
//!
//! The arithmetic is the TypeScript geometry library's and `formats.py`'s,
//! operation for operation and through `libm`, so a reference's digits
//! truncate at the same metre and its corner has the same bits everywhere.
use std::f64::consts::PI;

use crate::{
    geodesy::{
        ECCENTRICITY_SQUARED,
        SEMI_MAJOR_AXIS_METRES,
        THIRD_FLATTENING,
    },
    Pos,
};

const A: f64 = SEMI_MAJOR_AXIS_METRES;
const DEGREE: f64 = PI / 180.0;
/// UTM central scale.
const UTM_K0: f64 = 0.9996;
/// UPS scale at the pole.
const UPS_K0: f64 = 0.994;
const FALSE_EASTING: f64 = 500_000.0;
const SOUTH_FALSE_NORTHING: f64 = 10_000_000.0;
const UPS_FALSE_ORIGIN: f64 = 2_000_000.0;
const SQUARE_METRES: f64 = 100_000.0;

const N: f64 = THIRD_FLATTENING;
const N2: f64 = N * N;
const N3: f64 = N2 * N;
const N4: f64 = N3 * N;
const N5: f64 = N4 * N;
const N6: f64 = N5 * N;
/// The rectifying radius.
const TM_A: f64 = (A / (1.0 + N)) * (1.0 + N2 / 4.0 + N4 / 64.0 + N6 / 256.0);
/// Krüger's forward series: conformal latitude to rectifying coordinates.
const ALPHA: [f64; 6] = [
    N / 2.0 - (2.0 * N2) / 3.0 + (5.0 * N3) / 16.0 + (41.0 * N4) / 180.0 - (127.0 * N5) / 288.0
        + (7891.0 * N6) / 37800.0,
    (13.0 * N2) / 48.0 - (3.0 * N3) / 5.0 + (557.0 * N4) / 1440.0 + (281.0 * N5) / 630.0
        - (1983433.0 * N6) / 1935360.0,
    (61.0 * N3) / 240.0 - (103.0 * N4) / 140.0
        + (15061.0 * N5) / 26880.0
        + (167603.0 * N6) / 181440.0,
    (49561.0 * N4) / 161280.0 - (179.0 * N5) / 168.0 + (6601661.0 * N6) / 7257600.0,
    (34729.0 * N5) / 80640.0 - (3418889.0 * N6) / 1995840.0,
    (212378941.0 * N6) / 319334400.0,
];
/// Krüger's inverse series.
const BETA: [f64; 6] = [
    N / 2.0 - (2.0 * N2) / 3.0 + (37.0 * N3) / 96.0 - N4 / 360.0 - (81.0 * N5) / 512.0
        + (96199.0 * N6) / 604800.0,
    N2 / 48.0 + N3 / 15.0 - (437.0 * N4) / 1440.0 + (46.0 * N5) / 105.0
        - (1118711.0 * N6) / 3870720.0,
    (17.0 * N3) / 480.0 - (37.0 * N4) / 840.0 - (209.0 * N5) / 4480.0 + (5569.0 * N6) / 90720.0,
    (4397.0 * N4) / 161280.0 - (11.0 * N5) / 504.0 - (830251.0 * N6) / 7257600.0,
    (4583.0 * N5) / 161280.0 - (108847.0 * N6) / 3991680.0,
    (20648693.0 * N6) / 638668800.0,
];
/// Conformal to geodetic latitude.
const DELTA: [f64; 6] = [
    2.0 * N - (2.0 * N2) / 3.0 - 2.0 * N3 + (116.0 * N4) / 45.0 + (26.0 * N5) / 45.0
        - (2854.0 * N6) / 675.0,
    (7.0 * N2) / 3.0 - (8.0 * N3) / 5.0 - (227.0 * N4) / 45.0
        + (2704.0 * N5) / 315.0
        + (2323.0 * N6) / 945.0,
    (56.0 * N3) / 15.0 - (136.0 * N4) / 35.0 - (1262.0 * N5) / 105.0 + (73814.0 * N6) / 2835.0,
    (4279.0 * N4) / 630.0 - (332.0 * N5) / 35.0 - (399572.0 * N6) / 14175.0,
    (4174.0 * N5) / 315.0 - (144838.0 * N6) / 6237.0,
    (601676.0 * N6) / 22275.0,
];

const BANDS: &[u8; 20] = b"CDEFGHJKLMNPQRSTUVWX";
const ROWS: &[u8; 20] = b"ABCDEFGHJKLMNPQRSTUV";
const COLUMN_SETS: [&[u8; 8]; 3] = [b"STUVWXYZ", b"ABCDEFGH", b"JKLMNPQR"];
/// The lowest northing of each band's 2,000 km cycle, in band order.
const BAND_MIN_NORTHING: [f64; 20] = [
    1_100_000.0,
    2_000_000.0,
    2_800_000.0,
    3_700_000.0,
    4_600_000.0,
    5_500_000.0,
    6_400_000.0,
    7_300_000.0,
    8_200_000.0,
    9_100_000.0,
    0.0,
    800_000.0,
    1_700_000.0,
    2_600_000.0,
    3_500_000.0,
    4_400_000.0,
    5_300_000.0,
    6_200_000.0,
    7_000_000.0,
    7_900_000.0,
];
/// The longest reference text read, whitespace included.
pub const MAX_TEXT_BYTES: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hemisphere {
    North,
    South,
}

/// A projected position in metres.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Projected {
    pub easting: f64,
    pub northing: f64,
}

/// The projection an MGRS grid zone is drawn on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Projection {
    Utm { zone: u8, hemisphere: Hemisphere },
    Ups { hemisphere: Hemisphere },
}

/// Why text names no MGRS square, or a position has no reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MgrsRefusal {
    /// Not zone, band, square letters and an even count of up to 10 digits.
    Syntax,
    /// Well formed, but no square of the grid, or a position outside it.
    Coordinate,
}

/// Digits per axis, 0 (the 100 km square) to 5 (1 m).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Digits(u8);

impl Digits {
    pub const METRE: Digits = Digits(5);

    pub fn new(digits: u8) -> Option<Self> {
        (digits <= 5).then_some(Self(digits))
    }

    pub fn get(self) -> u8 {
        self.0
    }

    fn square_metres(self) -> f64 {
        [100_000.0, 10_000.0, 1_000.0, 100.0, 10.0, 1.0][self.0 as usize]
    }
}

/// How a reference is written: MGRS's compact `4QFJ1234567889`, or the four
/// space-separated parts USNG always uses, `4Q FJ 12345 67889`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Spelling {
    Compact,
    Spaced,
}

/// The UTM zone of a WGS84 position, with the Norway and Svalbard
/// exceptions; 180°E is zone 60.
pub fn utm_zone(position: Pos) -> u8 {
    let [lon, lat] = position;
    let mut zone = (libm::floor((lon + 180.0) / 6.0) as i64 + 1).clamp(1, 60) as u8;
    if (56.0..64.0).contains(&lat) && (3.0..12.0).contains(&lon) {
        zone = 32;
    }
    if (72.0..=84.0).contains(&lat) && (0.0..42.0).contains(&lon) {
        zone = if lon < 9.0 {
            31
        } else if lon < 21.0 {
            33
        } else if lon < 33.0 {
            35
        } else {
            37
        };
    }
    zone
}

fn central_meridian(zone: u8) -> f64 {
    f64::from(zone) * 6.0 - 183.0
}

/// WGS84 [longitude, latitude] to UTM in `zone`. The series is accurate
/// within a zone's area of use and its MGRS overlap; the caller chooses the
/// zone.
pub fn utm_forward(position: Pos, zone: u8, hemisphere: Hemisphere) -> Projected {
    let [lon, lat] = position;
    let e = (2.0 * N.sqrt()) / (1.0 + N);
    let phi = lat * DEGREE;
    let dl = (lon - central_meridian(zone)) * DEGREE;
    let s = libm::sin(phi);
    let t = libm::sinh(libm::atanh(s) - e * libm::atanh(e * s));
    let xi = libm::atan2(t, libm::cos(dl));
    let eta = libm::atanh(libm::sin(dl) / (1.0 + t * t).sqrt());
    let (mut x, mut y) = (eta, xi);
    for (j, alpha) in ALPHA.iter().enumerate() {
        let k = (2 * (j + 1)) as f64;
        x += alpha * libm::cos(k * xi) * libm::sinh(k * eta);
        y += alpha * libm::sin(k * xi) * libm::cosh(k * eta);
    }
    Projected {
        easting: FALSE_EASTING + UTM_K0 * TM_A * x,
        northing: match hemisphere {
            Hemisphere::North => 0.0,
            Hemisphere::South => SOUTH_FALSE_NORTHING,
        } + UTM_K0 * TM_A * y,
    }
}

/// UTM in `zone` to WGS84 [longitude, latitude], unchecked: MGRS squares at
/// a band or zone edge extend past the zone, and MGRS checks its own bands.
pub fn utm_inverse(projected: Projected, zone: u8, hemisphere: Hemisphere) -> Pos {
    let false_northing = match hemisphere {
        Hemisphere::North => 0.0,
        Hemisphere::South => SOUTH_FALSE_NORTHING,
    };
    let xi = (projected.northing - false_northing) / (UTM_K0 * TM_A);
    let eta = (projected.easting - FALSE_EASTING) / (UTM_K0 * TM_A);
    let (mut xi1, mut eta1) = (xi, eta);
    for (j, beta) in BETA.iter().enumerate() {
        let k = (2 * (j + 1)) as f64;
        xi1 -= beta * libm::sin(k * xi) * libm::cosh(k * eta);
        eta1 -= beta * libm::cos(k * xi) * libm::sinh(k * eta);
    }
    let chi = libm::asin(libm::sin(xi1) / libm::cosh(eta1));
    let mut phi = chi;
    for (j, delta) in DELTA.iter().enumerate() {
        phi += delta * libm::sin(((2 * (j + 1)) as f64) * chi);
    }
    let lon = central_meridian(zone) + libm::atan2(libm::sinh(eta1), libm::cos(xi1)) / DEGREE;
    [lon, phi / DEGREE]
}

/// The UPS constant C = sqrt((1 + e)^(1 + e) (1 − e)^(1 − e)) and e.
fn ups_constants() -> (f64, f64) {
    let e = ECCENTRICITY_SQUARED.sqrt();
    (
        (libm::pow(1.0 + e, 1.0 + e) * libm::pow(1.0 - e, 1.0 - e)).sqrt(),
        e,
    )
}

/// WGS84 [longitude, latitude] to UPS; the hemisphere is the latitude's.
pub fn ups_forward(position: Pos) -> Projected {
    let [lon, lat] = position;
    let (c, e) = ups_constants();
    let phi = lat.abs() * DEGREE;
    let s = libm::sin(phi);
    let t = libm::tan(PI / 4.0 - phi / 2.0) / libm::pow((1.0 - e * s) / (1.0 + e * s), e / 2.0);
    let rho = (2.0 * A * UPS_K0 * t) / c;
    let lambda = lon * DEGREE;
    Projected {
        easting: UPS_FALSE_ORIGIN + rho * libm::sin(lambda),
        northing: UPS_FALSE_ORIGIN + (if lat > 0.0 { -1.0 } else { 1.0 }) * rho * libm::cos(lambda),
    }
}

/// UPS in `hemisphere` to WGS84 [longitude, latitude]; the pole is at
/// longitude 0.
pub fn ups_inverse(projected: Projected, hemisphere: Hemisphere) -> Pos {
    let (c, _) = ups_constants();
    let north = hemisphere == Hemisphere::North;
    let dx = projected.easting - UPS_FALSE_ORIGIN;
    let dy = projected.northing - UPS_FALSE_ORIGIN;
    let rho = libm::hypot(dx, dy);
    let t = (rho * c) / (2.0 * A * UPS_K0);
    let chi = PI / 2.0 - 2.0 * libm::atan(t);
    let e2 = ECCENTRICITY_SQUARED;
    let e4 = e2 * e2;
    let e6 = e4 * e2;
    let e8 = e4 * e4;
    let phi = chi
        + (e2 / 2.0 + (5.0 * e4) / 24.0 + e6 / 12.0 + (13.0 * e8) / 360.0) * libm::sin(2.0 * chi)
        + ((7.0 * e4) / 48.0 + (29.0 * e6) / 240.0 + (811.0 * e8) / 11520.0) * libm::sin(4.0 * chi)
        + ((7.0 * e6) / 120.0 + (81.0 * e8) / 1120.0) * libm::sin(6.0 * chi)
        + ((4279.0 * e8) / 161280.0) * libm::sin(8.0 * chi);
    let lambda = if rho == 0.0 {
        0.0
    } else {
        libm::atan2(dx, if north { -dy } else { dy })
    };
    [
        lambda / DEGREE,
        (if north { 1.0 } else { -1.0 }) * (phi / DEGREE),
    ]
}

/// GEOTRANS's UPS letters for a zone: lowest and highest column letter,
/// highest row letter, false easting and false northing of its letters.
struct UpsLetters {
    low_column: u8,
    high_column: u8,
    high_row: u8,
    false_easting: f64,
    false_northing: f64,
}

fn ups_letters(zone: u8) -> Option<UpsLetters> {
    let (low_column, high_column, high_row, false_easting, false_northing) = match zone {
        b'A' => (b'J', b'Z', b'Z', 800_000.0, 800_000.0),
        b'B' => (b'A', b'R', b'Z', 2_000_000.0, 800_000.0),
        b'Y' => (b'J', b'Z', b'P', 800_000.0, 1_300_000.0),
        b'Z' => (b'A', b'J', b'P', 2_000_000.0, 1_300_000.0),
        _ => return None,
    };
    Some(UpsLetters {
        low_column,
        high_column,
        high_row,
        false_easting,
        false_northing,
    })
}

/// Whether a latitude is on the UTM grid; the rest is UPS.
fn on_utm(latitude: f64) -> bool {
    (-80.0..84.0).contains(&latitude)
}

/// The MGRS reference of the square of `digits` containing a WGS84
/// [longitude, latitude]: digits truncate, so the text names the square the
/// position lies in. The zone number has no leading zero.
pub fn reference(position: Pos, digits: Digits, spelling: Spelling) -> Result<String, MgrsRefusal> {
    let [lon, lat] = position;
    if !(lon.is_finite() && lat.is_finite() && lon.abs() <= 180.0 && lat.abs() <= 90.0) {
        return Err(MgrsRefusal::Coordinate);
    }
    let (zone, letters, projected) = if on_utm(lat) {
        let zone = utm_zone(position);
        let band = BANDS[(libm::floor((lat + 80.0) / 8.0) as usize).min(19)];
        let hemisphere = if lat < 0.0 {
            Hemisphere::South
        } else {
            Hemisphere::North
        };
        let projected = utm_forward(position, zone, hemisphere);
        let column = (libm::floor(projected.easting / SQUARE_METRES) as i64 - 1)
            .try_into()
            .ok()
            .and_then(|index: usize| COLUMN_SETS[usize::from(zone % 3)].get(index))
            .ok_or(MgrsRefusal::Coordinate)?;
        let row = ROWS[(libm::floor(projected.northing / SQUARE_METRES) as usize
            + if zone % 2 == 1 { 0 } else { 5 })
            % 20];
        (format!("{zone}{}", band as char), [*column, row], projected)
    } else {
        let projected = ups_forward(position);
        let east = projected.easting >= UPS_FALSE_ORIGIN;
        let zone = match (lat > 0.0, east) {
            (true, true) => b'Z',
            (true, false) => b'Y',
            (false, true) => b'B',
            (false, false) => b'A',
        };
        let table = ups_letters(zone).expect("a UPS zone");
        let mut row =
            libm::floor((projected.northing - table.false_northing) / SQUARE_METRES) as i64;
        if row > code(b'H') {
            row += 1;
        }
        if row > code(b'N') {
            row += 1;
        }
        let mut column = code(table.low_column)
            + libm::floor((projected.easting - table.false_easting) / SQUARE_METRES) as i64;
        if east {
            if column > code(b'C') {
                column += 2;
            }
            if column > code(b'H') {
                column += 1;
            }
            if column > code(b'L') {
                column += 3;
            }
        } else {
            if column > code(b'L') {
                column += 3;
            }
            if column > code(b'U') {
                column += 2;
            }
        }
        (
            (zone as char).to_string(),
            [letter(column)?, letter(row)?],
            projected,
        )
    };
    let width = usize::from(digits.get());
    let scale = digits.square_metres() as i64;
    let axis = |metres: f64| {
        let within = (libm::floor(metres) as i64).rem_euclid(100_000) / scale;
        format!("{within:0width$}")
    };
    let square = format!("{}{}", letters[0] as char, letters[1] as char);
    let mut parts = vec![zone, square];
    if width > 0 {
        parts.push(axis(projected.easting));
        parts.push(axis(projected.northing));
    }
    Ok(parts.join(match spelling {
        Spelling::Compact => "",
        Spelling::Spaced => " ",
    }))
}

/// The USNG reference of a WGS84 position: MGRS on the UTM zones, spaced.
/// The grid covers 80°S to 84°N and has no polar zones.
pub fn usng(position: Pos, digits: Digits) -> Result<String, MgrsRefusal> {
    if !on_utm(position[1]) {
        return Err(MgrsRefusal::Coordinate);
    }
    reference(position, digits, Spelling::Spaced)
}

fn code(letter: u8) -> i64 {
    i64::from(letter) - i64::from(b'A')
}

fn letter(code: i64) -> Result<u8, MgrsRefusal> {
    u8::try_from(code)
        .ok()
        .filter(|code| *code < 26)
        .map(|code| b'A' + code)
        .ok_or(MgrsRefusal::Coordinate)
}

/// The square an MGRS reference names: its south-west corner in its
/// projection and its side.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Square {
    pub projection: Projection,
    pub corner: Projected,
    pub size_metres: f64,
}

impl Square {
    /// The WGS84 [longitude, latitude] of the projected position `east` and
    /// `north` metres from the south-west corner. Longitudes are the
    /// projection's own, so a square at the antimeridian may pass ±180°.
    pub fn at(&self, east: f64, north: f64) -> Pos {
        let projected = Projected {
            easting: self.corner.easting + east,
            northing: self.corner.northing + north,
        };
        match self.projection {
            Projection::Utm { zone, hemisphere } => utm_inverse(projected, zone, hemisphere),
            Projection::Ups { hemisphere } => ups_inverse(projected, hemisphere),
        }
    }

    /// The four corners anticlockwise from the south-west one, closed. The
    /// sides are straight in the projection, so between corners this is the
    /// square to within its curvature.
    pub fn ring(&self) -> [Pos; 5] {
        let size = self.size_metres;
        [
            self.at(0.0, 0.0),
            self.at(size, 0.0),
            self.at(size, size),
            self.at(0.0, size),
            self.at(0.0, 0.0),
        ]
    }
}

/// The square an MGRS or USNG reference names. Spaces, tabs and line breaks
/// are ignored and letters may be lower case; 0 to 5 digits per axis, and a
/// one- or two-digit zone number (none for UPS). A reference is checked
/// against its corner: a UTM corner within half a degree of its band and
/// nine degrees of its central meridian, a UPS corner poleward of 83.5° or
/// 79.5°.
pub fn square(text: &str) -> Result<Square, MgrsRefusal> {
    let text: String = text
        .chars()
        .filter(|c| !matches!(c, ' ' | '\t' | '\r' | '\n'))
        .collect::<String>()
        .to_uppercase();
    let bytes = text.as_bytes();
    let zone_length = bytes.iter().take_while(|b| b.is_ascii_digit()).count();
    if zone_length > 2 {
        return Err(MgrsRefusal::Syntax);
    }
    let grid_letter = |index: usize| match bytes.get(index) {
        Some(&b) if b.is_ascii_uppercase() && b != b'I' && b != b'O' => Ok(b),
        _ => Err(MgrsRefusal::Syntax),
    };
    let first = grid_letter(zone_length)?;
    let column = grid_letter(zone_length + 1)?;
    let row = grid_letter(zone_length + 2)?;
    let digits = &bytes[zone_length + 3..];
    if !digits.iter().all(u8::is_ascii_digit) || digits.len() % 2 == 1 || digits.len() > 10 {
        return Err(MgrsRefusal::Syntax);
    }
    let precision = Digits::new((digits.len() / 2) as u8).ok_or(MgrsRefusal::Syntax)?;
    let size_metres = precision.square_metres();
    let number = |digits: &[u8]| {
        digits
            .iter()
            .fold(0.0, |value, digit| value * 10.0 + f64::from(digit - b'0'))
            * size_metres
    };
    let half = digits.len() / 2;
    let (east, north) = (number(&digits[..half]), number(&digits[half..]));
    let refused = Err(MgrsRefusal::Coordinate);
    if zone_length == 0 {
        let Some(table) = ups_letters(first) else {
            return refused;
        };
        if code(column) < code(table.low_column)
            || code(column) > code(table.high_column)
            || b"DEMNVW".contains(&column)
            || code(row) > code(table.high_row)
        {
            return refused;
        }
        let mut northing = (code(row) * 100_000) as f64 + table.false_northing;
        if code(row) > code(b'I') {
            northing -= SQUARE_METRES;
        }
        if code(row) > code(b'O') {
            northing -= SQUARE_METRES;
        }
        let mut easting =
            ((code(column) - code(table.low_column)) * 100_000) as f64 + table.false_easting;
        if table.low_column == b'A' {
            if code(column) > code(b'C') {
                easting -= 200_000.0;
            }
            if code(column) > code(b'I') {
                easting -= 100_000.0;
            }
            if code(column) > code(b'L') {
                easting -= 300_000.0;
            }
        } else {
            if code(column) > code(b'L') {
                easting -= 300_000.0;
            }
            if code(column) > code(b'U') {
                easting -= 200_000.0;
            }
        }
        let hemisphere = if matches!(first, b'Y' | b'Z') {
            Hemisphere::North
        } else {
            Hemisphere::South
        };
        let square = Square {
            projection: Projection::Ups { hemisphere },
            corner: Projected {
                easting: easting + east,
                northing: northing + north,
            },
            size_metres,
        };
        let [_, lat] = square.at(0.0, 0.0);
        let outside = match hemisphere {
            Hemisphere::North => lat < 83.5,
            Hemisphere::South => lat > -79.5,
        };
        return if outside { refused } else { Ok(square) };
    }
    let zone = text[..zone_length]
        .parse::<u8>()
        .map_err(|_| MgrsRefusal::Syntax)?;
    let Some(band) = BANDS.iter().position(|band| *band == first) else {
        return refused;
    };
    if !(1..=60).contains(&zone) {
        return refused;
    }
    let (Some(c), Some(r)) = (
        COLUMN_SETS[usize::from(zone % 3)]
            .iter()
            .position(|letter| *letter == column),
        ROWS.iter().position(|letter| *letter == row),
    ) else {
        return refused;
    };
    let r = if zone % 2 == 0 { (r + 15) % 20 } else { r };
    let mut northing = (r * 100_000) as f64;
    while northing < BAND_MIN_NORTHING[band] {
        northing += 2_000_000.0;
    }
    let hemisphere = if first < b'N' {
        Hemisphere::South
    } else {
        Hemisphere::North
    };
    let square = Square {
        projection: Projection::Utm { zone, hemisphere },
        corner: Projected {
            easting: ((c + 1) * 100_000) as f64 + east,
            northing: northing + north,
        },
        size_metres,
    };
    let [lon, lat] = square.at(0.0, 0.0);
    let low = -80.0 + band as f64 * 8.0;
    let high = if first == b'X' { 84.0 } else { low + 8.0 };
    if lat < low - 0.5 || lat > high + 0.5 || (lon - central_meridian(zone)).abs() > 9.0 {
        return refused;
    }
    Ok(square)
}

#[cfg(test)]
#[path = "mgrs_tests.rs"]
mod tests;
