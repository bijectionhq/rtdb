//! The bounded ISO WKB codec shared by retained datasets and native queries.
//! The caller declares the coordinate space and dimensions; headers never infer
//! it. SRIDs, M, EWKB, trailing bytes and malformed topology refuse.
use crate::{
    geojson,
    Coords,
    Geometry,
    GeometryRefusal,
    GeometrySpace,
    GeometryWork,
    Gv,
    Kind,
    Limits,
    Pos,
};

const WKB_Z: i64 = 1000;

fn wkb_error(detail: &str) -> GeometryRefusal {
    crate::invalid(detail)
}

impl Geometry {
    /// Exact encoded allocation; valid empty Points still carry their NaNs.
    pub fn wkb_len(&self) -> usize {
        let width = if self.space.is_ground() { 16 } else { 24 };
        let points = |n: usize| 4 + n * width;
        let rings =
            |rings: &Vec<Vec<Pos>>| 4 + rings.iter().map(|ring| points(ring.len())).sum::<usize>();
        5 + match (&self.coords, self.kind) {
            (Coords::Empty, Kind::Point) => width,
            (Coords::Empty, _) => 4,
            (Coords::Point(_), _) => width,
            (Coords::MultiPoint(ps), _) => 4 + ps.len() * (5 + width),
            (Coords::Points(ps), Kind::LineString) => points(ps.len()),
            (Coords::Points(ps), _) => 4 + ps.len() * (5 + width),
            (Coords::Lines(rs), Kind::Polygon) => rings(rs),
            (Coords::Lines(ls), _) => {
                4 + ls.iter().map(|line| 5 + points(line.len())).sum::<usize>()
            },
            (Coords::Polygons(ps), _) => {
                4 + ps.iter().map(|polygon| 5 + rings(polygon)).sum::<usize>()
            },
            (Coords::Collection(members), _) => {
                4 + members.iter().map(Self::wkb_len).sum::<usize>()
            },
        }
    }

    /// Little-endian ISO WKB (ISO WKB Z with heights).
    pub fn wkb(&self) -> Vec<u8> {
        struct W<'a> {
            out: Vec<u8>,
            heights: Option<&'a [f64]>,
            next: usize,
        }
        impl<'a> W<'a> {
            fn header(&mut self, kind: Kind) {
                self.out.push(1);
                let z = if self.heights.is_some() { WKB_Z } else { 0 };
                self.out
                    .extend_from_slice(&((kind.wkb_code() + z) as u32).to_le_bytes());
            }

            fn count(&mut self, n: usize) {
                self.out.extend_from_slice(&(n as u32).to_le_bytes());
            }

            fn point(&mut self, p: &Pos) {
                self.out.extend_from_slice(&p[0].to_le_bytes());
                self.out.extend_from_slice(&p[1].to_le_bytes());
                if let Some(h) = self.heights {
                    self.out.extend_from_slice(&h[self.next].to_le_bytes());
                }
                self.next += 1;
            }

            fn empty_point(&mut self) {
                for _ in 0..if self.heights.is_some() { 3 } else { 2 } {
                    self.out.extend_from_slice(&f64::NAN.to_le_bytes());
                }
            }

            fn points(&mut self, ps: &[Pos]) {
                self.count(ps.len());
                ps.iter().for_each(|p| self.point(p));
            }

            fn rings(&mut self, rs: &[Vec<Pos>]) {
                self.count(rs.len());
                rs.iter().for_each(|r| self.points(r));
            }

            fn geometry(&mut self, geometry: &'a Geometry) {
                self.heights = geometry.heights.as_deref();
                self.next = 0;
                self.header(geometry.kind);
                match (&geometry.coords, geometry.kind) {
                    (Coords::Collection(members), _) => {
                        self.count(members.len());
                        for member in members {
                            self.geometry(member);
                        }
                    },
                    (Coords::Empty, Kind::Point) => self.empty_point(),
                    (Coords::Empty, _) => self.count(0),
                    (Coords::Point(p), _) => self.point(p),
                    (Coords::MultiPoint(ps), _) => {
                        self.count(ps.len());
                        for p in ps {
                            self.header(Kind::Point);
                            match p {
                                Some(p) => self.point(p),
                                None => self.empty_point(),
                            }
                        }
                    },
                    (Coords::Points(ps), Kind::LineString) => self.points(ps),
                    (Coords::Points(ps), _) => {
                        self.count(ps.len());
                        for p in ps {
                            self.header(Kind::Point);
                            self.point(p);
                        }
                    },
                    (Coords::Lines(rs), Kind::Polygon) => self.rings(rs),
                    (Coords::Lines(ls), _) => {
                        self.count(ls.len());
                        for l in ls {
                            self.header(Kind::LineString);
                            self.points(l);
                        }
                    },
                    (Coords::Polygons(ps), _) => {
                        self.count(ps.len());
                        for p in ps {
                            self.header(Kind::Polygon);
                            self.rings(p);
                        }
                    },
                }
            }
        }
        let mut w = W {
            out: Vec::with_capacity(self.wkb_len()),
            heights: self.heights.as_deref(),
            next: 0,
        };
        w.geometry(self);
        w.out
    }
}

/// (type, coordinates) of one 2D ISO WKB value of the tier (with `z`, one
/// ISO WKB Z value), not yet admitted; the other dimension, M, EWKB,
/// trailing bytes and bounds are refused.
fn read(data: &[u8], limits: &Limits, z: bool) -> Result<(Kind, Gv), GeometryRefusal> {
    if data.len() > limits.wkb_max_bytes(z) {
        return Err(GeometryRefusal {
            code: crate::GeometryRefusalCode::SpatialWorkLimit,
            message: "Invalid geometry: WKB bound".into(),
            bound: Some(crate::GeometryBound {
                kind: crate::TierBound::WkbBytes,
                limit: limits.wkb_max_bytes(z) as u64,
            }),
        });
    }
    struct R<'a> {
        data: &'a [u8],
        offset: usize,
        z: bool,
        limits: &'a Limits,
    }
    impl R<'_> {
        fn take(&mut self, n: usize) -> Result<&[u8], GeometryRefusal> {
            if self.offset + n > self.data.len() {
                return Err(wkb_error("truncated WKB"));
            }
            let bytes = &self.data[self.offset..self.offset + n];
            self.offset += n;
            Ok(bytes)
        }

        fn u32(&mut self, order: u8) -> Result<u32, GeometryRefusal> {
            let raw: [u8; 4] = self.take(4)?.try_into().expect("four bytes");
            Ok(if order == 1 {
                u32::from_le_bytes(raw)
            } else {
                u32::from_be_bytes(raw)
            })
        }

        fn count(&mut self, order: u8) -> Result<usize, GeometryRefusal> {
            let n = self.u32(order)? as usize;
            if n > self.limits.ring_coordinates {
                return Err(self
                    .limits
                    .over("array bound".into(), crate::TierBound::Array));
            }
            Ok(n)
        }

        fn point(&mut self, order: u8) -> Result<Gv, GeometryRefusal> {
            let width = if self.z { 3 } else { 2 };
            let raw = self.take(8 * width)?;
            let values: Vec<_> = raw
                .chunks_exact(8)
                .map(|b| {
                    let b: [u8; 8] = b.try_into().expect("eight bytes");
                    Gv::Float(if order == 1 {
                        f64::from_le_bytes(b)
                    } else {
                        f64::from_be_bytes(b)
                    })
                })
                .collect();
            if values
                .iter()
                .all(|value| matches!(value, Gv::Float(value) if value.is_nan()))
            {
                return Ok(Gv::Array(vec![]));
            }
            Ok(Gv::Array(values))
        }

        fn line(&mut self, order: u8) -> Result<Gv, GeometryRefusal> {
            let n = self.count(order)?;
            Ok(Gv::Array(
                (0..n)
                    .map(|_| self.point(order))
                    .collect::<Result<_, GeometryRefusal>>()?,
            ))
        }

        fn one(
            &mut self,
            expected: Option<Kind>,
            depth: usize,
        ) -> Result<(Kind, Gv), GeometryRefusal> {
            if depth > 16 {
                return Err(self.limits.over(
                    "geometry collection nesting exceeds 16".into(),
                    crate::TierBound::Depth,
                ));
            }
            let order = self.take(1)?[0];
            if order > 1 {
                return Err(wkb_error("WKB byte order"));
            }
            let code = self.u32(order)? as i64 - if self.z { WKB_Z } else { 0 };
            let kind = match Kind::from_wkb_code(code) {
                Some(kind) if expected.is_none_or(|e| e == kind) => kind,
                _ => return Err(wkb_error("unsupported WKB geometry type")),
            };
            let coordinates = match kind {
                Kind::GeometryCollection => {
                    let n = self.count(order)?;
                    Gv::Array(
                        (0..n)
                            .map(|_| {
                                self.one(None, depth + 1)
                                    .map(|(kind, coordinates)| geojson(kind, coordinates))
                            })
                            .collect::<Result<_, GeometryRefusal>>()?,
                    )
                },
                Kind::Point => self.point(order)?,
                Kind::LineString => self.line(order)?,
                Kind::Polygon => {
                    let n = self.count(order)?;
                    Gv::Array(
                        (0..n)
                            .map(|_| self.line(order))
                            .collect::<Result<_, GeometryRefusal>>()?,
                    )
                },
                multi @ (Kind::MultiPoint | Kind::MultiLineString | Kind::MultiPolygon) => {
                    let n = self.count(order)?;
                    Gv::Array(
                        (0..n)
                            .map(|_| self.one(multi.member(), depth).map(|m| m.1))
                            .collect::<Result<_, GeometryRefusal>>()?,
                    )
                },
            };
            Ok((kind, coordinates))
        }
    }
    let mut r = R {
        data,
        offset: 0,
        z,
        limits,
    };
    let (kind, coordinates) = r.one(None, 0)?;
    if r.offset != data.len() {
        return Err(wkb_error("trailing WKB bytes"));
    }
    Ok((kind, coordinates))
}

/// Decode and admit ISO WKB in its mandatory coordinate-space declaration.
pub fn decode(
    data: &[u8],
    space: &GeometrySpace,
    limits: &Limits,
) -> Result<Geometry, GeometryRefusal> {
    decode_with_work(data, space, limits, &mut GeometryWork::default())
}

/// Canonical decoding keeps the declared frame and dimensions. A bare WKB
/// header never supplies or infers a coordinate reference system.
pub fn decode_with_work(
    data: &[u8],
    space: &GeometrySpace,
    limits: &Limits,
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    work.charge(data.len())?;
    work.allocate(data.len().saturating_mul(8).saturating_add(256))?;
    let z = !space.is_ground();
    let (kind, coordinates) = read(data, limits, z)?;
    crate::admit_coordinates(&geojson(kind, coordinates), space, limits, work)
}
