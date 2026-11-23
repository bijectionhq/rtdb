//! The bounded ISO WKB codec shared by retained datasets and native queries.
//! The caller declares the coordinate space and dimensions; headers never infer
//! it. SRIDs, M, EWKB, trailing bytes and malformed topology refuse.
use std::borrow::Cow;

use crate::{
    Coords,
    Geometry,
    GeometryRefusal,
    GeometrySpace,
    GeometryWork,
    Kind,
    Limits,
    Pos, Scalar, ValueView,
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

#[derive(Clone, Copy)]
struct Reader<'a> {
    data: &'a [u8],
    z: bool,
    limits: &'a Limits,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], GeometryRefusal> {
        let (head, tail) = self.data.split_at_checked(n).ok_or_else(|| wkb_error("truncated WKB"))?;
        self.data = tail;
        Ok(head)
    }

    fn u32(&mut self, order: u8) -> Result<u32, GeometryRefusal> {
        let raw = self.take(4)?.try_into().expect("four bytes");
        Ok(if order == 1 { u32::from_le_bytes(raw) } else { u32::from_be_bytes(raw) })
    }

    fn count(&mut self, order: u8) -> Result<usize, GeometryRefusal> {
        let n = self.u32(order)? as usize;
        if n > self.limits.ring_coordinates {
            return Err(self.limits.over("array bound".into(), crate::TierBound::Array));
        }
        Ok(n)
    }

    fn header(&mut self, expected: Option<Kind>) -> Result<(u8, Kind), GeometryRefusal> {
        let order = self.take(1)?[0];
        if order > 1 {
            return Err(wkb_error("WKB byte order"));
        }
        let code = self.u32(order)? as i64 - if self.z { WKB_Z } else { 0 };
        let kind = Kind::from_wkb_code(code).filter(|&kind| expected.is_none_or(|e| e == kind))
            .ok_or_else(|| wkb_error("unsupported WKB geometry type"))?;
        Ok((order, kind))
    }

    fn skip(&mut self, kind: Kind, order: u8, depth: usize) -> Result<(), GeometryRefusal> {
        let width = if self.z { 24 } else { 16 };
        match kind {
            Kind::Point => { self.take(width)?; }
            Kind::LineString => {
                let count = self.count(order)?;
                self.take(count.saturating_mul(width))?;
            }
            Kind::Polygon => {
                for _ in 0..self.count(order)? {
                    self.skip(Kind::LineString, order, depth)?;
                }
            }
            multi => {
                for _ in 0..self.count(order)? {
                    let nested = depth + usize::from(multi == Kind::GeometryCollection);
                    if nested > 16 {
                        return Err(self.limits.over(
                            "geometry collection nesting exceeds 16".into(), crate::TierBound::Depth,
                        ));
                    }
                    let (order, kind) = self.header(multi.member())?;
                    self.skip(kind, order, nested)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Value<'a> {
    Object(Reader<'a>, u8, Kind),
    Coordinates(Reader<'a>, u8, Kind),
    Text(&'static str),
    Float(f64),
}

impl<'a> ValueView<'a> for Value<'a> {
    fn scalar(self) -> Scalar {
        match self { Self::Float(value) => Scalar::Float(value), _ => Scalar::Other }
    }

    fn text(self) -> Option<Cow<'a, str>> {
        match self { Self::Text(text) => Some(text.into()), _ => None }
    }

    fn object(self) -> Option<impl Iterator<Item = (Cow<'a, str>, Self)>> {
        let Self::Object(reader, order, kind) = self else { return None; };
        Some([
            ("type".into(), Self::Text(kind.name())),
            (kind.contents_key().into(), Self::Coordinates(reader, order, kind)),
        ].into_iter())
    }

    fn array(self) -> Option<impl ExactSizeIterator<Item = Self>> {
        let Self::Coordinates(mut reader, order, kind) = self else { return None; };
        let mut point = [0.0; 3];
        let mut next = 0;
        let len = if kind == Kind::Point {
            let width = if reader.z { 3 } else { 2 };
            let bytes = reader.take(width * 8).expect("validated WKB");
            for (value, raw) in point.iter_mut().zip(bytes.chunks_exact(8)) {
                let raw = raw.try_into().expect("eight bytes");
                *value = if order == 1 { f64::from_le_bytes(raw) } else { f64::from_be_bytes(raw) };
            }
            if point[..width].iter().all(|value| value.is_nan()) { 0 } else { width }
        } else {
            reader.count(order).expect("validated WKB")
        };
        Some((0..len).map(move |_| {
            if kind == Kind::Point {
                let value = point[next];
                next += 1;
                return Self::Float(value);
            }
            let (child_order, child_kind) = match kind {
                Kind::LineString => (order, Kind::Point),
                Kind::Polygon => (order, Kind::LineString),
                _ => reader.header(kind.member()).expect("validated WKB"),
            };
            let start = reader;
            reader.skip(child_kind, child_order, 0).expect("validated WKB");
            if kind == Kind::GeometryCollection {
                Self::Object(start, child_order, child_kind)
            } else {
                Self::Coordinates(start, child_order, child_kind)
            }
        }))
    }
}

fn read<'a>(data: &'a [u8], limits: &'a Limits, z: bool) -> Result<Value<'a>, GeometryRefusal> {
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
    let mut reader = Reader { data, z, limits };
    let (order, kind) = reader.header(None)?;
    let start = reader;
    reader.skip(kind, order, 0)?;
    if !reader.data.is_empty() {
        return Err(wkb_error("trailing WKB bytes"));
    }
    Ok(Value::Object(start, order, kind))
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
    let value = read(data, limits, z)?;
    crate::admit_coordinates(value, space, limits, work)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Gv, LARGE, admit_geometry, geojson};

    #[test]
    fn spatial_arrow_typed_wkb_admission_preserves_canonical_bytes() {
        let space = GeometrySpace::wgs84_xy();
        let limits = LARGE;
        let point = |x: f64, y: f64| Gv::Array(vec![Gv::Float(x), Gv::Float(y)]);
        let value = geojson(Kind::GeometryCollection, Gv::Array(vec![
            geojson(Kind::MultiPoint, Gv::Array(vec![point(-0.0, 3.0), Gv::Array(vec![])])),
            geojson(Kind::LineString, Gv::Array(vec![point(1e-15, -5.5), point(180.0, 40.0)])),
        ]));
        let geometry = admit_geometry(&value, &space, &limits).unwrap();
        let bytes = geometry.wkb();
        assert_eq!(decode(&bytes, &space, &limits).unwrap().wkb(), bytes);
        let mut with_trailer = bytes.clone();
        with_trailer.push(0);
        assert_eq!(decode(&with_trailer, &space, &limits).unwrap_err().message,
                   "Invalid geometry: trailing WKB bytes");

        let mut big_endian = vec![0, 0, 0, 0, 1];
        big_endian.extend_from_slice(&(-0.0f64).to_be_bytes());
        big_endian.extend_from_slice(&3.0f64.to_be_bytes());
        let mut canonical = vec![1, 1, 0, 0, 0];
        canonical.extend_from_slice(&0.0f64.to_le_bytes());
        canonical.extend_from_slice(&3.0f64.to_le_bytes());
        assert_eq!(decode(&big_endian, &space, &limits).unwrap().wkb(), canonical);
    }
}
