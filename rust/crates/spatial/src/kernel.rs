use geometry::predicates::{
    self, CoverPredicate, TopologyPredicate,
    prepared::{Preparation, PreparedGeometry},
};
use geometry::spatial_index::{candidate_ranges, point_key, shape_keys};
use geometry::{
    Coords, Geometry, GeometryDeclaration, GeometrySpace, GeometryWork, KeyRange, Kind, Scalar, Size, ValueView,
};
use rtdb_types::{Anomaly, Result, SpatialGeometryEncoding};

pub(crate) const WORK_STEPS: usize = 10_000_000;
pub(crate) const WORK_BYTES: usize = 8 << 20;

pub(crate) struct Location {
    pub(crate) geometry: Geometry,
    position_accuracy: Option<Option<f64>>,
}

impl Location {
    pub(crate) fn decode(bytes: &[u8], work: &mut GeometryWork) -> Result<Self> {
        let Some((&tag, payload)) = bytes.split_first() else {
            return Err(invalid("Missing location encoding"));
        };
        let (position_accuracy, wkb) = match tag {
            0 => (None, payload),
            1 => (Some(None), payload),
            2 => {
                let Some((accuracy, wkb)) = payload.split_first_chunk::<8>() else {
                    return Err(invalid("Missing position accuracy"));
                };
                let accuracy = f64::from_le_bytes(*accuracy);
                if !accuracy.is_finite() || accuracy < 0.0 {
                    return Err(invalid("Position accuracy must be finite and nonnegative"));
                }
                (Some(Some(accuracy)), wkb)
            }
            _ => return Err(invalid("Unknown location encoding")),
        };
        let declaration = GeometryDeclaration {
            space: GeometrySpace::wgs84_xy(),
            size: Size::Large,
        };
        let geometry =
            geometry::wkb::decode_with_work(wkb, &declaration.space, &declaration.limits(), work)
                .map_err(geometry_error)?;
        if position_accuracy.is_some() && !matches!(&geometry.coords, Coords::Point(_)) {
            return Err(invalid("An observed position requires a nonempty Point"));
        }
        Ok(Self {
            geometry,
            position_accuracy,
        })
    }

    pub(crate) fn decode_stored(
        bytes: &[u8], encoding: SpatialGeometryEncoding, work: &mut GeometryWork,
    ) -> Result<Option<Self>> {
        if encoding == SpatialGeometryEncoding::TaggedWkb {
            return Self::decode(bytes, work).map(Some);
        }
        let value = crate::bijection_geometry::decode(bytes, work)?;
        if matches!(value.scalar(), Scalar::Null) {
            return Ok(None);
        }
        let declaration = GeometryDeclaration {
            space: GeometrySpace::wgs84_xy(),
            size: Size::Large,
        };
        if encoding == SpatialGeometryEncoding::BijectionPosition {
            let position = geometry::admit_position_view_with_work(value, &declaration, work)
                .map_err(geometry_error)?;
            return Ok(Some(Self {
                geometry: Geometry::new(Kind::Point, Coords::Point(position.point), declaration.space),
                position_accuracy: Some(position.accuracy_m),
            }));
        }
        let geometry = geometry::admit_view_with_work(value, &declaration, work)
            .map_err(geometry_error)?;
        Ok(Some(Self { geometry, position_accuracy: None }))
    }

    pub(crate) fn keys(&self, work: &mut GeometryWork) -> Result<Vec<i64>> {
        match &self.geometry.coords {
            Coords::Point(coords) => point_key(coords, self.position_accuracy.unwrap_or(Some(0.0)))
                .map(|key| vec![key])
                .map_err(|error| invalid(error.to_string())),
            _ => shape_keys(&self.geometry, work).map_err(geometry_error),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Predicate {
    All,
    Topology(TopologyPredicate),
    WithinDistance(f64),
    Classification(Classification),
    Nearest {
        radius_m: Option<f64>,
        reported_center: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Classification {
    Inside,
    Outside,
    Uncertain,
    Unknown,
}

impl Predicate {
    pub fn parse(name: &str, radius_m: Option<f64>, reported_center: bool) -> Result<Self> {
        if radius_m.is_some_and(|radius| !radius.is_finite() || radius < 0.0) {
            return Err(invalid("Radius must be finite and nonnegative"));
        }
        if name != "nearest" && reported_center {
            return Err(invalid("Reported-center ordering requires nearest"));
        }
        if !matches!(name, "nearest" | "within_distance") && radius_m.is_some() {
            return Err(invalid("This predicate does not take a radius"));
        }
        Ok(Self::Topology(match name {
            "all" => return Ok(Self::All),
            "intersects" => TopologyPredicate::Intersects,
            "disjoint" => TopologyPredicate::Disjoint,
            "equals" => TopologyPredicate::Equals,
            "covers" => TopologyPredicate::Covers,
            "covered_by" => TopologyPredicate::CoveredBy,
            "contains" => TopologyPredicate::Contains,
            "within" => TopologyPredicate::Within,
            "touches" => TopologyPredicate::Touches,
            "crosses" => TopologyPredicate::Crosses,
            "overlaps" => TopologyPredicate::Overlaps,
            "within_distance" => {
                return Ok(Self::WithinDistance(
                    radius_m.ok_or_else(|| invalid("Distance requires a radius"))?,
                ));
            }
            "inside" => return Ok(Self::Classification(Classification::Inside)),
            "outside" => return Ok(Self::Classification(Classification::Outside)),
            "uncertain" => return Ok(Self::Classification(Classification::Uncertain)),
            "unknown" => return Ok(Self::Classification(Classification::Unknown)),
            "nearest" => {
                return Ok(Self::Nearest {
                    radius_m,
                    reported_center,
                });
            }
            _ => return Err(invalid("Unknown spatial predicate")),
        }))
    }

    pub(crate) fn cover_predicate(self) -> Option<CoverPredicate> {
        Some(match self {
            Self::All
            | Self::Topology(TopologyPredicate::Disjoint)
            | Self::Classification(Classification::Outside | Classification::Unknown)
            | Self::Nearest { radius_m: None, .. } => return None,
            Self::Topology(_) => CoverPredicate::Touches,
            Self::WithinDistance(distance_m)
            | Self::Nearest { radius_m: Some(distance_m), .. } => CoverPredicate::Within { distance_m },
            Self::Classification(Classification::Inside) => CoverPredicate::ClassifyInside,
            Self::Classification(Classification::Uncertain) => CoverPredicate::Classify,
        })
    }

    pub(crate) fn prepare_geometry(self, region: &Geometry, work: &mut GeometryWork) -> Result<PreparedGeometry> {
        let mut prepared = PreparedGeometry::new(region.clone(), Preparation::Topology, work)
            .map_err(geometry_error)?;
        if matches!(self, Self::Topology(_) | Self::WithinDistance(_) | Self::Nearest { .. }) {
            prepared.prepare_reference_points();
        }
        if matches!(self, Self::WithinDistance(_) | Self::Nearest { .. } | Self::Classification(_)) {
            prepared.prepare_reference_segments();
        }
        Ok(prepared)
    }

    pub(crate) fn cover(self, region: &PreparedGeometry, work: &mut GeometryWork) -> Result<Option<Vec<KeyRange>>> {
        self.cover_predicate().map(|predicate| candidate_ranges(region, predicate, work).map_err(geometry_error)).transpose()
    }

    // Outer option denotes eligibility; the inner value is the exact nearest distance.
    pub(crate) fn evaluate(
        self,
        value: &Location,
        region: &Geometry,
        prepared: Option<&PreparedGeometry>,
        work: &mut GeometryWork,
    ) -> Result<Option<Option<f64>>> {
        let distance = |work: &mut GeometryWork| match prepared {
            Some(prepared) => prepared.exact_distance_from(&value.geometry, work),
            None => predicates::distance(&value.geometry, region, work),
        };
        let exact = match self {
            Self::All => return Ok(Some(None)),
            Self::Topology(predicate) => match prepared {
                Some(prepared) => prepared.topology_from(&value.geometry, predicate, work),
                None => predicates::topology(&value.geometry, region, predicate, work),
            },
            Self::WithinDistance(radius_m) => {
                distance(work)
                    .map(|distance| distance.is_finite() && distance <= radius_m)
            }
            Self::Classification(expected) => {
                match (&value.geometry.coords, value.position_accuracy) {
                    (Coords::Point(point), Some(accuracy)) => {
                        match prepared {
                            Some(prepared) => prepared.classify_exact(*point, accuracy, work),
                            None => predicates::classify(*point, accuracy, region, work),
                        }.map(
                            |actual| {
                                actual
                                    == match expected {
                                        Classification::Inside => "inside",
                                        Classification::Outside => "outside",
                                        Classification::Uncertain => "uncertain",
                                        Classification::Unknown => "unknown",
                                    }
                            },
                        )
                    }
                    _ => return Ok(None),
                }
            }
            Self::Nearest {
                radius_m,
                reported_center,
            } => {
                if value.position_accuracy.is_some() && !reported_center {
                    return Ok(None);
                }
                let distance = distance(work)
                    .map_err(geometry_error)?;
                return Ok((distance.is_finite()
                    && radius_m.is_none_or(|radius| distance <= radius))
                .then_some(Some(distance)));
            }
        };
        Ok(exact.map_err(geometry_error)?.then_some(None))
    }

    pub(crate) fn validate_region(self, region: &Location) -> Result<()> {
        if matches!(self, Self::Nearest { .. })
            && !matches!(region.geometry.coords, Coords::Point(_))
        {
            return Err(invalid("Nearest requires a nonempty Point origin"));
        }
        Ok(())
    }
}

pub(crate) fn invalid(message: impl Into<String>) -> Anomaly {
    Anomaly::incorrect("rtdb.spatial/invalid", message)
}

pub(crate) fn geometry_error(error: geometry::GeometryRefusal) -> Anomaly {
    if error.bound.as_ref().is_some_and(|bound| {
        matches!(
            bound.kind,
            geometry::TierBound::WorkSteps | geometry::TierBound::AllocatedBytes
        )
    }) {
        return Anomaly::new(
            rtdb_types::AnomalyCategory::Unavailable,
            "rtdb.spatial/work-limit",
            error.to_string(),
        );
    }
    invalid(error.to_string())
}
