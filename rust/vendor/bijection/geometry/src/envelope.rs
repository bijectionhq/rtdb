//! Conservative WGS84 coordinate envelopes for physical pruning. They carry
//! no membership decision: a positive overlap still needs native refinement.
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "[f64; 4]", into = "[f64; 4]")]
pub struct Envelope([f64; 4]);

impl TryFrom<[f64; 4]> for Envelope {
    type Error = String;

    fn try_from(mut bounds: [f64; 4]) -> Result<Self, Self::Error> {
        if bounds.iter().any(|v| !v.is_finite())
            || bounds[0] > bounds[2]
            || bounds[1] > bounds[3]
            || bounds[0] < -180.0
            || bounds[2] > 180.0
            || bounds[1] < -90.0
            || bounds[3] > 90.0
        {
            return Err("Invalid WGS84 pruning envelope".into());
        }
        bounds.iter_mut().for_each(|v| {
            if *v == 0.0 {
                *v = 0.0;
            }
        });
        Ok(Self(bounds))
    }
}

impl From<Envelope> for [f64; 4] {
    fn from(value: Envelope) -> Self {
        value.0
    }
}

impl Eq for Envelope {}
impl std::hash::Hash for Envelope {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for value in self.0 {
            state.write_u64(value.to_bits());
        }
    }
}

impl Envelope {
    pub fn of(
        geometry: &crate::Geometry,
        work: &mut crate::GeometryWork,
    ) -> Result<Option<Self>, crate::GeometryRefusal> {
        if geometry.space != crate::GeometrySpace::wgs84_xy() {
            return Err(crate::unsupported_space(
                "Spatial pruning requires native WGS84 XY",
            ));
        }
        // Almost antipodal arcs make extrema ill-conditioned. A coarse
        // physical summary is preferable to a numerically narrow rejection.
        work.allocate(geometry.position_count().saturating_mul(16))?;
        for path in geometry
            .lines()
            .into_iter()
            .chain(geometry.polygons().into_iter().flatten())
        {
            work.charge(path.len())?;
            for pair in path.windows(2) {
                let angle = crate::arcs::angle(
                    crate::arcs::direction(pair[0]),
                    crate::arcs::direction(pair[1]),
                );
                if !angle.is_finite() || angle >= std::f64::consts::PI - 1e-4 {
                    return Ok(Some(Self([-180.0, -90.0, 180.0, 90.0])));
                }
            }
        }
        let Some(mut bounds) = crate::arcs::geometry_bounds(geometry, work)? else {
            return Ok(None);
        };
        // Extrema include trigonometric rounding, not just endpoint ULPs.
        // This padding affects candidate work only, never exact predicates.
        const PAD_DEGREES: f64 = 1e-6;
        bounds[0] -= PAD_DEGREES;
        bounds[1] -= PAD_DEGREES;
        bounds[2] += PAD_DEGREES;
        bounds[3] += PAD_DEGREES;
        // Wrapping and pole aliases must not turn a geographic interval into
        // a smaller Cartesian one. A full longitude envelope is conservative.
        if bounds[0] < -180.0
            || bounds[2] > 180.0
            || bounds[0] > bounds[2]
            || bounds[1] <= -90.0
            || bounds[3] >= 90.0
        {
            bounds[0] = -180.0;
            bounds[2] = 180.0;
        }
        bounds[0] = bounds[0].next_down().max(-180.0);
        bounds[1] = bounds[1].next_down().max(-90.0);
        bounds[2] = bounds[2].next_up().min(180.0);
        bounds[3] = bounds[3].next_up().min(90.0);
        Self::try_from(bounds).map(Some).map_err(crate::invalid)
    }

    pub fn union(self, other: Self) -> Self {
        Self([
            self.0[0].min(other.0[0]),
            self.0[1].min(other.0[1]),
            self.0[2].max(other.0[2]),
            self.0[3].max(other.0[3]),
        ])
    }

    pub fn intersects(self, other: Self) -> bool {
        if self.0[1] > other.0[3] || other.0[1] > self.0[3] {
            return false;
        }
        // -180 and +180 denote the same meridian.
        [0.0, -360.0, 360.0]
            .into_iter()
            .any(|shift| self.0[0] <= other.0[2] + shift && other.0[0] + shift <= self.0[2])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelopes_preserve_seam_aliases_and_curved_edge_extrema() -> anyhow::Result<()> {
        let at = |x| Envelope::try_from([x, 0.0, x, 0.0]).map_err(anyhow::Error::msg);
        assert!(at(-180.0)?.intersects(at(180.0)?));
        assert!(!at(0.0)?.intersects(at(90.0)?));
        let line = crate::admit_value(
            &crate::Gv::from(&serde_json::json!({
                "type":"LineString", "coordinates":[[-60,60],[60,60]]
            })),
            &crate::GeometryDeclaration {
                space: crate::GeometrySpace::wgs84_xy(),
                size: crate::Size::Large,
            },
        )?;
        let bounds: [f64; 4] = Envelope::of(&line, &mut crate::GeometryWork::default())?
            .unwrap()
            .into();
        assert!(bounds[1] <= 60.0 && bounds[3] > 70.0);
        assert!(Envelope::try_from([0.0, 0.0, f64::NAN, 1.0]).is_err());
        Ok(())
    }
}
