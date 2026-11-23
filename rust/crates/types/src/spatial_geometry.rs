use crate::{Anomaly, Result};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SpatialGeometryEncoding {
    #[default]
    TaggedWkb,
    BijectionGeometry,
    BijectionPosition,
}

impl SpatialGeometryEncoding {
    pub fn name(self) -> &'static str {
        match self {
            Self::TaggedWkb => "tagged-wkb-v1",
            Self::BijectionGeometry => "bijection-geometry-v1",
            Self::BijectionPosition => "bijection-position-v1",
        }
    }

    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "tagged-wkb-v1" => Ok(Self::TaggedWkb),
            "bijection-geometry-v1" => Ok(Self::BijectionGeometry),
            "bijection-position-v1" => Ok(Self::BijectionPosition),
            _ => Err(Anomaly::incorrect(
                "rtdb.owned/spatial-encoding",
                "Unrecognized spatial geometry encoding",
            )),
        }
    }
}
