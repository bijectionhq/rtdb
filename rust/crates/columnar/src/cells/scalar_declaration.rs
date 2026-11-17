use rtdb_types::{ScalarType, TimeUnit};
use std::collections::HashMap;
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceScalarDeclaration {
    FixedBinary(i32),
    Time32(TimeUnit),
    Time64(TimeUnit),
    Uuid { inner_width: i32 },
}

impl SourceScalarDeclaration {
    pub(super) fn fixed_width(self) -> i32 {
        match self { Self::FixedBinary(width) | Self::Uuid { inner_width: width } => width, Self::Time32(_) => 4, Self::Time64(_) => 8 }
    }
    pub(super) fn bytes_method(self) -> bool { matches!(self, Self::FixedBinary(_) | Self::Uuid { .. }) }
    pub(super) fn class_name(self) -> &'static str {
        match self { Self::FixedBinary(_) => "FixedSizeBinaryVector", Self::Time32(_) => "Time32Vector", Self::Time64(_) => "Time64Vector", Self::Uuid { .. } => "UuidVector" }
    }
}

#[derive(Debug)]
pub(super) struct SourceScalarMetadata {
    declaration: SourceScalarDeclaration,
    field: Field,
    logical: LogicalType,
    reservation: Arc<MemoryReservation>,
}

impl SourceScalarMetadata {
    pub(super) fn new(name: &str, nullable: bool, declaration: SourceScalarDeclaration, memory: &Arc<crate::MemoryBudget>) -> Result<Self> {
        let headers = std::mem::size_of::<Self>().checked_add(std::mem::size_of::<VectorDescriptor>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PhysicalNode>()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<NodeOwner>())).ok_or_else(allocation)?;
        let bytes = name.len().checked_mul(4).and_then(|bytes| bytes.checked_add(4096))
            .and_then(|bytes| headers.checked_mul(2).and_then(|headers| bytes.checked_add(headers))).ok_or_else(allocation)?;
        let reservation = memory.reserve(bytes)?;
        let (data_type, scalar) = match declaration {
            SourceScalarDeclaration::FixedBinary(width) => (DataType::FixedSizeBinary(width), ScalarType::FixedSizeBinary(width)),
            SourceScalarDeclaration::Time32(unit) => (DataType::Time32(arrow_unit(unit)), ScalarType::Time { unit, bit_width: 32 }),
            SourceScalarDeclaration::Time64(unit) => (DataType::Time64(arrow_unit(unit)), ScalarType::Time { unit, bit_width: 64 }),
            SourceScalarDeclaration::Uuid { .. } => (DataType::FixedSizeBinary(16), ScalarType::Uuid),
        };
        let mut field = Field::new(name, data_type, nullable);
        if matches!(declaration, SourceScalarDeclaration::Uuid { .. }) {
            field = field.with_metadata(HashMap::from([
                (crate::EXTENSION_NAME_KEY.to_owned(), "uuid".to_owned()),
                (crate::EXTENSION_METADATA_KEY.to_owned(), String::new()),
            ]));
        }
        let logical = LogicalType::Scalar(scalar);
        let logical = if nullable { logical.maybe() } else { logical };
        Ok(Self { declaration, field, logical, reservation })
    }
    pub(super) fn declaration(&self) -> SourceScalarDeclaration { self.declaration }
    pub(super) fn field(&self) -> &Field { &self.field }
    pub(super) fn logical(&self) -> &LogicalType { &self.logical }
    pub(super) fn reservation(&self) -> &Arc<MemoryReservation> { &self.reservation }
    pub(super) fn rebuilt(&self, memory: &Arc<crate::MemoryBudget>) -> Result<Self> { Self::new(self.field.name(), self.field.is_nullable(), self.declaration, memory) }
    pub(super) fn with_nullable(&self, nullable: bool) -> Result<Self> { Self::new(self.field.name(), nullable, self.declaration, self.reservation.budget()) }
}

fn arrow_unit(unit: TimeUnit) -> arrow_schema::TimeUnit {
    match unit {
        TimeUnit::Second => arrow_schema::TimeUnit::Second,
        TimeUnit::Millisecond => arrow_schema::TimeUnit::Millisecond,
        TimeUnit::Microsecond => arrow_schema::TimeUnit::Microsecond,
        TimeUnit::Nanosecond => arrow_schema::TimeUnit::Nanosecond,
    }
}
