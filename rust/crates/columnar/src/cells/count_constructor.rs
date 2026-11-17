use rtdb_types::ScalarType;
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CountScalarClass { Null, IntArrow19Default }

#[derive(Debug)]
pub(super) struct CountScalarMetadata {
    class: CountScalarClass,
    field: Field,
    logical: LogicalType,
    reservation: Arc<MemoryReservation>,
}

impl CountScalarMetadata {
    pub(super) fn new(name: &str, nullable: bool, class: CountScalarClass, memory: &Arc<crate::MemoryBudget>) -> Result<Self> {
        let headers = std::mem::size_of::<Self>().checked_add(std::mem::size_of::<VectorDescriptor>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PhysicalNode>()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<NodeOwner>())).ok_or_else(allocation)?;
        let bytes = name.len().checked_mul(4).and_then(|bytes| bytes.checked_add(4096))
            .and_then(|bytes| headers.checked_mul(2).and_then(|headers| bytes.checked_add(headers))).ok_or_else(allocation)?;
        let reservation = memory.reserve(bytes)?;
        let (data_type, logical) = match class {
            CountScalarClass::Null => (DataType::Null, if nullable { LogicalType::Null } else { LogicalType::Nothing }),
            CountScalarClass::IntArrow19Default => {
                let logical = LogicalType::Scalar(ScalarType::I32);
                (DataType::Int32, if nullable { logical.maybe() } else { logical })
            },
        };
        Ok(Self { class, field: Field::new(name, data_type, nullable), logical, reservation })
    }
    pub(super) fn class(&self) -> CountScalarClass { self.class }
    pub(super) fn field(&self) -> &Field { &self.field }
    pub(super) fn logical(&self) -> &LogicalType { &self.logical }
    pub(super) fn reservation(&self) -> &Arc<MemoryReservation> { &self.reservation }
    pub(super) fn rebuilt(&self, memory: &Arc<crate::MemoryBudget>) -> Result<Self> { Self::new(self.field.name(), self.field.is_nullable(), self.class, memory) }
    pub(super) fn with_nullable(&self, nullable: bool) -> Result<Self> { Self::new(self.field.name(), nullable, self.class, self.reservation.budget()) }
}
