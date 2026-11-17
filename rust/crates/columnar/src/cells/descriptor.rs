use std::collections::{BTreeMap, HashMap};
use rtdb_types::{ListKind, RunEndWidth};

use super::*;

/// Discriminator interpretation for ordered dense-union children.
/// Native ordinals narrow to signed bytes on writes; every child remains in the descriptor.
#[derive(Debug)]
pub enum UnionEncoding { DeclaredIds(Vec<i8>), Ordinal }

/// Concrete container shape with retained children in source order.
/// The descriptor exposes this shape by borrow; child Arc clones retain their metadata leases.
#[derive(Debug)]
pub enum ContainerKind {
    Struct(Vec<Arc<VectorDescriptor>>),
    List(Arc<VectorDescriptor>),
    Set(Arc<VectorDescriptor>),
    FixedList { size: i32, elements: Arc<VectorDescriptor> },
    TimestampRange(Arc<VectorDescriptor>),
    Map { sorted: bool, entries: Arc<VectorDescriptor> },
    DenseUnion { encoding: UnionEncoding, children: Vec<Arc<VectorDescriptor>> },
    RunEnd { ends: Arc<VectorDescriptor>, values: Arc<VectorDescriptor> },
}

#[derive(Debug)]
enum DescriptorShape {
    Scalar(Field),
    Container { name: String, nullable: bool, metadata: HashMap<String, String>, kind: ContainerKind },
}

/// Immutable opened vector metadata, retained independently of payload storage.
/// Scalar leaves hold one validated SDK Field; containers retain native ordered shape rather than an SDK projection.
/// Cloned child Arcs retain their descriptor reservations.
#[derive(Debug)]
pub struct VectorDescriptor {
    shape: DescriptorShape,
    logical: LogicalType,
    reservation: Arc<MemoryReservation>,
}

/// Checked SDK metadata whose lease survives every shallow clone.
/// A caller copying the borrowed Field must account for that independently owned copy.
#[derive(Clone, Debug)]
pub struct RetainedArrowField { field: Arc<Field>, _reservation: Arc<MemoryReservation> }
impl RetainedArrowField { pub fn field(&self) -> &Field { &self.field } }

/// Original admitted Arrow declaration or the actual native descriptor when no Arrow declaration exists.
#[derive(Clone, Copy, Debug)]
pub enum DeclarationRef<'a> { Arrow(&'a Field), Native(&'a VectorDescriptor) }
impl<'a> DeclarationRef<'a> {
    /// Borrows an original Arrow declaration; native-written descriptors return None.
    pub fn arrow(self) -> Option<&'a Field> { match self { Self::Arrow(field) => Some(field), Self::Native(_) => None } }
}

#[derive(Debug)]
pub(super) enum Declaration { Arrow(Arc<Field>), Native }

impl ContainerKind {
    fn child_count(&self) -> usize {
        match self {
            Self::Struct(children) | Self::DenseUnion { children, .. } => children.len(),
            Self::RunEnd { .. } => 2,
            _ => 1,
        }
    }
    fn child(&self, index: usize) -> Option<&Arc<VectorDescriptor>> {
        match self {
            Self::Struct(children) | Self::DenseUnion { children, .. } => children.get(index),
            Self::RunEnd { ends, values } => match index { 0 => Some(ends), 1 => Some(values), _ => None },
            Self::List(child) | Self::Set(child) | Self::TimestampRange(child)
            | Self::FixedList { elements: child, .. } | Self::Map { entries: child, .. } => (index == 0).then_some(child),
        }
    }
    fn logical(&self) -> Result<LogicalType> {
        Ok(match self {
            Self::Struct(children) => {
                let mut fields = BTreeMap::new();
                for child in children {
                    if fields.insert(child.name().to_owned(), child.logical.clone()).is_some() { return Err(layout()); }
                }
                LogicalType::Struct(fields)
            },
            Self::DenseUnion { children, .. } => match LogicalType::join_all(children.iter().map(|child| &child.logical)) {
                LogicalType::Nothing => LogicalType::Null,
                logical => logical,
            },
            Self::RunEnd { ends, values } => LogicalType::RunEndEncoded {
                width: match ends.scalar_field().map(Field::data_type) {
                    Some(DataType::Int16) => RunEndWidth::I16,
                    Some(DataType::Int32) => RunEndWidth::I32,
                    Some(DataType::Int64) => RunEndWidth::I64,
                    _ => return Err(layout()),
                },
                value: Box::new(values.logical.clone()),
            },
            kind => {
                let (kind, child) = match kind {
                    Self::List(child) => (ListKind::List, child),
                    Self::Set(child) => (ListKind::Set, child),
                    Self::FixedList { size, elements } => (ListKind::FixedSizeList(*size), elements),
                    Self::TimestampRange(child) => (ListKind::TimestampTzRange, child),
                    Self::Map { sorted, entries } => (ListKind::Map { sorted: *sorted }, entries),
                    _ => return Err(layout()),
                };
                LogicalType::List { kind, element: Box::new(child.logical.clone()) }
            },
        })
    }
}

impl VectorDescriptor {
    pub fn name(&self) -> &str { match &self.shape { DescriptorShape::Scalar(field) => field.name(), DescriptorShape::Container { name, .. } => name } }
    pub fn is_nullable(&self) -> bool { match &self.shape { DescriptorShape::Scalar(field) => field.is_nullable(), DescriptorShape::Container { nullable, .. } => *nullable } }
    pub fn metadata(&self) -> &HashMap<String, String> { match &self.shape { DescriptorShape::Scalar(field) => field.metadata(), DescriptorShape::Container { metadata, .. } => metadata } }
    pub fn logical_type(&self) -> &LogicalType { &self.logical }
    /// Borrows the sole canonical scalar Field; containers return None and require checked SDK projection.
    pub fn scalar_field(&self) -> Option<&Field> { match &self.shape { DescriptorShape::Scalar(field) => Some(field), _ => None } }
    pub fn container(&self) -> Option<&ContainerKind> { match &self.shape { DescriptorShape::Container { kind, .. } => Some(kind), _ => None } }

    /// Allocates checked SDK metadata under a retained lease without changing this descriptor.
    /// Native union ordinals outside the SDK's unique nonnegative byte domain return an unsupported-schema error.
    /// Budget, depth or SDK validation errors release every unpublished projection allocation.
    pub fn arrow_field(&self) -> Result<RetainedArrowField> {
        let (bytes, depth) = self.bound(1)?;
        let reservation = self.reservation.budget().reserve(bytes.checked_mul(depth).and_then(|n| n.checked_mul(8)).ok_or_else(allocation)?)?;
        Ok(RetainedArrowField { field: Arc::new(self.project()?), _reservation: reservation })
    }

    fn bound(&self, depth: usize) -> Result<(usize, usize)> {
        if depth > 128 { return Err(Anomaly::unsupported("rtdb/vector-schema-depth", "Execution vector schema exceeds 128 levels")); }
        if let DescriptorShape::Scalar(field) = &self.shape { return super::open::schema_bound(field, depth); }
        let mut bytes = 4096usize.checked_add(self.name().len()).and_then(|n| n.checked_add(self.metadata().capacity().checked_mul(128)?)).ok_or_else(allocation)?;
        for (key, value) in self.metadata() { bytes = bytes.checked_add(key.len()).and_then(|n| n.checked_add(value.len())).ok_or_else(allocation)?; }
        let mut maximum = depth;
        if let Some(kind) = self.container() {
            for index in 0..kind.child_count() {
                let (child_bytes, child_depth) = kind.child(index).ok_or_else(layout)?.bound(depth + 1)?;
                bytes = bytes.checked_add(child_bytes).ok_or_else(allocation)?;
                maximum = maximum.max(child_depth);
            }
        }
        Ok((bytes, maximum))
    }

    fn project(&self) -> Result<Field> {
        let DescriptorShape::Container { name, nullable, metadata, kind } = &self.shape else { return Ok(self.scalar_field().ok_or_else(layout)?.clone()); };
        let data_type = match kind {
            ContainerKind::Struct(children) => DataType::Struct(children.iter().map(|child| child.project().map(Arc::new)).collect::<Result<Vec<_>>>()?.into()),
            ContainerKind::List(child) | ContainerKind::Set(child) => DataType::List(Arc::new(child.project()?)),
            ContainerKind::FixedList { size, elements } => DataType::FixedSizeList(Arc::new(elements.project()?), *size),
            ContainerKind::TimestampRange(child) => DataType::FixedSizeList(Arc::new(child.project()?), 2),
            ContainerKind::Map { sorted, entries } => DataType::Map(Arc::new(entries.project()?), *sorted),
            ContainerKind::DenseUnion { encoding, children } => {
                if matches!(encoding, UnionEncoding::Ordinal) && children.len() > 128 { return Err(unrepresentable()); }
                let ids = match encoding { UnionEncoding::DeclaredIds(ids) => ids.clone(), UnionEncoding::Ordinal => (0..children.len()).map(|index| index as i8).collect() };
                let fields = children.iter().map(|child| child.project().map(Arc::new)).collect::<Result<Vec<_>>>()?;
                DataType::Union(arrow_schema::UnionFields::try_new(ids, fields).map_err(crate::arrow_error)?, arrow_schema::UnionMode::Dense)
            },
            ContainerKind::RunEnd { ends, values } => DataType::RunEndEncoded(Arc::new(ends.project()?), Arc::new(values.project()?)),
        };
        Ok(Field::new(name, data_type, *nullable).with_metadata(metadata.clone()))
    }

    pub(super) fn opened(ingress: &Field, storage: &NodeLayout, reservation: Arc<MemoryReservation>) -> Result<Arc<Self>> {
        let kind = match (storage, ingress.data_type()) {
            (NodeLayout::Leaf { .. } | NodeLayout::Scalar(_), data_type) => {
                let field = ingress.clone().with_data_type(super::open::scalar_data_type(data_type)?);
                let logical = crate::execution_field_type(&field)?;
                return Ok(Arc::new(Self { shape: DescriptorShape::Scalar(field), logical, reservation }));
            },
            (NodeLayout::Struct { children, .. }, DataType::Struct(_)) => ContainerKind::Struct(children.iter().map(|child| Arc::clone(&child.descriptor)).collect()),
            (NodeLayout::List { elements, .. }, DataType::List(_)) => {
                let child = Arc::clone(&elements.descriptor);
                if ingress.metadata().get(crate::EXTENSION_NAME_KEY).is_some_and(|name| name == "set") { ContainerKind::Set(child) } else { ContainerKind::List(child) }
            },
            (NodeLayout::List { elements, .. }, DataType::Map(_, sorted)) => ContainerKind::Map { sorted: *sorted, entries: Arc::clone(&elements.descriptor) },
            (NodeLayout::FixedList { elements, .. }, DataType::FixedSizeList(_, size)) => {
                let elements = Arc::clone(&elements.descriptor);
                if ingress.metadata().get(crate::EXTENSION_NAME_KEY).is_some_and(|name| name == "xt/tstz-range") { ContainerKind::TimestampRange(elements) }
                else { ContainerKind::FixedList { size: *size, elements } }
            },
            (NodeLayout::Union { children, .. }, DataType::Union(fields, _)) => ContainerKind::DenseUnion {
                encoding: UnionEncoding::DeclaredIds(fields.iter().map(|(id, _)| id).collect()),
                children: children.iter().map(|child| Arc::clone(&child.descriptor)).collect(),
            },
            (NodeLayout::RunEnd { ends, values, .. }, DataType::RunEndEncoded(_, _)) => ContainerKind::RunEnd { ends: Arc::clone(&ends.descriptor), values: Arc::clone(&values.descriptor) },
            _ => return Err(layout()),
        };
        let nullable = !matches!(&kind, ContainerKind::DenseUnion { .. } | ContainerKind::RunEnd { .. }) && ingress.is_nullable();
        let logical = if nullable { kind.logical()?.maybe() } else { kind.logical()? };
        logical.validate()?;
        Ok(Arc::new(Self { shape: DescriptorShape::Container { name: ingress.name().clone(), nullable, metadata: ingress.metadata().clone(), kind }, logical, reservation }))
    }

    pub(super) fn ordinal_union(name: &str, children: &[Arc<Self>], memory: &Arc<crate::MemoryBudget>) -> Result<Arc<Self>> {
        vector_length(children.len())?;
        let mut bytes = 4096usize.checked_add(name.len()).and_then(|n| n.checked_add(children.len().checked_mul(std::mem::size_of::<Arc<Self>>())?)).ok_or_else(allocation)?;
        for child in children {
            let (_, join_depth) = child.bound(2)?;
            // LogicalType::join_all retains cloned ancestor Struct maps while recursively joining their fields.
            let join_bytes = child.logical.retained_bytes()?.checked_mul(join_depth).ok_or_else(allocation)?;
            bytes = bytes.checked_add(join_bytes).ok_or_else(allocation)?;
        }
        let reservation = memory.reserve(bytes.checked_mul(8).ok_or_else(allocation)?)?;
        let kind = ContainerKind::DenseUnion { encoding: UnionEncoding::Ordinal, children: children.to_vec() };
        let logical = kind.logical()?;
        Ok(Arc::new(Self { shape: DescriptorShape::Container { name: name.to_owned(), nullable: false, metadata: HashMap::new(), kind }, logical, reservation }))
    }

    pub(super) fn union_type_id(&self, index: usize) -> Result<i8> {
        let Some(ContainerKind::DenseUnion { encoding, children }) = self.container() else { return Err(layout()); };
        if index >= children.len() { return Err(layout()); }
        match encoding { UnionEncoding::DeclaredIds(ids) => ids.get(index).copied().ok_or_else(layout), UnionEncoding::Ordinal => Ok(index as i8) }
    }
    pub(super) fn union_index(&self, id: i8) -> Result<usize> {
        let Some(ContainerKind::DenseUnion { encoding, children }) = self.container() else { return Err(layout()); };
        let index = match encoding {
            UnionEncoding::DeclaredIds(ids) => ids.iter().position(|candidate| *candidate == id).ok_or_else(layout)?,
            UnionEncoding::Ordinal => usize::try_from(id).map_err(|_| layout())?,
        };
        if index >= children.len() { return Err(layout()); }
        Ok(index)
    }
}

fn unrepresentable() -> Anomaly { Anomaly::unsupported("rtdb/unrepresentable-vector-schema", "Native vector metadata cannot be represented by the Arrow SDK field model") }

#[cfg(test)]
mod tests {
    use arrow_array::Int32Array;
    use arrow_buffer::Buffer;
    use arrow_schema::{UnionFields, UnionMode};
    use crate::MemoryBudget;
    use super::*;

    fn scalar(name: &str, value: i32, memory: &Arc<MemoryBudget>) -> ExecutionVector {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![value]));
        let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
        ExecutionVector::with_reservation(Arc::new(Field::new(name, DataType::Int32, false)), array, reservation).unwrap()
    }

    fn bytes(bytes: &[u8], memory: &Arc<MemoryBudget>) -> RetainedBuffer {
        let reservation = memory.reserve(bytes.len() + 128).unwrap();
        RetainedBuffer::with_reservation(Buffer::from_slice_ref(bytes), reservation).unwrap()
    }

    #[test]
    fn checked_projection_retains_its_lease_after_vector_and_original_projection_drop() {
        let memory = MemoryBudget::new(1024 * 1024);
        let vector = scalar("custom-name", 42, &memory);
        let projected = vector.arrow_field().unwrap();
        let retained = projected.clone();
        assert!(Arc::ptr_eq(&projected.field, &retained.field));
        assert!(Arc::ptr_eq(&projected._reservation, &retained._reservation));
        drop(vector);
        drop(projected);
        assert_eq!(memory.used_bytes(), retained._reservation.bytes());
        assert_eq!(retained.field().name(), "custom-name");
        assert_eq!(retained.field().data_type(), &DataType::Int32);
        drop(retained);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn projection_budget_failure_keeps_the_descriptor_and_releases_unpublished_allocations() {
        let memory = MemoryBudget::new(1024 * 1024);
        let vector = scalar("i", 17, &memory);
        let before = memory.used_bytes();
        let blocker = memory.reserve(memory.limit_bytes() - before).unwrap();
        assert!(vector.arrow_field().is_err());
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
        assert_eq!(vector.descriptor().name(), "i");
        assert_eq!(vector.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 17);
        drop(blocker);
        assert_eq!(memory.used_bytes(), before);
        let projected = vector.arrow_field().unwrap();
        drop(projected);
        drop(vector);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn retained_descriptor_outlives_payload_and_failed_parent_admission() {
        let memory = MemoryBudget::new(1024 * 1024);
        let vector = scalar("child", 1, &memory);
        let descriptor = Arc::clone(&vector.node().descriptor);
        drop(vector);
        assert_eq!(memory.used_bytes(), descriptor.reservation.bytes());
        let strong = Arc::strong_count(&descriptor);
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        assert!(VectorDescriptor::ordinal_union("parent", std::slice::from_ref(&descriptor), &memory).is_err());
        assert_eq!(Arc::strong_count(&descriptor), strong);
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
        assert_eq!(descriptor.name(), "child");
        drop(blocker);
        drop(descriptor);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn declared_union_ids_and_custom_child_order_survive_checked_projection() {
        let memory = MemoryBudget::new(4 * 1024 * 1024);
        let first = scalar("z-custom", 11, &memory);
        let second = scalar("a-custom", 23, &memory);
        let fields = UnionFields::try_new([7, 2], [first.ingress_field().arrow().unwrap().clone(), second.ingress_field().arrow().unwrap().clone()]).unwrap();
        let vector = ExecutionVector::from_physical(Arc::new(Field::new("u", DataType::Union(fields, UnionMode::Dense), true)), PhysicalLayout::DenseUnion {
            type_ids: bytes(&[2, 7], &memory), offsets: bytes(&[0; 8], &memory), children: vec![first, second],
        }, &memory).unwrap();
        assert!(vector.ingress_field().arrow().unwrap().is_nullable());
        assert!(!vector.descriptor().is_nullable());
        let Some(ContainerKind::DenseUnion { encoding: UnionEncoding::DeclaredIds(ids), children }) = vector.descriptor().container() else { panic!(); };
        assert_eq!(ids, &[7, 2]);
        assert_eq!(children.iter().map(|child| child.name()).collect::<Vec<_>>(), vec!["z-custom", "a-custom"]);
        assert_eq!(vector.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 23);
        let projected = vector.arrow_field().unwrap();
        let DataType::Union(fields, UnionMode::Dense) = projected.field().data_type() else { panic!(); };
        assert_eq!(fields.iter().map(|(id, field)| (id, field.name().as_str())).collect::<Vec<_>>(), vec![(7, "z-custom"), (2, "a-custom")]);
        assert_eq!(vector.logical_type(), &crate::execution_field_type(projected.field()).unwrap());
        drop(projected);
        drop(vector);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn ordinal_union_keeps_wrapped_and_negative_legs_without_an_sdk_schema() {
        let memory = MemoryBudget::new(64 * 1024 * 1024);
        let children = (0..257).map(|index| scalar(&format!("leg-{index}"), index, &memory).into_direct().unwrap()).collect::<Vec<_>>();
        let descriptors = children.iter().map(|child| Arc::clone(&child.descriptor)).collect::<Vec<_>>();
        let descriptor = VectorDescriptor::ordinal_union("native", &descriptors, &memory).unwrap();
        drop(descriptors);
        assert_eq!(descriptor.union_type_id(127).unwrap(), 127);
        assert_eq!(descriptor.union_type_id(128).unwrap(), -128);
        assert_eq!(descriptor.union_type_id(255).unwrap(), -1);
        assert_eq!(descriptor.union_type_id(256).unwrap(), 0);
        assert_eq!(descriptor.union_index(0).unwrap(), 0);
        let before = memory.used_bytes();
        assert_eq!(descriptor.arrow_field().unwrap_err().code, "rtdb/unrepresentable-vector-schema");
        assert_eq!(memory.used_bytes(), before);
        let owner = Arc::new(NodeOwner { backing: BackingOwner::Native, metadata: Arc::clone(&descriptor.reservation) });
        let vector = ExecutionVector::direct(Arc::new(PhysicalNode {
            declaration: Declaration::Native, descriptor, length: 3,
            storage: NodeLayout::Union { type_ids: bytes(&[0, 128, 0], &memory), offsets: bytes(&[0; 12], &memory), children }, owner,
        }));
        assert!(matches!(vector.ingress_field(), DeclarationRef::Native(_)));
        assert!(vector.ingress_field().arrow().is_none());
        assert_eq!(vector.leg_names().unwrap().count(), 257);
        assert_eq!(vector.cell(1).unwrap().type_id().unwrap(), -128);
        assert!(vector.cell(1).unwrap().is_null().unwrap());
        assert!(matches!(vector.cell(1).unwrap().get_int(NullChecks::Disabled), Err(VectorReadError::AbsentPrimitiveReceiver)));
        let negative = vector.vector_for("leg-128").unwrap().unwrap();
        assert!(!negative.cell(1).unwrap().is_null().unwrap());
        assert_eq!(negative.cell(1).unwrap().get_int(NullChecks::Enabled).unwrap(), 128);
        let wrapped = vector.vector_for("leg-256").unwrap().unwrap();
        assert_eq!(wrapped.cell(2).unwrap().get_int(NullChecks::Enabled).unwrap(), 256);
        assert_eq!(vector.cell(2).unwrap().get_int(NullChecks::Enabled).unwrap(), 0);
        assert_eq!(wrapped.descriptor().name(), "leg-256");
        drop(negative);
        drop(wrapped);
        drop(vector);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn ordinal_projection_accepts_all_128_nonnegative_byte_ids() {
        let memory = MemoryBudget::new(32 * 1024 * 1024);
        let child = scalar("same-name", 1, &memory);
        let children = vec![Arc::clone(&child.node().descriptor); 128];
        let descriptor = VectorDescriptor::ordinal_union("u", &children, &memory).unwrap();
        let projected = descriptor.arrow_field().unwrap();
        let DataType::Union(fields, _) = projected.field().data_type() else { panic!(); };
        assert_eq!(fields.len(), 128);
        assert_eq!(fields.iter().last().unwrap().0, 127);
        drop(projected);
        drop(descriptor);
        drop(children);
        drop(child);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn empty_and_bottom_only_union_descriptors_have_null_logical_type() {
        let memory = MemoryBudget::new(4 * 1024 * 1024);
        let array: ArrayRef = Arc::new(arrow_array::NullArray::new(0));
        let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
        let bottom = ExecutionVector::with_reservation(Arc::new(Field::new("bottom", DataType::Null, false)), array, reservation).unwrap();
        for children in [vec![], vec![Arc::clone(&bottom.node().descriptor)]] {
            let descriptor = VectorDescriptor::ordinal_union("u", &children, &memory).unwrap();
            assert_eq!(descriptor.logical_type(), &LogicalType::Null);
            let projected = descriptor.arrow_field().unwrap();
            assert_eq!(descriptor.logical_type(), &crate::execution_field_type(projected.field()).unwrap());
        }
        let fields = UnionFields::try_new(Vec::<i8>::new(), Vec::<Field>::new()).unwrap();
        let vector = ExecutionVector::from_physical(Arc::new(Field::new("u", DataType::Union(fields, UnionMode::Dense), false)), PhysicalLayout::DenseUnion {
            type_ids: bytes(&[255], &memory), offsets: bytes(&[0; 4], &memory), children: vec![],
        }, &memory).unwrap();
        assert_eq!(vector.logical_type(), &LogicalType::Null);
        assert!(vector.cell(0).unwrap().is_null().unwrap());
        drop(vector);
        drop(bottom);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn checked_projection_preserves_nested_container_classes_and_metadata() {
        fn fields(integer: DataType, zone: &str) -> Vec<Field> {
            let element = Arc::new(Field::new("custom-element", integer.clone(), true));
            let extension = |name: &str| HashMap::from([(crate::EXTENSION_NAME_KEY.to_owned(), name.to_owned()), ("user".to_owned(), "retained".to_owned())]);
            vec![
                Field::new("list", DataType::List(Arc::clone(&element)), true),
                Field::new("set", DataType::List(Arc::clone(&element)), false).with_metadata(extension("set")),
                Field::new("fixed", DataType::FixedSizeList(element, 3), true),
                Field::new("range", DataType::FixedSizeList(Arc::new(Field::new("bounds", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(zone.into())), false)), 2), false).with_metadata(extension("xt/tstz-range")),
                Field::new("map", DataType::Map(Arc::new(Field::new("entries", DataType::Struct(vec![Field::new("key", integer.clone(), false), Field::new("value", integer.clone(), true)].into()), false)), true), true),
                Field::new("runs", DataType::RunEndEncoded(Arc::new(Field::new("run_ends", DataType::Int32, false)), Arc::new(Field::new("values", integer, true))), false),
            ]
        }
        let memory = MemoryBudget::new(16 * 1024 * 1024);
        let metadata = HashMap::from([("owner".to_owned(), "source".to_owned())]);
        let ingress = Field::new("root", DataType::Struct(fields(DataType::UInt32, "+01").into()), true).with_metadata(metadata.clone());
        let expected = Field::new("root", DataType::Struct(fields(DataType::Int32, "+01:00").into()), true).with_metadata(metadata);
        let array = arrow_array::new_empty_array(ingress.data_type());
        let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
        let vector = ExecutionVector::with_reservation(Arc::new(ingress.clone()), array, reservation).unwrap();
        assert_eq!(vector.ingress_field().arrow(), Some(&ingress));
        let Some(ContainerKind::Struct(children)) = vector.descriptor().container() else { panic!(); };
        assert!(matches!(children[0].container(), Some(ContainerKind::List(_))));
        assert!(matches!(children[1].container(), Some(ContainerKind::Set(_))));
        assert!(matches!(children[2].container(), Some(ContainerKind::FixedList { size: 3, .. })));
        assert!(matches!(children[3].container(), Some(ContainerKind::TimestampRange(_))));
        assert!(matches!(children[4].container(), Some(ContainerKind::Map { sorted: true, .. })));
        assert!(matches!(children[5].container(), Some(ContainerKind::RunEnd { .. })));
        let projected = vector.arrow_field().unwrap();
        assert_eq!(projected.field(), &expected);
        assert_eq!(vector.logical_type(), &crate::execution_field_type(projected.field()).unwrap());
        drop(vector);
        assert_eq!(memory.used_bytes(), projected._reservation.bytes());
        drop(projected);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn native_descriptor_admission_checks_depth_before_allocating() {
        let memory = MemoryBudget::new(16 * 1024 * 1024);
        let child = scalar("i", 1, &memory);
        let mut descriptor = Arc::clone(&child.node().descriptor);
        for _ in 1..128 { descriptor = VectorDescriptor::ordinal_union("u", &[descriptor], &memory).unwrap(); }
        let before = memory.used_bytes();
        assert_eq!(VectorDescriptor::ordinal_union("u", &[Arc::clone(&descriptor)], &memory).unwrap_err().code, "rtdb/vector-schema-depth");
        assert_eq!(memory.used_bytes(), before);
        drop(descriptor);
        drop(child);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn deep_struct_join_reserves_ancestor_copies_before_adopting_child_leases() {
        let source_memory = MemoryBudget::new(256 * 1024 * 1024);
        let mut data_type = DataType::Int32;
        for _ in 0..64 { data_type = DataType::Struct(vec![Field::new("x".repeat(1024), data_type, false)].into()); }
        let array = arrow_array::new_empty_array(&data_type);
        let reservation = source_memory.reserve(array.get_array_memory_size()).unwrap();
        let vector = ExecutionVector::with_reservation(Arc::new(Field::new("root", data_type, false)), array, reservation).unwrap();
        let child = Arc::clone(&vector.node().descriptor);
        drop(vector);
        let source_charge = source_memory.used_bytes();
        let source_references = Arc::strong_count(&child);
        let tight_memory = MemoryBudget::new(1024 * 1024);
        let one_tree_allowance = 8 * (4096 + "parent".len() + std::mem::size_of::<Arc<VectorDescriptor>>() + child.logical_type().retained_bytes().unwrap());
        assert!(one_tree_allowance < tight_memory.limit_bytes());
        assert_eq!(VectorDescriptor::ordinal_union("parent", std::slice::from_ref(&child), &tight_memory).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert_eq!(tight_memory.used_bytes(), 0);
        assert_eq!(source_memory.used_bytes(), source_charge);
        assert_eq!(Arc::strong_count(&child), source_references);
        assert_eq!(child.name(), "root");

        let parent_memory = MemoryBudget::new(64 * 1024 * 1024);
        let parent = VectorDescriptor::ordinal_union("parent", std::slice::from_ref(&child), &parent_memory).unwrap();
        assert_eq!(parent.logical_type(), child.logical_type());
        assert!(parent_memory.used_bytes() > one_tree_allowance);
        assert_eq!(source_memory.used_bytes(), source_charge);
        drop(child);
        let Some(ContainerKind::DenseUnion { children, .. }) = parent.container() else { panic!(); };
        assert_eq!(children[0].name(), "root");
        assert_eq!(source_memory.used_bytes(), source_charge);
        drop(parent);
        assert_eq!(source_memory.used_bytes(), 0);
        assert_eq!(parent_memory.used_bytes(), 0);
    }
}
