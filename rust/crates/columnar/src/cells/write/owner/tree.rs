use super::*;
use super::super::super::native_container::{CapturedChildren, NativeContainerParts, NativeFixedKind, NativeListKind, NativeStructInput, publish_native};

pub(super) struct NodeMeta { pub(super) name: String, pub(super) reservation: Arc<MemoryReservation> }
impl NodeMeta {
    fn new(name: &str, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = 4096usize.checked_add(name.len()).and_then(|n| n.checked_add(3 * std::mem::size_of::<WriterNode>())).ok_or_else(allocation)?;
        let reservation = memory.reserve(bytes)?;
        Ok(Self { name: name.to_owned(), reservation })
    }
    pub(super) fn memory(&self) -> &Arc<MemoryBudget> { self.reservation.budget() }
}

pub(super) struct ChildSlots<T> { pub(super) values: Vec<T>, reservation: Arc<MemoryReservation> }
impl<T> ChildSlots<T> {
    pub(super) fn new(capacity: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        vector_length(capacity)?;
        std::alloc::Layout::array::<T>(capacity).map_err(|_| allocation())?;
        let reservation = memory.reserve(capacity.checked_mul(std::mem::size_of::<T>()).and_then(|n| n.checked_add(128)).ok_or_else(allocation)?)?;
        Ok(Self { values: Vec::with_capacity(capacity), reservation })
    }
    fn push_initial(&mut self, value: T) -> Result<()> {
        if self.values.len() == self.values.capacity() { return Err(allocation()); }
        self.values.push(value);
        Ok(())
    }
    pub(super) fn append(&mut self, value: T, mutation: &mut Mutation) -> WriteResult<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self.values.capacity().checked_mul(2).and_then(|n| self.values.len().checked_add(1).map(|needed| n.max(needed).max(1))).ok_or_else(native_size)?;
            let mut replacement = Self::new(capacity, self.reservation.budget())?;
            replacement.values.extend(self.values.drain(..));
            *self = replacement;
            mutation.mark();
        }
        self.values.push(value);
        mutation.mark();
        Ok(())
    }
}

pub(super) struct WriterField { pub(super) key: String, pub(super) node: WriterNode, _key_reservation: Arc<MemoryReservation> }
pub(super) struct StructState { pub(super) count: SourceCount, pub(super) validity: Option<MutableBits>, pub(super) children: ChildSlots<WriterField>, pub(super) meta: NodeMeta }
pub(super) struct ListState { pub(super) kind: NativeListKind, pub(super) count: SourceCount, pub(super) validity: Option<MutableBits>, pub(super) offsets: MutableBytes, pub(super) last_offset: i32, pub(super) elements: Box<WriterNode>, pub(super) meta: NodeMeta }
pub(super) struct FixedListState { pub(super) kind: NativeFixedKind, pub(super) count: SourceCount, pub(super) validity: Option<MutableBits>, pub(super) elements: Box<WriterNode>, pub(super) meta: NodeMeta }
pub(super) struct UnionState { pub(super) count: SourceCount, pub(super) type_ids: MutableBytes, pub(super) offsets: MutableBytes, pub(super) children: ChildSlots<WriterNode>, pub(super) meta: NodeMeta }
pub(super) struct RunEndState { pub(super) count: SourceCount, pub(super) ends: Box<WriterNode>, pub(super) values: Box<WriterNode>, pub(super) meta: NodeMeta }
pub(super) enum WriterNode { Scalar(ScalarState), Struct(StructState), List(ListState), FixedList(FixedListState), Union(UnionState), RunEnd(RunEndState) }

pub(super) fn depth(depth: usize) -> Result<()> {
    if depth > 128 { Err(Anomaly::unsupported("rtdb/vector-schema-depth", "Execution vector writer exceeds 128 levels")) } else { Ok(()) }
}

impl WriterNode {
    pub(super) fn admit(descriptor: &VectorDescriptor, level: usize) -> Result<usize> {
        depth(level)?;
        let mut occurrences = 1usize;
        let mut child = |descriptor: &VectorDescriptor| -> Result<()> {
            occurrences = occurrences.checked_add(Self::admit(descriptor, level + 1)?).ok_or_else(allocation)?;
            Ok(())
        };
        match descriptor.container() {
            None => { super::super::super::open::schema_bound(descriptor.scalar_field().ok_or_else(layout)?, level)?; },
            Some(ContainerKind::Struct(fields)) => { vector_length(fields.len())?; for field in fields { child(field.descriptor())?; } },
            Some(ContainerKind::DenseUnion { children, .. }) => { vector_length(children.len())?; for descriptor in children { child(descriptor)?; } },
            Some(ContainerKind::RunEnd { ends, values }) => { child(ends)?; child(values)?; },
            Some(ContainerKind::List(elements) | ContainerKind::Set(elements) | ContainerKind::TimestampRange(elements)
                | ContainerKind::FixedList { elements, .. } | ContainerKind::Map { entries: elements, .. }) => child(elements)?,
        }
        Ok(occurrences)
    }
    fn scalar(field: &Field, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let (bound, depth) = super::super::super::open::schema_bound(field, 1)?;
        let scratch = memory.reserve(bound.checked_mul(depth).and_then(|n| n.checked_mul(16)).ok_or_else(allocation)?)?;
        let scalar = ScalarState::new(Arc::new(field.clone()), Arc::clone(memory))?;
        drop(scratch);
        Ok(Self::Scalar(scalar))
    }
    pub(super) fn null(memory: &Arc<MemoryBudget>, level: usize) -> Result<Self> {
        depth(level)?;
        let scratch = memory.reserve(4096usize.checked_add(std::mem::size_of::<Field>()).ok_or_else(allocation)?)?;
        let node = Self::scalar(&Field::new("null", DataType::Null, true), memory)?;
        drop(scratch);
        Ok(node)
    }
    pub(super) fn build(descriptor: &VectorDescriptor, memory: &Arc<MemoryBudget>) -> WriteResult<Self> {
        let Some(kind) = descriptor.container() else { return Ok(Self::scalar(descriptor.scalar_field().ok_or_else(layout)?, memory)?); };
        let meta = NodeMeta::new(descriptor.name(), memory)?;
        let count = SourceCount::new(0);
        let validity = || descriptor.is_nullable().then(|| MutableBits::new(memory));
        Ok(match kind {
            ContainerKind::Struct(fields) => {
                let mut children = ChildSlots::new(fields.len(), memory)?;
                for field in fields {
                    let node = Self::build(field.descriptor(), memory)?;
                    let key_reservation = memory.reserve(field.key().len().checked_add(128).ok_or_else(allocation)?)?;
                    children.push_initial(WriterField { key: field.key().to_owned(), node, _key_reservation: key_reservation })?;
                }
                Self::Struct(StructState { meta, count, validity: validity(), children })
            },
            ContainerKind::List(elements) | ContainerKind::Set(elements) | ContainerKind::Map { entries: elements, .. } => {
                let elements = Box::new(Self::build(elements, memory)?);
                let kind = match kind { ContainerKind::List(_) => NativeListKind::List, ContainerKind::Set(_) => NativeListKind::Set, ContainerKind::Map { sorted, .. } => NativeListKind::Map { sorted: *sorted }, _ => unreachable!() };
                Self::List(ListState { meta, kind, count, validity: validity(), offsets: MutableBytes::new(memory), last_offset: 0, elements })
            },
            ContainerKind::FixedList { elements, .. } | ContainerKind::TimestampRange(elements) => {
                let elements = Box::new(Self::build(elements, memory)?);
                let kind = match kind { ContainerKind::FixedList { size, .. } => NativeFixedKind::List(*size), _ => NativeFixedKind::TimestampRange };
                Self::FixedList(FixedListState { meta, kind, count, validity: validity(), elements })
            },
            ContainerKind::DenseUnion { children: fields, .. } => {
                let mut children = ChildSlots::new(fields.len(), memory)?;
                for field in fields { children.push_initial(Self::build(field, memory)?)?; }
                Self::Union(UnionState { meta, count, type_ids: MutableBytes::new(memory), offsets: MutableBytes::new(memory), children })
            },
            ContainerKind::RunEnd { ends, values } => {
                let ends = Box::new(Self::build(ends, memory)?);
                let values = Box::new(Self::build(values, memory)?);
                if ends.end_width().is_none() || ends.nullable() { return Err(WriteError::Reference(ReferenceFailure::RequireFailed)); }
                Self::RunEnd(RunEndState { meta, count, ends, values })
            },
        })
    }
    pub(super) fn name(&self) -> &str {
        match self { Self::Scalar(state) => state.field().name(), Self::Struct(state) => &state.meta.name, Self::List(state) => &state.meta.name, Self::FixedList(state) => &state.meta.name, Self::Union(state) => &state.meta.name, Self::RunEnd(state) => &state.meta.name }
    }
    pub(super) fn count(&self) -> SourceCount {
        match self { Self::Scalar(state) => state.count, Self::Struct(state) => state.count, Self::List(state) => state.count, Self::FixedList(state) => state.count, Self::Union(state) => state.count, Self::RunEnd(state) => state.count }
    }
    pub(super) fn nullable(&self) -> bool {
        match self { Self::Scalar(state) => state.field().is_nullable(), Self::Struct(state) => state.validity.is_some(), Self::List(state) => state.validity.is_some(), Self::FixedList(state) => state.validity.is_some(), Self::Union(_) => false, Self::RunEnd(state) => state.values.nullable() }
    }
    pub(super) fn null_type(&self) -> bool { match self { Self::Scalar(state) => state.field().data_type() == &DataType::Null, Self::RunEnd(state) => state.values.null_type(), _ => false } }
    pub(super) fn end_width(&self) -> Option<usize> {
        match self { Self::Scalar(state) if !state.extension() => match state.field().data_type() { DataType::Int16 => Some(2), DataType::Int32 => Some(4), DataType::Int64 => Some(8), _ => None }, Self::RunEnd(state) => state.values.end_width(), _ => None }
    }
    pub(super) fn clear(&mut self) {
        match self {
            Self::Scalar(state) => state.clear(),
            Self::Struct(state) => { if let Some(bits) = &mut state.validity { bits.clear(); } state.count = SourceCount::new(0); for child in &mut state.children.values { child.node.clear(); } },
            Self::List(state) => { if let Some(bits) = &mut state.validity { bits.clear(); } state.offsets.clear(); state.elements.clear(); state.count = SourceCount::new(0); state.last_offset = 0; },
            Self::FixedList(state) => { if let Some(bits) = &mut state.validity { bits.clear(); } state.elements.clear(); state.count = SourceCount::new(0); },
            Self::Union(state) => { state.type_ids.clear(); state.offsets.clear(); for child in &mut state.children.values { child.clear(); } state.count = SourceCount::new(0); },
            Self::RunEnd(state) => { state.ends.clear(); state.values.clear(); state.count = SourceCount::new(0); },
        }
    }
    pub(super) fn finish(self) -> Result<ExecutionVector> {
        let (meta, count, parts) = match self {
            Self::Scalar(state) => return state.finish(),
            Self::Struct(state) => {
                let validity = state.validity.map(MutableBits::finish).transpose()?;
                let slots = state.children;
                let mut children = CapturedChildren::new(slots.values.len(), state.meta.memory())?;
                for field in slots.values {
                    let child = field.node.finish()?;
                    children.push(NativeStructInput::new(&field.key, child, state.meta.memory())?)?;
                }
                (state.meta, state.count, NativeContainerParts::Struct { validity, children })
            },
            Self::List(state) => {
                let elements = state.elements.finish()?;
                let validity = state.validity.map(MutableBits::finish).transpose()?;
                let offsets = state.offsets.finish()?;
                (state.meta, state.count, NativeContainerParts::List { kind: state.kind, validity, offsets, last_offset: state.last_offset, elements })
            },
            Self::FixedList(state) => {
                let validity = state.validity.map(MutableBits::finish).transpose()?;
                let elements = state.elements.finish()?;
                (state.meta, state.count, NativeContainerParts::FixedList { kind: state.kind, validity, elements })
            },
            Self::Union(state) => {
                let slots = state.children;
                let mut children = CapturedChildren::new(slots.values.len(), state.meta.memory())?;
                for child in slots.values { children.push(child.finish()?)?; }
                let type_ids = state.type_ids.finish()?;
                let offsets = state.offsets.finish()?;
                (state.meta, state.count, NativeContainerParts::Union { type_ids, offsets, children })
            },
            Self::RunEnd(state) => {
                let ends = state.ends.finish()?;
                let values = state.values.finish()?;
                (state.meta, state.count, NativeContainerParts::RunEnd { ends, values })
            },
        };
        publish_native(&meta.name, count, parts, meta.memory())
    }
}
