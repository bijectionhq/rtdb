use super::*;
use super::tree::*;
use super::super::super::native_container::{NativeFixedKind, NativeListKind};

fn increment(count: &mut SourceCount, mutation: &mut Mutation) {
    *count = SourceCount::new(count.get().wrapping_add(1));
    mutation.mark();
}
fn valid(validity: &mut Option<MutableBits>, count: &mut SourceCount, value: bool, mutation: &mut Mutation) -> WriteResult<()> {
    if let Some(bits) = validity { bits.write_at(count.get(), value, mutation)?; }
    increment(count, mutation);
    Ok(())
}
fn promote(validity: &mut Option<MutableBits>, count: SourceCount, memory: &Arc<MemoryBudget>, mutation: &mut Mutation) -> WriteResult<()> {
    if validity.is_none() {
        *validity = Some(MutableBits::new(memory));
        mutation.mark();
        validity.as_mut().expect("validity installed").ones(count.get(), mutation)?;
    }
    Ok(())
}

impl WriterNode {
    fn promote_nullable(&mut self, mutation: &mut Mutation) -> WriteResult<()> {
        match self {
            Self::Scalar(state) => state.promote(mutation),
            Self::Struct(state) => promote(&mut state.validity, state.count, state.meta.memory(), mutation),
            Self::List(state) => promote(&mut state.validity, state.count, state.meta.memory(), mutation),
            Self::FixedList(state) => promote(&mut state.validity, state.count, state.meta.memory(), mutation),
            Self::Union(_) => Err(WriteError::Reference(ReferenceFailure::CannotSetUnionNullable)),
            Self::RunEnd(state) => state.values.promote_nullable(mutation),
        }
    }
    pub(super) fn undefined(&mut self, level: usize, mutation: &mut Mutation) -> WriteResult<()> {
        depth(level)?;
        match self {
            Self::Scalar(state) => state.undefined(mutation),
            Self::Struct(state) => {
                valid(&mut state.validity, &mut state.count, false, mutation)?;
                for child in &mut state.children.values { child.node.undefined(level + 1, mutation)?; }
                Ok(())
            },
            Self::List(state) => { state.offset(state.last_offset, mutation)?; valid(&mut state.validity, &mut state.count, false, mutation) },
            Self::FixedList(state) => {
                valid(&mut state.validity, &mut state.count, false, mutation)?;
                let width = match state.kind { NativeFixedKind::List(width) => width, NativeFixedKind::TimestampRange => 2 };
                for _ in 0..width { state.elements.undefined(level + 1, mutation)?; }
                Ok(())
            },
            Self::Union(state) => {
                state.type_ids.append_primitive(&[255], mutation)?;
                state.offsets.append_primitive(&0i32.to_le_bytes(), mutation)?;
                increment(&mut state.count, mutation);
                Ok(())
            },
            Self::RunEnd(state) => { state.values.undefined(level + 1, mutation)?; state.end_run(1, mutation) },
        }
    }
    pub(super) fn write_null(&mut self, level: usize, mutation: &mut Mutation) -> WriteResult<()> {
        depth(level)?;
        if let Self::Union(state) = self {
            let index = if let Some(index) = state.children.values.iter().position(|child| child.name() == "null") {
                let child = &mut state.children.values[index];
                if !child.null_type() { return Err(WriteError::Reference(ReferenceFailure::CannotPromoteUnionLeg)); }
                child.promote_nullable(mutation)?;
                index
            } else {
                let child = Self::null(state.meta.memory(), level + 1)?;
                let index = state.children.values.len();
                state.children.append(child, mutation)?;
                index
            };
            state.type_ids.append_primitive(&[index as i8 as u8], mutation)?;
            state.offsets.append_primitive(&state.children.values[index].count().get().to_le_bytes(), mutation)?;
            increment(&mut state.count, mutation);
            return state.children.values[index].write_null(level + 1, mutation);
        }
        self.promote_nullable(mutation)?;
        self.undefined(level, mutation)
    }
    pub(super) fn boolean(&mut self, value: bool, level: usize, mutation: &mut Mutation) -> WriteResult<()> {
        depth(level)?;
        match self { Self::Scalar(state) => state.boolean(value, mutation), Self::RunEnd(state) => { state.values.boolean(value, level + 1, mutation)?; state.end_run(1, mutation) }, _ => Err(unsupported_write("writeBoolean")) }
    }
    pub(super) fn primitive(&mut self, method: &'static str, bytes: &[u8], level: usize, mutation: &mut Mutation) -> WriteResult<()> {
        depth(level)?;
        match self { Self::Scalar(state) => state.primitive(method, bytes, mutation), Self::RunEnd(state) => { state.values.primitive(method, bytes, level + 1, mutation)?; state.end_run(1, mutation) }, _ => Err(unsupported_write(method)) }
    }
    pub(super) fn bytes(&mut self, bytes: &[u8], level: usize, mutation: &mut Mutation) -> WriteResult<()> {
        depth(level)?;
        match self { Self::Scalar(state) => state.bytes(bytes, mutation), Self::RunEnd(state) => { state.values.bytes(bytes, level + 1, mutation)?; state.end_run(1, mutation) }, _ => Err(unsupported_write("writeBytes")) }
    }
    pub(super) fn end_struct(&mut self, level: usize, mutation: &mut Mutation) -> WriteResult<()> {
        depth(level)?;
        let Self::Struct(state) = self else { return Err(unsupported_write("endStruct")); };
        valid(&mut state.validity, &mut state.count, true, mutation)?;
        for child in &mut state.children.values {
            for _ in 0..state.count.get().wrapping_sub(child.node.count().get()) { child.node.write_null(level + 1, mutation)?; }
        }
        Ok(())
    }
    pub(super) fn end_list(&mut self, level: usize, mutation: &mut Mutation) -> WriteResult<()> {
        depth(level)?;
        match self {
            Self::List(state) if !matches!(state.kind, NativeListKind::Set) => {
                let length = state.elements.count().get().wrapping_sub(state.last_offset);
                state.offset(state.last_offset.wrapping_add(length), mutation)?;
                valid(&mut state.validity, &mut state.count, true, mutation)
            },
            Self::FixedList(state) if matches!(state.kind, NativeFixedKind::List(_)) => valid(&mut state.validity, &mut state.count, true, mutation),
            _ => Err(unsupported_write("endList")),
        }
    }
}

impl ListState {
    fn offset(&mut self, value: i32, mutation: &mut Mutation) -> WriteResult<()> {
        if self.count.is_zero() { self.offsets.append_primitive(&0i32.to_le_bytes(), mutation)?; }
        self.offsets.append_primitive(&value.to_le_bytes(), mutation)?;
        self.last_offset = value;
        mutation.mark();
        Ok(())
    }
}
impl RunEndState {
    pub(super) fn end_run(&mut self, length: i32, mutation: &mut Mutation) -> WriteResult<()> {
        if length <= 0 { return Err(WriteError::Reference(ReferenceFailure::RequireFailed)); }
        if self.values.count().get() != self.ends.count().get().wrapping_add(1) { return Err(WriteError::Reference(ReferenceFailure::CheckFailed)); }
        let end = self.count.get().wrapping_add(length);
        let WriterNode::Scalar(ends) = self.ends.as_mut() else { return Err(WriteError::Reference(ReferenceFailure::RunEndReceiverClass)); };
        if ends.extension() { return Err(WriteError::Reference(ReferenceFailure::RunEndReceiverClass)); }
        match ends.field().data_type() {
            DataType::Int16 => {
                if end > i32::from(i16::MAX) { return Err(WriteError::Reference(ReferenceFailure::RequireFailed)); }
                ends.primitive("writeShort", &(end as i16).to_le_bytes(), mutation)?;
            },
            DataType::Int32 => ends.primitive("writeInt", &end.to_le_bytes(), mutation)?,
            DataType::Int64 => ends.primitive("writeLong", &i64::from(end).to_le_bytes(), mutation)?,
            _ => return Err(WriteError::Reference(ReferenceFailure::RunEndReceiverClass)),
        }
        self.count = SourceCount::new(end);
        mutation.mark();
        Ok(())
    }
}
