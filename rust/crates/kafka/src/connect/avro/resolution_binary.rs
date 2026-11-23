use super::*;
use super::super::{AvroMap, BinaryWindow, MapKey, Preflight, datum};
#[cfg(test)] #[path = "resolution_binary_tests.rs"] mod tests;

impl AvroResolution {
    pub fn decode_datum(&self, bytes: &[u8], context: &mut ConversionContext) -> Result<AvroDatum> {
        self.writer.retain_source(context)?;
        self.reader.retain_source(context)?;
        if self.writer.raw_bytes() { return self.resolve(self.writer.decode_datum(bytes, context)?, context); }
        for charge in &self.charges { context.retain(Arc::clone(charge))?; }
        let mut reader = Preflight { bytes, offset: 0, window: BinaryWindow::new(bytes.len()), allocated: 4096, limit: context.allocation_limit(), names: &self.writer.names, view_bytes: 0 };
        self.preflight(self.root, &self.writer.root, &mut reader, 0)?;
        context.reserve(reader.allocated)?;
        reader.offset = 0; reader.window = BinaryWindow::new(bytes.len());
        let value = self.read(self.root, &self.writer.root, &mut reader, 0)?;
        Ok(AvroDatum { value, plan: Arc::clone(&self.reader) })
    }

    fn preflight(&self, id: usize, schema: &Schema, reader: &mut Preflight<'_>, depth: usize) -> Result<()> {
        nesting(depth)?;
        let schema = physical(super::super::resolve(schema, &self.writer.names)?);
        match &self.actions[id] {
            Action::Error(message) => return Err(error("avro-resolution", message)),
            Action::Scalar { .. } => return reader.value(schema, depth),
            Action::Enum(symbols) => {
                let index = usize::try_from(reader.int()?).map_err(|cause| error("avro-decode", cause))?;
                let (_, symbol) = symbols.get(index).and_then(Option::as_ref).ok_or_else(|| error("avro-resolution", "Reader enum has no symbol or default"))?;
                reader.claim(symbol.len().checked_mul(4).ok_or_else(overflow)?)?;
            }
            Action::Record { fields, reader_names, defaults, .. } => {
                let Schema::Record(record) = schema else { return Err(invalid()); };
                for name in reader_names { reader.claim(name.len().checked_mul(4).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?)?; }
                for (_, value) in defaults { reader.claim(value_bound(value, depth + 1)?)?; }
                for (field, action) in record.fields.iter().zip(fields) {
                    if let Some((_, child)) = action { self.preflight(*child, &field.schema, reader, depth + 1)?; }
                    else { reader.skip(&field.schema, depth + 1)?; }
                }
            }
            Action::Array { child, .. } => {
                let Schema::Array(array) = schema else { return Err(invalid()); };
                self.preflight_collection(*child, &array.items, false, reader, depth + 1)?;
            }
            Action::Map { child, .. } => {
                let Schema::Map(map) = schema else { return Err(invalid()); };
                self.preflight_collection(*child, &map.types, true, reader, depth + 1)?;
            }
            Action::WriterUnion { branches, .. } => {
                let (index, branch) = branch(schema, reader)?;
                self.preflight(*branches.get(index).ok_or_else(invalid)?, branch, reader, depth + 1)?;
            }
            Action::ReaderUnion { child, .. } => self.preflight(*child, schema, reader, depth + 1)?,
        }
        reader.claim(4096)
    }

    fn can_pack_array(&self, child: usize, writer: datum::NumericInput<'_>, target: PackedLayout) -> bool {
        match writer {
            datum::NumericInput::Plain(kind) => self.packed_array_branch(child, Some(kind), None, target).is_some(),
            datum::NumericInput::Union(members) => {
                let Action::WriterUnion { branches, .. } = &self.actions[child] else { return false; };
                members.iter().zip(branches).enumerate().all(|(index, (schema, action))| {
                    // Preflight has already rejected any selected incompatible branch.
                    if matches!(&self.actions[*action], Action::Error(_)) { return true; }
                    let kind = NumericKind::from_wire_schema(schema);
                    (kind.is_some() || matches!(schema, Schema::Null))
                        && self.packed_array_branch(child, kind, Some(index as u32), target).is_some()
                })
            }
        }
    }

    fn preflight_collection(&self, child: usize, schema: &Schema, map: bool, reader: &mut Preflight<'_>, depth: usize) -> Result<()> {
        loop {
            let count = datum::read_block(reader)?;
            if count == 0 { return Ok(()); }
            reader.claim(count.checked_mul(4096).ok_or_else(overflow)?)?;
            for _ in 0..count {
                if map { reader.bytes()?; }
                self.preflight(child, schema, reader, depth)?;
            }
        }
    }

    fn read(&self, id: usize, schema: &Schema, reader: &mut Preflight<'_>, depth: usize) -> Result<Avro> {
        nesting(depth)?;
        let schema = physical(super::super::resolve(schema, &self.writer.names)?);
        match &self.actions[id] {
            Action::Error(message) => Err(error("avro-resolution", message)),
            Action::Scalar { .. } => self.apply(id, datum::read_value(schema, reader, depth)?, depth),
            Action::Enum(symbols) => {
                let index = usize::try_from(reader.int()?).map_err(|cause| error("avro-decode", cause))?;
                let (index, symbol) = symbols.get(index).and_then(Option::as_ref).ok_or_else(invalid)?;
                Ok(Avro::Enum(*index, symbol.clone()))
            }
            Action::Record { fields, reader_names, defaults, .. } => {
                let Schema::Record(record) = schema else { return Err(invalid()); };
                let mut output = vec![None; reader_names.len()];
                for (field, action) in record.fields.iter().zip(fields) {
                    if let Some((index, child)) = action { output[*index] = Some(self.read(*child, &field.schema, reader, depth + 1)?); }
                    else { reader.skip(&field.schema, depth + 1)?; }
                }
                for (index, value) in defaults { output[*index] = Some(value.clone()); }
                Ok(Avro::Record(reader_names.iter().cloned().zip(output).map(|(name, value)| Ok((name, value.ok_or_else(invalid)?))).collect::<Result<_>>()?))
            }
            Action::Array { child, packed } => {
                let Schema::Array(array) = schema else { return Err(invalid()); };
                if let Some(target) = packed
                    && let Some(writer) = datum::NumericInput::from_schema(&array.items)
                    && self.can_pack_array(*child, writer, *target)
                {
                    return Ok(Avro::PackedArray(datum::PackedArray::read(writer, *target, reader, depth + 1)?));
                }
                let mut values = Vec::new();
                loop {
                    let count = datum::read_block(reader)?;
                    if count == 0 { break; }
                    for _ in 0..count { values.push(self.read(*child, &array.items, reader, depth + 1)?); }
                }
                Ok(Avro::Array(values))
            }
            Action::Map { child, strings } => {
                let Schema::Map(map) = schema else { return Err(invalid()); };
                let first = datum::read_block(reader)?; let mut count = first; let mut entries = Vec::new();
                while count != 0 {
                    for _ in 0..count { entries.push((MapKey::Utf8(datum::read_bytes(reader)?), self.read(*child, &map.types, reader, depth + 1)?)); }
                    count = datum::read_block(reader)?;
                }
                Ok(Avro::Map(AvroMap::new(first, entries, *strings)))
            }
            Action::WriterUnion { branches, equivalent } => {
                let (index, branch) = branch(schema, reader)?;
                let value = self.read(*branches.get(index).ok_or_else(invalid)?, branch, reader, depth + 1)?;
                Ok(if *equivalent { Avro::Union(index as u32, Box::new(value)) } else { value })
            }
            Action::ReaderUnion { index, child } => Ok(Avro::Union(*index, Box::new(self.read(*child, schema, reader, depth + 1)?))),
        }
    }
}

fn branch<'a>(schema: &'a Schema, reader: &mut Preflight<'_>) -> Result<(usize, &'a Schema)> {
    let Schema::Union(union) = schema else { return Err(invalid()); };
    let index = usize::try_from(reader.int()?).map_err(|cause| error("avro-decode", cause))?;
    Ok((index, union.variants().get(index).ok_or_else(|| error("avro-decode", "Union index is out of range"))?))
}
