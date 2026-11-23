use super::{Action, Avro, AvroResolution, Kind, Result, error, invalid, nesting, overflow};

impl AvroResolution {
    pub(super) fn bound(&self, id: usize, datum: &Avro, depth: usize) -> Result<usize> {
        nesting(depth)?;
        let action = self.actions.get(id).ok_or_else(invalid)?;
        let add = |left: usize, right: usize| left.checked_add(right).ok_or_else(overflow);
        match (action, datum) {
            (Action::Error(message), _) => Err(error("avro-resolution", message)),
            (Action::Scalar { writer, fixed, .. }, value) => {
                if !scalar_matches(*writer, *fixed, value) { return Err(invalid()); }
                value_bound(value, depth)
            }
            (Action::Array(child), Avro::PackedArray(values)) => {
                if let Some((_, _, extra_resolution_level)) = self.packed_array_target(*child, values.kind, values.branches) {
                    packed_bound(values, depth, extra_resolution_level)
                } else {
                    self.bound(id, &Avro::Array(values.clone().into_physical_values()), depth)
                }
            }
            (Action::Array(child), Avro::Array(values)) => values.iter().try_fold(4096usize, |size, value| add(size, self.bound(*child, value, depth + 1)?)),
            (Action::Map { child, .. }, Avro::Map(values)) => values.iter().try_fold(4096usize, |size, (key, value)| add(add(size, key.len().checked_mul(8).ok_or_else(overflow)?)?, self.bound(*child, value, depth + 1)?)),
            (Action::Enum(symbols), Avro::Enum(index, _)) => match symbols.get(*index as usize).and_then(Option::as_ref) { Some((_, symbol)) => add(4096, symbol.len()), None => Err(error("avro-resolution", "Reader enum has no symbol or default")) },
            (Action::Record { writer_names, fields, reader_names, defaults }, Avro::Record(values)) => {
                if values.len() != writer_names.len() || values.iter().zip(writer_names).any(|((name, _), expected)| name != expected) { return Err(invalid()); }
                let mut bytes = reader_names.iter().try_fold(4096usize, |bytes, name| add(add(bytes, name.len())?, 256))?;
                for ((_, value), field) in values.iter().zip(fields) { if let Some((_, child)) = field { bytes = add(bytes, self.bound(*child, value, depth + 1)?)?; } }
                for (_, value) in defaults { bytes = add(bytes, value_bound(value, depth + 1)?)?; }
                Ok(bytes)
            }
            (Action::WriterUnion { branches, .. }, Avro::Union(index, value)) => add(4096, self.bound(*branches.get(*index as usize).ok_or_else(invalid)?, value, depth + 1)?),
            (Action::ReaderUnion { child, .. }, value) => add(4096, self.bound(*child, value, depth + 1)?),
            _ => Err(invalid()),
        }
    }

    pub(super) fn apply(&self, id: usize, datum: Avro, depth: usize) -> Result<Avro> {
        nesting(depth)?;
        match (self.actions.get(id).ok_or_else(invalid)?, datum) {
            (Action::Error(message), _) => Err(error("avro-resolution", message)),
            (Action::Scalar { reader, .. }, value) => promote(value, *reader),
            (Action::Array(child), Avro::PackedArray(values)) => {
                if let Some((reader, branches, _)) = self.packed_array_target(*child, values.kind, values.branches) {
                    let mut values = values.promote(reader);
                    values.branches = branches;
                    Ok(Avro::PackedArray(values))
                } else {
                    self.apply(id, Avro::Array(values.into_physical_values()), depth)
                }
            }
            (Action::Array(child), Avro::Array(values)) => Ok(Avro::Array(values.into_iter().map(|value| self.apply(*child, value, depth + 1)).collect::<Result<_>>()?)),
            (Action::Map { child, strings }, Avro::Map(values)) => Ok(Avro::Map(values.map_values(|value| self.apply(*child, value, depth + 1))?.with_strings(*strings))),
            (Action::Enum(symbols), Avro::Enum(index, _)) => match symbols.get(index as usize).and_then(Option::as_ref) { Some((index, symbol)) => Ok(Avro::Enum(*index, symbol.clone())), None => Err(invalid()) },
            (Action::Record { fields, reader_names, defaults, .. }, Avro::Record(values)) => {
                let mut output = vec![None; reader_names.len()];
                for ((_, value), field) in values.into_iter().zip(fields) { if let Some((index, child)) = field { output[*index] = Some(self.apply(*child, value, depth + 1)?); } }
                for (index, value) in defaults { output[*index] = Some(value.clone()); }
                Ok(Avro::Record(reader_names.iter().cloned().zip(output).map(|(name, value)| Ok((name, value.ok_or_else(invalid)?))).collect::<Result<_>>()?))
            }
            (Action::WriterUnion { branches, equivalent }, Avro::Union(index, value)) => {
                let value = self.apply(*branches.get(index as usize).ok_or_else(invalid)?, *value, depth + 1)?;
                Ok(if *equivalent { Avro::Union(index, Box::new(value)) } else { value })
            }
            (Action::ReaderUnion { index, child }, value) => Ok(Avro::Union(*index, Box::new(self.apply(*child, value, depth + 1)?))),
            _ => Err(invalid()),
        }
    }
}

pub(in crate::connect::avro) fn value_bound(value: &Avro, depth: usize) -> Result<usize> {
    nesting(depth)?;
    let sum = |left: usize, right: usize| left.checked_add(right).ok_or_else(overflow);
    match value {
        Avro::String(value) => sum(4096, value.len().checked_mul(4).ok_or_else(overflow)?),
        Avro::Utf8(value) | Avro::Bytes(value) | Avro::Fixed(_, value) => sum(4096, value.len().checked_mul(4).ok_or_else(overflow)?),
        Avro::Enum(_, value) => sum(4096, value.len()),
        Avro::PackedArray(values) => packed_bound(values, depth, 0),
        Avro::Array(values) => values.iter().try_fold(4096usize, |bytes, value| sum(bytes, value_bound(value, depth + 1)?)),
        Avro::Map(values) => values.iter().try_fold(4096usize, |bytes, (key, value)| sum(sum(bytes, key.len().checked_mul(8).ok_or_else(overflow)?)?, value_bound(value, depth + 1)?)),
        Avro::Record(values) => values.iter().try_fold(4096usize, |bytes, (key, value)| sum(sum(bytes, key.len())?, value_bound(value, depth + 1)?)),
        Avro::Union(_, value) => sum(4096, value_bound(value, depth + 1)?),
        Avro::Null | Avro::Boolean(_) | Avro::Int(_) | Avro::Long(_) | Avro::Float(_) | Avro::Double(_) => Ok(4096),
    }
}
fn packed_bound(values: &super::super::datum::PackedArray, depth: usize, extra_resolution_level: usize) -> Result<usize> {
    let child_levels = 1 + usize::from(values.branches.is_some()) + extra_resolution_level;
    if !values.values.is_empty() { nesting(depth + child_levels)?; }
    values.values.len().checked_mul(4096 * child_levels).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)
}

fn scalar_matches(kind: Kind, fixed: Option<usize>, value: &Avro) -> bool {
    match (kind, value) {
        (Kind::Null, Avro::Null) | (Kind::Boolean, Avro::Boolean(_)) | (Kind::Int, Avro::Int(_)) | (Kind::Long, Avro::Long(_)) | (Kind::Float, Avro::Float(_)) | (Kind::Double, Avro::Double(_)) | (Kind::String, Avro::String(_) | Avro::Utf8(_)) | (Kind::Bytes, Avro::Bytes(_)) => true,
        (Kind::Fixed, Avro::Fixed(size, bytes)) => fixed == Some(*size) && bytes.len() == *size,
        _ => false,
    }
}
fn promote(value: Avro, reader: Kind) -> Result<Avro> {
    Ok(match (value, reader) {
        (Avro::Int(value), Kind::Long) => Avro::Long(i64::from(value)), (Avro::Int(value), Kind::Float) => Avro::Float(value as f32), (Avro::Int(value), Kind::Double) => Avro::Double(f64::from(value)),
        (Avro::Long(value), Kind::Float) => Avro::Float(value as f32), (Avro::Long(value), Kind::Double) => Avro::Double(value as f64), (Avro::Float(value), Kind::Double) => Avro::Double(f64::from(value)),
        (Avro::String(value), Kind::Bytes) => Avro::Bytes(value.into_bytes()),
        (Avro::Utf8(value), Kind::Bytes) => Avro::Bytes(value),
        (Avro::Bytes(value), Kind::String) => match String::from_utf8(value) { Ok(text) => Avro::String(text), Err(cause) => Avro::Utf8(cause.into_bytes()) },
        (value, _) => value,
    })
}
