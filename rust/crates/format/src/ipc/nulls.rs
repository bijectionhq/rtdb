use std::{borrow::Cow, io::Read};
use arrow_ipc::{CompressionType, MetadataVersion, RecordBatch};
use arrow_schema::{DataType, Schema, UnionMode};
use rtdb_types::Result;
use super::invalid;

#[cfg(test)]
mod tests;

#[allow(deprecated)]
pub(super) fn normalize(metadata: &mut [u8], body: &[u8], schema: &Schema) -> Result<()> {
    normalize_inspecting_unions(metadata, body, schema).map(drop)
}

/// Normalizes unknown (`-1`) field null counts in `metadata` without modifying `body`, and
/// reports whether any union column in the batch may hold the reference's `-1` undefined cells.
#[allow(deprecated)]
pub(super) fn normalize_inspecting_unions(metadata: &mut [u8], body: &[u8], schema: &Schema) -> Result<bool> {
    let (patches, may_hold_undefined_union) = {
        let message = arrow_ipc::root_as_message(metadata).map_err(invalid)?;
        let batch = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|dictionary| dictionary.data()));
        let Some(batch) = batch else { return Ok(false); };
        let nodes = batch.nodes().ok_or_else(|| invalid("Arrow batch has no field nodes"))?;
        if nodes.iter().any(|node| node.null_count() < -1) { return Err(invalid("Arrow null count is below the unknown-count sentinel")); }
        if !nodes.iter().any(|node| node.null_count() == -1) { return Ok(false); }
        let mut counts = NullCounts { batch, body, version: message.version(), metadata_base: metadata.as_ptr() as usize,
            node: 0, buffer: 0, variadic: 0, patches: Vec::with_capacity(nodes.len()), may_hold_undefined_union: false };
        if let Some(dictionary) = message.header_as_dictionary_batch() {
            let fields = schema.fields_with_dict_id(dictionary.id());
            let field = fields.first().ok_or_else(|| invalid("Arrow dictionary has no schema field"))?;
            let DataType::Dictionary(_, value) = field.data_type() else { return Err(invalid("Arrow dictionary field has no dictionary type")); };
            counts.field(value, 0)?;
        } else {
            for field in schema.fields() { counts.field(field.data_type(), 0)?; }
        }
        if counts.node != nodes.len() || counts.buffer != batch.buffers().map_or(0, |buffers| buffers.len())
            || counts.variadic != batch.variadicBufferCounts().map_or(0, |counts| counts.len()) {
            return Err(invalid("Arrow schema and field-node inventory disagree"));
        }
        (counts.patches, counts.may_hold_undefined_union)
    };
    for (offset, count) in patches {
        let end = offset.checked_add(8).ok_or_else(|| invalid("Arrow null-count offset overflow"))?;
        metadata.get_mut(offset..end).ok_or_else(|| invalid("Arrow null-count slot exceeds metadata"))?.copy_from_slice(&count.to_le_bytes());
    }
    Ok(may_hold_undefined_union)
}

struct NullCounts<'a> {
    batch: RecordBatch<'a>,
    body: &'a [u8],
    version: MetadataVersion,
    metadata_base: usize,
    node: usize,
    buffer: usize,
    variadic: usize,
    patches: Vec<(usize, i64)>,
    may_hold_undefined_union: bool,
}

impl NullCounts<'_> {
    fn field(&mut self, data_type: &DataType, depth: usize) -> Result<()> {
        if depth > 128 { return Err(invalid("Arrow null-count schema exceeds 128 levels")); }
        let nodes = self.batch.nodes().ok_or_else(|| invalid("Arrow batch has no field nodes"))?;
        if self.node >= nodes.len() { return Err(invalid("Arrow schema exceeds its field-node inventory")); }
        let node = nodes.get(self.node);
        self.node += 1;
        let length = usize::try_from(node.length()).map_err(invalid)?;
        let physical = match data_type { DataType::Dictionary(key, _) => key.as_ref(), value => value };
        if matches!(physical, DataType::Union(..)) {
            let type_ids = self.buffer + usize::from(self.version < MetadataVersion::V5);
            self.may_hold_undefined_union |= self.union_may_hold_undefined(type_ids, length)?;
        }
        if node.null_count() == -1 {
            let count = match physical {
                DataType::Null => length,
                DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => 0,
                _ => self.validity_count(length)?,
            };
            let offset = (node.0.as_ptr() as usize).checked_sub(self.metadata_base).and_then(|offset| offset.checked_add(8))
                .ok_or_else(|| invalid("Arrow null-count slot precedes metadata"))?;
            self.patches.push((offset, i64::try_from(count).map_err(invalid)?));
        }
        let buffers = match physical {
            DataType::Null | DataType::RunEndEncoded(_, _) => 0,
            DataType::Union(_, mode) => usize::from(self.version < MetadataVersion::V5) + if *mode == UnionMode::Dense { 2 } else { 1 },
            DataType::Struct(_) | DataType::FixedSizeList(_, _) => 1,
            DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => 2,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary | DataType::ListView(_) | DataType::LargeListView(_) => 3,
            DataType::Utf8View | DataType::BinaryView => {
                let counts = self.batch.variadicBufferCounts().ok_or_else(|| invalid("Arrow view has no variadic buffer counts"))?;
                if self.variadic >= counts.len() { return Err(invalid("Arrow view exceeds its variadic inventory")); }
                let count = usize::try_from(counts.get(self.variadic)).map_err(invalid)?;
                self.variadic += 1;
                count.checked_add(2).ok_or_else(|| invalid("Arrow view buffer-count overflow"))?
            }
            _ => 2,
        };
        self.buffer = self.buffer.checked_add(buffers).ok_or_else(|| invalid("Arrow buffer-count overflow"))?;
        match physical {
            DataType::Struct(fields) => for field in fields { self.field(field.data_type(), depth + 1)?; },
            DataType::Union(fields, _) => for (_, field) in fields.iter() { self.field(field.data_type(), depth + 1)?; },
            DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) | DataType::Map(field, _)
                | DataType::ListView(field) | DataType::LargeListView(field) => self.field(field.data_type(), depth + 1)?,
            DataType::RunEndEncoded(ends, values) => { self.field(ends.data_type(), depth + 1)?; self.field(values.data_type(), depth + 1)?; }
            _ => {}
        }
        Ok(())
    }

    fn validity_count(&self, length: usize) -> Result<usize> {
        let buffers = self.batch.buffers().ok_or_else(|| invalid("Arrow batch has no buffer inventory"))?;
        if self.buffer >= buffers.len() { return Err(invalid("Arrow validity exceeds its buffer inventory")); }
        let buffer = buffers.get(self.buffer);
        let start = usize::try_from(buffer.offset()).map_err(invalid)?;
        let size = usize::try_from(buffer.length()).map_err(invalid)?;
        let end = start.checked_add(size).ok_or_else(|| invalid("Arrow validity buffer offset overflow"))?;
        let encoded = self.body.get(start..end).ok_or_else(|| invalid("Arrow validity exceeds the message body"))?;
        if encoded.is_empty() { return Ok(0); }
        let bits = if let Some(compression) = self.batch.compression() {
            let prefix: [u8; 8] = encoded.get(..8).ok_or_else(|| invalid("Compressed Arrow validity has no length"))?.try_into().map_err(invalid)?;
            let expected = i64::from_le_bytes(prefix);
            if expected == -1 { Cow::Borrowed(&encoded[8..]) } else if expected == 0 { Cow::Borrowed(&[][..]) } else {
                let expected = usize::try_from(expected).map_err(invalid)?;
                let decoded = match compression.codec() {
                    CompressionType::LZ4_FRAME => {
                        let limit = u64::try_from(expected).map_err(invalid)?.checked_add(1).ok_or_else(|| invalid("Arrow validity length overflow"))?;
                        let mut decoder = lz4_flex::frame::FrameDecoder::new(&encoded[8..]).take(limit);
                        let mut decoded = Vec::with_capacity(expected);
                        decoder.read_to_end(&mut decoded).map_err(invalid)?;
                        decoded
                    }
                    CompressionType::ZSTD => zstd::bulk::decompress(&encoded[8..], expected).map_err(invalid)?,
                    _ => return Err(invalid("Unknown Arrow validity compression codec")),
                };
                if decoded.len() != expected { return Err(invalid("Arrow validity differs from its declared buffer length")); }
                Cow::Owned(decoded)
            }
        } else { Cow::Borrowed(encoded) };
        if bits.is_empty() { return Ok(0); }
        let whole = length / 8;
        let remainder = length % 8;
        let required = whole.checked_add(usize::from(remainder != 0)).ok_or_else(|| invalid("Arrow validity length overflow"))?;
        if bits.len() < required { return Err(invalid("Arrow validity bitmap is shorter than its field")); }
        let mut valid = bits[..whole].iter().map(|byte| byte.count_ones() as usize).sum::<usize>();
        if remainder != 0 { valid += (bits[whole] & ((1u8 << remainder) - 1)).count_ones() as usize; }
        Ok(length - valid)
    }

    fn union_may_hold_undefined(&self, buffer_index: usize, length: usize) -> Result<bool> {
        let buffers = self.batch.buffers().ok_or_else(|| invalid("Arrow batch has no buffer inventory"))?;
        if buffer_index >= buffers.len() { return Err(invalid("Arrow union exceeds its buffer inventory")); }
        let buffer = buffers.get(buffer_index);
        let start = usize::try_from(buffer.offset()).map_err(invalid)?;
        let size = usize::try_from(buffer.length()).map_err(invalid)?;
        let end = start.checked_add(size).ok_or_else(|| invalid("Arrow union buffer offset overflow"))?;
        let encoded = self.body.get(start..end).ok_or_else(|| invalid("Arrow union exceeds the message body"))?;
        if encoded.is_empty() { return Ok(false); }
        if self.batch.compression().is_some() {
            let prefix: [u8; 8] = encoded.get(..8).ok_or_else(|| invalid("Compressed Arrow union has no length"))?.try_into().map_err(invalid)?;
            if i64::from_le_bytes(prefix) != -1 { return Ok(true); }
            return Ok(encoded[8..].iter().take(length).any(|byte| (*byte as i8) < 0));
        }
        Ok(encoded.iter().take(length).any(|byte| (*byte as i8) < 0))
    }
}
