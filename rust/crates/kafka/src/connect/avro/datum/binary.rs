use super::{BuildDatum, PhysicalBuilder};
use super::{Avro, AvroDatumPlan, AvroMap, BinaryWindow, MapKey, NumericInput, PackedArray, Preflight, Result, Schema, error, resolve, resolution};

pub(super) fn decode(plan: &AvroDatumPlan, bytes: &[u8]) -> Result<Avro> {
    let mut reader = Preflight { bytes, offset: 0, window: BinaryWindow::new(bytes.len()), allocated: 0, limit: usize::MAX, names: &plan.names, view_bytes: 0 };
    value(&plan.root, &mut reader, 0)
}
pub(in crate::connect::avro) fn bytes(reader: &mut Preflight<'_>) -> Result<Vec<u8>> {
    let count = usize::try_from(reader.long()?).map_err(|cause| error("avro-decode", cause))?;
    Ok(reader.take(count)?.to_vec())
}
pub(in crate::connect::avro) fn block(reader: &mut Preflight<'_>) -> Result<usize> {
    let count = reader.long()?;
    let count = if count < 0 { reader.long()?; count.checked_neg().ok_or_else(resolution::overflow)? } else { count };
    usize::try_from(count).map_err(|cause| error("avro-decode", cause))
}
pub(in crate::connect::avro) fn value(schema: &Schema, reader: &mut Preflight<'_>, depth: usize) -> Result<Avro> {
    read(schema, reader, depth, &mut PhysicalBuilder)
}

pub(in crate::connect::avro) fn read<B: BuildDatum>(schema: &Schema, reader: &mut Preflight<'_>, depth: usize, builder: &mut B) -> Result<B::Node> {
    if depth > 128 { return Err(error("avro-decode", "Avro datum nesting limit exceeded")); }
    let names = reader.names;
    let schema = resolve(schema, names)?;
    let value = match schema {
        Schema::Union(union) => {
            let index = usize::try_from(reader.int()?).map_err(|cause| error("avro-decode", cause))?;
            let schema = union.variants().get(index).ok_or_else(|| error("avro-decode", "Union index is out of range"))?;
            let child = read(schema, reader, depth + 1, builder)?;
            return Ok(builder.union(index as u32, child));
        }
        Schema::Record(record) => {
            let fields = record.fields.iter().map(|field| Ok((field.name.clone(), read(&field.schema, reader, depth + 1, builder)?))).collect::<Result<_>>()?;
            return Ok(builder.record(fields));
        }
        Schema::Array(array) => {
            if builder.accepts_packed_arrays()
                && let Some(layout) = PackedArray::layout(&array.items)
                && let Some(writer) = NumericInput::from_schema(&array.items) {
                return Ok(builder.scalar(Avro::PackedArray(PackedArray::read(writer, layout, reader, depth + 1)?)));
            }
            let mut values = Vec::new();
            loop {
                let count = block(reader)?;
                if count == 0 { break; }
                for _ in 0..count { values.push(read(&array.items, reader, depth + 1, builder)?); }
            }
            return Ok(builder.array(values));
        }
        Schema::Map(map) => {
            let first = block(reader)?;
            let mut count = first;
            let mut entries = Vec::new();
            while count != 0 {
                for _ in 0..count {
                    entries.push((MapKey::Utf8(bytes(reader)?), read(&map.types, reader, depth + 1, builder)?));
                }
                count = block(reader)?;
            }
            return Ok(builder.map(first, entries, map.attributes.get("avro.java.string").and_then(serde_json::Value::as_str) == Some("String")));
        }
        Schema::Decimal(decimal) => return read(&decimal.inner, reader, depth + 1, builder),
        _ => scalar(schema, reader)?,
    };
    Ok(builder.scalar(value))
}

pub(in crate::connect::avro) fn scalar(schema: &Schema, reader: &mut Preflight<'_>) -> Result<Avro> {
    Ok(match schema {
        Schema::Null => Avro::Null,
        Schema::Boolean => Avro::Boolean(reader.take(1)?[0] == 1),
        Schema::Int | Schema::Date | Schema::TimeMillis => Avro::Int(reader.int()?),
        Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos => Avro::Long(reader.long()?),
        Schema::Float => Avro::Float(super::numeric::float(reader)?),
        Schema::Double => Avro::Double(super::numeric::double(reader)?),
        Schema::String | Schema::Uuid => match String::from_utf8(bytes(reader)?) { Ok(text) => Avro::String(text), Err(cause) => Avro::Utf8(cause.into_bytes()) },
        Schema::Bytes | Schema::BigDecimal => Avro::Bytes(bytes(reader)?),
        Schema::Fixed(fixed) => Avro::Fixed(fixed.size, reader.take(fixed.size)?.to_vec()),
        Schema::Duration => Avro::Fixed(12, reader.take(12)?.to_vec()),
        Schema::Enum(enumeration) => {
            let index = usize::try_from(reader.int()?).map_err(|cause| error("avro-decode", cause))?;
            let symbol = enumeration.symbols.get(index).ok_or_else(|| error("avro-decode", "Enum index is out of range"))?;
            Avro::Enum(index as u32, symbol.clone())
        }
        _ => return Err(error("avro-schema", "Unresolved schema reference")),
    })
}

impl Preflight<'_> {
    pub(in crate::connect::avro) fn skip(&mut self, schema: &Schema, depth: usize) -> Result<()> {
        if depth > 128 { return Err(error("avro-decode", "Avro datum nesting limit exceeded")); }
        let names = self.names;
        match resolve(schema, names)? {
            Schema::Null => (),
            Schema::Boolean => { self.take(1)?; }
            Schema::Int | Schema::Date | Schema::TimeMillis | Schema::Enum(_) => { self.int()?; }
            Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos => { self.long()?; }
            Schema::Float => { self.number_bytes(4)?; }
            Schema::Double => { self.number_bytes(8)?; }
            Schema::String | Schema::Uuid | Schema::Bytes | Schema::BigDecimal => self.skip_bytes()?,
            Schema::Fixed(fixed) => { self.take(fixed.size)?; }
            Schema::Duration => { self.take(12)?; }
            Schema::Decimal(decimal) => self.skip(&decimal.inner, depth + 1)?,
            Schema::Record(record) => for field in &record.fields { self.skip(&field.schema, depth + 1)?; },
            Schema::Union(union) => {
                let index = usize::try_from(self.int()?).map_err(|cause| error("avro-decode", cause))?;
                let selected = union.variants().get(index).ok_or_else(|| error("avro-decode", "Union index is out of range"))?;
                self.skip(selected, depth + 1)?;
            }
            Schema::Array(array) => self.skip_collection(&array.items, false, depth + 1)?,
            Schema::Map(map) => self.skip_collection(&map.types, true, depth + 1)?,
            Schema::Ref { .. } => return Err(error("avro-schema", "Unresolved schema reference")),
        }
        Ok(())
    }

    fn skip_bytes(&mut self) -> Result<()> {
        let size = self.long()?;
        if size >= 0 { self.take(usize::try_from(size).map_err(|cause| error("avro-decode", cause))?)?; }
        else {
            let rewind = usize::try_from(size.unsigned_abs()).map_err(|cause| error("avro-decode", cause))?;
            let offset = self.offset.checked_sub(rewind).filter(|offset| *offset >= self.window.origin).ok_or_else(|| error("avro-decode", "Skipped byte length rewinds before the admitted decoder window"))?;
            self.window.rewind_remaining = self.window.rewind_remaining.checked_sub(rewind).ok_or_else(|| error("avro-decode", "Skipped byte lengths exceed the datum rewind allowance"))?;
            self.offset = offset;
        }
        Ok(())
    }

    fn skip_collection(&mut self, schema: &Schema, map: bool, depth: usize) -> Result<()> {
        loop {
            let count = self.long()?;
            if count == 0 { return Ok(()); }
            if count < 0 { self.skip_bytes()?; continue; }
            for _ in 0..count {
                let before = (self.offset, self.window.rewind_remaining);
                if map { self.skip_bytes()?; }
                self.skip(schema, depth)?;
                if (self.offset, self.window.rewind_remaining) == before { break; }
            }
        }
    }
}
