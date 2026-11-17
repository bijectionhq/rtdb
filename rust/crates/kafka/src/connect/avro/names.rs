use apache_avro::{AvroResult, error::Details, schema::{Name, Namespace, RecordField, RecordSchema, SchemaNamePolicy}};
use rtdb_types::Result;
use super::{Avro, error};

mod grammar;
pub(super) use grammar::validate_ordered;

static BMP_FLAGS: &[u8; 16384] = include_bytes!("names/bmp-flags.bin");

pub(super) struct JavaNames;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct NameParts<'a> { pub namespace: &'a str, pub simple: &'a str }

pub(super) fn unit_flags(unit: u16) -> u8 { (BMP_FLAGS[usize::from(unit) / 4] >> (2 * (unit % 4))) & 3 }
fn simple_name(name: &str) -> bool {
    let mut units = name.encode_utf16();
    units.next().is_some_and(|unit| unit_flags(unit) & 1 != 0) && units.all(|unit| unit_flags(unit) & 2 != 0)
}
impl<'a> NameParts<'a> {
    pub fn parse(name: &'a str, namespace: Option<&'a str>) -> AvroResult<Self> {
        let (namespace, simple) = name.rsplit_once('.').unwrap_or((namespace.unwrap_or(""), name));
        if !simple_name(simple) { return Err(Details::InvalidSchemaName(name.into(), "Java UTF16 letter or underscore, followed by letters, decimal digits or underscore").into()); }
        Ok(Self { namespace, simple })
    }
    pub fn owned(self) -> Name { Name { name: self.simple.into(), namespace: Some(self.namespace.into()) } }
    pub fn owned_text_bytes(self) -> Result<usize> { self.namespace.len().checked_add(self.simple.len()).ok_or_else(|| error("allocation-overflow", "Avro name size overflow")) }
    pub fn fullname_bytes(self) -> Result<usize> { self.owned_text_bytes()?.checked_add(usize::from(!self.namespace.is_empty())).ok_or_else(|| error("allocation-overflow", "Avro fullname size overflow")) }
    pub fn fullname(self) -> Result<String> {
        let mut name = String::with_capacity(self.fullname_bytes()?);
        if !self.namespace.is_empty() { name.push_str(self.namespace); name.push('.'); }
        name.push_str(self.simple);
        Ok(name)
    }
}
pub(in crate::connect) fn canonical_fullname(name: &str, namespace: Option<&str>) -> Result<String> {
    NameParts::parse(name, namespace).map_err(|cause| error("avro-schema", cause))?.fullname()
}
impl SchemaNamePolicy for JavaNames {
    fn name(&self, name: &str, namespace: &Namespace) -> AvroResult<Name> { NameParts::parse(name, namespace.as_deref()).map(NameParts::owned) }
    fn record_field(&self, name: &str) -> AvroResult<()> { if simple_name(name) { Ok(()) } else { Err(Details::FieldName(name.into()).into()) } }
    fn enum_symbol(&self, symbol: &str) -> AvroResult<()> { if simple_name(symbol) { Ok(()) } else { Err(Details::EnumSymbolName(symbol.into()).into()) } }
    fn reference_fallback(&self, name: &str, _namespace: &Namespace) -> AvroResult<Option<Name>> {
        if name.contains('.') { Ok(None) } else { NameParts::parse(name, None).map(|name| Some(name.owned())) }
    }
}

pub(super) struct LookupName<'a> { bytes: std::str::Bytes<'a>, scrub: bool, prefix: bool, pending: [u8; 2], offset: usize }
impl<'a> LookupName<'a> {
    pub fn new(name: &'a str, scrub: bool) -> Self { Self { bytes: name.bytes(), scrub, prefix: scrub && name.as_bytes().first().is_some_and(u8::is_ascii_digit), pending: [0; 2], offset: 2 } }
    pub fn matches(name: &str, candidate: &str, scrub: bool) -> bool { LookupName::new(name, scrub).eq(candidate.bytes()) }
}
impl Iterator for LookupName<'_> {
    type Item = u8;
    fn next(&mut self) -> Option<Self::Item> {
        if self.prefix { self.prefix = false; return Some(b'x'); }
        if self.offset < 2 { let byte = self.pending[self.offset]; self.offset += 1; return Some(byte); }
        let byte = self.bytes.next()?;
        Some(if !self.scrub || byte.is_ascii_alphanumeric() || byte == b'_' { byte }
        else if matches!(byte, b' ' | b'-' | b'.' | b'*') { b'_' }
        else { let hex = b"0123456789ABCDEF"; self.pending = [hex[usize::from(byte >> 4)], hex[usize::from(byte & 15)]]; self.offset = 0; b'_' })
    }
}
pub(super) fn record_field<'a>(record: &'a RecordSchema, destination_name: &str, scrub: bool) -> Option<(usize, &'a RecordField)> {
    record.fields.iter().enumerate().find(|(_, field)| LookupName::matches(destination_name, &field.name, scrub))
}
#[derive(Clone, Copy)]
pub(super) struct Selected<'a> { pub schema: &'a apache_avro::Schema, pub value: &'a Avro }
pub(super) fn record_value<'a>(record: &'a RecordSchema, values: &'a [(String, Avro)], destination_name: &str, scrub: bool) -> Result<Selected<'a>> {
    if values.len() != record.fields.len() || values.iter().zip(&record.fields).any(|((name, _), field)| name != &field.name) { return Err(error("avro-decode", "Record value does not match its actual schema")); }
    let (index, field) = record_field(record, destination_name, scrub).ok_or_else(|| error("avro-decode", "Record lookup field is absent"))?;
    Ok(Selected { schema: &field.schema, value: &values[index].1 })
}

#[cfg(test)] #[path = "names_tests.rs"] mod tests;
