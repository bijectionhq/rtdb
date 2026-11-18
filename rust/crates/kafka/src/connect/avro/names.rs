use apache_avro::{AvroResult, error::Details, schema::{Name, Namespace, SchemaNamePolicy}};
use rtdb_types::Result;
use super::error;


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
impl SchemaNamePolicy for JavaNames {
    fn name(&self, name: &str, namespace: &Namespace) -> AvroResult<Name> { NameParts::parse(name, namespace.as_deref()).map(NameParts::owned) }
    fn record_field(&self, name: &str) -> AvroResult<()> { if simple_name(name) { Ok(()) } else { Err(Details::FieldName(name.into()).into()) } }
    fn enum_symbol(&self, symbol: &str) -> AvroResult<()> { if simple_name(symbol) { Ok(()) } else { Err(Details::EnumSymbolName(symbol.into()).into()) } }
    fn reference_fallback(&self, name: &str, _namespace: &Namespace) -> AvroResult<Option<Name>> {
        if name.contains('.') { Ok(None) } else { NameParts::parse(name, None).map(|name| Some(name.owned())) }
    }
}

#[cfg(test)] #[path = "names_tests.rs"] mod tests;
