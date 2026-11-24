use super::*;
mod map_order;
mod binary;
mod numeric;
pub(super) use numeric::{NumericInput, NumericKind, PackedArray, PackedLayout};
mod build;
pub(super) use build::{BuildDatum, PhysicalBuilder};
pub(super) use binary::{bytes as read_bytes, block as read_block, read as read_with, scalar as read_scalar};
#[cfg(test)] mod utf8_tests;

#[derive(Clone, Debug, PartialEq)]
pub(super) enum PhysicalValue {
    Null, Boolean(bool), Int(i32), Long(i64), Float(f32), Double(f64),
    String(String), Utf8(Vec<u8>), Bytes(Vec<u8>), Fixed(usize, Vec<u8>), Enum(u32, String),
    Array(Vec<Self>), PackedArray(PackedArray), Map(AvroMap), Record(Vec<(String, Self)>), Union(u32, Box<Self>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum MapKey { Utf8(Vec<u8>), String(String) }
impl MapKey {
    pub(super) fn len(&self) -> usize { match self { Self::Utf8(value) => value.len(), Self::String(value) => value.len() } }
    pub(super) fn into_text(self) -> String { match self { Self::Utf8(value) => java_utf8(&value), Self::String(value) => value } }
    fn as_string(&self) -> Self { match self { Self::Utf8(value) => Self::String(java_utf8(value)), Self::String(value) => Self::String(value.clone()) } }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum AvroMap<T = Avro> { Insertions { first_block: usize, entries: Vec<(MapKey, T)>, strings: bool }, Ordered(Vec<(MapKey, T)>) }
impl<T> AvroMap<T> {
    pub(super) fn new(first_block: usize, entries: Vec<(MapKey, T)>, strings: bool) -> Self { Self::Insertions { first_block, entries, strings } }
    fn entries(&self) -> &[(MapKey, T)] { match self { Self::Insertions { entries, .. } | Self::Ordered(entries) => entries } }
    pub(super) fn iter(&self) -> impl Iterator<Item = &(MapKey, T)> { self.entries().iter() }
    pub(super) fn is_empty(&self) -> bool { self.entries().is_empty() }
    pub(super) fn with_strings(self, strings: bool) -> Self { match self { Self::Insertions { first_block, entries, .. } => Self::new(first_block, entries, strings), Self::Ordered(entries) => Self::new(entries.len(), entries, strings) } }
    pub(super) fn map_values(self, mut map: impl FnMut(T) -> Result<T>) -> Result<Self> {
        let transform = |entries: Vec<(MapKey, T)>, map: &mut dyn FnMut(T) -> Result<T>| entries.into_iter().map(|(key, value)| Ok((key, map(value)?))).collect::<Result<Vec<_>>>();
        Ok(match self { Self::Insertions { first_block, entries, strings } => Self::new(first_block, transform(entries, &mut map)?, strings), Self::Ordered(entries) => Self::Ordered(transform(entries, &mut map)?) })
    }
    pub(super) fn map_visible_values(self, map: impl FnMut(T) -> Result<T>) -> Result<Self> { Self::Ordered(self.ordered()?).map_values(map) }
    fn order(&self) -> Result<Vec<usize>> {
        match self {
            Self::Ordered(entries) => Ok((0..entries.len()).collect()),
            Self::Insertions { first_block, entries, strings: true } => {
                let keys = entries.iter().map(|(key, _)| key.as_string()).collect::<Vec<_>>();
                map_order::indices(*first_block, keys.iter())
            }
            Self::Insertions { first_block, entries, strings: false } => map_order::indices(*first_block, entries.iter().map(|(key, _)| key)),
        }
    }
    pub(super) fn first_value(&self) -> Result<Option<&T>> { Ok(self.order()?.first().map(|index| &self.entries()[*index].1)) }
    pub(super) fn ordered(self) -> Result<Vec<(MapKey, T)>> {
        if let Self::Ordered(entries) = self { return Ok(entries); }
        let order = self.order()?;
        let Self::Insertions { entries, strings, .. } = self else { unreachable!() };
        let mut entries = entries.into_iter().map(Some).collect::<Vec<_>>();
        Ok(order.into_iter().map(|index| {
            let (key, value) = entries[index].take().expect("unique map iteration index");
            (if strings { MapKey::String(key.into_text()) } else { key }, value)
        }).collect())
    }
}

pub(in crate::connect) struct AvroDatum { pub(super) value: Avro, pub(super) plan: Arc<AvroDatumPlan> }
impl AvroDatum {
    pub fn is_null(&self) -> bool { let mut value = &self.value; while let Avro::Union(_, child) = value { value = child; } matches!(value, Avro::Null) }
    #[cfg(test)]
    pub(super) fn untracked(plan: Arc<AvroDatumPlan>, value: Avro) -> Self { Self { value, plan } }
}
pub(super) fn decode(plan: &AvroDatumPlan, bytes: &[u8]) -> Result<Avro> { binary::decode(plan, bytes) }
pub(super) fn string_map_order<T>(entries: &[(MapKey, T)]) -> Result<Vec<usize>> { map_order::indices(entries.len(), entries.iter().map(|(key, _)| key)) }
pub(super) fn selected<'a>(mut schema: &'a Schema, mut value: &'a Avro, names: &'a BTreeMap<String, Schema>) -> Result<(&'a Schema, &'a Avro)> {
    loop {
        schema = resolve(schema, names)?;
        match (schema, value) {
            (Schema::Union(union), Avro::Union(index, child)) => { schema = union.variants().get(*index as usize).ok_or_else(|| error("avro-decode", "Invalid datum union branch"))?; value = child; }
            _ => return Ok((schema, value)),
        }
    }
}
pub(super) fn java_utf8(mut bytes: &[u8]) -> String {
    let mut output = String::new();
    while !bytes.is_empty() {
        match std::str::from_utf8(bytes) {
            Ok(text) => { output.push_str(text); break; }
            Err(error) => {
                let valid = error.valid_up_to();
                output.push_str(std::str::from_utf8(&bytes[..valid]).expect("validated UTF-8 prefix")); bytes = &bytes[valid..];
                let surrogate = bytes.len() >= 2 && bytes[0] == 0xed && (0xa0..=0xbf).contains(&bytes[1]);
                let skip = if surrogate { if bytes.get(2).is_some_and(|byte| (0x80..=0xbf).contains(byte)) { 3 } else { 2 } } else { error.error_len().unwrap_or(bytes.len()) };
                output.push('\u{fffd}'); bytes = &bytes[skip..];
            }
        }
    }
    output
}
