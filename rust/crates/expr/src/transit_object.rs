use std::sync::Arc;
use rtdb_format::transit::{TransitReadBuilder,TransitValue as T,decode_with};
use rtdb_types::{Anomaly,Result,TableRef};
mod number;
mod default_handlers;
mod collection;
mod equal;
mod emitted;
mod hash;
mod time;
pub(crate) mod legacy_date_text;
mod path;
mod throwable;
mod format_text;
pub(crate) use format_text::{FormatArgument,format_argument};
mod tagged_value;
mod render;
mod table;
mod allocation;
mod vector_type;
use crate::EvaluationScope;
use crate::admission::Admission;
#[cfg(test)]
mod admission_tests;
use allocation::wire_charge;
use collection::{Array,Map,Record,Entries};
use number::Number;
use time::Time;
use path::Path;
use throwable::Throwable;
use tagged_value::Tagged;
use vector_type::Type;

#[derive(Clone,Debug)]
pub(crate) enum Object {
    Null,Bool(bool),Number(Arc<Number>),String(String),Keyword(String),Symbol(String),Character(u16),
    Bytes(Arc<Vec<u8>>),Vector(Vec<Self>),List(Vec<Self>),Set(Map),Map(Map),
    Uri(String),Uuid([i64;2]),ByteBuffer(Arc<Vec<u8>>),KeyFn(String),ClojureForm(Box<Self>),Time(Time),Path(Path),TableRef(Arc<TableRef>),VectorType(Arc<Type>),
    TxKey(Record),Throwable(Arc<Throwable>),Link(Arc<Vec<(Self,Self)>>),Tagged(Arc<Tagged>),Reg(Arc<Reg>),
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum RegKind { Oid,RegClass,RegProc }
#[derive(Debug,PartialEq,Eq)]
pub(crate) struct Reg { kind: RegKind,oid: i32 }

impl RegKind {
    fn tag(self) -> &'static str { match self { Self::Oid => "xt/oid",Self::RegClass => "xt/regclass",Self::RegProc => "xt/regproc" } }
    fn class(self) -> &'static str { match self { Self::Oid => "Oid",Self::RegClass => "RegClass",Self::RegProc => "RegProc" } }
}

fn invalid() -> Anomaly { Anomaly::incorrect("xtdb.expression/invalid-transit-object", "Invalid stored Transit object representation") }
fn tagged(tag: &str,representation: T) -> T { T::Tagged { tag: tag.into(),representation: Box::new(representation) } }

fn wire_text_object(tag: &str, representation: &T) -> Result<Option<Object>> {
    if tag == "xtdb/path" {
        return Ok(Some(Object::Path(Path::read_wire(representation)?)));
    }
    if tag == "xtdb/tstz-range" { return Ok(Some(Object::Time(Time::read_wire_range(representation)?))); }
    if Time::recognizes(tag) && !matches!(tag, "m" | "t") {
        let T::String(source) = representation else { return Err(invalid()); };
        return Ok(Some(Time::read_text(tag, source)?.map_or(Object::Null, Object::Time)));
    }
    Ok(None)
}

fn read_object(bytes: &[u8],admission: Admission<'_>) -> Result<Object> { decode_with(bytes,&mut ObjectReadBuilder { admission }) }

/// `(pr-str v)` of the value serde reads from `bytes`.
pub(crate) fn printed_text(bytes: &[u8],admission: Admission<'_>) -> Result<String> { render::printed(&read_object(bytes,admission)?,admission) }

/// The `[:transit :utf8]` cast: a string is returned as itself, anything else as `(pr-str v)`.
pub(crate) fn cast_text(bytes: &[u8],admission: Admission<'_>) -> Result<String> {
    match read_object(bytes,admission)? { Object::String(value) => Ok(value),value => render::printed(&value,admission) }
}

/// `(str v)`, with `nil` kept distinct so that callers can pass it through as SQL NULL.
pub(crate) fn str_text(bytes: &[u8],admission: Admission<'_>) -> Result<Option<String>> {
    match read_object(bytes,admission)? { Object::Null => Ok(None),value => render::string(&value,admission).map(Some) }
}

pub(crate) struct Decoded<'a> { value: Object,allocation: &'a EvaluationScope }

pub(crate) fn scope(allocation: Option<&EvaluationScope>) -> Result<&EvaluationScope> {
    allocation.ok_or_else(||Anomaly::incorrect("rtdb.expr/missing-allocation-scope", "Stored Transit object evaluation requires an allocation scope"))
}

impl<'a> Decoded<'a> {
    pub(crate) fn read(bytes: &[u8],allocation: &'a EvaluationScope) -> Result<Self> {
        let count = bytes.len();
        let bytes_required = count.checked_mul(count).and_then(|bytes|bytes.checked_mul(16)).and_then(|bytes|count.checked_mul(64).and_then(|raw|bytes.checked_add(raw))).and_then(|bytes|bytes.checked_add(4096)).ok_or_else(crate::numeric::overflow)?;
        allocation.reserve(bytes_required)?;
        let value = decode_with(bytes,&mut ObjectReadBuilder { admission: Admission::Tracked(allocation) })?;
        Ok(Self { value,allocation })
    }
    pub(crate) fn field(self,name: Option<&str>) -> Result<Option<Self>> {
        let Self { value,allocation } = self;
        Ok(value.field(name)?.map(|value|Self { value,allocation }))
    }
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let value = self.value.wire(self.allocation)?;
        self.allocation.reserve(rtdb_format::transit::msgpack_allocation_bound(&value)?)?;
        rtdb_format::transit::encode_msgpack(&value)
    }
    pub(crate) fn equivalent(&self,other: &Self) -> Result<bool> { self.value.equivalent(&other.value) }
    pub(crate) fn equivalent_emitted(&self,other: &rtdb_types::Value) -> Result<bool> { self.value.equivalent_emitted(other,self.allocation) }
    pub(crate) fn equivalent_number(&self, other: &Number) -> bool {
        matches!(&self.value, Object::Number(number) if number.equivalent(other))
    }
}

struct ObjectReadBuilder<'a> { admission: Admission<'a> }
impl TransitReadBuilder for ObjectReadBuilder<'_> {
    type Value = Object;
    type Array = Array;
    type Map = Map;
    fn null(&mut self) -> Result<Object> { Ok(Object::Null) }
    fn boolean(&mut self,value: bool) -> Result<Object> { Ok(Object::Bool(value)) }
    fn integer(&mut self,value: i64) -> Result<Object> { Ok(Object::number(Number::Long(value))) }
    fn float(&mut self,value: f64) -> Result<Object> { Ok(Object::number(Number::Double(value))) }
    fn string(&mut self,value: String) -> Result<Object> { Ok(Object::String(value)) }
    fn keyword(&mut self,value: String) -> Result<Object> { Ok(Object::Keyword(value)) }
    fn symbol(&mut self,value: String) -> Result<Object> { Ok(Object::Symbol(value)) }
    fn bytes(&mut self,value: Vec<u8>) -> Result<Object> { Ok(Object::Bytes(Arc::new(value))) }
    fn binary(&mut self,representation: String) -> Result<Object> { default_handlers::binary(&representation) }
    fn array_handler(&self,tag: &str) -> bool { matches!(tag,"list" | "set" | "cmap") }
    fn start_array(&mut self,tag: Option<&str>,size: usize) -> Result<Array> { Array::new(tag,size) }
    fn array_item(&mut self,array: &mut Array,value: Object) -> Result<()> { array.push(value) }
    fn finish_array(&mut self,array: Array) -> Result<Object> { Ok(array.finish()) }
    fn start_map(&mut self,_size: usize) -> Result<Map> { Ok(Map::new()) }
    fn map_entry(&mut self,map: &mut Map,key: Object,value: Object) -> Result<()> { map.assoc(key,value) }
    fn finish_map(&mut self,map: Map) -> Result<Object> { Ok(Object::Map(map)) }
    fn tagged(&mut self,tag: String,representation: Object) -> Result<Object> { Object::registered(tag,representation,self.admission) }
}

impl Object {
    fn number(value: Number) -> Self { Self::Number(Arc::new(value)) }
    fn registered(tag: String,representation: Self,allocation: Admission<'_>) -> Result<Self> {
        Ok(match tag.as_str() {
            "'" => representation,
            "cmap" => Self::Null,
            "_" => Self::Null,
            "?" => Self::Bool(representation.text()? == "t"),
            ":" => match representation { Self::Keyword(value) | Self::Symbol(value) | Self::String(value) => Self::Keyword(value),_ => Self::Null },
            "$" => match representation { Self::Keyword(value) | Self::Symbol(value) | Self::String(value) => Self::Symbol(value),_ => return Err(invalid()) },
            "i" => Self::number(Number::parse_long(representation.text()?).map_err(number::error)?),
            "d" => Self::number(Number::parse_double(representation.text()?).map_err(number::error)?),
            "b" => default_handlers::binary(representation.text()?)?,
            "z" => Self::number(Number::Double(match representation.text()? { "NaN" => f64::NAN,"INF" => f64::INFINITY,"-INF" => f64::NEG_INFINITY,_ => return Err(invalid()) })),
            "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => Self::number(number::cast(&tag,&representation)?),
            "n" => Self::number(Number::integer(representation.text()?).map_err(number::error)?),
            "f" => Self::number(Number::decimal(representation.text()?).map_err(number::error)?),
            "ratio" => {
                let values = representation.list().ok_or_else(invalid)?;
                let (Some(Self::Number(left)),Some(Self::Number(right))) = (values.first(),values.get(1)) else { return Err(invalid()); };
                if left.is_nan() { return Ok(Self::Number(Arc::clone(left))); }
                if right.is_nan() { return Ok(Self::Number(Arc::clone(right))); }
                Self::number(Number::divide(left,right).map_err(number::error)?)
            }
            "c" => Self::Character(representation.text()?.encode_utf16().next().ok_or_else(invalid)?),
            "r" => { let value = representation.text()?;rtdb_types::UriParts::parse(value)?;Self::Uri(value.into()) }
            "u" => Self::Uuid(uuid(&representation)?),
            "xtdb/byte-array" => Self::ByteBuffer(Arc::new(byte_buffer(representation.text()?)?)),
            "xtdb/key-fn" => match representation {
                Self::KeyFn(value) => Self::KeyFn(value),
                Self::Keyword(value) if ["kebab-case-string","kebab-case-keyword","snake-case-string","snake-case-keyword"].contains(&value.as_str()) => Self::KeyFn(value),
                _ => return Err(invalid()),
            },
            "xtdb/clj-form" => { if matches!(representation,Self::Null) { return Err(invalid()); }Self::ClojureForm(Box::new(representation)) }
            "xtdb/period-duration" => return Err(invalid()),
            "list" | "set" => representation,
            "with-meta" => default_handlers::with_meta(representation)?,
            "link" => match representation { Self::Map(map) => Self::Link(Arc::new(map.into_entries())),Self::TxKey(record) => Self::Link(Arc::new(record.into_entries())),_ => return Err(invalid()) },
            "xtdb/tx-key" => Self::TxKey(Record::read(representation)?),
            "xtdb/path" => Self::Path(Path::read(representation.text()?)?),
            "xt/oid" | "xt/regclass" | "xt/regproc" => {
                let kind = match tag.as_str() { "xt/oid" => RegKind::Oid,"xt/regclass" => RegKind::RegClass,_ => RegKind::RegProc };
                let Number::Int(oid) = number::cast("i32",&representation)? else { return Err(invalid()); };
                Self::Reg(Arc::new(Reg { kind,oid }))
            }
            "xt/table" => Self::TableRef(Arc::new(table::read(representation,allocation)?)),
            "xtdb/type" => Self::VectorType(Type::read(&representation,allocation)?),
            "xt/error" | "xtdb/illegal-arg" | "xtdb/runtime-err" | "xtdb/exception-info" => Self::Throwable(Arc::new(Throwable::read(&tag,&representation)?)),
            tag if Time::recognizes(tag) => Time::read(tag,&representation)?.map_or(Self::Null,Self::Time),
            _ => Self::Tagged(Arc::new(Tagged::new(tag,representation))),
        })
    }
    fn text(&self) -> Result<&str> { match self { Self::String(value) => Ok(value),_ => Err(invalid()) } }
    fn list(&self) -> Option<&[Self]> { match self { Self::Vector(values) | Self::List(values) => Some(values),_ => None } }
    fn sequence(&self) -> Option<&[Self]> { if matches!(self,Self::Null) { Some(&[]) } else { self.list() } }
    fn truthy(&self) -> bool { !matches!(self,Self::Null | Self::Bool(false)) }
    fn entries(&self) -> Option<Entries<'_>> { match self { Self::Map(map) => Some(map.entries()),Self::TxKey(record) => Some(record.entries()),_ => None } }
    fn destructure_map(self) -> Result<Self> {
        Ok(match self {
            Self::List(mut values) => match values.len() {
                0 => Self::Map(Map::new()),1 => values.pop().unwrap(),_ => Self::Map(Map::from_flat_sequence(values)?),
            },
            value => value,
        })
    }
    fn lookup_keyword(&self,name: &str) -> Result<Self> {
        let key = Self::Keyword(name.into());
        Ok(match self {
            Self::Map(map) => map.get(&key)?.cloned().unwrap_or(Self::Null),
            Self::TxKey(record) => record.get(&key)?.cloned().unwrap_or(Self::Null),
            Self::Set(set) => if set.get(&key)?.is_some() { key } else { Self::Null },
            _ => Self::Null,
        })
    }
    fn assoc_keyword(&mut self,name: &str,value: Self) -> Result<()> {
        match self { Self::Map(map) => map.assoc(Self::Keyword(name.into()),value),Self::TxKey(record) => record.assoc_keyword(name,value),_ => Err(invalid()) }
    }
    fn strip_error_metadata(&mut self) -> Result<()> {
        match self {
            Self::Map(map) => for name in ["xtdb.error/cause","xtdb.error/code","cognitect.anomalies/category"] { map.remove_keyword(name); },
            Self::TxKey(record) => record.strip_error_metadata(),_ => return Err(invalid()),
        }Ok(())
    }
    fn take_error_cause(&mut self) -> Result<Option<Self>> {
        match self { Self::Map(map) => Ok(map.remove_keyword("xtdb.error/cause")),Self::TxKey(record) => Ok(record.take_error_cause()),_ => Err(invalid()) }
    }
    fn field(self,name: Option<&str>) -> Result<Option<Self>> {
        let object = match self {
            Self::Throwable(value) => value.data().clone(),value => value,
        };
        let Some(mut entries) = object.entries() else { return Ok(None); };
        let first = entries.find(|(key,_)| match (key,name) { (Self::Keyword(key),Some(name)) => key == name,(Self::Null,None) => true,_ => false }).map(|(_,value)|value);
        Ok(match first.filter(|value|value.truthy()) {
            Some(value) => Some(value.clone()),
            None => {
                let name = name.ok_or_else(||Anomaly::incorrect("xtdb.expression/missing-field-name", "Transit map access requires field metadata"))?;
                object.entries().ok_or_else(invalid)?.find(|(key,_)|matches!(key,Self::String(key) if key == name)).map(|(_,value)|value).filter(|value|value.truthy()).cloned()
            }
        })
    }
    fn wire(&self,allocation: &EvaluationScope) -> Result<T> {
        self.wire_with(Admission::Tracked(allocation))
    }
    fn wire_with(&self,allocation: Admission<'_>) -> Result<T> {
        wire_charge(self,allocation)?;
        Ok(match self {
            Self::Null => T::Null,Self::Bool(value) => T::Bool(*value),Self::Number(value) => number::wire(value),Self::String(value) => T::String(value.clone()),Self::Keyword(value) => T::Keyword(value.clone()),Self::Symbol(value) => T::Symbol(value.clone()),
            Self::Character(value) => tagged("c",T::String(char::from_u32(u32::from(*value)).unwrap_or('?').to_string())),
            Self::Bytes(value) => T::Bytes(value.as_ref().clone()),
            Self::Vector(values) => T::Array(wire_values(values.iter(),values.len(),allocation)?),
            Self::List(values) => tagged("list",T::Array(wire_values(values.iter(),values.len(),allocation)?)),
            Self::Set(values) => tagged("set",T::Array(wire_values(values.entries().map(|(key,_)|key),values.len(),allocation)?)),
            Self::Map(map) => map_wire(map,allocation)?,
            Self::TxKey(record) => tagged("xtdb/tx-key",map_wire(&Map::from_entries(record.entries().map(|(key,value)|(key.clone(),value.clone())))?,allocation)?),
            Self::Uri(value) => tagged("r",T::String(value.clone())),Self::Uuid(values) => tagged("u",T::Array(values.iter().map(|value|T::Integer(*value)).collect())),
            Self::ByteBuffer(value) => { let mut text = String::with_capacity(value.len().checked_mul(2).and_then(|bytes|bytes.checked_add(2)).ok_or_else(crate::numeric::overflow)?);text.push_str("0x");const HEX: &[u8] = b"0123456789abcdef";for byte in value.iter() { text.push(HEX[(byte >> 4) as usize] as char);text.push(HEX[(byte & 15) as usize] as char); }tagged("xtdb/byte-array",T::String(text)) }
            Self::KeyFn(value) => tagged("xtdb/key-fn",T::Keyword(value.clone())),Self::ClojureForm(value) => tagged("xtdb/clj-form",value.wire_with(allocation)?),
            Self::Time(value) => value.wire()?,
            Self::Path(value) => value.wire(),
            Self::TableRef(value) => T::table_ref(value),
            Self::VectorType(value) => value.wire(allocation)?,
            Self::Link(entries) => tagged("link",T::Map(wire_entries(entries.iter().map(|(key,value)|(key,value)),entries.len(),allocation)?)),
            Self::Throwable(value) => value.wire(allocation)?,
            Self::Tagged(value) => value.wire(allocation)?,
            Self::Reg(value) => tagged(value.kind.tag(),T::Integer(i64::from(value.oid))),
        })
    }
}

fn wire_values<'a>(values: impl Iterator<Item=&'a Object>,count: usize,allocation: Admission<'_>) -> Result<Vec<T>> {
    let mut output = Vec::with_capacity(count);
    for value in values { output.push(value.wire_with(allocation)?); }Ok(output)
}
fn wire_entries<'a>(values: impl Iterator<Item=(&'a Object,&'a Object)>,count: usize,allocation: Admission<'_>) -> Result<Vec<(T,T)>> {
    let mut output = Vec::with_capacity(count);
    for (key,value) in values { output.push((key.wire_with(allocation)?,value.wire_with(allocation)?)); }Ok(output)
}
fn map_wire(map: &Map,allocation: Admission<'_>) -> Result<T> { Ok(T::Map(wire_entries(map.entries(),map.len(),allocation)?)) }
fn keyword_value<'a>(object: &'a Object,name: &str) -> &'a Object { object.entries().and_then(|mut entries|entries.find(|(key,_)|matches!(key,Object::Keyword(key) if key == name))).map_or(&Object::Null,|(_,value)|value) }
fn uuid(value: &Object) -> Result<[i64;2]> {
    if let Object::String(value) = value {
        if value.encode_utf16().count() > 36 { return Err(invalid()); }
        let parts = value.split('-').collect::<Vec<_>>();if parts.len() != 5 { return Err(invalid()); }
        let mut values = [0u64;5];for (slot,text) in values.iter_mut().zip(parts) { *slot = java_hex(text)? as u64; }
        Ok([(((values[0] & 0xffff_ffff) << 32) | ((values[1] & 0xffff) << 16) | (values[2] & 0xffff)) as i64,(((values[3] & 0xffff) << 48) | (values[4] & 0xffff_ffff_ffff)) as i64])
    } else {
        let values = value.list().ok_or_else(invalid)?;
        let (Some(Object::Number(left)),Some(Object::Number(right))) = (values.first(),values.get(1)) else { return Err(invalid()); };
        let (Number::Long(left),Number::Long(right)) = (left.as_ref(),right.as_ref()) else { return Err(invalid()); };Ok([*left,*right])
    }
}
fn java_hex(text: &str) -> Result<i64> {
    let digits = text.strip_prefix('+').unwrap_or(text);if digits.is_empty() { return Err(invalid()); }
    digits.chars().try_fold(0i64,|value,digit|value.checked_mul(16).and_then(|value|value.checked_add(i64::from(rtdb_types::hexadecimal_digit(digit)?))).ok_or_else(invalid))
}
fn byte_buffer(text: &str) -> Result<Vec<u8>> {
    let units = text.encode_utf16().collect::<Vec<_>>();if units.len() < 2 || units.len() % 2 != 0 { return Err(invalid()); }
    units[2..].chunks_exact(2).map(|pair| {
        let digit = |value|char::from_u32(u32::from(value)).and_then(rtdb_types::hexadecimal_digit).ok_or_else(invalid);
        Ok(digit(pair[0])? * 16 + digit(pair[1])?)
    }).collect()
}

#[cfg(test)]
mod reg_tests {
    use super::*;
    use rtdb_format::transit::encode_msgpack;
    use rtdb_types::Value;

    #[test]
    fn pg_oid_tags_read_as_int_checked_values_equal_to_emitted_oids() {
        let allocation = EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(1 << 20)).unwrap();
        let read = |tag: &str,representation: T| Decoded::read(&encode_msgpack(&tagged(tag,representation)).unwrap(),&allocation);
        for (tag,emitted,printed) in [("xt/oid",Value::Oid(12345),"Oid(oid=12345)"),("xt/regclass",Value::RegClass(12345),"RegClass(oid=12345)"),("xt/regproc",Value::RegProc(12345),"RegProc(oid=12345)")] {
            let value = read(tag,T::Integer(12345)).unwrap();
            assert!(value.equivalent_emitted(&emitted).unwrap());
            assert!(!value.equivalent_emitted(&Value::I32(12345)).unwrap());
            assert!(value.equivalent(&read(tag,T::Integer(12345)).unwrap()).unwrap());
            assert!(!value.equivalent(&read(tag,T::Integer(1)).unwrap()).unwrap());
            assert_eq!(value.value.hasheq().unwrap(),12345);
            assert_eq!(render::string(&value.value,Admission::Standalone).unwrap(),printed);
            assert_eq!(value.encode().unwrap(),encode_msgpack(&tagged(tag,T::Integer(12345))).unwrap());
            assert!(read(tag,T::Integer(i64::from(i32::MAX) + 1)).is_err());
            assert!(read(tag,T::String("1".into())).is_err());
        }
        assert!(!read("xt/oid",T::Integer(1)).unwrap().equivalent(&read("xt/regclass",T::Integer(1)).unwrap()).unwrap());
        assert!(read("xt/regclass",T::Float(1.9)).unwrap().equivalent_emitted(&Value::RegClass(1)).unwrap());
    }
}
