use std::sync::Arc;
use rtdb_format::transit::{TransitReadBuilder,TransitValue as T,decode_with};
use rtdb_types::{Anomaly,Result,TableRef};
mod number;
mod default_handlers;
mod collection;
mod equal;
mod hash;
mod time;
mod path;
mod throwable;
mod tagged_value;
mod render;
mod table;
use collection::{Array,Map,Record,Entries};
use number::Number;
use time::Time;
use path::Path;
use throwable::Throwable;
use tagged_value::Tagged;

#[derive(Clone,Debug)]
pub(crate) enum Object {
    Null,Bool(bool),Number(Arc<Number>),String(String),Keyword(String),Symbol(String),Character(u16),
    Bytes(Arc<Vec<u8>>),Vector(Vec<Self>),List(Vec<Self>),Set(Map),Map(Map),
    Uri(String),Uuid([i64;2]),ByteBuffer(Arc<Vec<u8>>),KeyFn(String),ClojureForm(Box<Self>),Time(Time),Path(Path),TableRef(TableRef),
    TxKey(Record),Throwable(Arc<Throwable>),Link(Arc<Vec<(Self,Self)>>),Tagged(Arc<Tagged>),
}

fn invalid() -> Anomaly { Anomaly::incorrect("xtdb.expression/invalid-transit-object", "Invalid stored Transit object representation") }
fn tagged(tag: &str,representation: T) -> T { T::Tagged { tag: tag.into(),representation: Box::new(representation) } }

struct ObjectReadBuilder;
impl TransitReadBuilder for ObjectReadBuilder {
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
    fn tagged(&mut self,tag: String,representation: Object) -> Result<Object> { Object::registered(tag,representation) }
}

impl Object {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> { decode_with(bytes,&mut ObjectReadBuilder) }
    fn number(value: Number) -> Self { Self::Number(Arc::new(value)) }
    fn registered(tag: String,representation: Self) -> Result<Self> {
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
            "xt/table" => Self::TableRef(table::read(representation)?),
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
    pub(crate) fn field(self,name: Option<&str>) -> Result<Option<Self>> {
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
    pub(crate) fn wire(&self) -> Result<T> {
        Ok(match self {
            Self::Null => T::Null,Self::Bool(value) => T::Bool(*value),Self::Number(value) => number::wire(value),Self::String(value) => T::String(value.clone()),Self::Keyword(value) => T::Keyword(value.clone()),Self::Symbol(value) => T::Symbol(value.clone()),
            Self::Character(value) => tagged("c",T::String(char::from_u32(u32::from(*value)).unwrap_or('?').to_string())),
            Self::Bytes(value) => T::Bytes(value.as_ref().clone()),
            Self::Vector(values) => T::Array(values.iter().map(Self::wire).collect::<Result<_>>()?),
            Self::List(values) => tagged("list",T::Array(values.iter().map(Self::wire).collect::<Result<_>>()?)),
            Self::Set(values) => tagged("set",T::Array(values.entries().map(|(key,_)|key.wire()).collect::<Result<_>>()?)),
            Self::Map(map) => map_wire(map)?,
            Self::TxKey(record) => tagged("xtdb/tx-key",map_wire(&Map::from_entries(record.entries().map(|(key,value)|(key.clone(),value.clone())))?)?),
            Self::Uri(value) => tagged("r",T::String(value.clone())),Self::Uuid(values) => tagged("u",T::Array(values.iter().map(|value|T::Integer(*value)).collect())),
            Self::ByteBuffer(value) => { let mut text = String::from("0x");const HEX: &[u8] = b"0123456789abcdef";for byte in value.iter() { text.push(HEX[(byte >> 4) as usize] as char);text.push(HEX[(byte & 15) as usize] as char); }tagged("xtdb/byte-array",T::String(text)) }
            Self::KeyFn(value) => tagged("xtdb/key-fn",T::Keyword(value.clone())),Self::ClojureForm(value) => tagged("xtdb/clj-form",value.wire()?),
            Self::Time(value) => value.wire()?,
            Self::Path(value) => value.wire(),
            Self::TableRef(value) => table::wire(value),
            Self::Link(entries) => tagged("link",T::Map(entries.iter().map(|(key,value)|Ok((key.wire()?,value.wire()?))).collect::<Result<_>>()?)),
            Self::Throwable(value) => value.wire()?,
            Self::Tagged(value) => value.wire()?,
        })
    }
}

fn map_wire(map: &Map) -> Result<T> { Ok(T::Map(map.entries().map(|(key,value)|Ok((key.wire()?,value.wire()?))).collect::<Result<_>>()?)) }
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
fn hex_digit(value: char) -> Option<u8> { rtdb_types::decimal_digit(value).or_else(||match value { 'a'..='f' => Some(value as u8 - b'a' + 10),'A'..='F' => Some(value as u8 - b'A' + 10),'\u{ff41}'..='\u{ff46}' => Some((value as u32 - 0xff41 + 10) as u8),'\u{ff21}'..='\u{ff26}' => Some((value as u32 - 0xff21 + 10) as u8),_ => None }) }
fn java_hex(text: &str) -> Result<i64> {
    let digits = text.strip_prefix('+').unwrap_or(text);if digits.is_empty() { return Err(invalid()); }
    digits.chars().try_fold(0i64,|value,digit|value.checked_mul(16).and_then(|value|value.checked_add(i64::from(hex_digit(digit)?))).ok_or_else(invalid))
}
fn byte_buffer(text: &str) -> Result<Vec<u8>> {
    let units = text.encode_utf16().collect::<Vec<_>>();if units.len() < 2 || units.len() % 2 != 0 { return Err(invalid()); }
    units[2..].chunks_exact(2).map(|pair| {
        let digit = |value|char::from_u32(u32::from(value)).and_then(hex_digit).ok_or_else(invalid);
        Ok(digit(pair[0])? * 16 + digit(pair[1])?)
    }).collect()
}
