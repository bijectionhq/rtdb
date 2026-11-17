use rtdb_types::{Anomaly,Result};
use super::{Map,Object,Throwable,Time,hash,invalid,keyword_value,number};
use super::allocation::{Text,path_capacity};
use crate::EvaluationScope;
use crate::semantic_text::{self,Atom,Mode,SequenceKind,atom,quoted};

pub(super) fn string(value: &Object,allocation: &EvaluationScope) -> Result<String> {
    let mut text = Text::new(allocation);write_string(&mut text,value)?;Ok(text.finish())
}

fn write_string(text: &mut Text<'_>,value: &Object) -> Result<()> {
    match value {
        Object::Null => atom(text,Atom::Nil,Mode::Str),Object::Bool(value) => atom(text,Atom::Boolean(*value),Mode::Str),
        Object::Number(value) => text.temporary(number_capacity(value)?,||Ok(value.text())),
        Object::String(value) => atom(text,Atom::String(value),Mode::Str),Object::Symbol(value) => atom(text,Atom::Symbol(value),Mode::Str),
        Object::Uri(value) => text.push(value),Object::Keyword(value) => atom(text,Atom::Keyword(value),Mode::Str),
        Object::Character(value) => atom(text,Atom::Character(character(*value)),Mode::Str),
        Object::Bytes(value) => text.temporary(64,||Ok(format!("[B@{:x}",hash::identity(value) as u32))),
        Object::Vector(_) | Object::List(_) | Object::Set(_) | Object::Map(_) | Object::TxKey(_) => write_printed(text,value),
        Object::Uuid(value) => text.temporary(64,||Ok(uuid(value))),
        Object::ByteBuffer(value) => text.temporary(128,||Ok(format!("java.nio.HeapByteBuffer[pos=0 lim={} cap={}]",value.len(),value.len()))),
        Object::KeyFn(value) => {
            for ch in value.chars() { text.character(if ch == '-' { '_' } else { ch.to_ascii_uppercase() })?; }Ok(())
        }
        Object::ClojureForm(value) => { text.push("ClojureForm(form=")?;java_string(text,value)?;text.push(")") }
        Object::Time(value) => text.temporary(4096,||value.text()),
        Object::Path(value) => text.temporary(path_capacity(value)?,||Ok(value.text())),
        Object::TableRef(value) => { text.push("TableRef(schemaName=")?;text.push(&value.schema)?;text.push(", tableName=")?;text.push(&value.table)?;text.push(")") }
        Object::VectorType(value) => value.write_string(text),
        Object::Throwable(value) => match value.as_ref() {
            Throwable::Anomaly { message,data,.. } => {
                let Object::Keyword(category) = keyword_value(data,"cognitect.anomalies/category") else { return Err(invalid()); };
                let category = match category.rsplit('/').next().unwrap_or("") {
                    "incorrect" => "Incorrect","unsupported" => "Unsupported","not-found" => "NotFound","forbidden" => "Forbidden",
                    "conflict" => "Conflict","busy" => "Busy","unavailable" => "Unavailable","interrupted" => "Interrupted","fault" => "Fault",_ => return Err(invalid()),
                };
                text.push("xtdb.api.error.")?;text.push(category)?;
                if !matches!(message,Object::Null) { text.push(": ")?;write_string(text,message)?; }Ok(())
            }
            Throwable::ExceptionInfo { message,data } => { text.push("clojure.lang.ExceptionInfo: ")?;java_string(text,message)?;text.push(" ")?;write_printed(text,data) }
        },
        Object::Link(value) => text.temporary(128,||Ok(format!("com.cognitect.transit.impl.LinkImpl@{:x}",hash::identity(value) as u32))),
        Object::Tagged(value) => text.temporary(128,||Ok(format!("com.cognitect.transit.impl.TaggedValueImpl@{:x}",value.hash_code()? as u32))),
    }
}

fn java_string(text: &mut Text<'_>,value: &Object) -> Result<()> { if matches!(value,Object::Null) { text.push("null") } else { write_string(text,value) } }

#[cfg(test)]
pub(super) fn printed(value: &Object,allocation: &EvaluationScope) -> Result<String> {
    let mut text = Text::new(allocation);write_printed(&mut text,value)?;Ok(text.finish())
}

pub(super) fn write_printed(text: &mut Text<'_>,value: &Object) -> Result<()> {
    match value {
        Object::Null => atom(text,Atom::Nil,Mode::Readable),Object::Bool(value) => atom(text,Atom::Boolean(*value),Mode::Readable),
        Object::Number(value) => text.temporary(number_capacity(value)?,||Ok(value.printed())),
        Object::String(value) => atom(text,Atom::String(value),Mode::Readable),Object::Symbol(value) => atom(text,Atom::Symbol(value),Mode::Readable),
        Object::Keyword(value) => atom(text,Atom::Keyword(value),Mode::Readable),Object::Character(value) => atom(text,Atom::Character(character(*value)),Mode::Readable),
        Object::Bytes(value) => {
            text.push("#bytes \"")?;const HEX: &[u8] = b"0123456789abcdef";
            for byte in value.iter() { text.character(HEX[(byte >> 4) as usize] as char)?;text.character(HEX[(byte & 15) as usize] as char)?; }text.push("\"")
        }
        Object::Vector(values) => sequence(text,SequenceKind::Vector,values.iter()),Object::List(values) => sequence(text,SequenceKind::List,values.iter()),
        Object::Set(values) => sequence(text,SequenceKind::Set,values.entries().map(|(key,_)|key)),
        Object::Map(values) => map(text,values),
        Object::TxKey(value) => { text.push("#xt/tx-key ")?;map(text,&Map::from_entries(value.entries().map(|(key,value)|(key.clone(),value.clone())))?) }
        Object::Uri(value) => {
            let uri = rtdb_types::UriParts::parse(value)?;text.scope().reserve(uri.ascii_allocation_bound()?)?;
            text.push("#xt/uri ")?;quoted(text,&uri.to_ascii_string())
        }
        Object::Uuid(value) => { text.scope().reserve(64)?;text.push("#uuid ")?;quoted(text,&uuid(value)) }
        Object::ByteBuffer(bytes) => default_object(text,"java.nio.HeapByteBuffer",hash::identity(bytes),value),
        Object::KeyFn(value) => { text.push("#xt/key-fn :")?;text.push(value) }
        Object::ClojureForm(value) => { text.push("#xt/clj-form ")?;write_printed(text,value) }
        Object::Time(value) => time(text,value),
        Object::Path(value) => { text.scope().reserve(path_capacity(value)?)?;text.push("#xt/path ")?;quoted(text,&value.text()) }
        Object::TableRef(value) => {
            text.push("#xt/table ")?;if value.schema != "public" { text.push(&value.schema)?;text.push("/")?; }text.push(&value.table)
        }
        Object::VectorType(value) => value.write_printed(text),
        Object::Throwable(value) => match value.as_ref() {
            Throwable::Anomaly { message,data,.. } => {
                let Object::Keyword(category) = keyword_value(data,"cognitect.anomalies/category") else { return Err(invalid()); };
                let category = Object::Keyword(category.rsplit('/').next().ok_or_else(invalid)?.into());
                let code = keyword_value(data,"xtdb.error/code").clone();
                let mut data = data.clone();data.strip_error_metadata()?;
                text.push(" #xt/error ")?;sequence(text,SequenceKind::Vector,[category,code,message.clone(),data].iter())
            }
            Throwable::ExceptionInfo { .. } => Err(Anomaly::unsupported("xtdb.expression/transit-throwable-text", "Readable exception text requires its construction stack trace")),
        },
        Object::Link(link) => default_object(text,"com.cognitect.transit.impl.LinkImpl",hash::identity(link),value),
        Object::Tagged(tagged) => default_object(text,"com.cognitect.transit.impl.TaggedValueImpl",hash::identity(tagged),value),
    }
}

fn time(text: &mut Text<'_>,value: &Time) -> Result<()> {
    let tag = match value {
        Time::Period(_) => "period",Time::Duration(_) => "duration",Time::Date(_) => "date",Time::LocalTime(_) => "time",
        Time::LocalDateTime(_) => "ldt",Time::OffsetTime { .. } => "offset-time",Time::OffsetDateTime { .. } => "odt",
        Time::Instant(_) => "instant",Time::ZonedDateTime(_) => "zdt",Time::Zone(_) => "zone",Time::Year(_) => "year",
        Time::YearMonth { .. } => "year-month",Time::MonthDay { .. } => "month-day",Time::DayOfWeek(_) => "day-of-week",Time::Month(_) => "month",
        Time::Interval { .. } => "interval",
        Time::Range { from,to } => {
            let values = [from,to].map(|value|value.map_or(Object::Null,|value|Object::Time(Time::ZonedDateTime(value))));
            text.push("#xt/tstz-range ")?;return sequence(text,SequenceKind::Vector,values.iter());
        }
        Time::LegacyDate(_) => return Err(Anomaly::unsupported("xtdb.expression/transit-date-text", "Readable Date text requires its legacy calendar formatter")),
    };
    text.scope().reserve(4096)?;text.push("#xt/")?;text.push(tag)?;text.push(" ")?;quoted(text,&value.text()?)
}

fn sequence<'a>(text: &mut Text<'_>,kind: SequenceKind,values: impl IntoIterator<Item=&'a Object>) -> Result<()> {
    semantic_text::sequence(text,kind,values,write_printed)
}

fn map(text: &mut Text<'_>,value: &Map) -> Result<()> { semantic_text::map(text,value.entries(),write_printed) }

fn default_object(text: &mut Text<'_>,class: &str,identity: i32,value: &Object) -> Result<()> {
    text.push("#object[")?;text.push(class)?;text.temporary(32,||Ok(format!(" 0x{:x} ",identity as u32)))?;
    let rendered = string(value,text.scope())?;quoted(text,&rendered)?;text.push("]")
}
fn character(value: u16) -> char { char::from_u32(u32::from(value)).unwrap_or('?') }
fn number_capacity(value: &number::Number) -> Result<usize> { value.text_capacity().map_err(number::error)?.checked_mul(8).ok_or_else(crate::numeric::overflow) }
fn uuid(value: &[i64;2]) -> String { uuid::Uuid::from_u128((u128::from(value[0] as u64) << 64) | u128::from(value[1] as u64)).to_string() }
#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::transit::{TransitValue as T,encode_msgpack};
    use super::super::tagged;

    #[test]
    fn byte_buffer_aliases_keep_default_print_identity_and_content_writing() {
        let allocation = crate::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(1 << 20)).unwrap();
        let decoded = super::super::Decoded::read(&encode_msgpack(&tagged("xtdb/byte-array",T::String("0x0102".into()))).unwrap(),&allocation).unwrap();
        let value = &decoded.value;
        let alias = value.clone();
        let (Object::ByteBuffer(left),Object::ByteBuffer(right)) = (value,&alias) else { panic!("byte buffers") };
        assert!(std::sync::Arc::ptr_eq(left,right));
        assert_eq!(printed(value,&allocation).unwrap(),printed(&alias,&allocation).unwrap());
        let text = printed(value,&allocation).unwrap();
        assert!(text.starts_with("#object[java.nio.HeapByteBuffer 0x"));
        assert!(text.ends_with(" \"java.nio.HeapByteBuffer[pos=0 lim=2 cap=2]\"]"));
        assert_eq!(value.wire(&allocation).unwrap(),alias.wire(&allocation).unwrap());
    }
}
