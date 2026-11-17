use rtdb_types::{Anomaly,Result};
use super::{Map,Object,Throwable,Time,hash,invalid,keyword_value};

pub(super) fn string(value: &Object) -> Result<String> {
    Ok(match value {
        Object::Null => String::new(),Object::Bool(value) => value.to_string(),Object::Number(value) => value.text(),
        Object::String(value) | Object::Symbol(value) | Object::Uri(value) => value.clone(),
        Object::Keyword(value) => format!(":{value}"),Object::Character(value) => character(*value).to_string(),
        Object::Bytes(value) => format!("[B@{:x}",hash::identity(value) as u32),
        Object::Vector(_) | Object::List(_) | Object::Set(_) | Object::Map(_) | Object::TxKey(_) => printed(value)?,
        Object::Uuid(value) => uuid(value),
        Object::ByteBuffer(value) => format!("java.nio.HeapByteBuffer[pos=0 lim={} cap={}]",value.len(),value.len()),
        Object::KeyFn(value) => value.replace('-',"_").to_ascii_uppercase(),
        Object::ClojureForm(value) => format!("ClojureForm(form={})",java_string(value)?),
        Object::Time(value) => value.text()?,Object::Path(value) => value.text(),
        Object::TableRef(value) => format!("TableRef(schemaName={}, tableName={})",value.schema,value.table),
        Object::Throwable(value) => match value.as_ref() {
            Throwable::Anomaly { message,data,.. } => {
                let Object::Keyword(category) = keyword_value(data,"cognitect.anomalies/category") else { return Err(invalid()); };
                let category = match category.rsplit('/').next().unwrap_or("") {
                    "incorrect" => "Incorrect","unsupported" => "Unsupported","not-found" => "NotFound","forbidden" => "Forbidden",
                    "conflict" => "Conflict","busy" => "Busy","unavailable" => "Unavailable","interrupted" => "Interrupted","fault" => "Fault",_ => return Err(invalid()),
                };
                let name = format!("xtdb.api.error.{category}");
                if matches!(message,Object::Null) { name } else { format!("{name}: {}",string(message)?) }
            }
            Throwable::ExceptionInfo { message,data } => format!("clojure.lang.ExceptionInfo: {} {}",java_string(message)?,printed(data)?),
        },
        Object::Link(value) => format!("com.cognitect.transit.impl.LinkImpl@{:x}",hash::identity(value) as u32),
        Object::Tagged(value) => format!("com.cognitect.transit.impl.TaggedValueImpl@{:x}",value.hash_code()? as u32),
    })
}

fn java_string(value: &Object) -> Result<String> { if matches!(value,Object::Null) { Ok("null".into()) } else { string(value) } }

pub(super) fn printed(value: &Object) -> Result<String> {
    Ok(match value {
        Object::Null => "nil".into(),Object::Bool(value) => value.to_string(),Object::Number(value) => value.printed(),
        Object::String(value) => quoted(value),Object::Symbol(value) => value.clone(),Object::Keyword(value) => format!(":{value}"),
        Object::Character(value) => {
            let name = match *value { 10 => "newline",9 => "tab",32 => "space",8 => "backspace",12 => "formfeed",13 => "return",_ => "" };
            if name.is_empty() { format!("\\{}",character(*value)) } else { format!("\\{name}") }
        }
        Object::Bytes(value) => format!("#bytes {}",quoted(&hex(value))),
        Object::Vector(values) => sequence("[",values.iter(),"]")?,Object::List(values) => sequence("(",values.iter(),")")?,
        Object::Set(values) => sequence("#{",values.entries().map(|(key,_)|key),"}")?,
        Object::Map(values) => map(values)?,
        Object::TxKey(value) => format!("#xt/tx-key {}",map(&Map::from_entries(value.entries().map(|(key,value)|(key.clone(),value.clone())))?)?),
        Object::Uri(value) => format!("#xt/uri {}",quoted(&rtdb_types::UriParts::parse(value)?.to_ascii_string())),
        Object::Uuid(value) => format!("#uuid {}",quoted(&uuid(value))),
        Object::ByteBuffer(bytes) => default_object("java.nio.HeapByteBuffer",hash::identity(bytes),&string(value)?),
        Object::KeyFn(value) => format!("#xt/key-fn :{value}"),
        Object::ClojureForm(value) => format!("#xt/clj-form {}",printed(value)?),
        Object::Time(value) => time(value)?,Object::Path(value) => format!("#xt/path {}",quoted(&value.text())),
        Object::TableRef(value) => if value.schema == "public" { format!("#xt/table {}",value.table) } else { format!("#xt/table {}/{}",value.schema,value.table) },
        Object::Throwable(value) => match value.as_ref() {
            Throwable::Anomaly { message,data,.. } => {
                let Object::Keyword(category) = keyword_value(data,"cognitect.anomalies/category") else { return Err(invalid()); };
                let category = Object::Keyword(category.rsplit('/').next().ok_or_else(invalid)?.into());
                let code = keyword_value(data,"xtdb.error/code").clone();
                let mut data = data.clone();data.strip_error_metadata()?;
                format!(" #xt/error {}",sequence("[",[category,code,message.clone(),data].iter(),"]")?)
            }
            Throwable::ExceptionInfo { .. } => return Err(Anomaly::unsupported("xtdb.expression/transit-throwable-text", "Readable exception text requires its construction stack trace")),
        },
        Object::Link(link) => default_object("com.cognitect.transit.impl.LinkImpl",hash::identity(link),&string(value)?),
        Object::Tagged(tagged) => default_object("com.cognitect.transit.impl.TaggedValueImpl",hash::identity(tagged),&string(value)?),
    })
}

fn time(value: &Time) -> Result<String> {
    let tag = match value {
        Time::Period(_) => "period",Time::Duration(_) => "duration",Time::Date(_) => "date",Time::LocalTime(_) => "time",
        Time::LocalDateTime(_) => "ldt",Time::OffsetTime { .. } => "offset-time",Time::OffsetDateTime { .. } => "odt",
        Time::Instant(_) => "instant",Time::ZonedDateTime(_) => "zdt",Time::Zone(_) => "zone",Time::Year(_) => "year",
        Time::YearMonth { .. } => "year-month",Time::MonthDay { .. } => "month-day",Time::DayOfWeek(_) => "day-of-week",Time::Month(_) => "month",
        Time::Interval { .. } => "interval",
        Time::Range { from,to } => {
            let values = [from,to].map(|value|value.map_or(Object::Null,|value|Object::Time(Time::ZonedDateTime(value))));
            return Ok(format!("#xt/tstz-range {}",sequence("[",values.iter(),"]")?));
        }
        Time::LegacyDate(_) => return Err(Anomaly::unsupported("xtdb.expression/transit-date-text", "Readable Date text requires its legacy calendar formatter")),
    };
    Ok(format!("#xt/{tag} {}",quoted(&value.text()?)))
}

fn sequence<'a>(start: &str,values: impl IntoIterator<Item=&'a Object>,end: &str) -> Result<String> {
    let mut text = String::from(start);let mut first = true;
    for value in values { if !first { text.push(' '); }first = false;text.push_str(&printed(value)?); }
    text.push_str(end);Ok(text)
}

fn map(value: &Map) -> Result<String> {
    let mut text = String::from("{");
    for (index,(key,value)) in value.entries().enumerate() {
        if index != 0 { text.push_str(", "); }
        text.push_str(&printed(key)?);text.push(' ');text.push_str(&printed(value)?);
    }
    text.push('}');Ok(text)
}

fn default_object(class: &str,identity: i32,text: &str) -> String { format!("#object[{class} 0x{:x} {}]",identity as u32,quoted(text)) }
fn character(value: u16) -> char { char::from_u32(u32::from(value)).unwrap_or('?') }
fn quoted(value: &str) -> String {
    let mut text = String::from("\"");
    for ch in value.chars() { match ch { '\n' => text.push_str("\\n"),'\t' => text.push_str("\\t"),'\r' => text.push_str("\\r"),'"' => text.push_str("\\\""),'\\' => text.push_str("\\\\"),'\u{c}' => text.push_str("\\f"),'\u{8}' => text.push_str("\\b"),_ => text.push(ch) } }
    text.push('"');text
}
fn hex(value: &[u8]) -> String { value.iter().map(|value|format!("{value:02x}")).collect() }
fn uuid(value: &[i64;2]) -> String { uuid::Uuid::from_u128((u128::from(value[0] as u64) << 64) | u128::from(value[1] as u64)).to_string() }

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::transit::{TransitValue as T,encode_msgpack};
    use super::super::tagged;

    #[test]
    fn byte_buffer_aliases_keep_default_print_identity_and_content_writing() {
        let value = Object::decode(&encode_msgpack(&tagged("xtdb/byte-array",T::String("0x0102".into()))).unwrap()).unwrap();
        let alias = value.clone();
        let (Object::ByteBuffer(left),Object::ByteBuffer(right)) = (&value,&alias) else { panic!("byte buffers") };
        assert!(std::sync::Arc::ptr_eq(left,right));
        assert_eq!(printed(&value).unwrap(),printed(&alias).unwrap());
        let text = printed(&value).unwrap();
        assert!(text.starts_with("#object[java.nio.HeapByteBuffer 0x"));
        assert!(text.ends_with(" \"java.nio.HeapByteBuffer[pos=0 lim=2 cap=2]\"]"));
        assert_eq!(value.wire().unwrap(),alias.wire().unwrap());
    }
}
