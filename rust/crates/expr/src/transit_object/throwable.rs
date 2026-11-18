use std::sync::Arc;
use rtdb_format::transit::TransitValue as T;
use rtdb_types::Result;
use super::{Map,Object,invalid,keyword_value,tagged};

#[derive(Clone,Debug)]
pub(crate) enum Throwable {
    Anomaly { message: Object,data: Object,cause: Option<Arc<Self>> },
    ExceptionInfo { message: Object,data: Object },
}

impl Throwable {
    pub(super) fn read(tag: &str,representation: &Object) -> Result<Self> {
        let values = representation.sequence().ok_or_else(invalid)?;
        let at = |index|values.get(index).unwrap_or(&Object::Null);
        if tag == "xtdb/exception-info" {
            if !matches!(at(0),Object::Null | Object::String(_)) { return Err(invalid()); }
            let mut data = at(1).clone();if matches!(data,Object::Null) { data = Object::Map(Map::new()); }data.entries().ok_or_else(invalid)?;
            return Ok(Self::ExceptionInfo { message: at(0).clone(),data });
        }
        let modern = tag == "xt/error";let offset = usize::from(modern);
        let category = if modern {
            let Object::Keyword(category) = at(0) else { return Err(invalid()); };
            if !["incorrect","unsupported","not-found","forbidden","conflict","busy","unavailable","interrupted","fault"].contains(&category.as_str()) { return Err(invalid()); }category.as_str()
        } else { "incorrect" };
        let code = at(offset);let message = at(offset + 1);if !matches!(message,Object::Null | Object::String(_)) { return Err(invalid()); }
        let mut data = at(offset + 2).clone();if matches!(data,Object::Null) || !modern && !data.truthy() { data = Object::Map(Map::new()); }
        data.entries().ok_or_else(invalid)?;
        if !modern && at(3).truthy() { data.assoc_keyword("xtdb.error/cause",at(3).clone())?; }
        data.assoc_keyword("cognitect.anomalies/category",Object::Keyword(format!("cognitect.anomalies/{category}")))?;
        if code.truthy() { data.assoc_keyword("xtdb.error/code",code.clone())?; }
        let stored_cause = data.take_error_cause()?.unwrap_or(Object::Null);
        let cause = match stored_cause { Object::Null => None,Object::Throwable(cause) => Some(cause),_ => return Err(invalid()) };
        Ok(Self::Anomaly { message: message.clone(),data,cause })
    }
    pub(super) fn data(&self) -> &Object { match self { Self::Anomaly { data,.. } | Self::ExceptionInfo { data,.. } => data } }
    pub(super) fn wire(&self,allocation: super::Admission<'_>) -> Result<T> {
        Ok(match self {
            Self::ExceptionInfo { message,data } => tagged("xtdb/exception-info",T::Array(vec![message.wire_with(allocation)?,data.wire_with(allocation)?])),
            Self::Anomaly { message,data,.. } => {
                let Object::Keyword(category) = keyword_value(data,"cognitect.anomalies/category") else { return Err(invalid()); };
                let category = category.strip_prefix("cognitect.anomalies/").ok_or_else(invalid)?;
                let code = keyword_value(data,"xtdb.error/code").wire_with(allocation)?;
                let mut data = data.clone();data.strip_error_metadata()?;
                tagged("xt/error",T::Array(vec![T::Keyword(category.into()),code,message.wire_with(allocation)?,data.wire_with(allocation)?]))
            }
        })
    }
    pub(super) fn equal(left: &Arc<Self>,right: &Arc<Self>) -> Result<bool> {
        if Arc::ptr_eq(left,right) { return Ok(true); }
        let (Self::Anomaly { message: lm,data: ld,cause: lc },Self::Anomaly { message: rm,data: rd,cause: rc }) = (left.as_ref(),right.as_ref()) else { return Ok(false); };
        if !lm.java_equal(rm)? || !ld.equivalent(rd)? { return Ok(false); }
        match (lc,rc) { (None,None) => Ok(true),(Some(left),Some(right)) => Self::equal(left,right),_ => Ok(false) }
    }
    pub(super) fn hash(value: &Arc<Self>) -> Result<i32> {
        match value.as_ref() {
            Self::ExceptionInfo { .. } => Ok(super::hash::identity(value)),
            Self::Anomaly { message,data,cause } => Ok(1i32.wrapping_mul(31).wrapping_add(message.java_hash_code()?).wrapping_mul(31).wrapping_add(data.java_hash_code()?).wrapping_mul(31).wrapping_add(cause.as_ref().map(Self::hash).transpose()?.unwrap_or(0))),
        }
    }
}
