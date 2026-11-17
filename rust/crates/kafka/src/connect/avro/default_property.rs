use rtdb_types::Result;
use super::{default_json::DefaultJson, default_input::JsonNode, error};

#[derive(Clone,Copy,Debug)]
pub(super) struct PropertyNode<'a> { raw:&'a DefaultJson }
#[derive(Clone,Copy,Debug)]
pub(super) struct PropertyObject<'a> { raw:&'a DefaultJson }

pub(super) fn object(raw:Option<&DefaultJson>)->Result<Option<PropertyObject<'_>>> {
    let Some(raw)=raw else {return Ok(None);};
    object_scan(raw,0)?;
    if java_null(raw) {return Ok(None);}
    Ok(Some(PropertyObject{raw}))
}
pub(super) fn node(raw:Option<&DefaultJson>)->Result<Option<PropertyNode<'_>>> {
    object(raw)?.map(PropertyObject::node).transpose()
}
fn java_null(raw:&DefaultJson)->bool {
    match raw {
        DefaultJson::Number(value) if !value.contains(['.','e','E'])=>value.parse::<i64>().is_err(),
        _=>false,
    }
}
fn depth_guard(depth:usize)->Result<()> {
    if depth>128 {Err(error("batch-too-large","Default property nesting limit exceeded"))}else{Ok(())}
}
fn object_scan(raw:&DefaultJson,depth:usize)->Result<()> {
    depth_guard(depth)?;
    match raw {
        DefaultJson::Decimal(_)=>return Err(error("unsupported-avro-default","Schema properties require the Double-mode schema parser rather than a registry Decimal carrier")),
        DefaultJson::Array(values)=>for value in values {object_scan(value,depth+1)?;},
        DefaultJson::Object(values)=>for (_,value) in values {object_scan(value,depth+1)?;},
        _=>(),
    }
    Ok(())
}
fn encoding_scan(raw:&DefaultJson,depth:usize)->Result<()> {
    depth_guard(depth)?;
    if java_null(raw) {return Err(error("avro-default-unchecked","Java null in a default property cannot be encoded by JacksonUtils.toJson"));}
    match raw {
        DefaultJson::Array(values)=>for value in values {encoding_scan(value,depth+1)?;},
        DefaultJson::Object(values)=>for (_,value) in values {encoding_scan(value,depth+1)?;},
        _=>(),
    }
    Ok(())
}
impl<'a> PropertyNode<'a> {
    pub(super) fn raw(self)->&'a DefaultJson {self.raw}
    pub(super) fn at(self,schema:&'a apache_avro::Schema)->JsonNode<'a> {JsonNode::new(Some(self.raw),schema)}
}
impl<'a> PropertyObject<'a> {
    pub(super) fn node(self)->Result<PropertyNode<'a>> {
        encoding_scan(self.raw,0)?;
        Ok(PropertyNode{raw:self.raw})
    }
}

#[cfg(test)] mod tests;
