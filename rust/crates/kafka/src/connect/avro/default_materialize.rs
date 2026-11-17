use std::{collections::BTreeMap, mem::size_of};
use apache_avro::Schema;
use rtdb_types::{Result, Value};
use super::{default_input::{DefaultAttempt, JsonNode, ObjectKind}, default_json::DefaultJson, default_scalar::{self, ScalarInput, ConversionPath}, error};
use super::super::schema_graph::{SchemaView, ViewKind, Scalar};

#[derive(Debug)]
struct Converted { value:Option<Value>, validation:DefaultAttempt<()> }
struct Walk<'a, A, const BUILD: bool> { admit: &'a mut A, names: &'a BTreeMap<String, Schema>, demand: usize, remaining: usize, scrub: bool }

macro_rules! attempt {
    ($expression:expr) => { match $expression? { DefaultAttempt::Value(value) => value,DefaultAttempt::Invalid(cause) => return Ok(DefaultAttempt::Invalid(cause)) } };
}
fn invalid<T>(message: &'static str) -> DefaultAttempt<T> { DefaultAttempt::Invalid(error("avro-schema",message)) }
fn unsupported(message: &'static str) -> rtdb_types::Anomaly { error("unsupported-avro-default",message) }
fn add(left: usize,right: usize)->Result<usize> { left.checked_add(right).ok_or_else(super::resolution::overflow) }
fn mul(left: usize,right: usize)->Result<usize> { left.checked_mul(right).ok_or_else(super::resolution::overflow) }
fn struct_slot(name: &str)->Result<usize> { add(add(mul(size_of::<(String,Value)>(),16)?,256)?,name.len()) }

/// The caller retains every admission lease while the returned value lives.
pub(super) fn convert(input: ScalarInput<'_>, destination: SchemaView<'_>, path: ConversionPath, logical_name: Option<&str>, names: &BTreeMap<String,Schema>, scrub: bool, ceiling: usize, admit: &mut impl FnMut(usize)->Result<()>) -> Result<DefaultAttempt<Value>> {
    let mut measure=Walk::<_,false> {admit:&mut *admit,names,demand:0,remaining:ceiling,scrub};
    let measured=attempt!(measure.value(input,destination,path,logical_name,0));
    if let DefaultAttempt::Invalid(cause)=measured.validation {return Ok(DefaultAttempt::Invalid(cause));}
    let demand=measure.demand;
    drop(measure);
    admit(demand)?;
    let mut build=Walk::<_,true> {admit,names,demand:0,remaining:usize::MAX,scrub};
    let built=attempt!(build.value(input,destination,path,logical_name,0));
    if let DefaultAttempt::Invalid(cause)=built.validation {return Ok(DefaultAttempt::Invalid(cause));}
    match built.value {Some(value)=>Ok(DefaultAttempt::Value(value)),None=>Err(unsupported("Default materialization returned no value"))}
}
impl<A:FnMut(usize)->Result<()>, const BUILD:bool> Walk<'_,A,BUILD> {
    fn claim(&mut self,bytes:usize)->Result<()> {
        self.demand=add(self.demand,bytes)?;
        if !BUILD && self.demand>self.remaining {return Err(error("batch-too-large","Avro default allocation exceeds its remaining limit"));}
        Ok(())
    }
    fn scratch(&mut self,bytes:usize)->Result<()> {
        if !BUILD {
            (self.admit)(bytes)?;
            self.remaining=self.remaining.checked_sub(bytes).ok_or_else(|| error("batch-too-large","Avro default scratch exceeds its remaining limit"))?;
        }
        self.claim(bytes)
    }
    fn scalar(&mut self,input:ScalarInput<'_>,destination:SchemaView<'_>,path:ConversionPath,logical_name:Option<&str>)->Result<DefaultAttempt<Converted>> {
        let prepared=attempt!(default_scalar::prepare(input,destination,path,logical_name));
        let null_at_entry=matches!(path,ConversionPath::MapChild) && match input {
            ScalarInput::Native(Value::Null)=>true,
            ScalarInput::JsonNode(node)=>node.raw().is_none(),
            ScalarInput::FieldObject(source)=>matches!(source.kind()?,ObjectKind::Null) && !source.raw().is_null(),
            _=>false,
        };
        if null_at_entry && !destination.optional() {return Ok(invalid("Null input targets a required schema"));}
        let mut validation=prepared.validate_destination(destination,logical_name);
        if (prepared.is_missing() || prepared.is_null()) && !destination.optional() {validation=invalid("Null default targets a required schema");}
        self.claim(prepared.bound()?)?;
        Ok(DefaultAttempt::Value(Converted {value:if BUILD {Some(prepared.materialize()?)} else {None},validation}))
    }
    fn value(&mut self,input:ScalarInput<'_>,destination:SchemaView<'_>,path:ConversionPath,logical_name:Option<&str>,depth:usize)->Result<DefaultAttempt<Converted>> {
        if depth>128 {return Err(error("batch-too-large","Default conversion nesting limit exceeded"));}
        if let ScalarInput::JsonNode(node)=input { if node.raw().is_none() {return self.scalar(input,destination,path,logical_name);} }
        let input=match input {
            ScalarInput::JsonNode(node)=>{
                let names=self.names;
                let schema=node_schema(node.schema(),names)?;
                let input=ScalarInput::JsonNode(JsonNode::new(node.raw(),schema));
                if matches!(schema,Schema::Null) {return self.scalar(input,destination,path,logical_name);}
                input
            }
            input=>input,
        };
        let input_null=match input {ScalarInput::FieldObject(value)=>matches!(value.kind()?,ObjectKind::Null),ScalarInput::Native(Value::Null)=>true,_=>false};
        if input_null || matches!(destination.kind(),ViewKind::Scalar(_)) {return self.scalar(input,destination,path,logical_name);}
        if matches!(destination.kind(),ViewKind::Struct(_)) && (destination.name()==Some("io.confluent.connect.avro.Union") || destination.parameter("org.apache.kafka.connect.data.Union").is_some()) {
            return Err(unsupported("Default container conversion requires a selected Connect union member"));
        }
        if let ScalarInput::JsonNode(node)=input {
            let aligned=matches!((destination.kind(),node.schema()),(ViewKind::Struct(_),Schema::Record(_)) | (ViewKind::Array(_),Schema::Array(_)) | (ViewKind::Map(_,_),Schema::Map(_)));
            if !aligned {return Err(unsupported("JsonNode container conversion requires the complete cross-kind adapter"));}
        }
        self.claim(size_of::<Value>())?;
        let mut validation=DefaultAttempt::Value(());
        let converted=match destination.kind() {
            ViewKind::Struct(fields)=>{
                let mut output=if BUILD {Some(BTreeMap::new())} else {None};
                match input {
                    ScalarInput::FieldObject(source)=>{
                        if !matches!(source.kind()?,ObjectKind::Object(_)) {return Ok(invalid("Record default is not a Map"));}
                        for field in fields {
                            self.claim(struct_slot(field.name)?)?;
                            let child=match source.field(field.name,self.scrub)? {
                                Some(value)=>ScalarInput::FieldObject(value),
                                None=>ScalarInput::Native(field.schema.default_value().unwrap_or(&Value::Null)),
                            };
                            let value=attempt!(self.value(child,field.schema,ConversionPath::MapChild,field.schema.name(),depth+1));
                            if let DefaultAttempt::Invalid(cause)=value.validation {return Ok(DefaultAttempt::Invalid(cause));}
                            if let Some(output)=&mut output {output.insert(field.name.into(),value.value.ok_or_else(|| unsupported("Default child materialization returned no value"))?);}
                        }
                    }
                    ScalarInput::JsonNode(source)=>{
                        let Schema::Record(record)=source.schema() else {return Ok(invalid("Record JsonNode default lacks its actual schema"));};
                        if source.raw().and_then(DefaultJson::as_object).is_none() {return Ok(invalid("Record JsonNode default is not an object"));}
                        for actual in &record.fields {
                            let field=destination.field(&actual.name).ok_or_else(|| unsupported("JsonNode record default destination field is absent"))?;
                            self.claim(struct_slot(field.name)?)?;
                            let child=ScalarInput::JsonNode(source.field(&actual.name,&actual.schema));
                            let value=attempt!(self.value(child,field.schema,ConversionPath::Wrapper,field.schema.name(),depth+1));
                            if let DefaultAttempt::Invalid(cause)=value.validation {return Ok(DefaultAttempt::Invalid(cause));}
                            if let Some(output)=&mut output {output.insert(actual.name.clone(),value.value.ok_or_else(|| unsupported("Default child materialization returned no value"))?);}
                        }
                    }
                    ScalarInput::Native(Value::Map{entries,..})=>{
                        for field in fields {
                            self.claim(struct_slot(field.name)?)?;
                            let child=entries.iter().find_map(|(key,value)| match key {Value::Utf8(key) if super::names::LookupName::matches(field.name,key,self.scrub)=>Some(value),_=>None}).or_else(|| field.schema.default_value()).unwrap_or(&Value::Null);
                            let value=attempt!(self.value(ScalarInput::Native(child),field.schema,ConversionPath::MapChild,field.schema.name(),depth+1));
                            if let DefaultAttempt::Invalid(cause)=value.validation {return Ok(DefaultAttempt::Invalid(cause));}
                            if let Some(output)=&mut output {output.insert(field.name.into(),value.value.ok_or_else(|| unsupported("Default child materialization returned no value"))?);}
                        }
                    }
                    _=>return Ok(invalid("Record default value is neither a Map nor an IndexedRecord")),
                }
                output.map(Value::Struct)
            }
            ViewKind::Array(element)=>{
                let count=match input {
                    ScalarInput::FieldObject(source)=>match source.kind()? {ObjectKind::Array(values)=>values.len(),_=>return Ok(invalid("Array default is not a Collection"))},
                    ScalarInput::JsonNode(source)=>match source.raw().and_then(DefaultJson::as_array) {Some(values)=>values.len(),None=>return Ok(invalid("Array JsonNode default is not an array"))},
                    ScalarInput::Native(Value::List(values))=>values.len(),
                    _=>return Ok(invalid("Array default is not a Collection")),
                };
                self.claim(mul(count,size_of::<Value>())?)?;
                let mut output=if BUILD {Some(Vec::with_capacity(count))} else {None};
                for index in 0..count {
                    let (child,child_path)=match input {
                        ScalarInput::FieldObject(source)=>(ScalarInput::FieldObject(source.element(index)?.ok_or_else(|| unsupported("Default array element is absent"))?),ConversionPath::MapChild),
                        ScalarInput::JsonNode(source)=>{
                            let Schema::Array(array)=source.schema() else {return Err(unsupported("JsonNode array default source schema is not an array"));};
                            (ScalarInput::JsonNode(JsonNode::new(source.raw().and_then(DefaultJson::as_array).and_then(|values| values.get(index)),&array.items)),ConversionPath::Wrapper)
                        }
                        ScalarInput::Native(Value::List(values))=>(ScalarInput::Native(&values[index]),ConversionPath::MapChild),
                        _=>unreachable!(),
                    };
                    let value=attempt!(self.value(child,element,child_path,element.name(),depth+1));
                    if matches!(validation,DefaultAttempt::Value(())) {validation=value.validation;}
                    if let Some(output)=&mut output {output.push(value.value.ok_or_else(|| unsupported("Default array child materialization returned no value"))?);}
                }
                output.map(Value::List)
            }
            ViewKind::Map(key,element)=>{
                if key.optional() || !matches!(key.kind(),ViewKind::Scalar(Scalar::String)) {return Err(unsupported("Default container conversion requires the complete arbitrary-key MapEntry adapter"));}
                let count=attempt!(map_len(input));
                self.scratch(add(mul(count,8192)?,4096)?)?;
                self.claim(mul(count,size_of::<Option<(Value,Value)>>())?)?;
                let mut keys=Vec::with_capacity(count);
                let mut validations=Vec::with_capacity(count);
                let mut converted=if BUILD {Some(Vec::with_capacity(count))} else {None};
                for index in 0..count {
                    let (key,child,child_path)=map_entry(input,index)?;
                    keys.push(key);
                    self.claim(add(size_of::<Value>(),key.len())?)?;
                    let value=attempt!(self.value(child,element,child_path,element.name(),depth+1));
                    validations.push(Some(value.validation));
                    if let Some(converted)=&mut converted {converted.push(Some((Value::Utf8(key.into()),value.value.ok_or_else(|| unsupported("Default map child materialization returned no value"))?)));}
                }
                let order=rtdb_types::java_hash_map_order(count,keys.iter(),|key| key.encode_utf16().fold(0i32,|hash,unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit))),|left,right| left.encode_utf16().cmp(right.encode_utf16())).ok_or_else(super::resolution::overflow)?;
                self.claim(mul(order.len(),size_of::<(Value,Value)>())?)?;
                for index in &order {
                    let current=validations[*index].take().ok_or_else(|| unsupported("Default map validation index is duplicated"))?;
                    if matches!(validation,DefaultAttempt::Value(())) {
                        validation=default_scalar::validate_string_class(key,key.name());
                        if matches!(validation,DefaultAttempt::Value(())) {validation=current;}
                    }
                }
                if let Some(mut converted)=converted {
                    let mut output=Vec::with_capacity(order.len());
                    for index in order {output.push(converted[index].take().ok_or_else(|| unsupported("Default map index is duplicated"))?);}
                    Some(Value::Map{entries:output,sorted:false})
                } else {None}
            }
            _=>return Err(unsupported("Default container conversion requires a concrete supported schema")),
        };
        let map_struct_return=matches!(destination.kind(),ViewKind::Struct(_)) && matches!(path,ConversionPath::MapChild) && matches!(input,ScalarInput::FieldObject(_) | ScalarInput::Native(Value::Map{..}));
        if !map_struct_return && matches!(logical_name,Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp" | "org.apache.kafka.connect.data.Decimal")) {
            return Ok(invalid("Logical default has an incompatible container representation"));
        }
        if map_struct_return && matches!(logical_name,Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp" | "org.apache.kafka.connect.data.Decimal")) {validation=invalid("Logical default destination requires a logical object class");}
        Ok(DefaultAttempt::Value(Converted {value:converted,validation}))
    }
}
fn node_schema<'a>(mut schema:&'a Schema,names:&'a BTreeMap<String,Schema>)->Result<&'a Schema> {
    let mut depth=0;
    loop {
        if depth>128 {return Err(error("batch-too-large","Default schema nesting limit exceeded"));}
        schema=match schema {Schema::Ref{..}=>super::resolve(schema,names)?,Schema::Decimal(value)=>&value.inner,Schema::Union(_)=>return Err(unsupported("JsonNode default conversion requires the complete first-member union adapter")),_=>return Ok(schema)};
        depth+=1;
    }
}
fn map_len(input:ScalarInput<'_>)->Result<DefaultAttempt<usize>> {
    match input {
        ScalarInput::FieldObject(source)=>match source.kind()? {ObjectKind::Object(values)=>Ok(DefaultAttempt::Value(values.len())),_=>Ok(invalid("Map default is not a Map"))},
        ScalarInput::JsonNode(source)=>match source.raw().and_then(DefaultJson::as_object) {Some(values)=>Ok(DefaultAttempt::Value(values.len())),None=>Ok(invalid("Map JsonNode default is not an object"))},
        ScalarInput::Native(Value::Map{entries,..})=>Ok(DefaultAttempt::Value(entries.len())),
        _=>Ok(invalid("Map default is not a Map")),
    }
}
fn map_entry<'a>(input:ScalarInput<'a>,index:usize)->Result<(&'a str,ScalarInput<'a>,ConversionPath)> {
    match input {
        ScalarInput::FieldObject(source)=>{
            let (key,_)=source.raw().as_object().and_then(|values| values.iter().nth(index)).ok_or_else(|| unsupported("Default map entry is absent"))?;
            Ok((key,ScalarInput::FieldObject(source.field(key,false)?.ok_or_else(|| unsupported("Default map source field is absent"))?),ConversionPath::MapChild))
        }
        ScalarInput::JsonNode(source)=>{
            let Schema::Map(map)=source.schema() else {return Err(unsupported("JsonNode map default source schema is not a map"));};
            let (key,value)=source.raw().and_then(DefaultJson::as_object).and_then(|values| values.iter().nth(index)).ok_or_else(|| unsupported("Default map entry is absent"))?;
            Ok((key,ScalarInput::JsonNode(JsonNode::new(Some(value),&map.types)),ConversionPath::Wrapper))
        }
        ScalarInput::Native(Value::Map{entries,..})=>match entries.get(index) {Some((Value::Utf8(key),value))=>Ok((key,ScalarInput::Native(value),ConversionPath::MapChild)),_=>Err(unsupported("Native default map has an unsupported key class"))},
        _=>Err(unsupported("Map default is not a Map")),
    }
}

#[cfg(test)] mod tests;
