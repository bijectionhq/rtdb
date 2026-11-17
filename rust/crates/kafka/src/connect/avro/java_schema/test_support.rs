use super::*;
use rtdb_columnar::MemoryBudget;

pub(super) fn context()->ConversionContext {ConversionContext::new(MemoryBudget::new(usize::MAX))}
pub(super) fn text(value:&str,context:&mut ConversionContext)->Text {Text::copy(JavaText::from_utf8(value),context).unwrap()}
pub(super) fn name(value:&str,context:&mut ConversionContext)->Name {Name::new(Some(JavaText::from_utf8(value)),None,context).unwrap()}
pub(super) fn record(value:&str,context:&mut ConversionContext)->SchemaHandle {
    let mut builder=SchemasBuilder::new(context).unwrap();let id=builder.record(name(value,context),false,context).unwrap();builder.fields_with(id,0,context,||Ok(Vec::new())).unwrap();builder.finish(id,context).unwrap()
}
pub(super) fn scalar_schema(kind:Type,context:&mut ConversionContext)->SchemaHandle {let mut builder=SchemasBuilder::new(context).unwrap();let id=builder.primitive(kind,context).unwrap();builder.finish(id,context).unwrap()}
#[derive(Debug,PartialEq,Eq)]
pub(super) enum Event {Type(Type),Identity(SchemaId),Pair}
pub(super) struct Observed {pub events:Vec<Event>,pub fail:Option<Type>,pub type_value:i32}
impl Default for Observed {fn default()->Self {Self{events:Vec::new(),fail:None,type_value:7}}}
impl Witness for Observed {
    fn type_hash(&mut self,kind:Type,_:&mut ConversionContext)->Result<i32> {self.events.push(Event::Type(kind));if self.fail==Some(kind) {Err(error("schema-witness-test","Rejected schema type hash"))} else {Ok(self.type_value)}}
    fn schema_identity(&mut self,schema:&SchemaHandle,_:&mut ConversionContext)->Result<i32> {self.events.push(Event::Identity(schema.id()));Ok(11)}
    fn pair_identity(&mut self,_:evaluation::PairId,_:&mut ConversionContext)->Result<i32> {self.events.push(Event::Pair);Ok(13)}
}
pub(super) fn node(value:JsonValue,context:&mut ConversionContext)->JsonNode {JsonNode::with(4096,context,|_|Ok(value)).unwrap()}
pub(super) fn properties(entries:Vec<(&str,JsonNode)>,context:&mut ConversionContext)->Properties {
    let entries=entries.into_iter().map(|(key,value)|(text(key,context),value)).collect::<Vec<_>>();let members=Members::with(entries.len()*4096,context,||Ok(entries)).unwrap();Properties::from_members(members,context).unwrap()
}
