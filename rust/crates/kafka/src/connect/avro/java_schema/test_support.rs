use super::*;
use rtdb_columnar::MemoryBudget;

pub(super) fn context()->ConversionContext {ConversionContext::new(MemoryBudget::new(usize::MAX))}
pub(super) fn text(value:&str,context:&mut ConversionContext)->Text {Text::copy(JavaText::from_utf8(value),context).unwrap()}
pub(super) fn name(value:&str,context:&mut ConversionContext)->Name {Name::new(Some(JavaText::from_utf8(value)),None,context).unwrap()}
pub(super) fn scalar_schema(kind:Type,context:&mut ConversionContext)->SchemaHandle {let mut builder=SchemasBuilder::new(context).unwrap();let id=builder.primitive(kind,context).unwrap();builder.finish(id,context).unwrap()}
pub(super) fn node(value:JsonValue,context:&mut ConversionContext)->JsonNode {JsonNode::with(4096,context,|_|Ok(value)).unwrap()}
pub(super) fn properties(entries:Vec<(&str,JsonNode)>,context:&mut ConversionContext)->Properties {
    let entries=entries.into_iter().map(|(key,value)|(text(key,context),value)).collect::<Vec<_>>();let members=Members::with(entries.len()*4096,context,||Ok(entries)).unwrap();Properties::from_members(members,context).unwrap()
}
