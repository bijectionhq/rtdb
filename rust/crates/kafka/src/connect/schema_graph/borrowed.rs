use std::{collections::BTreeMap, ops::Bound};
use super::*;

#[derive(Clone, Copy)]
pub(crate) struct SchemaView<'a> { source: Source<'a> }
#[derive(Clone, Copy)]
enum Source<'a> { Schema(&'a ConnectSchema), Graph(&'a SchemaGraph, NodeId) }

pub(crate) enum ViewKind<'a> {
    Scalar(Scalar), Array(SchemaView<'a>), ArrayOfNull,
    Map(SchemaView<'a>, SchemaView<'a>), MapWithNullValues(SchemaView<'a>), MapOfNull,
    Struct(Fields<'a>),
}
#[derive(Clone, Copy)]
pub(crate) struct Field<'a> { pub name: &'a str, pub schema: SchemaView<'a> }
pub(crate) struct Fields<'a> { source: FieldSource<'a>, offset: usize }
enum FieldSource<'a> { Schema(&'a [ConnectField]), Graph(&'a SchemaGraph, &'a [(String, NodeId)]) }
pub(crate) struct Parameters<'a> { schema: SchemaView<'a>, previous: Option<&'a str> }
struct MetadataView<'a> {
    optional: bool, name: Option<&'a str>, version: Option<i32>, doc: Option<&'a str>,
    parameters: &'a BTreeMap<String, String>, default: Option<&'a Value>,
}
struct MetadataChain<'a> { source: Option<Source<'a>> }

impl<'a> SchemaView<'a> {
    pub(crate) fn new(schema: &'a ConnectSchema) -> Self { Self { source: Source::Schema(schema) } }
    pub(super) fn node(graph: &'a SchemaGraph, node: NodeId) -> Self { Self { source: Source::Graph(graph, node) } }
    fn metadata(self) -> MetadataChain<'a> { MetadataChain { source: Some(self.source) } }
    pub(crate) fn optional(self) -> bool { self.metadata().any(|metadata| metadata.optional) }
    pub(crate) fn name(self) -> Option<&'a str> { self.metadata().find_map(|metadata| metadata.name) }
    pub(crate) fn version(self) -> Option<i32> { self.metadata().find_map(|metadata| metadata.version) }
    pub(crate) fn doc(self) -> Option<&'a str> { self.metadata().find_map(|metadata| metadata.doc) }
    pub(crate) fn default_value(self) -> Option<&'a Value> { self.metadata().find_map(|metadata| metadata.default) }
    pub(crate) fn parameter(self, name: &str) -> Option<&'a str> { self.metadata().find_map(|metadata| metadata.parameters.get(name).map(String::as_str)) }
    pub(crate) fn parameters(self) -> Parameters<'a> { Parameters { schema: self, previous: None } }
    pub(crate) fn legacy_resolution_bound(self) -> Result<usize> {
        let graph = match self.source {
            Source::Schema(ConnectSchema { kind: SchemaKind::Reference(reference), .. }) => (&*reference.graph, reference.node),
            Source::Schema(_) => return Ok(0),
            Source::Graph(graph, node) => (graph, node),
        };
        super::admitted::resolution_bound(graph.0, graph.1, self.metadata_allocation_bound()?)
    }
    pub(crate) fn identity(self) -> Option<(usize, usize)> {
        match self.source {
            Source::Schema(ConnectSchema { kind: SchemaKind::Reference(reference), .. }) => Some(reference.identity()),
            Source::Schema(_) => None,
            Source::Graph(graph, node) => Some((graph as *const SchemaGraph as usize, node.0)),
        }
    }
    pub(crate) fn field(self, name: &str) -> Option<Field<'a>> {
        match self.kind() { ViewKind::Struct(mut fields) => fields.find(|field| field.name == name), _ => None }
    }
    pub(crate) fn kind(self) -> ViewKind<'a> {
        match self.source {
            Source::Schema(schema) => match &schema.kind {
                SchemaKind::Bool => ViewKind::Scalar(Scalar::Bool), SchemaKind::I8 => ViewKind::Scalar(Scalar::I8), SchemaKind::I16 => ViewKind::Scalar(Scalar::I16),
                SchemaKind::I32 => ViewKind::Scalar(Scalar::I32), SchemaKind::I64 => ViewKind::Scalar(Scalar::I64), SchemaKind::F32 => ViewKind::Scalar(Scalar::F32),
                SchemaKind::F64 => ViewKind::Scalar(Scalar::F64), SchemaKind::String => ViewKind::Scalar(Scalar::String), SchemaKind::Bytes => ViewKind::Scalar(Scalar::Bytes),
                SchemaKind::Array(element) => ViewKind::Array(Self::new(element)), SchemaKind::ArrayOfNull => ViewKind::ArrayOfNull,
                SchemaKind::Map(key, value) => ViewKind::Map(Self::new(key), Self::new(value)),
                SchemaKind::MapWithNullValues(key) => ViewKind::MapWithNullValues(Self::new(key)), SchemaKind::MapOfNull => ViewKind::MapOfNull,
                SchemaKind::Struct(fields) => ViewKind::Struct(Fields { source: FieldSource::Schema(fields), offset: 0 }),
                SchemaKind::Reference(reference) => Self::node(&reference.graph, reference.node).kind(),
            },
            Source::Graph(graph, node) => match &graph.nodes[graph.target(node).0].kind {
                NodeKind::Scalar(scalar) => ViewKind::Scalar(*scalar), NodeKind::Array(element) => ViewKind::Array(Self::node(graph, *element)),
                NodeKind::Map(key, value) => ViewKind::Map(Self::node(graph, *key), Self::node(graph, *value)),
                NodeKind::Struct(fields) => ViewKind::Struct(Fields { source: FieldSource::Graph(graph, fields), offset: 0 }),
                NodeKind::Alias(_) => unreachable!("sealed aliases resolve to a value node"),
            },
        }
    }
}
impl<'a> Iterator for MetadataChain<'a> {
    type Item = MetadataView<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let source = self.source.take()?;
        Some(match source {
            Source::Schema(schema) => MetadataView { optional: schema.optional, name: schema.name.as_deref(), version: schema.version, doc: schema.doc.as_deref(), parameters: &schema.parameters, default: schema.default_value() },
            Source::Graph(graph, id) => {
                let node = &graph.nodes[id.0];
                if let NodeKind::Alias(next) = node.kind { self.source = Some(Source::Graph(graph, next)); }
                let metadata = &node.metadata;
                MetadataView { optional: metadata.optional, name: metadata.name.as_deref(), version: metadata.version, doc: metadata.doc.as_deref(), parameters: &metadata.parameters, default: metadata.default.as_ref().filter(|value| **value != Value::Null) }
            }
        })
    }
}
impl<'a> Iterator for Fields<'a> {
    type Item = Field<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let field = match &self.source {
            FieldSource::Schema(fields) => { let field = fields.get(self.offset)?; Field { name: &field.name, schema: SchemaView::new(&field.schema) } },
            FieldSource::Graph(graph, fields) => { let (name, node) = fields.get(self.offset)?; Field { name, schema: SchemaView::node(graph, *node) } },
        };
        self.offset += 1;
        Some(field)
    }
    fn size_hint(&self) -> (usize, Option<usize>) { let remaining = self.len(); (remaining, Some(remaining)) }
}
impl ExactSizeIterator for Fields<'_> {
    fn len(&self) -> usize { (match &self.source { FieldSource::Schema(fields) => fields.len(), FieldSource::Graph(_, fields) => fields.len() }) - self.offset }
}
impl<'a> Iterator for Parameters<'a> {
    type Item = (&'a str, &'a str);
    fn next(&mut self) -> Option<Self::Item> {
        let name = self.schema.metadata().filter_map(|metadata| match self.previous {
            Some(previous) => metadata.parameters.range::<str, _>((Bound::Excluded(previous), Bound::Unbounded)).next(),
            None => metadata.parameters.iter().next(),
        }.map(|(name, _)| name.as_str())).min()?;
        self.previous = Some(name);
        Some((name, self.schema.parameter(name).expect("parameter came from metadata chain")))
    }
}

#[cfg(test)]
mod tests;
