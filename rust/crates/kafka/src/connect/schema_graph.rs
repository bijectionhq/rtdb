use std::{collections::BTreeMap, sync::Arc};
use rtdb_types::{Result, Value};
use super::{ConnectField, ConnectSchema, SchemaKind, error};
mod borrowed;
pub(crate) use borrowed::{SchemaView, ViewKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NodeId(usize);
#[derive(Clone, Debug, PartialEq)]
/// An immutable graph owner; clone a containing schema to retain recursive definitions.
pub struct SchemaReference { graph: Arc<SchemaGraph>, node: NodeId }
#[derive(Clone, Debug, PartialEq)]
struct SchemaGraph { nodes: Vec<Node>, retained_bytes: usize, view_bytes: usize }
#[derive(Clone, Debug)]
pub(crate) struct Metadata {
    pub optional: bool, pub name: Option<String>, pub version: Option<i32>, pub doc: Option<String>,
    pub parameters: BTreeMap<String, String>, pub default: Option<Value>,
}
impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.optional == other.optional && self.name == other.name && self.version == other.version
            && self.doc == other.doc && self.parameters == other.parameters
            && super::data::defaults_equal(self.default.as_ref(), other.default.as_ref())
    }
}
impl Default for Metadata {
    fn default() -> Self { Self { optional: false, name: None, version: None, doc: None, parameters: BTreeMap::new(), default: None } }
}
impl Metadata {
    pub fn from_schema(schema: &ConnectSchema) -> Self {
        Self { optional: schema.optional, name: schema.name.clone(), version: schema.version, doc: schema.doc.clone(), parameters: schema.parameters.clone(), default: schema.default_value().cloned() }
    }
    pub fn schema(mut self, kind: SchemaKind) -> ConnectSchema {
        self.normalize_default();
        ConnectSchema { kind, optional: self.optional, name: self.name, version: self.version, doc: self.doc, parameters: self.parameters, default: self.default }
    }
    fn normalize_default(&mut self) { if self.default == Some(Value::Null) { self.default = None; } }
    fn merge(&mut self, outer: &Self) {
        self.optional |= outer.optional;
        if outer.name.is_some() { self.name.clone_from(&outer.name); }
        if outer.version.is_some() { self.version = outer.version; }
        if outer.doc.is_some() { self.doc.clone_from(&outer.doc); }
        self.parameters.extend(outer.parameters.clone());
        if outer.default.is_some() { self.default.clone_from(&outer.default); }
    }
    fn retained_bytes(&self) -> Result<usize> {
        let mut bytes = 4096usize.saturating_add(self.name.as_ref().map_or(0, String::capacity)).saturating_add(self.doc.as_ref().map_or(0, String::capacity));
        for (name, value) in &self.parameters { bytes = bytes.saturating_add(1024).saturating_add(name.capacity()).saturating_add(value.capacity()); }
        if let Some(value) = &self.default { bytes = bytes.saturating_add(value.retained_bytes()?); }
        Ok(bytes)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Scalar { Bool, I8, I16, I32, I64, F32, F64, String, Bytes }
impl Scalar {
    fn kind(self) -> SchemaKind { match self { Self::Bool => SchemaKind::Bool, Self::I8 => SchemaKind::I8, Self::I16 => SchemaKind::I16, Self::I32 => SchemaKind::I32, Self::I64 => SchemaKind::I64, Self::F32 => SchemaKind::F32, Self::F64 => SchemaKind::F64, Self::String => SchemaKind::String, Self::Bytes => SchemaKind::Bytes } }
}
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum NodeKind { Scalar(Scalar), Array(NodeId), Map(NodeId, NodeId), Struct(Vec<(String, NodeId)>), Alias(NodeId) }
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Node { pub kind: NodeKind, pub metadata: Metadata }
pub(crate) struct GraphBuilder { nodes: Vec<Option<Node>> }
impl GraphBuilder {
    pub fn new() -> Self { Self { nodes: Vec::new() } }
    pub fn reserve(&mut self) -> NodeId { let id = NodeId(self.nodes.len()); self.nodes.push(None); id }
    pub fn define(&mut self, id: NodeId, node: Node) -> Result<()> {
        let slot = self.nodes.get_mut(id.0).ok_or_else(|| error("schema-graph", "Schema node was not reserved"))?;
        if slot.is_some() { return Err(error("schema-graph", "Schema node is already defined")); }
        *slot = Some(node); Ok(())
    }
    pub fn insert(&mut self, node: Node) -> NodeId { let id = NodeId(self.nodes.len()); self.nodes.push(Some(node)); id }
    #[cfg(test)]
    pub fn finish(self, root: NodeId) -> Result<Arc<ConnectSchema>> {
        self.finish_with(root, |_, schema| Ok(Metadata::from_schema(schema)))
    }
    pub fn finish_with(self, root: NodeId, mut metadata: impl FnMut(NodeId, &ConnectSchema) -> Result<Metadata>) -> Result<Arc<ConnectSchema>> {
        let nodes = self.nodes.into_iter().map(|node| {
            let mut node = node.ok_or_else(|| error("schema-graph", "Schema graph contains an undefined node"))?;
            node.metadata.normalize_default();
            Ok(node)
        }).collect::<Result<Vec<_>>>()?;
        if root.0 >= nodes.len() { return Err(error("schema-graph", "Schema graph root is absent")); }
        let mut retained_bytes = 4096usize;
        for node in &nodes {
            retained_bytes = retained_bytes.checked_add(node.metadata.retained_bytes()?).ok_or_else(|| error("allocation-overflow", "Schema graph size overflow"))?;
            let edges = match &node.kind {
                NodeKind::Scalar(_) => Vec::new(), NodeKind::Array(id) | NodeKind::Alias(id) => vec![*id], NodeKind::Map(key, value) => vec![*key, *value],
                NodeKind::Struct(fields) => {
                    let mut names = std::collections::BTreeSet::new();
                    for (name, _) in fields {
                        if !names.insert(name) { return Err(error("schema-graph", "Schema graph has duplicate Struct fields")); }
                        retained_bytes = retained_bytes.checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| error("allocation-overflow", "Schema graph fields overflow"))?;
                    }
                    fields.iter().map(|(_, id)| *id).collect()
                }
            };
            if edges.iter().any(|id| id.0 >= nodes.len()) { return Err(error("schema-graph", "Schema graph edge is out of range")); }
        }
        for index in 0..nodes.len() {
            let mut id = NodeId(index); let mut remaining = nodes.len();
            while let NodeKind::Alias(next) = nodes[id.0].kind {
                if remaining == 0 { return Err(error("schema-graph", "Schema aliases form a cycle without a value type")); }
                remaining -= 1; id = next;
            }
        }
        let order = metadata_order(&nodes);
        let mut graph = Arc::new(SchemaGraph { nodes, retained_bytes, view_bytes: retained_bytes });
        for id in order {
            let mut updated = metadata(id, &graph.view(id))?;
            updated.normalize_default();
            Arc::get_mut(&mut graph).expect("metadata contains no graph references").nodes[id.0].metadata = updated;
        }
        let graph_mut = Arc::get_mut(&mut graph).expect("metadata contains no graph references");
        graph_mut.retained_bytes = graph_mut.nodes.iter().try_fold(retained_bytes, |bytes, node| bytes.checked_add(node.metadata.retained_bytes()?).ok_or_else(|| error("allocation-overflow", "Schema graph metadata overflow")))?;
        graph_mut.view_bytes = (0..graph_mut.nodes.len()).map(|index| {
            let id = NodeId(index);
            let mut bytes = graph_mut.metadata(id).retained_bytes()?.saturating_mul(2);
            match &graph_mut.nodes[graph_mut.target(id).0].kind {
                NodeKind::Struct(fields) => for (name, child) in fields { bytes = bytes.saturating_add(name.len()).saturating_add(graph_mut.metadata(*child).retained_bytes()?).saturating_add(512); },
                NodeKind::Array(child) => bytes = bytes.saturating_add(graph_mut.metadata(*child).retained_bytes()?).saturating_add(512),
                NodeKind::Map(key, value) => bytes = bytes.saturating_add(graph_mut.metadata(*key).retained_bytes()?).saturating_add(graph_mut.metadata(*value).retained_bytes()?).saturating_add(1024),
                _ => (),
            }
            Ok(bytes)
        }).collect::<Result<Vec<_>>>()?.into_iter().max().unwrap_or(4096);
        Ok(Arc::new(graph.view(root)))
    }
}
fn metadata_order(nodes: &[Node]) -> Vec<NodeId> {
    let mut visited = vec![false; nodes.len()]; let mut order = Vec::with_capacity(nodes.len());
    let mut pending = Vec::new();
    for index in 0..nodes.len() {
        pending.push((NodeId(index), false));
        while let Some((id, expanded)) = pending.pop() {
            if expanded { order.push(id); continue; }
            if std::mem::replace(&mut visited[id.0], true) { continue; }
            pending.push((id, true));
            match &nodes[id.0].kind {
                NodeKind::Scalar(_) => (),
                NodeKind::Array(child) | NodeKind::Alias(child) => pending.push((*child, false)),
                NodeKind::Map(key, value) => { pending.push((*value, false)); pending.push((*key, false)); }
                NodeKind::Struct(fields) => pending.extend(fields.iter().rev().map(|(_, id)| (*id, false))),
            }
        }
    }
    order
}
impl SchemaGraph {
    fn target(&self, mut id: NodeId) -> NodeId { while let NodeKind::Alias(next) = self.nodes[id.0].kind { id = next; } id }
    fn metadata(&self, id: NodeId) -> Metadata {
        let target = self.target(id); let mut metadata = self.nodes[target.0].metadata.clone();
        let mut aliases = Vec::new(); let mut current = id;
        while let NodeKind::Alias(next) = self.nodes[current.0].kind { aliases.push(current); current = next; }
        for id in aliases.into_iter().rev() { metadata.merge(&self.nodes[id.0].metadata); } metadata
    }
    fn reference(self: &Arc<Self>, id: NodeId) -> Arc<ConnectSchema> {
        Arc::new(self.metadata(id).schema(SchemaKind::Reference(SchemaReference { graph: Arc::clone(self), node: id })))
    }
    fn view(self: &Arc<Self>, id: NodeId) -> ConnectSchema {
        let kind = match &self.nodes[self.target(id).0].kind {
            NodeKind::Scalar(scalar) => scalar.kind(), NodeKind::Array(element) => SchemaKind::Array(self.reference(*element)),
            NodeKind::Map(key, value) => SchemaKind::Map(self.reference(*key), self.reference(*value)),
            NodeKind::Struct(fields) => SchemaKind::Struct(fields.iter().map(|(name, id)| ConnectField { name: name.clone(), schema: self.reference(*id) }).collect()),
            NodeKind::Alias(_) => unreachable!("sealed aliases resolve to a value node"),
        };
        self.metadata(id).schema(kind)
    }
}
impl SchemaReference {
    pub(crate) fn resolve(&self) -> ConnectSchema { self.graph.view(self.node) }
    pub(crate) fn retained_bytes(&self) -> usize { self.graph.retained_bytes }
    pub(crate) fn view_allocation_bound(&self) -> usize { self.graph.view_bytes }
    pub(crate) fn owner_id(&self) -> usize { Arc::as_ptr(&self.graph) as usize }
    pub(crate) fn identity(&self) -> (usize, usize) { (self.owner_id(), self.node.0) }
    pub(crate) fn flattened_name_bound(&self, prefix: usize, delimiter: usize, depth: usize) -> Result<usize> { self.graph.flattened_name_bound(self.node, prefix, delimiter, depth) }
}
impl SchemaGraph {
    fn flattened_name_bound(&self, id: NodeId, prefix: usize, delimiter: usize, depth: usize) -> Result<usize> {
        if depth > 128 { return Err(error("transform-error", "Flatten nesting limit exceeded")); }
        let NodeKind::Struct(fields) = &self.nodes[self.target(id).0].kind else { return Ok(0); };
        fields.iter().try_fold(0usize, |bytes, (name, child)| {
            let name = prefix.checked_add(name.len()).and_then(|bytes| bytes.checked_add(if prefix == 0 { 0 } else { delimiter })).ok_or_else(|| error("allocation-overflow", "Flatten field length overflow"))?;
            let nested = self.flattened_name_bound(*child, name, delimiter, depth + 1)?;
            bytes.checked_add(name).and_then(|bytes| bytes.checked_add(nested)).ok_or_else(|| error("allocation-overflow", "Flatten schema length overflow"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recursive_views_keep_the_graph_alive_after_the_root_is_dropped() {
        let mut builder = GraphBuilder::new(); let root = builder.reserve();
        let value = builder.insert(Node { kind: NodeKind::Scalar(Scalar::I64), metadata: Metadata::default() });
        let optional = builder.insert(Node { kind: NodeKind::Alias(root), metadata: Metadata { optional: true, ..Default::default() } });
        builder.define(root, Node { kind: NodeKind::Struct(vec![("value".into(), value), ("next".into(), optional)]), metadata: Metadata { name: Some("Node".into()), ..Default::default() } }).unwrap();
        let root = builder.finish(root).unwrap(); let next = root.field("next").unwrap().schema;
        let SchemaKind::Reference(reference) = &next.kind else { panic!("expected graph reference") };
        let owner = Arc::downgrade(&reference.graph);
        drop(root); assert!(next.optional); assert!(matches!(next.resolved().kind, SchemaKind::Struct(_)));
        assert_eq!(next.field("value").unwrap().schema.resolved().kind, SchemaKind::I64);
        drop(next); assert!(owner.upgrade().is_none());
    }
    #[test]
    fn schema_graph_rejects_undefined_nodes_and_alias_only_cycles() {
        let mut builder = GraphBuilder::new(); let root = builder.reserve(); assert!(builder.finish(root).is_err());
        let mut builder = GraphBuilder::new(); let root = builder.reserve(); builder.define(root, Node { kind: NodeKind::Alias(root), metadata: Metadata::default() }).unwrap(); assert!(builder.finish(root).is_err());
    }
    #[test]
    fn recursive_graph_null_defaults_have_the_same_schema_identity_and_accounting_as_absence() {
        let build = |explicit: bool| {
            let metadata = || Metadata { optional: true, default: explicit.then_some(Value::Null), ..Default::default() };
            let mut builder = GraphBuilder::new();
            let root = builder.reserve();
            let value = builder.insert(Node { kind: NodeKind::Scalar(Scalar::String), metadata: metadata() });
            let array = builder.insert(Node { kind: NodeKind::Array(value), metadata: metadata() });
            let map = builder.insert(Node { kind: NodeKind::Map(value, array), metadata: metadata() });
            let next = builder.insert(Node { kind: NodeKind::Alias(root), metadata: metadata() });
            builder.define(root, Node { kind: NodeKind::Struct(vec![("value".into(), value), ("items".into(), map), ("next".into(), next)]), metadata: metadata() }).unwrap();
            builder.finish_with(root, |_, schema| {
                let mut metadata = Metadata::from_schema(schema);
                metadata.default = explicit.then_some(Value::Null);
                Ok(metadata)
            }).unwrap()
        };
        let absent = build(false);
        let explicit = build(true);
        assert_eq!(absent, explicit);
        assert_eq!(absent.retained_bytes().unwrap(), explicit.retained_bytes().unwrap());
        assert_eq!(absent.view_allocation_bound(), explicit.view_allocation_bound());
        assert!(explicit.default.is_none());
        for field in ["value", "items", "next"] { assert!(explicit.field(field).unwrap().schema.default.is_none()); }
    }
    #[test]
    fn recursive_graph_defaults_canonicalize_nan_equality_and_distinguish_signed_zero() {
        let build = |default| {
            let mut builder = GraphBuilder::new();
            let root = builder.reserve();
            let value = builder.insert(Node { kind: NodeKind::Scalar(Scalar::F64), metadata: Metadata { default: Some(Value::F64(default)), ..Default::default() } });
            let next = builder.insert(Node { kind: NodeKind::Alias(root), metadata: Metadata { optional: true, ..Default::default() } });
            builder.define(root, Node { kind: NodeKind::Struct(vec![("value".into(), value), ("next".into(), next)]), metadata: Metadata::default() }).unwrap();
            builder.finish(root).unwrap()
        };
        assert_eq!(build(f64::from_bits(0x7ff8_0000_0000_0001)), build(f64::from_bits(0xfff8_0000_0000_0002)));
        assert_ne!(build(0.0), build(-0.0));
    }
}
