use std::{collections::BTreeSet, mem::{align_of, size_of}};
use super::*;

fn overflow() -> rtdb_types::Anomaly { error("allocation-overflow", "Schema graph allocation size overflow") }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(overflow) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(overflow) }
fn tree_bound(entries: usize, entry: usize) -> Result<usize> { mul(entries, add(mul(entry, 16)?, 256)?) }
fn arc_bound<T>() -> Result<usize> { add(add(size_of::<T>(), mul(2, size_of::<usize>())?)?, align_of::<T>()) }

impl SchemaView<'_> {
    pub(crate) fn metadata_allocation_bound(self) -> Result<usize> {
        let mut bytes = size_of::<Metadata>();
        if let Some(name) = self.name() { bytes = add(bytes, name.len())?; }
        if let Some(doc) = self.doc() { bytes = add(bytes, doc.len())?; }
        let mut count = 0usize;
        for (name, value) in self.parameters() { count = add(count, 1)?; bytes = add(add(bytes, name.len())?, value.len())?; }
        bytes = add(bytes, tree_bound(count, size_of::<(String, String)>())?)?;
        if let Some(default) = self.default_value() { bytes = add(bytes, default.retained_bytes()?)?; }
        Ok(bytes)
    }
    pub(crate) fn metadata_owned(self, admit: &mut impl FnMut(usize) -> Result<()>) -> Result<Metadata> {
        admit(self.metadata_allocation_bound()?)?;
        Ok(owned_metadata(self))
    }
}
fn owned_metadata(view: SchemaView<'_>) -> Metadata {
    let mut parameters = BTreeMap::new();
    for (name, value) in view.parameters() { parameters.insert(name.into(), value.into()); }
    Metadata {
        optional: view.optional(), name: view.name().map(str::to_owned), version: view.version(), doc: view.doc().map(str::to_owned),
        parameters, default: view.default_value().cloned(),
    }
}
fn metadata_retained(metadata: &Metadata) -> Result<usize> {
    let mut bytes = 4096usize;
    if let Some(name) = &metadata.name { bytes = add(bytes, name.capacity())?; }
    if let Some(doc) = &metadata.doc { bytes = add(bytes, doc.capacity())?; }
    for (name, value) in &metadata.parameters { bytes = add(add(add(bytes, 1024)?, name.capacity())?, value.capacity())?; }
    if let Some(default) = &metadata.default { bytes = add(bytes, default.retained_bytes()?)?; }
    Ok(bytes)
}
fn frontier(nodes: &[Option<Node>]) -> Result<(usize, usize)> {
    let mut edges = 0usize;
    let mut bytes = mul(nodes.len(), size_of::<Node>())?;
    for node in nodes {
        let node = node.as_ref().ok_or_else(|| error("schema-graph", "Schema graph contains an undefined node"))?;
        let count = match &node.kind {
            NodeKind::Scalar(_) => 0, NodeKind::Array(_) | NodeKind::Alias(_) => 1, NodeKind::Map(_, _) => 2,
            NodeKind::Struct(fields) => {
                bytes = add(bytes, tree_bound(fields.len(), size_of::<&str>())?)?;
                fields.len()
            }
        };
        edges = add(edges, count)?;
    }
    let pending = add(mul(nodes.len(), 2)?, edges)?;
    bytes = add(bytes, mul(nodes.len(), add(size_of::<bool>(), size_of::<NodeId>())?)?)?;
    bytes = add(bytes, mul(pending, size_of::<(NodeId, bool)>())?)?;
    Ok((bytes, pending))
}
fn check_nodes(nodes: &[Node]) -> Result<usize> {
    let mut retained = 4096usize;
    for node in nodes {
        retained = add(retained, metadata_retained(&node.metadata)?)?;
        let valid = |id: NodeId| if id.0 < nodes.len() { Ok(()) } else { Err(error("schema-graph", "Schema graph edge is out of range")) };
        match &node.kind {
            NodeKind::Scalar(_) => (), NodeKind::Array(id) | NodeKind::Alias(id) => valid(*id)?,
            NodeKind::Map(key, value) => { valid(*key)?; valid(*value)?; },
            NodeKind::Struct(fields) => {
                let mut names = BTreeSet::new();
                for (name, id) in fields {
                    if !names.insert(name.as_str()) { return Err(error("schema-graph", "Schema graph has duplicate Struct fields")); }
                    retained = add(add(retained, name.capacity())?, 256)?;
                    valid(*id)?;
                }
            }
        }
    }
    for index in 0..nodes.len() {
        let mut id = NodeId(index);
        let mut remaining = nodes.len();
        while let NodeKind::Alias(next) = nodes[id.0].kind {
            if remaining == 0 { return Err(error("schema-graph", "Schema aliases form a cycle without a value type")); }
            remaining -= 1;
            id = next;
        }
    }
    Ok(retained)
}
fn order(nodes: &[Node], pending_capacity: usize) -> Vec<NodeId> {
    let mut visited = vec![false; nodes.len()];
    let mut order = Vec::with_capacity(nodes.len());
    let mut pending = Vec::with_capacity(pending_capacity);
    for index in 0..nodes.len() {
        pending.push((NodeId(index), false));
        while let Some((id, expanded)) = pending.pop() {
            if expanded { order.push(id); continue; }
            if std::mem::replace(&mut visited[id.0], true) { continue; }
            pending.push((id, true));
            match &nodes[id.0].kind {
                NodeKind::Scalar(_) => (), NodeKind::Array(id) | NodeKind::Alias(id) => pending.push((*id, false)),
                NodeKind::Map(key, value) => { pending.push((*value, false)); pending.push((*key, false)); },
                NodeKind::Struct(fields) => pending.extend(fields.iter().rev().map(|(_, id)| (*id, false))),
            }
        }
    }
    order
}
impl GraphBuilder {
    /// `metadata` must admit its own scratch and returned metadata/default allocations before constructing them.
    /// The caller retains all reservations for at least the returned graph's lifetime.
    pub(crate) fn finish_with_admission<A, M>(self, root: NodeId, mut admit: A, mut metadata: M) -> Result<Arc<ConnectSchema>>
    where A: FnMut(usize) -> Result<()>, M: for<'a> FnMut(NodeId, SchemaView<'a>, &mut A) -> Result<Metadata> {
        let (bytes, pending_capacity) = frontier(&self.nodes)?;
        if root.0 >= self.nodes.len() { return Err(error("schema-graph", "Schema graph root is absent")); }
        admit(bytes)?;
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for node in self.nodes {
            let mut node = node.expect("frontier checked every node");
            node.metadata.normalize_default();
            nodes.push(node);
        }
        let retained_bytes = check_nodes(&nodes)?;
        let order = order(&nodes, pending_capacity);
        let mut graph = SchemaGraph { nodes, retained_bytes, view_bytes: 0 };
        for id in order {
            let mut updated = metadata(id, SchemaView::node(&graph, id), &mut admit)?;
            updated.normalize_default();
            graph.nodes[id.0].metadata = updated;
        }
        graph.retained_bytes = graph.nodes.iter().try_fold(retained_bytes, |bytes, node| add(bytes, metadata_retained(&node.metadata)?))?;
        let mut maximum = 4096usize;
        for index in 0..graph.nodes.len() { maximum = maximum.max(legacy_view_bound(&graph, NodeId(index))?); }
        graph.view_bytes = maximum;
        let root_bytes = add(add(arc_bound::<SchemaGraph>()?, arc_bound::<ConnectSchema>()?)?, owned_view_bound(&graph, root)?)?;
        admit(root_bytes)?;
        let graph = Arc::new(graph);
        Ok(Arc::new(owned_view(&graph, root)))
    }
}
fn owned_child_bound(graph: &SchemaGraph, id: NodeId) -> Result<usize> {
    add(arc_bound::<ConnectSchema>()?, SchemaView::node(graph, id).metadata_allocation_bound()?)
}
fn owned_view_bound(graph: &SchemaGraph, id: NodeId) -> Result<usize> {
    let mut bytes = SchemaView::node(graph, id).metadata_allocation_bound()?;
    match &graph.nodes[graph.target(id).0].kind {
        NodeKind::Scalar(_) => (), NodeKind::Array(id) => bytes = add(bytes, owned_child_bound(graph, *id)?)?,
        NodeKind::Map(key, value) => bytes = add(add(bytes, owned_child_bound(graph, *key)?)?, owned_child_bound(graph, *value)?)?,
        NodeKind::Struct(fields) => {
            bytes = add(bytes, mul(fields.len(), size_of::<ConnectField>())?)?;
            for (name, id) in fields { bytes = add(add(bytes, name.len())?, owned_child_bound(graph, *id)?)?; }
        }
        NodeKind::Alias(_) => unreachable!("sealed aliases resolve to a value node"),
    }
    Ok(bytes)
}

fn owned_reference(graph: &Arc<SchemaGraph>, id: NodeId) -> Arc<ConnectSchema> {
    Arc::new(owned_metadata(SchemaView::node(graph, id)).schema(SchemaKind::Reference(SchemaReference { graph: Arc::clone(graph), node: id })))
}
fn owned_view(graph: &Arc<SchemaGraph>, id: NodeId) -> ConnectSchema {
    let kind = match &graph.nodes[graph.target(id).0].kind {
        NodeKind::Scalar(scalar) => scalar.kind(), NodeKind::Array(id) => SchemaKind::Array(owned_reference(graph, *id)),
        NodeKind::Map(key, value) => SchemaKind::Map(owned_reference(graph, *key), owned_reference(graph, *value)),
        NodeKind::Struct(fields) => {
            let mut result = Vec::with_capacity(fields.len());
            for (name, id) in fields { result.push(ConnectField { name: name.clone(), schema: owned_reference(graph, *id) }); }
            SchemaKind::Struct(result)
        }
        NodeKind::Alias(_) => unreachable!("sealed aliases resolve to a value node"),
    };
    owned_metadata(SchemaView::node(graph, id)).schema(kind)
}
fn legacy_metadata_bound(graph: &SchemaGraph, id: NodeId) -> Result<usize> {
    let mut total = 0usize;
    let mut count = 0usize;
    let mut current = id;
    loop {
        let node = &graph.nodes[current.0];
        total = add(total, metadata_retained(&node.metadata)?)?;
        total = add(total, tree_bound(node.metadata.parameters.len(), size_of::<(String, String)>())?)?;
        count = add(count, 1)?;
        match node.kind { NodeKind::Alias(next) => current = next, _ => break }
    }
    let copies = add(mul(count, 2)?, 4)?;
    add(mul(total, copies)?, mul(add(mul(count, 3)?, 4)?, size_of::<NodeId>())?)
}
fn legacy_child_bound(graph: &SchemaGraph, id: NodeId) -> Result<usize> { add(arc_bound::<ConnectSchema>()?, legacy_metadata_bound(graph, id)?) }
fn legacy_view_bound(graph: &SchemaGraph, id: NodeId) -> Result<usize> {
    let mut bytes = legacy_metadata_bound(graph, id)?;
    match &graph.nodes[graph.target(id).0].kind {
        NodeKind::Scalar(_) => (), NodeKind::Array(id) => bytes = add(bytes, legacy_child_bound(graph, *id)?)?,
        NodeKind::Map(key, value) => bytes = add(add(bytes, legacy_child_bound(graph, *key)?)?, legacy_child_bound(graph, *value)?)?,
        NodeKind::Struct(fields) => {
            bytes = add(bytes, mul(add(mul(fields.len(), 2)?, 4)?, size_of::<ConnectField>())?)?;
            for (name, id) in fields { bytes = add(add(bytes, name.len())?, legacy_child_bound(graph, *id)?)?; }
        }
        NodeKind::Alias(_) => unreachable!("sealed aliases resolve to a value node"),
    }
    add(bytes, mul(SchemaView::node(graph, id).metadata_allocation_bound()?, 2)?)
}

pub(super) fn resolution_bound(graph: &SchemaGraph, node: NodeId, outer_metadata: usize) -> Result<usize> { add(legacy_view_bound(graph, node)?, outer_metadata) }

#[cfg(test)] mod tests;
