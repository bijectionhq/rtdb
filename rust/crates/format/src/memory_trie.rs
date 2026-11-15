use prost::Message;
use rtdb_trie::{PersistedNode, PersistedTrie};
use rtdb_types::{Anomaly, Result};

use crate::generated::xtdb::trie::proto as pb;

pub fn encode(trie: &PersistedTrie) -> Result<Vec<u8>> {
    let mut root = encode_node(Some(&trie.root))?;
    if !trie.root_path.is_empty() { root.path = Some(trie.root_path.clone()); }
    let wire = pb::HashTrie {
        root_node: Some(root),
        log_limit: Some(i32::try_from(trie.log_limit).map_err(invalid)?),
        page_limit: Some(i32::try_from(trie.page_limit).map_err(invalid)?),
    };
    Ok(wire.encode_to_vec())
}

pub fn decode(bytes: &[u8]) -> Result<PersistedTrie> {
    let wire = pb::HashTrie::decode(bytes).map_err(invalid)?;
    let root = wire.root_node.ok_or_else(|| invalid("Trie has no root"))?;
    let root_path = root.path.clone().unwrap_or_default();
    if root_path.len() > 64 || root_path.iter().any(|bucket| *bucket > 3) { return Err(invalid("Invalid trie root path")); }
    let root = decode_node(root, root_path.len())?.ok_or_else(|| invalid("Trie root is absent"))?;
    let log_limit = usize::try_from(wire.log_limit.unwrap_or_default()).map_err(invalid)?;
    let page_limit = usize::try_from(wire.page_limit.unwrap_or_default()).map_err(invalid)?;
    if log_limit == 0 || page_limit == 0 { return Err(invalid("Trie limits must be positive")); }
    Ok(PersistedTrie { log_limit, page_limit, root_path, root })
}

fn encode_node(node: Option<&PersistedNode>) -> Result<pb::HashTrieNode> {
    Ok(pb::HashTrieNode {
        path: None,
        node: match node {
            None => None,
            Some(PersistedNode::Leaf(rows)) => Some(pb::hash_trie_node::Node::Leaf(pb::Leaf {
                data: rows.iter().map(|row| i32::try_from(*row).map_err(invalid)).collect::<Result<_>>()?,
            })),
            Some(PersistedNode::Branch(children)) => {
                if children.len() != 4 { return Err(invalid("An IID trie branch must have four buckets")); }
                Some(pb::hash_trie_node::Node::Branch(pb::Branch {
                    children: children.iter().map(|child| encode_node(child.as_ref())).collect::<Result<_>>()?,
                }))
            }
        },
    })
}

fn decode_node(node: pb::HashTrieNode, depth: usize) -> Result<Option<PersistedNode>> {
    if depth > 64 { return Err(invalid("Trie exceeds IID bit depth")); }
    Ok(match node.node {
        None => None,
        Some(pb::hash_trie_node::Node::Leaf(leaf)) => Some(PersistedNode::Leaf(leaf.data.into_iter().map(|row| u32::try_from(row).map_err(invalid)).collect::<Result<_>>()?)),
        Some(pb::hash_trie_node::Node::Branch(branch)) => {
            if branch.children.len() != 4 { return Err(invalid("An IID trie branch must have four buckets")); }
            Some(PersistedNode::Branch(branch.children.into_iter().map(|child| decode_node(child, depth + 1)).collect::<Result<_>>()?))
        }
    })
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-memory-trie", error.to_string())
}
