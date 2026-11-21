#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/trie/src/leaf_load_step.rs"]
mod leaf_load_step;
#[path = "../../../rust/crates/trie/src/leaf_merge_step.rs"]
mod leaf_merge_step;
#[path = "../trie_leaf_merge/laws.rs"]
mod laws;

fn main() {}
