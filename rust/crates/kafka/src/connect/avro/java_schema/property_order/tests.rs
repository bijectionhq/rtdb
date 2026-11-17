use super::*;
use super::super::test_support::*;

const NAMES:[&str;16]=["AaAaAaAa","AaAaAaBB","AaAaBBAa","AaAaBBBB","AaBBAaAa","AaBBAaBB","AaBBBBAa","AaBBBBBB","BBAaAaAa","BBAaAaBB","BBAaBBAa","BBAaBBBB","BBBBAaAa","BBBBAaBB","BBBBBBAa","BBBBBBBB"];
fn members(names:impl IntoIterator<Item=String>,context:&mut ConversionContext)->Members {let mut entries=Vec::new();for name in names {entries.push((text(&name,context),node(JsonValue::Null,context)));}Members::with(entries.len()*4096,context,||Ok(entries)).unwrap()}
fn geometry(members:&Members)->PropertyOrder<'_> {PropertyOrder{members,entries:(0..members.len()).map(|_|Entry{hash:0,links:Links::default()}).collect(),table:Vec::new(),len:0}}
fn encounter(owner:&PropertyOrder<'_>)->Vec<usize> {let mut order=Vec::new();for bin in &owner.table {let mut next=bin.first();while let Some(index)=next {order.push(index);next=owner.node(index).links.next;}}order}
fn check(owner:&PropertyOrder<'_>) {
    fn branch(owner:&PropertyOrder<'_>,index:usize,parent:Option<usize>)->usize {let node=owner.node(index);assert_eq!(node.links.parent,parent);if node.links.red {assert!(!node.links.left.is_some_and(|left|owner.node(left).links.red));assert!(!node.links.right.is_some_and(|right|owner.node(right).links.red));}let left=node.links.left.map_or(1,|left| {assert_eq!(owner.direction(owner.node(left).hash,owner.name(left),index),Compare::Less);branch(owner,left,Some(index))});let right=node.links.right.map_or(1,|right| {assert_eq!(owner.direction(owner.node(right).hash,owner.name(right),index),Compare::Greater);branch(owner,right,Some(index))});assert_eq!(left,right);left+usize::from(!node.links.red)}
    let mut visited=std::collections::BTreeSet::new();for (bucket,bin) in owner.table.iter().enumerate() {if let Bin::Tree{root,..}=*bin {assert!(!owner.node(root).links.red);branch(owner,root,None);}let mut previous=None;let mut next=bin.first();while let Some(index)=next {assert!(visited.insert(index));assert_eq!(owner.node(index).hash&(owner.table.len()-1),bucket);assert_eq!(owner.node(index).links.previous,previous);previous=Some(index);next=owner.node(index).links.next;}}assert_eq!(visited.len(),owner.len);
}

#[test]
fn property_treeification_preserves_first_list_and_following_insertions_prepend() {
    let mut context=context();let members=members(NAMES.map(str::to_owned),&mut context);let mut owner=geometry(&members);for _ in 0..3 {owner.resize(&mut context).unwrap();}for index in 0..9 {owner.put(index,&mut context).unwrap();}assert_eq!(encounter(&owner),(0..9).collect::<Vec<_>>());let bucket=owner.node(0).hash&63;assert!(matches!(owner.table[bucket],Bin::Tree{first:0,root:3}));owner.put(9,&mut context).unwrap();assert_eq!(encounter(&owner),std::iter::once(9).chain(0..9).collect::<Vec<_>>());assert!(matches!(owner.table[bucket],Bin::Tree{first:9,..}));check(&owner);
}

#[test]
fn default_property_collision_presizing_precedes_treeification_and_count_update() {
    let mut context=context();let members=members(NAMES.map(str::to_owned),&mut context);let mut owner=geometry(&members);for index in 0..9 {owner.put(index,&mut context).unwrap();}assert_eq!(owner.table.len(),128);assert_eq!(owner.len,9);assert!(matches!(owner.table[owner.node(0).hash&127],Bin::List(_)));owner.put(9,&mut context).unwrap();assert_eq!(encounter(&owner),(0..10).collect::<Vec<_>>());assert!(matches!(owner.table[owner.node(0).hash&127],Bin::Tree{first:0,..}));owner.put(10,&mut context).unwrap();assert_eq!(encounter(&owner),std::iter::once(10).chain(0..10).collect::<Vec<_>>());check(&owner);
}

#[test]
fn property_transfer_splits_six_node_sides_in_first_list_order() {
    let mut context=context();let names=NAMES[..6].iter().map(|name|format!("{name}A")).chain(NAMES[..6].iter().map(|name|format!("{name}\u{81}")));let members=members(names,&mut context);let mut owner=geometry(&members);for _ in 0..3 {owner.resize(&mut context).unwrap();}for index in 0..12 {owner.put(index,&mut context).unwrap();}assert_eq!(owner.node(0).hash,426657327);assert_eq!(owner.node(6).hash,426657519);owner.resize(&mut context).unwrap();assert!(matches!(owner.table[47],Bin::List(_)));assert!(matches!(owner.table[111],Bin::List(_)));assert_eq!(encounter(&owner),(0..6).chain((9..12).rev()).chain(6..9).collect::<Vec<_>>());check(&owner);
}

#[test]
fn larger_split_trees_and_unsplit_tree_preserve_source_first_and_root_distinctions() {
    let mut context=context();let names=NAMES[..8].iter().map(|name|format!("{name}A")).chain(NAMES[..8].iter().map(|name|format!("{name}\u{81}")));let members=members(names,&mut context);let mut owner=geometry(&members);for _ in 0..3 {owner.resize(&mut context).unwrap();}for index in 0..16 {owner.put(index,&mut context).unwrap();}owner.resize(&mut context).unwrap();assert!(matches!(owner.table[47],Bin::Tree{first:0,root} if root!=0));assert!(matches!(owner.table[111],Bin::Tree{first:15,..}));assert_eq!(encounter(&owner),(0..8).chain((9..16).rev()).chain(std::iter::once(8)).collect::<Vec<_>>());check(&owner);let prior=encounter(&owner);owner.resize(&mut context).unwrap();assert_eq!(encounter(&owner),prior);check(&owner);
}

#[test]
fn snapshot_returns_chm_table_traversal_positions_separate_from_entry_set_order() {
    let mut context=context();let members=members(["b".to_owned(),"a".to_owned()],&mut context);let (order,_lease)=snapshot(&members,&mut context).unwrap();assert_eq!(order,vec![1,0]);assert_eq!(members.iter().map(|(key,_)|key.view().units().collect::<Vec<_>>()).collect::<Vec<_>>(),vec![vec![98],vec![97]]);
}

#[test]
fn list_transfer_keeps_the_last_run_and_reverses_only_each_preceding_side() {
    let mut context=context();let members=members(["a","q","A","Q","1"].map(str::to_owned),&mut context);let mut owner=geometry(&members);for index in 0..5 {owner.put(index,&mut context).unwrap();}assert_eq!(owner.table.len(),16);assert_eq!(encounter(&owner),vec![0,1,2,3,4]);owner.resize(&mut context).unwrap();assert_eq!(encounter(&owner),vec![2,0,1,3,4]);check(&owner);
}

#[test]
fn same_hash_tree_comparison_uses_unsigned_utf16_units() {
    let mut context=context();let supplementary=Text::copy_units(&[0xdbff,0xdfff,0xffff,0xffff],&mut context).unwrap();let bmp=Text::copy_units(&[0xe000,0x652b,0xd7ff,0xfd74],&mut context).unwrap();assert_eq!(supplementary.view().java_hash_code(),0x676a0bc0);assert_eq!(supplementary.view().java_hash_code(),bmp.view().java_hash_code());assert!(supplementary<bmp);let first=node(JsonValue::Null,&mut context);let second=node(JsonValue::Null,&mut context);let members=Members::with(4096,&mut context,||Ok(vec![(bmp,first),(supplementary,second)])).unwrap();let mut owner=geometry(&members);for _ in 0..3 {owner.resize(&mut context).unwrap();}owner.put(0,&mut context).unwrap();owner.put(1,&mut context).unwrap();let root=owner.build_tree(0);assert_eq!(root,0);assert_eq!(owner.node(root).links.left,Some(1));check(&owner);
}
