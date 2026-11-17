use super::*;
use rtdb_types::MemoryBudget;

const NAMES: [&str; 16] = [
    "AaAaAaAa", "AaAaAaBB", "AaAaBBAa", "AaAaBBBB", "AaBBAaAa", "AaBBAaBB", "AaBBBBAa", "AaBBBBBB",
    "BBAaAaAa", "BBAaAaBB", "BBAaBBAa", "BBAaBBBB", "BBBBAaAa", "BBBBAaBB", "BBBBBBAa", "BBBBBBBB",
];
fn text(name: &str, budget: &mut Budget) -> Text { Text::from_raw(name.as_bytes(), budget).unwrap() }
fn sized(budget: &mut Budget) -> ActiveTopics {
    let mut owner = ActiveTopics::new();
    for _ in 0..3 { owner.resize(budget).unwrap(); }
    assert_eq!(owner.table.len(), 64);
    owner
}
fn encounter(owner: &ActiveTopics) -> Vec<&str> { owner.iter().map(Text::as_str).collect() }
fn tree(owner: &ActiveTopics, name: &Text) -> (usize, usize) {
    match owner.table.as_slice()[ActiveTopics::hash(name) & (owner.table.len() - 1)] {
        Bin::Tree { first, root } => (first, root),
        _ => panic!("expected source tree bin"),
    }
}
fn check_branch(owner: &ActiveTopics, index: usize, parent: Option<usize>) -> usize {
    let node = owner.node(index);
    assert_eq!(node.links.parent, parent);
    if node.links.red {
        assert!(!node.links.left.is_some_and(|left| owner.node(left).links.red));
        assert!(!node.links.right.is_some_and(|right| owner.node(right).links.red));
    }
    let left = node.links.left.map_or(1, |left| {
        assert_eq!(owner.direction(owner.node(left).hash, &owner.node(left).name, index), std::cmp::Ordering::Less);
        check_branch(owner, left, Some(index))
    });
    let right = node.links.right.map_or(1, |right| {
        assert_eq!(owner.direction(owner.node(right).hash, &owner.node(right).name, index), std::cmp::Ordering::Greater);
        check_branch(owner, right, Some(index))
    });
    assert_eq!(left, right);
    left + usize::from(!node.links.red)
}
fn check(owner: &ActiveTopics) {
    let mut seen = std::collections::HashSet::new();
    for (bucket, bin) in owner.table.as_slice().iter().copied().enumerate() {
        if let Bin::Tree { root, .. } = bin { assert!(!owner.node(root).links.red); check_branch(owner, root, None); }
        let mut previous = None;
        let mut next = bin.first();
        while let Some(index) = next {
            assert!(seen.insert(index));
            let node = owner.node(index);
            assert_eq!(node.hash & (owner.table.len() - 1), bucket);
            assert_eq!(node.links.previous, previous);
            assert_eq!(owner.find(&node.name), Some(index));
            previous = Some(index); next = node.links.next;
        }
    }
    assert_eq!(seen.len(), owner.len);
    assert_eq!(owner.iter().len(), owner.len);
    assert_eq!(owner.iter().count(), owner.len);
}

#[test]
fn treeification_preserves_first_list_and_later_insertions_prepend_without_moving_root() {
    let memory = MemoryBudget::new(1 << 20);
    let mut budget = Budget(memory.clone());
    let mut owner = sized(&mut budget);
    let keys: Vec<_> = NAMES.iter().map(|name| text(name, &mut budget)).collect();
    for key in &keys[..9] { assert!(owner.put(key.clone(), 10, &mut budget).unwrap()); }
    assert_eq!(encounter(&owner), NAMES[..9]);
    let (first, root) = tree(&owner, &keys[0]);
    assert_eq!((first, root), (0, 3));
    assert_ne!(first, root);
    assert!(owner.put(keys[9].clone(), 20, &mut budget).unwrap());
    assert_eq!(encounter(&owner), [NAMES[9]].into_iter().chain(NAMES[..9].iter().copied()).collect::<Vec<_>>());
    let (first, root) = tree(&owner, &keys[0]);
    assert_eq!(first, 9); assert_ne!(first, root);
    let equal = text(NAMES[0], &mut budget);
    assert!(!equal.same_object(&keys[0]));
    assert!(!owner.put(equal, 30, &mut budget).unwrap());
    assert_eq!(owner.expiry(&keys[0]), Some(30));
    assert!(owner.node(owner.find(&keys[0]).unwrap()).name.same_object(&keys[0]));
    assert_eq!(encounter(&owner), [NAMES[9]].into_iter().chain(NAMES[..9].iter().copied()).collect::<Vec<_>>());
    check(&owner);
    drop(owner); drop(keys); drop(budget); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn collision_presizing_precedes_treeification_and_global_size_counting() {
    let mut budget = Budget(MemoryBudget::new(1 << 20));
    let mut owner = ActiveTopics::new();
    let keys: Vec<_> = NAMES.iter().map(|name| text(name, &mut budget)).collect();
    for key in &keys[..9] { owner.put(key.clone(), 0, &mut budget).unwrap(); }
    assert_eq!(owner.table.len(), 128);
    assert_eq!(owner.len, 9);
    assert!(matches!(owner.table.as_slice()[ActiveTopics::hash(&keys[0]) & 127], Bin::List(_)));
    assert_eq!(encounter(&owner), NAMES[..9]);
    owner.put(keys[9].clone(), 0, &mut budget).unwrap();
    assert_eq!(encounter(&owner), NAMES[..10]);
    tree(&owner, &keys[0]);
    owner.put(keys[10].clone(), 0, &mut budget).unwrap();
    assert_eq!(encounter(&owner), [NAMES[10]].into_iter().chain(NAMES[..10].iter().copied()).collect::<Vec<_>>());
    check(&owner);
}

#[test]
fn tree_removal_uses_source_shape_and_can_untreeify_seven_survivors() {
    let mut budget = Budget(MemoryBudget::new(1 << 20));
    let mut owner = sized(&mut budget);
    let keys: Vec<_> = NAMES[..9].iter().map(|name| text(name, &mut budget)).collect();
    for key in &keys { owner.put(key.clone(), 0, &mut budget).unwrap(); }
    assert_eq!(tree(&owner, &keys[0]), (0, 3));
    assert!(owner.remove(&keys[0]));
    assert_eq!(tree(&owner, &keys[1]), (1, 3));
    assert_eq!(encounter(&owner), NAMES[1..9]);
    check(&owner);
    assert!(owner.remove(&keys[8]));
    assert_eq!(owner.len, 7);
    assert!(matches!(owner.table.as_slice()[ActiveTopics::hash(&keys[1]) & 63], Bin::List(_)));
    assert_eq!(encounter(&owner), NAMES[1..8]);
    assert!(!owner.remove(&keys[0]));
    for key in &keys[1..8] { assert!(owner.remove(key)); check(&owner); }
    assert_eq!(owner.len, 0);
    assert!(encounter(&owner).is_empty());
}

#[test]
fn tree_deletion_balances_root_head_tail_and_interior_without_reordering_survivors() {
    for removed_first in 0..NAMES.len() {
        let mut budget = Budget(MemoryBudget::new(1 << 20));
        let mut owner = sized(&mut budget);
        let keys: Vec<_> = NAMES.iter().map(|name| text(name, &mut budget)).collect();
        for key in &keys { owner.put(key.clone(), 0, &mut budget).unwrap(); }
        let mut expected: Vec<_> = (9..16).rev().chain(0..9).collect();
        assert_eq!(encounter(&owner), expected.iter().map(|&index| NAMES[index]).collect::<Vec<_>>());
        for removed in (removed_first..16).chain(0..removed_first) {
            assert!(owner.remove(&keys[removed]));
            expected.retain(|&index| index != removed);
            assert_eq!(encounter(&owner), expected.iter().map(|&index| NAMES[index]).collect::<Vec<_>>());
            check(&owner);
        }
        assert_eq!(owner.entries.as_slice().iter().filter(|entry| entry.is_some()).count(), 0);
    }
}

#[test]
fn transfer_untreeifies_six_entry_sides_in_original_first_list_order() {
    let mut budget = Budget(MemoryBudget::new(1 << 20));
    let mut owner = sized(&mut budget);
    let low: Vec<_> = NAMES[..6].iter().map(|name| text(&format!("{name}A"), &mut budget)).collect();
    let high: Vec<_> = NAMES[..6].iter().map(|name| text(&format!("{name}\u{81}"), &mut budget)).collect();
    assert_eq!(ActiveTopics::hash(&low[0]), 426657327);
    assert_eq!(ActiveTopics::hash(&high[0]), 426657519);
    for key in low.iter().chain(high.iter()) { owner.put(key.clone(), 0, &mut budget).unwrap(); }
    tree(&owner, &low[0]);
    owner.resize(&mut budget).unwrap();
    assert_eq!(owner.table.len(), 128);
    assert!(matches!(owner.table.as_slice()[47], Bin::List(_)));
    assert!(matches!(owner.table.as_slice()[111], Bin::List(_)));
    let expected: Vec<_> = low.iter().chain(high[3..6].iter().rev()).chain(high[..3].iter()).map(Text::as_str).collect();
    assert_eq!(encounter(&owner), expected);
    check(&owner);
}

#[test]
fn transfer_rebuilds_two_larger_tree_sides_without_root_front_movement() {
    let mut budget = Budget(MemoryBudget::new(1 << 20));
    let mut owner = sized(&mut budget);
    let low: Vec<_> = NAMES[..8].iter().map(|name| text(&format!("{name}A"), &mut budget)).collect();
    let high: Vec<_> = NAMES[..8].iter().map(|name| text(&format!("{name}\u{81}"), &mut budget)).collect();
    for key in low.iter().chain(high.iter()) { owner.put(key.clone(), 0, &mut budget).unwrap(); }
    owner.resize(&mut budget).unwrap();
    let (first, root) = tree(&owner, &low[0]);
    assert!(owner.node(first).name.same_object(&low[0]));
    assert!(!owner.node(root).name.same_object(&low[0]));
    let (first, _) = tree(&owner, &high[0]);
    assert!(owner.node(first).name.same_object(&high[7]));
    let expected: Vec<_> = low.iter().chain(high[1..8].iter().rev()).chain(high[..1].iter()).map(Text::as_str).collect();
    assert_eq!(encounter(&owner), expected);
    check(&owner);
}

#[test]
fn transfer_preserves_an_unsplit_larger_tree_root_and_first_list() {
    let mut budget = Budget(MemoryBudget::new(1 << 20));
    let mut owner = sized(&mut budget);
    let keys: Vec<_> = NAMES.iter().map(|name| text(name, &mut budget)).collect();
    for key in &keys { owner.put(key.clone(), 0, &mut budget).unwrap(); }
    let geometry = tree(&owner, &keys[0]);
    let expected = encounter(&owner).into_iter().map(str::to_owned).collect::<Vec<_>>();
    owner.resize(&mut budget).unwrap();
    assert_eq!(tree(&owner, &keys[0]), geometry);
    assert_eq!(encounter(&owner), expected);
    check(&owner);
}

#[test]
fn same_hash_string_tree_order_uses_unsigned_utf16_units() {
    let mut budget = Budget(MemoryBudget::new(1 << 20));
    let mut owner = sized(&mut budget);
    let supplementary = text("\u{10ffff}\u{ffff}\u{ffff}", &mut budget);
    let bmp = text("\u{e000}\u{652b}\u{d7ff}\u{fd74}", &mut budget);
    assert_eq!(supplementary.hash(), 0x676a0bc0);
    assert_eq!(supplementary.hash(), bmp.hash());
    assert!(supplementary.as_str() > bmp.as_str());
    assert!(supplementary.as_str().encode_utf16().cmp(bmp.as_str().encode_utf16()).is_lt());
    owner.put(bmp.clone(), 1, &mut budget).unwrap();
    owner.put(supplementary.clone(), 2, &mut budget).unwrap();
    let first = owner.find(&bmp).unwrap();
    let root = owner.build_tree(first);
    let bucket = ActiveTopics::hash(&bmp) & 63;
    owner.table.as_mut_slice()[bucket] = Bin::Tree { first, root };
    assert!(owner.node(root).links.left.is_some_and(|left| owner.node(left).name == supplementary));
    assert_eq!(owner.expiry(&bmp), Some(1));
    assert_eq!(owner.expiry(&supplementary), Some(2));
    check(&owner);
}

#[test]
fn denied_tree_table_replacement_precedes_link_mutation_and_releases_every_owner() {
    let memory = MemoryBudget::new(1 << 20);
    let mut budget = Budget(memory.clone());
    let mut owner = sized(&mut budget);
    let keys: Vec<_> = NAMES.iter().map(|name| text(name, &mut budget)).collect();
    for key in &keys { owner.put(key.clone(), 0, &mut budget).unwrap(); }
    let geometry = tree(&owner, &keys[0]);
    let expected = encounter(&owner).into_iter().map(str::to_owned).collect::<Vec<_>>();
    let before = memory.used_bytes();
    let blocker = memory.reserve(memory.limit_bytes() - before).unwrap();
    assert!(owner.resize(&mut budget).is_err());
    assert_eq!(owner.table.len(), 64);
    assert_eq!(tree(&owner, &keys[0]), geometry);
    assert_eq!(encounter(&owner), expected);
    check(&owner);
    drop(blocker); assert_eq!(memory.used_bytes(), before);
    drop(owner); drop(keys); drop(budget); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn removed_tree_slots_reuse_their_admitted_inline_geometry_without_new_storage() {
    let memory = MemoryBudget::new(1 << 20);
    let mut budget = Budget(memory.clone());
    let mut owner = sized(&mut budget);
    let keys: Vec<_> = NAMES.iter().map(|name| text(name, &mut budget)).collect();
    for key in &keys[..15] { owner.put(key.clone(), 0, &mut budget).unwrap(); }
    assert!(owner.remove(&keys[10]));
    let before = memory.used_bytes();
    let blocker = memory.reserve(memory.limit_bytes() - before).unwrap();
    assert!(owner.put(keys[15].clone(), 99, &mut budget).unwrap());
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
    assert_eq!(owner.expiry(&keys[15]), Some(99));
    assert!(owner.iter().next().unwrap().same_object(&keys[15]));
    check(&owner);
    drop(blocker); drop(owner); drop(keys); drop(budget); assert_eq!(memory.used_bytes(), 0);
}
