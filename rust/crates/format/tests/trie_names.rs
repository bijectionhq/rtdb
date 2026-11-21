use rtdb_format::keys::{PartGroup, TrieKey};

#[test]
fn a_naming_epoch_is_part_of_the_name_and_round_trips() {
    let before: TrieKey = "l02-rc-p1-b05".parse().unwrap();
    assert_eq!(before.epoch(), 0);
    let after = before.clone().with_epoch(1);
    assert_eq!(after.to_string(), "l02-rc-p1-b05-e01");
    assert_ne!(after, before, "the same position in a later epoch is a different trie");
    assert_eq!(after.to_string().parse::<TrieKey>().unwrap(), after);
    assert_eq!((after.level(), after.recency(), after.part(), after.block_index()), (before.level(), before.recency(), before.part(), before.block_index()));
}

#[test]
fn a_partition_group_carries_its_members_epoch() {
    let member = "l02-rc-p3-b05".parse::<TrieKey>().unwrap().with_epoch(2);
    let group = member.part_group().unwrap();
    assert_eq!(group.to_string(), "g02-rc-b05-e02");
    assert_eq!(group.to_string().parse::<PartGroup>().unwrap(), group);
    assert!(group.members().iter().all(|sibling| sibling.epoch() == 2));
    assert!(group.members().contains(&member));
}

#[test]
fn an_explicit_zero_epoch_is_not_a_canonical_name() {
    assert!("l01-rc-b00-e00".parse::<TrieKey>().is_err());
    assert!("l01-rc-b00-e".parse::<TrieKey>().is_err());
}
