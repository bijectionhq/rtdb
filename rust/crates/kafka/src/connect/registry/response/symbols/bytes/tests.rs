use super::*;
use rtdb_types::{AnomalyCategory, MemoryBudget};

const PROFILES: [Profile; 2] = [Profile::Jackson216, Profile::Jackson221];
fn factory(profile: Profile) -> FactoryProfile { FactoryProfile { profile, seed: 7 } }
#[derive(Default)]
struct Intern { names: Vec<Vec<u16>>, fail_at: Option<usize>, replace: Option<Arc<SymbolName>> }
impl InternAuthority for Intern {
    fn intern(&mut self, candidate: Arc<SymbolName>, _: &mut ConversionContext) -> rtdb_types::Result<Arc<SymbolName>> {
        self.names.push(candidate.text.view().units().collect());
        if self.fail_at == Some(self.names.len()) { return Err(Anomaly::incorrect("fixture/byte-intern", "Byte interning denied")); }
        Ok(self.replace.as_ref().map_or(candidate, Arc::clone))
    }
}
fn name(text: &str, context: &mut ConversionContext) -> Arc<SymbolName> { prepare_name(&text.encode_utf16().collect::<Vec<_>>(), context).unwrap() }
fn add(child: &mut ByteChild, quads: &[u32], text: &str, intern: &mut Intern, context: &mut ConversionContext) -> JavaString {
    child.add(Quads::new(quads).unwrap(), name(text, context), intern, context).unwrap()
}
fn is(text: &JavaString, expected: &str) { assert!(text.view().units().eq(expected.encode_utf16())); }
fn run(mut inspect: impl FnMut(Profile, &mut ConversionContext)) {
    for profile in PROFILES {
        let memory = MemoryBudget::new(1 << 29);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        inspect(profile, &mut context);
        drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}
fn colliding(index: u32) -> [u32; 5] { [0, 0, 0, index, 1000 - index] }
fn private(child: &ByteChild) -> &Table { match &child.state { ChildState::Private(table) => table, _ => panic!("expected private byte table") } }

#[test]
fn empty_short_and_long_quad_lookups_retain_exact_added_names() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        assert_eq!(root.factory(), factory(profile));
        let mut child = root.child(context).unwrap();
        assert_eq!(child.factory(), factory(profile));
        assert!(Quads::new(&[]).is_none());
        is(&child.find(&[]).unwrap().unwrap(), "");
        let mut intern = Intern::default();
        for length in 1..=7 {
            let quads: Vec<_> = (0..length).map(|i| 0x0102_0304 + i).collect();
            assert!(child.find(&quads).unwrap().is_none());
            let expected = format!("name{length}");
            add(&mut child, &quads, &expected, &mut intern, context);
            is(&child.find(&quads).unwrap().unwrap(), &expected);
            let mut different = quads.clone(); *different.last_mut().unwrap() ^= 1;
            assert!(child.find(&different).unwrap().is_none());
        }
        assert_eq!(intern.names.len(), 7);
        assert_eq!(child.release(context).unwrap(), Release::Published);
    });
}

#[test]
fn prepared_names_and_table_hits_preserve_unmatched_utf16_and_original_backing() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let mut child = root.child(context).unwrap();
        let candidate = prepare_name(&[0xd800, 0, 0xdfff], context).unwrap();
        let charge = candidate.text.retained_bytes();
        let result = child.add(Quads::new(&[1, 2, 3, 4]).unwrap(), Arc::clone(&candidate), &mut Intern::default(), context).unwrap();
        assert_eq!(candidate.text.view().units().collect::<Vec<_>>(), result.view().units().collect::<Vec<_>>());
        assert_eq!(result.retained_bytes(), charge);
        let hit = child.find(&[1, 2, 3, 4]).unwrap().unwrap();
        assert_eq!(hit.retained_bytes(), charge);
        assert_eq!(hit.view().units().collect::<Vec<_>>(), [0xd800, 0, 0xdfff]);
    });
}

#[test]
fn actual_padded_utf8_quad_names_survive_repeated_factory_parsers() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let cases: &[(&[u32], &str)] = &[
            (&[0xffff_ff61], "a"), (&[0x6162_6364], "abcd"), (&[0x7363_6865, 0xffff_6d61], "schema"),
            (&[0xffff_c3a9], "é"), (&[0xf09f_9880], "😀"), (&[0x6162_0063], "ab\0c"),
        ];
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        for (quads, text) in cases { add(&mut child, quads, text, &mut intern, context); }
        child.release(context).unwrap();
        let child = root.child(context).unwrap();
        for (quads, text) in cases { is(&child.find(quads).unwrap().unwrap(), text); }
        assert_eq!(intern.names.len(), cases.len());
    });
}

#[test]
fn all_four_hash_shapes_preserve_java_wrapping_and_signed_long_quad_mixing() {
    assert_eq!(hash(0, &[0]), 0);
    assert_eq!(hash(0, &[0, 0]), 0);
    assert_eq!(hash(0, &[0, 0, 0]), 0);
    assert_eq!(hash(0, &[0, 0, 0, 0]), 0);
    assert_eq!(hash(7, &[0]), 63);
    assert_eq!(hash(7, &[0, 0]), 63);
    assert_eq!(hash(7, &[0, 0, 0]), 3_894_712);
    assert_eq!(hash(7, &[0, 0, 0, u32::MAX]), 508_735_817);
    assert_eq!(hash(-1, &[0x1234_5678]), 2_209_376_324);
    for index in 0..274 { assert_eq!(hash(7, &colliding(index)), hash(7, &colliding(0))); }
}

#[test]
fn collision_regions_are_searched_in_primary_secondary_tertiary_and_spill_order() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        for index in 0..13 { add(&mut child, &colliding(index), &format!("c{index}"), &mut intern, context); }
        let table = private(&child);
        assert_eq!(table.size, 64);
        assert_eq!(table.count, 13);
        assert_eq!(table.spill_end, 64 * 7 + 7 * 4);
        let primary = table.primary(hash(7, &colliding(0)));
        let secondary = table.size * 4 + (primary >> 3) * 4;
        let tertiary = table.size * 6 + ((primary >> (table.tertiary_shift + 2)) << table.tertiary_shift);
        is(&table.names.values[primary >> 2].as_ref().unwrap().text, "c0");
        is(&table.names.values[secondary >> 2].as_ref().unwrap().text, "c1");
        is(&table.names.values[tertiary >> 2].as_ref().unwrap().text, "c2");
        is(&table.names.values[(table.size * 7) >> 2].as_ref().unwrap().text, "c6");
        for index in 0..13 { is(&child.find(&colliding(index)).unwrap().unwrap(), &format!("c{index}")); }
        assert!(child.find(&colliding(999)).unwrap().is_none());
    });
}

#[test]
fn first_add_copies_shared_arrays_without_changing_other_children() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let mut first = root.child(context).unwrap();
        let other = root.child(context).unwrap();
        add(&mut first, &[1], "one", &mut Intern::default(), context);
        assert!(other.find(&[1]).unwrap().is_none());
        assert_eq!(first.release(context).unwrap(), Release::Published);
        assert!(other.find(&[1]).unwrap().is_none());
        let later = root.child(context).unwrap();
        is(&later.find(&[1]).unwrap().unwrap(), "one");
        assert_eq!(later.release(context).unwrap(), Release::Unchanged);
    });
}

#[test]
fn same_count_skips_publication_and_different_count_replaces_instead_of_union_merging() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let mut first = root.child(context).unwrap();
        let mut equal = root.child(context).unwrap();
        let mut larger = root.child(context).unwrap();
        let mut intern = Intern::default();
        add(&mut first, &[1], "first", &mut intern, context);
        add(&mut equal, &[2], "equal", &mut intern, context);
        add(&mut larger, &[3], "third", &mut intern, context);
        add(&mut larger, &[4], "fourth", &mut intern, context);
        assert_eq!(first.release(context).unwrap(), Release::Published);
        assert_eq!(equal.release(context).unwrap(), Release::Unchanged);
        assert_eq!(larger.release(context).unwrap(), Release::Published);
        let final_child = root.child(context).unwrap();
        assert!(final_child.find(&[1]).unwrap().is_none());
        assert!(final_child.find(&[2]).unwrap().is_none());
        is(&final_child.find(&[3]).unwrap().unwrap(), "third");
        is(&final_child.find(&[4]).unwrap().unwrap(), "fourth");
    });
}

#[test]
fn stale_compare_snapshot_does_not_overwrite_a_later_publication() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let old = Arc::clone(&root.state.lock().unwrap());
        let mut child = root.child(context).unwrap();
        add(&mut child, &[1], "one", &mut Intern::default(), context);
        child.release(context).unwrap();
        let replacement = Arc::new(Table::new(64, 512, 128, context).unwrap());
        assert!(!root.compare_publish(&old, replacement).unwrap());
        is(&root.child(context).unwrap().find(&[1]).unwrap().unwrap(), "one");
    });
}

#[test]
fn primary_empty_avoids_rehash_even_when_the_size_threshold_is_reached() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        let mut occupied = [false; 64];
        for quad in 0..100_000u32 {
            let slot = hash(7, &[quad]) as usize & 63;
            if occupied[slot] { continue; }
            occupied[slot] = true;
            add(&mut child, &[quad], &format!("p{quad}"), &mut intern, context);
            if intern.names.len() == 53 { break; }
        }
        assert_eq!(intern.names.len(), 53);
        assert_eq!(private(&child).size, 64);
        assert_eq!(private(&child).count, 53);
        assert!(private(&child).needs_rehash(profile));
        let quad = (100_000..200_000u32).find(|quad| occupied[hash(7, &[*quad]) as usize & 63]).unwrap();
        add(&mut child, &[quad], "occupied", &mut intern, context);
        assert_eq!(private(&child).size, 128);
        assert_eq!(private(&child).count, 54);
        assert_eq!(intern.names.len(), 107);
    });
}

#[test]
fn spill_resize_interns_the_incoming_name_before_existing_names_in_slot_order() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        for index in 0..22 { add(&mut child, &colliding(index), &format!("c{index}"), &mut intern, context); }
        assert_eq!(private(&child).size, 128);
        let expected: Vec<_> = (0..22).chain(0..21).map(|index| format!("c{index}").encode_utf16().collect::<Vec<_>>()).collect();
        assert_eq!(intern.names, expected);
        for index in 0..22 { is(&child.find(&colliding(index)).unwrap().unwrap(), &format!("c{index}")); }
    });
}

#[test]
fn failure_during_rehash_interning_poison_drops_the_unpublished_prefix() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern { fail_at: Some(23), ..Intern::default() };
        for index in 0..21 { add(&mut child, &colliding(index), &format!("c{index}"), &mut intern, context); }
        let candidate = name("c21", context);
        match child.add(Quads::new(&colliding(21)).unwrap(), candidate, &mut intern, context) {
            Err(Failure::Native(cause)) => assert_eq!(cause.code, "fixture/byte-intern"), _ => panic!("expected exact interning failure"),
        }
        assert_eq!(intern.names[21], "c21".encode_utf16().collect::<Vec<_>>());
        assert_eq!(intern.names[22], "c0".encode_utf16().collect::<Vec<_>>());
        assert!(matches!(child.find(&colliding(0)), Err(Failure::Poisoned)));
        assert_eq!(child.release(context).unwrap(), Release::Poisoned);
        assert!(root.child(context).unwrap().find(&colliding(0)).unwrap().is_none());
    });
}

#[test]
fn large_spill_overflow_preserves_its_mutated_cursor_and_releasable_source_prefix() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        for index in 0..529 { add(&mut child, &colliding(index), &format!("c{index}"), &mut intern, context); }
        let candidate = name("c529", context);
        match child.add(Quads::new(&colliding(529)).unwrap(), candidate, &mut intern, context) {
            Err(Failure::CollisionOverflow { count, size }) => { assert_eq!(count, 529); assert_eq!(size, 2048); }
            _ => panic!("expected source collision failure"),
        }
        assert_eq!(private(&child).spill_end, 2048 * 8);
        assert_eq!(intern.names.len(), 1063);
        assert!(child.find(&colliding(529)).unwrap().is_none());
        assert_eq!(child.release(context).unwrap(), Release::Published);
        let later = root.child(context).unwrap();
        is(&later.find(&colliding(528)).unwrap().unwrap(), "c528");
        assert!(later.find(&colliding(529)).unwrap().is_none());
    });
}

#[test]
fn long_quad_growth_keeps_prior_offsets_and_rehash_copies_all_quads() {
    run(|profile, context| {
        let mut table = Table::new(64, 512, 128, context).unwrap();
        let mut intern = Intern::default();
        for index in 0..22 { super::add(&mut table, &colliding(index), name(&format!("l{index}"), context), factory(profile), &mut intern, context).unwrap(); }
        assert_eq!(table.size, 128);
        assert!(table.area.values.len() > table.size * 8);
        for index in 0..22 { is(&table.find(&colliding(index), hash(7, &colliding(index))).unwrap().text, &format!("l{index}")); }
        assert_eq!(table.long_end, table.size * 8 + 22 * 5);
    });
}

#[test]
fn long_name_and_array_length_overflow_retain_their_source_stage() {
    run(|profile, context| {
        let mut table = Table::new(64, 512, 128, context).unwrap();
        table.long_end = i32::MAX as usize - 2;
        let mut intern = Intern::default();
        let candidate = name("long", context);
        match super::add(&mut table, &[1, 2, 3, 4], candidate, factory(profile), &mut intern, context) {
            Err(Failure::LongNameOffset { start, length, profile: selected }) => {
                assert_eq!(start, i32::MAX as usize - 2); assert_eq!(length, 4); assert_eq!(selected, profile);
            }
            _ => panic!("expected profile-specific source long-offset failure"),
        }
        assert_eq!(intern.names.len(), 1);
        let offset = table.primary(hash(7, &[1, 2, 3, 4]));
        assert_eq!(table.area.values[offset], hash(7, &[1, 2, 3, 4]));
        assert_eq!(table.area.values[offset + 3], 0);
        assert_eq!(table.count, 0);
        assert!(matches!(expanded_length(i32::MAX as usize - 16, 64), Err(Failure::ArrayLength { .. })));
        assert_eq!(expanded_length(512, 64).unwrap(), 576);
    });
}

#[test]
fn maximum_size_rehash_clears_both_arrays_without_disabling_later_additions() {
    run(|profile, context| {
        let mut table = Table::new(MAX_SIZE, MAX_SIZE * 8 + 64, MAX_SIZE * 2, context).unwrap();
        let mut intern = Intern::default();
        super::add(&mut table, &[1, 2, 3, 4], name("old", context), factory(profile), &mut intern, context).unwrap();
        rehash(&mut table, factory(profile), &mut intern, context).unwrap();
        assert_eq!(table.size, MAX_SIZE); assert_eq!(table.count, 0);
        assert_eq!(table.spill_end, MAX_SIZE * 7); assert_eq!(table.long_end, MAX_SIZE * 8);
        assert!(table.area.values.iter().all(|value| *value == 0));
        assert!(table.names.values.iter().all(Option::is_none));
        assert_eq!(intern.names.len(), 1);
        super::add(&mut table, &[2], name("new", context), factory(profile), &mut intern, context).unwrap();
        is(&table.find(&[2], hash(7, &[2])).unwrap().text, "new");
    });
}

#[test]
fn source_rehash_count_guard_keeps_the_completed_new_table_prefix() {
    run(|profile, context| {
        let mut table = Table::new(64, 512, 128, context).unwrap();
        let mut intern = Intern::default();
        super::add(&mut table, &[1], name("one", context), factory(profile), &mut intern, context).unwrap();
        table.count = 2;
        match rehash(&mut table, factory(profile), &mut intern, context) {
            Err(Failure::RehashCount { expected, actual }) => { assert_eq!(expected, 2); assert_eq!(actual, 1); }
            _ => panic!("expected source consistency failure"),
        }
        assert_eq!(table.size, 128); assert_eq!(table.count, 1);
        is(&table.find(&[1], hash(7, &[1])).unwrap().text, "one");
        assert_eq!(intern.names.len(), 2);
    });
}

#[test]
fn reusable_root_limit_resets_to_an_initial_table_after_completed_additions() {
    run(|profile, context| {
        let root = ByteSymbols::new(factory(profile), context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        for quad in 0..6001 { add(&mut child, &[quad], "same", &mut intern, context); }
        assert_eq!(private(&child).count, 6001);
        assert_eq!(child.release(context).unwrap(), Release::Reset);
        let snapshot = root.state.lock().unwrap();
        assert_eq!(snapshot.count, 0); assert_eq!(snapshot.size, 64);
        assert_eq!(snapshot.area.values.len(), 512); assert_eq!(snapshot.names.values.len(), 128);
    });
}

#[test]
fn native_copy_admission_and_changed_intern_results_poison_only_the_local_child() {
    for profile in PROFILES {
        let memory = MemoryBudget::new(1 << 24);
        let mut context = ConversionContext::with_limit(Arc::clone(&memory), 1 << 16);
        let root = ByteSymbols::new(factory(profile), &mut context).unwrap();
        let mut child = root.child(&mut context).unwrap();
        let candidate = name("one", &mut context);
        let mut intern = Intern::default();
        let remaining = context.allocation_remaining(); context.reserve(remaining).unwrap();
        match child.add(Quads::new(&[1]).unwrap(), candidate, &mut intern, &mut context) {
            Err(Failure::Native(cause)) => assert!(cause.code.ends_with("/batch-too-large")), _ => panic!("expected admission failure"),
        }
        assert!(intern.names.is_empty());
        assert_eq!(child.release(&mut context).unwrap(), Release::Poisoned);
        drop(context);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let mut child = root.child(&mut context).unwrap();
        assert!(child.find(&[1]).unwrap().is_none());
        let replacement = name("different", &mut context);
        let mut intern = Intern { replace: Some(replacement), ..Intern::default() };
        match child.add(Quads::new(&[1]).unwrap(), name("one", &mut context), &mut intern, &mut context) {
            Err(Failure::Native(cause)) => assert_eq!(cause.category, AnomalyCategory::Unsupported), _ => panic!("expected changed interning result"),
        }
        assert_eq!(child.release(&mut context).unwrap(), Release::Poisoned);
        drop(intern); drop(root); drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn inherited_snapshot_admission_is_required_and_returned_text_owns_its_original_lease() {
    for profile in PROFILES {
        let memory = MemoryBudget::new(1 << 24);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let root = ByteSymbols::new(factory(profile), &mut context).unwrap();
        let mut child = root.child(&mut context).unwrap();
        add(&mut child, &[1], "owned", &mut Intern::default(), &mut context);
        child.release(&mut context).unwrap();
        drop(context);
        let retained = memory.used_bytes();
        let mut denied = ConversionContext::with_limit(Arc::clone(&memory), retained - 1);
        assert!(matches!(root.child(&mut denied), Err(Failure::Native(_))));
        drop(denied);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let child = root.child(&mut context).unwrap();
        let text = child.find(&[1]).unwrap().unwrap();
        drop(child); drop(context); drop(root);
        assert_eq!(memory.used_bytes(), text.retained_bytes());
        is(&text, "owned");
        drop(text); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn long_area_growth_denial_happens_after_interning_and_discards_only_unpublished_names() {
    for profile in PROFILES {
        let memory = MemoryBudget::new(1 << 24);
        let mut context = ConversionContext::with_limit(Arc::clone(&memory), 1 << 16);
        let root = ByteSymbols::new(factory(profile), &mut context).unwrap();
        let mut child = root.child(&mut context).unwrap();
        let mut intern = Intern::default();
        add(&mut child, &[1], "first", &mut intern, &mut context);
        let candidate = name("long", &mut context);
        let remaining = context.allocation_remaining(); context.reserve(remaining).unwrap();
        match child.add(Quads::new(&[1, 2, 3, 4]).unwrap(), candidate, &mut intern, &mut context) {
            Err(Failure::Native(cause)) => assert!(cause.code.ends_with("/batch-too-large")), _ => panic!("expected long-area admission denial"),
        }
        assert_eq!(intern.names.len(), 2);
        assert_eq!(intern.names[1], "long".encode_utf16().collect::<Vec<_>>());
        assert_eq!(child.release(&mut context).unwrap(), Release::Poisoned);
        drop(context);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let child = root.child(&mut context).unwrap();
        assert!(child.find(&[1]).unwrap().is_none());
        assert!(child.find(&[1, 2, 3, 4]).unwrap().is_none());
        drop(child); drop(root); drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}
