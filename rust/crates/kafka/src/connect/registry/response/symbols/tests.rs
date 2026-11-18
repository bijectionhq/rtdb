use super::*;
use rtdb_types::MemoryBudget;

#[derive(Default)]
struct Intern {
    calls: usize, save: bool, saved: Option<Arc<SymbolName>>, denied: bool, replacement: Option<Arc<SymbolName>>,
}
impl InternAuthority for Intern {
    fn intern(&mut self, candidate: Arc<SymbolName>, _: &mut ConversionContext) -> rtdb_types::Result<Arc<SymbolName>> {
        self.calls += 1;
        if self.save { self.saved = Some(Arc::clone(&candidate)); }
        if self.denied { return Err(Anomaly::incorrect("fixture/intern", "Intern authority denied the candidate")); }
        Ok(self.replacement.as_ref().map_or(candidate, Arc::clone))
    }
}
fn each(mut run: impl FnMut(FactoryProfile, &mut ConversionContext)) {
    for profile in [Profile::Jackson216, Profile::Jackson221] {
        let memory = MemoryBudget::new(1 << 28);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        run(FactoryProfile { profile, identity_hash_seed: 7 }, &mut context);
        drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}
fn units(text: &str) -> Vec<u16> { text.encode_utf16().collect() }
fn text(value: JavaString, expected: &[u16]) { assert!(value.view().units().eq(expected.iter().copied())); }
fn metrics(child: &ChildSymbols) -> (usize, usize, i32) {
    let ChildState::Active(active) = &child.state else { panic!("expected active source state") };
    let table = active.storage.table();
    (table.primary.len(), table.count, table.longest)
}
fn collider(index: usize) -> Vec<u16> {
    (0..9).flat_map(|bit| if index & (1 << bit) == 0 { [65, 97] } else { [66, 64] }).collect()
}

#[test]
fn persistent_children_reuse_exact_units_without_repeating_the_intern_demand() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut first = root.child(context).unwrap();
        assert_eq!(first.factory(), factory);
        let name = [0x61, 0xd800, 0xd83d, 0xde00];
        let mut intern = Intern::default();
        text(first.find(&name, &mut intern, context).unwrap(), &name);
        text(first.find(&name, &mut intern, context).unwrap(), &name);
        assert_eq!(intern.calls, 1);
        assert_eq!(first.release(context).unwrap(), Release::Published);
        let mut second = root.child(context).unwrap();
        text(second.find(&name, &mut intern, context).unwrap(), &name);
        assert_eq!(intern.calls, 1);
        assert_eq!(second.release(context).unwrap(), Release::Unchanged);
    });
}

#[test]
fn an_empty_name_uses_the_constant_before_constraints_interning_or_additional_admission() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut child = root.child(context).unwrap();
        let remaining = context.allocation_remaining();
        let _full = context.reserve(remaining).unwrap();
        let mut intern = Intern { denied: true, ..Intern::default() };
        text(child.find(&[], &mut intern, context).unwrap(), &[]);
        assert_eq!(intern.calls, 0);
        assert_eq!(metrics(&child), (64, 0, 0));
        assert_eq!(child.release(context).unwrap(), Release::Unchanged);
    });
}

#[test]
fn name_length_failure_precedes_copying_or_interning_and_does_not_dirty_shared_state() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut child = root.child(context).unwrap();
        let remaining = context.allocation_remaining();
        let mut intern = Intern::default();
        match child.find(&vec![65; NAME_LIMIT + 1], &mut intern, context) {
            Err(Failure::NameLength(length)) => assert_eq!(length, 50_001),
            _ => panic!("expected the source name-length failure"),
        }
        assert_eq!(context.allocation_remaining(), remaining);
        assert_eq!(intern.calls, 0);
        assert_eq!(child.release(context).unwrap(), Release::Unchanged);
    });
}

#[test]
fn the_first_shared_miss_copies_before_a_later_private_miss_can_rehash() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut first = root.child(context).unwrap();
        let mut intern = Intern::default();
        for index in 0..48 { first.find(&units(&format!("initial{index}")), &mut intern, context).unwrap(); }
        assert_eq!(metrics(&first).0, 64);
        first.release(context).unwrap();
        let mut child = root.child(context).unwrap();
        child.find(&units("first-new"), &mut intern, context).unwrap();
        assert_eq!(metrics(&child).0, 64);
        assert_eq!(metrics(&child).1, 49);
        child.find(&units("second-new"), &mut intern, context).unwrap();
        assert_eq!(metrics(&child).0, 128);
        assert_eq!(metrics(&child).1, 50);
        for index in 0..48 { text(child.find(&units(&format!("initial{index}")), &mut intern, context).unwrap(), &units(&format!("initial{index}"))); }
        assert_eq!(intern.calls, 50);
        child.release(context).unwrap();
    });
}

#[test]
fn equal_count_children_leave_the_current_root_contents_unchanged() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut first = root.child(context).unwrap();
        let mut second = root.child(context).unwrap();
        let mut intern = Intern::default();
        first.find(&units("first"), &mut intern, context).unwrap();
        second.find(&units("second"), &mut intern, context).unwrap();
        assert_eq!(first.release(context).unwrap(), Release::Published);
        assert_eq!(second.release(context).unwrap(), Release::Unchanged);
        let mut child = root.child(context).unwrap();
        let mut observed = Intern::default();
        child.find(&units("first"), &mut observed, context).unwrap();
        assert_eq!(observed.calls, 0);
        child.find(&units("second"), &mut observed, context).unwrap();
        assert_eq!(observed.calls, 1);
    });
}

#[test]
fn a_stale_different_count_child_replaces_current_root_instead_of_union_merging_it() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut older = root.child(context).unwrap();
        let mut newer = root.child(context).unwrap();
        let mut intern = Intern::default();
        older.find(&units("older-one"), &mut intern, context).unwrap();
        older.find(&units("older-two"), &mut intern, context).unwrap();
        newer.find(&units("newer"), &mut intern, context).unwrap();
        newer.release(context).unwrap();
        assert_eq!(older.release(context).unwrap(), Release::Published);
        let mut child = root.child(context).unwrap();
        let mut observed = Intern::default();
        child.find(&units("older-one"), &mut observed, context).unwrap();
        assert_eq!(observed.calls, 0);
        child.find(&units("newer"), &mut observed, context).unwrap();
        assert_eq!(observed.calls, 1);
    });
}

#[test]
fn a_root_change_after_merge_snapshot_prevents_a_lost_compare_publication_from_overwriting_it() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut initial = root.child(context).unwrap();
        let mut intern = Intern::default();
        initial.find(&units("initial"), &mut intern, context).unwrap();
        initial.release(context).unwrap();
        let mut paused = root.child(context).unwrap();
        paused.find(&units("paused"), &mut intern, context).unwrap();
        let previous = Arc::clone(&root.state.lock().unwrap());
        let mut winner = root.child(context).unwrap();
        winner.find(&units("winner-one"), &mut intern, context).unwrap();
        winner.find(&units("winner-two"), &mut intern, context).unwrap();
        winner.release(context).unwrap();
        let ChildState::Active(active) = paused.state else { panic!("expected active paused child") };
        let Storage::Private(table) = active.storage else { panic!("expected an admitted private table") };
        assert!(!root.compare_publish(&previous, Arc::new(table)).unwrap());
        let mut child = root.child(context).unwrap();
        let mut observed = Intern::default();
        child.find(&units("winner-one"), &mut observed, context).unwrap();
        assert_eq!(observed.calls, 0);
        child.find(&units("paused"), &mut observed, context).unwrap();
        assert_eq!(observed.calls, 1);
    });
}

#[test]
fn reader_zero_hash_stays_raw_until_the_rehash_path_normalizes_it() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        let zero_hash = [1, 26, 8, 3, 24, 11, 16];
        assert_eq!(parser_hash(factory.identity_hash_seed, &zero_hash), 0);
        text(child.find(&zero_hash, &mut intern, context).unwrap(), &zero_hash);
        for index in 0..48 { child.find(&units(&format!("growth{index}")), &mut intern, context).unwrap(); }
        assert_eq!(metrics(&child).0, 128);
        let before = intern.calls;
        text(child.find(&zero_hash, &mut intern, context).unwrap(), &zero_hash);
        assert_eq!(intern.calls, before + 1);
        assert_eq!(metrics(&child).1, 50);
    });
}

#[test]
fn first_collision_spill_clears_its_bucket_and_second_spill_preserves_the_source_failure_prefix() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        for index in 0..152 { child.find(&collider(index), &mut intern, context).unwrap(); }
        assert_eq!(metrics(&child), (256, 1, -1));
        text(child.find(&collider(151), &mut intern, context).unwrap(), &collider(151));
        assert_eq!(intern.calls, 152);
        for index in 152..302 { child.find(&collider(index), &mut intern, context).unwrap(); }
        match child.find(&collider(302), &mut intern, context) {
            Err(Failure::CollisionOverflow { count }) => assert_eq!(count, 152),
            _ => panic!("expected the second source collision overflow"),
        }
        assert_eq!(metrics(&child).1, 152);
        assert_eq!(child.release(context).unwrap(), Release::Published);
        let mut next = root.child(context).unwrap();
        let bucket = mixed_index(parser_hash(factory.identity_hash_seed, &collider(0)), 256) >> 1;
        let mut added = 0;
        for index in 0..1000 {
            let name = units(&format!("other{index}"));
            if mixed_index(parser_hash(factory.identity_hash_seed, &name), 256) >> 1 == bucket { continue; }
            if added == 40 {
                match next.find(&name, &mut intern, context) {
                    Err(Failure::RehashCount { expected, actual }) => assert_eq!((expected, actual), (192, 191)),
                    _ => panic!("expected the retained source count mismatch on rehash"),
                }
                assert_eq!(next.release(context).unwrap(), Release::Published);
                return;
            }
            next.find(&name, &mut intern, context).unwrap();
            added += 1;
        }
        panic!("fixture failed to supply enough noncolliding growth names");
    });
}

#[test]
fn source_name_constraint_failure_can_release_the_preceding_accepted_table_prefix() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        child.find(&units("accepted"), &mut intern, context).unwrap();
        assert!(matches!(child.find(&vec![65; NAME_LIMIT + 1], &mut intern, context), Err(Failure::NameLength(50_001))));
        assert_eq!(child.release(context).unwrap(), Release::Published);
        let mut child = root.child(context).unwrap();
        child.find(&units("accepted"), &mut intern, context).unwrap();
        assert_eq!(intern.calls, 1);
    });
}

#[test]
fn exceeding_the_reuse_limit_resets_the_factory_root_after_the_parse() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        for index in 0..12_001 { child.find(&units(&format!("name{index}")), &mut intern, context).unwrap(); }
        assert_eq!(child.release(context).unwrap(), Release::Reset);
        let mut child = root.child(context).unwrap();
        assert_eq!(metrics(&child), (64, 0, 0));
        let before = intern.calls;
        child.find(&units("name0"), &mut intern, context).unwrap();
        assert_eq!(intern.calls, before + 1);
    });
}

#[test]
fn maximum_table_growth_disables_future_canonicalization_but_finishes_the_current_add() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern::default();
        for index in 0..49_153 { child.find(&units(&format!("name{index}")), &mut intern, context).unwrap(); }
        assert_eq!(metrics(&child).0, 64);
        assert_eq!(metrics(&child).1, 1);
        assert_eq!(intern.calls, 49_153);
        text(child.find(&units("bypass"), &mut intern, context).unwrap(), &units("bypass"));
        assert_eq!(intern.calls, 49_153);
        assert_eq!(child.release(context).unwrap(), Release::Disabled);
        assert_eq!(metrics(&root.child(context).unwrap()), (64, 0, 0));
    });
}

#[test]
fn admission_failure_before_copying_poisoned_state_cannot_publish_a_partial_child() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut child = root.child(context).unwrap();
        let remaining = context.allocation_remaining();
        let _full = context.reserve(remaining).unwrap();
        let mut intern = Intern::default();
        assert!(matches!(child.find(&units("denied"), &mut intern, context), Err(Failure::Native(_))));
        assert!(matches!(child.find(&[], &mut intern, context), Err(Failure::Poisoned)));
        assert_eq!(intern.calls, 0);
        assert_eq!(child.release(context).unwrap(), Release::Poisoned);
        assert_eq!(root.state.lock().unwrap().count, 0);
    });
}

#[test]
fn interning_failure_is_unchanged_and_its_retained_candidate_outlives_parser_and_factory_owners() {
    for profile in [Profile::Jackson216, Profile::Jackson221] {
        let memory = MemoryBudget::new(1 << 24);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, &mut context).unwrap();
        let mut child = root.child(&mut context).unwrap();
        let mut intern = Intern { save: true, denied: true, ..Intern::default() };
        match child.find(&units("retained"), &mut intern, &mut context) {
            Err(Failure::Native(cause)) => assert_eq!(cause, Anomaly::incorrect("fixture/intern", "Intern authority denied the candidate")),
            _ => panic!("expected unchanged interning authority failure"),
        }
        assert_eq!(child.release(&mut context).unwrap(), Release::Poisoned);
        drop(root); drop(context);
        assert_eq!(memory.used_bytes(), intern.saved.as_ref().unwrap().charge.bytes());
        assert_eq!(intern.saved.as_ref().unwrap().text().view().units().collect::<Vec<_>>(), units("retained"));
        drop(intern);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn interning_cannot_substitute_different_utf16_content_for_a_completed_name() {
    each(|factory, context| {
        let root = ReaderSymbols::new(factory, context).unwrap();
        let mut child = root.child(context).unwrap();
        let mut intern = Intern { save: true, ..Intern::default() };
        child.find(&units("original"), &mut intern, context).unwrap();
        intern.replacement = intern.saved.clone(); intern.save = false;
        match child.find(&units("different"), &mut intern, context) {
            Err(Failure::Native(cause)) => assert_eq!(cause.category, rtdb_types::AnomalyCategory::Unsupported),
            _ => panic!("expected the incompatible interning authority failure"),
        }
        assert_eq!(child.release(context).unwrap(), Release::Poisoned);
        assert_eq!(root.state.lock().unwrap().count, 0);
    });
}

#[test]
fn inherited_snapshot_leases_are_reattached_before_a_new_context_can_use_them() {
    for profile in [Profile::Jackson216, Profile::Jackson221] {
        let memory = MemoryBudget::new(1 << 24);
        let mut first_context = ConversionContext::new(Arc::clone(&memory));
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, &mut first_context).unwrap();
        let mut child = root.child(&mut first_context).unwrap();
        let mut intern = Intern::default();
        let result = child.find(&units("survivor"), &mut intern, &mut first_context).unwrap();
        child.release(&mut first_context).unwrap();
        drop(first_context);
        assert!(memory.used_bytes() > 0);
        let retained = memory.used_bytes();
        let mut denied = ConversionContext::with_limit(Arc::clone(&memory), 0);
        assert!(matches!(root.child(&mut denied), Err(Failure::Native(_))));
        drop(denied);
        let mut second_context = ConversionContext::new(Arc::clone(&memory));
        let mut child = root.child(&mut second_context).unwrap();
        assert_eq!(memory.used_bytes(), retained);
        text(child.find(&units("survivor"), &mut intern, &mut second_context).unwrap(), &units("survivor"));
        assert_eq!(intern.calls, 1);
        child.release(&mut second_context).unwrap();
        drop(root); drop(second_context);
        assert_eq!(memory.used_bytes(), result.retained_bytes());
        text(result, &units("survivor"));
        assert_eq!(memory.used_bytes(), 0);
    }
}
