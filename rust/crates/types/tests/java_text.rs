use rtdb_types::{AllocationScope, MemoryBudget};
use rtdb_types::java_text::{JavaString, JavaText};

#[test]
fn utf8_and_utf16_backings_preserve_equal_units_and_java_hashes() {
    let memory = MemoryBudget::new(1 << 20);
    for (source, expected_hash) in [("", 0), ("abc", 96_354), ("\0", 0), ("é", 233), ("😀", 1_772_899)] {
        let units = source.encode_utf16().collect::<Vec<_>>();
        let utf8 = JavaString::copy_utf8(source, &memory).unwrap();
        let utf16 = JavaString::copy_utf16(&units, &memory).unwrap();
        assert_eq!(utf8, utf16);
        assert_eq!(utf8.view().units().collect::<Vec<_>>(), units);
        assert_eq!(utf16.view().units().collect::<Vec<_>>(), units);
        assert_eq!(utf8.view().java_hash_code(), expected_hash);
        assert_eq!(utf16.view().java_hash_code(), expected_hash);
        assert_eq!(utf8.view().utf8_source(), Some(source));
        assert_eq!(utf16.view().utf8_source(), None);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn unmatched_units_survive_copy_clone_slice_equality_and_debug() {
    let source = [0xd800, 0xdc00, 0xd800, 0xd800, 0x61, 0xdc00, 0xdc00, 0xffff];
    let memory = MemoryBudget::new(4096);
    let value = JavaString::copy_utf16(&source, &memory).unwrap();
    let cloned = value.clone();
    assert_eq!(value, cloned);
    assert_eq!(cloned.view().units().collect::<Vec<_>>(), source);
    assert_eq!(value.view().slice_units(2..7).unwrap().units().collect::<Vec<_>>(), source[2..7]);
    assert_eq!(format!("{value:?}"), r#"JavaString("\ud800\udc00\ud800\ud800\u0061\udc00\udc00\uffff")"#);
    for unit in [0xd800, 0xdc00] {
        let raw = JavaString::copy_utf16(&[unit], &memory).unwrap();
        assert_ne!(raw, JavaString::copy_utf8("?", &memory).unwrap());
        assert_ne!(raw, JavaString::copy_utf8("�", &memory).unwrap());
    }
    assert_ne!(JavaString::copy_utf16(&[0xd800], &memory).unwrap(), JavaString::copy_utf16(&[0xdc00], &memory).unwrap());
}

#[test]
fn utf8_unit_ranges_can_split_pairs_and_nested_ranges_are_relative() {
    let value = JavaText::from_utf8("a😀z");
    assert_eq!(value.len_units(), 4);
    assert_eq!(value.code_points().collect::<Vec<_>>(), [0x61, 0x1f600, 0x7a]);
    let pair = value.slice_units(1..3).unwrap();
    assert_eq!(pair.utf8_source(), Some("😀"));
    assert_eq!(pair.units().collect::<Vec<_>>(), [0xd83d, 0xde00]);
    let high = pair.slice_units(0..1).unwrap();
    let low = pair.slice_units(1..2).unwrap();
    assert_eq!(high.units().collect::<Vec<_>>(), [0xd83d]);
    assert_eq!(low.units().collect::<Vec<_>>(), [0xde00]);
    assert_eq!(high.utf8_source(), None);
    assert_eq!(low.utf8_source(), None);
    assert_eq!(high.code_points().collect::<Vec<_>>(), [0xd83d]);
    assert_eq!(low.code_points().collect::<Vec<_>>(), [0xde00]);
    assert_eq!(high.java_utf8_bytes().collect::<Vec<_>>(), b"?");
    assert_eq!(low.java_utf8_bytes().collect::<Vec<_>>(), b"?");
    assert_eq!(pair.java_utf8_bytes().collect::<Vec<_>>(), "😀".as_bytes());
    assert_eq!(value.slice_units(3..4).unwrap().utf8_source(), Some("z"));
}

#[test]
fn ranges_validate_relative_bounds_and_empty_pair_interiors_remain_exact() {
    for value in [JavaText::from_utf8("😀"), JavaText::from_utf16(&[0xd83d, 0xde00])] {
        assert!(value.slice_units(2..1).is_none());
        assert!(value.slice_units(0..3).is_none());
        assert!(value.slice_units(usize::MAX..usize::MAX).is_none());
        for index in 0..=2 {
            let empty = value.slice_units(index..index).unwrap();
            assert!(empty.is_empty());
            assert_eq!(empty.len_units(), 0);
            assert_eq!(empty.units().next(), None);
            assert_eq!(empty.code_points().next(), None);
            assert_eq!(empty.java_utf8_bytes().next(), None);
            assert_eq!(empty.java_utf8_len().unwrap(), 0);
            assert_eq!(empty.java_hash_code(), 0);
            assert!(empty.slice_units(0..1).is_none());
        }
    }
    let utf8 = JavaText::from_utf8("😀");
    assert_eq!(utf8.slice_units(0..0).unwrap().utf8_source(), Some(""));
    assert_eq!(utf8.slice_units(1..1).unwrap().utf8_source(), None);
    assert_eq!(utf8.slice_units(2..2).unwrap().utf8_source(), Some(""));
}

#[test]
fn code_points_combine_only_adjacent_pairs_within_the_selected_range() {
    let source = [0xd800, 0xd800, 0xdc00, 0xdc00, 0xdbff, 0xdfff];
    let value = JavaText::from_utf16(&source);
    assert_eq!(value.code_points().collect::<Vec<_>>(), [0xd800, 0x10000, 0xdc00, 0x10ffff]);
    assert_eq!(value.slice_units(0..2).unwrap().code_points().collect::<Vec<_>>(), [0xd800, 0xd800]);
    assert_eq!(value.slice_units(2..6).unwrap().code_points().collect::<Vec<_>>(), [0xdc00, 0xdc00, 0x10ffff]);
}

#[test]
fn java_utf8_encoding_replaces_only_unmatched_units() {
    for (units, bytes) in [
        (&[][..], &b""[..]),
        (&[0xd800], &b"?"[..]),
        (&[0xdc00], &b"?"[..]),
        (&[0xd800, 0xd800], &b"??"[..]),
        (&[0xdc00, 0xdc00], &b"??"[..]),
        (&[0xdc00, 0xd800], &b"??"[..]),
        (&[0xd800, 0xdc00], &[0xf0, 0x90, 0x80, 0x80][..]),
        (&[0xd800, 0xd800, 0xdc00], &[b'?', 0xf0, 0x90, 0x80, 0x80][..]),
        (&[0, 0x7f, 0x80, 0x7ff, 0x800, 0xffff], &[0, 0x7f, 0xc2, 0x80, 0xdf, 0xbf, 0xe0, 0xa0, 0x80, 0xef, 0xbf, 0xbf][..]),
        (&[0x3f, 0xfffd, 0xdbff, 0xdfff], &[b'?', 0xef, 0xbf, 0xbd, 0xf4, 0x8f, 0xbf, 0xbf][..]),
    ] {
        let value = JavaText::from_utf16(units);
        assert_eq!(value.java_utf8_bytes().collect::<Vec<_>>(), bytes);
        assert_eq!(value.java_utf8_len().unwrap(), bytes.len());
    }
}

#[test]
fn iterator_clones_keep_pending_units_points_and_utf8_bytes() {
    let mut units = JavaText::from_utf8("😀x").units();
    assert_eq!(units.len(), 3);
    assert_eq!(units.next(), Some(0xd83d));
    assert_eq!(units.len(), 2);
    assert_eq!(units.clone().collect::<Vec<_>>(), [0xde00, 0x78]);
    assert_eq!(units.collect::<Vec<_>>(), [0xde00, 0x78]);
    let mut points = JavaText::from_utf16(&[0xd800, 0x61, 0xd83d, 0xde00]).code_points();
    assert_eq!(points.next(), Some(0xd800));
    assert_eq!(points.clone().collect::<Vec<_>>(), [0x61, 0x1f600]);
    assert_eq!(points.collect::<Vec<_>>(), [0x61, 0x1f600]);
    let mut bytes = JavaText::from_utf8("😀x").java_utf8_bytes();
    assert_eq!(bytes.next(), Some(0xf0));
    assert_eq!(bytes.clone().collect::<Vec<_>>(), [0x9f, 0x98, 0x80, b'x']);
    assert_eq!(bytes.by_ref().collect::<Vec<_>>(), [0x9f, 0x98, 0x80, b'x']);
    assert_eq!(bytes.next(), None);
    assert_eq!(bytes.next(), None);
}

#[test]
fn iterator_size_hints_cover_remaining_output_after_surrogate_lookahead() {
    let source = [0xd800, 0x61, 0xd83d, 0xde00, 0xdc00];
    let mut points = JavaText::from_utf16(&source).code_points();
    let mut bytes = JavaText::from_utf16(&source).java_utf8_bytes();
    loop {
        let remaining = points.clone().count();
        let (lower, upper) = points.size_hint();
        assert!(lower <= remaining);
        assert!(remaining <= upper.unwrap());
        if points.next().is_none() { break; }
    }
    loop {
        let remaining = bytes.clone().count();
        let (lower, upper) = bytes.size_hint();
        assert!(lower <= remaining);
        assert!(remaining <= upper.unwrap());
        if bytes.next().is_none() { break; }
    }
    assert_eq!(points.size_hint(), (0, Some(0)));
    assert_eq!(bytes.size_hint(), (0, Some(0)));
}

#[test]
fn exact_advertised_budgets_cover_both_backings_and_reject_one_byte_less() {
    for source in ["", "plain", "a😀z"] {
        let bound = JavaString::allocation_bound_utf8(source).unwrap();
        let denied = MemoryBudget::new(bound - 1);
        assert_eq!(JavaString::copy_utf8(source, &denied).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert_eq!(denied.used_bytes(), 0);
        let memory = MemoryBudget::new(bound);
        let value = JavaString::copy_utf8(source, &memory).unwrap();
        assert_eq!(value.retained_bytes(), bound);
        assert_eq!(memory.used_bytes(), bound);
        drop(value);
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(memory.peak_bytes(), bound);
    }
    for source in [&[][..], &[0xd800][..], &[0x61, 0xd83d, 0xde00][..]] {
        let bound = JavaString::allocation_bound_utf16(source).unwrap();
        let denied = MemoryBudget::new(bound - 1);
        assert_eq!(JavaString::copy_utf16(source, &denied).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert_eq!(denied.used_bytes(), 0);
        let memory = MemoryBudget::new(bound);
        let value = JavaString::copy_utf16(source, &memory).unwrap();
        assert_eq!(value.retained_bytes(), bound);
        assert_eq!(memory.used_bytes(), bound);
        drop(value);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn failed_copies_preserve_existing_charges_and_clones_acquire_none() {
    let bound = JavaString::allocation_bound_utf8("retained").unwrap();
    let memory = MemoryBudget::new(bound);
    let first = JavaString::copy_utf8("retained", &memory).unwrap();
    let second = first.clone();
    assert!(JavaString::copy_utf8("", &memory).is_err());
    assert!(JavaString::copy_utf16(&[], &memory).is_err());
    assert_eq!(memory.used_bytes(), bound);
    assert_eq!(memory.peak_bytes(), bound);
    assert_eq!(first, second);
    assert_eq!(second.view().len_units(), 8);
    assert_eq!(second.view().java_utf8_bytes().count(), 8);
    drop(first);
    assert_eq!(memory.used_bytes(), bound);
    drop(second);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn retained_leases_outlive_local_scopes_and_budget_handles() {
    let root = MemoryBudget::new(1 << 20);
    let child = root.scope();
    let weak_child = std::sync::Arc::downgrade(&child);
    let value = {
        let scope = AllocationScope::new(&child).unwrap();
        scope.reserve(32).unwrap();
        JavaString::copy_utf16(&[0xd800, 0x61], &child).unwrap()
    };
    let bound = value.retained_bytes();
    assert_eq!(child.used_bytes(), bound);
    assert_eq!(root.used_bytes(), bound);
    let cloned = value.clone();
    drop(child);
    drop(value);
    assert_eq!(weak_child.upgrade().unwrap().used_bytes(), bound);
    assert_eq!(cloned.view().units().collect::<Vec<_>>(), [0xd800, 0x61]);
    drop(cloned);
    assert!(weak_child.upgrade().is_none());
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn ancestor_admission_failures_leave_no_retained_charge() {
    let bound = JavaString::allocation_bound_utf16(&[0xd800]).unwrap();
    let root = MemoryBudget::new(bound - 1);
    let child = root.scope();
    assert!(JavaString::copy_utf16(&[0xd800], &child).is_err());
    assert_eq!(child.used_bytes(), 0);
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn explicit_untracked_values_and_borrowed_views_do_not_need_a_budget() {
    let zero = MemoryBudget::new(0);
    let utf8 = JavaString::copy_utf8_untracked("a😀").unwrap();
    let utf16 = JavaString::copy_utf16_untracked(&[0x61, 0xd83d, 0xde00]).unwrap();
    assert_eq!(utf8, utf16);
    assert!(JavaString::copy_utf8("", &zero).is_err());
    assert!(JavaString::copy_utf16(&[], &zero).is_err());
    assert_eq!(JavaText::from_utf16(&[0xd800]).java_utf8_bytes().next(), Some(b'?'));
    assert_eq!(zero.used_bytes(), 0);
    assert!(utf8.retained_bytes() > 0);
    assert!(utf16.retained_bytes() > 0);
}

#[test]
fn scope_budget_builds_an_independently_retained_java_string() {
    let memory = MemoryBudget::new(8192);
    let value = {
        let scope = AllocationScope::new(&memory).unwrap();
        assert!(std::sync::Arc::ptr_eq(scope.memory_budget(), &memory));
        JavaString::copy_utf16(&[0xd800, 0x61], scope.memory_budget()).unwrap()
    };
    assert_eq!(memory.used_bytes(), value.retained_bytes());
    assert_eq!(value.view().units().collect::<Vec<_>>(), [0xd800, 0x61]);
    drop(value);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn scope_budget_does_not_transfer_or_bypass_existing_charges() {
    let bound = JavaString::allocation_bound_utf16(&[0xd800]).unwrap();
    let memory = MemoryBudget::new(4096 + bound - 1);
    let scope = AllocationScope::new(&memory).unwrap();
    assert_eq!(JavaString::copy_utf16(&[0xd800], scope.memory_budget()).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 4096);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn admitted_utf16_copy_uses_the_supplied_authority_once_and_retains_its_full_lease() {
    let source = [0xd800, 0x61, 0xdc00];
    let bound = JavaString::allocation_bound_utf16(&source).unwrap();
    for extra in [0, 37] {
        let memory = MemoryBudget::new(bound + extra);
        let weak = std::sync::Arc::downgrade(&memory);
        let mut calls = 0;
        let value = JavaString::copy_utf16_with_admission(&source, |bytes| {
            calls += 1;
            assert_eq!(bytes, bound);
            assert_eq!(memory.used_bytes(), 0);
            memory.reserve(bytes + extra)
        }).unwrap();
        assert_eq!(calls, 1);
        assert_eq!(value.view().units().collect::<Vec<_>>(), source);
        assert_eq!(value.retained_bytes(), bound + extra);
        assert_eq!(memory.used_bytes(), bound + extra);
        let clone = value.clone();
        drop(value);
        assert_eq!(memory.used_bytes(), bound + extra);
        drop(memory);
        assert_eq!(weak.upgrade().unwrap().used_bytes(), bound + extra);
        assert_eq!(clone.view().units().collect::<Vec<_>>(), source);
        drop(clone);
        assert!(weak.upgrade().is_none());
    }
}

#[test]
fn admitted_utf16_copy_preserves_authority_failure_before_copying_storage() {
    let source = [0xd800, 0xdc00];
    let mut calls = 0;
    let cause = JavaString::copy_utf16_with_admission(&source, |bytes| {
        calls += 1;
        assert_eq!(bytes, JavaString::allocation_bound_utf16(&source).unwrap());
        Err(rtdb_types::Anomaly::incorrect("fixture/text-admission-denied", "denied by caller"))
    }).unwrap_err();
    assert_eq!(calls, 1);
    assert_eq!(cause.code, "fixture/text-admission-denied");
    assert_eq!(cause.message.as_deref(), Some("denied by caller"));
    assert_eq!(source, [0xd800, 0xdc00]);
}

#[test]
fn admitted_utf16_copy_rejects_insufficient_leases_and_releases_the_returned_owner() {
    for source in [&[][..], &[0xd800, 0x61][..]] {
        let bound = JavaString::allocation_bound_utf16(source).unwrap();
        let memory = MemoryBudget::new(bound);
        let mut calls = 0;
        let cause = JavaString::copy_utf16_with_admission(source, |bytes| {
            calls += 1;
            assert_eq!(bytes, bound);
            memory.reserve(bytes - 1)
        }).unwrap_err();
        assert_eq!(calls, 1);
        assert_eq!(cause.code, "rtdb.types/java-text-admission");
        assert_eq!(memory.used_bytes(), 0);
    }
}
