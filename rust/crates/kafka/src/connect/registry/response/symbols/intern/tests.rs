use rtdb_types::MemoryBudget;
use super::*;

fn candidate(units: &[u16], context: &mut ConversionContext) -> Arc<SymbolName> {
    SymbolName::copy(units, context).unwrap()
}

#[test]
fn duplicate_names_share_the_canonical_owner_across_factories() {
    let budget = MemoryBudget::new(1 << 20);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let mut first = InternCache::open(Profile::Jackson221, &mut context).unwrap();
    let mut second = first.clone();
    let name = first.intern(candidate(&[0xd800, 0x61], &mut context), &mut context).unwrap();
    let duplicate = second.intern(candidate(&[0xd800, 0x61], &mut context), &mut context).unwrap();
    let different = second.intern(candidate(&[0xfffd, 0x61], &mut context), &mut context).unwrap();
    assert!(Arc::ptr_eq(&name, &duplicate));
    assert!(!Arc::ptr_eq(&name, &different));
    drop((first, second, context, name, duplicate, different));
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn a_full_cache_clears_on_a_miss_and_live_symbol_tables_still_intern() {
    for (profile, capacity) in [(Profile::Jackson216, 180u16), (Profile::Jackson221, 280)] {
        let budget = MemoryBudget::new(8 << 20);
        let mut context = ConversionContext::new(Arc::clone(&budget));
        let mut cache = InternCache::open(profile, &mut context).unwrap();
        let first = cache.intern(candidate(&[0], &mut context), &mut context).unwrap();
        for unit in 1..capacity {
            cache.intern(candidate(&[unit], &mut context), &mut context).unwrap();
        }
        cache.intern(candidate(&[1], &mut context), &mut context).unwrap();
        assert_eq!(cache.0.entries.lock().unwrap().strong.len(), usize::from(capacity));
        cache.intern(candidate(&[capacity], &mut context), &mut context).unwrap();
        {
            let entries = cache.0.entries.lock().unwrap();
            assert_eq!(entries.strong.len(), 1);
            assert_eq!(entries.weak.len(), 2);
        }
        let again = cache.intern(candidate(&[0], &mut context), &mut context).unwrap();
        assert!(Arc::ptr_eq(&first, &again));
    }
}

#[test]
fn an_index_admission_failure_leaves_the_candidate_unpublished() {
    let budget = MemoryBudget::new(1 << 20);
    let mut setup = ConversionContext::new(Arc::clone(&budget));
    let mut cache = InternCache::open(Profile::Jackson221, &mut setup).unwrap();
    let first = candidate(&[7], &mut setup);
    let mut limited = ConversionContext::with_limit(Arc::clone(&budget), cache.0.charge.bytes());
    assert!(cache.intern(Arc::clone(&first), &mut limited).is_err());
    let second = candidate(&[7], &mut setup);
    let result = cache.intern(Arc::clone(&second), &mut setup).unwrap();
    assert!(!Arc::ptr_eq(&first, &result));
    assert!(Arc::ptr_eq(&second, &result));
}
