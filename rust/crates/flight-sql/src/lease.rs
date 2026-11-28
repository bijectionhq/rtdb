use std::{collections::BTreeMap, sync::{Arc, Mutex}, time::Duration};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::Clock;
use rtdb_types::{Anomaly, Result, Timestamp};

pub(crate) trait Leased: Send + Sync {
    fn close(&self);
    fn hard_deadline(&self) -> Option<tokio::time::Instant> { None }
    fn retain_close_on_expiry(&self) -> bool { false }
    fn closed_reads_pending(&self) -> bool { false }
}
struct Live<V> { value: Arc<V>, since: Timestamp, limit: Duration, idle: Duration, users: usize, _reservation: Arc<MemoryReservation> }
struct PendingClose<V> { value: Arc<V>, drained_at: Option<Timestamp> }
enum Entry<V> {
    Live(Live<V>),
    Expired { at: Timestamp, after: Duration, close: Option<PendingClose<V>>, _reservation: Arc<MemoryReservation> },
}
pub(crate) enum Lookup<V> { Found(Arc<V>), Expired(Duration), Unknown }
/// Leased handles by key; `live` counts the live entries, kept with every transition so the status count is not a scan.
pub(crate) struct Registry<K, V> { entries: Mutex<BTreeMap<K, Entry<V>>>, live: std::sync::atomic::AtomicUsize, clock: Arc<dyn Clock>, memory: Arc<MemoryBudget> }
impl<K: Ord + Clone, V: Leased> Registry<K, V> {
    pub fn new(clock: Arc<dyn Clock>, memory: Arc<MemoryBudget>) -> Self { Self { entries: Mutex::new(BTreeMap::new()), live: Default::default(), clock, memory } }
    pub fn insert(&self, key: K, value: Arc<V>, idle: Duration, first: Duration, acquired: bool, key_bytes: usize) -> Result<()> {
        let reservation = self.memory.reserve(key_bytes.checked_add(256).ok_or_else(|| Anomaly::incorrect("rtdb.flight-sql/lease-size", "Lease identity exceeds addressable memory").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded))?)?;
        let mut entries = self.entries.lock().expect("lease lock");
        if entries.contains_key(&key) { return Err(Anomaly::fault("rtdb.flight-sql/duplicate-handle", "Generated Flight handle already exists")); }
        entries.insert(key, Entry::Live(Live { value, since: self.clock.now(), limit: first, idle, users: usize::from(acquired), _reservation: reservation }));
        self.live.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }
    pub fn lookup(&self, key: &K, acquire: bool) -> Lookup<V> {
        self.lookup_at(key, acquire, false)
    }
    pub fn contains(&self, key: &K) -> bool {
        self.entries.lock().expect("lease lock").contains_key(key)
    }
    pub fn lookup_for_close(&self, key: &K) -> Lookup<V> {
        self.lookup_at(key, true, true)
    }
    fn lookup_at(&self, key: &K, acquire: bool, close: bool) -> Lookup<V> {
        let now = self.clock.now();
        let mut entries = self.entries.lock().expect("lease lock");
        let Some(entry) = entries.get_mut(key) else { return Lookup::Unknown; };
        self.expire(entry, now);
        match entry {
            Entry::Live(live) => { live.since = now; live.limit = live.idle; live.users += usize::from(acquire); Lookup::Found(Arc::clone(&live.value)) }
            Entry::Expired { close: Some(retained), .. } if close => Lookup::Found(Arc::clone(&retained.value)),
            Entry::Expired { after, .. } => Lookup::Expired(*after),
        }
    }
    pub fn release(&self, key: &K) {
        if let Some(Entry::Live(live)) = self.entries.lock().expect("lease lock").get_mut(key) {
            assert!(live.users != 0); live.users -= 1; live.since = self.clock.now();
        }
    }
    pub fn take(&self, key: &K) -> Lookup<V> {
        let mut entries = self.entries.lock().expect("lease lock");
        let Some(entry) = entries.get_mut(key) else { return Lookup::Unknown; };
        self.expire(entry, self.clock.now());
        match entry {
            Entry::Expired { after, .. } => Lookup::Expired(*after),
            Entry::Live(_) => {
                self.live.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                match entries.remove(key).expect("present lease") { Entry::Live(live) => Lookup::Found(live.value), Entry::Expired { .. } => unreachable!() }
            }
        }
    }
    /// Retire a credential's live registration while preserving an enrolled
    /// receiver's close-only identity.
    pub fn retire(&self, key: &K) {
        let mut entries = self.entries.lock().expect("lease lock");
        let Some(entry) = entries.get_mut(key) else { return; };
        let Entry::Live(live) = entry else { return; };
        if live.value.retain_close_on_expiry() {
            *entry = expired(live, self.clock.now());
        } else if let Some(Entry::Live(live)) = entries.remove(key) {
            live.value.close();
        }
        self.live.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
    pub fn sweep(&self) {
        let now = self.clock.now();
        self.entries.lock().expect("lease lock").retain(|_, entry| {
            self.expire(entry, now);
            match entry {
                Entry::Live(_) => true,
                Entry::Expired { after, close: Some(retained), .. } => {
                    if retained.value.closed_reads_pending() { return true; }
                    let drained = *retained.drained_at.get_or_insert(now);
                    elapsed(drained, now) < *after
                }
                Entry::Expired { at, after, .. } => elapsed(*at, now) < *after,
            }
        });
    }
    pub fn close(&self) {
        let entries = std::mem::take(&mut *self.entries.lock().expect("lease lock"));
        self.live.store(0, std::sync::atomic::Ordering::Release);
        for entry in entries.into_values() { if let Entry::Live(live) = entry { live.value.close(); } }
    }
    pub fn close_where(&self, predicate: impl Fn(&V) -> bool) {
        let mut closed = Vec::new();
        {
            let mut entries = self.entries.lock().expect("lease lock");
            entries.retain(|_, entry| {
                if let Entry::Live(live) = entry && predicate(&live.value) {
                    closed.push(Arc::clone(&live.value));
                    false
                } else {
                    true
                }
            });
            self.live.fetch_sub(closed.len(), std::sync::atomic::Ordering::AcqRel);
        }
        for value in closed { value.close(); }
    }
    pub fn len(&self) -> usize { self.live.load(std::sync::atomic::Ordering::Acquire) }
    fn expire(&self, entry: &mut Entry<V>, now: Timestamp) {
        if expire(entry, now) { self.live.fetch_sub(1, std::sync::atomic::Ordering::AcqRel); }
    }
}
fn elapsed(since: Timestamp, now: Timestamp) -> Duration { Duration::from_micros(now.0.saturating_sub(since.0).max(0) as u64) }
/// Expires a live entry at its absolute ceiling or its idle limit.
fn expire<V: Leased>(entry: &mut Entry<V>, now: Timestamp) -> bool {
    if let Entry::Live(live) = entry {
        let ceiling_elapsed = live.value.hard_deadline().is_some_and(|deadline| tokio::time::Instant::now() >= deadline);
        if ceiling_elapsed || live.users == 0 && elapsed(live.since, now) >= live.limit {
            *entry = expired(live, now);
            return true;
        }
    }
    false
}

fn expired<V: Leased>(live: &Live<V>, now: Timestamp) -> Entry<V> {
    live.value.close();
    let close = live.value.retain_close_on_expiry().then(|| PendingClose {
        value: Arc::clone(&live.value),
        drained_at: (!live.value.closed_reads_pending()).then_some(now),
    });
    Entry::Expired { at: now, after: live.limit, close, _reservation: Arc::clone(&live._reservation) }
}

pub(crate) fn duration(value: Duration) -> String {
    let seconds = value.as_secs(); let hours = seconds / 3600; let minutes = seconds % 3600 / 60; let seconds = seconds % 60;
    let mut output = "PT".to_owned();
    if hours != 0 { output.push_str(&format!("{hours}H")); }
    if minutes != 0 { output.push_str(&format!("{minutes}M")); }
    if seconds != 0 || value.subsec_nanos() != 0 || output == "PT" {
        output.push_str(&seconds.to_string());
        if value.subsec_nanos() != 0 { output.push('.'); output.push_str(format!("{:09}", value.subsec_nanos()).trim_end_matches('0')); }
        output.push('S');
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
    struct TestClock(AtomicI64);
    impl Clock for TestClock { fn now(&self) -> Timestamp { Timestamp(self.0.load(Ordering::Relaxed)) } }
    struct Resource(AtomicUsize);
    impl Leased for Resource { fn close(&self) { self.0.fetch_add(1, Ordering::Relaxed); } }
    #[test]
    fn an_expired_hard_deadline_closes_even_an_acquired_resource() {
        struct Expired(Resource, tokio::time::Instant);
        impl Leased for Expired {
            fn close(&self) { self.0.close(); }
            fn hard_deadline(&self) -> Option<tokio::time::Instant> { Some(self.1) }
        }
        let registry = Registry::new(Arc::new(TestClock(AtomicI64::new(0))), MemoryBudget::new(4096));
        let resource = Arc::new(Expired(Resource(AtomicUsize::new(0)), tokio::time::Instant::now()));
        registry.insert(1, resource.clone(), Duration::from_secs(30), Duration::from_secs(1), true, 8).unwrap();
        assert!(matches!(registry.lookup(&1, false), Lookup::Expired(_)));
        registry.release(&1);
        assert_eq!(registry.len(), 0);
        assert_eq!(resource.0.0.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn acquired_session_survives_idle_deadline_and_release_restarts_first_limit() {
        let clock = Arc::new(TestClock(AtomicI64::new(0))); let memory = MemoryBudget::new(4096);
        let registry = Registry::new(clock.clone(), memory.clone()); let resource = Arc::new(Resource(AtomicUsize::new(0)));
        registry.insert(1, resource.clone(), Duration::from_secs(30), Duration::from_secs(1), true, 8).unwrap();
        clock.0.store(5_000_000, Ordering::Relaxed); registry.sweep(); assert_eq!(resource.0.load(Ordering::Relaxed), 0);
        registry.release(&1); clock.0.store(6_000_000, Ordering::Relaxed);
        assert!(matches!(registry.lookup(&1, false), Lookup::Expired(after) if after == Duration::from_secs(1)));
        assert_eq!(resource.0.load(Ordering::Relaxed), 1); registry.sweep(); assert!(memory.used_bytes() > 0);
        clock.0.store(7_000_000, Ordering::Relaxed); registry.sweep(); assert_eq!(memory.used_bytes(), 0); assert!(matches!(registry.lookup(&1, false), Lookup::Unknown));
    }
    #[test]
    fn expired_or_retired_store_session_remains_close_only_after_its_last_reply_buffer() {
        struct StoreSession { closed: AtomicBool, pending: AtomicBool }
        impl Leased for StoreSession {
            fn close(&self) { self.closed.store(true, Ordering::Release); }
            fn retain_close_on_expiry(&self) -> bool { true }
            fn closed_reads_pending(&self) -> bool { self.pending.load(Ordering::Acquire) }
        }
        for retire in [false, true] {
            let clock = Arc::new(TestClock(AtomicI64::new(0)));
            let registry = Registry::new(clock.clone(), MemoryBudget::new(4096));
            let original = Arc::new(StoreSession { closed: AtomicBool::new(false), pending: AtomicBool::new(true) });
            let idle = Duration::from_secs(1);
            registry.insert(1, original.clone(), idle, idle, retire, 8).unwrap();
            if retire { registry.retire(&1); }
            clock.0.store(1_000_000, Ordering::Relaxed);
            registry.sweep();
            clock.0.store(3_000_000, Ordering::Relaxed);
            registry.sweep();
            assert_eq!(registry.len(), 0);
            assert!(original.closed.load(Ordering::Acquire));
            assert!(matches!(registry.lookup(&1, true), Lookup::Expired(_)));
            assert!(matches!(registry.lookup_for_close(&1), Lookup::Found(value) if Arc::ptr_eq(&value, &original)));
            assert!(registry.contains(&1));
            original.pending.store(false, Ordering::Release);
            registry.sweep();
            assert!(matches!(registry.lookup_for_close(&1), Lookup::Found(_)));
            clock.0.store(4_000_000, Ordering::Relaxed);
            registry.sweep();
            assert!(matches!(registry.lookup_for_close(&1), Lookup::Unknown));
            assert!(!registry.contains(&1));
        }
    }

    #[test]
    fn presenting_session_switches_to_idle_limit_and_nested_calls_each_release_once() {
        let clock = Arc::new(TestClock(AtomicI64::new(0))); let registry = Registry::new(clock.clone(), MemoryBudget::new(4096));
        registry.insert(1, Arc::new(Resource(AtomicUsize::new(0))), Duration::from_secs(30), Duration::from_secs(1), false, 8).unwrap();
        clock.0.store(999_999, Ordering::Relaxed); assert!(matches!(registry.lookup(&1, true), Lookup::Found(_)));
        assert!(matches!(registry.lookup(&1, true), Lookup::Found(_))); registry.release(&1);
        clock.0.store(100_000_000, Ordering::Relaxed); registry.sweep(); assert_eq!(registry.len(), 1); registry.release(&1);
        clock.0.store(130_000_000, Ordering::Relaxed); assert!(matches!(registry.take(&1), Lookup::Expired(after) if after == Duration::from_secs(30)));
    }
    #[test]
    fn ended_handle_transfers_close_to_claimant_and_does_not_keep_tombstone() {
        let clock = Arc::new(TestClock(AtomicI64::new(0))); let memory = MemoryBudget::new(4096); let registry = Registry::new(clock, memory.clone());
        let resource = Arc::new(Resource(AtomicUsize::new(0))); registry.insert(1, resource.clone(), Duration::from_secs(1), Duration::from_secs(1), false, 8).unwrap();
        let Lookup::Found(claimed) = registry.take(&1) else { panic!("live resource"); };
        assert_eq!(resource.0.load(Ordering::Relaxed), 0); assert_eq!(memory.used_bytes(), 0); claimed.close(); assert!(matches!(registry.take(&1), Lookup::Unknown));
        assert_eq!(resource.0.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn idle_duration_uses_java_iso_format() {
        assert_eq!(duration(Duration::from_secs(1800)), "PT30M"); assert_eq!(duration(Duration::from_millis(1250)), "PT1.25S"); assert_eq!(duration(Duration::from_secs(3661)), "PT1H1M1S");
    }
}
