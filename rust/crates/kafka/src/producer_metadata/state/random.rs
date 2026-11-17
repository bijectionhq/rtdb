use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use rtdb_types::{Anomaly, Result};

use super::{JavaShuffleObservations, ShuffleAdmission};

const MULTIPLIER: u64 = 0x5deece66d;
const ADDEND: u64 = 0xb;
const SEED_MASK: u64 = (1 << 48) - 1;
const INITIAL_UNIQUIFIER: u64 = 8682522807148012;
const UNIQUIFIER_MULTIPLIER: u64 = 1181783497276652981;

/// Owns the sequence of one stock OpenJDK Random instance.
/// Shared callers advance the seed atomically for each candidate, including rejected candidates.
pub struct JavaRandom {
    seed: AtomicU64,
}

impl JavaRandom {
    /// Initializes the sequence as with stock `Random(long seed)`.
    pub fn from_seed(seed: i64) -> Self {
        Self { seed: AtomicU64::new((seed as u64 ^ MULTIPLIER) & SEED_MASK) }
    }

    /// Returns the next bounded int in `0..bound`.
    /// A nonpositive bound returns an error before advancing the seed.
    pub fn next_int(&self, bound: i32) -> Result<i32> {
        if bound <= 0 {
            return Err(Anomaly::incorrect("rtdb/kafka-java-random-bound", "Java Random bound must be positive"));
        }
        let candidate = self.next_31();
        let maximum = bound - 1;
        if bound & maximum == 0 {
            return Ok(((i64::from(bound) * i64::from(candidate)) >> 31) as i32);
        }
        let mut candidate = candidate;
        loop {
            let remainder = candidate % bound;
            if (candidate - remainder).wrapping_add(maximum) >= 0 {
                return Ok(remainder);
            }
            candidate = self.next_31();
        }
    }

    fn next_31(&self) -> i32 {
        let mut old = self.seed.load(SeqCst);
        loop {
            let next = old.wrapping_mul(MULTIPLIER).wrapping_add(ADDEND) & SEED_MASK;
            match self.seed.compare_exchange(old, next, SeqCst, SeqCst) {
                Ok(_) => return (next >> 17) as i32,
                Err(current) => old = current,
            }
        }
    }
}

/// Owns the default-constructor seed uniquifier for one qualified OpenJDK Random class history.
pub struct JavaRandomSeeds {
    uniquifier: AtomicU64,
}

impl JavaRandomSeeds {
    /// Initializes the uniquifier at class initialization, before any default constructor calls.
    pub const fn new() -> Self {
        Self::from_uniquifier(INITIAL_UNIQUIFIER as i64)
    }

    /// Continues an observed history from its current signed seed-uniquifier value.
    pub const fn from_uniquifier(current: i64) -> Self {
        Self { uniquifier: AtomicU64::new(current as u64) }
    }

    /// Advances the uniquifier before demanding the constructor's signed `System.nanoTime()` observation.
    /// An unavailable observation returns its error and leaves that advance in the history.
    pub fn create(&self, nano_time: impl FnOnce() -> Result<i64>) -> Result<JavaRandom> {
        let mut old = self.uniquifier.load(SeqCst);
        loop {
            let next = old.wrapping_mul(UNIQUIFIER_MULTIPLIER);
            match self.uniquifier.compare_exchange(old, next, SeqCst, SeqCst) {
                Ok(_) => return Ok(JavaRandom::from_seed(next as i64 ^ nano_time()?)),
                Err(current) => old = current,
            }
        }
    }
}

/// Borrows the default Collections shuffle generator slot for serialized acquisition and draws.
/// The slot and seed owner must represent the same OpenJDK history.
pub struct JavaShuffleRandom<'a, F> {
    slot: &'a mut Option<JavaRandom>,
    seeds: &'a JavaRandomSeeds,
    nano_time: F,
}

impl<'a, F: FnMut() -> Result<i64>> JavaShuffleRandom<'a, F> {
    /// Invokes `nano_time` during `begin` only when the slot is empty.
    /// If it returns an error, the slot stays empty and the uniquifier advance is consumed.
    pub fn new(slot: &'a mut Option<JavaRandom>, seeds: &'a JavaRandomSeeds, nano_time: F) -> Self {
        Self { slot, seeds, nano_time }
    }
}

impl<F: FnMut() -> Result<i64>> JavaShuffleObservations for JavaShuffleRandom<'_, F> {
    fn begin(&mut self, _: &mut ShuffleAdmission<'_>) -> Result<()> {
        if self.slot.is_none() {
            *self.slot = Some(self.seeds.create(&mut self.nano_time)?);
        }
        Ok(())
    }

    fn next_int(&mut self, bound: i32, _: &mut ShuffleAdmission<'_>) -> Result<i32> {
        self.slot.as_ref().ok_or_else(|| Anomaly::incorrect(
            "rtdb/kafka-java-shuffle-uninitialized", "The Java shuffle generator must be acquired before drawing",
        ))?.next_int(bound)
    }
}
