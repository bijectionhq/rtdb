use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use rtdb_types::{Anomaly, Result};

const SEEDER_INCREMENT: u64 = 0xbb67_ae85_84ca_a73b;
const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
const DOUBLE_UNIT: f64 = 1.0 / (1u64 << 53) as f64;

fn mix_product(value: u64) -> u64 {
    let first = (value ^ (value >> 33)).wrapping_mul(0xff51_afd7_ed55_8ccd);
    (first ^ (first >> 33)).wrapping_mul(0xc4ce_b9fe_1a85_ec53)
}

fn mix_murmur(value: u64) -> u64 {
    let product = mix_product(value);
    product ^ (product >> 33)
}

/// Owns OpenJDK 25's atomic ThreadLocalRandom seeder for one class history.
pub struct JavaThreadLocalSeeds {
    seeder: AtomicU64,
}

impl JavaThreadLocalSeeds {
    /// Constructs the class seeder from the original wall milliseconds, then monotonic nanoseconds.
    /// This is the source path with `java.util.secureRandomSeed` disabled.
    /// A wall observation failure stops before the nanosecond observation.
    pub fn from_class_clocks(
        wall_ms: impl FnOnce() -> Result<i64>, monotonic_ns: impl FnOnce() -> Result<i64>,
    ) -> Result<Self> {
        let wall = mix_murmur(wall_ms()? as u64);
        let monotonic = mix_murmur(monotonic_ns()? as u64);
        Ok(Self::from_seeder((wall ^ monotonic) as i64))
    }

    /// Continues from the observed signed class seeder after any secure override and prior seed demands.
    pub const fn from_seeder(current: i64) -> Self {
        Self { seeder: AtomicU64::new(current as u64) }
    }

    /// Consumes one localInit seed demand from a platform thread whose source probe is zero.
    /// The caller owns the Java probe state and thread registration.
    /// Virtual-thread carrier initialization requires the corresponding observed seed history.
    pub fn initialize_platform_thread(&self, thread_id: i64) -> JavaThreadLocalRandom {
        let old = self.seeder.fetch_add(SEEDER_INCREMENT, SeqCst);
        JavaThreadLocalRandom::from_initialized_thread(thread_id, mix_murmur(old) as i64)
    }
}

/// Owns the mutable seed for one initialized OpenJDK 25 thread exercising Random.nextDouble.
pub struct JavaThreadLocalRandom {
    seed: u64,
    increment: u64,
}

impl JavaThreadLocalRandom {
    /// Restores the exact signed thread ID and nextSeed input from a qualified thread history.
    pub const fn from_initialized_thread(thread_id: i64, seed: i64) -> Self {
        Self { seed: seed as u64, increment: (thread_id as u64).wrapping_shl(1).wrapping_add(GOLDEN_GAMMA) }
    }

    fn next_bits(&mut self, bits: u32) -> u64 {
        self.seed = self.seed.wrapping_add(self.increment);
        mix_product(self.seed) >> (64 - bits)
    }

    /// Draws a bounded double using the original two source seed advances.
    /// Requires finite bounds with origin strictly below bound and rejects other ranges before drawing.
    /// The result includes origin and excludes bound.
    pub fn next_double(&mut self, origin: f64, bound: f64) -> Result<f64> {
        if !(f64::NEG_INFINITY < origin && origin < bound && bound < f64::INFINITY) {
            return Err(Anomaly::incorrect("rtdb/kafka-java-thread-random-range", "Invalid Java ThreadLocalRandom range"));
        }
        let bits = (self.next_bits(26) << 27) + self.next_bits(27);
        let unit = bits as f64 * DOUBLE_UNIT;
        let width = bound - origin;
        let scaled = if width < f64::INFINITY {
            unit * width + origin
        } else {
            let half_origin = 0.5 * origin;
            (unit * (0.5 * bound - half_origin) + half_origin) * 2.0
        };
        Ok(if scaled >= bound { bound.next_down() } else { scaled })
    }
}

#[cfg(test)]
#[path = "jitter/tests.rs"]
mod tests;
