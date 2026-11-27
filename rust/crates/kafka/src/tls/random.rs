use std::{collections::BTreeMap, fs::File, io::Read, sync::{Arc, Mutex, Weak}, time::{Duration, SystemTime}};

use openssl::sha::{sha1, sha256, Sha256};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;
use zeroize::{Zeroize, Zeroizing};

mod native;
pub(crate) use native::Context;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Algorithm { Native, Blocking, NonBlocking, Sha1, Drbg, Windows }

impl Algorithm {
    fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_uppercase().as_str() {
            "NATIVEPRNG" => Ok(Self::Native),
            "NATIVEPRNGBLOCKING" => Ok(Self::Blocking),
            "NATIVEPRNGNONBLOCKING" => Ok(Self::NonBlocking),
            "SHA1PRNG" => Ok(Self::Sha1),
            "DRBG" => Ok(Self::Drbg),
            "WINDOWS-PRNG" if cfg!(windows) => Ok(Self::Windows),
            _ => Err(super::invalid("SecureRandom algorithm is unavailable")),
        }
    }
}

enum Random { Native(Arc<SharedNative>), Sha1(Sha1), Drbg(HashDrbg), Windows(Windows) }

impl Random {
    fn new(algorithm: Algorithm, budget: &Arc<MemoryBudget>) -> super::TlsConfigResult<Self> {
        match algorithm {
            Algorithm::Native | Algorithm::Blocking | Algorithm::NonBlocking => SharedNative::new(algorithm, budget).map(Self::Native),
            Algorithm::Sha1 => {
                let mut seed = Zeroizing::new([0; 20]);
                entropy(seed.as_mut()).map_err(super::TlsConfigError::from_invalid_anomaly)?;
                Ok(Self::Sha1(Sha1::new(seed.as_ref())))
            }
            Algorithm::Drbg => {
                Ok(Self::Drbg(HashDrbg::from_default_entropy(entropy, &DRBG_NONCE).map_err(super::TlsConfigError::from_invalid_anomaly)?))
            }
            Algorithm::Windows => Windows::new().map(Self::Windows).map_err(super::TlsConfigError::from_invalid_anomaly),
        }
    }

    fn fill(&mut self, output: &mut [u8]) -> Result<()> {
        if output.len() > i32::MAX as usize { return Err(super::invalid("SecureRandom request exceeds Java array bounds")); }
        match self {
            Self::Native(source) => source.random.lock().map_err(|_| super::invalid("SecureRandom state is unavailable"))?.fill(output),
            Self::Sha1(source) => { source.fill(output); Ok(()) }
            Self::Drbg(source) => {
                if source.counter > i32::MAX as u64 {
                    let mut seed = Zeroizing::new([0; 16]);
                    entropy(seed.as_mut())?;
                    source.reseed(seed.as_ref());
                }
                source.fill(output);
                Ok(())
            }
            Self::Windows(source) => source.fill(output),
        }
    }
}

fn entropy(output: &mut [u8]) -> Result<()> {
    #[cfg(unix)]
    if let Ok(mut file) = File::open("/dev/random") { return file.read_exact(output).map_err(super::invalid); }
    openssl::rand::rand_bytes(output).map_err(super::invalid)
}

struct Windows {
    #[cfg(windows)]
    handle: usize,
}

impl Windows {
    fn new() -> Result<Self> {
        #[cfg(windows)]
        {
            let mut handle = 0;
            if unsafe { CryptAcquireContextW(&mut handle, std::ptr::null(), std::ptr::null(), 1, 0xf0000000) } == 0 {
                return Err(super::invalid("Windows random provider is unavailable"));
            }
            Ok(Self { handle })
        }
        #[cfg(not(windows))]
        { Err(super::invalid("SecureRandom algorithm is unavailable")) }
    }

    fn fill(&mut self, output: &mut [u8]) -> Result<()> {
        #[cfg(windows)]
        {
            for chunk in output.chunks_mut(u32::MAX as usize) {
                if unsafe { CryptGenRandom(self.handle, chunk.len() as u32, chunk.as_mut_ptr()) } == 0 { return Err(super::invalid("Windows secure random generation failed")); }
            }
            Ok(())
        }
        #[cfg(not(windows))]
        { let _ = output; Err(super::invalid("SecureRandom algorithm is unavailable")) }
    }
}

impl Drop for Windows {
    fn drop(&mut self) {
        #[cfg(windows)]
        unsafe { CryptReleaseContext(self.handle, 0); }
    }
}

#[cfg(windows)]
#[link(name = "advapi32")]
unsafe extern "system" {
    fn CryptAcquireContextW(handle: *mut usize, container: *const u16, provider: *const u16, kind: u32, flags: u32) -> i32;
    fn CryptGenRandom(handle: usize, length: u32, output: *mut u8) -> i32;
    fn CryptReleaseContext(handle: usize, flags: u32) -> i32;
}

struct Native {
    _seed: File,
    next: File,
    mixer: Sha1,
    buffer: Zeroizing<Box<[u8]>>,
    size: usize,
    position: usize,
    changes: i32,
    refreshed: SystemTime,
    _charge: Arc<MemoryReservation>,
}

struct SharedNative { random: Mutex<Native>, registration: (usize, Algorithm) }

static NATIVE_SOURCES: Mutex<BTreeMap<(usize, Algorithm), Weak<SharedNative>>> = Mutex::new(BTreeMap::new());
const NATIVE_ALLOCATION_BOUND: usize = 73_728;

impl SharedNative {
    fn new(algorithm: Algorithm, budget: &Arc<MemoryBudget>) -> super::TlsConfigResult<Arc<Self>> {
        let registration = (Arc::as_ptr(budget) as usize, algorithm);
        let mut sources = NATIVE_SOURCES.lock().map_err(|_| super::TlsConfigError::invalid("SecureRandom source registry is unavailable"))?;
        if let Some(source) = sources.get(&registration).and_then(Weak::upgrade) {
            drop(sources);
            return Ok(source);
        }
        let random = Native::new(algorithm, budget.reserve(NATIVE_ALLOCATION_BOUND).map_err(super::TlsConfigError::Other)?)
            .map_err(super::TlsConfigError::from_invalid_anomaly)?;
        let source = Arc::new(Self { random: Mutex::new(random), registration });
        sources.insert(registration, Arc::downgrade(&source));
        drop(sources);
        Ok(source)
    }
}

impl Drop for SharedNative {
    fn drop(&mut self) {
        let mut sources = NATIVE_SOURCES.lock().unwrap_or_else(|error| error.into_inner());
        // A replacement can register after this Arc reaches zero but before Drop acquires the registry.
        if sources.get(&self.registration).is_some_and(|source| std::ptr::eq(source.as_ptr(), self)) {
            sources.remove(&self.registration);
        }
    }
}

impl Native {
    fn new(algorithm: Algorithm, charge: Arc<MemoryReservation>) -> Result<Self> {
        if !cfg!(unix) { return Err(super::invalid("SecureRandom algorithm is unavailable")); }
        let seed_path = if algorithm == Algorithm::NonBlocking { "/dev/urandom" } else { "/dev/random" };
        let next_path = if algorithm == Algorithm::Blocking { "/dev/random" } else { "/dev/urandom" };
        let seed = File::open(seed_path).map_err(|_| super::invalid("SecureRandom algorithm is unavailable"))?;
        let mut next = File::open(next_path).map_err(|_| super::invalid("SecureRandom algorithm is unavailable"))?;
        let mut bytes = Zeroizing::new([0; 20]);
        next.read_exact(bytes.as_mut()).map_err(super::invalid)?;
        Ok(Self { _seed: seed, next, mixer: Sha1::new(bytes.as_ref()), buffer: Zeroizing::new(vec![0; 65_536].into_boxed_slice()), size: 256, position: 256, changes: 0, refreshed: SystemTime::UNIX_EPOCH, _charge: charge })
    }

    fn fill(&mut self, output: &mut [u8]) -> Result<()> {
        self.mixer.fill(output);
        let mut offset = 0;
        while offset < output.len() {
            self.refresh(SystemTime::now())?;
            let count = (self.size - self.position).min(output.len() - offset);
            for (target, source) in output[offset..offset + count].iter_mut().zip(&self.buffer[self.position..self.position + count]) { *target ^= *source; }
            self.position += count;
            offset += count;
        }
        Ok(())
    }

    fn refresh(&mut self, now: SystemTime) -> Result<()> {
        if self.position < self.size {
            if now.duration_since(self.refreshed).unwrap_or(Duration::ZERO) < Duration::from_millis(100) { return Ok(()); }
            self.changes -= 1;
        } else { self.changes += 1; }
        if self.changes > 1000 { self.size = (self.size * 2).min(65_536); self.changes = 0; }
        if self.changes < -100 { self.size = (self.size / 2).max(32); self.changes = 0; }
        self.position = self.size;
        self.next.read_exact(&mut self.buffer[..self.size]).map_err(super::invalid)?;
        self.refreshed = now;
        self.position = 0;
        Ok(())
    }
}

struct Sha1 { state: [u8; 20], remainder: [u8; 20], position: usize }

impl Sha1 {
    fn new(seed: &[u8]) -> Self { Self { state: sha1(seed), remainder: [0; 20], position: 20 } }

    fn fill(&mut self, output: &mut [u8]) {
        for target in output {
            if self.position == 20 {
                self.remainder = sha1(&self.state);
                let mut carry = 1i32;
                let mut changed = false;
                for (state, value) in self.state.iter_mut().zip(self.remainder) {
                    let sum = i32::from(*state as i8) + i32::from(value as i8) + carry;
                    let next = sum as u8;
                    changed |= next != *state;
                    *state = next;
                    carry = sum >> 8;
                }
                if !changed { self.state[0] = self.state[0].wrapping_add(1); }
                self.position = 0;
            }
            *target = self.remainder[self.position];
            self.remainder[self.position] = 0;
            self.position += 1;
        }
    }
}

impl Drop for Sha1 {
    fn drop(&mut self) { self.state.zeroize(); self.remainder.zeroize(); }
}

struct HashDrbg { value: [u8; 55], constant: [u8; 55], counter: u64 }

static DRBG_NONCE: Mutex<[u8; 16]> = Mutex::new([0; 16]);

impl HashDrbg {
    fn from_default_entropy(mut entropy: impl FnMut(&mut [u8]) -> Result<()>, nonces: &Mutex<[u8; 16]>) -> Result<Self> {
        let mut seed = Zeroizing::new([0; 16]);
        entropy(seed.as_mut())?;
        let nonce = {
            let mut nonce = nonces.lock().map_err(|_| super::invalid("SecureRandom nonce state is unavailable"))?;
            add(&mut *nonce, &[1]);
            *nonce
        };
        Ok(Self::new(seed.as_ref(), &nonce))
    }

    fn new(entropy: &[u8], nonce: &[u8]) -> Self {
        let value = hash_df(&[entropy, nonce]);
        Self { constant: hash_df(&[&[0], &value]), value, counter: 1 }
    }

    fn reseed(&mut self, entropy: &[u8]) {
        self.value = hash_df(&[&[1], &self.value, entropy]);
        self.constant = hash_df(&[&[0], &self.value]);
        self.counter = 1;
    }

    fn fill(&mut self, output: &mut [u8]) {
        let mut current = Zeroizing::new(self.value);
        for chunk in output.chunks_mut(32) {
            let block = Zeroizing::new(sha256(current.as_ref()));
            chunk.copy_from_slice(&block[..chunk.len()]);
            add(current.as_mut(), &[1]);
        }
        let mut hash = Sha256::new();
        hash.update(&[3]);
        hash.update(&self.value);
        let hash = Zeroizing::new(hash.finish());
        add(&mut self.value, hash.as_ref());
        add(&mut self.value, &self.constant);
        add(&mut self.value, &self.counter.to_be_bytes());
        self.counter += 1;
    }
}

impl Drop for HashDrbg {
    fn drop(&mut self) { self.value.zeroize(); self.constant.zeroize(); self.counter.zeroize(); }
}

fn hash_df(inputs: &[&[u8]]) -> [u8; 55] {
    let mut output = [0; 55];
    for (index, chunk) in output.chunks_mut(32).enumerate() {
        let mut hash = Sha256::new();
        hash.update(&[index as u8 + 1]);
        hash.update(&440u32.to_be_bytes());
        for input in inputs { hash.update(input); }
        let block = Zeroizing::new(hash.finish());
        chunk.copy_from_slice(&block[..chunk.len()]);
    }
    output
}

fn add(value: &mut [u8], addend: &[u8]) {
    let mut carry = 0u16;
    for (target, addend) in value.iter_mut().rev().zip(addend.iter().rev().copied().chain(std::iter::repeat(0))) {
        let next = u16::from(*target) + u16::from(addend) + carry;
        *target = next as u8;
        carry = next >> 8;
    }
}

#[cfg(test)]
mod tests;
