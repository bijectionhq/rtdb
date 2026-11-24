use std::collections::{BTreeMap, btree_map::Entry};

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use super::{DataKey, Generation, Kms, aead, erased_under_view, invalid_envelope};

struct WrappedGeneration {
    wrapping_key: DataKey,
    wrapped_key: Vec<u8>,
}

impl WrappedGeneration {
    fn new() -> Result<Self> {
        let wrapping_key = aead::random_key()?;
        let key = aead::random_key()?;
        let wrapped_key = aead::seal(&wrapping_key, b"rtdb/v1/local/generation", &*key)?;
        Ok(Self {
            wrapping_key,
            wrapped_key,
        })
    }

    fn key(&self) -> Result<DataKey> {
        let bytes = aead::open(
            &self.wrapping_key,
            b"rtdb/v1/local/generation",
            &self.wrapped_key,
        )?;
        Ok(DataKey::new(
            bytes
                .as_slice()
                .try_into()
                .map_err(|_| invalid_envelope())?,
        ))
    }
}

enum State {
    Active(WrappedGeneration),
    Retiring {
        key: WrappedGeneration,
        unreadable_by: i64,
    },
    Destroyed,
}

impl State {
    fn read(&self, now: i64) -> Result<&WrappedGeneration> {
        match self {
            Self::Active(key) => Ok(key),
            Self::Retiring { key, unreadable_by } if now < *unreadable_by => Ok(key),
            Self::Retiring { .. } | Self::Destroyed => Err(erased_under_view()),
        }
    }
}

/// Process-local key custody for in-memory databases and tests.
/// Provide an authoritative, nondecreasing UTC microsecond clock.
pub struct LocalKms {
    keys: Mutex<BTreeMap<Generation, State>>,
    now: Box<dyn Fn() -> i64 + Send + Sync>,
}

impl LocalKms {
    pub fn new(now: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        Self {
            keys: Mutex::new(BTreeMap::new()),
            now: Box::new(now),
        }
    }
}

#[async_trait]
impl Kms for LocalKms {
    async fn create(&self, generation: Generation) -> Result<()> {
        match self.keys.lock().entry(generation) {
            Entry::Vacant(entry) => {
                entry.insert(State::Active(WrappedGeneration::new()?));
                Ok(())
            }
            Entry::Occupied(entry) => match entry.get() {
                State::Active(_) => Ok(()),
                State::Retiring { .. } | State::Destroyed => Err(erased_under_view()),
            },
        }
    }

    async fn wrap(&self, generation: Generation, key: &DataKey, context: &[u8]) -> Result<Vec<u8>> {
        let keys = self.keys.lock();
        match keys.get(&generation).ok_or_else(unknown_generation)? {
            State::Active(generation) => aead::seal(&generation.key()?, context, &**key),
            State::Retiring { .. } | State::Destroyed => Err(erased_under_view()),
        }
    }

    async fn unwrap(
        &self,
        generation: Generation,
        wrapped: &[u8],
        context: &[u8],
    ) -> Result<DataKey> {
        let keys = self.keys.lock();
        let key = keys
            .get(&generation)
            .ok_or_else(unknown_generation)?
            .read((self.now)())?
            .key()?;
        let plaintext = aead::open(&key, context, wrapped)?;
        Ok(DataKey::new(
            plaintext
                .as_slice()
                .try_into()
                .map_err(|_| invalid_envelope())?,
        ))
    }

    async fn retire(&self, generation: Generation, unreadable_by: i64) -> Result<()> {
        let mut keys = self.keys.lock();
        let state = keys.get_mut(&generation).ok_or_else(unknown_generation)?;
        match state {
            State::Active(_) => {
                let State::Active(key) = std::mem::replace(state, State::Destroyed) else {
                    unreachable!();
                };
                *state = State::Retiring { key, unreadable_by };
                Ok(())
            }
            State::Retiring {
                unreadable_by: existing,
                ..
            } if *existing != unreadable_by => Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.crypt/retirement-deadline",
                "Generation already has a retirement deadline",
            )),
            State::Retiring { .. } | State::Destroyed => Ok(()),
        }
    }

    async fn check_readable(&self, generation: Generation) -> Result<()> {
        self.keys
            .lock()
            .get(&generation)
            .ok_or_else(unknown_generation)?
            .read((self.now)())?;
        Ok(())
    }

    async fn destroy(&self, generation: Generation) -> Result<()> {
        let mut keys = self.keys.lock();
        let state = keys.get_mut(&generation).ok_or_else(unknown_generation)?;
        match state {
            State::Retiring { unreadable_by, .. } if (self.now)() >= *unreadable_by => {
                *state = State::Destroyed;
                Ok(())
            }
            State::Destroyed => Ok(()),
            State::Active(_) | State::Retiring { .. } => Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.crypt/retirement-pending",
                "Key custody must remain through the retirement deadline",
            )),
        }
    }
}

fn unknown_generation() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.crypt/unknown-generation",
        "Key custody is unavailable for the requested generation",
    )
}
