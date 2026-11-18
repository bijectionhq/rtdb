use std::{
    hash::{BuildHasher, RandomState},
    sync::{Arc, Mutex},
};
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Anomaly, Result};
use crate::connect::{ConversionContext, avro::{AvroSchemaRuntime, SchemaStringFactory, ThreadTokenPool}};
use super::reader_symbols::{FactoryProfile, InternCache, Profile};

const PROFILE: Profile = Profile::Jackson221;

#[derive(Clone, Default)]
pub(in crate::connect) struct ParserFactories(Arc<Mutex<Factories>>);

#[derive(Default)]
struct Factories {
    shared: Option<Shared>,
    avro: Option<AvroFactory>,
}

struct Shared { intern: InternCache, pool: Arc<ThreadTokenPool> }
struct AvroFactory { root: SchemaStringFactory, _identity: Arc<MemoryReservation> }

impl ParserFactories {
    pub(in crate::connect) fn avro(&self, context: &mut ConversionContext) -> Result<AvroSchemaRuntime> {
        let (root, intern, pool) = {
            let mut factories = self.0.lock().map_err(|_| Anomaly::fault("xtdb.kafka-connect-source/registry-parser-state", "Parser factory catalog lock is poisoned"))?;
            let Factories { shared, avro } = &mut *factories;
            if shared.is_none() {
                let pool = ThreadTokenPool::new(PROFILE, context)?;
                let intern = InternCache::open(PROFILE, context)?;
                *shared = Some(Shared { intern, pool });
            }
            let shared = shared.as_ref().expect("Jackson classloader authorities initialized");
            if avro.is_none() {
                let identity = context.reserve(512)?;
                let root = SchemaStringFactory::new_default(char_profile(&identity), context)?;
                *avro = Some(AvroFactory { root, _identity: identity });
            }
            let avro = avro.as_ref().expect("Avro factory initialized");
            (avro.root.clone(), shared.intern.clone(), Arc::clone(&shared.pool))
        };
        AvroSchemaRuntime::new(root, pool.buffers(), intern, context)
    }
}

fn char_profile(identity: &Arc<MemoryReservation>) -> FactoryProfile {
    let hash = RandomState::new().hash_one(Arc::as_ptr(identity) as usize) & i32::MAX as u64;
    FactoryProfile { profile: PROFILE, identity_hash_seed: (hash as i32).max(1) }
}
