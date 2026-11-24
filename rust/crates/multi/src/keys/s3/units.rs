//! The S3 vault registers an immutable slot before sending its conditional key
//! creation. A frozen shard carries every identity that could still take effect.

use futures::{StreamExt, TryStreamExt, stream};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use super::{
    Current, Precondition, S3KeyService, UnitScope, UnitSlot, key_error, malformed, new_key,
    unavailable,
};

const MAX_SHARD_SLOTS: usize = 8192;
const MAX_SHARD_BYTES: usize = 2 + MAX_SHARD_SLOTS * 32;
const MUTATION_ATTEMPTS: usize = 8;
const RETIREMENT_PARALLELISM: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Open = 0,
    Frozen = 1,
    Settled = 2,
}

#[derive(Debug, PartialEq, Eq)]
struct Census {
    state: State,
    slots: Vec<[u8; 32]>,
}

impl Census {
    fn decode(shard: u8, encoded: &[u8]) -> Result<Self> {
        let state = match encoded.get(..2) {
            Some([1, 0]) => State::Open,
            Some([1, 1]) => State::Frozen,
            Some([1, 2]) => State::Settled,
            _ => return Err(malformed()),
        };
        let body = &encoded[2..];
        if body.len() % 32 != 0 || encoded.len() > MAX_SHARD_BYTES {
            return Err(malformed());
        }
        let slots: Vec<[u8; 32]> = body
            .chunks_exact(32)
            .map(|slot| slot.try_into().expect("fixed-width slot"))
            .collect();
        if slots.iter().any(|slot| slot[0] != shard)
            || slots.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(malformed());
        }
        Ok(Self { state, slots })
    }

    fn encode(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(2 + self.slots.len() * 32);
        body.extend_from_slice(&[1, self.state as u8]);
        for slot in &self.slots {
            body.extend_from_slice(slot);
        }
        body
    }

    fn register(&mut self, slot: [u8; 32]) -> Result<bool> {
        if self.state != State::Open {
            return Err(rtdb_storage::Error::ErasedUnderView.into());
        }
        let index = match self.slots.binary_search(&slot) {
            Ok(_) => return Ok(false),
            Err(index) => index,
        };
        if self.slots.len() == MAX_SHARD_SLOTS {
            return Err(Anomaly::new(
                AnomalyCategory::Busy,
                "rtdb.multi/unit-census-full",
                "The immutable publication namespace has exhausted its key registration shard",
            ));
        }
        self.slots.insert(index, slot);
        Ok(true)
    }
}

fn marker(scope: UnitScope) -> String {
    format!("keys/{}", scope.namespace())
}

fn shard_name(scope: UnitScope, shard: u8) -> String {
    format!("{}-census-{shard:02x}", marker(scope))
}

fn condition(etag: Option<&str>) -> Precondition<'_> {
    match etag {
        None => Precondition::Absent,
        Some(etag) => Precondition::Matches(etag),
    }
}

impl S3KeyService {
    pub(super) async fn require_open_unit_scope(&self, scope: UnitScope) -> Result<()> {
        match self.current(&marker(scope)).await? {
            Current::Absent => Ok(()),
            Current::Withdrawn => Err(rtdb_storage::Error::ErasedUnderView.into()),
            Current::Live { .. } => Err(malformed()),
        }
    }

    pub(super) async fn load_unit_slot(
        &self,
        slot: &UnitSlot,
        provisions: bool,
    ) -> rtdb_storage::Result<[u8; 32]> {
        let path = format!("keys/{}", slot.id());
        self.require_open_unit_scope(slot.scope)
            .await
            .map_err(key_error)?;
        let mut current = self.current(&path).await.map_err(key_error)?;
        if matches!(current, Current::Absent) && provisions {
            self.register_unit(slot).await.map_err(key_error)?;
            self.put(&path, &new_key().map_err(key_error)?, Precondition::Absent)
                .await
                .map_err(key_error)?;
            current = self.current(&path).await.map_err(key_error)?;
        }
        match current {
            Current::Live { mut key, .. } => {
                if let Err(error) = self.require_open_unit_scope(slot.scope).await {
                    key.fill(0);
                    return Err(key_error(error));
                }
                Ok(key)
            }
            Current::Withdrawn => Err(rtdb_storage::Error::ErasedUnderView),
            Current::Absent => Err(key_error(unavailable("confirm immutable-unit custody"))),
        }
    }

    async fn read_census(&self, name: &str, shard: u8) -> Result<(Census, Option<String>)> {
        match self.read_vault_object(name, MAX_SHARD_BYTES).await? {
            Some((encoded, etag)) => Ok((Census::decode(shard, &encoded)?, Some(etag))),
            None => Ok((
                Census {
                    state: State::Open,
                    slots: Vec::new(),
                },
                None,
            )),
        }
    }

    async fn register_unit(&self, slot: &UnitSlot) -> Result<()> {
        let shard = slot.digest[0];
        let name = shard_name(slot.scope, shard);
        for _ in 0..MUTATION_ATTEMPTS {
            self.require_open_unit_scope(slot.scope).await?;
            let (mut census, etag) = self.read_census(&name, shard).await?;
            if !census.register(slot.digest)?
                || self
                    .put(&name, &census.encode(), condition(etag.as_deref()))
                    .await?
            {
                return Ok(());
            }
        }
        Err(unavailable("register the publication key slot"))
    }

    pub(super) async fn withdraw_registered_scope(&self, scope: UnitScope) -> Result<()> {
        self.require_never_versioned().await?;
        self.withdraw_current(&marker(scope)).await?;
        // The scope marker closes admission before the grant window starts.
        // The individual materials remain intact during that window.
        rtdb_storage::sealing::drain_unit_delivery().await;
        stream::iter(0..=u8::MAX)
            .map(|shard| self.withdraw_shard(scope, shard))
            .buffer_unordered(RETIREMENT_PARALLELISM)
            .try_for_each(|()| async { Ok(()) })
            .await?;
        self.require_never_versioned().await
    }

    async fn withdraw_shard(&self, scope: UnitScope, shard: u8) -> Result<()> {
        let name = shard_name(scope, shard);
        for _ in 0..MUTATION_ATTEMPTS {
            let (mut census, etag) = self.read_census(&name, shard).await?;
            match census.state {
                State::Settled => return Ok(()),
                State::Open => {
                    census.state = if census.slots.is_empty() {
                        State::Settled
                    } else {
                        State::Frozen
                    };
                    if self
                        .put(&name, &census.encode(), condition(etag.as_deref()))
                        .await?
                        && census.state == State::Settled
                    {
                        return Ok(());
                    }
                }
                State::Frozen => {
                    for &digest in &census.slots {
                        let slot = UnitSlot { scope, digest };
                        self.withdraw_current(&format!("keys/{}", slot.id()))
                            .await?;
                    }
                    census.state = State::Settled;
                    if self
                        .put(&name, &census.encode(), condition(etag.as_deref()))
                        .await?
                    {
                        return Ok(());
                    }
                }
            }
        }
        Err(unavailable("settle the publication key census"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frozen_census_keeps_every_registered_slot_and_refuses_new_keys() {
        let mut census = Census {
            state: State::Open,
            slots: Vec::new(),
        };
        let first = [3; 32];
        let mut second = [8; 32];
        second[0] = 3;
        assert!(census.register(second).unwrap());
        assert!(census.register(first).unwrap());
        assert!(!census.register(first).unwrap());
        census.state = State::Frozen;
        let mut reopened = Census::decode(3, &census.encode()).unwrap();
        assert_eq!(reopened.slots, [first, second]);
        assert!(reopened.register(first).is_err());
        assert!(Census::decode(4, &census.encode()).is_err());
    }
}
