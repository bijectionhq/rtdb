use std::{num::NonZeroU64, sync::Arc};

use ring::signature::{Ed25519KeyPair, KeyPair};
use rtdb_storage::IoRegion;
use rtdb_types::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::keys::{AssertionKeyService, KeyService, LocalKeyService};

use super::{AssertionDeadline, CustodyKind, DeadlineJournal, Obligation, Phase, Record, Settlement, changed, hash, invalid};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    provider: [u8; 32],
    deployment: Uuid,
    incarnation: NonZeroU64,
    family: Uuid,
    obligation: Obligation,
}

impl Binding {
    fn disposition(self, order: Uuid) -> Result<[u8; 32]> {
        hash(&("rtdb.multi/assertion-vault-tombstones/v1", order, self))
    }

    pub(super) fn require_settlement(self, order: Uuid, settlement: &Settlement) -> Result<()> {
        if settlement.obligation != self.obligation || settlement.disposition != self.disposition(order)? {
            return Err(changed());
        }
        Ok(())
    }
}

/// One commissioned assertion-stage holder. The family fence, the order and
/// the stage tombstones are read from the original provider's independent vault.
pub struct AssertionVaultCustodian<'a> {
    provider: &'a dyn KeyService,
    deadline: AssertionDeadline<'a>,
    expected_namespace: [u8; 32],
    provider_binding: [u8; 32],
    deployment: Uuid,
    incarnation: NonZeroU64,
    region: Arc<IoRegion>,
    signer: &'a Ed25519KeyPair,
}

impl LocalKeyService {
    pub fn assertion_vault_custodian<'a>(
        &'a self, expected_namespace: [u8; 32], deployment: Uuid, incarnation: NonZeroU64, region: Arc<IoRegion>,
        signer: &'a Ed25519KeyPair,
    ) -> Result<AssertionVaultCustodian<'a>> {
        self.require_assertion_broker()?;
        AssertionVaultCustodian::new(self, expected_namespace, deployment, incarnation, region, signer)
    }
}

#[cfg(feature = "s3-key-vault")]
impl crate::keys::s3::S3KeyService {
    pub fn assertion_vault_custodian<'a>(
        &'a self, expected_namespace: [u8; 32], deployment: Uuid, incarnation: NonZeroU64, region: Arc<IoRegion>,
        signer: &'a Ed25519KeyPair,
    ) -> Result<AssertionVaultCustodian<'a>> {
        AssertionVaultCustodian::new(self, expected_namespace, deployment, incarnation, region, signer)
    }
}

impl<'a> AssertionVaultCustodian<'a> {
    fn new(
        provider: &'a (impl DeadlineJournal + KeyService), expected_namespace: [u8; 32], deployment: Uuid,
        incarnation: NonZeroU64, region: Arc<IoRegion>, signer: &'a Ed25519KeyPair,
    ) -> Result<Self> {
        if deployment.is_nil() || expected_namespace == [0; 32] {
            return Err(invalid("The assertion vault requires its original deployment"));
        }
        let provider_binding = hash(&(provider.custody_namespace()?, expected_namespace))?;
        Ok(Self { provider, deadline: AssertionDeadline::new(provider), expected_namespace, provider_binding,
            deployment, incarnation, region, signer })
    }

    /// Enrolls the retained, committed family fence before starting work on its exact stage inventory.
    pub async fn bind(&self, order: Uuid, family: Uuid, obligation: Obligation) -> Result<()> {
        self.require_namespace().await?;
        let binding = Binding { provider: self.provider_binding, deployment: self.deployment,
            incarnation: self.incarnation, family, obligation };
        let (record, _) = self.deadline.read(order).await?.ok_or_else(changed)?;
        self.require_owner(&record, binding)?;
        if record.vault_binding == Some(binding) { return Ok(()); }
        self.fence(order, binding, false).await?;
        self.deadline.update(order, |stored| {
            if stored.order != record.order { return Err(changed()); }
            self.require_owner(stored, binding)?;
            stored.vault_binding = Some(binding);
            Ok(())
        }).await?;
        Ok(())
    }

    /// Joins the commissioned holder's signed observation after all of its stage
    /// tombstones are durable. The source and the original request data may be gone.
    pub async fn reconcile(&self, order: Uuid) -> Result<Settlement> {
        self.reconcile_at(order, || u64::try_from(rtdb_directory::now_ms())
            .map_err(|_| invalid("The assertion vault clock is invalid"))).await
    }

    async fn reconcile_at(
        &self, order: Uuid, now: impl Fn() -> Result<u64>,
    ) -> Result<Settlement> {
        self.require_namespace().await?;
        let (record, _) = self.deadline.read(order).await?.ok_or_else(changed)?;
        let binding = record.vault_binding.ok_or_else(changed)?;
        self.require_owner(&record, binding)?;
        let disposition = binding.disposition(order)?;
        let obligation = binding.obligation;
        let observed_at = now()?;
        let (ordered_at, retained) = require_destruction(&record, binding, observed_at)?;
        if let Some(retained) = retained {
            return Ok(retained);
        }

        let fence = match self.fence(order, binding, true).await {
            Ok(fence) => fence,
            Err(error) => {
                // Terminal family cleanup can win after this attempt read the order.
                let (current, _) = self.deadline.read(order).await?.ok_or_else(changed)?;
                self.require_owner(&current, binding)?;
                if let Some(retained) = require_destruction(&current, binding, now()?)?.1 {
                    return Ok(retained);
                }
                return Err(error);
            }
        };
        let keys: &dyn AssertionKeyService = self.provider.assertion_keys().ok_or_else(changed)?;
        for &stage in fence.assertion_capabilities.union(&fence.link_capabilities) {
            keys.retire_assertion_stage(stage, &self.region).await?;
        }

        let observed_at = now()?;
        let mut candidate = Settlement { obligation, observed_at_ms: observed_at, disposition, signature: Vec::new() };
        candidate.signature = self.signer.sign(&candidate.signing_bytes(&record.order, ordered_at)?).as_ref().to_vec();
        let terminal = self.deadline.update(order, |stored| {
            let (stored_at, retained) = require_destruction(stored, binding, now()?)?;
            if stored.order != record.order || stored_at != ordered_at {
                return Err(changed());
            }
            if retained.is_some() { return Ok(()); }
            stored.join(candidate.clone(), observed_at)
        }).await?;
        require_destruction(&terminal, binding, now()?)?.1.ok_or_else(changed)
    }

    fn require_owner(&self, record: &Record, binding: Binding) -> Result<()> {
        let mut holders = record.order.obligations.iter().copied().filter(|item| item.kind == CustodyKind::AssertionVault);
        if binding.family.is_nil() || binding.provider != self.provider_binding
            || binding.deployment != self.deployment || binding.incarnation != self.incarnation
            || holders.next() != Some(binding.obligation) || holders.next().is_some()
            || self.signer.public_key().as_ref() != binding.obligation.holder_key
            || record.vault_binding.is_some_and(|prior| prior != binding)
        {
            return Err(changed());
        }
        Ok(())
    }

    async fn require_namespace(&self) -> Result<()> {
        if hash(&(self.provider.custody_namespace()?, self.expected_namespace))? != self.provider_binding
            || self.provider.existing_namespace_id().await? != Some(self.expected_namespace)
        {
            return Err(invalid("The configured assertion vault is not the commissioned provider"));
        }
        Ok(())
    }

    async fn fence(&self, order: Uuid, binding: Binding, require_drained: bool) -> Result<crate::keys::FamilyFence> {
        let families = self.provider.assertion_families(Arc::clone(&self.region)).ok_or_else(changed)?;
        let fence = families.retirement_fence(binding.family, order, require_drained).await?;
        if fence.vault_inventory() != binding.obligation.inventory {
            return Err(changed());
        }
        let mut stages = fence.assertion_capabilities.union(&fence.link_capabilities);
        if stages.any(|stage| stage.deployment() != self.deployment || stage.incarnation() != self.incarnation) {
            return Err(invalid("The assertion vault does not own the complete fenced inventory"));
        }
        Ok(fence)
    }
}

fn require_destruction(record: &Record, binding: Binding, now: u64) -> Result<(u64, Option<Settlement>)> {
    if record.vault_binding != Some(binding) {
        return Err(changed());
    }
    let Phase::Destroying { ordered_at_ms, settlements } = &record.phase else { return Err(changed()); };
    if *ordered_at_ms > now {
        return Err(changed());
    }
    for obligation in record.order.obligations.iter().take_while(|item| item.kind == CustodyKind::ReleaseDrain) {
        let drain = settlements.iter().find(|settlement| settlement.obligation == *obligation).ok_or_else(changed)?;
        drain.verify(&record.order, *ordered_at_ms, now)?;
    }
    let retained = settlements.iter().find(|settlement| settlement.obligation == binding.obligation);
    if let Some(retained) = retained {
        binding.require_settlement(record.order.id, retained)?;
        retained.verify(&record.order, *ordered_at_ms, now)?;
    }
    Ok((*ordered_at_ms, retained.cloned()))
}

#[cfg(test)]
mod tests;
