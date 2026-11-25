use rtdb_node::{
    Clock, Node, SystemClock,
    serving::{
        ServingDriver,
        custody_release::{CustodyCarrierAuthority, CustodyResponsePart, OriginalReceipt},
    },
};
use rtdb_types::{
    Timestamp,
    rtdb_v1::{
        DependentCustodyReceipt, NativeCheckpointCustody, NativeDependentRead,
        NativeDependentSourceGeneration,
    },
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::*;
use crate::multi::catalog::Status;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InheritedProfile {
    pub(super) client_certificate_sha256: [u8; 32],
    pub(super) source_incarnation: Uuid,
    pub(super) destination_incarnation: Uuid,
    pub(super) destination: String,
    pub(super) owner: [u8; 32],
    pub(super) application_scope: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedReadProof {
    pub read: NativeDependentRead,
    pub owner: [u8; 32],
    pub application_scope: [u8; 32],
    pub enrollment: DependentCustodyReceipt,
    pub authority: CustodyCarrierAuthority,
    pub original_writer: Basis,
    pub source_generation: NativeDependentSourceGeneration,
    pub original: [u8; 32],
    pub carrier_receipt: OriginalReceipt,
    pub carrier_sha256: [u8; 32],
    pub link_sha256: Option<[u8; 32]>,
    pub envelope_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedRelease {
    pub proof: InheritedReadProof,
    pub part: CustodyResponsePart,
    pub frame_sha256: [u8; 32],
    pub expires_at: Timestamp,
}

impl InheritedRelease {
    fn check(&self, now: Timestamp) -> Result<()> {
        if self.frame_sha256 == [0; 32]
            || [
                self.part.transport_history,
                self.part.producer,
                self.part.consumer,
                self.part.response,
            ]
            .contains(&[0; 16])
            || self.expires_at <= now
            || self.expires_at.0 > now.0.saturating_add(120_000_000)
        {
            return Err(refused());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedRegistration {
    pub release: InheritedRelease,
    pub receipt: DependentCustodyReceipt,
}

impl InheritedRegistration {
    fn check(&self, release: &InheritedRelease) -> Result<()> {
        if &self.release != release
            || self.receipt.key == [0; 32]
            || self.receipt.evidence_sha256 == [0; 32]
        {
            return Err(refused());
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InheritedReadRequest {
    pub(super) version: u32,
    pub(super) read: NativeDependentRead,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InheritedReleaseRequest {
    pub(super) version: u32,
    pub(super) release: InheritedRelease,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum InheritedResult {
    SourceGeneration {
        source_generation: NativeDependentSourceGeneration,
    },
    Read {
        proof: InheritedReadProof,
    },
    ReleaseChecked {
        receipt: DependentCustodyReceipt,
        expires_at: Timestamp,
    },
}

impl InheritedProfile {
    pub(super) async fn source_generation(
        &self,
        runtime: &MultiRuntime,
        scope: NativeDependentCustodyScope,
    ) -> Result<InheritedResult> {
        let (source, expected) = runtime
            .native_dependent_source(&self.destination, scope.reservation, scope.group)
            .await?;
        if scope != expected {
            return Err(refused());
        }
        let source_generation = source.native_dependent_source_generation(scope).await?;
        Ok(InheritedResult::SourceGeneration { source_generation })
    }

    pub(super) async fn read(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        read: NativeDependentRead,
        cancellation: &CancellationToken,
    ) -> Result<InheritedResult> {
        let proof = self.proof(runtime, node, read, cancellation).await?;
        Ok(InheritedResult::Read { proof })
    }

    pub(super) async fn release(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        release: InheritedRelease,
        cancellation: &CancellationToken,
    ) -> Result<InheritedResult> {
        release.check(node.now())?;
        let current = self
            .proof(runtime, node, release.proof.read.clone(), cancellation)
            .await?;
        if current != release.proof {
            return Err(refused());
        }
        let observer = runtime
            .native_dependent_custody
            .as_deref()
            .ok_or_else(pending)?;
        let observed = observer
            .observe_inherited_registration(&release, cancellation)
            .await?;
        observed.check(&release)?;
        let current = self
            .proof(runtime, node, release.proof.read.clone(), cancellation)
            .await?;
        if current != release.proof {
            return Err(refused());
        }
        release.check(node.now())?;
        Ok(InheritedResult::ReleaseChecked {
            receipt: observed.receipt,
            expires_at: release.expires_at,
        })
    }

    async fn proof(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        read: NativeDependentRead,
        cancellation: &CancellationToken,
    ) -> Result<InheritedReadProof> {
        let (source_name, custody) = self.destination(runtime, &read).await?;
        let source = runtime
            .ready_driver(&source_name, self.source_incarnation)
            .await?;
        let catalog = node.catalog();
        let attachment = catalog.database(&source_name)?;
        let driver: &dyn ServingDriver = source.as_ref();
        if !attachment
            .serving_driver()
            .is_some_and(|attached| std::ptr::addr_eq(attached, driver))
        {
            return Err(refused());
        }
        let owner = catalog.original_custody_response_owner(&source_name)?;
        let mounted = owner.response_release(&SystemClock, cancellation)?;
        let authority = mounted.release().original.authority(driver)?;
        let writer = mounted.original_writer_fence().await?;

        if self.destination(runtime, &read).await? != (source_name.clone(), custody.clone()) {
            return Err(refused());
        }
        let current = source
            .native_dependent_original(&custody, &read, &writer)
            .await?;
        if current.owner != self.owner
            || current.application_scope != self.application_scope
            || catalog
                .original_custody_response_owner(&source_name)?
                .mount_id()
                != owner.mount_id()
            || cancellation.is_cancelled()
        {
            return Err(refused());
        }
        Ok(InheritedReadProof {
            read,
            owner: current.owner,
            application_scope: current.application_scope,
            enrollment: current.enrollment,
            authority,
            original_writer: writer.written,
            source_generation: current.source_generation,
            original: current.original,
            carrier_receipt: OriginalReceipt {
                key: current.receipt.key,
                basis: current.receipt.basis,
                system_time: current.receipt.system_time,
            },
            carrier_sha256: current.carrier_sha256,
            link_sha256: current.link_sha256,
            envelope_sha256: current.envelope_sha256,
        })
    }

    async fn destination(
        &self,
        runtime: &MultiRuntime,
        read: &NativeDependentRead,
    ) -> Result<(String, NativeCheckpointCustody)> {
        let scope = read.scope;
        let selected = runtime
            .managed_entry(&self.destination, self.destination_incarnation)
            .await?;
        let Status::ManagedTransferReady { transfer, basis } = selected.status else {
            return Err(pending());
        };
        if basis != scope.destination
            || transfer.reservation != scope.reservation
            || runtime
                .native_checkpoint_provider(&self.destination, &transfer)
                .is_none()
        {
            return Err(refused());
        }
        let destination = runtime
            .ready_driver(&self.destination, self.destination_incarnation)
            .await?;
        let (restored, view, generation) = destination
            .managed_checkpoint_view(scope.reservation)
            .await?;
        if restored != scope.destination || generation != read.destination_generation {
            return Err(refused());
        }
        let custody = view.check_native_dependent_destination(read, SystemClock.now())?;
        Ok((transfer.source, custody.clone()))
    }
}
