use rtdb_types::{EntityId, TableRef, Timestamp};
use sha2::{Digest, Sha256};

use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginalReceipt {
    pub key: Vec<u8>,
    pub basis: Basis,
    pub system_time: Timestamp,
}

impl OriginalReceipt {
    pub fn commitment(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/custody-carrier-receipt\0");
        hash.update((self.key.len() as u64).to_be_bytes());
        hash.update(&self.key);
        hash.update(self.basis.to_bytes());
        hash.update(self.system_time.0.to_be_bytes());
        hash.finalize().into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginalWriterPoint {
    pub table: TableRef,
    pub id: EntityId,
    pub valid_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginalCarrierReceipt {
    pub registration: CustodyReleaseRegistration,
    pub receipt: OriginalReceipt,
    pub observation: CarrierObservation,
}

/// The installed original provider reads its independent frontier and exact output registry.
#[async_trait]
pub trait OriginalCarrierObservations: Send + Sync {
    async fn confirmed_writer(
        &self,
        authority: CustodyCarrierAuthority,
        cancellation: &CancellationToken,
    ) -> Result<OriginalReceipt>;
    /// Reads the live original response part after the exact frame and complete contributors are registered.
    async fn registered_response(
        &self,
        authority: CustodyCarrierAuthority,
        part: CustodyResponsePart,
        cancellation: &CancellationToken,
    ) -> Result<RegisteredCustodyResponse>;

    async fn registered_carrier(
        &self,
        registration: CustodyReleaseRegistration,
        cancellation: &CancellationToken,
    ) -> Result<OriginalCarrierReceipt>;
}

pub struct AttachedCustodyCarrierJournal<'a> {
    attachment: &'a dyn ServingDriver,
    authority: CustodyCarrierAuthority,
    writer: OriginalWriterPoint,
    original: &'a dyn OriginalCarrierObservations,
    lifetime: Option<&'a CancellationToken>,
}

impl<'a> AttachedCustodyCarrierJournal<'a> {
    pub(crate) fn new(
        attachment: &'a dyn ServingDriver,
        authority: CustodyCarrierAuthority,
        writer: OriginalWriterPoint,
        original: &'a dyn OriginalCarrierObservations,
    ) -> Result<Self> {
        if authority.incarnation != attachment.incarnation()
            || authority.source_history == [0; 32]
            || authority.installation == [0; 16]
            || authority.registry == [0; 32]
            || authority.original_writer == [0; 32]
        {
            return Err(release_unavailable());
        }
        Ok(Self {
            attachment,
            authority,
            writer,
            original,
            lifetime: None,
        })
    }

    pub(super) fn with_lifetime(mut self, lifetime: &'a CancellationToken) -> Result<Self> {
        if lifetime.is_cancelled() {
            return Err(release_unavailable());
        }
        self.lifetime = Some(lifetime);
        Ok(self)
    }

    pub(super) fn attachment(&self) -> &dyn ServingDriver {
        self.attachment
    }

    async fn confirm_receipt(
        &self,
        expected: &OriginalReceipt,
        cancellation: &CancellationToken,
    ) -> Result<Receipt> {
        if expected.basis.incarnation() != self.authority.incarnation {
            return Err(release_unavailable());
        }
        let receipt = self
            .attachment
            .original_custody_receipt(&expected.key, expected.basis, cancellation)
            .await?;
        if receipt.key != expected.key
            || receipt.basis != expected.basis
            || receipt.system_time != expected.system_time
            || matches!(receipt.outcome, Outcome::Refused { .. })
        {
            return Err(release_unavailable());
        }
        Ok(receipt)
    }
}

pub fn original_writer_commitment(installation: [u8; 16], request_key: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rtdb/v1/custody-original-writer\0");
    hash.update(installation);
    hash.update(request_key);
    hash.finalize().into()
}

#[async_trait]
impl CustodyCarrierJournal for AttachedCustodyCarrierJournal<'_> {
    fn authority(&self, attachment: &dyn ServingDriver) -> Result<CustodyCarrierAuthority> {
        if self.lifetime.is_some_and(CancellationToken::is_cancelled)
            || !std::ptr::addr_eq(attachment, self.attachment)
        {
            return Err(release_unavailable());
        }
        Ok(self.authority)
    }

    async fn writer_fence(&self, cancellation: &CancellationToken) -> Result<CustodyWriterFence> {
        let observed = self
            .original
            .confirmed_writer(self.authority, cancellation)
            .await?;
        if original_writer_commitment(self.authority.installation, &observed.key)
            != self.authority.original_writer
            || self.confirm_receipt(&observed, cancellation).await?.outcome != Outcome::Then
        {
            return Err(release_unavailable());
        }
        Ok(CustodyWriterFence {
            table: self.writer.table.clone(),
            id: self.writer.id.clone(),
            valid_at: self.writer.valid_at,
            written: observed.basis,
        })
    }

    async fn observe(
        &self,
        registration: CustodyReleaseRegistration,
        cancellation: &CancellationToken,
    ) -> Result<CarrierObservation> {
        if registration.authority != self.authority {
            return Err(release_unavailable());
        }
        let observed = self
            .original
            .registered_carrier(registration, cancellation)
            .await?;
        if observed.registration != registration
            || observed.receipt.commitment() != registration.carrier_receipt_sha256
        {
            return Err(release_unavailable());
        }
        self.confirm_receipt(&observed.receipt, cancellation)
            .await?;
        let index = self.attachment.current_custody(cancellation).await?;
        index.check_custody_carrier_receipt(registration, observed.receipt.basis)?;
        Ok(observed.observation)
    }
}
