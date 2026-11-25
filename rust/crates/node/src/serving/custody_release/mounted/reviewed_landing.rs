use bytes::Bytes;
use rtdb_index::branches::{
    BranchIndex, BranchView, CustodyOccurrence, CustodyPlacementId, CustodyReview, CustodyTarget,
};
use rtdb_tx::transact::{Outcome, Receipt, ValidTime};
use rtdb_types::{
    EntityId, TableRef, Value,
    rtdb_v1::{At, Layer, MAIN_BRANCH},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::*;

mod authoring;
pub use authoring::{NativeAuthoring, NativeAuthoringLine, NativeAuthoringRewrite};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeLandingReview {
    pub group: CustodyGroupId,
    pub cut: CustodyReview,
    pub original: [u8; 32],
    pub selected: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeLandingWitness {
    pub table: TableRef,
    pub id: EntityId,
    pub valid_at: Timestamp,
    pub column: String,
}

impl NativeLandingWitness {
    pub fn same_cell(&self, other: &Self) -> bool {
        self.table == other.table && self.id == other.id && self.column == other.column
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeLandingOrder {
    pub review: NativeLandingReview,
    pub reserve_key: Vec<u8>,
    pub accept_key: Vec<u8>,
    pub destination: CustodyPlacementId,
    pub link: CustodyPlacementId,
    pub target: CustodyTarget<ValidTime>,
    pub review_sha256: [u8; 32],
    pub request_sha256: [u8; 32],
    pub receipt: NativeLandingWitness,
    pub proof: NativeLandingWitness,
}

pub struct VerifiedNativeLanding {
    receipt: Receipt,
    occurrence: CustodyOccurrence,
    carrier: Bytes,
    link: Bytes,
}

impl VerifiedNativeLanding {
    pub fn receipt(&self) -> &Receipt {
        &self.receipt
    }
    pub fn occurrence(&self) -> &CustodyOccurrence {
        &self.occurrence
    }
    pub fn carrier(&self) -> &Bytes {
        &self.carrier
    }
    pub fn link(&self) -> &Bytes {
        &self.link
    }
}

impl MountedResponseRelease<'_> {
    pub fn native_landing_authority(&self) -> CustodyCarrierAuthority {
        self.owner.provider.installation.authority
    }

    pub async fn verify_native_landing_review(
        &self,
        review: &NativeLandingReview,
        original_carrier: &[u8],
        selected_carrier: &[u8],
    ) -> Result<[(CustodyOccurrence, Timestamp); 2]> {
        let release = self.release();
        let authority = self.native_landing_authority();
        let verified = self
            .while_mounted(async {
                let (index, writer) = release.current_with_writer(authority).await?;
                let cut = &review.cut;
                index.view(
                    &cut.branch,
                    At::Exactly(cut.basis),
                    Layer::Merged,
                    self.clock.now(),
                )?;
                let (observed, original, selected) = index.custody_review_at(
                    &cut.branch,
                    cut.basis,
                    cut.valid_at,
                    review.group,
                    review.selected,
                )?;
                if observed != *cut
                    || original.commitment(review.group) != review.original
                    || original.carrier_sha256 != hash(original_carrier)
                    || selected.carrier_sha256 != hash(selected_carrier)
                {
                    return Err(release_unavailable());
                }
                for occurrence in [original, selected] {
                    let receipt = release
                        .driver
                        .original_custody_receipt(
                            &occurrence.receipt_key,
                            occurrence.written,
                            self.cancellation,
                        )
                        .await?;
                    confirm_occurrence(&index, occurrence, &receipt)?;
                }
                release
                    .driver
                    .check_custody_release_delivery(
                        &BTreeSet::from([review.group]),
                        &writer,
                        None,
                        self.cancellation,
                    )
                    .await?;
                Ok([
                    (original.clone(), index.exact(original.written)?.system_time),
                    (selected.clone(), index.exact(selected.written)?.system_time),
                ])
            })
            .await?;
        self.owner
            .with_live_mount(|| release.require_authority(authority))?;
        Ok(verified)
    }

    pub async fn verify_native_landing_acceptance(
        &self,
        order: &NativeLandingOrder,
        body_sha256: [u8; 32],
    ) -> Result<VerifiedNativeLanding> {
        let release = self.release();
        let authority = self.native_landing_authority();
        let verified = self
            .while_mounted(async {
                if !(16..=64).contains(&order.reserve_key.len())
                    || !(16..=64).contains(&order.accept_key.len())
                    || order.reserve_key == order.accept_key
                    || order.receipt.same_cell(&order.proof)
                    || body_sha256 == [0; 32]
                {
                    return Err(release_unavailable());
                }
                let (index, writer) = release.current_with_writer(authority).await?;
                let reserved = release
                    .driver
                    .tx_status(&order.reserve_key, self.cancellation)
                    .await?;
                let accepted = release
                    .driver
                    .tx_status(&order.accept_key, self.cancellation)
                    .await?;
                if reserved.outcome != Outcome::Then
                    || accepted.outcome != Outcome::Then
                    || reserved.key != order.reserve_key
                    || accepted.key != order.accept_key
                    || reserved.basis >= accepted.basis
                    || order.review.cut.basis >= reserved.basis
                    || index.exact(reserved.basis)?.system_time != reserved.system_time
                {
                    return Err(release_unavailable());
                }

                let view = index.view(
                    MAIN_BRANCH,
                    At::Exactly(accepted.basis),
                    Layer::Merged,
                    self.clock.now(),
                )?;
                let entry = view
                    .custody_placements()
                    .find(|entry| {
                        entry.group == order.review.group && entry.placement_id == order.destination
                    })
                    .ok_or_else(release_unavailable)?;
                let (reviewed, reservation) = entry.reviewed.ok_or_else(release_unavailable)?;
                let occurrence = entry.occurrence.ok_or_else(release_unavailable)?;
                if reviewed.source != order.review.cut
                    || reviewed.receipt_key != order.reserve_key
                    || reservation.original != order.review.original
                    || reservation.source != order.review.selected
                    || reservation.destination != order.destination
                    || reservation.link != order.link
                    || reservation.target != order.target
                    || reservation.review_sha256 != order.review_sha256
                    || reservation.carrier_sha256 != occurrence.carrier_sha256
                    || Some(reservation.link_sha256) != occurrence.link_sha256
                    || occurrence.reservation != entry.reservation
                    || entry.enrolled != reserved.basis
                    || !view.lineage().iter().any(|line| {
                        line.branch == order.review.cut.branch
                            && line.dropped == Some(accepted.basis)
                    })
                {
                    return Err(release_unavailable());
                }
                confirm_occurrence(&index, occurrence, &accepted)?;
                confirm_witness(&view, &order.receipt, body_sha256)?;
                confirm_witness(&view, &order.proof, body_sha256)?;
                let group = order.review.group;
                let payload = index.custody_occurrence_payload(
                    accepted.basis,
                    group,
                    occurrence.commitment(group),
                )?;
                let vault = self.owner.database.token_authority().await?.custody_vault();
                let cells = crate::serving::custody::open(&vault, group, payload).await?;
                let mut carrier = None;
                let mut link = None;
                for value in cells.values() {
                    let (Value::Binary(bytes) | Value::FixedSizeBinary(bytes)) = value else {
                        return Err(release_unavailable());
                    };
                    let digest = hash(bytes);
                    if digest == reservation.carrier_sha256 && carrier.is_none() {
                        carrier = Some(Bytes::copy_from_slice(bytes));
                    } else if digest == reservation.link_sha256 && link.is_none() {
                        link = Some(Bytes::copy_from_slice(bytes));
                    } else {
                        return Err(release_unavailable());
                    }
                }
                if reservation.carrier_sha256 == reservation.link_sha256 {
                    return Err(release_unavailable());
                }
                release
                    .driver
                    .check_custody_release_delivery(
                        &BTreeSet::from([group]),
                        &writer,
                        None,
                        self.cancellation,
                    )
                    .await?;
                Ok(VerifiedNativeLanding {
                    receipt: accepted,
                    occurrence: occurrence.clone(),
                    carrier: carrier.ok_or_else(release_unavailable)?,
                    link: link.ok_or_else(release_unavailable)?,
                })
            })
            .await?;
        self.owner
            .with_live_mount(|| release.require_authority(authority))?;
        Ok(verified)
    }

    pub async fn verify_native_landing_drain(
        &self,
        group: CustodyGroupId,
        revoked_at: Basis,
        inventory_sha256: [u8; 32],
    ) -> Result<()> {
        let release = self.release();
        let authority = self.native_landing_authority();
        if !self
            .while_mounted(release.drained(authority, group, revoked_at, inventory_sha256))
            .await?
        {
            return Err(release_unavailable());
        }
        self.owner
            .with_live_mount(|| release.require_authority(authority))
    }

    pub async fn verify_native_landing_live(&self, group: CustodyGroupId) -> Result<()> {
        let release = self.release();
        let authority = self.native_landing_authority();
        self.while_mounted(async {
            let (_, writer) = release.current_with_writer(authority).await?;
            release
                .driver
                .check_custody_release_delivery(
                    &BTreeSet::from([group]),
                    &writer,
                    None,
                    self.cancellation,
                )
                .await
        })
        .await?;
        self.owner
            .with_live_mount(|| release.require_authority(authority))
    }
}

fn confirm_occurrence(
    index: &BranchIndex,
    occurrence: &CustodyOccurrence,
    receipt: &Receipt,
) -> Result<()> {
    if occurrence.receipt_key != receipt.key
        || occurrence.written != receipt.basis
        || index.exact(receipt.basis)?.system_time != receipt.system_time
        || matches!(receipt.outcome, Outcome::Refused { .. })
    {
        return Err(release_unavailable());
    }
    Ok(())
}

fn confirm_witness(
    view: &BranchView<'_>,
    witness: &NativeLandingWitness,
    body_sha256: [u8; 32],
) -> Result<()> {
    let event = view
        .row(&witness.table, &witness.id, witness.valid_at)
        .ok_or_else(release_unavailable)?;
    let image = event
        .image_at(witness.valid_at)
        .ok_or_else(release_unavailable)?;
    if event.cut != view.cut() || image.custody_group.is_some() {
        return Err(release_unavailable());
    }
    match image.cells.get(&witness.column) {
        Some(Value::Binary(bytes) | Value::FixedSizeBinary(bytes))
            if hash(bytes) == body_sha256 =>
        {
            Ok(())
        }
        _ => Err(release_unavailable()),
    }
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[cfg(test)]
mod tests {
    use rtdb_index::branches::{BranchCut, BranchEffect, BranchEvent, RowImage, WrittenVersion};
    use rtdb_types::{Interval, StructFields};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn landing_witness_requires_exact_body_at_the_accepting_basis() -> Result<()> {
        let incarnation = [1; 16];
        let mut index = BranchIndex::new(incarnation);
        let witness = NativeLandingWitness {
            table: TableRef::new("public", "receipts"),
            id: EntityId::Integer(1),
            valid_at: Timestamp(10),
            column: "body".into(),
        };
        let cut = BranchCut {
            basis: Basis::new(incarnation, 1),
            system_time: Timestamp(10),
        };
        let body = b"accepted body";
        index.advance(cut)?;
        index.apply(BranchEffect::Write {
            branch: MAIN_BRANCH.into(),
            table: witness.table.clone(),
            event: BranchEvent {
                id: witness.id.clone(),
                valid: Interval::ALL,
                cut,
                written: WrittenVersion::Branch,
                image: Some(Arc::new(RowImage {
                    cells: StructFields::from([(
                        witness.column.clone(),
                        Value::Binary(body.to_vec()),
                    )]),
                    custody_group: None,
                    custody_payload: None,
                    expires_at: None,
                })),
            },
        })?;
        let selected = index.view(
            MAIN_BRANCH,
            At::Exactly(cut.basis),
            Layer::Merged,
            Timestamp(10),
        )?;
        confirm_witness(&selected, &witness, hash(body))?;
        assert!(confirm_witness(&selected, &witness, hash(b"canonical request")).is_err());
        let later = BranchCut {
            basis: Basis::new(incarnation, 2),
            system_time: Timestamp(11),
        };
        index.advance(later)?;
        let selected = index.view(
            MAIN_BRANCH,
            At::Exactly(later.basis),
            Layer::Merged,
            Timestamp(11),
        )?;
        assert!(confirm_witness(&selected, &witness, hash(body)).is_err());
        Ok(())
    }
}
