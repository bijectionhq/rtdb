use sha2::{Digest, Sha256};
use rtdb_types::rtdb_v1::{ValidTime, ValidTimeBound};

use super::*;

pub type CustodyPlacementId = [u8; 16];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyTarget<T = Interval> {
    pub branch: Branch,
    pub table: TableRef,
    pub id: EntityId,
    pub valid: T,
}

impl<T> CustodyTarget<T> {
    fn commit_row(&self, hash: &mut Sha256) {
        for part in [
            self.branch.as_bytes(),
            self.table.schema.as_bytes(),
            self.table.table.as_bytes(),
            &self.id.handle_bytes(),
        ] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part);
        }
    }

    fn with_valid<U>(&self, valid: U) -> CustodyTarget<U> {
        CustodyTarget { branch: self.branch.clone(), table: self.table.clone(), id: self.id.clone(), valid }
    }
}

fn commit_interval(hash: &mut Sha256, valid: Interval) {
    hash.update(valid.lower().0.to_be_bytes());
    hash.update(valid.upper().0.to_be_bytes());
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedCustodyReservation {
    pub original_placement_id: CustodyPlacementId,
    pub link: CustodyPlacementId,
    pub review_sha256: [u8; 32],
    pub carrier_sha256: [u8; 32],
    pub link_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyReservation {
    pub original: [u8; 32],
    pub source: [u8; 32],
    pub destination: CustodyPlacementId,
    pub link: CustodyPlacementId,
    pub target: CustodyTarget<ValidTime>,
    pub review_sha256: [u8; 32],
    pub carrier_sha256: [u8; 32],
    pub link_sha256: [u8; 32],
}

impl CustodyReservation {
    pub fn commitment(&self, incarnation: [u8; 16], group: CustodyGroupId) -> [u8; 32] {
        let mut hash = Sha256::new();
        if self.target.valid.is_literal() {
            hash.update(b"rtdb/v1/custody-reservation\0");
        } else {
            hash.update(b"rtdb/v1/custody-reservation-relative\0");
        }
        hash.update(incarnation);
        hash.update(group);
        hash.update(self.original);
        hash.update(self.source);
        hash.update(self.destination);
        hash.update(self.link);
        self.target.commit_row(&mut hash);
        match self.target.valid {
            ValidTime::Literal(valid) => commit_interval(&mut hash, valid),
            ValidTime::Relative { from, to } => {
                for bound in [from, to] {
                    let (tag, micros) = match bound {
                        ValidTimeBound::Instant(at) => (0, at.0),
                        ValidTimeBound::TransactionMicros(offset) => (1, offset),
                    };
                    hash.update([tag]);
                    hash.update(micros.to_be_bytes());
                }
            }
        }
        hash.update(self.review_sha256);
        hash.update(self.carrier_sha256);
        hash.update(self.link_sha256);
        hash.finalize().into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyOccurrence {
    pub placement_id: CustodyPlacementId,
    pub target: CustodyTarget,
    pub written: Basis,
    /// Locator for the applied carrier receipt. Earlier records retain the
    /// occurrence identity but have no independently recoverable locator.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub receipt_key: Vec<u8>,
    pub ordinal: u64,
    pub carrier_sha256: [u8; 32],
    pub envelope_sha256: [u8; 32],
    pub reservation: Option<[u8; 32]>,
    pub link_sha256: Option<[u8; 32]>,
}

impl CustodyOccurrence {
    fn matches_event(&self, group: CustodyGroupId, event: &BranchEvent) -> bool {
        event.cut.basis == self.written
            && event.valid == self.target.valid
            && event.id == self.target.id
            && event.image.as_ref().is_some_and(|image| {
                image.custody_group == Some(group) && image.custody_payload.as_ref().is_some_and(|payload| {
                    <[u8; 32]>::from(Sha256::digest(&payload.envelope)) == self.envelope_sha256
                })
            })
    }

    pub fn commitment(&self, group: CustodyGroupId) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/custody-occurrence\0");
        hash.update(group);
        hash.update(self.placement_id);
        self.target.commit_row(&mut hash);
        commit_interval(&mut hash, self.target.valid);
        hash.update(self.written.to_bytes());
        hash.update(self.ordinal.to_be_bytes());
        hash.update(self.carrier_sha256);
        hash.update(self.envelope_sha256);
        for commitment in [self.reservation, self.link_sha256] {
            hash.update([u8::from(commitment.is_some())]);
            if let Some(commitment) = commitment {
                hash.update(commitment);
            }
        }
        hash.finalize().into()
    }
}

pub struct CustodyPlacementEntry<'a> {
    pub group: CustodyGroupId,
    pub placement_id: CustodyPlacementId,
    pub enrolled: Basis,
    pub reservation: Option<[u8; 32]>,
    pub occurrence: Option<&'a CustodyOccurrence>,
    pub reviewed: Option<(&'a ReviewedCustodyReservation, &'a CustodyReservation)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyReview {
    pub branch: Branch,
    pub basis: Basis,
    pub valid_at: Timestamp,
    pub inventory_sha256: [u8; 32],
}

impl CustodyReview {
    pub fn commitment(&self, group: CustodyGroupId, original: [u8; 32], selected: [u8; 32]) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/custody-review-source\0");
        hash.update(self.basis.to_bytes());
        hash.update(self.valid_at.0.to_be_bytes());
        hash.update(self.inventory_sha256);
        hash.update(group);
        hash.update(original);
        hash.update(selected);
        hash.update(self.branch.as_bytes());
        hash.finalize().into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewedCustodyReservation {
    pub source: CustodyReview,
    pub receipt_key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ReservedPlacement {
    pub at: Basis,
    pub reservation: CustodyReservation,
    pub occurrence: Option<CustodyOccurrence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewed: Option<ReviewedCustodyReservation>,
}

impl ReservedPlacement {
    pub(super) fn commitment(&self, incarnation: [u8; 16], group: CustodyGroupId) -> [u8; 32] {
        let reservation = self.reservation.commitment(incarnation, group);
        let Some(reviewed) = &self.reviewed else { return reservation; };
        let source = &reviewed.source;
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/reviewed-custody-reservation\0");
        hash.update(reservation);
        hash.update(self.at.to_bytes());
        hash.update(source.commitment(group, self.reservation.original, self.reservation.source));
        hash.update(&reviewed.receipt_key);
        hash.finalize().into()
    }
}

impl BranchIndex {
    pub fn check_custody_reservation(
        &self,
        group: CustodyGroupId,
        reservation: &CustodyReservation,
    ) -> Result<bool> {
        let record = self.live_custody(group)?;
        if let Some(prior) = record.reservations.get(&reservation.destination) {
            return if prior.reviewed.is_none() && &prior.reservation == reservation {
                Ok(false)
            } else {
                Err(placement_conflict())
            };
        }
        if reservation.destination == [0; 16]
            || reservation.link == [0; 16]
            || reservation.destination == reservation.link
            || reservation.review_sha256 == [0; 32]
            || reservation.carrier_sha256 == [0; 32]
            || reservation.link_sha256 == [0; 32]
            || record
                .original
                .as_ref()
                .map(|original| original.commitment(group))
                != Some(reservation.original)
            || record.occurrence(group, reservation.source).is_none()
            || self
                .used_custody_placements
                .contains(&reservation.destination)
            || self.used_custody_placements.contains(&reservation.link)
        {
            return Err(placement_conflict());
        }
        Ok(true)
    }

    fn check_custody_occurrence(
        &self,
        group: CustodyGroupId,
        occurrence: &CustodyOccurrence,
    ) -> Result<()> {
        let record = self.live_custody(group)?;
        match occurrence.reservation {
            None if record.original.is_none()
                && occurrence.link_sha256.is_none()
                && !self
                    .used_custody_placements
                    .contains(&occurrence.placement_id) =>
            {
                Ok(())
            }
            Some(commitment) => {
                let pending = record
                    .reservations
                    .get(&occurrence.placement_id)
                    .ok_or_else(placement_conflict)?;
                let reservation = &pending.reservation;
                if pending.occurrence.is_some()
                    || pending.at >= occurrence.written
                    || pending.commitment(self.head.basis.incarnation(), group) != commitment
                    || reservation.target.with_valid(reservation.target.valid.resolve(self.head.system_time)?) != occurrence.target
                    || reservation.carrier_sha256 != occurrence.carrier_sha256
                    || Some(reservation.link_sha256) != occurrence.link_sha256
                {
                    return Err(placement_conflict());
                }
                Ok(())
            }
            None => Err(placement_conflict()),
        }
    }

    pub fn next_custody_ordinal(&self) -> u64 {
        self.writes.len() as u64 + 1
    }

    pub fn custody_review_at(
        &self, branch: &str, basis: Basis, valid_at: Timestamp, group: CustodyGroupId, selected: [u8; 32],
    ) -> Result<(CustodyReview, &CustodyOccurrence, &CustodyOccurrence)> {
        if branch == MAIN_BRANCH || self.visibility_projection { return Err(placement_conflict()); }
        self.writable(branch)?;
        let record = self.live_custody(group)?;
        let inventory = record.inventory(group, basis).ok_or_else(placement_conflict)?;
        let original = record.original.as_ref().ok_or_else(placement_conflict)?;
        let source = record.occurrence(group, selected).ok_or_else(placement_conflict)?;
        let view = self.view(branch, At::Exactly(basis), Layer::Merged, self.head.system_time)?;
        let members = record.members.iter().filter(|member| member.written <= basis);
        let occurrences = || record.occurrences().filter(|occurrence| occurrence.written <= basis);
        if original.written > basis || source.written > basis
            || members.count() != occurrences().count()
            || occurrences().any(|occurrence| {
                occurrence.receipt_key.is_empty() || occurrence.ordinal.checked_sub(1).is_none_or(|ordinal| {
                    !record.members.contains(&super::custody::CustodyMember {
                        branch: occurrence.target.branch.clone(), table: occurrence.target.table.clone(),
                        id: occurrence.target.id.clone(), valid: occurrence.target.valid,
                        written: occurrence.written, ordinal,
                    })
                })
            })
            || self.local_custody_event(group, original).is_none()
            || !self.custody_winner(&view, group, source, valid_at)
        {
            return Err(placement_conflict());
        }
        let review = CustodyReview { branch: branch.into(), basis, valid_at, inventory_sha256: inventory.sha256 };
        Ok((review, original, source))
    }

    fn custody_winner(&self, view: &BranchView<'_>, group: CustodyGroupId, source: &CustodyOccurrence, valid_at: Timestamp) -> bool {
        let target = &source.target;
        view.row(&target.table, &target.id, valid_at)
            .zip(self.local_custody_event(group, source))
            .is_some_and(|(winner, authored)| std::ptr::eq(winner, authored))
    }

    pub fn check_current_custody_review(
        &self, group: CustodyGroupId, original: [u8; 32], selected: [u8; 32], review: &CustodyReview,
        except: Option<CustodyPlacementId>,
    ) -> Result<()> {
        let (observed, founding, source) = self.custody_review_at(
            &review.branch, review.basis, review.valid_at, group, selected,
        )?;
        let view = self.view(&review.branch, At::Latest, Layer::Merged, self.head.system_time)?;
        let current = self.live_custody(group)?.inventory_except(group, self.head.basis, except);
        if observed != *review || founding.commitment(group) != original
            || current.is_none_or(|inventory| inventory.sha256 != review.inventory_sha256)
            || view.data.changed.is_some_and(|changed| changed > review.basis)
            || !self.custody_winner(&view, group, source, review.valid_at)
        {
            return Err(placement_conflict());
        }
        Ok(())
    }

    pub fn check_reviewed_custody_reservation(
        &self, group: CustodyGroupId, reservation: &CustodyReservation, reviewed: &ReviewedCustodyReservation,
    ) -> Result<()> {
        if reservation.target.branch != MAIN_BRANCH || reviewed.source.basis >= self.head.basis
            || !(16..=64).contains(&reviewed.receipt_key.len())
            || !self.check_custody_reservation(group, reservation)?
        {
            return Err(placement_conflict());
        }
        self.check_current_custody_review(group, reservation.original, reservation.source, &reviewed.source, None)
    }

    pub fn check_custody_activation_route(
        &self, group: CustodyGroupId, destination: CustodyPlacementId,
        review: Option<(&str, Basis, &[u8])>,
    ) -> Result<()> {
        let record = self.live_custody(group)?;
        let pending = record.reservations.get(&destination);
        match (pending.and_then(|entry| entry.reviewed.as_ref()), review) {
            (None, None) => Ok(()),
            (Some(reviewed), Some((branch, basis, key))) => {
                let pending = pending.ok_or_else(placement_conflict)?;
                if reviewed.source.branch != branch || pending.at != basis || reviewed.receipt_key.as_slice() != key
                    || pending.occurrence.is_some()
                {
                    return Err(placement_conflict());
                }
                self.check_current_custody_review(
                    group, pending.reservation.original, pending.reservation.source, &reviewed.source, Some(destination),
                )
            }
            _ => Err(placement_conflict()),
        }
    }

    /// Resolve one exact commitment from a captured Query inventory. The
    /// caller must still confirm the selected occurrence's applied receipt.
    pub fn custody_read_occurrence(
        &self, selected: Basis, group: CustodyGroupId, commitment: [u8; 32],
    ) -> Result<([u8; 32], &CustodyOccurrence)> {
        self.exact(selected)?;
        let record = self.live_custody(group)?;
        let original = record.original.as_ref().ok_or_else(placement_conflict)?;
        let occurrence = record.occurrence(group, commitment).ok_or_else(placement_conflict)?;
        if original.written > selected || occurrence.written > selected || occurrence.receipt_key.is_empty() {
            return Err(placement_conflict());
        }
        self.local_custody_event(group, occurrence).ok_or_else(placement_conflict)?;
        Ok((original.commitment(group), occurrence))
    }

    pub fn custody_reservation_at(
        &self, group: CustodyGroupId, placement: CustodyPlacementId, at: Basis,
    ) -> Result<(&CustodyReservation, Basis)> {
        self.exact(at)?;
        let reservation = self.live_custody(group)?.reservations.get(&placement)
            .filter(|entry| entry.at <= at)
            .ok_or_else(placement_conflict)?;
        Ok((&reservation.reservation, reservation.at))
    }

    pub fn custody_occurrence_payload(
        &self, selected: Basis, group: CustodyGroupId, commitment: [u8; 32],
    ) -> Result<&super::CustodyPayload> {
        let (_, occurrence) = self.custody_read_occurrence(selected, group, commitment)?;
        self.local_custody_event(group, occurrence)
            .and_then(|event| event.image.as_ref()?.custody_payload.as_ref())
            .ok_or_else(placement_conflict)
    }

    fn local_custody_event(&self, group: CustodyGroupId, occurrence: &CustodyOccurrence) -> Option<&BranchEvent> {
        let target = &occurrence.target;
        let events = self.branches.get(&target.branch)?.tables.get(&target.table)?.events.get(&target.id)?;
        let first = events.partition_point(|(_, event)| event.cut.basis < occurrence.written);
        let at_write = events[first..].iter().map(|(_, event)| event)
            .take_while(|event| event.cut.basis == occurrence.written);
        if let Some(event) = at_write.clone().find(|event| event.written == WrittenVersion::Custody(Some(occurrence.ordinal))) {
            return occurrence.matches_event(group, event).then_some(event);
        }
        // Older checkpoints store the occurrence ordinal without stamping it on the row.
        let mut matches = at_write.filter(|event| event.written == WrittenVersion::Branch && occurrence.matches_event(group, event));
        let event = matches.next()?;
        matches.next().is_none().then_some(event)
    }

    pub(super) fn reserve_custody_placement(
        &mut self,
        group: CustodyGroupId,
        reservation: CustodyReservation,
    ) -> Result<()> {
        if !self.check_custody_reservation(group, &reservation)? {
            return Err(placement_conflict());
        }
        self.record_custody_reservation(group, reservation, None, None);
        Ok(())
    }

    pub(super) fn reserve_reviewed_custody_placement(
        &mut self, group: CustodyGroupId, reservation: CustodyReservation, reviewed: ReviewedCustodyReservation,
    ) -> Result<()> {
        self.check_reviewed_custody_reservation(group, &reservation, &reviewed)?;
        self.record_custody_reservation(group, reservation, None, Some(reviewed));
        Ok(())
    }

    fn record_custody_reservation(
        &mut self,
        group: CustodyGroupId,
        reservation: CustodyReservation,
        occurrence: Option<CustodyOccurrence>,
        reviewed: Option<ReviewedCustodyReservation>,
    ) {
        self.used_custody_placements.insert(reservation.destination);
        self.used_custody_placements.insert(reservation.link);
        let record = Arc::make_mut(self.custody.get_mut(&group).expect("checked custody group"));
        record.reservations.insert(
            reservation.destination,
            ReservedPlacement {
                at: self.head.basis,
                reservation,
                occurrence,
                reviewed,
            },
        );
    }

    fn derived_custody_reservation(
        &self,
        group: CustodyGroupId,
        occurrence: &CustodyOccurrence,
        derived: DerivedCustodyReservation,
    ) -> Result<CustodyReservation> {
        let original = self.live_custody(group)?.original.as_ref().ok_or_else(placement_conflict)?;
        if occurrence.reservation.is_some()
            || occurrence.receipt_key.is_empty()
            || original.placement_id != derived.original_placement_id
            || original.written != occurrence.written
            || original.receipt_key != occurrence.receipt_key
            || original.ordinal >= occurrence.ordinal
            || self.local_custody_event(group, original).is_none()
            || occurrence.carrier_sha256 != derived.carrier_sha256
            || occurrence.link_sha256 != Some(derived.link_sha256)
        {
            return Err(placement_conflict());
        }
        let original = original.commitment(group);
        let reservation = CustodyReservation {
            original,
            source: original,
            destination: occurrence.placement_id,
            link: derived.link,
            target: occurrence.target.with_valid(occurrence.target.valid.into()),
            review_sha256: derived.review_sha256,
            carrier_sha256: derived.carrier_sha256,
            link_sha256: derived.link_sha256,
        };
        if !self.check_custody_reservation(group, &reservation)? {
            return Err(placement_conflict());
        }
        Ok(reservation)
    }

    pub(super) fn activate_custody_occurrence(
        &mut self,
        group: CustodyGroupId,
        mut occurrence: CustodyOccurrence,
        derived: Option<DerivedCustodyReservation>,
    ) -> Result<()> {
        if occurrence.written != self.head.basis
            || occurrence.placement_id == [0; 16]
            || occurrence.carrier_sha256 == [0; 32]
            || occurrence.envelope_sha256 == [0; 32]
            || !occurrence.receipt_key.is_empty() && !(16..=64).contains(&occurrence.receipt_key.len())
        {
            return Err(placement_conflict());
        }
        let derived = if let Some(derived) = derived {
            let reservation = self.derived_custody_reservation(group, &occurrence, derived)?;
            occurrence.reservation = Some(reservation.commitment(self.head.basis.incarnation(), group));
            Some(reservation)
        } else {
            self.check_custody_occurrence(group, &occurrence)?;
            None
        };
        let target = &occurrence.target;
        let matching = self.writes.len() as u64 == occurrence.ordinal && self.writes.last().is_some_and(|write| {
            write.branch == target.branch
                && write.table == target.table
                && write.id == target.id
                && write.valid == Some(target.valid)
                && write.erased_written_basis.is_none()
        });
        let head = self.head;
        let event = self
            .branches
            .get_mut(&target.branch)
            .and_then(|branch| Arc::make_mut(branch).tables.get_mut(&target.table))
            .and_then(|rows| Arc::make_mut(rows).events.get_mut(&target.id))
            .and_then(|events| Arc::make_mut(events).last_mut())
            .map(|(_, event)| event)
            .filter(|event| {
                matching && event.cut == head && event.written == WrittenVersion::Custody(None)
                    && occurrence.matches_event(group, event)
            })
            .ok_or_else(|| Anomaly::fault(
                "rtdb/custody-fold",
                "Custody activation must follow its exact protected write",
            ))?;
        event.written = WrittenVersion::Custody(Some(occurrence.ordinal));
        if let Some(reservation) = derived {
            self.record_custody_reservation(group, reservation, Some(occurrence), None);
        } else if occurrence.reservation.is_some() {
            let placement_id = occurrence.placement_id;
            Arc::make_mut(self.custody.get_mut(&group).expect("checked custody group"))
                .reservations
                .get_mut(&placement_id)
                .expect("checked custody reservation")
                .occurrence = Some(occurrence);
        } else {
            self.used_custody_placements.insert(occurrence.placement_id);
            Arc::make_mut(self.custody.get_mut(&group).expect("checked custody group")).original = Some(occurrence);
        }
        Ok(())
    }
}

impl BranchView<'_> {
    pub fn custody_event_contributor(&self, table: &TableRef, event: &BranchEvent) -> Option<(CustodyGroupId, [u8; 32])> {
        if self.index.visibility_projection || event.cut.basis > self.cut.basis { return None; }
        let group = event.image.as_ref()?.custody_group?;
        let mut matches = self.index.live_custody(group).ok()?.occurrences().filter(|occurrence| {
            occurrence.written == event.cut.basis && occurrence.target.table == *table
                && occurrence.target.id == event.id
                && self.index.local_custody_event(group, occurrence).is_some_and(|local| std::ptr::eq(local, event))
        });
        let occurrence = matches.next()?;
        if matches.next().is_some() { return None; }
        Some((group, occurrence.commitment(group)))
    }
}

fn placement_conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/custody-placement",
        "Custody placement differs from its original, reservation or ordered occurrence",
    )
}
