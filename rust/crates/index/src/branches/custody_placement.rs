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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ReservedPlacement {
    pub at: Basis,
    pub reservation: CustodyReservation,
    pub occurrence: Option<CustodyOccurrence>,
}

impl BranchIndex {
    pub fn check_custody_reservation(
        &self,
        group: CustodyGroupId,
        reservation: &CustodyReservation,
    ) -> Result<bool> {
        let record = self.live_custody(group)?;
        if let Some(prior) = record.reservations.get(&reservation.destination) {
            return if &prior.reservation == reservation {
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
                    || reservation.commitment(self.head.basis.incarnation(), group) != commitment
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
        Ok((original.commitment(group), occurrence))
    }

    pub(super) fn reserve_custody_placement(
        &mut self,
        group: CustodyGroupId,
        reservation: CustodyReservation,
    ) -> Result<()> {
        if !self.check_custody_reservation(group, &reservation)? {
            return Err(placement_conflict());
        }
        self.record_custody_reservation(group, reservation, None);
        Ok(())
    }

    fn record_custody_reservation(
        &mut self,
        group: CustodyGroupId,
        reservation: CustodyReservation,
        occurrence: Option<CustodyOccurrence>,
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
        let matching = self.writes.last().is_some_and(|write| {
            write.branch == target.branch
                && write.table == target.table
                && write.id == target.id
                && write.valid == Some(target.valid)
                && write.erased_written_basis.is_none()
        });
        let payload = self
            .branches
            .get(&target.branch)
            .and_then(|branch| branch.tables.get(&target.table))
            .and_then(|rows| rows.events.get(&target.id))
            .and_then(|events| events.last())
            .filter(|(_, event)| event.cut == self.head && event.valid == target.valid)
            .and_then(|(_, event)| event.image.as_ref())
            .filter(|image| image.custody_group == Some(group))
            .and_then(|image| image.custody_payload.as_ref());
        if !matching
            || self.writes.len() as u64 != occurrence.ordinal
            || !payload.is_some_and(|payload| {
                <[u8; 32]>::from(Sha256::digest(&payload.envelope)) == occurrence.envelope_sha256
            })
        {
            return Err(Anomaly::fault(
                "rtdb/custody-fold",
                "Custody activation must follow its exact protected write",
            ));
        }
        if let Some(reservation) = derived {
            self.record_custody_reservation(group, reservation, Some(occurrence));
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
        if self.index.visibility_projection { return None; }
        let image = event.image.as_ref()?;
        let group = image.custody_group?;
        let payload = image.custody_payload.as_ref()?;
        let envelope_sha256: [u8; 32] = Sha256::digest(&payload.envelope).into();
        let mut matches = self.index.custody.get(&group)?.occurrences().filter(|occurrence| {
            occurrence.written == event.cut.basis && occurrence.target.table == *table
                && occurrence.target.id == event.id && occurrence.target.valid == event.valid
                && occurrence.envelope_sha256 == envelope_sha256
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
