use sha2::{Digest, Sha256};

use super::*;
use rtdb_types::rtdb_v1::ManagedCheckpointReservation;

mod dependent;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Forward {
    custody: NativeCheckpointCustody,
    destination: Option<Basis>,
    #[serde(default, with = "entries", skip_serializing_if = "Map::is_empty")]
    dependents: Map<CustodyGroupId, dependent::Dependent>,
}

impl BranchIndex {
    pub fn inherited_custody(&self) -> Option<&NativeCheckpointCustody> {
        self.inherited_custody.as_ref()
    }

    pub fn require_local_custody(&self) -> Result<()> {
        if self.inherited_custody.is_some() {
            return Err(Anomaly::unsupported(
                "rtdb/inherited-custody",
                "Inherited custody requires its original transfer and response authority",
            ));
        }
        Ok(())
    }

    pub fn native_checkpoint_at(&self, basis: Basis) -> Result<Self> {
        self.require_local_custody()?;
        self.exact(basis)?;
        if basis != self.head.basis || self.latest_custody_revocation.is_some() {
            return Err(Anomaly::unsupported(
                "rtdb/native-checkpoint-cut",
                "Native custody transfer requires the settled source head with live custody",
            ));
        }
        self.native_checkpoint_groups()?;
        let mut checkpoint = self.clone();
        checkpoint.custody_transfers = Map::new();
        Ok(checkpoint)
    }

    pub fn native_checkpoint_groups(&self) -> Result<Vec<(CustodyGroupId, [u8; 32])>> {
        self.require_local_custody()?;
        if self.custody.is_empty() || self.custody.len() > 512 {
            return Err(inventory_unavailable());
        }
        let mut retained = BTreeSet::new();
        for (branch, table, event, image) in self.retained_images() {
            if Self::unowned_checkpoint_image(table, image) {
                return Err(inventory_unavailable());
            }
            match (&image.custody_group, &image.custody_payload) {
                (None, None) => (),
                (Some(group), Some(payload)) => {
                    let occurrence = self.checkpoint_occurrence(
                        *group,
                        branch,
                        table,
                        event,
                        &payload.envelope,
                    )?;
                    if !retained.insert((*group, occurrence.commitment(*group))) {
                        return Err(inventory_unavailable());
                    }
                }
                _ => return Err(inventory_unavailable()),
            }
        }

        let mut groups = Vec::with_capacity(self.custody.len());
        for (&group, record) in &self.custody {
            if record.original.is_none()
                || record.revoked.is_some()
                || record.members.len() != record.occurrences().count()
                || record
                    .occurrences()
                    .any(|occurrence| !retained.remove(&(group, occurrence.commitment(group))))
            {
                return Err(inventory_unavailable());
            }
            groups.push((group, self.native_group_commitment(group)?));
        }
        if !retained.is_empty() {
            return Err(inventory_unavailable());
        }
        Ok(groups)
    }

    pub fn native_checkpoint_receipts(&self) -> impl Iterator<Item = (&[u8], Basis)> {
        self.custody
            .values()
            .flat_map(|group| group.occurrences())
            .map(|occurrence| (occurrence.receipt_key.as_slice(), occurrence.written))
    }

    pub fn unowned_checkpoint_image(table: &TableRef, image: &RowImage) -> bool {
        Self::has_unowned_checkpoint_carrier(table, &image.cells)
    }

    /// Identifies owned relations and carrier encodings that need their Original custody authority.
    pub fn has_unowned_checkpoint_carrier(table: &TableRef, cells: &rtdb_types::StructFields<Value>) -> bool {
        TableRef::owned_schema(&table.schema) || cells.iter().any(|(column, value)| unowned_carrier(column, value))
    }

    fn native_group_commitment(&self, group: CustodyGroupId) -> Result<[u8; 32]> {
        let record = self.live_custody(group)?;
        let inventory = record
            .inventory(group, self.head.basis)
            .ok_or_else(inventory_unavailable)?;
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/native-checkpoint-group\0");
        hash.update(inventory.sha256);
        for (attempt, release) in &record.releases {
            let registration = release.registration;
            hash.update(attempt);
            hash.update(registration.authority.installation);
            hash.update(registration.authority.registry);
            hash.update(registration.authority.original_writer);
            hash.update(registration.carrier_receipt_sha256);
            hash.update(registration.expires_at.0.to_be_bytes());
            match release.phase {
                CustodyReleasePhase::Private => hash.update([0]),
                CustodyReleasePhase::Promoted => hash.update([1]),
                CustodyReleasePhase::Cancelled => hash.update([2]),
                CustodyReleasePhase::Disposed { evidence_sha256 } => {
                    hash.update([3]);
                    hash.update(evidence_sha256);
                }
            }
        }
        Ok(hash.finalize().into())
    }

    pub fn native_checkpoint_forward(
        &self,
        reservation: ManagedCheckpointReservation,
    ) -> Result<Option<(&NativeCheckpointCustody, Option<Basis>)>> {
        let Some(forward) = self.custody_transfers.get(&reservation.target.incarnation) else {
            return Ok(None);
        };
        if forward.custody.reservation != reservation {
            return Err(transfer_conflict());
        }
        Ok(Some((&forward.custody, forward.destination)))
    }

    pub fn native_checkpoint_pin_used(&self, pin: [u8; 16]) -> bool {
        self.custody_transfers
            .values()
            .any(|forward| forward.custody.reservation.source.pin == pin)
    }

    pub fn check_native_custody_transfer(
        &self,
        custody: &NativeCheckpointCustody,
        destination: Option<Basis>,
    ) -> Result<()> {
        self.require_local_custody()?;
        require_binding_shape(custody)?;
        let reservation = custody.reservation;
        if reservation.source.basis.incarnation() != self.head.basis.incarnation() {
            return Err(transfer_conflict());
        }
        let prior = self.native_checkpoint_forward(reservation)?;
        match (destination, prior) {
            (None, None)
                if custody.enrolled_at == self.head.basis
                    && !self.native_checkpoint_pin_used(reservation.source.pin) =>
            {
                for &(group, expected) in &custody.groups {
                    if self.native_group_commitment(group)? != expected {
                        return Err(transfer_conflict());
                    }
                }
                Ok(())
            }
            (Some(destination), Some((prior, None)))
                if prior == custody
                    && destination
                        == Basis::new(
                            reservation.target.incarnation,
                            reservation.source.basis.position(),
                        ) =>
            {
                Ok(())
            }
            _ => Err(transfer_conflict()),
        }
    }

    pub(super) fn apply_native_custody_transfer(
        &mut self,
        custody: NativeCheckpointCustody,
        destination: Option<Basis>,
    ) -> Result<()> {
        self.check_native_custody_transfer(&custody, destination)?;
        self.custody_transfers.insert(
            custody.reservation.target.incarnation,
            Forward {
                custody,
                destination,
                dependents: Map::new(),
            },
        );
        Ok(())
    }

    pub(super) fn custody_inventory_at(
        &self,
        group: CustodyGroupId,
        at: Basis,
    ) -> Option<CustodyInventory> {
        if self.inherited_custody.is_some() {
            return None;
        }
        let mut inventory = self.custody.get(&group)?.inventory(group, at)?;
        let forwards = self.custody_transfers.values().filter(|forward| {
            forward.custody.enrolled_at <= at && contains(&forward.custody, group)
        });
        let mut hash = None::<Sha256>;
        for forward in forwards {
            let reservation = forward.custody.reservation;
            let hash = hash.get_or_insert_with(|| {
                let mut hash = Sha256::new();
                hash.update(b"rtdb/v1/native-checkpoint-fanout\0");
                hash.update(inventory.sha256);
                hash
            });
            hash.update(reservation.source.basis.to_bytes());
            hash.update(reservation.source.pin);
            hash.update(reservation.source.manifest_sha256);
            hash.update(reservation.target.incarnation);
            hash.update(reservation.target.request);
            hash.update(reservation.reserved_at.to_bytes());
            hash.update(forward.custody.enrolled_at.to_bytes());
        }
        if let Some(hash) = hash {
            inventory.sha256 = hash.finalize().into();
        }
        Some(inventory)
    }

    pub(super) fn native_custody_dependents_drained(&self, group: CustodyGroupId) -> bool {
        self.custody_transfers
            .values()
            .filter(|forward| contains(&forward.custody, group))
            .all(|forward| forward.destination.is_some()
                && forward.dependents.get(&group).is_some_and(|dependent| dependent.disposition.is_some()))
    }

    pub fn restore_native_checkpoint(&mut self, custody: &NativeCheckpointCustody) -> Result<()> {
        require_binding_shape(custody)?;
        let reservation = custody.reservation;
        if self.head.basis != reservation.source.basis
            || !self.custody_transfers.is_empty()
            || self.native_checkpoint_groups()? != custody.groups
        {
            return Err(transfer_conflict());
        }
        self.rebase_history(reservation.target.incarnation);
        self.inherited_custody = Some(custody.clone());
        Ok(())
    }

    pub fn native_inherited_carrier(
        &self,
        branch: &str,
        table: &TableRef,
        id: &EntityId,
        envelope: &[u8],
    ) -> Result<(CustodyGroupId, [u8; 32])> {
        let custody = self
            .inherited_custody
            .as_ref()
            .ok_or_else(transfer_conflict)?;
        let mut found = None;
        for (name, relation, event, image) in self.retained_images() {
            if name != branch
                || relation != table
                || event.id != *id
                || !image
                    .custody_payload
                    .as_ref()
                    .is_some_and(|payload| payload.envelope == envelope)
            {
                continue;
            }
            let group = image
                .custody_group
                .filter(|&group| contains(custody, group))
                .ok_or_else(transfer_conflict)?;
            let occurrence = self.checkpoint_occurrence(group, branch, table, event, envelope)?;
            if found
                .replace((group, occurrence.commitment(group)))
                .is_some()
            {
                return Err(transfer_conflict());
            }
        }
        found.ok_or_else(transfer_conflict)
    }

    fn checkpoint_occurrence(
        &self,
        group: CustodyGroupId,
        branch: &str,
        table: &TableRef,
        event: &BranchEvent,
        envelope: &[u8],
    ) -> Result<&CustodyOccurrence> {
        let source = self
            .inherited_custody
            .as_ref()
            .map_or(event.cut.basis.incarnation(), |custody| {
                custody.reservation.source.basis.incarnation()
            });
        let written = Basis::new(source, event.cut.basis.position());
        let digest: [u8; 32] = Sha256::digest(envelope).into();
        let record = self.live_custody(group)?;
        let mut matches = record.occurrences().filter(|occurrence| {
            occurrence.written == written
                && occurrence.target.branch == branch
                && occurrence.target.table == *table
                && occurrence.target.id == event.id
                && occurrence.target.valid == event.valid
                && occurrence.envelope_sha256 == digest
                && !occurrence.receipt_key.is_empty()
        });
        let occurrence = matches.next().ok_or_else(inventory_unavailable)?;
        let ordinal = occurrence
            .ordinal
            .checked_sub(1)
            .ok_or_else(inventory_unavailable)?;
        if matches.next().is_some()
            || !record.members.contains(&custody::CustodyMember {
                branch: branch.to_owned(),
                table: table.clone(),
                id: event.id.clone(),
                written,
                valid: event.valid,
                ordinal,
            })
        {
            return Err(inventory_unavailable());
        }
        Ok(occurrence)
    }

    pub fn check_native_export_read(
        &self,
        custody: &NativeCheckpointCustody,
        destination: Basis,
        group: CustodyGroupId,
        occurrence: [u8; 32],
    ) -> Result<()> {
        if !contains(custody, group)
            || self.native_checkpoint_forward(custody.reservation)?
                != Some((custody, Some(destination)))
            || self
                .live_custody(group)?
                .occurrence(group, occurrence)
                .is_none()
        {
            return Err(transfer_conflict());
        }
        for &(group, _) in &custody.groups {
            self.live_custody(group)?;
        }
        Ok(())
    }
}

fn require_binding_shape(custody: &NativeCheckpointCustody) -> Result<()> {
    let reservation = custody.reservation;
    if reservation.target.incarnation == reservation.source.basis.incarnation()
        || reservation.target.incarnation == [0; 16]
        || reservation.source.pin == [0; 16]
        || reservation.source.until.is_some()
        || reservation.target.request == [0; 32]
        || reservation.source.manifest_sha256 == [0; 32]
        || reservation.reserved_at.incarnation() == [0; 16]
        || reservation.reserved_at.position() == 0
        || custody.enrolled_at.incarnation() != reservation.source.basis.incarnation()
        || custody.enrolled_at <= reservation.source.basis
        || custody.groups.is_empty()
        || custody.groups.len() > 512
        || custody.groups.windows(2).any(|pair| pair[0].0 >= pair[1].0)
    {
        return Err(transfer_conflict());
    }
    Ok(())
}

fn contains(custody: &NativeCheckpointCustody, group: CustodyGroupId) -> bool {
    custody
        .groups
        .binary_search_by_key(&group, |(group, _)| *group)
        .is_ok()
}

fn unowned_carrier(column: &str, value: &Value) -> bool {
    matches!(
        column,
        "sealed_payload" | "sealed_world_projection" | "sealed_root_projection"
    ) || match value {
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => [
            b"BJWC1".as_slice(),
            b"BJWP1",
            b"BJRL1",
            b"RTAS\x02",
            b"RTDBE\x01",
        ]
        .iter()
        .any(|prefix| bytes.starts_with(prefix)),
        Value::Struct(fields) => fields
            .iter()
            .any(|(column, value)| unowned_carrier(column, value)),
        Value::Set(values) => values.iter().any(|value| unowned_carrier("", value)),
        Value::Map { entries, .. } => entries
            .iter()
            .any(|(key, value)| unowned_carrier("", key) || unowned_carrier("", value)),
        Value::List(values) | Value::FixedSizeList(values) => values
            .view()
            .iter()
            .any(|value| unowned_carrier("", value.as_ref())),
        _ => false,
    }
}

fn transfer_conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/native-checkpoint-binding",
        "The native transfer differs from its original and reciprocal checkpoint binding",
    )
}

fn inventory_unavailable() -> Anomaly {
    Anomaly::unsupported(
        "rtdb/native-checkpoint-inventory",
        "The retained checkpoint requires complete native carrier and group evidence",
    )
}
