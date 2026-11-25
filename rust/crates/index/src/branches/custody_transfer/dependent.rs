use rtdb_types::rtdb_v1::{
    DependentCustodyReceipt, DependentKeyCustody, DependentKeyTerminal, NativeCheckpointMember,
    NativeCustodyFence, NativeDependentCheckpointMember, NativeDependentCheckpointMetadata,
    NativeDependentCheckpointRow, NativeDependentCustodyDisposition,
    NativeDependentCustodyEnrollment, NativeDependentCustodyScope, NativeDependentPoint,
    NativeDependentRead,
};

use super::*;

const MAX_MEMBERS: usize = 512;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Dependent {
    enrollment: NativeDependentCustodyEnrollment,
    pub(super) disposition: Option<NativeDependentCustodyDisposition>,
}

struct PointSlot<'a> {
    branch: &'a str,
    table: &'a TableRef,
    id: &'a EntityId,
    valid_at: Timestamp,
    placement: CustodyPlacementId,
}

impl BranchIndex {
    pub fn check_native_dependent_destination(&self, read: &NativeDependentRead, now: Timestamp) -> Result<&NativeCheckpointCustody> {
        self.native_dependent_destination(read, now).map(|(custody, _)| custody)
    }

    pub fn native_dependent_destination(
        &self, read: &NativeDependentRead, now: Timestamp,
    ) -> Result<(&NativeCheckpointCustody, [u8; 32])> {
        let custody = self.native_destination_scope(read.scope)?;
        let (_, payload) = self.native_dependent_point(read, read.scope.destination, now)?;
        Ok((custody, payload.binding))
    }

    pub fn native_dependent_point_selector(
        &self, point: &NativeDependentPoint, now: Timestamp,
    ) -> Result<(Basis, Interval, NativeDependentCheckpointMember)> {
        let scope = point.scope;
        self.native_destination_scope(scope)?;
        let slot = PointSlot {
            branch: &point.branch,
            table: &point.table,
            id: &point.id,
            valid_at: point.valid_at,
            placement: point.placement,
        };
        let (created, occurrence, _) =
            self.native_dependent_winner(scope.group, slot, scope.destination, now)?;
        let enrolled = if let Some(reservation) = occurrence.reservation {
            let entry = self.live_custody(scope.group)?.reservations
                .get(&point.placement).ok_or_else(dependent_conflict)?;
            if entry.at > occurrence.written
                || entry.commitment(scope.reservation.source.basis.incarnation(), scope.group) != reservation
            {
                return Err(dependent_conflict());
            }
            entry.at
        } else {
            occurrence.written
        };
        Ok((created, occurrence.target.valid, NativeDependentCheckpointMember {
            native: NativeCheckpointMember {
                placement: occurrence.placement_id,
                reservation: occurrence.reservation,
                occurrence: Some(occurrence.commitment(scope.group)),
                carrier_sha256: occurrence.carrier_sha256,
                link_sha256: occurrence.link_sha256,
            },
            source_basis: occurrence.written,
            enrolled,
            branch: occurrence.target.branch.clone(),
            table: occurrence.target.table.to_string(),
            target: occurrence.target.id.clone(),
        }))
    }

    fn native_destination_scope(&self, scope: NativeDependentCustodyScope) -> Result<&NativeCheckpointCustody> {
        let custody = self.inherited_custody().ok_or_else(dependent_conflict)?;
        if custody.reservation != scope.reservation
            || custody.enrolled_at != scope.source_enrolled_at
            || scope.destination.incarnation() != scope.reservation.target.incarnation
            || scope.destination.position() != scope.reservation.source.basis.position()
            || custody.groups.binary_search(&(scope.group, scope.checkpoint_group_sha256)).is_err()
        {
            return Err(dependent_conflict());
        }
        Ok(custody)
    }

    pub fn native_dependent_original(
        &self,
        custody: &NativeCheckpointCustody,
        read: &NativeDependentRead,
        now: Timestamp,
    ) -> Result<(&NativeDependentCustodyEnrollment, [u8; 32], &CustodyOccurrence)> {
        let scope = read.scope;
        self.check_native_export_read(custody, scope.destination, scope.group, read.occurrence)?;
        let Some((enrollment, None)) = self.native_dependent_custody(scope)? else {
            return Err(dependent_conflict());
        };
        let pinned = scope.reservation.source.basis;
        let (selected, _) = self.native_dependent_point(read, pinned, now)?;
        let (original, occurrence) = self.custody_read_occurrence(pinned, scope.group, read.occurrence)?;
        let member = NativeCheckpointMember {
            placement: occurrence.placement_id,
            reservation: occurrence.reservation,
            occurrence: Some(read.occurrence),
            carrier_sha256: occurrence.carrier_sha256,
            link_sha256: occurrence.link_sha256,
        };
        if selected != occurrence {
            return Err(dependent_conflict());
        }
        if !enrollment.members.iter().any(|enrolled| {
            enrolled.native == member && enrolled.application_keys.iter().any(|key| {
                key.key == read.application_key
                    && key.carriers.contains(&member.carrier_sha256)
                    && member.link_sha256.is_none_or(|link| key.carriers.contains(&link))
            })
        }) {
            return Err(dependent_conflict());
        }
        Ok((enrollment, original, occurrence))
    }

    pub fn native_dependent_contributors(&self, read: &NativeDependentRead) -> Result<BTreeMap<[u8; 32], &CustodyOccurrence>> {
        let scope = read.scope;
        let cut = scope.reservation.source.basis;
        let group = self.live_custody(scope.group)?;
        let Some((enrollment, None)) = self.native_dependent_custody(scope)? else {
            return Err(dependent_conflict());
        };
        let mut contributors = BTreeMap::new();
        let mut selected = read.occurrence;
        let mut address_bytes = 64_usize * 1024;
        loop {
            let (original, occurrence) = self.custody_read_occurrence(cut, scope.group, selected)?;
            let target = &occurrence.target;
            let id_bytes = match &target.id {
                EntityId::String(id) => id.len(),
                EntityId::Keyword(id) => id.as_str().len(),
                EntityId::Binary(id) => id.len(),
                EntityId::Integer(_) | EntityId::Uuid(_) => 16,
            };
            address_bytes = address_bytes
                .checked_sub(id_bytes + target.branch.len() + target.table.schema.len() + target.table.table.len())
                .ok_or_else(dependent_conflict)?;
            if contributors.len() == 32 || contributors.insert(selected, occurrence).is_some()
                || !enrollment.members.iter().any(|member| {
                    member.native.placement == occurrence.placement_id
                        && member.native.occurrence == Some(selected)
                })
            {
                return Err(dependent_conflict());
            }
            if selected == original {
                if occurrence.reservation.is_some() { return Err(dependent_conflict()); }
                return Ok(contributors);
            }
            let reservation = group.reservations.get(&occurrence.placement_id).ok_or_else(dependent_conflict)?;
            if reservation.reservation.original != original || reservation.at > occurrence.written
                || Some(reservation.commitment(cut.incarnation(), scope.group)) != occurrence.reservation
            {
                return Err(dependent_conflict());
            }
            selected = reservation.reservation.source;
        }
    }

    fn native_dependent_point<'a>(
        &'a self, read: &'a NativeDependentRead, pinned: Basis, now: Timestamp,
    ) -> Result<(&'a CustodyOccurrence, &'a CustodyPayload)> {
        let slot = PointSlot {
            branch: &read.branch,
            table: &read.table,
            id: &read.id,
            valid_at: read.valid_at,
            placement: read.placement,
        };
        let (created, occurrence, envelope) = self.native_dependent_winner(read.scope.group, slot, pinned, now)?;
        if read.branch_created != Basis::new(read.scope.destination.incarnation(), created.position())
            || occurrence.commitment(read.scope.group) != read.occurrence
        {
            return Err(dependent_conflict());
        }
        Ok((occurrence, envelope))
    }

    fn native_dependent_winner<'a>(
        &'a self, group: CustodyGroupId, slot: PointSlot<'a>, pinned: Basis, now: Timestamp,
    ) -> Result<(Basis, &'a CustodyOccurrence, &'a CustodyPayload)> {
        let historical = self.view(slot.branch, At::Exactly(pinned), Layer::Local, now)?;
        let current = self.view(slot.branch, At::Latest, Layer::Local, now)?;
        let created = historical.data.created;
        let selected = historical.row(slot.table, slot.id, slot.valid_at).ok_or_else(dependent_conflict)?;
        if current.data.created != created
            || current.row(slot.table, slot.id, slot.valid_at).is_none_or(|row| !std::ptr::eq(row, selected))
        {
            return Err(dependent_conflict());
        }
        let envelope = selected.image_at(now.max(self.head.system_time))
            .filter(|image| image.custody_group == Some(group))
            .and_then(|image| image.custody_payload.as_ref())
            .ok_or_else(dependent_conflict)?;
        let occurrence = self.checkpoint_occurrence(
            group, slot.branch, slot.table, selected, &envelope.envelope, Some(slot.placement),
        )?;
        if occurrence.placement_id != slot.placement
            || matches!(selected.written, WrittenVersion::Custody(Some(ordinal)) if ordinal != occurrence.ordinal)
        {
            return Err(dependent_conflict());
        }
        Ok((created, occurrence, envelope))
    }

    pub fn native_dependent_custody_roster(
        &self,
        scope: NativeDependentCustodyScope,
    ) -> Result<Vec<NativeCheckpointMember>> {
        self.native_dependent_forward(scope)?;
        Ok(self
            .native_checkpoint_members(scope)?
            .into_iter()
            .map(|member| member.native)
            .collect())
    }

    fn native_checkpoint_members(
        &self,
        scope: NativeDependentCustodyScope,
    ) -> Result<Vec<NativeDependentCheckpointMember>> {
        let group = &self.custody[&scope.group];
        let at = scope.reservation.source.basis;
        let mut members = Vec::new();
        for entry in self
            .custody_placements(at)
            .filter(|entry| entry.group == scope.group)
        {
            let (carrier_sha256, link_sha256, branch, table, target) = match entry.occurrence {
                Some(occurrence) => (
                    occurrence.carrier_sha256,
                    occurrence.link_sha256,
                    &occurrence.target.branch,
                    &occurrence.target.table,
                    &occurrence.target.id,
                ),
                None => {
                    let pending = &group.reservations[&entry.placement_id].reservation;
                    (
                        pending.carrier_sha256,
                        Some(pending.link_sha256),
                        &pending.target.branch,
                        &pending.target.table,
                        &pending.target.id,
                    )
                }
            };
            if members.len() == MAX_MEMBERS
                || entry.placement_id == [0; 16]
                || carrier_sha256 == [0; 32]
                || link_sha256 == Some([0; 32])
            {
                return Err(inventory_unavailable());
            }
            members.push(NativeDependentCheckpointMember {
                native: NativeCheckpointMember {
                    placement: entry.placement_id,
                    reservation: entry.reservation,
                    occurrence: entry
                        .occurrence
                        .map(|occurrence| occurrence.commitment(scope.group)),
                    carrier_sha256,
                    link_sha256,
                },
                source_basis: entry
                    .occurrence
                    .map_or(entry.enrolled, |occurrence| occurrence.written),
                enrolled: entry.enrolled,
                branch: branch.clone(),
                table: table.to_string(),
                target: target.clone(),
            });
        }
        if members.is_empty() {
            return Err(inventory_unavailable());
        }
        members.sort_unstable_by_key(|member| member.native.placement);
        Ok(members)
    }

    pub fn native_dependent_checkpoint_metadata(
        &self,
        scope: NativeDependentCustodyScope,
        relation: &TableRef,
        body_column: &str,
        max_body_bytes: usize,
    ) -> Result<NativeDependentCheckpointMetadata> {
        let custody = self.inherited_custody().ok_or_else(dependent_conflict)?;
        if custody.reservation != scope.reservation
            || custody.enrolled_at != scope.source_enrolled_at
            || self.head.basis != scope.destination
            || custody
                .groups
                .binary_search(&(scope.group, scope.checkpoint_group_sha256))
                .is_err()
        {
            return Err(dependent_conflict());
        }
        let definition = self
            .definition(relation, scope.destination)
            .filter(|table| !table.dropped && table.visibility.is_none())
            .ok_or_else(dependent_conflict)?;
        let members = self.native_checkpoint_members(scope)?;
        let prefix = format!("{:032x}:", u128::from_be_bytes(scope.group));
        let prefix_id = EntityId::String(prefix.clone());
        let mut rows = Vec::new();
        let mut remaining = max_body_bytes;
        for (branch, data) in &self.branches {
            let Some(table) = data.tables.get(relation) else {
                continue;
            };
            if table.erased.range(prefix_id.clone()..).next().is_some_and(
                |(id, _)| matches!(id, EntityId::String(id) if id.starts_with(&prefix)),
            ) {
                return Err(inventory_unavailable());
            }
            for (id, history) in table.events.range(prefix_id.clone()..) {
                let EntityId::String(id) = id else {
                    break;
                };
                if !id.starts_with(&prefix) {
                    break;
                }
                if id.len() > 255 {
                    return Err(inventory_unavailable());
                }
                for (generation, event) in history.iter() {
                    let image = event.image.as_ref().ok_or_else(inventory_unavailable)?;
                    if image.custody_group.is_some()
                        || image.custody_payload.is_some()
                        || image.expires_at.is_some()
                        || rows.len() == 2 * MAX_MEMBERS
                        || event.written != WrittenVersion::Branch
                        || *generation != definition.generation
                    {
                        return Err(inventory_unavailable());
                    }
                    let Some(Value::Binary(body) | Value::FixedSizeBinary(body)) =
                        image.cells.get(body_column)
                    else {
                        return Err(inventory_unavailable());
                    };
                    if body.is_empty() {
                        return Err(inventory_unavailable());
                    }
                    remaining = remaining
                        .checked_sub(body.len())
                        .ok_or_else(inventory_unavailable)?;
                    rows.push(NativeDependentCheckpointRow {
                        branch: branch.clone(),
                        id: id.clone(),
                        body: body.clone(),
                        written: event.cut.basis,
                    });
                }
            }
        }
        if rows.is_empty() {
            return Err(inventory_unavailable());
        }
        Ok(NativeDependentCheckpointMetadata {
            scope,
            members,
            rows,
        })
    }

    pub fn native_dependent_custody(
        &self,
        scope: NativeDependentCustodyScope,
    ) -> Result<
        Option<(
            &NativeDependentCustodyEnrollment,
            Option<&NativeDependentCustodyDisposition>,
        )>,
    > {
        Ok(self
            .native_dependent_forward(scope)?
            .dependents
            .get(&scope.group)
            .map(|dependent| (&dependent.enrollment, dependent.disposition.as_ref())))
    }

    pub fn require_native_dependent_publication(
        &self,
        reservation: ManagedCheckpointReservation,
        basis: Basis,
    ) -> Result<()> {
        self.require_local_custody()?;
        let forward = self
            .custody_transfers
            .get(&reservation.target.incarnation)
            .ok_or_else(dependent_conflict)?;
        if forward.custody.reservation != reservation
            || forward.destination != Some(basis)
            || forward
                .custody
                .groups
                .iter()
                .any(|(group, _)| !forward.dependents.contains_key(group))
        {
            return Err(dependent_conflict());
        }
        Ok(())
    }

    fn native_dependent_forward(&self, scope: NativeDependentCustodyScope) -> Result<&Forward> {
        self.require_local_custody()?;
        let forward = self
            .custody_transfers
            .get(&scope.reservation.target.incarnation)
            .filter(|forward| {
                forward.custody.reservation == scope.reservation
                    && forward.custody.enrolled_at == scope.source_enrolled_at
                    && forward.destination == Some(scope.destination)
                    && forward
                        .custody
                        .groups
                        .binary_search(&(scope.group, scope.checkpoint_group_sha256))
                        .is_ok()
                    && self.custody.contains_key(&scope.group)
            })
            .ok_or_else(dependent_conflict)?;
        Ok(forward)
    }

    pub fn require_native_dependent_fence(
        &self,
        scope: NativeDependentCustodyScope,
        fence: &NativeCustodyFence,
    ) -> Result<()> {
        self.native_dependent_forward(scope)?;
        let inventory = self
            .custody_inventory(scope.group)
            .ok_or_else(dependent_conflict)?;
        if inventory.revoked_at != Some(fence.revoked_at)
            || inventory.sha256 != fence.inventory_sha256
            || !(16..=64).contains(&fence.revoke_key.len())
        {
            return Err(dependent_conflict());
        }
        Ok(())
    }

    pub fn check_native_dependent_custody(
        &self,
        order: &NativeDependentCustodyOrder,
    ) -> Result<()> {
        let scope = order.scope();
        let forward = self.native_dependent_forward(scope)?;
        let prior = forward.dependents.get(&scope.group);
        match order {
            NativeDependentCustodyOrder::Enroll(enrollment) => {
                if prior.is_some()
                    || enrollment.owner == [0; 32]
                    || enrollment.application_scope == [0; 32]
                    || forward.dependents.values().next().is_some_and(|prior| {
                        prior.enrollment.owner != enrollment.owner
                            || prior.enrollment.application_scope != enrollment.application_scope
                    })
                    || !positive_receipt(enrollment.receipt)
                    || !self
                        .native_dependent_custody_roster(scope)?
                        .into_iter()
                        .eq(enrollment.members.iter().map(|member| member.native))
                    || enrollment
                        .members
                        .iter()
                        .map(|member| member.application_keys.len())
                        .try_fold(0_usize, |total, len| total.checked_add(len))
                        .is_none_or(|total| total > MAX_MEMBERS)
                    || enrollment.members.iter().any(|member| {
                        let keys = &member.application_keys;
                        keys.is_empty()
                            || keys.len() > MAX_MEMBERS
                            || keys.windows(2).any(|pair| pair[0].key >= pair[1].key)
                            || keys.iter().any(|key| {
                                key.key == [0; 32]
                                    || key.carriers.is_empty()
                                    || key.carriers.len() > 2
                                    || key.carriers.windows(2).any(|pair| pair[0] >= pair[1])
                            })
                            || keys
                                .iter()
                                .flat_map(|key| key.carriers.iter().copied())
                                .collect::<BTreeSet<_>>()
                                != BTreeSet::from_iter(
                                    [member.native.carrier_sha256]
                                        .into_iter()
                                        .chain(member.native.link_sha256),
                                )
                    })
                {
                    return Err(dependent_conflict());
                }
                application_keys(enrollment)?;
            }
            NativeDependentCustodyOrder::Dispose(disposition) => {
                let Some(Dependent {
                    enrollment,
                    disposition: None,
                }) = prior
                else {
                    return Err(dependent_conflict());
                };
                self.require_native_dependent_fence(scope, &disposition.fence)?;
                let keys = application_keys(enrollment)?;
                if disposition.enrollment != enrollment.receipt
                    || !positive_receipt(disposition.receipt)
                    || !keys.into_iter().eq(disposition.keys.iter().map(|key| {
                        let custody = match key.terminal {
                            DependentKeyTerminal::Destroyed(_) => {
                                DependentKeyCustody::DestinationOwned
                            }
                            DependentKeyTerminal::ReleasedOriginalReference(_) => {
                                DependentKeyCustody::OriginalReference
                            }
                        };
                        (key.key, custody)
                    }))
                    || disposition.keys.iter().any(|key| {
                        let evidence = match key.terminal {
                            DependentKeyTerminal::Destroyed(evidence)
                            | DependentKeyTerminal::ReleasedOriginalReference(evidence) => evidence,
                        };
                        [
                            key.reader_drain_sha256,
                            key.part_disposition_sha256,
                            evidence,
                        ]
                        .contains(&[0; 32])
                    })
                {
                    return Err(dependent_conflict());
                }
            }
        }
        Ok(())
    }

    pub(in crate::branches) fn apply_native_dependent_custody(
        &mut self,
        order: NativeDependentCustodyOrder,
    ) -> Result<()> {
        self.check_native_dependent_custody(&order)?;
        let scope = order.scope();
        let forward = self
            .custody_transfers
            .get_mut(&scope.reservation.target.incarnation)
            .ok_or_else(dependent_conflict)?;
        match order {
            NativeDependentCustodyOrder::Enroll(enrollment) => {
                forward.dependents.insert(
                    scope.group,
                    Dependent {
                        enrollment,
                        disposition: None,
                    },
                );
            }
            NativeDependentCustodyOrder::Dispose(disposition) => {
                forward
                    .dependents
                    .get_mut(&scope.group)
                    .ok_or_else(dependent_conflict)?
                    .disposition = Some(disposition);
            }
        }
        Ok(())
    }
}

fn application_keys(
    enrollment: &NativeDependentCustodyEnrollment,
) -> Result<BTreeMap<[u8; 32], DependentKeyCustody>> {
    let mut keys = BTreeMap::new();
    for key in enrollment
        .members
        .iter()
        .flat_map(|member| &member.application_keys)
    {
        if keys
            .insert(key.key, key.custody)
            .is_some_and(|prior| prior != key.custody)
        {
            return Err(dependent_conflict());
        }
    }
    Ok(keys)
}

fn positive_receipt(receipt: DependentCustodyReceipt) -> bool {
    receipt.key != [0; 32] && receipt.evidence_sha256 != [0; 32]
}

fn dependent_conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/native-dependent-custody",
        "The dependent custody evidence differs from the exact transfer, census or revocation",
    )
}
