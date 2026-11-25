use rtdb_types::rtdb_v1::{
    DependentCustodyReceipt, DependentKeyCustody, DependentKeyTerminal, NativeCheckpointMember,
    NativeCustodyFence, NativeDependentCustodyDisposition, NativeDependentCustodyEnrollment,
    NativeDependentCustodyScope,
};

use super::*;

const MAX_MEMBERS: usize = 512;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Dependent {
    enrollment: NativeDependentCustodyEnrollment,
    pub(super) disposition: Option<NativeDependentCustodyDisposition>,
}

impl BranchIndex {
    pub fn native_dependent_custody_roster(
        &self,
        scope: NativeDependentCustodyScope,
    ) -> Result<Vec<NativeCheckpointMember>> {
        self.native_dependent_forward(scope)?;
        let group = &self.custody[&scope.group];
        let at = scope.reservation.source.basis;
        let mut members = Vec::new();
        for entry in self
            .custody_placements(at)
            .filter(|entry| entry.group == scope.group)
        {
            let (carrier_sha256, link_sha256) = match entry.occurrence {
                Some(occurrence) => (occurrence.carrier_sha256, occurrence.link_sha256),
                None => {
                    let pending = &group.reservations[&entry.placement_id].reservation;
                    (pending.carrier_sha256, Some(pending.link_sha256))
                }
            };
            if members.len() == MAX_MEMBERS
                || entry.placement_id == [0; 16]
                || carrier_sha256 == [0; 32]
                || link_sha256 == Some([0; 32])
            {
                return Err(inventory_unavailable());
            }
            members.push(NativeCheckpointMember {
                placement: entry.placement_id,
                reservation: entry.reservation,
                occurrence: entry
                    .occurrence
                    .map(|occurrence| occurrence.commitment(scope.group)),
                carrier_sha256,
                link_sha256,
            });
        }
        if members.is_empty() {
            return Err(inventory_unavailable());
        }
        members.sort_unstable_by_key(|member| member.placement);
        Ok(members)
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
