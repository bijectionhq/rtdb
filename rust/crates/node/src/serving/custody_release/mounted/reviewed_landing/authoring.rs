use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeAuthoring {
    pub group: CustodyGroupId,
    pub placement: CustodyPlacementId,
    pub target: CustodyTarget<ValidTime>,
    pub branch_created: Basis,
    pub write_key: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<NativeAuthoringLine>,
    pub rewrite: Option<NativeAuthoringRewrite>,
}

impl NativeAuthoring {
    pub fn has_bound_line(&self) -> bool {
        if self.target.branch == MAIN_BRANCH { self.line.is_none() } else { self.line.is_some() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeAuthoringLine {
    pub generation: [u8; 16],
    pub creation_key: Vec<u8>,
    pub registry: TableRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeAuthoringRewrite {
    pub original: [u8; 32],
    pub source: [u8; 32],
    pub link: CustodyPlacementId,
    pub reserve_key: Vec<u8>,
    pub review_sha256: [u8; 32],
}

impl MountedResponseRelease<'_> {
    pub async fn verify_native_authoring_scope(&self, authoring: &NativeAuthoring) -> Result<()> {
        let release = self.release();
        let authority = self.native_landing_authority();
        self.while_mounted(async {
            if !authoring.has_bound_line() {
                return Err(release_unavailable());
            }
            let (index, writer) = release.current_with_writer(authority).await?;
            let view = index.view(&authoring.target.branch, At::Latest, Layer::Merged, self.clock.now())?;
            if !view.lineage().iter().any(|branch| {
                branch.branch == authoring.target.branch
                    && branch.created == authoring.branch_created
                    && branch.writable
                    && branch.dropped.is_none()
            }) {
                return Err(release_unavailable());
            }
            if let Some(line) = &authoring.line {
                self.confirm_native_authoring_line(&index, &writer, authoring, line).await?;
            }
            if let Some(rewrite) = &authoring.rewrite {
                let (original, source) =
                    index.custody_read_occurrence(index.head().basis, authoring.group, rewrite.source)?;
                if original != rewrite.original {
                    return Err(release_unavailable());
                }
                let receipt = release
                    .driver
                    .original_custody_receipt(&source.receipt_key, source.written, self.cancellation)
                    .await?;
                confirm_occurrence(&index, source, &receipt)?;
            } else if index.custody_inventory(authoring.group).is_some() {
                return Err(release_unavailable());
            }
            Ok(())
        })
        .await?;
        self.owner.with_live_mount(|| release.require_authority(authority))
    }

    pub async fn verify_native_retained_authoring_line(&self, authoring: &NativeAuthoring) -> Result<()> {
        let release = self.release();
        let authority = self.native_landing_authority();
        self.while_mounted(async {
            let line = authoring.line.as_ref().ok_or_else(release_unavailable)?;
            let (index, writer) = release.current_with_writer(authority).await?;
            rtdb_tx::transact::workload::require_family_branch(
                &index, &authoring.target.branch, authoring.branch_created, self.clock.now(),
            )?;
            self.confirm_native_authoring_line(&index, &writer, authoring, line).await
        }).await?;
        self.owner.with_live_mount(|| release.require_authority(authority))
    }

    async fn confirm_native_authoring_line(
        &self,
        index: &BranchIndex,
        writer: &CustodyWriterFence,
        authoring: &NativeAuthoring,
        line: &NativeAuthoringLine,
    ) -> Result<()> {
        let clock = index.exact(authoring.branch_created)?.system_time;
        require_line_registration(index, authoring, line, clock)?;
        let receipt = self.release().driver.original_line_creation_receipt(
            &line.creation_key, authoring.branch_created, self.cancellation,
        ).await?;
        if receipt.outcome != Outcome::Then
            || receipt.key != line.creation_key
            || receipt.basis != authoring.branch_created
            || receipt.system_time != clock
        {
            return Err(release_unavailable());
        }
        require_writer_at(index, writer, receipt.basis, receipt.system_time)
    }

    pub async fn confirm_native_authoring(
        &self,
        authoring: &NativeAuthoring,
        carrier_sha256: [u8; 32],
        link_sha256: Option<[u8; 32]>,
    ) -> Result<(CustodyOccurrence, Timestamp, [u8; 32])> {
        let release = self.release();
        let authority = self.native_landing_authority();
        let confirmed = self
            .while_mounted(async {
                if !authoring.has_bound_line() {
                    return Err(release_unavailable());
                }
                let (index, writer) = release.current_with_writer(authority).await?;
                let current = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, self.clock.now())?;
                let written = current.custody_placements()
                    .find(|entry| entry.group == authoring.group && entry.placement_id == authoring.placement)
                    .and_then(|entry| entry.occurrence)
                    .filter(|occurrence| occurrence.receipt_key == authoring.write_key)
                    .ok_or_else(release_unavailable)?.written;
                let receipt = release.driver.original_custody_receipt(
                    &authoring.write_key, written, self.cancellation,
                ).await?;
                if receipt.outcome != Outcome::Then || receipt.key != authoring.write_key || receipt.basis != written {
                    return Err(release_unavailable());
                }
                let view = index.view(MAIN_BRANCH, At::Exactly(written), Layer::Merged, self.clock.now())?;
                if !view.lineage().iter().any(|branch| {
                    branch.branch == authoring.target.branch
                        && branch.created == authoring.branch_created
                        && branch.writable
                        && branch.dropped.is_none()
                }) {
                    return Err(release_unavailable());
                }
                let placement = view
                    .custody_placements()
                    .find(|entry| entry.group == authoring.group && entry.placement_id == authoring.placement)
                    .ok_or_else(release_unavailable)?;
                let occurrence = placement.occurrence.ok_or_else(release_unavailable)?;
                if placement.reviewed.is_some()
                    || occurrence.target.branch != authoring.target.branch
                    || occurrence.target.table != authoring.target.table
                    || occurrence.target.id != authoring.target.id
                    || occurrence.target.valid != authoring.target.valid.resolve(receipt.system_time)?
                    || occurrence.carrier_sha256 != carrier_sha256
                    || occurrence.link_sha256 != link_sha256
                {
                    return Err(release_unavailable());
                }
                confirm_occurrence(&index, occurrence, &receipt)?;
                require_writer_at(&index, &writer, receipt.basis, receipt.system_time)?;
                let commitment = occurrence.commitment(authoring.group);
                let (original, _) = index.custody_read_occurrence(receipt.basis, authoring.group, commitment)?;
                match &authoring.rewrite {
                    Some(rewrite) => {
                        let (reserved, at) =
                            index.custody_reservation_at(authoring.group, authoring.placement, receipt.basis)?;
                        let reserve = release.driver.original_custody_receipt(
                            &rewrite.reserve_key, at, self.cancellation,
                        ).await?;
                        if reserve.outcome != Outcome::Then
                            || reserve.key != rewrite.reserve_key
                            || reserve.basis != at
                            || reserve.basis >= receipt.basis
                            || index.exact(at)?.system_time != reserve.system_time
                            || original != rewrite.original
                            || reserved.original != original
                            || reserved.source != rewrite.source
                            || reserved.destination != authoring.placement
                            || reserved.link != rewrite.link
                            || reserved.target != authoring.target
                            || reserved.review_sha256 != rewrite.review_sha256
                            || reserved.carrier_sha256 != carrier_sha256
                            || Some(reserved.link_sha256) != link_sha256
                            || placement.reservation != occurrence.reservation
                            || placement.enrolled != at
                        {
                            return Err(release_unavailable());
                        }
                        require_writer_at(&index, &writer, reserve.basis, reserve.system_time)?;
                    }
                    None if original != commitment || occurrence.reservation.is_some() || link_sha256.is_some() => {
                        return Err(release_unavailable());
                    }
                    None => (),
                }
                release
                    .driver
                    .check_custody_release_delivery(
                        &BTreeSet::from([authoring.group]),
                        &writer,
                        None,
                        self.cancellation,
                    )
                    .await?;
                Ok((occurrence.clone(), receipt.system_time, original))
            })
            .await?;
        self.owner.with_live_mount(|| release.require_authority(authority))?;
        Ok(confirmed)
    }

    pub async fn verify_native_group_revocation(
        &self,
        group: CustodyGroupId,
        key: &[u8],
        revoked_at: Basis,
        inventory_sha256: [u8; 32],
    ) -> Result<Timestamp> {
        let release = self.release();
        let authority = self.native_landing_authority();
        let time = self
            .while_mounted(async {
                let (index, writer) = release.current_with_writer(authority).await?;
                let inventory = index.custody_inventory(group).ok_or_else(release_unavailable)?;
                let receipt = release.driver.tx_status(key, self.cancellation).await?;
                if inventory.revoked_at != Some(revoked_at)
                    || inventory.sha256 != inventory_sha256
                    || receipt.outcome != Outcome::Then
                    || receipt.key != key
                    || receipt.basis != revoked_at
                    || index.exact(revoked_at)?.system_time != receipt.system_time
                {
                    return Err(release_unavailable());
                }
                require_writer_at(&index, &writer, revoked_at, receipt.system_time)?;
                Ok(receipt.system_time)
            })
            .await?;
        self.owner.with_live_mount(|| release.require_authority(authority))?;
        Ok(time)
    }
}

fn require_writer_at(index: &BranchIndex, writer: &CustodyWriterFence, basis: Basis, clock: Timestamp) -> Result<()> {
    let view = index.view(MAIN_BRANCH, At::Exactly(basis), Layer::Local, clock)?;
    if [writer.valid_at, clock]
        .into_iter()
        .all(|at| view.row(&writer.table, &writer.id, at).is_some_and(|event| event.cut.basis == writer.written))
    {
        Ok(())
    } else {
        Err(release_unavailable())
    }
}

fn require_line_registration(
    index: &BranchIndex,
    authoring: &NativeAuthoring,
    line: &NativeAuthoringLine,
    clock: Timestamp,
) -> Result<()> {
    let created = index.view(MAIN_BRANCH, At::Exactly(authoring.branch_created), Layer::Local, clock)?;
    let registration = created
        .row(&line.registry, &EntityId::String(authoring.target.branch.clone()), Timestamp(0))
        .ok_or_else(release_unavailable)?;
    let image = registration.image_at(Timestamp(0)).ok_or_else(release_unavailable)?;
    if registration.cut != created.cut()
        || registration.written_basis() != Some(authoring.branch_created)
        || image.custody_group.is_some()
        || line.generation == [0; 16]
        || !matches!(image.cells.get("line_generation"),
            Some(Value::Binary(bytes) | Value::FixedSizeBinary(bytes)) if bytes.as_slice() == line.generation.as_slice())
    {
        return Err(release_unavailable());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rtdb_index::branches::{BranchCut, BranchEffect, BranchEvent, RowImage, WrittenVersion};
    use rtdb_types::{Interval, StructFields};

    use super::*;

    #[test]
    fn named_authoring_line_is_the_exact_creation_row_value() -> Result<()> {
        let incarnation = [1; 16];
        let mut index = BranchIndex::new(incarnation);
        let created = BranchCut { basis: Basis::new(incarnation, 1), system_time: Timestamp(10) };
        let later = BranchCut { basis: Basis::new(incarnation, 2), system_time: Timestamp(20) };
        let unwritten = BranchCut { basis: Basis::new(incarnation, 3), system_time: Timestamp(30) };
        let branch = "authored".to_owned();
        let line = NativeAuthoringLine {
            generation: [2; 16],
            creation_key: vec![3; 16],
            registry: TableRef::new("public", "registered_lines"),
        };
        let authoring = NativeAuthoring {
            group: [4; 16],
            placement: [5; 16],
            target: CustodyTarget {
                branch: branch.clone(),
                table: TableRef::new("public", "facts"),
                id: EntityId::Integer(42),
                valid: Interval::ALL.into(),
            },
            branch_created: created.basis,
            write_key: vec![6; 16],
            line: Some(line.clone()),
            rewrite: None,
        };
        let registration = |cut, generation: [u8; 16]| BranchEffect::Write {
            branch: MAIN_BRANCH.into(),
            table: line.registry.clone(),
            event: BranchEvent {
                id: EntityId::String(branch.clone()),
                cut,
                valid: Interval::ALL,
                written: WrittenVersion::Branch,
                image: Some(Arc::new(RowImage {
                    cells: StructFields::from([("line_generation".into(), Value::Binary(generation.to_vec()))]),
                    expires_at: None,
                    custody_group: None,
                    custody_payload: None,
                })),
            },
        };
        index.advance(created)?;
        index.apply(BranchEffect::Fork {
            child: branch.clone(),
            from: (MAIN_BRANCH.into(), Basis::new(incarnation, 0)),
            writable: true,
            expires_at: None,
        })?;
        index.apply(registration(created, line.generation))?;
        index.advance(later)?;
        index.apply(registration(later, [7; 16]))?;
        index.advance(unwritten)?;

        require_line_registration(&index, &authoring, &line, created.system_time)?;
        let forged = NativeAuthoringLine { generation: [7; 16], ..line.clone() };
        assert!(require_line_registration(&index, &authoring, &forged, created.system_time).is_err());
        let stale = NativeAuthoring { branch_created: unwritten.basis, ..authoring.clone() };
        assert!(require_line_registration(&index, &stale, &forged, unwritten.system_time).is_err());
        let wrong_row = NativeAuthoring {
            target: CustodyTarget { branch: "different".into(), ..authoring.target.clone() },
            ..authoring.clone()
        };
        assert!(require_line_registration(&index, &wrong_row, &line, created.system_time).is_err());
        assert!(!NativeAuthoring { line: None, ..authoring }.has_bound_line());
        Ok(())
    }
}
