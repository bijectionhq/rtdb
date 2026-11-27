use sha2::{Digest, Sha256};

use super::{*, custody_placement::ReservedPlacement};

pub type CustodyGroupId = [u8; 16];

pub struct CustodyInventory {
    pub group: CustodyGroupId,
    pub placements: u64,
    pub sha256: [u8; 32],
    pub revoked_at: Option<Basis>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CustodyPayload {
    pub binding: [u8; 32],
    pub envelope: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct CustodyMember {
    pub branch: Branch,
    pub table: TableRef,
    pub id: EntityId,
    pub written: Basis,
    #[serde(default = "all_valid")]
    pub valid: Interval,
    #[serde(default)]
    pub ordinal: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct CustodyGroup {
    #[serde(default)]
    created: Option<Basis>,
    pub(super) revoked: Option<Basis>,
    pub(super) members: imbl::OrdSet<CustodyMember>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) original: Option<CustodyOccurrence>,
    #[serde(default, with = "entries", skip_serializing_if = "Map::is_empty")]
    pub(super) reservations: Map<CustodyPlacementId, ReservedPlacement>,
    #[serde(default, with = "entries", skip_serializing_if = "Map::is_empty")]
    pub(super) releases: Map<[u8; 32], CustodyRelease>,
    #[serde(default)]
    pub(super) live_releases: usize,
}

/// The classification and lifetime needed to reconstruct visible cut numbers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct RetiredLabel {
    pub revoked: Basis,
    pub image: Arc<RowImage>,
}

impl CustodyGroup {
    pub(super) fn occurrence(&self, group: CustodyGroupId, commitment: [u8; 32]) -> Option<&CustodyOccurrence> {
        self.occurrences().find(|occurrence| occurrence.commitment(group) == commitment)
    }

    pub(super) fn occurrences(&self) -> impl Iterator<Item = &CustodyOccurrence> {
        self.original.iter().chain(self.reservations.values().filter_map(|entry| entry.occurrence.as_ref()))
    }

    pub(super) fn inventory(&self, group: CustodyGroupId, at: Basis) -> Option<CustodyInventory> {
        self.inventory_except(group, at, None)
    }

    pub(super) fn inventory_except(&self, group: CustodyGroupId, at: Basis, except: Option<CustodyPlacementId>) -> Option<CustodyInventory> {
        if self.created.is_some_and(|created| created > at) { return None; }
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/custody-inventory\0");
        hash.update(at.incarnation());
        hash.update(group);
        let mut placements = 0;
        for member in self.members.iter().filter(|member| member.written <= at) {
            for part in [member.branch.as_bytes(), member.table.schema.as_bytes(),
                member.table.table.as_bytes(), &member.id.handle_bytes()]
            {
                hash.update((part.len() as u64).to_be_bytes());
                hash.update(part);
            }
            hash.update(member.written.to_bytes());
            hash.update(member.valid.lower().0.to_be_bytes());
            hash.update(member.valid.upper().0.to_be_bytes());
            hash.update(member.ordinal.to_be_bytes());
            placements += 1;
        }
        if let Some(original) = self.original.as_ref().filter(|original| original.written <= at) {
            hash.update(b"original\0");
            hash.update(original.commitment(group));
        }
        for (_, pending) in self.reservations.iter().filter(|(destination, pending)| {
            Some(**destination) != except && pending.at <= at
        }) {
            hash.update(b"reservation\0");
            hash.update(pending.at.to_bytes());
            hash.update(pending.commitment(at.incarnation(), group));
            if let Some(occurrence) = pending.occurrence.as_ref().filter(|occurrence| occurrence.written <= at) {
                hash.update([1]);
                hash.update(occurrence.commitment(group));
            } else {
                hash.update([0]);
                placements += 1;
            }
        }
        Some(CustodyInventory {
            group, placements, sha256: hash.finalize().into(),
            revoked_at: self.revoked.filter(|revocation| *revocation <= at),
        })
    }
}

impl BranchIndex {
    pub fn has_custody_groups(&self) -> bool { !self.custody.is_empty() }

    pub fn has_custody_groups_at(&self, at: Basis) -> bool {
        self.custody.values().any(|group| group.created.is_none_or(|created| created <= at))
    }

    pub fn require_classic_write(&self, table: &TableRef) -> Result<()> {
        if self.definition(table, self.head().basis).is_some_and(|definition| definition.visibility.is_some())
            || self.has_custody_relation(table)
        {
            return Err(Anomaly::new(AnomalyCategory::Forbidden, "rtdb/protected-relation", "A protected relation requires canonical writer admission"));
        }
        Ok(())
    }

    pub fn has_custody_relation(&self, table: &TableRef) -> bool {
        self.custody.values().any(|group| group.members.iter().any(|member| &member.table == table))
    }

    pub fn has_custody_address(&self, table: &TableRef, id: &EntityId) -> bool {
        self.custody.values().any(|group| group.members.iter().any(|member| &member.table == table && &member.id == id))
    }

    pub fn custody_group_live(&self, group: CustodyGroupId) -> Result<bool> {
        self.custody.get(&group).map(|group| group.revoked.is_none()).ok_or_else(|| {
            Anomaly::new(AnomalyCategory::NotFound, "rtdb/custody-group-unknown", "Unknown custody group")
        })
    }

    pub(super) fn has_custody_group(&self, group: CustodyGroupId) -> bool {
        self.custody.contains_key(&group)
    }

    pub(super) fn has_live_custody_group(&self, group: CustodyGroupId) -> bool {
        self.custody.get(&group).is_some_and(|group| group.revoked.is_none())
    }

    pub(super) fn live_custody(&self, group: CustodyGroupId) -> Result<&CustodyGroup> {
        if !self.custody_group_live(group)? {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/erased-under-view", "The custody group has been revoked"));
        }
        Ok(&self.custody[&group])
    }

    pub(super) fn custody_placements(&self, at: Basis) -> impl Iterator<Item = CustodyPlacementEntry<'_>> + '_ {
        self.custody.iter().flat_map(move |(&group, record)| {
            let original = record.original.iter().filter(move |original| original.written <= at)
                .map(move |original| CustodyPlacementEntry {
                    group, placement_id: original.placement_id, enrolled: original.written, reservation: None, occurrence: Some(original), reviewed: None,
                });
            let reserved = record.reservations.iter().filter(move |(_, entry)| entry.at <= at)
                .map(move |(&placement_id, entry)| CustodyPlacementEntry {
                    group, placement_id, enrolled: entry.at,
                    reservation: Some(entry.commitment(at.incarnation(), group)),
                    occurrence: entry.occurrence.as_ref().filter(|occurrence| occurrence.written <= at),
                    reviewed: entry.reviewed.as_ref().map(|reviewed| (reviewed, &entry.reservation)),
                });
            original.chain(reserved)
        })
    }

    pub fn custody_placement_at(
        &self, group: CustodyGroupId, placement_id: CustodyPlacementId, at: Basis,
    ) -> Result<Option<CustodyPlacementEntry<'_>>> {
        self.exact(at)?;
        let record = self.live_custody(group)?;
        if self.visibility_projection {
            return Ok(None);
        }
        if let Some(original) = record.original.as_ref().filter(|original| {
            original.placement_id == placement_id && original.written <= at
        }) {
            return Ok(Some(CustodyPlacementEntry {
                group, placement_id, enrolled: original.written, reservation: None, occurrence: Some(original), reviewed: None,
            }));
        }
        Ok(record.reservations.get(&placement_id).filter(|entry| entry.at <= at).map(|entry| CustodyPlacementEntry {
            group, placement_id, enrolled: entry.at,
            reservation: Some(entry.commitment(at.incarnation(), group)),
            occurrence: entry.occurrence.as_ref().filter(|occurrence| occurrence.written <= at),
            reviewed: entry.reviewed.as_ref().map(|reviewed| (reviewed, &entry.reservation)),
        }))
    }

    pub fn matches_reviewed_activation(
        &self, group: CustodyGroupId, placement: CustodyPlacementId, basis: Basis, key: &[u8],
    ) -> Result<bool> {
        let record = self.live_custody(group)?;
        let mut matches = record.reservations.values().filter_map(|entry| {
            entry.reviewed.as_ref()?;
            entry.occurrence.as_ref().filter(|row| {
                row.target.branch == MAIN_BRANCH && row.written <= basis && row.receipt_key == key
            })
        });
        Ok(matches.next().is_some_and(|row| row.placement_id == placement && row.written == basis)
            && matches.next().is_none())
    }

    pub fn custody_revocation(&self, group: CustodyGroupId) -> Option<Basis> {
        self.custody.get(&group)?.revoked
    }

    pub fn custody_revocations(&self) -> impl Iterator<Item = (CustodyGroupId, Basis)> + '_ {
        self.custody.iter().filter_map(|(id, group)| group.revoked.map(|basis| (*id, basis)))
    }

    pub fn custody_inventory(&self, group: CustodyGroupId) -> Option<CustodyInventory> {
        self.custody_inventory_at(group, self.head.basis)
    }

    pub(super) fn custody_inventories(&self, at: Basis) -> impl Iterator<Item = CustodyInventory> + '_ {
        self.custody.keys().filter_map(move |&id| self.custody_inventory_at(id, at))
    }

    pub fn custody_revocation_writes(&self, id: CustodyGroupId, cut: BranchCut) -> Result<Vec<BranchWrite>> {
        let Some(group) = self.custody.get(&id).filter(|group| group.revoked == Some(cut.basis)) else {
            return Ok(Vec::new());
        };
        let previous = self.before(cut.basis)?.ok_or_else(|| {
            Anomaly::fault("rtdb/custody-fold", "Revocation has no preceding cut")
        })?;
        let mut writes = Vec::new();
        for (branch, data) in self.branches.iter().filter(|(_, branch)| branch.active(previous, cut.system_time)) {
            let mut bounds = BTreeMap::new();
            let mut layer = Some((branch.as_str(), data.as_ref(), cut.basis));
            while let Some((name, data, basis)) = layer {
                bounds.insert(name, basis);
                layer = data.base.as_ref().and_then(|(parent, basis)| {
                    self.branches.get(parent).map(|data| (parent.as_str(), data.as_ref(), *basis))
                });
            }
            writes.extend(group.members.iter().filter(|member| {
                bounds.get(member.branch.as_str()).is_some_and(|basis| member.written <= *basis)
            }).map(|member| BranchWrite {
                branch: branch.clone(),
                table: member.table.clone(),
                id: member.id.clone(),
                valid: Some(member.valid),
                erased_written_basis: Some(member.written),
            }));
        }
        Ok(writes)
    }

    pub(super) fn create_custody_group(&mut self, id: CustodyGroupId) -> Result<()> {
        if id == [0; 16] || self.custody.contains_key(&id) {
            return Err(Anomaly::fault("rtdb/custody-fold", "Invalid resolved custody group creation"));
        }
        self.custody.insert(id, Arc::new(CustodyGroup {
            created: Some(self.head.basis), ..CustodyGroup::default()
        }));
        Ok(())
    }

    pub(super) fn admit_custody_member(&mut self, id: CustodyGroupId, member: CustodyMember) -> Result<()> {
        let group = Arc::make_mut(self.custody.get_mut(&id).ok_or_else(|| {
            Anomaly::fault("rtdb/custody-fold", "Resolved write names an unknown custody group")
        })?);
        if group.revoked.is_some() {
            return Err(Anomaly::fault("rtdb/custody-fold", "Resolved write names a revoked custody group"));
        }
        group.members.insert(member);
        Ok(())
    }

    pub(super) fn revoke_custody_group(&mut self, id: CustodyGroupId) -> Result<()> {
        let group = Arc::make_mut(self.custody.get_mut(&id).ok_or_else(|| {
            Anomaly::fault("rtdb/custody-fold", "Resolved revocation names an unknown custody group")
        })?);
        if group.revoked.is_some() {
            return Ok(());
        }
        let basis = self.head.basis;
        group.revoked = Some(basis);
        let members = group.members.clone();
        for member in members.iter() {
            let visibility = self.definition(&member.table, member.written)
                .and_then(|table| table.visibility.clone());
            let Some(branch) = self.branches.get_mut(&member.branch).map(Arc::make_mut) else { continue };
            let Some(rows) = branch.tables.get_mut(&member.table).map(Arc::make_mut) else { continue };
            let Some(events) = rows.events.get_mut(&member.id).map(Arc::make_mut) else { continue };
            for (ordinal, (_, event)) in events.iter_mut().enumerate().filter(|(_, (_, event))| event.cut.basis == member.written) {
                let Some(image) = event.image.take_if(|image| image.custody_group == Some(id)) else { continue };
                if let Some(visibility) = &visibility
                    && let Some(label) = visibility.label(&image.cells)
                {
                    let marker = RowImage {
                        cells: StructFields::from([
                            ("_id".into(), event.id.as_value()),
                            (visibility.column.clone(), Value::Binary(label.to_vec())),
                        ]),
                        expires_at: image.expires_at, custody_group: Some(id), custody_payload: None,
                    };
                    Arc::make_mut(rows.retired_labels.entry(member.id.clone()).or_default())
                        .insert(ordinal, RetiredLabel { revoked: basis, image: Arc::new(marker) });
                }
            }
            branch.changed = Some(basis);
        }
        let notices = self.custody_revocation_writes(id, self.head)?;
        Arc::make_mut(&mut self.writes).extend(notices);
        self.latest_custody_revocation = Some(basis);
        Ok(())
    }
}

fn all_valid() -> Interval { Interval::ALL }
