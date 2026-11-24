use super::*;

pub type CustodyGroupId = [u8; 16];

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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct CustodyGroup {
    revoked: Option<Basis>,
    members: imbl::OrdSet<CustodyMember>,
}

impl BranchIndex {
    pub fn has_custody_groups(&self) -> bool { !self.custody.is_empty() }

    pub fn custody_group_live(&self, group: CustodyGroupId) -> Result<bool> {
        self.custody.get(&group).map(|group| group.revoked.is_none()).ok_or_else(|| {
            Anomaly::new(AnomalyCategory::NotFound, "rtdb/custody-group-unknown", "Unknown custody group")
        })
    }

    pub fn custody_revocation(&self, group: CustodyGroupId) -> Option<Basis> {
        self.custody.get(&group)?.revoked
    }

    pub fn custody_revocations(&self) -> impl Iterator<Item = (CustodyGroupId, Basis)> + '_ {
        self.custody.iter().filter_map(|(id, group)| group.revoked.map(|basis| (*id, basis)))
    }

    pub(super) fn create_custody_group(&mut self, id: CustodyGroupId) -> Result<()> {
        if id == [0; 16] || self.custody.contains_key(&id) {
            return Err(Anomaly::fault("rtdb/custody-fold", "Invalid resolved custody group creation"));
        }
        self.custody.insert(id, Arc::new(CustodyGroup::default()));
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
        for member in group.members.iter() {
            let Some(branch) = self.branches.get_mut(&member.branch).map(Arc::make_mut) else { continue };
            let Some(rows) = branch.tables.get_mut(&member.table).map(Arc::make_mut) else { continue };
            let Some(events) = rows.events.get_mut(&member.id).map(Arc::make_mut) else { continue };
            for (_, event) in events.iter_mut().filter(|(_, event)| event.cut.basis == member.written) {
                if event.image.as_ref().is_some_and(|image| image.custody_group == Some(id)) {
                    event.image = None;
                }
            }
            branch.changed = Some(basis);
            Arc::make_mut(&mut self.writes).push(BranchWrite {
                branch: member.branch.clone(),
                table: member.table.clone(),
                id: member.id.clone(),
                valid: None,
            });
        }
        self.latest_custody_revocation = Some(basis);
        Ok(())
    }
}
