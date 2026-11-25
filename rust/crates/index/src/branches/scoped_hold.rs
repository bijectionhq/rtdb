use super::*;

const MAX_DATABASE_HOLDS: usize = 256;
const MAX_SCOPE_HOLDS: usize = 32;
const MAX_HOLD_MICROS: i64 = 3_600_000_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedHold {
    pub id: [u8; 16],
    pub owner: [u8; 32],
    pub basis: Basis,
    pub valid_at: Timestamp,
    pub expires_at: Timestamp,
    pub publication_table: TableRef,
    pub publication_id: EntityId,
    pub publication_written: Basis,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScopedHoldCommand {
    Retain(ScopedHold),
    Release { id: [u8; 16], owner: [u8; 32] },
}

impl ScopedHoldCommand {
    pub fn id(&self) -> [u8; 16] {
        match self {
            Self::Retain(hold) => hold.id,
            Self::Release { id, .. } => *id,
        }
    }

    pub fn owner(&self) -> [u8; 32] {
        match self {
            Self::Retain(hold) => hold.owner,
            Self::Release { owner, .. } => *owner,
        }
    }
}

impl BranchIndex {
    pub fn open_scoped_hold(
        &self,
        id: [u8; 16],
        owner: [u8; 32],
        scope: &LabelScope,
        now: Timestamp,
    ) -> Result<&ScopedHold> {
        let hold = self
            .scoped_holds
            .get(&(owner, id))
            .filter(|hold| now < hold.expires_at)
            .ok_or_else(unavailable)?;
        self.check_scoped_publication(hold, &self.project_main_visibility(scope)?)?;
        Ok(hold)
    }

    pub fn check_scoped_hold_view(
        &self,
        id: [u8; 16],
        owner: [u8; 32],
        now: Timestamp,
        projection: &VisibleHistory,
        basis: Basis,
    ) -> Result<()> {
        let hold = self
            .scoped_holds
            .get(&(owner, id))
            .filter(|hold| now < hold.expires_at && hold.basis == basis)
            .ok_or_else(unavailable)?;
        self.check_scoped_publication(hold, projection)
    }

    pub fn check_scoped_hold(
        &self,
        command: &ScopedHoldCommand,
        scope: &LabelScope,
        now: Timestamp,
    ) -> Result<()> {
        if command.id() == [0; 16] || command.owner() == [0; 32] {
            return Err(unavailable());
        }
        match command {
            ScopedHoldCommand::Retain(hold) => {
                if !(1..=MAX_HOLD_MICROS).contains(&hold.expires_at.0.saturating_sub(now.0))
                    || self.scoped_holds.contains_key(&(hold.owner, hold.id))
                {
                    return Err(unavailable());
                }
                let live_count = self
                    .scoped_holds
                    .values()
                    .filter(|prior| now < prior.expires_at)
                    .count();
                let scope_count = self
                    .scoped_holds
                    .values()
                    .filter(|prior| prior.owner == hold.owner && now < prior.expires_at)
                    .count();
                if live_count >= MAX_DATABASE_HOLDS || scope_count >= MAX_SCOPE_HOLDS {
                    return Err(Anomaly::new(
                        AnomalyCategory::Busy,
                        "rtdb/scoped-hold-quota",
                        "The projected retention quota is full",
                    ));
                }
                self.check_scoped_publication(hold, &self.project_main_visibility(scope)?)
            }
            ScopedHoldCommand::Release { id, owner } => self
                .scoped_holds
                .get(&(*owner, *id))
                .map(|_| ())
                .ok_or_else(unavailable),
        }
    }

    fn check_scoped_publication(
        &self,
        hold: &ScopedHold,
        projection: &VisibleHistory,
    ) -> Result<()> {
        let projected_index = projection.index();
        if projected_index
            .latest_custody_revocation
            .is_some_and(|revoked| revoked > hold.basis)
        {
            return Err(unavailable());
        }
        let view = projected_index
            .view(
                MAIN_BRANCH,
                At::Exactly(hold.basis),
                Layer::Merged,
                hold.valid_at,
            )
            .map_err(|_| unavailable())?;
        let event = view
            .row(&hold.publication_table, &hold.publication_id, hold.valid_at)
            .ok_or_else(unavailable)?;
        if event.written_basis() != Some(hold.publication_written)
            || event
                .image
                .as_ref()
                .is_none_or(|image| image.custody_group.is_some())
        {
            return Err(unavailable());
        }
        Ok(())
    }

    pub(super) fn apply_scoped_hold(&mut self, command: ScopedHoldCommand, observed_at: Timestamp) {
        let expired = self
            .scoped_holds
            .iter()
            .filter(|(_, hold)| hold.expires_at <= observed_at)
            .map(|(&id, _)| id)
            .collect::<Vec<_>>();
        for id in expired {
            self.scoped_holds.remove(&id);
        }
        match command {
            ScopedHoldCommand::Retain(hold) => {
                self.scoped_holds.insert((hold.owner, hold.id), hold);
            }
            ScopedHoldCommand::Release { id, owner } => {
                self.scoped_holds.remove(&(owner, id));
            }
        }
    }
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::NotFound,
        "rtdb/scoped-hold",
        "The projected hold is unavailable",
    )
}
