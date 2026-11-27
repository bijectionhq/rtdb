//! Durable RTDB resolver driver for SS8 footprint grants, work-keyed OCC
//! debits, strict work-key age rotation, lease-loss transfer, and all-writer
//! (Flight + SQL) fencing.
use std::collections::BTreeSet;

use rtdb_columnar::read_value;
use rtdb_types::{
    Anomaly, AnomalyCategory, AnomalyKind, EntityId, Result, StructFields, TableRef, Timestamp, Value,
};
use sha2::{Digest, Sha256};

use super::footprint_grant::{
    self, DEFAULT_OPTIMISTIC_CONFLICT_LIMIT, GrantHolder, GrantProposalVerdict, TransferVerdict,
    WaiterEntry, WriterKind,
};
use super::{OpenTransaction, interval};
use crate::OwnedFootprintGrantOp;

pub(crate) const SS8_WORK_DEBITS: &str = "ss8_work_debits";
pub(crate) const SS8_FOOTPRINT_GRANTS: &str = "ss8_footprint_grants";
const STATUS_ACTIVE: &str = "active";
const STATUS_WAITING: &str = "waiting";

pub(crate) fn ss8_work_debits_table() -> TableRef {
    TableRef::new("xt", SS8_WORK_DEBITS)
}

pub(crate) fn ss8_footprint_grants_table() -> TableRef {
    TableRef::new("xt", SS8_FOOTPRINT_GRANTS)
}

pub(crate) fn is_ss8_system_table(table: &TableRef) -> bool {
    table.schema == "xt" && matches!(table.table.as_str(), SS8_WORK_DEBITS | SS8_FOOTPRINT_GRANTS)
}

pub(crate) fn work_key_id(work_key: &str) -> u64 {
    let digest = Sha256::digest(work_key.as_bytes());
    u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

/// XTDB's code for a failed `ASSERT`, the conflict a transaction raises against data that changed under it.
pub(crate) const ASSERT_FAILED: &str = "xtdb/assert-failed";

/// A failed `ASSERT`: XTDB's conflict for data that changed under the transaction.
pub(crate) fn assert_failed(message: String) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, ASSERT_FAILED, message).with_kind(AnomalyKind::OptimisticConflict)
}

fn footprint_grant_fenced() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/footprint-grant-fenced", "Target footprint is fenced by a live durable SS8 grant")
        .with_kind(AnomalyKind::OptimisticConflict)
}

/// Whether an abort is one of the optimistic conflicts a work key's durable grant debits: a failed assertion, or a write
/// fenced by another work key's grant. These are the only conflicts raised with the optimistic-conflict kind; the resolver's
/// other conflicts (expired deadlines, existing declarations, stale owned observations) are not debited.
pub(crate) fn is_definite_occ_conflict(error: &Anomaly) -> bool { error.is(AnomalyKind::OptimisticConflict) }

#[derive(Clone, Debug)]
pub(crate) struct DurableGrantRow {
    pub work_key: String,
    pub work_age: u64,
    pub holder_attempt: u64,
    pub reopened_cut: i64,
    pub progress_step: u64,
    pub footprint: BTreeSet<String>,
    pub active: bool,
}

impl DurableGrantRow {
    fn as_holder(&self) -> GrantHolder {
        GrantHolder {
            work_key_id: work_key_id(&self.work_key),
            work_age: self.work_age,
            holder_attempt: self.holder_attempt,
            reopened_cut: self.reopened_cut,
            progress_step: self.progress_step,
            footprint_len: self.footprint.len() as u64,
            active: self.active,
        }
    }

    fn as_waiter(&self) -> WaiterEntry {
        WaiterEntry {
            work_key_id: work_key_id(&self.work_key),
            work_age: self.work_age,
        }
    }

    fn overlaps(&self, tables: &BTreeSet<String>) -> bool {
        self.footprint.is_empty() || tables.iter().any(|t| self.footprint.contains(t))
    }

    fn overlaps_footprint(&self, other: &BTreeSet<String>) -> bool {
        self.footprint.is_empty()
            || other.is_empty()
            || self.footprint.iter().any(|t| other.contains(t))
    }

    fn to_document(&self) -> StructFields<Value> {
        let footprint_list = self
            .footprint
            .iter()
            .cloned()
            .map(Value::Utf8)
            .collect::<Vec<_>>();
        StructFields::from([
            ("_id".into(), Value::Utf8(self.work_key.clone())),
            ("work_age".into(), Value::I64(self.work_age as i64)),
            ("holder_attempt".into(), Value::I64(self.holder_attempt as i64)),
            ("reopened_cut".into(), Value::I64(self.reopened_cut)),
            ("progress_step".into(), Value::I64(self.progress_step as i64)),
            ("footprint".into(), Value::list(footprint_list)),
            (
                "status".into(),
                Value::Utf8(if self.active { STATUS_ACTIVE } else { STATUS_WAITING }.into()),
            ),
        ])
    }
}

impl OpenTransaction<'_> {
    pub(super) async fn load_ss8_grants(&self) -> Result<Vec<DurableGrantRow>> {
        let table = ss8_footprint_grants_table();
        if !self.snapshot()?.contains_table(&table) {
            return Ok(Vec::new());
        }
        let mut query = self
            .open_query(
                "SELECT _id, work_age, holder_attempt, reopened_cut, progress_step, footprint, status FROM xt.ss8_footprint_grants FOR ALL VALID_TIME",
                Vec::new(),
            )
            .await?;
        let mut rows = Vec::new();
        while let Some(batch) = query.next_batch().await? {
            let batch = batch.batch();
            for row in 0..batch.num_rows() {
                let cell = |index: usize| {
                    read_value(batch.schema().field(index), batch.column(index).as_ref(), row)
                };
                let (
                    Value::Utf8(work_key),
                    Value::I64(work_age),
                    Value::I64(holder_attempt),
                    Value::I64(reopened_cut),
                    Value::I64(progress_step),
                    footprint_val,
                    Value::Utf8(status),
                ) = (
                    cell(0)?,
                    cell(1)?,
                    cell(2)?,
                    cell(3)?,
                    cell(4)?,
                    cell(5)?,
                    cell(6)?,
                )
                else {
                    return Err(Anomaly::fault(
                        "rtdb.tx/ss8-grant-row",
                        "Durable SS8 footprint grant row has an invalid schema",
                    ));
                };
                let mut footprint = BTreeSet::new();
                if let Value::List(items) = footprint_val {
                    for index in 0..items.len() {
                        if let Some(name) = items.view().utf8(index) {
                            footprint.insert(name.to_owned());
                        }
                    }
                }
                rows.push(DurableGrantRow {
                    work_key,
                    work_age: work_age as u64,
                    holder_attempt: holder_attempt as u64,
                    reopened_cut,
                    progress_step: progress_step as u64,
                    footprint,
                    active: status == STATUS_ACTIVE,
                });
            }
        }
        Ok(rows)
    }

    pub(super) async fn load_ss8_debit(&self, work_key: &str) -> Result<Option<(u64, u8)>> {
        let table = ss8_work_debits_table();
        if !self.snapshot()?.contains_table(&table) {
            return Ok(None);
        }
        let mut query = self
            .open_query(
                "SELECT work_age, remaining_credits FROM xt.ss8_work_debits FOR ALL VALID_TIME WHERE _id = ?",
                vec![Value::Utf8(work_key.to_owned())],
            )
            .await?;
        while let Some(batch) = query.next_batch().await? {
            let batch = batch.batch();
            if batch.num_rows() == 0 {
                continue;
            }
            let cell = |index: usize| {
                read_value(batch.schema().field(index), batch.column(index).as_ref(), 0)
            };
            if let (Value::I64(work_age), Value::I64(credits)) = (cell(0)?, cell(1)?) {
                let credits = u8::try_from(credits.clamp(0, u8::MAX as i64)).unwrap_or(0);
                return Ok(Some((work_age as u64, credits)));
            }
        }
        Ok(None)
    }

    fn write_ss8_grant_row(&mut self, row: &DurableGrantRow) -> Result<()> {
        self.buffer.put(
            ss8_footprint_grants_table(),
            row.to_document(),
            interval(Timestamp(0), Timestamp::MAX)?,
        )
    }

    fn delete_ss8_grant_row(&mut self, work_key: &str) -> Result<()> {
        self.buffer.delete(
            ss8_footprint_grants_table(),
            &EntityId::String(work_key.to_owned()),
            interval(Timestamp(0), Timestamp::MAX)?,
        )
    }

    fn delete_ss8_debit_row(&mut self, work_key: &str) -> Result<()> {
        self.buffer.delete(
            ss8_work_debits_table(),
            &EntityId::String(work_key.to_owned()),
            interval(Timestamp(0), Timestamp::MAX)?,
        )
    }

    /// Enforce the all-writer fence (Flight and SQL) against all live durable
    /// SS8 grants whose footprint overlaps `written_tables`.
    pub(super) async fn check_ss8_writer_fence(
        &self,
        writer: WriterKind,
        written_tables: &BTreeSet<String>,
        carried_grant: Option<(&str, u64)>,
    ) -> Result<()> {
        if written_tables.is_empty() {
            return Ok(());
        }
        let grants = self.load_ss8_grants().await?;
        for grant in &grants {
            if !grant.active {
                continue;
            }
            let overlapping = grant.overlaps(written_tables);
            let matches_holder = carried_grant.is_some_and(|(wk, attempt)| {
                matches!(
                    footprint_grant::admit_grant_proposal(grant.as_holder(), work_key_id(wk), attempt),
                    GrantProposalVerdict::AdmitAndRelease
                ) && grant.work_key == wk
            });
            // Round-4 T3 label-dominance non-interference: a reservation fences
            // every writer whose commit label dominates the held commit label
            // (all writers dominate the bottom/public label; non-dominating
            // writers below or beside a scoped label are never held).
            let writer_dominates_grant = true;
            if !footprint_grant::writer_may_apply(
                writer,
                overlapping,
                writer_dominates_grant,
                grant.active,
                matches_holder,
            ) {
                return Err(footprint_grant_fenced());
            }
        }
        Ok(())
    }

    /// Record a durable work-keyed OCC conflict debit atomically with a
    /// definite OCC abort.
    pub(super) async fn record_ss8_occ_debit(
        &mut self,
        work_key: &str,
        work_age: u64,
        limit: u8,
    ) -> Result<()> {
        let initial = if limit == 0 {
            DEFAULT_OPTIMISTIC_CONFLICT_LIMIT
        } else {
            limit
        };
        let (retained_age, current_credits) = match self.load_ss8_debit(work_key).await? {
            Some((age, credits)) => (age, credits),
            None => (work_age, initial),
        };
        let next_credits = footprint_grant::debit_occ_conflict(current_credits);
        let row = StructFields::from([
            ("_id".into(), Value::Utf8(work_key.to_owned())),
            ("work_age".into(), Value::I64(retained_age as i64)),
            ("remaining_credits".into(), Value::I64(i64::from(next_credits))),
            ("last_conflict_tx".into(), Value::I64(self.tx_key().tx_id.0)),
        ]);
        self.buffer.put(
            ss8_work_debits_table(),
            row,
            interval(Timestamp(0), Timestamp::MAX)?,
        )
    }

    /// Apply an explicit durable footprint grant management operation.
    pub(super) async fn apply_ss8_grant_op(&mut self, op: &OwnedFootprintGrantOp) -> Result<()> {
        match op {
            OwnedFootprintGrantOp::DebitOnConflict { .. } => Ok(()),
            OwnedFootprintGrantOp::CarryGrant {
                work_key,
                holder_attempt,
                ..
            } => {
                let grants = self.load_ss8_grants().await?;
                let Some(grant) = grants.iter().find(|g| g.work_key == *work_key) else {
                    return Err(Anomaly::new(
                        AnomalyCategory::Conflict,
                        "rtdb.owned/footprint-grant-missing",
                        "Carried SS8 grant is not active",
                    ));
                };
                match footprint_grant::admit_grant_proposal(
                    grant.as_holder(),
                    work_key_id(work_key),
                    *holder_attempt,
                ) {
                    GrantProposalVerdict::AdmitAndRelease => Ok(()),
                    GrantProposalVerdict::FenceStaleAttempt => Err(Anomaly::new(
                        AnomalyCategory::Conflict,
                        "rtdb.owned/footprint-grant-stale-attempt",
                        "Carried SS8 grant attempt was superseded by lease-loss transfer",
                    )),
                    GrantProposalVerdict::FenceInactiveGrant
                    | GrantProposalVerdict::FenceWrongWorkKey => Err(Anomaly::new(
                        AnomalyCategory::Conflict,
                        "rtdb.owned/footprint-grant-inactive",
                        "Carried SS8 grant does not match an active holder",
                    )),
                }
            }
            OwnedFootprintGrantOp::AcquireOrWait {
                work_key,
                work_age,
                attempt,
                footprint,
            } => {
                let mut grants = self.load_ss8_grants().await?;
                let requested: BTreeSet<String> = footprint.iter().cloned().collect();
                let existing = grants.iter().position(|g| g.work_key == *work_key);
                let mut entry = match existing {
                    Some(idx) => {
                        let mut prior = grants.remove(idx);
                        // Durable union: never discard previously covered footprint members.
                        prior.footprint.extend(requested);
                        if prior.active {
                            // Already holds the durable grant; keep active without lapsing.
                            if *attempt > prior.holder_attempt {
                                prior.holder_attempt = *attempt;
                            }
                            self.write_ss8_grant_row(&prior)?;
                            return Ok(());
                        }
                        prior
                    }
                    None => DurableGrantRow {
                        work_key: work_key.clone(),
                        work_age: *work_age,
                        holder_attempt: *attempt,
                        reopened_cut: self.tx_key().tx_id.0,
                        progress_step: 0,
                        footprint: requested,
                        active: false,
                    },
                };
                let blocked_by_active = grants
                    .iter()
                    .any(|g| g.active && g.overlaps_footprint(&entry.footprint));
                if !blocked_by_active {
                    let mut overlapping_waiters: Vec<WaiterEntry> = grants
                        .iter()
                        .filter(|g| !g.active && g.overlaps_footprint(&entry.footprint))
                        .map(DurableGrantRow::as_waiter)
                        .collect();
                    overlapping_waiters.push(entry.as_waiter());
                    if let Some(winner) = footprint_grant::select_next_holder(&overlapping_waiters)
                        && winner == entry.as_waiter()
                    {
                        entry.active = true;
                        entry.reopened_cut = self.tx_key().tx_id.0;
                    }
                }
                self.write_ss8_grant_row(&entry)
            }
            OwnedFootprintGrantOp::TransferOnLeaseLoss {
                work_key,
                replacement_attempt,
                progress_step,
                additional_footprint,
            } => {
                let grants = self.load_ss8_grants().await?;
                let Some(mut grant) = grants.into_iter().find(|g| g.work_key == *work_key) else {
                    return Err(Anomaly::new(
                        AnomalyCategory::Conflict,
                        "rtdb.owned/footprint-grant-missing",
                        "No durable SS8 grant exists for lease-loss transfer",
                    ));
                };
                let mut next_footprint = grant.footprint.clone();
                next_footprint.extend(additional_footprint.iter().cloned());
                let verdict = footprint_grant::transfer_on_lease_loss(
                    grant.as_holder(),
                    work_key_id(work_key),
                    *replacement_attempt,
                    *progress_step,
                    next_footprint.len() as u64,
                );
                match verdict {
                    TransferVerdict::Transferred(transferred) => {
                        grant.holder_attempt = transferred.holder_attempt;
                        grant.progress_step = transferred.progress_step;
                        grant.footprint = next_footprint;
                        grant.active = transferred.active;
                        self.write_ss8_grant_row(&grant)
                    }
                    TransferVerdict::RejectNotActive
                    | TransferVerdict::RejectWrongWorkKey
                    | TransferVerdict::RejectNonAdvancingAttempt
                    | TransferVerdict::RejectRegressingProgress => Err(Anomaly::new(
                        AnomalyCategory::Conflict,
                        "rtdb.owned/footprint-grant-transfer-rejected",
                        "Lease-loss transfer requires an active durable grant and advancing replacement attempt",
                    )),
                }
            }
            OwnedFootprintGrantOp::Release {
                work_key,
                holder_attempt,
            } => {
                self.release_and_rotate_ss8_grant(work_key, Some(*holder_attempt))
                    .await
            }
        }
    }

    /// Retire a work key's durable debit and (if active) release its grant and
    /// rotate to the strictly oldest eligible waiting work key.
    pub(super) async fn retire_ss8_work_on_commit(
        &mut self,
        work_key: &str,
        holder_attempt: Option<u64>,
    ) -> Result<()> {
        if self.load_ss8_debit(work_key).await?.is_some() {
            self.delete_ss8_debit_row(work_key)?;
        }
        self.release_and_rotate_ss8_grant(work_key, holder_attempt)
            .await
    }

    async fn release_and_rotate_ss8_grant(
        &mut self,
        work_key: &str,
        expected_attempt: Option<u64>,
    ) -> Result<()> {
        let mut grants = self.load_ss8_grants().await?;
        let Some(index) = grants.iter().position(|g| g.work_key == work_key) else {
            return Ok(());
        };
        let released = grants.remove(index);
        if let Some(attempt) = expected_attempt
            && released.active
            && !matches!(
                footprint_grant::admit_grant_proposal(
                    released.as_holder(),
                    work_key_id(work_key),
                    attempt,
                ),
                GrantProposalVerdict::AdmitAndRelease
            )
        {
            return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.owned/footprint-grant-stale-attempt",
                "Cannot release a durable SS8 grant with a stale attempt token",
            ));
        }
        self.delete_ss8_grant_row(work_key)?;
        // Rotate strictly by work-key age among remaining eligible waiters
        // whose footprint is not blocked by another active grant.
        let eligible: Vec<WaiterEntry> = grants
            .iter()
            .filter(|candidate| {
                !candidate.active
                    && !grants
                        .iter()
                        .any(|other| other.active && other.overlaps_footprint(&candidate.footprint))
            })
            .map(DurableGrantRow::as_waiter)
            .collect();
        if let Some(winner) = footprint_grant::select_next_holder(&eligible)
            && let Some(promoted) = grants
                .iter_mut()
                .find(|g| !g.active && g.as_waiter() == winner)
        {
            promoted.active = true;
            promoted.reopened_cut = self.tx_key().tx_id.0;
            self.write_ss8_grant_row(promoted)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_grant_debits_only_the_conflicts_the_resolver_raises_for_changed_data() {
        assert!(is_definite_occ_conflict(&footprint_grant_fenced()));
        assert!(is_definite_occ_conflict(&assert_failed("Assert failed".into())));
        assert!(!is_definite_occ_conflict(&Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/expired", "deadline")));
        assert!(!is_definite_occ_conflict(&Anomaly::new(AnomalyCategory::Conflict, ASSERT_FAILED, "a code alone is not the kind")));
        for error in [footprint_grant_fenced(), assert_failed("Assert failed".into())] {
            assert_eq!(AnomalyKind::from_wire_code(&error.code), error.kind, "{error}");
        }
    }
}
