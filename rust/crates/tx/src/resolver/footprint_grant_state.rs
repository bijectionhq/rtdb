//! Durable RTDB resolver driver for SS8 footprint grants, work-keyed OCC
//! debits, strict work-key age rotation, lease-loss transfer, and all-writer
//! (Flight + SQL) fencing.
use std::{collections::BTreeSet, sync::Arc};

use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, builder::StringBuilder, cast::AsArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_types::{
    Anomaly, AnomalyCategory, AnomalyKind, EntityId, Result, StructFields, TableRef, Timestamp, Value,
};
use sha2::{Digest, Sha256};

use super::footprint_grant::{
    self, GrantHolder, GrantProposalVerdict, TransferVerdict,
    WaiterEntry, WriterKind,
};
use super::{OpenTransaction, interval, overflow, positioned_control::{ControlCell, control_record}};
use crate::OwnedFootprintGrantOp;

/// The resolver's durable footprint grant settings, configured from the node configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FootprintGrantOptions {
    /// The optimistic conflicts a work key may absorb before its durable debit is exhausted, where its
    /// `DebitOnConflict` operation names no limit of its own.
    pub optimistic_conflict_limit: std::num::NonZeroU8,
}

impl FootprintGrantOptions {
    pub const DEFAULT_OPTIMISTIC_CONFLICT_LIMIT: std::num::NonZeroU8 = std::num::NonZeroU8::new(3).unwrap();
}

impl Default for FootprintGrantOptions {
    fn default() -> Self { Self { optimistic_conflict_limit: Self::DEFAULT_OPTIMISTIC_CONFLICT_LIMIT } }
}

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

/// The credits a work key's first durable debit starts from: the operation's own limit, else the configured one.
fn initial_occ_credits(limit: u8, options: FootprintGrantOptions) -> u8 {
    if limit == 0 { options.optimistic_conflict_limit.get() } else { limit }
}

#[derive(Clone, Debug)]
pub(crate) struct DurableGrantRow {
    pub work_key: String,
    pub work_age: u64,
    pub holder_attempt: u64,
    pub reopened_cut: i64,
    pub progress_step: u64,
    pub footprint: BTreeSet<String>,
    pub active: bool,
    positioned_footprint: Option<ArrayRef>,
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

    fn positioned_document(&self, source: &ArrayRef, memory: &Arc<MemoryBudget>) -> Result<Option<ArrayRef>> {
        let Some(footprint) = footprint_with_additions(source, &self.footprint, memory)? else {
            return Ok(None);
        };
        control_record(memory, &[
            ("_id", ControlCell::Value(Value::Utf8(self.work_key.clone()))),
            ("work_age", ControlCell::Value(Value::I64(self.work_age as i64))),
            ("holder_attempt", ControlCell::Value(Value::I64(self.holder_attempt as i64))),
            ("reopened_cut", ControlCell::Value(Value::I64(self.reopened_cut))),
            ("progress_step", ControlCell::Value(Value::I64(self.progress_step as i64))),
            ("footprint", ControlCell::Funded(&footprint)),
            ("status", ControlCell::Value(Value::Utf8(self.status().into()))),
        ]).map(Some)
    }

    fn status(&self) -> &'static str {
        if self.active { STATUS_ACTIVE } else { STATUS_WAITING }
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
                Value::Utf8(self.status().into()),
            ),
        ])
    }
}

#[derive(Clone, Copy)]
enum RetainFootprint<'a> {
    WorkKey(&'a str),
    FirstWaiterExcept(&'a str),
}

enum GrantScan {
    Complete(Vec<DurableGrantRow>),
    RetryWithoutFootprint,
}

enum StoredFootprint {
    Decoded(Value),
    Arrow(ArrayRef),
}

impl StoredFootprint {
    fn normalize(self) -> (BTreeSet<String>, Option<ArrayRef>) {
        match self {
            Self::Arrow(arrow) => {
                let items = arrow.as_list::<i32>().value(0);
                let names = items.as_string::<i32>().iter().flatten().map(str::to_owned).collect();
                (names, Some(arrow))
            }
            Self::Decoded(Value::List(items)) => {
                let names = (0..items.len()).filter_map(|index| items.view().utf8(index))
                    .map(str::to_owned).collect();
                (names, None)
            }
            Self::Decoded(_) => (BTreeSet::new(), None),
        }
    }
}

fn canonical_footprint(batch: &RecordBatch, row: usize) -> Option<ArrayRef> {
    let (slot, field) = batch.schema_ref().column_with_name("footprint")?;
    let array = batch.column(slot);
    let list = array.as_any().downcast_ref::<ListArray>()?;
    let DataType::List(item) = field.data_type() else { return None; };
    if !field.metadata().is_empty() || item.as_ref() != &Field::new("$data$", DataType::Utf8, false)
        || list.is_null(row)
    {
        return None;
    }
    let items = list.value(row);
    let strings = items.as_string::<i32>();
    if strings.is_empty() || strings.null_count() != 0
        || (1..strings.len()).any(|index| strings.value(index - 1) >= strings.value(index))
    {
        return None;
    }
    Some(array.slice(row, 1))
}

fn copy_footprint(batch: &RecordBatch, row: usize, memory: &Arc<MemoryBudget>) -> Option<ArrayRef> {
    let source = canonical_footprint(batch, row)?;
    let field = batch.schema_ref().column_with_name("footprint")?.1;
    let bound = rtdb_columnar::estimate_event_copy_cell_bytes(field, source.as_ref(), 0).ok()?
        .checked_mul(4)?.checked_add(4096)?;
    let mut charge = memory.try_reserve(bound)?;
    let copied = rtdb_columnar::copy_event_cells(&[source.as_ref()], &[Some((0, 0))]).ok()?;
    Arc::get_mut(&mut charge)?.shrink_to(copied.get_array_memory_size()).ok()?;
    rtdb_columnar::retain_array(copied, &charge).ok()
}

fn footprint_with_additions(
    source: &ArrayRef, footprint: &BTreeSet<String>, memory: &Arc<MemoryBudget>,
) -> Result<Option<ArrayRef>> {
    let DataType::List(item) = source.data_type() else { return Ok(None); };
    let children = source.as_list::<i32>().value(0);
    let prior = children.as_string::<i32>();
    if prior.is_empty() || prior.null_count() != 0 || prior.len() > footprint.len()
        || !prior.iter().flatten().all(|table| footprint.contains(table))
    {
        return Ok(None);
    }
    if prior.len() == footprint.len() {
        return Ok(Some(Arc::clone(source)));
    }

    let bytes = footprint.iter().try_fold(0usize, |bytes, name| {
        bytes.checked_add(name.len()).ok_or_else(overflow)
    })?;
    if i32::try_from(bytes).is_err() || i32::try_from(footprint.len()).is_err() {
        return Ok(None);
    }
    let bound = footprint.len().checked_mul(64)
        .and_then(|rows| bytes.checked_mul(4)?.checked_add(rows))
        .and_then(|bytes| bytes.checked_add(8192)).ok_or_else(overflow)?;
    let mut charge = memory.reserve(bound)?;
    let mut additions = StringBuilder::with_capacity(footprint.len() - prior.len(), 0);
    let mut previous = prior.iter().flatten().enumerate().peekable();
    let mut new_rows = 0usize;
    let mut positions = Vec::with_capacity(footprint.len());
    for name in footprint {
        if previous.peek().is_some_and(|&(_, table)| table == name.as_str()) {
            let (index, _) = previous.next().expect("present footprint member");
            positions.push(Some((0, index)));
        } else {
            additions.append_value(name);
            positions.push(Some((1, new_rows)));
            new_rows += 1;
        }
    }
    let additions = additions.finish();
    let entries = rtdb_columnar::copy_event_cells(&[prior, &additions], &positions)?;
    drop(positions);
    drop(additions);
    let list = ListArray::try_new(Arc::clone(item), OffsetBuffer::from_lengths([footprint.len()]), entries, None)
        .map_err(rtdb_columnar::arrow_error)?;
    Arc::get_mut(&mut charge).expect("Footprint construction owns its reservation")
        .shrink_to(list.get_array_memory_size())?;
    rtdb_columnar::retain_array(Arc::new(list), &charge).map(Some)
}

impl OpenTransaction<'_> {
    async fn load_ss8_grants(&self, retain: Option<RetainFootprint<'_>>) -> Result<Vec<DurableGrantRow>> {
        match self.scan_ss8_grants(retain).await? {
            GrantScan::Complete(rows) => Ok(rows),
            GrantScan::RetryWithoutFootprint => match self.scan_ss8_grants(None).await? {
                GrantScan::Complete(rows) => Ok(rows),
                GrantScan::RetryWithoutFootprint => unreachable!("An ordinary scan retains no footprint"),
            },
        }
    }

    async fn scan_ss8_grants(&self, mut retain: Option<RetainFootprint<'_>>) -> Result<GrantScan> {
        let table = ss8_footprint_grants_table();
        if !self.snapshot()?.contains_table(&table) {
            return Ok(GrantScan::Complete(Vec::new()));
        }
        let mut query = self
            .open_query(
                "SELECT _id, work_age, holder_attempt, reopened_cut, progress_step, footprint, status FROM xt.ss8_footprint_grants FOR ALL VALID_TIME",
                Vec::new(),
            )
            .await?;
        let mut rows = Vec::new();
        let mut retained_footprint = false;
        loop {
            let next = match query.next_batch().await {
                Err(error) if retained_footprint && error.is(AnomalyKind::ResourceExhausted) => {
                    return Ok(GrantScan::RetryWithoutFootprint);
                }
                other => other?,
            };
            let Some(batch) = next else { return Ok(GrantScan::Complete(rows)); };
            let batch = batch.batch();
            for row in 0..batch.num_rows() {
                let cell = |index: usize| {
                    read_value(batch.schema().field(index), batch.column(index).as_ref(), row)
                };
                let work_key_cell = cell(0)?;
                let keep_footprint = matches!(
                    (&work_key_cell, retain),
                    (Value::Utf8(key), Some(RetainFootprint::WorkKey(target))) if key.as_str() == target
                );
                if keep_footprint {
                    retain = None;
                }
                let footprint_cell = || {
                    let positioned = if keep_footprint { copy_footprint(batch, row, &self.context.memory) } else { None };
                    match positioned {
                        Some(arrow) => Ok(StoredFootprint::Arrow(arrow)),
                        None => cell(5).map(StoredFootprint::Decoded),
                    }
                };
                let (work_age, holder_attempt, reopened_cut, progress_step, stored_footprint, status) = (
                    cell(1)?, cell(2)?, cell(3)?, cell(4)?, footprint_cell()?, cell(6)?,
                );
                let (
                    Value::Utf8(work_key),
                    Value::I64(work_age),
                    Value::I64(holder_attempt),
                    Value::I64(reopened_cut),
                    Value::I64(progress_step),
                    stored_footprint,
                    Value::Utf8(status),
                ) = (work_key_cell, work_age, holder_attempt, reopened_cut, progress_step, stored_footprint, status)
                else {
                    return Err(Anomaly::fault(
                        "rtdb.tx/ss8-grant-row",
                        "Durable SS8 footprint grant row has an invalid schema",
                    ));
                };
                let (footprint, mut positioned_footprint) = stored_footprint.normalize();
                if status == STATUS_WAITING
                    && matches!(retain, Some(RetainFootprint::FirstWaiterExcept(released)) if work_key.as_str() != released)
                {
                    retain = None;
                    positioned_footprint = copy_footprint(batch, row, &self.context.memory);
                }
                retained_footprint |= positioned_footprint.is_some();
                rows.push(DurableGrantRow {
                    work_key,
                    work_age: work_age as u64,
                    holder_attempt: holder_attempt as u64,
                    reopened_cut,
                    progress_step: progress_step as u64,
                    footprint,
                    active: status == STATUS_ACTIVE,
                    positioned_footprint,
                });
            }
        }
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

    fn write_ss8_grant_row(&mut self, mut row: DurableGrantRow) -> Result<()> {
        let table = ss8_footprint_grants_table();
        let valid = interval(Timestamp(0), Timestamp::MAX)?;
        let staged = row.positioned_footprint.take().is_some_and(|source| {
            row.positioned_document(&source, &self.context.memory).ok().flatten().is_some_and(|document| {
                let iid = EntityId::string_iid(&row.work_key);
                self.try_stage_control_put(&table, iid, valid, document)
            })
        });
        if staged {
            return Ok(());
        }
        self.buffer.put(table, row.to_document(), valid)
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
        let grants = self.load_ss8_grants(None).await?;
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
    /// definite OCC abort. An operation limit of zero takes the configured
    /// optimistic conflict limit.
    pub(super) async fn record_ss8_occ_debit(
        &mut self,
        work_key: &str,
        work_age: u64,
        limit: u8,
    ) -> Result<()> {
        let initial = initial_occ_credits(limit, self.context.footprint_grants);
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
                let grants = self.load_ss8_grants(None).await?;
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
                let mut grants = self.load_ss8_grants(Some(RetainFootprint::WorkKey(work_key))).await?;
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
                            self.write_ss8_grant_row(prior)?;
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
                        positioned_footprint: None,
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
                self.write_ss8_grant_row(entry)
            }
            OwnedFootprintGrantOp::TransferOnLeaseLoss {
                work_key,
                replacement_attempt,
                progress_step,
                additional_footprint,
            } => {
                let grants = self.load_ss8_grants(Some(RetainFootprint::WorkKey(work_key))).await?;
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
                        self.write_ss8_grant_row(grant)
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
        let mut grants = self.load_ss8_grants(Some(RetainFootprint::FirstWaiterExcept(work_key))).await?;
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
        let mut candidates = grants.into_iter();
        let promoted = footprint_grant::select_next_holder(&eligible)
            .and_then(|winner| candidates.find(|grant| !grant.active && grant.as_waiter() == winner));
        drop(candidates);
        self.stage_ss8_rotation(work_key, promoted)
    }

    fn stage_ss8_rotation(&mut self, work_key: &str, mut promoted: Option<DurableGrantRow>) -> Result<()> {
        match self.delete_ss8_grant_row(work_key) {
            Err(error) if error.is(AnomalyKind::ResourceExhausted) => {
                let retained = promoted.as_mut().and_then(|grant| grant.positioned_footprint.take());
                if retained.is_none() {
                    return Err(error);
                }
                drop(retained);
                self.delete_ss8_grant_row(work_key)?;
            }
            result => result?,
        }
        if let Some(mut promoted) = promoted {
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
    fn a_grant_reuses_prior_members_of_a_normalized_footprint() -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        let mut grant = DurableGrantRow {
            work_key: "work".into(), work_age: 1, holder_attempt: 2, reopened_cut: 3, progress_step: 0,
            footprint: ["public.a".into(), "public.b".into()].into(), active: false, positioned_footprint: None,
        };
        let prior = Value::Struct(grant.to_document());
        let arrow = rtdb_columnar::build_array(&prior.logical_type(), &[prior])?;
        let arrow = rtdb_columnar::retain_array(arrow, &memory.reserve(1 << 15)?)?;
        let batch = RecordBatch::from(arrow.as_struct().clone());
        assert!(copy_footprint(&batch, 0, &MemoryBudget::new(1)).is_none());
        grant.positioned_footprint = copy_footprint(&batch, 0, &memory);
        assert!(grant.positioned_footprint.is_some());
        grant.active = true;
        grant.holder_attempt = 4;
        grant.progress_step = 3;
        let source = grant.positioned_footprint.as_ref().unwrap();
        let positioned = grant.positioned_document(source, &memory)?.expect("same canonical footprint");
        super::super::positioned_control::assert_control_bytes(
            &memory, &ss8_footprint_grants_table(), grant.to_document(), positioned, None,
        )?;

        grant.footprint.extend(["public.aa".into(), "public.b".into(), "public.z".into()]);
        assert_eq!(
            footprint_with_additions(source, &grant.footprint, &MemoryBudget::new(1)).unwrap_err().code,
            "rtdb/memory-budget-exceeded",
        );
        let positioned = grant.positioned_document(source, &memory)?.expect("canonical footprint with additions");
        super::super::positioned_control::assert_control_bytes(
            &memory, &ss8_footprint_grants_table(), grant.to_document(), positioned, None,
        )?;

        grant.footprint.remove("public.a");
        assert!(grant.positioned_document(source, &memory)?.is_none());
        for names in [["public.b", "public.a"], ["public.a", "public.a"]] {
            let list = Value::list(names.map(|name| Value::Utf8(name.into())).into());
            let ty = list.logical_type();
            let field = rtdb_columnar::to_field("footprint", &ty)?;
            let array = rtdb_columnar::build_array(&ty, &[list])?;
            let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![field])), vec![array])
                .map_err(rtdb_columnar::arrow_error)?;
            assert!(canonical_footprint(&batch, 0).is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn rotation_can_publish_one_funded_waiter_without_changing_event_bytes() -> Result<()> {
        use rtdb_index::{PartitionState, TransactionBuffer};
        use rtdb_types::{TransactionKey, TxId};

        let memory = MemoryBudget::new(1 << 24);
        let context = grant_test_context(&memory);
        let options = crate::SubmitOptions::default();
        let base = PartitionState::empty(Default::default())?;
        let prior = TransactionKey { tx_id: TxId(1), system_time: Timestamp(10) };
        let key = TransactionKey { tx_id: TxId(2), system_time: Timestamp(20) };
        let table = ss8_footprint_grants_table();
        let valid = interval(Timestamp(0), Timestamp::MAX)?;
        let mut waiter = DurableGrantRow {
            work_key: "waiter".into(), work_age: 2, holder_attempt: 1, reopened_cut: prior.tx_id.0, progress_step: 0,
            footprint: ["public.a".into(), "public.b".into()].into(), active: false, positioned_footprint: None,
        };
        let mut released = waiter.clone();
        released.work_key = "released".into();
        released.active = true;
        let mut seed = TransactionBuffer::new(base.snapshot(), prior, Arc::clone(&memory))?;
        seed.put(table.clone(), released.to_document(), valid)?;
        seed.put(table.clone(), waiter.to_document(), valid)?;
        base.apply(&seed.stage()?)?;

        let mut transaction = OpenTransaction::new(base.snapshot(), key, &options, &context)?;
        let rows = transaction.load_ss8_grants(Some(RetainFootprint::FirstWaiterExcept("released"))).await?;
        let selected = rows.iter().filter(|grant| grant.positioned_footprint.is_some())
            .map(|grant| grant.work_key.as_str()).collect::<Vec<_>>();
        assert_eq!(selected, ["waiter"]);
        drop(selected);
        drop(rows);
        transaction.release_and_rotate_ss8_grant("released", Some(released.holder_attempt)).await?;

        let mut baseline = TransactionBuffer::new(base.snapshot(), key, Arc::clone(&memory))?;
        baseline.delete(table.clone(), &EntityId::String(released.work_key), valid)?;
        waiter.active = true;
        waiter.reopened_cut = key.tx_id.0;
        baseline.put(table.clone(), waiter.to_document(), valid)?;

        let expected = baseline.stage()?;
        let actual = transaction.buffer.stage()?;
        let encode = |batch: &RecordBatch| crate::ipc::encode(batch.schema_ref(), [batch], &memory);
        let expected = expected.tables()[&table].batch().unwrap().arrow();
        let actual = actual.tables()[&table].batch().unwrap().arrow();
        assert_eq!(encode(expected)?.as_slice(), encode(actual)?.as_slice());
        Ok(())
    }

    #[test]
    fn rotation_releases_its_copy_when_only_the_ordinary_writes_fit() -> Result<()> {
        use rtdb_index::PartitionState;
        use rtdb_types::{TransactionKey, TxId};

        let table = ss8_footprint_grants_table();
        let base = PartitionState::empty(Default::default())?;
        let options = crate::SubmitOptions::default();
        let key = TransactionKey { tx_id: TxId(1), system_time: Timestamp(10) };
        let mut waiter = DurableGrantRow {
            work_key: "waiter".into(), work_age: 2, holder_attempt: 1, reopened_cut: key.tx_id.0, progress_step: 0,
            footprint: ["public.a".into(), "public.b".into()].into(), active: true, positioned_footprint: None,
        };
        let delete_bytes = 4096 + table.schema.len() + table.table.len();
        let put_bytes = delete_bytes + Value::document_retained_bytes(&waiter.to_document())?;
        waiter.active = false;
        let footprint = waiter.to_document().remove("footprint").unwrap();
        let array = rtdb_columnar::build_array(&footprint.logical_type(), &[footprint])?;
        let compact_bytes = array.get_array_memory_size();
        assert!(compact_bytes < put_bytes && put_bytes < 8192);

        for footprint_bytes in [compact_bytes, put_bytes + 1] {
            let memory = MemoryBudget::new(delete_bytes + put_bytes);
            let context = grant_test_context(&memory);
            let mut transaction = OpenTransaction::new(base.snapshot(), key, &options, &context)?;
            let mut promoted = waiter.clone();
            promoted.positioned_footprint = Some(rtdb_columnar::retain_array(Arc::clone(&array), &memory.reserve(footprint_bytes)?)?);
            transaction.stage_ss8_rotation("released", Some(promoted))?;
            assert!(transaction.buffer.has_pending_table(&table));
            assert_eq!(memory.used_bytes(), put_bytes + delete_bytes);
        }
        Ok(())
    }

    fn grant_test_context(memory: &Arc<MemoryBudget>) -> super::super::ResolveContext {
        use rtdb_query::QueryOptions;
        use rtdb_storage::cache::BufferPool;
        use tokio_util::sync::CancellationToken;

        struct AllowRoles;
        impl super::super::TransactionAuthorization for AllowRoles {
            fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) }
        }

        super::super::ResolveContext {
            footprint_grants: Default::default(), decision_clock: None,
            write_event_origins: false, placed_receipt_retirement: false, owned_witness_scope: None,
            world_constraints: None, database: "application".into(),
            storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(memory), query_options: QueryOptions::default(), scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(AllowRoles),
            execution_observer: None, tracer: None,
        }
    }

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

    #[test]
    fn an_unlimited_debit_starts_from_the_configured_optimistic_conflict_limit() {
        assert_eq!(FootprintGrantOptions::default().optimistic_conflict_limit.get(), 3);
        assert_eq!(initial_occ_credits(0, FootprintGrantOptions::default()), 3);
        let configured = FootprintGrantOptions { optimistic_conflict_limit: std::num::NonZeroU8::new(7).unwrap() };
        assert_eq!(initial_occ_credits(0, configured), 7);
        assert_eq!(initial_occ_credits(2, configured), 2, "an operation's own limit wins over the configured one");
    }
}
