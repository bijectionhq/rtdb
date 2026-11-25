use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_db::classic_import_basis;
use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_index::branches::{BranchEvent, BranchIndex, BranchView, WrittenVersion};
use rtdb_types::{
    EntityId, EventHeader, EventPosition, Interval, Result, StructFields, TableRef, TimeUnit,
    Timestamp, TransactionKey, TxId, Value,
    rtdb_v1::{At, Layer, MAIN_BRANCH},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::layout::{ApplicationLayout, SqlReleaseLayout};
use super::points::{PointHistories, PointHistoryRead, PointSelector};
use super::receipt_policy::{ReceiptPolicyReadback, ReceiptPolicySelection};
use super::{RelationInventory, RetainedInventory, inconsistent, unavailable};
use crate::archive::{encode, sha256};
use crate::recovery::classic_provider::ReleaseSelection;

mod provider;

const MAX_ROWS: usize = 4096;
const MAX_INVENTORY_BYTES: usize = 2 << 20;

pub(super) struct Application {
    layout: Arc<ApplicationLayout>,
    begin: [u8; 32],
    cut: TransactionKey,
    authority: Uuid,
    committed: Option<[u8; 32]>,
    events: BTreeMap<(TableRef, EntityId, TxId, Timestamp, Timestamp), EventHeader>,
    charges: Vec<Arc<MemoryReservation>>,
    points: Option<PointHistoryRead>,
    receipt_policy: Option<ReceiptPolicySelection>,
    provider_release: Option<ReleaseSelection>,
}

pub(crate) enum ApplicationEvidence {
    Closure(ApplicationReadback),
    Points(PointHistories),
    ReceiptPolicy(ReceiptPolicyReadback),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DrainSelection {
    begin_sha256: [u8; 32],
    custody: [u8; 32],
    writer: WriterSelection,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriterSelection {
    nonce: String,
    written: TransactionKey,
    acknowledged: TransactionKey,
}

#[derive(Serialize)]
pub(crate) struct ApplicationReadback {
    pub(crate) classic_cut: TransactionKey,
    pub(crate) source_epoch: u64,
    pub(crate) source_assignment: [u8; 16],
    pub(crate) custody_sha256: [u8; 32],
    pub(crate) selection_sha256: [u8; 32],
    closure: Stamp,
    sql_release_cutoff: Stamp,
    closed_writer: Stamp,
    predecessor_writer: Stamp,
    custody_pin: Stamp,
    writer_acknowledged: TransactionKey,
    inventory: Vec<DispositionRelation>,
    retained: RetainedInventory,
    #[serde(skip)]
    pub(crate) provider_release: Option<(TransactionKey, [u8; 32])>,
    #[serde(skip)]
    _charges: Vec<Arc<MemoryReservation>>,
}

#[derive(Serialize)]
struct Stamp {
    written: TransactionKey,
    position: Option<EventPosition>,
    commit_position: u32,
    valid_time: Interval,
    row_sha256: [u8; 32],
}

#[derive(Serialize)]
struct DispositionRelation {
    table: String,
    history_sha256: Option<[u8; 32]>,
    rows: Vec<DispositionRow>,
}

#[derive(Serialize)]
struct DispositionRow {
    id: EntityId,
    stamp: Stamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    admission_sha256: Option<[u8; 32]>,
}

impl Application {
    pub(super) fn new(
        layout: Arc<ApplicationLayout>,
        begin: [u8; 32],
        cut: TransactionKey,
        authority: Uuid,
    ) -> Self {
        Self {
            layout,
            begin,
            cut,
            authority,
            committed: None,
            events: BTreeMap::new(),
            charges: Vec::new(),
            points: None,
            receipt_policy: None,
            provider_release: None,
        }
    }

    pub(super) fn with_points(mut self, points: Option<PointHistoryRead>) -> Self {
        self.points = points;
        self
    }

    pub(super) fn with_receipt_policy(mut self, selection: Option<ReceiptPolicySelection>) -> Self {
        self.receipt_policy = selection;
        self
    }

    pub(super) fn with_provider_release(mut self, release: Option<ReleaseSelection>) -> Self {
        self.provider_release = release;
        self
    }

    pub(super) fn observe_transaction(
        &mut self,
        event: &StoredEvent,
        sha256: [u8; 32],
    ) -> Result<()> {
        let StoredOperation::Put(row) = &event.operation else {
            return Ok(());
        };
        if row.get("_id") != Some(&Value::I64(self.cut.tx_id.0)) {
            if matches!(row.get("system_time"), Some(Value::Timestamp { value, unit: TimeUnit::Microsecond, .. }) if *value == self.cut.system_time.0)
                && row.get("committed") == Some(&Value::Bool(true))
            {
                return Err(invalid());
            }
            return Ok(());
        }
        if self.committed.is_some_and(|prior| prior != sha256)
            || event.system_from != self.cut.system_time
            || event
                .position
                .is_some_and(|position| position.transaction != self.cut.tx_id)
            || !matches!(row.get("system_time"), Some(Value::Timestamp { value, unit: TimeUnit::Microsecond, .. }) if *value == self.cut.system_time.0)
            || row.get("committed") != Some(&Value::Bool(true))
        {
            return Err(invalid());
        }
        self.committed = Some(sha256);
        Ok(())
    }

    pub(super) fn observe_event(
        &mut self,
        table: &TableRef,
        tx: TxId,
        event: &StoredEvent,
        memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        if tx > self.cut.tx_id {
            return Ok(());
        }
        if let Some(points) = &mut self.points {
            points.observe(table, tx, event, memory)?;
        }
        if !self.layout.observes(table) {
            return Ok(());
        }
        let StoredOperation::Put(cells) = &event.operation else {
            return Ok(());
        };
        let id = cells.get("_id").ok_or_else(invalid)?.entity_id()?;
        if self.points.is_some()
            && ![&self.layout.closure, &self.layout.writer]
                .iter()
                .any(|point| &point.table == table && point.id == id)
        {
            return Ok(());
        }
        let header = event.header();
        let key = (
            table.clone(),
            id,
            tx,
            header.valid_time.lower(),
            header.valid_time.upper(),
        );
        if self
            .events
            .get(&key)
            .is_none_or(|prior| prior.precedence() <= header.precedence())
        {
            if !self.events.contains_key(&key) {
                self.charges
                    .push(memory.reserve(encode(&key)?.len() + 256)?);
            }
            self.events.insert(key, header);
        }
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        index: &BranchIndex,
        retained: &RetainedInventory,
        relations: &[RelationInventory],
        terminal_key: TransactionKey,
        memory: &Arc<MemoryBudget>,
    ) -> Result<ApplicationEvidence> {
        if self.committed.is_none() {
            return Err(invalid());
        }
        let at = self.cut.system_time;
        let view = self.view(index, self.cut)?;
        let layout = &self.layout;
        let (closure, closure_cells) = point(&view, &layout.closure, at)?;
        let body = binary(closure_cells, "binding")?;
        if body.len() > 16 << 10 {
            return Err(invalid());
        }
        let selected: DrainSelection = serde_json::from_slice(body).map_err(|_| invalid())?;
        if selected.begin_sha256 != self.begin
            || selected.custody == [0; 32]
            || transaction(closure)? != self.cut
            || selected.writer.nonce.is_empty()
            || !selected
                .writer
                .written
                .at_or_before(selected.writer.acknowledged)
            || !before(selected.writer.acknowledged, self.cut)
        {
            return Err(invalid());
        }
        self.view(index, selected.writer.acknowledged)?;
        let (closed, closed_cells) = point(&view, &layout.writer, at)?;
        let (epoch, assignment) = placement(closed_cells, self.authority)?;
        if transaction(closed)? != self.cut
            || closed_cells.get("writer") != Some(&Value::Utf8(String::new()))
        {
            return Err(invalid());
        }
        let previous = self.view(index, selected.writer.written)?;
        let (predecessor, prior_cells) = point(
            &previous,
            &layout.writer,
            selected.writer.written.system_time,
        )?;
        if transaction(predecessor)? != selected.writer.written
            || placement(prior_cells, self.authority)? != (epoch, assignment)
            || prior_cells.get("writer") != Some(&Value::Utf8(selected.writer.nonce))
        {
            return Err(invalid());
        }
        let (custody_pin, custody) = point(&view, &layout.custody, at)?;
        if binary(custody, "fingerprint")? != selected.custody.as_slice() {
            return Err(invalid());
        }
        let cutoff = layout.cutoff(epoch);
        let (release, release_cells) = point(&view, &cutoff, at)?;
        let release_cut = transaction(release)?;
        if binary(release_cells, "binding")? != body
            || !before(selected.writer.written, release_cut)
            || !before(release_cut, self.cut)
        {
            return Err(invalid());
        }

        let provider_release = self.provider_release.as_ref().map(|selection| {
            self.read_provider_release(index, &view, selection, epoch, release_cut, sha256(body))
        }).transpose()?;

        let terminal = index.head();
        let end = self.view(index, terminal_key)?;
        if end.cut() != terminal {
            return Err(invalid());
        }
        if point(&end, &layout.closure, terminal.system_time)?.0 != closure
            || point(&end, &cutoff, terminal.system_time)?.0 != release
            || point(&end, &layout.writer, terminal.system_time)?.0 != closed
            || point(&end, &layout.custody, terminal.system_time)?.0 != custody_pin
        {
            return Err(invalid());
        }

        let closure_stamp = self.stamp(&layout.closure.table, closure, closure_cells)?;
        let closed_writer = self.stamp(&layout.writer.table, closed, closed_cells)?;
        if closure_stamp.position.is_none()
            || closed_writer.position.is_none()
            || closure_stamp.position == closed_writer.position
        {
            return Err(invalid());
        }
        if let Some(points) = self.points.take() {
            return Ok(ApplicationEvidence::Points(points.finish(&view, self.cut)?));
        }

        if let Some(read) = self.receipt_policy.take() {
            return Ok(ApplicationEvidence::ReceiptPolicy(read.read(
                &end, relations, terminal_key, memory,
            )?));
        }

        let mut inventory = Vec::with_capacity(layout.inventory.len());
        let mut charges = Vec::new();
        let mut bytes = 0usize;
        for table in &layout.inventory {
            let history = relations.iter().find(|relation| &relation.table == table);
            if history.is_some() && view.table(table).is_none() {
                return Err(unavailable(
                    "The application disposition relation lacks retained literal coverage",
                ));
            }
            let raw_events = history.map_or(0, |entry| entry.events);
            let set_bytes = usize::try_from(raw_events)
                .ok()
                .and_then(|events| events.checked_mul(64))
                .ok_or_else(|| {
                    unavailable(
                        "The retained Classic application inventory exceeds the memory bound",
                    )
                })?;
            let _set_charge = memory.reserve(set_bytes)?;
            let mut rows = Vec::new();
            for id in view.ids(table) {
                let Some(row) = view.row(table, id, at) else {
                    continue;
                };
                let cells = &row.image_at(at).ok_or_else(invalid)?.cells;
                require_sql_disposition(&layout.sql_release, &view, at, table, row, cells)?;
                let selected_named_hold = layout.named_holds.as_ref().is_some_and(|named| {
                    &named.table == table
                        && matches!(id, EntityId::String(id) if id.starts_with(&named.id_prefix))
                });
                let row = DispositionRow {
                    id: id.clone(),
                    stamp: self.stamp(table, row, cells)?,
                    admission_sha256: selected_named_hold
                        .then(|| binary(cells, "admission").map(sha256))
                        .transpose()?,
                };
                let len = encode(&row)?.len();
                bytes = bytes
                    .checked_add(len)
                    .filter(|total| *total <= MAX_INVENTORY_BYTES)
                    .ok_or_else(|| {
                        unavailable(
                            "The retained Classic application inventory exceeds the reply bound",
                        )
                    })?;
                if rows.len() == MAX_ROWS {
                    return Err(unavailable(
                        "The retained Classic application relation exceeds the row bound",
                    ));
                }
                charges.push(memory.reserve(len + 256)?);
                rows.push(row);
            }
            inventory.push(DispositionRelation {
                table: table.table.clone(),
                history_sha256: history.map(|entry| entry.sha256),
                rows,
            });
        }
        Ok(ApplicationEvidence::Closure(ApplicationReadback {
            classic_cut: self.cut,
            source_epoch: epoch,
            source_assignment: assignment,
            custody_sha256: selected.custody,
            selection_sha256: sha256(body),
            closure: closure_stamp,
            sql_release_cutoff: self.stamp(&cutoff.table, release, release_cells)?,
            closed_writer,
            predecessor_writer: self.stamp(&layout.writer.table, predecessor, prior_cells)?,
            custody_pin: self.stamp(&layout.custody.table, custody_pin, custody)?,
            writer_acknowledged: selected.writer.acknowledged,
            inventory,
            retained: retained.clone(),
            provider_release,
            _charges: charges,
        }))
    }

    fn view<'a>(&self, index: &'a BranchIndex, cut: TransactionKey) -> Result<BranchView<'a>> {
        let basis = classic_import_basis(*self.authority.as_bytes(), cut.tx_id)?;
        let view = index.view(
            MAIN_BRANCH,
            At::Exactly(basis),
            Layer::Local,
            cut.system_time,
        )?;
        if view.cut().system_time != cut.system_time {
            return Err(invalid());
        }
        Ok(view)
    }

    fn stamp(
        &self,
        table: &TableRef,
        row: &BranchEvent,
        cells: &StructFields<Value>,
    ) -> Result<Stamp> {
        let written = transaction(row)?;
        let key = (
            table.clone(),
            row.id.clone(),
            written.tx_id,
            row.valid.lower(),
            row.valid.upper(),
        );
        let header = self
            .events
            .get(&key)
            .filter(|header| header.system_from == written.system_time)
            .ok_or_else(invalid)?;
        Ok(Stamp {
            written,
            position: header.position,
            commit_position: header.commit_position,
            valid_time: row.valid,
            row_sha256: sha256(&encode(cells)?),
        })
    }
}

impl ApplicationReadback {
    pub(crate) fn sql_cutoff(&self) -> TransactionKey { self.sql_release_cutoff.written }
}

fn require_sql_disposition(
    roles: &SqlReleaseLayout,
    view: &BranchView<'_>,
    at: Timestamp,
    table: &TableRef,
    row: &BranchEvent,
    cells: &StructFields<Value>,
) -> Result<()> {
    if table == &roles.rows || table == &roles.execution_units {
        return Err(invalid());
    }
    if table == &roles.joined {
        return if row.id == EntityId::String(response_key(cells)?) {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    if table == &roles.stopped {
        return verify_stop(cells);
    }
    if table != &roles.grants {
        return Ok(());
    }
    let Value::Utf8(phase) = cells.get("phase").ok_or_else(invalid)? else {
        return Err(invalid());
    };
    let response = response_key(cells)?;
    let (table, id) = match phase.as_str() {
        "cancelled" => return Ok(()),
        "joined" => (&roles.joined, EntityId::String(response)),
        "stopped" => (&roles.stopped, EntityId::String(stop_key(cells)?)),
        _ => return Err(invalid()),
    };
    let receipt = view.row(table, &id, at).ok_or_else(invalid)?;
    let receipt_cells = &receipt.image_at(at).ok_or_else(invalid)?.cells;
    if !transaction(receipt)?.at_or_before(transaction(row)?) {
        return Err(invalid());
    }
    if table == &roles.stopped {
        verify_stop(receipt_cells)
    } else if receipt.id == EntityId::String(response_key(receipt_cells)?) {
        Ok(())
    } else {
        Err(invalid())
    }
}

fn response_key(cells: &StructFields<Value>) -> Result<String> {
    Ok(format!(
        "{}/{}/{}",
        stop_key(cells)?,
        identity(cells, "conductor")?,
        identity(cells, "response")?
    ))
}

fn stop_key(cells: &StructFields<Value>) -> Result<String> {
    Ok(format!(
        "{}/{}",
        identity(cells, "cell_history")?,
        identity(cells, "usher")?
    ))
}

fn identity<'a>(cells: &'a StructFields<Value>, column: &str) -> Result<&'a str> {
    let Some(Value::Utf8(value)) = cells.get(column) else {
        return Err(invalid());
    };
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || value.bytes().all(|byte| byte == b'0')
    {
        return Err(invalid());
    }
    Ok(value)
}

fn verify_stop(cells: &StructFields<Value>) -> Result<()> {
    let bytes: [u8; 8] = binary(cells, "stopped_at")?
        .try_into()
        .map_err(|_| invalid())?;
    if u64::from_be_bytes(bytes) == 0 {
        return Err(invalid());
    }
    Ok(())
}

fn point<'a>(
    view: &BranchView<'a>,
    point: &PointSelector,
    at: Timestamp,
) -> Result<(&'a BranchEvent, &'a StructFields<Value>)> {
    let row = view.row(&point.table, &point.id, at).ok_or_else(invalid)?;
    if row.valid.lower() != Timestamp(0) || row.valid.upper() != Timestamp::MAX {
        return Err(invalid());
    }
    let image = row.image_at(at).ok_or_else(invalid)?;
    Ok((row, &image.cells))
}

fn placement(cells: &StructFields<Value>, authority: Uuid) -> Result<(u64, [u8; 16])> {
    let Some(Value::Utf8(epoch)) = cells.get("placement_epoch") else {
        return Err(invalid());
    };
    let number: u64 = epoch.parse().map_err(|_| invalid())?;
    let assignment = binary(cells, "placement_assignment")?
        .try_into()
        .map_err(|_| invalid())?;
    if number == 0
        || *epoch != format!("{number:020}")
        || assignment == [0; 16]
        || binary(cells, "incarnation")? != authority.as_bytes()
        || !matches!(cells.get("kind"), Some(Value::Utf8(kind)) if kind == "hosted")
    {
        return Err(invalid());
    }
    Ok((number, assignment))
}

fn binary<'a>(cells: &'a StructFields<Value>, column: &str) -> Result<&'a [u8]> {
    match cells.get(column) {
        Some(Value::Binary(value)) => Ok(value),
        _ => Err(invalid()),
    }
}

fn before(before: TransactionKey, after: TransactionKey) -> bool {
    before.tx_id < after.tx_id && before.system_time < after.system_time
}

pub(super) fn transaction(row: &BranchEvent) -> Result<TransactionKey> {
    let id = row
        .cut
        .basis
        .position()
        .checked_sub(1)
        .and_then(|id| i64::try_from(id).ok())
        .ok_or_else(invalid)?;
    if !matches!(row.written, WrittenVersion::Classic(written) if written.0 == id)
        && row.written != WrittenVersion::Unpositioned
    {
        return Err(invalid());
    }
    Ok(TransactionKey {
        tx_id: TxId(id),
        system_time: row.cut.system_time,
    })
}

fn invalid() -> rtdb_types::Anomaly {
    inconsistent("The retained Classic application closure differs from the exact Begin or cut")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_index::branches::BranchCut;

    const HOLD: &str = "hold_controls";
    const INSTALLATION: &str = "installation";
    const CUSTODY: &str = "custody_binding";

    fn layout() -> Result<Arc<ApplicationLayout>> {
        let table = |name: &str| serde_json::json!({"schema":"public","table":name});
        let point =
            |name: &str, id: &str| serde_json::json!({"table":table(name),"id":{"String":id}});
        let inventory = [HOLD, "grants", "rows", "units", "joins", "stops"].map(table);
        let mounts = serde_json::json!({
            "format":1,"closure":point(HOLD,"application-close"),"writer":point(INSTALLATION,"authority"),
            "custody":point(CUSTODY,"authority"),"sql_cutoff_prefix":"sql-cutoff/",
            "inventory":inventory,
            "sql_release":{"grants":table("grants"),"rows":table("rows"),"execution_units":table("units"),
                           "joined":table("joins"),"stopped":table("stops")},
        });
        ApplicationLayout::parse(&encode(&mounts)?).map(Arc::new)
    }

    fn key(id: i64) -> TransactionKey {
        TransactionKey {
            tx_id: TxId(id),
            system_time: Timestamp(id * 10),
        }
    }

    fn event(
        id: EntityId,
        at: TransactionKey,
        ordinal: u64,
        mut cells: StructFields<Value>,
    ) -> StoredEvent {
        cells.insert("_id".into(), id.as_value());
        StoredEvent {
            position: Some(EventPosition {
                transaction: at.tx_id,
                ordinal,
            }),
            origin: None,
            entity_key: None,
            iid: id.iid(),
            system_from: at.system_time,
            commit_position: ordinal as u32,
            valid_time: Interval::new(Timestamp(0), Timestamp::MAX).unwrap(),
            recency: None,
            operation: StoredOperation::Put(cells),
        }
    }

    fn install(authority: Uuid, writer: &str) -> StructFields<Value> {
        StructFields::from([
            (
                "incarnation".into(),
                Value::Binary(authority.as_bytes().to_vec()),
            ),
            ("kind".into(), Value::Utf8("hosted".into())),
            ("placement_assignment".into(), Value::Binary(vec![4; 16])),
            ("placement_epoch".into(), Value::Utf8(format!("{:020}", 7))),
            ("writer".into(), Value::Utf8(writer.into())),
        ])
    }

    #[test]
    fn sealed_application_readback_matches_the_committed_fence_and_preserves_distinct_stamps()
    -> Result<()> {
        let authority = Uuid::from_bytes([1; 16]);
        let chosen = [2; 32];
        let custody = [3; 32];
        let body = encode(&serde_json::json!({"begin_sha256":chosen,"custody":custody,
            "writer":{"nonce":"writer-1","written":key(1),"acknowledged":key(1)}}))?;
        let memory = MemoryBudget::new(1 << 20);
        let layout = layout()?;
        assert!(layout.admits(&PointHistoryRead::new(
            Timestamp(30),
            vec![layout.closure.clone(), layout.writer.clone()]
        )?));
        assert!(!layout.admits(&PointHistoryRead::new(
            Timestamp(30),
            vec![layout.custody.clone()]
        )?));
        let mut application = Application::new(Arc::clone(&layout), chosen, key(3), authority);
        let mut index = BranchIndex::new(*authority.as_bytes());
        let mut write =
            |id: i64, relations: &[(&str, &str, u64, StructFields<Value>)]| -> Result<()> {
                let at = key(id);
                index.advance_classic(BranchCut {
                    basis: classic_import_basis(*authority.as_bytes(), at.tx_id)?,
                    system_time: at.system_time,
                })?;
                for (table, id, ordinal, columns) in relations {
                    let table = TableRef::new("public", *table);
                    let raw = event(
                        EntityId::String((*id).into()),
                        at,
                        *ordinal,
                        columns.clone(),
                    );
                    application.observe_event(&table, at.tx_id, &raw, &memory)?;
                    index.apply_classic_event(&table, raw, Some(at.tx_id))?;
                }
                Ok(())
            };
        write(
            1,
            &[
                (INSTALLATION, "authority", 1, install(authority, "writer-1")),
                (
                    CUSTODY,
                    "authority",
                    2,
                    StructFields::from([("fingerprint".into(), Value::Binary(custody.to_vec()))]),
                ),
            ],
        )?;
        let marker = StructFields::from([("binding".into(), Value::Binary(body.clone()))]);
        let response = StructFields::from([
            ("cell_history".into(), Value::Utf8("01".repeat(16))),
            ("usher".into(), Value::Utf8("02".repeat(16))),
            ("conductor".into(), Value::Utf8("03".repeat(16))),
            ("response".into(), Value::Utf8("04".repeat(16))),
        ]);
        let join_id = response_key(&response)?;
        let stop_id = stop_key(&response)?;
        let grant = |phase: &str| {
            let mut cells = response.clone();
            cells.insert("phase".into(), Value::Utf8(phase.into()));
            cells
        };
        write(
            2,
            &[
                (HOLD, "sql-cutoff/00000000000000000007", 4, marker.clone()),
                ("joins", &join_id, 5, response.clone()),
                (
                    "stops",
                    &stop_id,
                    6,
                    StructFields::from([(
                        "stopped_at".into(),
                        Value::Binary(15u64.to_be_bytes().to_vec()),
                    )]),
                ),
            ],
        )?;
        write(
            3,
            &[
                (HOLD, "application-close", 5, marker),
                (INSTALLATION, "authority", 6, install(authority, "")),
                ("grants", "join", 7, grant("joined")),
                ("grants", "stop", 9, grant("stopped")),
                ("grants", "cancel", 10, grant("cancelled")),
            ],
        )?;
        write(4, &[])?;

        let committed = event(
            EntityId::Integer(3),
            key(3),
            8,
            StructFields::from([
                (
                    "system_time".into(),
                    Value::Timestamp {
                        value: 30,
                        unit: TimeUnit::Microsecond,
                        timezone: None,
                    },
                ),
                ("committed".into(), Value::Bool(true)),
            ]),
        );
        let mut other_cut = Application::new(Arc::clone(&layout), chosen, key(4), authority);
        other_cut.observe_transaction(&committed, [6; 32])?;
        let retained = RetainedInventory {
            worlds: 0,
            suffix_pins: 0,
            other_captures: 0,
            pending_capture_releases: 0,
            sha256: [7; 32],
        };
        assert!(other_cut.finish(&index, &retained, &[], key(4), &memory).is_err());
        application.observe_transaction(&committed, [6; 32])?;
        let ApplicationEvidence::Closure(proof) =
            application.finish(&index, &retained, &[], key(4), &memory)?
        else {
            unreachable!()
        };
        assert_eq!(proof.classic_cut, key(3));
        assert_eq!(proof.selection_sha256, sha256(&body));
        assert_eq!(proof.source_epoch, 7);
        assert_eq!(proof.sql_release_cutoff.written, key(2));
        assert_eq!(proof.closure.position.unwrap().ordinal, 5);
        assert_eq!(proof.closed_writer.position.unwrap().ordinal, 6);
        let grants = &layout.sql_release.grants;
        let basis = classic_import_basis(*authority.as_bytes(), key(3).tx_id)?;
        let view = index.view(MAIN_BRANCH, At::Exactly(basis), Layer::Local, Timestamp(30))?;
        let joined = view
            .row(grants, &EntityId::String("join".into()), Timestamp(30))
            .unwrap();
        let mut wrong = grant("joined");
        wrong.insert("response".into(), Value::Utf8("05".repeat(16)));
        assert!(
            require_sql_disposition(
                &layout.sql_release,
                &view,
                Timestamp(30),
                grants,
                joined,
                &wrong
            )
            .is_err()
        );
        assert_eq!(
            proof
                .inventory
                .iter()
                .find(|entry| entry.table == HOLD)
                .unwrap()
                .rows
                .len(),
            2
        );
        Ok(())
    }
}
