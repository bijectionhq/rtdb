use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_index::branches::{BranchEvent, BranchView};
use rtdb_types::{EntityId, Result, StructFields, TableRef, Timestamp, TransactionKey, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{RelationInventory, RelationKind, inconsistent, unavailable};
use crate::archive::{encode, sha256};

const ADMISSION: &str = "bj_fact_root_admission";
const DECLARATIONS: &str = "bj_relation_declarations";
const MAX_DECLARATIONS: usize = 128;
const MAX_DECLARATION_EVENTS: u64 = 1 << 16;
const MAX_REPLY_BYTES: usize = 3 << 20;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReceiptPolicySelection {
    receipt_epoch: Uuid,
    session: String,
    request: u32,
}

#[derive(Serialize)]
pub(crate) struct ReceiptPolicyReadback {
    selection: ReceiptPolicySelection,
    terminal: TransactionKey,
    declared_relations_sha256: [u8; 32],
    declarations: Vec<PolicyRow>,
    controls: Vec<PolicyPoint>,
    #[serde(skip)]
    _charges: Vec<Arc<MemoryReservation>>,
}

#[derive(Serialize)]
struct PolicyPoint {
    table: TableRef,
    id: EntityId,
    history_sha256: [u8; 32],
    row: Option<PolicyRow>,
}

#[derive(Serialize)]
struct PolicyRow {
    id: EntityId,
    written: TransactionKey,
    document: StructFields<Value>,
}

impl ReceiptPolicySelection {
    pub(super) fn read(
        self,
        view: &BranchView<'_>,
        relations: &[RelationInventory],
        terminal: TransactionKey,
        memory: &Arc<MemoryBudget>,
    ) -> Result<ReceiptPolicyReadback> {
        let session: Uuid = self.session.parse().map_err(|_| invalid())?;
        if self.receipt_epoch.is_nil() || session.is_nil() || session.to_string() != self.session {
            return Err(invalid());
        }

        let receipt = format!(
            "session/{}/{}/{}",
            self.receipt_epoch, self.session, self.request
        );
        let standing = format!(
            "sync_session:{}",
            lower_hex(sha256(self.session.as_bytes()))
        );
        let decision = format!("root_decision:{}", lower_hex(sha256(receipt.as_bytes())));

        let mut readback = ReceiptPolicyReadback {
            selection: self,
            terminal,
            declared_relations_sha256: [0; 32],
            declarations: Vec::new(),
            controls: Vec::new(),
            _charges: Vec::new(),
        };
        let mut budget = ReplyBudget {
            bytes: 0,
            memory,
            charges: &mut readback._charges,
        };
        let declaration_table = TableRef::new("public", DECLARATIONS);
        let declaration_history = complete_relation(view, relations, &declaration_table)?;
        if declaration_history.events > MAX_DECLARATION_EVENTS {
            return Err(oversized());
        }
        let _ids = memory.reserve(declaration_history.events as usize * 64)?;
        readback.declared_relations_sha256 = declaration_history.sha256;
        for id in view.ids(&declaration_table) {
            let Some(row) = view.row(&declaration_table, id, terminal.system_time) else {
                continue;
            };
            let cells = clear_cells(row, terminal.system_time)?;
            let Some(Value::I64(world)) = cells.get("world") else {
                return Err(invalid());
            };
            if *world != 0 {
                continue;
            }
            if readback.declarations.len() == MAX_DECLARATIONS {
                return Err(oversized());
            }
            let row = project(row, cells, &["table_name", "world", "declaration", "names"])?;
            budget.retain(&row)?;
            readback.declarations.push(row);
        }

        for (relation, id, fields) in [
            (
                "bj_installation",
                "authority",
                &["incarnation", "kind", "writer"][..],
            ),
            ("bj_rtdb_restore_hold", "restore", &["binding"]),
            ("bj_rtdb_restore_hold", "released", &[]),
            (
                "bj_fact_root_owner",
                "root",
                &["layout_digest", "audience_tablet"],
            ),
            (
                "bj_fact_root_program",
                "active",
                &[
                    "generation",
                    "artifact",
                    "closure_digest",
                    "closure",
                    "application_schema",
                    "source_reference",
                    "environment_contract",
                ],
            ),
            (ADMISSION, "application", &["admission"]),
            (ADMISSION, "authorization", &[]),
            (ADMISSION, "provider_issuers", &["admission"]),
            (ADMISSION, "browser_policy", &["admission"]),
            (ADMISSION, "registered_receipt_coverage", &["admission"]),
            (ADMISSION, standing.as_str(), &["admission"]),
            (ADMISSION, decision.as_str(), &["admission"]),
        ] {
            let point = control(
                view,
                relations,
                relation,
                id,
                fields,
                terminal.system_time,
                memory,
            )?;
            budget.retain(&point)?;
            readback.controls.push(point);
        }

        Ok(readback)
    }
}

fn complete_relation<'a>(
    view: &BranchView<'_>,
    relations: &'a [RelationInventory],
    table: &TableRef,
) -> Result<&'a RelationInventory> {
    relations
        .iter()
        .find(|relation| {
            &relation.table == table
                && relation.kind == RelationKind::Literal
                && relation.redacted == 0
                && relation.erasures == 0
                && view.table(table).is_some()
        })
        .ok_or_else(invalid)
}

fn control(
    view: &BranchView<'_>,
    relations: &[RelationInventory],
    relation: &str,
    id: &str,
    fields: &[&str],
    at: Timestamp,
    memory: &Arc<MemoryBudget>,
) -> Result<PolicyPoint> {
    let table = TableRef::new("public", relation);
    let id = EntityId::String(id.into());
    let history = complete_relation(view, relations, &table)?;
    let _positions = memory.reserve(
        usize::try_from(history.events)
            .ok()
            .and_then(|events| events.checked_mul(std::mem::size_of::<&BranchEvent>()))
            .ok_or_else(oversized)?,
    )?;
    if view
        .history(&table, &id)
        .iter()
        .any(|row| row.valid.lower() != Timestamp(0) || row.valid.upper() != Timestamp::MAX)
    {
        return Err(invalid());
    }
    let row = view
        .row(&table, &id, at)
        .map(|row| project(row, clear_cells(row, at)?, fields))
        .transpose()?;
    Ok(PolicyPoint {
        table,
        id,
        history_sha256: history.sha256,
        row,
    })
}

fn clear_cells(row: &BranchEvent, at: Timestamp) -> Result<&StructFields<Value>> {
    let image = row.image_at(at).ok_or_else(invalid)?;
    if image.custody_group.is_some() || image.custody_payload.is_some() {
        return Err(invalid());
    }
    Ok(&image.cells)
}

fn project(row: &BranchEvent, cells: &StructFields<Value>, fields: &[&str]) -> Result<PolicyRow> {
    Ok(PolicyRow {
        id: row.id.clone(),
        written: super::application::transaction(row)?,
        document: fields
            .iter()
            .filter_map(|name| {
                cells
                    .get(*name)
                    .map(|value| ((*name).to_owned(), value.clone()))
            })
            .collect(),
    })
}

struct ReplyBudget<'a> {
    bytes: usize,
    memory: &'a Arc<MemoryBudget>,
    charges: &'a mut Vec<Arc<MemoryReservation>>,
}

impl ReplyBudget<'_> {
    fn retain(&mut self, value: &impl Serialize) -> Result<()> {
        let len = encode(value)?.len();
        self.bytes = self
            .bytes
            .checked_add(len)
            .filter(|total| *total <= MAX_REPLY_BYTES)
            .ok_or_else(oversized)?;
        self.charges.push(self.memory.reserve(len * 2 + 512)?);
        Ok(())
    }
}

fn lower_hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut out, "{byte:02x}").expect("writing to a String");
    }
    out
}

fn invalid() -> rtdb_types::Anomaly {
    inconsistent("The certified Classic replay does not cover this registered receipt Policy read")
}

fn oversized() -> rtdb_types::Anomaly {
    unavailable("The certified Classic receipt Policy read exceeds its reply bound")
}
