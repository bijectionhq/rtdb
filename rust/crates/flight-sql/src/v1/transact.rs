use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::MemoryBudget;
use rtdb_tx::transact::{
    self as tx, Cell, CountRange, Declaration, Guard, IndexDefinition, Op, Row, Transact, Version,
    When, Window,
};
use rtdb_types::{
    EntityId, Interval, TableRef, Timestamp, Value,
    rtdb_v1::{Basis, Branch, Claim, valid_branch_name},
};
use tonic::Status;

use super::{error, proto};

pub(super) fn request(
    request: proto::TransactRequest,
    memory: &Arc<MemoryBudget>,
) -> Result<Transact, Status> {
    super::check_key(&request.key)?;
    let mut require = request.require;
    require.sort_unstable();
    require.dedup();
    Ok(Transact {
        key: request.key,
        require,
        window: request
            .window
            .map_or_else(Window::default, |window| Window {
                not_before: window.not_before_micros.map(Timestamp),
                before: window.before_micros.map(Timestamp),
            }),
        guards: request
            .guards
            .into_iter()
            .map(|guard| decode_guard(guard, memory))
            .collect::<Result<_, _>>()?,
        ops: request
            .ops
            .into_iter()
            .map(|op| decode_op(op, memory))
            .collect::<Result<_, _>>()?,
        otherwise: request
            .otherwise
            .into_iter()
            .map(|op| decode_op(op, memory))
            .collect::<Result<_, _>>()?,
    })
}

fn decode_guard(guard: proto::Guard, memory: &Arc<MemoryBudget>) -> Result<Guard, Status> {
    use proto::guard::Guard as WireGuard;
    use proto::version::Expect;
    Ok(
        match guard
            .guard
            .ok_or_else(|| error::incorrect("Missing guard"))?
        {
            WireGuard::Unchanged(claim) => Guard::Unchanged(Claim::from_bytes(claim.value)),
            WireGuard::Version(version) => Guard::Version {
                branch: branch(version.branch)?,
                table: table(&version.table)?,
                id: entity(version.id, memory)?,
                valid_at: version.valid_at_micros.map(Timestamp),
                expect: match version
                    .expect
                    .ok_or_else(|| error::incorrect("Version needs an expectation"))?
                {
                    Expect::Absent(_) => Version::Absent,
                    Expect::Present(_) => Version::Present,
                    Expect::Written(cut) => Version::Written(basis(cut)?),
                },
            },
            WireGuard::CountAtMost(count) => Guard::CountAtMost {
                branch: branch(count.branch)?,
                range: CountRange {
                    table: table(&count.table)?,
                    index: count.index,
                    lower: count
                        .lower
                        .map(|bound| bound_bytes(bound, memory))
                        .transpose()?,
                    upper: count
                        .upper
                        .map(|bound| bound_bytes(bound, memory))
                        .transpose()?,
                    valid_at: count.valid_at_micros.map(Timestamp),
                    n: count.n,
                },
                when: match proto::When::try_from(count.when)
                    .map_err(|_| error::incorrect("Invalid count phase"))?
                {
                    proto::When::Before => When::Before,
                    proto::When::After => When::After,
                },
            },
        },
    )
}

fn decode_op(op: proto::Op, memory: &Arc<MemoryBudget>) -> Result<Op, Status> {
    use proto::op::Op as WireOp;
    Ok(
        match op.op.ok_or_else(|| error::incorrect("Missing operation"))? {
            WireOp::Put(put) => Op::Put {
                branch: branch(put.branch)?,
                table: table(&put.table)?,
                rows: put
                    .rows
                    .into_iter()
                    .map(|row| decode_row(row, memory))
                    .collect::<Result<_, _>>()?,
                if_absent: put.if_absent,
            },
            WireOp::Delete(delete) => Op::Delete {
                branch: branch(delete.branch)?,
                table: table(&delete.table)?,
                ids: delete
                    .ids
                    .into_iter()
                    .map(|id| entity(Some(id), memory))
                    .collect::<Result<_, _>>()?,
                valid: delete.valid.map(interval).transpose()?,
            },
            WireOp::Erase(erase) => Op::Erase {
                branch: erase.branch.map(branch).transpose()?,
                table: table(&erase.table)?,
                ids: erase
                    .ids
                    .into_iter()
                    .map(|id| entity(Some(id), memory))
                    .collect::<Result<_, _>>()?,
            },
            WireOp::Declare(declare) => Op::Declare {
                table: table(&declare.table)?,
                change: declaration(declare.change, memory)?,
            },
            WireOp::Fork(fork) => {
                let from = fork
                    .from
                    .ok_or_else(|| error::incorrect("Fork needs a source"))?;
                Op::Fork {
                    child: branch(fork.child)?,
                    from: (
                        branch(from.branch)?,
                        basis(
                            from.basis
                                .ok_or_else(|| error::incorrect("Fork needs a basis"))?,
                        )?,
                    ),
                    writable: fork.writable,
                    expires_at: fork.expires_at_micros.map(Timestamp),
                }
            }
            WireOp::Renew(renew) => Op::Renew {
                branch: branch(renew.branch)?,
                expires_at: Timestamp(renew.expires_at_micros),
            },
            WireOp::Merge(merge) => Op::Merge {
                from: branch(merge.from)?,
                into: branch(merge.into)?,
            },
            WireOp::DropBranch(drop) => Op::DropBranch {
                branch: branch(drop.branch)?,
            },
        },
    )
}

fn declaration(
    change: Option<proto::declare::Change>,
    memory: &Arc<MemoryBudget>,
) -> Result<Declaration, Status> {
    use proto::declare::Change;
    Ok(
        match change.ok_or_else(|| error::incorrect("Missing declaration"))? {
            Change::Schema(schema) => {
                let size = schema
                    .arrow_schema_ipc
                    .len()
                    .checked_mul(8)
                    .ok_or_else(|| error::incorrect("Arrow schema exceeds addressable memory"))?;
                let _charge = memory.reserve(size).map_err(error::anomaly)?;
                let schema = rtdb_format::ipc::read_schema_message(&schema.arrow_schema_ipc)
                    .map_err(error::anomaly)?;
                let columns = schema
                    .fields()
                    .iter()
                    .map(|field| {
                        Ok((
                            field.name().clone(),
                            rtdb_columnar::from_field(field).map_err(error::anomaly)?,
                        ))
                    })
                    .collect::<Result<rtdb_types::StructFields<_>, Status>>()?;
                if columns.len() != schema.fields().len() {
                    return Err(error::incorrect("Duplicate column in schema"));
                }
                Declaration::Schema(columns)
            }
            Change::Index(index) => {
                if index.kind != proto::IndexKind::Ordered as i32 {
                    return Err(error::unsupported("declare.index.kind"));
                }
                let [column]: [String; 1] = index
                    .columns
                    .try_into()
                    .map_err(|_| error::incorrect("Ordered index needs one column"))?;
                Declaration::Index(IndexDefinition {
                    name: index.name,
                    column,
                })
            }
            Change::Ttl(ttl) => Declaration::Ttl(ttl.duration_micros),
            Change::Constraint(_) => return Err(error::unsupported("declare.constraint")),
            Change::Drop(_) => Declaration::Drop,
            Change::Visibility(value) => Declaration::Visibility(rtdb_tx::transact::Visibility { column: value.column }),
            Change::SingleLabelPerTx(_) => Declaration::SingleLabelPerTx,
        },
    )
}

fn decode_row(row: proto::Row, memory: &Arc<MemoryBudget>) -> Result<Row, Status> {
    let mut fields = row.cells.into_iter().collect::<BTreeMap<_, _>>();
    let id = entity(fields.remove("_id"), memory)?;
    let from = fields.remove("_valid_from");
    let to = fields.remove("_valid_to");
    let valid = if from.is_some() || to.is_some() {
        let bound = |field: Option<proto::Cell>, default| match field.map(|field| cell(field, memory)).transpose()? {
            None => Ok(default),
            Some(Cell::Value(Value::I64(micros))) => Ok(Timestamp(micros)),
            _ => Err(error::incorrect("Valid-time bounds require literal signed microseconds")),
        };
        Some(Interval::new(bound(from, Timestamp::MIN)?, bound(to, Timestamp::MAX)?).map_err(error::anomaly)?)
    } else { None };
    let cells = fields
        .into_iter()
        .map(|(name, field)| Ok((name, cell(field, memory)?)))
        .collect::<Result<_, Status>>()?;
    Ok(Row {
        id,
        valid,
        cells,
    })
}

fn interval(valid: proto::Interval) -> Result<Interval, Status> {
    Interval::new(
        valid.from_micros.map_or(Timestamp::MIN, Timestamp),
        valid.to_micros.map_or(Timestamp::MAX, Timestamp),
    )
    .map_err(error::anomaly)
}

fn cell(cell: proto::Cell, memory: &Arc<MemoryBudget>) -> Result<Cell, Status> {
    use proto::cell::Value as WireValue;
    Ok(
        match cell
            .value
            .ok_or_else(|| error::incorrect("Missing cell value"))?
        {
            WireValue::TxTime(_) => Cell::TxTime,
            WireValue::TxBasis(_) => Cell::TxBasis,
            WireValue::ArrowIpc(bytes) => {
                let stream =
                    rtdb_format::ipc::OwnedStream::read(&bytes, memory).map_err(error::anomaly)?;
                let [batch] = stream.pages() else {
                    return Err(error::incorrect("A scalar cell needs one Arrow batch"));
                };
                let batch = batch.batch();
                if batch.num_rows() != 1 || batch.num_columns() != 1 {
                    return Err(error::incorrect(
                        "A scalar cell needs one Arrow row and column",
                    ));
                }
                Cell::Value(
                    rtdb_columnar::read_value(
                        batch.schema_ref().field(0),
                        batch.column(0).as_ref(),
                        0,
                    )
                    .map_err(error::anomaly)?,
                )
            }
        },
    )
}

fn literal(cell_value: proto::Cell, memory: &Arc<MemoryBudget>) -> Result<Value, Status> {
    match cell(cell_value, memory)? {
        Cell::Value(value) => Ok(value),
        Cell::TxTime | Cell::TxBasis => Err(error::incorrect("Expected a literal cell")),
    }
}

fn entity(id: Option<proto::Cell>, memory: &Arc<MemoryBudget>) -> Result<EntityId, Status> {
    literal(
        id.ok_or_else(|| error::incorrect("Missing entity ID"))?,
        memory,
    )?
    .entity_id()
    .map_err(error::anomaly)
}

fn bound_bytes(bound: proto::Cell, memory: &Arc<MemoryBudget>) -> Result<Vec<u8>, Status> {
    match literal(bound, memory)? {
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => Ok(bytes),
        Value::Utf8(value) => Ok(value.into_bytes()),
        _ => Err(error::incorrect("A count bound must be binary or text")),
    }
}

fn branch(branch: Branch) -> Result<Branch, Status> {
    if !valid_branch_name(&branch) {
        return Err(error::incorrect("Invalid branch name"));
    }
    Ok(branch)
}

fn table(name: &str) -> Result<TableRef, Status> {
    TableRef::parse(name).map_err(error::anomaly)
}

fn basis(basis: proto::Basis) -> Result<Basis, Status> {
    Basis::from_bytes(&basis.value).map_err(error::anomaly)
}

pub(super) fn receipt(receipt: tx::Receipt) -> proto::Receipt {
    use proto::receipt::Outcome;
    proto::Receipt {
        key: receipt.key,
        basis: Some(receipt.basis.into()),
        system_time_micros: receipt.system_time.0,
        outcome: Some(match receipt.outcome {
            tx::Outcome::Then => Outcome::Then(()),
            tx::Outcome::Otherwise { failed } => Outcome::Otherwise(proto::Otherwise {
                failed: failed
                    .into_iter()
                    .map(|index| u32::try_from(index).expect("guard index fits in the wire frame"))
                    .collect(),
            }),
            tx::Outcome::Refused { error: anomaly } => Outcome::Refused(error::detail(anomaly)),
        }),
        replayed: receipt.replayed,
        erasure: None,
    }
}
