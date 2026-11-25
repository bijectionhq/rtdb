use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::MemoryBudget;
use rtdb_tx::transact::{
    self as tx, Cell, CountRange, Declaration, GroupedCount, Guard, IndexDefinition, Op, Row, Transact, Version,
    CustodyPlacement, CustodyReservation, CustodyReview, CustodyTarget, DerivedCustodyReservation,
    ReviewedCustodyPlacement, ValidTime, ValidTimeBound, When, Window,
    RankedIndexDefinition, RankedIndexKind,
};
use rtdb_types::{
    EntityId, TableRef, Timestamp, Value,
    rtdb_v1::{Basis, Branch, Claim, valid_branch_name},
};
use tonic::Status;

use super::{error, proto};

pub(super) fn request(
    request: proto::TransactRequest,
    memory: &Arc<MemoryBudget>,
) -> Result<Transact, Status> {
    super::check_key(&request.key)?;
    if request.decision_clock_offer.is_some() {
        return Err(error::incorrect("A DecisionClock booking must be consumed through Transact"));
    }
    let mut require = request.require;
    require.sort_unstable();
    require.dedup();
    Ok(Transact {
        custody_request_digest: None,
        request_memory: None,
        key: request.key,
        require,
        window: request
            .window
            .map_or_else(Window::default, |window| Window {
                not_before: window.not_before_micros.map(Timestamp),
                before: window.before_micros.map(Timestamp),
            }),
        requires: request
            .requires
            .into_iter()
            .map(|guard| decode_guard(guard, memory))
            .collect::<Result<_, _>>()?,
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
            WireOp::Declare(declare) => {
                let table = table(&declare.table)?;
                let change = declaration(&table, declare.change, memory)?;
                Op::Declare { table, change }
            }
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
            WireOp::CreateCustodyGroup(create) => Op::CreateCustodyGroup {
                group: custody_identity(&create.group)?,
            },
            WireOp::RevokeCustodyGroup(revoke) => Op::RevokeCustodyGroup {
                group: custody_identity(&revoke.group)?,
                inventory_sha256: commitment(&revoke.inventory_sha256)?,
            },
            WireOp::ReserveCustodyPlacement(reserve) => {
                let (group, reservation) = decode_custody_reservation(reserve, memory)?;
                Op::ReserveCustodyPlacement { group, reservation }
            }
            WireOp::PlaceCustodyRow(place) => {
                let (branch, table, row, placement) = decode_custody_placement(place, memory)?;
                Op::PlaceCustodyRow { branch, table, row, placement }
            }
            WireOp::ReserveReviewedCustodyPlacement(reserve) => {
                let (group, reservation) = decode_custody_reservation(
                    reserve.placement.ok_or_else(|| error::incorrect("Missing custody reservation"))?, memory,
                )?;
                let source = reserve.source.ok_or_else(|| error::incorrect("Missing reviewed source cut"))?;
                let source = CustodyReview {
                    branch: branch(source.branch)?,
                    basis: basis(source.basis.ok_or_else(|| error::incorrect("Missing reviewed Basis"))?)?,
                    valid_at: Timestamp(source.valid_at_micros),
                    inventory_sha256: commitment(&source.inventory_sha256)?,
                };
                Op::ReserveReviewedCustodyPlacement { group, reservation, source, read_proof: reserve.read_proof }
            }
            WireOp::LandCustodyBranch(land) => {
                if !(1..=512).contains(&land.placements.len()) {
                    return Err(error::incorrect("A branch landing needs a bounded set of placements"));
                }
                let placements = land.placements.into_iter().map(|reviewed| {
                    super::check_key(&reviewed.reserve_key)?;
                    let (branch, table, row, placement) = decode_custody_placement(
                        reviewed.placement.ok_or_else(|| error::incorrect("Missing reviewed placement"))?, memory,
                    )?;
                    if branch != rtdb_types::rtdb_v1::MAIN_BRANCH {
                        return Err(error::incorrect("A branch landing places its rows on main"));
                    }
                    let reserved_at = basis(reviewed.reserved_at.ok_or_else(|| error::incorrect("Missing reservation Basis"))?)?;
                    Ok(ReviewedCustodyPlacement { table, row, placement, reserved_at, reserve_key: reviewed.reserve_key })
                }).collect::<Result<_, Status>>()?;
                Op::LandCustodyBranch { branch: branch(land.branch)?, placements }
            }
        },
    )
}

fn decode_custody_reservation(
    reserve: proto::ReserveCustodyPlacement, memory: &Arc<MemoryBudget>,
) -> Result<(tx::CustodyGroupId, CustodyReservation), Status> {
    let target = reserve.target.ok_or_else(|| error::incorrect("Missing custody placement target"))?;
    Ok((custody_identity(&reserve.group)?, CustodyReservation {
        original: commitment(&reserve.original_occurrence_sha256)?,
        source: commitment(&reserve.source_occurrence_sha256)?,
        destination: custody_identity(&reserve.destination)?,
        link: custody_identity(&reserve.link)?,
        target: custody_target(target, memory)?,
        review_sha256: commitment(&reserve.review_sha256)?,
        carrier_sha256: commitment(&reserve.carrier_sha256)?,
        link_sha256: commitment(&reserve.link_sha256)?,
    }))
}

pub(super) fn custody_target(
    target: proto::CustodyPlacementTarget, memory: &Arc<MemoryBudget>,
) -> Result<CustodyTarget<ValidTime>, Status> {
    let valid = target.valid.ok_or_else(|| error::incorrect("Missing custody placement interval"))?;
    Ok(CustodyTarget {
        branch: branch(target.branch)?, table: table(&target.table)?,
        id: entity(target.id, memory)?, valid: interval(valid)?,
    })
}

fn decode_custody_placement(
    place: proto::PlaceCustodyRow, memory: &Arc<MemoryBudget>,
) -> Result<(Branch, TableRef, Row, CustodyPlacement), Status> {
    if place.reservation_sha256.is_some() && place.derived_reservation.is_some() {
        return Err(error::incorrect("Custody placement takes one reservation"));
    }
    let placement = CustodyPlacement {
        placement_id: custody_identity(&place.placement_id)?,
        reservation: place.reservation_sha256.as_deref().map(commitment).transpose()?,
        derived: place.derived_reservation.map(|derived| -> Result<_, Status> {
            Ok(DerivedCustodyReservation {
                original_placement_id: custody_identity(&derived.original_placement_id)?,
                link: custody_identity(&derived.link)?,
                review_sha256: commitment(&derived.review_sha256)?,
                carrier_sha256: commitment(&derived.carrier_sha256)?,
                link_sha256: commitment(&derived.link_sha256)?,
            })
        }).transpose()?,
        carrier_column: place.carrier_column,
        link_column: place.link_column,
        commitments: None,
    };
    let row = decode_row(place.row.ok_or_else(|| error::incorrect("Missing custody row"))?, memory)?;
    Ok((branch(place.branch)?, table(&place.table)?, row, placement))
}

fn declaration(
    table: &TableRef,
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
                use proto::{IndexKind, index::Options};
                let kind = IndexKind::try_from(index.kind).map_err(|_| error::unsupported("declare.index.kind"))?;
                let ranked = match (kind, index.options) {
                    (IndexKind::Ordered, None) => {
                        let mut columns = index.columns.into_iter();
                        let column = columns.next().ok_or_else(|| error::incorrect("Ordered index needs a column"))?;
                        return Ok(Declaration::Index(IndexDefinition { name: index.name, column, additional_columns: columns.collect() }));
                    }
                    (IndexKind::Search, Some(Options::Search(options))) => RankedIndexKind::Search {
                        analyzer: options.analyzer, history: options.history,
                    },
                    (IndexKind::Spatial, Some(Options::Spatial(options))) => RankedIndexKind::Spatial {
                        geometry_encoding: options.geometry_encoding,
                    },
                    (IndexKind::Vector, Some(Options::Vector(options))) if options.method == "ivf" => RankedIndexKind::Ivf {
                        dimensions: options.dimensions, recall_bits: options.recall.to_bits(),
                    },
                    (IndexKind::Unspecified, _) => return Err(error::unsupported("declare.index.kind")),
                    _ => return Err(error::incorrect("Index kind and options must agree; the vector method is ivf")),
                };
                Declaration::RankedIndex { name: index.name, definition: RankedIndexDefinition { columns: index.columns, kind: ranked } }
            }
            Change::Ttl(ttl) => Declaration::Ttl(ttl.duration_micros),
            Change::Constraint(constraint) => {
                use proto::constraint::Change as ConstraintChange;
                if !constraint.expression.is_empty() {
                    return Err(error::unsupported("declare.constraint.expression"));
                }
                let range = match constraint.change {
                    Some(ConstraintChange::CountRange(count)) => Some(CountRange {
                        table: table.clone(),
                        index: count.index,
                        lower: count.lower.map(|bound| bound_bytes(bound, memory)).transpose()?,
                        upper: count.upper.map(|bound| bound_bytes(bound, memory)).transpose()?,
                        valid_at: count.valid_at_micros.map(Timestamp),
                        n: count.n,
                    }),
                    Some(ConstraintChange::Drop(())) => None,
                    Some(ConstraintChange::GroupedCount(count)) => return Ok(Declaration::GroupedConstraint {
                        name: constraint.name,
                        rule: GroupedCount {
                            column: count.column,
                            prefix_bytes: count.prefix_bytes,
                            equals: count.equals.into_iter().collect(),
                            valid_at: count.valid_at_micros.map(Timestamp),
                            n: count.n,
                        },
                    }),
                    None => return Err(error::incorrect("Constraint needs a count range or an explicit drop")),
                };
                Declaration::Constraint { name: constraint.name, range }
            }
            Change::Drop(_) => Declaration::Drop,
            Change::Visibility(value) => Declaration::Visibility(rtdb_tx::transact::Visibility { column: value.column }),
            Change::SingleLabelPerTx(_) => Declaration::SingleLabelPerTx,
        },
    )
}

fn decode_row(row: proto::Row, memory: &Arc<MemoryBudget>) -> Result<Row, Status> {
    let custody_group = (!row.custody_group.is_empty()).then(|| custody_identity(&row.custody_group)).transpose()?;
    let mut fields = row.cells.into_iter().collect::<BTreeMap<_, _>>();
    let id = entity(fields.remove("_id"), memory)?;
    let from = fields.remove("_valid_from");
    let to = fields.remove("_valid_to");
    let valid = if from.is_some() || to.is_some() {
        Some(ValidTime::new(
            put_valid_bound(from, Timestamp::MIN, memory)?,
            put_valid_bound(to, Timestamp::MAX, memory)?,
        ).map_err(error::anomaly)?)
    } else { None };
    let cells = fields
        .into_iter()
        .map(|(name, field)| Ok((name, cell(field, memory)?)))
        .collect::<Result<_, Status>>()?;
    let mut protected_columns = row.protected_columns;
    protected_columns.sort_unstable();
    if protected_columns.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(error::incorrect("Protected columns must be distinct"));
    }
    if !protected_columns.is_empty() && custody_group.is_none() {
        return Err(error::incorrect("Protected columns require a custody group"));
    }
    Ok(Row {
        id,
        valid,
        cells,
        custody_group,
        protected_columns,
        custody_payload: None,
    })
}

fn custody_identity(bytes: &[u8]) -> Result<[u8; 16], Status> {
    let identity = bytes.try_into().map_err(|_| error::incorrect("A custody identity must contain 16 bytes"))?;
    if identity == [0; 16] {
        return Err(error::incorrect("A custody identity must be nonzero"));
    }
    Ok(identity)
}

fn commitment(bytes: &[u8]) -> Result<[u8; 32], Status> {
    bytes.try_into().map_err(|_| error::incorrect("A custody commitment must contain 32 bytes"))
}

fn put_valid_bound(field: Option<proto::Cell>, default: Timestamp, memory: &Arc<MemoryBudget>) -> Result<ValidTimeBound, Status> {
    use proto::cell::Value as WireValue;
    match field {
        None => Ok(ValidTimeBound::Instant(default)),
        Some(proto::Cell { value: Some(WireValue::TxTime(_)) }) => Ok(ValidTimeBound::TransactionMicros(0)),
        Some(proto::Cell { value: Some(WireValue::TxTimeOffsetMicros(offset)) }) => Ok(ValidTimeBound::TransactionMicros(offset)),
        Some(field) => valid_bound(literal(field, memory)?).map(ValidTimeBound::Instant),
    }
}

/// A bound is signed microseconds, or a zoned Arrow timestamp; a naive timestamp names no instant.
fn valid_bound(value: Value) -> Result<Timestamp, Status> {
    match value {
        Value::I64(micros) => Ok(Timestamp(micros)),
        Value::Timestamp { value, unit, timezone: Some(_) } => {
            let per_second = unit.units_per_second();
            let micros = if per_second >= 1_000_000 {
                Some(value.div_euclid(per_second / 1_000_000))
            } else {
                value.checked_mul(1_000_000 / per_second)
            };
            micros.map(Timestamp).ok_or_else(|| error::incorrect("The valid-time bound is out of range"))
        }
        _ => Err(error::incorrect("Valid-time bounds require signed microseconds or a zoned timestamp")),
    }
}

fn interval(valid: proto::Interval) -> Result<ValidTime, Status> {
    let bound = |instant, offset, default| match (instant, offset) {
        (Some(_), Some(_)) => Err(error::incorrect("A bound cannot be both literal and transaction-relative")),
        (None, Some(offset)) => Ok(ValidTimeBound::TransactionMicros(offset)),
        (instant, None) => Ok(ValidTimeBound::Instant(instant.map_or(default, Timestamp))),
    };
    ValidTime::new(
        bound(valid.from_micros, valid.from_tx_offset_micros, Timestamp::MIN)?,
        bound(valid.to_micros, valid.to_tx_offset_micros, Timestamp::MAX)?,
    ).map_err(error::anomaly)
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
            WireValue::TxTimeOffsetMicros(_) => return Err(error::incorrect("A transaction offset can only be used as a valid-time bound")),
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
        decision_clock: None,
        erasure: match receipt.erasure {
            Some(tx::Erasure::Armed { unreadable_by }) => Some(proto::Erasure { unreadable_by_micros: unreadable_by.0 }),
            _ => None,
        },
    }
}

pub(super) fn decision_clock(offer: tx::FundedReceipt, scoped: bool) -> proto::DecisionClockOffer {
    proto::DecisionClockOffer {
        booking: offer.booking().to_vec(),
        predecessor: Some(offer.predecessor().into()),
        reading_micros: (!scoped).then(|| offer.reading().0),
        before_micros: (!scoped).then(|| offer.before().0),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray};
    use rtdb_types::Interval;

    use super::*;

    fn wire(column: ArrayRef) -> proto::Cell {
        let batch = RecordBatch::try_from_iter([("value", column)]).unwrap();
        proto::Cell {
            value: Some(proto::cell::Value::ArrowIpc(
                rtdb_format::ipc::write_stream(batch.schema_ref(), &[batch.clone()]).unwrap(),
            )),
        }
    }

    fn row(bounds: impl IntoIterator<Item = (&'static str, ArrayRef)>) -> Result<Row, Status> {
        let mut cells: std::collections::HashMap<_, _> = bounds.into_iter()
            .map(|(name, column)| (name.to_owned(), wire(column))).collect();
        cells.insert("_id".into(), wire(Arc::new(Int64Array::from(vec![1]))));
        cells.insert("title".into(), wire(Arc::new(Int64Array::from(vec![7]))));
        decode_row(proto::Row { cells: cells.into_iter().collect(), custody_group: Vec::new(), protected_columns: Vec::new() }, &MemoryBudget::new(1 << 20))
    }

    #[test]
    fn put_rows_carry_their_explicit_valid_interval() {
        let unbounded = row([]).unwrap();
        assert_eq!(unbounded.valid, None);
        assert_eq!(unbounded.cells.len(), 1);

        let micros = row([
            ("_valid_from", Arc::new(Int64Array::from(vec![10])) as ArrayRef),
            ("_valid_to", Arc::new(Int64Array::from(vec![20]))),
        ]).unwrap();
        assert_eq!(micros.valid, Some(Interval::new(Timestamp(10), Timestamp(20)).unwrap().into()));
        assert!(!micros.cells.contains_key("_valid_from") && !micros.cells.contains_key("_valid_to"));

        let open_ended = row([("_valid_from", Arc::new(TimestampMicrosecondArray::from(vec![30]).with_timezone("UTC")) as ArrayRef)]).unwrap();
        assert_eq!(open_ended.valid, Some(Interval::new(Timestamp(30), Timestamp::MAX).unwrap().into()));

        let units = row([
            ("_valid_from", Arc::new(TimestampMillisecondArray::from(vec![2]).with_timezone("UTC")) as ArrayRef),
            ("_valid_to", Arc::new(TimestampNanosecondArray::from(vec![5_000_999]).with_timezone("+01:00"))),
        ]).unwrap();
        assert_eq!(units.valid, Some(Interval::new(Timestamp(2_000), Timestamp(5_000)).unwrap().into()));
    }

    #[test]
    fn put_and_delete_decode_transaction_relative_valid_time() {
        let memory = MemoryBudget::new(1 << 20);
        let row = decode_row(proto::Row {
            cells: [
                ("_id".into(), wire(Arc::new(Int64Array::from(vec![1])))),
                ("_valid_from".into(), proto::Cell { value: Some(proto::cell::Value::TxTime(())) }),
                ("_valid_to".into(), proto::Cell { value: Some(proto::cell::Value::TxTimeOffsetMicros(60)) }),
            ].into_iter().collect(),
            custody_group: Vec::new(),
            protected_columns: Vec::new(),
        }, &memory).unwrap();
        assert_eq!(row.valid.unwrap().resolve(Timestamp(20)).unwrap(), Interval::new(Timestamp(20), Timestamp(80)).unwrap());
        let delete = interval(proto::Interval { from_tx_offset_micros: Some(-10), to_micros: Some(50), ..Default::default() }).unwrap();
        assert_eq!(delete.resolve(Timestamp(20)).unwrap(), Interval::new(Timestamp(10), Timestamp(50)).unwrap());
        assert!(delete.resolve(Timestamp(i64::MIN)).is_err());
        assert!(interval(proto::Interval { from_tx_offset_micros: Some(0), from_micros: Some(50), ..Default::default() }).is_err());
    }

    #[test]
    fn put_rows_refuse_naive_or_inverted_valid_intervals() {
        let naive = row([("_valid_from", Arc::new(TimestampMicrosecondArray::from(vec![30])) as ArrayRef)]);
        assert_eq!(naive.unwrap_err().code(), tonic::Code::InvalidArgument);
        let inverted = row([
            ("_valid_from", Arc::new(Int64Array::from(vec![20])) as ArrayRef),
            ("_valid_to", Arc::new(Int64Array::from(vec![10]))),
        ]);
        assert!(inverted.is_err());
    }
}
