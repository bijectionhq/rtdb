use std::{mem::size_of, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, UnionArray,
    builder::{ArrayBuilder, BinaryBuilder, FixedSizeBinaryBuilder, Int64Builder, StringBuilder, UInt64Builder},
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, FieldRef, Fields, UnionMode};
use rtdb_columnar::{
    EntityIdRef, MemoryBudget, MemoryReservation, arrow_error, build_list_array, build_struct_array,
};
use rtdb_node::OwnedChangeCommit;
use rtdb_types::{
    Anomaly, EntityId, Interval, LogicalType, OwnedChange, OwnedChangeGroup, OwnedChangeKind, ScalarType,
    StructFields, Timestamp, Value,
};
use tonic::Status;

use crate::{error::status, wire};

// Match the buffer capacity used by Arrow byte arrays for the IPC scratch charge.
const BYTE_ARRAY_DATA_CAPACITY: usize = 1024;

pub(super) fn touches_type() -> LogicalType {
    let logical_id = LogicalType::from_legs(
        [ScalarType::I64, ScalarType::Utf8, ScalarType::Keyword, ScalarType::Uuid, ScalarType::Binary]
            .map(LogicalType::Scalar),
    );
    LogicalType::list(LogicalType::Struct(StructFields::from([
        ("_id".into(), logical_id),
        ("op".into(), LogicalType::Scalar(ScalarType::Utf8)),
        ("valid_from_micros".into(), LogicalType::Scalar(ScalarType::I64)),
        ("valid_to_micros".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
        ("created_micros".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
        ("expiry_micros".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
        ("assertion_ordinal".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
    ])))
}

pub(super) fn merged_type() -> LogicalType {
    LogicalType::list(LogicalType::Struct(StructFields::from([
        ("relation".into(), LogicalType::Scalar(ScalarType::Utf8)),
        ("relation_sequence".into(), LogicalType::Scalar(ScalarType::U64)),
        ("touches".into(), touches_type()),
    ])))
}

pub(super) fn touches_column(
    memory: &Arc<MemoryBudget>, logical: &LogicalType, field: &Field, groups: Vec<OwnedChangeGroup>,
) -> Result<(ArrayRef, Arc<MemoryReservation>), Status> {
    let mut counts = TouchCounts::default();
    let mut bytes = schema_bytes(logical)?;
    for group in &groups {
        bytes = wire::add(bytes, counts.list_size(&group.touches)?.bytes()?)?;
    }
    let charge = memory.reserve(bytes).map_err(status)?;
    Ok((build_touches(field, groups.into_iter().map(|group| group.touches), counts)?, charge))
}

pub(super) fn merged_column(
    memory: &Arc<MemoryBudget>, logical: &LogicalType, field: &Field, mut commits: Vec<OwnedChangeCommit>,
) -> Result<(ArrayRef, Arc<MemoryReservation>), Status> {
    let mut counts = TouchCounts::default();
    let mut bytes = schema_bytes(logical)?;
    let mut relations = 0;
    for commit in &commits {
        let mut list = BuilderSize::cell();
        for change in &commit.changes {
            relations = wire::add(relations, 1)?;
            let mut group = BuilderSize::cell();
            group.field("relation", BuilderSize::text(change.relation.name())?)?;
            group.field("relation_sequence", BuilderSize::scalar(32, 0)?)?;
            group.field("touches", counts.list_size(&change.touches)?)?;
            list.child(group)?;
        }
        bytes = wire::add(bytes, list.bytes()?)?;
    }
    let charge = memory.reserve(bytes).map_err(status)?;

    let item = list_item(field)?;
    let fields = struct_fields(item)?;
    let groups = commits.iter_mut().flat_map(|commit| &mut commit.changes)
        .map(|change| std::mem::take(&mut change.touches));
    let touches = build_touches(&fields[2], groups, counts)?;
    let mut names = StringBuilder::with_capacity(relations, BYTE_ARRAY_DATA_CAPACITY);
    let mut sequences = UInt64Builder::with_capacity(relations);
    let mut offsets = vec![0i32];
    for commit in commits {
        for change in &commit.changes {
            names.append_value(change.relation.name());
            sequences.append_value(change.sequence);
        }
        offsets.push(list_offset(sequences.len())?);
    }
    let children: [ArrayRef; 3] = [Arc::new(names.finish()), Arc::new(sequences.finish()), touches];
    let groups = build_struct_array(fields.clone(), &children, relations, None).map_err(status)?;
    let changes = build_list_array(Arc::clone(item), offsets.into(), &groups, None).map_err(status)?;
    Ok((changes, charge))
}

fn build_touches(
    field: &Field, groups: impl IntoIterator<Item = Vec<OwnedChange>>, counts: TouchCounts,
) -> Result<ArrayRef, Status> {
    let item = list_item(field)?;
    let fields = struct_fields(item)?;
    let DataType::Union(id_fields, UnionMode::Dense) = fields[0].data_type() else {
        return Err(layout_error());
    };
    let len = counts.integer + counts.text + counts.keyword + counts.uuid + counts.binary;
    let mut integers = Int64Builder::with_capacity(counts.integer);
    let mut strings = StringBuilder::with_capacity(counts.text, BYTE_ARRAY_DATA_CAPACITY);
    let mut keywords = StringBuilder::with_capacity(counts.keyword, BYTE_ARRAY_DATA_CAPACITY);
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(counts.uuid, 16);
    let new_binary_builder = || BinaryBuilder::with_capacity(counts.binary, BYTE_ARRAY_DATA_CAPACITY);
    let mut binary_builder = None;
    let mut moved_binary = None;
    let mut id_tags = Vec::<i8>::with_capacity(len);
    let mut id_offsets = Vec::with_capacity(len);
    let mut ops = StringBuilder::with_capacity(len, BYTE_ARRAY_DATA_CAPACITY);
    let mut valid_from = Int64Builder::with_capacity(len);
    let mut valid_to = Int64Builder::with_capacity(len);
    let mut created = Int64Builder::with_capacity(len);
    let mut expiry = Int64Builder::with_capacity(len);
    let mut ordinals = Int64Builder::with_capacity(len);
    let mut offsets = vec![0i32];
    for touches in groups {
        for touch in touches {
            let upper = valid_upper(&touch);
            // Tags follow the Changes schema, whose integer leg comes first.
            let (tag, offset) = match touch.id {
                EntityId::Integer(value) => {
                    let offset = integers.len();
                    integers.append_value(value);
                    (0, offset)
                }
                EntityId::String(value) => {
                    let offset = strings.len();
                    strings.append_value(value);
                    (1, offset)
                }
                EntityId::Keyword(value) => {
                    let offset = keywords.len();
                    keywords.append_value(value.as_str());
                    (2, offset)
                }
                EntityId::Uuid(value) => {
                    let offset = uuids.len();
                    uuids.append_value(value).map_err(arrow_error).map_err(status)?;
                    (3, offset)
                }
                EntityId::Binary(value) => {
                    // IPC scratch is charged by allocation capacity.
                    if counts.binary == 1 && value.capacity() == BYTE_ARRAY_DATA_CAPACITY {
                        let end = value.len() as i32;
                        moved_binary = Some(BinaryArray::try_new(
                            OffsetBuffer::new(vec![0, end].into()), value.into(), None,
                        ).map_err(arrow_error).map_err(status)?);
                        (4, 0)
                    } else {
                        let builder = binary_builder.get_or_insert_with(&new_binary_builder);
                        let offset = builder.len();
                        builder.append_value(value);
                        (4, offset)
                    }
                }
            };
            id_tags.push(tag);
            id_offsets.push(i32::try_from(offset).map_err(|_| status(Anomaly::incorrect(
                "rtdb/union-offset-overflow", "Union child exceeds i32 offsets",
            ).with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded)))?);
            let (op, lifetime) = touch_operation(touch.kind);
            ops.append_value(op);
            valid_from.append_value(touch.valid.lower().0);
            valid_to.append_option(upper);
            created.append_option(lifetime.map(|period| period.lower().0));
            expiry.append_option(lifetime.map(|period| period.upper().0));
            ordinals.append_option(touch.assertion_ordinal.map(|ordinal| i64::from(ordinal.get())));
        }
        offsets.push(list_offset(id_tags.len())?);
    }
    let binaries = moved_binary.unwrap_or_else(|| binary_builder.unwrap_or_else(new_binary_builder).finish());
    let id_children: Vec<ArrayRef> = vec![
        Arc::new(integers.finish()), Arc::new(strings.finish()),
        Arc::new(keywords.finish()), Arc::new(uuids.finish()), Arc::new(binaries),
    ];
    let ids = UnionArray::try_new(id_fields.clone(), id_tags.into(), Some(id_offsets.into()), id_children)
        .map_err(arrow_error).map_err(status)?;
    let children: [ArrayRef; 7] = [
        Arc::new(ids), Arc::new(ops.finish()), Arc::new(valid_from.finish()), Arc::new(valid_to.finish()),
        Arc::new(created.finish()), Arc::new(expiry.finish()), Arc::new(ordinals.finish()),
    ];
    let items = build_struct_array(fields.clone(), &children, len, None).map_err(status)?;
    build_list_array(Arc::clone(item), offsets.into(), &items, None).map_err(status)
}

fn touch_operation(kind: OwnedChangeKind) -> (&'static str, Option<Interval>) {
    match kind {
        OwnedChangeKind::Put => ("put", None),
        OwnedChangeKind::Delete => ("delete", None),
        OwnedChangeKind::Redacted => ("redacted", None),
        OwnedChangeKind::Erase => ("erase", None),
        OwnedChangeKind::Expired(period) => ("expired", Some(period)),
    }
}

fn valid_upper(touch: &OwnedChange) -> Option<i64> {
    let upper = touch.valid.upper();
    (upper != Timestamp::MAX).then_some(upper.0)
}

fn list_item(field: &Field) -> Result<&FieldRef, Status> {
    match field.data_type() {
        DataType::List(item) => Ok(item),
        _ => Err(layout_error()),
    }
}

fn struct_fields(field: &Field) -> Result<&Fields, Status> {
    match field.data_type() {
        DataType::Struct(fields) => Ok(fields),
        _ => Err(layout_error()),
    }
}

fn list_offset(length: usize) -> Result<i32, Status> {
    i32::try_from(length).map_err(|_| status(Anomaly::incorrect(
        "rtdb/list-offset-overflow", "List children exceed i32 offsets",
    ).with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded)))
}

fn layout_error() -> Status {
    status(Anomaly::incorrect("rtdb/array-layout", "Arrow array and logical field layout disagree"))
}

fn schema_bytes(logical: &LogicalType) -> Result<usize, Status> {
    rtdb_columnar::estimate_builder_bytes(logical, &[]).map_err(status)
}

#[derive(Default)]
struct TouchCounts {
    integer: usize,
    text: usize,
    keyword: usize,
    uuid: usize,
    binary: usize,
}

impl TouchCounts {
    fn list_size(&mut self, touches: &[OwnedChange]) -> Result<BuilderSize, Status> {
        let mut list = BuilderSize::cell();
        for touch in touches {
            let id = EntityIdRef::from(&touch.id);
            let count = match id {
                EntityIdRef::Integer(_) => &mut self.integer,
                EntityIdRef::String(_) => &mut self.text,
                EntityIdRef::Keyword(_) => &mut self.keyword,
                EntityIdRef::Uuid(_) => &mut self.uuid,
                EntityIdRef::Binary(_) => &mut self.binary,
            };
            *count = wire::add(*count, 1)?;
            let (op, lifetime) = touch_operation(touch.kind);
            let payload = id.owned_payload_bytes();
            let width = match id {
                EntityIdRef::String(_) | EntityIdRef::Keyword(_) | EntityIdRef::Binary(_) => payload,
                EntityIdRef::Integer(_) | EntityIdRef::Uuid(_) => 32,
            };
            let mut entry = BuilderSize::cell();
            let mut identity = BuilderSize::scalar(width, payload)?;
            identity.cells = wire::add(identity.cells, BuilderSize::PRESENT)?;
            entry.field("_id", identity)?;
            entry.field("op", BuilderSize::text(op)?)?;
            entry.field("valid_from_micros", BuilderSize::scalar(32, 0)?)?;
            entry.field("valid_to_micros", BuilderSize::nullable_int(valid_upper(touch).is_some()))?;
            entry.sparse_int("created_micros", lifetime.is_some())?;
            entry.sparse_int("expiry_micros", lifetime.is_some())?;
            entry.sparse_int("assertion_ordinal", touch.assertion_ordinal.is_some())?;
            list.child(entry)?;
        }
        Ok(list)
    }
}

// Match the legacy Value builder reservation so memory pressure keeps the same refusal boundary.
struct BuilderSize {
    cells: usize,
    clones: usize,
    depth: usize,
}

impl BuilderSize {
    const VALUE: usize = size_of::<Value>();
    const PRESENT: usize = 2 * Self::VALUE + 128;
    const STRUCT_ENTRY: usize = 2 * (Self::VALUE + size_of::<String>()) + 64;

    fn cell() -> Self {
        Self { cells: Self::PRESENT, clones: Self::VALUE, depth: 1 }
    }

    fn scalar(width: usize, payload: usize) -> Result<Self, Status> {
        Ok(Self {
            cells: wire::add(Self::PRESENT, width)?,
            clones: wire::add(Self::VALUE, payload)?,
            depth: 1,
        })
    }

    fn text(text: &str) -> Result<Self, Status> {
        Self::scalar(text.len(), text.len())
    }

    fn child(&mut self, child: Self) -> Result<(), Status> {
        self.cells = wire::add(self.cells, child.cells)?;
        self.clones = wire::add(self.clones, child.clones)?;
        self.depth = self.depth.max(child.depth + 1);
        Ok(())
    }

    fn field(&mut self, name: &str, child: Self) -> Result<(), Status> {
        self.clones = wire::add(self.clones, Self::STRUCT_ENTRY + name.len())?;
        self.child(child)
    }

    fn nullable_int(present: bool) -> Self {
        Self { cells: if present { 2 * Self::PRESENT + 32 } else { 2 + 16 }, ..Self::cell() }
    }

    fn sparse_int(&mut self, name: &str, present: bool) -> Result<(), Status> {
        let child = Self::nullable_int(present);
        if present {
            self.field(name, child)
        } else {
            self.cells = wire::add(self.cells, child.cells)?;
            Ok(())
        }
    }

    fn bytes(&self) -> Result<usize, Status> {
        wire::mul(wire::add(self.cells, wire::mul(self.clones, self.depth)?)?, 4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    use arrow_array::{ListArray, RecordBatch, StructArray};
    use arrow_ipc::writer::IpcWriteOptions;
    use arrow_schema::Schema;
    use rtdb_node::OwnedRelationChanges;
    use rtdb_types::{Keyword, OwnedRelation, TransactionKey, TxId};

    use crate::actions::OwnedIpcReply;

    fn touches() -> Vec<OwnedChange> {
        let bounded = Interval::new(Timestamp(100), Timestamp(105)).unwrap();
        [
            (EntityId::Integer(-8), OwnedChangeKind::Put, bounded, 3),
            (EntityId::String("identity".repeat(150)), OwnedChangeKind::Delete, Interval::ALL, 4),
            (EntityId::Keyword(Keyword::from_name("app/key")), OwnedChangeKind::Redacted, bounded, 0),
            (EntityId::Uuid([0xa3; 16]), OwnedChangeKind::Erase, Interval::ALL, 5),
            (EntityId::Integer(18), OwnedChangeKind::Expired(bounded), Interval::ALL, 0),
            (EntityId::Binary(vec![0, 255, 0]), OwnedChangeKind::Put, bounded, 6),
        ].into_iter().map(|(id, kind, valid, ordinal)| OwnedChange {
            id, kind, valid, assertion_ordinal: NonZeroU32::new(ordinal),
        }).collect()
    }

    fn legacy_touches(touches: &[OwnedChange]) -> Value {
        Value::list(touches.iter().map(OwnedChange::to_value).collect())
    }

    fn list_items(list: &ArrayRef) -> &StructArray {
        let list = list.as_any().downcast_ref::<ListArray>().unwrap();
        list.values().as_any().downcast_ref().unwrap()
    }

    fn touch_ids(touches: &ArrayRef) -> &UnionArray {
        list_items(touches).column(0).as_any().downcast_ref().unwrap()
    }

    fn assert_legacy_column(
        logical: &LogicalType, field: Field, values: &[Value],
        column: ArrayRef, charge: Arc<MemoryReservation>,
    ) {
        let legacy = rtdb_columnar::build_array(logical, values).unwrap();
        assert_eq!(charge.bytes(), rtdb_columnar::estimate_builder_bytes(logical, values).unwrap());
        assert_eq!(column.get_array_memory_size(), legacy.get_array_memory_size());
        assert_eq!(column.to_data(), legacy.to_data());

        let schema = Arc::new(Schema::new(vec![field]).with_metadata([
            ("rtdb.owned.change_coverage_micros".into(), "27".into()),
            ("rtdb.owned.world_range_witness".into(), "b6f78ebe2f444140994bc3d20957cd9a".into()),
        ].into()));
        let legacy = RecordBatch::try_new(Arc::clone(&schema), vec![legacy]).unwrap();
        let actual = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        let unary = |pages: &[RecordBatch]| {
            rtdb_format::ipc::write_arrow_stream(&schema, pages, IpcWriteOptions::default()).unwrap()
        };
        let legacy = unary(&[legacy]);
        assert_eq!(unary(std::slice::from_ref(&actual)), legacy);
        let memory = MemoryBudget::new(1 << 20);
        for (pages, expected) in [(std::slice::from_ref(&actual), legacy), (&[][..], unary(&[]))] {
            let OwnedIpcReply::Frames(frames) =
                OwnedIpcReply::encode(&memory, &schema, pages, true, 1 << 20).unwrap()
            else {
                panic!("expected a framed Changes reply");
            };
            assert_eq!(frames.len(), if pages.is_empty() { 2 } else { 3 });
            assert_eq!(frames.last().unwrap().body.as_ref(), [255, 255, 255, 255, 0, 0, 0, 0]);
            assert_eq!(frames.iter().flat_map(|frame| frame.body.iter().copied()).collect::<Vec<_>>(), expected);
        }
    }

    #[test]
    fn held_change_touches_keep_legacy_bytes_and_reservation() {
        let groups = vec![
            OwnedChangeGroup { revision: 1, system_time: Timestamp(25), touches: touches() },
            OwnedChangeGroup { revision: 2, system_time: Timestamp(27), touches: vec![] },
        ];
        let logical = touches_type();
        let field = rtdb_columnar::to_field("touches", &logical).unwrap();
        let values = groups.iter().map(|group| legacy_touches(&group.touches)).collect::<Vec<_>>();
        let bytes = rtdb_columnar::estimate_builder_bytes(&logical, &values).unwrap();
        let refused = touches_column(&MemoryBudget::new(bytes - 1), &logical, &field, groups.clone()).unwrap_err();
        assert_eq!(refused.code(), tonic::Code::InvalidArgument);
        assert_eq!(refused.message(), format!("Cannot reserve {bytes} bytes from budget {} with 0 retained", bytes - 1));
        let (column, charge) = touches_column(&MemoryBudget::new(bytes), &logical, &field, groups).unwrap();
        assert_legacy_column(&logical, field, &values, column, charge);
    }

    #[test]
    fn singleton_binary_id_moves_when_the_original_buffer_capacity_matches() {
        let logical = touches_type();
        let field = rtdb_columnar::to_field("touches", &logical).unwrap();
        for size in [BYTE_ARRAY_DATA_CAPACITY, BYTE_ARRAY_DATA_CAPACITY + 1] {
            let payload = vec![0xa5; size];
            let original = payload.as_ptr();
            let mut touches = touches();
            touches[5].id = EntityId::Binary(payload);
            let values = [legacy_touches(&touches)];
            let bytes = rtdb_columnar::estimate_builder_bytes(&logical, &values).unwrap();
            let (column, charge) = touches_column(&MemoryBudget::new(bytes), &logical, &field, vec![
                OwnedChangeGroup { revision: 1, system_time: Timestamp(25), touches },
            ]).unwrap();
            let ids = touch_ids(&column);
            let binary = ids.child(4).as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(ids.type_id(5), 4);
            assert_eq!(ids.value_offset(5), 0);
            assert_eq!(binary.value(0).as_ptr() == original, size == BYTE_ARRAY_DATA_CAPACITY);
            assert_legacy_column(&logical, field.clone(), &values, column, charge);
        }
    }

    #[test]
    fn merged_reply_moves_the_only_binary_id_across_relation_groups() {
        let payload = vec![0x55; BYTE_ARRAY_DATA_CAPACITY];
        let original = payload.as_ptr();
        let mut first = touches();
        first[5].id = EntityId::Binary(payload);
        let second = touches()[..5].to_vec();
        let group = |name, sequence, touches| OwnedRelationChanges {
            relation: OwnedRelation::new(name).unwrap(), sequence, touches,
        };
        let logical = merged_type();
        let field = rtdb_columnar::to_field("changes", &logical).unwrap();
        let commits = vec![OwnedChangeCommit {
            publication: TransactionKey { tx_id: TxId(81), system_time: Timestamp(27) },
            changes: vec![group("alpha", 2, first), group("beta", 3, second)],
        }];
        let values = legacy_commits(&commits);
        let bytes = rtdb_columnar::estimate_builder_bytes(&logical, &values).unwrap();
        let (column, charge) = merged_column(&MemoryBudget::new(bytes), &logical, &field, commits).unwrap();
        let ids = touch_ids(list_items(&column).column(2));
        let binary = ids.child(4).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary.value(0).as_ptr(), original);
        assert_legacy_column(&logical, field, &values, column, charge);
    }

    fn legacy_commits(commits: &[OwnedChangeCommit]) -> Vec<Value> {
        commits.iter().map(|commit| Value::list(commit.changes.iter().map(|change| {
            Value::Struct(StructFields::from([
                ("relation".into(), Value::Utf8(change.relation.name().into())),
                ("relation_sequence".into(), Value::U64(change.sequence)),
                ("touches".into(), legacy_touches(&change.touches)),
            ]))
        }).collect())).collect()
    }

    #[test]
    fn positioned_relation_groups_keep_legacy_bytes_and_reservation() {
        let group = |name, sequence, touches| OwnedRelationChanges {
            relation: OwnedRelation::new(name).unwrap(), sequence, touches,
        };
        let commits = vec![
            OwnedChangeCommit {
                publication: TransactionKey { tx_id: TxId(81), system_time: Timestamp(27) },
                changes: vec![group("alpha", 2, touches()), group("beta", 3, touches()[2..].to_vec())],
            },
            OwnedChangeCommit {
                publication: TransactionKey { tx_id: TxId(80), system_time: Timestamp(27) },
                changes: vec![group("beta", 2, vec![])],
            },
        ];
        let logical = merged_type();
        let field = rtdb_columnar::to_field("changes", &logical).unwrap();
        let values = legacy_commits(&commits);
        let bytes = rtdb_columnar::estimate_builder_bytes(&logical, &values).unwrap();
        let (column, charge) = merged_column(&MemoryBudget::new(bytes), &logical, &field, commits).unwrap();
        assert_legacy_column(&logical, field, &values, column, charge);
    }
}
