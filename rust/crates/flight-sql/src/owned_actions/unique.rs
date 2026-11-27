use std::borrow::Cow;

use rtdb_types::{AnomalyKind, Interval};

use super::*;

pub(crate) const READ_HELD_UNIQUE_WINDOW_V1: &str = "rtdb.owned.ReadHeldUniqueWindow.v1";

pub(crate) const READ_HELD_UNIQUE_V1: &str = "rtdb.owned.ReadHeldUnique.v1";

impl Service {
    pub(crate) async fn held_owned_unique_window_action(
        &self,
        call: &Call,
        body: &Bytes,
        stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Owned unique window has no handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (mode, request) = request.split_first().ok_or_else(|| invalid("Owned unique window has no mask mode"))?;
        let (mask, request) = match *mode {
            0 => (None, request),
            1 => {
                let (mask, request) = split_position_mask(request)?;
                (Some(mask), request)
            },
            _ => return Err(invalid("Invalid owned unique window mask mode")),
        };
        let (relation, request) = split_relation(request)?;
        let (path, request_ipc) = split_relation(request)?;
        let input = decode_retained_batch(call, call.retain_request_bytes(body.slice_ref(request_ipc)))?;
        let input = input.batch();
        if input.num_rows() != 1 || input.num_columns() != 5 {
            return Err(invalid("Owned unique window needs one selector, system cut, valid window and limit"));
        }
        let key = input.column_by_name("key").and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or_else(|| invalid("Owned unique window key must be nullable Binary"))?;
        let key = (!key.is_null(0)).then(|| key.value(0));
        let system = history_request_endpoint(input, "system_micros")?;
        let from = history_request_endpoint(input, "valid_from_micros")?;
        let to = history_request_endpoint(input, "valid_to_micros")?;
        let valid = Interval::new(from, to).map_err(status)?;
        let limit = usize::try_from(history_request_integer(input, "limit")?)
            .map_err(|_| invalid("Invalid owned unique window limit"))?;
        let (pages, column, witness) = held.snapshot.unique_window(
            &relation, &path, key, valid, system, mask, limit,
        ).await.map_err(status)?;
        let bytes = pages.iter().try_fold(4096, |size, page| wire::add(size, page.batch().get_array_memory_size()))?;
        if bytes > self.0.config.maximum_message_bytes {
            return Err(Status::resource_exhausted("Owned unique window exceeds the response limit"));
        }
        let row_count = pages.iter().map(|page| page.batch().num_rows()).sum();
        let id_scratch = wire::mul(row_count, std::mem::size_of::<EntityIdRef<'_>>())?;
        let _copy = call.reserve(wire::add(bytes, id_scratch)?)?;
        let rows = unique_window_rows(&pages, &column, key, valid, row_count, call.memory())?;
        let mut metadata = [
            (OWNED_PRESENT.into(), system.0.to_string()),
            (OWNED_VALID_FROM.into(), from.0.to_string()),
            (OWNED_VALID_TO.into(), to.0.to_string()),
        ].into_iter().collect::<std::collections::HashMap<_, _>>();
        if let Some(witness) = witness {
            let nonce = held.remember(call, &relation, witness)?;
            metadata.insert(OWNED_RANGE_WITNESS.into(), nonce.simple().to_string());
        }
        let schema = Arc::new(rows.schema_ref().as_ref().clone().with_metadata(metadata));
        let reply = if rows.num_rows() == 0 {
            self.encode_owned_stream_reply(self.memory(), &schema, &[], stream)?
        } else {
            let rows = RecordBatch::try_new(Arc::clone(&schema), rows.columns().to_vec()).map_err(arrow)?;
            self.encode_owned_stream_reply(self.memory(), &schema, &[rows], stream)?
        };
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_unique_action(
        &self,
        call: &Call,
        envelope: &Bytes,
    ) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions {
            return Err(unsupported());
        }
        let (handle, body) = envelope
            .split_first_chunk::<16>()
            .ok_or_else(|| invalid("Owned held unique has no handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, body) = split_relation(body)?;
        let (path, arrow) = split_relation(body)?;
        let batch = decode_retained_batch(call, call.retain_request_bytes(envelope.slice_ref(arrow)))?;
        let (key, valid) = unique_point_selector(batch.batch())?;
        let (hit, witness) = held
            .snapshot
            .unique_observed(&relation, &path, key, valid)
            .await
            .map_err(status)?;
        let present = held.snapshot.system_time();
        let empty = Schema::empty();
        let row = hit.as_ref().map(|hit| hit.row.batch());
        let schema = row.map_or(&empty, |row| row.schema_ref().as_ref());
        let (mut bytes, charge) = Self::with_owned_point_page(
            self.memory(), schema, row, present, valid, hit.as_ref().map(|hit| hit.revision), None,
            |schema, row| encode_unique_point(
                call.memory(), schema, row, self.0.config.maximum_message_bytes,
            ),
        )?;
        let nonce = held.remember(call, &relation, witness)?;
        bytes[..16].copy_from_slice(nonce.as_bytes());
        held.snapshot.check_relations([&relation]).map_err(status)?;
        Ok(ActionResult {
            body: wire::retain(bytes, charge),
        })
    }
}

fn unique_point_selector(batch: &RecordBatch) -> Result<(&[u8], Timestamp), Status> {
    if batch.num_rows() != 1 || batch.num_columns() != 2 {
        return Err(invalid("Owned held unique requires exactly key and valid_micros"));
    }
    let (Some(key), Some(valid)) = (
        ArrowCell::named(batch, "key", 0).and_then(ArrowCell::binary),
        ArrowCell::named(batch, "valid_micros", 0).and_then(ArrowCell::int64),
    ) else {
        return Err(invalid("Owned held unique requires a Binary key and Int64 valid_micros"));
    };
    Ok((key, Timestamp(valid)))
}

fn encode_unique_point(
    memory: &Arc<MemoryBudget>, schema: &Schema, row: Option<&RecordBatch>, maximum: usize,
) -> Result<(Vec<u8>, Arc<MemoryReservation>), Status> {
    let body = row.map_or(0, RecordBatch::get_array_memory_size);
    let bound = wire::add(wire::schema_bound(schema)?, wire::add(16, wire::mul(body, 8)?)?)?;
    let _encoding = memory.reserve(bound).map_err(status)?;
    let rows = row.map(std::slice::from_ref).unwrap_or_default();
    let bytes = rtdb_format::ipc::write_prefixed_arrow_stream(
        schema, rows, arrow_ipc::writer::IpcWriteOptions::default(), &[0; 16],
    ).map_err(status)?;
    let size = bytes.len();
    let encoded = wire::add(1, wire::add(prost::length_delimiter_len(size), size)?)?;
    if encoded > maximum {
        return Err(Status::resource_exhausted("Owned unique reply exceeds the Flight response limit"));
    }
    let retained = memory.reserve(bytes.capacity()).map_err(status)?;
    Ok((bytes, retained))
}

fn unique_window_rows(
    pages: &[OwnedBatch], column: &str, key: Option<&[u8]>, valid: Interval, row_count: usize,
    memory: &Arc<MemoryBudget>,
) -> Result<RecordBatch, Status> {
    use arrow_array::builder::{BinaryBuilder, Int64Builder};

    let mut ids = Vec::with_capacity(row_count);
    let mut keys = BinaryBuilder::new();
    let mut starts = Int64Builder::with_capacity(row_count);
    let mut ends = Int64Builder::with_capacity(row_count);
    for page in pages {
        let batch = page.batch();
        let field = |name| batch.schema_ref().column_with_name(name).ok_or_else(history_invalid);
        let (id_index, id_field) = field("_id")?;
        let (key_index, key_field) = field(column)?;
        for row in 0..batch.num_rows() {
            let (id, value) = unique_window_keys(
                id_field, batch.column(id_index).as_ref(),
                key_field, batch.column(key_index).as_ref(), row,
            )?;
            let start = history_row_time(batch, "_valid_from", row)?.ok_or_else(history_invalid)?;
            let end = history_row_time(batch, "_valid_to", row)?.ok_or_else(history_invalid)?;
            if start < valid.lower() || end > valid.upper() || start >= end
                || value.is_empty() || value.len() > rtdb_query::OWNED_UNIQUE_KEY_MAX
                || key.is_some_and(|key| key != value.as_ref())
            {
                return Err(history_invalid());
            }
            ids.push(id);
            keys.append_value(value);
            starts.append_value(start.0);
            ends.append_value(end.0);
        }
    }
    let ids = match compact_unique_window_ids(pages, memory) {
        Some(compact) => compact,
        None => rtdb_columnar::entity_id_array(ids).map_err(status)?,
    };
    RecordBatch::try_from_iter([
        ("_id", ids),
        ("key", Arc::new(keys.finish())),
        ("valid_from_micros", Arc::new(starts.finish())),
        ("valid_to_micros", Arc::new(ends.finish())),
    ]).map_err(arrow)
}

fn compact_unique_window_ids(pages: &[OwnedBatch], memory: &Arc<MemoryBudget>) -> Option<ArrayRef> {
    let target = rtdb_columnar::to_field("_id", &entity_id_type()).ok()?;
    let DataType::Union(legs, arrow_schema::UnionMode::Dense) = target.data_type() else { return None; };
    let (_, source) = pages.first()?.batch().schema_ref().column_with_name("_id")?;
    let already_union = source.data_type() == target.data_type() && source.metadata() == target.metadata();
    if !already_union && !legs.iter().any(|(_, leg)| {
        source.data_type() == leg.data_type() && source.metadata() == leg.metadata()
    }) {
        return None;
    }

    let descriptors = pages.len().checked_mul(256)?.checked_add(4096)?;
    let mut scratch = memory.try_reserve(descriptors)?;
    let mut arrays = Vec::with_capacity(pages.len());
    let mut copy_bytes = descriptors;
    for page in pages {
        let batch = page.batch();
        let (index, field) = batch.schema_ref().column_with_name("_id")?;
        if field.data_type() != source.data_type() || field.metadata() != source.metadata() {
            return None;
        }
        let array = batch.column(index).as_ref();
        for row in 0..array.len() {
            let cell_bytes = rtdb_columnar::estimate_copy_cell_bytes(field, array, row).ok()?;
            copy_bytes = copy_bytes.checked_add(cell_bytes)?.checked_add(64)?;
        }
        arrays.push(array);
    }
    let scratch = Arc::get_mut(&mut scratch)?;
    scratch.grow_to(copy_bytes).ok()?;
    let cells = arrays.iter().enumerate()
        .flat_map(|(page, array)| (0..array.len()).map(move |row| (page, row)))
        .collect::<Vec<_>>();
    let copied = rtdb_columnar::copy_selected_cells(&arrays, &cells).ok()?;
    let canonical = if already_union {
        copied
    } else {
        let relayout = rtdb_columnar::arrow_relayout_bound(copied.as_ref(), source, &target).ok()?;
        scratch.grow_to(copy_bytes.checked_add(relayout)?).ok()?;
        let (field, canonical) = rtdb_columnar::relayout_arrow_column(copied, source, &target).ok()?;
        if field != target { return None; }
        canonical
    };
    let reply_allowance = arrays.iter().try_fold(4096usize, |bytes, array| {
        bytes.checked_add(array.get_array_memory_size())
    })?;
    (canonical.get_array_memory_size() <= reply_allowance).then_some(canonical)
}

fn unique_window_keys<'a>(
    id_field: &Field,
    ids: &'a dyn Array,
    key_field: &Field,
    keys: &'a dyn Array,
    row: usize,
) -> Result<(EntityIdRef<'a>, Cow<'a, [u8]>), Status> {
    let id = match rtdb_columnar::read_execution_owned_entity_id(id_field, ids, row) {
        Ok(id) => id,
        Err(error) if error.is(AnomalyKind::InvalidEntityId) => None,
        Err(error) => return Err(status(error)),
    };
    let key = if let Some(keys) = keys.as_any().downcast_ref::<BinaryArray>().filter(|_| key_field.metadata().is_empty()) {
        (!keys.is_null(row)).then(|| Cow::Borrowed(keys.value(row)))
    } else {
        match rtdb_columnar::read_execution_value(key_field, keys, row).map_err(status)? {
            Value::Binary(key) => Some(Cow::Owned(key)),
            _ => None,
        }
    };
    match (id, key) {
        (Some(id), Some(key)) => Ok((id, key)),
        _ => Err(history_invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_point_borrows_key_and_emits_the_exact_prefixed_reply() {
        let batch = RecordBatch::try_from_iter([
            ("key", Arc::new(BinaryArray::from_iter_values([&[0_u8, 255, 42][..]])) as ArrayRef),
            ("valid_micros", Arc::new(Int64Array::from(vec![83]))),
        ]).unwrap();
        let (key, valid) = unique_point_selector(&batch).unwrap();
        let source = batch.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(key, &[0, 255, 42]);
        assert_eq!(key.as_ptr(), source.value(0).as_ptr());
        assert_eq!(valid, Timestamp(83));

        let memory = MemoryBudget::new(1 << 20);
        for row in [None, Some(&batch)] {
            let schema = row.map_or_else(Schema::empty, |row| row.schema_ref().as_ref().clone());
            let old = rtdb_format::ipc::write_arrow_stream(
                &schema, row.map(std::slice::from_ref).unwrap_or_default(),
                arrow_ipc::writer::IpcWriteOptions::default(),
            ).unwrap();
            let size = old.len() + 16;
            let limit = 1 + prost::length_delimiter_len(size) + size;
            let (reply, charge) = encode_unique_point(&memory, &schema, row, limit).unwrap();
            assert_eq!(&reply[..16], &[0; 16]);
            assert_eq!(&reply[16..], old.as_slice());
            assert_eq!(charge.bytes(), reply.capacity());
            let error = encode_unique_point(&memory, &schema, row, limit - 1).err().unwrap();
            assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        }
    }

    #[test]
    fn unique_window_borrows_plain_cells_and_preserves_the_fallback() {
        let id_field = Field::new("_id", DataType::Utf8, true);
        let key_field = Field::new("key", DataType::Binary, true);
        let tagged_id = id_field.clone().with_metadata([("fixture".into(), "value".into())].into());
        let ids = StringArray::from(vec![Some("α"), None]);
        let key = &[0, 255, 23][..];
        let keys = BinaryArray::from_iter([Some(key), Some(key)]);

        let plain = unique_window_keys(&id_field, &ids, &key_field, &keys, 0).unwrap();
        assert!(matches!(plain.0, EntityIdRef::String(id) if id.as_ptr() == ids.value(0).as_ptr()));
        assert!(matches!(&plain.1, Cow::Borrowed(_)));
        assert_eq!(plain, unique_window_keys(&tagged_id, &ids, &key_field, &keys, 0).unwrap());
        assert_eq!(plain.0, EntityIdRef::String("α"));
        assert_eq!(plain.1.as_ref(), key);

        let plain_error = unique_window_keys(&id_field, &ids, &key_field, &keys, 1).unwrap_err();
        let fallback_error = unique_window_keys(&tagged_id, &ids, &key_field, &keys, 1).unwrap_err();
        assert_eq!(plain_error.code(), fallback_error.code());
        assert_eq!(plain_error.message(), fallback_error.message());
    }

    #[test]
    fn unique_window_rejects_invalid_ids_and_preserves_cell_read_errors() {
        let id_field = Field::new("_id", DataType::Boolean, false);
        let ids = arrow_array::BooleanArray::from(vec![true]);
        let key_field = Field::new("key", DataType::Binary, false);
        let keys = BinaryArray::from_iter_values([b"key".as_slice()]);

        let invalid_id = unique_window_keys(&id_field, &ids, &key_field, &keys, 0).unwrap_err();
        assert_eq!(invalid_id.code(), tonic::Code::Internal);
        assert_eq!(invalid_id.message(), "Owned history returned invalid or overlapping slices");

        let read_error = unique_window_keys(&id_field, &ids, &key_field, &keys, 1).unwrap_err();
        assert_eq!(read_error.code(), tonic::Code::InvalidArgument);
        assert_eq!(read_error.message(), "Cell index exceeds array length");
    }

    #[test]
    fn unique_window_reply_keeps_typed_identities_and_repeated_slices_in_source_order() {
        let memory = MemoryBudget::new(1 << 20);
        let key = &[9, 255][..];
        let id = EntityIdRef::Binary(&[0, 255, 0]);
        let ids = [id, EntityIdRef::String("00ff00"), id];
        let times = |values| Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef;
        let page = RecordBatch::try_from_iter([
            ("_id", rtdb_columnar::entity_id_array(ids).unwrap()),
            ("unique_key", Arc::new(BinaryArray::from_iter_values([key; 3]))),
            ("_valid_from", times(vec![10, 12, 20])),
            ("_valid_to", times(vec![20, 14, 30])),
        ]).unwrap();
        let pages = [OwnedBatch::new(page, &memory).unwrap()];
        let valid = Interval::from_micros(10, 30).unwrap();
        let reply = unique_window_rows(&pages, "unique_key", Some(key), valid, ids.len(), &memory).unwrap();
        let schema = reply.schema_ref();
        let field = rtdb_columnar::to_field("_id", &entity_id_type()).unwrap();
        assert_eq!(schema.field(0), &field);
        for (row, id) in ids.into_iter().enumerate() {
            assert_eq!(read_owned_entity_id(&field, reply.column(0).as_ref(), row).unwrap(), id);
        }
        for (column, expected) in [("valid_from_micros", [10, 12, 20]), ("valid_to_micros", [20, 14, 30])] {
            let times = reply.column_by_name(column).unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(times.values().as_ref(), &expected);
        }
        let empty = unique_window_rows(&[], "unique_key", Some(key), valid, 0, &memory).unwrap();
        assert_eq!(empty.schema_ref(), schema);
        assert_eq!(empty.num_rows(), 0);
        let error = unique_window_rows(&pages, "unique_key", Some(&[4]), valid, ids.len(), &memory).unwrap_err();
        assert_eq!(error.message(), history_invalid().message());
    }

    #[test]
    fn unique_window_compacts_sliced_identity_columns_with_identical_ipc() {
        let memory = MemoryBudget::new(1 << 20);
        let key = b"cell".as_slice();
        let valid = Interval::from_micros(10, 30).unwrap();
        let dense = rtdb_columnar::entity_id_array([
            EntityIdRef::Binary(b"hidden"),
            EntityIdRef::String("first"),
            EntityIdRef::Binary(b"same"),
            EntityIdRef::Binary(b"same"),
        ]).unwrap();
        let scalar: ArrayRef = Arc::new(StringArray::from(vec!["hidden", "first", "same", "same"]));
        for ids in [dense, scalar] {
            let page = RecordBatch::try_from_iter([
                ("_id", ids),
                ("unique_key", Arc::new(BinaryArray::from_iter_values([key; 4])) as ArrayRef),
                ("_valid_from", Arc::new(TimestampMicrosecondArray::from(vec![10, 10, 15, 20]))),
                ("_valid_to", Arc::new(TimestampMicrosecondArray::from(vec![15, 15, 20, 25]))),
            ]).unwrap();
            let pages = [
                OwnedBatch::new(page.slice(1, 1), &memory).unwrap(),
                OwnedBatch::new(page.slice(2, 2), &memory).unwrap(),
            ];
            for (sources, rows) in [(&pages[..1], 1), (&pages[..], 3)] {
                let compact = compact_unique_window_ids(sources, &memory).unwrap();
                let no_scratch = MemoryBudget::new(0);
                assert!(compact_unique_window_ids(sources, &no_scratch).is_none());
                let actual = unique_window_rows(sources, "unique_key", Some(key), valid, rows, &memory).unwrap();
                let fallback = unique_window_rows(sources, "unique_key", Some(key), valid, rows, &no_scratch).unwrap();
                assert_eq!(actual.column(0).to_data(), compact.to_data());
                let ipc = |batch: &RecordBatch| {
                    rtdb_format::ipc::write_arrow_stream(
                        batch.schema_ref(), std::slice::from_ref(batch), arrow_ipc::writer::IpcWriteOptions::default(),
                    ).unwrap()
                };
                assert_eq!(ipc(&actual), ipc(&fallback));
            }
        }
    }

    #[test]
    fn unique_window_declines_identity_compaction_above_the_reply_allowance() {
        use arrow_array::UnionArray;

        let memory = MemoryBudget::new(1 << 20);
        let payload = vec![0x7f; 8192];
        let dense = rtdb_columnar::entity_id_array([EntityIdRef::Binary(&payload)]).unwrap();
        let dense = dense.as_any().downcast_ref::<UnionArray>().unwrap();
        let DataType::Union(fields, _) = dense.data_type() else { unreachable!() };
        let repeated = UnionArray::try_new(
            fields.clone(),
            vec![dense.type_id(0); 8].into(),
            Some(vec![0; 8].into()),
            fields.iter().map(|(tag, _)| Arc::clone(dense.child(tag))).collect(),
        ).unwrap();
        let page = RecordBatch::try_from_iter([("_id", Arc::new(repeated) as ArrayRef)]).unwrap();
        let pages = [OwnedBatch::new(page, &memory).unwrap()];
        assert!(compact_unique_window_ids(&pages, &memory).is_none());
    }
}
