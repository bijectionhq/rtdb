use std::borrow::Cow;

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
        use arrow_array::builder::{BinaryBuilder, Int64Builder, StringBuilder};
        use rtdb_types::Interval;

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
        let _copy = call.reserve(bytes)?;
        let mut ids = StringBuilder::new();
        let mut keys = BinaryBuilder::new();
        let mut starts = Int64Builder::new();
        let mut ends = Int64Builder::new();
        for page in pages {
            let batch = page.batch();
            let field = |name| batch.schema_ref().column_with_name(name).ok_or_else(history_invalid);
            let (id_index, id_field) = field("_id")?;
            let (key_index, key_field) = field(&column)?;
            for row in 0..batch.num_rows() {
                let (id, value) = unique_window_keys(
                    id_field, batch.column(id_index).as_ref(),
                    key_field, batch.column(key_index).as_ref(), row,
                )?;
                let start = history_row_time(batch, "_valid_from", row)?.ok_or_else(history_invalid)?;
                let end = history_row_time(batch, "_valid_to", row)?.ok_or_else(history_invalid)?;
                if start < from || end > to || start >= end
                    || value.is_empty() || value.len() > rtdb_query::OWNED_UNIQUE_KEY_MAX
                    || key.is_some_and(|key| key != value.as_ref())
                {
                    return Err(history_invalid());
                }
                ids.append_value(id);
                keys.append_value(value);
                starts.append_value(start.0);
                ends.append_value(end.0);
            }
        }
        let rows = RecordBatch::try_from_iter([
            ("_id", Arc::new(ids.finish()) as Arc<dyn Array>),
            ("key", Arc::new(keys.finish())),
            ("valid_from_micros", Arc::new(starts.finish())),
            ("valid_to_micros", Arc::new(ends.finish())),
        ]).map_err(arrow)?;
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

fn unique_window_keys<'a>(
    id_field: &Field,
    ids: &'a dyn Array,
    key_field: &Field,
    keys: &'a dyn Array,
    row: usize,
) -> Result<(Cow<'a, str>, Cow<'a, [u8]>), Status> {
    if id_field.metadata().is_empty() && key_field.metadata().is_empty()
        && let (Some(ids), Some(keys)) = (
            ids.as_any().downcast_ref::<StringArray>(),
            keys.as_any().downcast_ref::<BinaryArray>(),
        )
    {
        if ids.is_null(row) || keys.is_null(row) {
            return Err(history_invalid());
        }
        return Ok((Cow::Borrowed(ids.value(row)), Cow::Borrowed(keys.value(row))));
    }
    let id = rtdb_columnar::read_execution_value(id_field, ids, row).map_err(status)?;
    let key = rtdb_columnar::read_execution_value(key_field, keys, row).map_err(status)?;
    let (Value::Utf8(id), Value::Binary(key)) = (id, key) else { return Err(history_invalid()); };
    Ok((Cow::Owned(id), Cow::Owned(key)))
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
        assert!(matches!(&plain.0, Cow::Borrowed(_)));
        assert!(matches!(&plain.1, Cow::Borrowed(_)));
        assert_eq!(plain, unique_window_keys(&tagged_id, &ids, &key_field, &keys, 0).unwrap());
        assert_eq!(plain.0, "α");
        assert_eq!(plain.1.as_ref(), key);

        let plain_error = unique_window_keys(&id_field, &ids, &key_field, &keys, 1).unwrap_err();
        let fallback_error = unique_window_keys(&tagged_id, &ids, &key_field, &keys, 1).unwrap_err();
        assert_eq!(plain_error.code(), fallback_error.code());
        assert_eq!(plain_error.message(), fallback_error.message());
    }
}
