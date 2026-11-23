use super::*;

pub(crate) const READ_HELD_UNIQUE_WINDOW_V1: &str = "rtdb.owned.ReadHeldUniqueWindow.v1";

pub(crate) const READ_HELD_UNIQUE_V1: &str = "rtdb.owned.ReadHeldUnique.v1";

impl Service {
    pub(crate) async fn held_owned_unique_window_action(
        &self,
        call: &Call,
        body: &[u8],
    ) -> Result<ActionResult, Status> {
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
        let (path, arrow) = split_relation(request)?;
        let input = request_batch(call, arrow)?;
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
                let id = rtdb_columnar::read_execution_value(id_field, batch.column(id_index).as_ref(), row).map_err(status)?;
                let value = rtdb_columnar::read_execution_value(key_field, batch.column(key_index).as_ref(), row).map_err(status)?;
                let (Value::Utf8(id), Value::Binary(value)) = (id, value) else { return Err(history_invalid()); };
                let start = history_row_time(batch, "_valid_from", row)?.ok_or_else(history_invalid)?;
                let end = history_row_time(batch, "_valid_to", row)?.ok_or_else(history_invalid)?;
                if start < from || end > to || start >= end
                    || value.is_empty() || value.len() > rtdb_query::OWNED_UNIQUE_KEY_MAX
                    || key.is_some_and(|key| key != value.as_slice())
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
            self.owned_stream_reply(&schema, &[])?
        } else {
            let rows = RecordBatch::try_new(Arc::clone(&schema), rows.columns().to_vec()).map_err(arrow)?;
            self.owned_stream_reply(&schema, &[rows])?
        };
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_unique_action(
        &self,
        call: &Call,
        body: &[u8],
    ) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions {
            return Err(unsupported());
        }
        let (handle, body) = body
            .split_first_chunk::<16>()
            .ok_or_else(|| invalid("Owned held unique has no handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, body) = split_relation(body)?;
        let (path, arrow) = split_relation(body)?;
        let batch = request_batch(call, arrow)?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 2 {
            return Err(invalid(
                "Owned held unique requires exactly key and valid_micros",
            ));
        }
        let (rows, _decoded) = upload::domain_documents(batch.batch(), self)?;
        let (Some(Value::Binary(key)), Some(Value::I64(valid))) =
            (rows[0].get("key"), rows[0].get("valid_micros"))
        else {
            return Err(invalid(
                "Owned held unique requires a Binary key and Int64 valid_micros",
            ));
        };
        let valid = Timestamp(*valid);
        let (hit, witness) = held
            .snapshot
            .unique_observed(&relation, &path, key, valid)
            .await
            .map_err(status)?;
        let present = held.snapshot.system_time();
        let reply = if let Some(hit) = hit {
            let row = hit.row.batch();
            self.owned_point_reply(
                row.schema_ref(),
                Some(row),
                present,
                valid,
                Some(hit.revision),
            )?
        } else {
            self.owned_point_reply(&Schema::empty(), None, present, valid, None)?
        };
        let size = wire::add(16, reply.body.len())?;
        let encoded = wire::add(1, wire::add(prost::length_delimiter_len(size), size)?)?;
        if encoded > self.0.config.maximum_message_bytes {
            return Err(Status::resource_exhausted(
                "Owned unique reply exceeds the Flight response limit",
            ));
        }
        let charge = call.reserve(size)?;
        let nonce = held.remember(call, &relation, witness)?;
        let mut bytes = Vec::with_capacity(size);
        bytes.extend_from_slice(nonce.as_bytes());
        bytes.extend_from_slice(&reply.body);
        held.snapshot.check_relations([&relation]).map_err(status)?;
        Ok(ActionResult {
            body: wire::retain(bytes, charge),
        })
    }
}
