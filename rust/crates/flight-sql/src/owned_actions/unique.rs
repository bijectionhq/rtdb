use super::*;

pub(crate) const READ_HELD_UNIQUE_V1: &str = "rtdb.owned.ReadHeldUnique.v1";

impl Service {
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
