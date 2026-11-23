use super::*;

pub(crate) const READ_HELD_HOSTED_CONTROL_V2: &str = "rtdb.owned.ReadHeldHostedControl.v2";

impl Service {
    pub(super) fn hosted_control_policy(
        &self,
        call: &Call,
    ) -> Result<&crate::config::HostedControlConfig, Status> {
        let policy = self
            .0
            .config
            .hosted_controls
            .as_ref()
            .ok_or_else(unsupported)?;
        if !self.0.config.owned_native_actions
            || call.session.placed_database() != Some(call.database.as_str())
            || call.session.caller.principal.user_id() != Some(policy.owner.as_str())
            || matches!(call.session.caller.owner, crate::state::Owner::Anonymous)
        {
            return Err(Status::permission_denied(
                "Hosted controls require the installed placed writer",
            ));
        }
        Ok(policy)
    }

    pub(super) async fn hosted_control_scope(
        &self,
        call: &Call,
    ) -> Result<rtdb_types::PlacedAttemptScope, Status> {
        let connection = self.connection(call, &call.database).await?;
        let native = connection.borrow().await?;
        require_idle(&native)?;
        native.placed_history().map_err(status)?.ok_or_else(|| {
            Status::failed_precondition("Hosted controls require live ordered placement")
        })
    }

    pub(crate) async fn read_held_hosted_control_action(
        &self,
        call: &Call,
        body: &[u8],
    ) -> Result<ActionResult, Status> {
        let policy = self.hosted_control_policy(call)?;
        let (handle, body) = body
            .split_first_chunk::<16>()
            .ok_or_else(|| invalid("Hosted control point has no held handle"))?;
        let held = self.retained_owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (table, ipc) = split_relation(body)?;
        admit_control_targets(policy, [&table])?;
        let batch = request_batch(call, ipc)?;
        let batch = batch.batch();
        if batch.num_rows() != 1
            || batch.num_columns() != 1
            || batch.schema_ref().field_with_name("_id").is_err()
            || !batch.schema_ref().field(0).metadata().is_empty()
        {
            return Err(invalid(
                "Hosted control point requires one row with a Utf8 _id",
            ));
        }
        let id = batch
            .column_by_name("_id")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .filter(|ids| !ids.is_null(0))
            .ok_or_else(|| invalid("Hosted control point requires a complete Utf8 _id"))?;
        let id = id.value(0);
        if id.is_empty() || id.len() > 4096 {
            return Err(invalid("Hosted control point ID exceeds its bound"));
        }
        let _key = call.reserve(id.len() * 4 + 1024)?;
        let scope = self.hosted_control_scope(call).await?;
        let target = rtdb_tx::HostedControlWrite::check(
            table,
            scope,
            None,
            StructFields::from([("_id".into(), Value::Utf8(id.to_owned()))]),
        )
        .map_err(status)?;
        let point = held
            .snapshot
            .hosted_control_point(&target)
            .await
            .map_err(status)?;
        let present = held.snapshot.system_time();
        let fields = |schema: &Schema| {
            schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| !matches!(field.name().as_str(), "_valid_from" | "_valid_to" | rtdb_types::HOSTED_RECEIPT_TX_ID))
                .map(|(index, _)| index)
                .collect::<Vec<_>>()
        };
        let reply = match point.row {
            Some((row, revision)) => {
                let row = row
                    .batch()
                    .project(&fields(row.batch().schema_ref()))
                    .map_err(arrow)?;
                let mut schema = row.schema_ref().as_ref().clone();
                if let Some(tx_id) = point.receipt_tx_id {
                    let mut metadata = schema.metadata().clone();
                    metadata.insert("rtdb.owned.receipt_tx_id".into(), tx_id.to_string());
                    schema = schema.with_metadata(metadata);
                }
                self.owned_point_reply(
                    &schema,
                    Some(&row),
                    present,
                    Timestamp(0),
                    Some(revision),
                )?
            }
            None => {
                let schema = wire::plan_schema(point.schema.schema(), self.memory())?;
                let schema = schema.project(&fields(&schema)).map_err(arrow)?;
                self.owned_point_reply(&schema, None, present, Timestamp(0), None)?
            }
        };
        held.snapshot.check().map_err(status)?;
        Ok(reply)
    }
}

pub(super) fn admit_control_targets<'a>(
    policy: &crate::config::HostedControlConfig,
    tables: impl IntoIterator<Item = &'a OwnedRelation>,
) -> Result<(), Status> {
    if tables
        .into_iter()
        .any(|table| !policy.relations.contains(table.name()))
    {
        return Err(Status::permission_denied(
            "Hosted control targets are outside the installed allowlist",
        ));
    }
    Ok(())
}
