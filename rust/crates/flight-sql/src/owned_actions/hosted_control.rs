use super::*;
use rtdb_node::RootCatalogRead;

pub(crate) const READ_HELD_HOSTED_CONTROL_V3: &str = "rtdb.owned.ReadHeldHostedControl.v3";
pub(crate) const READ_HELD_HOSTED_CONTROL_AT_PUBLICATION_V1: &str = "rtdb.owned.ReadHeldHostedControlAtPublication.v1";
pub(crate) const READ_HELD_ROOT_CATALOG_V1: &str = "rtdb.owned.ReadHeldRootCatalog.v1";

pub(super) fn require_audience_control(
    session: &crate::state::Session, relation: &str, id: &str,
) -> Result<(), Status> {
    if session.audience_source().is_some() && !audience_control_admitted(relation, id) {
        return Err(Status::permission_denied("The control is outside the prepared audience"));
    }
    Ok(())
}

fn audience_control_admitted(relation: &str, id: &str) -> bool {
    // Provider discovery belongs to the ordinary authentication path.
    (relation, id) != ("bj_fact_root_admission", "provider_issuers")
}

impl Service {
    pub(crate) async fn read_held_root_catalog_action(
        &self,
        call: &Call,
        body: &[u8],
    ) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if call.session.audience_source().is_some()
            || call.session.caller.principal.user_id().is_none()
            || matches!(call.session.caller.owner, crate::state::Owner::Anonymous)
        {
            return Err(Status::permission_denied("Root catalog reads require an ordinary authenticated session"));
        }
        let (handle, body) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Root catalog read requires a held handle"))?;
        let read = match body {
            [0] => RootCatalogRead::Installation,
            [1] => RootCatalogRead::RestoreHold,
            [2] => RootCatalogRead::RestoreRelease,
            [3] => RootCatalogRead::Custody,
            [4] => RootCatalogRead::Owner,
            [5] => RootCatalogRead::Admission,
            [6] => RootCatalogRead::ProgramSchema,
            [7] => RootCatalogRead::Declarations,
            _ => return Err(invalid("Invalid held Root catalog projection")),
        };
        if call.session.placed_database().is_some() {
            let policy = self.hosted_control_policy(call)?;
            let relation = OwnedRelation::new(read.relation()).map_err(status)?;
            admit_control_targets(policy, [&relation])?;
        }
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let mut cursor = held.snapshot.open_root_catalog(read).await.map_err(status)?;
        let mut retained = Vec::new();
        let mut rows = 0;
        while let Some(page) = cursor.next_batch().await.map_err(status)? {
            let count = page.batch().num_rows();
            if count == 0 { continue; }
            rows += count;
            if rows > read.maximum_rows() {
                return Err(Status::resource_exhausted("Root catalog exceeds its row bound"));
            }
            retained.push(page);
        }
        let schema = match retained.first() {
            Some(page) => page.batch().schema_ref().as_ref().clone(),
            None => (*wire::plan_schema(cursor.runtime_schema(), call.memory())?).clone(),
        };
        let mut metadata = schema.metadata().clone();
        metadata.insert(OWNED_PRESENT.into(), held.snapshot.system_time().0.to_string());
        let schema = Arc::new(schema.with_metadata(metadata));
        let pages = retained.iter().map(|page| {
            RecordBatch::try_new(Arc::clone(&schema), page.batch().columns().to_vec())
        }).collect::<Result<Vec<_>, _>>().map_err(arrow)?;
        let reply = self.owned_stream_reply_in(call.memory(), &schema, &pages)?;
        held.snapshot.check().map_err(status)?;
        Ok(reply)
    }

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
        positioned: bool,
    ) -> Result<ActionResult, Status> {
        let policy = self.hosted_control_policy(call)?;
        let (handle, body) = body
            .split_first_chunk::<16>()
            .ok_or_else(|| invalid("Hosted control point has no held handle"))?;
        let held = self.retained_owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (at, body) = if positioned {
            if call.session.audience_source().is_some() { return Err(unsupported()); }
            let (at, body) = split_world_publication(body)?;
            (Some(at), body)
        } else {
            (None, body)
        };
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
        require_audience_control(&call.session, table.name(), id)?;
        let scope = self.hosted_control_scope(call).await?;
        if positioned && !matches!((table.name(), id),
            ("bj_fact_root_program", "active") | ("bj_fact_root_admission", "application"))
        {
            return Err(Status::permission_denied("A public publication admits executable controls only"));
        }
        let target = rtdb_tx::HostedControlWrite::check(
            table,
            scope,
            None,
            StructFields::from([("_id".into(), Value::Utf8(id.to_owned()))]),
        )
        .map_err(status)?;
        let point = held
            .snapshot
            .hosted_control_point(&target, at)
            .await
            .map_err(status)?;
        let present = at.map_or_else(|| held.snapshot.system_time(), |at| at.system_time);
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
                self.owned_point_reply_in(
                    call.memory(),
                    &schema,
                    Some(&row),
                    present,
                    Timestamp(0),
                    Some(revision),
                    None,
                )?
            }
            None => {
                let schema = wire::plan_schema(point.schema.schema(), call.memory())?;
                let schema = schema.project(&fields(&schema)).map_err(arrow)?;
                self.owned_point_reply_in(call.memory(), &schema, None, present, Timestamp(0), None, None)?
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

#[cfg(test)]
mod audience_tests {
    use super::*;

    #[test]
    fn provider_discovery_is_outside_audience_control_admission() {
        assert!(!audience_control_admitted("bj_fact_root_admission", "provider_issuers"));
        assert!(audience_control_admitted("bj_fact_root_admission", "application"));
    }
}
