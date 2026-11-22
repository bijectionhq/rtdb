use std::{collections::BTreeSet, sync::Arc};

use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result, TimeUnit, Timestamp, Value,
};

use super::OwnedReadSession;

pub(super) struct Scope {
    pub(super) system_time: Timestamp,
    pub(super) relations: BTreeSet<OwnedRelation>,
    pub(super) purge_context: Arc<rtdb_query::QueryContext>,
}

const MAX_SCOPE_RELATIONS: usize = 32;

impl OwnedReadSession {
    /// Opens an independent point-only hold at the last visible marker in this captured generation.
    pub async fn derive_as_of(
        &self,
        requested: Timestamp,
        publication: &OwnedRelation,
        marker: &EntityId,
        relations: Vec<OwnedRelation>,
    ) -> Result<Option<Self>> {
        self.admit_read().await?;
        if requested > self.present.system_time
            || !(1..=MAX_SCOPE_RELATIONS).contains(&relations.len())
        {
            return Err(unavailable());
        }
        let charge = self
            .context
            .memory
            .reserve(self.database.len() + self.owner.len() + relations.len() * 1024 + 4096)?;
        let count = relations.len();
        let relations: BTreeSet<_> = relations.into_iter().collect();
        if relations.len() != count || !relations.contains(publication) {
            return Err(unavailable());
        }
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(unavailable());
        };
        if snapshot.tx_basis() != Some(self.present)
            || snapshot.system_time_cap() != Some(self.present.system_time)
        {
            return Err(unavailable());
        }
        let mut floor = None;
        for relation in &relations {
            OwnedRelation::new(relation.name())?;
            rtdb_query::require_owned_address_coverage(&self.context, &self.owner, relation)?;
            if let Some(required) = self.certify_historical_cut(relation, requested).await? {
                floor = Some(floor.map_or(required, |floor: Timestamp| floor.max(required)));
            }
        }

        let cancellation = self.session_cancellation.child_token();
        let requested_cut = self
            .context
            .with_owned_system_cut(requested, &cancellation)?;
        let Some(selected) = self
            .publication_at(requested_cut, publication, marker)
            .await?
        else {
            self.check()?;
            return Ok(None);
        };
        if selected > requested || floor.is_some_and(|floor| selected < floor) {
            return Err(unavailable());
        }
        self.check_point_purge_floor(publication, marker, selected).await?;
        let context = self
            .context
            .with_owned_system_cut(selected, &cancellation)?;
        self.check()?;
        context.check_cancelled()?;
        Ok(Some(Self {
            context,
            node: self.node.clone(),
            identity: Arc::clone(&self.identity),
            database: self.database.clone(),
            owner: self.owner.clone(),
            present: self.present,
            witness_scope: None,
            witness_until: selected,
            cancellation,
            session_cancellation: self.session_cancellation.clone(),
            historical: Some(Scope {
                system_time: selected,
                relations,
                purge_context: Arc::clone(&self.context),
            }),
            _reservation: charge,
        }))
    }

    async fn publication_at(
        &self,
        context: Arc<rtdb_query::QueryContext>,
        publication: &OwnedRelation,
        marker: &EntityId,
    ) -> Result<Option<Timestamp>> {
        let Some(mut cursor) = rtdb_query::open_owned_point(
            context,
            &self.owner,
            publication,
            marker,
            None,
            Some(&["_valid_from", "_valid_to"]),
        )
        .await?
        else {
            return Ok(None);
        };
        let mut selected = None;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            if batch.num_rows() == 0 {
                continue;
            }
            if selected.is_some() || batch.num_rows() != 1 {
                return Err(unavailable());
            }
            let committed_at =
                rtdb_query::owned_row_revision(batch, 0, marker, &self.context.memory)?;
            let read = |name| {
                let (column, field) = batch
                    .schema_ref()
                    .column_with_name(name)
                    .ok_or_else(unavailable)?;
                rtdb_columnar::read_value(field, batch.column(column).as_ref(), 0)
            };
            let from = read("_valid_from")?;
            let to = read("_valid_to")?;
            if !matches!(from, Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } if value == Timestamp::MIN.0)
                || to != Value::Null
            {
                return Err(unavailable());
            }
            selected = Some(committed_at);
        }
        Ok(selected)
    }
}

pub(super) fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/historical-uncovered",
        "The held scope cannot certify this historical root cut",
    )
}
