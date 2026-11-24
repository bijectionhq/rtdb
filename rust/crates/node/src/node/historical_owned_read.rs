use std::{collections::BTreeSet, sync::Arc};

use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result, TimeUnit, Timestamp, TransactionKey, Value,
};

use super::OwnedReadSession;

pub(super) struct Scope {
    pub(super) publication: TransactionKey,
    pub(super) relations: BTreeSet<OwnedRelation>,
    // Erasure floors and journal coverage use the original generation's head.
    pub(super) root_context: Arc<rtdb_query::QueryContext>,
}

const MAX_SCOPE_RELATIONS: usize = 32;

impl OwnedReadSession {
    /// Opens an independent read-only hold at the last visible marker in this captured generation.
    pub async fn derive_as_of(
        &self,
        requested: Timestamp,
        publication: &OwnedRelation,
        marker: &EntityId,
        relations: Vec<OwnedRelation>,
    ) -> Result<Option<Self>> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        if self.is_applied() || requested > self.present.system_time
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
            if let Some(required) = self.certify_historical_cut(relation, requested).await? {
                floor = Some(floor.map_or(required, |floor: Timestamp| floor.max(required)));
            }
        }

        let cancellation = self.session_cancellation.child_token();
        let requested_cut = self.guard_context(
            self.context.with_owned_system_cut(requested, &cancellation)?,
            &relations,
        )?;
        let Some(selected) = self
            .publication_at(requested_cut, publication, marker)
            .await?
        else {
            self.check_relations(&relations)?;
            return Ok(None);
        };
        if !selected.at_or_before(self.present) || selected.system_time > requested {
            return Err(unavailable());
        }
        if let Some(floor) = floor.filter(|floor| selected.system_time < *floor) {
            return Err(below_floor(floor));
        }
        for relation in &relations {
            self.certify_historical_position(relation, selected).await?;
        }
        self.check_point_purge_floor(publication, marker, selected.system_time).await?;
        let context = self
            .context
            .with_owned_transaction_cut(selected, &cancellation)?;
        let root_context = if selected == self.present {
            Arc::clone(&context)
        } else {
            self.context
                .with_owned_system_cut(self.present.system_time, &cancellation)?
        };
        self.check_relations(&relations)?;
        context.check_cancelled()?;
        Ok(Some(Self {
            context,
            node: self.node.clone(),
            identity: Arc::clone(&self.identity),
            database: self.database.clone(),
            owner: self.owner.clone(),
            present: self.present,
            witness_scope: None,
            applied_scope: None,
            applied_preparation: None,
            witness_until: selected.system_time,
            fork_until: self.fork_until,
            cancellation,
            session_cancellation: self.session_cancellation.clone(),
            attachment: self.attachment.clone(),
            scoped_root: None,
            historical: Some(Scope {
                publication: selected,
                relations,
                root_context,
            }),
            _reservation: charge,
        }))
    }

    async fn publication_at(
        &self,
        context: Arc<rtdb_query::QueryContext>,
        publication: &OwnedRelation,
        marker: &EntityId,
    ) -> Result<Option<TransactionKey>> {
        let Some(mut cursor) = rtdb_query::open_owned_point(
            context,
            &self.owner,
            publication,
            marker,
            None,
            Some(&["_valid_from", "_valid_to", rtdb_types::OWNED_EVENT_POSITION_COLUMN]),
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
            let event = rtdb_query::owned_row_event_position(batch, 0)?;
            selected = Some(TransactionKey { tx_id: event.transaction, system_time: committed_at });
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

pub(super) fn below_floor(floor: Timestamp) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/history-floor",
        "The requested system cut precedes the certified owned history floor",
    )
    .with_context("history-floor-micros", floor.0.into())
}
