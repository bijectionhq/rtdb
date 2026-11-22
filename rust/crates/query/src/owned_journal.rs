use std::sync::Arc;

use rtdb_types::{EntityId, OwnedRelation, Result, Timestamp, Value, change_feed_uncovered};

use crate::{QueryContext, owned_relation::open_owned_table_point, owned_row_revision};

/// The durable order and coverage fields on one owner-scoped journal row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedJournalHeader {
    pub committed_at: Timestamp,
    pub through_events: i64,
}

/// Probes the immutable revision without loading or decoding its touch list.
pub async fn read_owned_journal_header(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    revision: i64,
) -> Result<OwnedJournalHeader> {
    let id = EntityId::Integer(revision);
    let memory = Arc::clone(&context.memory);
    let journal = relation.change_journal(owner)?;
    let Some(mut cursor) = open_owned_table_point(
        context,
        journal,
        id.iid(),
        Some(&id),
        None,
        Some(&["through_events"]),
        false,
    )
    .await?
    else {
        return Err(change_feed_uncovered());
    };
    let mut header = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            if header.is_some() {
                return Err(change_feed_uncovered());
            }
            let committed_at = owned_row_revision(batch, row, &id, &memory)?;
            let (column, field) = batch
                .schema_ref()
                .column_with_name("through_events")
                .ok_or_else(change_feed_uncovered)?;
            let Value::I64(through_events) =
                rtdb_columnar::read_value(field, batch.column(column).as_ref(), row)?
            else {
                return Err(change_feed_uncovered());
            };
            if through_events < 0 {
                return Err(change_feed_uncovered());
            }
            header = Some(OwnedJournalHeader {
                committed_at,
                through_events,
            });
        }
    }
    header.ok_or_else(change_feed_uncovered)
}
