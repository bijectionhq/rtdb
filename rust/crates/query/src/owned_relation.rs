use std::sync::Arc;

use rtdb_plan::{Catalog, ColumnId, Expr, Field, QualifiedTable, Relation, Scan, TemporalFilter};
use rtdb_types::{Anomaly, Iid, Interval, OwnedRelation, Result, TimeUnit, Timestamp, Value};

use crate::{PreparedQuery, QueryContext, QueryCursor};

/// Opens a single admitted IID read without granting SQL access to the owned namespace.
pub async fn open_owned_point(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    iid: Iid,
    valid: Option<Interval>,
    columns: Option<&[&str]>,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/no-owner", "Owned relations require an authenticated principal"));
    }
    let name = QualifiedTable {
        database: context.catalog.default_database().to_owned(),
        table: relation.table(owner),
    };
    let context = context.for_owned_relation(name.clone());
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let fields = columns.map_or_else(|| table.columns.keys().map(String::as_str).collect(), |names| names.to_vec());
    let columns = fields.into_iter().enumerate().map(|(index, name)| Field {
        id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false,
        unnamed: false, data_type: table.scan_column_type(name),
    }).collect();
    let timestamp = |time: Timestamp| Expr::Literal(Value::Timestamp {
        value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
    });
    let valid_time = valid.map_or(TemporalFilter::Default, |valid| TemporalFilter::FromTo {
        from: timestamp(valid.lower()), to: timestamp(valid.upper()),
    });
    let scan = Arc::new(Relation::scan(Scan {
        table, columns, valid_time, system_time: TemporalFilter::Default, clamp_valid_time: false,
        iids: Some([iid].into()), predicates: Vec::new(), matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    let query = PreparedQuery::new_in(scan, &context.memory)?;
    query.execute(context, Vec::new()).await.map(Some)
}
