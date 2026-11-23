use rtdb_plan::{JoinKind, Relation, RelationNode};
use rtdb_types::Result;
use crate::{context::Execution, invalid};

fn size_error() -> rtdb_types::Anomaly { invalid("join-size","Join output or comparator metadata exceeds supported allocation bounds") }

pub(crate) fn tracks_build(kind: JoinKind, build_left: bool) -> bool {
    kind == JoinKind::Full || kind == JoinKind::Left && build_left || kind == JoinKind::Right && !build_left
}

pub(crate) fn preserves_probe(kind: JoinKind, build_left: bool) -> bool {
    matches!(kind, JoinKind::Full | JoinKind::Single)
        || kind == JoinKind::Left && !build_left || kind == JoinKind::Right && build_left
}

pub(crate) fn build_left(left: &Relation,right: &Relation,kind: JoinKind,execution: &Execution) -> Result<bool> {
    Ok(match kind {
        JoinKind::Left => row_count(left,execution)? < row_count(right,execution)?,
        JoinKind::Right => row_count(left,execution)? <= row_count(right,execution)?,
        JoinKind::Inner | JoinKind::Full => true,
        JoinKind::Semi | JoinKind::Anti | JoinKind::Single => false,
        JoinKind::Cross => return Err(invalid("join-kind","Cross join has no hash build side")),
    })
}
fn row_count(relation: &Relation,execution: &Execution) -> Result<i64> {
    match relation.node() {
        RelationNode::Values { rows } => i64::try_from(rows.len()).map_err(|_|size_error()),
        RelationNode::Scan(scan) => {
            let partitions = &execution.context.catalog.database(&scan.table.name.database)?.partitions;
            if partitions.is_empty() { return Ok(i64::MAX); }
            let mut rows = 0i64;
            for partition in partitions {
                let Some(table) = partition.table_catalog().metadata(&scan.table.name.table) else { return Ok(i64::MAX); };
                rows = rows.checked_add(table.row_count).ok_or_else(size_error)?;
            }
            Ok(rows)
        }
        RelationNode::Project { input,.. } | RelationNode::Extend { input,.. } | RelationNode::Rename { input,.. } | RelationNode::Filter { input,.. } => row_count(input,execution),
        RelationNode::Reference { binding } => row_count(&binding.relation,execution),
        RelationNode::Let { body,.. } => row_count(body,execution),
        _ => Ok(i64::MAX),
    }
}

/// The rows a join input holds at most: its tables' flushed block rows and live rows, or `i64::MAX` where it can't tell.
pub(crate) fn rows_at_most(relation: &Relation,execution: &Execution) -> Result<i64> {
    match relation.node() {
        RelationNode::Scan(scan) => {
            let partitions = &execution.context.catalog.database(&scan.table.name.database)?.partitions;
            if partitions.is_empty() { return Ok(i64::MAX); }
            let mut rows = 0i64;
            for partition in partitions {
                let table = &scan.table.name.table;
                let flushed = partition.table_catalog().metadata(table).map(|meta| meta.row_count);
                let live = partition.iter_segments(table).map(|segment| i64::from(segment.row_count())).sum::<i64>();
                if flushed.is_none() && live == 0 { return Ok(i64::MAX); }
                rows = rows.checked_add(flushed.unwrap_or(0)).and_then(|rows| rows.checked_add(live)).ok_or_else(size_error)?;
            }
            Ok(rows)
        }
        RelationNode::Project { input,.. } | RelationNode::Extend { input,.. } | RelationNode::Rename { input,.. } | RelationNode::Filter { input,.. } => rows_at_most(input,execution),
        RelationNode::Values { rows } => i64::try_from(rows.len()).map_err(|_|size_error()),
        _ => Ok(i64::MAX),
    }
}

#[cfg(test)]
#[path = "join_empty_page_tests.rs"]
mod tests;
