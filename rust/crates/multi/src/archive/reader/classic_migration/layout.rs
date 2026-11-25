use std::{collections::BTreeSet, fs::File, io::Read, path::Path, sync::Arc};

use rtdb_types::{Anomaly, EntityId, Result, TableRef};
use serde::Deserialize;

use super::points::{PointHistoryRead, PointSelector, valid_table};

const MAX_LAYOUT_BYTES: u64 = 16 << 10;
const MAX_RELATIONS: usize = 64;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApplicationLayout {
    format: u32,
    pub(super) closure: PointSelector,
    pub(super) writer: PointSelector,
    pub(super) custody: PointSelector,
    pub(super) sql_cutoff_prefix: String,
    pub(super) inventory: Vec<TableRef>,
    pub(super) sql_release: SqlReleaseLayout,
    pub(super) external_key_recovery: Option<TableRef>,
    pub(super) named_holds: Option<NamedHoldLayout>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NamedHoldLayout {
    pub(super) table: TableRef,
    pub(super) id_prefix: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SqlReleaseLayout {
    pub(super) grants: TableRef,
    pub(super) rows: TableRef,
    pub(super) execution_units: TableRef,
    pub(super) joined: TableRef,
    pub(super) stopped: TableRef,
}

impl ApplicationLayout {
    pub(crate) fn installed(path: &Path) -> Result<Arc<Self>> {
        let mut bytes = Vec::new();
        File::open(path)
            .and_then(|file| file.take(MAX_LAYOUT_BYTES + 1).read_to_end(&mut bytes))
            .map_err(|_| invalid())?;
        Self::parse(&bytes).map(Arc::new)
    }

    pub(super) fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() as u64 > MAX_LAYOUT_BYTES {
            return Err(invalid());
        }
        let layout: Self = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        let points = [&layout.closure, &layout.writer, &layout.custody];
        let inventory = layout.inventory.iter().collect::<BTreeSet<_>>();
        let roles = &layout.sql_release;
        let sql = [
            &roles.grants,
            &roles.rows,
            &roles.execution_units,
            &roles.joined,
            &roles.stopped,
        ];
        if layout.format != 1
            || layout.sql_cutoff_prefix.is_empty()
            || layout.sql_cutoff_prefix.len() > 128
            || !layout
                .sql_cutoff_prefix
                .bytes()
                .all(|byte| byte.is_ascii_graphic())
            || points.iter().any(|point| {
                matches!(&point.id, EntityId::String(id)
                if point.table == layout.closure.table && id.starts_with(&layout.sql_cutoff_prefix))
            })
            || layout.closure == layout.writer
            || layout.closure == layout.custody
            || layout.writer == layout.custody
            || layout.inventory.len() > MAX_RELATIONS
            || inventory.len() != layout.inventory.len()
            || !layout.inventory.iter().all(valid_table)
            || sql.iter().copied().collect::<BTreeSet<_>>().len() != sql.len()
            || !layout.inventory.contains(&layout.closure.table)
            || !sql.iter().all(|table| layout.inventory.contains(*table))
            || layout.external_key_recovery.as_ref().is_some_and(|table| {
                !layout.inventory.contains(table)
                    || points.iter().any(|point| &point.table == table)
                    || sql.contains(&table)
            })
            || layout.named_holds.as_ref().is_some_and(|named| {
                !inventory.contains(&named.table)
                    || named.id_prefix.is_empty()
                    || named.id_prefix.len() > 128
                    || !named.id_prefix.bytes().all(|byte| byte.is_ascii_graphic())
            })
        {
            return Err(invalid());
        }
        for point in points {
            if !point.valid()? {
                return Err(invalid());
            }
        }
        Ok(layout)
    }

    pub(super) fn cutoff(&self, epoch: u64) -> PointSelector {
        PointSelector {
            table: self.closure.table.clone(),
            id: EntityId::String(format!("{}{epoch:020}", self.sql_cutoff_prefix)),
        }
    }

    pub(super) fn observes(&self, table: &TableRef) -> bool {
        table == &self.closure.table
            || table == &self.writer.table
            || table == &self.custody.table
            || self.inventory.contains(table)
    }

    pub(crate) fn admits(&self, points: &PointHistoryRead) -> bool {
        points.selects_only(&self.closure, &self.writer)
    }
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.multi/classic-application-layout",
        "Invalid installed Classic application replay layout",
    )
}
