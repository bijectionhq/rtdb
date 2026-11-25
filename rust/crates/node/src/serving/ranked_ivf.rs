use std::{collections::BTreeSet, sync::Arc};

use rtdb_index::branches::RankedIndexDeclaration;
use rtdb_plan::Catalog as _;
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, AnomalyCategory, Result, TableRef, Timestamp, rtdb_v1::RANKED_IVF};

#[derive(Clone, PartialEq, Eq)]
pub struct Target {
    pub table: TableRef,
    pub index: String,
    pub generation: u64,
}

pub struct Request {
    pub target: Target,
    pub valid: Timestamp,
    pub k: usize,
    pub probe: Vec<f32>,
    pub filters: Vec<(String, Vec<Vec<u8>>)>,
    pub staged: Vec<Staged>,
}

pub struct Staged {
    pub identity_key: Vec<u8>,
    pub replacement: Option<Replacement>,
}

pub struct Replacement {
    pub embedding: Vec<f32>,
    pub filter_values: Vec<Option<Vec<u8>>>,
}

impl Request {
    pub fn validate(&self) -> Result<()> {
        let name =
            |name: &str| !name.is_empty() && name.len() <= 255 && !name.contains(['\0', '/']);
        if self.target.generation == 0
            || !name(&self.target.table.schema)
            || !name(&self.target.table.table)
            || matches!(
                self.target.table.schema.as_str(),
                "xt" | "information_schema" | "pg_catalog"
            )
            || TableRef::owned_schema(&self.target.table.schema)
            || !name(&self.target.index)
            || !(1..=256).contains(&self.k)
            || !(1..=4096).contains(&self.probe.len())
            || self.probe.iter().any(|value| !value.is_finite())
            || !self.probe.iter().any(|&value| value != 0.0)
            || self.filters.len() > 8
            || self.staged.len() > 64
            || self.filters.iter().any(|(column, values)| {
                !name(column)
                    || !(1..=64).contains(&values.len())
                    || values.iter().any(|value| value.len() > 1024)
            })
            || self.staged.iter().any(|row| {
                row.identity_key.len() > 4096
                    || row.replacement.as_ref().is_some_and(|replacement| {
                        replacement.embedding.len() != self.probe.len()
                            || replacement.embedding.iter().any(|value| !value.is_finite())
                            || replacement.filter_values.len() != self.filters.len()
                            || replacement
                                .filter_values
                                .iter()
                                .flatten()
                                .any(|value| value.len() > 1024)
                    })
            })
        {
            return Err(invalid());
        }
        if self
            .filters
            .iter()
            .map(|(column, _)| column)
            .collect::<BTreeSet<_>>()
            .len()
            != self.filters.len()
        {
            return Err(invalid());
        }
        let mut masks = BTreeSet::new();
        for row in &self.staged {
            let id = rtdb_columnar::EntityIdRef::from_handle_bytes(&row.identity_key)?;
            if !masks.insert(id) {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

pub struct Catalog {
    pub(crate) target: Target,
    pub(crate) declaration: RankedIndexDeclaration,
    pub(crate) physical: rtdb_types::CatalogCut,
    pub(crate) storage: Arc<BufferPool>,
    pub(crate) mounted: bool,
}

impl Catalog {
    pub fn mounted_storage(&self) -> Option<&Arc<BufferPool>> {
        self.mounted.then_some(&self.storage)
    }

    pub async fn execute(
        self,
        context: Arc<QueryContext>,
        request: Request,
    ) -> Result<(QueryCursor, usize, bool)> {
        #[cfg(not(feature = "search"))]
        {
            let _ = (
                self.target,
                self.declaration,
                self.physical,
                self.storage,
                context,
                request,
            );
            Err(unsupported())
        }
        #[cfg(feature = "search")]
        {
            use rtdb_columnar::EntityIdRef;
            use rtdb_index::branches::RankedIndexKind;
            use rtdb_search::{HeldVectorOverlay, NearestQuery, nearest::NativeNearest};

            if request.target != self.target {
                return Err(uncovered());
            }
            let database = context
                .catalog
                .database(context.catalog.default_database())?;
            let [snapshot] = database.partitions.as_slice() else {
                return Err(uncovered());
            };
            if snapshot.catalog_cut() != Some(self.physical)
                || !Arc::ptr_eq(&database.storage, &self.storage)
            {
                return Err(uncovered());
            }
            let RankedIndexKind::Ivf { dimensions, .. } = self.declaration.definition.kind else {
                return Err(uncovered());
            };
            let Some((_, columns)) = self.declaration.definition.columns.split_first() else {
                return Err(uncovered());
            };
            if request.probe.len() != dimensions as usize
                || request
                    .filters
                    .iter()
                    .any(|(column, _)| !columns.contains(column))
            {
                return Err(invalid());
            }
            let bytes = request
                .filters
                .iter()
                .flat_map(|(_, values)| values)
                .fold(4096usize, |bytes, value| {
                    bytes.saturating_add(value.len()).saturating_add(128)
                });
            let _filter_memory = context.memory.reserve(bytes)?;
            let (filters, values): (Vec<_>, Vec<_>) = request
                .filters
                .into_iter()
                .map(|(column, values)| (column, values.into_iter().collect()))
                .unzip();
            let query = NearestQuery::new(request.probe, values, request.k)?;
            let mut overlay = HeldVectorOverlay::new(&context.memory)?;
            for staged in &request.staged {
                let id = EntityIdRef::from_handle_bytes(&staged.identity_key)?;
                let vector = staged
                    .replacement
                    .as_ref()
                    .map(|replacement| replacement.embedding.as_slice());
                let values = staged
                    .replacement
                    .as_ref()
                    .map_or(&[][..], |replacement| replacement.filter_values.as_slice());
                overlay.shadow_staged(
                    0,
                    id,
                    vector,
                    values.iter().map(Option::as_deref),
                    &query,
                )?;
            }
            let nearest = NativeNearest {
                table: &self.target.table,
                index: &self.target.index,
                declaration: &self.declaration,
                valid: request.valid,
                filters: &filters,
            };
            if self.mounted {
                let definition = rtdb_vector::mounted_native_definition(&self.target.table, &self.target.index, &self.declaration)?;
                nearest.query_mounted(context, &query, &overlay, &definition).await
            } else {
                nearest.query(context, &query, &overlay).await
            }
        }
    }
}

pub(crate) fn unsupported() -> Anomaly {
    Anomaly::unsupported(
        "rtdb.vector/native-unsupported",
        format!("The route does not support {RANKED_IVF}"),
    )
}

pub(crate) fn uncovered() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.vector/native-uncovered",
        "The native IVF generation cannot cover this bounded exact-cut query",
    )
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.vector/native-request",
        "Invalid bounded native IVF request",
    )
}
