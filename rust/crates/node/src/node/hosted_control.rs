use std::sync::Arc;

use rtdb_query::{HostedControlPoint, QueryCursor};
use rtdb_tx::HostedControlWrite;
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use super::{OwnedReadSession, prepare_query};

#[derive(Clone, Copy)]
pub enum RootCatalogRead {
    Installation,
    RestoreHold,
    RestoreRelease,
    Custody,
    Owner,
    Admission,
    ProgramSchema,
    Declarations,
}

impl RootCatalogRead {
    pub fn relation(self) -> &'static str {
        match self {
            Self::Installation => "bj_installation",
            Self::RestoreHold | Self::RestoreRelease => "bj_rtdb_restore_hold",
            Self::Custody => "bj_rtdb_custody_binding",
            Self::Owner => "bj_fact_root_owner",
            Self::Admission => "bj_fact_root_admission",
            Self::ProgramSchema => "bj_fact_root_program",
            Self::Declarations => "bj_relation_declarations",
        }
    }

    pub fn maximum_rows(self) -> usize {
        match self {
            Self::Declarations => 128,
            _ => 1,
        }
    }

    fn query(self) -> String {
        let (columns, condition) = match self {
            Self::Installation => ("incarnation, kind, writer, _system_from", "_id = 'authority'"),
            Self::RestoreHold => ("binding", "_id = 'restore'"),
            Self::RestoreRelease => ("_system_from", "_id = 'released'"),
            Self::Custody => ("fingerprint", "_id = 'authority'"),
            Self::Owner => ("layout_digest, audience_tablet, _system_from", "_id = 'root'"),
            Self::Admission => ("admission, _system_from", "_id = 'application'"),
            Self::ProgramSchema => (
                "_id, generation, artifact, closure_digest, closure, application_schema, source_reference, environment_contract, _system_from",
                "_id = 'active'",
            ),
            Self::Declarations => ("_id, table_name, world, declaration, _system_from, names", "world = 0"),
        };
        format!("SELECT {columns} FROM public.{} WHERE {condition} LIMIT {}", self.relation(), self.maximum_rows() + 1)
    }
}

impl OwnedReadSession {
    pub async fn open_root_catalog(&self, read: RootCatalogRead) -> Result<QueryCursor> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let options = rtdb_sql::PrepareOptions {
            default_timezone: self.context.timezone.clone(),
            ..Default::default()
        };
        let cursor = prepare_query(&read.query(), &self.context, &options)?
            .execute(Arc::clone(&self.context), Vec::new()).await?;
        self.check()?;
        Ok(cursor)
    }

    /// Read an admitted control on the original hold or one of its public prefixes.
    pub async fn hosted_control_point(
        &self,
        target: &HostedControlWrite,
        at: Option<rtdb_types::TransactionKey>,
    ) -> Result<HostedControlPoint> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let unavailable = || {
            Anomaly::new(
                AnomalyCategory::Forbidden,
                "rtdb.hosted/control-scope",
                "Hosted controls require their original live placed hold",
            )
        };
        if Some(target.scope()) != self.witness_scope.or(self.applied_scope) {
            return Err(unavailable());
        }
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(unavailable());
        };
        if snapshot.tx_basis() != Some(self.present) {
            return Err(unavailable());
        }
        let context = if let Some(at) = at {
            let marker = rtdb_types::OwnedRelation::new(rtdb_types::OWNED_PUBLIC_ROOT_VERSIONS_RELATION)?;
            if self.lookup_publication(&marker, super::OwnedPublicationLookup::Position(at)).await?.is_none() {
                return Err(unavailable());
            }
            self.context.with_owned_transaction_cut(at, &self.context.cancellation)?
        } else {
            Arc::clone(&self.context)
        };
        let limit = at.unwrap_or(self.present).system_time;
        let point = rtdb_query::read_hosted_control_point(
            context,
            target.table(),
            target.id(),
            true,
        )
        .await?;
        if point
            .row
            .as_ref()
            .is_some_and(|(_, revision)| *revision > limit)
        {
            return Err(unavailable());
        }
        self.check()?;
        Ok(point)
    }
}
