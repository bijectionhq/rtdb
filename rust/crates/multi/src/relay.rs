//! The compaction relay: a worker's compaction reaches the deployment's holder, which alone appends it (`AdoptCompactionOutputs`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;
use rtdb_directory::{Directory, effective_holder, now_ms};
use rtdb_format::log::proto::TriesAdded;
use rtdb_types::{Anomaly, AnomalyCategory, Result, SourcePosition};

use crate::control::{self, Request, Response};

pub struct HolderRelay { pub directory: Arc<dyn Directory> }

#[async_trait]
impl rtdb_node::CompactionRelay for HolderRelay {
    async fn publish(&self, database: &str, notification: TriesAdded) -> Result<SourcePosition> {
        let record = self.directory.deployment_by_name(database).await?.ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment is named {database}")))?;
        let members = self.directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect();
        let holder = effective_holder(&record, &members, now_ms()).and_then(|holder| members.get(holder)).and_then(|member| member.control_address.clone())
            .ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-holder", format!("{database} has no holder to publish its compaction")))?;
        match control::call(&holder, &Request::PublishCompaction { name: database.to_owned(), notification: notification.encode_to_vec() }, Duration::from_secs(35)).await? {
            Response::Published { position } => Ok(SourcePosition(position)),
            Response::Refused { code, message } => Err(Anomaly::new(AnomalyCategory::Unavailable, code, message)),
            other => Err(Anomaly::fault("rtdb.multi/compaction-relay", format!("The holder answered {other:?}"))),
        }
    }
}
