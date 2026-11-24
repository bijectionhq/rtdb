use std::{
    net::SocketAddr,
    sync::{Arc, Weak},
};

use rtdb_node::{Database, MultiFollower, Node};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use uuid::Uuid;

/// The trusted cell owner's exact assignment for one local Multi replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeFollowerAssignment {
    pub database: String,
    pub incarnation: Uuid,
    pub placement_epoch: u64,
    pub assignment: Uuid,
}

pub(crate) struct FollowerRoute {
    pub assignment: NativeFollowerAssignment,
    pub endpoint: SocketAddr,
    pub reader: MultiFollower,
    attachment: Weak<Database>,
    selector: String,
}

impl FollowerRoute {
    pub fn new(
        assignment: NativeFollowerAssignment,
        endpoint: SocketAddr,
        attachment: &Arc<Database>,
        reader: MultiFollower,
    ) -> Result<Self> {
        if assignment.database != attachment.name()
            || assignment.database.contains('@')
            || assignment.incarnation.is_nil()
            || assignment.placement_epoch == 0
            || assignment.assignment.is_nil()
            || !attachment.serving_driver().is_some_and(|driver| {
                driver.incarnation() == *assignment.incarnation.as_bytes()
                    && reader.matches_driver(driver)
            })
        {
            return Err(Anomaly::incorrect(
                "rtdb/follower-assignment",
                "The assignment does not name this Multi replica",
            ));
        }
        let selector = format!(
            "{}@{}",
            assignment.database,
            assignment.incarnation.simple()
        );
        if selector.parse::<tonic::metadata::AsciiMetadataValue>().is_err() {
            return Err(Anomaly::incorrect(
                "rtdb/follower-assignment",
                "The assignment cannot be represented as a Flight database selector",
            ));
        }
        Ok(Self {
            assignment,
            endpoint,
            reader,
            attachment: Arc::downgrade(attachment),
            selector,
        })
    }

    pub fn select(&self, headers: &tonic::metadata::MetadataMap) -> Result<&str> {
        let mut selection = headers.get_all("x-xtdb-database").iter();
        if selection.next().and_then(|value| value.to_str().ok()) != Some(self.selector.as_str())
            || selection.next().is_some()
        {
            return Err(Anomaly::new(
                AnomalyCategory::Forbidden,
                "rtdb/follower-assignment",
                "The request must name the assigned database incarnation",
            ));
        }
        Ok(&self.assignment.database)
    }

    pub fn current(&self, node: &Node) -> Result<Arc<Database>> {
        node.catalog()
            .database(&self.assignment.database)
            .ok()
            .filter(|database| std::ptr::eq(Arc::as_ptr(database), self.attachment.as_ptr()))
            .ok_or_else(|| {
                Anomaly::new(
                    AnomalyCategory::Unavailable,
                    "rtdb/follower-unavailable",
                    "The assigned local replica has detached",
                )
            })
    }
}
