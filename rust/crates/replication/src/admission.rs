use rtdb_format::catalog::BlockCatalog;
use rtdb_types::Result;

use crate::admission_step::{Admission, TermFence, Verdict};
use crate::{PartitionLogs, ReplicaRecord};

pub(crate) struct AdmittedReplica(ReplicaRecord);

impl AdmittedReplica {
    pub(crate) async fn resolve_payload(self, logs: &PartitionLogs) -> Result<Self> {
        Ok(Self(logs.resolve_oversized(self.0).await?))
    }

    pub(crate) async fn resolve_payload_fresh(self, logs: &PartitionLogs) -> Result<Self> {
        Ok(Self(logs.resolve_oversized_fresh(self.0).await?))
    }

    pub(crate) async fn resolve_payload_for_readback(self, logs: &PartitionLogs, expected: Option<crate::readback_identity::ReplicaFingerprint>) -> Result<Self> {
        Ok(Self(logs.resolve_oversized_for_readback(self.0, expected).await?))
    }
}

impl std::ops::Deref for AdmittedReplica {
    type Target = ReplicaRecord;

    fn deref(&self) -> &ReplicaRecord { &self.0 }
}

impl TermFence {
    pub fn from_catalog(block: &BlockCatalog) -> Result<Self> { Self::from_boundary(block.boundary_term(), block.boundary_term_sequence()) }

    pub(crate) fn admit_record(&mut self, record: ReplicaRecord) -> Result<(Admission, Option<AdmittedReplica>)> {
        let admission = self.admit(record.envelope.term, record.envelope.term_sequence)?;
        let admitted = (admission.verdict == Verdict::Apply).then(|| AdmittedReplica(record));
        Ok((admission, admitted))
    }
}
