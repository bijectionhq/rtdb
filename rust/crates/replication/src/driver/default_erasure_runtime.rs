use crate::default_erasure::{self, DefaultErasure, ErasureRetirement, SelectedRewrite};

use super::*;

impl ReplicationDriver {
    /// Supplies the local provider's read-grant horizon, including clock uncertainty.
    pub fn with_default_erasure_grant(mut self, grant_micros: u64) -> Self {
        self.default_erasure_grant_micros = Some(grant_micros);
        self
    }

    pub(super) async fn drive_default_erasure(&mut self) -> Result<()> {
        if !self.leader.as_ref().is_some_and(|leader| {
            leader.confirmed && leader.accepting() && leader.drained_for_strict_source()
        }) {
            return Ok(());
        }
        match &self.default_erasure {
            Some(DefaultErasure::Applied(captured)) => {
                if self.progress.source_position() != captured.cut.source
                    || self
                        .leader
                        .as_ref()
                        .ok_or_else(term_ended)?
                        .resolver()?
                        .source_position()
                        != captured.cut.source
                {
                    return Err(default_erasure::invalid());
                }
                self.cut().await
            }
            Some(DefaultErasure::Selected(selected)) if selected.uploaded.is_some() => {
                let Some(grant) = self.default_erasure_grant_micros else {
                    return Ok(());
                };
                let upper = Timestamp(
                    (self.clock)()
                        .0
                        .saturating_add(SERVING_CLOCK_UNCERTAINTY_MICROS),
                );
                let intent = ErasureRetirement::observe(*selected, upper, grant)?;
                let block = self.partition.value().live().block_index();
                self.leader.as_mut().ok_or_else(term_ended)?.enqueue(
                    WriteBody::Message(ReplicaBody::NoOp(intent.message()?)),
                    block,
                    None,
                    &self.resolve.memory,
                )
            }
            Some(DefaultErasure::Retiring {
                intent,
                local_logs_settled: false,
            }) => {
                let intent = *intent;
                let local_logs_settled = intent.settle_local_logs(&self.logs).await?;
                self.default_erasure = Some(DefaultErasure::Retiring {
                    intent,
                    local_logs_settled,
                });
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(super) fn retry_default_erasure(&self) -> bool {
        self.leader.as_ref().is_some_and(|leader| leader.confirmed)
            && matches!(
                self.default_erasure,
                Some(DefaultErasure::Retiring {
                    local_logs_settled: false,
                    ..
                })
            )
    }

    pub(super) fn require_default_erasure_order(&self, record: &ReplicaRecord) -> Result<()> {
        let admitted = match (&self.default_erasure, &record.envelope.message) {
            (None, _) => true,
            (
                Some(DefaultErasure::Applied(_)),
                ReplicaBody::BlockBoundary(_) | ReplicaBody::BlockUploaded(_),
            ) => true,
            (
                Some(DefaultErasure::Selected(_) | DefaultErasure::Retiring { .. }),
                ReplicaBody::BlockUploaded(_),
            ) => true,
            (Some(_), ReplicaBody::NoOp(message)) => ErasureRetirement::decode(message)?.is_some(),
            _ => false,
        };
        if admitted {
            Ok(())
        } else {
            Err(default_erasure::invalid())
        }
    }

    pub(super) fn apply_default_erasure_control(
        &mut self,
        message: &proto::NoOp,
        position: ReplicaPosition,
    ) -> Result<bool> {
        let Some(intent) = ErasureRetirement::decode(message)? else {
            return Ok(false);
        };
        if let Some(DefaultErasure::Retiring { intent: prior, .. }) = self.default_erasure.as_ref()
        {
            return if prior == &intent {
                Ok(true)
            } else {
                Err(default_erasure::invalid())
            };
        }
        let Some(DefaultErasure::Selected(selected)) = self.default_erasure.as_ref() else {
            return Err(default_erasure::invalid());
        };
        intent.require_selected(*selected, position)?;
        self.default_erasure = Some(DefaultErasure::Retiring {
            intent,
            local_logs_settled: false,
        });
        Ok(true)
    }

    pub(super) fn observe_default_erasure_upload(&mut self, record: &ReplicaRecord) -> Result<()> {
        let Some(DefaultErasure::Selected(selected)) = &mut self.default_erasure else {
            return Ok(());
        };
        let ReplicaBody::BlockUploaded(upload) = &record.envelope.message else {
            return Err(default_erasure::invalid());
        };
        if upload.block_index == Some(selected.checkpoint.block.0) {
            selected.observe_upload(record)?;
        }
        Ok(())
    }

    pub(super) async fn verify_default_erasure_upload(
        &self,
        catalog: &BlockCatalog,
        boundary: &ReplicaRecord,
        upload: &ReplicaRecord,
    ) -> Result<Option<SelectedRewrite>> {
        let Some(DefaultErasure::Applied(captured)) = &self.default_erasure else {
            return Ok(None);
        };
        let authority = self
            .serving
            .as_ref()
            .ok_or_else(serving::unavailable)?
            .checkpoint_authority();
        let mut selected = default_erasure::selected_rewrite(
            authority,
            &self.blocks,
            &self.logs,
            catalog,
            &self.cancellation,
        )
        .await?
        .ok_or_else(default_erasure::invalid)?;
        if selected.evidence.cut != captured.cut
            || selected.evidence.boundary != boundary.metadata.position
        {
            return Err(default_erasure::invalid());
        }
        selected.observe_upload(upload)?;
        Ok(Some(selected))
    }
}
