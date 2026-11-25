use crate::default_erasure::{
    self, DefaultErasure, ErasureCompleted, ErasureRetirement, SelectedRewrite,
};

use super::*;

impl ReplicationDriver {
    /// Supplies the local provider's read-grant horizon, including clock uncertainty.
    pub fn with_default_erasure_grant(mut self, grant_micros: u64) -> Self {
        self.default_erasure_grant_micros = Some(grant_micros);
        self
    }

    pub fn with_default_erasure_read_fence(
        mut self,
        fence: Arc<dyn crate::DefaultErasureReadFence>,
    ) -> Self {
        self.default_erasure_read_fence = Some(fence);
        self
    }

    pub fn with_local_default_erasure_read_fence(mut self, fence: Arc<crate::LocalDefaultErasureReadFence>) -> Self {
        self.local_erasure_read_fence = Some(Arc::clone(&fence));
        self.with_default_erasure_read_fence(fence)
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
                let Some(fence) = self.default_erasure_read_fence.as_deref() else { return Ok(()); };
                let predecessor = selected.evidence.prior_generation.ok_or_else(default_erasure::unsupported)?;
                fence.seal_prior(uuid::Uuid::from_bytes(selected.evidence.cut.basis.incarnation()),
                    predecessor, selected.evidence.boundary).await?;
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
                ordered_intent,
                log_prefixes_settled,
                objects_settled,
            }) => {
                let intent = *intent;
                let ordered_intent = *ordered_intent;
                let Some(fence) = self.default_erasure_read_fence.as_deref() else {
                    return Ok(());
                };
                let authority = self
                    .serving
                    .as_ref()
                    .ok_or_else(serving::unavailable)?
                    .checkpoint_authority();
                if !intent.binds_predecessor()
                    || !authority
                        .checkpoints
                        .as_ref()
                        .is_some_and(|keys| keys.kms.supports_durable_generation_retirement())
                {
                    return Ok(());
                }
                let cut = intent.read_cut()?;
                let Some(read_closure) = fence.close(cut).await? else {
                    return Ok(());
                };
                if !fence.verify(cut, read_closure).await? {
                    return Err(default_erasure::invalid());
                }
                let log_prefixes_settled =
                    *log_prefixes_settled || intent.settle_log_prefixes(&self.logs).await?;
                let now_lower = Timestamp(
                    (self.clock)()
                        .0
                        .saturating_sub(SERVING_CLOCK_UNCERTAINTY_MICROS),
                );
                let objects_settled = *objects_settled
                    || log_prefixes_settled
                        && intent
                            .settle_objects(&self.blocks, &self.logs, now_lower)
                            .await?;
                if objects_settled && intent.destroy_predecessor(authority, now_lower).await? {
                    let completed = ErasureCompleted {
                        intent,
                        ordered_intent,
                        read_closure,
                    };
                    completed.verify(&self.blocks, &self.logs, authority, fence).await?;
                    let block = self.partition.value().live().block_index();
                    self.leader.as_mut().ok_or_else(term_ended)?.enqueue(
                        WriteBody::Message(ReplicaBody::NoOp(completed.message()?)),
                        block,
                        None,
                        &self.resolve.memory,
                    )?;
                }
                self.default_erasure = Some(DefaultErasure::Retiring {
                    intent,
                    ordered_intent,
                    log_prefixes_settled,
                    objects_settled,
                });
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(super) fn retry_default_erasure(&self) -> bool {
        self.leader.as_ref().is_some_and(|leader| leader.confirmed)
            && self.default_erasure_read_fence.is_some()
            && matches!(self.default_erasure, Some(DefaultErasure::Retiring { .. }))
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
            (Some(_), ReplicaBody::NoOp(message)) => {
                ErasureRetirement::decode(message)?.is_some()
                    || ErasureCompleted::decode(message)?.is_some()
            }
            _ => false,
        };
        if admitted {
            Ok(())
        } else {
            Err(default_erasure::invalid())
        }
    }

    pub(super) async fn apply_default_erasure_control(
        &mut self,
        message: &proto::NoOp,
        position: ReplicaPosition,
    ) -> Result<bool> {
        if let Some(completed) = ErasureCompleted::decode(message)? {
            self.apply_default_erasure_completion(completed, position)
                .await?;
            return Ok(true);
        }
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
        if self.default_erasure_read_fence.is_some() {
            let authority = self.serving.as_ref().ok_or_else(serving::unavailable)?.checkpoint_authority();
            intent.arm_predecessor(authority).await?;
        }
        self.default_erasure = Some(DefaultErasure::Retiring {
            intent,
            ordered_intent: position,
            log_prefixes_settled: false,
            objects_settled: false,
        });
        Ok(true)
    }

    async fn apply_default_erasure_completion(
        &mut self,
        completed: ErasureCompleted,
        position: ReplicaPosition,
    ) -> Result<()> {
        let Some(DefaultErasure::Retiring {
            intent,
            ordered_intent,
            ..
        }) = self.default_erasure
        else {
            return Err(default_erasure::invalid());
        };
        completed.require_order(intent, ordered_intent, position)?;
        let fence = self
            .default_erasure_read_fence
            .as_deref()
            .ok_or_else(default_erasure::unsupported)?;
        let serving = self.serving.as_mut().ok_or_else(serving::unavailable)?;
        completed
            .verify(&self.blocks, &self.logs, serving.checkpoint_authority(), fence)
            .await?;
        let cut = intent.read_cut()?;
        serving.complete_erasure(intent.cut(), cut.successor, cut.unreadable_by)?;
        self.default_erasure = None;
        if let Some(leader) = &mut self.leader
            && leader.source_tail.is_none()
            && leader.source_poll.is_none()
            && leader.source_head.is_none()
            && leader.source_batch.is_empty()
        {
            leader.source_tail = Some(
                self.logs
                    .source_replay(self.progress.source_position())
                    .await?,
            );
        }
        Ok(())
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

    /// Borrows only the fields it reads, so the future stays `Send` while the driver itself (holding its
    /// partition tails) is not `Sync`.
    pub(super) fn verify_default_erasure_upload<'a>(
        &'a self,
        catalog: &'a BlockCatalog,
        boundary: &'a ReplicaRecord,
        upload: &'a ReplicaRecord,
    ) -> impl Future<Output = Result<Option<SelectedRewrite>>> + Send + 'a {
        let (erasure, native, blocks, logs, cancellation) =
            (&self.default_erasure, &self.serving, &self.blocks, &self.logs, &self.cancellation);
        async move {
            let Some(DefaultErasure::Applied(captured)) = erasure else {
                return Ok(None);
            };
            let authority = native
                .as_ref()
                .ok_or_else(serving::unavailable)?
                .checkpoint_authority();
            let mut selected = default_erasure::selected_rewrite(
                authority,
                blocks,
                logs,
                catalog,
                cancellation,
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
}
