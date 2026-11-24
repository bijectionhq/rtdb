use rtdb_directory::ReplicaDraftRetirement;
use rtdb_s3::CreateAttemptPublication;

use super::{*, transfer::{current_generation_abandoned, transfer_excluded, transfer_pointer_digest}};

const MAX_STAGES: usize = 128;
const AUDIT_WIDTH: i64 = 64;

pub(super) fn reserve_body(
    record: &DeploymentRecord, key: &ObjectKey, route: PublishRoute, body_sha256: [u8; 32],
) -> Result<ReplicaPreparedPublication> {
    let stages = record.storage_regions.values().filter_map(|region| match region {
        StorageRegion::ReplicaPublication { key, draft: Some(_), .. } => Some(key),
        _ => None,
    });
    let mut count = 0;
    for existing in stages {
        if existing == key.as_str() { return Err(refused()); }
        count += 1;
    }
    if count >= MAX_STAGES {
        return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb.multi/replay-staging-full",
            "The detached journal preparation ledger is full"));
    }
    // A published checkpoint covers a committed position. Its own offset
    // cannot contain a reference to a newly prepared body.
    let scanned_through = record.checkpoint.filter(|root| root.root_sha256.is_some()).map_or(-1, |root| root.boundary)
        .max(record.replica_anchor.as_ref().map_or(-1, |anchor| anchor.interval[1]));
    if !(-1..i64::MAX).contains(&scanned_through) { return Err(refused()); }
    Ok(ReplicaPreparedPublication { route, scanned_through, body_sha256, attempt: None, version: None, reference: None, retirement: None })
}

impl Service {
    pub(super) async fn finish_staged_upload(
        &self, history: History, route: PublishRoute, id: Uuid, key: &ObjectKey, version: &PhysicalObjectVersion,
        pointer: Option<&ReplicaEnvelope>,
    ) -> Result<()> {
        let CellStorage::S3 { config } = &self.storage else { return Err(refused()); };
        let admitted = self.current(history).await?;
        let Some(StorageRegion::ReplicaPublication { draft: Some(stage), .. }) = admitted.storage_regions.get(&id)
            else { return Err(refused()); };
        let attempt = stage.attempt.ok_or_else(refused)?;
        let reference = pointer.map(|pointer| transfer_pointer_digest(pointer, version)).transpose()?;
        let physical = ObjectKey::new(format!("{}/{key}", residence(&admitted)))?;
        let inventory = CellStorage::s3_inventory(config).await?;
        if !matches!(inventory.observe_create_attempt(&physical, Some(attempt)).await?,
            CreateAttemptPublication::Published(written) if &written == version)
        {
            return Err(refused());
        }
        crate::admin::update_installation(self.directory.as_ref(), &history.deployment.to_string(), |record| {
            require_publication(history, route, record, key)?;
            let Some(StorageRegion::ReplicaPublication { closed, publications, draft: Some(stage), .. }) =
                record.storage_regions.get_mut(&id) else { return Err(refused()); };
            if !publications.is_empty() || stage.route != route || stage.attempt != Some(attempt)
                || stage.retirement.is_some() || stage.version.as_ref().is_some_and(|prior| prior != version)
                || reference.is_some() && stage.reference.is_some_and(|prior| Some(prior) != reference)
            {
                return Err(refused());
            }
            stage.version = Some(version.clone());
            if reference.is_some() { stage.reference = reference; }
            *closed = true;
            Ok(())
        }).await?;
        Ok(())
    }

    pub(super) async fn admit_staged_append(&self, history: History, epoch: u64, frame: &[u8]) -> Result<()> {
        let reference = decode_reference(frame)?;
        if !matches!(self.storage, CellStorage::S3 { .. }) { return Ok(()); }
        let version = reference.publication().ok_or_else(refused)?;
        let hash = ring::digest::digest(&ring::digest::SHA256, frame);
        let digest: [u8; 32] = hash.as_ref().try_into().expect("SHA-256 width");
        crate::admin::update_installation(self.directory.as_ref(), &history.deployment.to_string(), |record| {
            require_writer(history, epoch, record)?;
            let stage = record.storage_regions.values_mut().find_map(|region| match region {
                StorageRegion::ReplicaPublication { key, closed: true, draft: Some(stage), .. }
                    if key == reference.object().as_str() => Some(stage),
                _ => None,
            }).ok_or_else(refused)?;
            if stage.route != (PublishRoute::Writer { epoch }) || stage.version.as_ref() != Some(version) || stage.body_sha256 != reference.body_sha256()
                || stage.retirement.is_some()
                || stage.reference.is_some_and(|prior| prior != digest)
            {
                return Err(refused());
            }
            stage.reference = Some(digest);
            Ok(())
        }).await?;
        Ok(())
    }

    pub(super) async fn observe_staged_append(&self, history: History, position: i64, frame: &[u8]) -> Result<()> {
        let reference = decode_reference(frame)?;
        if !matches!(self.storage, CellStorage::S3 { .. }) { return Ok(()); }
        let version = reference.publication().ok_or_else(refused)?;
        let record = self.current(history).await?;
        let hash = ring::digest::digest(&ring::digest::SHA256, frame);
        let Some((id, prior)) = record.storage_regions.iter().find(|(_, region)| {
            matches!(region, StorageRegion::ReplicaPublication { key, closed: true, draft: Some(stage), .. }
                if key == reference.object().as_str() && stage.version.as_ref() == Some(version)
                    && stage.reference.as_ref().is_some_and(|digest| digest.as_slice() == hash.as_ref())
                    && stage.retirement.is_none() && matches!(stage.route, PublishRoute::Writer { .. })
                    && position > stage.scanned_through)
        }) else { return Ok(()); };
        let stream = crate::admin::administration_stream(self.directory.as_ref(), &record, crate::process::MAXIMUM_LOG_MESSAGE_BYTES).await?;
        let proof = rtdb_logkeeper::certify_committed_range(&stream, position, position).await?;
        let mut reader = rtdb_logkeeper::CertifiedRangeReader::new(&stream, &proof)?;
        let page = reader.next_page().await?.ok_or_else(refused)?;
        if page.len() != 1 || page[0].lsn != position || !crate::logs::commits_payload_reference(&page[0].payload, frame)
            || reader.next_page().await?.is_some()
        {
            return Err(refused());
        }
        self.replace_stage(&record, *id, prior, None).await
    }

    pub(super) async fn reclaim_staged(
        &self, inventory: &rtdb_s3::VersionedInventory, record: &DeploymentRecord, id: Uuid, region: &StorageRegion,
    ) -> Result<()> {
        let StorageRegion::ReplicaPublication { key, custody, closed, publications, draft: Some(stage) } = region
            else { return Err(refused()); };
        if !*closed {
            let ReplayCustody::Hosted { owner: Some(owner) } = custody else { return Ok(()); };
            if !self.directory.hosted_task_stopped(owner).await? { return Ok(()); }
        }
        if !*closed || !publications.is_empty() {
            for publication in publications.values() {
                inventory.abort_publication(&ObjectKey::new(&publication.key)?, &publication.upload_id).await?;
            }
            let mut drained = region.clone();
            let StorageRegion::ReplicaPublication { closed, publications, .. } = &mut drained else { unreachable!(); };
            *closed = true;
            publications.clear();
            self.replace_stage(record, id, region, Some(drained)).await?;
            return Ok(());
        }

        let logical = ObjectKey::new(key)?;
        check_publish_key(&logical)?;
        let physical = ObjectKey::new(format!("{}/{}", residence(record), logical))?;
        if let Some(grant) = &stage.retirement {
            self.withdraw_body_key(record, &logical).await?;
            if let Some(version) = &grant.version {
                inventory.retire_observed_version(&physical, version).await?;
            }
            self.replace_stage(record, id, region, None).await?;
            return Ok(());
        }
        let audit = match (stage.route, stage.reference) {
            (PublishRoute::Writer { epoch }, _) => self.audit_staged_append(record, stage, epoch).await?,
            _ if transfer_excluded(record, stage.route) => {
                let position = stage.route.transfer_position().ok_or_else(refused)?;
                if record.requires_replica_offset(position, rtdb_directory::now_ms()) {
                    Audit::Pending
                } else {
                    Audit::Excluded
                }
            }
            (_, Some(digest)) => {
                let position = stage.route.transfer_position().ok_or_else(refused)?;
                if self.transfer_committed(record, stage.route, position, digest).await? {
                    Audit::Committed
                } else {
                    Audit::Pending
                }
            }
            _ => Audit::Pending,
        };
        match audit {
            Audit::Pending => Ok(()),
            Audit::Committed => self.replace_stage(record, id, region, None).await.map(drop),
            Audit::Through(through) => {
                let mut pending = region.clone();
                let StorageRegion::ReplicaPublication { draft: Some(stage), .. } = &mut pending else { unreachable!(); };
                stage.scanned_through = through;
                self.replace_stage(record, id, region, Some(pending)).await.map(drop)
            }
            Audit::Excluded => {
                let version = match &stage.version {
                    Some(version) => Some(version.clone()),
                    None => {
                        match inventory.observe_create_attempt(&physical, stage.attempt).await? {
                            CreateAttemptPublication::Published(version) => Some(version),
                            CreateAttemptPublication::Absent => None,
                            CreateAttemptPublication::Other => {
                                // A conditional upload that lost to an existing
                                // publication owns neither that body nor its slot.
                                return self.replace_stage(record, id, region, None).await;
                            }
                        }
                    }
                };
                if let Some(version) = &version
                    && (!inventory.owns_publication(&physical, version)
                        || !inventory.exact_retirement_available(&physical, version).await?)
                {
                    return Ok(());
                }
                let mut granted = region.clone();
                let StorageRegion::ReplicaPublication { draft: Some(stage), .. } = &mut granted else { unreachable!(); };
                stage.retirement = Some(ReplicaDraftRetirement { token: Uuid::new_v4(), version });
                self.replace_stage(record, id, region, Some(granted)).await.map(drop)
            }
        }
    }

    pub(super) async fn replace_stage(
        &self, record: &DeploymentRecord, id: Uuid, prior: &StorageRegion, next: Option<StorageRegion>,
    ) -> Result<()> {
        let history = History::of(record)?;
        let transfer_position = match &next {
            Some(StorageRegion::ReplicaPublication { draft: Some(draft), .. }) if draft.retirement.is_some() =>
                draft.route.transfer_position(),
            _ => None,
        };
        crate::admin::update_installation(self.directory.as_ref(), &record.id, |current| {
            if history.matches(current) && current.epoch == record.epoch
                && current.ensemble == record.ensemble && current.storage_regions.get(&id) == Some(prior)
                && current.lifecycle == record.lifecycle && current.move_from == record.move_from
                && current_generation_abandoned(current) == current_generation_abandoned(record)
                && transfer_position.is_none_or(|position| !current.requires_replica_offset(position, rtdb_directory::now_ms()))
            {
                match &next {
                    Some(stage) => { current.storage_regions.insert(id, stage.clone()); }
                    None => { current.storage_regions.remove(&id); }
                }
            }
            Ok(())
        }).await?;
        Ok(())
    }

    async fn audit_staged_append(&self, record: &DeploymentRecord, stage: &ReplicaPreparedPublication, epoch: u64) -> Result<Audit> {
        let replaced = record.epoch > epoch;
        let Some(reference) = stage.reference else {
            return Ok(if replaced { Audit::Excluded } else { Audit::Pending });
        };
        if record.epoch < epoch { return Err(refused()); }
        let stream = crate::admin::administration_stream(self.directory.as_ref(), record, crate::process::MAXIMUM_LOG_MESSAGE_BYTES).await?;
        let frontier = if record.holder.is_none() || record.is_resident_idle() {
            if !replaced { return Ok(Audit::Pending); }
            let (trim, through) = rtdb_logkeeper::seal_recover_bounds(&stream, record.epoch).await?;
            if trim.0 > stage.scanned_through { return Err(refused()); }
            through.0
        } else {
            let reader = rtdb_logkeeper::open_reader(stream.clone(), CancellationToken::new()).await?;
            let index = reader.read_index(record.epoch).await?;
            if index.decision().is_none() && index.handoff().is_none() { return Ok(Audit::Pending); }
            index.through().0
        };
        let Some([first, through]) = next_audit(stage.scanned_through, frontier)? else {
            return Ok(if replaced { Audit::Excluded } else { Audit::Pending });
        };
        let proof = rtdb_logkeeper::certify_committed_range(&stream, first, through).await?;
        let mut reader = rtdb_logkeeper::CertifiedRangeReader::new(&stream, &proof)?;
        let mut found = false;
        while let Some(page) = reader.next_page().await? {
            for entry in page {
                let digest = crate::logs::payload_reference_digest(&entry.payload).ok_or_else(refused)?;
                found |= digest == reference;
            }
        }
        Ok(audited_result(found, replaced, through, frontier))
    }

    pub(super) async fn withdraw_body_key(&self, record: &DeploymentRecord, logical: &ObjectKey) -> Result<()> {
        let io = IoRegion::new();
        let scope = UnitScope::replay_objects(self.namespace, &residence(record), payload_log_epoch(record));
        let ledger = UnitLedger::new(Arc::clone(&self.keys), scope, Arc::clone(&io));
        let withdrawn = ledger.withdraw(logical).await;
        io.close_and_wait().await;
        Ok(withdrawn?)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Audit {
    Pending,
    Committed,
    Through(i64),
    Excluded,
}

fn audited_result(found: bool, replaced: bool, scanned: i64, frontier: i64) -> Audit {
    if found { return Audit::Committed; }
    if replaced && scanned == frontier { return Audit::Excluded; }
    Audit::Through(scanned)
}

fn next_audit(scanned: i64, frontier: i64) -> Result<Option<[i64; 2]>> {
    if !(-1..i64::MAX).contains(&scanned) || frontier < scanned || frontier == i64::MAX {
        return Err(refused());
    }
    if scanned == frontier { return Ok(None); }
    Ok(Some([scanned + 1, frontier.min(scanned.saturating_add(AUDIT_WIDTH))]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_committed_reference_wins_and_absence_requires_the_fenced_end() {
        assert_eq!(audited_result(true, true, 64, 64), Audit::Committed);
        assert_eq!(audited_result(false, false, 64, 64), Audit::Through(64));
        assert_eq!(audited_result(false, true, 63, 64), Audit::Through(63));
        assert_eq!(audited_result(false, true, 64, 64), Audit::Excluded);
    }

    #[test]
    fn an_empty_certified_window_requires_an_exact_frontier() {
        assert_eq!(next_audit(-1, -1).unwrap(), None);
        assert_eq!(next_audit(40, 41).unwrap(), Some([41, 41]));
        assert_eq!(next_audit(40, 400).unwrap(), Some([41, 104]));
        assert!(next_audit(41, 40).is_err());
    }
}
