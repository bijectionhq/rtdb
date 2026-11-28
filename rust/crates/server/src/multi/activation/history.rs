use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchivedControlRecord {
    position: Lsn,
    record: String,
}

impl ActivationAuthority {
    fn history_stream(&self, name: &str, predecessor: Binding) -> Result<StreamId> {
        let activation = self.stream_id(name)?;
        let mut digest = Sha256::new();
        digest.update(b"rtdb.activation.publication-history/v1\0");
        digest.update(activation.0.to_be_bytes());
        digest.update(predecessor.incarnation.as_bytes());
        digest.update(predecessor.revision.to_be_bytes());
        Ok(StreamId(u128::from_be_bytes(digest.finalize()[..16].try_into().unwrap())))
    }

    pub(super) async fn preserve_publication(&self, name: &str, state: &State, position: Lsn, record: Bytes) -> Result<()> {
        let Some(publication) = state.publication.as_ref().filter(|publication| publication.recovery.is_some()) else {
            return Ok(());
        };
        let stream = self.history_stream(name, publication.predecessor)?;
        let local = self.access.is_local();
        if local {
            self.journal.create(stream).await
        } else {
            self.journal.create_immutable(stream).await
        }.map_err(journal_error)?;
        let bytes = if publication.segment.is_some() {
            let record = std::str::from_utf8(&record).map_err(|_| corrupt())?.to_owned();
            serde_json::to_vec(&ArchivedControlRecord { position, record })
        } else {
            serde_json::to_vec(publication)
        }.map(Bytes::from).map_err(|_| corrupt())?;
        let mut epoch = Epoch(u64::from(local));
        for _ in 0..ATTEMPTS {
            if let Some((prior, _)) = self.read_publication(stream).await? {
                return if &prior == publication { Ok(()) } else { Err(corrupt()) };
            }
            let fence = match self.seal_fence(stream, epoch).await {
                Ok((Lsn(0), fence)) => fence,
                Ok(_) => continue,
                Err(JournalError::Fenced { current }) => {
                    epoch = Epoch(current.0.checked_add(u64::from(local)).ok_or_else(unavailable)?);
                    continue;
                }
                Err(error) => return Err(journal_error(error)),
            };
            let appended = match fence {
                Fence::Local(epoch) => self.journal.append(stream, epoch, Lsn(1), bytes.clone()).await,
                Fence::Immutable(fence) => self.journal.append_session_batch(fence, Lsn(1), vec![bytes.clone()]).await,
            };
            match appended {
                Ok(Lsn(1)) => return Ok(()),
                Ok(_) => return Err(corrupt()),
                Err(JournalError::Conflict { .. } | JournalError::Fenced { .. }) => continue,
                Err(error) => return Err(journal_error(error)),
            }
        }
        Err(unavailable())
    }

    pub(super) async fn archived_publication(&self, name: &str, predecessor: Binding) -> Result<Option<Publication>> {
        let stream = self.history_stream(name, predecessor)?;
        let found = self.read_publication(stream).await?.map(|(publication, _)| publication);
        if found.as_ref().is_some_and(|publication| publication.predecessor != predecessor || publication.recovery.is_none()) {
            return Err(corrupt());
        }
        Ok(found)
    }

    pub(crate) async fn lineage_publication_proof(&self, name: &str, predecessor: Binding) -> Result<rtdb_lineage::PublicationProof> {
        let journal = self.authenticated_journal.as_ref().ok_or_else(unavailable)?;
        let stream = self.stream_id(name)?;
        let (position, observed) = self.read_record(stream).await?;
        let (state, record) = observed.ok_or_else(unavailable)?;
        let history = self.history_stream(name, predecessor)?;
        let (position, record) = if let Some((publication, archived)) = self.read_publication(history).await? {
            if publication.predecessor != predecessor || publication.segment.is_none() { return Err(conflict()); }
            let archived = archived.ok_or_else(corrupt)?;
            (archived.position, archived.record.into_bytes())
        } else if state.publication.as_ref().is_some_and(|publication| {
            publication.predecessor == predecessor && publication.segment.is_some()
        }) {
            (position, record.to_vec())
        } else {
            return Err(unavailable());
        };
        let inclusion = journal.inclusion(stream, position, Some(position.0)).await.map_err(journal_error)?;
        let leaf = inclusion.verify(inclusion.certification.history.genesis.root()).map_err(|_| corrupt())?;
        if leaf.at != position || leaf.stream != stream || leaf.payload_sha256 != <[u8; 32]>::from(Sha256::digest(&record)) {
            return Err(corrupt());
        }
        Ok(rtdb_lineage::PublicationProof { control_record: record, control_inclusion: inclusion })
    }

    /// Publishes the serving incarnation's authenticated prefix through
    /// `through` as its lineage floor and returns the control proof an issuer
    /// settles against. Every later cut keeps the floor, so `through` is a
    /// prefix the operator can restore, such as a retained backup's cut.
    pub(crate) async fn publish_lineage_floor(
        &self,
        name: &str,
        expected: Binding,
        application: &rtdb_stream_journal::ConsensusJournal,
        through: Lsn,
    ) -> Result<rtdb_lineage::PublicationProof> {
        self.access.require_operator()?;
        let control = self.authenticated_journal.as_ref().ok_or_else(unavailable)?;
        let stream = self.stream_id(name)?;
        let (head, certification) = application
            .authenticated_head(StreamId(expected.incarnation.as_u128()), Some(through.0))
            .await
            .map_err(journal_error)?;
        let floor = rtdb_lineage::LineageFloor {
            binding: expected,
            source: rtdb_db::AuthenticatedCut { head: head.head, catalog_root: certification.history.genesis.root() },
        };
        if !floor.valid() {
            return Err(conflict());
        }
        self.change(stream, |state| {
            let state = state.ok_or_else(unavailable)?;
            state.require(expected)?;
            if !state.uses_lineage() || state.recovery.is_some() {
                return Err(conflict());
            }
            if state.floor.as_ref().is_some_and(|prior| prior.end() >= floor.end()) {
                return Ok(None);
            }
            let mut next = state.clone();
            next.floor = Some(floor.clone());
            Ok(Some(next))
        })
        .await?;
        let (position, observed) = self.read_record(stream).await?;
        let (state, record) = observed.ok_or_else(unavailable)?;
        if state.floor.as_ref().is_none_or(|published| published.binding != expected || published.end() < floor.end()) {
            return Err(unavailable());
        }
        let inclusion = control.inclusion(stream, position, Some(position.0)).await.map_err(journal_error)?;
        Ok(rtdb_lineage::PublicationProof { control_record: record.to_vec(), control_inclusion: inclusion })
    }

    async fn read_publication(&self, stream: StreamId) -> Result<Option<(Publication, Option<ArchivedControlRecord>)>> {
        let position = Lsn(1);
        if !self.access.is_local() {
            match self.journal.require_immutable(stream).await {
                Ok(()) => (),
                Err(JournalError::NotFound) => return Ok(None),
                Err(error) => return Err(journal_error(error)),
            }
        }
        match self.journal.head(stream).await {
            Ok(Lsn(0)) | Err(JournalError::NotFound) => return Ok(None),
            Ok(Lsn(1)) => (),
            Ok(_) => return Err(corrupt()),
            Err(error) => return Err(journal_error(error)),
        }
        let page = if self.access.is_local() {
            self.journal.read(stream, position, 1, Duration::ZERO).await
        } else {
            self.journal.read_page(stream, position, position, 1, Duration::ZERO).await
        }.map_err(journal_error)?;
        match page.records.as_slice() {
            [(at, bytes)] if *at == position && page.head == position => {
                if let Ok(archived) = serde_json::from_slice::<ArchivedControlRecord>(bytes) {
                    let state = serde_json::from_str::<State>(&archived.record).map_err(|_| corrupt())?;
                    let lineage = state.uses_lineage();
                    let publication = state.publication.filter(|publication| publication.segment.is_some()).ok_or_else(corrupt)?;
                    if archived.position.0 == 0 || !lineage { return Err(corrupt()); }
                    Ok(Some((publication, Some(archived))))
                } else {
                    let publication: Publication = serde_json::from_slice(bytes).map_err(|_| corrupt())?;
                    if publication.segment.is_some() { return Err(corrupt()); }
                    Ok(Some((publication, None)))
                }
            }
            _ => Err(corrupt()),
        }
    }
}
