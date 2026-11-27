use super::*;

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

    pub(super) async fn preserve_publication(&self, name: &str, state: &State) -> Result<()> {
        // Copy the decision already committed in the authority's previous binding before replacing it.
        let Some(publication) = state.publication.filter(|publication| publication.recovery.is_some()) else {
            return Ok(());
        };
        let stream = self.history_stream(name, publication.predecessor)?;
        let local = self.access.is_local();
        if local {
            self.journal.create(stream).await
        } else {
            self.journal.create_immutable(stream).await
        }.map_err(journal_error)?;
        let bytes = Bytes::from(serde_json::to_vec(&publication).map_err(|_| corrupt())?);
        let mut epoch = Epoch(u64::from(local));
        for _ in 0..ATTEMPTS {
            if let Some(prior) = self.read_publication(stream).await? {
                return if prior == publication { Ok(()) } else { Err(corrupt()) };
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
        let found = self.read_publication(stream).await?;
        if found.is_some_and(|publication| publication.predecessor != predecessor || publication.recovery.is_none()) {
            return Err(corrupt());
        }
        Ok(found)
    }

    async fn read_publication(&self, stream: StreamId) -> Result<Option<Publication>> {
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
            [(at, bytes)] if *at == position && page.head == position =>
                serde_json::from_slice(bytes).map(Some).map_err(|_| corrupt()),
            _ => Err(corrupt()),
        }
    }
}
