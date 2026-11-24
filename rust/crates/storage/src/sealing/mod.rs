//! Sealed immutable objects with provider-owned data key slots.

pub mod cipher;
mod classic;
mod delivery;
mod publication;
pub use publication::{PublicationSealing, PUBLICATION_ID_BYTES};
pub use delivery::{DeliveryDeadline, DeliveryIo, current_delivery_deadline, PlaintextDelivery, PlaintextDependencies, PlaintextScopes, PlaintextView, drain_unit_delivery};
pub(crate) use classic::LOG_SEAL_OVERHEAD;
pub use classic::{ClassicKeyVault, ClassicObjectKeys, KAFKA_LOG_SEAL_OVERHEAD};

use std::{collections::BTreeMap, ops::Range, path::Path, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use rtdb_format::trie_retirement::MAXIMUM_RECEIPT_BYTES;
use rtdb_types::{Anomaly, Result};

use crate::{
    Error, ObjectKey, ObjectObservation, ObjectReadAllocation, ObjectStore, ReadReservation,
    RetainedBytes, StoredObject, object_store::IndexPageLifecycle,
};
use cipher::{
    Cipher,
    object::{self, FramedObject},
};

#[async_trait]
pub trait ObjectCipher: Send + Sync {
    async fn operate(
        &self,
        name: &ObjectKey,
        operation: UnitOperation,
    ) -> crate::Result<UnitOutput>;

    /// An enclosing provider lease can shorten a unit delivery grant.
    fn delivery_deadline(&self) -> Option<std::time::Instant> { None }

    async fn withdraw(&self, _name: &ObjectKey) -> crate::Result<()> {
        Err(Error::Closed(
            "The object cipher has no Classic withdrawal authority".into(),
        ))
    }

    async fn withdraw_all(&self, names: &[ObjectKey]) -> crate::Result<()> {
        for name in names { self.withdraw(name).await?; }
        Ok(())
    }
}

pub enum UnitOperation {
    Seal(Bytes),
    Open(Bytes),
    OpenRetained(RetainedBytes),
    Header(Bytes),
    Frames(FramedObject, Range<usize>, Bytes),
    SealValue(String, Bytes),
    OpenValue(String, Bytes),
    AtLogicalName(ObjectKey, Box<UnitOperation>),
    Grant,
}

impl UnitOperation {
    pub fn provisions(&self) -> bool {
        match self {
            Self::Seal(_) | Self::SealValue(..) => true,
            Self::AtLogicalName(_, operation) => operation.provisions(),
            _ => false,
        }
    }

    pub fn apply(self, key: &Cipher, name: &ObjectKey) -> crate::Result<UnitOutput> {
        match self {
            Self::Grant => Ok(UnitOutput::Granted),
            Self::Seal(body) => key.seal_object(name, &body).map(UnitOutput::Bytes),
            Self::Open(sealed) => key.open_object(name, &sealed).map(UnitOutput::Bytes),
            Self::OpenRetained(sealed) => key
                .open_object(name, sealed.as_slice())
                .map(UnitOutput::Bytes),
            Self::Header(sealed) => {
                FramedObject::open_header(key, name, &sealed).map(UnitOutput::Header)
            }
            Self::Frames(layout, frames, sealed) => layout
                .open_frames(key, name, frames, &sealed)
                .map(UnitOutput::Bytes),
            Self::SealValue(associated, body) => key
                .seal_deterministic(&associated, &body)
                .map(UnitOutput::Bytes),
            Self::OpenValue(associated, sealed) => {
                key.open(&associated, &sealed).map(UnitOutput::Bytes)
            }
            Self::AtLogicalName(logical, operation) => operation.apply(key, &logical),
        }
    }
}

pub enum UnitOutput {
    Bytes(Bytes),
    Header(FramedObject),
    Granted,
    HeldGrant { until: Option<std::time::Instant>, custody: Arc<dyn Send + Sync> },
}

impl UnitOutput {
    pub fn bytes(self) -> crate::Result<Bytes> {
        match self {
            Self::Bytes(bytes) => Ok(bytes),
            _ => Err(Error::Task("Expected unit bytes".into())),
        }
    }

    fn granted(self) -> crate::Result<(Option<std::time::Instant>, Option<Arc<dyn Send + Sync>>)> {
        match self {
            Self::Granted => Ok((None, None)),
            Self::HeldGrant { until, custody } => Ok((until, Some(custody))),
            _ => Err(Error::Task("Expected a key owner’s delivery grant".into())),
        }
    }

    pub fn header(self) -> crate::Result<FramedObject> {
        match self {
            Self::Header(header) => Ok(header),
            _ => Err(Error::Task("Expected a sealed unit header".into())),
        }
    }
}

pub struct EncryptingStore {
    inner: Arc<dyn ObjectStore>,
    keys: ObjectKeys,
    published: Option<PublicationSealing>,
    classic_control: Option<ObjectKey>,
}

enum ObjectKeys {
    Deployment { cipher: Cipher, delivery: Option<Arc<dyn ObjectCipher>> },
    Units(Arc<dyn ObjectCipher>),
}

impl EncryptingStore {
    pub fn new(inner: Arc<dyn ObjectStore>, key: &[u8]) -> Result<Self> {
        Ok(Self {
            inner,
            keys: ObjectKeys::Deployment { cipher: Cipher::new(key)?, delivery: None },
            published: None,
            classic_control: None,
        })
    }

    /// Opens a deployment-key store whose custody owner can confirm answer delivery.
    pub fn with_deployment_delivery(
        inner: Arc<dyn ObjectStore>, key: &[u8], authority: Arc<dyn ObjectCipher>,
    ) -> Result<Self> {
        Ok(Self {
            inner,
            keys: ObjectKeys::Deployment { cipher: Cipher::new(key)?, delivery: Some(authority) },
            published: None,
            classic_control: None,
        })
    }

    pub fn with_units(inner: Arc<dyn ObjectStore>, ledger: Arc<dyn ObjectCipher>) -> Self {
        Self {
            inner,
            keys: ObjectKeys::Units(ledger),
            published: None,
            classic_control: None,
        }
    }

    pub fn with_publications(mut self, published: PublicationSealing) -> Self {
        self.published = Some(published);
        self
    }

    pub fn published(inner: Arc<dyn ObjectStore>, sealing: PublicationSealing) -> Self {
        Self::with_units(inner, sealing.ledger()).with_publications(sealing)
    }

    fn publication(&self, key: &ObjectKey) -> Option<&PublicationSealing> {
        self.published.as_ref().filter(|scope| scope.applies(key))
    }

    fn sealed_size(&self, key: &ObjectKey, length: usize) -> crate::Result<usize> {
        object::sealed_size(key, length)?.checked_add(self.publication(key).map_or(0, |_| PUBLICATION_ID_BYTES))
            .ok_or_else(|| Error::Task("Sealed publication length overflow".into()))
    }

    fn plain(&self, stored: StoredObject) -> crate::Result<StoredObject> {
        let overhead = self.publication(&stored.key).map_or(0, |_| PUBLICATION_ID_BYTES as u64);
        let body = stored.size.checked_sub(overhead).ok_or_else(|| Error::ConflictingObject(stored.key.to_string()))?;
        Ok(StoredObject { size: object::plaintext_size(&stored.key, body)?, ..stored })
    }
    fn with_classic_control(mut self, marker: ObjectKey) -> Self {
        self.classic_control = Some(marker);
        self
    }

    async fn crypt(&self, name: &ObjectKey, operation: UnitOperation) -> crate::Result<UnitOutput> {
        match &self.keys {
            ObjectKeys::Deployment { cipher, delivery } => {
                let reads_plaintext = !operation.provisions();
                let output = operation.apply(cipher, name)?;
                if reads_plaintext && let Some(authority) = delivery {
                    PlaintextView::track(authority, name)?;
                }
                Ok(output)
            }
            ObjectKeys::Units(ledger) => {
                let reads_plaintext = !operation.provisions();
                let output = ledger.operate(name, operation).await?;
                if reads_plaintext { PlaintextView::track(ledger, name)?; }
                Ok(output)
            }
        }
    }

    async fn seal(&self, key: &ObjectKey, plaintext: Bytes) -> crate::Result<Bytes> {
        if let Some(publication) = self.publication(key) {
            return publication.seal(key, plaintext).await;
        }
        self.crypt(key, UnitOperation::Seal(plaintext))
            .await?
            .bytes()
    }

    async fn open(&self, key: &ObjectKey, sealed: Bytes) -> crate::Result<Bytes> {
        if let Some(publication) = self.publication(key) {
            return publication.open(key, sealed).await;
        }
        self.crypt(key, UnitOperation::Open(sealed)).await?.bytes()
    }

    async fn open_retained(&self, key: &ObjectKey, sealed: RetainedBytes) -> crate::Result<Bytes> {
        if let Some(publication) = self.publication(key) {
            return publication.open_retained(key, sealed).await;
        }
        self.crypt(key, UnitOperation::OpenRetained(sealed)).await?.bytes()
    }

    async fn seal_missing(&self, key: &ObjectKey, bytes: Bytes) -> crate::Result<Option<Bytes>> {
        match self.get_fresh(key).await {
            Ok(existing) if existing == bytes => Ok(None),
            Ok(_) => Err(Error::ConflictingObject(key.to_string())),
            Err(Error::MissingObject(_)) => Ok(Some(self.seal(key, bytes).await?)),
            Err(error) => Err(error),
        }
    }

    async fn read_frames(
        &self,
        key: &ObjectKey,
        range: Range<u64>,
    ) -> crate::Result<ObjectObservation<Bytes>> {
        use object::{FRAME_BYTES, SEALED_HEADER_BYTES};

        let head = self.inner.observe_head(key).await?;
        let publication = self.publication(key);
        let prefix = publication.map_or(0, |_| PUBLICATION_ID_BYTES as u64);
        let header = self
            .inner
            .observe_range(key, 0..prefix + SEALED_HEADER_BYTES as u64)
            .await?;
        let (layout, origin) = match publication {
            Some(publication) => {
                let (layout, origin) = publication.header(key, header.value).await?;
                (layout, Some((publication, origin)))
            }
            None => (self.crypt(key, UnitOperation::Header(header.value)).await?.header()?, None),
        };
        if header.version != head.version
            || head.value.size != self.sealed_size(key, layout.length())? as u64
        {
            return Err(Error::ConflictingObject(key.to_string()));
        }
        let frames = layout.covering_frames(key, &range)?;
        if frames.is_empty() {
            return Ok(ObjectObservation {
                value: Bytes::new(),
                version: header.version,
            });
        }
        let physical = layout.physical_range(&frames);
        let sealed = self
            .inner
            .observe_range(key, prefix + physical.start..prefix + physical.end)
            .await?;
        if sealed.version != header.version {
            return Err(Error::ConflictingObject(key.to_string()));
        }
        let operation = UnitOperation::Frames(layout, frames, sealed.value);
        let plaintext = match origin {
            Some((publication, origin)) => publication.operate(key, origin, operation).await?,
            None => self.crypt(key, operation).await?,
        }.bytes()?;
        let start = range.start as usize % FRAME_BYTES;
        let end = start + (range.end - range.start) as usize;
        let bytes = plaintext
            .get(start..end)
            .ok_or_else(|| Error::ConflictingObject(key.to_string()))?;
        Ok(ObjectObservation {
            value: Bytes::copy_from_slice(bytes),
            version: sealed.version,
        })
    }
}

#[async_trait]
impl ObjectStore for EncryptingStore {
    fn s3_publication_address(&self, key: &ObjectKey) -> Option<(rtdb_types::S3ObjectNamespace, String)> {
        self.inner.s3_publication_address(key)
    }
    fn local_maintenance_store(&self) -> Option<&crate::LocalStore> {
        if self.classic_control.is_some() {
            self.inner.local_maintenance_store()
        } else {
            None
        }
    }

    fn syncs_checkpoints(&self) -> bool {
        self.classic_control.is_some() && self.inner.syncs_checkpoints()
    }

    fn index_page_lifecycle(&self) -> Option<&dyn IndexPageLifecycle> {
        self.inner.index_page_lifecycle()
    }
    async fn retain_catalog(
        &self,
        cut: Option<rtdb_types::CatalogCut>,
    ) -> Result<Option<Arc<dyn Send + Sync>>> {
        self.inner.retain_catalog(cut).await
    }
    async fn retain_catalog_with_paths(
        &self,
        cut: Option<rtdb_types::CatalogCut>,
        paths: Arc<[String]>,
    ) -> rtdb_types::Result<Option<Arc<dyn Send + Sync>>> {
        self.inner.retain_catalog_with_paths(cut, paths).await
    }
    fn defers_trie_releases(&self) -> bool {
        self.inner.defers_trie_releases()
    }
    fn releases_receipt_l0(&self) -> bool {
        self.inner.releases_receipt_l0()
    }
    async fn advance_catalog_floor(&self, installed: rtdb_types::CatalogCut) -> Result<bool> {
        self.inner.advance_catalog_floor(installed).await
    }
    async fn release_trie_files(&self, receipt: &ObjectKey, contents: Bytes) -> Result<bool> {
        if contents.len() > MAXIMUM_RECEIPT_BYTES
            || self.get_fresh_exact(receipt, contents.len()).await? != contents
        {
            return Err(Anomaly::incorrect(
                "rtdb.storage/trie-release-receipt",
                "The trie release does not match its stored installation receipt",
            ));
        }
        if self.classic_control.is_some()
            && let ObjectKeys::Units(ledger) = &self.keys
        {
            return crate::classic_retirement::retire_with_keys(
                self,
                receipt,
                contents,
                Some(ledger.as_ref()),
            )
            .await;
        }
        self.inner.release_trie_files(receipt, contents).await
    }

    async fn close_classic_trie_admission(&self, cut: rtdb_types::CatalogCut, paths: &[Option<&str>]) -> Result<Vec<bool>> {
        self.inner.close_classic_trie_admission(cut, paths).await
    }

    async fn classic_trie_retirement_ready(&self, key: &ObjectKey, version: Option<&rtdb_types::PhysicalObjectVersion>) -> Result<bool> {
        self.inner.classic_trie_retirement_ready(key, version).await
    }

    async fn settle_classic_trie(&self, key: &ObjectKey, version: Option<&rtdb_types::PhysicalObjectVersion>) -> Result<()> {
        if self.classic_control.as_ref() == Some(key) {
            return Err(Error::InvalidKey(key.to_string()).into());
        }
        self.inner.settle_classic_trie(key, version).await
    }

    async fn search_staging_owner(&self) -> Result<Option<rtdb_format::search_manifest::SearchStagingOwner>> {
        self.inner.search_staging_owner().await
    }
    async fn search_staging_drained(&self, owner: rtdb_format::search_manifest::SearchStagingOwner) -> rtdb_types::Result<bool> {
        self.inner.search_staging_drained(owner).await
    }
    fn releases_exact_search_runs(&self) -> bool { self.inner.releases_exact_search_runs() }
    async fn release_search_run(
        &self,
        key: &ObjectKey,
        publication: Option<&rtdb_format::log::proto::PublishedObjectVersion>,
        cut: Option<rtdb_types::CatalogCut>,
    ) -> Result<bool> {
        if rtdb_format::search_manifest::is_ranged_run(key.as_str()) && self.local_maintenance_store().is_some()
            && let ObjectKeys::Units(ledger) = &self.keys {
            return crate::classic_retirement::retire_search_run(self, key, cut, Some(ledger.as_ref())).await;
        }
        self.inner.release_search_run(key, publication, cut).await
    }
    async fn head(&self, key: &ObjectKey) -> crate::Result<StoredObject> {
        self.inner.head(key).await.and_then(|stored| self.plain(stored))
    }
    async fn head_fresh(&self, key: &ObjectKey) -> crate::Result<StoredObject> {
        self.inner.head_fresh(key).await.and_then(|stored| self.plain(stored))
    }
    async fn observe_head(
        &self,
        key: &ObjectKey,
    ) -> crate::Result<ObjectObservation<StoredObject>> {
        let observed = self.inner.observe_head(key).await?;
        Ok(ObjectObservation {
            value: self.plain(observed.value)?,
            version: observed.version,
        })
    }
    async fn observe_heads(
        &self,
        keys: &[ObjectKey],
    ) -> crate::Result<BTreeMap<ObjectKey, ObjectObservation<StoredObject>>> {
        let mut observed = self.inner.observe_heads(keys).await?;
        for head in observed.values_mut() {
            head.value.size = self.plain(head.value.clone())?.size;
        }
        Ok(observed)
    }
    async fn retirement_version(
        &self,
        key: &ObjectKey,
    ) -> crate::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        self.inner.retirement_version(key).await
    }
    async fn retirement_publication(
        &self,
        key: &ObjectKey,
    ) -> crate::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        self.inner.retirement_publication(key).await
    }
    async fn get(&self, key: &ObjectKey) -> crate::Result<Bytes> {
        self.open(key, self.inner.get(key).await?).await
    }
    async fn get_fresh(&self, key: &ObjectKey) -> crate::Result<Bytes> {
        self.open(key, self.inner.get_fresh(key).await?).await
    }
    fn whole_object_allocation(
        &self,
        key: &ObjectKey,
        length: usize,
    ) -> crate::Result<ObjectReadAllocation> {
        let sealed = self.sealed_size(key, length)?;
        let source = self.inner.whole_object_allocation(key, sealed)?;
        let scratch_bytes = source
            .retained_bytes
            .checked_add(source.scratch_bytes)
            .ok_or_else(|| Error::Task("Encrypted object allocation size overflow".into()))?;
        Ok(ObjectReadAllocation {
            retained_bytes: length,
            scratch_bytes,
        })
    }
    async fn get_reserved_object(
        &self,
        key: &ObjectKey,
        mut reservation: ReadReservation,
    ) -> crate::Result<RetainedBytes> {
        if !reservation.covers(&ObjectReadAllocation {
            retained_bytes: reservation.length(),
            scratch_bytes: 0,
        }) {
            return Err(Error::Closed(
                "Encrypted object read has no retained reservation".into(),
            ));
        }
        let sealed_length = self.sealed_size(key, reservation.length())?;
        let allocation = self.inner.whole_object_allocation(key, sealed_length)?;
        let source = reservation.take_scratch_read(sealed_length, allocation)?;
        let sealed = reservation
            .retaining_io(self.inner.get_reserved_object(key, source))
            .await?;
        let plaintext = reservation
            .retaining_io(self.open_retained(key, sealed))
            .await?;
        reservation.finish(plaintext)
    }
    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> crate::Result<Bytes> {
        let exact = self.sealed_size(key, length)?;
        self.open(key, self.inner.get_fresh_exact(key, exact).await?)
            .await
    }
    fn exact_object_allocation(
        &self,
        key: &ObjectKey,
        length: usize,
    ) -> crate::Result<ObjectReadAllocation> {
        let sealed = self.sealed_size(key, length)?;
        let scratch_bytes = sealed
            .checked_mul(3)
            .ok_or_else(|| Error::Task("Encrypted object allocation size overflow".into()))?;
        Ok(ObjectReadAllocation {
            retained_bytes: sealed,
            scratch_bytes,
        })
    }
    async fn observe_fresh_exact(
        &self,
        key: &ObjectKey,
        length: usize,
    ) -> crate::Result<ObjectObservation<Bytes>> {
        let exact = self.sealed_size(key, length)?;
        let observed = self.inner.observe_fresh_exact(key, exact).await?;
        Ok(ObjectObservation {
            value: self.open(key, observed.value).await?,
            version: observed.version,
        })
    }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> crate::Result<Bytes> {
        if object::framed(key) {
            return Ok(self.observe_range(key, range).await?.value);
        }
        let plaintext = self.get(key).await?;
        let length = plaintext.len() as u64;
        if range.start > range.end || range.end > length {
            return Err(Error::InvalidRange {
                key: key.to_string(),
                start: range.start,
                end: range.end,
                length,
            });
        }
        Ok(plaintext.slice(range.start as usize..range.end as usize))
    }
    async fn observe_range(
        &self,
        key: &ObjectKey,
        range: Range<u64>,
    ) -> crate::Result<ObjectObservation<Bytes>> {
        if object::framed(key) {
            if !self.supports_scan_ranges(key) {
                return Err(Error::Closed(
                    "The encrypted object has no authenticated ranges".into(),
                ));
            }
            return self.read_frames(key, range).await;
        }
        let length = self.head_fresh(key).await?.size;
        if range.start > range.end || range.end > length {
            return Err(Error::InvalidRange {
                key: key.to_string(),
                start: range.start,
                end: range.end,
                length,
            });
        }
        let exact = usize::try_from(length)
            .map_err(|_| Error::Task("Encrypted object length overflow".into()))?;
        let observed = self.observe_fresh_exact(key, exact).await?;
        Ok(ObjectObservation {
            value: observed
                .value
                .slice(range.start as usize..range.end as usize),
            version: observed.version,
        })
    }
    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool {
        object::framed(key) && self.inner.supports_observed_scan_ranges(key)
    }
    async fn get_reserved_range(
        &self,
        key: &ObjectKey,
        range: Range<u64>,
        reservation: ReadReservation,
    ) -> crate::Result<RetainedBytes> {
        if object::framed(key) {
            return self.get_reserved_scan_range(key, range, reservation).await;
        }
        reservation.check_range(&range)?;
        let bytes = reservation.retaining_io(self.get_range(key, range)).await?;
        reservation.finish(bytes)
    }
    fn scan_range_allocation(
        &self,
        key: &ObjectKey,
        length: usize,
    ) -> crate::Result<ObjectReadAllocation> {
        let overflow = || Error::Task("Encrypted range allocation size overflow".into());
        if !object::framed(key) {
            return Ok(ObjectReadAllocation {
                retained_bytes: length,
                scratch_bytes: 0,
            });
        }
        // Round both edges outwards and allow coexisting provider, decoded and
        // resealed archive buffers. The retained value owns just the requested bytes.
        let expanded = length
            .checked_add(2 * (object::FRAME_BYTES - 1))
            .ok_or_else(overflow)?;
        let scratch_bytes = self.sealed_size(key, expanded)?
            .checked_mul(4)
            .ok_or_else(overflow)?;
        Ok(ObjectReadAllocation {
            retained_bytes: length,
            scratch_bytes,
        })
    }
    async fn get_reserved_scan_range(
        &self,
        key: &ObjectKey,
        range: Range<u64>,
        reservation: ReadReservation,
    ) -> crate::Result<RetainedBytes> {
        reservation.check_range(&range)?;
        if !self.supports_scan_ranges(key)
            || !reservation.covers(&self.scan_range_allocation(key, reservation.length())?)
        {
            return Err(Error::Closed(
                "Encrypted scan range has no scratch reservation".into(),
            ));
        }
        let plaintext = reservation.retaining_io(self.get_range(key, range)).await?;
        reservation.finish(plaintext)
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> crate::Result<()> {
        let sealed = match self.seal_missing(key, bytes.clone()).await? {
            Some(sealed) => sealed,
            None => return Ok(()),
        };
        match self.inner.put(key, sealed).await {
            Err(Error::ConflictingObject(_)) if self.get_fresh(key).await? == bytes => Ok(()),
            written => written,
        }
    }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> crate::Result<()> {
        let bytes = tokio::fs::read(source)
            .await
            .map_err(|error| Error::Task(format!("cannot read {}: {error}", source.display())))?;
        self.put(key, Bytes::from(bytes)).await
    }
    async fn put_with_publication(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> crate::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        let sealed = if matches!(&self.keys, ObjectKeys::Units(_)) {
            match self.seal_missing(key, bytes.clone()).await? {
                Some(sealed) => sealed,
                None => return self.inner.retirement_publication(key).await,
            }
        } else {
            self.seal(key, bytes.clone()).await?
        };
        match self.inner.put_with_publication(key, sealed).await {
            Err(Error::ConflictingObject(_)) if self.get_fresh(key).await? == bytes => {
                self.inner.retirement_publication(key).await
            }
            written => written,
        }
    }
    async fn put_file_with_publication(
        &self,
        key: &ObjectKey,
        source: &Path,
    ) -> crate::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        let bytes = tokio::fs::read(source)
            .await
            .map_err(|error| Error::Task(format!("cannot read {}: {error}", source.display())))?;
        self.put_with_publication(key, Bytes::from(bytes)).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> crate::Result<Vec<StoredObject>> {
        self.inner
            .list(directory)
            .await?
            .into_iter()
            .filter(|object| self.classic_control.as_ref() != Some(&object.key))
            .map(|stored| self.plain(stored))
            .collect()
    }
    async fn delete(&self, key: &ObjectKey) -> crate::Result<()> {
        if self.classic_control.as_ref() == Some(key) {
            return Err(Error::InvalidKey(key.to_string()));
        }
        self.inner.delete(key).await
    }
    async fn forget(&self, key: &ObjectKey) -> crate::Result<()> {
        self.inner.forget(key).await
    }
}
