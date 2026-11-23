use std::sync::Arc;

use bytes::Bytes;
use rtdb_format::cow_tree::{PageDigest, PageStore, page_key};
use rtdb_types::{Anomaly, Result};

use crate::{ObjectKey, ObjectStore, cache::BufferPool};

pub struct ObjectPageStore {
    pub store: Arc<dyn ObjectStore>,
    pub storage: Arc<BufferPool>,
    pub prefix: String,
}

#[async_trait::async_trait]
impl PageStore for ObjectPageStore {
    async fn get(&self, digest: PageDigest, length: u64) -> Result<Bytes> {
        crate::audience_work::decode_bytes(
            usize::try_from(length).map_err(|error| {
                Anomaly::fault("rtdb.storage/cow-page-length", error.to_string())
            })?,
        )?;
        let key = ObjectKey::new(page_key(&self.prefix, digest))?;
        let lease = if self.store.supports_scan_ranges(&key) {
            if self.storage.scan_length(&key).await? != length {
                return Err(Anomaly::fault(
                    "rtdb.storage/cow-page-length",
                    "Directory page length differs from its reference",
                ));
            }
            self.storage.get_scan_range(&key, 0..length).await?
        } else {
            self.storage.get(&key).await?
        };
        Ok(Bytes::from_owner(lease))
    }

    async fn put(&self, digest: PageDigest, bytes: Bytes) -> Result<()> {
        let key = ObjectKey::new(page_key(&self.prefix, digest))?;
        // A store that frames objects compares encodings, and two encodings of one content-addressed page differ.
        match self.store.put(&key, bytes.clone()).await {
            Err(crate::Error::ConflictingObject(_)) if self.store.get(&key).await? == bytes => Ok(()),
            result => Ok(result?),
        }
    }
}

pub struct FreshPages<'a> {
    pub store: &'a dyn ObjectStore,
    pub prefix: &'a str,
}

#[async_trait::async_trait]
impl PageStore for FreshPages<'_> {
    async fn get(&self, digest: PageDigest, length: u64) -> Result<Bytes> {
        let length = usize::try_from(length)
            .map_err(|error| Anomaly::fault("rtdb.storage/cow-page-length", error.to_string()))?;
        Ok(self
            .store
            .get_fresh_exact(&ObjectKey::new(page_key(self.prefix, digest))?, length)
            .await?)
    }

    async fn put(&self, _: PageDigest, _: Bytes) -> Result<()> {
        Err(Anomaly::unsupported(
            "rtdb.storage/read-only-pages",
            "The page store is read only",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;
    use rtdb_format::cow_tree::{self, Edit, NoSummary, Row};
    use rtdb_types::MemoryBudget;

    #[tokio::test]
    async fn a_published_page_is_read_by_digest_through_storage() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let pooled = ObjectPageStore {
            storage: Arc::new(BufferPool::new(store.clone(), 1 << 20)),
            store,
            prefix: "tables/public$docs/entity-directory".into(),
        };
        let memory = MemoryBudget::new(1 << 20);
        let row = Row {
            key: vec![0],
            value: vec![1],
        };
        let (root, _) = cow_tree::apply(
            &pooled,
            None,
            vec![Edit::Put(row.clone())],
            &NoSummary,
            &memory,
        )
        .await
        .unwrap();
        let root = root.unwrap();
        let fresh = FreshPages {
            store: pooled.store.as_ref(),
            prefix: &pooled.prefix,
        };
        for pages in [&pooled as &dyn PageStore, &fresh] {
            assert_eq!(
                cow_tree::scan(pages, &root, vec![0]..vec![1], &memory)
                    .await
                    .unwrap(),
                [row.clone()]
            );
        }
    }
}
