use bytes::Bytes;
use rtdb_format::trie_retirement::InstalledRetirement;
use rtdb_types::{Anomaly, Result};

use crate::{ObjectKey, ObjectStore};

struct TrieDeletionGrant<'a> {
    store: &'a dyn ObjectStore,
    files: Vec<ObjectKey>,
}

impl<'a> TrieDeletionGrant<'a> {
    async fn authorize(store: &'a dyn ObjectStore, receipt: &ObjectKey, contents: Bytes) -> Result<Option<Self>> {
        if !store.defers_trie_releases() {
            return Err(Anomaly::unsupported("rtdb.storage/trie-release", "This store has no coordinated trie retirement"));
        }
        let installed = InstalledRetirement::decode(receipt.as_str(), &contents)?;
        if store.get_fresh_exact(receipt, contents.len()).await? != contents {
            return Err(Anomaly::incorrect("rtdb.storage/trie-release", "The durable trie retirement receipt differs"));
        }
        let mut selected: Vec<_> = installed.files().iter().collect();
        selected.sort_by_key(|file| file.release_order());
        let files = selected.into_iter().map(|file| ObjectKey::new(&file.key))
            .collect::<crate::Result<Vec<_>>>()?;

        // The current conservative predicate uses the installed cut. Admission
        // and this grant can evolve independently once holds carry reachability.
        if !store.advance_catalog_floor(installed.cut()).await? { return Ok(None); }
        Ok(Some(Self { store, files }))
    }

    async fn erase(self) -> Result<()> {
        for file in self.files { self.store.delete(&file).await?; }
        Ok(())
    }
}

pub(crate) async fn retire(store: &dyn ObjectStore, receipt: &ObjectKey, contents: Bytes) -> Result<bool> {
    let Some(grant) = TrieDeletionGrant::authorize(store, receipt, contents).await? else { return Ok(false); };
    grant.erase().await?;
    Ok(true)
}
