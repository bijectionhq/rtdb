use std::{
    path::PathBuf,
    sync::Arc,
};

use bytes::Bytes;
use rtdb_crypt::{
    FileKms, Kms, LocalKms,
    tokens::{TokenAuthority, TokenKeys},
};
use rtdb_format::database::proto::{DatabaseConfig, database_config};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_store::StoredTokenKeys;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::Clock;

enum Custody {
    Configured(Arc<dyn Kms>),
    Local(PathBuf),
    Volatile,
    Missing,
}

pub(crate) struct DatabaseTokens {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    read_only: bool,
    custody: Custody,
    clock: Arc<dyn Clock>,
    driver_incarnation: Option<Uuid>,
    incarnation: OnceCell<Uuid>,
    authority: OnceCell<DatabaseAuthority>,
}

struct DatabaseAuthority {
    kms: Arc<dyn Kms>,
    keys: Arc<StoredTokenKeys>,
    authority: TokenAuthority,
}

impl DatabaseTokens {
    pub(crate) fn supported(&self) -> bool {
        !matches!(self.custody, Custody::Missing)
    }

    pub(super) fn supports_persistent_custody(&self) -> bool {
        matches!(self.custody, Custody::Configured(_) | Custody::Local(_))
    }

    pub(super) fn new(
        name: &str,
        config: &DatabaseConfig,
        read_only: bool,
        store: Arc<dyn ObjectStore>,
        kms: Option<Arc<dyn Kms>>,
        clock: Arc<dyn Clock>,
        driver_incarnation: Option<Uuid>,
    ) -> Self {
        let volatile = matches!(
            &config.storage,
            None | Some(database_config::Storage::InMemoryStorage(_))
        );
        let custody = match (&config.storage, kms) {
            (_, Some(kms)) => Custody::Configured(kms),
            (None | Some(database_config::Storage::InMemoryStorage(_)), None) => Custody::Volatile,
            (Some(database_config::Storage::LocalStorage(storage)), None) => storage
                .path
                .as_ref()
                .map(|path| Custody::Local(PathBuf::from(path).join(".rtdb-token-keys")))
                .unwrap_or(Custody::Missing),
            (_, None) => Custody::Missing,
        };
        Self {
            store,
            prefix: format!("~rtdb-v1/{:x}", Sha256::digest(name)),
            read_only: read_only && !volatile,
            custody,
            clock,
            driver_incarnation,
            incarnation: OnceCell::new(),
            authority: OnceCell::new(),
        }
    }

    pub(super) async fn incarnation(&self) -> Result<Uuid> {
        if let Some(incarnation) = self.driver_incarnation {
            return Ok(incarnation);
        }
        self.incarnation
            .get_or_try_init(|| async {
                let marker = ObjectKey::new(format!("{}/incarnation", self.prefix))?;
                let bytes = match self.store.get_fresh_exact(&marker, 16).await {
                    Ok(bytes) => bytes,
                    Err(Error::MissingObject(_)) if self.read_only => return Err(unavailable()),
                    Err(Error::MissingObject(_)) => {
                        let candidate = Uuid::new_v4();
                        let publication = self
                            .store
                            .put(&marker, Bytes::copy_from_slice(candidate.as_bytes()))
                            .await;
                        match self.store.get_fresh_exact(&marker, 16).await {
                            Ok(bytes) => bytes,
                            Err(error) => {
                                publication?;
                                return Err(error.into());
                            }
                        }
                    }
                    Err(error) => return Err(error.into()),
                };
                Uuid::from_slice(&bytes).map_err(|_| invalid_metadata())
            })
            .await
            .copied()
    }

    pub(crate) async fn authority(&self) -> Result<TokenAuthority> {
        Ok(self.database_authority().await?.authority.clone())
    }

    async fn database_authority(&self) -> Result<&DatabaseAuthority> {
        self.authority
            .get_or_try_init(|| async {
                let clock = Arc::clone(&self.clock);
                let kms: Arc<dyn Kms> = match &self.custody {
                    Custody::Configured(kms) => Arc::clone(kms),
                    Custody::Volatile => Arc::new(LocalKms::new(move || clock.now().0)),
                    Custody::Local(path) => {
                        let path = path.clone();
                        let read_only = self.read_only;
                        Arc::new(
                            tokio::task::spawn_blocking(move || {
                                if !read_only {
                                    FileKms::create_directory(&path)?;
                                }
                                FileKms::open(&path, move || clock.now().0)
                            })
                            .await
                            .map_err(|error| {
                                Anomaly::fault("rtdb/token-key-vault", error.to_string())
                            })??,
                        )
                    }
                    Custody::Missing => return Err(unavailable()),
                };
                let incarnation = self.incarnation().await?;
                let keys = Arc::new(StoredTokenKeys::new(Arc::clone(&self.store), self.prefix.clone(), incarnation, 0));
                if self.read_only {
                    keys.get(0).await?.ok_or_else(unavailable)?;
                } else {
                    keys.install(kms.as_ref(), 0).await?;
                }
                let authority = TokenAuthority::new(incarnation, Arc::clone(&kms), keys.clone());
                Ok(DatabaseAuthority {
                    kms,
                    keys,
                    authority,
                })
            })
            .await
    }

    pub(super) async fn apply_rotation(&self, next: u64, unreadable_by: i64) -> Result<()> {
        if self.read_only {
            return Err(unavailable());
        }
        let authority = self.database_authority().await?;
        authority.keys.rotate(authority.kms.as_ref(), next, unreadable_by).await
    }

    pub(super) async fn destroy_retired(&self, number: u64) -> Result<()> {
        if self.read_only {
            return Err(unavailable());
        }
        let authority = self.database_authority().await?;
        authority.keys.destroy_retired(authority.kms.as_ref(), number).await
    }
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/token-authority-unavailable",
        "The database token authority is unavailable",
    )
}

fn invalid_metadata() -> Anomaly {
    Anomaly::fault(
        "rtdb/token-authority-metadata",
        "Invalid database token authority metadata",
    )
}
