use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use zeroize::Zeroizing;

use super::{DataKey, Generation, Kms, aead, erased_under_view};

#[derive(Clone)]
pub struct FileKms {
    root: Arc<Path>,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl FileKms {
    pub fn open(path: &Path, now: impl Fn() -> i64 + Send + Sync + 'static) -> Result<Self> {
        let root = path.canonicalize().map_err(io_error)?;
        let metadata = root.metadata().map_err(io_error)?;
        if !metadata.is_dir() {
            return Err(invalid_vault());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(invalid_vault());
            }
        }
        Ok(Self {
            root: root.into(),
            now: Arc::new(now),
        })
    }

    pub fn create_directory(path: &Path) -> Result<()> {
        let mut directory = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        match directory.create(path) {
            Ok(()) => File::open(path.parent().ok_or_else(invalid_vault)?)
                .and_then(|parent| parent.sync_all())
                .map_err(io_error),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(io_error(error)),
        }
    }

    async fn access<T: Send + 'static>(
        &self,
        generation: Generation,
        action: impl FnOnce(&Slot, Option<State>, i64) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let vault = self.clone();
        tokio::task::spawn_blocking(move || {
            let path = vault.root.join(format!(
                "{}-{:016x}.key",
                generation.database.simple(),
                generation.number
            ));
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path.with_extension("lock"))
                .map_err(io_error)?;
            lock.lock().map_err(io_error)?;
            let slot = Slot {
                path,
                root: vault.root,
            };
            let state = slot.read()?;
            action(&slot, state, (vault.now)())
        })
        .await
        .map_err(|error| Anomaly::fault("rtdb.crypt/key-vault", error.to_string()))?
    }
}

enum State {
    Active(DataKey),
    Retiring { key: DataKey, unreadable_by: i64 },
    Destroyed,
}

impl State {
    fn readable(&self, now: i64) -> Result<&DataKey> {
        match self {
            Self::Active(key) => Ok(key),
            Self::Retiring { key, unreadable_by } if now < *unreadable_by => Ok(key),
            Self::Retiring { .. } | Self::Destroyed => Err(erased_under_view()),
        }
    }
}

struct Slot {
    path: PathBuf,
    root: Arc<Path>,
}

impl Slot {
    fn read(&self) -> Result<Option<State>> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error(error)),
        };
        let mut bytes = Zeroizing::new(Vec::with_capacity(34));
        file.take(34).read_to_end(&mut bytes).map_err(io_error)?;
        let state = match bytes.as_slice() {
            [0, key @ ..] if key.len() == 32 => {
                let key = DataKey::new(key.try_into().map_err(|_| invalid_slot())?);
                match fs::read(self.path.with_extension("retirement")) {
                    Ok(deadline) => State::Retiring {
                        key,
                        unreadable_by: i64::from_be_bytes(
                            deadline.try_into().map_err(|_| invalid_slot())?,
                        ),
                    },
                    Err(error) if error.kind() == io::ErrorKind::NotFound => State::Active(key),
                    Err(error) => return Err(io_error(error)),
                }
            }
            [2] => State::Destroyed,
            _ => return Err(invalid_slot()),
        };
        Ok(Some(state))
    }

    fn write_key(&self, key: &DataKey) -> Result<()> {
        let mut bytes = Zeroizing::new([0; 33]);
        bytes[1..].copy_from_slice(key.as_ref());
        self.write(&self.path, bytes.as_ref())
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        let mut file = tempfile::NamedTempFile::new_in(&self.root).map_err(io_error)?;
        file.write_all(bytes).map_err(io_error)?;
        file.as_file().sync_all().map_err(io_error)?;
        file.persist(path).map_err(|error| io_error(error.error))?;
        File::open(&self.root)
            .and_then(|root| root.sync_all())
            .map_err(io_error)
    }
}

#[async_trait]
impl Kms for FileKms {
    async fn create(&self, generation: Generation) -> Result<()> {
        self.access(generation, |slot, state, _| match state {
            None => slot.write_key(&aead::random_key()?),
            Some(State::Active(_)) => Ok(()),
            Some(_) => Err(erased_under_view()),
        })
        .await
    }

    async fn wrap(&self, generation: Generation, key: &DataKey, context: &[u8]) -> Result<Vec<u8>> {
        let key = key.clone();
        let context = context.to_vec();
        self.access(generation, move |_, state, _| {
            match state.ok_or_else(unknown_generation)? {
                State::Active(wrapping) => aead::seal(&wrapping, &context, key.as_ref()),
                _ => Err(erased_under_view()),
            }
        })
        .await
    }

    async fn unwrap(
        &self,
        generation: Generation,
        wrapped: &[u8],
        context: &[u8],
    ) -> Result<DataKey> {
        let wrapped = wrapped.to_vec();
        let context = context.to_vec();
        self.access(generation, move |_, state, now| {
            let state = state.ok_or_else(unknown_generation)?;
            let plaintext = aead::open(state.readable(now)?, &context, &wrapped)?;
            Ok(DataKey::new(
                plaintext
                    .as_slice()
                    .try_into()
                    .map_err(|_| invalid_slot())?,
            ))
        })
        .await
    }

    async fn retire(&self, generation: Generation, unreadable_by: i64) -> Result<()> {
        self.access(generation, move |slot, state, _| {
            match state.ok_or_else(unknown_generation)? {
                // The retirement record carries no key material, including in an interrupted write.
                State::Active(_) => slot.write(
                    &slot.path.with_extension("retirement"),
                    &unreadable_by.to_be_bytes(),
                ),
                State::Retiring {
                    unreadable_by: existing,
                    ..
                } if existing != unreadable_by => {
                    Err(conflict("Generation already has a retirement deadline"))
                }
                _ => Ok(()),
            }
        })
        .await
    }

    async fn check_readable(&self, generation: Generation) -> Result<()> {
        self.access(generation, |_, state, now| {
            state
                .ok_or_else(unknown_generation)?
                .readable(now)
                .map(|_| ())
        })
        .await
    }

    async fn destroy(&self, generation: Generation) -> Result<()> {
        self.access(generation, |slot, state, now| {
            match state.ok_or_else(unknown_generation)? {
                State::Retiring { unreadable_by, .. } if now >= unreadable_by => {
                    slot.write(&slot.path, &[2])
                }
                State::Destroyed => Ok(()),
                _ => Err(conflict(
                    "Key custody must remain through the retirement deadline",
                )),
            }
        })
        .await
    }
}

fn invalid_vault() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.crypt/key-vault",
        "The key vault must be a private directory",
    )
}

fn io_error(error: io::Error) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.crypt/key-vault-io",
        error.to_string(),
    )
}

fn invalid_slot() -> Anomaly {
    Anomaly::fault(
        "rtdb.crypt/key-vault-slot",
        "Invalid key generation metadata",
    )
}

fn unknown_generation() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.crypt/unknown-generation",
        "Key custody is unavailable for the requested generation",
    )
}

fn conflict(message: &str) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.crypt/key-retirement",
        message,
    )
}
