use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use zeroize::Zeroizing;

use super::{DataKey, Generation, Kms, RecoveryKeyHold, aead, erased_under_view};

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

    pub fn vault_path(&self) -> &Path {
        &self.root
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

struct State {
    key: Option<DataKey>,
    custody: Custody,
}

impl State {
    fn writable(&self) -> Result<&DataKey> {
        if self.custody.deadline().is_some() {
            return Err(erased_under_view());
        }
        self.key.as_ref().ok_or_else(erased_under_view)
    }

    fn readable(&self, now: i64) -> Result<&DataKey> {
        if self
            .custody
            .deadline()
            .is_some_and(|deadline| now >= deadline)
        {
            return Err(erased_under_view());
        }
        self.key.as_ref().ok_or_else(erased_under_view)
    }
}

#[derive(Clone, Copy)]
enum Custody {
    Unrestricted,
    Retiring(i64),
    Recovery(RecoveryCustody),
}

#[derive(Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum RecoveryCustody {
    Held {
        hold: RecoveryKeyHold,
    },
    Released {
        hold: RecoveryKeyHold,
        unreadable_by: Option<i64>,
    },
}

impl Custody {
    fn deadline(self) -> Option<i64> {
        match self {
            Self::Retiring(unreadable_by) => Some(unreadable_by),
            Self::Recovery(RecoveryCustody::Released { unreadable_by, .. }) => unreadable_by,
            _ => None,
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
        let key = match bytes.as_slice() {
            [0, key @ ..] if key.len() == 32 => {
                Some(DataKey::new(key.try_into().map_err(|_| invalid_slot())?))
            }
            [2] => None,
            _ => return Err(invalid_slot()),
        };
        Ok(Some(State {
            key,
            custody: self.read_custody()?,
        }))
    }

    fn read_custody(&self) -> Result<Custody> {
        let path = self.path.with_extension("retirement");
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(Custody::Unrestricted);
            }
            Err(error) => return Err(io_error(error)),
        };
        let mut bytes = Vec::new();
        file.take(1025).read_to_end(&mut bytes).map_err(io_error)?;
        if let Ok(deadline) = bytes.as_slice().try_into() {
            return Ok(Custody::Retiring(i64::from_be_bytes(deadline)));
        }
        if bytes.len() > 1024 {
            return Err(invalid_slot());
        }
        serde_json::from_slice(&bytes)
            .map(Custody::Recovery)
            .map_err(|_| invalid_slot())
    }

    fn write_retirement(&self, deadline: i64) -> Result<()> {
        self.write(
            &self.path.with_extension("retirement"),
            &deadline.to_be_bytes(),
        )
    }

    fn write_recovery_custody(&self, custody: RecoveryCustody) -> Result<()> {
        let bytes = serde_json::to_vec(&custody).map_err(|_| invalid_slot())?;
        self.write(&self.path.with_extension("retirement"), &bytes)
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
        self.sync()
    }

    fn sync(&self) -> Result<()> {
        File::open(&self.root)
            .and_then(|root| root.sync_all())
            .map_err(io_error)
    }
}

#[async_trait]
impl Kms for FileKms {
    fn supports_durable_generation_retirement(&self) -> bool {
        true
    }

    async fn generation_destroyed(&self, generation: Generation) -> Result<bool> {
        self.access(generation, |_, state, _| {
            Ok(state.is_some_and(|state| state.key.is_none()))
        })
        .await
    }

    async fn create(&self, generation: Generation) -> Result<()> {
        self.access(generation, |slot, state, _| match state {
            None => slot.write_key(&aead::random_key()?),
            Some(state) => state.writable().map(|_| ()),
        })
        .await
    }

    async fn wrap(&self, generation: Generation, key: &DataKey, context: &[u8]) -> Result<Vec<u8>> {
        let key = key.clone();
        let context = context.to_vec();
        self.access(generation, move |_, state, _| {
            let state = state.ok_or_else(unknown_generation)?;
            aead::seal(state.writable()?, &context, key.as_ref())
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
            let state = state.ok_or_else(unknown_generation)?;
            if state.key.is_none() {
                return Ok(());
            }
            match state.custody {
                Custody::Unrestricted => slot.write_retirement(unreadable_by),
                Custody::Recovery(RecoveryCustody::Held { .. }) => {
                    Err(conflict("The staged recovery still holds this generation"))
                }
                Custody::Recovery(RecoveryCustody::Released {
                    hold,
                    unreadable_by: None,
                }) => slot.write_recovery_custody(RecoveryCustody::Released {
                    hold,
                    unreadable_by: Some(unreadable_by),
                }),
                Custody::Retiring(existing)
                | Custody::Recovery(RecoveryCustody::Released {
                    unreadable_by: Some(existing),
                    ..
                }) => {
                    if existing == unreadable_by {
                        slot.sync()
                    } else {
                        Err(conflict("Generation already has a retirement deadline"))
                    }
                }
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
            let state = state.ok_or_else(unknown_generation)?;
            if state.key.is_none() {
                return Ok(());
            }
            if state
                .custody
                .deadline()
                .is_some_and(|deadline| now >= deadline)
            {
                return slot.write(&slot.path, &[2]);
            }
            Err(conflict(
                "Key custody must remain through the retirement deadline",
            ))
        })
        .await
    }

    async fn retain_recovery_key(&self, hold: RecoveryKeyHold) -> Result<()> {
        validate_hold(hold)?;
        self.access(hold.generation, move |slot, state, _| {
            let state = state.ok_or_else(unknown_generation)?;
            state.writable()?;
            match state.custody {
                Custody::Unrestricted => {
                    slot.write_recovery_custody(RecoveryCustody::Held { hold })
                }
                Custody::Recovery(RecoveryCustody::Held { hold: existing }) if existing == hold => {
                    slot.sync()
                }
                _ => Err(conflict(
                    "This generation already has a different recovery custody decision",
                )),
            }
        })
        .await
    }

    async fn require_recovery_key(&self, hold: RecoveryKeyHold) -> Result<()> {
        validate_hold(hold)?;
        self.access(hold.generation, move |slot, state, _| {
            let state = state.ok_or_else(unknown_generation)?;
            state.writable()?;
            match state.custody {
                Custody::Recovery(RecoveryCustody::Held { hold: existing }) if existing == hold => {
                    slot.sync()
                }
                _ => Err(conflict(
                    "This generation has no matching active recovery hold",
                )),
            }
        })
        .await
    }

    async fn release_recovery_key(&self, hold: RecoveryKeyHold) -> Result<()> {
        validate_hold(hold)?;
        self.access(hold.generation, move |slot, state, _| {
            let state = state.ok_or_else(unknown_generation)?;
            match state.custody {
                Custody::Recovery(RecoveryCustody::Held { hold: existing }) if existing == hold => {
                    slot.write_recovery_custody(RecoveryCustody::Released {
                        hold,
                        unreadable_by: None,
                    })
                }
                Custody::Recovery(RecoveryCustody::Released { hold: existing, .. })
                    if existing == hold =>
                {
                    slot.sync()
                }
                _ => Err(conflict(
                    "This generation has no matching recovery hold to release",
                )),
            }
        })
        .await
    }
}

fn validate_hold(hold: RecoveryKeyHold) -> Result<()> {
    if hold.authority.is_nil()
        || hold.authority_sha256 == [0; 32]
        || hold.operation.is_nil()
        || hold.generation.database.is_nil()
        || hold.installation_sha256 == [0; 32]
    {
        return Err(Anomaly::incorrect(
            "rtdb.crypt/recovery-key-hold",
            "Invalid recovery key hold identity",
        ));
    }
    Ok(())
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
