use std::{fmt::Write as _, num::NonZeroU64};

use rtdb_directory::{ClassicCatalogProvider, Version};
use rtdb_types::{Anomaly, Result, TransactionKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::keys::{FamilyReleaseRegistration, FamilyResponsePart};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseSelection {
    pub(crate) locator: String,
    pub(crate) address: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(tag = "population", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Population {
    LocalCatalog,
    HostedCatalog,
    FamilyRelease { registration: ReleaseSelection },
}

impl Population {
    pub(super) fn catalog_provider(&self) -> Option<ClassicCatalogProvider> {
        match self {
            Self::LocalCatalog => Some(ClassicCatalogProvider::Local),
            Self::HostedCatalog => Some(ClassicCatalogProvider::HostedS3),
            Self::FamilyRelease { .. } => None,
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "population", rename_all = "snake_case")]
pub(super) enum Terminal {
    LocalCatalog(CatalogDrain),
    HostedCatalog(CatalogDrain),
    FamilyRelease(ReleaseDrain),
}

#[derive(Serialize)]
pub(super) struct ApplicationClose {
    pub(super) classic_cut: TransactionKey,
    pub(super) sql_cutoff: TransactionKey,
    pub(super) epoch: u64,
    pub(super) assignment: [u8; 16],
    pub(super) custody_sha256: [u8; 32],
    pub(super) selection_sha256: [u8; 32],
}

impl ReleaseSelection {
    pub(crate) fn registration(&self) -> Result<FamilyReleaseRegistration> {
        if self.locator.len() != 278 {
            return Err(mismatch());
        }
        let mut fields = self.locator.split('/');
        let origin = hex(fields.next())?;
        let attempt = hex(fields.next())?;
        let part = FamilyResponsePart {
            cell_history: hex(fields.next())?,
            usher: hex(fields.next())?,
            conductor: hex(fields.next())?,
            response: hex(fields.next())?,
            ordinal: NonZeroU64::new(u64::from_be_bytes(hex(fields.next())?))
                .ok_or_else(mismatch)?,
        };
        if fields.next().is_some() {
            return Err(mismatch());
        }
        FamilyReleaseRegistration::from_address(origin, attempt, part, &self.address)
    }
}

fn hex<const N: usize>(field: Option<&str>) -> Result<[u8; N]> {
    let field = field
        .filter(|field| {
            field.len() == N * 2
                && field
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(mismatch)?;
    let mut bytes = [0; N];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&field[index * 2..index * 2 + 2], 16).map_err(|_| mismatch())?;
    }
    Ok(bytes)
}

pub(crate) fn locator_digest(locator: &str) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in crate::archive::sha256(locator.as_bytes()) {
        write!(encoded, "{byte:02x}").expect("writing to a string");
    }
    encoded
}

#[derive(Serialize)]
pub(super) struct CatalogDrain {
    pub(super) fenced_at: Version,
    pub(super) drained_at: Version,
    pub(super) residence: String,
    pub(super) object_namespace: Uuid,
    pub(super) readers: Vec<Uuid>,
}

#[derive(Serialize)]
pub(super) struct ReleaseDrain {
    pub(super) locator: String,
    pub(super) address_sha256: [u8; 32],
    pub(super) settled_at: TransactionKey,
    pub(super) settlement_sha256: [u8; 32],
    pub(super) disposition: &'static str,
    pub(super) terminal_sha256: [u8; 32],
}

pub(crate) fn mismatch() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.multi/classic-provider-drain",
        "The selected Classic provider evidence differs from the Original cut",
    )
}
