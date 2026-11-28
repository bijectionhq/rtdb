use bytes::Bytes;
use journal_contract::Lsn;
use rtdb_storage::{ObjectKey, ObjectVersionHold};
use rtdb_types::PhysicalObjectVersion;
use rtdb_stream_journal::authenticated::Archive;

use crate::multi::activation::{ActivationAuthority, Binding, recovery::Status};

use super::{held_objects::read_published_lineage, *};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LineageObject {
    sha256: [u8; 32],
    bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HeldLineage {
    pub(super) object: LineageObject,
    pub(super) manifest: [u8; 32],
    pub(super) version: PhysicalObjectVersion,
}

impl LineageObject {
    pub(super) fn describe(bytes: &[u8]) -> Self {
        Self { sha256: Sha256::digest(bytes).into(), bytes: bytes.len() }
    }

    pub(super) fn key(self, candidate: Uuid, manifest: [u8; 32]) -> Result<ObjectKey> {
        if self.bytes == 0 || self.bytes > PortableBackup::MAX_BYTES || self.sha256 == [0; 32] || manifest == [0; 32] {
            return Err(invalid());
        }
        Ok(ObjectKey::new(format!("db/{candidate}/lineage/{}/{}", digest_hex(&manifest), digest_hex(&self.sha256)))?)
    }

    pub(super) async fn read(
        self, candidate: Uuid, manifest: [u8; 32], store: &dyn ObjectStore, holds: Option<&dyn ObjectVersionHold>,
    ) -> Result<(HeldLineage, Bytes)> {
        let key = self.key(candidate, manifest)?;
        let observed = store.observe_fresh_exact(&key, self.bytes).await?;
        let version = observed.version.ok_or_else(invalid)?;
        if Self::describe(&observed.value) != self { return Err(invalid()); }
        if let Some(holds) = holds { holds.require_held(&key, &version).await?; }
        Ok((HeldLineage { object: self, manifest, version }, observed.value))
    }
}

#[derive(Clone, Debug)]
pub(super) struct HeldInclusion {
    database: String,
    authority: Uuid,
    url: String,
    tls: PathBuf,
    predecessor: Binding,
    catalog_root: CatalogRoot,
    position: Lsn,
    objects: rtdb_s3::S3Config,
}

impl HeldInclusion {
    pub(super) fn parse(mut fields: BTreeMap<String, String>) -> Result<Self> {
        let database = fields.remove("--database").unwrap_or_else(|| DEVELOPMENT_DATABASE.to_owned());
        let authority = backup_uuid(&backup_field(&mut fields, "--authority-id")?)?;
        let url = backup_field(&mut fields, "--authority-url")?;
        let tls = PathBuf::from(backup_field(&mut fields, "--authority-tls-dir")?);
        let incarnation = backup_uuid(&backup_field(&mut fields, "--expected-incarnation")?)?;
        let revision = nonzero(&mut fields, "--expected-revision")?;
        let catalog_root = CatalogRoot(activation::operator::hex_digest(backup_field(&mut fields, "--catalog-root")?)?);
        let position = Lsn(nonzero(&mut fields, "--position")?);
        let objects = hosted_s3(&mut fields)?;
        if !fields.is_empty() || !url.starts_with(CONSENSUS_SCHEME) || !tls.is_absolute() || catalog_root.0 == [0; 32] {
            return Err(arguments_error());
        }
        Ok(Self { database, authority, url, tls, predecessor: Binding { incarnation, revision }, catalog_root, position, objects })
    }

    pub(super) async fn run(self) -> Result<Value> {
        let authority = ActivationAuthority::connect_reader(&self.url, crate::cli::journal_credentials(&self.tls)?, self.authority).await?;
        self.run_with_authority(&authority).await
    }

    pub(super) async fn run_with_authority(self, authority: &ActivationAuthority) -> Result<Value> {
        let Status::Published { intent, binding, installation, .. } =
            authority.recovery_status_for_predecessor(&self.database, self.predecessor).await?
        else { return Err(unavailable()); };
        let (manifest, key_hold, objects_sha256, source) = installation.lineage_custody().ok_or_else(unavailable)?;
        if authority.identity() != self.authority || intent.operation.predecessor != self.predecessor
            || intent.operation.operation_id != key_hold.operation || key_hold.authority != self.authority
            || key_hold.generation.database != binding.incarnation || source.catalog_root != self.catalog_root
            || self.position.0 > source.head.size
        {
            return Err(invalid());
        }
        let intent = activation::operator::attestation::Intent {
            authority_id: self.authority, database: self.database, expected: self.predecessor,
            operation_id: key_hold.operation, cell: intent.destination_cell,
        };
        let root = ObjectKey::new("multi")?;
        let remotes = BTreeMap::new();
        let inventory = self.objects.versioned_inventory(&root, &remotes).await?;
        inventory.require_versioned().await?;
        let io = IoRegion::new();
        let result = async {
            let store = self.objects.open(&root, &remotes, Arc::clone(&io)).await?;
            let held = read_published_lineage(store.as_ref(), &intent, manifest, key_hold, objects_sha256).await?;
            let pinned = inventory.pinned_read_store(std::slice::from_ref(&held.version))?;
            let (observed, bytes) = held.object.read(binding.incarnation, manifest, pinned.as_ref(), Some(&inventory)).await?;
            if observed != held { return Err(invalid()); }
            checked_inclusion(&bytes, source, self.position)
        }.await;
        io.close_and_wait().await;
        result
    }
}

fn checked_inclusion(bytes: &[u8], source: rtdb_db::AuthenticatedCut, position: Lsn) -> Result<Value> {
    let mut input = bytes;
    let archive = Archive::read_from(&mut input).map_err(|_| invalid())?;
    if !input.is_empty() || archive.signed_head.head != source.head { return Err(invalid()); }
    archive.verify(source.catalog_root).map_err(|_| invalid())?;
    let proof = archive.inclusion(position).map_err(|_| invalid())?;
    serde_json::to_value(proof).map_err(|_| invalid())
}

fn nonzero(fields: &mut BTreeMap<String, String>, name: &str) -> Result<u64> {
    backup_field(fields, name)?.parse::<u64>().ok().filter(|value| *value > 0).ok_or_else(arguments_error)
}

pub(super) fn invalid() -> Anomaly {
    Anomaly::incorrect("rtdb/held-lineage-proof", "The retained proof does not match the published recovery cut")
}

pub(super) fn unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/held-lineage-proof-unavailable", "The recovery has no published held lineage proof")
}
