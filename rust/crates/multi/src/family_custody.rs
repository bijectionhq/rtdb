use std::num::NonZeroU64;

use ring::{digest::{Context, SHA256, digest}, signature::{ED25519, UnparsedPublicKey}};
use rtdb_directory::{Checkpoint, DeploymentRecord};
use rtdb_flight_sql::{FamilyOccurrenceWire, FamilyStage};
use rtdb_node::RetiredOriginalFamilySource;
use rtdb_types::{OwnedWorld, Result, RootLandingBinding, SourcePosition, StorageEpoch, Timestamp, TransactionKey, TxId};
use uuid::Uuid;

use crate::{capability::CellSecret, keys::{WorldRankTransferBinding, assertion_root_line}};

pub(crate) const FAMILY_CUSTODY_GRANT_BYTES: usize = 716;
const GRANT_DOMAIN: &[u8] = b"rtdb.multi/original-family-custody/v1\0";

pub(crate) struct FamilyCustodyGrant {
    pub(crate) source: RetiredOriginalFamilySource,
    incarnation: NonZeroU64,
    custody: [u8; 32],
    pub(crate) family: Uuid,
    pub(crate) stage: FamilyStage,
    pub(crate) root: RootLandingBinding,
    pub(crate) replay: [u8; 32],
    reader: [u8; 32],
    controller: [u8; 32],
}

fn unpack<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N]> {
    let (field, tail) = bytes.split_first_chunk::<N>().ok_or_else(OwnedWorld::unavailable)?;
    *bytes = tail;
    Ok(*field)
}

fn transaction(bytes: &mut &[u8]) -> Result<TransactionKey> {
    let tx_id = TxId(i64::from_be_bytes(unpack(bytes)?));
    let system_time = Timestamp(i64::from_be_bytes(unpack(bytes)?));
    if tx_id.0 < 0 || system_time.0 <= 0 { return Err(OwnedWorld::unavailable()); }
    Ok(TransactionKey { tx_id, system_time })
}

fn identity(bytes: &mut &[u8]) -> Result<Uuid> {
    let id = Uuid::from_bytes(unpack(bytes)?);
    if id.is_nil() { return Err(OwnedWorld::unavailable()); }
    Ok(id)
}

fn commitment(bytes: &mut &[u8]) -> Result<[u8; 32]> {
    let value = unpack(bytes)?;
    if value == [0; 32] { return Err(OwnedWorld::unavailable()); }
    Ok(value)
}

pub(crate) fn framed_digest(domain: &[u8], fields: &[&[u8]]) -> [u8; 32] {
    let mut hash = Context::new(&SHA256);
    hash.update(domain);
    for field in fields { hash.update(field); }
    hash.finish().as_ref().try_into().expect("SHA-256 commitment")
}

pub(crate) fn grant_id(body: &[u8]) -> [u8; 32] {
    digest(&SHA256, body).as_ref().try_into().expect("SHA-256 commitment")
}

fn mac_input(body: &[u8]) -> Vec<u8> {
    [GRANT_DOMAIN, body].concat()
}

impl FamilyCustodyGrant {
    fn decode(mut body: &[u8]) -> Result<Self> {
        let unavailable = OwnedWorld::unavailable;
        if body.len() != FAMILY_CUSTODY_GRANT_BYTES || unpack::<4>(&mut body)? != *b"RFC1" {
            return Err(unavailable());
        }
        let deployment = identity(&mut body)?;
        let incarnation = NonZeroU64::new(u64::from_be_bytes(unpack(&mut body)?)).ok_or_else(unavailable)?;
        let history = commitment(&mut body)?;
        let custody = commitment(&mut body)?;
        let installation = identity(&mut body)?;
        let target_installation = identity(&mut body)?;
        let audience_tablet = unpack(&mut body)?;
        let row_custody = commitment(&mut body)?;
        let handoff = commitment(&mut body)?;
        let carrier = transaction(&mut body)?;
        let fence = transaction(&mut body)?;
        let through = transaction(&mut body)?;
        let captured = transaction(&mut body)?;
        let _target_replay = transaction(&mut body)?;
        let replay = commitment(&mut body)?;
        commitment(&mut body)?;
        let family = identity(&mut body)?;
        let stage_deployment = identity(&mut body)?;
        let stage_incarnation = u64::from_be_bytes(unpack(&mut body)?);
        let stage_id = identity(&mut body)?;
        let root = RootLandingBinding::from_bytes(unpack(&mut body)?).map_err(|_| unavailable())?;
        let reader = commitment(&mut body)?;
        let controller = commitment(&mut body)?;
        if installation == target_installation || audience_tablet == [0; 16]
            || carrier == fence || !carrier.at_or_before(fence)
            || !fence.at_or_before(through) || !through.at_or_before(captured)
            || deployment != stage_deployment || incarnation.get() != stage_incarnation
            || root.stage != *stage_id.as_bytes() || u64::try_from(carrier.system_time.0).ok() != Some(root.position.publication)
            || reader == controller
        {
            return Err(unavailable());
        }
        Ok(Self {
            source: RetiredOriginalFamilySource {
                deployment, history, installation, target_installation, audience_tablet, row_custody,
                handoff, carrier, fence, through, captured,
            },
            incarnation, custody, family,
            stage: FamilyStage { deployment, incarnation, stage: stage_id },
            root, replay, reader, controller,
        })
    }

    fn require_original(&self, record: &DeploymentRecord) -> Result<()> {
        let unavailable = OwnedWorld::unavailable;
        let deployment = Uuid::parse_str(&record.id).map_err(|_| unavailable())?;
        let history = rtdb_types::PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?
            .history_fingerprint();
        let namespaces = record.storage_namespaces.as_ref().ok_or_else(unavailable)?;
        if self.source.deployment != deployment || self.incarnation.get() != record.incarnation
            || record.writer_pool().is_none() || self.source.history != history
            || self.root.position.line_generation != assertion_root_line(deployment, record.log_epoch)
            || self.custody != family_custody_id(deployment, self.source.installation, &namespaces.keys)
        {
            return Err(unavailable());
        }
        Ok(())
    }

    pub(crate) fn authenticate(binding: &[u8], id: [u8; 32], secret: &CellSecret, record: &DeploymentRecord) -> Result<Self> {
        let (body, proof) = binding.split_first_chunk::<FAMILY_CUSTODY_GRANT_BYTES>().ok_or_else(OwnedWorld::unavailable)?;
        let proof = proof.try_into().map_err(|_| OwnedWorld::unavailable())?;
        if grant_id(body) != id || !secret.verify_route(&mac_input(body), proof) {
            return Err(OwnedWorld::unavailable());
        }
        let grant = Self::decode(body)?;
        grant.require_original(record)?;
        Ok(grant)
    }

    pub(crate) fn occurrence(&self) -> FamilyOccurrenceWire {
        FamilyOccurrenceWire { position: self.root.position, assertion: self.root.assertion, capability: self.stage }
    }

    pub(crate) fn verify_reader(&self, digest: &[u8; 32], proof: &[u8; 64]) -> Result<()> {
        UnparsedPublicKey::new(&ED25519, self.reader).verify(digest, proof).map_err(|_| OwnedWorld::unavailable())
    }

    pub(crate) fn verify_controller(&self, digest: &[u8; 32], proof: &[u8]) -> Result<()> {
        UnparsedPublicKey::new(&ED25519, self.controller).verify(digest, proof).map_err(|_| OwnedWorld::unavailable())
    }
}

fn family_custody_id(deployment: Uuid, installation: Uuid, namespace: &[u8; 32]) -> [u8; 32] {
    framed_digest(b"rtdb.multi/original-family-custody-id/v1\0", &[deployment.as_bytes(), installation.as_bytes(), namespace])
}

pub(crate) fn original_family_custody(record: &DeploymentRecord, authority: Uuid) -> Result<([u8; 32], [u8; 32])> {
    if Uuid::parse_str(&record.id).ok() != Some(authority) { return Err(OwnedWorld::unavailable()); }
    let namespaces = record.storage_namespaces.as_ref().ok_or_else(OwnedWorld::unavailable)?;
    let history = rtdb_types::PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?.history_fingerprint();
    Ok((history, family_custody_id(authority, authority, &namespaces.keys)))
}

pub(crate) fn endorse_family_custody(
    secret: &CellSecret, record: &DeploymentRecord, authority: Uuid, capture: Checkpoint, body: &[u8],
) -> Result<[u8; 32]> {
    let grant = FamilyCustodyGrant::decode(body)?;
    grant.require_original(record)?;
    if grant.source.deployment != authority || grant.source.installation != authority
        || grant.source.captured.system_time != Timestamp(capture.system_time)
    {
        return Err(OwnedWorld::unavailable());
    }
    Ok(secret.sign_route(&mac_input(body)))
}

pub(crate) const WORLD_RANK_CUSTODY_GRANT_BYTES: usize = 228;

pub(crate) struct WorldRankCustodyGrant {
    pub capture: Uuid,
    pub archive: [u8; 32],
    pub captured: TransactionKey,
    pub binding: WorldRankTransferBinding,
    pub inventory: [u8; 32],
    reader: [u8; 32],
    controller: [u8; 32],
}

impl WorldRankCustodyGrant {
    fn decode(mut body: &[u8]) -> Result<Self> {
        let unavailable = OwnedWorld::unavailable;
        if body.len() != WORLD_RANK_CUSTODY_GRANT_BYTES || unpack::<4>(&mut body)? != *b"WRC1" {
            return Err(unavailable());
        }
        let grant = Self {
            capture: identity(&mut body)?,
            archive: commitment(&mut body)?,
            captured: transaction(&mut body)?,
            binding: WorldRankTransferBinding {
                address: commitment(&mut body)?,
                agreement: commitment(&mut body)?,
            },
            inventory: commitment(&mut body)?,
            reader: commitment(&mut body)?,
            controller: commitment(&mut body)?,
        };
        if grant.reader == grant.controller {
            return Err(unavailable());
        }
        Ok(grant)
    }

    fn mac_input(&self, record: &DeploymentRecord, body: &[u8]) -> Result<[u8; 32]> {
        let deployment = Uuid::parse_str(&record.id).map_err(|_| OwnedWorld::unavailable())?;
        let (history, custody) = original_family_custody(record, deployment)?;
        let world = WorldRankTransferBinding::archive_address(history, self.archive);
        let landed = WorldRankTransferBinding::landed_archive_address(history, self.archive);
        if record.parent.is_some() || record.writer_pool().is_none()
            || (self.binding.address != world && self.binding.address != landed)
        {
            return Err(OwnedWorld::unavailable());
        }
        Ok(framed_digest(b"rtdb.multi/original-world-rank-custody/v1\0", &[
            deployment.as_bytes(), &history, &custody, &record.incarnation.to_be_bytes(), body,
        ]))
    }

    pub(crate) fn authenticate(enrollment: &[u8], secret: &CellSecret, record: &DeploymentRecord) -> Result<Self> {
        let (body, proof) = enrollment.split_first_chunk::<WORLD_RANK_CUSTODY_GRANT_BYTES>()
            .ok_or_else(OwnedWorld::unavailable)?;
        let proof = proof.try_into().map_err(|_| OwnedWorld::unavailable())?;
        let grant = Self::decode(body)?;
        if !secret.verify_route(&grant.mac_input(record, body)?, proof) {
            return Err(OwnedWorld::unavailable());
        }
        Ok(grant)
    }

    pub(crate) fn endorse(
        secret: &CellSecret, record: &DeploymentRecord, capture: Uuid, archive: [u8; 32],
        checkpoint: Checkpoint, body: &[u8],
    ) -> Result<[u8; 32]> {
        let grant = Self::decode(body)?;
        let captured = SourcePosition::from_epoch_offset(StorageEpoch(record.log_epoch), checkpoint.source)?;
        if grant.capture != capture || grant.archive != archive
            || grant.captured != (TransactionKey { tx_id: TxId(captured.0), system_time: Timestamp(checkpoint.system_time) })
        {
            return Err(OwnedWorld::unavailable());
        }
        Ok(secret.sign_route(&grant.mac_input(record, body)?))
    }

    pub(crate) fn verify(&self, controller: bool, digest: &[u8; 32], proof: &[u8]) -> Result<()> {
        let key = if controller { self.controller } else { self.reader };
        UnparsedPublicKey::new(&ED25519, key).verify(digest, proof).map_err(|_| OwnedWorld::unavailable())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use ring::signature::{Ed25519KeyPair, KeyPair};
    use rtdb_types::{AssertionId, Position, RootLandingTarget};

    use super::*;

    #[test]
    fn world_rank_grant_binds_the_exact_capture_and_distinct_roles() -> Result<()> {
        let secret = CellSecret::new(&[31; 32])?;
        let mut record = DeploymentRecord::new("original", "example", vec![], 0);
        record.epoch = 1;
        record.storage_namespaces = Some(rtdb_directory::StorageNamespaces {
            objects: Uuid::from_bytes([2; 16]), keys: [3; 32],
        });
        let deployment = Uuid::parse_str(&record.id).unwrap();
        let (history, _) = original_family_custody(&record, deployment)?;
        let capture = Uuid::from_bytes([4; 16]);
        let archive = [5; 32];
        let address = WorldRankTransferBinding::archive_address(history, archive);
        let reader = Ed25519KeyPair::from_seed_unchecked(&[6; 32]).unwrap();
        let controller = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let checkpoint = Checkpoint { block: 1, boundary: 5, source: 4, system_time: 40, root_sha256: Some([8; 32]) };
        let last = SourcePosition::from_epoch_offset(StorageEpoch(record.log_epoch), checkpoint.source)?;
        let mut body = b"WRC1".to_vec();
        body.extend_from_slice(capture.as_bytes());
        body.extend_from_slice(&archive);
        body.extend_from_slice(&last.0.to_be_bytes());
        body.extend_from_slice(&checkpoint.system_time.to_be_bytes());
        body.extend_from_slice(&address);
        body.extend_from_slice(&[9; 32]);
        body.extend_from_slice(&[10; 32]);
        body.extend_from_slice(reader.public_key().as_ref());
        body.extend_from_slice(controller.public_key().as_ref());
        assert_eq!(body.len(), WORLD_RANK_CUSTODY_GRANT_BYTES);

        let wrong_source = Checkpoint { source: 3, ..checkpoint };
        assert!(WorldRankCustodyGrant::endorse(&secret, &record, capture, archive, wrong_source, &body).is_err());
        assert!(WorldRankCustodyGrant::endorse(&secret, &record, capture, [11; 32], checkpoint, &body).is_err());
        assert!(WorldRankCustodyGrant::endorse(&secret, &record, Uuid::nil(), archive, checkpoint, &body).is_err());
        let proof = WorldRankCustodyGrant::endorse(&secret, &record, capture, archive, checkpoint, &body)?;
        body.extend_from_slice(&proof);
        let grant = WorldRankCustodyGrant::authenticate(&body, &secret, &record)?;
        assert_eq!(grant.binding.address, address);
        let digest = [12; 32];
        let reader = reader.sign(&digest);
        let controller = controller.sign(&digest);
        grant.verify(false, &digest, reader.as_ref())?;
        grant.verify(true, &digest, controller.as_ref())?;
        assert!(grant.verify(true, &digest, reader.as_ref()).is_err());
        assert!(grant.verify(false, &digest, controller.as_ref()).is_err());
        let landed = WorldRankTransferBinding::landed_archive_address(history, archive);
        assert_ne!(landed, address);
        body[68..100].copy_from_slice(&landed);
        assert!(WorldRankCustodyGrant::authenticate(&body, &secret, &record).is_err());
        body.truncate(WORLD_RANK_CUSTODY_GRANT_BYTES);
        let proof = WorldRankCustodyGrant::endorse(&secret, &record, capture, archive, checkpoint, &body)?;
        body.extend_from_slice(&proof);
        let grant = WorldRankCustodyGrant::authenticate(&body, &secret, &record)?;
        assert_eq!(grant.binding.address, landed);
        assert_eq!(grant.binding.agreement, [9; 32]);
        assert_eq!(grant.inventory, [10; 32]);
        body[100] ^= 1;
        assert!(WorldRankCustodyGrant::authenticate(&body, &secret, &record).is_err());
        body[100] ^= 1;
        record.incarnation += 1;
        assert!(WorldRankCustodyGrant::authenticate(&body, &secret, &record).is_err());
        Ok(())
    }

    #[test]
    fn enrolled_custody_binds_original_incarnation_and_distinct_operation_roles() -> Result<()> {
        let secret = CellSecret::new(&[90; 32])?;
        let mut record = DeploymentRecord::new("original", "example", vec![], 0);
        record.epoch = 1;
        record.id = Uuid::from_bytes([1; 16]).to_string();
        record.storage_namespaces = Some(rtdb_directory::StorageNamespaces {
            objects: Uuid::from_bytes([2; 16]), keys: [3; 32],
        });
        let deployment = Uuid::from_bytes([1; 16]);
        let installation = deployment;
        let history = rtdb_types::PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?.history_fingerprint();
        let reader = Ed25519KeyPair::from_seed_unchecked(&[5; 32]).unwrap();
        let controller = Ed25519KeyPair::from_seed_unchecked(&[6; 32]).unwrap();
        let stage = Uuid::from_bytes([7; 16]);
        let binding = RootLandingBinding {
            position: Position { line_generation: assertion_root_line(deployment, 0), publication: 10, ordinal: NonZeroU64::MIN },
            assertion: AssertionId::from_bytes([8; 32]),
            stage: *stage.as_bytes(),
            target: RootLandingTarget { receipt: [9; 32], review_slot: 0, address_extent: [10; 32], disclosure: [11; 32] },
            payload_digest: [12; 32], link_digest: [13; 32],
        };
        let mut body = b"RFC1".to_vec();
        body.extend_from_slice(deployment.as_bytes());
        body.extend_from_slice(&1_u64.to_be_bytes());
        body.extend_from_slice(&history);
        body.extend_from_slice(&family_custody_id(deployment, installation, &[3; 32]));
        body.extend_from_slice(installation.as_bytes());
        body.extend_from_slice(&[14; 16]);
        body.extend_from_slice(&[15; 16]);
        body.extend_from_slice(&[16; 32]);
        body.extend_from_slice(&[17; 32]);
        for (id, micros) in [(1_i64, 10_i64), (2, 20), (3, 30), (4, 40), (5, 50)] {
            body.extend_from_slice(&id.to_be_bytes());
            body.extend_from_slice(&micros.to_be_bytes());
        }
        body.extend_from_slice(&[18; 32]);
        body.extend_from_slice(&[19; 32]);
        body.extend_from_slice(&[20; 16]);
        body.extend_from_slice(deployment.as_bytes());
        body.extend_from_slice(&1_u64.to_be_bytes());
        body.extend_from_slice(stage.as_bytes());
        body.extend_from_slice(&binding.to_bytes());
        body.extend_from_slice(reader.public_key().as_ref());
        body.extend_from_slice(controller.public_key().as_ref());
        assert_eq!(body.len(), FAMILY_CUSTODY_GRANT_BYTES);
        let id = grant_id(&body);
        let checkpoint = Checkpoint { block: 1, boundary: 4, source: 4, system_time: 40, root_sha256: Some([22; 32]) };
        assert!(endorse_family_custody(&secret, &record, Uuid::from_bytes([4; 16]), checkpoint, &body).is_err());
        let stale = Checkpoint { system_time: 39, ..checkpoint };
        assert!(endorse_family_custody(&secret, &record, installation, stale, &body).is_err());
        let proof = endorse_family_custody(&secret, &record, installation, checkpoint, &body)?;
        body.extend_from_slice(&proof);
        let grant = FamilyCustodyGrant::authenticate(&body, id, &secret, &record)?;
        let request = [21; 32];
        let reader_signature = reader.sign(&request);
        let controller_signature = controller.sign(&request);
        grant.verify_reader(&request, reader_signature.as_ref().try_into().unwrap())?;
        grant.verify_controller(&request, controller_signature.as_ref())?;
        assert!(grant.verify_controller(&request, reader_signature.as_ref()).is_err());
        assert!(FamilyCustodyGrant::authenticate(&body, [0; 32], &secret, &record).is_err());
        record.incarnation += 1;
        assert!(FamilyCustodyGrant::authenticate(&body, id, &secret, &record).is_err());
        Ok(())
    }
}
