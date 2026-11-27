use bytes::Bytes;
use rtdb_types::{AssertionId, Position};

use crate::keys::{AssertionAuthoring, AssertionStage, EnrolledWorldRow, FamilyOccurrence, LandingReservation};

use super::{super::{ErasureOrder, READER_DRAIN_LIMIT_MS}, *};

#[tokio::test]
async fn provider_reconciles_exact_vault_tombstones_after_source_loss() -> Result<()> {
    let deployment = Uuid::new_v4();
    let incarnation = NonZeroU64::MIN;
    let family = Uuid::new_v4();
    let id = Uuid::new_v4();
    let source = tempfile::tempdir().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let provider = LocalKeyService::with_archive_broker(directory.path())?;
    let namespace = provider.namespace_id().await?;
    let region = IoRegion::new();
    let families = provider.assertion_families(Arc::clone(&region)).unwrap();
    let signer = Ed25519KeyPair::from_seed_unchecked(&[1; 32]).unwrap();
    let drain_signer = Ed25519KeyPair::from_seed_unchecked(&[2; 32]).unwrap();
    let stage = || AssertionStage::new(deployment, incarnation, Uuid::new_v4());
    let original = FamilyOccurrence {
        position: Position { line_generation: [3; 16], publication: 1, ordinal: NonZeroU64::MIN },
        assertion: AssertionId::from_bytes([4; 32]),
        capability: stage()?,
    };
    let pending = LandingReservation {
        source: original.position, destination: AssertionId::from_bytes([5; 32]),
        capability: stage()?, link_capability: stage()?, review_commitment: [6; 32],
    };
    let outside = stage()?;
    let authoring = AssertionAuthoring { assertion_id: original.assertion, scope_commitment: [7; 32], codec: 1 };
    families.enroll_original(family, EnrolledWorldRow {
        world: Uuid::from_bytes(original.position.line_generation), assertion: original.assertion,
        capability: original.capability, scope_commitment: authoring.scope_commitment, slot_commitment: [8; 32], codec: 1,
    }).await?;
    families.register(family, original).await?;
    families.reserve_landing(family, pending).await?;
    let payload = Bytes::from_static(b"the original assertion");
    let original_frame = provider.seal_assertion_stage(original.capability, authoring, &region, payload.clone()).await?;
    let outside_frame = provider.seal_assertion_stage(outside, authoring, &region, payload.clone()).await?;
    std::fs::write(source.path().join("original"), payload.as_ref()).unwrap();

    let fence = families.fence(family, original.position, id).await?;
    let vault = Obligation {
        kind: CustodyKind::AssertionVault, inventory: fence.vault_inventory(),
        holder_key: signer.public_key().as_ref().try_into().unwrap(),
    };
    let drain = Obligation {
        kind: CustodyKind::ReleaseDrain, inventory: [9; 32],
        holder_key: drain_signer.public_key().as_ref().try_into().unwrap(),
    };
    let backup = Obligation { kind: CustodyKind::KeyServiceBackup, inventory: [10; 32], ..drain };
    let order = ErasureOrder {
        domain: Uuid::new_v4(), id, logical_commit: [11; 32], committed_at_ms: 1_000,
        initial_holds: vec![], obligations: vec![drain, vault, backup],
    };
    let ordered_at = order.committed_at_ms + READER_DRAIN_LIMIT_MS;
    let deadline = AssertionDeadline::new(&provider);
    let holder = provider.assertion_vault_custodian(namespace, deployment, incarnation, Arc::clone(&region), &signer)?;
    assert!(holder.bind(id, family, vault).await.is_err());
    deadline.publish(order.clone()).await?;
    assert!(holder.bind(id, Uuid::new_v4(), vault).await.is_err());
    assert!(holder.bind(id, family, Obligation { inventory: [12; 32], ..vault }).await.is_err());
    let other = provider.assertion_vault_custodian(namespace, deployment, incarnation, Arc::clone(&region), &drain_signer)?;
    assert!(other.bind(id, family, vault).await.is_err());
    let wrong_incarnation = provider.assertion_vault_custodian(
        namespace, deployment, NonZeroU64::new(2).unwrap(), Arc::clone(&region), &signer,
    )?;
    assert!(wrong_incarnation.bind(id, family, vault).await.is_err());
    holder.bind(id, family, vault).await?;
    assert!(holder.reconcile_at(id, || Ok(ordered_at)).await.is_err());
    deadline.update(id, |record| record.begin(ordered_at)).await?;
    assert!(holder.reconcile_at(id, || Ok(ordered_at)).await.is_err());
    assert_eq!(provider.open_assertion_for_resolution(original.capability, authoring, &region, original_frame.clone()).await?, payload);

    let signed = |obligation| -> Result<Settlement> {
        let mut receipt = Settlement { obligation, observed_at_ms: ordered_at, disposition: [13; 32], signature: Vec::new() };
        receipt.signature = drain_signer.sign(&receipt.signing_bytes(&order, ordered_at)?).as_ref().to_vec();
        Ok(receipt)
    };
    let drained = signed(drain)?;
    deadline.update(id, |record| record.join(drained.clone(), ordered_at)).await?;

    // A response was lost after one tombstone, before any vault receipt was retained.
    provider.retire_assertion_stage(original.capability, &region).await?;
    drop((holder, other, wrong_incarnation, deadline, families));
    drop((source, provider, region));

    let reopened = LocalKeyService::with_archive_broker(directory.path())?;
    let region = IoRegion::new();
    assert_eq!(reopened.page(None).await?, vec![id]);
    let holder = reopened.assertion_vault_custodian(namespace, deployment, incarnation, Arc::clone(&region), &signer)?;
    let now = ordered_at + 1;
    let (first, retry) = tokio::join!(holder.reconcile_at(id, || Ok(now)), holder.reconcile_at(id, || Ok(now)));
    let receipt = first?;
    assert_eq!(retry?, receipt);
    assert_eq!(receipt.observed_at_ms, now);
    assert_eq!(receipt.obligation, vault);
    receipt.verify(&order, ordered_at, now)?;
    assert_eq!(holder.reconcile_at(id, || Ok(now + 8)).await?, receipt);

    let copy = tempfile::tempdir().unwrap();
    let substitute = LocalKeyService::with_archive_broker(copy.path())?;
    let (copied_order, _) = DeadlineJournal::read(&reopened, id).await?.unwrap();
    assert!(substitute.compare_and_set(id, None, copied_order).await?);
    std::fs::copy(directory.path().join(".namespace"), copy.path().join(".namespace")).unwrap();
    let substitute = substitute.assertion_vault_custodian(namespace, deployment, incarnation, Arc::clone(&region), &signer)?;
    assert!(substitute.reconcile_at(id, || Ok(now + 8)).await.is_err());

    assert!(reopened.open_assertion_for_resolution(original.capability, authoring, &region, original_frame).await.is_err());
    for pending_stage in [pending.capability, pending.link_capability] {
        assert!(reopened.seal_assertion_stage(pending_stage, authoring, &region, payload.clone()).await.is_err());
    }
    assert_eq!(reopened.open_assertion_for_resolution(outside, authoring, &region, outside_frame).await?, payload);

    // The vault's result alone cannot certify other custody, even after all its keys are gone.
    let deadline = AssertionDeadline::new(&reopened);
    assert!(deadline.read(id).await?.unwrap().0.report(&signer)?.is_none());
    let retained_backup = signed(backup)?;
    deadline.update(id, |record| {
        record.join(retained_backup.clone(), now)?;
        record.attestation = record.report(&signer)?;
        Ok(())
    }).await?;
    let families = reopened.assertion_families(Arc::clone(&region)).unwrap();
    families.settle(family, &deadline, &vault.holder_key).await?;
    assert_eq!(holder.reconcile_at(id, || Ok(now + 9)).await?, receipt);
    Ok(())
}
