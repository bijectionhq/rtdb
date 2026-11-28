use std::collections::BTreeMap;

use ring::signature::{Ed25519KeyPair, KeyPair};
use rtdb_stream_journal::authenticated::{
    Archive, CatalogAuthority, CatalogHistory, Certification, QuorumSignature, SignedHead,
    SignedStreamAuthority, StreamAuthority, Tree, VoterKeys,
};

use super::*;

const AUTHORITY: Uuid = Uuid::from_u128(3);
const SOURCE: Uuid = Uuid::from_u128(4);
const CONTROL: Uuid = Uuid::from_u128(5);
const PREDECESSOR: Binding = Binding {
    incarnation: Uuid::from_u128(6),
    revision: 1,
};
const SUCCESSOR: Binding = Binding {
    incarnation: Uuid::from_u128(7),
    revision: 2,
};
const STREAM: StreamId = StreamId(PREDECESSOR.incarnation.as_u128());
const DATABASE: &str = "lineagetest";

fn key(seed: u8) -> Ed25519KeyPair {
    Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap()
}

fn keeper(seed: u8) -> Uuid {
    Uuid::from_u128(seed.into())
}

fn archive(catalog: Uuid, stream: StreamId, leaves: impl IntoIterator<Item = Leaf>) -> Archive {
    let roster = VoterKeys::Stable(
        (1..=3)
            .map(|seed| {
                (
                    keeper(seed),
                    key(seed).public_key().as_ref().try_into().unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    );
    let sign = |statement: &[u8]| {
        Vec::from([1, 3].map(|seed| QuorumSignature {
            keeper: keeper(seed),
            signature: key(seed).sign(statement).as_ref().to_vec(),
        }))
    };
    let genesis = CatalogAuthority {
        catalog,
        revision: 0,
        predecessor_sha256: None,
        catalog_voters: roster.clone(),
    };
    let authority = StreamAuthority {
        catalog,
        stream,
        revision: 0,
        predecessor_sha256: None,
        catalog_sha256: genesis.digest(),
        voters: roster,
    };
    let mut tree = Tree::default();
    tree.extend(leaves).unwrap();
    let head = Head {
        catalog,
        stream,
        size: tree.size(),
        root: tree.root(),
        timestamp_micros: 100,
        authority_sha256: authority.digest(),
    };
    Archive {
        signed_head: SignedHead {
            head,
            signatures: sign(&head.statement()),
        },
        certification: Certification {
            history: CatalogHistory {
                genesis,
                transitions: Vec::new(),
            },
            stream: vec![SignedStreamAuthority {
                signatures: sign(&authority.statement()),
                authority,
            }],
        },
        tree,
    }
}

fn authorization(at: u64) -> Authorization {
    Authorization {
        catalog: SOURCE,
        stream: STREAM,
        writer_epoch: Epoch(1),
        at: Lsn(at),
        payload_sha256: [21; 32],
        timing: Timing {
            exclusive_cutoff_micros: 50,
            receiver_certificate_sha256: [22; 32],
        },
    }
}

fn leaf(at: u64) -> Leaf {
    let auth = authorization(at);
    Leaf {
        stream: STREAM,
        at: auth.at,
        writer_epoch: auth.writer_epoch,
        payload_sha256: auth.payload_sha256,
        quorum_durable_upper_micros: 49,
        timing: Some(auth.timing),
    }
}

fn activation_record(binding: Binding) -> serde_json::Value {
    serde_json::json!({
        "version": LINEAGE_PROTOCOL,
        "lineage_base_version": 2,
        "binding": binding,
        "grants_closed": false,
        "grants_end": null,
        "retired": [],
        "publication": null,
    })
}

fn cut(source: &Archive) -> AuthenticatedCut {
    AuthenticatedCut { head: source.signed_head.head, catalog_root: source.certification.history.genesis.root() }
}

fn segment(source: &Archive) -> LineageSegment {
    LineageSegment {
        authority: AUTHORITY,
        database: DATABASE.into(),
        predecessor: PREDECESSOR,
        successor: SUCCESSOR,
        source: cut(source),
        previous: Some([9; 32]),
    }
}

fn publication(source: &Archive) -> (PublicationProof, CatalogRoot) {
    publication_with(source, |_| ())
}

fn publication_with(source: &Archive, amend: impl FnOnce(&mut serde_json::Value)) -> (PublicationProof, CatalogRoot) {
    let control_stream = activation_stream_id(AUTHORITY, DATABASE).unwrap();
    let segment = segment(source);
    let mut state = activation_record(SUCCESSOR);
    state["publication"] = serde_json::json!({
        "operation": Uuid::from_u128(8),
        "predecessor": PREDECESSOR,
        "manifest_digest": Vec::from([1u8; 32]),
        "segment": segment,
    });
    amend(&mut state);
    let control_record = serde_json::to_vec(&state).unwrap();
    let control = archive(
        CONTROL,
        control_stream,
        [Leaf {
            stream: control_stream,
            at: Lsn(1),
            writer_epoch: Epoch(1),
            payload_sha256: Sha256::digest(&control_record).into(),
            quorum_durable_upper_micros: 70,
            timing: None,
        }],
    );
    (
        PublicationProof {
            control_record,
            control_inclusion: control.inclusion(Lsn(1)).unwrap(),
        },
        control.certification.history.genesis.root(),
    )
}

#[test]
fn lineage_capture_waits_for_publication() {
    let source = archive(SOURCE, STREAM, [leaf(1)]);
    let inclusion = source.inclusion(Lsn(1)).unwrap();
    let (publication, control_root) = publication(&source);
    let verify = |proof| {
        verify_settlement(
            AUTHORITY,
            DATABASE,
            control_root,
            source.certification.history.genesis.root(),
            authorization(1),
            proof,
            Some(&inclusion),
        )
        .unwrap()
    };
    assert_eq!(verify(None), Settlement::Pending);
    assert_eq!(verify(Some(&publication)), Settlement::Capture);
}

#[test]
fn selected_settlement_waits_for_evidence_and_refuses_a_substituted_cut() {
    let source = archive(SOURCE, STREAM, [leaf(1)]);
    let inclusion = source.inclusion(Lsn(1)).unwrap();
    let (publication, control_root) = publication(&source);
    let application_root = source.certification.history.genesis.root();
    let cut = SettlementCut { predecessor: PREDECESSOR, successor: SUCCESSOR, end: Lsn(1) };
    let verify = |cut, proof, inclusion| {
        verify_settlement_at(
            AUTHORITY, DATABASE, control_root, application_root, authorization(1),
            proof, inclusion, Some(cut),
        )
    };

    assert_eq!(verify(cut, None, Some(&inclusion)).unwrap(), Settlement::Pending);
    assert_eq!(verify(cut, Some(&publication), None).unwrap(), Settlement::Pending);
    assert_eq!(verify(cut, Some(&publication), Some(&inclusion)).unwrap(), Settlement::Capture);
    for changed in [
        SettlementCut { predecessor: Binding { revision: 3, ..PREDECESSOR },
            successor: Binding { revision: 4, ..SUCCESSOR }, ..cut },
        SettlementCut { successor: Binding { incarnation: Uuid::from_u128(9), ..SUCCESSOR }, ..cut },
        SettlementCut { end: Lsn(0), ..cut },
    ] {
        let error = verify(changed, Some(&publication), Some(&inclusion)).unwrap_err();
        assert_eq!(error.code, "rtdb/lineage-proof");
    }

    assert!(verify_settlement_at(
        AUTHORITY, DATABASE, application_root, application_root, authorization(1), None, None, Some(cut),
    ).is_err());
}

#[test]
fn lineage_leaves_a_matching_untimed_record_pending() {
    let source = archive(SOURCE, STREAM, [Leaf { timing: None, ..leaf(1) }]);
    let inclusion = source.inclusion(Lsn(1)).unwrap();
    let (publication, control_root) = publication(&source);
    assert_eq!(
        verify_settlement(
            AUTHORITY,
            DATABASE,
            control_root,
            source.certification.history.genesis.root(),
            authorization(1),
            Some(&publication),
            Some(&inclusion),
        )
        .unwrap(),
        Settlement::Pending,
    );
}

#[test]
fn lineage_rejects_conflicting_timing_for_a_matching_record() {
    let source = archive(
        SOURCE,
        STREAM,
        [Leaf {
            timing: Some(Timing {
                receiver_certificate_sha256: [23; 32],
                ..authorization(1).timing
            }),
            ..leaf(1)
        }],
    );
    let inclusion = source.inclusion(Lsn(1)).unwrap();
    let (publication, control_root) = publication(&source);
    let error = verify_settlement(
        AUTHORITY,
        DATABASE,
        control_root,
        source.certification.history.genesis.root(),
        authorization(1),
        Some(&publication),
        Some(&inclusion),
    )
    .unwrap_err();
    assert_eq!(error.code, "rtdb/lineage-proof");
}

#[test]
fn lineage_releases_an_authorized_position_filled_by_a_replacement() {
    let replacement = Leaf {
        writer_epoch: Epoch(2),
        timing: None,
        ..leaf(1)
    };
    let source = archive(SOURCE, STREAM, [replacement]);
    let inclusion = source.inclusion(Lsn(1)).unwrap();
    let (publication, control_root) = publication(&source);
    let verify = |inclusion| {
        verify_settlement(
            AUTHORITY,
            DATABASE,
            control_root,
            source.certification.history.genesis.root(),
            authorization(1),
            Some(&publication),
            inclusion,
        )
        .unwrap()
    };
    assert_eq!(verify(None), Settlement::Pending);
    assert_eq!(verify(Some(&inclusion)), Settlement::Release);
}

#[test]
fn lineage_releases_a_committed_append_discarded_by_a_lossy_cut() {
    let old_source = archive(SOURCE, STREAM, [leaf(1), leaf(2)]);
    let cut = archive(SOURCE, STREAM, [leaf(1)]);
    let discarded = old_source.inclusion(Lsn(2)).unwrap();
    let source_root = old_source.certification.history.genesis.root();
    assert_eq!(discarded.verify(source_root), Ok(&leaf(2)));
    let (publication, control_root) = publication(&cut);
    assert_eq!(
        verify_settlement(
            AUTHORITY,
            DATABASE,
            control_root,
            source_root,
            authorization(2),
            Some(&publication),
            Some(&discarded),
        )
        .unwrap(),
        Settlement::Release
    );
}

#[test]
fn lineage_rejects_a_lossy_cut_from_another_application_root() {
    let source = archive(SOURCE, STREAM, [leaf(1)]);
    let (publication, control_root) = publication(&source);
    assert!(
        verify_settlement(
            AUTHORITY,
            DATABASE,
            control_root,
            CatalogRoot([3; 32]),
            authorization(2),
            Some(&publication),
            None,
        )
        .is_err()
    );
}

#[test]
fn lineage_rejects_substituted_publication_and_application_head() {
    let source = archive(SOURCE, STREAM, [leaf(1)]);
    let different_head = archive(SOURCE, STREAM, [leaf(1), leaf(2)]);
    let unselected = different_head.inclusion(Lsn(1)).unwrap();
    let (mut publication, control_root) = publication(&source);
    assert!(
        verify_settlement(
            AUTHORITY,
            DATABASE,
            control_root,
            source.certification.history.genesis.root(),
            authorization(1),
            Some(&publication),
            Some(&unselected),
        )
        .is_err()
    );
    publication.control_record.push(b' ');
    assert!(
        publication
            .verify(AUTHORITY, DATABASE, control_root)
            .is_err()
    );
}

#[test]
fn lineage_proof_verifies_a_record_carrying_fields_of_a_later_release() {
    let source = archive(SOURCE, STREAM, [leaf(1)]);
    let (publication, control_root) = publication_with(&source, |state| {
        state["later"] = serde_json::json!({ "field": true });
        state["publication"]["later"] = serde_json::json!(1);
    });
    let segment = publication.verify(AUTHORITY, DATABASE, control_root).unwrap();
    assert_eq!(segment.successor, SUCCESSOR);
}

fn floor_publication(source: &Archive) -> (PublicationProof, CatalogRoot) {
    let control_stream = activation_stream_id(AUTHORITY, DATABASE).unwrap();
    let floor = LineageFloor {
        binding: PREDECESSOR,
        source: cut(source),
    };
    let mut state = activation_record(PREDECESSOR);
    state["floor"] = serde_json::to_value(floor).unwrap();
    let control_record = serde_json::to_vec(&state).unwrap();
    let control = archive(
        CONTROL,
        control_stream,
        [Leaf {
            stream: control_stream,
            at: Lsn(1),
            writer_epoch: Epoch(1),
            payload_sha256: Sha256::digest(&control_record).into(),
            quorum_durable_upper_micros: 70,
            timing: None,
        }],
    );
    (
        PublicationProof {
            control_record,
            control_inclusion: control.inclusion(Lsn(1)).unwrap(),
        },
        control.certification.history.genesis.root(),
    )
}

#[test]
fn lineage_floor_captures_a_serving_incarnations_included_append() {
    let source = archive(SOURCE, STREAM, [leaf(1)]);
    let inclusion = source.inclusion(Lsn(1)).unwrap();
    let (floor, control_root) = floor_publication(&source);
    let verify = |authorization, inclusion| {
        verify_settlement(
            AUTHORITY, DATABASE, control_root, source.certification.history.genesis.root(), authorization, Some(&floor), inclusion,
        )
        .unwrap()
    };
    assert_eq!(verify(authorization(1), None), Settlement::Pending);
    assert_eq!(verify(authorization(1), Some(&inclusion)), Settlement::Capture);
    assert_eq!(verify(authorization(2), None), Settlement::Pending, "an open floor still grows");
    assert!(floor.verify(AUTHORITY, DATABASE, control_root).is_err());
}

#[test]
fn lineage_floor_releases_a_position_it_filled_with_another_append() {
    let replacement = Leaf { writer_epoch: Epoch(2), timing: None, ..leaf(1) };
    let source = archive(SOURCE, STREAM, [replacement]);
    let inclusion = source.inclusion(Lsn(1)).unwrap();
    let (floor, control_root) = floor_publication(&source);
    let application_root = source.certification.history.genesis.root();
    assert_eq!(
        verify_settlement(
            AUTHORITY, DATABASE, control_root, application_root, authorization(1), Some(&floor), Some(&inclusion),
        )
        .unwrap(),
        Settlement::Release
    );
    let cut = SettlementCut { predecessor: PREDECESSOR, successor: SUCCESSOR, end: Lsn(1) };
    assert_eq!(
        verify_settlement_at(
            AUTHORITY, DATABASE, control_root, application_root, authorization(1), Some(&floor), Some(&inclusion),
            Some(cut),
        )
        .unwrap_err()
        .code,
        "rtdb/lineage-proof"
    );
}

#[test]
fn authorization_reads_a_full_width_stream_id() {
    let authorization = Authorization { stream: StreamId(1 << 100), ..authorization(1) };
    let json = serde_json::to_vec(&authorization).unwrap();
    assert_eq!(serde_json::from_slice::<Authorization>(&json).unwrap(), authorization);
}

fn published_segment(source: &Archive) -> serde_json::Value {
    let head = source.signed_head.head;
    serde_json::json!({
        "authority": AUTHORITY,
        "database": DATABASE,
        "predecessor": PREDECESSOR,
        "successor": SUCCESSOR,
        "source_catalog": head.catalog,
        "source_root": source.certification.history.genesis.root(),
        "source_stream": head.stream,
        "source_head": head,
        "end": head.size,
        "previous": Some([9u8; 32]),
    })
}

#[test]
fn segment_keeps_its_published_form_and_digest() {
    let source = archive(SOURCE, STREAM, [leaf(1), leaf(2)]);
    let published = published_segment(&source);
    let segment: LineageSegment = serde_json::from_value(published.clone()).unwrap();
    assert_eq!(segment, self::segment(&source));
    assert_eq!(segment.end(), Lsn(2));
    assert_eq!(serde_json::to_value(&segment).unwrap(), published);

    let head = source.signed_head.head;
    let mut digest = Sha256::new();
    digest.update(b"rtdb/activation/lineage-segment/v1\0");
    digest.update(AUTHORITY.as_bytes());
    digest.update((DATABASE.len() as u64).to_be_bytes());
    digest.update(DATABASE.as_bytes());
    for binding in [PREDECESSOR, SUCCESSOR] {
        digest.update(binding.incarnation.as_bytes());
        digest.update(binding.revision.to_be_bytes());
    }
    digest.update(head.catalog.as_bytes());
    digest.update(source.certification.history.genesis.root().0);
    digest.update(head.stream.0.to_be_bytes());
    digest.update(head.statement());
    digest.update(head.size.to_be_bytes());
    digest.update([1]);
    digest.update([9; 32]);
    assert_eq!(segment.digest(), <Hash>::from(digest.finalize()));
}

#[test]
fn segment_refuses_a_published_form_that_disagrees_with_its_head() {
    let source = archive(SOURCE, STREAM, [leaf(1)]);
    for (field, value) in [
        ("end", serde_json::json!(2)),
        ("source_stream", serde_json::json!(STREAM.0 + 1)),
        ("source_catalog", serde_json::json!(CONTROL)),
    ] {
        let mut published = published_segment(&source);
        published[field] = value;
        assert!(serde_json::from_value::<LineageSegment>(published).is_err(), "{field}");
    }
}

#[test]
fn floor_keeps_its_published_form() {
    let source = archive(SOURCE, STREAM, [leaf(1)]);
    let published = serde_json::json!({
        "binding": PREDECESSOR,
        "source_root": source.certification.history.genesis.root(),
        "source_head": source.signed_head.head,
    });
    let floor: LineageFloor = serde_json::from_value(published.clone()).unwrap();
    assert_eq!(floor, LineageFloor { binding: PREDECESSOR, source: cut(&source) });
    assert_eq!(serde_json::to_value(&floor).unwrap(), published);
}
