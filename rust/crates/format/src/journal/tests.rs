use super::*;

fn object(kind: ObjectKind) -> ObjectId {
    ObjectId {
        database: Uuid::from_u128(7),
        kind,
        epoch: u64::MAX,
        seq: 42,
        digest: [0xab; 32],
    }
}

#[test]
fn record_kinds_round_trip_with_fixed_tags() {
    let manifest = object(ObjectKind::Manifest);
    let pin = Pin {
        id: Uuid::from_u128(42),
        manifest,
        until: Some(0),
    };
    let records = [
        Record::Tx {
            payload: vec![2, 4, 8],
            objects: vec![object(ObjectKind::File)],
        },
        Record::Checkpoint(Checkpoint {
            manifest,
            through: 9,
            key_generation: 2,
            restore: true,
        }),
        Record::Lease { until: i64::MIN },
        Record::Pin(pin.clone()),
        Record::Unpin { id: pin.id },
        Record::KeyRotate {
            generation: 3,
            at: u64::MAX,
        },
        Record::Settle { tx_lsn: 1 },
        Record::Refuse { tx_lsn: u64::MAX },
        Record::KeyRetirement {
            generation: 3,
            rotation_lsn: u64::MAX,
            observed_at: i64::MIN,
        },
    ];
    for (i, record) in records.into_iter().enumerate() {
        let encoded = record.encode().unwrap();
        assert_eq!(encoded[RECORD_HEADER.len()], ((i as u8 + 1) << 3) | 2);
        assert_eq!(Record::decode(&encoded).unwrap(), record);
    }
}

#[test]
fn malformed_record_envelopes_are_refused() {
    let mut bytes = Record::Lease { until: 10 }.encode().unwrap();
    for end in 0..bytes.len() {
        assert!(Record::decode(&bytes[..end]).is_err());
    }
    bytes[RECORD_HEADER.len() - 1] += 1;
    assert!(Record::decode(&bytes).is_err());

    let mut unknown = RECORD_HEADER.to_vec();
    unknown.extend_from_slice(&[0x52, 0]);
    assert!(Record::decode(&unknown).is_err());
    assert!(
        Record::Checkpoint(Checkpoint {
            manifest: object(ObjectKind::File),
            through: 3,
            key_generation: 1,
            restore: false,
        })
        .encode()
        .is_err()
    );
}

#[test]
fn manifest_round_trips_complete_file_and_pin_state() {
    let manifest = Manifest {
        files: vec![object(ObjectKind::File)],
        pins: vec![Pin {
            id: Uuid::from_u128(42),
            manifest: object(ObjectKind::Manifest),
            until: None,
        }],
        payload: vec![1, 2, 3],
    };
    let encoded = manifest.encode().unwrap();
    assert_eq!(Manifest::decode(&encoded).unwrap(), manifest);
    assert!(Record::decode(&encoded).is_err());
}

#[test]
fn key_parsing_accepts_only_canonical_paths() {
    let id = object(ObjectKind::File);
    let key = id.to_string();
    assert_eq!(key.parse::<ObjectId>().unwrap(), id);
    for key in [
        key.replace("/42/", "/042/"),
        key.replace("ab", "AB"),
        format!("{key}/suffix"),
        key.replace("/file/", "/other/"),
    ] {
        assert!(key.parse::<ObjectId>().is_err(), "{key}");
    }
}
