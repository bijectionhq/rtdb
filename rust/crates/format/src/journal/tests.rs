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
            recovery_epoch: 7,
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
            unreadable_by: Some(i64::MAX),
            armed: true,
        },
        Record::WriterRelease { lease_until: i64::MAX },
        Record::DeploymentRetirement { lease_until: i64::MAX },
        Record::RankedCatalog(RankedCatalog {
            manifest,
            position: 8,
            system_time: i64::MIN,
            key_generation: 2,
        }),
        Record::Recovered {
            head: u64::MAX,
            epoch: 3,
            time: i64::MIN,
        },
        Record::Commissioned,
        Record::StorePurgeFloor {
            since: Basis::new([0xab; 16], u64::MAX),
            keep_from: u64::MAX - 1,
        },
        Record::StorePurgeReclaim {
            since: Basis::new([0xab; 16], u64::MAX),
            keep_from: u64::MAX - 1,
            clock: StorePurgeClock { census_micros: 7, lower_bound_micros: 8 },
        },
        Record::StoreHold {
            change: StoreHoldChange::Retain(OrderedStoreHold {
                basis: Basis::new([0xab; 16], 12), id: [1; 16], reader: [2; 32], expires_at_micros: 25,
            }),
            lower_bound_micros: 4,
        },
    ];
    for (i, record) in records.into_iter().enumerate() {
        let encoded = record.encode().unwrap();
        let tag = ((i as u8 + 1) << 3) | 2;
        if tag >= 128 {
            assert_eq!(&encoded[RECORD_HEADER.len()..][..2], &[tag, 1]);
        } else {
            assert_eq!(encoded[RECORD_HEADER.len()], tag);
        }
        assert_eq!(Record::decode(&encoded).unwrap(), record);
    }
}

#[test]
fn store_purge_census_requires_ordered_clock_coverage() {
    let since = Basis::new([0xab; 16], 8);
    let record = |census_micros, lower_bound_micros| Record::StorePurgeReclaim {
        since, keep_from: 7, clock: StorePurgeClock { census_micros, lower_bound_micros },
    };
    assert!(record(7, 6).encode().is_err());
    assert!(record(-1, 6).encode().is_err());
    let before_first_sample = record(0, -1);
    assert_eq!(Record::decode(&before_first_sample.encode().unwrap()).unwrap(), before_first_sample);
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
    unknown.extend_from_slice(&[0x92, 0x01, 0]);
    assert!(Record::decode(&unknown).is_err());
    let mut missing_floor_basis = RECORD_HEADER.to_vec();
    missing_floor_basis.extend_from_slice(&[0x7a, 0]);
    assert!(Record::decode(&missing_floor_basis).is_err());
    assert!(
        Record::Checkpoint(Checkpoint {
            manifest: object(ObjectKind::File),
            through: 3,
            key_generation: 1,
            restore: false,
            recovery_epoch: 0,
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
        store_holds: vec![OrderedStoreHold { basis: Basis::new([0xab; 16], 12), id: [1; 16], reader: [2; 32], expires_at_micros: 25 }],
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
