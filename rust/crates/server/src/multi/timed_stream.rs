//! Installed trust and proof conversion for the native StreamJournal.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{self, Read},
    path::Path,
    sync::Arc,
    time::Duration,
};

use journal_contract::JournalError;
use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_stream_journal::{Credentials, KeeperId, StreamJournal, TimedOriginalVerifier};
use rtdb_types::Timestamp;
use serde::Deserialize;
use uuid::Uuid;

const MAX_TRUST_BYTES: u64 = 64 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustConfiguration {
    keepers: Vec<TrustedKeeper>,
    maximum_payload_bytes: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustedKeeper {
    identity: Uuid,
    verification_key: [u8; 32],
}

struct Trust(BTreeMap<u128, [u8; 32]>);

impl TimedOriginalVerifier for Trust {
    fn verify(&self, keeper: KeeperId, statement: &[u8], signature: &[u8; 64]) -> bool {
        self.0.get(&keeper.0).is_some_and(|key| {
            UnparsedPublicKey::new(&ED25519, key)
                .verify(statement, signature)
                .is_ok()
        })
    }
}

pub(super) async fn connect(
    url: &str,
    credentials: &Credentials,
    deadline: Duration,
) -> io::Result<StreamJournal> {
    let Some(path) = std::env::var_os("RTDB_TIMED_ORIGINAL_TRUST_FILE") else {
        return StreamJournal::connect(url, credentials, deadline).await;
    };
    let path = Path::new(&path);
    if !path.is_absolute() {
        return Err(invalid_trust());
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_TRUST_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_TRUST_BYTES {
        return Err(invalid_trust());
    }
    let config: TrustConfiguration = serde_json::from_slice(&bytes).map_err(|_| invalid_trust())?;
    let mut keys = BTreeMap::new();
    let mut authorities = BTreeSet::new();
    for keeper in config.keepers {
        if keeper.identity.is_nil()
            || keeper.verification_key == [0; 32]
            || !authorities.insert(keeper.verification_key)
            || keys
                .insert(keeper.identity.as_u128(), keeper.verification_key)
                .is_some()
        {
            return Err(invalid_trust());
        }
    }
    if keys.len() < 2 || config.maximum_payload_bytes == Some(0) {
        return Err(invalid_trust());
    }
    StreamJournal::connect_with_timed_original(
        url,
        credentials,
        deadline,
        Arc::new(Trust(keys)),
        config.maximum_payload_bytes,
    )
    .await
}

fn invalid_trust() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid native journal timed-original trust",
    )
}

pub(super) fn request(original: rtdb_db::TimedOriginal) -> rtdb_stream_journal::TimedOriginal {
    rtdb_stream_journal::TimedOriginal {
        stream: original.stream,
        epoch: original.epoch,
        at: original.at,
        payload_sha256: original.payload_sha256,
        exclusive_journal_cutoff_micros: original.cutoff.before().0,
        original_receiver_certificate_sha256: original.cutoff.certificate_sha256(),
    }
}

pub(super) fn proof(
    original: rtdb_db::TimedOriginal,
    proof: rtdb_stream_journal::TimedOriginalProof,
) -> journal_contract::Result<rtdb_db::TimedOriginalProof> {
    if proof.original != request(original) {
        return Err(JournalError::Unavailable);
    }
    Ok(rtdb_db::TimedOriginalProof {
        original,
        quorum_sync_at: Timestamp(proof.original_quorum_sync_upper_micros),
        evidence_sha256: proof.verified_evidence_sha256,
    })
}
