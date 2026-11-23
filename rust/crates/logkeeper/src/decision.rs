//! Portable decision evidence for one physical journal ensemble.

use bytes::Bytes;
use prost::Message;
use rtdb_storage::attestation::{SigningKey, verifies};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

use crate::attestation::journal_id;
use crate::protocol::{before_guarded_cutoff, ensemble_identity};

const MAX_JOURNALS: usize = 31;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct DecisionScope {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    #[prost(string, repeated, tag = "3")]
    pub journals: Vec<String>,
    #[prost(uint32, tag = "4")]
    pub quorum: u32,
}

impl DecisionScope {
    pub(crate) fn ensemble(&self) -> Option<[u8; 32]> {
        let quorum = self.quorum as usize;
        if self.stream.is_empty() || self.epoch == 0 || self.journals.len() > MAX_JOURNALS
            || quorum <= self.journals.len() / 2 || quorum > self.journals.len()
            || !self.journals.windows(2).all(|pair| pair[0] < pair[1])
            || !self.journals.iter().all(|id| rtdb_types::valid_journal_identity(id))
        {
            return None;
        }
        Some(ensemble_identity(true, u64::from(self.quorum), self.journals.iter().map(String::as_str)))
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct DecisionPrefix {
    #[prost(sint64, tag = "1")]
    pub predecessor: i64,
    #[prost(bytes = "bytes", tag = "2")]
    pub predecessor_digest: Bytes,
    #[prost(sint64, tag = "3")]
    pub through: i64,
    #[prost(bytes = "bytes", tag = "4")]
    pub digest: Bytes,
    #[prost(bool, tag = "5")]
    pub guarded: bool,
    #[prost(sfixed64, optional, tag = "6")]
    pub guarded_before: Option<i64>,
}

impl DecisionPrefix {
    fn valid(&self) -> bool {
        self.predecessor >= -1 && self.through > self.predecessor && self.through < i64::MAX
            && self.digest.len() == 32 && self.predecessor_digest.len() == 32
            && (self.guarded || self.guarded_before.is_none())
            && self.guarded_before.is_none_or(|cutoff| cutoff > 0)
            && (!self.guarded || self.predecessor.checked_add(1) == Some(self.through))
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct DecisionStatement {
    #[prost(message, required, tag = "1")]
    pub scope: DecisionScope,
    #[prost(message, required, tag = "2")]
    pub prefix: DecisionPrefix,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub(crate) struct DecisionVoteRequest {
    #[prost(message, required, tag = "1")]
    pub statement: DecisionStatement,
    #[prost(message, optional, tag = "2")]
    pub previous: Option<StableDecision>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct DecisionVote {
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    pub signature: Bytes,
    #[prost(sfixed64, optional, tag = "3")]
    pub original_upper: Option<i64>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct DecisionCertificate {
    #[prost(message, required, tag = "1")]
    pub statement: DecisionStatement,
    #[prost(message, repeated, tag = "2")]
    pub votes: Vec<DecisionVote>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct DecisionInstallation {
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    pub signature: Bytes,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct StableDecision {
    #[prost(message, required, tag = "1")]
    pub certificate: DecisionCertificate,
    #[prost(message, repeated, tag = "2")]
    pub installed: Vec<DecisionInstallation>,
}

fn signed_bytes(domain: &[u8], value: &impl Message) -> Vec<u8> {
    let mut message = Vec::with_capacity(domain.len() + value.encoded_len());
    message.extend_from_slice(domain);
    value.encode(&mut message).expect("encoding into a vector has enough capacity");
    message
}

impl DecisionStatement {
    fn vote_bytes(&self, original_upper: Option<i64>) -> Vec<u8> {
        let mut bytes = signed_bytes(b"rtdb.logkeeper/decision-vote/v1\0", self);
        match original_upper {
            Some(upper) => {
                bytes.push(1);
                bytes.extend_from_slice(&upper.to_be_bytes());
            }
            None => bytes.push(0),
        }
        bytes
    }

    fn installation_bytes(&self) -> Vec<u8> {
        signed_bytes(b"rtdb.logkeeper/decision-installation/v1\0", self)
    }

    pub(crate) fn valid(&self) -> bool {
        self.scope.ensemble().is_some() && self.prefix.valid()
    }

    pub(crate) fn vote(&self, signer: &SigningKey, original_upper: Option<i64>) -> DecisionVote {
        DecisionVote {
            key: Bytes::copy_from_slice(&signer.public_key()),
            signature: Bytes::copy_from_slice(&signer.sign(&self.vote_bytes(original_upper))),
            original_upper,
        }
    }

    pub(crate) fn installation(&self, signer: &SigningKey) -> DecisionInstallation {
        DecisionInstallation {
            key: Bytes::copy_from_slice(&signer.public_key()),
            signature: Bytes::copy_from_slice(&signer.sign(&self.installation_bytes())),
        }
    }

    pub(crate) fn origin_digest(&self) -> [u8; 32] {
        crate::history::RetainedChain::new(&self.scope.stream, self.prefix.predecessor + 1).bytes()
    }

    pub fn fingerprint(&self) -> [u8; 32] {
        Sha256::digest(signed_bytes(b"rtdb.logkeeper/decision/v1\0", self)).into()
    }

    fn verify_signature(&self, key: &Bytes, signature: &Bytes, message: &[u8]) -> Option<String> {
        let key = <&[u8; 32]>::try_from(key.as_ref()).ok()?;
        let signature = <&[u8; 64]>::try_from(signature.as_ref()).ok()?;
        let id = journal_id(key).to_string();
        (self.scope.journals.binary_search(&id).is_ok() && verifies(key, message, signature))
            .then_some(id)
    }

    pub(crate) fn verify_vote(&self, vote: &DecisionVote) -> Option<String> {
        let admissible = match (self.prefix.guarded, vote.original_upper) {
            (false, None) => true,
            (true, Some(upper)) => before_guarded_cutoff(upper, self.prefix.guarded_before),
            _ => false,
        };
        if !admissible { return None; }
        self.verify_signature(&vote.key, &vote.signature, &self.vote_bytes(vote.original_upper))
    }

    pub(crate) fn verify_installation(&self, install: &DecisionInstallation) -> Option<String> {
        self.verify_signature(&install.key, &install.signature, &self.installation_bytes())
    }
}

impl DecisionCertificate {
    pub fn authentic(&self) -> bool {
        if !self.statement.valid() || self.votes.len() != self.statement.scope.quorum as usize {
            return false;
        }
        let mut voters = BTreeSet::new();
        self.votes.iter().all(|vote| self.statement.verify_vote(vote).is_some_and(|id| voters.insert(id)))
    }
}

impl StableDecision {
    pub(crate) fn observe(&self, ensemble: &[u8]) {
        use rtdb_types::residence_trace::{CertifiedDecision, logkeeper_decision};
        logkeeper_decision(|| {
            let statement = &self.certificate.statement;
            let journal = |key: &Bytes| journal_id(key.as_ref().try_into().expect("an authenticated journal key")).to_string();
            CertifiedDecision {
                stream: &statement.scope.stream, epoch: statement.scope.epoch, ensemble,
                through: statement.prefix.through, predecessor: statement.prefix.predecessor,
                decision: statement.fingerprint(), statement: statement.encode_to_vec(),
                prefix_digest: &statement.prefix.digest, predecessor_digest: &statement.prefix.predecessor_digest,
                roster: &statement.scope.journals, quorum: statement.scope.quorum,
                voters: self.certificate.votes.iter().map(|vote| journal(&vote.key)).collect(),
                installers: self.installed.iter().map(|install| journal(&install.key)).collect(),
            }
        });
    }

    pub fn authentic(&self) -> bool {
        if !self.certificate.authentic()
            || self.installed.len() != self.certificate.statement.scope.quorum as usize
        {
            return false;
        }
        let mut installers = BTreeSet::new();
        self.installed.iter().all(|install| {
            self.certificate.statement.verify_installation(install).is_some_and(|id| installers.insert(id))
        })
    }
}
