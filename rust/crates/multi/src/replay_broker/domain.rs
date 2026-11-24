//! Admission and drain records held by the credential-separated custody owner.

use std::collections::BTreeMap;

use bytes::Bytes;
use ring::{
    digest::{SHA256, digest},
    signature::{ED25519, UnparsedPublicKey},
};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::History;
use crate::journal::hosted::erasure::CustodyWork;

const MAX_RECORD_BYTES: u64 = 1 << 20;
const MAX_ATTEMPTS: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Root {
    pub history: [u8; 32],
    pub verification_key: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Duty {
    pub root: Uuid,
    pub history: [u8; 32],
    pub kind: CustodyWork,
    pub verification_key: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    pub domain: Uuid,
    pub generation: u64,
    pub erase: Uuid,
    pub target: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootDrain {
    pub original_head: [u8; 32],
    pub successor: [u8; 32],
    pub ordered_switch: [u8; 32],
    pub joined_custody: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedRootDrain {
    pub drain: RootDrain,
    pub signature: Vec<u8>,
}

impl RootDrain {
    pub fn signing_bytes(
        self,
        operation: Operation,
        root: Uuid,
        history: [u8; 32],
    ) -> Result<Vec<u8>> {
        statement(
            b"rtdb.custody/root-drain/v1\0",
            &(operation, root, history, self),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootDestruction {
    pub domain_drain: [u8; 32],
    pub provider_disposition: [u8; 32],
    pub signature: Vec<u8>,
}

impl RootDestruction {
    pub fn signing_bytes(
        &self,
        operation: Operation,
        root: Uuid,
        history: [u8; 32],
    ) -> Result<Vec<u8>> {
        statement(
            b"rtdb.custody/root-destruction/v1\0",
            &(
                operation,
                root,
                history,
                self.domain_drain,
                self.provider_disposition,
            ),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Completion {
    pub operation: Operation,
    pub evidence: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkJoin {
    pub observation: [u8; 32],
    pub signature: Vec<u8>,
}

impl WorkJoin {
    pub fn signing_bytes(
        domain: Uuid,
        generation: u64,
        duty_id: Uuid,
        duty: Duty,
        observation: [u8; 32],
    ) -> Result<Vec<u8>> {
        statement(
            b"rtdb.custody/work-joined/v1\0",
            &(domain, generation, duty_id, duty, observation),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Work {
    duty: Duty,
    join: Option<WorkJoin>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    domain: Uuid,
    generation: u64,
    revision: u64,
    roots: BTreeMap<Uuid, Root>,
    duties: BTreeMap<Uuid, Work>,
    operation: Option<Operation>,
    root_drains: BTreeMap<Uuid, SignedRootDrain>,
    destruction: BTreeMap<Uuid, RootDestruction>,
    last_completed: Option<Completion>,
}

impl State {
    fn new(domain: Uuid) -> Self {
        Self {
            domain,
            generation: 1,
            revision: 0,
            roots: BTreeMap::new(),
            duties: BTreeMap::new(),
            operation: None,
            root_drains: BTreeMap::new(),
            destruction: BTreeMap::new(),
            last_completed: None,
        }
    }

    fn require_open(&self, generation: u64) -> Result<()> {
        if generation != self.generation || self.operation.is_some() {
            return Err(conflict("The custody generation no longer admits work"));
        }
        Ok(())
    }

    fn view(&self) -> Result<View> {
        let membership = fingerprint(b"rtdb.custody/domain-roots/v1\0", &self.roots)?;
        let roster = self
            .duties
            .iter()
            .map(|(id, work)| (*id, work.duty))
            .collect::<BTreeMap<_, _>>();
        let roster = fingerprint(b"rtdb.custody/domain-roster/v1\0", &roster)?;
        let drained = match self.operation {
            Some(operation)
                if self.root_drains.len() == self.roots.len()
                    && self.duties.values().all(|work| work.join.is_some()) =>
            {
                Some(fingerprint(
                    b"rtdb.custody/domain-drain/v1\0",
                    &(
                        operation,
                        membership,
                        roster,
                        &self.root_drains,
                        &self.duties,
                    ),
                )?)
            }
            _ => None,
        };
        Ok(View {
            generation: self.generation,
            operation: self.operation,
            membership,
            roster,
            drained,
            last_completed: self.last_completed,
        })
    }

    fn apply(&mut self, action: &Action) -> Result<()> {
        match action {
            Action::Status => (),
            Action::Enroll {
                generation,
                root,
                enrollment,
            } => {
                if let Some(prior) = self
                    .roots
                    .get(root)
                    .filter(|_| *generation == self.generation)
                {
                    return if prior == enrollment {
                        Ok(())
                    } else {
                        Err(conflict("A different root history is enrolled"))
                    };
                }
                self.require_open(*generation)?;
                if root.is_nil()
                    || enrollment.history == [0; 32]
                    || enrollment.verification_key == [0; 32]
                {
                    return Err(conflict("A root needs an exact history and custody signer"));
                }
                self.roots.insert(*root, *enrollment);
            }
            Action::Begin {
                operation,
                target,
                generation,
            } => {
                let closing = Operation {
                    domain: self.domain,
                    generation: *generation,
                    erase: *operation,
                    target: *target,
                };
                if self.operation == Some(closing) {
                    return Ok(());
                }
                self.require_open(*generation)?;
                if self.roots.is_empty() || operation.is_nil() || *target == [0; 32] {
                    return Err(conflict(
                        "Erasure requires enrolled roots and an exact target",
                    ));
                }
                self.operation = Some(closing);
            }
            Action::Admit {
                generation,
                id,
                duty,
            } => {
                self.require_open(*generation)?;
                if id.is_nil()
                    || self
                        .roots
                        .get(&duty.root)
                        .is_none_or(|root| root.history != duty.history)
                    || duty.verification_key == [0; 32]
                {
                    return Err(conflict(
                        "Work must name an enrolled root and its drain signer",
                    ));
                }
                if let Some(prior) = self.duties.get(id) {
                    return if prior.duty == *duty {
                        Ok(())
                    } else {
                        Err(conflict("The duty identifier is already in use"))
                    };
                }
                self.duties.insert(
                    *id,
                    Work {
                        duty: *duty,
                        join: None,
                    },
                );
            }
            Action::Join {
                generation,
                id,
                join,
            } => {
                if self.generation != *generation || join.observation == [0; 32] {
                    return Err(conflict("The drain observation belongs to different work"));
                }
                let Some(work) = self.duties.get_mut(id) else {
                    return Ok(());
                };
                let bytes = WorkJoin::signing_bytes(
                    self.domain,
                    *generation,
                    *id,
                    work.duty,
                    join.observation,
                )?;
                verify(work.duty.verification_key, &bytes, &join.signature)?;
                if self.operation.is_none() {
                    // Finished work leaves the open roster. A racing closure
                    // either includes its signed observation or sees it absent.
                    self.duties.remove(id);
                } else if let Some(prior) = &work.join {
                    if prior != join {
                        return Err(conflict(
                            "This work already has a different drain observation",
                        ));
                    }
                } else {
                    work.join = Some(join.clone());
                }
            }
            Action::RootDrain {
                operation,
                root,
                evidence,
            } => {
                if self.operation != Some(*operation) {
                    return Err(conflict(
                        "The root cut does not belong to the closing operation",
                    ));
                }
                let enrolled = self
                    .roots
                    .get(root)
                    .ok_or_else(|| conflict("The root is outside the closed membership"))?;
                let drain = evidence.drain;
                if [
                    drain.original_head,
                    drain.successor,
                    drain.ordered_switch,
                    drain.joined_custody,
                ]
                .contains(&[0; 32])
                {
                    return Err(conflict("The root lacks exact switch and drain evidence"));
                }
                verify(
                    enrolled.verification_key,
                    &drain.signing_bytes(*operation, *root, enrolled.history)?,
                    &evidence.signature,
                )?;
                if let Some(prior) = self.root_drains.get(root) {
                    return if prior == evidence {
                        Ok(())
                    } else {
                        Err(conflict(
                            "The root already supplied different erasure evidence",
                        ))
                    };
                }
                self.root_drains.insert(*root, evidence.clone());
            }
            Action::RootDestruction {
                operation,
                root,
                evidence,
            } => {
                if self.operation != Some(*operation)
                    || self.view()?.drained != Some(evidence.domain_drain)
                    || evidence.provider_disposition == [0; 32]
                {
                    return Err(conflict(
                        "Provider destruction requires the complete domain drain",
                    ));
                }
                let enrolled = self
                    .roots
                    .get(root)
                    .ok_or_else(|| conflict("The root is outside the closed membership"))?;
                verify(
                    enrolled.verification_key,
                    &evidence.signing_bytes(*operation, *root, enrolled.history)?,
                    &evidence.signature,
                )?;
                if let Some(prior) = self.destruction.get(root) {
                    return if prior == evidence {
                        Ok(())
                    } else {
                        Err(conflict(
                            "The root already supplied a different provider result",
                        ))
                    };
                }
                self.destruction.insert(*root, evidence.clone());
            }
            Action::Complete { operation } => {
                if self
                    .last_completed
                    .is_some_and(|completion| completion.operation == *operation)
                {
                    return Ok(());
                }
                if self.operation != Some(*operation) || self.destruction.len() != self.roots.len()
                {
                    return Err(conflict(
                        "The provider has not settled every root in the domain",
                    ));
                }
                let evidence = fingerprint(
                    b"rtdb.custody/domain-destruction/v1\0",
                    &(*operation, &self.destruction),
                )?;
                self.last_completed = Some(Completion {
                    operation: *operation,
                    evidence,
                });
                self.generation = self
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| conflict("The custody generation space is exhausted"))?;
                self.operation = None;
                self.duties.clear();
                self.root_drains.clear();
                self.destruction.clear();
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Status,
    Enroll {
        generation: u64,
        root: Uuid,
        enrollment: Root,
    },
    Begin {
        operation: Uuid,
        target: [u8; 32],
        generation: u64,
    },
    Admit {
        generation: u64,
        id: Uuid,
        duty: Duty,
    },
    Join {
        generation: u64,
        id: Uuid,
        join: WorkJoin,
    },
    RootDrain {
        operation: Operation,
        root: Uuid,
        evidence: SignedRootDrain,
    },
    RootDestruction {
        operation: Operation,
        root: Uuid,
        evidence: RootDestruction,
    },
    Complete {
        operation: Operation,
    },
}

impl Action {
    pub(super) fn requires_custodian(&self) -> bool {
        matches!(
            self,
            Self::Enroll { .. }
                | Self::Begin { .. }
                | Self::Admit { .. }
                | Self::RootDrain { .. }
                | Self::RootDestruction { .. }
                | Self::Complete { .. }
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct View {
    pub generation: u64,
    pub operation: Option<Operation>,
    pub membership: [u8; 32],
    pub roster: [u8; 32],
    pub drained: Option<[u8; 32]>,
    pub last_completed: Option<Completion>,
}

/// The directory domain is scoped to the original broker namespace and lineage.
pub fn directory_domain(namespace: [u8; 32], organisation: &str, lineage: &str) -> Result<Uuid> {
    let identity = fingerprint(
        b"rtdb.custody/directory-domain/v1\0",
        &(namespace, organisation, lineage),
    )?;
    Ok(Uuid::from_bytes(
        identity[..16].try_into().expect("UUID width"),
    ))
}

pub(super) fn directory_history(history: History) -> Result<[u8; 32]> {
    fingerprint(b"rtdb.custody/directory-root/v1\0", &history)
}

fn verify(key: [u8; 32], message: &[u8], signature: &[u8]) -> Result<()> {
    UnparsedPublicKey::new(&ED25519, key)
        .verify(message, signature)
        .map_err(|_| conflict("The enrolled custody signer did not attest this evidence"))
}

fn statement<T: Serialize>(tag: &[u8], payload: &T) -> Result<Vec<u8>> {
    let mut bytes = tag.to_vec();
    serde_json::to_writer(&mut bytes, payload)
        .map_err(|error| Anomaly::fault("rtdb.multi/custody-domain-encoding", error.to_string()))?;
    Ok(bytes)
}

fn fingerprint<T: Serialize>(tag: &[u8], payload: &T) -> Result<[u8; 32]> {
    Ok(digest(&SHA256, &statement(tag, payload)?)
        .as_ref()
        .try_into()
        .expect("SHA-256 width"))
}

pub(super) fn conflict(message: &str) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.multi/custody-domain",
        message,
    )
}

fn record_key(domain: Uuid, revision: u64) -> Result<ObjectKey> {
    Ok(ObjectKey::new(format!(
        "~custody-domains/{domain}/{revision:020}"
    ))?)
}

async fn exists(store: &dyn ObjectStore, domain: Uuid, revision: u64) -> Result<bool> {
    match store.head_fresh(&record_key(domain, revision)?).await {
        Ok(_) => Ok(true),
        Err(Error::MissingObject(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn load(store: &dyn ObjectStore, domain: Uuid) -> Result<State> {
    // Compare-and-create successors leave a contiguous immutable sequence.
    // Probe it directly so a long-running domain need not list its history.
    let mut lower = 0;
    let mut upper: u64 = 1;
    while exists(store, domain, upper).await? {
        lower = upper;
        upper = upper.checked_mul(2).unwrap_or(u64::MAX);
        if lower == upper {
            return Err(conflict("The domain revision space is exhausted"));
        }
    }
    while upper - lower > 1 {
        let middle = lower + (upper - lower) / 2;
        if exists(store, domain, middle).await? {
            lower = middle;
        } else {
            upper = middle;
        }
    }
    if lower == 0 {
        return Ok(State::new(domain));
    }
    let key = record_key(domain, lower)?;
    let size = store.head_fresh(&key).await?.size;
    if size > MAX_RECORD_BYTES {
        return Err(conflict("The domain record exceeds its reservation"));
    }
    let bytes = store.get_fresh_exact(&key, size as usize).await?;
    let state: State = serde_json::from_slice(&bytes)
        .map_err(|error| Anomaly::fault("rtdb.multi/custody-domain-encoding", error.to_string()))?;
    if state.domain != domain || state.revision != lower || state.generation == 0 {
        return Err(conflict("The domain ledger returned a different revision"));
    }
    Ok(state)
}

pub(super) async fn execute(store: &dyn ObjectStore, domain: Uuid, action: Action) -> Result<View> {
    if domain.is_nil() {
        return Err(conflict("The custody domain has no identity"));
    }
    for _ in 0..MAX_ATTEMPTS {
        let state = load(store, domain).await?;
        let mut next = state.clone();
        next.apply(&action)?;
        if next == state {
            return next.view();
        }
        next.revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| conflict("The domain revision space is exhausted"))?;
        let key = record_key(domain, next.revision)?;
        let bytes = serde_json::to_vec(&next).map_err(|error| {
            Anomaly::fault("rtdb.multi/custody-domain-encoding", error.to_string())
        })?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(conflict(
                "The domain exceeded its enrolled custody reservation",
            ));
        }
        match store.put(&key, Bytes::from(bytes)).await {
            Ok(()) => return next.view(),
            Err(Error::ConflictingObject(_)) => (),
            Err(error) => return Err(error.into()),
        }
    }
    Err(Anomaly::new(
        AnomalyCategory::Busy,
        "rtdb.multi/custody-domain-contended",
        "The domain admission ledger kept changing",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    #[test]
    fn closure_requires_every_enrolled_root_and_every_admitted_duty() {
        let mut state = State::new(Uuid::new_v4());
        let signer = Ed25519KeyPair::from_seed_unchecked(&[17; 32]).unwrap();
        let verification_key = signer.public_key().as_ref().try_into().unwrap();
        let first = Uuid::new_v4();
        let dependent = Uuid::new_v4();
        let enrollment = Root {
            history: [18; 32],
            verification_key,
        };
        for root in [first, dependent] {
            state
                .apply(&Action::Enroll {
                    generation: 1,
                    root,
                    enrollment,
                })
                .unwrap();
        }
        let duty = Duty {
            root: dependent,
            history: enrollment.history,
            kind: CustodyWork::Copy,
            verification_key,
        };
        let id = Uuid::new_v4();
        state
            .apply(&Action::Admit {
                generation: 1,
                id,
                duty,
            })
            .unwrap();
        state
            .apply(&Action::Begin {
                operation: Uuid::new_v4(),
                target: [19; 32],
                generation: 1,
            })
            .unwrap();
        let operation = state.operation.unwrap();
        assert!(
            state
                .apply(&Action::Admit {
                    generation: 1,
                    id: Uuid::new_v4(),
                    duty
                })
                .is_err()
        );
        assert!(
            state
                .apply(&Action::Enroll {
                    generation: 1,
                    root: Uuid::new_v4(),
                    enrollment
                })
                .is_err()
        );

        let drain = RootDrain {
            original_head: [20; 32],
            successor: [21; 32],
            ordered_switch: [22; 32],
            joined_custody: [23; 32],
        };
        let signed = |root| SignedRootDrain {
            drain,
            signature: signer
                .sign(
                    &drain
                        .signing_bytes(operation, root, enrollment.history)
                        .unwrap(),
                )
                .as_ref()
                .to_vec(),
        };
        state
            .apply(&Action::RootDrain {
                operation,
                root: first,
                evidence: signed(first),
            })
            .unwrap();
        assert!(
            state
                .apply(&Action::RootDrain {
                    operation,
                    root: dependent,
                    evidence: signed(first)
                })
                .is_err()
        );
        let observation = [24; 32];
        let statement = WorkJoin::signing_bytes(state.domain, 1, id, duty, observation).unwrap();
        let join = WorkJoin {
            observation,
            signature: signer.sign(&statement).as_ref().to_vec(),
        };
        state
            .apply(&Action::Join {
                generation: 1,
                id,
                join,
            })
            .unwrap();
        assert_eq!(state.view().unwrap().drained, None);
        state
            .apply(&Action::RootDrain {
                operation,
                root: dependent,
                evidence: signed(dependent),
            })
            .unwrap();
        let domain_drain = state.view().unwrap().drained.unwrap();

        assert!(state.apply(&Action::Complete { operation }).is_err());
        for root in [first, dependent] {
            let mut evidence = RootDestruction {
                domain_drain,
                provider_disposition: [25; 32],
                signature: Vec::new(),
            };
            evidence.signature = signer
                .sign(
                    &evidence
                        .signing_bytes(operation, root, enrollment.history)
                        .unwrap(),
                )
                .as_ref()
                .to_vec();
            state
                .apply(&Action::RootDestruction {
                    operation,
                    root,
                    evidence,
                })
                .unwrap();
        }
        state.apply(&Action::Complete { operation }).unwrap();
        assert_eq!(state.view().unwrap().generation, 2);
        assert_eq!(
            state.view().unwrap().last_completed.unwrap().operation,
            operation
        );
    }
}
