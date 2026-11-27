use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundedOutcome {
    pub booking: [u8; 16],
    pub owner: [u8; 32],
    pub predecessor: Basis,
    pub intent_sha256: [u8; 32],
    pub appointment: bool,
    pub activation_lsn: Option<u64>,
    pub consumption_lsn: Option<u64>,
    pub evidence_sha256: [u8; 32],
}

impl FundedOutcome {
    fn valid(&self, source: [u8; 16], namespace: Option<[u8; 32]>) -> bool {
        source != [0; 16]
            && self.booking != [0; 16]
            && self.owner != [0; 32]
            && self.intent_sha256 != [0; 32]
            && self.evidence_sha256 != [0; 32]
            && self.predecessor.incarnation() != [0; 16]
            && match namespace {
                Some(namespace) => namespace == self.owner,
                None => self.predecessor.incarnation() == source,
            }
            && self
                .activation_lsn
                .is_none_or(|at| self.appointment && at != 0)
            && self.consumption_lsn.is_none_or(|at| {
                at != 0
                    && (!self.appointment
                        || self
                            .activation_lsn
                            .is_some_and(|activation| activation < at))
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundedSettlement {
    pub namespace: Option<[u8; 32]>,
    pub key: Vec<u8>,
    pub outcome: FundedOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalFunding {
    pub source: [u8; 16],
    pub original_receipt_basis: Option<Basis>,
    pub outcome: FundedOutcome,
}

/// Identity admitted at the serving incarnation before looking up a retired booking.
pub enum FundedStatus<'a> {
    Owner([u8; 32]),
    Scoped(&'a (dyn Fn([u8; 16]) -> [u8; 32] + Sync)),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetiredFunding {
    pub(super) history: HistoricalFunding,
    pub(super) appointment_address: Option<[u8; 32]>,
}

impl RetiredFunding {
    pub(super) fn valid(&self, stored: &StoredReceipt) -> bool {
        let receipt = &stored.receipt;
        let history = &self.history;
        history
            .outcome
            .valid(history.source, stored.receipt_namespace)
            && receipt.decision_clock.is_none()
            && receipt.erasure.is_none()
            && stored.installed.is_none()
            && stored.workload.is_none()
            && stored.workload_digest.is_none()
            && self
                .appointment_address
                .is_none_or(|address| address != [0; 32] && history.outcome.appointment)
            && history.original_receipt_basis.map_or_else(
                || {
                    history.outcome.activation_lsn.is_none()
                        && history.outcome.consumption_lsn.is_none()
                },
                |basis| {
                    basis.incarnation() == history.source
                        && basis.position() == receipt.basis.position()
                },
            )
            && matches!(receipt.outcome, Outcome::Refused { .. })
                == history.outcome.consumption_lsn.is_none()
    }
}

fn invalid_settlement() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/backup-funded-settlement",
        "The reconciled funded bookings must match the original checkpoint receipts",
    )
}

fn retired_outcome() -> Outcome {
    Outcome::Refused {
        error: Anomaly::new(
            AnomalyCategory::Conflict,
            "rtdb/funded-booking-closed",
            "The original issuer closed this booking during recovery",
        ),
    }
}

fn inventory_digest<'a>(
    source: [u8; 16],
    entries: impl Iterator<Item = (Option<[u8; 32]>, &'a [u8], &'a FundedOutcome)>,
) -> Result<Option<[u8; 32]>> {
    let mut digest = Sha256::new();
    digest.update(b"rtdb.backup.funded-inventory/v1\0");
    digest.update(source);
    let mut prior = None;
    for (namespace, key, outcome) in entries {
        let identity = (namespace, key);
        if prior.is_some_and(|prior| prior >= identity) || !outcome.valid(source, namespace) {
            return Err(invalid_settlement());
        }
        validate_key(key)?;
        prior = Some(identity);
        let encoded =
            serde_json::to_vec(&(namespace, key, outcome)).map_err(|_| invalid_settlement())?;
        digest.update((encoded.len() as u64).to_be_bytes());
        digest.update(encoded);
    }
    Ok(prior.map(|_| digest.finalize().into()))
}

/// Binds the complete reconciled inventory in namespace and key order to a source incarnation.
pub fn funded_settlement_digest(
    source: [u8; 16],
    settlements: &[FundedSettlement],
) -> Result<Option<[u8; 32]>> {
    inventory_digest(
        source,
        settlements
            .iter()
            .map(|settled| (settled.namespace, settled.key.as_slice(), &settled.outcome)),
    )
}

impl TransactState {
    /// Converts reconciled offers in one source cut into terminal, non-executable receipts.
    pub fn settle_funded_checkpoint(
        &self,
        settlements: &[FundedSettlement],
        physical_offset: u64,
    ) -> Result<Self> {
        let cut = self.index.head();
        let source = cut.basis.incarnation();
        let expected = funded_settlement_digest(source, settlements)?;
        let mut portable = self.clone();
        for settled in settlements {
            let receipts = portable.receipts.entry(settled.namespace).or_default();
            let stored = match receipts.get(&settled.key) {
                Some(existing) => retire(existing, &settled.outcome, physical_offset)?,
                None if settled.outcome.activation_lsn.is_none()
                    && settled.outcome.consumption_lsn.is_none() =>
                {
                    StoredReceipt {
                        receipt: Receipt {
                            key: settled.key.clone(),
                            basis: cut.basis,
                            system_time: cut.system_time,
                            outcome: retired_outcome(),
                            replayed: false,
                            erasure: None,
                            decision_clock: None,
                        },
                        fingerprint: settled.outcome.intent_sha256,
                        receipt_namespace: settled.namespace,
                        installed: None,
                        workload: None,
                        workload_digest: None,
                        retired_funding: Some(RetiredFunding {
                            history: HistoricalFunding {
                                source,
                                original_receipt_basis: None,
                                outcome: settled.outcome.clone(),
                            },
                            appointment_address: None,
                        }),
                    }
                }
                None => return Err(invalid_settlement()),
            };
            receipts.insert(settled.key.clone(), stored);
            if settled.namespace.is_some() && !settled.outcome.appointment {
                portable.scoped_funding_origins.insert(source);
            }
        }
        portable.require_portable_checkpoint()?;
        if portable.funded_inventory_digest(source)? != expected {
            return Err(invalid_settlement());
        }
        Ok(portable)
    }

    /// Computes the reconciled inventory represented by this checkpoint for one origin.
    pub fn funded_inventory_digest(&self, source: [u8; 16]) -> Result<Option<[u8; 32]>> {
        inventory_digest(
            source,
            self.receipts.iter().flat_map(|(namespace, receipts)| {
                receipts.iter().filter_map(move |(key, stored)| {
                    let history = &stored.retired_funding.as_ref()?.history;
                    (history.source == source).then_some((
                        *namespace,
                        key.as_slice(),
                        &history.outcome,
                    ))
                })
            }),
        )
    }

    /// Retrieves the terminal booking under the caller's authorized receipt namespace.
    pub fn historical_funding(
        &self,
        key: &[u8],
        namespace: Option<[u8; 32]>,
    ) -> Result<Option<(Receipt, HistoricalFunding)>> {
        validate_key(key)?;
        Ok(self
            .receipts
            .get(&namespace)
            .and_then(|namespace| namespace.get(key))
            .and_then(|stored| {
                let history = stored.retired_funding.as_ref()?.history.clone();
                Some((replay(&stored.receipt), history))
            }))
    }

    /// Resolves a retired appointment by its owner or a retired scoped transaction by its namespace.
    pub fn historical_funding_status(&self, key: &[u8], access: FundedStatus<'_>) -> Result<Receipt> {
        let namespace_at = match access {
            FundedStatus::Owner(owner) => {
                return self.historical_funding(key, None)?
                    .filter(|(_, history)| history.outcome.appointment && history.outcome.owner == owner)
                    .map(|(receipt, _)| receipt)
                    .ok_or_else(unknown_receipt);
            }
            FundedStatus::Scoped(namespace_at) => namespace_at,
        };
        let mut matched = None;
        for &origin in &self.scoped_funding_origins {
            let Some((receipt, history)) = self.historical_funding(key, Some(namespace_at(origin)))? else {
                continue;
            };
            if history.source != origin || history.outcome.appointment {
                continue;
            }
            if matched.replace(receipt).is_some() {
                return Err(conflicting_key());
            }
        }
        matched.ok_or_else(unknown_receipt)
    }
}

fn retire(
    existing: &StoredReceipt,
    outcome: &FundedOutcome,
    physical_offset: u64,
) -> Result<StoredReceipt> {
    let offer = existing
        .receipt
        .decision_clock
        .ok_or_else(invalid_settlement)?;
    let physical_lsn = |basis: Basis| -> Result<u64> {
        basis
            .position()
            .checked_sub(physical_offset)
            .filter(|position| *position != 0)
            .ok_or_else(invalid_settlement)
    };
    let source_basis = existing.receipt.basis;
    let appointment = offer.portable_appointment();
    let intent = appointment.map_or(existing.fingerprint, |(intent, _)| intent);
    let activation = appointment
        .and_then(|(_, cut)| cut)
        .map(|cut| physical_lsn(cut.basis))
        .transpose()?;
    let consumed = if offer.booked() || matches!(existing.receipt.outcome, Outcome::Refused { .. })
    {
        None
    } else {
        Some(physical_lsn(source_basis)?)
    };
    if offer.booking() != outcome.booking
        || !offer.belongs_to(outcome.owner)
        || offer.predecessor() != outcome.predecessor
        || outcome.intent_sha256 != intent
        || outcome.appointment != appointment.is_some()
        || outcome.activation_lsn != activation
        || outcome.consumption_lsn != consumed
        || existing.receipt.erasure.is_some()
        || existing.installed.is_some()
        || existing.workload.is_some()
        || existing.workload_digest.is_some()
    {
        return Err(invalid_settlement());
    }
    let mut stored = existing.clone();
    if consumed.is_none() && !matches!(stored.receipt.outcome, Outcome::Refused { .. }) {
        stored.receipt.outcome = retired_outcome();
    }
    stored.receipt.decision_clock = None;
    stored.retired_funding = Some(RetiredFunding {
        history: HistoricalFunding {
            source: source_basis.incarnation(),
            original_receipt_basis: Some(source_basis),
            outcome: outcome.clone(),
        },
        appointment_address: offer.appointment_address(),
    });
    Ok(stored)
}
