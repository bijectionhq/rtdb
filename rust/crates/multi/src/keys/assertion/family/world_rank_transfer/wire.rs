//! Canonical raw custody evidence. Binary rank addresses are length-delimited.

use rtdb_types::{AssertionId, Timestamp, TxId};

use super::*;

const AGREEMENT: &[u8; 4] = b"WRT1";
const RECORD: &[u8; 4] = b"WRV1";

pub(crate) struct Frame<'a>(pub(crate) &'a [u8]);

impl<'a> Frame<'a> {
    pub(crate) fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (head, tail) = self.0.split_first_chunk::<N>().ok_or_else(conflict)?;
        self.0 = tail;
        Ok(*head)
    }

    pub(crate) fn payload(&mut self, limit: usize) -> Result<&'a [u8]> {
        let length = u32::from_be_bytes(self.take()?) as usize;
        if length == 0 || length > limit { return Err(conflict()); }
        let (payload, tail) = self.0.split_at_checked(length).ok_or_else(conflict)?;
        self.0 = tail;
        Ok(payload)
    }

    fn counted(&mut self) -> Result<&'a [u8]> {
        let n = u16::from_be_bytes(self.take()?) as usize;
        let (field, tail) = self.0.split_at_checked(n).ok_or_else(conflict)?;
        self.0 = tail;
        Ok(field)
    }

    pub(crate) fn uuid(&mut self) -> Result<Uuid> {
        Ok(Uuid::from_bytes(self.take()?))
    }

    fn incarnation(&mut self) -> Result<NonZeroU64> {
        NonZeroU64::new(u64::from_be_bytes(self.take()?)).ok_or_else(conflict)
    }

    pub(crate) fn transaction(&mut self) -> Result<TransactionKey> {
        Ok(TransactionKey {
            tx_id: TxId(i64::from_be_bytes(self.take()?)),
            system_time: Timestamp(i64::from_be_bytes(self.take()?)),
        })
    }

    fn position(&mut self) -> Result<Position> {
        Position::from_bytes(self.take()?).map_err(|_| conflict())
    }

    fn stage(&mut self) -> Result<AssertionStage> {
        AssertionStage::new(self.uuid()?, self.incarnation()?, self.uuid()?)
    }

    pub(crate) fn replay(&mut self) -> Result<AcceptedWorldRankReplay> {
        Ok(AcceptedWorldRankReplay {
            transaction: self.transaction()?, receipt: self.take()?, backing: self.take()?,
        })
    }

    pub(crate) fn state(&mut self, phase: u8) -> Result<WorldRankTransferState> {
        Ok(match phase {
            0 => WorldRankTransferState::Prepared,
            1 => WorldRankTransferState::Installed { replay: self.replay()?, copy_drain: self.take()? },
            2 => WorldRankTransferState::Discarded { copy_drain: self.take()? },
            _ => return Err(conflict()),
        })
    }

    pub(crate) fn finish(self) -> Result<()> {
        if self.0.is_empty() { Ok(()) } else { Err(conflict()) }
    }
}

pub(super) fn put_transaction(out: &mut Vec<u8>, key: TransactionKey) {
    out.extend_from_slice(&key.tx_id.0.to_be_bytes());
    out.extend_from_slice(&key.system_time.0.to_be_bytes());
}

fn put_stage(out: &mut Vec<u8>, stage: AssertionStage) {
    out.extend_from_slice(stage.deployment().as_bytes());
    out.extend_from_slice(&stage.incarnation().get().to_be_bytes());
    out.extend_from_slice(stage.identity().as_bytes());
}

impl WorldRankTransfer {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_RECORD_BYTES { return Err(too_large()); }
        let mut frame = Frame(bytes);
        if &frame.take::<4>()? != AGREEMENT { return Err(conflict()); }
        let source = WorldRankTransferSource {
            original: RetiredOriginalFamilySource {
                deployment: frame.uuid()?,
                history: frame.take()?,
                installation: frame.uuid()?,
                target_installation: frame.uuid()?,
                audience_tablet: frame.take()?,
                row_custody: frame.take()?,
                handoff: frame.take()?,
                carrier: frame.transaction()?,
                fence: frame.transaction()?,
                through: frame.transaction()?,
                captured: frame.transaction()?,
            },
            custody_incarnation: frame.incarnation()?,
            archive: frame.take()?,
        };
        let destination = WorldRankTransferDestination {
            deployment: frame.uuid()?,
            installation: frame.uuid()?,
            custody_incarnation: frame.incarnation()?,
            hold: frame.take()?,
            preparation: frame.take()?,
        };
        let count = u16::from_be_bytes(frame.take()?) as usize;
        if !(1..=MAX_CONTRIBUTORS).contains(&count) { return Err(conflict()); }
        let mut contributors = Vec::with_capacity(count);
        for _ in 0..count {
            let family = frame.uuid()?;
            let occurrence = FamilyOccurrence {
                position: frame.position()?,
                assertion: AssertionId::from_bytes(frame.take()?),
                capability: frame.stage()?,
            };
            let transaction = frame.transaction()?;
            let rank_position = frame.position()?;
            let rank_stage = frame.stage()?;
            let relation = frame.counted()?;
            let key = frame.counted()?;
            if relation.len() > 255 || key.len() > 256 { return Err(conflict()); }
            let relation = std::str::from_utf8(relation).map_err(|_| conflict())?.to_owned();
            contributors.push(WorldRankTransferContributor {
                family, occurrence, transaction, rank_position,
                rank: DerivedRow {
                    member: occurrence.assertion,
                    world: Some(Uuid::from_bytes(rank_position.line_generation)),
                    relation,
                    key: key.to_vec(),
                    capability: Some(rank_stage),
                },
                disclosure: frame.take()?,
                founder_carrier_sha256: frame.take()?,
                rank_carrier_sha256: frame.take()?,
                frozen_effects_sha256: frame.take()?,
            });
        }
        frame.finish()?;
        let agreement = Self { source, destination, contributors };
        agreement.validate()?;
        Ok(agreement)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut out = AGREEMENT.to_vec();
        let original = self.source.original;
        out.extend_from_slice(original.deployment.as_bytes());
        out.extend_from_slice(&original.history);
        out.extend_from_slice(original.installation.as_bytes());
        out.extend_from_slice(original.target_installation.as_bytes());
        out.extend_from_slice(&original.audience_tablet);
        out.extend_from_slice(&original.row_custody);
        out.extend_from_slice(&original.handoff);
        for key in [original.carrier, original.fence, original.through, original.captured] {
            put_transaction(&mut out, key);
        }
        out.extend_from_slice(&self.source.custody_incarnation.get().to_be_bytes());
        out.extend_from_slice(&self.source.archive);
        let target = self.destination;
        out.extend_from_slice(target.deployment.as_bytes());
        out.extend_from_slice(target.installation.as_bytes());
        out.extend_from_slice(&target.custody_incarnation.get().to_be_bytes());
        out.extend_from_slice(&target.hold);
        out.extend_from_slice(&target.preparation);
        self.put_inventory(&mut out)?;
        Ok(out)
    }

    pub(super) fn put_inventory(&self, out: &mut Vec<u8>) -> Result<()> {
        out.extend_from_slice(&(self.contributors.len() as u16).to_be_bytes());
        for member in &self.contributors {
            out.extend_from_slice(member.family.as_bytes());
            out.extend_from_slice(&member.occurrence.position.to_bytes());
            out.extend_from_slice(&member.occurrence.assertion.to_bytes());
            put_stage(out, member.occurrence.capability);
            put_transaction(out, member.transaction);
            out.extend_from_slice(&member.rank_position.to_bytes());
            put_stage(out, member.rank.capability.ok_or_else(conflict)?);
            for field in [member.rank.relation.as_bytes(), member.rank.key.as_slice()] {
                out.extend_from_slice(&(field.len() as u16).to_be_bytes());
                out.extend_from_slice(field);
            }
            out.extend_from_slice(&member.disclosure);
            out.extend_from_slice(&member.founder_carrier_sha256);
            out.extend_from_slice(&member.rank_carrier_sha256);
            out.extend_from_slice(&member.frozen_effects_sha256);
        }
        Ok(())
    }
}

impl TransferRecord {
    pub(super) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut frame = Frame(bytes);
        if &frame.take::<4>()? != RECORD { return Err(malformed()); }
        let delivery_revoked = match frame.take::<1>()?[0] {
            0 => false,
            1 => true,
            _ => return Err(malformed()),
        };
        let phase = match frame.take::<1>()?[0] {
            0 => Phase::Preparing,
            1 => Phase::Bound,
            2 => {
                let tag = frame.take::<1>()?[0];
                let destination = frame.state(tag)?;
                let source_drain = frame.take()?;
                let generation = frame.take()?;
                Phase::Drained { destination, source_drain, generation }
            }
            _ => return Err(malformed()),
        };
        Ok(Self { agreement: WorldRankTransfer::from_bytes(frame.0)?, phase, delivery_revoked })
    }

    pub(super) fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut out = RECORD.to_vec();
        out.push(u8::from(self.delivery_revoked));
        match self.phase {
            Phase::Preparing => out.push(0),
            Phase::Bound => out.push(1),
            Phase::Drained { destination, source_drain, generation } => {
                out.push(2);
                match destination {
                    WorldRankTransferState::Installed { replay, copy_drain } => {
                        out.push(1);
                        put_transaction(&mut out, replay.transaction);
                        out.extend_from_slice(&replay.receipt);
                        out.extend_from_slice(&replay.backing);
                        out.extend_from_slice(&copy_drain);
                    }
                    WorldRankTransferState::Discarded { copy_drain } => {
                        out.push(2);
                        out.extend_from_slice(&copy_drain);
                    }
                    WorldRankTransferState::Prepared => return Err(malformed()),
                }
                out.extend_from_slice(&source_drain);
                out.extend_from_slice(&generation);
            }
        }
        out.extend_from_slice(&self.agreement.to_bytes()?);
        Ok(out)
    }
}
