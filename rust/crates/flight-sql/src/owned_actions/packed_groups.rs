use std::collections::VecDeque;

use super::*;

pub(crate) const DECIDE_HOSTED_PACKED_V1: &str = "rtdb.owned.DecideHostedPacked.v1";

pub(super) struct PackedGroups<'a> {
    schedule: Vec<usize>,
    streams: Vec<Stream<'a>>,
    _framing: Arc<MemoryReservation>,
}

struct Stream<'a> {
    relation: OwnedRelation,
    count: usize,
    arrow: &'a [u8],
}

impl<'a> PackedGroups<'a> {
    pub(super) fn parse(call: &Call, body: &'a [u8]) -> Result<Self, Status> {
        let (runs, body) = count(body, "Missing packed fact run count")?;
        let (stream_count, body) = count(body, "Missing packed fact stream count")?;
        if !(1..runs).contains(&stream_count) || runs > MAX_GROUPED_COMMIT_ROWS {
            return Err(invalid("Invalid packed fact group counts"));
        }
        let framing = call.reserve(wire::mul(runs + stream_count, 512)?)?;
        let (schedule, mut body) = body.split_at_checked(runs * 2)
            .ok_or_else(|| invalid("Truncated packed fact schedule"))?;
        let schedule = schedule.as_chunks::<2>().0.iter().map(|index| usize::from(u16::from_be_bytes(*index))).collect::<Vec<_>>();
        let mut counts = vec![0; stream_count];
        for &index in &schedule {
            *counts.get_mut(index).ok_or_else(|| invalid("Unknown packed fact stream"))? += 1;
        }

        let mut streams = Vec::with_capacity(stream_count);
        let mut relations = BTreeSet::new();
        for count in counts {
            if count == 0 {
                return Err(invalid("Unreferenced packed fact stream"));
            }
            let (relation, rest) = split_relation(body)?;
            let (length, rest) = rest.split_first_chunk::<4>().ok_or_else(|| invalid("Missing packed fact stream length"))?;
            let (arrow, tail) = rest.split_at_checked(u32::from_be_bytes(*length) as usize)
                .ok_or_else(|| invalid("Truncated packed fact stream"))?;
            if arrow.is_empty() {
                return Err(invalid("Empty packed fact stream"));
            }
            relations.insert(relation.clone());
            if relations.len() > MAX_COMMIT_GROUPS {
                return Err(invalid("Owned commit exceeds its distinct relation limit"));
            }
            streams.push(Stream { relation, count, arrow });
            body = tail;
        }
        if !body.is_empty() {
            return Err(invalid("Packed facts have trailing bytes"));
        }
        Ok(Self { schedule, streams, _framing: framing })
    }

    pub(super) fn relations(&self) -> impl Iterator<Item = &OwnedRelation> {
        self.streams.iter().map(|stream| &stream.relation)
    }

    pub(super) fn decode(
        self, service: &Service, call: &Call, envelope: &Bytes, writes: &mut Vec<TxOp>,
        rows_left: &mut usize, effects: CommitEffects,
    ) -> Result<Vec<(Arc<MemoryReservation>, Arc<MemoryReservation>)>, Status> {
        let mut opened: Vec<Option<VecDeque<OwnedBatch>>> = vec![None; self.streams.len()];
        let mut retained = Vec::with_capacity(self.schedule.len());
        for index in self.schedule {
            let stream = &self.streams[index];
            if opened[index].is_none() {
                let bytes = arrow_buffer::Buffer::from(envelope.slice_ref(stream.arrow));
                let input = OwnedStream::read_buffer(bytes, call.memory()).map_err(status)?;
                if input.pages().len() != stream.count {
                    return Err(invalid("Packed fact stream disagrees with its schedule"));
                }
                opened[index] = Some(input.into_pages().into());
            }
            let batch = opened[index].as_mut().and_then(VecDeque::pop_front)
                .ok_or_else(|| invalid("Packed fact schedule exhausted its stream"))?;
            let batch = require_identity(batch)?;
            if batch.batch().num_rows() == 0 || batch.batch().column_by_name("_op").is_none() {
                return Err(invalid("Packed fact run requires rows with an operation"));
            }
            retained.push(service.append_decoded_owned_group(
                call, &stream.relation, &batch, writes, rows_left, effects,
            )?);
        }
        Ok(retained)
    }
}

fn count<'a>(body: &'a [u8], message: &str) -> Result<(usize, &'a [u8]), Status> {
    let (count, body) = body.split_first_chunk::<2>().ok_or_else(|| invalid(message))?;
    Ok((usize::from(u16::from_be_bytes(*count)), body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_counts_are_big_endian_and_leave_the_schedule() {
        assert_eq!(count(&[0, 2, 0, 1], "missing").unwrap(), (2, &[0, 1][..]));
        assert!(count(&[0], "missing").is_err());
    }
}
