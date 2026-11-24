//! Complete membership of the installed FactStore sample assertion relation.

use rtdb_types::WorldShadow;
use sha2::{Digest, Sha256};

use super::*;

pub(in crate::resolver) const SAMPLE_MEMBERSHIP: &str = "sample_membership";

const NAME_BYTES: usize = 34;
const ID_BYTES: usize = 32;

fn sample_identity_digest(id: &EntityId) -> Result<[u8; ID_BYTES]> {
    let mut digest = Sha256::new();
    match id {
        EntityId::String(id) => digest.update(id.as_bytes()),
        EntityId::Binary(id) => {
            digest.update([0xff]);
            digest.update(id);
        }
        _ => return Err(uncovered()),
    }
    Ok(digest.finalize().into())
}

pub(in crate::resolver) struct SampleProjection<'a> {
    pub(in crate::resolver) fields: Vec<&'a str>,
    excluded: BTreeSet<[u8; ID_BYTES]>,
}

impl<'a> SampleProjection<'a> {
    pub(in crate::resolver) fn decode(bytes: &'a [u8]) -> Result<Self> {
        let (&series, bytes) = bytes.split_first().ok_or_else(uncovered)?;
        let (names, suffix) = bytes.split_at_checked((usize::from(series) + 4) * NAME_BYTES)
            .ok_or_else(uncovered)?;
        let fields = names.chunks_exact(NAME_BYTES).map(|name| {
            let name = std::str::from_utf8(name).map_err(|_| uncovered())?;
            if !name.starts_with("c_") || !name[2..].bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)) {
                return Err(uncovered());
            }
            Ok(name)
        }).collect::<Result<Vec<_>>>()?;
        let (ids, tail) = suffix.as_chunks::<ID_BYTES>();
        if !tail.is_empty() || !ids.windows(2).all(|pair| pair[0] < pair[1])
            || fields.iter().collect::<BTreeSet<_>>().len() != fields.len()
        {
            return Err(uncovered());
        }
        Ok(Self { fields, excluded: ids.iter().copied().collect() })
    }

    pub(in crate::resolver) fn require_suffix(
        &self, relation: &OwnedRelation, operations: &[Operation], landing: bool,
    ) -> Result<()> {
        if self.excluded.is_empty() { return Ok(()); }
        if !landing { return Err(uncovered()); }
        let mut unmatched = self.excluded.clone();
        for operation in operations {
            let mut admit = |id| {
                if let Ok(digest) = sample_identity_digest(&id) {
                    unmatched.remove(&digest);
                }
            };
            match operation {
                Operation::Public(TxOp::OwnedArrowDocs { relation: target, documents, create_only: true, valid_from: None, valid_to: None })
                | Operation::OwnedArrowIids { relation: target, documents, create_only: true, valid_from: None, valid_to: None, .. }
                    if target == relation =>
                {
                    for row in 0..documents.row_count() { admit(documents.owned_id(row)?); }
                }
                Operation::Public(TxOp::OwnedInsertDocs { relation: target, documents, valid_from: None, valid_to: None })
                | Operation::OwnedWriteIids { relation: target, documents, create_only: true, valid_from: None, valid_to: None, .. }
                    if target == relation =>
                {
                    for row in documents.rows() {
                        admit(row.get("_id").ok_or_else(uncovered)?.owned_entity_id()?);
                    }
                }
                _ => (),
            }
        }
        if !unmatched.is_empty() { return Err(uncovered()); }
        Ok(())
    }
}

#[derive(Default)]
pub(in crate::resolver) struct SamplePopulation {
    assertions: Vec<SampleAssertion>,
    reservations: Vec<Arc<MemoryReservation>>,
}

struct SampleAssertion {
    time: Timestamp,
    ordinal: [u8; 9],
    series: Vec<Vec<u8>>,
    at: [u8; 9],
    effect: SampleEffect,
}

enum SampleEffect {
    Append,
    Retract(Option<[u8; 9]>),
}

impl SampleEffect {
    fn apply(self, at: [u8; 9], members: &mut BTreeSet<[u8; 9]>) {
        match self {
            Self::Append => { members.insert(at); }
            Self::Retract(None) => { members.remove(&at); }
            Self::Retract(Some(end)) => {
                let mut suffix = members.split_off(&at).split_off(&end);
                members.append(&mut suffix);
            }
        }
    }
}

impl SamplePopulation {
    pub(in crate::resolver) async fn collect(
        &mut self, context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
        ordered: Timestamp, projection: &SampleProjection<'_>,
    ) -> Result<()> {
        let world = relation.world().is_some();
        let mut names = projection.fields.clone();
        names.extend(["_id", "_system_from"]);
        if world { names.push(WorldShadow::FIELDS); }
        let cursor = if world {
            rtdb_query::open_owned_world_constraint_projection(Arc::clone(&context), owner, relation, ordered, &names).await?
        } else {
            rtdb_query::open_owned_root_projection(Arc::clone(&context), owner, relation, ordered, &names).await?
        };
        let Some(mut cursor) = cursor else { return Ok(()); };
        let (series, [at, end, kind, ordinal]) = projection.fields.as_slice().split_last_chunk::<4>()
            .ok_or_else(uncovered)?;
        while let Some(batch) = cursor.next_batch().await? {
            let rows = batch.batch();
            for row in 0..rows.num_rows() {
                context.check_cancelled_at(row)?;
                let read = |column: &str| occupancy_value_at(&context.memory, rows, row, column, uncovered);
                let id = read("_id")?.owned_entity_id()?;
                if projection.excluded.contains(&sample_identity_digest(&id)?) { continue; }
                if world && !matches!(read(WorldShadow::FIELDS)?, Value::List(_)) { return Err(uncovered()); }
                let time = rtdb_query::owned_row_revision(rows, row, &id, &context.memory)?;
                let image = |column| match read(column)? {
                    Value::Binary(bytes) => Ok(bytes),
                    _ => Err(uncovered()),
                };
                let word = |column, tag| -> Result<[u8; 9]> {
                    let bytes: [u8; 9] = image(column)?.try_into().map_err(|_| uncovered())?;
                    if bytes[0] != tag { return Err(uncovered()); }
                    Ok(bytes)
                };
                let at = word(at, 8)?;
                let effect = match word(kind, 4)? {
                    [4, 128, 0, 0, 0, 0, 0, 0, 0] => SampleEffect::Append,
                    [4, 128, 0, 0, 0, 0, 0, 0, 1] => {
                        let end = match read(end)? {
                            Value::Undefined | Value::Null => None,
                            Value::Binary(_) => Some(word(end, 8)?),
                            _ => return Err(uncovered()),
                        };
                        if end.is_some_and(|end| end <= at) { return Err(uncovered()); }
                        SampleEffect::Retract(end)
                    }
                    _ => return Err(uncovered()),
                };
                let ordinal = word(ordinal, 4)?;
                let series = series.iter().map(|field| image(field)).collect::<Result<Vec<_>>>()?;
                let bytes = series.iter().map(Vec::capacity).sum::<usize>() + 256;
                self.reservations.push(context.memory.reserve(bytes)?);
                self.assertions.push(SampleAssertion { time, ordinal, series, at, effect });
            }
        }
        Ok(())
    }

    pub(in crate::resolver) fn count(mut self, context: &QueryContext) -> Result<i64> {
        self.assertions.sort_unstable_by(|left, right| {
            (&left.series, left.time, left.ordinal).cmp(&(&right.series, right.time, right.ordinal))
        });
        let mut members = BTreeSet::new();
        let mut count = 0i64;
        let mut previous = None::<(Vec<Vec<u8>>, Timestamp, [u8; 9])>;
        for (index, assertion) in self.assertions.into_iter().enumerate() {
            context.check_cancelled_at(index)?;
            if let Some((series, time, ordinal)) = &previous {
                if *series != assertion.series {
                    count = count.checked_add(members.len() as i64).ok_or_else(overflow)?;
                    members.clear();
                } else if (*time, *ordinal) == (assertion.time, assertion.ordinal) {
                    return Err(uncovered());
                }
            }
            assertion.effect.apply(assertion.at, &mut members);
            previous = Some((assertion.series, assertion.time, assertion.ordinal));
        }
        count.checked_add(members.len() as i64).ok_or_else(overflow)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_sample_membership_resolves_repeated_appends_and_retractions() {
        let at = |n| [8, 128, 0, 0, 0, 0, 0, 0, n];
        let mut members = BTreeSet::new();
        for point in [1, 2, 3, 3, 4] {
            SampleEffect::Append.apply(at(point), &mut members);
        }
        SampleEffect::Retract(Some(at(4))).apply(at(2), &mut members);
        SampleEffect::Retract(None).apply(at(1), &mut members);
        SampleEffect::Append.apply(at(3), &mut members);
        assert_eq!(members, BTreeSet::from([at(3), at(4)]));
    }
}
