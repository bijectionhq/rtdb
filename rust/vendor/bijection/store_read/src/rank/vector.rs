//! Family 7: exact cosine nearest neighbours over owned relations
//! (store-cut RANKED §1.2, §4.1, §4.2; RTDB `NearestHeld.v4`).
//!
//! A vector path indexes one list column and names its filter columns.
//! Each put image whose vector cell is not null writes a vector entry
//! (values `0x00`) and, per filter column holding a Binary key, a filter
//! entry (values `0x01 ‖ ordinal u16 ‖ key`). A read takes the vector run,
//! or for a filtered read the intersection of each filter's allowed-value
//! runs, resolves every candidate at the cut and computes RTDB's cosine
//! distance over the resolved image (the entries only nominate). The read
//! set is exactly the runs consulted: a row that could enter the window
//! writes an entry into each of them.
//!
//! A declared recall marks the approximate route. The approximate index is
//! absent from the store cut (coordinator decision): a recall path ranks
//! exactly, which meets every recall target.
use std::collections::BTreeSet;

use anyhow::{
    ensure,
    Context,
};

use super::{
    frame::Frame,
    text::run_ids,
};
use crate::{
    catalog::{
        self,
        PathKind,
    },
    event::Image,
    keys::{
        self,
        RowId,
    },
    source::RangeSource,
    store::KeyRange,
};

pub const MAX_NEAREST_K: usize = 256;
pub const MAX_NEAREST_DIMENSIONS: usize = 4096;
pub const MAX_NEAREST_FILTERS: usize = 8;
pub const MAX_NEAREST_FILTER_VALUES: usize = 64;
pub const MAX_NEAREST_FILTER_BYTES: usize = 1024;
pub const MAX_NEAREST_SHADOWS: usize = 64;
pub const MAX_NEAREST_PARTITIONS: usize = 32;
/// More scored candidates than this refuse the read (`rtdb.owned/nearest-scan-limit`, NN:255).
pub const MAX_NEAREST_CANDIDATES: usize = 100_000;

const VECTOR: u8 = 0x00;
const FILTER: u8 = 0x01;

/// The cell tags of the Arrow adapter's value encoding
/// (`store_flight::owned_engine::value`).
const NULL_CELL: u8 = 0x00;
const LIST_CELL: u8 = 0x14;
const BINARY_CELL: u8 = 0x0e;

fn dimension_error() -> anyhow::Error {
    anyhow::anyhow!("rtdb.owned/nearest-dimension: a stored vector does not have the probe's finite dimensions")
}

fn unescape(bytes: &[u8]) -> anyhow::Result<(Vec<u8>, &[u8])> {
    let mut out = Vec::new();
    let mut at = 0;
    loop {
        match bytes.get(at..at + 1) {
            Some([0]) => match bytes.get(at + 1) {
                Some(0) => return Ok((out, &bytes[at + 2..])),
                Some(0xff) => {
                    out.push(0);
                    at += 2;
                },
                _ => anyhow::bail!("Invalid owned byte string escape"),
            },
            Some([byte]) => {
                out.push(*byte);
                at += 1;
            },
            _ => anyhow::bail!("Truncated owned byte string"),
        }
    }
}

/// The key a filter cell holds: `None` for a null; a Binary key otherwise.
fn filter_key(cell: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    match cell.split_first() {
        Some((&NULL_CELL, [])) => Ok(None),
        Some((&BINARY_CELL, rest)) => {
            let (key, rest) = unescape(rest)?;
            ensure!(rest.is_empty(), "rtdb.owned/vector-path: a filter column holds Binary keys");
            Ok(Some(key))
        },
        _ => anyhow::bail!("rtdb.owned/vector-path: a filter column holds Binary keys"),
    }
}

fn component(tag: u8, bytes: &[u8]) -> anyhow::Result<(f64, usize)> {
    let take = |count: usize| bytes.get(..count).ok_or_else(dimension_error);
    Ok(match tag {
        0x02 => ((take(1)?[0] ^ 0x80) as i8 as f64, 1),
        0x03 => (((u16::from_be_bytes(take(2)?.try_into()?) ^ 0x8000) as i16) as f64, 2),
        0x04 => (((u32::from_be_bytes(take(4)?.try_into()?) ^ 0x8000_0000) as i32) as f64, 4),
        0x05 => (((u64::from_be_bytes(take(8)?.try_into()?) ^ (1 << 63)) as i64) as f64, 8),
        0x0b => {
            let bits = u32::from_be_bytes(take(4)?.try_into()?);
            let bits = if bits & 0x8000_0000 != 0 { bits ^ 0x8000_0000 } else { !bits };
            (f64::from(f32::from_bits(bits)), 4)
        },
        0x0c => {
            let bits = u64::from_be_bytes(take(8)?.try_into()?);
            let bits = if bits & (1 << 63) != 0 { bits ^ (1 << 63) } else { !bits };
            (f64::from_bits(bits), 8)
        },
        _ => return Err(dimension_error()),
    })
}

/// The components of a vector cell, `None` for a null vector. A null,
/// unsigned or non-numeric component fails the read
/// (`rtdb.owned/nearest-dimension`, NN:748, 801); a non-finite one fails it
/// only when the row is scored, after its filters.
pub fn cell_vector(cell: &[u8]) -> anyhow::Result<Option<Vec<f64>>> {
    let (tag, mut rest) = cell.split_first().ok_or_else(dimension_error)?;
    match *tag {
        NULL_CELL if rest.is_empty() => return Ok(None),
        LIST_CELL => (),
        _ => return Err(dimension_error()),
    }
    let mut out = Vec::new();
    loop {
        match rest.split_first() {
            Some((0, [])) => return Ok(Some(out)),
            Some((1, item)) => {
                let (tag, value) = item.split_first().ok_or_else(dimension_error)?;
                let (component, used) = component(*tag, value)?;
                out.push(component);
                rest = &value[used..];
            },
            _ => return Err(dimension_error()),
        }
    }
}

/// The entries one image writes into a vector path.
pub fn entries(column: &str, filters: &[String], image: &Image) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let Some(cell) = image.get(column) else { return Ok(Vec::new()) };
    if cell.as_slice() == [NULL_CELL] {
        return Ok(Vec::new());
    }
    let mut out = vec![(vec![VECTOR], Vec::new())];
    for (ordinal, filter) in filters.iter().enumerate() {
        let Some(key) = image.get(filter).map(|cell| filter_key(cell)).transpose()?.flatten() else { continue };
        out.push((filter_values(ordinal, &key), Vec::new()));
    }
    Ok(out)
}

fn filter_values(ordinal: usize, key: &[u8]) -> Vec<u8> {
    [&[FILTER][..], &(ordinal as u16).to_be_bytes(), key].concat()
}

struct Probe {
    probe: Vec<f32>,
    norm: f64,
}

impl Probe {
    /// RTDB's cosine distance (NN:80-100): `None` for a zero-norm or
    /// non-finite result, an error for another dimension.
    fn distance(&self, vector: &[f64]) -> anyhow::Result<Option<f64>> {
        if vector.len() != self.probe.len() {
            return Err(dimension_error());
        }
        let (mut dot, mut norm) = (0.0, 0.0);
        for (&left, &right) in self.probe.iter().zip(vector) {
            if !right.is_finite() {
                return Err(dimension_error());
            }
            dot += f64::from(left) * right;
            norm += right * right;
        }
        let norm = f64::sqrt(norm);
        if norm == 0.0 {
            return Ok(None);
        }
        let distance = 1.0 - dot / (self.norm * norm);
        Ok(distance.is_finite().then_some(distance))
    }
}

/// One partition of a nearest read: a relation's vector path in one frame,
/// and the filter columns the request's filters apply to, in request order.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VectorPartition {
    pub frame: usize,
    pub relation: String,
    pub path: String,
    pub filters: Vec<String>,
    /// The partition's position in the caller's request.
    pub ordinal: usize,
}

/// One staged vector row: it shadows the stored row of `id` in the
/// partition at position `partition`; with a vector whose filters admit
/// it, it is ranked.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StagedVector {
    pub partition: usize,
    pub id: RowId,
    #[serde(with = "super::wire::option_f32_vec_bits")]
    pub vector: Option<Vec<f32>>,
    /// One key per request filter (`None` is null).
    pub filters: Vec<Option<Vec<u8>>>,
}

/// An exact nearest read (`NearestHeld.v4`).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VectorQuery {
    pub partitions: Vec<VectorPartition>,
    /// The allowed keys of each filter, in request order.
    pub filters: Vec<Vec<Vec<u8>>>,
    #[serde(with = "super::wire::f32_vec_bits")]
    pub probe: Vec<f32>,
    pub k: usize,
    pub valid: Option<i64>,
    pub staged: Vec<StagedVector>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VectorHit {
    pub id: RowId,
    #[serde(with = "super::wire::f64_bits")]
    pub distance: f64,
    /// The partition's ordinal.
    pub partition: usize,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VectorPage {
    pub present_micros: i64,
    pub valid_micros: i64,
    /// Distance ascending, then id, then partition; at most `k`.
    pub hits: Vec<VectorHit>,
    /// Every filtered, scored candidate.
    pub total: usize,
    /// The read set of each partition, by ordinal.
    pub reads: Vec<Vec<KeyRange>>,
}

/// The global order of nearest hits.
pub fn order(hits: &mut [VectorHit]) {
    hits.sort_by(|left, right| left.distance.total_cmp(&right.distance).then_with(|| left.id.cmp(&right.id)).then_with(|| left.partition.cmp(&right.partition)));
}

/// Rank the vectors of the request's partitions by exact cosine distance.
pub async fn nearest<S: RangeSource + ?Sized>(source: &S, frames: &[Frame], request: &VectorQuery) -> anyhow::Result<VectorPage> {
    let filter_count = request.filters.len();
    ensure!(
        (1..=MAX_NEAREST_DIMENSIONS).contains(&request.probe.len())
            && (1..=MAX_NEAREST_K).contains(&request.k)
            && (1..=MAX_NEAREST_PARTITIONS).contains(&request.partitions.len())
            && filter_count <= MAX_NEAREST_FILTERS
            && request.filters.iter().all(|values| {
                (1..=MAX_NEAREST_FILTER_VALUES).contains(&values.len()) && values.iter().all(|value| value.len() <= MAX_NEAREST_FILTER_BYTES)
            })
            && request.partitions.iter().all(|partition| partition.filters.len() == filter_count)
            && request.probe.iter().all(|value| value.is_finite()),
        "rtdb.owned/nearest-limit: owned nearest exceeds its probe, filter or result bound"
    );
    let norm = request.probe.iter().map(|&value| f64::from(value) * f64::from(value)).sum::<f64>().sqrt();
    ensure!(norm != 0.0 && norm.is_finite(), "rtdb.owned/nearest-probe: owned nearest requires a nonzero probe");
    ensure!(request.staged.is_empty() || request.valid.is_none(), "A staged nearest read requires current validity");
    ensure!(
        request.staged.len() <= MAX_NEAREST_SHADOWS
            && request.staged.windows(2).all(|pair| (pair[0].partition, &pair[0].id) < (pair[1].partition, &pair[1].id))
            && request.staged.iter().all(|staged| staged.partition < request.partitions.len() && staged.filters.len() == filter_count),
        "rtdb.owned/nearest-overlay: a staged nearest read holds at most {MAX_NEAREST_SHADOWS} distinct rows of its partitions, in order"
    );
    ensure!(request.partitions.iter().all(|partition| partition.frame < frames.len()), "A nearest partition names one of the request's frames");
    let distinct = request.partitions.iter().map(|partition| (frames[partition.frame].place, &partition.relation, &partition.path)).collect::<BTreeSet<_>>();
    ensure!(distinct.len() == request.partitions.len(), "rtdb.owned/nearest-relations: a nearest read names each partition once");
    let probe = Probe { probe: request.probe.clone(), norm };
    let allowed = request.filters.iter().map(|values| values.iter().cloned().collect::<BTreeSet<_>>()).collect::<Vec<_>>();
    let admits = |keys: &[Option<Vec<u8>>]| keys.iter().zip(&allowed).all(|(key, allowed)| key.as_ref().is_some_and(|key| allowed.contains(key)));
    let first = &frames[request.partitions[0].frame];
    let mut hits = Vec::new();
    let mut reads = Vec::with_capacity(request.partitions.len());
    let mut nominated = 0usize;
    for (index, partition) in request.partitions.iter().enumerate() {
        let frame = &frames[partition.frame];
        let valid = request.valid.unwrap_or(frame.present_micros);
        frame.admit(&partition.relation)?;
        let declaration = catalog::declaration(source, frame.place, &partition.relation, Some(frame.version)).await?.context("rtdb.owned/unknown-relation")?;
        ensure!(
            declaration.lifetime_micros.is_none() || valid == frame.present_micros,
            "rtdb.owned/lifetime-uncovered: a lifetime relation is read at the held present"
        );
        let installed = catalog::path(source, frame.place, &partition.relation, &partition.path, Some(frame.version))
            .await?
            .context("rtdb.owned/nearest-uncovered: the partition has no vector path")?;
        let PathKind::Vector { dimensions, filters: declared, .. } = &installed.kind else {
            anyhow::bail!("rtdb.owned/nearest-relations: {}.{} is not a vector path", partition.relation, partition.path);
        };
        ensure!(usize::from(*dimensions) == request.probe.len(), "rtdb.owned/nearest-dimension: the probe differs from the path's dimensions");
        let ordinals = partition
            .filters
            .iter()
            .map(|filter| declared.iter().position(|name| name == filter).context("rtdb.owned/nearest-relations: the filter column is not one of the path's"))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let column = &installed.columns[0];
        // The consulted runs: the vector run, or every allowed value's run
        // of every filter.
        let mut partition_reads = Vec::new();
        let mut world_reads = match &frame.world {
            Some(world) => frame.world_reads(world, &partition.relation, &partition.path),
            None => Vec::new(),
        };
        let candidates = if ordinals.is_empty() {
            partition_reads.push(keys::path_run(frame.place, &partition.relation, &partition.path, &[VECTOR]));
            run_ids(source, frame, &partition.relation, &partition.path, &[VECTOR], &mut nominated).await?
        } else {
            let mut intersection: Option<BTreeSet<RowId>> = None;
            for (ordinal, values) in ordinals.iter().zip(&request.filters) {
                let mut union = BTreeSet::new();
                for value in values {
                    let run = filter_values(*ordinal, value);
                    partition_reads.push(keys::path_run(frame.place, &partition.relation, &partition.path, &run));
                    union.extend(run_ids(source, frame, &partition.relation, &partition.path, &run, &mut nominated).await?);
                }
                intersection = Some(match intersection {
                    None => union,
                    Some(found) => found.intersection(&union).cloned().collect(),
                });
            }
            intersection.unwrap_or_default()
        };
        let staged = request.staged.iter().filter(|staged| staged.partition == index).collect::<Vec<_>>();
        for staged in &staged {
            let Some(vector) = &staged.vector else { continue };
            if !admits(&staged.filters) {
                continue;
            }
            let vector = vector.iter().map(|&component| f64::from(component)).collect::<Vec<_>>();
            if let Some(distance) = probe.distance(&vector)? {
                hits.push(VectorHit { id: staged.id.clone(), distance, partition: partition.ordinal });
            }
        }
        for id in candidates {
            if staged.iter().any(|staged| staged.id == id) {
                continue;
            }
            let Some(image) = frame.image(source, &partition.relation, &id, valid, &mut world_reads).await? else { continue };
            let Some(vector) = image.get(column).map(|cell| cell_vector(cell)).transpose()?.flatten() else { continue };
            let keys = partition
                .filters
                .iter()
                .map(|filter| image.get(filter).map(|cell| filter_key(cell)).transpose().map(Option::flatten))
                .collect::<anyhow::Result<Vec<_>>>()?;
            if !admits(&keys) {
                continue;
            }
            if let Some(distance) = probe.distance(&vector)? {
                hits.push(VectorHit { id, distance, partition: partition.ordinal });
                ensure!(hits.len() <= MAX_NEAREST_CANDIDATES, "rtdb.owned/nearest-scan-limit: owned nearest exceeded its candidate budget");
            }
        }
        match &frame.world {
            None => reads.push((partition.ordinal, partition_reads)),
            Some(_) => reads.push((partition.ordinal, world_reads)),
        }
    }
    order(&mut hits);
    let total = hits.len();
    hits.truncate(request.k);
    reads.sort_by_key(|(ordinal, _)| *ordinal);
    Ok(VectorPage {
        present_micros: first.page_present(),
        valid_micros: request.valid.unwrap_or(first.present_micros),
        hits,
        total,
        reads: reads.into_iter().map(|(_, reads)| reads).collect(),
    })
}
