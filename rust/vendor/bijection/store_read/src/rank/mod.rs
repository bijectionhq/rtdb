//! Ranked reads (RTDB-PLAN Q1): text BM25, exact cosine nearest neighbours
//! and spatial predicates and nearest, defined once.
//!
//! A [`RankRequest`] is a whole ranked program: its frames (one per label
//! partition, each with its version and cut, never a label), the query, and
//! the digest of the code it expects. [`rank`] runs it over any
//! [`RangeSource`]. `fact_store`'s `Persistence::rank` runs it over the
//! port's own reads; RTDB runs this same code next to the data
//! (`rtdb.store.v1.Rank`). A request whose digest differs from
//! [`RANK_DIGEST`] is refused (`rtdb.store/rank-version`): the two sides
//! deploy together and nothing falls back.
//!
//! Sealed (family-held) frames hold ciphertext in the store, so the store
//! refuses them; [`plan`] splits a request by its frames' class into the
//! store's part and Bijection's, which runs [`rank_opened`] over a source
//! that opens values through custody. [`merge`] is the one combination of
//! the two parts' answers.
pub mod frame;
pub mod spatial;
pub mod text;
pub mod vector;
pub mod wire;

use anyhow::{
    ensure,
    Context,
};

use self::{
    frame::Frame,
    spatial::{
        SpatialPage,
        SpatialQuery,
    },
    text::{
        TextFeatures,
        TextPage,
        TextQuery,
    },
    vector::{
        VectorPage,
        VectorQuery,
    },
};
use crate::{
    source::RangeSource,
    store::KeyRange,
};

/// SHA-256 over this crate's, `kernel_text`'s and `geometry`'s sources and
/// the pinned dependency versions ([`PINS`]), computed by `build.rs` from
/// the files compiled: a copy that differs by one byte has another digest.
pub const RANK_DIGEST: &str = env!("STORE_READ_RANK_DIGEST");

/// The exact dependency versions the ranking semantics rest on (Unicode
/// tables, the Snowball stemmers, the numeric kernels and the spatial predicates).
/// They join [`RANK_DIGEST`]; `fact_store` checks them against Bijection's
/// `Cargo.lock` and RTDB against its own.
pub const PINS: &str = include_str!("../../pins.txt");

/// A ranked program.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RankRequest {
    /// The [`RANK_DIGEST`] the caller was built with.
    pub digest: String,
    pub frames: Vec<Frame>,
    pub query: RankQuery,
    pub mode: RankMode,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RankQuery {
    Text(TextQuery),
    Vector(VectorQuery),
    Spatial(SpatialQuery),
}

/// What a ranked read returns: its window, or (for one part of a read
/// split by seal class) what the merge needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RankMode {
    Window,
    Features,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RankReply {
    Text(TextPage),
    TextFeatures(TextFeatures),
    Vector(VectorPage),
    Spatial(SpatialPage),
}

impl RankReply {
    /// The consulted ranges of each partition, by ordinal.
    pub fn reads(&self) -> Vec<&[KeyRange]> {
        match self {
            Self::Text(page) => page.reads.iter().map(Vec::as_slice).collect(),
            Self::TextFeatures(features) => features.reads.iter().map(|(_, reads)| reads.as_slice()).collect(),
            Self::Vector(page) => page.reads.iter().map(Vec::as_slice).collect(),
            Self::Spatial(page) => page.reads.iter().map(Vec::as_slice).collect(),
        }
    }
}

impl RankRequest {
    /// A request for this build's code.
    pub fn new(frames: Vec<Frame>, query: RankQuery) -> Self {
        Self { digest: RANK_DIGEST.to_owned(), frames, query, mode: RankMode::Window }
    }

    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).context("rtdb.store/rank-invalid: the ranked program does not decode")
    }
}

impl RankReply {
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).context("rtdb.store/rank-invalid: the ranked reply does not decode")
    }
}

fn check_digest(request: &RankRequest) -> anyhow::Result<()> {
    ensure!(
        request.digest == RANK_DIGEST,
        "rtdb.store/rank-version: the request expects ranking code {} but this store runs {RANK_DIGEST}",
        request.digest
    );
    Ok(())
}

/// Run a ranked program over a store's ranges. Sealed frames are refused:
/// the store holds them as ciphertext (`rtdb.store/rank-sealed`).
pub async fn rank<S: RangeSource + ?Sized>(source: &S, request: RankRequest) -> anyhow::Result<RankReply> {
    check_digest(&request)?;
    ensure!(
        request.frames.iter().all(|frame| !frame.sealed),
        "rtdb.store/rank-sealed: a sealed partition is ranked by Bijection over opened values, never by the store"
    );
    run(source, &request).await
}

/// Run a ranked program over a source that opens sealed values (Bijection's
/// side of a read split by seal class).
pub async fn rank_opened<S: RangeSource + ?Sized>(source: &S, request: RankRequest) -> anyhow::Result<RankReply> {
    check_digest(&request)?;
    run(source, &request).await
}

async fn run<S: RangeSource + ?Sized>(source: &S, request: &RankRequest) -> anyhow::Result<RankReply> {
    let frames = &request.frames;
    Ok(match (&request.query, request.mode) {
        (RankQuery::Text(query), RankMode::Window) => RankReply::Text(text::finish(query, vec![text::gather(source, frames, query).await?])?),
        (RankQuery::Text(query), RankMode::Features) => RankReply::TextFeatures(text::gather(source, frames, query).await?),
        (RankQuery::Vector(query), _) => RankReply::Vector(vector::nearest(source, frames, query).await?),
        (RankQuery::Spatial(query), mode) => RankReply::Spatial(spatial::page(source, frames, query, mode == RankMode::Features).await?),
    })
}

/// A request split by its frames' seal class: the part the store ranks
/// (clear frames) and the part Bijection ranks over opened values (sealed
/// frames). Each part names only its own frames and partitions; ordinals
/// stay the caller's, so the parts merge back into one order.
#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub store: Option<RankRequest>,
    pub opened: Option<RankRequest>,
}

/// Split a request by seal class. A request whose frames are all of one
/// class is one part in window mode; a mixed one is two parts in features
/// mode.
pub fn plan(request: RankRequest) -> anyhow::Result<Plan> {
    let sealed = request.frames.iter().filter(|frame| frame.sealed).count();
    if sealed == 0 {
        return Ok(Plan { store: Some(request), opened: None });
    }
    if sealed == request.frames.len() {
        return Ok(Plan { store: None, opened: Some(request) });
    }
    let side = |want_sealed: bool| -> anyhow::Result<RankRequest> {
        let kept = request.frames.iter().enumerate().filter(|(_, frame)| frame.sealed == want_sealed).map(|(index, _)| index).collect::<Vec<_>>();
        let remap = |frame: usize| kept.iter().position(|kept| *kept == frame);
        let frames = kept.iter().map(|index| request.frames[*index].clone()).collect();
        let query = match &request.query {
            RankQuery::Text(query) => {
                let positions = query.partitions.iter().enumerate().filter_map(|(position, partition)| remap(partition.frame).map(|frame| (position, frame))).collect::<Vec<_>>();
                let partitions = positions.iter().map(|(position, frame)| text::TextPartition { frame: *frame, ..query.partitions[*position].clone() }).collect();
                let staged = query
                    .staged
                    .iter()
                    .filter_map(|staged| positions.iter().position(|(position, _)| *position == staged.partition).map(|partition| text::Staged { partition, ..staged.clone() }))
                    .collect();
                RankQuery::Text(TextQuery { partitions, staged, ..query.clone() })
            },
            RankQuery::Vector(query) => {
                let positions = query.partitions.iter().enumerate().filter_map(|(position, partition)| remap(partition.frame).map(|frame| (position, frame))).collect::<Vec<_>>();
                let partitions = positions.iter().map(|(position, frame)| vector::VectorPartition { frame: *frame, ..query.partitions[*position].clone() }).collect();
                let staged = query
                    .staged
                    .iter()
                    .filter_map(|staged| positions.iter().position(|(position, _)| *position == staged.partition).map(|partition| vector::StagedVector { partition, ..staged.clone() }))
                    .collect();
                RankQuery::Vector(VectorQuery { partitions, staged, ..query.clone() })
            },
            RankQuery::Spatial(query) => {
                let partitions = query.partitions.iter().filter_map(|partition| remap(partition.frame).map(|frame| spatial::SpatialPartition { frame, ..partition.clone() })).collect();
                RankQuery::Spatial(SpatialQuery { partitions, ..query.clone() })
            },
        };
        Ok(RankRequest { digest: request.digest.clone(), frames, query, mode: RankMode::Features })
    };
    Ok(Plan { store: Some(side(false)?), opened: Some(side(true)?) })
}

/// Combine the answers of a split request's parts (each with the part
/// request it answers) into the one answer the unsplit request has: the
/// same order, paging, totals and read sets.
pub fn merge(request: &RankRequest, parts: Vec<(RankRequest, RankReply)>) -> anyhow::Result<RankReply> {
    let first_frame = match &request.query {
        RankQuery::Text(query) => query.partitions.first().map(|partition| partition.frame),
        RankQuery::Vector(query) => query.partitions.first().map(|partition| partition.frame),
        RankQuery::Spatial(query) => query.partitions.first().map(|partition| partition.frame),
    };
    let frame = request.frames.get(first_frame.context("A ranked read names a partition")?).context("A partition names one of the request's frames")?;
    let valid = |valid: Option<i64>| valid.unwrap_or(frame.present_micros);
    Ok(match &request.query {
        RankQuery::Text(query) => {
            let features = parts
                .into_iter()
                .map(|(_, part)| match part {
                    RankReply::TextFeatures(features) => Ok(features),
                    _ => anyhow::bail!("A split text read merges gathered features"),
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let mut page = text::finish(query, features)?;
            (page.present_micros, page.valid_micros) = (frame.page_present(), valid(query.valid));
            RankReply::Text(page)
        },
        RankQuery::Vector(query) => {
            let (mut hits, mut total, mut reads) = (Vec::new(), 0, Vec::new());
            for (part, reply) in parts {
                let (RankQuery::Vector(part), RankReply::Vector(page)) = (&part.query, reply) else {
                    anyhow::bail!("A split vector read merges vector pages");
                };
                hits.extend(page.hits);
                total += page.total;
                reads.extend(part.partitions.iter().map(|partition| partition.ordinal).zip(page.reads));
            }
            vector::order(&mut hits);
            hits.truncate(query.k);
            reads.sort_by_key(|(ordinal, _)| *ordinal);
            RankReply::Vector(VectorPage {
                present_micros: frame.page_present(),
                valid_micros: valid(query.valid),
                hits,
                total,
                reads: reads.into_iter().map(|(_, reads)| reads).collect(),
            })
        },
        RankQuery::Spatial(query) => {
            let (mut rows, mut reads) = (Vec::new(), Vec::new());
            for (part, reply) in parts {
                let (RankQuery::Spatial(part), RankReply::Spatial(page)) = (&part.query, reply) else {
                    anyhow::bail!("A split spatial read merges spatial pages");
                };
                rows.extend(page.rows);
                reads.extend(part.partitions.iter().map(|partition| partition.ordinal).zip(page.reads));
            }
            let (rows, more, next_distance_m) = spatial::window(query, rows);
            reads.sort_by_key(|(ordinal, _)| *ordinal);
            RankReply::Spatial(SpatialPage {
                present_micros: frame.page_present(),
                valid_micros: valid(query.valid),
                rows,
                more,
                next_distance_m,
                reads: reads.into_iter().map(|(_, reads)| reads).collect(),
            })
        },
    })
}
