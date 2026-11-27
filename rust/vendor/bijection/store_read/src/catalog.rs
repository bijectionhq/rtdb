//! The installed catalog of owned relations: relation and path
//! declarations (catalog keys holding JSON), and the composite values an
//! ordered path indexes.
use anyhow::{
    ensure,
    Context,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    event::Image,
    keys,
    layout::Place,
    rank::{
        spatial,
        text,
        vector,
    },
    segment,
    source::{
        point_value,
        RangeSource,
    },
    store::Version,
};

/// How a relation's rows get their valid time (RTDB `DeclaredValidity`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Validity {
    /// Writers may state valid bounds; the default is `[system_time, MAX)`.
    Explicit,
    /// Every write is valid over `[system_time, MAX)`; stated bounds are refused.
    Acceptance,
    /// A point read must state its valid instant.
    BoundedPeriod,
}

/// An installed relation declaration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Declaration {
    pub fields: Vec<String>,
    pub validity: Validity,
    /// Rows are valid over `[created, created + lifetime)`; requires
    /// acceptance validity.
    pub lifetime_micros: Option<i64>,
    /// The adapter's own schema document (for example the Arrow schema),
    /// opaque to the engine.
    #[serde(with = "hex_bytes")]
    pub document: Vec<u8>,
    /// Set once by [`OwnedStore::retire`]: the retention it was retired with.
    #[serde(default)]
    pub retired: Option<i64>,
}

/// Hex for opaque bytes inside JSON catalog values.
pub mod hex_bytes {
    pub fn serialize<S: serde::Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>())
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(text.get(at..at + 2).unwrap_or("x"), 16))
            .collect::<Result<_, _>>()
            .map_err(serde::de::Error::custom)
    }
}

/// A secondary index of a relation: its columns and its kind. Every kind
/// writes its entries in the same append as the event they index
/// (RANKED §4.1): enter entries at each put image, leave entries at the
/// image it replaces.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathDeclaration {
    pub columns: Vec<String>,
    /// Each composite value maps to at most one id at any valid instant,
    /// enforced at commit (`rtdb.owned/unique-taken`).
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub kind: PathKind,
}

/// What a path's entries hold (gap G1).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PathKind {
    /// One entry per image at its composite column value, ordered.
    #[default]
    Ordered,
    /// An inverted text index over the single column (RANKED §1.1): one
    /// posting per distinct analysed term and one document entry per image
    /// whose cell holds text.
    Search {
        /// A `kernel_text::Analyzer` name.
        analyzer: String,
        /// `kernel_text::ANALYZER_VERSION` at declaration; a read under
        /// another version is refused.
        analyzer_version: i32,
        /// The path keeps corpus counters (documents and tokens) with
        /// history in keys. Only relations whose rows are valid from their
        /// commit onwards keep them (acceptance validity, no lifetime):
        /// their present corpus changes only at commits.
        counted: bool,
    },
    /// Exact cosine vectors over the first column (RANKED §1.2): one
    /// vector entry per image with a vector, and one entry per named
    /// filter column holding a Binary key.
    Vector {
        dimensions: u16,
        /// The declared recall target (f64 bits) of the approximate route;
        /// absent from the store cut, so every read is exact.
        recall: Option<u64>,
        /// The filter columns, by ordinal.
        filters: Vec<String>,
    },
    /// Spatial cells over the first column (a location), partitioned by the
    /// rest (RANKED §1.3): one entry per cell key of each image.
    Spatial {
        /// `tagged-wkb-v1`, `bijection-geometry-v1` or `bijection-position-v1`.
        encoding: String,
    },
}

impl PathDeclaration {
    /// The columns of an ordered path; other kinds have no composite order.
    pub fn ordered(&self) -> anyhow::Result<&[String]> {
        ensure!(self.kind == PathKind::Ordered, "rtdb.owned/path-kind: the path is not an ordered path");
        Ok(&self.columns)
    }

    /// The `(values, entry value)` pairs one image writes into this path.
    pub fn entries(&self, image: &Image) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match &self.kind {
            PathKind::Ordered => Ok(vec![(path_values(&self.columns, image), Vec::new())]),
            PathKind::Search { analyzer, .. } => text::entries(text::analyzer(analyzer)?, &self.columns[0], image),
            PathKind::Vector { filters, .. } => vector::entries(&self.columns[0], filters, image),
            PathKind::Spatial { encoding } => spatial::entries(&self.columns, encoding, image),
        }
    }
}

/// The composite path value of an image: one segment per column, each
/// `0x00` (absent) or `0x01 ‖ value`. Field values are order-preserving
/// encodings (the adapter's duty), so the composite is too.
pub fn path_values(columns: &[String], image: &Image) -> Vec<u8> {
    let parts = columns.iter().map(|column| image.get(column).map(Vec::as_slice)).collect::<Vec<_>>();
    path_values_of(&parts)
}

/// The composite path value of explicit column values (`None` is absent).
pub fn path_values_of(values: &[Option<&[u8]>]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        match value {
            None => out.extend_from_slice(&segment::segment(&[0])),
            Some(value) => out.extend_from_slice(&segment::segment(&[&[1u8][..], value].concat())),
        }
    }
    out
}


/// The installed declaration of `relation` at `at` (the latest when `None`).
pub async fn declaration<S: RangeSource + ?Sized>(source: &S, place: Place, relation: &str, at: Option<Version>) -> anyhow::Result<Option<Declaration>> {
    let (_, value) = point_value(source, keys::relation(place, relation), at).await?;
    value.map(|bytes| serde_json::from_slice(&bytes).context("Invalid owned declaration")).transpose()
}

/// The installed path `path` of `relation` at `at` (the latest when `None`).
pub async fn path<S: RangeSource + ?Sized>(source: &S, place: Place, relation: &str, path: &str, at: Option<Version>) -> anyhow::Result<Option<PathDeclaration>> {
    let (_, value) = point_value(source, keys::path(place, relation, path), at).await?;
    value.map(|bytes| serde_json::from_slice(&bytes).context("Invalid owned path")).transpose()
}
