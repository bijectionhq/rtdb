use crate::{ColumnId, Expr, Field, Schema, TableBinding};
use rtdb_types::{Anomaly, Iid, LogicalType, Result, ScalarType, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TemporalFilter {
    Default,
    All,
    At(Expr),
    FromTo { from: Expr, to: Expr },
    Between { from: Expr, to: Expr },
}

impl TemporalFilter {
    pub fn expressions(&self) -> Vec<&Expr> {
        match self { Self::At(at) => vec![at], Self::FromTo { from, to } | Self::Between { from, to } => vec![from, to], _ => vec![] }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Scan {
    pub table: TableBinding,
    pub columns: Vec<Field>,
    pub valid_time: TemporalFilter,
    pub system_time: TemporalFilter,
    pub clamp_valid_time: bool,
    pub iids: Option<BTreeSet<Iid>>,
    #[serde(default)]
    pub predicates: Vec<Expr>,
    /// Set when the scan's rows feed a bounded sort by `search_score(column, query)`; see fts.allium.
    #[serde(default)]
    pub top_k: Option<ScanTopK>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordered_top_k: Option<OrderedTopK>,
    /// The MATCHes the scan evaluates itself, each projected as a boolean column of its own; see fts.allium.
    #[serde(default)]
    pub matches: Vec<ScanMatch>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanTopK {
    pub column: ColumnId,
    pub query: Expr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corpus: Option<Box<[Expr; 3]>>,
    #[serde(with = "analyzer_name")]
    pub analyzer: rtdb_text::Analyzer,
    pub limit: Expr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<Expr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<ColumnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ascending_id: Option<ColumnId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderedTopK {
    pub order: Vec<SortExpr>,
    pub residuals: Vec<Expr>,
    /// The output is one plain column, also the complete sort key.
    pub distinct: bool,
}

/// `match(column, query[, analyzer])` with a constant query, as the scan's column `id`: the scan may answer it from an index under the same analyzer, reading no text.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanMatch {
    pub id: ColumnId, pub column: String, pub query: Expr,
    #[serde(with = "analyzer_name")]
    pub analyzer: rtdb_text::Analyzer,
}

/// Serializes a search analyzer as its name.
pub(crate) mod analyzer_name {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub(crate) fn serialize<S: Serializer>(analyzer: &rtdb_text::Analyzer, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(analyzer.name())
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<rtdb_text::Analyzer, D::Error> {
        let name = String::deserialize(deserializer)?;
        rtdb_text::Analyzer::parse(&name).ok_or_else(|| D::Error::custom(format!("unknown search analyzer {name}")))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NamedExpr { pub field: Field, pub expr: Expr }

pub fn unnest_element_type(ty: &LogicalType) -> LogicalType {
    let types = ty.legs().into_iter().filter_map(|ty| match ty {
        LogicalType::List { kind: rtdb_types::ListKind::List | rtdb_types::ListKind::Set | rtdb_types::ListKind::FixedSizeList(_), element } => Some(*element),
        _ => None,
    }).collect::<Vec<_>>();
    if types.is_empty() { LogicalType::Null } else { LogicalType::join_all(&types) }
}

/// An immutable lexical binding; references retain this definition and are opened within its `Let` scope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MaterializedBinding { pub name: String, pub relation: Arc<Relation> }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinKind { Inner, Left, Right, Full, Semi, Anti, Cross, Single }

impl JoinKind {
    fn result_schema(self, left: &Schema, right: &Schema) -> Result<Schema> {
        match self {
            Self::Semi | Self::Anti => Ok(left.clone()),
            Self::Left | Self::Single => left.concat(&right.nullable()?),
            Self::Right => left.nullable()?.concat(right),
            Self::Full => left.nullable()?.concat(&right.nullable()?),
            Self::Inner | Self::Cross => left.concat(right),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Evaluates the dependent relation for each input row with apply-local argument bindings.
/// Scalar and Pull reject multiple dependent rows; Scalar, Pull, and PullMany yield null on no rows.
pub enum ApplyKind {
    Inner, Left, Single, Semi, Anti, Mark, Scalar, Exists, Pull, PullMany,
    /// Left Apply with page-batched expansion of a captured list when the reached layouts are compatible.
    LeftUnnest,
}

impl ApplyKind {
    pub fn result_schema(self, input: &Schema, dependent: &Schema, output: Option<&Field>) -> Result<Schema> {
        let invalid = |message| Anomaly::fault("rtdb.plan/invalid-relation", message);
        match self {
            Self::Inner | Self::Left | Self::LeftUnnest | Self::Single | Self::Semi | Self::Anti => {
                if output.is_some() { return Err(invalid("Relational apply cannot declare a scalar output")); }
                match self {
                    Self::Inner => input.concat(dependent),
                    Self::Left | Self::LeftUnnest | Self::Single => input.concat(&dependent.nullable()?),
                    _ => Ok(input.clone()),
                }
            }
            _ => {
                let mut output = output.cloned().ok_or_else(|| invalid("Scalar apply requires an output field"))?;
                output.data_type = match self {
                    Self::Mark | Self::Scalar => {
                        let [field] = dependent.fields() else { return Err(invalid("Mark and scalar apply require one dependent column")); };
                        if self == Self::Mark {
                            if field.data_type.legs().iter().any(|ty| !matches!(ty, LogicalType::Null | LogicalType::Nothing | LogicalType::Scalar(ScalarType::Bool))) {
                                return Err(invalid("Mark apply requires a Boolean or null dependent column"));
                            }
                            LogicalType::Scalar(ScalarType::Bool).maybe()
                        } else { field.data_type.clone().maybe() }
                    }
                    Self::Exists => LogicalType::Scalar(ScalarType::Bool),
                    Self::Pull | Self::PullMany => {
                        let document = LogicalType::Struct(dependent.fields().iter().map(|field| (field.name.clone(), field.data_type.clone())).collect());
                        if self == Self::PullMany { LogicalType::list(document).maybe() } else { document.maybe() }
                    }
                    _ => unreachable!(),
                };
                input.concat(&Schema::new(vec![output])?)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOperator { Union, Intersect, Except }

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum OrdinalityScope { #[default] Page, Relation }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SortExpr { pub expr: Expr, pub descending: bool, pub nulls_first: bool }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PercentileKind { Continuous, Discrete }

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
/// Parameters use zero-based indices and must contain numeric values when the cursor opens.
/// Literal fractions are unrestricted: discrete percentiles clamp them, while continuous ranks can fail outside the input.
pub enum PercentileFraction { Integer(i64), Literal(#[serde(with = "percentile_fraction_bits")] f64), Parameter { index: usize } }

mod percentile_fraction_bits {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> std::result::Result<S::Ok, S::Error> { serializer.serialize_u64(value.to_bits()) }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<f64, D::Error> { u64::deserialize(deserializer).map(f64::from_bits) }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count, CountStar, Sum, Average, Min, Max, ArrayAgg, VecAgg, Every, Any,
    VariancePopulation, VarianceSample, StddevPopulation, StddevSample,
    /// Sorts the nonnull column in `AggregateExpr.argument`; DISTINCT and `AggregateExpr.order_by` must be absent.
    /// Continuous output is nullable F64; discrete output retains the nullable input type.
    Percentile { kind: PercentileKind, fraction: PercentileFraction, descending: bool },
    Alias(AggregateAlias),
    Named(AggregateName),
}

/// Supported factory spellings with the same evaluation as a canonical aggregate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateAlias {
    BoolAnd, RowCountUnderscore, ArrayAggUnderscore,
    CountAll, SumAll, AverageAll, MinAll, MaxAll,
    BoolAndHyphen, BoolOrHyphen, VecAggHyphen, VariancePopulationHyphen, VarianceSampleHyphen,
    StddevPopulationHyphen, StddevSampleHyphen, CountAllHyphen, SumAllHyphen, AverageAllHyphen, MinAllHyphen, MaxAllHyphen,
    CountDistinctHyphen, SumDistinctHyphen, AverageDistinctHyphen, MinDistinctHyphen, MaxDistinctHyphen, ArrayAggDistinctHyphen, ArrayAggUnderscoreDistinctHyphen,
}
impl AggregateAlias {
    /// Returns the factory symbol retained in the plan.
    pub fn name(self) -> &'static str {
        match self {
            Self::BoolAnd => "bool_and", Self::RowCountUnderscore => "row_count", Self::ArrayAggUnderscore => "array_agg",
            Self::CountAll => "count_all", Self::SumAll => "sum_all", Self::AverageAll => "avg_all", Self::MinAll => "min_all", Self::MaxAll => "max_all",
            Self::BoolAndHyphen => "bool-and", Self::BoolOrHyphen => "bool-or", Self::VecAggHyphen => "vec-agg",
            Self::VariancePopulationHyphen => "var-pop", Self::VarianceSampleHyphen => "var-samp", Self::StddevPopulationHyphen => "stddev-pop", Self::StddevSampleHyphen => "stddev-samp",
            Self::CountAllHyphen => "count-all", Self::SumAllHyphen => "sum-all", Self::AverageAllHyphen => "avg-all", Self::MinAllHyphen => "min-all", Self::MaxAllHyphen => "max-all",
            Self::CountDistinctHyphen => "count-distinct", Self::SumDistinctHyphen => "sum-distinct", Self::AverageDistinctHyphen => "avg-distinct", Self::MinDistinctHyphen => "min-distinct", Self::MaxDistinctHyphen => "max-distinct", Self::ArrayAggDistinctHyphen => "array-agg-distinct", Self::ArrayAggUnderscoreDistinctHyphen => "array_agg-distinct",
        }
    }
    /// Resolves the spelling to its evaluator variant.
    pub fn canonical(self) -> AggregateFunction {
        match self {
            Self::BoolAnd | Self::BoolAndHyphen => AggregateFunction::Every,
            Self::BoolOrHyphen => AggregateFunction::Any,
            Self::RowCountUnderscore => AggregateFunction::CountStar,
            Self::ArrayAggUnderscore | Self::ArrayAggDistinctHyphen | Self::ArrayAggUnderscoreDistinctHyphen => AggregateFunction::ArrayAgg,
            Self::VecAggHyphen => AggregateFunction::VecAgg,
            Self::CountAll | Self::CountAllHyphen | Self::CountDistinctHyphen => AggregateFunction::Count,
            Self::SumAll | Self::SumAllHyphen | Self::SumDistinctHyphen => AggregateFunction::Sum,
            Self::AverageAll | Self::AverageAllHyphen | Self::AverageDistinctHyphen => AggregateFunction::Average,
            Self::MinAll | Self::MinAllHyphen | Self::MinDistinctHyphen => AggregateFunction::Min,
            Self::MaxAll | Self::MaxAllHyphen | Self::MaxDistinctHyphen => AggregateFunction::Max,
            Self::VariancePopulationHyphen => AggregateFunction::VariancePopulation,
            Self::VarianceSampleHyphen => AggregateFunction::VarianceSample,
            Self::StddevPopulationHyphen => AggregateFunction::StddevPopulation,
            Self::StddevSampleHyphen => AggregateFunction::StddevSample,
        }
    }
    /// Returns the quantifier fixed by this spelling, or None when it remains a separate argument.
    pub fn distinct(self) -> Option<bool> {
        match self {
            Self::CountAll | Self::SumAll | Self::AverageAll | Self::MinAll | Self::MaxAll | Self::CountAllHyphen | Self::SumAllHyphen | Self::AverageAllHyphen | Self::MinAllHyphen | Self::MaxAllHyphen => Some(false),
            Self::CountDistinctHyphen | Self::SumDistinctHyphen | Self::AverageDistinctHyphen | Self::MinDistinctHyphen | Self::MaxDistinctHyphen | Self::ArrayAggDistinctHyphen | Self::ArrayAggUnderscoreDistinctHyphen => Some(true),
            _ => None,
        }
    }
}

/// A validated ordinary aggregate factory symbol with its original spelling.
/// Factory lookup follows XTQL name normalization; the spelling is retained for plan descriptions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AggregateName { spelling: String, canonical: CanonicalAggregate, distinct: bool }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CanonicalAggregate {
    Count, CountStar, Sum, Average, Min, Max, ArrayAgg, VecAgg, Every, Any,
    VariancePopulation, VarianceSample, StddevPopulation, StddevSample,
}

impl CanonicalAggregate {
    fn into_function(self) -> AggregateFunction {
        match self {
            Self::Count => AggregateFunction::Count, Self::CountStar => AggregateFunction::CountStar,
            Self::Sum => AggregateFunction::Sum, Self::Average => AggregateFunction::Average,
            Self::Min => AggregateFunction::Min, Self::Max => AggregateFunction::Max,
            Self::ArrayAgg => AggregateFunction::ArrayAgg, Self::VecAgg => AggregateFunction::VecAgg,
            Self::Every => AggregateFunction::Every, Self::Any => AggregateFunction::Any,
            Self::VariancePopulation => AggregateFunction::VariancePopulation,
            Self::VarianceSample => AggregateFunction::VarianceSample,
            Self::StddevPopulation => AggregateFunction::StddevPopulation,
            Self::StddevSample => AggregateFunction::StddevSample,
        }
    }
}

impl AggregateName {
    /// Rejects names without an ordinary aggregate factory, including unsupported ALL/DISTINCT combinations.
    pub fn new(spelling: impl Into<String>) -> Result<Self> {
        use CanonicalAggregate as C;
        let spelling = spelling.into();
        let normalized = crate::Identifier::unquoted(&spelling).normalized();
        let (canonical, distinct) = match normalized.as_str() {
            "row_count" => (C::CountStar, false),
            "count" | "count_all" => (C::Count, false), "count_distinct" => (C::Count, true),
            "sum" | "sum_all" => (C::Sum, false), "sum_distinct" => (C::Sum, true),
            "avg" | "avg_all" => (C::Average, false), "avg_distinct" => (C::Average, true),
            "min" | "min_all" => (C::Min, false), "min_distinct" => (C::Min, true),
            "max" | "max_all" => (C::Max, false), "max_distinct" => (C::Max, true),
            "array_agg" => (C::ArrayAgg, false), "array_agg_distinct" => (C::ArrayAgg, true),
            "vec_agg" => (C::VecAgg, false), "every" | "bool_and" => (C::Every, false), "bool_or" => (C::Any, false),
            "var_pop" => (C::VariancePopulation, false), "var_samp" => (C::VarianceSample, false),
            "stddev_pop" => (C::StddevPopulation, false), "stddev_samp" => (C::StddevSample, false),
            _ => return Err(Anomaly::incorrect("rtdb.plan/aggregate-name", format!("Unknown aggregate factory: {spelling}"))),
        };
        Ok(Self { spelling, canonical, distinct })
    }
    /// Returns the original, unnormalized factory symbol.
    pub fn spelling(&self) -> &str { &self.spelling }
    /// Whether this factory symbol requests duplicate elimination.
    pub fn distinct(&self) -> bool { self.distinct }
}
impl TryFrom<String> for AggregateName {
    type Error = Anomaly;
    fn try_from(spelling: String) -> Result<Self> { Self::new(spelling) }
}
impl From<AggregateName> for String { fn from(name: AggregateName) -> Self { name.spelling } }

impl AggregateFunction {
    /// Resolves alias spellings to evaluator variants.
    pub fn canonical(&self) -> Self { match self { Self::Alias(alias) => alias.canonical(), Self::Named(name) => name.canonical.into_function(), function => function.clone() } }
    /// Returns the quantifier fixed by a factory spelling, when present.
    pub fn quantifier(&self) -> Option<bool> { match self { Self::Alias(alias) => alias.distinct(), Self::Named(name) => Some(name.distinct()), _ => None } }
    /// Includes the descriptor and any retained original-name capacity.
    pub fn retained_bytes(&self) -> Result<usize> {
        std::mem::size_of::<Self>().checked_add(match self { Self::Named(name) => name.spelling.capacity(), _ => 0 })
            .ok_or_else(|| Anomaly::incorrect("rtdb.plan/aggregate-size", "Aggregate descriptor exceeds addressable memory").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded))
    }
    /// Returns the factory symbol used by plan descriptions.
    pub fn name(&self) -> &str {
        match self {
            Self::Count => "count", Self::CountStar => "row-count", Self::Sum => "sum", Self::Average => "avg", Self::Min => "min", Self::Max => "max",
            Self::ArrayAgg => "array-agg", Self::VecAgg => "vec_agg", Self::Every => "every", Self::Any => "bool_or",
            Self::VariancePopulation => "var_pop", Self::VarianceSample => "var_samp", Self::StddevPopulation => "stddev_pop", Self::StddevSample => "stddev_samp",
            Self::Percentile { kind: PercentileKind::Continuous, .. } => "percentile_cont", Self::Percentile { .. } => "percentile_disc", Self::Alias(alias) => alias.name(), Self::Named(name) => name.spelling(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub field: Field,
    pub function: AggregateFunction,
    pub argument: Option<Expr>,
    pub distinct: bool,
    pub filter: Option<Expr>,
    pub order_by: Vec<SortExpr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowFunction {
    RowNumber,
    Lead { column: ColumnId, offset: u64 },
    Lag { column: ColumnId, offset: u64 },
}

impl WindowFunction {
    /// LEAD/LAG retain the input type with nullability, including at offset zero.
    /// An absent column or an offset above the signed 64-bit range is an error.
    pub fn result_type(&self, input: &Schema) -> Result<LogicalType> {
        match self {
            Self::RowNumber => Ok(LogicalType::Scalar(ScalarType::I64)),
            Self::Lead { column, offset } | Self::Lag { column, offset } => {
                if *offset > i64::MAX as u64 { return Err(Anomaly::incorrect("xtdb/window-offset", "Window offset exceeds signed 64 bits")); }
                input.get(*column).map(|field| field.data_type.clone().maybe()).ok_or_else(|| Anomaly::incorrect("xtdb/window-column", "Window input column is absent"))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WindowExpr { pub field: Field, pub function: WindowFunction }

#[derive(Clone, Debug)]
pub struct LiteralArrowPages(Arc<[rtdb_columnar::OwnedBatch]>);

impl LiteralArrowPages {
    pub fn new(pages: Vec<rtdb_columnar::OwnedBatch>) -> Self { Self(pages.into()) }
    pub fn pages(&self) -> &[rtdb_columnar::OwnedBatch] { &self.0 }

    fn serialize_legacy<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(LegacyLiteralPage))
    }
}

struct LegacyLiteralPage<'a>(&'a rtdb_columnar::OwnedBatch);

impl Serialize for LegacyLiteralPage<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq((0..self.0.batch().num_rows()).map(|row| LegacyLiteralRow(self.0, row)))
    }
}

struct LegacyLiteralRow<'a>(&'a rtdb_columnar::OwnedBatch, usize);

impl Serialize for LegacyLiteralRow<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeSeq};

        let batch = self.0.batch();
        let mut row = serializer.serialize_seq(Some(batch.num_columns()))?;
        for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
            let value = rtdb_columnar::read_value(field, column.as_ref(), self.1).map_err(S::Error::custom)?;
            row.serialize_element(&Expr::Literal(value))?;
        }
        row.end()
    }
}

impl PartialEq for LiteralArrowPages {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
            || self.0.iter().map(|page| page.batch()).eq(other.0.iter().map(|page| page.batch()))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RelationNode {
    Values { rows: Vec<Vec<Expr>> },
    /// Rows from an explicit sequence of input pages, including empty pages.
    LiteralPages { pages: Vec<Vec<Vec<Expr>>> },
    List { expr: Expr, element: Field, #[serde(default)] page_rows: Option<std::num::NonZeroUsize> },
    /// Materializes the selected list during opening and publishes one page, including an empty page.
    /// Statically non-list scalar payloads are not demanded; null and non-list results produce an empty page.
    ColumnTable { expr: Expr, element: Field },
    #[serde(alias = "PageOrdinality")]
    Ordinality { input: Arc<Relation>, field: Field, #[serde(default)] scope: OrdinalityScope },
    Scan(Scan),
    Project { input: Arc<Relation>, expressions: Vec<NamedExpr> },
    /// Appends new fields to the input schema in expression order.
    /// Each expression reads the original input fields.
    Extend { input: Arc<Relation>, expressions: Vec<NamedExpr> },
    Rename { input: Arc<Relation>, prefix: String, expressions: Vec<NamedExpr> },
    Let { binding: Arc<MaterializedBinding>, body: Arc<Relation> },
    Reference { binding: Arc<MaterializedBinding> },
    Filter { input: Arc<Relation>, predicate: Expr },
    Join { left: Arc<Relation>, right: Arc<Relation>, kind: JoinKind, condition: Option<Expr> },
    Apply { input: Arc<Relation>, dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>, kind: ApplyKind, output: Option<Field> },
    /// Appends a nullable Boolean mark to each input row.
    /// - `dependent` has one candidate column; `arguments` bind unique zero-based dependent parameters from input expressions.
    /// - `comparison` reads the combined input/candidate schema and returns Boolean or null.
    /// - `output` has nullable Boolean type and is appended to the input schema.
    /// - A match yields true, an unmatched unknown comparison yields null, and an empty or all-false comparison set yields false.
    /// Empty arguments select a shared build before input evaluation, with scalar equality hash selection and whole-build null facts.
    /// Correlated builds evaluate their complete comparison batch for each input row; shared comparisons stop at true.
    Mark { input: Arc<Relation>, dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>, #[serde(default)] condition: Option<Expr>, comparison: Expr, output: Field },
    Aggregate { input: Arc<Relation>, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr> },
    Window { input: Arc<Relation>, partition_by: Vec<ColumnId>, order_by: Vec<SortExpr>, expressions: Vec<WindowExpr> },
    Sort { input: Arc<Relation>, order_by: Vec<SortExpr> },
    SortLimit { input: Arc<Relation>, order_by: Vec<SortExpr>, offset: Option<Expr>, fetch: Option<Expr> },
    Limit { input: Arc<Relation>, offset: Option<Expr>, fetch: Option<Expr> },
    Distinct { input: Arc<Relation> },
    Set { left: Arc<Relation>, right: Arc<Relation>, operator: SetOperator, all: bool },
    Unnest { input: Arc<Relation>, expr: Expr, element: Field, ordinality: Option<Field>, outer: bool },
    /// Input is ordered by IID then valid-from, with nonoverlapping intervals per IID.
    PatchGaps { input: Arc<Relation>, iid: ColumnId, valid_from: ColumnId, valid_to: ColumnId, document: ColumnId, from: Expr, to: Expr },
    #[serde(rename(serialize = "LiteralPages"), skip_deserializing)]
    ArrowLiteralPages { #[serde(serialize_with = "LiteralArrowPages::serialize_legacy")] pages: LiteralArrowPages },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinBuildReason { SmallerInput, KnownInput, StableTie, UnknownInputs, ExistingOrder }

impl JoinBuildReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SmallerInput => "smaller-input",
            Self::KnownInput => "known-input",
            Self::StableTie => "stable-tie",
            Self::UnknownInputs => "unknown-inputs",
            Self::ExistingOrder => "existing-order",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinPlanEvidence {
    pub build_left: bool,
    pub build_rows: Option<u64>,
    pub probe_rows: Option<u64>,
    pub reason: JoinBuildReason,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "RelationParts", into = "RelationParts")]
pub struct Relation { schema: Schema, node: RelationNode, join_evidence: Option<JoinPlanEvidence> }

impl PartialEq for Relation {
    fn eq(&self, other: &Self) -> bool { self.schema == other.schema && self.node == other.node }
}

#[derive(Serialize, Deserialize)]
struct RelationParts { schema: Schema, node: RelationNode }

impl Relation {
    pub fn new(schema: Schema, node: RelationNode) -> Result<Self> {
        let result = Self { schema, node, join_evidence: None };
        result.validate()?;
        Ok(result)
    }
    pub fn schema(&self) -> &Schema { &self.schema }
    pub fn node(&self) -> &RelationNode { &self.node }
    pub fn empty(schema: Schema) -> Self { Self { schema, node: RelationNode::Values { rows: vec![] }, join_evidence: None } }
    pub fn unit() -> Self { Self { schema: Schema::default(), node: RelationNode::Values { rows: vec![vec![]] }, join_evidence: None } }

    pub fn with_join_evidence(mut self, evidence: JoinPlanEvidence) -> Self {
        self.join_evidence = Some(evidence);
        self
    }
    pub fn join_evidence(&self) -> Option<JoinPlanEvidence> { self.join_evidence }

    pub fn scan(scan: Scan) -> Result<Self> { Self::new(Schema::new(scan.columns.clone())?, RelationNode::Scan(scan)) }
    pub fn project(self: Arc<Self>, expressions: Vec<NamedExpr>) -> Result<Self> {
        Self::new(Schema::new(expressions.iter().map(|expr| expr.field.clone()).collect())?, RelationNode::Project { input: self, expressions })
    }
    pub fn extend(self: Arc<Self>, expressions: Vec<NamedExpr>) -> Result<Self> {
        let schema = Schema::new(self.schema.fields().iter().cloned().chain(expressions.iter().map(|expr| expr.field.clone())).collect())?;
        Self::new(schema, RelationNode::Extend { input: self, expressions })
    }
    pub fn filter(self: Arc<Self>, predicate: Expr) -> Result<Self> {
        Self::new(self.schema.clone(), RelationNode::Filter { input: self, predicate })
    }
    pub fn join(left: Arc<Self>, right: Arc<Self>, kind: JoinKind, condition: Option<Expr>) -> Result<Self> {
        let schema = kind.result_schema(left.schema(), right.schema())?;
        Self::new(schema, RelationNode::Join { left, right, kind, condition })
    }
    pub fn with_ordinality(self: Arc<Self>, field: Field, scope: OrdinalityScope) -> Result<Self> {
        let schema = Schema::new(self.schema.fields().iter().cloned().chain([field.clone()]).collect())?;
        Self::new(schema, RelationNode::Ordinality { input: self, field, scope })
    }
    pub fn sort(self: Arc<Self>, order_by: Vec<SortExpr>) -> Result<Self> {
        Self::new(self.schema.clone(), RelationNode::Sort { input: self, order_by })
    }
    pub fn limit(self: Arc<Self>, offset: Option<Expr>, fetch: Option<Expr>) -> Result<Self> {
        Self::new(self.schema.clone(), RelationNode::Limit { input: self, offset, fetch })
    }

    pub fn children(&self) -> Vec<&Arc<Self>> {
        match &self.node {
            RelationNode::Values { .. } | RelationNode::LiteralPages { .. } | RelationNode::ArrowLiteralPages { .. } | RelationNode::List { .. } | RelationNode::ColumnTable { .. } | RelationNode::Scan(_) | RelationNode::Reference { .. } => vec![],
            RelationNode::Project { input, .. } | RelationNode::Extend { input, .. } | RelationNode::Rename { input, .. } | RelationNode::Filter { input, .. }
            | RelationNode::Aggregate { input, .. } | RelationNode::Window { input, .. } | RelationNode::Sort { input, .. } | RelationNode::SortLimit { input, .. }
            | RelationNode::Limit { input, .. } | RelationNode::Distinct { input }
            | RelationNode::Unnest { input, .. } | RelationNode::Ordinality { input, .. } | RelationNode::PatchGaps { input, .. } => vec![input],
            RelationNode::Join { left, right, .. } | RelationNode::Set { left, right, .. } => vec![left, right],
            RelationNode::Apply { input, dependent, .. } | RelationNode::Mark { input, dependent, .. } => vec![input, dependent],
            RelationNode::Let { binding, body } => vec![&binding.relation, body],
        }
    }

    pub fn expressions(&self) -> Vec<&Expr> {
        match &self.node {
            RelationNode::Values { rows } => rows.iter().flatten().collect(),
            RelationNode::LiteralPages { pages } => pages.iter().flatten().flatten().collect(),
            RelationNode::ArrowLiteralPages { .. } => vec![],
            RelationNode::Scan(scan) => scan.valid_time.expressions().into_iter().chain(scan.system_time.expressions()).chain(scan.predicates.iter()).collect(),
            RelationNode::Project { expressions, .. } | RelationNode::Extend { expressions, .. } | RelationNode::Rename { expressions, .. } => expressions.iter().map(|expr| &expr.expr).collect(),
            RelationNode::Filter { predicate, .. } => vec![predicate],
            RelationNode::Join { condition, .. } => condition.iter().collect(),
            RelationNode::Apply { arguments, .. } => arguments.iter().map(|(_, expr)| expr).collect(),
            RelationNode::Mark { arguments, condition, comparison, .. } => arguments.iter().map(|(_, expr)| expr).chain(condition.iter()).chain(std::iter::once(comparison)).collect(),
            RelationNode::Aggregate { groups, aggregates, .. } => groups.iter().map(|expr| &expr.expr).chain(aggregates.iter().flat_map(|agg| {
                agg.argument.iter().chain(agg.filter.iter()).chain(agg.order_by.iter().map(|order| &order.expr))
            })).collect(),
            RelationNode::Sort { order_by, .. } => order_by.iter().map(|order| &order.expr).collect(),
            RelationNode::SortLimit { order_by, offset, fetch, .. } => order_by.iter().map(|order| &order.expr).chain(offset.iter()).chain(fetch.iter()).collect(),
            RelationNode::Window { order_by, .. } => order_by.iter().map(|order| &order.expr).collect(),
            RelationNode::Limit { offset, fetch, .. } => offset.iter().chain(fetch.iter()).collect(),
            RelationNode::Unnest { expr, .. } | RelationNode::List { expr, .. } | RelationNode::ColumnTable { expr, .. } => vec![expr],
            RelationNode::PatchGaps { from, to, .. } => vec![from, to],
            RelationNode::Distinct { .. } | RelationNode::Set { .. } | RelationNode::Let { .. } | RelationNode::Reference { .. } | RelationNode::Ordinality { .. } => vec![],
        }
    }

    pub fn parameter_count(&self) -> usize {
        self.parameter_indices().last().map_or(0, |index| index.saturating_add(1))
    }

    pub fn parameter_indices(&self) -> BTreeSet<usize> {
        let mut indices = BTreeSet::new();
        if let RelationNode::Aggregate { aggregates, .. } = &self.node {
            for aggregate in aggregates {
                if let AggregateFunction::Percentile { fraction: PercentileFraction::Parameter { index }, .. } = &aggregate.function { indices.insert(*index); }
            }
        }
        for expr in self.expressions() {
            expr.visit(&mut |expr| { if let Expr::Parameter { index, .. } = expr { indices.insert(*index); } });
        }
        if let RelationNode::Apply { input, dependent, arguments, .. } | RelationNode::Mark { input, dependent, arguments, .. } = &self.node {
            indices.extend(input.parameter_indices());
            let bound: BTreeSet<_> = arguments.iter().map(|(index, _)| *index).collect();
            indices.extend(dependent.parameter_indices().difference(&bound));
        } else {
            for child in self.children() { indices.extend(child.parameter_indices()); }
        }
        indices
    }

    pub fn dependencies(&self) -> Vec<TableBinding> {
        let mut result = if let RelationNode::Scan(scan) = &self.node { vec![scan.table.clone()] } else { vec![] };
        for child in self.children() {
            for dependency in child.dependencies() { if !result.contains(&dependency) { result.push(dependency); } }
        }
        result
    }

    pub fn validate(&self) -> Result<()> {
        if let RelationNode::Limit { offset,fetch,.. } | RelationNode::SortLimit { offset,fetch,.. } = &self.node {
            for expression in offset.iter().chain(fetch) {
                let valid = match expression {
                    Expr::Parameter { .. } => true,
                    Expr::Literal(Value::I8(value)) => *value >= 0,Expr::Literal(Value::I16(value)) => *value >= 0,
                    Expr::Literal(Value::I32(value)) => *value >= 0,Expr::Literal(Value::I64(value)) => *value >= 0,
                    _ => false,
                };
                if !valid { return Err(Anomaly::incorrect("xtdb/invalid-row-count","LIMIT and OFFSET require a non-negative integer literal or parameter")); }
            }
        }
        let inputs: BTreeSet<ColumnId> = if matches!(self.node, RelationNode::Scan(_)) { self.schema.ids() } else { self.children().iter().flat_map(|child| child.schema.ids()).collect() };
        for expr in self.expressions() { expr.validate_columns(&inputs)?; }
        let invalid = |message| Err(Anomaly::fault("rtdb.plan/invalid-relation", message));
        match &self.node {
            RelationNode::Values { rows } => {
                if rows.iter().any(|row| row.len() != self.schema.len()) { return invalid("VALUES row width differs from schema"); }
            }
            RelationNode::LiteralPages { pages } => {
                if pages.iter().flatten().any(|row| row.len() != self.schema.len()) { return invalid("Literal page row width differs from schema"); }
            }
            RelationNode::ArrowLiteralPages { pages } => {
                if pages.pages().iter().any(|page| page.batch().num_columns() != self.schema.len()) {
                    return invalid("Literal page row width differs from schema");
                }
            }
            RelationNode::Scan(scan) => {
                if self.schema.fields() != scan.columns { return invalid("Scan columns differ from output schema"); }
                for expression in scan.valid_time.expressions().into_iter().chain(scan.system_time.expressions()) { expression.validate_columns(&BTreeSet::new())?; }
            }
            RelationNode::Project { expressions, .. } | RelationNode::Rename { expressions, .. } => {
                if !self.schema.fields().iter().eq(expressions.iter().map(|expr| &expr.field)) { return invalid("Projection fields differ from output schema"); }
            }
            RelationNode::Extend { input, expressions } => {
                let expected = input.schema().fields().iter().chain(expressions.iter().map(|expr| &expr.field));
                if !self.schema.fields().iter().eq(expected) { return invalid("Extension schema differs from its input and appended fields"); }
            }
            RelationNode::Let { binding, body } => {
                if binding.name.is_empty() { return invalid("Materialized binding name is empty"); }
                if self.schema != body.schema { return invalid("Let output differs from its body schema"); }
            }
            RelationNode::Reference { binding } => {
                if self.schema != binding.relation.schema { return invalid("Reference differs from its bound relation schema"); }
            }
            RelationNode::Filter { input, .. } | RelationNode::Sort { input, .. } | RelationNode::SortLimit { input, .. }
            | RelationNode::Limit { input, .. } | RelationNode::Distinct { input } => {
                if self.schema != input.schema { return invalid("Pass-through operator changes its schema"); }
            }
            RelationNode::Join { left, right, kind, condition } => {
                if *kind == JoinKind::Cross && condition.is_some() { return invalid("Cross join cannot declare a condition"); }
                let expected = kind.result_schema(left.schema(), right.schema())?;
                if self.schema != expected { return invalid("Join schema does not describe its output"); }
            }
            RelationNode::Mark { input, arguments, condition, comparison, output, .. } => {
                let mut parameters = BTreeSet::new();
                for (index, expr) in arguments {
                    if !parameters.insert(index) { return invalid("Mark parameter is bound more than once"); }
                    expr.validate_columns(&input.schema.ids())?;
                }
                for predicate in condition.iter().chain([comparison]) {
                    if predicate.data_type().legs().iter().any(|ty| !matches!(ty, LogicalType::Null | LogicalType::Nothing | LogicalType::Scalar(ScalarType::Bool))) { return invalid("Mark predicates must be Boolean or null"); }
                }
                if output.data_type != LogicalType::Scalar(ScalarType::Bool).maybe() { return invalid("Mark output must be nullable Boolean"); }
                if self.schema != input.schema.concat(&Schema::new(vec![output.clone()])?)? { return invalid("Mark schema does not describe its output"); }
            }
            RelationNode::Apply { input, dependent, arguments, kind, output } => {
                let mut parameters = BTreeSet::new();
                for (index, expr) in arguments {
                    if !parameters.insert(index) { return invalid("Apply parameter is bound more than once"); }
                    expr.validate_columns(&input.schema.ids())?;
                }
                let expected = kind.result_schema(input.schema(), dependent.schema(), output.as_ref())?;
                if output.as_ref().is_some_and(|field| expected.get(field.id) != Some(field)) { return invalid("Scalar apply output type differs from its result"); }
                if self.schema != expected { return invalid("Apply schema does not describe its output"); }
            }
            RelationNode::Aggregate { groups, aggregates, .. } => {
                if !self.schema.fields().iter().eq(groups.iter().map(|group| &group.field).chain(aggregates.iter().map(|agg| &agg.field))) {
                    return invalid("Aggregate schema does not describe its output");
                }
                for aggregate in aggregates {
                    if aggregate.function.quantifier().is_some_and(|distinct| distinct != aggregate.distinct) { return invalid("Aggregate quantifier differs from its factory spelling"); }
                    if (aggregate.function.canonical() == AggregateFunction::CountStar) != aggregate.argument.is_none() {
                        return invalid("Only COUNT(*) omits its input expression");
                    }
                    if aggregate.argument.as_ref().is_some_and(|argument| !matches!(argument, Expr::Column { .. })) || aggregate.order_by.iter().any(|order| !matches!(order.expr, Expr::Column { .. })) {
                        return invalid("Aggregate inputs and ordering expressions must be projected to columns before filtering");
                    }
                    if aggregate.distinct && !matches!(aggregate.function.canonical(), AggregateFunction::Count | AggregateFunction::Sum | AggregateFunction::Average | AggregateFunction::Min | AggregateFunction::Max | AggregateFunction::ArrayAgg) {
                        return invalid("The aggregate factory does not support DISTINCT");
                    }
                    if matches!(aggregate.function, AggregateFunction::Percentile { .. }) && (aggregate.distinct || !aggregate.order_by.is_empty()) {
                        return invalid("Percentiles use their single input and direction, without DISTINCT or additional ordering");
                    }
                }
            }
            RelationNode::Window { input, partition_by, expressions, .. } => {
                if partition_by.iter().any(|id| input.schema.get(*id).is_none()) { return invalid("Window partition column is absent from input"); }
                if expressions.is_empty() { return invalid("Window requires at least one result"); }
                if !self.schema.fields().iter().eq(input.schema.fields().iter().chain(expressions.iter().map(|expression| &expression.field))) { return invalid("Window schema differs from its input and results"); }
                for expression in expressions {
                    if expression.field.data_type != expression.function.result_type(&input.schema)? { return invalid("Window result type differs from its function"); }
                }
            }
            RelationNode::Set { left, right, .. } => {
                if self.schema.len() != left.schema.len() || self.schema.len() != right.schema.len() { return invalid("Set inputs have different widths"); }
                for ((output, left), right) in self.schema.fields().iter().zip(left.schema.fields()).zip(right.schema.fields()) {
                    if output.data_type != left.data_type.join(&right.data_type) { return invalid("Set output type is not the join of its inputs"); }
                }
            }
            RelationNode::List { element, .. } | RelationNode::ColumnTable { element, .. } => {
                if self.schema.fields() != [element.clone()] { return invalid("List schema differs from its element"); }
            }
            RelationNode::Ordinality { input, field, .. } => {
                if field.data_type != LogicalType::Scalar(ScalarType::I64) { return invalid("Ordinality requires an I64 result"); }
                if !self.schema.fields().iter().eq(input.schema.fields().iter().chain([field])) { return invalid("Ordinality schema differs from its input and result"); }
            }
            RelationNode::Unnest { input, element, ordinality, .. } => {
                if !self.schema.fields().iter().eq(input.schema.fields().iter().chain([element]).chain(ordinality.iter())) {
                    return invalid("Unnest fields differ from output schema");
                }
            }
            RelationNode::PatchGaps { input, iid, valid_from, valid_to, document, from, to } => {
                crate::PatchEndpoint::from_expr(from)?;
                crate::PatchEndpoint::from_expr(to)?;
                if [*iid, *valid_from, *valid_to, *document].into_iter().collect::<BTreeSet<_>>().len() != 4 { return invalid("Patch gap columns must be distinct"); }
                for id in [iid, valid_from, valid_to, document] {
                    if input.schema.get(*id).is_none() { return invalid("Patch gap column is absent from input"); }
                }
                from.validate_columns(&BTreeSet::new())?;
                to.validate_columns(&BTreeSet::new())?;
                let fields = input.schema.fields().iter().cloned().map(|mut field| {
                    if field.id == *document { field.data_type = field.data_type.maybe(); }
                    field
                }).collect();
                if self.schema != Schema::new(fields)? { return invalid("Patch gap schema must make only its document nullable"); }
            }
        }
        Ok(())
    }

    pub fn explain_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(|error| Anomaly::fault("rtdb.plan/serialization", error.to_string()))
    }
}

impl TryFrom<RelationParts> for Relation {
    type Error = Anomaly;
    fn try_from(parts: RelationParts) -> Result<Self> { Self::new(parts.schema, parts.node) }
}
impl From<Relation> for RelationParts { fn from(relation: Relation) -> Self { Self { schema: relation.schema, node: relation.node } } }

/// Removes literal identity filters without moving a value predicate below temporal resolution.
pub fn simplify(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    if let RelationNode::Filter { input, predicate: Expr::Literal(value) } = relation.node() {
        match value {
            Value::Bool(true) => return simplify(input.clone()),
            Value::Bool(false) | Value::Null => return Ok(Arc::new(Relation::empty(relation.schema.clone()))),
            _ => (),
        }
    }
    Ok(relation)
}
