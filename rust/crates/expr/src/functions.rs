use rtdb_types::StructFields;
use crate::bitwise::BitwiseFunction;
use crate::collection::CollectionFunction;
use crate::compare::{null_eq_scoped, nullish, sql_eq_scoped};
use crate::interval_constructor::IntervalConstructorFunction;
use crate::numeric::{decimal, decimal_value, floating_value, inapplicable, integer, integer_value, number, overflow};
use crate::period::PeriodPredicate;
use crate::search::SearchFunction;
use crate::session_info::SessionInfoFunction;
use crate::spatial::SpatialFunction;
use crate::string::StringFunction;
use crate::temporal_functions::TemporalFunction;
use crate::{EvalContext, cast, compare_in_timezone};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TimeUnit, Value};
use std::borrow::Cow;
use std::cmp::Ordering;

pub use rtdb_plan::function_type;

fn text(value: &Value) -> Result<&str> { match value { Value::Utf8(text) => Ok(text), _ => Err(inapplicable("text", &[value])) } }
fn count(value: usize) -> Result<Value> { Ok(Value::I32(i32::try_from(value).map_err(|_| overflow())?)) }

/// A known scalar function resolved once from its call spelling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScalarFunction {
    CosineDistance,
    ApproximateNearest,
    CosineDistanceF32,
    EntitySortKey,
    RankAdmittedCount,
    Period { predicate: PeriodPredicate, question: bool },
    Search(SearchFunction),
    Spatial(SpatialFunction),
    String(StringFunction),
    SessionInfo(SessionInfoFunction),
    Collection(CollectionFunction),
    Bitwise(BitwiseFunction),
    Temporal(TemporalFunction),
    Interval(IntervalConstructorFunction),
    GetField,
    GenerateSeries,
    Compare,
    CompareNullsFirst,
    CompareNullsLast,
    UriScheme,
    UriUserInfo,
    UriHost,
    UriPort,
    UriPath,
    UriQuery,
    UriFragment,
    ColDescription,
    PgIndexamHasProperty,
    Random,
    Sleep,
    FormatType,
    Patch,
    Coalesce,
    Nullif,
    NilQ,
    IsNull,
    TrueQ,
    IsTrue,
    FalseQ,
    IsFalse,
    DistinctFrom,
    MetaDouble,
    Boolean,
    NullEq,
    StrictEq,
    PeriodConstructor,
    Set,
    SnapshotToken,
    Abs,
    Lower,
    Upper,
    LowerInf,
    UpperInf,
    Length,
    EpochNanosBe,
    Iid,
    Keyword,
    Namespace,
    LocalName,
    Cardinality,
    Nth,
    Greatest,
    Least,
    LikeRegex,
    RegexpReplace,
    Floor,
    Ceil,
    Ceiling,
    Round,
    Mod,
    FloatMath(FloatMath),
    IdEqAny,
}

impl ScalarFunction {
    pub(crate) fn resolve(name: &str) -> Option<Self> {
        let canonical = canonical_name(name);
        Self::from_canonical(&canonical)
    }

    pub(crate) fn from_canonical(name: &str) -> Option<Self> {
        Some(match name {
            "cosine_distance" => Self::CosineDistance,
            "approximate_nearest" => Self::ApproximateNearest,
            "cosine_distance_f32" => Self::CosineDistanceF32,
            "entity_sort_key" => Self::EntitySortKey,
            "rank_admitted_count" => Self::RankAdmittedCount,
            "get_field" => Self::GetField,
            "generate_series" => Self::GenerateSeries,
            "compare" => Self::Compare,
            "compare_nulls_first" => Self::CompareNullsFirst,
            "compare_nulls_last" => Self::CompareNullsLast,
            "uri_scheme" => Self::UriScheme,
            "uri_user_info" => Self::UriUserInfo,
            "uri_host" => Self::UriHost,
            "uri_port" => Self::UriPort,
            "uri_path" => Self::UriPath,
            "uri_query" => Self::UriQuery,
            "uri_fragment" => Self::UriFragment,
            "col_description" => Self::ColDescription,
            "pg_indexam_has_property" => Self::PgIndexamHasProperty,
            "random" => Self::Random,
            "sleep" => Self::Sleep,
            "format_type" => Self::FormatType,
            "_patch" => Self::Patch,
            "coalesce" => Self::Coalesce,
            "nullif" => Self::Nullif,
            "nil?" => Self::NilQ,
            "is_null" => Self::IsNull,
            "true?" => Self::TrueQ,
            "is_true" => Self::IsTrue,
            "false?" => Self::FalseQ,
            "is_false" => Self::IsFalse,
            "distinct_from" => Self::DistinctFrom,
            "_meta_double" => Self::MetaDouble,
            "boolean" => Self::Boolean,
            "null_eq" => Self::NullEq,
            "===" => Self::StrictEq,
            "period" => Self::PeriodConstructor,
            "set" => Self::Set,
            "snapshot_token" => Self::SnapshotToken,
            "abs" => Self::Abs,
            "lower" => Self::Lower,
            "upper" => Self::Upper,
            "lower_inf" => Self::LowerInf,
            "upper_inf" => Self::UpperInf,
            "length" => Self::Length,
            "epoch_nanos_be" => Self::EpochNanosBe,
            "_iid" => Self::Iid,
            "keyword" => Self::Keyword,
            "namespace" => Self::Namespace,
            "local_name" => Self::LocalName,
            "cardinality" => Self::Cardinality,
            "nth" => Self::Nth,
            "greatest" => Self::Greatest,
            "least" => Self::Least,
            "like_regex" => Self::LikeRegex,
            "regexp_replace" => Self::RegexpReplace,
            "floor" => Self::Floor,
            "ceil" => Self::Ceil,
            "ceiling" => Self::Ceiling,
            "round" => Self::Round,
            "mod" => Self::Mod,
            "_id_eq_any" => Self::IdEqAny,
            name => {
                if let Some(math) = FloatMath::resolve(name) { return Some(Self::FloatMath(math)); }
                if let Some((predicate, question)) = PeriodPredicate::from_canonical(name) { return Some(Self::Period { predicate, question }); }
                if let Some(func) = SearchFunction::from_canonical(name) { return Some(Self::Search(func)); }
                if let Some(func) = SpatialFunction::from_canonical(name) { return Some(Self::Spatial(func)); }
                if let Some(func) = StringFunction::from_canonical(name) { return Some(Self::String(func)); }
                if let Some(func) = SessionInfoFunction::from_canonical(name) { return Some(Self::SessionInfo(func)); }
                if let Some(func) = CollectionFunction::from_canonical(name) { return Some(Self::Collection(func)); }
                if let Some(func) = BitwiseFunction::from_canonical(name) { return Some(Self::Bitwise(func)); }
                if let Some(func) = TemporalFunction::from_canonical(name) { return Some(Self::Temporal(func)); }
                if let Some(func) = IntervalConstructorFunction::from_canonical(name) { return Some(Self::Interval(func)); }
                return None;
            }
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::CosineDistance => "cosine_distance",
            Self::ApproximateNearest => "approximate_nearest",
            Self::CosineDistanceF32 => "cosine_distance_f32",
            Self::EntitySortKey => "entity_sort_key",
            Self::RankAdmittedCount => "rank_admitted_count",
            Self::Period { predicate, question } => predicate.canonical_name(question),
            Self::Search(func) => func.canonical_name(),
            Self::Spatial(func) => func.canonical_name(),
            Self::String(func) => func.canonical_name(),
            Self::SessionInfo(func) => func.canonical_name(),
            Self::Collection(func) => func.canonical_name(),
            Self::Bitwise(func) => func.canonical_name(),
            Self::Temporal(func) => func.canonical_name(),
            Self::Interval(func) => func.canonical_name(),
            Self::GetField => "get_field",
            Self::GenerateSeries => "generate_series",
            Self::Compare => "compare",
            Self::CompareNullsFirst => "compare_nulls_first",
            Self::CompareNullsLast => "compare_nulls_last",
            Self::UriScheme => "uri_scheme",
            Self::UriUserInfo => "uri_user_info",
            Self::UriHost => "uri_host",
            Self::UriPort => "uri_port",
            Self::UriPath => "uri_path",
            Self::UriQuery => "uri_query",
            Self::UriFragment => "uri_fragment",
            Self::ColDescription => "col_description",
            Self::PgIndexamHasProperty => "pg_indexam_has_property",
            Self::Random => "random",
            Self::Sleep => "sleep",
            Self::FormatType => "format_type",
            Self::Patch => "_patch",
            Self::Coalesce => "coalesce",
            Self::Nullif => "nullif",
            Self::NilQ => "nil?",
            Self::IsNull => "is_null",
            Self::TrueQ => "true?",
            Self::IsTrue => "is_true",
            Self::FalseQ => "false?",
            Self::IsFalse => "is_false",
            Self::DistinctFrom => "distinct_from",
            Self::MetaDouble => "_meta_double",
            Self::Boolean => "boolean",
            Self::NullEq => "null_eq",
            Self::StrictEq => "===",
            Self::PeriodConstructor => "period",
            Self::Set => "set",
            Self::SnapshotToken => "snapshot_token",
            Self::Abs => "abs",
            Self::Lower => "lower",
            Self::Upper => "upper",
            Self::LowerInf => "lower_inf",
            Self::UpperInf => "upper_inf",
            Self::Length => "length",
            Self::EpochNanosBe => "epoch_nanos_be",
            Self::Iid => "_iid",
            Self::Keyword => "keyword",
            Self::Namespace => "namespace",
            Self::LocalName => "local_name",
            Self::Cardinality => "cardinality",
            Self::Nth => "nth",
            Self::Greatest => "greatest",
            Self::Least => "least",
            Self::LikeRegex => "like_regex",
            Self::RegexpReplace => "regexp_replace",
            Self::Floor => "floor",
            Self::Ceil => "ceil",
            Self::Ceiling => "ceiling",
            Self::Round => "round",
            Self::Mod => "mod",
            Self::FloatMath(math) => math.canonical_name(),
            Self::IdEqAny => "_id_eq_any",
        }
    }

    pub(crate) fn is_ordinary_call(self) -> bool {
        matches!(
            self,
            Self::Abs | Self::Floor | Self::Ceil | Self::Ceiling | Self::Round | Self::FloatMath(_) | Self::Sleep
            | Self::Lower | Self::Upper | Self::Length | Self::Cardinality
            | Self::String(
                StringFunction::Reverse | StringFunction::Trim | StringFunction::TrimLeading
                | StringFunction::TrimTrailing | StringFunction::Substring | StringFunction::Overlay
                | StringFunction::Replace | StringFunction::Position | StringFunction::OctetPosition
                | StringFunction::QuoteIdent | StringFunction::ParseIdent | StringFunction::CharacterLength
                | StringFunction::OctetLength | StringFunction::DefaultOverlayLength
                | StringFunction::Str | StringFunction::Concat | StringFunction::Format
                | StringFunction::StringToArray
            )
            | Self::Iid | Self::Keyword | Self::Namespace | Self::LocalName
            | Self::SessionInfo(SessionInfoFunction::CurrentSchemas | SessionInfoFunction::CurrentSetting)
            | Self::UriScheme | Self::UriUserInfo | Self::UriHost | Self::UriPort | Self::UriPath | Self::UriQuery | Self::UriFragment
            | Self::LikeRegex | Self::Patch | Self::Boolean | Self::TrueQ | Self::FalseQ | Self::NilQ | Self::IsNull
            | Self::NullEq | Self::StrictEq | Self::FormatType | Self::PeriodConstructor
            | Self::LowerInf | Self::UpperInf | Self::Coalesce | Self::Nullif | Self::Greatest | Self::Least
            | Self::Period { .. }
        )
    }

    pub(crate) fn is_prepared_call(self) -> bool {
        self.is_ordinary_call()
            || matches!(
                self,
                Self::Interval(_) | Self::Bitwise(_)
                | Self::Nth | Self::ColDescription | Self::PgIndexamHasProperty | Self::Mod
                | Self::Collection(CollectionFunction::ArrayUpper | CollectionFunction::ArrayLower | CollectionFunction::ArrayLength)
                | Self::GenerateSeries | Self::Temporal(TemporalFunction::RangeBins | TemporalFunction::DateBin)
                | Self::IsTrue | Self::IsFalse | Self::DistinctFrom | Self::MetaDouble | Self::GetField
                | Self::Compare | Self::CompareNullsFirst | Self::CompareNullsLast
                | Self::String(StringFunction::Like | StringFunction::Ilike)
            )
    }

    pub(crate) fn is_strict_temporal(self) -> bool {
        matches!(self, Self::Temporal(func) if func.is_strict())
    }

    pub(crate) fn is_collection_call(self) -> bool {
        matches!(self, Self::IdEqAny | Self::Set | Self::Collection(CollectionFunction::TrimArray))
    }

    pub(crate) fn is_self_typed(self) -> bool {
        matches!(self, Self::Set | Self::Collection(CollectionFunction::TrimArray) | Self::RegexpReplace)
    }

    pub(crate) fn is_equality_call(self) -> bool {
        matches!(self, Self::NullEq | Self::DistinctFrom | Self::Nullif)
    }

    pub(crate) fn shortcut_null(self) -> bool {
        !matches!(
            self,
            Self::IsTrue | Self::IsFalse | Self::IsNull | Self::TrueQ | Self::FalseQ | Self::NilQ | Self::Boolean
            | Self::StrictEq | Self::NullEq | Self::CompareNullsFirst | Self::CompareNullsLast | Self::DistinctFrom
            | Self::PeriodConstructor | Self::String(StringFunction::Str | StringFunction::StringToArray | StringFunction::Format)
            | Self::Patch | Self::FormatType
        )
    }

    pub(crate) fn literal_argument(self, index: usize) -> bool {
        match self {
            Self::Round => index == 1,
            Self::Temporal(TemporalFunction::Extract | TemporalFunction::DateTrunc) => index == 0 || index == 2,
            Self::Interval(_) => index != 0,
            Self::Temporal(
                TemporalFunction::CurrentTimestamp | TemporalFunction::CurrentTime
                | TemporalFunction::LocalTimestamp | TemporalFunction::LocalTime
            ) => true,
            _ => false,
        }
    }
}

/// A call's resolved identity: a known `ScalarFunction`, or an unregistered spelling preserved for null/error admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedFunction<'a> {
    Known(ScalarFunction),
    Unknown(&'a str),
}

impl<'a> ResolvedFunction<'a> {
    pub(crate) fn from_option(function: Option<ScalarFunction>, name: &'a str) -> Self {
        match function {
            Some(function) => Self::Known(function),
            None => Self::Unknown(name),
        }
    }

    pub(crate) fn resolve(name: &'a str) -> Self {
        Self::from_option(ScalarFunction::resolve(name), name)
    }

    pub(crate) fn canonical_name(self) -> Cow<'a, str> {
        match self {
            Self::Known(function) => Cow::Borrowed(function.canonical_name()),
            Self::Unknown(name) => canonical_name(name),
        }
    }

    pub(crate) fn is_ordinary_call(self) -> bool { matches!(self, Self::Known(function) if function.is_ordinary_call()) }
    pub(crate) fn is_prepared_call(self) -> bool { matches!(self, Self::Known(function) if function.is_prepared_call()) }
    pub(crate) fn is_strict_temporal(self) -> bool { matches!(self, Self::Known(function) if function.is_strict_temporal()) }
    pub(crate) fn is_collection_call(self) -> bool { matches!(self, Self::Known(function) if function.is_collection_call()) }
    pub(crate) fn is_self_typed(self) -> bool { matches!(self, Self::Known(function) if function.is_self_typed()) }
    pub(crate) fn is_cosine(self) -> bool { self == Self::Known(ScalarFunction::CosineDistance) }
    pub(crate) fn is_equality_call(self) -> bool { matches!(self, Self::Known(function) if function.is_equality_call()) }
    pub(crate) fn shortcut_null(self) -> bool { match self { Self::Known(function) => function.shortcut_null(), Self::Unknown(_) => true } }
    pub(crate) fn literal_argument(self, index: usize) -> bool { matches!(self, Self::Known(function) if function.literal_argument(index)) }
}

/// Runs a scalar call on the caller's future; SLEEP borrows the Tokio time driver without spawning a task.
/// Dropping this future cancels its timer; callers retain the evaluation allocation scope and own cancellation.
/// Temporal GENERATE_SERIES and RANGE_BINS yield while constructing results; cancellation drops that construction.
/// Remaining scalar calls use the synchronous implementation and return the same errors.
pub async fn scalar_call_async(name: &str, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    call_resolved_async(ResolvedFunction::resolve(name), args, context).await
}

pub(crate) async fn call_resolved_async(function: ResolvedFunction<'_>, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    match function {
        ResolvedFunction::Known(ScalarFunction::GenerateSeries) => {
            crate::series::prepare(args, context)?.materialize_async(context).await
        }
        ResolvedFunction::Known(ScalarFunction::Temporal(TemporalFunction::RangeBins)) => {
            match crate::temporal_functions::RangeBins::prepare(args, context)? {
                Some(bins) => bins.materialize_async(context).await,
                None => Ok(Value::Null),
            }
        }
        ResolvedFunction::Known(ScalarFunction::Sleep) => {
            let bytes = args.iter().try_fold(4096usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
            context.reserve(bytes.checked_mul(4).ok_or_else(overflow)?)?;
            function_type("sleep", &args.iter().map(Value::logical_type).collect::<Vec<_>>())?;
            let mut remaining = crate::volatile::sleep_millis(&args[0])?;
            while remaining != 0 {
                let millis = remaining.min(86_400_000);
                tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
                remaining -= millis;
            }
            Ok(Value::Null)
        }
        other => call_resolved(other, args, context),
    }
}

pub fn scalar_call(name: &str, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    call_resolved(ResolvedFunction::resolve(name), args, context)
}

pub(crate) fn call_resolved(function: ResolvedFunction<'_>, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    let bytes = args.iter().try_fold(256usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
    context.reserve(bytes.checked_mul(16).ok_or_else(overflow)?)?;
    match function {
        ResolvedFunction::Known(ScalarFunction::CosineDistance) => {
            let [left, right] = args else {
                return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "cosine_distance requires two arguments"));
            };
            return crate::vector::cosine_distance(left, right);
        }
        ResolvedFunction::Known(ScalarFunction::ApproximateNearest) => {
            let [column, _, _] = args else {
                return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "approximate_nearest requires a column, a probe vector and a limit"));
            };
            return Ok(Value::Bool(!nullish(column)));
        }
        _ => {}
    }
    let canonical = function.canonical_name();
    let return_type = function_type(&canonical, &args.iter().map(Value::logical_type).collect::<Vec<_>>())?;
    let ResolvedFunction::Known(function) = function else {
        if args.iter().any(nullish) { return Ok(Value::Null); }
        return Err(Anomaly::unsupported("xtdb/unknown-function", format!("Unknown scalar function: {canonical}")));
    };
    match function {
        ScalarFunction::CosineDistance | ScalarFunction::ApproximateNearest => unreachable!(),
        ScalarFunction::CosineDistanceF32 => return crate::vector::cosine_distance_f32(&args[0], &args[1]),
        ScalarFunction::EntitySortKey => return if nullish(&args[0]) { Ok(Value::Null) } else {
            Ok(Value::Binary(args[0].entity_id()?.handle_bytes().into()))
        },
        ScalarFunction::RankAdmittedCount => {
            let nonnegative = |value| integer(value).and_then(|count| i64::try_from(count).ok()).filter(|count| *count >= 0);
            let (Some(count), Some(limit)) = (nonnegative(&args[0]), nonnegative(&args[1])) else {
                return Err(Anomaly::incorrect("rtdb/rank-bound", "Rank admission requires nonnegative integer counts"));
            };
            if count > limit {
                return Err(Anomaly::unsupported("rtdb/rank-bound", "The selected candidate set exceeds the ranking bound").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded));
            }
            return Ok(Value::I64(count));
        }
        ScalarFunction::Period { predicate, question } => return predicate.eval(question, args, context),
        ScalarFunction::Search(func) => return func.eval(args, context),
        ScalarFunction::Spatial(func) => return func.eval(args, context),
        ScalarFunction::String(func) => return func.eval(args, context),
        ScalarFunction::SessionInfo(func) => return func.eval(args, context),
        ScalarFunction::Collection(func) => return func.eval(args, context),
        ScalarFunction::Bitwise(func) => return func.eval_typed(args, return_type),
        ScalarFunction::Temporal(func) => return func.eval(args, context),
        ScalarFunction::Interval(func) => return func.eval(args),
        ScalarFunction::GetField => return crate::field::get(&args[0], None, context),
        ScalarFunction::GenerateSeries => return crate::series::prepare(args, context)?.materialize(context),
        ScalarFunction::Compare | ScalarFunction::CompareNullsFirst | ScalarFunction::CompareNullsLast => {
            if function == ScalarFunction::Compare && args.iter().any(nullish) { return Ok(Value::Null); }
            return crate::ordered::scalar_compare(&args[0], &args[1], function == ScalarFunction::CompareNullsFirst, context.timezone).map(Value::I32);
        }
        ScalarFunction::UriScheme | ScalarFunction::UriUserInfo | ScalarFunction::UriHost | ScalarFunction::UriPort
        | ScalarFunction::UriPath | ScalarFunction::UriQuery | ScalarFunction::UriFragment => {
            if nullish(&args[0]) { return Ok(Value::Null); }
            let Value::Uri(text) = &args[0] else { return Err(inapplicable(function.canonical_name(), &[&args[0]])); };
            let uri = rtdb_types::UriParts::parse(text)?;
            if function == ScalarFunction::UriPort { return Ok(uri.port().map(Value::I32).unwrap_or(Value::Null)); }
            let component = match function {
                ScalarFunction::UriScheme => uri.scheme().map(Cow::Borrowed),
                ScalarFunction::UriHost => uri.host().map(Cow::Borrowed),
                ScalarFunction::UriUserInfo => uri.user_info(),
                ScalarFunction::UriPath => uri.path(),
                ScalarFunction::UriQuery => uri.query(),
                _ => uri.fragment(),
            };
            return Ok(component.map(|text| Value::Utf8(text.into_owned())).unwrap_or(Value::Null));
        }
        ScalarFunction::ColDescription | ScalarFunction::PgIndexamHasProperty => return Ok(Value::Null),
        ScalarFunction::Random => return crate::volatile::random().map(Value::F64),
        ScalarFunction::Sleep => return if crate::volatile::sleep_millis(&args[0])? == 0 { Ok(Value::Null) }
            else { Err(Anomaly::unsupported("rtdb.expr/async-required", "SLEEP requires asynchronous expression evaluation")) },
        ScalarFunction::FormatType => {
            if nullish(&args[0]) { return Ok(Value::Null); }
            let oid = match &args[0] { Value::Oid(oid) => *oid, value => i32::try_from(integer(value).ok_or_else(|| inapplicable("format_type", &[value]))?).map_err(|_| overflow())? };
            let modifier = if nullish(&args[1]) { -1 } else { i32::try_from(integer(&args[1]).ok_or_else(|| inapplicable("format_type", &[&args[1]]))?).map_err(|_| overflow())? };
            return Ok(Value::Utf8(rtdb_types::PgType::format_oid(oid, modifier)));
        }
        ScalarFunction::Patch => {
            let Value::Struct(right) = &args[1] else { return Err(inapplicable("_patch", &[&args[0], &args[1]])); };
            let mut fields = match &args[0] {
                Value::Struct(left) => left.clone(),
                Value::Null | Value::Undefined => StructFields::new(),
                _ => return Err(inapplicable("_patch", &[&args[0], &args[1]])),
            };
            for (name, value) in right {
                if !nullish(value) || !fields.contains_key(name) { fields.insert(name.clone(), value.clone()); }
            }
            return Ok(Value::Struct(fields));
        }
        ScalarFunction::Coalesce => return Ok(args.iter().find(|value| !nullish(value)).cloned().unwrap_or(Value::Null)),
        ScalarFunction::Nullif => return Ok(if sql_eq_scoped(&args[0], &args[1], context.timezone, context.allocation)? == Some(true) { Value::Null } else { args[0].clone() }),
        ScalarFunction::NilQ | ScalarFunction::IsNull => return Ok(Value::Bool(nullish(&args[0]))),
        ScalarFunction::TrueQ | ScalarFunction::IsTrue => return Ok(Value::Bool(args[0] == Value::Bool(true))),
        ScalarFunction::FalseQ | ScalarFunction::IsFalse => return Ok(Value::Bool(args[0] == Value::Bool(false))),
        ScalarFunction::DistinctFrom => return if args.iter().any(nullish) { Ok(Value::Bool(!args.iter().all(nullish))) }
            else { Ok(crate::compare::sql_not_eq_scoped(&args[0], &args[1], context.timezone, context.allocation)?.map(Value::Bool).unwrap_or(Value::Null)) },
        ScalarFunction::MetaDouble => return Ok(if let Some(value) = number(&args[0]) { Value::F64(value) }
            else if let Some(days) = crate::temporal::date_days(&args[0]) { Value::F64(days as f64 * 86_400.0) }
            else if let Value::Timestamp { value, unit, .. } | Value::Time { value, unit, .. } | Value::Duration { value, unit } = &args[0] { Value::F64(*value as f64 / unit.units_per_second() as f64) }
            else { Value::Null }),
        ScalarFunction::Boolean => return Ok(Value::Bool(!nullish(&args[0]) && args[0] != Value::Bool(false))),
        ScalarFunction::NullEq => return Ok(Value::Bool(null_eq_scoped(&args[0], &args[1], context.timezone, context.allocation)?)),
        ScalarFunction::StrictEq => return Ok(Value::Bool(crate::compare::strict_eq_scoped(&args[0], &args[1], context.allocation)?)),
        ScalarFunction::PeriodConstructor => {
            let timestamp = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
            let endpoint = |value: &Value, unbounded| -> Result<i64> {
                if nullish(value) { return Ok(unbounded); }
                let Value::Timestamp { value, .. } = cast(value, &timestamp, context.timezone)? else { return Err(inapplicable("period", &[value])); };
                Ok(value)
            };
            let lower = endpoint(&args[0], i64::MIN)?;
            let upper = endpoint(&args[1], i64::MAX)?;
            crate::period::validate_period_bounds(lower, upper)?;
            return Ok(Value::TimestampTzRange { lower, upper });
        }
        ScalarFunction::Set => {
            let mut items = Vec::new();
            for value in args {
                let mut found = false;
                for old in &items {
                    let equal = if nullish(old) || nullish(value) { nullish(old) && nullish(value) } else {
                        crate::compare::sql_eq_value(old, value, context.timezone, context.allocation)? == Some(true)
                    };
                    if equal { found = true; break; }
                }
                if !found { items.push(value.clone()); }
            }
            return Ok(Value::Set(items));
        }
        ScalarFunction::SnapshotToken => {
            if let Some(token) = context.snapshot_token {
                if let Some(allocation) = context.allocation { allocation.reserve(token.len())?; }
                return Ok(Value::Utf8(token.to_owned()));
            }
            return Ok(Value::Null);
        }
        _ => (),
    }
    if args.iter().any(nullish) { return Ok(Value::Null); }
    if function == ScalarFunction::Abs { if let Some(result) = crate::temporal::absolute(&args[0]) { return result; } }
    if let Some(Value::TimestampTzRange { lower, upper }) = args.first() {
        match function {
            ScalarFunction::Lower => return Ok(Value::Timestamp { value: *lower, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }),
            ScalarFunction::Upper => return Ok(if *upper == i64::MAX { Value::Null } else { Value::Timestamp { value: *upper, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }),
            ScalarFunction::LowerInf => return Ok(Value::Bool(*lower == i64::MIN)),
            ScalarFunction::UpperInf => return Ok(Value::Bool(*upper == i64::MAX)),
            _ => (),
        }
    }
    let name = function.canonical_name();
    match function {
        ScalarFunction::Lower => Ok(Value::Utf8(crate::string::case_language()?.to_lower(text(&args[0])?))),
        ScalarFunction::Upper => Ok(Value::Utf8(crate::string::case_language()?.to_upper(text(&args[0])?))),
        ScalarFunction::Length => match &args[0] { Value::Utf8(text) => count(text.chars().count()), Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => count(bytes.len()), Value::List(items) => count(items.len()), value => Err(inapplicable(name, &[value])) },
        ScalarFunction::EpochNanosBe => {
            let Value::Timestamp { value, unit, timezone: Some(_) } = &args[0] else {
                return Err(inapplicable("epoch_nanos_be", &[&args[0]]));
            };
            let nanos = i128::from(*value) * (1_000_000_000 / i128::from(unit.units_per_second()));
            let nanos = u64::try_from(nanos).map_err(|_| overflow())?;
            Ok(Value::Binary(nanos.to_be_bytes().to_vec()))
        }
        ScalarFunction::Iid => Ok(Value::FixedSizeBinary(args[0].entity_id()?.iid().0.to_vec())),
        ScalarFunction::Keyword => match &args[0] {
            Value::Utf8(text) => Ok(Value::Keyword(rtdb_types::Keyword::from_name(text))),
            value => Err(inapplicable(name, &[value])),
        },
        ScalarFunction::Namespace | ScalarFunction::LocalName => {
            let Value::Keyword(keyword) = &args[0] else { return Err(inapplicable(name, &[&args[0]])); };
            let parts = keyword.as_str().split_once('/');
            Ok(if function == ScalarFunction::Namespace { parts.map(|(namespace, _)| Value::Utf8(namespace.into())).unwrap_or(Value::Null) }
                else { Value::Utf8(parts.map_or(keyword.as_str(), |(_, local)| local).into()) })
        }
        ScalarFunction::Cardinality => match &args[0] { Value::List(_) | Value::Set(_) | Value::FixedSizeList(_) => count(args[0].list_view().unwrap().len()), _ => Err(inapplicable(name, &[&args[0]])) },
        ScalarFunction::Nth => {
            let index = integer(&args[1]).ok_or_else(|| inapplicable(name, &[&args[1]]))?;
            let items = match &args[0] { Value::List(_) | Value::FixedSizeList(_) => args[0].list_view().unwrap(), _ => return Ok(Value::Null) };
            let mut value = usize::try_from(index).ok().and_then(|index| items.get(index)).map(Cow::into_owned).unwrap_or(Value::Null);
            value.admit_box_date()?;
            Ok(value)
        }
        ScalarFunction::Greatest | ScalarFunction::Least => {
            let Some(first) = args.first() else { return Ok(Value::Null); };
            let mut best = first.clone();
            for value in &args[1..] {
                let unordered = number(&best).is_some_and(f64::is_nan) || number(value).is_some_and(f64::is_nan);
                let ordering = if unordered { Ordering::Equal } else { compare_in_timezone(&best, value, false, context.timezone)? };
                if ordering != if function == ScalarFunction::Greatest { Ordering::Greater } else { Ordering::Less } { best = value.clone(); }
            }
            Ok(best)
        }
        ScalarFunction::LikeRegex => crate::string::pattern::matches(args, context),
        ScalarFunction::RegexpReplace => crate::string::pattern::replace(args, context),
        ScalarFunction::Abs | ScalarFunction::Floor | ScalarFunction::Ceil | ScalarFunction::Ceiling | ScalarFunction::Round => {
            let scale = args.get(1).map(|value| integer(value).ok_or_else(|| inapplicable(name, &[value]))).transpose()?.unwrap_or(0);
            if matches!(function, ScalarFunction::Floor | ScalarFunction::Ceil | ScalarFunction::Ceiling) {
                let value = number(&args[0]).ok_or_else(|| inapplicable(name, &[&args[0]]))?;
                return Ok(Value::F64(if function == ScalarFunction::Floor { value.floor() } else { value.ceil() }));
            }
            if let Value::Decimal(value) = &args[0] {
                if function == ScalarFunction::Abs { return decimal_value(decimal(value).abs(), value.precision, value.scale, value.bit_width); }
                return crate::decimal::rounded(value, i32::try_from(scale).map_err(|_| overflow())?, bigdecimal::RoundingMode::HalfUp).map(Value::Decimal);
            }
            let LogicalType::Scalar(ty) = args[0].logical_type() else { return Err(inapplicable(name, &[&args[0]])); };
            if let Some(integer) = integer(&args[0]) {
                if function == ScalarFunction::Round && args.len() == 2 {
                    let precision = match ty { ScalarType::I32 => 10, ScalarType::I64 => 19, _ => return Err(inapplicable(name, &[&args[0]])) };
                    let value = crate::decimal::from_coefficient(integer.into(), precision, 0, 128)?;
                    return crate::decimal::rounded(&value, i32::try_from(scale).map_err(|_| overflow())?, bigdecimal::RoundingMode::HalfUp).map(Value::Decimal);
                }
                return integer_value(if function == ScalarFunction::Abs { integer.checked_abs().ok_or_else(overflow)? } else { integer }, &ty);
            }
            let x = number(&args[0]).ok_or_else(|| inapplicable(name, &[&args[0]]))?;
            if function == ScalarFunction::Round {
                if !x.is_finite() { return Ok(Value::F64(x)); }
                return round_float(x, i64::try_from(scale).map_err(|_| overflow())?).map(Value::F64);
            }
            Ok(floating_value(x.abs(), &ty))
        }
        ScalarFunction::Mod => crate::arithmetic(rtdb_plan::BinaryOp::Modulo, &args[0], &args[1]),
        ScalarFunction::FloatMath(math) => {
            let x = number(&args[0]).ok_or_else(|| inapplicable(name, &[&args[0]]))?;
            let y = args.get(1).map(|value| number(value).ok_or_else(|| inapplicable(name, &[value]))).transpose()?;
            math.apply(x, y).map(Value::F64)
        }
        _ => Err(Anomaly::unsupported("xtdb/unknown-function", format!("Unknown scalar function: {name}"))),
    }
}

/// The one spelling a function name is matched in: lower case, with underscores for hyphens. A name already so
/// spelled, as a call's name is once canonicalized, is borrowed rather than copied on every call of every row.
pub(crate) fn canonical_name(name: &str) -> Cow<'_, str> {
    if name.bytes().all(|byte| byte.is_ascii() && !byte.is_ascii_uppercase() && byte != b'-') { return Cow::Borrowed(name); }
    Cow::Owned(name.to_lowercase().replace('-', "_"))
}

/// The reference rounds a double as `floor(v + 0.5)` from zero up and `ceil(v - 0.5)` below zero, not to the nearest
/// integer: the addition itself rounds, so 0.49999999999999994 becomes 1 and 2^52 + 1 becomes 2^52 + 2.
fn reference_round(value: f64) -> f64 { if value >= 0.0 { (value + 0.5).floor() } else { (value - 0.5).ceil() } }

/// The functions computing a double from one or two numbers read as doubles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FloatMath { Sqrt, Exp, Ln, Log, Log10, Power, Sin, Cos, Tan, Sinh, Cosh, Tanh, Double, Asin, Acos, Atan }

impl FloatMath {
    /// The function a canonically spelled name calls.
    pub fn resolve(name: &str) -> Option<Self> {
        Some(match name {
            "sqrt" => Self::Sqrt, "exp" => Self::Exp, "ln" => Self::Ln, "log" => Self::Log, "log10" => Self::Log10,
            "power" => Self::Power, "sin" => Self::Sin, "cos" => Self::Cos, "tan" => Self::Tan, "sinh" => Self::Sinh,
            "cosh" => Self::Cosh, "tanh" => Self::Tanh, "double" => Self::Double, "asin" => Self::Asin, "acos" => Self::Acos,
            "atan" => Self::Atan,
            _ => return None,
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::Sqrt => "sqrt", Self::Exp => "exp", Self::Ln => "ln", Self::Log => "log", Self::Log10 => "log10",
            Self::Power => "power", Self::Sin => "sin", Self::Cos => "cos", Self::Tan => "tan", Self::Sinh => "sinh",
            Self::Cosh => "cosh", Self::Tanh => "tanh", Self::Double => "double", Self::Asin => "asin", Self::Acos => "acos",
            Self::Atan => "atan",
        }
    }

    /// The function of one double, for every function but LOG and POWER, which read a second; any second is ignored.
    pub fn unary(self) -> Option<fn(f64) -> f64> {
        let function: fn(f64) -> f64 = match self {
            Self::Sqrt => f64::sqrt, Self::Exp => f64::exp, Self::Ln => f64::ln, Self::Log10 => f64::log10,
            Self::Sinh => f64::sinh, Self::Cosh => f64::cosh, Self::Tanh => f64::tanh, Self::Double => |x| x,
            Self::Sin => f64::sin, Self::Cos => f64::cos, Self::Tan => f64::tan, Self::Asin => f64::asin,
            Self::Acos => f64::acos, Self::Atan => f64::atan,
            Self::Log | Self::Power => return None,
        };
        Some(function)
    }

    pub(crate) fn apply(self, x: f64, y: Option<f64>) -> Result<f64> {
        if let Some(function) = self.unary() { return Ok(function(x)); }
        Ok(match self {
            Self::Log => y.ok_or_else(|| inapplicable("log", &[]))?.log(x),
            _ => java_pow(x, y.ok_or_else(|| inapplicable("power", &[]))?),
        })
    }
}

/// `java.lang.Math.pow`: differs from IEEE 754 / C99 `pow` when the exponent is `NaN` or when `|base| == 1` and the
/// exponent is infinite, both of which yield `NaN` rather than `1.0`.
fn java_pow(base: f64, exponent: f64) -> f64 {
    if exponent.is_nan() || (base.abs() == 1.0 && exponent.is_infinite()) { return f64::NAN; }
    base.powf(exponent)
}

/// ROUND of a double to `scale` decimal places, half away from zero; a non-finite double rounds to itself.
pub(crate) fn round_float(x: f64, scale: i64) -> Result<f64> {
    if !x.is_finite() { return Ok(x); }
    let factor = 10.0_f64.powf((scale as f64).abs());
    let value = if scale >= 0 { x * factor } else { x / factor };
    if !value.is_finite() { return Err(overflow()); }
    Ok(if scale >= 0 { reference_round(value) / factor } else { reference_round(value) * factor })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// XTDB rounds a non-finite double to itself, with or without a scale: `floor(v + 0.5)` and `ceil(v - 0.5)` keep
    /// NaN and the infinities, and the scaled form returns them before scaling.
    #[test]
    fn round_of_a_non_finite_double_is_that_double() {
        let context = EvalContext::default();
        for x in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            for args in [vec![Value::F64(x)], vec![Value::F64(x), Value::I64(2)], vec![Value::F64(x), Value::I64(-400)]] {
                let Value::F64(rounded) = scalar_call("round", &args, &context).unwrap() else { panic!("ROUND of a double is a double") };
                assert_eq!(rounded.to_bits(), x.to_bits(), "ROUND{args:?}");
            }
        }
    }

    #[test]
    fn round_of_a_finite_double_whose_scaling_overflows_raises() {
        let context = EvalContext::default();
        assert!(scalar_call("round", &[Value::F64(1e300), Value::I64(400)], &context).is_err());
        assert!(scalar_call("round", &[Value::F64(0.0), Value::I64(400)], &context).is_err());
    }

    #[test]
    fn float_math_names_resolve_in_either_spelling_and_case() {
        let context = EvalContext::default();
        assert_eq!(scalar_call("SQRT", &[Value::F64(4.0)], &context).unwrap(), Value::F64(2.0));
        assert_eq!(scalar_call("power", &[Value::F64(2.0), Value::F64(3.0)], &context).unwrap(), Value::F64(8.0));
        assert!(FloatMath::resolve("power").unwrap().unary().is_none());
        assert!(FloatMath::resolve("floor").is_none());
    }

    #[test]
    fn every_scalar_function_canonical_name_round_trips_through_resolve() {
        let period_predicates = [
            PeriodPredicate::Contains, PeriodPredicate::StrictlyContains, PeriodPredicate::Overlaps,
            PeriodPredicate::StrictlyOverlaps, PeriodPredicate::Equals, PeriodPredicate::Precedes,
            PeriodPredicate::StrictlyPrecedes, PeriodPredicate::ImmediatelyPrecedes, PeriodPredicate::Succeeds,
            PeriodPredicate::StrictlySucceeds, PeriodPredicate::ImmediatelySucceeds, PeriodPredicate::Leads,
            PeriodPredicate::StrictlyLeads, PeriodPredicate::ImmediatelyLeads, PeriodPredicate::Lags,
            PeriodPredicate::StrictlyLags, PeriodPredicate::ImmediatelyLags,
        ];
        let mut all = vec![
            ScalarFunction::CosineDistance, ScalarFunction::ApproximateNearest, ScalarFunction::CosineDistanceF32,
            ScalarFunction::EntitySortKey, ScalarFunction::RankAdmittedCount, ScalarFunction::GetField,
            ScalarFunction::GenerateSeries, ScalarFunction::Compare, ScalarFunction::CompareNullsFirst,
            ScalarFunction::CompareNullsLast, ScalarFunction::UriScheme, ScalarFunction::UriUserInfo,
            ScalarFunction::UriHost, ScalarFunction::UriPort, ScalarFunction::UriPath, ScalarFunction::UriQuery,
            ScalarFunction::UriFragment, ScalarFunction::ColDescription, ScalarFunction::PgIndexamHasProperty,
            ScalarFunction::Random, ScalarFunction::Sleep, ScalarFunction::FormatType, ScalarFunction::Patch,
            ScalarFunction::Coalesce, ScalarFunction::Nullif, ScalarFunction::NilQ, ScalarFunction::IsNull,
            ScalarFunction::TrueQ, ScalarFunction::IsTrue, ScalarFunction::FalseQ, ScalarFunction::IsFalse,
            ScalarFunction::DistinctFrom, ScalarFunction::MetaDouble, ScalarFunction::Boolean, ScalarFunction::NullEq,
            ScalarFunction::StrictEq, ScalarFunction::PeriodConstructor, ScalarFunction::Set,
            ScalarFunction::SnapshotToken, ScalarFunction::Abs, ScalarFunction::Lower, ScalarFunction::Upper,
            ScalarFunction::LowerInf, ScalarFunction::UpperInf, ScalarFunction::Length, ScalarFunction::EpochNanosBe,
            ScalarFunction::Iid, ScalarFunction::Keyword, ScalarFunction::Namespace, ScalarFunction::LocalName,
            ScalarFunction::Cardinality, ScalarFunction::Nth, ScalarFunction::Greatest, ScalarFunction::Least,
            ScalarFunction::LikeRegex, ScalarFunction::RegexpReplace, ScalarFunction::Floor, ScalarFunction::Ceil,
            ScalarFunction::Ceiling, ScalarFunction::Round, ScalarFunction::Mod, ScalarFunction::IdEqAny,
            ScalarFunction::Spatial(SpatialFunction::Match), ScalarFunction::Spatial(SpatialFunction::Distance),
            ScalarFunction::Interval(IntervalConstructorFunction::SingleField),
            ScalarFunction::Interval(IntervalConstructorFunction::MultiField),
            ScalarFunction::Collection(CollectionFunction::ArrayUpper),
            ScalarFunction::Collection(CollectionFunction::ArrayLower),
            ScalarFunction::Collection(CollectionFunction::ArrayLength),
            ScalarFunction::Collection(CollectionFunction::TrimArray),
            ScalarFunction::Bitwise(BitwiseFunction::BitNot), ScalarFunction::Bitwise(BitwiseFunction::BitAnd),
            ScalarFunction::Bitwise(BitwiseFunction::BitOr), ScalarFunction::Bitwise(BitwiseFunction::BitXor),
            ScalarFunction::Bitwise(BitwiseFunction::BitShiftLeft), ScalarFunction::Bitwise(BitwiseFunction::BitShiftRight),
            ScalarFunction::SessionInfo(SessionInfoFunction::CurrentUser),
            ScalarFunction::SessionInfo(SessionInfoFunction::CurrentDatabase),
            ScalarFunction::SessionInfo(SessionInfoFunction::CurrentSchema),
            ScalarFunction::SessionInfo(SessionInfoFunction::CurrentSchemas),
            ScalarFunction::SessionInfo(SessionInfoFunction::CurrentSetting),
            ScalarFunction::SessionInfo(SessionInfoFunction::Version),
            ScalarFunction::SessionInfo(SessionInfoFunction::XtVersion),
            ScalarFunction::Search(SearchFunction::Match), ScalarFunction::Search(SearchFunction::SearchScore),
            ScalarFunction::Search(SearchFunction::SearchLength), ScalarFunction::Search(SearchFunction::SearchTerms),
            ScalarFunction::Search(SearchFunction::SearchCorpusFingerprint),
            ScalarFunction::Search(SearchFunction::SearchTokenCount),
            ScalarFunction::Search(SearchFunction::SearchTermMatches),
            ScalarFunction::Temporal(TemporalFunction::Extract), ScalarFunction::Temporal(TemporalFunction::DateTrunc),
            ScalarFunction::Temporal(TemporalFunction::Age), ScalarFunction::Temporal(TemporalFunction::DateBin),
            ScalarFunction::Temporal(TemporalFunction::RangeBins),
            ScalarFunction::Temporal(TemporalFunction::CurrentTimestamp),
            ScalarFunction::Temporal(TemporalFunction::CurrentDate),
            ScalarFunction::Temporal(TemporalFunction::CurrentTime),
            ScalarFunction::Temporal(TemporalFunction::CurrentTimezone),
            ScalarFunction::Temporal(TemporalFunction::LocalDate),
            ScalarFunction::Temporal(TemporalFunction::LocalTime),
            ScalarFunction::Temporal(TemporalFunction::LocalTimestamp),
            ScalarFunction::String(StringFunction::Like), ScalarFunction::String(StringFunction::Ilike),
            ScalarFunction::String(StringFunction::Str), ScalarFunction::String(StringFunction::Format),
            ScalarFunction::String(StringFunction::Concat), ScalarFunction::String(StringFunction::Trim),
            ScalarFunction::String(StringFunction::TrimLeading), ScalarFunction::String(StringFunction::TrimTrailing),
            ScalarFunction::String(StringFunction::Substring), ScalarFunction::String(StringFunction::Overlay),
            ScalarFunction::String(StringFunction::Position), ScalarFunction::String(StringFunction::OctetPosition),
            ScalarFunction::String(StringFunction::CharacterLength), ScalarFunction::String(StringFunction::OctetLength),
            ScalarFunction::String(StringFunction::DefaultOverlayLength), ScalarFunction::String(StringFunction::Replace),
            ScalarFunction::String(StringFunction::Reverse), ScalarFunction::String(StringFunction::StringToArray),
            ScalarFunction::String(StringFunction::ParseIdent), ScalarFunction::String(StringFunction::QuoteIdent),
            ScalarFunction::FloatMath(FloatMath::Sqrt), ScalarFunction::FloatMath(FloatMath::Exp),
            ScalarFunction::FloatMath(FloatMath::Ln), ScalarFunction::FloatMath(FloatMath::Log),
            ScalarFunction::FloatMath(FloatMath::Log10), ScalarFunction::FloatMath(FloatMath::Power),
            ScalarFunction::FloatMath(FloatMath::Sin), ScalarFunction::FloatMath(FloatMath::Cos),
            ScalarFunction::FloatMath(FloatMath::Tan), ScalarFunction::FloatMath(FloatMath::Sinh),
            ScalarFunction::FloatMath(FloatMath::Cosh), ScalarFunction::FloatMath(FloatMath::Tanh),
            ScalarFunction::FloatMath(FloatMath::Double), ScalarFunction::FloatMath(FloatMath::Asin),
            ScalarFunction::FloatMath(FloatMath::Acos), ScalarFunction::FloatMath(FloatMath::Atan),
        ];
        for predicate in period_predicates {
            all.push(ScalarFunction::Period { predicate, question: false });
            all.push(ScalarFunction::Period { predicate, question: true });
        }
        for function in all {
            let name = function.canonical_name();
            assert_eq!(ScalarFunction::resolve(name), Some(function), "{name}");
            assert_eq!(ScalarFunction::resolve(&name.to_ascii_uppercase()), Some(function), "upper {name}");
            assert_eq!(ScalarFunction::resolve(&name.replace('_', "-")), Some(function), "hyphen {name}");
        }
        assert_eq!(ScalarFunction::resolve("now"), Some(ScalarFunction::Temporal(TemporalFunction::CurrentTimestamp)));
        assert_eq!(ScalarFunction::resolve("NOW"), Some(ScalarFunction::Temporal(TemporalFunction::CurrentTimestamp)));
        assert_eq!(ScalarFunction::resolve("localdate"), Some(ScalarFunction::Temporal(TemporalFunction::LocalDate)));
        assert_eq!(ScalarFunction::resolve("localtime"), Some(ScalarFunction::Temporal(TemporalFunction::LocalTime)));
        assert_eq!(ScalarFunction::resolve("localtimestamp"), Some(ScalarFunction::Temporal(TemporalFunction::LocalTimestamp)));
        assert_eq!(ResolvedFunction::resolve("unregistered-fn"), ResolvedFunction::Unknown("unregistered-fn"));
    }

    #[test]
    fn resolved_dispatch_keeps_error_semantics_and_aliases_identical() {
        let context = EvalContext::default();
        let period = Value::TimestampTzRange { lower: 0, upper: 100 };
        let point = Value::Timestamp { value: 50, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        assert_eq!(scalar_call("contains?", &[period.clone(), point.clone()], &context).unwrap(), Value::Bool(true));
        assert_eq!(scalar_call("CONTAINS?", &[period.clone(), point.clone()], &context).unwrap(), Value::Bool(true));
        assert_eq!(scalar_call("contains", &[period, point], &context).unwrap_err().code, "xtdb.expression/function-type-mismatch");
        assert_eq!(scalar_call("bit-and", &[Value::I64(6), Value::I64(3)], &context).unwrap(), Value::I64(2));
        assert_eq!(scalar_call("cosine-distance", &[Value::Null], &context).unwrap_err().code, "xtdb/invalid-function-arity");
        assert_eq!(scalar_call("approximate-nearest", &[Value::Null], &context).unwrap_err().code, "xtdb/invalid-function-arity");
        assert_eq!(scalar_call("unregistered_fn", &[Value::Null], &context).unwrap(), Value::Null);
        assert_eq!(scalar_call("unregistered_fn", &[Value::I64(1)], &context).unwrap_err().code, "xtdb.expression/function-type-mismatch");
    }
}
