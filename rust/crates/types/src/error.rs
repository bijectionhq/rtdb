use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt};

pub type Result<T> = std::result::Result<T, Anomaly>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AnomalyCategory {
    Incorrect,
    Unsupported,
    NotFound,
    Forbidden,
    Conflict,
    Busy,
    Unavailable,
    Interrupted,
    Fault,
}

/// What went wrong, for the anomalies a caller acts on by what happened rather than by whose fault it was.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AnomalyKind {
    /// A memory budget, byte limit or allocator had no room for the work.
    ResourceExhausted,
    /// The work outgrew a limit the operator configured, such as the query spill cap.
    ConfigurationLimitExceeded,
    /// The work outgrew a fixed limit of the implementation, such as the size a compiled pattern may reach.
    ProgramLimitExceeded,
    /// A number did not fit the type it was to be stored or sent as.
    NumericValueOutOfRange,
    /// A table name resolved to no table.
    TableNotFound,
    /// A transaction conflicted with data that changed under it: a failed `ASSERT`, or a write fenced by another work's grant.
    OptimisticConflict,
    /// The object store holds no object under the key, as when a read reaches a file collected after its block left the retention window.
    MissingObject,
    /// A stored object could not be decoded as the artefact it should hold.
    CorruptObject,
    /// An index build was handed rows outside the order it requires.
    InputOutOfOrder,
    /// A construct the vendor SQL parser admits but XTDB's grammar has no production for.
    GrammarRejected,
    /// A read asked for a basis this node has not applied yet.
    BasisAhead,
    /// A read asked for a basis this database never had, or one from another incarnation.
    UnknownBasis,
    /// This node holds no writer lease for the database, so a write must go to the node that does.
    NoWriter,
    /// A read's lease or plaintext delivery grant expired before its answer was delivered.
    ReadExpired,
}

impl AnomalyKind {
    /// The kind of an anomaly read from a format that carries only its code, such as XTDB's Transit errors.
    /// Every code listed here is raised only with its kind, so the code alone recovers it.
    pub fn from_wire_code(code: &str) -> Option<Self> {
        match code {
            "rtdb/memory-budget-exceeded" | "rtdb.storage/memory-limit"
            | "rtdb/allocation-failed" | "rtdb.expr/allocation-failed" => Some(Self::ResourceExhausted),
            "rtdb.query/spill-limit" | "rtdb.storage/message-too-large" | "rtdb.auth/response-too-large" => Some(Self::ConfigurationLimitExceeded),
            "rtdb.expr/regex-limit" | "rtdb.expr/regex-host-limit" | "rtdb.query/sort-size" | "rtdb.query/shared-plan-size"
            | "rtdb.sql/allocation-size" | "rtdb.sql/token-array-size" | "rtdb.sql/split-size" | "rtdb.sql/control-size" | "rtdb.sql/batch-size"
            | "rtdb/list-offset-overflow" | "rtdb/union-offset-overflow" | "rtdb/allocation-overflow" | "rtdb/decimal-width"
            | "rtdb.query/expression-size" | "rtdb.query/parameter-size" | "rtdb.expr/allocation-size" | "rtdb.flight-sql/lease-size"
            | "rtdb.index/schema-size" | "rtdb.index/snapshot-size" | "rtdb.metrics/allocation-overflow" | "rtdb.metrics/export-size"
            | "rtdb.metrics/render-limit" | "rtdb.multi/assertion-family-size" | "rtdb.multi/assertion-size" | "rtdb.multi/replay-broker-size"
            | "rtdb.node/preparation-size" | "rtdb.owned/change-group-too-large" | "rtdb.owned/merged-change-commit-size"
            | "rtdb.owned/prepared-generation-bound" | "rtdb.owned/purge-handle-bound" | "rtdb.owned/read-size" | "rtdb.owned/replica-inline-bound"
            | "rtdb.pgwire/history-label-bound" | "rtdb.plan/aggregate-size" | "rtdb.plan/catalog-size" | "rtdb.plan/qualifier-size"
            | "rtdb.plan/regex-metadata-size" | "rtdb.plan/settings-size" | "rtdb.temporal/ceiling-size" | "rtdb.temporal/history-row-size"
            | "rtdb.tx/allocation-overflow" | "rtdb.types/clojure-index-size" | "rtdb.types/java-text-size" | "rtdb/allocation-bound"
            | "rtdb/classic-projection-size" | "rtdb/column-size" | "rtdb/decision-clock-record-size" | "rtdb/journal-record-too-large"
            | "rtdb/reservation-size" | "rtdb/run-end-overflow" | "rtdb/run-end-selection-width" | "rtdb/trie-row-overflow" | "rtdb/trie-size"
            | "rtdb/type-size" | "rtdb/value-size-overflow" => Some(Self::ProgramLimitExceeded),
            "rtdb/decimal-precision" | "rtdb.pgwire/integer-out-of-range" | "rtdb/time-overflow" => Some(Self::NumericValueOutOfRange),
            "xtdb/table-not-found" => Some(Self::TableNotFound),
            "xtdb/assert-failed" | "rtdb.owned/footprint-grant-fenced" => Some(Self::OptimisticConflict),
            "rtdb.storage/missing-object" => Some(Self::MissingObject),
            "rtdb.vector/corrupt-generation" => Some(Self::CorruptObject),
            "rtdb.search/input-order" => Some(Self::InputOutOfOrder),
            "rtdb/basis-ahead" => Some(Self::BasisAhead),
            "rtdb/unknown-basis" => Some(Self::UnknownBasis),
            "rtdb/no-writer" => Some(Self::NoWriter),
            "rtdb.storage/read-lease-expired" | "rtdb.storage/plaintext-delivery-expired" => Some(Self::ReadExpired),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Anomaly {
    pub category: AnomalyCategory,
    pub code: String,
    pub message: Option<String>,
    #[serde(with = "context")]
    pub context: BTreeMap<String, Value>,
    #[serde(default)]
    pub kind: Option<AnomalyKind>,
}

/// A JSON value decodes only from a self-describing format, so a binary
/// encoding carries each context value as its JSON text.
mod context {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};
    use serde_json::Value;
    use std::collections::BTreeMap;

    pub fn serialize<S: Serializer>(context: &BTreeMap<String, Value>, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() { return context.serialize(serializer); }
        serializer.collect_map(context.iter().map(|(key, value)| (key, value.to_string())))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<BTreeMap<String, Value>, D::Error> {
        if deserializer.is_human_readable() { return BTreeMap::deserialize(deserializer); }
        BTreeMap::<String, String>::deserialize(deserializer)?.into_iter()
            .map(|(key, text)| serde_json::from_str(&text).map(|value| (key, value)).map_err(D::Error::custom))
            .collect()
    }
}

impl Anomaly {
    pub fn new(category: AnomalyCategory, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new_nullable(category, code, Some(message.into()))
    }

    pub fn new_nullable(category: AnomalyCategory, code: impl Into<String>, message: Option<String>) -> Self {
        Self { category, code: code.into(), message, context: BTreeMap::new(), kind: None }
    }

    pub fn incorrect(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(AnomalyCategory::Incorrect, code, message)
    }

    pub fn unsupported(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(AnomalyCategory::Unsupported, code, message)
    }

    pub fn fault(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(AnomalyCategory::Fault, code, message)
    }

    pub fn with_context(mut self, key: impl Into<String>, value: Value) -> Self {
        self.context.insert(key.into(), value);
        self
    }

    pub fn with_kind(mut self, kind: AnomalyKind) -> Self {
        self.kind = Some(kind);
        self
    }

    pub fn is(&self, kind: AnomalyKind) -> bool { self.kind == Some(kind) }

    pub fn with_transit_context(self, key: impl Into<String>, value: &crate::transit_value::TransitValue) -> Result<Self> {
        Ok(self.with_context(key, crate::transit_value::to_json_value(value)?))
    }

    pub fn is_caller_error(&self) -> bool {
        matches!(self.category, AnomalyCategory::Incorrect | AnomalyCategory::Unsupported
            | AnomalyCategory::NotFound | AnomalyCategory::Forbidden | AnomalyCategory::Conflict)
    }
}

impl fmt::Display for Anomaly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.code)?;
        if let Some(message) = &self.message { write!(f, ": {message}")?; }
        Ok(())
    }
}

impl std::error::Error for Anomaly {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_preserve_present_empty_and_absent_messages() {
        for error in [Anomaly::new(AnomalyCategory::Fault, "test/error", ""), Anomaly::fault("test/error", ""), Anomaly::incorrect("test/error", ""), Anomaly::unsupported("test/error", "")] {
            assert_eq!(error.message.as_deref(), Some(""));
        }
        for message in [None, Some(String::new()), Some("detail".into())] {
            let error = Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", message.clone());
            assert_eq!(error.message, message);
            assert_eq!(error.clone(), error);
            assert_eq!(serde_json::from_str::<Anomaly>(&serde_json::to_string(&error).unwrap()).unwrap(), error);
        }
        assert_ne!(Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", None), Anomaly::fault("test/error", ""));
    }

    #[test]
    fn a_kind_survives_serialisation_and_an_absent_kind_adds_no_field() {
        let exhausted = Anomaly::incorrect("test/error", "detail").with_kind(AnomalyKind::ResourceExhausted);
        assert!(exhausted.is(AnomalyKind::ResourceExhausted));
        let json = serde_json::to_value(&exhausted).unwrap();
        assert_eq!(json["kind"], "resource-exhausted");
        assert_eq!(serde_json::from_value::<Anomaly>(json).unwrap(), exhausted);

        let plain = Anomaly::incorrect("test/error", "detail");
        assert!(!plain.is(AnomalyKind::ResourceExhausted));
        let json = serde_json::to_value(&plain).unwrap();
        assert!(json.get("kind").is_none());
        assert_eq!(serde_json::from_value::<Anomaly>(json).unwrap(), plain);
    }

    #[test]
    fn wire_codes_of_exhausted_limits_decode_to_their_kind() {
        assert_eq!(AnomalyKind::from_wire_code("rtdb/memory-budget-exceeded"), Some(AnomalyKind::ResourceExhausted));
        assert_eq!(AnomalyKind::from_wire_code("rtdb.storage/memory-limit"), Some(AnomalyKind::ResourceExhausted));
        assert_eq!(AnomalyKind::from_wire_code("rtdb/allocation-failed"), Some(AnomalyKind::ResourceExhausted));
        assert_eq!(AnomalyKind::from_wire_code("rtdb.query/spill-limit"), Some(AnomalyKind::ConfigurationLimitExceeded));
        assert_eq!(AnomalyKind::from_wire_code("rtdb.expr/regex-limit"), Some(AnomalyKind::ProgramLimitExceeded));
        for code in ["rtdb.query/shared-plan-size", "rtdb.sql/allocation-size", "rtdb.sql/token-array-size", "rtdb.sql/split-size", "rtdb.sql/control-size",
            "rtdb.sql/batch-size", "rtdb/list-offset-overflow", "rtdb/union-offset-overflow", "rtdb/allocation-overflow", "rtdb/decimal-width"] {
            assert_eq!(AnomalyKind::from_wire_code(code), Some(AnomalyKind::ProgramLimitExceeded), "{code}");
        }
        assert_eq!(AnomalyKind::from_wire_code("rtdb.query/expression-size"), Some(AnomalyKind::ProgramLimitExceeded));
        assert_eq!(AnomalyKind::from_wire_code("rtdb.storage/message-too-large"), Some(AnomalyKind::ConfigurationLimitExceeded));
        assert_eq!(AnomalyKind::from_wire_code("rtdb/time-overflow"), Some(AnomalyKind::NumericValueOutOfRange));
        assert_eq!(AnomalyKind::from_wire_code("rtdb/decimal-precision"), Some(AnomalyKind::NumericValueOutOfRange));
        assert_eq!(AnomalyKind::from_wire_code("rtdb.pgwire/integer-out-of-range"), Some(AnomalyKind::NumericValueOutOfRange));
        assert_eq!(AnomalyKind::from_wire_code("xtdb/table-not-found"), Some(AnomalyKind::TableNotFound));
        assert_eq!(AnomalyKind::from_wire_code("xtdb/assert-failed"), Some(AnomalyKind::OptimisticConflict));
        assert_eq!(AnomalyKind::from_wire_code("rtdb.owned/footprint-grant-fenced"), Some(AnomalyKind::OptimisticConflict));
        assert_eq!(AnomalyKind::from_wire_code("rtdb.storage/missing-object"), Some(AnomalyKind::MissingObject));
        assert_eq!(AnomalyKind::from_wire_code("rtdb.vector/corrupt-generation"), Some(AnomalyKind::CorruptObject));
        assert_eq!(AnomalyKind::from_wire_code("rtdb.search/input-order"), Some(AnomalyKind::InputOutOfOrder));
        assert_eq!(AnomalyKind::from_wire_code("rtdb/basis-ahead"), Some(AnomalyKind::BasisAhead));
        assert_eq!(AnomalyKind::from_wire_code("rtdb/unknown-basis"), Some(AnomalyKind::UnknownBasis));
        assert_eq!(AnomalyKind::from_wire_code("rtdb/no-writer"), Some(AnomalyKind::NoWriter));
        assert_eq!(AnomalyKind::from_wire_code("rtdb.storage/read-lease-expired"), Some(AnomalyKind::ReadExpired));
        assert_eq!(AnomalyKind::from_wire_code("xtdb/sql-error"), None);
        assert_eq!(AnomalyKind::from_wire_code("xtdb.expression/overflow-error"), None);
    }

    #[test]
    fn every_kind_serialises_under_its_kebab_case_name() {
        for (kind, name) in [(AnomalyKind::ResourceExhausted, "resource-exhausted"), (AnomalyKind::ConfigurationLimitExceeded, "configuration-limit-exceeded"),
            (AnomalyKind::ProgramLimitExceeded, "program-limit-exceeded"), (AnomalyKind::NumericValueOutOfRange, "numeric-value-out-of-range"),
            (AnomalyKind::TableNotFound, "table-not-found"), (AnomalyKind::OptimisticConflict, "optimistic-conflict"),
            (AnomalyKind::MissingObject, "missing-object"), (AnomalyKind::CorruptObject, "corrupt-object"),
            (AnomalyKind::InputOutOfOrder, "input-out-of-order"), (AnomalyKind::GrammarRejected, "grammar-rejected"),
            (AnomalyKind::BasisAhead, "basis-ahead"), (AnomalyKind::UnknownBasis, "unknown-basis"), (AnomalyKind::NoWriter, "no-writer"),
            (AnomalyKind::ReadExpired, "read-expired")] {
            assert_eq!(serde_json::to_value(kind).unwrap(), name);
            assert_eq!(serde_json::from_value::<AnomalyKind>(name.into()).unwrap(), kind);
        }
    }

    #[test]
    fn display_omits_the_separator_only_for_absent_messages() {
        assert_eq!(Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", None).to_string(), "test/error");
        assert_eq!(Anomaly::fault("test/error", "").to_string(), "test/error: ");
        assert_eq!(Anomaly::fault("test/error", "detail").to_string(), "test/error: detail");
    }
}
