mod inventory;
mod resolved;

use std::{cell::RefCell, collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_server::config::{ConfigurationEnvironment, EnvironmentValue, ParsedConfiguration, ParsedYamlDocument};
use rtdb_types::{Anomaly, Result};
use rtdb_types::AnomalyKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const MAX_INPUT_BYTES: usize = 1024 * 1024;
pub const REPORT_VERSION: u32 = 1;

#[derive(Clone, Copy)]
pub enum InputFormat { Yaml, IngestYaml, Edn }

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub reference_revision: String,
    pub implementation_revision: String,
    pub server_artifact_sha256: String,
    pub migrator_artifact_sha256: String,
}

#[derive(Serialize)]
pub struct Finding {
    pub code: &'static str,
    pub path: String,
    pub message: &'static str,
}

#[derive(Serialize)]
pub struct DatabaseReport {
    pub name: String,
    pub partitions: i32,
    pub mode: i32,
    pub log: Value,
    pub storage: Value,
    pub external_source: Option<Value>,
}

#[derive(Serialize)]
pub struct QualificationReport {
    pub schema_version: u32,
    pub configuration_compatible: bool,
    pub cutover_eligible: bool,
    pub configuration_sha256: String,
    pub environment_input_sha256: String,
    pub provenance_input_sha256: String,
    pub provenance: Option<Provenance>,
    pub compiled_features: Vec<&'static str>,
    pub databases: Vec<DatabaseReport>,
    pub authentication: Option<Value>,
    pub remotes: BTreeMap<String, Value>,
    pub modules: Vec<Value>,
    pub runtime: Option<Value>,
    pub environment_lookups: BTreeSet<String>,
    pub configuration_gaps: Vec<Finding>,
    pub cutover_requirements: Vec<&'static str>,
    pub assumptions: Vec<&'static str>,
    pub unsupported_jvm_extensions: Vec<&'static str>,
    #[serde(skip)]
    reservations: Vec<Arc<MemoryReservation>>,
}
impl QualificationReport {
    pub(crate) fn gap(&mut self, code: &'static str, path: impl Into<String>, message: &'static str) {
        if self.configuration_gaps.len() < 9_999 { self.configuration_gaps.push(Finding { code, path: path.into(), message }); }
        else if self.configuration_gaps.len() == 9_999 { self.configuration_gaps.push(Finding { code: "inventory-limit", path: "$".into(), message: "The report finding limit was reached; remaining configuration paths are unqualified." }); }
    }
    pub(crate) fn feature(&mut self, feature: &'static str, path: &str) {
        if !self.compiled_features.contains(&feature) { self.gap("feature-not-compiled", path, "The paired server and migrator must be packaged with this backend feature."); }
    }
}

/// Inspects supplied bytes with the production decoder; it opens no deployment files, services or credential providers.
/// Environment JSON is the complete explicit lookup map, not a patch over the process environment.
/// The returned report owns its allocation reservation; caller-owned input buffers remain the caller's responsibility.
/// Invalid configuration produces a redacted, ineligible report; size/budget exhaustion returns an error.
pub fn qualify(configuration: &str, format: InputFormat, environment_json: &[u8], provenance_json: &[u8], memory: &Arc<MemoryBudget>) -> Result<QualificationReport> {
    for length in [configuration.len(), environment_json.len(), provenance_json.len()] {
        if length > MAX_INPUT_BYTES { return Err(invalid("A qualification input exceeds the one MiB limit")); }
    }
    let total = configuration.len().checked_add(environment_json.len()).and_then(|value| value.checked_add(provenance_json.len())).ok_or_else(|| invalid("Qualification size overflow"))?;
    let bytes = total.checked_mul(256).and_then(|value| value.checked_add(1024 * 1024)).ok_or_else(|| invalid("Qualification size overflow"))?;
    let reservation = memory.reserve(bytes)?;
    let mut report = QualificationReport {
        schema_version: REPORT_VERSION, configuration_compatible: false, cutover_eligible: false,
        configuration_sha256: digest(configuration.as_bytes()), environment_input_sha256: digest(environment_json), provenance_input_sha256: digest(provenance_json),
        provenance: None, compiled_features: compiled_features(), databases: Vec::new(), authentication: None, remotes: BTreeMap::new(), modules: Vec::new(), runtime: None,
        environment_lookups: BTreeSet::new(), configuration_gaps: Vec::new(),
        cutover_requirements: vec![
            "Inventory attached databases, partitions, every writer, CDC source, compactor, GC worker, client version and restore procedure.",
            "Approve a write-pause budget, recovery-time target, retained log range and protected object versions.",
            "Qualify shadow reads with read-only credentials at matching valid time, system time and durable basis.",
            "Pause ingress and upstream intake, drain accepted work, reconcile ambiguous outcomes, and confirm finalized blocks.",
            "Fence every JVM publisher and upstream acknowledgement owner; retain the operator-owned writer lease.",
            "Capture and verify finalized block, source/replica positions, durable transaction, term/sequence, storage version/epoch and external token for every partition.",
            "Recover Rust through that manifest, verify restore, canary read-back from a second node, restart and historical reads.",
            "Rehearse JVM recovery through acknowledged Rust writes and partially uploaded compaction output before production writes.",
            "Keep ingress paused on mismatch; retain Rust logs/files if reverse recovery cannot reach the last acknowledged position.",
            "Close the rollback window explicitly; simultaneous JVM/Rust writers require separate qualification.",
        ],
        assumptions: vec![
            "This report validates supplied data only; artifact hashes and revisions are operator declarations, not build attestations.",
            "No log, object store, credential provider, TLS keystore, properties file, schema registry or attached-database catalog is opened.",
            "Local paths are lexical and must be resolved on the deployment host; symlinks, permissions, disk capacity and retention are unverified.",
            "Only the supplied environment map is used; omitted implicit environment settings take production defaults.",
            "Default compactor thread count depends on this host; set threads explicitly for reproducible deployment settings.",
            "Configuration compatibility does not establish functional parity, durable state compatibility or permission to transfer writers.",
        ],
        unsupported_jvm_extensions: vec![
            "Programmatic JVM module factories, classpath registrations and custom lifecycle objects require a native adapter or separately deployed bridge.",
            "Arbitrary Java/Kotlin/Clojure callbacks, custom PostgreSQL indexers and Kafka Connect converter classes are not executable native configuration.",
            "EDN reader evaluation, object constructors and executable Clojure configuration require an explicit data-only migration.",
            "Custom credential-provider classes and unsupported JAAS login modules require a native credential adapter.",
        ], reservations: vec![reservation],
    };
    report.provenance = serde_json::from_slice::<Provenance>(provenance_json).ok().filter(|value| {
        hex(&value.reference_revision, 40) && hex(&value.implementation_revision, 40) && hex(&value.server_artifact_sha256, 64) && hex(&value.migrator_artifact_sha256, 64)
    });
    if report.provenance.is_none() { report.gap("invalid-provenance", "provenance", "Supply exact reference/implementation revisions and SHA256 digests for both paired artifacts."); }
    let values = match serde_json::from_slice::<EnvironmentMap>(environment_json) {
        Ok(values) => values.0,
        Err(_) => { report.gap("invalid-environment", "environment", "Environment input must be a JSON object of string values."); return Ok(report); }
    };
    let environment = ExplicitEnvironment { values, requested: RefCell::new(BTreeSet::new()) };
    let parsed = match format {
        InputFormat::Yaml | InputFormat::IngestYaml => ParsedYamlDocument::from_yaml(configuration, &environment, memory).and_then(|document| {
            report.reservations.push(memory.reserve(document.allocation_bound().checked_mul(2).ok_or_else(|| invalid("Qualification size overflow"))?)?);
            inventory::inspect(document.value(), matches!(format, InputFormat::IngestYaml), &mut report);
            document.into_node_configuration(&environment, matches!(format, InputFormat::IngestYaml))
        }),
        InputFormat::Edn => {
            report.gap("edn-projection-unqualified", "configuration", "The production EDN adapter projects recognized fields and can omit nested JVM options; convert to explicit YAML and compare every option before qualification.");
            ParsedConfiguration::from_edn(configuration, &environment, memory)
        }
    };
    match parsed {
        Ok(parsed) => {
            report.reservations.push(memory.reserve(parsed.allocation_bound().checked_mul(2).ok_or_else(|| invalid("Qualification size overflow"))?)?);
            if !parsed.warnings().is_empty() { report.gap("ignored-edn-options", "configuration", "The production decoder reported ignored configuration keys; none are accepted for cutover."); }
            resolved::describe(parsed.configuration(), &mut report);
        }
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => return Err(error),
        Err(_) => report.gap("production-decoder-rejected", "configuration", "The bounded production decoder rejected the configuration or its environment; diagnostic values are withheld to protect secrets."),
    }
    report.environment_lookups = std::mem::take(&mut *environment.requested.borrow_mut());
    report.configuration_compatible = report.configuration_gaps.is_empty();
    Ok(report)
}

struct ExplicitEnvironment { values: BTreeMap<String, String>, requested: RefCell<BTreeSet<String>> }
struct EnvironmentMap(BTreeMap<String, String>);
impl<'de> Deserialize<'de> for EnvironmentMap {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct MapVisitor;
        impl<'de> serde::de::Visitor<'de> for MapVisitor {
            type Value = EnvironmentMap;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("an environment map with unique string keys") }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> std::result::Result<Self::Value, A::Error> {
                let mut values = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, String>()? {
                    if values.insert(key, value).is_some() { return Err(serde::de::Error::custom("Duplicate environment key")); }
                }
                Ok(EnvironmentMap(values))
            }
        }
        deserializer.deserialize_map(MapVisitor)
    }
}
impl ConfigurationEnvironment for ExplicitEnvironment {
    fn lookup(&self, name: &str, memory: &Arc<MemoryBudget>) -> Result<Option<EnvironmentValue>> {
        self.requested.borrow_mut().insert(name.into());
        self.values.get(name).map(|value| EnvironmentValue::copy(value, memory)).transpose()
    }
}
impl Drop for ExplicitEnvironment {
    fn drop(&mut self) { use zeroize::Zeroize; for value in self.values.values_mut() { value.zeroize(); } }
}

pub fn compiled_features() -> Vec<&'static str> {
    [("local", true), ("kafka", cfg!(feature = "kafka")), ("kafka-connect", cfg!(feature = "kafka-connect")), ("s3", cfg!(feature = "s3")),
        ("azure", cfg!(feature = "azure")), ("gcs", cfg!(feature = "gcs")), ("pg-source", cfg!(feature = "pg-source")), ("cloudwatch", cfg!(feature = "cloudwatch"))]
        .into_iter().filter_map(|(name, enabled)| enabled.then_some(name)).collect()
}
pub fn digest(bytes: &[u8]) -> String { Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect() }
fn hex(value: &str, length: usize) -> bool { value.len() == length && value.bytes().all(|value| value.is_ascii_hexdigit()) }
fn invalid(message: &'static str) -> Anomaly { Anomaly::incorrect("rtdb.config/qualification", message) }
