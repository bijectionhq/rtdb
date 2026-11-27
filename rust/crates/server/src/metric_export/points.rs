use std::collections::BTreeMap;
use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::CatalogMetric;
use rtdb_types::{Anomaly, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Destination { CloudWatch, Azure }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Unit { None, Count, Milliseconds, Bytes, Seconds }
impl Unit {
    fn from_catalog(unit: Option<rtdb_plan::CatalogMetricUnit>) -> Self { match unit { None => Self::None, Some(rtdb_plan::CatalogMetricUnit::Bytes) => Self::Bytes, Some(rtdb_plan::CatalogMetricUnit::Seconds) => Self::Seconds } }
}
pub(super) struct Point { pub name: String, pub tags: BTreeMap<String, String>, pub value: f64, pub unit: Unit, pub aggregate: Option<(i32, f64)> }
pub(super) struct Points { pub values: Vec<Point>, pub _reservation: Arc<MemoryReservation> }

impl Points {
    pub fn new(metrics: &[CatalogMetric], destination: Destination, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bound = metrics.iter().try_fold(4096usize, |sum, metric| {
            let fields = metric.tags().iter().try_fold(metric.name().len() + 1024, |bytes, (key, value)| bytes.checked_add(key.len()).and_then(|bytes| bytes.checked_add(value.len())).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error))?;
            sum.checked_add(fields.checked_mul(128).ok_or_else(size_error)?).ok_or_else(size_error)
        })?;
        let reservation = memory.reserve(bound)?;
        let mut values = Vec::with_capacity(metrics.len().checked_mul(10).ok_or_else(size_error)?);
        for metric in metrics {
            let mut add = |suffix: &str, value: f64, unit: Unit, aggregate: Option<(i32, f64)>, phi: Option<&str>| {
                if !value.is_finite() && (destination == Destination::Azure || value.is_nan()) { return; }
                let name = format!("{}{suffix}", metric.name());
                let mut tags = metric.tags().clone();
                if let Some(phi) = phi { tags.insert("phi".into(), phi.into()); }
                let (name, tags, value) = match destination {
                    Destination::CloudWatch => (name, tags.into_iter().filter(|(_, value)| !value.chars().all(java_whitespace)).take(30).map(|(key, value)| (truncate_utf16(&key, 255), truncate_utf16(&value, 1024))).collect(), cloudwatch_value(value)),
                    Destination::Azure => (truncate_utf16(&azure_name(&name), 1024), tags.into_iter().fold(BTreeMap::new(), |mut tags, (key, value)| { tags.entry(truncate_utf16(&azure_name(&key), 150)).or_insert_with(|| truncate_utf16(&value, 8192)); tags }), value),
                };
                values.push(Point { name, tags, value, unit, aggregate });
            };
            let scalar_suffix = if destination == Destination::CloudWatch { ".value" } else { "" };
            match metric {
                CatalogMetric::Gauge { value, unit, .. } => add(scalar_suffix, *value, Unit::from_catalog(*unit), None, None),
                CatalogMetric::Counter { count, .. } => add(if destination == Destination::CloudWatch { ".count" } else { "" }, *count, Unit::Count, None, None),
                CatalogMetric::Timer { count, total_ns, max_ns, percentiles_ns, .. } => {
                    let total = *total_ns / 1_000_000.0;
                    let maximum = max_ns.unwrap_or(0) as f64 / 1_000_000.0;
                    if destination == Destination::CloudWatch {
                        add(".sum", total, Unit::Milliseconds, None, None);
                        add(".count", *count as f64, Unit::Count, None, None);
                        if *count > 0 { add(".avg", total / *count as f64, Unit::Milliseconds, None, None); add(".max", maximum, Unit::Milliseconds, None, None); }
                    } else if *count != 0 { add("", total, Unit::Milliseconds, Some(((*count).min(i64::from(i32::MAX)) as i32, maximum)), None); }
                    for (phi, value) in ["0.75", "0.85", "0.95", "0.98", "0.99", "0.999"].into_iter().zip(percentiles_ns) {
                        add(if destination == Destination::CloudWatch { ".percentile.value" } else { ".percentile" }, *value / 1_000_000.0, Unit::Milliseconds, None, Some(phi));
                    }
                }
                CatalogMetric::DistributionSummary { count, total, maximum, percentiles, unit, .. } => {
                    let unit = Unit::from_catalog(*unit);
                    if destination == Destination::CloudWatch {
                        add(".sum", *total, unit, None, None); add(".count", *count as f64, Unit::Count, None, None);
                        if *count > 0 { add(".avg", *total / *count as f64, unit, None, None); add(".max", *maximum, unit, None, None); }
                    } else if *count != 0 { add("", *total, unit, Some(((*count).min(i64::from(i32::MAX)) as i32, *maximum)), None); }
                    for (phi, value) in ["0.5", "0.95", "0.99"].into_iter().zip(percentiles) {
                        add(if destination == Destination::CloudWatch { ".percentile.value" } else { ".percentile" }, *value, unit, None, Some(phi));
                    }
                }
            }
        }
        Ok(Self { values, _reservation: reservation })
    }
}

fn cloudwatch_value(value: f64) -> f64 { if value == 0.0 { 0.0 } else { value.abs().clamp(8.515920e-109, 1.174271e108).copysign(value) } }
fn azure_name(value: &str) -> String { value.trim_end_matches('.').chars().map(|character| if character.is_ascii_alphanumeric() || character == '-' { character } else { '_' }).collect() }
fn java_whitespace(character: char) -> bool { matches!(character, '\u{1c}'..='\u{1f}') || character.is_whitespace() && !matches!(character, '\u{85}' | '\u{a0}' | '\u{2007}' | '\u{202f}') }
fn truncate_utf16(value: &str, units: usize) -> String {
    let mut left = units;
    value.chars().take_while(|character| { let size = character.len_utf16(); if size > left { false } else { left -= size; true } }).collect()
}
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.metrics/export-size", "Metric export allocation size overflow").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }

#[cfg(test)]
mod tests {
    use super::*;
    fn timer(count: i64) -> CatalogMetric { CatalogMetric::Timer { name: "query.timer".into(), tags: BTreeMap::from([("node-id".into(), "n".into())]), count, total_ns: 6_000_000.0, percentiles_ns: [1_000_000.0; 6], mean_ns: Some(3_000_000), max_ns: Some(4_000_000), p75_ns: None, p95_ns: None, p99_ns: None, p999_ns: None } }
    #[test]
    fn cloudwatch_preserves_declared_gauge_and_summary_units() {
        let memory = MemoryBudget::new(1 << 22);
        let rows = Points::new(&[
            CatalogMetric::Gauge { name: "allocated".into(), tags: BTreeMap::new(), unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value: 42.0 },
            CatalogMetric::DistributionSummary { name: "lag".into(), tags: BTreeMap::new(), unit: Some(rtdb_plan::CatalogMetricUnit::Seconds), count: 1, total: 0.5, maximum: 0.5, percentiles: [0.5; 3] },
        ], Destination::CloudWatch, &memory).unwrap();
        assert_eq!(rows.values[0].unit, Unit::Bytes);
        assert!(rows.values.iter().filter(|point| point.name.starts_with("lag") && point.name != "lag.count").all(|point| point.unit == Unit::Seconds));
    }
    #[test]
    fn cloudwatch_meter_registry_timer_suffixes_units_and_percentile_tags() {
        let memory = MemoryBudget::new(1 << 20);
        let rows = Points::new(&[timer(2)], Destination::CloudWatch, &memory).unwrap();
        assert_eq!(rows.values.len(), 10);
        assert_eq!(rows.values.iter().find(|point| point.name == "query.timer.sum").unwrap().value, 6.0);
        assert_eq!(rows.values.iter().find(|point| point.name == "query.timer.avg").unwrap().unit, Unit::Milliseconds);
        assert_eq!(rows.values.iter().filter(|point| point.name == "query.timer.percentile.value" && point.tags.contains_key("phi")).count(), 6);
        assert_eq!(Points::new(&[timer(0)], Destination::CloudWatch, &memory).unwrap().values.len(), 8);
        drop(rows); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn azure_monitor_timer_aggregation_and_empty_step_keep_percentile_gauges() {
        let memory = MemoryBudget::new(1 << 20);
        let rows = Points::new(&[timer(2)], Destination::Azure, &memory).unwrap();
        assert_eq!(rows.values[0].name, "query_timer"); assert_eq!(rows.values[0].aggregate, Some((2, 4.0)));
        assert_eq!(Points::new(&[timer(0)], Destination::Azure, &memory).unwrap().values.len(), 6);
    }
    #[test]
    fn cloudwatch_clamps_infinity_drops_nan_and_limits_nonblank_dimensions() {
        let memory = MemoryBudget::new(1 << 22);
        let tags = (0..40).map(|index| (format!("tag{index:02}"), if index == 0 { " ".into() } else { "v".into() })).collect();
        let rows = Points::new(&[CatalogMetric::Gauge { name: "x".into(), tags, unit: None, value: f64::INFINITY }, CatalogMetric::Gauge { name: "nan".into(), tags: BTreeMap::new(), unit: None, value: f64::NAN }], Destination::CloudWatch, &memory).unwrap();
        assert_eq!(rows.values.len(), 1); assert_eq!(rows.values[0].value, 1.174271e108); assert_eq!(rows.values[0].tags.len(), 30);
        assert_eq!(truncate_utf16("a😀b", 3), "a😀");
    }
}
