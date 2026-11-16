use std::fmt::Write;
use std::sync::Arc;

use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::CatalogMetric;
use rtdb_types::{Anomaly, Result};

struct ReservedText { value: String, reservation: Arc<MemoryReservation>, limit: usize }
impl ReservedText {
    fn new(limit: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(limit.checked_mul(2).and_then(|bytes| bytes.checked_add(128)).ok_or_else(overflow)?)?;
        Ok(Self { value: String::with_capacity(limit), reservation, limit })
    }
}
impl Write for ReservedText {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        if self.value.len().checked_add(text.len()).is_none_or(|length| length > self.limit) { return Err(std::fmt::Error); }
        self.value.push_str(text);
        Ok(())
    }
}
impl AsRef<[u8]> for ReservedText { fn as_ref(&self) -> &[u8] { self.value.as_bytes() } }

/// Returns Prometheus 0.0.4 text whose bytes retain the rendering reservation.
/// Timer observations are exported in seconds; catalog observations remain nanoseconds.
pub fn render(metrics: &[CatalogMetric], memory: &Arc<MemoryBudget>) -> Result<Bytes> {
    let bound = metrics.iter().try_fold(1024usize, |bytes, metric| {
        let labels = metric.tags().iter().try_fold(metric.name().len(), |bytes, (key, value)| bytes.checked_add(key.len()).and_then(|bytes| bytes.checked_add(value.len())).ok_or_else(overflow))?;
        bytes.checked_add(labels.checked_mul(64).ok_or_else(overflow)?).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(overflow)
    })?;
    let mut output = ReservedText::new(bound, memory)?;
    for metric in metrics {
        let mut name = metric.name().chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == ':' { c } else { '_' }).collect::<String>();
        if let CatalogMetric::Gauge { unit: Some(unit), .. } | CatalogMetric::DistributionSummary { unit: Some(unit), .. } = metric {
            let suffix = match unit { rtdb_plan::CatalogMetricUnit::Bytes => "_bytes", rtdb_plan::CatalogMetricUnit::Seconds => "_seconds" };
            if !name.ends_with(suffix) { name.push_str(suffix); }
        }
        let tags = labels(metric.tags(), None);
        match metric {
            CatalogMetric::Gauge { value, .. } => writeln!(&mut output, "{name}{tags} {}", number(*value)),
            CatalogMetric::Counter { count, .. } => writeln!(&mut output, "{}_total{tags} {}", name.strip_suffix("_total").unwrap_or(&name), number(*count)),
            CatalogMetric::Timer { count, total_ns, max_ns, percentiles_ns, .. } => {
                for (quantile, value) in ["0.75", "0.85", "0.95", "0.98", "0.99", "0.999"].into_iter().zip(percentiles_ns) {
                    writeln!(&mut output, "{name}_seconds{} {}", labels(metric.tags(), Some(("quantile", quantile))), *value / 1e9).map_err(|_| overflow())?;
                }
                writeln!(&mut output, "{name}_seconds_count{tags} {count}").map_err(|_| overflow())?;
                writeln!(&mut output, "{name}_seconds_sum{tags} {}", *total_ns / 1e9).map_err(|_| overflow())?;
                writeln!(&mut output, "{name}_seconds_max{tags} {}", max_ns.unwrap_or(0) as f64 / 1e9)
            }
            CatalogMetric::DistributionSummary { count, total, maximum, percentiles, .. } => {
                for (quantile, value) in ["0.5", "0.95", "0.99"].into_iter().zip(percentiles) {
                    writeln!(&mut output, "{name}{} {}", labels(metric.tags(), Some(("quantile", quantile))), number(*value)).map_err(|_| overflow())?;
                }
                writeln!(&mut output, "{name}_count{tags} {count}").map_err(|_| overflow())?;
                writeln!(&mut output, "{name}_sum{tags} {}", number(*total)).map_err(|_| overflow())?;
                writeln!(&mut output, "{name}_max{tags} {}", number(*maximum))
            }
        }.map_err(|_| overflow())?;
    }
    Ok(Bytes::from_owner(output))
}

fn labels(tags: &std::collections::BTreeMap<String, String>, extra: Option<(&str, &str)>) -> String {
    if tags.is_empty() && extra.is_none() { return String::new(); }
    let mut output = String::from("{");
    for (index, (key, value)) in tags.iter().map(|(key, value)| (key.as_str(), value.as_str())).chain(extra).enumerate() {
        if index != 0 { output.push(','); }
        for c in key.chars() { output.push(if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }); }
        output.push_str("=\"");
        for c in value.chars() { match c { '\\' => output.push_str("\\\\"), '"' => output.push_str("\\\""), '\n' => output.push_str("\\n"), c => output.push(c) } }
        output.push('"');
    }
    output.push('}');
    output
}
fn number(value: f64) -> String { if value.is_nan() { "NaN".into() } else if value == f64::INFINITY { "+Inf".into() } else if value == f64::NEG_INFINITY { "-Inf".into() } else { value.to_string() } }
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb.metrics/render-limit", "Metrics output exceeds its allocation bound") }
