use std::fmt::Write;
use std::sync::Arc;

use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::CatalogMetric;
use rtdb_types::{Anomaly, Result};

struct ReservedText { value: String, _reservation: Arc<MemoryReservation>, limit: usize }
impl ReservedText {
    fn new(limit: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(limit.checked_mul(2).and_then(|bytes| bytes.checked_add(128)).ok_or_else(overflow)?)?;
        Ok(Self { value: String::with_capacity(limit), _reservation: reservation, limit })
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
/// Each family is written once, under its `# HELP` and `# TYPE` lines, with every series of that family after them.
pub fn render(metrics: &[CatalogMetric], memory: &Arc<MemoryBudget>) -> Result<Bytes> {
    let bound = metrics.iter().try_fold(1024usize, |bytes, metric| {
        let labels = metric.tags().iter().try_fold(metric.name().len(), |bytes, (key, value)| bytes.checked_add(key.len()).and_then(|bytes| bytes.checked_add(value.len())).ok_or_else(overflow))?;
        bytes.checked_add(labels.checked_mul(64).ok_or_else(overflow)?).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(overflow)
    })?;
    let mut output = ReservedText::new(bound, memory)?;
    for family in families(metrics) {
        // Micrometer's help text for a meter without a description is a single space.
        writeln!(&mut output, "# HELP {}  \n# TYPE {} {}", family.name, family.name, family.kind.as_str()).map_err(|_| overflow())?;
        for (metric, part) in family.members { write_series(&mut output, &family.name, metric, part).map_err(|_| overflow())?; }
    }
    Ok(Bytes::from_owner(output))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind { Gauge, Counter, Summary }
impl Kind { fn as_str(self) -> &'static str { match self { Self::Gauge => "gauge", Self::Counter => "counter", Self::Summary => "summary" } } }
/// Micrometer exports a timer or distribution summary's maximum as its own `_max` gauge family.
#[derive(Clone, Copy)]
enum Part { Whole, Maximum }
struct Family<'a> { name: String, kind: Kind, members: Vec<(&'a CatalogMetric, Part)> }

fn families(metrics: &[CatalogMetric]) -> Vec<Family<'_>> {
    let mut families = Families::default();
    for metric in metrics {
        let name = base_name(metric);
        match metric {
            CatalogMetric::Gauge { .. } => families.add(name, Kind::Gauge, metric, Part::Whole),
            CatalogMetric::Counter { .. } => families.add(format!("{}_total", name.strip_suffix("_total").unwrap_or(&name)), Kind::Counter, metric, Part::Whole),
            CatalogMetric::Timer { .. } => {
                families.add(format!("{name}_seconds"), Kind::Summary, metric, Part::Whole);
                families.add(format!("{name}_seconds_max"), Kind::Gauge, metric, Part::Maximum);
            }
            CatalogMetric::DistributionSummary { .. } => {
                let maximum = format!("{name}_max");
                families.add(name, Kind::Summary, metric, Part::Whole);
                families.add(maximum, Kind::Gauge, metric, Part::Maximum);
            }
        }
    }
    families.families
}

/// Families in the order of their first series.
#[derive(Default)]
struct Families<'a> { families: Vec<Family<'a>>, index: std::collections::BTreeMap<(String, &'static str), usize> }
impl<'a> Families<'a> {
    fn add(&mut self, name: String, kind: Kind, metric: &'a CatalogMetric, part: Part) {
        match self.index.get(&(name.clone(), kind.as_str())) {
            Some(&position) => self.families[position].members.push((metric, part)),
            None => {
                self.index.insert((name.clone(), kind.as_str()), self.families.len());
                self.families.push(Family { name, kind, members: vec![(metric, part)] });
            }
        }
    }
}

fn base_name(metric: &CatalogMetric) -> String {
    let mut name = metric.name().chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == ':' { c } else { '_' }).collect::<String>();
    if let CatalogMetric::Gauge { unit: Some(unit), .. } | CatalogMetric::DistributionSummary { unit: Some(unit), .. } = metric {
        let suffix = match unit { rtdb_plan::CatalogMetricUnit::Bytes => "_bytes", rtdb_plan::CatalogMetricUnit::Seconds => "_seconds" };
        if !name.ends_with(suffix) { name.push_str(suffix); }
    }
    name
}

fn write_series(output: &mut ReservedText, family: &str, metric: &CatalogMetric, part: Part) -> std::fmt::Result {
    let tags = labels(metric.tags(), None);
    match (metric, part) {
        (CatalogMetric::Gauge { value, .. }, _) => writeln!(output, "{family}{tags} {}", number(*value)),
        (CatalogMetric::Counter { count, .. }, _) => writeln!(output, "{family}{tags} {}", number(*count)),
        (CatalogMetric::Timer { max_ns, .. }, Part::Maximum) => writeln!(output, "{family}{tags} {}", max_ns.unwrap_or(0) as f64 / 1e9),
        (CatalogMetric::Timer { count, total_ns, percentiles_ns, .. }, Part::Whole) => {
            for (quantile, value) in ["0.75", "0.85", "0.95", "0.98", "0.99", "0.999"].into_iter().zip(percentiles_ns) {
                writeln!(output, "{family}{} {}", labels(metric.tags(), Some(("quantile", quantile))), *value / 1e9)?;
            }
            writeln!(output, "{family}_count{tags} {count}")?;
            writeln!(output, "{family}_sum{tags} {}", *total_ns / 1e9)
        }
        (CatalogMetric::DistributionSummary { maximum, .. }, Part::Maximum) => writeln!(output, "{family}{tags} {}", number(*maximum)),
        (CatalogMetric::DistributionSummary { count, total, percentiles, .. }, Part::Whole) => {
            for (quantile, value) in ["0.5", "0.95", "0.99"].into_iter().zip(percentiles) {
                writeln!(output, "{family}{} {}", labels(metric.tags(), Some(("quantile", quantile))), number(*value))?;
            }
            writeln!(output, "{family}_count{tags} {count}")?;
            writeln!(output, "{family}_sum{tags} {}", number(*total))
        }
    }
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
