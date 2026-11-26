mod azure;
#[cfg(feature = "cloudwatch")]
mod cloudwatch;
mod points;

use std::{sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{CatalogMetric, CatalogMetrics};
use rtdb_types::{Anomaly, Result};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use crate::config::ModuleSettings;

const STEP: i64 = 60_000;

#[async_trait::async_trait]
trait MetricPublisher: Send + Sync {
    async fn publish(&self, metrics: &[CatalogMetric], timestamp: i64) -> Result<()>;
}
struct Publisher { value: Arc<dyn MetricPublisher>, next: Instant, busy: bool, last_published_step: i64 }
type Publications = FuturesUnordered<BoxFuture<'static, (usize, Result<()>)>>;

#[derive(Clone)]
pub struct MetricExportHandle { stop: CancellationToken }
impl MetricExportHandle {
    /// Requests the bounded shutdown drain; its owner must still await `MetricsExporter::run`.
    pub fn close(&self) { self.stop.cancel(); }
}

pub struct MetricsExporter {
    handle: MetricExportHandle, metrics: Arc<dyn CatalogMetrics>, publishers: Vec<Publisher>,
    _reservation: Arc<MemoryReservation>,
}
impl MetricsExporter {
    /// Initializes configured clients without starting a task; the server owns and joins `run`.
    pub async fn open(modules: &[ModuleSettings], metrics: Arc<dyn CatalogMetrics>, memory: Arc<MemoryBudget>) -> Result<Option<(MetricExportHandle, Self)>> {
        if modules.is_empty() { return Ok(None); }
        let reservation = memory.reserve(modules.len().checked_mul(4096).and_then(|size| size.checked_add(4096)).ok_or_else(|| invalid("Metric publisher allocation size overflow"))?)?;
        let mut publishers = Vec::with_capacity(modules.len());
        for module in modules {
            let value: Arc<dyn MetricPublisher> = match module {
                ModuleSettings::AzureMonitor { connection_string } => Arc::new(azure::Azure::open(connection_string.expose(), Arc::clone(&memory))?),
                ModuleSettings::CloudWatch { namespace } => {
                    #[cfg(feature = "cloudwatch")]
                    { Arc::new(cloudwatch::CloudWatch::open(namespace, Arc::clone(&memory)).await?) }
                    #[cfg(not(feature = "cloudwatch"))]
                    { let _ = namespace; return Err(Anomaly::unsupported("rtdb.metrics/cloudwatch-not-compiled", "CloudWatch metrics require the cloudwatch server feature")); }
                }
            };
            let now = wall_time_millis();
            let random = uuid::Uuid::new_v4().as_u128() as u64;
            let offset = (random % 48_000).saturating_sub(2) as i64;
            let delay = STEP - now.rem_euclid(STEP) + 2 + offset;
            publishers.push(Publisher { value, next: Instant::now() + Duration::from_millis(delay as u64), busy: false, last_published_step: 0 });
        }
        let handle = MetricExportHandle { stop: CancellationToken::new() };
        Ok(Some((handle.clone(), Self { handle, metrics, publishers, _reservation: reservation })))
    }

    /// Drives rollover and publication until the handle closes, then joins sends and exports the final partial step.
    pub async fn run(mut self) {
        let now = wall_time_millis();
        let mut roll = tokio::time::interval_at(Instant::now() + Duration::from_millis((STEP - now.rem_euclid(STEP) + 1) as u64), Duration::from_millis(STEP as u64));
        roll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut pending = Publications::new();
        let mut last_polled_step = None;
        loop {
            let next = self.publishers.iter().map(|publisher| publisher.next).min().expect("configured publishers");
            tokio::select! {
                biased;
                _ = self.handle.stop.cancelled() => break,
                _ = roll.tick() => { let now = wall_time_millis(); self.metrics.roll_step(now, false); last_polled_step = Some(now / STEP); }
                Some((index, result)) = pending.next(), if !pending.is_empty() => { self.publishers[index].busy = false; report(result); }
                _ = tokio::time::sleep_until(next) => {
                    let now = Instant::now();
                    for (index, publisher) in self.publishers.iter_mut().enumerate() {
                        if publisher.next > now { continue; }
                        while publisher.next <= now { publisher.next += Duration::from_millis(STEP as u64); }
                        if !publisher.busy { publisher.busy = true; publisher.last_published_step = wall_time_millis() / STEP; pending.push(publish(index, Arc::clone(&publisher.value), Arc::clone(&self.metrics))); }
                    }
                }
            }
        }
        if tokio::time::timeout(Duration::from_secs(30), self.close_exports(&mut pending, last_polled_step)).await.is_err() { ::tracing::warn!("Metric export drain timed out"); }
    }
    async fn close_exports(&mut self, pending: &mut Publications, last_polled_step: Option<i64>) {
        let flush = self.publishers.iter().enumerate().filter_map(|(index, publisher)| (!publisher.busy && last_polled_step.is_some_and(|step| step > publisher.last_published_step)).then_some(index)).collect::<Vec<_>>();
        while let Some((index, result)) = pending.next().await { self.publishers[index].busy = false; report(result); }
        for index in flush { pending.push(publish(index, Arc::clone(&self.publishers[index].value), Arc::clone(&self.metrics))); }
        while let Some((_, result)) = pending.next().await { report(result); }
        self.metrics.roll_step(wall_time_millis(), true);
        for (index, publisher) in self.publishers.iter().enumerate() { pending.push(publish(index, Arc::clone(&publisher.value), Arc::clone(&self.metrics))); }
        while let Some((_, result)) = pending.next().await { report(result); }
    }
}
impl Drop for MetricsExporter { fn drop(&mut self) { self.handle.close(); } }

fn publish(index: usize, publisher: Arc<dyn MetricPublisher>, metrics: Arc<dyn CatalogMetrics>) -> BoxFuture<'static, (usize, Result<()>)> {
    async move {
        let work = async { let rows = metrics.sample_step().await?; publisher.publish(rows.rows(), wall_time_millis()).await };
        let result = tokio::time::timeout(Duration::from_secs(60), work).await.unwrap_or_else(|_| Err(invalid("Metric publication deadline expired")));
        (index, result)
    }.boxed()
}
fn wall_time_millis() -> i64 { SystemTime::now().duration_since(UNIX_EPOCH).map(|elapsed| elapsed.as_millis().min(i64::MAX as u128) as i64).unwrap_or(0) }
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.metrics/export", message) }
fn report(result: Result<()>) { if let Err(error) = result { ::tracing::warn!("Metric export failed: {error}"); } }

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_plan::{CatalogRows, ExecutionError, ExecutionObserver};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Sink { samples: std::sync::Mutex<Vec<f64>> }
    #[async_trait::async_trait]
    impl MetricPublisher for Sink {
        async fn publish(&self, metrics: &[CatalogMetric], _: i64) -> Result<()> {
            for metric in metrics { if let CatalogMetric::Counter { name, count, .. } = metric { if name == "tx.error" { self.samples.lock().unwrap().push(*count); } } }
            Ok(())
        }
    }
    #[cfg(not(feature = "cloudwatch"))]
    #[tokio::test]
    async fn configured_cloudwatch_requires_its_compiled_backend() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = rtdb_metrics::Registry::new("fixture", Arc::clone(&memory)).unwrap();
        let result = MetricsExporter::open(&[ModuleSettings::CloudWatch { namespace: "fixture".into() }], registry, memory).await;
        assert!(matches!(result, Err(error) if error.code == "rtdb.metrics/cloudwatch-not-compiled"));
    }
    #[tokio::test]
    async fn step_meter_registry_close_publishes_partial_step_to_each_destination_once() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = rtdb_metrics::Registry::new("fixture", Arc::clone(&memory)).unwrap();
        registry.record_error(ExecutionError::Transaction);
        let sink = Arc::new(Sink { samples: std::sync::Mutex::new(Vec::new()) });
        let handle = MetricExportHandle { stop: CancellationToken::new() };
        let exporter = MetricsExporter { handle: handle.clone(), metrics: registry.clone(), publishers: vec![Publisher { value: sink.clone(), next: Instant::now() + Duration::from_secs(60), busy: false, last_published_step: i64::MAX }], _reservation: memory.reserve(4096).unwrap() };
        handle.close(); exporter.run().await;
        assert_eq!(*sink.samples.lock().unwrap(), vec![1.0]);
        assert!(registry.metrics().unwrap().rows().iter().any(|row| matches!(row, CatalogMetric::Counter { name, count, .. } if name == "tx.error" && *count == 1.0)));
    }
    struct StepFixture { step: AtomicUsize, memory: Arc<MemoryBudget> }
    #[async_trait::async_trait]
    impl CatalogMetrics for StepFixture {
        fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { let reservation = self.memory.reserve(1024)?; Ok(CatalogRows::new(vec![CatalogMetric::Counter { name: "tx.error".into(), tags: Default::default(), count: self.step.load(Ordering::SeqCst) as f64 }], reservation)) }
        async fn sample_step(&self) -> Result<CatalogRows<CatalogMetric>> { tokio::task::yield_now().await; self.metrics() }
        fn roll_step(&self, _: i64, closing: bool) { if closing { self.step.store(2, Ordering::SeqCst); } }
    }
    #[tokio::test]
    async fn step_meter_registry_close_waits_for_inflight_sampling_across_rollover_without_republishing_that_step() {
        let memory = MemoryBudget::new(1 << 20);
        let metrics: Arc<dyn CatalogMetrics> = Arc::new(StepFixture { step: AtomicUsize::new(1), memory: Arc::clone(&memory) });
        let sink = Arc::new(Sink { samples: std::sync::Mutex::new(Vec::new()) });
        let handle = MetricExportHandle { stop: CancellationToken::new() };
        let mut exporter = MetricsExporter { handle, metrics: Arc::clone(&metrics), publishers: vec![Publisher { value: sink.clone(), next: Instant::now(), busy: true, last_published_step: 0 }], _reservation: memory.reserve(4096).unwrap() };
        let mut pending = Publications::new(); pending.push(publish(0, sink.clone(), metrics));
        exporter.close_exports(&mut pending, Some(1)).await;
        assert_eq!(*sink.samples.lock().unwrap(), vec![1.0, 2.0]);
    }
    struct Pending { memory: Arc<MemoryBudget>, drops: Arc<AtomicUsize> }
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard { fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); } }
    #[async_trait::async_trait]
    impl CatalogMetrics for Pending {
        fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { Ok(CatalogRows::new(Vec::new(), self.memory.reserve(0)?)) }
        async fn sample_step(&self) -> Result<CatalogRows<CatalogMetric>> { let _guard = Guard(Arc::clone(&self.drops)); let _reservation = self.memory.reserve(1024)?; std::future::pending().await }
    }
    #[tokio::test]
    async fn dropping_publication_cancels_provider_sampling_and_releases_its_memory() {
        let memory = MemoryBudget::new(1 << 20); let drops = Arc::new(AtomicUsize::new(0));
        let mut future = publish(0, Arc::new(Sink { samples: std::sync::Mutex::new(Vec::new()) }), Arc::new(Pending { memory: Arc::clone(&memory), drops: Arc::clone(&drops) }));
        tokio::select! { biased; _ = &mut future => panic!("sampling unexpectedly completed"), _ = std::future::ready(()) => {} }
        assert_eq!(memory.used_bytes(), 1024); drop(future); assert_eq!(memory.used_bytes(), 0); assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
