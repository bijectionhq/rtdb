use std::{sync::Arc, time::Duration};
use aws_sdk_cloudwatch::{Client, primitives::DateTime, types::{Dimension, MetricDatum, StandardUnit}};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::CatalogMetric;
use rtdb_types::{Anomaly, Result};
use super::{MetricPublisher, points::{Destination, Point, Points, Unit}};

pub(super) struct CloudWatch { client: Client, namespace: String, memory: Arc<MemoryBudget>, _reservation: Arc<MemoryReservation> }
impl CloudWatch {
    pub async fn open(namespace: &str, memory: Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(namespace.len().checked_mul(8).and_then(|size| size.checked_add(65_536)).ok_or_else(|| invalid("CloudWatch namespace allocation size overflow"))?)?;
        let configuration = tokio::time::timeout(Duration::from_secs(10), aws_config::load_defaults(aws_config::BehaviorVersion::latest())).await.map_err(|_| invalid("CloudWatch AWS configuration deadline expired"))?;
        Ok(Self { client: Client::new(&configuration), namespace: namespace.to_owned(), memory, _reservation: reservation })
    }
}
#[async_trait::async_trait]
impl MetricPublisher for CloudWatch {
    async fn publish(&self, metrics: &[CatalogMetric], timestamp: i64) -> Result<()> {
        let points = Points::new(metrics, Destination::CloudWatch, &self.memory)?;
        let mut failure = None;
        let mut remaining = points.values.as_slice();
        while !remaining.is_empty() {
            let count = batch_size(remaining, &self.namespace)?;
            let (points, rest) = remaining.split_at(count); remaining = rest;
            let size = points.iter().try_fold(65_536usize, |size, point| {
                let strings = point.tags.iter().try_fold(point.name.len() + self.namespace.len() + 2048, |size, (key, value)| size.checked_add(key.len()).and_then(|size| size.checked_add(value.len())).and_then(|size| size.checked_add(512)).ok_or_else(|| invalid("CloudWatch metric allocation size overflow")))?;
                size.checked_add(strings.checked_mul(32).ok_or_else(|| invalid("CloudWatch request allocation size overflow"))?).ok_or_else(|| invalid("CloudWatch request allocation size overflow"))
            })?;
            let _reservation = self.memory.reserve(size)?;
            let data = points.iter().map(|point| MetricDatum::builder().metric_name(&point.name).value(point.value).timestamp(DateTime::from_millis(timestamp)).storage_resolution(60)
                .unit(match point.unit { Unit::None => StandardUnit::None, Unit::Count => StandardUnit::Count, Unit::Milliseconds => StandardUnit::Milliseconds, Unit::Bytes => StandardUnit::Bytes, Unit::Seconds => StandardUnit::Seconds })
                .set_dimensions(Some(point.tags.iter().map(|(key, value)| Dimension::builder().name(key).value(value).build()).collect())).build()).collect();
            let request = self.client.put_metric_data().namespace(&self.namespace).set_metric_data(Some(data)).send();
            let result = match tokio::time::timeout(Duration::from_secs(10), request).await { Ok(result) => result.map_err(|error| invalid(format!("CloudWatch PutMetricData failed: {error}"))), Err(_) => Err(invalid("CloudWatch request deadline expired")) };
            if let Err(error) = result { if failure.is_none() { failure = Some(error); } }
        }
        failure.map_or(Ok(()), Err)
    }
}
fn batch_size(points: &[Point], namespace: &str) -> Result<usize> {
    const LIMIT: usize = 1 << 20;
    let mut size = namespace.len().checked_mul(6).and_then(|size| size.checked_add(4096)).ok_or_else(|| invalid("CloudWatch namespace size overflow"))?;
    let mut count = 0;
    for point in points.iter().take(1000) {
        let mut bound = point.name.len().checked_mul(6).and_then(|size| size.checked_add(2048)).ok_or_else(|| invalid("CloudWatch metric size overflow"))?;
        for (key, value) in &point.tags { bound = key.len().checked_add(value.len()).and_then(|size| size.checked_mul(6)).and_then(|size| size.checked_add(512)).and_then(|size| size.checked_add(bound)).ok_or_else(|| invalid("CloudWatch dimension size overflow"))?; }
        if size.checked_add(bound).is_none_or(|size| size > LIMIT) { break; }
        size += bound; count += 1;
    }
    if count == 0 { return Err(invalid("A CloudWatch metric exceeds the request byte limit")); }
    Ok(count)
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.metrics/cloudwatch-export", message) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_partitioning_limits_encoded_dimension_bytes_before_the_thousand_point_cap() {
        let points = (0..100).map(|_| Point { name: "fixture".into(), tags: (0..30).map(|index| (format!("dimension{index}"), "\u{1}".repeat(1024))).collect(), value: 1.0, unit: Unit::None, aggregate: None }).collect::<Vec<_>>();
        let count = batch_size(&points, "fixture").unwrap(); assert!(count > 0 && count < 100);
        let oversized = Point { name: "x".repeat(1 << 20), tags: Default::default(), value: 1.0, unit: Unit::None, aggregate: None };
        assert!(batch_size(&[oversized], "fixture").is_err());
    }
}
