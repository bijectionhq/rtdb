use std::{collections::BTreeMap, num::NonZeroU64, time::{Duration, Instant}};

use futures::{StreamExt, stream};
use rtdb_directory::{Directory, MemberId, MemberRecord, now_ms};
use rtdb_types::Result;
use serde::{Deserialize, Serialize};

use crate::{admin::move_from_holding, control::{self, ControlKey, Request, Response}};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct LoadVector {
    #[serde(default)]
    pub write_bytes_per_second: u64,
    #[serde(default)]
    pub query_cpu_millis_per_second: u64,
    pub tail_bytes: u64,
    pub cache_bytes: u64,
}

impl LoadVector {
    fn dimensions(self) -> [u64; 4] {
        [self.write_bytes_per_second, self.query_cpu_millis_per_second, self.tail_bytes, self.cache_bytes]
    }

    fn plus(self, other: Self) -> Self {
        Self { write_bytes_per_second: self.write_bytes_per_second.saturating_add(other.write_bytes_per_second),
            query_cpu_millis_per_second: self.query_cpu_millis_per_second.saturating_add(other.query_cpu_millis_per_second),
            tail_bytes: self.tail_bytes.saturating_add(other.tail_bytes),
            cache_bytes: self.cache_bytes.saturating_add(other.cache_bytes) }
    }
    fn minus(self, other: Self) -> Self {
        Self { write_bytes_per_second: self.write_bytes_per_second.saturating_sub(other.write_bytes_per_second),
            query_cpu_millis_per_second: self.query_cpu_millis_per_second.saturating_sub(other.query_cpu_millis_per_second),
            tail_bytes: self.tail_bytes.saturating_sub(other.tail_bytes),
            cache_bytes: self.cache_bytes.saturating_sub(other.cache_bytes) }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct HoldingLoad {
    pub name: String,
    pub deployment: String,
    pub epoch: u64,
    pub load: LoadVector,
    #[serde(default)]
    pub submitted_bytes: Option<u64>,
    #[serde(default)]
    pub query_cpu_nanos: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LoadLimits {
    pub write_bytes_per_second: Option<NonZeroU64>,
    pub query_cpu_millis_per_second: Option<NonZeroU64>,
    pub tail_bytes: Option<NonZeroU64>,
    pub cache_bytes: Option<NonZeroU64>,
}

impl LoadLimits {
    fn dimensions(self) -> [Option<NonZeroU64>; 4] {
        [self.write_bytes_per_second, self.query_cpu_millis_per_second, self.tail_bytes, self.cache_bytes]
    }

    pub fn enabled(self) -> bool { self.dimensions().into_iter().any(|limit| limit.is_some()) }

    fn utilization(self, load: LoadVector) -> [f64; 4] {
        let limits = self.dimensions();
        let amounts = load.dimensions();
        std::array::from_fn(|index| limits[index].map_or(0.0, |limit| amounts[index] as f64 / limit.get() as f64))
    }

    fn pressure(self, load: LoadVector) -> f64 {
        self.utilization(load).into_iter().fold(0.0, f64::max)
    }

    fn excess(self, load: LoadVector) -> f64 {
        self.utilization(load).into_iter().map(|dimension| (dimension - 1.0).max(0.0)).sum()
    }

    fn largest_dimension(self, load: LoadVector) -> usize {
        self.utilization(load).into_iter().enumerate()
            .max_by(|(_, left), (_, right)| left.total_cmp(right)).expect("load has dimensions").0
    }
}

#[derive(Default)]
pub struct LoadSamples {
    holdings: BTreeMap<(MemberId, String, u64), Sample>,
}

struct Sample {
    observed_at: Instant,
    submitted_bytes: Option<u64>,
    query_cpu_nanos: Option<u64>,
}

impl LoadSamples {
    fn observe(&mut self, member: &MemberId, node: &mut NodeLoad, previous: &Self, limits: LoadLimits) -> bool {
        let mut complete = true;
        for holding in &mut node.holdings {
            let id = (member.clone(), holding.deployment.clone(), holding.epoch);
            let sample = Sample { observed_at: node.observed_at, submitted_bytes: holding.submitted_bytes, query_cpu_nanos: holding.query_cpu_nanos };
            let last = previous.holdings.get(&id);
            let write = last.and_then(|last| rate(sample.submitted_bytes, last.submitted_bytes, sample.observed_at, last.observed_at));
            let cpu = last.and_then(|last| rate(sample.query_cpu_nanos, last.query_cpu_nanos, sample.observed_at, last.observed_at));
            holding.load.write_bytes_per_second = write.unwrap_or(0);
            holding.load.query_cpu_millis_per_second = cpu.unwrap_or(0) / 1_000_000;
            complete &= (limits.write_bytes_per_second.is_none() || write.is_some())
                && (limits.query_cpu_millis_per_second.is_none() || cpu.is_some());
            self.holdings.insert(id, sample);
        }
        complete
    }
}

fn rate(amount: Option<u64>, prior: Option<u64>, at: Instant, previous: Instant) -> Option<u64> {
    let delta = amount?.checked_sub(prior?)?;
    let nanos = at.saturating_duration_since(previous).as_nanos();
    (nanos > 0).then(|| (u128::from(delta) * 1_000_000_000 / nanos).min(u128::from(u64::MAX)) as u64)
}

struct NodeLoad {
    observed_at: Instant,
    total: LoadVector,
    holdings: Vec<HoldingLoad>,
}

async fn probes(members: &BTreeMap<MemberId, MemberRecord>, key: &ControlKey, measure_query_cpu: bool) -> BTreeMap<MemberId, NodeLoad> {
    let candidates = members.values().filter(|member| member.may_receive_placements(now_ms()))
        .filter_map(|member| member.control_address.as_ref().map(|address| (member.id.clone(), address.as_str())));
    stream::iter(candidates).map(|(member, address)| async move {
        let Response::HoldingLoads { holdings } = control::call(address, &Request::HoldingLoads { measure_query_cpu }, Duration::from_secs(2), key).await.ok()? else { return None };
        Some((member, NodeLoad { observed_at: Instant::now(), total: LoadVector::default(), holdings }))
    }).buffer_unordered(16).filter_map(std::future::ready).collect().await
}

/// Moves a bounded number of placements away from configured resource watermarks.
pub async fn rebalance(directory: &dyn Directory, moves: usize, limits: LoadLimits, key: &ControlKey, samples: &mut LoadSamples) -> Result<usize> {
    if !limits.enabled() || moves == 0 { return Ok(0); }
    let members: BTreeMap<_, _> = directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect();
    let measure_query_cpu = limits.query_cpu_millis_per_second.is_some();
    let use_rates = limits.write_bytes_per_second.is_some() || measure_query_cpu;
    let mut observed = probes(&members, key, measure_query_cpu).await;
    let previous = std::mem::take(samples);
    observed.retain(|member, node| {
        if use_rates && !samples.observe(member, node, &previous, limits) { return false; }
        node.total = node.holdings.iter().fold(LoadVector::default(), |total, holding| total.plus(holding.load));
        true
    });
    let mut sources: Vec<_> = observed.keys().cloned().collect();
    sources.sort_by(|a, b| limits.pressure(observed[b].total).total_cmp(&limits.pressure(observed[a].total)));
    let mut moved = 0;
    for source in sources {
        let pressure = limits.pressure(observed[&source].total);
        if moved == moves || pressure <= 1.0 { continue; }
        let node = observed.get_mut(&source).expect("the source was sampled");
        let mut holdings = std::mem::take(&mut node.holdings);
        let dimension = limits.largest_dimension(node.total);
        holdings.sort_by_key(|holding| std::cmp::Reverse(holding.load.dimensions()[dimension]));

        for holding in holdings {
            let previous = observed[&source].total;
            if moved == moves || limits.pressure(previous) <= 1.0 { break; }
            if limits.excess(previous.minus(holding.load)) >= limits.excess(previous) { continue; }
            let Some(record) = directory.deployment(&holding.deployment).await? else { continue; };
            if record.epoch != holding.epoch || record.name != holding.name || record.holder.as_ref() != Some(&source) || record.is_resident_idle() { continue; }
            let now = now_ms();
            let candidates: Vec<_> = observed.iter().filter_map(|(target, profile)| {
                let member = &members[target];
                (target != &source && record.may_place_on(member, now)
                    && record.storage_namespaces.is_none_or(|namespaces| member.storage_namespaces == Some(namespaces))
                    && limits.pressure(profile.total.plus(holding.load)) <= 1.0).then_some(member)
            }).collect();
            let ranked = rtdb_affinity::rank(&record.storage_identity(), &candidates, |member| member.id.as_str());
            let Some(target) = ranked.first() else { continue; };
            match move_from_holding(directory, &holding.name, &target.id, Some((&source, holding.epoch)), key).await {
                Ok(()) => {}
                Err(error) if matches!(error.code.as_str(), "rtdb.multi/move-source-changed" | "rtdb.multi/move-in-progress") => return Ok(moved),
                Err(error) => return Err(error),
            }
            observed.get_mut(&source).expect("the source was sampled").total = previous.minus(holding.load);
            let destination = observed.get_mut(&target.id).expect("the target was sampled");
            destination.total = destination.total.plus(holding.load);
            moved += 1;
        }
    }
    Ok(moved)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_at(observed_at: Instant, epoch: u64, submitted_bytes: Option<u64>) -> NodeLoad {
        NodeLoad {
            observed_at,
            total: LoadVector::default(),
            holdings: vec![HoldingLoad {
                name: "orders".into(),
                deployment: "installation".into(),
                epoch,
                load: LoadVector::default(),
                submitted_bytes,
                query_cpu_nanos: submitted_bytes.map(|bytes| bytes * 1_000_000),
            }],
        }
    }

    #[test]
    fn rates_use_only_two_samples_from_the_same_placement() {
        let member = MemberId("node".into());
        let limits = LoadLimits {
            write_bytes_per_second: NonZeroU64::new(1000),
            query_cpu_millis_per_second: NonZeroU64::new(1000),
            ..Default::default()
        };
        let start = Instant::now();
        let mut first = LoadSamples::default();
        let mut next = LoadSamples::default();
        let mut load = load_at(start, 3, Some(1000));
        assert!(!first.observe(&member, &mut load, &LoadSamples::default(), limits));

        load = load_at(start + Duration::from_millis(250), 3, Some(1500));
        assert!(next.observe(&member, &mut load, &first, limits));
        assert_eq!(load.holdings[0].load.write_bytes_per_second, 2000);
        assert_eq!(load.holdings[0].load.query_cpu_millis_per_second, 2000);

        load = load_at(start + Duration::from_secs(1), 4, Some(50));
        assert!(!first.observe(&member, &mut load, &next, limits));
        assert_eq!(load.holdings[0].load.write_bytes_per_second, 0);
        load = load_at(start + Duration::from_secs(2), 4, None);
        assert!(!next.observe(&member, &mut load, &first, limits));
    }
}
