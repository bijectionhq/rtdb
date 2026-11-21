use std::{collections::BTreeMap, num::NonZeroU64, time::Duration};

use futures::{StreamExt, stream};
use rtdb_directory::{Directory, MemberId, MemberRecord, now_ms};
use rtdb_types::Result;
use serde::{Deserialize, Serialize};

use crate::{admin::move_from_holding, control::{self, ControlKey, Request, Response}};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct LoadVector {
    pub tail_bytes: u64,
    pub cache_bytes: u64,
}

impl LoadVector {
    fn plus(self, other: Self) -> Self {
        Self { tail_bytes: self.tail_bytes.saturating_add(other.tail_bytes),
            cache_bytes: self.cache_bytes.saturating_add(other.cache_bytes) }
    }
    fn minus(self, other: Self) -> Self {
        Self { tail_bytes: self.tail_bytes.saturating_sub(other.tail_bytes),
            cache_bytes: self.cache_bytes.saturating_sub(other.cache_bytes) }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct HoldingLoad {
    pub name: String,
    pub deployment: String,
    pub epoch: u64,
    pub load: LoadVector,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LoadLimits {
    pub tail_bytes: Option<NonZeroU64>,
    pub cache_bytes: Option<NonZeroU64>,
}

impl LoadLimits {
    pub(crate) fn enabled(self) -> bool { self.tail_bytes.is_some() || self.cache_bytes.is_some() }

    fn utilization(self, load: LoadVector) -> (f64, f64) {
        (self.tail_bytes.map_or(0.0, |limit| load.tail_bytes as f64 / limit.get() as f64),
         self.cache_bytes.map_or(0.0, |limit| load.cache_bytes as f64 / limit.get() as f64))
    }

    fn pressure(self, load: LoadVector) -> f64 {
        let (tail, cache) = self.utilization(load);
        tail.max(cache)
    }

    fn excess(self, load: LoadVector) -> f64 {
        let (tail, cache) = self.utilization(load);
        (tail - 1.0).max(0.0) + (cache - 1.0).max(0.0)
    }

    fn largest_dimension_is_tail(self, load: LoadVector) -> bool {
        let (tail, cache) = self.utilization(load);
        tail >= cache
    }
}

struct NodeLoad {
    total: LoadVector,
    holdings: Vec<HoldingLoad>,
}

async fn probes(members: &BTreeMap<MemberId, MemberRecord>, key: &ControlKey) -> BTreeMap<MemberId, NodeLoad> {
    let candidates = members.values().filter(|member| member.may_receive_placements(now_ms()))
        .filter_map(|member| member.control_address.as_ref().map(|address| (member.id.clone(), address.as_str())));
    stream::iter(candidates).map(|(member, address)| async move {
        let Response::HoldingLoads { holdings } = control::call(address, &Request::HoldingLoads, Duration::from_secs(2), key).await.ok()? else { return None };
        let total = holdings.iter().fold(LoadVector::default(), |total, holding| total.plus(holding.load));
        Some((member, NodeLoad { total, holdings }))
    }).buffer_unordered(16).filter_map(std::future::ready).collect().await
}

/// Moves a bounded number of placements away from configured resource watermarks.
pub async fn rebalance(directory: &dyn Directory, moves: usize, limits: LoadLimits, key: &ControlKey) -> Result<usize> {
    if !limits.enabled() || moves == 0 { return Ok(0); }
    let members: BTreeMap<_, _> = directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect();
    let mut observed = probes(&members, key).await;
    let mut sources: Vec<_> = observed.keys().cloned().collect();
    sources.sort_by(|a, b| limits.pressure(observed[b].total).total_cmp(&limits.pressure(observed[a].total)));
    let mut moved = 0;
    for source in sources {
        let pressure = limits.pressure(observed[&source].total);
        if moved == moves || pressure <= 1.0 { continue; }
        let node = observed.get_mut(&source).expect("the source was sampled");
        let mut holdings = std::mem::take(&mut node.holdings);
        let tail = limits.largest_dimension_is_tail(node.total);
        holdings.sort_by_key(|holding| std::cmp::Reverse(if tail { holding.load.tail_bytes } else { holding.load.cache_bytes }));

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
