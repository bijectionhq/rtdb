use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use futures::StreamExt;
use rtdb_directory::{Cas, Directory, Lifecycle, MemberId, MemberPool, MemberState, now_ms};
use rtdb_types::Result;
use serde::{Deserialize, Serialize};

use crate::control::{self, ControlKey, Request, Response};

const QUIET_SAMPLES: u8 = 3;
const ADJUSTMENTS_PER_PASS: usize = 8;

#[derive(Clone, Copy, Debug)]
pub struct ReadWidthOptions {
    pub sessions_per_node: NonZeroU64,
    pub max_width: NonZeroU32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadLoad {
    pub deployment: String,
    pub sessions: u64,
    pub placement: ReadPlacement,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadPlacement {
    Writer { epoch: u64, width: u32 },
    Replica,
}

struct Writer {
    member: MemberId,
    epoch: u64,
    width: u32,
}

#[derive(Default)]
struct Observed {
    sessions: u64,
    writer: Option<Writer>,
}

#[derive(PartialEq, Eq)]
struct Quiet {
    epoch: u64,
    width: u32,
    samples: u8,
}

#[derive(Default)]
pub(crate) struct ReadWidthScaler {
    quiet: BTreeMap<String, Quiet>,
    last_adjusted: Option<String>,
}

fn demand_width(sessions: u64, per_node: NonZeroU64, max_width: NonZeroU32) -> u32 {
    let per_node = per_node.get();
    if sessions <= per_node {
        1
    } else {
        sessions.div_ceil(per_node).saturating_add(1).min(u64::from(max_width.get())) as u32
    }
}

impl ReadWidthScaler {
    pub(crate) async fn adjust(&mut self, directory: &dyn Directory, options: ReadWidthOptions, key: &ControlKey, timeout: Duration) -> Result<()> {
        let mut previous = std::mem::take(&mut self.quiet);
        let now = now_ms();
        let members: BTreeMap<_, _> = directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect();
        let readers: Vec<_> = members.values().filter(|member| member.may_hold(now) && member.pool != MemberPool::Compaction)
            .map(|member| (member.id.clone(), member.control_address.clone())).collect();
        let probes: Vec<_> = futures::stream::iter(readers.into_iter().map(|(member, address)| async move {
            let loads = match address {
                Some(address) => match control::call(&address, &Request::ReadLoads, timeout, key).await {
                    Ok(Response::ReadLoads { loads }) => Some(loads),
                    _ => None,
                },
                None => None,
            };
            (member, loads)
        })).buffer_unordered(16).collect().await;

        let mut observations = BTreeMap::<String, Observed>::new();
        let mut sampled = BTreeSet::new();
        let mut incomplete = HashSet::new();
        for (member, loads) in probes {
            let Some(loads) = loads else {
                incomplete.insert(members[&member].pool);
                continue;
            };
            sampled.insert(member.clone());
            for load in loads {
                let observed = observations.entry(load.deployment).or_default();
                observed.sessions = observed.sessions.saturating_add(load.sessions);
                if let ReadPlacement::Writer { epoch, width } = load.placement
                    && observed.writer.as_ref().is_none_or(|writer| writer.epoch < epoch)
                {
                    observed.writer = Some(Writer { member: member.clone(), epoch, width });
                }
            }
        }

        let mut observations: Vec<_> = observations.into_iter().collect();
        if let Some(last) = &self.last_adjusted {
            let start = observations.partition_point(|(deployment, _)| deployment <= last);
            observations.rotate_left(start);
        }
        let mut adjusted = 0;
        for (deployment, observed) in observations {
            let Some(writer) = observed.writer else { continue };
            if members[&writer.member].state != MemberState::Eligible { continue; }
            let demand = demand_width(observed.sessions, options.sessions_per_node, options.max_width);
            let quiet_demand = demand_width(observed.sessions.saturating_mul(2), options.sessions_per_node, options.max_width);
            let next = if demand > writer.width {
                writer.width.saturating_add(1)
            } else if quiet_demand < writer.width && !incomplete.contains(&members[&writer.member].pool) {
                let samples = previous.remove(&deployment)
                    .filter(|old| old.epoch == writer.epoch && old.width == writer.width)
                    .map_or(1, |old| old.samples.saturating_add(1));
                self.quiet.insert(deployment.clone(), Quiet { epoch: writer.epoch, width: writer.width, samples });
                if samples < QUIET_SAMPLES { continue; }
                writer.width - 1
            } else {
                continue;
            };
            if adjusted == ADJUSTMENTS_PER_PASS { continue; }

            let Some(mut record) = directory.deployment(&deployment).await? else { continue };
            let now = now_ms();
            if record.lifecycle != Lifecycle::Live || record.is_resident_idle()
                || record.epoch != writer.epoch || record.holder.as_ref() != Some(&writer.member)
                || record.read_width != writer.width || !record.may_place_on(&members[&writer.member], now)
                || record.move_from.is_some() || record.active_move(now).is_some()
            {
                continue;
            }
            if next > writer.width {
                let ranks = crate::deployment::ranked_read_replicas(&record, &members, now);
                if ranks.get((next - 2) as usize).is_none_or(|member| !sampled.contains(&member.id)) { continue; }
            }
            let expected = record.version;
            record.read_width = next;
            adjusted += 1;
            self.last_adjusted = Some(deployment.clone());
            if let Cas::Applied(_) = directory.replace_deployment(record, expected).await? {
                self.quiet.remove(&deployment);
            }
        }
        Ok(())
    }
}
