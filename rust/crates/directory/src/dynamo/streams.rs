//! Following the deployments table's stream of new images into a `ChangeFeed`.
//!
//! A stream is ordered per item within a shard's lineage, so a child shard is read only once its parent has been read through.
//! Following starts at the latest position of every open shard.
//! Whole-feed consumers get a rescan on startup and when a position is lost.
//! Keyed watches read their own records when they join, when the follower opens its iterators, and on their safety-net polls.

use std::collections::{BTreeMap, HashMap};
use std::sync::Weak;
use std::time::{Duration, Instant};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodbstreams::Client as StreamsClient;
use aws_sdk_dynamodbstreams::types::{AttributeValue, ShardIteratorType};
use rtdb_types::{Anomaly, Result};

use super::{EPOCH, ID, NAME, RECORD, VERSION, deployment_record, parked, sdk_anomaly};
use crate::{ChangeFeed, DeploymentRecord};

/// How long an idle follower waits before reading its shards again; a shard allows five reads a second.
const POLL: Duration = Duration::from_millis(250);
/// How often the shard listing is refreshed, to find the children of shards that split or closed.
const DISCOVER: Duration = Duration::from_secs(1);
/// How long a follower that cannot reach the stream waits before trying again.
const RETRY: Duration = Duration::from_secs(1);

/// One shard's place in the follower's reading.
enum Shard {
    /// Read from its start once its parent has been read through.
    Unread { parent: Option<String> },
    Reading { iterator: String, resume: Resume },
    /// Closed and read to its end, so its children may be read.
    Finished,
}

/// Where an expired iterator is re-opened.
#[derive(Clone)]
enum Resume { Latest, TrimHorizon, After(String) }

/// The stream being followed and every shard of it the follower knows.
struct Following { arn: String, shards: BTreeMap<String, Shard>, next_discovery: Instant }

/// Why following has to start again from the table.
struct Restart;

pub(super) struct Follower {
    pub(super) client: Client,
    pub(super) streams: StreamsClient,
    pub(super) table: String,
    /// The directory's feed; the follower stops once the directory has been dropped.
    pub(super) changes: Weak<ChangeFeed>,
}

impl Follower {
    pub(super) async fn run(self) {
        let mut following = None;
        loop {
            if self.changes.strong_count() == 0 { return; }
            let pause = match following.take() {
                None => match self.start().await {
                    Ok(started) => { following = Some(started); Duration::ZERO }
                    Err(_) => RETRY,
                },
                Some(mut current) => match self.step(&mut current).await {
                    Ok(read_any) => { following = Some(current); if read_any { Duration::ZERO } else { POLL } }
                    Err(Restart) => RETRY,
                },
            };
            if !pause.is_zero() { tokio::time::sleep(pause).await; }
        }
    }

    fn publish(&self, record: DeploymentRecord) {
        if let Some(changes) = self.changes.upgrade() { changes.publish(record); }
    }

    async fn start(&self) -> Result<Following> {
        let described = self.client.describe_table().table_name(&self.table).send().await.map_err(|error| sdk_anomaly("DescribeTable", error))?;
        let arn = described.table().and_then(|table| table.latest_stream_arn())
            .ok_or_else(|| Anomaly::incorrect("rtdb.directory/no-stream", format!("DynamoDB table {} has no stream", self.table)))?
            .to_owned();
        let mut shards = BTreeMap::new();
        for listed in self.list_shards(&arn).await? {
            let shard = if listed.open {
                Shard::Reading { iterator: self.iterator(&arn, &listed.id, &Resume::Latest).await?, resume: Resume::Latest }
            } else {
                Shard::Finished
            };
            shards.insert(listed.id, shard);
        }
        self.rescan().await?;
        Ok(Following { arn, shards, next_discovery: Instant::now() + DISCOVER })
    }

    async fn rescan(&self) -> Result<()> {
        if let Some(feed) = self.changes.upgrade() { feed.refresh_watches(); }
        let mut pages = self.client.scan().table_name(&self.table).consistent_read(true).into_paginator().send();
        while self.changes.upgrade().is_some_and(|feed| feed.has_broadcast_receivers()) {
            let Some(page) = pages.next().await else { break };
            for item in page.map_err(|error| sdk_anomaly("Scan", error))?.items() {
                if let Ok(Some(record)) = deployment_record(Some(item)) { self.publish(record); }
            }
        }
        Ok(())
    }

    /// One read of every readable shard; whether any of them returned records.
    async fn step(&self, following: &mut Following) -> std::result::Result<bool, Restart> {
        if Instant::now() >= following.next_discovery { self.discover(following).await?; }

        let ready = following.shards.iter()
            .filter_map(|(id, shard)| match shard {
                Shard::Unread { parent } => parent.as_ref().is_none_or(|parent| matches!(following.shards.get(parent), None | Some(Shard::Finished))).then(|| id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        for id in ready {
            let Ok(iterator) = self.iterator(&following.arn, &id, &Resume::TrimHorizon).await else { continue };
            following.shards.insert(id, Shard::Reading { iterator, resume: Resume::TrimHorizon });
        }

        let mut read_any = false;
        let reading = following.shards.iter().filter(|(_, shard)| matches!(shard, Shard::Reading { .. })).map(|(id, _)| id.clone()).collect::<Vec<_>>();
        for id in reading {
            let Some(Shard::Reading { iterator, resume }) = following.shards.get(&id) else { continue };
            let (iterator, mut resume) = (iterator.clone(), resume.clone());
            let next = match self.streams.get_records().shard_iterator(&iterator).limit(1000).send().await {
                Ok(read) => {
                    for record in read.records() {
                        let Some(stream_record) = record.dynamodb() else { continue };
                        if let Some(sequence) = stream_record.sequence_number() { resume = Resume::After(sequence.to_owned()); }
                        read_any = true;
                        if let Some(deployment) = stream_record.new_image().and_then(decode_image) { self.publish(deployment); }
                    }
                    match read.next_shard_iterator() {
                        Some(iterator) => Shard::Reading { iterator: iterator.to_owned(), resume },
                        None => { following.next_discovery = Instant::now(); Shard::Finished }
                    }
                }
                Err(error) => match error.as_service_error() {
                    Some(service) if service.is_expired_iterator_exception() => {
                        let Ok(iterator) = self.iterator(&following.arn, &id, &resume).await else { continue };
                        // Re-opening at the latest position skips whatever was written while the old iterator lay unread.
                        if matches!(resume, Resume::Latest) && self.rescan().await.is_err() { return Err(Restart); }
                        Shard::Reading { iterator, resume }
                    }
                    Some(service) if service.is_trimmed_data_access_exception() => {
                        let Ok(iterator) = self.iterator(&following.arn, &id, &Resume::TrimHorizon).await else { continue };
                        if self.rescan().await.is_err() { return Err(Restart); }
                        Shard::Reading { iterator, resume: Resume::TrimHorizon }
                    }
                    Some(service) if service.is_resource_not_found_exception() => return Err(Restart),
                    _ => continue,
                },
            };
            following.shards.insert(id, next);
        }
        Ok(read_any)
    }

    /// Adds newly listed shards, forgets those no longer listed, and restarts once every shard has closed, as when the stream is disabled or replaced.
    async fn discover(&self, following: &mut Following) -> std::result::Result<(), Restart> {
        let listed = self.list_shards(&following.arn).await.map_err(|_| Restart)?;
        following.next_discovery = Instant::now() + DISCOVER;
        let parents = listed.iter().map(|shard| (shard.id.clone(), shard.parent.clone())).collect::<HashMap<_, _>>();
        following.shards.retain(|id, _| parents.contains_key(id));
        for (id, parent) in parents { following.shards.entry(id).or_insert(Shard::Unread { parent }); }
        let open = listed.iter().any(|shard| shard.open);
        let unfinished = following.shards.values().any(|shard| !matches!(shard, Shard::Finished));
        if !open && !unfinished { return Err(Restart); }
        Ok(())
    }

    async fn list_shards(&self, arn: &str) -> Result<Vec<ListedShard>> {
        let mut shards = Vec::new();
        let mut start: Option<String> = None;
        loop {
            let described = self.streams.describe_stream().stream_arn(arn).set_exclusive_start_shard_id(start.take()).send().await
                .map_err(|error| sdk_anomaly("DescribeStream", error))?;
            let Some(description) = described.stream_description() else { return Ok(shards) };
            for shard in description.shards() {
                let Some(id) = shard.shard_id() else { continue };
                let open = shard.sequence_number_range().is_none_or(|range| range.ending_sequence_number().is_none());
                shards.push(ListedShard { id: id.to_owned(), parent: shard.parent_shard_id().map(str::to_owned), open });
            }
            match description.last_evaluated_shard_id() { Some(last) => start = Some(last.to_owned()), None => return Ok(shards) }
        }
    }

    async fn iterator(&self, arn: &str, shard: &str, resume: &Resume) -> Result<String> {
        let request = self.streams.get_shard_iterator().stream_arn(arn).shard_id(shard);
        let request = match resume {
            Resume::Latest => request.shard_iterator_type(ShardIteratorType::Latest),
            Resume::TrimHorizon => request.shard_iterator_type(ShardIteratorType::TrimHorizon),
            Resume::After(sequence) => request.shard_iterator_type(ShardIteratorType::AfterSequenceNumber).sequence_number(sequence),
        };
        let opened = request.send().await.map_err(|error| sdk_anomaly("GetShardIterator", error))?;
        opened.shard_iterator().map(str::to_owned)
            .ok_or_else(|| Anomaly::fault("rtdb.directory/no-shard-iterator", format!("DynamoDB Streams returned no iterator for shard {shard}")))
    }
}

struct ListedShard { id: String, parent: Option<String>, open: bool }

/// The deployment in a stream image, with the item's `version` attribute as its version; `None` for an image that is not a deployment item.
fn decode_image(image: &HashMap<String, AttributeValue>) -> Option<DeploymentRecord> {
    let version = image.get(VERSION)?.as_n().ok()?.parse().ok()?;
    match image.get(RECORD)? {
        AttributeValue::S(json) => {
            let mut record: DeploymentRecord = serde_json::from_str(json).ok()?;
            record.version = version;
            Some(record)
        }
        AttributeValue::B(bytes) => parked::decode(bytes.as_ref(), image.get(ID)?.as_s().ok()?, image.get(NAME)?.as_s().ok()?,
            image.get(EPOCH)?.as_n().ok()?.parse().ok()?, version).ok(),
        _ => None,
    }
}
