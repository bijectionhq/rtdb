//! The production directory: DynamoDB, single region, on-demand capacity.
//!
//! One table per record kind, keyed by id, plus a names table that makes deployment names unique.
//! Each item carries the record as a JSON blob beside the attributes DynamoDB has to see: the key, `version` for conditions, `holder` for the index.
//! Every replace is a `PutItem` conditional on `version = expected`, and every read is strongly consistent except the holder index query.
//! The deployments table streams its new images, which the directory follows as its change feed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, ConditionCheck, GlobalSecondaryIndex, IndexStatus, KeySchemaElement, KeyType, Projection, ProjectionType, Put,
    ReturnValuesOnConditionCheckFailure, ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus, TransactWriteItem,
};
use aws_sdk_dynamodbstreams::Client as StreamsClient;
use parking_lot::Mutex;
use tokio::sync::broadcast;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{check_fork_parent, exists, fork_conflict, fork_parent_id, Cas, ChangeFeed, DeploymentRecord, Directory, MemberId, MemberRecord, Version};

mod streams;

type Item = HashMap<String, AttributeValue>;

/// The partition key of the deployments and members tables, and the names table's pointer to its deployment.
const ID: &str = "id";
const NAME: &str = "name";
const VERSION: &str = "version";
const HOLDER: &str = "holder";
const RECORD: &str = "record";
/// The index on the deployments table answering `deployments_held_by`; it projects only keys, since every hit is re-read strongly.
pub const HOLDER_INDEX: &str = "holder";

/// The cell's directory tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DynamoTables {
    pub deployments: String,
    /// One item per deployment name, written in the same transaction as the deployment it names.
    pub deployment_names: String,
    pub members: String,
}

impl DynamoTables {
    /// `{prefix}-deployments`, `{prefix}-deployment-names` and `{prefix}-members`.
    pub fn with_prefix(prefix: &str) -> Self {
        Self { deployments: format!("{prefix}-deployments"), deployment_names: format!("{prefix}-deployment-names"), members: format!("{prefix}-members") }
    }
}

pub struct DynamoDirectory {
    client: Client,
    streams: StreamsClient,
    tables: DynamoTables,
    /// The deployments table's stream, followed from the first `deployment_changes` until the directory is dropped.
    changes: Arc<ChangeFeed>,
    following: Mutex<bool>,
}

impl DynamoDirectory {
    pub fn new(client: Client, streams: StreamsClient, tables: DynamoTables) -> Self {
        Self { client, streams, tables, changes: Arc::new(ChangeFeed::new()), following: Mutex::new(false) }
    }

    pub fn from_sdk_config(config: &aws_types::SdkConfig, tables: DynamoTables) -> Self { Self::new(Client::new(config), StreamsClient::new(config), tables) }

    /// A directory on DynamoDB Local at `endpoint`, with static dummy credentials.
    pub fn local(endpoint: &str, tables: DynamoTables) -> Self {
        Self::new(Self::local_client(endpoint), Self::local_streams_client(endpoint), tables)
    }

    /// A client for DynamoDB Local at `endpoint`, with static dummy credentials.
    pub fn local_client(endpoint: &str) -> Client {
        use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new("local", "local", None, None, "rtdb-directory"))
            .build();
        Client::from_conf(config)
    }

    /// A DynamoDB Streams client for DynamoDB Local at `endpoint`, with static dummy credentials.
    pub fn local_streams_client(endpoint: &str) -> StreamsClient {
        use aws_sdk_dynamodbstreams::config::{BehaviorVersion, Credentials, Region};
        let config = aws_sdk_dynamodbstreams::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new("local", "local", None, None, "rtdb-directory"))
            .build();
        StreamsClient::from_conf(config)
    }

    pub fn tables(&self) -> &DynamoTables { &self.tables }

    /// Creates any of the tables that do not exist, on-demand, with the holder index and the deployments' stream of new images, and waits until all are active.
    /// For tests and local cells; production tables belong to the cell's infrastructure code.
    pub async fn create_tables(&self) -> Result<()> {
        self.create_table(&self.tables.deployments, ID).await?;
        self.create_table(&self.tables.deployment_names, NAME).await?;
        self.create_table(&self.tables.members, ID).await?;
        for table in [&self.tables.deployments, &self.tables.deployment_names, &self.tables.members] { self.await_active(table).await?; }
        Ok(())
    }

    async fn create_table(&self, table: &str, key: &str) -> Result<()> {
        let mut request = self.client.create_table().table_name(table)
            .attribute_definitions(attribute_definition(key)?)
            .key_schema(key_schema(key)?)
            .billing_mode(BillingMode::PayPerRequest);
        if table == self.tables.deployments {
            let index = GlobalSecondaryIndex::builder().index_name(HOLDER_INDEX).key_schema(key_schema(HOLDER)?)
                .projection(Projection::builder().projection_type(ProjectionType::KeysOnly).build())
                .build().map_err(build_fault)?;
            let stream = StreamSpecification::builder().stream_enabled(true).stream_view_type(StreamViewType::NewImage).build().map_err(build_fault)?;
            request = request.attribute_definitions(attribute_definition(HOLDER)?).global_secondary_indexes(index).stream_specification(stream);
        }
        match request.send().await {
            Ok(_) => Ok(()),
            Err(error) if error.as_service_error().is_some_and(|service| service.is_resource_in_use_exception()) => Ok(()),
            Err(error) => Err(sdk_anomaly("CreateTable", error)),
        }
    }

    async fn await_active(&self, table: &str) -> Result<()> {
        for _ in 0..600 {
            let described = self.client.describe_table().table_name(table).send().await.map_err(|error| sdk_anomaly("DescribeTable", error))?;
            let description = described.table();
            let table_active = description.and_then(|table| table.table_status()) == Some(&TableStatus::Active);
            let indexes_active = description.map(|table| table.global_secondary_indexes()).unwrap_or_default().iter()
                .all(|index| index.index_status() == Some(&IndexStatus::Active));
            if table_active && indexes_active { return Ok(()); }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.directory/table-not-active", format!("DynamoDB table {table} did not become active")))
    }

    async fn get(&self, table: &str, key: &str, value: &str) -> Result<Option<Item>> { get_item(&self.client, table, key, value).await }

    async fn scan(&self, table: &str) -> Result<Vec<Item>> { scan_items(&self.client, table).await }

    async fn put_new(&self, table: &str, item: Item) -> Result<bool> {
        let written = self.client.put_item().table_name(table).set_item(Some(item))
            .condition_expression("attribute_not_exists(#id)").expression_attribute_names("#id", ID).send().await;
        match written {
            Ok(_) => Ok(true),
            Err(error) if error.as_service_error().is_some_and(|service| service.is_conditional_check_failed_exception()) => Ok(false),
            Err(error) => Err(sdk_anomaly("PutItem", error)),
        }
    }

    /// Writes `item` only if the stored item's version is `expected` and, when `name` is given, its name is `name`.
    /// On a failed condition, returns the stored item, if there is one.
    async fn put_conditional(&self, table: &str, item: Item, expected: Version, name: Option<&str>) -> Result<std::result::Result<(), Option<Item>>> {
        let key = item.get(ID).cloned().ok_or_else(|| Anomaly::fault("rtdb.directory/item", "A directory item has no id"))?;
        let mut request = self.client.put_item().table_name(table).set_item(Some(item))
            .expression_attribute_names("#v", VERSION)
            .expression_attribute_values(":expected", number(expected))
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld);
        request = match name {
            Some(name) => request.condition_expression("#v = :expected AND #n = :name").expression_attribute_names("#n", NAME)
                .expression_attribute_values(":name", AttributeValue::S(name.to_owned())),
            None => request.condition_expression("#v = :expected"),
        };
        match request.send().await {
            Ok(_) => Ok(Ok(())),
            Err(error) => match error.as_service_error() {
                Some(service) if service.is_conditional_check_failed_exception() => {
                    let returned = match service { aws_sdk_dynamodb::operation::put_item::PutItemError::ConditionalCheckFailedException(failed) => failed.item.clone(), _ => None };
                    let current = match returned {
                        Some(item) => Some(item),
                        None => { let AttributeValue::S(id) = &key else { return Err(Anomaly::fault("rtdb.directory/item", "A directory item's id is not a string")) };
                            self.get(table, ID, id).await? }
                    };
                    Ok(Err(current))
                }
                _ => Err(sdk_anomaly("PutItem", error)),
            },
        }
    }
}

async fn scan_items(client: &Client, table: &str) -> Result<Vec<Item>> {
    let mut pages = client.scan().table_name(table).consistent_read(true).into_paginator().send();
    let mut items = Vec::new();
    while let Some(page) = pages.next().await {
        items.extend(page.map_err(|error| sdk_anomaly("Scan", error))?.items.unwrap_or_default());
    }
    Ok(items)
}

async fn get_item(client: &Client, table: &str, key: &str, value: &str) -> Result<Option<Item>> {
    let output = client.get_item().table_name(table).key(key, AttributeValue::S(value.to_owned())).consistent_read(true).send().await
        .map_err(|error| sdk_anomaly("GetItem", error))?;
    Ok(output.item)
}

fn attribute_definition(name: &str) -> Result<AttributeDefinition> {
    AttributeDefinition::builder().attribute_name(name).attribute_type(ScalarAttributeType::S).build().map_err(build_fault)
}

fn key_schema(name: &str) -> Result<KeySchemaElement> {
    KeySchemaElement::builder().attribute_name(name).key_type(KeyType::Hash).build().map_err(build_fault)
}

fn build_fault(error: aws_sdk_dynamodb::error::BuildError) -> Anomaly { Anomaly::fault("rtdb.directory/dynamodb-request", error.to_string()) }

fn sdk_anomaly<E, R>(operation: &str, error: SdkError<E, R>) -> Anomaly
where E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static, R: std::fmt::Debug {
    let category = match &error {
        SdkError::ServiceError(service) => match service.err().code() {
            Some("ThrottlingException" | "ProvisionedThroughputExceededException" | "RequestLimitExceeded" | "TransactionConflictException"
                | "TransactionInProgressException") => AnomalyCategory::Busy,
            Some("InternalServerError" | "ServiceUnavailable") => AnomalyCategory::Unavailable,
            Some("ResourceNotFoundException") => AnomalyCategory::Incorrect,
            _ => AnomalyCategory::Fault,
        },
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => AnomalyCategory::Unavailable,
        _ => AnomalyCategory::Fault,
    };
    Anomaly::new(category, "rtdb.directory/dynamodb", format!("DynamoDB {operation} failed: {}", DisplayErrorContext(&error)))
        .with_context("operation", operation.into())
}

fn number(value: u64) -> AttributeValue { AttributeValue::N(value.to_string()) }

fn string_attribute<'a>(item: &'a Item, name: &str) -> Result<&'a str> {
    match item.get(name) {
        Some(AttributeValue::S(value)) => Ok(value),
        _ => Err(Anomaly::fault("rtdb.directory/item", format!("A directory item has no string attribute {name}"))),
    }
}

fn version_attribute(item: &Item) -> Result<Version> {
    match item.get(VERSION) {
        Some(AttributeValue::N(value)) => value.parse().map_err(|_| Anomaly::fault("rtdb.directory/item", format!("A directory item's version is not a u64: {value}"))),
        _ => Err(Anomaly::fault("rtdb.directory/item", "A directory item has no version")),
    }
}

fn blob<T: Serialize>(record: &T) -> Result<AttributeValue> {
    serde_json::to_string(record).map(AttributeValue::S).map_err(|error| Anomaly::fault("rtdb.directory/encode", error.to_string()))
}

/// The record in `item`, with the item's `version` attribute as its version: that attribute is the one conditions check.
fn decode<T: DeserializeOwned>(item: &Item, set_version: impl FnOnce(&mut T, Version)) -> Result<T> {
    let mut record: T = serde_json::from_str(string_attribute(item, RECORD)?).map_err(|error| Anomaly::fault("rtdb.directory/decode", error.to_string()))?;
    set_version(&mut record, version_attribute(item)?);
    Ok(record)
}

fn decode_deployment(item: &Item) -> Result<DeploymentRecord> { decode(item, |record: &mut DeploymentRecord, version| record.version = version) }

fn decode_member(item: &Item) -> Result<MemberRecord> { decode(item, |record: &mut MemberRecord, version| record.version = version) }

fn deployment_item(record: &DeploymentRecord) -> Result<Item> {
    let mut item = Item::from([
        (ID.to_owned(), AttributeValue::S(record.id.clone())),
        (NAME.to_owned(), AttributeValue::S(record.name.clone())),
        (VERSION.to_owned(), number(record.version)),
        ("epoch".to_owned(), number(record.epoch)),
        (RECORD.to_owned(), blob(record)?),
    ]);
    // Absent rather than null, so an unheld deployment is not in the sparse holder index at all.
    if let Some(holder) = &record.holder { item.insert(HOLDER.to_owned(), AttributeValue::S(holder.0.clone())); }
    Ok(item)
}

fn member_item(record: &MemberRecord) -> Result<Item> {
    Ok(Item::from([
        (ID.to_owned(), AttributeValue::S(record.id.0.clone())),
        (VERSION.to_owned(), number(record.version)),
        (RECORD.to_owned(), blob(record)?),
    ]))
}

impl DynamoDirectory {
    async fn insert_deployment(&self, mut record: DeploymentRecord, parent: Option<ConditionCheck>) -> Result<DeploymentRecord> {
        record.version = 1;
        let deployment = Put::builder().table_name(&self.tables.deployments).set_item(Some(deployment_item(&record)?))
            .condition_expression("attribute_not_exists(#id)").expression_attribute_names("#id", ID).build().map_err(build_fault)?;
        let name_item = Item::from([(NAME.to_owned(), AttributeValue::S(record.name.clone())), (ID.to_owned(), AttributeValue::S(record.id.clone()))]);
        let name = Put::builder().table_name(&self.tables.deployment_names).set_item(Some(name_item))
            .condition_expression("attribute_not_exists(#n)").expression_attribute_names("#n", NAME).build().map_err(build_fault)?;
        let transaction = self.client.transact_write_items()
            .transact_items(TransactWriteItem::builder().put(deployment).build())
            .transact_items(TransactWriteItem::builder().put(name).build());
        let transaction = match parent {
            Some(parent) => transaction.transact_items(TransactWriteItem::builder().condition_check(parent).build()),
            None => transaction,
        };
        match transaction.send().await {
            Ok(_) => Ok(record),
            Err(error) => {
                let refused = |index: usize, reasons: &[aws_sdk_dynamodb::types::CancellationReason]| reasons.get(index).and_then(|reason| reason.code()) == Some("ConditionalCheckFailed");
                if let Some(aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(cancelled)) = error.as_service_error() {
                    let reasons = cancelled.cancellation_reasons();
                    if refused(0, reasons) { return Err(exists("Deployment", &record.id)); }
                    if refused(1, reasons) { return Err(exists("Deployment name", &record.name)); }
                    if refused(2, reasons) { return Err(fork_conflict()); }
                }
                Err(sdk_anomaly("TransactWriteItems", error))
            }
        }
    }
}

#[async_trait]
impl Directory for DynamoDirectory {
    async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord> {
        self.insert_deployment(record, None).await
    }

    async fn create_fork(&self, child: DeploymentRecord, expected_parent: Version) -> Result<DeploymentRecord> {
        let id = fork_parent_id(&child)?;
        let parent = self.deployment(id).await?;
        check_fork_parent(&child, parent.as_ref(), expected_parent)?;
        let condition = ConditionCheck::builder().table_name(&self.tables.deployments)
            .key(ID, AttributeValue::S(id.to_owned()))
            .condition_expression("#v = :expected")
            .expression_attribute_names("#v", VERSION)
            .expression_attribute_values(":expected", number(expected_parent))
            .build().map_err(build_fault)?;
        self.insert_deployment(child, Some(condition)).await
    }

    async fn deployment(&self, id: &str) -> Result<Option<DeploymentRecord>> {
        self.get(&self.tables.deployments, ID, id).await?.as_ref().map(decode_deployment).transpose()
    }

    async fn deployment_by_name(&self, name: &str) -> Result<Option<DeploymentRecord>> {
        let Some(named) = self.get(&self.tables.deployment_names, NAME, name).await? else { return Ok(None) };
        self.deployment(string_attribute(&named, ID)?).await
    }

    async fn deployments(&self) -> Result<Vec<DeploymentRecord>> {
        self.scan(&self.tables.deployments).await?.iter().map(decode_deployment).collect()
    }

    /// The index lags writes, so a deployment claimed a moment ago may be missing; every hit is re-read strongly, so none is stale.
    async fn deployments_held_by(&self, member: &MemberId) -> Result<Vec<DeploymentRecord>> {
        let mut pages = self.client.query().table_name(&self.tables.deployments).index_name(HOLDER_INDEX)
            .key_condition_expression("#h = :holder").expression_attribute_names("#h", HOLDER)
            .expression_attribute_values(":holder", AttributeValue::S(member.0.clone()))
            .into_paginator().send();
        let mut ids = Vec::new();
        while let Some(page) = pages.next().await {
            for item in page.map_err(|error| sdk_anomaly("Query", error))?.items.unwrap_or_default() { ids.push(string_attribute(&item, ID)?.to_owned()); }
        }
        let mut reads = tokio::task::JoinSet::new();
        for id in ids {
            let (client, table) = (self.client.clone(), self.tables.deployments.clone());
            reads.spawn(async move { get_item(&client, &table, ID, &id).await?.as_ref().map(decode_deployment).transpose() });
        }
        let mut held = Vec::new();
        while let Some(read) = reads.join_next().await {
            let read = read.map_err(|error| Anomaly::fault("rtdb.directory/task", error.to_string()))?;
            held.extend(read?.filter(|record| record.holder.as_ref() == Some(member)));
        }
        held.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(held)
    }

    async fn replace_deployment(&self, mut record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> {
        record.version = expected + 1;
        let name = record.name.clone();
        match self.put_conditional(&self.tables.deployments, deployment_item(&record)?, expected, Some(&name)).await? {
            Ok(()) => Ok(Cas::Applied(record)),
            Err(None) => Ok(Cas::Conflict(None)),
            Err(Some(current)) => {
                let current = decode_deployment(&current)?;
                if current.version == expected && current.name != name { return Err(Anomaly::incorrect("rtdb.directory/rename", "A deployment's name never changes")); }
                Ok(Cas::Conflict(Some(current)))
            }
        }
    }

    async fn register_member(&self, mut record: MemberRecord) -> Result<MemberRecord> {
        record.version = 1;
        if self.put_new(&self.tables.members, member_item(&record)?).await? { Ok(record) } else { Err(exists("Member", record.id.as_str())) }
    }

    async fn member(&self, id: &MemberId) -> Result<Option<MemberRecord>> {
        self.get(&self.tables.members, ID, id.as_str()).await?.as_ref().map(decode_member).transpose()
    }

    async fn members(&self) -> Result<Vec<MemberRecord>> {
        self.scan(&self.tables.members).await?.iter().map(decode_member).collect()
    }

    async fn replace_member(&self, mut record: MemberRecord, expected: Version) -> Result<Cas<MemberRecord>> {
        record.version = expected + 1;
        match self.put_conditional(&self.tables.members, member_item(&record)?, expected, None).await? {
            Ok(()) => Ok(Cas::Applied(record)),
            Err(current) => Ok(Cas::Conflict(current.as_ref().map(decode_member).transpose()?)),
        }
    }

    /// Outside a Tokio runtime there is nothing to follow the stream on, so there is no feed.
    fn deployment_changes(&self) -> Option<broadcast::Receiver<DeploymentRecord>> {
        let mut following = self.following.lock();
        if !*following {
            let runtime = tokio::runtime::Handle::try_current().ok()?;
            let follower = streams::Follower { client: self.client.clone(), streams: self.streams.clone(), table: self.tables.deployments.clone(),
                changes: Arc::downgrade(&self.changes) };
            runtime.spawn(follower.run());
            *following = true;
        }
        Some(self.changes.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::contract;

    /// DynamoDB Local's endpoint, e.g. `http://localhost:8000`; the tests are skipped without it.
    const ENDPOINT: &str = "RTDB_DYNAMODB_ENDPOINT";

    async fn local() -> Option<DynamoDirectory> {
        let Ok(endpoint) = std::env::var(ENDPOINT) else {
            eprintln!("skipped: set {ENDPOINT} (e.g. http://localhost:8000) to run the DynamoDB directory tests against DynamoDB Local");
            return None;
        };
        let prefix = format!("rtdb-test-{}", uuid::Uuid::new_v4().simple());
        let directory = DynamoDirectory::local(&endpoint, DynamoTables::with_prefix(&prefix));
        directory.create_tables().await.expect("create tables");
        Some(directory)
    }

    #[tokio::test]
    async fn dynamodb_directory_meets_the_contract() {
        let Some(directory) = local().await else { return };
        contract::run(&directory).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_claims_on_dynamodb_have_one_winner_per_epoch() {
        let Some(directory) = local().await else { return };
        let directory: Arc<dyn Directory> = Arc::new(directory);
        contract::concurrent_claims(&[directory], 8, 10).await;
    }

    #[tokio::test]
    async fn an_unheld_deployment_leaves_the_holder_index() {
        let Some(directory) = local().await else { return };
        let holder = MemberId(format!("m-{}", uuid::Uuid::new_v4().simple()));
        let created = directory.create_deployment(DeploymentRecord::new(format!("held-{}", holder.0), "acme", vec![], 0)).await.unwrap();
        let mut held = created.clone();
        held.holder = Some(holder.clone());
        held.epoch = 1;
        let held = directory.replace_deployment(held, created.version).await.unwrap().applied().unwrap();
        assert_eq!(directory.deployments_held_by(&holder).await.unwrap(), vec![held.clone()]);
        let parked = crate::park(&held, &holder, held.epoch).unwrap();
        directory.replace_deployment(parked, held.version).await.unwrap().applied().unwrap();
        assert_eq!(directory.deployments_held_by(&holder).await.unwrap(), vec![]);
    }

    /// Waits until `receiver` has seen every one of `records`, which shows the follower has reached the stream.
    async fn await_versions(receiver: &mut broadcast::Receiver<DeploymentRecord>, records: &[DeploymentRecord]) {
        let mut awaited = records.iter().map(|record| (record.id.clone(), record.version)).collect::<HashMap<_, _>>();
        tokio::time::timeout(Duration::from_secs(30), async {
            while !awaited.is_empty() {
                let seen = receiver.recv().await.expect("the feed runs");
                if awaited.get(&seen.id).is_some_and(|version| seen.version >= *version) { awaited.remove(&seen.id); }
            }
        }).await.expect("the records arrive");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_dynamodb_stream_delivers_every_change_in_version_order() {
        let Some(directory) = local().await else { return };
        let directory = Arc::new(directory);
        let mut records = Vec::new();
        for index in 0..3 { records.push(directory.create_deployment(DeploymentRecord::new(format!("shop-{index}"), "acme", vec![], 0)).await.unwrap()); }
        let mut receiver = directory.deployment_changes().expect("the DynamoDB directory follows its stream");
        await_versions(&mut receiver, &records).await;

        let seen = records.iter().map(|record| (record.id.clone(), record.version)).collect::<std::collections::BTreeMap<_, _>>();
        let last = crate::feed::tests::replace_concurrently(directory.clone(), records, 10).await;
        crate::feed::tests::expect_every_change(&mut receiver, seen, &last).await;

        let created = directory.create_deployment(DeploymentRecord::new("shop-late", "acme", vec![], 0)).await.unwrap();
        await_versions(&mut receiver, std::slice::from_ref(&created)).await;
    }

    #[tokio::test]
    async fn a_watch_over_the_dynamodb_stream_sees_a_change_long_before_the_next_poll() {
        let Some(directory) = local().await else { return };
        let directory = Arc::new(directory);
        let created = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let mut probe = directory.deployment_changes().unwrap();
        await_versions(&mut probe, std::slice::from_ref(&created)).await;
        let mut watched = crate::watch_deployment(directory.clone(), created.id.clone(), Duration::from_secs(60));
        tokio::time::timeout(Duration::from_secs(5), watched.changed()).await.unwrap().unwrap();

        let mut changed = created.clone();
        changed.epoch = 1;
        let changed = directory.replace_deployment(changed, created.version).await.unwrap().applied().unwrap();
        tokio::time::timeout(Duration::from_secs(5), watched.changed()).await.expect("the change arrives long before the next poll").unwrap();
        assert_eq!(watched.borrow().as_ref().unwrap().version, changed.version);
    }
}
