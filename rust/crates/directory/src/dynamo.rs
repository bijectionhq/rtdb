//! The production directory: DynamoDB, single region, on-demand capacity.
//!
//! One table per record kind, keyed by id, plus a names table that makes deployment names unique.
//! Each item carries a versioned record beside the attributes DynamoDB uses for lookups, conditions and the holder index.
//! Every replace is a `PutItem` conditional on `version = expected`, and every read is strongly consistent except the holder index query.
//! The deployments table streams its new images, which the directory follows as its change feed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::installed_reservation::dynamo::{EXACT_VERSION, ID, INSTALLED_JOURNAL, NEW_ITEM, VERSION, reservation_fields};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, ConditionCheck, GlobalSecondaryIndex, IndexStatus, KeySchemaElement, KeyType, Projection, ProjectionType, Put,
    ReturnValuesOnConditionCheckFailure, ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus, TransactWriteItem,
};
use aws_sdk_dynamodbstreams::Client as StreamsClient;
use parking_lot::Mutex;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{Cas, ChangeFeed, DeploymentRecord, Directory, ForkPublication, ReferenceDisposal, ReplaceOutcome, HostedTask, LineageChange, LineageRetirement, LineageState, MemberId, MemberRecord, REFERENCE_REVISION_PREFIX, Version, exists, fork_conflict, fork_parent_id, require_deployment_name, shared_fork_lineage};
use crate::lineage::{disposal_changed, lineage_busy};
use crate::{AUDIENCE_SEAT_PREFIX, AudienceSeatClaim, reserved_deployment_name};
use audience_seat::{SeatCondition, SeatPredecessor, SeatRevision, acquire_condition, claim_predecessor, release_condition};
use crate::{
    CheckpointTransferActivation, CheckpointTransferBootstrap, CheckpointTransferFence, CheckpointTransferReplica, CheckpointTransferSelection, CheckpointTransferTerminal, ForkPreparation, fork_stage_changed,
    fork_stage_version, prepare_checkpoint_transfer, prepare_checkpoint_transfer_activation, prepare_checkpoint_transfer_bootstrap, prepare_checkpoint_transfer_replica, prepare_checkpoint_transfer_terminal, require_fork_stage_update,
    classic_flight, installed_journal_id, require_transfer_update, stage_fork_record, transfer_changed,
};

mod audience_seat;
mod parked;
mod streams;

type Item = HashMap<String, AttributeValue>;

const NAME: &str = "name";
const EPOCH: &str = "epoch";
const HOLDER: &str = "holder";
const RECORD: &str = "record";
const LINEAGE_VERSION: &str = "lineage_version";
const CHECKPOINT_TRANSFER_FENCE: &str = "checkpoint_transfer_fence";
const CLASSIC_FLIGHT: &str = "classic_flight";
const FORK_STAGE: &str = "fork_stage";
const WRITER_HANDOFF: &str = "writer_handoff";
const ELIGIBILITY_EPOCH: &str = "eligibility_epoch";
const ELIGIBILITY: &str = "eligibility";
/// The index on the deployments table answering `deployments_held_by`; it projects only keys, since every hit is re-read strongly.
pub const HOLDER_INDEX: &str = "holder";

/// The cell's directory tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DynamoTables {
    pub deployments: String,
    /// One item per deployment name, written in the same transaction as the deployment it names.
    pub deployment_names: String,
    pub members: String,
    /// Written by the ECS stop-event owner, never by serving nodes.
    pub stopped_tasks: String,
}

impl DynamoTables {
    /// The deployment, name, member and stopped-task tables for this cell.
    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            deployments: format!("{prefix}-deployments"), deployment_names: format!("{prefix}-deployment-names"),
            members: format!("{prefix}-members"), stopped_tasks: format!("{prefix}-stopped-tasks"),
        }
    }
}

pub struct DynamoDirectory {
    client: Client,
    audience_reader: Option<Client>,
    streams: StreamsClient,
    tables: DynamoTables,
    pack_parked_records: bool,
    /// The deployments table's stream, followed from the first `deployment_changes` until the directory is dropped.
    changes: Arc<ChangeFeed>,
    following: Mutex<bool>,
}

impl DynamoDirectory {
    pub fn new(client: Client, streams: StreamsClient, tables: DynamoTables) -> Self {
        Self { client, audience_reader: None, streams, tables, pack_parked_records: false, changes: Arc::new(ChangeFeed::new()), following: Mutex::new(false) }
    }

    pub fn from_sdk_config(config: &aws_types::SdkConfig, tables: DynamoTables) -> Self { Self::new(Client::new(config), StreamsClient::new(config), tables) }

    /// Installs the reader created with the audience's own connector and credential provider.
    pub fn with_audience_reader(mut self, reader: Client) -> Result<Self> {
        if self.client.config().region() != reader.config().region() {
            return Err(Anomaly::incorrect("rtdb.directory/audience-region", "Both directory clients must name the same region"));
        }
        self.audience_reader = Some(reader);
        Ok(self)
    }

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

    /// Enables binary writes for eligible unheld deployments after every directory reader supports the format.
    pub fn with_packed_parked_records(mut self) -> Self {
        self.pack_parked_records = true;
        self
    }

    /// Creates any of the tables that do not exist, on-demand, with the holder index and the deployments' stream of new images, and waits until all are active.
    /// For tests and local cells; production tables belong to the cell's infrastructure code.
    pub async fn create_tables(&self) -> Result<()> {
        self.create_table(&self.tables.deployments, ID).await?;
        self.create_table(&self.tables.deployment_names, NAME).await?;
        self.create_table(&self.tables.members, ID).await?;
        self.create_table(&self.tables.stopped_tasks, ID).await?;
        for table in [&self.tables.deployments, &self.tables.deployment_names, &self.tables.members, &self.tables.stopped_tasks] { self.await_active(table).await?; }
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
            .condition_expression(NEW_ITEM).expression_attribute_names("#id", ID).send().await;
        match written {
            Ok(_) => Ok(true),
            Err(error) if error.as_service_error().is_some_and(|service| service.is_conditional_check_failed_exception()) => Ok(false),
            Err(error) => Err(sdk_anomaly("PutItem", error)),
        }
    }

    /// Writes `item` only if the stored item's version is `expected` and, when `name` is given, its name is `name`.
    /// On a failed condition, returns the stored item, if there is one.
    async fn put_conditional(&self, table: &str, item: Item, expected: Version, name: Option<&str>) -> PutConditional {
        let Some(key) = item.get(ID).cloned() else {
            return PutConditional::Refused(Anomaly::fault("rtdb.directory/item", "A directory item has no id"));
        };
        let staged = item.contains_key(FORK_STAGE);
        let transfer = item.contains_key(CHECKPOINT_TRANSFER_FENCE);
        let writer_handoff = item.contains_key(WRITER_HANDOFF);
        let classic_flight = item.contains_key(CLASSIC_FLIGHT);
        let mut request = self.client.put_item().table_name(table).set_item(Some(item))
            .expression_attribute_names("#v", VERSION)
            .expression_attribute_values(":expected", number(expected))
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld);
        request = match name {
            Some(name) => {
                let condition = match (staged, transfer) {
                    (false, false) => "#v = :expected AND #n = :name AND attribute_not_exists(#stage) AND attribute_not_exists(#transfer)",
                    (true, false) => "#v = :expected AND #n = :name AND attribute_exists(#stage) AND attribute_not_exists(#transfer)",
                    (false, true) => "#v = :expected AND #n = :name AND attribute_not_exists(#stage) AND attribute_exists(#transfer)",
                    (true, true) => return PutConditional::Refused(transfer_changed()),
                };
                request = request.expression_attribute_names("#n", NAME).expression_attribute_names("#stage", FORK_STAGE)
                    .expression_attribute_names("#transfer", CHECKPOINT_TRANSFER_FENCE)
                    .expression_attribute_values(":name", AttributeValue::S(name.to_owned()));
                let mut condition = condition.to_owned();
                if !writer_handoff {
                    condition.push_str(" AND attribute_not_exists(#handoff)");
                    request = request.expression_attribute_names("#handoff", WRITER_HANDOFF);
                }
                if !classic_flight {
                    condition.push_str(" AND attribute_not_exists(#classicFlight)");
                    request = request.expression_attribute_names("#classicFlight", CLASSIC_FLIGHT);
                }
                request.condition_expression(condition)
            }
            None => request.condition_expression(EXACT_VERSION),
        };
        match request.send().await {
            Ok(_) => PutConditional::Applied,
            Err(error) => match error.as_service_error() {
                Some(service) if service.is_conditional_check_failed_exception() => {
                    let returned = match service { aws_sdk_dynamodb::operation::put_item::PutItemError::ConditionalCheckFailedException(failed) => failed.item.clone(), _ => None };
                    let current = match returned {
                        Some(item) => Some(item),
                        None => {
                            let AttributeValue::S(id) = &key else {
                                return PutConditional::Refused(Anomaly::fault("rtdb.directory/item", "A directory item's id is not a string"));
                            };
                            match self.get(table, ID, id).await {
                                Ok(item) => item,
                                Err(error) => return PutConditional::Refused(error),
                            }
                        }
                    };
                    PutConditional::Conflict(current)
                }
                _ if write_error_is_unknown(&error) => PutConditional::Unknown(sdk_anomaly("PutItem", error)),
                _ => PutConditional::Refused(sdk_anomaly("PutItem", error)),
            },
        }
    }
}

enum PutConditional {
    Applied,
    Conflict(Option<Item>),
    Refused(Anomaly),
    Unknown(Anomaly),
}

const SEAT_REVISION: &str = "seat_revision";
const SEAT_CHARGED: &str = "seat_charged";
const SEAT_LEASE_UNTIL: &str = "seat_lease_until_ms";

fn seat_lease_until_ms(now_ms: i64) -> i64 {
    now_ms.saturating_add(i64::try_from(crate::AUDIENCE_SEAT_LEASE.as_millis()).unwrap_or(i64::MAX))
}

fn seat_lease_expired(lease_until_ms: Option<i64>, now_ms: i64) -> bool {
    lease_until_ms.is_some_and(|until| now_ms >= until)
}

struct DynamoAudienceSeat {
    client: Client,
    names: String,
    deployment: ConditionCheck,
    name: String,
    predecessor: SeatPredecessor,
    claimed: u128,
    released: u128,
    task: Option<HostedTask>,
}

impl DynamoAudienceSeat {
    fn write(&self, condition: &SeatCondition, revision: u128, charged: bool, lease_until_ms: Option<i64>) -> Result<Put> {
        let value = |revision| AttributeValue::S(uuid::Uuid::from_u128(revision).to_string());
        let mut item = Item::from([
            (NAME.to_owned(), AttributeValue::S(self.name.clone())),
            (SEAT_REVISION.to_owned(), value(revision)),
            (SEAT_CHARGED.to_owned(), AttributeValue::Bool(charged)),
        ]);
        if charged {
            if let Some(task) = self.task.as_ref() {
                item.insert(RECORD.to_owned(), blob(task)?);
            }
            if let Some(until) = lease_until_ms {
                item.insert(SEAT_LEASE_UNTIL.to_owned(), AttributeValue::N(until.to_string()));
            }
        }
        let put = Put::builder().table_name(&self.names).set_item(Some(item));
        let put = match *condition {
            SeatCondition::Missing => put.condition_expression("attribute_not_exists(#name)")
                .expression_attribute_names("#name", NAME),
            SeatCondition::Exact(revision) => put.condition_expression("#revision = :prior")
                .expression_attribute_names("#revision", SEAT_REVISION).expression_attribute_values(":prior", value(revision)),
            SeatCondition::MissingOr(claimed) => put.condition_expression("attribute_not_exists(#name) OR #revision = :claim")
                .expression_attribute_names("#name", NAME).expression_attribute_names("#revision", SEAT_REVISION)
                .expression_attribute_values(":claim", value(claimed)),
            SeatCondition::Either(prior, claimed) => put.condition_expression("#revision IN (:prior, :claim)")
                .expression_attribute_names("#revision", SEAT_REVISION)
                .expression_attribute_values(":prior", value(prior)).expression_attribute_values(":claim", value(claimed)),
        };
        put.build().map_err(build_fault)
    }
}

#[async_trait]
impl AudienceSeatClaim for DynamoAudienceSeat {
    async fn acquire(&self) -> Result<bool> {
        let condition = acquire_condition(&self.predecessor);
        let put = self.write(&condition, self.claimed, true, Some(seat_lease_until_ms(crate::now_ms())))?;
        let placement = TransactWriteItem::builder().condition_check(self.deployment.clone()).build();
        let seat = TransactWriteItem::builder().put(put).build();
        write_conditional_transaction(&self.client, vec![placement, seat]).await
    }

    async fn renew(&self) -> Result<bool> {
        let condition = SeatCondition::Exact(self.claimed);
        let put = self.write(&condition, self.claimed, true, Some(seat_lease_until_ms(crate::now_ms())))?;
        let request = self.client.put_item().table_name(&self.names).set_item(Some(put.item))
            .set_condition_expression(put.condition_expression)
            .set_expression_attribute_names(put.expression_attribute_names)
            .set_expression_attribute_values(put.expression_attribute_values);
        match request.send().await {
            Ok(_) => Ok(true),
            Err(error) if error.as_service_error().is_some_and(|error| error.is_conditional_check_failed_exception()) => Ok(false),
            Err(error) => Err(sdk_anomaly("PutItem", error)),
        }
    }

    async fn release(&self) -> Result<()> {
        let condition = release_condition(&self.predecessor, self.claimed);
        let put = self.write(&condition, self.released, false, None)?;
        let request = self.client.put_item().table_name(&self.names).set_item(Some(put.item))
            .set_condition_expression(put.condition_expression)
            .set_expression_attribute_names(put.expression_attribute_names)
            .set_expression_attribute_values(put.expression_attribute_values);
        match request.send().await {
            Ok(_) => Ok(()),
            Err(error) if error.as_service_error().is_some_and(|error| error.is_conditional_check_failed_exception()) => Ok(()),
            Err(error) => Err(sdk_anomaly("PutItem", error)),
        }
    }
}

fn journal_eligibility_changed(deployment: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.directory/journal-eligibility-changed",
        format!("A logkeeper that deployment {deployment} newly names changed its eligibility during the write"))
}

async fn write_conditional_transaction(client: &Client, items: Vec<TransactWriteItem>) -> Result<bool> {
    use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
    match client.transact_write_items().set_transact_items(Some(items)).send().await {
        Ok(_) => Ok(true),
        Err(error) => match error.as_service_error() {
            Some(TransactWriteItemsError::TransactionCanceledException(refusal))
                if refusal.cancellation_reasons().iter().any(|reason|
                    matches!(reason.code(), Some("ConditionalCheckFailed" | "TransactionConflict"))) => Ok(false),
            _ => Err(sdk_anomaly("TransactWriteItems", error)),
        },
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

fn write_error_is_unknown<E, R>(error: &SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    match error {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => true,
        SdkError::ServiceError(service) => matches!(
            service.err().code(),
            Some("InternalServerError" | "ServiceUnavailable") | None
        ),
        SdkError::ConstructionFailure(_) => false,
        _ => true,
    }
}

fn write_sdk_outcome<E, R, T>(operation: &str, error: SdkError<E, R>) -> ReplaceOutcome<T>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug,
{
    if write_error_is_unknown(&error) {
        ReplaceOutcome::Unknown(sdk_anomaly(operation, error))
    } else {
        ReplaceOutcome::Refused(sdk_anomaly(operation, error))
    }
}

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

fn number_attribute(item: &Item, name: &str) -> Result<u64> {
    match item.get(name) {
        Some(AttributeValue::N(value)) => value.parse().map_err(|_| Anomaly::fault("rtdb.directory/item", format!("A directory item's {name} is not a u64: {value}"))),
        _ => Err(Anomaly::fault("rtdb.directory/item", format!("A directory item has no number attribute {name}"))),
    }
}

fn blob<T: Serialize>(record: &T) -> Result<AttributeValue> {
    serde_json::to_string(record).map(AttributeValue::S).map_err(|error| Anomaly::fault("rtdb.directory/encode", error.to_string()))
}

/// The record in `item`, with the item's `version` attribute as its version: that attribute is the one conditions check.
fn decode<T: DeserializeOwned>(item: &Item, set_version: impl FnOnce(&mut T, Version)) -> Result<T> {
    let mut record: T = serde_json::from_str(string_attribute(item, RECORD)?).map_err(|error| Anomaly::fault("rtdb.directory/decode", error.to_string()))?;
    set_version(&mut record, number_attribute(item, VERSION)?);
    Ok(record)
}

fn decode_deployment(item: &Item) -> Result<DeploymentRecord> {
    match item.get(RECORD) {
        Some(AttributeValue::B(bytes)) => parked::decode(bytes.as_ref(), string_attribute(item, ID)?, string_attribute(item, NAME)?,
            number_attribute(item, EPOCH)?, number_attribute(item, VERSION)?),
        _ => decode(item, |record: &mut DeploymentRecord, version| record.version = version),
    }
}

fn is_installed_journal(item: &Item) -> bool {
    item.get(INSTALLED_JOURNAL) == Some(&AttributeValue::Bool(true))
}

fn deployment_record(item: Option<&Item>) -> Result<Option<DeploymentRecord>> {
    item.filter(|item| !is_installed_journal(item)).map(decode_deployment).transpose()
}

fn decode_member(item: &Item) -> Result<MemberRecord> { decode(item, |record: &mut MemberRecord, version| record.version = version) }

fn deployment_item(record: &DeploymentRecord, pack_parked: bool) -> Result<Item> {
    let packed = pack_parked.then(|| parked::encode(record)).flatten();
    let body = match packed { Some(bytes) => AttributeValue::B(Blob::new(bytes)), None => blob(record)? };
    let mut item = Item::from([
        (ID.to_owned(), AttributeValue::S(record.id.clone())),
        (NAME.to_owned(), AttributeValue::S(record.name.clone())),
        (VERSION.to_owned(), number(record.version)),
        (EPOCH.to_owned(), number(record.epoch)),
        (RECORD.to_owned(), body),
    ]);
    // Absent rather than null, so an unheld deployment is not in the sparse holder index at all.
    if let Some(holder) = &record.holder { item.insert(HOLDER.to_owned(), AttributeValue::S(holder.0.clone())); }
    if record.fork_stage.is_some() { item.insert(FORK_STAGE.to_owned(), AttributeValue::Bool(true)); }
    if !record.writer_handoffs.is_empty() { item.insert(WRITER_HANDOFF.to_owned(), AttributeValue::Bool(true)); }
    if record.checkpoint_transfer_fence.is_some() {
        item.insert(CHECKPOINT_TRANSFER_FENCE.to_owned(), AttributeValue::Bool(true));
    }
    if record.classic_flight.is_some() { item.insert(CLASSIC_FLIGHT.to_owned(), AttributeValue::Bool(true)); }
    Ok(item)
}

fn member_item(record: &MemberRecord) -> Result<Item> {
    Ok(Item::from([
        (ID.to_owned(), AttributeValue::S(record.id.0.clone())),
        (VERSION.to_owned(), number(record.version)),
        (ELIGIBILITY_EPOCH.to_owned(), number(record.eligibility_epoch)),
        (ELIGIBILITY.to_owned(), eligibility(record)?),
        (RECORD.to_owned(), blob(record)?),
    ]))
}

/// The member's state and decommission, compared when a write keeps its eligibility epoch.
fn eligibility(record: &MemberRecord) -> Result<AttributeValue> {
    serde_json::to_string(&(record.state, &record.decommission)).map(AttributeValue::S)
        .map_err(|error| Anomaly::fault("rtdb.directory/encode", error.to_string()))
}

impl DynamoDirectory {
    async fn prepare_seat_claim(&self, source: uuid::Uuid, index: usize, task: Option<&HostedTask>,
        deployment: ConditionCheck) -> Result<Option<Box<dyn AudienceSeatClaim>>> {
        let name = format!("{AUDIENCE_SEAT_PREFIX}{source}/{index}");
        let revision = match self.get(&self.tables.deployment_names, NAME, &name).await? {
            None => SeatRevision::Missing,
            Some(item) => {
                let revision = uuid::Uuid::parse_str(string_attribute(&item, SEAT_REVISION)?)
                    .map_err(|error| Anomaly::fault("rtdb.directory/audience-seat-revision", error.to_string()))?.as_u128();
                match item.get(SEAT_CHARGED) {
                    Some(AttributeValue::Bool(false)) => SeatRevision::Free(revision),
                    Some(AttributeValue::Bool(true)) => {
                        let lease_until_ms = match item.get(SEAT_LEASE_UNTIL) {
                            Some(AttributeValue::N(value)) => Some(value.parse::<i64>()
                                .map_err(|error| Anomaly::fault("rtdb.directory/audience-seat-lease", error.to_string()))?),
                            None => None,
                            _ => return Err(Anomaly::fault("rtdb.directory/audience-seat-lease", "Invalid audience seat lease")),
                        };
                        let stopped = seat_lease_expired(lease_until_ms, crate::now_ms()) || match item.get(RECORD) {
                            Some(AttributeValue::S(record)) => {
                                let owner: HostedTask = serde_json::from_str(record)
                                    .map_err(|error| Anomaly::fault("rtdb.directory/audience-seat-owner", error.to_string()))?;
                                self.hosted_task_stopped(&owner).await?
                            }
                            None => false,
                            _ => return Err(Anomaly::fault("rtdb.directory/audience-seat-owner", "Invalid audience seat owner")),
                        };
                        SeatRevision::Charged { revision, stopped }
                    }
                    _ => return Err(Anomaly::fault("rtdb.directory/audience-seat-state", "Invalid audience seat state")),
                }
            }
        };
        let Some(predecessor) = claim_predecessor(revision) else { return Ok(None); };
        Ok(Some(Box::new(DynamoAudienceSeat {
            client: self.client.clone(), names: self.tables.deployment_names.clone(), deployment, name, predecessor,
            claimed: uuid::Uuid::new_v4().as_u128(), released: uuid::Uuid::new_v4().as_u128(), task: task.cloned(),
        })))
    }

    async fn insert_deployment(&self, mut record: DeploymentRecord, fork: Vec<TransactWriteItem>,
        stage_version: Option<Version>) -> Result<DeploymentRecord> {
        require_deployment_name(&record.name)?;
        if record.classic_flight.is_some() || record.classic_catalog_drain.is_some() || record.checkpoint_transfer_fence.is_some() || record.checkpoint_transfer_terminal.is_some() || record.checkpoint_transfer_bootstrap.is_some() || record.checkpoint_transfer_replica.is_some() || record.checkpoint_transfer_activation.is_some() { return Err(transfer_changed()); }
        record.version = stage_version.unwrap_or(0) + 1;
        let deployment = Put::builder().table_name(&self.tables.deployments).set_item(Some(deployment_item(&record, self.pack_parked_records)?));
        let deployment = match stage_version {
            Some(expected) => deployment.condition_expression(EXACT_VERSION).expression_attribute_names("#v", VERSION)
                .expression_attribute_values(":expected", number(expected)),
            None => deployment.condition_expression(NEW_ITEM).expression_attribute_names("#id", ID),
        }.build().map_err(build_fault)?;
        let name_item = Item::from([(NAME.to_owned(), AttributeValue::S(record.name.clone())), (ID.to_owned(), AttributeValue::S(record.id.clone()))]);
        let name = Put::builder().table_name(&self.tables.deployment_names).set_item(Some(name_item))
            .condition_expression("attribute_not_exists(#n)").expression_attribute_names("#n", NAME).build().map_err(build_fault)?;
        let publishing_fork = !fork.is_empty();
        let staged = match stage_version { Some(_) => self.deployment(&record.id).await?, None => None };
        let fence = self.journal_fence(staged.as_ref(), &record).await?;
        let mut items = vec![TransactWriteItem::builder().put(deployment).build(), TransactWriteItem::builder().put(name).build()];
        items.extend(fork);
        let fenced_from = items.len();
        items.extend(fence);
        let transaction = self.client.transact_write_items().set_transact_items(Some(items));
        match transaction.send().await {
            Ok(_) => Ok(record),
            Err(error) => {
                let refused = |index: usize, reasons: &[aws_sdk_dynamodb::types::CancellationReason]| reasons.get(index).and_then(|reason| reason.code()) == Some("ConditionalCheckFailed");
                if let Some(aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(cancelled)) = error.as_service_error() {
                    let reasons = cancelled.cancellation_reasons();
                    if refused(0, reasons) {
                        return Err(if stage_version.is_some() { fork_stage_changed() } else { exists("Deployment", &record.id) });
                    }
                    if refused(1, reasons) { return Err(exists("Deployment name", &record.name)); }
                    if (fenced_from..reasons.len()).any(|index| refused(index, reasons)) { return Err(journal_eligibility_changed(&record.id)); }
                    if refused(2, reasons) || refused(3, reasons)
                        || (publishing_fork && reasons.iter().any(|reason| reason.code() == Some("TransactionConflict")))
                    {
                        return Err(fork_conflict());
                    }
                }
                Err(sdk_anomaly("TransactWriteItems", error))
            }
        }
    }

    fn require_deployment_version(&self, id: &str, expected_parent: Version) -> Result<TransactWriteItem> {
        let condition = ConditionCheck::builder().table_name(&self.tables.deployments)
            .key(ID, AttributeValue::S(id.to_owned()))
            .condition_expression(EXACT_VERSION)
            .expression_attribute_names("#v", VERSION)
            .expression_attribute_values(":expected", number(expected_parent))
            .build().map_err(build_fault)?;
        Ok(TransactWriteItem::builder().condition_check(condition).build())
    }

    /// Condition checks holding each logkeeper `next` newly names at the eligibility epoch it was observed accepting references at (`JournalReferenceRequiresCurrentEligibility`).
    async fn journal_fence(&self, current: Option<&DeploymentRecord>, next: &DeploymentRecord) -> Result<Vec<TransactWriteItem>> {
        if !crate::may_add_journal_references(current, next) { return Ok(Vec::new()); }
        let members = self.members().await?;
        crate::journal_fence(current, next, &members)?.into_iter().map(|(id, epoch)| {
            let condition = if epoch == 0 { "attribute_not_exists(#epoch) OR #epoch = :epoch" } else { "#epoch = :epoch" };
            let check = ConditionCheck::builder().table_name(&self.tables.members).key(ID, AttributeValue::S(id.0))
                .condition_expression(condition).expression_attribute_names("#epoch", ELIGIBILITY_EPOCH)
                .expression_attribute_values(":epoch", number(epoch)).build().map_err(build_fault)?;
            Ok(TransactWriteItem::builder().condition_check(check).build())
        }).collect()
    }

    /// Replaces a deployment that may newly name a logkeeper in one transaction with the fence on its eligibility, or None when the write needs no fence.
    /// The record it diffs against is the one at `expected`, which the conditional put then requires, so the fence covers exactly what the write adds.
    async fn replace_fenced_deployment(&self, record: &DeploymentRecord, expected: Version) -> Option<ReplaceOutcome<DeploymentRecord>> {
        use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;

        for _ in 0..8 {
            let current = match self.deployment(&record.id).await {
                Ok(Some(current)) => current,
                Ok(None) => return Some(ReplaceOutcome::Conflict(None)),
                Err(error) => return Some(ReplaceOutcome::Refused(error)),
            };
            if current.version != expected { return Some(ReplaceOutcome::Conflict(Some(current))); }
            let fence = match self.journal_fence(Some(&current), record).await {
                Ok(fence) => fence,
                Err(error) => return Some(ReplaceOutcome::Refused(error)),
            };
            if fence.is_empty() { return None; }
            if current.name != record.name {
                return Some(ReplaceOutcome::Refused(Anomaly::incorrect("rtdb.directory/rename", "A deployment's name never changes")));
            }
            if let Err(error) = require_fork_stage_update(&current, record) { return Some(ReplaceOutcome::Refused(error)); }
            if let Err(error) = require_transfer_update(&current, record) { return Some(ReplaceOutcome::Refused(error)); }
            if let Err(error) = crate::require_writer_handoff_update(&current, record) { return Some(ReplaceOutcome::Refused(error)); }
            let mut next = record.clone();
            next.version = expected + 1;
            let item = match deployment_item(&next, self.pack_parked_records) {
                Ok(item) => item,
                Err(error) => return Some(ReplaceOutcome::Refused(error)),
            };
            let put = match Put::builder().table_name(&self.tables.deployments).set_item(Some(item))
                .condition_expression(EXACT_VERSION).expression_attribute_names("#v", VERSION)
                .expression_attribute_values(":expected", number(expected)).build().map_err(build_fault)
            {
                Ok(put) => put,
                Err(error) => return Some(ReplaceOutcome::Refused(error)),
            };
            let mut items = vec![TransactWriteItem::builder().put(put).build()];
            items.extend(fence);
            match self.client.transact_write_items().set_transact_items(Some(items)).send().await {
                Ok(_) => return Some(ReplaceOutcome::Applied(next)),
                Err(error) => match error.as_service_error() {
                    Some(TransactWriteItemsError::TransactionCanceledException(refusal))
                        if refusal.cancellation_reasons().iter().any(|reason|
                            matches!(reason.code(), Some("ConditionalCheckFailed" | "TransactionConflict"))) => {}
                    _ => return Some(write_sdk_outcome("TransactWriteItems", error)),
                },
            }
        }
        Some(ReplaceOutcome::Refused(journal_eligibility_changed(&record.id)))
    }

    async fn write_retirement(&self, items: Vec<TransactWriteItem>) -> Result<bool> {
        write_conditional_transaction(&self.client, items).await
    }

    async fn write_owner_and_lineage(&self, record: DeploymentRecord, expected: Version,
        current: &LineageState, next: &LineageState, legacy: bool) -> Result<Cas<DeploymentRecord>> {
        let put = Put::builder().table_name(&self.tables.deployments).set_item(Some(deployment_item(&record, self.pack_parked_records)?))
            .condition_expression(EXACT_VERSION).expression_attribute_names("#v", VERSION)
            .expression_attribute_values(":expected", number(expected)).build().map_err(build_fault)?;
        let stored = self.deployment(&record.id).await?;
        let fence = self.journal_fence(stored.as_ref(), &record).await?;
        let mut items = vec![
            TransactWriteItem::builder().put(put).build(),
            self.write_lineage(&record.lineage, next, current.version(), legacy)?,
        ];
        items.extend(fence);
        if self.write_retirement(items).await? {
            Ok(Cas::Applied(record))
        } else {
            Ok(Cas::Conflict(self.deployment(&record.id).await?))
        }
    }

    async fn read_lineage(&self, lineage: &str) -> Result<(LineageState, bool)> {
        let key = format!("{REFERENCE_REVISION_PREFIX}{lineage}");
        let Some(item) = self.get(&self.tables.deployment_names, NAME, &key).await? else {
            return Ok((LineageState::default(), true));
        };
        if !item.contains_key(RECORD) {
            return Ok((LineageState::legacy(number_attribute(&item, VERSION)?), true));
        }
        let version = number_attribute(&item, LINEAGE_VERSION)?;
        let state: LineageState = decode(&item, |_, _| {})?;
        if state.version() != version || number_attribute(&item, VERSION)? != Version::MAX {
            return Err(Anomaly::fault("rtdb.directory/lineage-state", "The stored lineage state has inconsistent versions"));
        }
        Ok((state, false))
    }

    fn write_lineage(&self, lineage: &str, next: &LineageState, expected: Version, legacy: bool) -> Result<TransactWriteItem> {
        let mut item = Item::from([(NAME.to_owned(), AttributeValue::S(format!("{REFERENCE_REVISION_PREFIX}{lineage}")))]);
        if legacy && let Some(revision) = next.legacy_revision() {
            item.insert(VERSION.to_owned(), number(revision));
        } else {
            // Revision-only clients cannot advance the old counter past this value.
            item.insert(VERSION.to_owned(), number(Version::MAX));
            item.insert(LINEAGE_VERSION.to_owned(), number(next.version()));
            item.insert(RECORD.to_owned(), blob(next)?);
        }
        let put = Put::builder().table_name(&self.tables.deployment_names).set_item(Some(item));
        let put = if expected == 0 {
            put.condition_expression("attribute_not_exists(#n)").expression_attribute_names("#n", NAME)
        } else {
            put.condition_expression("#v = :expected").expression_attribute_names("#v", if legacy { VERSION } else { LINEAGE_VERSION })
                .expression_attribute_values(":expected", number(expected))
        };
        Ok(TransactWriteItem::builder().put(put.build().map_err(build_fault)?).build())
    }
}

#[async_trait]
impl Directory for DynamoDirectory {
    async fn receiver_clock(&self, domain: uuid::Uuid) -> Result<Option<crate::ReceiverClockSample>> {
        let name = format!("{}{domain}", crate::CLOCK_PREFIX);
        let client = self.audience_reader.as_ref().unwrap_or(&self.client);
        get_item(client, &self.tables.deployment_names, NAME, &name).await?.map(|item| {
            let sample: crate::ReceiverClockSample = serde_json::from_str(string_attribute(&item, RECORD)?)
                .map_err(|error| Anomaly::fault("rtdb.directory/clock-record", error.to_string()))?;
            if sample.domain != domain || sample.sequence != number_attribute(&item, VERSION)? {
                return Err(Anomaly::fault("rtdb.directory/clock-record", "Receiver clock record differs from its identity"));
            }
            Ok(sample)
        }).transpose()
    }

    async fn publish_receiver_clock(&self, domain: uuid::Uuid, publisher: uuid::Uuid, observation: rtdb_types::Timestamp) -> Result<()> {
        let previous = self.receiver_clock(domain).await?;
        let Some(sample) = crate::ReceiverClockSample::advance(previous, domain, publisher, observation)? else {
            return Ok(());
        };
        let name = format!("{}{domain}", crate::CLOCK_PREFIX);
        let request = self.client.put_item().table_name(&self.tables.deployment_names)
            .item(NAME, AttributeValue::S(name))
            .item(VERSION, number(sample.sequence))
            .item(RECORD, blob(&sample)?);
        let request = match previous {
            Some(last) => request.condition_expression("#version = :last")
                .expression_attribute_names("#version", VERSION)
                .expression_attribute_values(":last", number(last.sequence)),
            None => request.condition_expression("attribute_not_exists(#name)").expression_attribute_names("#name", NAME),
        };
        match request.send().await {
            Ok(_) => Ok(()),
            Err(error) if error.as_service_error().is_some_and(|service| service.is_conditional_check_failed_exception()) => Ok(()),
            Err(error) => Err(sdk_anomaly("PutItem", error)),
        }
    }

    async fn reserve_installed_journal(&self, id: uuid::Uuid) -> Result<()> {
        let id = installed_journal_id(id)?;
        // The reservation occupies the deployment key without a version that could pass a deployment CAS.
        let [key, marker] = reservation_fields();
        let reservation = Item::from([
            (key.to_owned(), AttributeValue::S(id.clone())),
            (marker.to_owned(), AttributeValue::Bool(true)),
        ]);
        let table = &self.tables.deployments;
        if self.put_new(table, reservation).await?
            || self.get(table, ID, &id).await?.as_ref().is_some_and(is_installed_journal)
        {
            return Ok(());
        }
        Err(exists("Deployment", &id))
    }

    async fn installed_journal_reservations(&self) -> Result<Vec<uuid::Uuid>> {
        let mut start = None;
        let mut reservations = Vec::new();
        loop {
            let page = self.client.scan().table_name(&self.tables.deployments)
                .projection_expression("#id")
                .filter_expression("#installed = :installed")
                .expression_attribute_names("#id", ID)
                .expression_attribute_names("#installed", INSTALLED_JOURNAL)
                .expression_attribute_values(":installed", AttributeValue::Bool(true))
                .set_exclusive_start_key(start)
                .send().await.map_err(|error| sdk_anomaly("Scan", error))?;
            for item in page.items() {
                reservations.push(crate::decode_installed_journal_id(string_attribute(item, ID)?)?);
            }
            start = page.last_evaluated_key().filter(|key| !key.is_empty()).cloned();
            if start.is_none() {
                return Ok(reservations);
            }
        }
    }

    async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord> {
        if record.fork_stage.is_some() { return Err(fork_stage_changed()); }
        self.insert_deployment(record, Vec::new(), None).await
    }

    async fn stage_fork(&self, child: DeploymentRecord, preparation: ForkPreparation) -> Result<DeploymentRecord> {
        require_deployment_name(&child.name)?;
        let parent = self.deployment(fork_parent_id(&child)?).await?.ok_or_else(fork_conflict)?;
        for _ in 0..8 {
            let (lineage, legacy) = self.read_lineage(&parent.lineage).await?;
            let mut stage = stage_fork_record(child.clone(), &parent, &lineage, preparation)?;
            stage.version = 1;
            let insert = Put::builder().table_name(&self.tables.deployments).set_item(Some(deployment_item(&stage, self.pack_parked_records)?))
                .condition_expression(NEW_ITEM).expression_attribute_names("#id", ID).build().map_err(build_fault)?;
            let mut items = vec![TransactWriteItem::builder().put(insert).build(),
                self.write_lineage(&parent.lineage, &lineage, lineage.version(), legacy)?];
            items.extend(self.journal_fence(None, &stage).await?);
            if self.write_retirement(items).await? { return Ok(stage); }
            if self.deployment(&child.id).await?.is_some() { return Err(exists("Deployment", &child.id)); }
        }
        Err(lineage_busy())
    }

    async fn publish_fork(&self, child: DeploymentRecord, expected_parent: Version, preparation: Option<ForkPublication>) -> Result<DeploymentRecord> {
        let id = fork_parent_id(&child)?;
        let parent = self.deployment(id).await?.ok_or_else(fork_conflict)?;
        let shared = shared_fork_lineage(&child, Some(&parent), expected_parent)?.is_some();
        let staged = if matches!(preparation, Some(ForkPublication::Hosted(_) | ForkPublication::Local(_))) { self.deployment(&child.id).await? } else { None };
        let stage_version = fork_stage_version(&child, staged.as_ref(), &parent, preparation)?;
        let mut checks = vec![self.require_deployment_version(id, expected_parent)?];
        if shared || preparation.is_some() {
            let (state, legacy) = self.read_lineage(&parent.lineage).await?;
            checks.push(self.write_lineage(&parent.lineage, &state.published(preparation, shared)?, state.version(), legacy)?);
        }
        self.insert_deployment(child, checks, stage_version).await
    }

    async fn lineage_state(&self, lineage: &str) -> Result<LineageState> {
        Ok(self.read_lineage(lineage).await?.0)
    }

    async fn change_lineage(&self, lineage: &str, change: LineageChange) -> Result<LineageState> {
        for _ in 0..8 {
            let (state, legacy) = self.read_lineage(lineage).await?;
            let next = state.changed(change.clone())?;
            if next == state { return Ok(state); }
            let item = self.write_lineage(lineage, &next, state.version(), legacy)?;
            match self.client.transact_write_items().transact_items(item).send().await {
                Ok(_) => return Ok(next),
                Err(error) => {
                    if let Some(aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(cancelled)) = error.as_service_error()
                        && cancelled.cancellation_reasons().iter().any(|reason|
                            matches!(reason.code(), Some("ConditionalCheckFailed" | "TransactionConflict")))
                    {
                        continue;
                    }
                    return Err(sdk_anomaly("TransactWriteItems", error));
                }
            }
        }
        Err(lineage_busy())
    }

    async fn authorize_owned_retirement(&self, id: &str, expected: Version, retirement: LineageRetirement) -> Result<LineageState> {
        let owner = self.deployment(id).await?.filter(|owner| owner.version == expected).ok_or_else(lineage_busy)?;
        let (current, legacy) = self.read_lineage(&owner.lineage).await?;
        let next = current.authorize_exact_owned(&owner, &retirement)?;
        if current == next { return Ok(next); }
        let items = vec![
            self.require_deployment_version(id, expected)?,
            self.write_lineage(&owner.lineage, &next, current.version(), legacy)?,
        ];
        if !self.write_retirement(items).await? { return Err(lineage_busy()); }
        Ok(next)
    }

    async fn settle_owned_version(&self, id: &str, expected: Version, retirement: LineageRetirement) -> Result<Cas<DeploymentRecord>> {
        let Some(owner) = self.deployment(id).await? else { return Ok(Cas::Conflict(None)); };
        if owner.version != expected { return Ok(Cas::Conflict(Some(owner))); }
        let (current, legacy) = self.read_lineage(&owner.lineage).await?;
        let (next, next_lineage) = current.settle_exact_owned(&owner, &retirement)?;
        require_transfer_update(&owner, &next)?;
        self.write_owner_and_lineage(next, expected, &current, &next_lineage, legacy).await
    }

    async fn begin_reference_disposal(&self, id: &str, expected: Version) -> Result<ReferenceDisposal> {
        let owner = self.deployment(id).await?.filter(|owner| owner.version == expected).ok_or_else(disposal_changed)?;
        let (current, legacy) = self.read_lineage(&owner.lineage).await?;
        let (permit, next) = current.begin_disposal(&owner)?;
        if current == next {
            return Ok(permit);
        }
        let items = vec![
            self.require_deployment_version(id, expected)?,
            self.write_lineage(&owner.lineage, &next, current.version(), legacy)?,
        ];
        if !self.write_retirement(items).await? {
            return Err(lineage_busy());
        }
        Ok(permit)
    }

    async fn settle_reference_disposal(&self, permit: &ReferenceDisposal, mut record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> {
        let Some(owner) = self.deployment(&record.id).await? else { return Ok(Cas::Conflict(None)); };
        if owner.version != expected {
            return Ok(Cas::Conflict(Some(owner)));
        }
        require_fork_stage_update(&owner, &record)?;
        require_transfer_update(&owner, &record)?;
        let (current, legacy) = self.read_lineage(&permit.lineage).await?;
        let next = current.finish_disposal(permit, &owner, &record)?;
        record.version = expected + 1;
        self.write_owner_and_lineage(record, expected, &current, &next, legacy).await
    }

    async fn hosted_task_stopped(&self, task: &HostedTask) -> Result<bool> {
        HostedTask::new(task.cluster_arn(), task.task_arn())?;
        let Some(item) = self.get(&self.tables.stopped_tasks, ID, task.task_arn()).await? else {
            return Ok(false);
        };
        Ok(string_attribute(&item, ID)? == task.task_arn()
            && string_attribute(&item, "cluster_arn")? == task.cluster_arn()
            && string_attribute(&item, "status")? == "STOPPED")
    }

    fn require_audience_reads(&self) -> Result<()> {
        if self.audience_reader.is_some() { Ok(()) }
        else { Err(Anomaly::unsupported("rtdb.directory/audience-transport", "The audience read transport was not installed")) }
    }

    async fn audience_holder(&self, id: &str) -> Result<Option<(DeploymentRecord, MemberRecord)>> {
        let reader = self.audience_reader.as_ref().ok_or_else(|| Anomaly::unsupported(
            "rtdb.directory/audience-transport", "The audience read transport was not installed",
        ))?;
        let item = get_item(reader, &self.tables.deployments, ID, id).await?;
        let Some(record) = deployment_record(item.as_ref())? else {
            return Ok(None);
        };
        let Some(holder) = &record.holder else { return Ok(None) };
        let member = get_item(reader, &self.tables.members, ID, holder.as_str()).await?;
        Ok(member.as_ref().map(decode_member).transpose()?.map(|member| (record, member)))
    }

    async fn audience_authority(&self, id: &str, member: &MemberId) -> Result<Option<(DeploymentRecord, MemberRecord)>> {
        let reader = self.audience_reader.as_ref().ok_or_else(|| Anomaly::unsupported(
            "rtdb.directory/audience-transport", "The audience read transport was not installed",
        ))?;
        let (record, member) = tokio::try_join!(
            get_item(reader, &self.tables.deployments, ID, id),
            get_item(reader, &self.tables.members, ID, member.as_str()),
        )?;
        let record = deployment_record(record.as_ref())?;
        let member = member.as_ref().map(decode_member).transpose()?;
        Ok(record.zip(member))
    }

    async fn prepare_audience_seat(&self, source: uuid::Uuid, index: usize, member: &MemberId,
        epoch: u64, task: Option<&HostedTask>) -> Result<Option<Box<dyn AudienceSeatClaim>>> {
        let deployment = ConditionCheck::builder().table_name(&self.tables.deployments)
            .key(ID, AttributeValue::S(source.to_string()))
            .condition_expression("#holder = :holder AND #epoch = :epoch")
            .expression_attribute_names("#holder", HOLDER).expression_attribute_names("#epoch", EPOCH)
            .expression_attribute_values(":holder", AttributeValue::S(member.as_str().to_owned()))
            .expression_attribute_values(":epoch", number(epoch))
            .build().map_err(build_fault)?;
        self.prepare_seat_claim(source, index, task, deployment).await
    }

    fn require_installed_audience_seats(&self) -> Result<()> { Ok(()) }

    async fn prepare_installed_audience_seat(&self, source: uuid::Uuid, index: usize,
        task: Option<&HostedTask>) -> Result<Option<Box<dyn AudienceSeatClaim>>> {
        let deployment = ConditionCheck::builder().table_name(&self.tables.deployments)
            .key(ID, AttributeValue::S(installed_journal_id(source)?))
            .condition_expression("#installed = :installed")
            .expression_attribute_names("#installed", INSTALLED_JOURNAL)
            .expression_attribute_values(":installed", AttributeValue::Bool(true))
            .build().map_err(build_fault)?;
        self.prepare_seat_claim(source, index, task, deployment).await
    }

    async fn deployment(&self, id: &str) -> Result<Option<DeploymentRecord>> {
        deployment_record(self.get(&self.tables.deployments, ID, id).await?.as_ref())
    }

    async fn deployment_by_name(&self, name: &str) -> Result<Option<DeploymentRecord>> {
        if reserved_deployment_name(name) { return Ok(None); }
        let Some(named) = self.get(&self.tables.deployment_names, NAME, name).await? else { return Ok(None) };
        self.deployment(string_attribute(&named, ID)?).await
    }

    async fn deployments(&self) -> Result<Vec<DeploymentRecord>> {
        self.scan(&self.tables.deployments).await?.iter().filter(|item| !is_installed_journal(item)).map(decode_deployment).collect()
    }

    /// The index lags writes, so a deployment claimed a moment ago may be missing; every hit is re-read strongly, so none is stale.
    async fn deployments_held_by(&self, member: &MemberId) -> Result<Vec<DeploymentRecord>> {
        const CONCURRENT_POINT_READS: usize = 16;
        let mut pages = self.client.query().table_name(&self.tables.deployments).index_name(HOLDER_INDEX)
            .key_condition_expression("#h = :holder").expression_attribute_names("#h", HOLDER)
            .expression_attribute_values(":holder", AttributeValue::S(member.0.clone()))
            .into_paginator().send();
        let mut ids = Vec::new();
        while let Some(page) = pages.next().await {
            for item in page.map_err(|error| sdk_anomaly("Query", error))?.items.unwrap_or_default() { ids.push(string_attribute(&item, ID)?.to_owned()); }
        }
        let mut ids = ids.into_iter();
        let mut reads = tokio::task::JoinSet::new();
        let mut held = Vec::new();
        loop {
            for id in ids.by_ref().take(CONCURRENT_POINT_READS - reads.len()) {
                let (client, table) = (self.client.clone(), self.tables.deployments.clone());
                reads.spawn(async move { get_item(&client, &table, ID, &id).await?.as_ref().map(decode_deployment).transpose() });
            }
            let Some(read) = reads.join_next().await else { break; };
            let read = read.map_err(|error| Anomaly::fault("rtdb.directory/task", error.to_string()))?;
            held.extend(read?.filter(|record| record.holder.as_ref() == Some(member)));
        }
        held.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(held)
    }

    async fn rebind_classic_flight(&self, previous: &classic_flight::Commission, replacement: &classic_flight::Commission) -> Result<()> {
        let id = replacement.intent.source.to_string();
        let source = self.deployment(&id).await?.ok_or_else(classic_flight::conflict)?;
        if classic_flight::commission(&source) == Some(replacement) { return Ok(()); }
        let excluded_id = previous.intent.destination.to_string();
        let scratch_id = replacement.intent.destination.to_string();
        let (excluded, scratch) = tokio::try_join!(self.deployment(&excluded_id), self.deployment(&scratch_id))?;
        let excluded = excluded.ok_or_else(classic_flight::conflict)?;
        let rebound = classic_flight::prepare_rebind(&source, &excluded, scratch.as_ref(), previous, replacement)?;
        let put = Put::builder().table_name(&self.tables.deployments)
            .set_item(Some(deployment_item(&rebound, self.pack_parked_records)?))
            .condition_expression(EXACT_VERSION)
            .expression_attribute_names("#v", VERSION)
            .expression_attribute_values(":expected", number(source.version))
            .build().map_err(build_fault)?;
        let replacement_check = if let Some(scratch) = scratch {
            self.require_deployment_version(&scratch_id, scratch.version)?
        } else {
            let missing = ConditionCheck::builder().table_name(&self.tables.deployments)
                .key(ID, AttributeValue::S(scratch_id))
                .condition_expression(NEW_ITEM)
                .expression_attribute_names("#id", ID)
                .build().map_err(build_fault)?;
            TransactWriteItem::builder().condition_check(missing).build()
        };
        let mut items = vec![
            self.require_deployment_version(&excluded_id, excluded.version)?,
            replacement_check,
            TransactWriteItem::builder().put(put).build(),
        ];
        items.extend(self.journal_fence(Some(&source), &rebound).await?);
        if self.write_retirement(items).await? || self.deployment(&id).await?.as_ref().and_then(classic_flight::commission) == Some(replacement) {
            return Ok(());
        }
        Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.directory/classic-flight-rebind",
            "The Classic Flight commission or a scratch revision changed during rebind"))
    }

    async fn fence_checkpoint_transfer(&self, selection: &CheckpointTransferSelection) -> Result<CheckpointTransferFence> {
        let source = self.deployment(&selection.stage.intent.source.to_string()).await?
            .ok_or_else(transfer_changed)?;
        let destination = self.deployment(&selection.stage.intent.destination.to_string()).await?
            .ok_or_else(transfer_changed)?;
        let (fenced, held, fence) = prepare_checkpoint_transfer(&source, &destination, selection)?;
        if fenced.version == source.version { return Ok(fence); }
        let mut items = Vec::with_capacity(2);
        for (record, expected) in [(&fenced, source.version), (&held, destination.version)] {
            let put = Put::builder().table_name(&self.tables.deployments)
                .set_item(Some(deployment_item(record, self.pack_parked_records)?))
                .condition_expression(EXACT_VERSION)
                .expression_attribute_names("#v", VERSION)
                .expression_attribute_values(":expected", number(expected))
                .build().map_err(build_fault)?;
            items.push(TransactWriteItem::builder().put(put).build());
        }
        items.extend(self.journal_fence(Some(&source), &fenced).await?);
        items.extend(self.journal_fence(Some(&destination), &held).await?);
        if !self.write_retirement(items).await? { return Err(transfer_changed()); }
        Ok(fence)
    }

    async fn record_checkpoint_transfer_terminal(&self, terminal: &CheckpointTransferTerminal) -> Result<CheckpointTransferTerminal> {
        let id = terminal.fence.destination.intent.source.to_string();
        let source = self.deployment(&id).await?.ok_or_else(transfer_changed)?;
        let next = prepare_checkpoint_transfer_terminal(&source, terminal)?;
        if next.version == source.version { return Ok(terminal.clone()); }
        let put = Put::builder().table_name(&self.tables.deployments)
            .set_item(Some(deployment_item(&next, self.pack_parked_records)?))
            .condition_expression(EXACT_VERSION)
            .expression_attribute_names("#v", VERSION)
            .expression_attribute_values(":expected", number(source.version))
            .build().map_err(build_fault)?;
        let mut items = vec![TransactWriteItem::builder().put(put).build()];
        items.extend(self.journal_fence(Some(&source), &next).await?);
        if !self.write_retirement(items).await? {
            return Err(transfer_changed());
        }
        Ok(terminal.clone())
    }

    async fn record_checkpoint_transfer_bootstrap(&self, bootstrap: &CheckpointTransferBootstrap) -> Result<CheckpointTransferBootstrap> {
        let intent = &bootstrap.terminal.fence.destination.intent;
        let source = self.deployment(&intent.source.to_string()).await?.ok_or_else(transfer_changed)?;
        let destination = self.deployment(&intent.destination.to_string()).await?.ok_or_else(transfer_changed)?;
        let next = prepare_checkpoint_transfer_bootstrap(&source, &destination, bootstrap)?;
        if next.version == destination.version { return Ok(bootstrap.clone()); }
        let put = Put::builder().table_name(&self.tables.deployments)
            .set_item(Some(deployment_item(&next, self.pack_parked_records)?))
            .condition_expression(EXACT_VERSION)
            .expression_attribute_names("#v", VERSION)
            .expression_attribute_values(":expected", number(destination.version))
            .build().map_err(build_fault)?;
        let source_check = self.require_deployment_version(&source.id, source.version)?;
        let mut items = vec![source_check, TransactWriteItem::builder().put(put).build()];
        items.extend(self.journal_fence(Some(&destination), &next).await?);
        if !self.write_retirement(items).await? {
            return Err(transfer_changed());
        }
        Ok(bootstrap.clone())
    }

    async fn reserve_checkpoint_transfer_replica(&self, replica: &CheckpointTransferReplica) -> Result<CheckpointTransferReplica> {
        let intent = &replica.bootstrap.terminal.fence.destination.intent;
        let source = self.deployment(&intent.source.to_string()).await?.ok_or_else(transfer_changed)?;
        let destination = self.deployment(&intent.destination.to_string()).await?.ok_or_else(transfer_changed)?;
        let next = prepare_checkpoint_transfer_replica(&source, &destination, replica)?;
        let reserved = next.checkpoint_transfer_replica.as_deref().ok_or_else(transfer_changed)?.clone();
        if next.version == destination.version { return Ok(reserved); }
        let put = Put::builder().table_name(&self.tables.deployments)
            .set_item(Some(deployment_item(&next, self.pack_parked_records)?))
            .condition_expression(EXACT_VERSION)
            .expression_attribute_names("#v", VERSION)
            .expression_attribute_values(":expected", number(destination.version))
            .build().map_err(build_fault)?;
        let source_check = self.require_deployment_version(&source.id, source.version)?;
        let mut items = vec![source_check, TransactWriteItem::builder().put(put).build()];
        items.extend(self.journal_fence(Some(&destination), &next).await?);
        if !self.write_retirement(items).await? {
            return Err(transfer_changed());
        }
        Ok(reserved)
    }

    async fn activate_checkpoint_transfer(&self, activation: &CheckpointTransferActivation) -> Result<CheckpointTransferActivation> {
        let intent = &activation.replica.bootstrap.terminal.fence.destination.intent;
        let source = self.deployment(&intent.source.to_string()).await?.ok_or_else(transfer_changed)?;
        let destination = self.deployment(&intent.destination.to_string()).await?.ok_or_else(transfer_changed)?;
        let next = prepare_checkpoint_transfer_activation(&source, &destination, activation)?;
        if next.version == destination.version { return Ok(activation.clone()); }
        let put = Put::builder().table_name(&self.tables.deployments)
            .set_item(Some(deployment_item(&next, self.pack_parked_records)?))
            .condition_expression(EXACT_VERSION)
            .expression_attribute_names("#v", VERSION)
            .expression_attribute_values(":expected", number(destination.version))
            .build().map_err(build_fault)?;
        let source_check = self.require_deployment_version(&source.id, source.version)?;
        let mut items = vec![source_check, TransactWriteItem::builder().put(put).build()];
        items.extend(self.journal_fence(Some(&destination), &next).await?);
        if !self.write_retirement(items).await? {
            return Err(transfer_changed());
        }
        Ok(activation.clone())
    }

    async fn replace_deployment(&self, mut record: DeploymentRecord, expected: Version) -> ReplaceOutcome<DeploymentRecord> {
        if !record.writer_handoffs.is_empty() || record.fork_stage.is_some() || record.classic_flight.is_some() || record.classic_catalog_drain.is_some() || record.checkpoint_transfer_fence.is_some() || record.checkpoint_transfer_terminal.is_some() || record.checkpoint_transfer_bootstrap.is_some() || record.checkpoint_transfer_replica.is_some() || record.checkpoint_transfer_activation.is_some() {
            let current = match self.deployment(&record.id).await {
                Ok(Some(current)) => current,
                Ok(None) => return ReplaceOutcome::Conflict(None),
                Err(error) => return ReplaceOutcome::Refused(error),
            };
            if current.version != expected { return ReplaceOutcome::Conflict(Some(current)); }
            if let Err(error) = require_fork_stage_update(&current, &record) { return ReplaceOutcome::Refused(error); }
            if let Err(error) = require_transfer_update(&current, &record) { return ReplaceOutcome::Refused(error); }
            if let Err(error) = crate::require_writer_handoff_update(&current, &record) { return ReplaceOutcome::Refused(error); }
        }
        if let Some(fenced) = self.replace_fenced_deployment(&record, expected).await { return fenced; }
        record.version = expected + 1;
        let name = record.name.clone();
        let item = match deployment_item(&record, self.pack_parked_records) {
            Ok(item) => item,
            Err(error) => return ReplaceOutcome::Refused(error),
        };
        match self.put_conditional(&self.tables.deployments, item, expected, Some(&name)).await {
            PutConditional::Applied => ReplaceOutcome::Applied(record),
            PutConditional::Refused(error) => ReplaceOutcome::Refused(error),
            PutConditional::Unknown(error) => ReplaceOutcome::Unknown(error),
            PutConditional::Conflict(None) => ReplaceOutcome::Conflict(None),
            PutConditional::Conflict(Some(current)) => {
                let current = match deployment_record(Some(&current)) {
                    Ok(Some(current)) => current,
                    Ok(None) => return ReplaceOutcome::Conflict(None),
                    Err(error) => return ReplaceOutcome::Refused(error),
                };
                if current.version == expected && current.name != name {
                    return ReplaceOutcome::Refused(Anomaly::incorrect("rtdb.directory/rename", "A deployment's name never changes"));
                }
                if current.version == expected {
                    if let Err(error) = require_fork_stage_update(&current, &record) { return ReplaceOutcome::Refused(error); }
                    if let Err(error) = require_transfer_update(&current, &record) { return ReplaceOutcome::Refused(error); }
                    if let Err(error) = crate::require_writer_handoff_update(&current, &record) { return ReplaceOutcome::Refused(error); }
                }
                ReplaceOutcome::Conflict(Some(current))
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
        let request = self.client.put_item().table_name(&self.tables.members).set_item(Some(member_item(&record)?))
            .condition_expression("#v = :expected AND (attribute_not_exists(#epoch) OR #epoch < :epoch OR (#epoch = :epoch AND #eligibility = :eligibility))")
            .expression_attribute_names("#v", VERSION).expression_attribute_names("#epoch", ELIGIBILITY_EPOCH)
            .expression_attribute_names("#eligibility", ELIGIBILITY)
            .expression_attribute_values(":expected", number(expected)).expression_attribute_values(":epoch", number(record.eligibility_epoch))
            .expression_attribute_values(":eligibility", eligibility(&record)?)
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld);
        match request.send().await {
            Ok(_) => Ok(Cas::Applied(record)),
            Err(error) => match error.as_service_error() {
                Some(aws_sdk_dynamodb::operation::put_item::PutItemError::ConditionalCheckFailedException(failed)) => {
                    let current = match failed.item.clone() {
                        Some(item) => Some(item),
                        None => self.get(&self.tables.members, ID, record.id.as_str()).await?,
                    };
                    let Some(current) = current.as_ref().map(decode_member).transpose()? else { return Ok(Cas::Conflict(None)) };
                    if current.version == expected { crate::require_epoch_follows(&current, &record)?; }
                    Ok(Cas::Conflict(Some(current)))
                }
                _ => Err(sdk_anomaly("PutItem", error)),
            },
        }
    }

    /// Outside a Tokio runtime there is nothing to follow the stream on, so there is no feed.
    fn deployment_changes(&self) -> Option<tokio::sync::broadcast::Receiver<DeploymentRecord>> {
        let receiver = self.changes.subscribe();
        self.change_feed()?;
        Some(receiver)
    }

    fn change_feed(&self) -> Option<&ChangeFeed> {
        let mut following = self.following.lock();
        if !*following {
            let runtime = tokio::runtime::Handle::try_current().ok()?;
            let follower = streams::Follower { client: self.client.clone(), streams: self.streams.clone(), table: self.tables.deployments.clone(),
                changes: Arc::downgrade(&self.changes) };
            runtime.spawn(follower.run());
            *following = true;
        }
        Some(&self.changes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tokio::sync::broadcast;

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

    #[tokio::test]
    async fn a_stale_lineage_revision_cancels_the_entire_fork_publication() {
        let Some(directory) = local().await else { return };
        let parent = directory.create_deployment(DeploymentRecord::new("parent", "acme", Vec::new(), 0)).await.unwrap();
        let child = |name| {
            let mut child = DeploymentRecord::new(name, &parent.org, Vec::new(), 0);
            child.parent = Some(parent.id.clone());
            child.lineage.clone_from(&parent.lineage);
            child
        };
        let stale = directory.write_lineage(&parent.lineage, &LineageState::default().published(None, true).unwrap(), 0, true).unwrap();
        directory.create_fork(child("first"), parent.version).await.unwrap();

        let second = child("second");
        let error = directory.insert_deployment(second.clone(), vec![stale], None).await.expect_err("a stale reference revision cannot publish a child");
        assert_eq!(error.code, "rtdb.directory/fork-parent-changed");
        assert!(directory.deployment(&second.id).await.unwrap().is_none());
        assert!(directory.deployment_by_name(&second.name).await.unwrap().is_none());
        assert_eq!(directory.shared_reference_revision(&parent.lineage).await.unwrap(), 1);

        directory.create_fork(second, parent.version).await.unwrap();
        assert_eq!(directory.shared_reference_revision(&parent.lineage).await.unwrap(), 2);
    }

    fn parked_record() -> DeploymentRecord {
        DeploymentRecord::new("parked-999999", "bench", (0..3).map(|_| MemberId::fresh()).collect(), 1_795_224_375_000)
    }

    #[test]
    fn common_parked_items_round_trip_below_the_record_budget() {
        let pristine = parked_record();
        let mut reopened = pristine.clone();
        reopened.epoch = 17;
        reopened.version = 65;
        reopened.checkpoint = Some(crate::Checkpoint { block: 87, boundary: 34_567, source: 31_234, system_time: 1_795_224_375_000_000, root_sha256: Some([42; 32]) });
        reopened.recovery_manifest_sha256 = Some([43; 32]);
        reopened.storage_namespaces = Some(crate::StorageNamespaces { objects: uuid::Uuid::new_v4(), keys: [42; 32] });
        let mut hosted = reopened.clone();
        hosted.replica_journals.extend(["10.42.0.10:7654", "10.42.1.10:7654", "10.42.2.10:7654"].map(rtdb_types::JournalEndpoint::from));
        for (name, record) in [("pristine", &pristine), ("previously active", &reopened), ("with journal custody", &hosted)] {
            let compact = deployment_item(record, true).unwrap();
            assert!(matches!(compact[RECORD], AttributeValue::B(_)));
            assert_eq!(&decode_deployment(&compact).unwrap(), record);
            let item_size = |item: &Item| item.iter().map(|(name, value)| name.len() + match value {
                AttributeValue::S(text) => text.len(),
                AttributeValue::B(bytes) => bytes.as_ref().len(),
                AttributeValue::N(number) => number.len().div_ceil(2) + 1,
                other => panic!("unexpected item attribute {other:?}"),
            }).sum::<usize>();
            let json = deployment_item(record, false).unwrap();
            assert_eq!(&decode_deployment(&json).unwrap(), record);
            println!("{name}: packed item {} bytes, JSON item {} bytes", item_size(&compact), item_size(&json));
            let mut unreceipted = record.clone();
            if let Some(checkpoint) = &mut unreceipted.checkpoint { checkpoint.root_sha256 = None; }
            unreceipted.recovery_manifest_sha256 = None;
            let unreceipted = deployment_item(&unreceipted, true).unwrap();
            let legacy_size = item_size(&unreceipted);
            assert!(legacy_size < 300);
            let digest_bytes = if record.checkpoint.is_some_and(|checkpoint| checkpoint.root_sha256.is_some()) { 34 } else { 0 };
            let manifest_bytes = if record.recovery_manifest_sha256.is_some() { 34 } else { 0 };
            assert_eq!(item_size(&compact), legacy_size + digest_bytes + manifest_bytes);
        }

        reopened.org = "other".into();
        reopened.ensemble[0] = MemberId("legacy-address-based-member".into());
        let fallback = deployment_item(&reopened, true).unwrap();
        assert!(matches!(fallback[RECORD], AttributeValue::S(_)));
        assert_eq!(decode_deployment(&fallback).unwrap(), reopened);
    }

    #[test]
    fn a_parked_inventory_preserves_its_journal_identity_in_both_formats() {
        let mut record = parked_record();
        record.replica_journals.insert(rtdb_types::JournalEndpoint::Member {
            address: "10.42.0.10:7654".into(),
            journal: record.ensemble[0].0.clone(),
        });
        record.replica_journals.insert("10.42.1.10:7654".into());
        let compact = deployment_item(&record, true).unwrap();
        let expanded = deployment_item(&record, false).unwrap();
        assert!(matches!(&compact[RECORD], AttributeValue::B(bytes) if bytes.as_ref()[0] == 2));
        assert_eq!(decode_deployment(&compact).unwrap(), record);
        assert_eq!(decode_deployment(&expanded).unwrap(), record);
    }

    #[test]
    fn parked_items_preserve_ordered_index_fences_in_json() {
        let mut with_index = parked_record();
        with_index.ordered_indexes.insert("index".into(), crate::OrderedIndexStorage {
            oldest_generation: 7, dropped: true, ..Default::default()
        });
        let mut with_catalog = parked_record();
        with_catalog.ordered_index_catalogs.insert("store".into(), crate::OrderedIndexCatalog {
            through: 11, active: Default::default(), prior_prefixes_discovered: true,
        });
        for record in [with_index, with_catalog] {
            let item = deployment_item(&record, true).unwrap();
            assert!(matches!(item[RECORD], AttributeValue::S(_)));
            assert_eq!(decode_deployment(&item).unwrap(), record);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn packed_and_legacy_items_are_readable_by_both_read_modes_and_the_change_feed() {
        let Some(reader) = local().await else { return };
        let writer = DynamoDirectory::new(reader.client.clone(), reader.streams.clone(), reader.tables.clone()).with_packed_parked_records();
        let legacy = reader.create_deployment(DeploymentRecord::new("legacy", "bench", vec![], 0)).await.unwrap();
        assert_eq!(writer.deployment_by_name("legacy").await.unwrap().as_ref(), Some(&legacy));
        let mut feed = reader.deployment_changes().unwrap();
        await_versions(&mut feed, &[legacy]).await;

        let packed = writer.create_deployment(parked_record()).await.unwrap();
        await_versions(&mut feed, std::slice::from_ref(&packed)).await;
        let raw = reader.get(&reader.tables.deployments, ID, &packed.id).await.unwrap().unwrap();
        assert!(matches!(raw[RECORD], AttributeValue::B(_)));
        assert_eq!(reader.deployment_by_name(&packed.name).await.unwrap().as_ref(), Some(&packed));
        let mut next = packed.clone();
        let holder = MemberId::fresh();
        next.holder = Some(holder.clone());
        next.epoch = 1;
        let held = writer.replace_deployment(next, packed.version).await.applied().unwrap();
        assert_eq!(reader.deployments_held_by(&holder).await.unwrap(), [held.clone()]);
        let mut parked = crate::park(&held, &holder, held.epoch).unwrap();
        parked.checkpoint = Some(crate::Checkpoint { block: 0, boundary: 2, source: 1, system_time: 65, root_sha256: None });
        let parked = writer.replace_deployment(parked, held.version).await.applied().unwrap();
        await_versions(&mut feed, std::slice::from_ref(&parked)).await;
        assert_eq!(reader.deployment(&parked.id).await.unwrap().as_ref(), Some(&parked));
        assert!(matches!(writer.replace_deployment(packed, held.version).await, ReplaceOutcome::Conflict(Some(current)) if current == parked));
    }

    #[tokio::test]
    async fn hosted_stop_evidence_matches_the_exact_task_and_terminal_state() {
        let Some(directory) = local().await else { return };
        let cluster = "arn:aws:ecs:us-east-1:111122223333:cluster/fixture";
        let task = HostedTask::new(cluster, "arn:aws:ecs:us-east-1:111122223333:task/fixture/84d0704dd3e548d496883a065ccc01c7").unwrap();
        assert!(!directory.hosted_task_stopped(&task).await.unwrap());

        for (observed_cluster, status, accepted) in [
            ("another-cluster", "STOPPED", false),
            (cluster, "STOPPING", false),
            (cluster, "STOPPED", true),
        ] {
            directory.client.put_item().table_name(&directory.tables().stopped_tasks)
                .item(ID, AttributeValue::S(task.task_arn().into()))
                .item("cluster_arn", AttributeValue::S(observed_cluster.into()))
                .item("status", AttributeValue::S(status.into()))
                .send().await.unwrap();
            assert_eq!(directory.hosted_task_stopped(&task).await.unwrap(), accepted);
        }
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
        let held = directory.replace_deployment(held, created.version).await.applied().unwrap();
        assert_eq!(directory.deployments_held_by(&holder).await.unwrap(), vec![held.clone()]);
        let parked = crate::park(&held, &holder, held.epoch).unwrap();
        directory.replace_deployment(parked, held.version).await.applied().unwrap();
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

    #[derive(Debug)]
    struct CountScans(Arc<std::sync::atomic::AtomicUsize>);

    impl aws_sdk_dynamodb::config::Intercept for CountScans {
        fn name(&self) -> &'static str { "CountScans" }
        fn read_before_transmit(
            &self,
            context: &aws_sdk_dynamodb::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
            _: &aws_sdk_dynamodb::config::RuntimeComponents,
            _: &mut aws_sdk_dynamodb::config::ConfigBag,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if context.request().headers().get("x-amz-target").is_some_and(|target| target.ends_with(".Scan")) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct HoldIterator(Arc<std::sync::atomic::AtomicBool>);

    impl aws_sdk_dynamodbstreams::config::Intercept for HoldIterator {
        fn name(&self) -> &'static str { "HoldIterator" }
        fn read_before_transmit(
            &self,
            context: &aws_sdk_dynamodbstreams::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
            _: &aws_sdk_dynamodbstreams::config::RuntimeComponents,
            _: &mut aws_sdk_dynamodbstreams::config::ConfigBag,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if context.request().headers().get("x-amz-target").is_some_and(|target| target.ends_with(".GetShardIterator"))
                && !self.0.load(std::sync::atomic::Ordering::Relaxed)
            {
                return Err("the stream iterator is held until the first update commits".into());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_keyed_dynamodb_watch_recovers_an_update_during_startup_without_scanning_the_table() {
        let Some(writer) = local().await else { return };
        let mut record = writer.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let scans = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let client = Client::from_conf(writer.client.config().to_builder().interceptor(CountScans(scans.clone())).build());
        let streams = StreamsClient::from_conf(writer.streams.config().to_builder().interceptor(HoldIterator(ready.clone())).build());
        let reader = Arc::new(DynamoDirectory::new(client, streams, writer.tables.clone()));
        let mut watched = crate::watch_deployment(reader, record.id.clone(), Duration::from_secs(60));
        drop(watched.wait_for(|current| current.is_some()).await.unwrap());

        record.epoch = 1;
        let version = record.version;
        writer.replace_deployment(record, version).await.applied().unwrap();
        ready.store(true, std::sync::atomic::Ordering::Relaxed);
        drop(tokio::time::timeout(Duration::from_secs(10), watched.wait_for(|record| {
            record.as_ref().is_some_and(|record| record.epoch == 1)
        })).await.unwrap().unwrap());
        assert_eq!(scans.load(std::sync::atomic::Ordering::Relaxed), 0);
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
        let changed = directory.replace_deployment(changed, created.version).await.applied().unwrap();
        tokio::time::timeout(Duration::from_secs(5), watched.changed()).await.expect("the change arrives long before the next poll").unwrap();
        assert_eq!(watched.borrow().as_ref().unwrap().version, changed.version);
    }

    #[test]
    fn write_error_classification_distinguishes_refusals_from_unknown_outcomes() {
        use aws_sdk_dynamodb::error::ErrorMetadata;
        use aws_sdk_dynamodb::operation::put_item::PutItemError;
        use aws_sdk_dynamodb::types::error::{InternalServerError, ProvisionedThroughputExceededException, ResourceNotFoundException};

        let meta = |code: &str| ErrorMetadata::builder().code(code).build();

        let internal: SdkError<PutItemError, ()> = SdkError::service_error(
            PutItemError::InternalServerError(InternalServerError::builder().meta(meta("InternalServerError")).build()),
            (),
        );
        assert!(write_error_is_unknown(&internal));

        let throttled: SdkError<PutItemError, ()> = SdkError::service_error(
            PutItemError::ProvisionedThroughputExceededException(
                ProvisionedThroughputExceededException::builder().meta(meta("ProvisionedThroughputExceededException")).build(),
            ),
            (),
        );
        assert!(!write_error_is_unknown(&throttled));

        let missing_table: SdkError<PutItemError, ()> = SdkError::service_error(
            PutItemError::ResourceNotFoundException(
                ResourceNotFoundException::builder().meta(meta("ResourceNotFoundException")).build(),
            ),
            (),
        );
        assert!(!write_error_is_unknown(&missing_table));

        let construction: SdkError<PutItemError, ()> = SdkError::construction_failure("bad request");
        assert!(!write_error_is_unknown(&construction));

        let timeout: SdkError<PutItemError, ()> = SdkError::timeout_error("timed out");
        assert!(write_error_is_unknown(&timeout));
    fn seat_lease_expiry_marks_unreleased_charged_seat_reclaimable() {
        assert!(!seat_lease_expired(None, 1_000));
        assert!(!seat_lease_expired(Some(2_000), 1_999));
        assert!(seat_lease_expired(Some(2_000), 2_000));
        assert!(seat_lease_expired(Some(2_000), 2_500));
        assert!(matches!(
            claim_predecessor(SeatRevision::Charged { revision: 42, stopped: seat_lease_expired(Some(2_000), 1_999) }),
            None
        ));
        assert!(matches!(
            claim_predecessor(SeatRevision::Charged { revision: 42, stopped: seat_lease_expired(Some(2_000), 2_000) }),
            Some(SeatPredecessor::Exact(42))
        ));
    }
}
