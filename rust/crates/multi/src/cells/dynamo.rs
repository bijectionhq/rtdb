//! The cell map on DynamoDB within one region, with strongly consistent lookups and conditional placement writes.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::client::Waiters;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, Put, ScalarAttributeType, TransactWriteItem};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use super::CellMap;

const ID: &str = "id";
const GATEWAY: &str = "gateway";
const CELL: &str = "cell";

type Item = HashMap<String, AttributeValue>;

pub struct DynamoCellMap {
    client: Client,
    cells: String,
    placements: String,
}

impl DynamoCellMap {
    pub fn new(client: Client, prefix: &str) -> Self {
        Self { client, cells: format!("{prefix}-cells"), placements: format!("{prefix}-placements") }
    }

    /// Creates the two on-demand tables for a local or test cell map.
    /// Production tables can be provisioned before starting any gateway.
    pub async fn create_tables(&self) -> Result<()> {
        for table in [&self.cells, &self.placements] {
            let key = KeySchemaElement::builder().attribute_name(ID).key_type(KeyType::Hash).build().map_err(invalid)?;
            let attribute = AttributeDefinition::builder().attribute_name(ID).attribute_type(ScalarAttributeType::S).build().map_err(invalid)?;
            let created = self.client.create_table().table_name(table).key_schema(key).attribute_definitions(attribute)
                .billing_mode(BillingMode::PayPerRequest).send().await;
            match created {
                Ok(_) => {}
                Err(error) if error.as_service_error().is_some_and(|service| service.is_resource_in_use_exception()) => {}
                Err(error) => return Err(unavailable(error)),
            }
            self.client.wait_until_table_exists().table_name(table).wait(Duration::from_secs(60)).await.map_err(unavailable)?;
        }
        Ok(())
    }

    async fn lookup(&self, table: &str, id: &str, field: &str) -> Result<Option<String>> {
        let response = self.client.get_item().table_name(table).key(ID, string(id)).consistent_read(true).send().await.map_err(unavailable)?;
        response.item().map(|item| value(item, field)).transpose()
    }
}

#[async_trait]
impl CellMap for DynamoCellMap {
    async fn register_cell(&self, cell: &str, gateway: &str) -> Result<()> {
        self.client.put_item().table_name(&self.cells).item(ID, string(cell)).item(GATEWAY, string(gateway))
            .send().await.map_err(unavailable)?;
        Ok(())
    }

    async fn gateway(&self, cell: &str) -> Result<Option<String>> {
        self.lookup(&self.cells, cell, GATEWAY).await
    }

    async fn cells(&self) -> Result<BTreeMap<String, String>> {
        let mut addresses = BTreeMap::new();
        let mut pages = self.client.scan().table_name(&self.cells).consistent_read(true).into_paginator().send();
        while let Some(page) = pages.next().await {
            for item in page.map_err(unavailable)?.items() {
                addresses.insert(value(item, ID)?, value(item, GATEWAY)?);
            }
        }
        Ok(addresses)
    }

    async fn cell_of(&self, deployment: &str) -> Result<Option<String>> {
        self.lookup(&self.placements, deployment, CELL).await
    }

    async fn assign(&self, deployment: &str, cell: &str, expected: Option<&str>) -> Result<bool> {
        let put = Put::builder().table_name(&self.placements).item(ID, string(deployment)).item(CELL, string(cell));
        let put = match expected {
            Some(current) => put.condition_expression("#cell = :current").expression_attribute_names("#cell", CELL)
                .expression_attribute_values(":current", string(current)),
            None => put.condition_expression("attribute_not_exists(#id)").expression_attribute_names("#id", ID),
        }.build().map_err(invalid)?;
        // A retry after a lost response must recover the write's result as well as its effects.
        let written = self.client.transact_write_items()
            .client_request_token(uuid::Uuid::new_v4().to_string())
            .transact_items(TransactWriteItem::builder().put(put).build()).send().await;
        match written {
            Ok(_) => Ok(true),
            Err(error) if matches!(error.as_service_error(),
                Some(TransactWriteItemsError::TransactionCanceledException(cancelled))
                    if cancelled.cancellation_reasons().first().and_then(|reason| reason.code()) == Some("ConditionalCheckFailed")) => Ok(false),
            Err(error) => Err(unavailable(error)),
        }
    }
}

fn string(value: &str) -> AttributeValue { AttributeValue::S(value.to_owned()) }

fn value(item: &Item, field: &str) -> Result<String> {
    item.get(field).and_then(|value| value.as_s().ok()).cloned()
        .ok_or_else(|| invalid(format!("Cell map item has no string {field} attribute")))
}

fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.multi/cell-map-item", error.to_string()) }
fn unavailable(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/cell-map-dynamodb", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dynamodb_placements_survive_client_restart_and_serialize_concurrent_moves() {
        let Ok(endpoint) = std::env::var("RTDB_DYNAMODB_ENDPOINT") else { return };
        let client = rtdb_directory::DynamoDirectory::local_client(&endpoint);
        let prefix = format!("map-{}", uuid::Uuid::new_v4().simple());
        let map = DynamoCellMap::new(client.clone(), &prefix);
        map.create_tables().await.unwrap();
        super::super::tests::contract(&map).await;
        assert!(map.assign("shop", "eu-1", None).await.unwrap());
        drop(map);

        let left = DynamoCellMap::new(client.clone(), &prefix);
        let right = DynamoCellMap::new(client, &prefix);
        let (west, east) = tokio::join!(left.assign("shop", "us-1", Some("eu-1")), right.assign("shop", "asia-1", Some("eu-1")));
        let (west, east) = (west.unwrap(), east.unwrap());
        assert_ne!(west, east);
        assert_eq!(left.cell_of("shop").await.unwrap().as_deref(), Some(if west { "us-1" } else { "asia-1" }));
        for table in [left.cells, left.placements] { left.client.delete_table().table_name(table).send().await.unwrap(); }
    }
}
