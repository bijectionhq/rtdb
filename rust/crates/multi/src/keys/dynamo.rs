//! A regional key vault independent of RTDB's object and log backups.
use aws_sdk_dynamodb::{Client, primitives::Blob, types::AttributeValue};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use super::{KEY_BYTES, KeyService, malformed, new_key};

const ID: &str = "id";
const KEY: &str = "material";
const DESTROYED: &str = "destroyed";

#[derive(Clone)]
pub struct DynamoKeyService {
    client: Client,
    table: String,
}

impl DynamoKeyService {
    pub fn new(client: Client, table: String) -> Result<Self> {
        if !(3..=255).contains(&table.len()) || !table.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.')) {
            return Err(Anomaly::incorrect("rtdb.multi/key-table", "Invalid deployment key table"));
        }
        Ok(Self { client, table })
    }

    async fn existing(&self, id: &str) -> Result<Option<[u8; KEY_BYTES]>> {
        let item = self.client.get_item().table_name(&self.table).key(ID, string(id))
            .consistent_read(true).send().await.map_err(|_| unavailable("read"))?;
        let Some(item) = item.item() else { return Ok(None); };
        if item.get(DESTROYED).and_then(|value| value.as_bool().ok()) == Some(&true) {
            return Err(Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/key-destroyed", "The deployment key was destroyed"));
        }
        let key = item.get(KEY).and_then(|value| value.as_b().ok()).ok_or_else(malformed)?;
        Ok(Some(key.as_ref().try_into().map_err(|_| malformed())?))
    }

    async fn insert(&self, id: &str, key: [u8; KEY_BYTES]) -> Result<bool> {
        let result = self.client.put_item().table_name(&self.table)
            .item(ID, string(id)).item(KEY, AttributeValue::B(Blob::new(key.to_vec())))
            .condition_expression("attribute_not_exists(#id)").expression_attribute_names("#id", ID)
            .send().await;
        match result {
            Ok(_) => Ok(true),
            Err(error) if error.as_service_error().is_some_and(|error| error.is_conditional_check_failed_exception()) => Ok(false),
            Err(_) => Err(unavailable("write")),
        }
    }
}

#[async_trait::async_trait]
impl KeyService for DynamoKeyService {
    fn retain_deployment_delivery(&self, id: String, material: &[u8; KEY_BYTES])
        -> Result<std::sync::Arc<dyn rtdb_storage::sealing::ObjectCipher>>
    {
        Ok(super::DeploymentDelivery::new(std::sync::Arc::new(self.clone()), id, material))
    }

    async fn grant_deployment_key(&self, id: &str, binding: [u8; KEY_BYTES]) -> rtdb_storage::Result<super::UnitOutput> {
        let material = self.existing_key(id).await.map_err(super::journal::key_error)?;
        super::delivery::require_deployment_key(material, binding)?;
        Ok(super::UnitOutput::Granted)
    }

    fn custody_namespace(&self) -> Result<String> {
        Ok(format!("dynamodb:{}", self.table))
    }

    async fn existing_key(&self, deployment: &str) -> Result<Option<[u8; KEY_BYTES]>> {
        self.existing(deployment).await
    }

    async fn key(&self, id: &str) -> Result<[u8; KEY_BYTES]> {
        if let Some(key) = self.existing(id).await? { return Ok(key); }
        let created = new_key()?;
        if self.insert(id, created).await? { return Ok(created); }
        self.existing(id).await?.ok_or_else(|| unavailable("recover"))
    }

    async fn copy(&self, from: &str, to: &str) -> Result<()> {
        self.install(to, <Self as KeyService>::existing(self, from).await?).await
    }

    async fn destroy(&self, id: &str) -> Result<()> {
        self.client.update_item().table_name(&self.table).key(ID, string(id))
            .update_expression("SET #destroyed = :yes REMOVE #material")
            .expression_attribute_names("#destroyed", DESTROYED)
            .expression_attribute_names("#material", KEY)
            .expression_attribute_values(":yes", AttributeValue::Bool(true))
            .send().await.map_err(|_| unavailable("destroy"))?;
        rtdb_storage::sealing::drain_unit_delivery().await;
        Ok(())
    }

    async fn install(&self, id: &str, key: [u8; KEY_BYTES]) -> Result<()> {
        if self.insert(id, key).await? || self.existing(id).await? == Some(key) {
            return Ok(());
        }
        Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/key-conflict", "The deployment already has a different key"))
    }
}

fn string(value: &str) -> AttributeValue { AttributeValue::S(value.to_owned()) }

fn unavailable(operation: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/key-vault", format!("Deployment key {operation} could not be confirmed"))
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType};

    use super::*;

    #[tokio::test]
    #[ignore = "needs DynamoDB Local at RTDB_DYNAMODB_ENDPOINT"]
    async fn destroyed_deployment_keys_are_unavailable_even_after_reopen() {
        let endpoint = std::env::var("RTDB_DYNAMODB_ENDPOINT").unwrap();
        let client = rtdb_directory::DynamoDirectory::local_client(&endpoint);
        let table = format!("rtdb-key-test-{}", uuid::Uuid::new_v4().simple());
        client.create_table().table_name(&table).billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(AttributeDefinition::builder().attribute_name(ID).attribute_type(ScalarAttributeType::S).build().unwrap())
            .key_schema(KeySchemaElement::builder().attribute_name(ID).key_type(KeyType::Hash).build().unwrap())
            .send().await.unwrap();
        let vault = DynamoKeyService::new(client.clone(), table.clone()).unwrap();
        let (first, second) = tokio::join!(vault.key("deployment"), vault.key("deployment"));
        let key = first.unwrap();
        assert_eq!(second.unwrap(), key);
        vault.install("deployment", key).await.unwrap();
        vault.destroy("deployment").await.unwrap();
        vault.destroy("deployment").await.unwrap();
        let reopened = DynamoKeyService::new(client.clone(), table.clone()).unwrap();
        assert_eq!(reopened.key("deployment").await.unwrap_err().code, "rtdb.multi/key-destroyed");
        assert_eq!(reopened.install("deployment", key).await.unwrap_err().code, "rtdb.multi/key-destroyed");
        assert!(reopened.key("another").await.is_ok());
        client.delete_table().table_name(&table).send().await.unwrap();
    }
}
