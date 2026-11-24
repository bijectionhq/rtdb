//! A regional key vault independent of RTDB's object and log backups.
use std::{sync::Arc, time::Instant};

use aws_sdk_dynamodb::{
    Client,
    primitives::Blob,
    types::{AttributeValue, ConditionCheck, Put, TransactWriteItem},
};
use rtdb_storage::{IoRegion, ObjectKey};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use super::{
    KEY_BYTES, KeyService, UnitOperation, UnitOutput, UnitScope, UnitSlot,
    archive::{
        ArchiveKey, CaptureCryptography, matches_authority_key, tracked_archive_key,
        with_archive_unit,
    },
    journal::key_error,
    malformed, new_key,
    unit::{self, OPERATION_LIMIT},
};

const ID: &str = "id";
const KEY: &str = "material";
const DESTROYED: &str = "destroyed";

#[derive(Clone)]
pub struct DynamoKeyService {
    client: Client,
    table: String,
    reads: Arc<IoRegion>,
}

impl DynamoKeyService {
    pub fn new(client: Client, table: String) -> Result<Self> {
        if !(3..=255).contains(&table.len())
            || !table
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(Anomaly::incorrect(
                "rtdb.multi/key-table",
                "Invalid deployment key table",
            ));
        }
        Ok(Self { client, table, reads: IoRegion::new() })
    }

    async fn read_item(&self, id: &str, operation: &'static str) -> Result<aws_sdk_dynamodb::operation::get_item::GetItemOutput> {
        self.reads.read_async(Arc::new(self.clone()), id.to_owned(), |vault, id| async move {
            Ok(vault.client.get_item().table_name(&vault.table).key(ID, string(&id)).consistent_read(true).send().await)
        }).await.map_err(|_| unavailable(operation))?.map_err(|_| unavailable(operation))
    }

    async fn existing(&self, id: &str) -> Result<Option<[u8; KEY_BYTES]>> {
        let item = self.read_item(id, "read").await?;
        let Some(item) = item.item() else {
            return Ok(None);
        };
        if item.get(DESTROYED).and_then(|value| value.as_bool().ok()) == Some(&true) {
            return Err(Anomaly::new(
                AnomalyCategory::Forbidden,
                "rtdb.multi/key-destroyed",
                "The deployment key was destroyed",
            ));
        }
        let key = item
            .get(KEY)
            .and_then(|value| value.as_b().ok())
            .ok_or_else(malformed)?;
        Ok(Some(key.as_ref().try_into().map_err(|_| malformed())?))
    }

    async fn insert(&self, id: &str, key: [u8; KEY_BYTES]) -> Result<bool> {
        let result = self
            .client
            .put_item()
            .table_name(&self.table)
            .item(ID, string(id))
            .item(KEY, AttributeValue::B(Blob::new(key.to_vec())))
            .condition_expression("attribute_not_exists(#id)")
            .expression_attribute_names("#id", ID)
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(_) => Err(unavailable("write")),
        }
    }

    async fn withdraw_item(&self, id: &str) -> Result<()> {
        self.client
            .update_item()
            .table_name(&self.table)
            .key(ID, string(id))
            .update_expression("SET #destroyed = :yes REMOVE #material")
            .expression_attribute_names("#destroyed", DESTROYED)
            .expression_attribute_names("#material", KEY)
            .expression_attribute_values(":yes", AttributeValue::Bool(true))
            .send()
            .await
            .map_err(|_| unavailable("destroy"))?;
        let observed = self.read_item(id, "read back withdrawal").await?;
        let item = observed
            .item()
            .ok_or_else(|| unavailable("read back withdrawal"))?;
        if item.get(DESTROYED).and_then(|value| value.as_bool().ok()) != Some(&true)
            || item.contains_key(KEY)
        {
            return Err(unavailable("read back withdrawal"));
        }
        Ok(())
    }

    async fn unit_material(&self, slot: &UnitSlot, provision: bool) -> Result<[u8; KEY_BYTES]> {
        let namespace = slot.scope.namespace();
        let id = slot.id();
        self.existing(&namespace).await?;
        if provision && self.existing(&id).await?.is_none() {
            let fence = ConditionCheck::builder()
                .table_name(&self.table)
                .key(ID, string(&namespace))
                .condition_expression("attribute_not_exists(#id)")
                .expression_attribute_names("#id", ID)
                .build()
                .map_err(|_| malformed())?;
            let slot_put = Put::builder()
                .table_name(&self.table)
                .item(ID, string(&id))
                .item(KEY, AttributeValue::B(Blob::new(new_key()?.to_vec())))
                .condition_expression("attribute_not_exists(#id)")
                .expression_attribute_names("#id", ID)
                .build()
                .map_err(|_| malformed())?;
            // An ambiguous create can be resolved by reading the one write-once slot.
            let _ = self
                .client
                .transact_write_items()
                .transact_items(TransactWriteItem::builder().condition_check(fence).build())
                .transact_items(TransactWriteItem::builder().put(slot_put).build())
                .send()
                .await;
        }
        let material = self.existing(&id).await?;
        self.existing(&namespace).await?;
        material.ok_or_else(|| unavailable("resolve immutable unit"))
    }

    async fn close_units(&self, scope: UnitScope) -> Result<()> {
        let namespace = scope.namespace();
        self.withdraw_item(&namespace).await?;
        rtdb_storage::sealing::drain_unit_delivery().await;
        let prefix = format!("{namespace}-");
        let mut start = None;
        loop {
            let page = self.reads.read_async(Arc::new(self.clone()), (prefix.clone(), start), |vault, (prefix, start)| async move {
                Ok(vault.client.scan()
                    .table_name(&vault.table)
                    .consistent_read(true)
                    .set_exclusive_start_key(start)
                    .limit(128)
                    .filter_expression("begins_with(#id, :prefix)")
                    .projection_expression("#id")
                    .expression_attribute_names("#id", ID)
                    .expression_attribute_values(":prefix", string(&prefix))
                    .send()
                    .await)
            }).await.map_err(|_| unavailable("read unit census"))?
                .map_err(|_| unavailable("read unit census"))?;
            for item in page.items() {
                let id = item
                    .get(ID)
                    .and_then(|id| id.as_s().ok())
                    .ok_or_else(malformed)?;
                self.withdraw_item(id).await?;
            }
            start = page.last_evaluated_key;
            if start.as_ref().is_none_or(|key| key.is_empty()) {
                break;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl KeyService for DynamoKeyService {
    async fn grant_deployment_key(
        &self,
        id: &str,
        binding: [u8; KEY_BYTES],
    ) -> rtdb_storage::Result<super::UnitOutput> {
        super::require_general_key_namespace(id).map_err(key_error)?;
        let material = self
            .existing_key(id)
            .await
            .map_err(super::journal::key_error)?;
        super::delivery::require_deployment_key(material, binding)?;
        Ok(super::UnitOutput::Granted)
    }

    fn custody_namespace(&self) -> Result<String> {
        Ok(format!("dynamodb:{}", self.table))
    }

    async fn existing_key(&self, deployment: &str) -> Result<Option<[u8; KEY_BYTES]>> {
        super::require_general_key_namespace(deployment)?;
        self.existing(deployment).await
    }

    async fn key(&self, id: &str) -> Result<[u8; KEY_BYTES]> {
        super::require_general_key_namespace(id)?;
        if let Some(key) = self.existing(id).await? {
            return Ok(key);
        }
        let created = new_key()?;
        if self.insert(id, created).await? {
            return Ok(created);
        }
        self.existing(id)
            .await?
            .ok_or_else(|| unavailable("recover"))
    }

    async fn copy(&self, from: &str, to: &str) -> Result<()> {
        self.install(to, <Self as KeyService>::existing(self, from).await?)
            .await
    }

    async fn destroy(&self, id: &str) -> Result<()> {
        super::require_general_key_namespace(id)?;
        self.withdraw_item(id).await?;
        self.close_units(UnitScope::archive(id)).await
    }

    async fn use_unit_key(
        &self,
        slot: UnitSlot,
        name: ObjectKey,
        operation: UnitOperation,
        _region: &Arc<IoRegion>,
    ) -> rtdb_storage::Result<UnitOutput> {
        let deadline = Instant::now() + OPERATION_LIMIT;
        tokio::time::timeout_at(deadline.into(), async {
            let material = self
                .unit_material(&slot, operation.provisions())
                .await
                .map_err(key_error)?;
            unit::with_material(material, deadline, |key| operation.apply(key, &name))
        })
        .await
        .map_err(|_| unit::expired())?
    }

    async fn withdraw_unit_key(&self, slot: UnitSlot, _region: &Arc<IoRegion>) -> Result<()> {
        self.withdraw_item(&slot.id()).await?;
        rtdb_storage::sealing::drain_unit_delivery().await;
        Ok(())
    }

    async fn withdraw_unit_scope(&self, scope: UnitScope, _region: &Arc<IoRegion>) -> Result<()> {
        self.close_units(scope).await
    }

    async fn archive_key(
        &self,
        id: &str,
        provision: bool,
        _region: Arc<IoRegion>,
    ) -> Result<Arc<dyn ArchiveKey>> {
        if provision {
            self.key(id).await?;
        } else {
            KeyService::existing(self, id).await?;
        }
        Ok(tracked_archive_key(DynamoArchive {
            vault: self.clone(),
            id: id.to_owned(),
        }))
    }

    async fn scoped_unit_key(
        &self,
        id: &str,
        region: Arc<IoRegion>,
    ) -> Result<Arc<dyn ArchiveKey>> {
        self.archive_key(id, false, region).await
    }

    async fn install(&self, id: &str, key: [u8; KEY_BYTES]) -> Result<()> {
        super::require_general_key_namespace(id)?;
        if self.insert(id, key).await? || self.existing(id).await? == Some(key) {
            return Ok(());
        }
        Err(Anomaly::new(
            AnomalyCategory::Conflict,
            "rtdb.multi/key-conflict",
            "The deployment already has a different key",
        ))
    }
}

struct DynamoArchive {
    vault: DynamoKeyService,
    id: String,
}

#[async_trait::async_trait]
impl CaptureCryptography for DynamoArchive {
    async fn crypt(
        &self,
        name: ObjectKey,
        operation: UnitOperation,
    ) -> rtdb_storage::Result<UnitOutput> {
        let slot = UnitScope::archive(&self.id).slot(&name);
        let deadline = Instant::now() + OPERATION_LIMIT;
        tokio::time::timeout_at(deadline.into(), async {
            let authority = KeyService::existing(&self.vault, &self.id)
                .await
                .map_err(key_error)?;
            let material = self
                .vault
                .unit_material(&slot, operation.provisions())
                .await
                .map_err(key_error)?;
            with_archive_unit(&authority, material, deadline, &name, operation)
        })
        .await
        .map_err(|_| unit::expired())?
    }

    async fn matches_authority_key(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        let authority = KeyService::existing(&self.vault, &self.id).await?;
        Ok(matches_authority_key(&authority, candidate))
    }
}

fn string(value: &str) -> AttributeValue {
    AttributeValue::S(value.to_owned())
}

fn unavailable(operation: &str) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/key-vault",
        format!("Deployment key {operation} could not be confirmed"),
    )
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    use super::*;

    #[tokio::test]
    #[ignore = "needs DynamoDB Local at RTDB_DYNAMODB_ENDPOINT"]
    async fn destroyed_deployment_keys_are_unavailable_even_after_reopen() {
        let endpoint = std::env::var("RTDB_DYNAMODB_ENDPOINT").unwrap();
        let client = rtdb_directory::DynamoDirectory::local_client(&endpoint);
        let table = format!("rtdb-key-test-{}", uuid::Uuid::new_v4().simple());
        client
            .create_table()
            .table_name(&table)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(ID)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(ID)
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        let vault = DynamoKeyService::new(client.clone(), table.clone()).unwrap();
        let (first, second) = tokio::join!(vault.key("deployment"), vault.key("deployment"));
        let key = first.unwrap();
        assert_eq!(second.unwrap(), key);
        vault.install("deployment", key).await.unwrap();
        let io = IoRegion::new();
        let staged = vault
            .scoped_unit_key("deployment", Arc::clone(&io))
            .await
            .unwrap();
        let member = ObjectKey::new("blocks/b00.binpb").unwrap();
        let body = bytes::Bytes::from_static(b"independent durable member");
        let frame = staged.seal_object(&member, body.clone()).await.unwrap();
        assert_eq!(
            staged.open_object(&member, frame.clone()).await.unwrap(),
            body
        );
        let slot = UnitScope::archive("deployment").slot(&member);
        assert!(vault.existing_key(&slot.id()).await.is_err());

        vault.destroy("deployment").await.unwrap();
        assert!(matches!(
            staged.open_object(&member, frame).await,
            Err(rtdb_storage::Error::ErasedUnderView)
        ));
        assert!(vault.existing(&slot.id()).await.is_err());

        vault.destroy("deployment").await.unwrap();
        let reopened = DynamoKeyService::new(client.clone(), table.clone()).unwrap();
        assert_eq!(
            reopened.key("deployment").await.unwrap_err().code,
            "rtdb.multi/key-destroyed"
        );
        assert_eq!(
            reopened.install("deployment", key).await.unwrap_err().code,
            "rtdb.multi/key-destroyed"
        );
        assert!(reopened.key("another").await.is_ok());
        client
            .delete_table()
            .table_name(&table)
            .send()
            .await
            .unwrap();
    }
}
