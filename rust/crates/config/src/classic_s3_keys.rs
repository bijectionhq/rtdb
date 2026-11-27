use serde::Serialize;

use crate::{Finding, digest};

#[derive(Serialize)]
pub(super) struct Preflight {
    inventory_sha256: String,
    #[serde(flatten)]
    outcome: Outcome,
    #[cfg(feature = "s3")]
    #[serde(skip_serializing_if = "Option::is_none")]
    evidence: Option<Evidence>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Outcome {
    #[cfg(feature = "s3")]
    Eligible {
        layout: Layout,
    },
    Refused {
        findings: Vec<Finding>,
    },
}

impl Preflight {
    pub(super) fn refused(
        input: &[u8],
        code: &'static str,
        path: &str,
        message: &'static str,
    ) -> Self {
        Self {
            inventory_sha256: digest(input),
            outcome: Outcome::Refused {
                findings: vec![finding(code, path, message)],
            },
            #[cfg(feature = "s3")]
            evidence: None,
        }
    }
}

fn finding(code: &'static str, path: impl Into<String>, message: &'static str) -> Finding {
    Finding {
        code,
        path: path.into(),
        message,
    }
}

#[cfg(feature = "s3")]
use s3::{Evidence, Layout};

#[cfg(feature = "s3")]
pub(super) use s3::inspect;

#[cfg(feature = "s3")]
mod s3 {
    use std::collections::{BTreeMap, BTreeSet};

    use prost::Message;
    use rtdb_format::{
        database::proto::{DatabaseConfig, database_config::Storage},
        generated::xtdb::aws::proto::S3ObjectStoreConfig,
        keys::{CURRENT_STORAGE_VERSION, storage_root},
    };
    use rtdb_s3::{S3Config, classic_key_aliases};
    use rtdb_server::config::NodeConfiguration;
    use rtdb_storage::{ObjectKey, valid_object_version};
    use rtdb_types::{S3ObjectNamespace, StorageEpoch};
    use serde::{Deserialize, Serialize};

    use super::{Outcome, Preflight, finding};
    use crate::{ExplicitEnvironment, Finding, digest};

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Inventory {
        schema_version: u32,
        database: String,
        namespace: S3ObjectNamespace,
        complete: bool,
        listed_prefixes: Vec<String>,
        logical_objects: Vec<LogicalObject>,
        versions: Vec<VersionEntry>,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct LogicalObject {
        key: String,
        requires_current_version: bool,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct VersionEntry {
        key: String,
        version_id: String,
        kind: EntryKind,
        is_latest: bool,
    }

    #[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    enum EntryKind {
        Object,
        DeleteMarker,
    }

    #[derive(Serialize)]
    struct Aliases {
        logical: LogicalObject,
        jvm_key: String,
        legacy_rtdb_key: String,
    }

    #[derive(Serialize)]
    pub(in crate::classic_s3_keys) struct Evidence {
        database: String,
        namespace: S3ObjectNamespace,
        listed_prefixes: Vec<String>,
        objects: Vec<Aliases>,
        versions: Vec<VersionEntry>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "snake_case")]
    pub(in crate::classic_s3_keys) enum Layout {
        Shared,
        JvmLiteral,
        LegacyRtdb,
    }

    pub(crate) fn inspect(
        input: &[u8],
        node: &NodeConfiguration,
        environment: &ExplicitEnvironment,
    ) -> Preflight {
        let refuse = |code, path, message| Preflight::refused(input, code, path, message);
        let Ok(mut inventory) = serde_json::from_slice::<Inventory>(input) else {
            return refuse(
                "invalid-inventory",
                "$",
                "The S3 key inventory must be a valid JSON manifest.",
            );
        };
        if inventory.schema_version != 1 {
            return refuse(
                "unsupported-inventory-version",
                "schema_version",
                "Expected S3 key inventory schema version 1.",
            );
        }
        if !inventory.complete {
            return refuse(
                "incomplete-inventory",
                "complete",
                "Both provider listings and the recorded logical object inventory must be complete at the same cut.",
            );
        }
        if inventory.logical_objects.is_empty() {
            return refuse(
                "no-logical-objects",
                "logical_objects",
                "An empty logical inventory cannot establish an existing key layout.",
            );
        }
        if node.default_v1_serving
            || [
                "AWS_ENDPOINT",
                "AWS_ENDPOINT_URL",
                "AWS_ENDPOINT_URL_S3",
                "AWS_S3_EXPRESS",
            ]
            .into_iter()
            .any(|name| environment.get(name).is_some())
        {
            return refuse(
                "unqualified-s3-configuration",
                "database",
                "Key qualification requires Classic S3 with provider addressing declared in YAML.",
            );
        }
        let database = match &node.fixed_databases {
            Some(databases) => databases.get(&inventory.database),
            None => (inventory.database == "xtdb").then_some(&node.primary),
        };
        let Some((namespace, root)) = database.and_then(declared_scope) else {
            return refuse(
                "unresolved-s3-scope",
                "database",
                "Select a configured single-partition Classic S3 database with an explicit region and valid storage root.",
            );
        };
        if namespace != inventory.namespace {
            return refuse(
                "s3-namespace-mismatch",
                "namespace",
                "The listed bucket, region and effective endpoint must match the declared S3 namespace.",
            );
        }

        let Ok((literal_root, legacy_root)) = classic_key_aliases(&root) else {
            return refuse(
                "unsupported-storage-root",
                "listed_prefixes",
                "The configured root has no supported pair of Classic S3 key spellings.",
            );
        };
        let mut expected_prefixes = vec![format!("{literal_root}/"), format!("{legacy_root}/")];
        expected_prefixes.sort_unstable();
        expected_prefixes.dedup();
        if node.fixed_databases.as_ref().is_some_and(|databases| {
            databases
                .iter()
                .filter(|(name, _)| *name != &inventory.database)
                .any(|(_, database)| {
                    let Some((other_namespace, other_root)) = declared_scope(database) else {
                        return false;
                    };
                    if other_namespace != namespace {
                        return false;
                    }
                    let Ok((other_literal, other_legacy)) = classic_key_aliases(&other_root) else {
                        return false;
                    };
                    [other_literal, other_legacy].into_iter().any(|root| {
                        let directory = format!("{root}/");
                        expected_prefixes.iter().any(|prefix| {
                            directory.starts_with(prefix) || prefix.starts_with(&directory)
                        })
                    })
                })
        }) {
            return refuse(
                "overlapping-database-prefixes",
                "database",
                "Another configured database uses the same or an overlapping S3 storage directory.",
            );
        }
        inventory.listed_prefixes.sort_unstable();
        if inventory.listed_prefixes != expected_prefixes {
            return refuse(
                "s3-listing-scope-mismatch",
                "listed_prefixes",
                "Supply exactly the complete literal and legacy storage directory prefixes, including their trailing slashes.",
            );
        }

        let mut findings = Vec::new();
        let mut objects = Vec::new();
        for (index, logical) in inventory.logical_objects.into_iter().enumerate() {
            let rooted = ObjectKey::new(format!("{root}/{}", logical.key));
            if let Some((jvm_key, legacy_rtdb_key)) =
                rooted.ok().and_then(|key| classic_key_aliases(&key).ok())
            {
                objects.push(Aliases {
                    logical,
                    jvm_key,
                    legacy_rtdb_key,
                });
            } else {
                findings.push(finding(
                    "invalid-logical-key",
                    format!("logical_objects[{index}]"),
                    "The recorded logical path is outside the shared Classic S3 key domain.",
                ));
            }
        }

        let mut versions = inventory.versions;
        let supplied_versions = versions.len();
        versions.retain(|version| {
            expected_prefixes
                .iter()
                .any(|prefix| version.key.starts_with(prefix))
        });
        if versions.len() != supplied_versions {
            findings.push(finding(
                "provider-key-out-of-scope",
                "versions",
                "At least one version was listed outside both declared storage directories.",
            ));
        }
        versions.sort_unstable_by(|left, right| {
            (&left.key, &left.version_id).cmp(&(&right.key, &right.version_id))
        });

        let outcome = classify(&objects, &versions, findings);
        Preflight {
            inventory_sha256: digest(input),
            outcome,
            evidence: Some(Evidence {
                database: inventory.database,
                namespace,
                listed_prefixes: expected_prefixes,
                objects,
                versions,
            }),
        }
    }

    fn declared_scope(database: &DatabaseConfig) -> Option<(S3ObjectNamespace, ObjectKey)> {
        if database.partitions.unwrap_or(1) != 1 {
            return None;
        }
        let Storage::RemoteStorage(storage) = database.storage.as_ref()? else {
            return None;
        };
        let provider = storage.object_store.as_ref()?;
        if provider.type_url.rsplit('/').next()? != "xtdb.aws.proto.S3ObjectStoreConfig" {
            return None;
        }
        let config = S3ObjectStoreConfig::decode(provider.value.as_slice()).ok()?;
        let root = storage_root(
            CURRENT_STORAGE_VERSION,
            StorageEpoch(storage.epoch.unwrap_or(0)),
            0,
            1,
        )
        .ok()?;
        S3Config::from_proto(&config)
            .declared_classic_key_scope(&ObjectKey::new(root).ok()?)
            .ok()
    }

    fn classify(
        objects: &[Aliases],
        versions: &[VersionEntry],
        mut findings: Vec<Finding>,
    ) -> Outcome {
        let mut candidates = BTreeSet::new();
        for (index, object) in objects.iter().enumerate() {
            let alternate = (object.jvm_key != object.legacy_rtdb_key)
                .then_some((object.legacy_rtdb_key.as_str(), "legacy_rtdb_key"));
            let literal = (object.jvm_key.as_str(), "jvm_key");
            for (key, field) in std::iter::once(literal).chain(alternate) {
                if !candidates.insert(key) {
                    findings.push(finding("logical-key-alias-collision", format!("evidence.objects[{index}].{field}"), "This exact provider key also belongs to a different logical inventory entry."));
                }
            }
        }

        let mut by_key = BTreeMap::new();
        let mut index = 0;
        for group in versions.chunk_by(|left, right| left.key == right.key) {
            let key = group[0].key.as_str();
            let path = format!("evidence.versions[{index}]");
            index += group.len();
            if !candidates.contains(key) {
                findings.push(finding(
                    "unaccounted-provider-key",
                    path.as_str(),
                    "A listed provider key has no matching recorded logical path.",
                ));
            }
            if group
                .iter()
                .any(|entry| !valid_object_version(entry.version_id.as_bytes()))
            {
                findings.push(finding("missing-exact-version", path.as_str(), "Each object version or delete marker must have an immutable provider version ID."));
            }
            if group
                .windows(2)
                .any(|pair| pair[0].version_id == pair[1].version_id)
                || group.iter().filter(|entry| entry.is_latest).count() != 1
            {
                findings.push(finding(
                    "inconsistent-version-history",
                    path,
                    "Each exact key must have distinct version IDs and exactly one latest entry.",
                ));
            }
            by_key.insert(key, group);
        }

        let mut literal_only = false;
        let mut legacy_only = false;
        for (index, object) in objects.iter().enumerate() {
            let path = || format!("evidence.objects[{index}]");
            let literal = by_key.get(object.jvm_key.as_str());
            let legacy = by_key.get(object.legacy_rtdb_key.as_str());
            let history = match (literal, legacy) {
                (None, None) => {
                    findings.push(finding(
                        "missing-provider-key",
                        path(),
                        "Neither exact provider key has version evidence.",
                    ));
                    continue;
                }
                (Some(history), Some(_)) if object.jvm_key == object.legacy_rtdb_key => history,
                (Some(_), Some(_)) => {
                    findings.push(finding(
                        "both-key-aliases-occupied",
                        path(),
                        "Both exact key spellings have version history for this logical key.",
                    ));
                    continue;
                }
                (Some(history), None) => {
                    literal_only = true;
                    history
                }
                (None, Some(history)) => {
                    legacy_only = true;
                    history
                }
            };
            if object.logical.requires_current_version
                && !history
                    .iter()
                    .any(|entry| entry.is_latest && entry.kind == EntryKind::Object)
            {
                findings.push(finding(
                    "required-key-deleted",
                    path(),
                    "The selected key has no visible current data version.",
                ));
            }
        }

        if literal_only && legacy_only {
            findings.push(finding(
                "mixed-key-layout",
                "evidence.objects",
                "The recorded keys require a mix of literal JVM and legacy RTDB spellings.",
            ));
        }
        if !findings.is_empty() {
            return Outcome::Refused { findings };
        }
        let layout = if literal_only {
            Layout::JvmLiteral
        } else if legacy_only {
            Layout::LegacyRtdb
        } else {
            Layout::Shared
        };
        Outcome::Eligible { layout }
    }
}
