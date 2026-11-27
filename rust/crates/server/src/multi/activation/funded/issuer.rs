use std::{collections::BTreeMap, time::Duration};

use journal_contract::{Epoch, Lsn};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{ActivationAuthority, Binding, Closure, Enrollment, conflict, settlement::Resolution};
use crate::multi::provider_http::{Connection, Error, Profile};

const RECOVERY_ENDPOINT: &str = "/rtdb/funded-issuer/v1/recovery";
const VERSION: u32 = 1;
const MAX_BYTES: usize = 16 * 1024;
const DEADLINE: Duration = Duration::from_secs(15);

#[derive(Default, Deserialize)]
#[serde(transparent)]
pub(in crate::multi::activation) struct Settings(Vec<Issuer>);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Issuer {
    enrollment: Enrollment,
    connection: Profile,
}

#[derive(Default)]
pub(in crate::multi::activation) struct Providers(BTreeMap<Enrollment, Connection>);

impl Settings {
    #[cfg(any(unix, test))]
    pub(in crate::multi::activation) fn connect(
        self,
        open_private_key: impl Fn(&std::path::Path) -> Result<std::fs::File>,
    ) -> Result<Providers> {
        let mut providers = BTreeMap::new();
        for issuer in self.0 {
            if !issuer.enrollment.valid() || providers.contains_key(&issuer.enrollment) {
                return Err(provider_error(Error::Configuration));
            }
            let private_key = open_private_key(&issuer.connection.client_private_key_file)?;
            let key = crate::multi::provider_http::read_private_key(private_key)
                .map_err(provider_error)?;
            let connection =
                Connection::from_profile_with_key(issuer.connection, key, MAX_BYTES, DEADLINE)
                    .map_err(provider_error)?;
            providers.insert(issuer.enrollment, connection);
        }
        Ok(Providers(providers))
    }
}

#[derive(Serialize)]
struct Request<'a> {
    version: u32,
    authority_id: Uuid,
    database: &'a str,
    predecessor: Binding,
    scope_sha256: [u8; 32],
    action: Action,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum Action {
    Close,
    Outcome {
        inventory_sha256: [u8; 32],
        index: u64,
        source_fence: Option<SourceFence>,
    },
}

#[derive(Serialize)]
struct SourceFence {
    writer_epoch: Epoch,
    sealed_head: Lsn,
}

#[derive(Debug, Serialize)]
pub(in crate::multi::activation) struct Progress {
    scope_sha256: Option<[u8; 32]>,
    bookings: u64,
    next_index: Option<u64>,
    next_offset: Option<usize>,
}

impl Providers {
    pub(in crate::multi::activation) async fn reconcile(
        &self,
        authority: &ActivationAuthority,
        database: &str,
        predecessor: Binding,
        offset: usize,
    ) -> Result<Progress> {
        let page = authority
            .funded_scope_page(database, predecessor, offset)
            .await?;
        if !page.grants_closed {
            return Err(conflict());
        }
        let Some(scope) = page.scope else {
            return Ok(Progress {
                scope_sha256: None,
                bookings: 0,
                next_index: None,
                next_offset: None,
            });
        };
        let scope_sha256 = scope.scope_sha256;
        let request = |action| Request {
            version: VERSION,
            authority_id: authority.identity(),
            database,
            predecessor,
            scope_sha256,
            action,
        };
        let mut progress = Progress {
            scope_sha256: Some(scope_sha256),
            bookings: 0,
            next_index: None,
            next_offset: page.next_offset,
        };
        if scope.closure.is_none() {
            let closure: Closure = self
                .provider(scope.enrollment)?
                .post(RECOVERY_ENDPOINT, &request(Action::Close), VERSION)
                .await
                .map_err(provider_error)?;
            if closure.scope_sha256 != scope_sha256 {
                return Err(provider_error(Error::Refused));
            }
            let closed = authority
                .record_funded_closure(database, predecessor, closure)
                .await?;
            let closure = closed.closure.as_ref().ok_or_else(conflict)?;
            progress.bookings = closure.bookings;
            progress.next_index = authority.next_funded_index(&closed, closure).await?;
            return Ok(progress);
        }
        let (scope, source_fence) = authority
            .closed_funded_scope(database, predecessor, scope_sha256)
            .await?;
        let closure = scope.closure.as_ref().ok_or_else(conflict)?;
        progress.bookings = closure.bookings;
        if let Some(index) = authority.next_funded_index(&scope, closure).await? {
            let action = Action::Outcome {
                inventory_sha256: closure.inventory_sha256,
                index,
                source_fence: source_fence.map(|(writer_epoch, sealed_head)| SourceFence {
                    writer_epoch,
                    sealed_head,
                }),
            };
            let resolution: Resolution = self
                .provider(scope.enrollment)?
                .post(RECOVERY_ENDPOINT, &request(action), VERSION)
                .await
                .map_err(provider_error)?;
            if !resolution.matches(scope_sha256, index) {
                return Err(provider_error(Error::Refused));
            }
            authority
                .record_funded_outcome(database, predecessor, resolution)
                .await?;
            progress.next_index = index
                .checked_add(1)
                .filter(|next| *next <= closure.bookings);
        }
        Ok(progress)
    }

    fn provider(&self, enrollment: Enrollment) -> Result<&Connection> {
        self.0.get(&enrollment).ok_or_else(|| {
            Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb/activation-funded-issuer",
                "No original issuer connection is configured for the enrolled scope",
            )
        })
    }
}

fn provider_error(error: Error) -> Anomaly {
    match error {
        Error::Configuration => Anomaly::incorrect(
            "rtdb/activation-funded-issuer-config",
            "Invalid original funded issuer connection",
        ),
        Error::Unavailable => Anomaly::new(
            AnomalyCategory::Unavailable,
            "rtdb/activation-funded-issuer-pending",
            "The original issuer's recovery evidence is pending",
        ),
        Error::Refused => Anomaly::incorrect(
            "rtdb/activation-funded-issuer-response",
            "The issuer response does not match the recovery request",
        ),
    }
}
