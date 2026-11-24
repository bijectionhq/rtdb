mod http;
mod oidc;
mod password;
mod visibility_scope;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::{Clock, DatabaseAccess, SessionIdentity};
use rtdb_tx::TransactionAuthorization;
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

pub use http::{HttpRequest, HttpResponse, HttpTransport, NativeHttp};
pub use oidc::{DeviceAuthorization, OidcConfig};
pub use password::PasswordHash;
pub use visibility_scope::VerifiedLabelScope;

#[derive(Clone)]
pub struct Secret(Zeroizing<String>);
impl Secret {
    pub fn new(value: impl Into<String>) -> Self { Self(Zeroizing::new(value.into())) }
    pub fn expose(&self) -> &str { self.0.as_str() }
}
impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("[REDACTED]") }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Method { Trust, Password, DeviceAuth, ClientCredentials }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MethodRule {
    pub method: Method,
    pub user: Option<String>,
    pub remote_address: Option<String>,
}
impl MethodRule {
    pub fn any(method: Method) -> Self { Self { method, user: None, remote_address: None } }
}

/// Matches in order; an address-constrained matching user rejects an unknown address immediately.
pub fn method_for(rules: &[MethodRule], user: Option<&str>, remote_address: Option<&str>) -> Option<Method> {
    for rule in rules {
        if rule.user.as_deref().is_some_and(|required| Some(required) != user) { continue; }
        match (&rule.remote_address, remote_address) {
            (None, _) => return Some(rule.method),
            (Some(_), None) => return None,
            (Some(required), Some(actual)) if required == actual => return Some(rule.method),
            _ => (),
        }
    }
    None
}

#[derive(Clone, Debug)]
pub enum AuthConfig {
    SingleRootUser { password: Option<Secret> },
    /// A named user's exact database rule takes priority over its `*` default.
    UserList { users: BTreeMap<String, PasswordHash>, rules: Vec<MethodRule>, grants: Grants },
    OpenIdConnect(OidcConfig),
}

/// For each user limited to some databases, the access granted on each.
pub type Grants = BTreeMap<String, BTreeMap<String, GrantedAccess>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrantedAccess {
    Deny,
    Read,
    Write,
}

impl GrantedAccess {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "DENY",
            Self::Read => "READ",
            Self::Write => "READ_WRITE",
        }
    }
}

impl AuthConfig {
    pub fn uses_remote_address(&self) -> bool {
        let rules = match self {
            Self::SingleRootUser { .. } => return false,
            Self::UserList { rules, .. } => rules,
            Self::OpenIdConnect(config) => &config.rules,
        };
        rules.iter().any(|rule| rule.remote_address.is_some())
    }
}

impl Default for AuthConfig {
    fn default() -> Self { Self::SingleRootUser { password: std::env::var("XTDB_PASSWORD").ok().map(Secret::new) } }
}

enum Authentication {
    SingleRootUser { password: Option<Secret> },
    UserList { users: BTreeMap<String, PasswordHash>, rules: Vec<MethodRule>, grants: Grants },
    OpenIdConnect(Arc<oidc::Provider>),
}

pub struct Authenticator { authentication: Authentication, memory: Arc<MemoryBudget> }
impl Authenticator {
    /// Discovery completes before returning; no task or runtime is started by this factory.
    pub async fn open(config: AuthConfig, memory: Arc<MemoryBudget>, clock: Arc<dyn Clock>, http: Arc<dyn HttpTransport>) -> Result<Arc<Self>> {
        let authentication = match config {
            AuthConfig::SingleRootUser { password } => Authentication::SingleRootUser { password },
            AuthConfig::UserList { users, rules, grants } => Authentication::UserList { users, rules, grants },
            AuthConfig::OpenIdConnect(config) => Authentication::OpenIdConnect(oidc::Provider::open(config, Arc::clone(&memory), clock, http).await?),
        };
        Ok(Arc::new(Self { authentication, memory }))
    }
    pub fn method_for(&self, user: Option<&str>, remote_address: Option<&str>) -> Option<Method> {
        match &self.authentication {
            Authentication::SingleRootUser { password } => Some(if password.is_some() { Method::Password } else { Method::Trust }),
            Authentication::UserList { rules, .. } => method_for(rules, user, remote_address),
            Authentication::OpenIdConnect(provider) => method_for(&provider.rules, user, remote_address),
        }
    }
    pub fn known_users(&self) -> Vec<String> {
        match &self.authentication { Authentication::UserList { users, .. } => users.keys().cloned().collect(), _ => vec!["xtdb".into()] }
    }
    pub fn is_superuser(&self, user: &str) -> bool { user == "xtdb" && !matches!(self.authentication, Authentication::OpenIdConnect(_)) }

    /// Trusted embedded callers may construct a principal regardless of network authentication rules.
    pub fn authenticate_trusted(&self, user: &str) -> Result<Arc<Principal>> { self.principal(Some(user)) }
    pub fn unauthenticated(&self) -> Result<Arc<Principal>> { self.principal(None) }

    /// The principal for `user`, bound by whatever databases the configuration grants it.
    fn principal(&self, user: Option<&str>) -> Result<Arc<Principal>> {
        let grants = match (&self.authentication, user) { (Authentication::UserList { grants, .. }, Some(user)) => grants.get(user).cloned(), _ => None };
        Principal::static_user(user, grants, &self.memory)
    }

    pub async fn verify_password(&self, user: &str, password: &str) -> Result<Arc<Principal>> {
        match &self.authentication {
            Authentication::SingleRootUser { password: configured } => {
                let matches = configured.as_ref().is_some_and(|configured| {
                    let left = Sha256::digest(configured.expose().as_bytes());
                    let right = Sha256::digest(password.as_bytes());
                    bool::from(left.as_slice().ct_eq(right.as_slice())) && user == "xtdb"
                });
                if matches { self.principal(Some(user)) } else { Err(password_failed(user)) }
            }
            Authentication::UserList { users, .. } => {
                if let Some(hash) = users.get(user) {
                    if hash.verify(password, &self.memory)? { return self.principal(Some(user)); }
                }
                Err(password_failed(user))
            }
            Authentication::OpenIdConnect(provider) => provider.password(user, password).await,
        }
    }
    pub async fn verify_client_credentials(&self, client_id: &str, client_secret: &str) -> Result<Arc<Principal>> {
        match &self.authentication {
            Authentication::OpenIdConnect(provider) => provider.client_credentials(client_id, client_secret).await,
            _ => Err(Anomaly::unsupported("verifyClientCredentials", "Authenticator does not support client credentials")),
        }
    }
    pub async fn start_device_auth(&self, _user: &str) -> Result<DeviceAuthorization> {
        match &self.authentication {
            Authentication::OpenIdConnect(provider) => provider.device().await,
            _ => Err(Anomaly::unsupported("startDeviceAuth", "Authenticator does not support device authorization")),
        }
    }
}

impl TransactionAuthorization for Authenticator {
    fn check_role_membership(&self, database: &str, principal: Option<&str>) -> Result<()> {
        if database != "xtdb" {
            return Err(Anomaly::incorrect("xtdb/role-membership-non-primary", "Role membership can only be managed on the primary 'xtdb' database.").with_context("db", database.into()));
        }
        if principal.is_some_and(|user| self.is_superuser(user)) { Ok(()) }
        else { Err(Anomaly::incorrect("xtdb/not-authorized", "Only a superuser may GRANT/REVOKE role membership.").with_context("user", principal.map_or(serde_json::Value::Null, Into::into))) }
    }
}

impl rtdb_plan::CatalogUsers for Authenticator {
    fn is_superuser(&self, user: &str) -> bool { Authenticator::is_superuser(self, user) }
    fn users(&self) -> Result<rtdb_plan::CatalogRows<rtdb_plan::CatalogUser>> {
        let names: Vec<&str> = match &self.authentication { Authentication::UserList { users, .. } => users.keys().map(String::as_str).collect(), _ => vec!["xtdb"] };
        let bytes = names.iter().try_fold(names.capacity() * std::mem::size_of::<rtdb_plan::CatalogUser>(), |bytes, name| bytes.checked_add(name.len()).ok_or_else(|| Anomaly::incorrect("rtdb.auth/user-allocation", "Configured user allocation exceeds addressable memory")))?;
        let reservation = self.memory.reserve(bytes)?;
        let rows = names.into_iter().map(|name| rtdb_plan::CatalogUser { name: name.into(), superuser: self.is_superuser(name) }).collect();
        Ok(rtdb_plan::CatalogRows::new(rows, reservation))
    }
}

enum Credentials { Static, Oidc(oidc::Credentials) }

pub struct Principal {
    user: Option<String>,
    credentials: Credentials,
    /// The databases this principal may use and how, if it is limited to some.
    grants: Option<BTreeMap<String, GrantedAccess>>,
    label_scope: Option<VerifiedLabelScope>,
    _reservation: Arc<MemoryReservation>,
}
impl Principal {
    fn static_user(user: Option<&str>, grants: Option<BTreeMap<String, GrantedAccess>>, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let granted = grants.iter().flatten().try_fold(0usize, |bytes, (database, _)| bytes.checked_add(database.len() + 64));
        let bytes = granted.and_then(|granted| user.map_or(0, str::len).checked_add(granted)?.checked_add(std::mem::size_of::<Self>())).ok_or_else(|| Anomaly::incorrect("rtdb.auth/principal-allocation", "Principal allocation exceeds addressable memory"))?;
        let reservation = memory.reserve(bytes)?;
        Ok(Arc::new(Self { user: user.map(str::to_owned), credentials: Credentials::Static, grants, label_scope: None, _reservation: reservation }))
    }
    pub fn user_id(&self) -> Option<&str> { self.user.as_deref() }
    pub fn label_scope(&self) -> Option<&VerifiedLabelScope> { self.label_scope.as_ref() }
}
#[async_trait]
impl SessionIdentity for Principal {
    fn user(&self) -> Option<&str> { self.user.as_deref() }
    async fn revalidate(&self, now: Timestamp) -> Result<()> {
        if let Some(scope) = &self.label_scope { return scope.revalidate(now); }
        match &self.credentials {
            Credentials::Static => Ok(()),
            Credentials::Oidc(credentials) => credentials.revalidate(now, self.user.as_deref().unwrap_or_default()).await,
        }
    }
    fn delegable(&self) -> bool { matches!(self.credentials, Credentials::Static) }
    fn authorize_database(&self, database: &str, access: DatabaseAccess) -> Result<()> {
        if self.label_scope.is_some() {
            return Err(forbidden("rtdb.auth/scoped-route", "The credential requires a scoped database route"));
        }
        let Some(grants) = &self.grants else { return Ok(()) };
        match (grants.get(database).or_else(|| grants.get("*")), access) {
            (Some(GrantedAccess::Write), _) | (Some(GrantedAccess::Read), DatabaseAccess::Read) => Ok(()),
            (Some(GrantedAccess::Read), DatabaseAccess::Write) => Err(forbidden("xtdb.authz/read-only-grant", format!("{} may only read database '{database}'", self.user.as_deref().unwrap_or_default()))),
            (Some(GrantedAccess::Deny) | None, _) => Err(forbidden("xtdb.authz/database-not-granted", format!("{} has no access to database '{database}'", self.user.as_deref().unwrap_or_default()))),
        }
    }
}

fn password_failed(user: &str) -> Anomaly { forbidden("xtdb.authn/failed", format!("password authentication failed for user: {user}")) }
fn forbidden(code: &str, message: impl Into<String>) -> Anomaly { Anomaly::new(AnomalyCategory::Forbidden, code, message) }
