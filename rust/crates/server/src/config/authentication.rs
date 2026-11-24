use std::collections::BTreeMap;
use rtdb_auth::{AddressBlock, AuthConfig, GrantedAccess, Method, MethodRule, OidcConfig, PasswordHash, Secret, WorkloadGrantConfig};
use rtdb_format::config::{Yaml, YamlFields, scalar_string};
use rtdb_tx::transact::workload::Point;
use rtdb_types::{Anomaly, Result, Timestamp};
use super::invalid;

pub(super) fn default_auth(environment: &impl Fn(&str) -> Option<String>) -> AuthConfig { AuthConfig::SingleRootUser { password: environment("XTDB_PASSWORD").map(Secret::new) } }
pub(super) fn decode(value: Yaml, environment: &impl Fn(&str) -> Option<String>) -> Result<AuthConfig> {
    let (tag, mut fields) = YamlFields::tagged(value, "authn")?;
    let config = match tag.as_str() {
        "!SingleRootUser" => {
            let password = match fields.take("password") { None => environment("XTDB_PASSWORD").map(Secret::new), Some(Yaml::Null) => None, Some(value) => Some(Secret::new(scalar_string(&value, "password")?)) };
            AuthConfig::SingleRootUser { password }
        }
        "!UserList" => {
            let mut users = BTreeMap::new();
            for (name, value) in YamlFields::new(fields.require("users")?, "users")?.into_entries() {
                let Yaml::Tagged(tagged) = value else { return Err(invalid("Password hashes require !Argon2id or !BCrypt tags")); };
                let encoded = scalar_string(&tagged.value, "password hash")?;
                let hash = if tagged.tag == "Argon2id" { PasswordHash::Argon2id(encoded) } else if tagged.tag == "BCrypt" { PasswordHash::BCrypt(encoded) } else { return Err(invalid("Password hashes require !Argon2id or !BCrypt tags")); };
                users.insert(name, hash);
            }
            let mut grants = rtdb_auth::Grants::new();
            if let Some(value) = fields.take("grants") {
                for (user, databases) in YamlFields::new(value, "grants")?.into_entries() {
                    let mut granted = BTreeMap::new();
                    for (database, access) in YamlFields::new(databases, "grant")?.into_entries() {
                        let access = match scalar_string(&access, "access")?.as_str() {
                            "DENY" => GrantedAccess::Deny,
                            "READ" => GrantedAccess::Read,
                            "READ_WRITE" => GrantedAccess::Write,
                            _ => return Err(invalid("A grant's access is DENY, READ or READ_WRITE")),
                        };
                        granted.insert(database, access);
                    }
                    grants.insert(user, granted);
                }
            }
            AuthConfig::UserList { users, rules: rules(fields.sequence("rules")?, Method::Password)?, grants }
        }
        "!OpenIdConnect" => {
            let config = OidcConfig {
                issuer_url: fields.required_string("issuerUrl")?.parse()
                    .map_err(|_| invalid("issuerUrl must be an absolute URL"))?,
                issuer_ca_pem: fields.nullable_string("issuerCaCertificate")?,
                client_id: fields.required_string("clientId")?,
                client_secret: Secret::new(fields.required_string("clientSecret")?),
                rules: rules(fields.sequence("rules")?, Method::Trust)?,
                workload_grants: fields.take("workloadGrants").map(workload_grants).transpose()?,
            };
            if let Some(ca) = &config.issuer_ca_pem
                && (config.issuer_url.scheme() != "https" || ca.trim().is_empty() || ca.len() > 64 * 1024)
            {
                return Err(invalid("issuerCaCertificate requires an HTTPS issuer and a PEM bundle up to 64 KiB"));
            }
            AuthConfig::OpenIdConnect(config)
        },
        _ => return Err(invalid(format!("Unknown authentication configuration tag '{tag}'"))),
    };
    fields.finish()?;
    if let AuthConfig::OpenIdConnect(oidc) = &config
        && let Some(grants) = &oidc.workload_grants
    {
        grants.validate_rules(&oidc.rules)?;
    }
    Ok(config)
}

fn workload_grants(value: Yaml) -> Result<WorkloadGrantConfig> {
    let mut fields = YamlFields::new(value, "workloadGrants")?;
    let mut config = WorkloadGrantConfig::new(
        fields.required_string("audience")?,
        fields.required_string("clientIdPrefix")?,
    )?;
    if let Some(assignment) = fields.take("assignment") {
        let mut fields = YamlFields::new(assignment, "workloadGrants.assignment")?;
        let relation = fields.required_string("relation")?;
        let row_id = fields.required_string("rowId")?;
        let valid_at = fields.required_string("validAtMicros")?.parse()
            .map_err(|_| invalid("workloadGrants.assignment.validAtMicros must be an integer"))?;
        fields.finish()?;
        config = config.with_assignment(Point::new(&relation, row_id, Timestamp(valid_at))?);
    }
    fields.finish()?;
    Ok(config)
}

fn rules(values: Option<Vec<Yaml>>, default: Method) -> Result<Vec<MethodRule>> {
    values.map(|values| values.into_iter().map(|value| {
        let mut fields = YamlFields::new(value, "authentication rule")?;
        let method = match fields.required_string("method")?.as_str() {
            "TRUST" => Method::Trust, "PASSWORD" => Method::Password, "DEVICE_AUTH" => Method::DeviceAuth, "CLIENT_CREDENTIALS" => Method::ClientCredentials,
            _ => return Err(invalid("Unknown authentication method")),
        };
        let rule = MethodRule { method, user: fields.nullable_string("user")?, remote_address: fields.nullable_string("remoteAddress")?.as_deref().map(AddressBlock::parse).transpose()? };
        fields.finish()?;
        Ok::<_, Anomaly>(rule)
    }).collect()).unwrap_or_else(|| Ok(vec![MethodRule::any(default)]))
}

#[cfg(feature = "multi")]
#[derive(Debug)]
struct DeploymentAdministrators {
    administrators: std::collections::BTreeSet<String>,
    creators: std::collections::BTreeSet<String>,
}

#[cfg(feature = "multi")]
impl rtdb_flight_sql::DeploymentAdminAuthority for DeploymentAdministrators {
    fn authorize(
        &self, principal: &rtdb_auth::Principal, action: rtdb_flight_sql::DeploymentAdminAction<'_>,
    ) -> Result<()> {
        if principal.user_id().is_some_and(|user| {
            self.administrators.contains(user)
                || matches!(action, rtdb_flight_sql::DeploymentAdminAction::Create { .. })
                    && self.creators.contains(user)
        }) {
            return Ok(());
        }
        Err(Anomaly::new(
            rtdb_types::AnomalyCategory::Forbidden,
            "rtdb/deployment-admin-principal",
            "The authenticated principal has no deployment administration grant",
        ))
    }
}

/// Parses an explicit host grant. Flight verifies the credential and scope before applying it.
#[cfg(feature = "multi")]
pub(super) fn deployment_administrators(
    administrators: Option<Vec<Yaml>>,
    creators: Option<Vec<Yaml>>,
) -> Result<Option<std::sync::Arc<dyn rtdb_flight_sql::DeploymentAdminAuthority>>> {
    let administrators = deployment_principal_names("deploymentAdminPrincipals", administrators)?;
    let creators = deployment_principal_names("deploymentCreatePrincipals", creators)?;
    Ok((!administrators.is_empty() || !creators.is_empty()).then(|| {
        std::sync::Arc::new(DeploymentAdministrators { administrators, creators }) as std::sync::Arc<dyn rtdb_flight_sql::DeploymentAdminAuthority>
    }))
}

#[cfg(feature = "multi")]
fn deployment_principal_names(
    field: &str, values: Option<Vec<Yaml>>,
) -> Result<std::collections::BTreeSet<String>> {
    let mut principals = std::collections::BTreeSet::new();
    for value in values.into_iter().flatten() {
        let Yaml::String(principal) = value else {
            return Err(invalid(format!("{field} requires a sequence of principal names")));
        };
        if principal.is_empty() || principal.trim() != principal || principal.chars().any(char::is_control) {
            return Err(invalid("An administrator name must be a nonempty authenticated principal name"));
        }
        principals.insert(principal);
    }
    Ok(principals)
}
