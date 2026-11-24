use std::collections::BTreeMap;
use rtdb_auth::{AddressBlock, AuthConfig, GrantedAccess, Method, MethodRule, OidcConfig, PasswordHash, Secret};
use rtdb_format::config::{Yaml, YamlFields, scalar_string};
use rtdb_types::{Anomaly, Result};
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
        "!OpenIdConnect" => AuthConfig::OpenIdConnect(OidcConfig {
            issuer_url: fields.required_string("issuerUrl")?.parse().map_err(|_| invalid("issuerUrl must be an absolute URL"))?,
            client_id: fields.required_string("clientId")?, client_secret: Secret::new(fields.required_string("clientSecret")?), rules: rules(fields.sequence("rules")?, Method::Trust)?,
        }),
        _ => return Err(invalid(format!("Unknown authentication configuration tag '{tag}'"))),
    };
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
