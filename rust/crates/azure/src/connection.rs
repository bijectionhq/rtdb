use std::collections::BTreeMap;
use object_store::azure::{AzureAccessKey, AzureCredential};
use rtdb_types::Result;
use url::Url;
use crate::{AzureConfig, AzureCredentials, invalid};

pub(super) struct Connection { pub account: String, pub endpoint: Url, pub credential: Option<AzureCredential> }

fn endpoint(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| invalid("Invalid Azure storage endpoint"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() || url.query().is_some() || url.fragment().is_some() {
        return Err(invalid("Azure endpoint must be an HTTP URL without embedded credentials, query, or fragment"));
    }
    Ok(url)
}

pub(super) fn resolve(config: &AzureConfig, credentials: AzureCredentials) -> Result<Connection> {
    let default_endpoint = config.storage_account_endpoint.clone().unwrap_or_else(|| format!("https://{}.blob.core.windows.net", config.storage_account));
    if let Some(key) = credentials.storage_account_key {
        return Ok(Connection { account: config.storage_account.clone(), endpoint: endpoint(&default_endpoint)?, credential: Some(AzureCredential::AccessKey(AzureAccessKey::try_new(&key).map_err(invalid)?)) });
    }
    let Some(connection_string) = credentials.connection_string else {
        return Ok(Connection { account: config.storage_account.clone(), endpoint: endpoint(&default_endpoint)?, credential: None });
    };
    let mut fields = BTreeMap::new();
    for part in connection_string.split(';').filter(|part| !part.trim().is_empty()) {
        let (key, value) = part.split_once('=').ok_or_else(|| invalid("Invalid Azure connection-string component"))?;
        if fields.insert(key.trim(), value.trim()).is_some() { return Err(invalid("Duplicate Azure connection-string component")); }
    }
    if fields.get("UseDevelopmentStorage") == Some(&"true") {
        let mut proxy = endpoint(fields.get("DevelopmentStorageProxyUri").copied().unwrap_or("http://127.0.0.1"))?;
        proxy.set_port(Some(10000)).map_err(|_| invalid("Invalid Azure emulator endpoint"))?;
        proxy.set_path("/devstoreaccount1");
        let key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        return Ok(Connection { account: "devstoreaccount1".into(), endpoint: proxy, credential: Some(AzureCredential::AccessKey(AzureAccessKey::try_new(key).map_err(invalid)?)) });
    }
    let account = fields.get("AccountName").copied().or_else(|| (fields.contains_key("BlobEndpoint") && fields.contains_key("SharedAccessSignature")).then_some(config.storage_account.as_str()))
        .filter(|account| !account.is_empty()).ok_or_else(|| invalid("Azure connection string has no account name"))?.to_string();
    let address = fields.get("BlobEndpoint").map(|value| value.to_string()).unwrap_or_else(|| format!("{}://{account}.blob.{}", fields.get("DefaultEndpointsProtocol").copied().unwrap_or("https"), fields.get("EndpointSuffix").copied().unwrap_or("core.windows.net")));
    let credential = if let Some(key) = fields.get("AccountKey") {
        AzureCredential::AccessKey(AzureAccessKey::try_new(key).map_err(invalid)?)
    } else if let Some(sas) = fields.get("SharedAccessSignature") {
        AzureCredential::SASToken(url::form_urlencoded::parse(sas.trim_start_matches('?').as_bytes()).into_owned().collect())
    } else { return Err(invalid("Azure connection string has neither an account key nor a shared access signature")); };
    Ok(Connection { account, endpoint: endpoint(&address)?, credential: Some(credential) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_explicit_remote_key_takes_precedence_over_its_connection_string() {
        let config = AzureConfig::new("configured", "container");
        let connection = resolve(&config, AzureCredentials { storage_account_key: Some("a2V5".into()), connection_string: Some("invalid".into()) }).unwrap();
        assert_eq!(connection.account, "configured");
        assert!(matches!(connection.credential, Some(AzureCredential::AccessKey(_))));
    }

    #[test]
    fn connection_string_sas_and_emulator_account_paths_are_preserved() {
        let config = AzureConfig::new("configured", "container");
        let connection = resolve(&config, AzureCredentials { storage_account_key: None, connection_string: Some("AccountName=other;BlobEndpoint=https://private.example/;SharedAccessSignature=sv=1&sig=a%2Bb%3D".into()) }).unwrap();
        assert_eq!(connection.account, "other");
        assert_eq!(connection.endpoint.as_str(), "https://private.example/");
        let Some(AzureCredential::SASToken(pairs)) = connection.credential else { panic!("SAS credential required") };
        assert_eq!(pairs[1], ("sig".into(), "a+b=".into()));
        let emulator = resolve(&config, AzureCredentials { storage_account_key: None, connection_string: Some("UseDevelopmentStorage=true".into()) }).unwrap();
        assert_eq!(emulator.endpoint.as_str(), "http://127.0.0.1:10000/devstoreaccount1");
    }
}
