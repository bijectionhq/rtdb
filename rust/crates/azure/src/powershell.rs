use std::ffi::OsStr;
use std::sync::Arc;
use async_trait::async_trait;
use azure_core::{credentials::{AccessToken, TokenCredential, TokenRequestOptions}, error::ErrorKind, time::{Duration, OffsetDateTime}};
use azure_identity::Executor;
use tokio::sync::Mutex;

pub(crate) struct PowerShellCredential { executor: Arc<dyn Executor>, tenant: Option<String>, cached: Mutex<Option<AccessToken>> }

impl std::fmt::Debug for PowerShellCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("PowerShellCredential") }
}

impl PowerShellCredential {
    pub fn new(executor: Arc<dyn Executor>, tenant: Option<String>) -> azure_core::Result<Arc<Self>> {
        if tenant.as_ref().is_some_and(|tenant| tenant.is_empty() || !tenant.bytes().all(|byte| byte.is_ascii_alphanumeric() || b".-".contains(&byte))) {
            return Err(invalid("Azure tenant ID contains unsupported characters"));
        }
        Ok(Arc::new(Self { executor, tenant, cached: Mutex::new(None) }))
    }
}

#[async_trait]
impl TokenCredential for PowerShellCredential {
    async fn get_token(&self, scopes: &[&str], _: Option<TokenRequestOptions<'_>>) -> azure_core::Result<AccessToken> {
        if scopes != ["https://storage.azure.com/.default"] { return Err(invalid("PowerShell storage credential requires the storage scope")); }
        let mut cached = self.cached.lock().await;
        if let Some(token) = cached.as_ref().filter(|token| token.expires_on > OffsetDateTime::now_utc() + Duration::seconds(60)) { return Ok(token.clone()); }
        let tenant = self.tenant.as_ref().map(|tenant| format!(" -TenantId '{tenant}'")).unwrap_or_default();
        let command = format!("$ErrorActionPreference='Stop'; $ProgressPreference='SilentlyContinue'; Import-Module Az.Accounts -MinimumVersion 2.2.0; $t=Get-AzAccessToken -ResourceUrl 'https://storage.azure.com'{tenant} -WarningAction SilentlyContinue; $v=$t.Token; if ($v -is [System.Security.SecureString]) {{ $v=[System.Net.NetworkCredential]::new('', $v).Password }}; @{{token=$v;expiresOn=$t.ExpiresOn.ToUniversalTime().ToString('o')}} | ConvertTo-Json -Compress");
        let args = [OsStr::new("-NoLogo"), OsStr::new("-NonInteractive"), OsStr::new("-NoProfile"), OsStr::new("-Command"), OsStr::new(&command)];
        let output = match self.executor.run(OsStr::new("pwsh"), &args).await {
            #[cfg(windows)]
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => self.executor.run(OsStr::new("powershell"), &args).await,
            result => result,
        }.map_err(|_| invalid("Azure PowerShell credential command could not complete"))?;
        if !output.status.success() { return Err(invalid("Azure PowerShell is unavailable or could not acquire a token")); }
        let token = parse_token(&output.stdout)?;
        *cached = Some(token.clone());
        Ok(token)
    }
}

fn parse_token(bytes: &[u8]) -> azure_core::Result<AccessToken> {
    #[derive(serde::Deserialize)]
    struct Token { token: String, #[serde(rename = "expiresOn")] expires_on: String }
    let token: Token = serde_json::from_slice(bytes).map_err(|_| invalid("Azure PowerShell returned an invalid token response"))?;
    if token.token.is_empty() { return Err(invalid("Azure PowerShell returned an empty token")); }
    let timestamp = chrono::DateTime::parse_from_rfc3339(&token.expires_on).map_err(|_| invalid("Azure PowerShell returned an invalid token expiration"))?;
    let expiry = OffsetDateTime::from_unix_timestamp(timestamp.timestamp()).map_err(|_| invalid("Azure PowerShell token expiration exceeds the supported range"))?;
    Ok(AccessToken::new(token.token, expiry))
}

fn invalid(message: &str) -> azure_core::Error { azure_core::Error::with_message(ErrorKind::Credential, message.to_owned()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn powershell_token_dates_accept_fractional_utc_and_errors_hide_payloads() {
        let token = parse_token(br#"{"token":"secret","expiresOn":"2099-01-01T00:00:00.0000000Z"}"#).unwrap();
        assert_eq!(token.token.secret(), "secret");
        assert_eq!(token.expires_on.year(), 2099);
        let error = parse_token(br#"{"token":"secret","expiresOn":"bad"}"#).unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
}
