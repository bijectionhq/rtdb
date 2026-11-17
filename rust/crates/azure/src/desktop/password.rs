use std::{collections::BTreeMap, sync::Arc, time::Duration};
use async_trait::async_trait;
use azure_core::{credentials::{AccessToken, Secret, TokenCredential, TokenRequestOptions}, time::OffsetDateTime};
use tokio::sync::Mutex;
use super::{AccountId, ExchangeFailure, Session, Sessions, STORAGE_SCOPE, GRANTED_SCOPES, authority::Authority, federation, invalid, java_blank, parse_token};

pub(crate) struct PasswordCredential {
    authority: Authority,
    client: reqwest::Client,
    client_id: String,
    username: Secret,
    password: Secret,
    sessions: Mutex<Sessions>,
}

impl std::fmt::Debug for PasswordCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("PasswordCredential([redacted])") }
}

impl PasswordCredential {
    pub fn new(environment: &BTreeMap<&str, String>) -> azure_core::Result<Arc<Self>> {
        let required = |name| environment.get(name).cloned().ok_or_else(|| invalid("Azure username/password environment configuration is incomplete"));
        let client_id = required("AZURE_CLIENT_ID")?;
        let username = Secret::new(required("AZURE_USERNAME")?);
        let password = Secret::new(required("AZURE_PASSWORD")?);
        let tenant = environment.get("AZURE_TENANT_ID").map(String::as_str).unwrap_or("organizations");
        let host = environment.get("AZURE_AUTHORITY_HOST").map(String::as_str).unwrap_or("https://login.microsoftonline.com");
        let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5)).timeout(Duration::from_secs(30)).build()
            .map_err(|_| invalid("Azure username/password HTTP client could not be created"))?;
        let authority = Authority::new(host, tenant, client.clone())?;
        let sessions = Mutex::new(Sessions::for_client_id(client_id.clone()));
        Ok(Arc::new(Self { authority, client, client_id, username, password, sessions }))
    }

    async fn exchange(&self, refresh_token: Option<&Secret>) -> Result<Session, ExchangeFailure> {
        if java_blank(&self.client_id) || refresh_token.is_none() && (java_blank(self.username.secret()) || self.password.secret().is_empty()) {
            return Err(invalid("Azure username/password client and credentials must not be empty").into());
        }
        let assertion = if refresh_token.is_none() {
            match self.authority.user_realm_endpoint(self.username.secret())? {
                Some(endpoint) => federation::discover(&self.client, endpoint, self.username.secret(), self.password.secret()).await?,
                None => None,
            }
        } else { None };
        let endpoint = self.authority.token_endpoint().await?;
        let mut form = url::form_urlencoded::Serializer::new(String::new());
        form.append_pair("client_id", &self.client_id)
            .append_pair("scope", GRANTED_SCOPES)
            .append_pair("client_info", "1");
        match refresh_token {
            Some(refresh_token) => { form.append_pair("grant_type", "refresh_token").append_pair("refresh_token", refresh_token.secret()); }
            None => {
                form.append_pair("grant_type", assertion.as_ref().map_or("password", |assertion| assertion.grant_type))
                    .append_pair("username", self.username.secret()).append_pair("password", self.password.secret());
                if let Some(assertion) = assertion { form.append_pair("assertion", &assertion.encoded); }
            }
        }
        let response = self.client.post(endpoint.clone()).header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(form.finish()).send().await.map_err(|_| invalid("Azure username/password token endpoint could not be reached"))?;
        if response.status() != reqwest::StatusCode::OK { return Err(ExchangeFailure::Service(invalid("Azure username/password token exchange failed"))); }
        parse_token(&federation::read_body(response).await?, endpoint.path().starts_with("/adfs/"), OffsetDateTime::now_utc()).map_err(ExchangeFailure::Other)
    }

    async fn silent(&self, sessions: &mut Sessions, account: &Option<AccountId>, force: bool) -> azure_core::Result<Session> {
        let now = OffsetDateTime::now_utc();
        let cached = sessions.lookup(account, now);
        if account.is_none() { return cached.ok_or_else(|| invalid("Azure public client has no cached token")); }
        if account == &Some(AccountId::Missing) { return Err(invalid("Azure public client account has no identifier")); }
        let usable = cached.as_ref().is_some_and(|session| session.usable_at(now));
        let proactive = !force && usable && cached.as_ref().is_some_and(|session| session.proactive_at(now));
        if !force && usable && !proactive { return Ok(cached.expect("usable cached token")); }
        let refresh_token = sessions.refresh_token(account).ok_or_else(|| invalid("Azure public client has no cached refresh token"))?;
        match self.exchange(Some(refresh_token)).await {
            Ok(next) => { sessions.store(&next)?; Ok(next) }
            Err(ExchangeFailure::Service(_)) if proactive => Ok(cached.expect("usable proactive token")),
            Err(error) => Err(error.error()),
        }
    }

    async fn password_token(&self, sessions: &mut Sessions) -> azure_core::Result<AccessToken> {
        let next = self.exchange(None).await.map_err(ExchangeFailure::error)?;
        sessions.store(&next)?;
        sessions.last_account = Some(next.account);
        Ok(next.token)
    }
}

#[async_trait]
impl TokenCredential for PasswordCredential {
    async fn get_token(&self, scopes: &[&str], _: Option<TokenRequestOptions<'_>>) -> azure_core::Result<AccessToken> {
        if scopes != [STORAGE_SCOPE] { return Err(invalid("Azure username/password storage credential requires the storage scope")); }
        let mut sessions = self.sessions.lock().await;
        if let Some(account) = sessions.last_account.clone() {
            if let Ok(mut next) = self.silent(&mut sessions, &account, false).await {
                if next.token.expires_on <= OffsetDateTime::now_utc() + azure_core::time::Duration::seconds(300) {
                    match self.silent(&mut sessions, &account, true).await { Ok(forced) => next = forced, Err(_) => return self.password_token(&mut sessions).await }
                }
                sessions.last_account = Some(next.account);
                return Ok(next.token);
            }
        }
        self.password_token(&mut sessions).await
    }
}

#[cfg(test)]
#[path = "password_tests.rs"]
mod tests;
