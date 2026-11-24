use std::sync::Arc;
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use rtdb_auth::{Authenticator, Method};
use rtdb_node::SessionIdentity;
use rtdb_types::{Anomaly, AnomalyCategory};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tonic::{Request, Status};
use uuid::Uuid;
use zeroize::Zeroizing;
use crate::state::{Caller, Owner};

pub(crate) const COOKIE: &str = "arrow_flight_session_id";
pub(crate) enum Credentials<'a> { None, Basic { raw: &'a str, user: Zeroizing<String>, secret: Zeroizing<String> }, Client { raw: &'a str, id: Zeroizing<String>, secret: Zeroizing<String> }, Bearer(&'a str) }
impl<'a> Credentials<'a> {
    pub fn parse<T>(request: &'a Request<T>) -> Result<Self, Status> {
        let mut values = request.metadata().get_all("authorization").iter();
        let Some(value) = values.next() else { return Ok(Self::None); };
        if values.next().is_some() { return Err(unauthenticated("multiple authorization headers")); }
        let raw = value.to_str().map_err(|_| unauthenticated("invalid authorization header"))?;
        let (scheme, encoded) = raw.trim().split_once(' ').ok_or_else(|| unauthenticated("malformed authorization header"))?;
        if scheme.eq_ignore_ascii_case("bearer") { return if encoded.is_empty() { Err(unauthenticated("empty bearer token")) } else { Ok(Self::Bearer(encoded)) }; }
        if !scheme.eq_ignore_ascii_case("basic") && !scheme.eq_ignore_ascii_case("client") { return Err(unauthenticated("unsupported authorization scheme")); }
        let bytes = Zeroizing::new(STANDARD.decode(encoded).map_err(|_| unauthenticated("malformed authorization credentials"))?);
        let decoded = std::str::from_utf8(&bytes).map_err(|_| unauthenticated("authorization credentials are not UTF-8"))?;
        let (user, secret) = decoded.split_once(':').filter(|(user, _)| !user.is_empty()).ok_or_else(|| unauthenticated("authorization credentials require a nonempty identity"))?;
        let user = Zeroizing::new(user.into()); let secret = Zeroizing::new(secret.into());
        Ok(if scheme.eq_ignore_ascii_case("basic") { Self::Basic { raw, user, secret } } else { Self::Client { raw, id: user, secret } })
    }
    pub fn raw(&self) -> Option<&str> { match self { Self::Basic { raw, .. } | Self::Client { raw, .. } => Some(raw), _ => None } }
    pub fn bearer(&self) -> Option<&str> { if let Self::Bearer(id) = self { Some(id) } else { None } }
}
pub(crate) fn cookie<T>(request: &Request<T>) -> Option<&str> {
    request.metadata().get_all("cookie").iter().filter_map(|value| value.to_str().ok()).flat_map(|value| value.split(';')).filter_map(|part| part.trim().split_once('=')).find_map(|(name, value)| (name == COOKIE && !value.is_empty()).then_some(value))
}
pub(crate) fn equal(left: &[u8], right: &[u8]) -> bool { bool::from(left.ct_eq(right)) }
pub(crate) struct Authentication { authenticator: Arc<Authenticator>, key: Zeroizing<[u8; 32]> }
impl Authentication {
    pub fn new(authenticator: Arc<Authenticator>) -> Self { let mut key = [0; 32]; key[..16].copy_from_slice(Uuid::new_v4().as_bytes()); key[16..].copy_from_slice(Uuid::new_v4().as_bytes()); Self { authenticator, key: Zeroizing::new(key) } }
    pub fn mac(&self, raw: &str) -> [u8; 32] { let mut mac = Hmac::<Sha256>::new_from_slice(self.key.as_ref()).expect("HMAC key"); mac.update(raw.as_bytes()); mac.finalize().into_bytes().into() }
    pub async fn authenticate(&self, credentials: &Credentials<'_>, peer: Option<&str>) -> Result<Arc<Caller>, Status> {
        let (principal, verified_credential) = match credentials {
            Credentials::None => { if self.authenticator.method_for(None, peer) != Some(Method::Trust) { return Err(unauthenticated("credentials required")); } (self.authenticator.unauthenticated().map_err(auth_error)?, false) }
            Credentials::Bearer(_) => return Err(unauthenticated("unknown session")),
            Credentials::Basic { user, secret, .. } => match self.authenticator.method_for(Some(user), peer) {
                Some(Method::Trust) => (self.authenticator.authenticate_trusted(user).map_err(auth_error)?, false),
                Some(Method::Password) => (self.authenticator.verify_password(user, secret).await.map_err(auth_error)?, true),
                Some(Method::ClientCredentials) => return Err(unauthenticated("client credentials require Client authorization")),
                Some(Method::DeviceAuth) => return Err(Status::unimplemented("device authorization is not supported over Flight SQL")),
                None => return Err(unauthenticated("no matching authentication rule")),
            },
            Credentials::Client { id, secret, .. } => {
                let principal = self.authenticator.verify_client_credentials(id, secret).await.map_err(auth_error)?;
                if self.authenticator.method_for(principal.user(), peer) != Some(Method::ClientCredentials) { return Err(unauthenticated("client credentials authentication failed")); } (principal, true)
            }
        };
        let owner = if matches!(credentials, Credentials::None) { Owner::Anonymous } else { Owner::Authenticated(Uuid::new_v4()) };
        Ok(Arc::new(Caller { owner, principal, mac: credentials.raw().map(|raw| self.mac(raw)), verified_credential }))
    }
}
pub(crate) fn auth_error(error: Anomaly) -> Status { match error.category { AnomalyCategory::Fault => Status::internal("authentication failed"), AnomalyCategory::Unsupported => Status::unimplemented(error.message.unwrap_or_default()), _ => unauthenticated(error.message.unwrap_or_else(|| "authentication failed".into())) } }
pub(crate) fn unauthenticated(message: impl Into<String>) -> Status { Status::unauthenticated(message) }

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn label_scope_survives_flight_credential_authentication() {
        use rtdb_auth::{AuthConfig, HttpRequest, HttpResponse, HttpTransport, MethodRule, OidcConfig, Secret};
        use rtdb_columnar::MemoryBudget;
        use serde_json::json;

        struct Issuer(Arc<MemoryBudget>);

        #[async_trait::async_trait]
        impl HttpTransport for Issuer {
            async fn request(&self, request: HttpRequest) -> rtdb_types::Result<HttpResponse> {
                let body = match request {
                    HttpRequest::Get { bearer: None, .. } => json!({
                        "token_endpoint": "https://issuer.example/token",
                        "userinfo_endpoint": "https://issuer.example/userinfo",
                    }),
                    HttpRequest::Form { .. } => json!({"access_token":"token", "expires_in":60}),
                    HttpRequest::Get { bearer: Some(_), .. } => {
                        let incarnation = [1; 16];
                        json!({ "sub":"client", "rtdb_label_scope": {
                            "database":"main", "incarnation":incarnation, "read":[[1]], "write":[],
                        } })
                    }
                };
                HttpResponse::new(200, body, &self.0)
            }
        }

        let memory = MemoryBudget::new(8 << 20);
        let authenticator = Authenticator::open(AuthConfig::OpenIdConnect(OidcConfig {
            issuer_url: "https://issuer.example".parse().unwrap(),
            client_id: "rtdb".into(),
            client_secret: Secret::new("configured"),
            rules: vec![MethodRule::any(Method::ClientCredentials)],
        }), memory.clone(), Arc::new(rtdb_node::SystemClock), Arc::new(Issuer(memory))).await.unwrap();
        let mut request = Request::new(());
        request.metadata_mut().insert("authorization", "Client Y2xpZW50OnNlY3JldA==".parse().unwrap());
        let credentials = Credentials::parse(&request).unwrap();
        let caller = Authentication::new(authenticator).authenticate(&credentials, None).await.unwrap();
        assert!(caller.principal.label_scope().is_some());
        assert!(caller.principal.authorize_database("main", rtdb_node::DatabaseAccess::Read).is_err());
    }

    #[tokio::test]
    async fn trusted_login_has_no_verified_custody_credential() {
        use rtdb_auth::{AuthConfig, NativeHttp, Secret};
        use rtdb_columnar::MemoryBudget;
        use std::time::Duration;

        let memory = MemoryBudget::new(1 << 20);
        let mut request = Request::new(());
        request.metadata_mut().insert("authorization", "Basic eHRkYjpzZWNyZXQ=".parse().unwrap());
        let credentials = Credentials::parse(&request).unwrap();
        for password in [None, Some(Secret::new("secret"))] {
            let verified = password.is_some();
            let http = Arc::new(NativeHttp::new(memory.clone(), Duration::from_secs(1), 1024).unwrap());
            let auth = Authenticator::open(AuthConfig::SingleRootUser { password }, memory.clone(), Arc::new(rtdb_node::SystemClock), http).await.unwrap();
            let caller = Authentication::new(auth).authenticate(&credentials, None).await.unwrap();
            assert_eq!(caller.principal.user_id(), Some("xtdb"));
            assert_eq!(caller.verified_credential, verified);
        }
    }

    #[test]
    fn absent_authentication_messages_follow_category_specific_policy() {
        for (category, code, absent, present) in [(AnomalyCategory::Fault, tonic::Code::Internal, "authentication failed", "authentication failed"), (AnomalyCategory::Unsupported, tonic::Code::Unimplemented, "", ""), (AnomalyCategory::Forbidden, tonic::Code::Unauthenticated, "authentication failed", "")] {
            let status = auth_error(Anomaly::new_nullable(category, "test/auth", None));
            assert_eq!(status.code(), code); assert_eq!(status.message(), absent);
            assert_eq!(auth_error(Anomaly::new(category, "test/auth", "")).message(), present);
        }
        let response = auth_error(Anomaly::new_nullable(AnomalyCategory::Unsupported, "test/auth", None)).into_http::<()>();
        assert!(!response.headers().contains_key("grpc-message"));
    }
    #[test]
    fn verifiable_credentials_preserve_raw_header_and_empty_secret() {
        let mut request = Request::new(()); request.metadata_mut().insert("authorization", "  bAsIc dXNlcjo=  ".parse().unwrap());
        let Credentials::Basic { raw, user, secret } = Credentials::parse(&request).unwrap() else { panic!("basic credentials"); };
        assert_eq!(raw, "  bAsIc dXNlcjo=  "); assert_eq!(&*user, "user"); assert!(secret.is_empty());
        request.metadata_mut().insert("authorization", "Client aWQ6c2VjcmV0OnRhaWw=".parse().unwrap());
        let Credentials::Client { id, secret, .. } = Credentials::parse(&request).unwrap() else { panic!("client credentials"); };
        assert_eq!(&*id, "id"); assert_eq!(&*secret, "secret:tail");
    }
    #[test]
    fn duplicate_auth_headers_and_empty_id_are_rejected() {
        let mut request = Request::new(()); request.metadata_mut().append("authorization", "Basic Og==".parse().unwrap());
        assert!(Credentials::parse(&request).is_err()); request.metadata_mut().append("authorization", "Bearer token".parse().unwrap()); assert!(Credentials::parse(&request).is_err());
    }
    #[test]
    fn cookie_parser_selects_first_nonempty_session_cookie() {
        let mut request = Request::new(()); request.metadata_mut().append("cookie", "x=y; arrow_flight_session_id=".parse().unwrap()); request.metadata_mut().append("cookie", " arrow_flight_session_id=one; arrow_flight_session_id=two".parse().unwrap());
        assert_eq!(cookie(&request), Some("one"));
    }
}
