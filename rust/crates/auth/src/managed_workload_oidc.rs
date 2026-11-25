use std::sync::Arc;
use std::time::Duration;

use rtdb_columnar::MemoryBudget;
use rtdb_node::Clock;
use rtdb_types::{Anomaly, Result};
use url::Url;

use crate::{
    HttpTransport, NativeHttp, Principal, VerifiedWorkloadGrant, WorkloadGrantConfig, oidc,
    workload_grant,
};

#[derive(Clone, Debug)]
pub struct WorkloadIssuerConfig {
    issuer_url: Url,
    issuer_ca_pem: String,
    grants: WorkloadGrantConfig,
}

impl WorkloadIssuerConfig {
    pub fn new(
        issuer_url: Url,
        issuer_ca_pem: String,
        grants: WorkloadGrantConfig,
    ) -> Result<Self> {
        validate_issuer(&issuer_url, &issuer_ca_pem)?;
        Ok(Self {
            issuer_url,
            issuer_ca_pem,
            grants,
        })
    }

    pub fn issuer_url(&self) -> &Url {
        &self.issuer_url
    }
    pub fn grants(&self) -> &WorkloadGrantConfig {
        &self.grants
    }
}

#[derive(Clone, Debug)]
pub struct ScopedAudienceIssuerConfig {
    issuer_url: Url,
    issuer_ca_pem: String,
    client_id_prefix: String,
}

impl ScopedAudienceIssuerConfig {
    pub fn new(issuer_url: Url, issuer_ca_pem: String, client_id_prefix: String) -> Result<Self> {
        validate_issuer(&issuer_url, &issuer_ca_pem)?;
        if !workload_grant::valid_client_id_prefix(&client_id_prefix) {
            return Err(invalid(
                "The scoped audience issuer requires a bounded client ID prefix",
            ));
        }
        Ok(Self {
            issuer_url,
            issuer_ca_pem,
            client_id_prefix,
        })
    }

    pub fn issuer_url(&self) -> &Url {
        &self.issuer_url
    }
    pub fn client_id_prefix(&self) -> &str {
        &self.client_id_prefix
    }
}

#[derive(Clone, Debug)]
pub struct ManagedWorkloadOidcConfig {
    create: WorkloadIssuerConfig,
    assignment: WorkloadIssuerConfig,
    scoped_audience: Option<ScopedAudienceIssuerConfig>,
    original_family: Option<WorkloadIssuerConfig>,
}

impl ManagedWorkloadOidcConfig {
    pub fn new(create: WorkloadIssuerConfig, assignment: WorkloadIssuerConfig) -> Result<Self> {
        if create.issuer_url.origin() == assignment.issuer_url.origin() {
            return Err(invalid(
                "Create and assignment issuers require distinct HTTPS origins",
            ));
        }
        let create_prefix = create.grants.client_id_prefix();
        let assignment_prefix = assignment.grants.client_id_prefix();
        if prefixes_overlap(create_prefix, assignment_prefix) {
            return Err(invalid(
                "Create and assignment client ID prefixes must be disjoint",
            ));
        }
        if create.grants.assignment().is_some() || assignment.grants.assignment().is_none() {
            return Err(invalid(
                "Only the assignment issuer requires the configured Original authority point",
            ));
        }
        Ok(Self {
            create,
            assignment,
            scoped_audience: None,
            original_family: None,
        })
    }

    pub fn with_scoped_audience(mut self, scope: ScopedAudienceIssuerConfig) -> Result<Self> {
        for workload in [&self.create, &self.assignment].into_iter().chain(self.original_family.as_ref()) {
            if scope.issuer_url.origin() == workload.issuer_url.origin() {
                return Err(invalid(
                    "The scoped audience issuer requires a distinct HTTPS origin",
                ));
            }
            if prefixes_overlap(&scope.client_id_prefix, workload.grants.client_id_prefix()) {
                return Err(invalid(
                    "The scoped audience and workload client ID prefixes must be disjoint",
                ));
            }
        }
        self.scoped_audience = Some(scope);
        Ok(self)
    }

    pub fn with_original_family(mut self, mut issuer: WorkloadIssuerConfig) -> Result<Self> {
        let conflicts = |url: &Url, prefix: &str| issuer.issuer_url.origin() == url.origin()
            || prefixes_overlap(issuer.grants.client_id_prefix(), prefix);
        if issuer.grants.assignment().is_some()
            || [&self.create, &self.assignment].iter().any(|other|
                conflicts(&other.issuer_url, other.grants.client_id_prefix()))
            || self.scoped_audience.as_ref().is_some_and(|scope|
                conflicts(&scope.issuer_url, &scope.client_id_prefix))
        {
            return Err(invalid("Original family authoring requires a distinct issuer origin and client ID prefix"));
        }
        issuer.grants = issuer.grants.for_original_family(
            self.assignment.grants.assignment().ok_or_else(|| invalid("Missing Original authority point"))?.clone(),
        );
        self.original_family = Some(issuer);
        Ok(self)
    }

    pub fn create(&self) -> &WorkloadIssuerConfig {
        &self.create
    }
    pub fn assignment(&self) -> &WorkloadIssuerConfig {
        &self.assignment
    }
    pub fn scoped_audience(&self) -> Option<&ScopedAudienceIssuerConfig> {
        self.scoped_audience.as_ref()
    }

    pub fn original_family(&self) -> Option<&WorkloadIssuerConfig> {
        self.original_family.as_ref()
    }
}

pub(crate) struct ManagedWorkloadOidc {
    create: Arc<oidc::Provider>,
    assignment: Arc<oidc::Provider>,
    scoped_audience: Option<Arc<oidc::Provider>>,
    original_family: Option<Arc<oidc::Provider>>,
}

impl ManagedWorkloadOidc {
    pub async fn open(
        config: ManagedWorkloadOidcConfig,
        memory: Arc<MemoryBudget>,
        clock: Arc<dyn Clock>,
        timeout: Duration,
        maximum_response_bytes: usize,
    ) -> Result<Self> {
        let transport = |_: &Url, ca: &str| -> Result<Arc<dyn HttpTransport>> {
            Ok(Arc::new(NativeHttp::with_issuer_ca(
                Arc::clone(&memory),
                timeout,
                maximum_response_bytes,
                Some(ca),
            )?))
        };
        Self::discover(config, &memory, clock, transport).await
    }

    async fn discover(
        config: ManagedWorkloadOidcConfig,
        memory: &Arc<MemoryBudget>,
        clock: Arc<dyn Clock>,
        transport: impl Fn(&Url, &str) -> Result<Arc<dyn HttpTransport>>,
    ) -> Result<Self> {
        let create_http = transport(&config.create.issuer_url, &config.create.issuer_ca_pem)?;
        let assignment_http = transport(
            &config.assignment.issuer_url,
            &config.assignment.issuer_ca_pem,
        )?;
        let scoped = config
            .scoped_audience
            .map(|scope| {
                let http = transport(&scope.issuer_url, &scope.issuer_ca_pem)?;
                Ok::<_, Anomaly>((scope, http))
            })
            .transpose()?;
        let create = oidc::Provider::open_workload(
            config.create.issuer_url,
            config.create.grants,
            Arc::clone(memory),
            Arc::clone(&clock),
            create_http,
        )
        .await?;
        let assignment = oidc::Provider::open_workload(
            config.assignment.issuer_url,
            config.assignment.grants,
            Arc::clone(memory),
            Arc::clone(&clock),
            assignment_http,
        )
        .await?;
        let original_family = match config.original_family {
            Some(issuer) => {
                let http = transport(&issuer.issuer_url, &issuer.issuer_ca_pem)?;
                Some(oidc::Provider::open_workload(
                    issuer.issuer_url, issuer.grants, Arc::clone(memory), Arc::clone(&clock), http,
                ).await?)
            }
            None => None,
        };
        let scoped_audience = match scoped {
            Some((scope, http)) => Some(
                oidc::Provider::open_scoped_audience(
                    scope.issuer_url,
                    scope.client_id_prefix,
                    Arc::clone(memory),
                    clock,
                    http,
                )
                .await?,
            ),
            None => None,
        };
        Ok(Self {
            create,
            assignment,
            scoped_audience,
            original_family,
        })
    }

    pub async fn verify(&self, id: &str, secret: &str) -> Result<Arc<Principal>> {
        let (provider, permits): (_, fn(&VerifiedWorkloadGrant) -> bool) = match (
            self.create.workload_config(id),
            self.assignment.workload_config(id),
            self.scoped_audience
                .as_ref()
                .filter(|provider| provider.accepts_scoped_audience(id)),
            self.original_family.as_ref().filter(|provider| provider.workload_config(id).is_some()),
        ) {
            (Some(_), None, None, None) => (&self.create, VerifiedWorkloadGrant::is_create),
            (None, Some(_), None, None) => (&self.assignment, VerifiedWorkloadGrant::is_assignment_issued),
            (None, None, Some(provider), None) => return provider.client_credentials(id, secret).await,
            (None, None, None, Some(provider)) => (provider, VerifiedWorkloadGrant::is_original_family),
            _ => return Err(workload_grant::invalid_grant()),
        };
        let principal = provider.client_credentials(id, secret).await?;
        if principal.workload_grant().is_some_and(permits) {
            return Ok(principal);
        }
        Err(workload_grant::invalid_grant())
    }
}

fn validate_issuer(url: &Url, ca: &str) -> Result<()> {
    if !oidc::scoped_issuer_url(url) || ca.trim().is_empty() || ca.len() > 64 * 1024 {
        return Err(invalid(
            "Managed OIDC issuers require an HTTPS issuer URL and a private CA bundle up to 64 KiB",
        ));
    }
    Ok(())
}

fn prefixes_overlap(left: &str, right: &str) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb.auth/managed-workload-oidc-config", message)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rtdb_node::SessionIdentity;
    use rtdb_tx::transact::workload::Point;
    use rtdb_types::Timestamp;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        AuthConfig, Authentication, Authenticator, HttpRequest, HttpResponse, Method, MethodRule,
        OidcConfig, Secret, WorkloadOperation,
    };

    const CREATE: &str = "https://create.example.invalid/";
    const ASSIGNMENT: &str = "https://assignment.example.invalid/";
    const AUDIENCE: &str = "https://audience.example.invalid/";

    struct FixedClock;
    impl Clock for FixedClock {
        fn now(&self) -> Timestamp {
            Timestamp(0)
        }
    }

    struct Issuer {
        origin: &'static str,
        prefix: &'static str,
        discovery: Value,
        token_status: u16,
        userinfo: Value,
        requests: AtomicUsize,
        memory: Arc<MemoryBudget>,
    }

    impl Issuer {
        fn new(
            origin: &'static str,
            prefix: &'static str,
            claim: Value,
            memory: &Arc<MemoryBudget>,
        ) -> Self {
            Self {
                origin,
                prefix,
                userinfo: json!({"sub": "subject", "rtdb_workload_grant": claim}),
                memory: Arc::clone(memory),
                token_status: 200,
                requests: AtomicUsize::new(0),
                discovery: json!({
                    "issuer": origin,
                    "token_endpoint": format!("{origin}token"),
                    "userinfo_endpoint": format!("{origin}userinfo"),
                }),
            }
        }

        fn requests(&self) -> usize {
            self.requests.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl HttpTransport for Issuer {
        async fn request(&self, request: HttpRequest) -> Result<HttpResponse> {
            self.requests.fetch_add(1, Ordering::Relaxed);
            let (status, body) = match request {
                HttpRequest::Get { url, bearer: None } => {
                    assert_eq!(
                        url.as_str(),
                        format!("{}.well-known/openid-configuration", self.origin)
                    );
                    (200, self.discovery.clone())
                }
                HttpRequest::Form { url, fields } => {
                    assert_eq!(url.as_str(), format!("{}token", self.origin));
                    assert_eq!(fields.len(), 4);
                    assert_eq!(
                        fields["client_id"].expose(),
                        format!("{}client", self.prefix)
                    );
                    assert_eq!(fields["client_secret"].expose(), "fixture");
                    assert_eq!(fields["scope"].expose(), "openid");
                    assert_eq!(fields["grant_type"].expose(), "client_credentials");
                    (
                        self.token_status,
                        json!({"access_token": "fixture-token", "expires_in": 1}),
                    )
                }
                HttpRequest::Get {
                    url,
                    bearer: Some(token),
                } => {
                    assert_eq!(url.as_str(), format!("{}userinfo", self.origin));
                    assert_eq!(token.expose(), "fixture-token");
                    (200, self.userinfo.clone())
                }
            };
            HttpResponse::new(status, body, &self.memory)
        }
    }

    fn config() -> ManagedWorkloadOidcConfig {
        let grants = |prefix: &str| WorkloadGrantConfig::new("cell".into(), prefix.into()).unwrap();
        let point =
            Point::new("public.app_installation", "authority".into(), Timestamp(0)).unwrap();
        let issuer = |url: &str, grants| {
            WorkloadIssuerConfig::new(url.parse().unwrap(), "mock CA".into(), grants).unwrap()
        };
        ManagedWorkloadOidcConfig::new(
            issuer(CREATE, grants("create-")),
            issuer(ASSIGNMENT, grants("assignment-").with_assignment(point)),
        )
        .unwrap()
    }

    fn claim(assignment: bool) -> Value {
        let incarnation = [7; 16];
        let grant_id = [9; 16];
        let mut claim = json!({
            "version": if assignment { 2 } else { 1 }, "audience": "cell", "database": "db",
            "incarnation": incarnation, "grant_id": grant_id, "expires_at_micros": 800_000,
        });
        if assignment {
            let original_namespace = [8; 32];
            claim["authority"] = json!({"role": "receipt-recovery", "original_namespace": original_namespace, "family": "write"});
        } else {
            claim["operations"] = json!(["create-deployment"]);
        }
        claim
    }

    async fn authenticate_with(create: Arc<Issuer>, assignment: Arc<Issuer>) -> Authenticator {
        let memory = Arc::clone(&create.memory);
        let providers = ManagedWorkloadOidc::discover(
            config(),
            &memory,
            Arc::new(FixedClock),
            transports(&create, &assignment, None),
        )
        .await
        .unwrap();
        Authenticator {
            authentication: Authentication::ManagedWorkloadOidc(providers),
            memory,
        }
    }

    fn transports<'a>(
        create: &'a Arc<Issuer>,
        assignment: &'a Arc<Issuer>,
        audience: Option<&'a Arc<Issuer>>,
    ) -> impl Fn(&Url, &str) -> Result<Arc<dyn HttpTransport>> + 'a {
        move |issuer, ca| {
            let transport = match issuer.as_str() {
                CREATE => create,
                ASSIGNMENT => assignment,
                AUDIENCE => {
                    assert_eq!(ca, "audience CA");
                    audience.expect("scoped audience transport")
                }
                _ => panic!("unexpected issuer"),
            };
            Ok(transport.clone())
        }
    }

    #[tokio::test]
    async fn enrolled_client_ids_select_separate_issuers_and_preserve_exact_grants() {
        let memory = MemoryBudget::new(8 << 20);
        let create = Arc::new(Issuer::new(CREATE, "create-", claim(false), &memory));
        let assignment = Arc::new(Issuer::new(ASSIGNMENT, "assignment-", claim(true), &memory));
        let auth = authenticate_with(create.clone(), assignment.clone()).await;
        assert_eq!(auth.method_for(None, None), Some(Method::ClientCredentials));
        assert!(!auth.is_superuser("xtdb"));
        assert!(auth.verify_password("subject", "fixture").await.is_err());
        assert!(auth.start_device_auth("subject").await.is_err());
        assert!(
            auth.verify_client_credentials("unknown-client", "fixture")
                .await
                .is_err()
        );
        assert_eq!((create.requests(), assignment.requests()), (1, 1));

        let principal = auth
            .verify_client_credentials("create-client", "fixture")
            .await
            .unwrap();
        let create_grant = principal.workload_grant().unwrap();
        assert_eq!((create.requests(), assignment.requests()), (3, 1));
        let operation = WorkloadOperation::CreateDeployment;
        assert!(
            create_grant
                .admit(operation, "db", [7; 16], Timestamp(0))
                .is_ok()
        );
        assert!(
            create_grant
                .admit(operation, "other", [7; 16], Timestamp(0))
                .is_err()
        );
        assert!(
            create_grant
                .admit(operation, "db", [6; 16], Timestamp(0))
                .is_err()
        );
        assert!(
            create_grant
                .assignment("db", [7; 16], Timestamp(0))
                .is_err()
        );
        assert!(principal.revalidate(Timestamp(800_000)).await.is_err());

        let principal = auth
            .verify_client_credentials("assignment-client", "fixture")
            .await
            .unwrap();
        let assignment_grant = principal.workload_grant().unwrap();
        assert_eq!((create.requests(), assignment.requests()), (3, 3));
        let (_, point, _) = assignment_grant
            .assignment("db", [7; 16], Timestamp(0))
            .unwrap();
        assert_eq!(Some(point), config().assignment.grants.assignment());
        assert!(
            assignment_grant
                .assignment("other", [7; 16], Timestamp(0))
                .is_err()
        );
        assert!(
            assignment_grant
                .assignment("db", [6; 16], Timestamp(0))
                .is_err()
        );
        assert!(
            assignment_grant
                .admit(operation, "db", [7; 16], Timestamp(0))
                .is_err()
        );
        assert_ne!(
            create_grant.receipt_namespace(),
            assignment_grant.receipt_namespace()
        );

        let configuration = config();
        for (issuer, transport, grant) in [
            (&configuration.create, create, create_grant),
            (&configuration.assignment, assignment, assignment_grant),
        ] {
            let oidc = AuthConfig::OpenIdConnect(OidcConfig {
                issuer_url: issuer.issuer_url.clone(),
                issuer_ca_pem: None,
                client_id: "service-client".into(),
                client_secret: Secret::new("unused"),
                rules: vec![MethodRule::any(Method::ClientCredentials)],
                workload_grants: Some(issuer.grants.clone()),
            });
            let id = format!("{}client", issuer.grants.client_id_prefix());
            let legacy =
                Authenticator::open(oidc, Arc::clone(&memory), Arc::new(FixedClock), transport)
                    .await
                    .unwrap();
            let principal = legacy
                .verify_client_credentials(&id, "fixture")
                .await
                .unwrap();
            assert_eq!(
                principal.workload_grant().unwrap().receipt_namespace(),
                grant.receipt_namespace()
            );
        }
    }

    #[tokio::test]
    async fn managed_original_control_requires_its_exact_issuer_authority_and_operation() {
        use rtdb_node::{DatabaseAccess, serving::custody_release::CustodyCarrierAuthority};
        use crate::OriginalCustodyOperation;

        let memory = MemoryBudget::new(8 << 20);
        let authority = CustodyCarrierAuthority {
            incarnation: [7; 16], source_history: [2; 32], installation: [3; 16],
            registry: [4; 32], original_writer: [5; 32],
        };
        let grant_id = [9_u8; 16];
        for (name, operation, other) in [
            ("release", OriginalCustodyOperation::Release, OriginalCustodyOperation::Dispose),
            ("dispose", OriginalCustodyOperation::Dispose, OriginalCustodyOperation::Release),
        ] {
            let claim = json!({
                "version": 3, "audience": "cell", "database": "db", "incarnation": authority.incarnation,
                "grant_id": grant_id, "expires_at_micros": 800_000,
                "original_custody": { "operation": name, "authority": authority },
            });
            let create = Arc::new(Issuer::new(CREATE, "create-", claim.clone(), &memory));
            let assignment = Arc::new(Issuer::new(ASSIGNMENT, "assignment-", claim, &memory));
            let auth = authenticate_with(create, assignment).await;
            assert!(auth.verify_client_credentials("create-client", "fixture").await.is_err());
            let principal = auth.verify_client_credentials("assignment-client", "fixture").await.unwrap();
            assert!(principal.authorize_database("db", DatabaseAccess::Write).is_err());
            let grant = principal.workload_grant().unwrap();
            assert!(grant.is_original_custody_control());
            assert!(!grant.is_assignment());
            assert!(grant.assignment("db", authority.incarnation, Timestamp(0)).is_err());
            assert_eq!(grant.admit_original_custody("db", authority, operation, Timestamp(0)).unwrap(), Timestamp(800_000));
            assert!(grant.admit_original_custody("db", authority, other, Timestamp(0)).is_err());
            assert!(grant.admit_original_custody("db", authority, operation, Timestamp(800_000)).is_err());
            assert!(grant.admit_original_custody("other", authority, operation, Timestamp(0)).is_err());
            assert!(grant.admit_original_custody("db", CustodyCarrierAuthority { original_writer: [6; 32], ..authority }, operation, Timestamp(0)).is_err());
        }
    }

    #[tokio::test]
    async fn wrong_role_or_token_failure_never_falls_through_to_the_other_issuer() {
        let memory = MemoryBudget::new(8 << 20);
        for (status, requests) in [(200, 3), (401, 2)] {
            let mut create = Issuer::new(CREATE, "create-", claim(true), &memory);
            create.token_status = status;
            let create = Arc::new(create);
            let assignment = Arc::new(Issuer::new(
                ASSIGNMENT,
                "assignment-",
                claim(false),
                &memory,
            ));
            let auth = authenticate_with(create.clone(), assignment.clone()).await;
            assert!(
                auth.verify_client_credentials("create-client", "fixture")
                    .await
                    .is_err()
            );
            assert_eq!((create.requests(), assignment.requests()), (requests, 1));
            assert!(
                auth.verify_client_credentials("assignment-client", "fixture")
                    .await
                    .is_err()
            );
            assert_eq!((create.requests(), assignment.requests()), (requests, 3));
        }
    }

    #[tokio::test]
    async fn discovery_rejects_the_other_issuer_and_its_endpoints() {
        let memory = MemoryBudget::new(8 << 20);
        for (field, value) in [
            ("issuer", ASSIGNMENT),
            ("token_endpoint", "https://assignment.example.invalid/token"),
            (
                "userinfo_endpoint",
                "https://assignment.example.invalid/userinfo",
            ),
        ] {
            let mut create = Issuer::new(CREATE, "create-", claim(false), &memory);
            create.discovery[field] = json!(value);
            let create = Arc::new(create);
            let assignment = Arc::new(Issuer::new(ASSIGNMENT, "assignment-", claim(true), &memory));
            let result = ManagedWorkloadOidc::discover(
                config(),
                &memory,
                Arc::new(FixedClock),
                transports(&create, &assignment, None),
            )
            .await;
            assert_eq!(
                result.err().unwrap().code,
                "xtdb.authn/oidc-discovery-failed"
            );
            assert_eq!((create.requests(), assignment.requests()), (1, 0));
        }
    }

    #[tokio::test]
    async fn scoped_audience_only_accepts_an_expiring_label_reader_from_its_own_issuer() {
        let memory = MemoryBudget::new(8 << 20);
        let incarnation = [7; 16];
        let scope = json!({"database":"db", "incarnation":incarnation, "read":[[6]], "write":[], "protected_reads":true});
        let create = Arc::new(Issuer::new(CREATE, "create-", claim(false), &memory));
        let mut assignment = Issuer::new(ASSIGNMENT, "assignment-", claim(true), &memory);
        assignment.userinfo = json!({"sub":"subject", "rtdb_label_scope":scope});
        let assignment = Arc::new(assignment);
        let mut writing = scope.clone();
        writing["write"] = json!([[6]]);
        for (userinfo, allowed) in [
            (json!({"sub":"subject", "rtdb_label_scope":scope}), true),
            (json!({"sub":"subject"}), false),
            (json!({"sub":"subject", "rtdb_label_scope":writing}), false),
            (
                json!({"sub":"subject", "rtdb_workload_grant":claim(true)}),
                false,
            ),
            (
                json!({"sub":"subject", "rtdb_label_scope":scope, "rtdb_workload_grant":null}),
                false,
            ),
        ] {
            let mut audience = Issuer::new(AUDIENCE, "audience-", Value::Null, &memory);
            audience.userinfo = userinfo;
            let audience = Arc::new(audience);
            let config = config()
                .with_scoped_audience(
                    ScopedAudienceIssuerConfig::new(
                        AUDIENCE.parse().unwrap(),
                        "audience CA".into(),
                        "audience-".into(),
                    )
                    .unwrap(),
                )
                .unwrap();
            let providers = ManagedWorkloadOidc::discover(
                config,
                &memory,
                Arc::new(FixedClock),
                transports(&create, &assignment, Some(&audience)),
            )
            .await
            .unwrap();
            let before = (create.requests(), assignment.requests());
            let result = providers.verify("audience-client", "fixture").await;
            assert_eq!((create.requests(), assignment.requests()), before);
            assert_eq!(audience.requests(), 3);
            if allowed {
                let principal = result.unwrap();
                let scope = principal.label_scope().unwrap();
                scope.admit("db", incarnation).unwrap();
                assert!(scope.admit("other", incarnation).is_err());
                assert!(scope.admit("db", [8; 16]).is_err());
                assert!(scope.permits_protected_reads());
                assert!(principal.workload_grant().is_none());
                assert!(principal.revalidate(Timestamp(1_000_000)).await.is_err());
                assert!(
                    principal
                        .authorize_database("db", rtdb_node::DatabaseAccess::Write)
                        .is_err()
                );
                assert!(
                    providers
                        .verify("assignment-client", "fixture")
                        .await
                        .is_err()
                );
                assert_eq!(audience.requests(), 3);
                let legacy = Authenticator::open(
                    AuthConfig::OpenIdConnect(OidcConfig {
                        issuer_url: AUDIENCE.parse().unwrap(),
                        issuer_ca_pem: None,
                        client_id: "service-client".into(),
                        client_secret: Secret::new("unused"),
                        rules: vec![MethodRule::any(Method::ClientCredentials)],
                        workload_grants: None,
                    }),
                    Arc::clone(&memory),
                    Arc::new(FixedClock),
                    audience,
                )
                .await
                .unwrap();
                let previous = legacy
                    .verify_client_credentials("audience-client", "fixture")
                    .await
                    .unwrap();
                assert_eq!(
                    scope.receipt_namespace(),
                    previous.label_scope().unwrap().receipt_namespace()
                );
            } else {
                assert!(result.is_err());
            }
        }
    }
}
