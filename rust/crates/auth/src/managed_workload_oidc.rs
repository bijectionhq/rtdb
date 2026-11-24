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
        if !oidc::workload_issuer_url(&issuer_url)
            || issuer_ca_pem.trim().is_empty()
            || issuer_ca_pem.len() > 64 * 1024
        {
            return Err(invalid(
                "Managed workload issuers require an HTTPS issuer URL and a private CA bundle up to 64 KiB",
            ));
        }
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
pub struct ManagedWorkloadOidcConfig {
    create: WorkloadIssuerConfig,
    assignment: WorkloadIssuerConfig,
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
        if create_prefix.starts_with(assignment_prefix)
            || assignment_prefix.starts_with(create_prefix)
        {
            return Err(invalid(
                "Create and assignment client ID prefixes must be disjoint",
            ));
        }
        if create.grants.assignment().is_some() || assignment.grants.assignment().is_none() {
            return Err(invalid(
                "Only the assignment issuer requires the configured Original authority point",
            ));
        }
        Ok(Self { create, assignment })
    }

    pub fn create(&self) -> &WorkloadIssuerConfig {
        &self.create
    }
    pub fn assignment(&self) -> &WorkloadIssuerConfig {
        &self.assignment
    }
}

pub(crate) struct ManagedWorkloadOidc {
    create: Arc<oidc::Provider>,
    assignment: Arc<oidc::Provider>,
}

impl ManagedWorkloadOidc {
    pub async fn open(
        config: ManagedWorkloadOidcConfig,
        memory: Arc<MemoryBudget>,
        clock: Arc<dyn Clock>,
        timeout: Duration,
        maximum_response_bytes: usize,
    ) -> Result<Self> {
        let transport = |issuer: &WorkloadIssuerConfig| {
            NativeHttp::with_issuer_ca(
                Arc::clone(&memory),
                timeout,
                maximum_response_bytes,
                Some(&issuer.issuer_ca_pem),
            )
            .map(Arc::new)
        };
        let create_http = transport(&config.create)?;
        let assignment_http = transport(&config.assignment)?;
        Self::discover(config, memory, clock, create_http, assignment_http).await
    }

    async fn discover(
        config: ManagedWorkloadOidcConfig,
        memory: Arc<MemoryBudget>,
        clock: Arc<dyn Clock>,
        create_http: Arc<dyn HttpTransport>,
        assignment_http: Arc<dyn HttpTransport>,
    ) -> Result<Self> {
        let create = oidc::Provider::open_workload(
            config.create.issuer_url,
            config.create.grants,
            Arc::clone(&memory),
            Arc::clone(&clock),
            create_http,
        )
        .await?;
        let assignment = oidc::Provider::open_workload(
            config.assignment.issuer_url,
            config.assignment.grants,
            memory,
            clock,
            assignment_http,
        )
        .await?;
        Ok(Self { create, assignment })
    }

    pub async fn verify(&self, id: &str, secret: &str) -> Result<Arc<Principal>> {
        let (provider, permits): (_, fn(&VerifiedWorkloadGrant) -> bool) = match (
            self.create.workload_config(id),
            self.assignment.workload_config(id),
        ) {
            (Some(_), None) => (&self.create, VerifiedWorkloadGrant::is_create),
            (None, Some(_)) => (&self.assignment, VerifiedWorkloadGrant::is_assignment),
            _ => return Err(workload_grant::invalid_grant()),
        };
        let principal = provider.client_credentials(id, secret).await?;
        if principal.workload_grant().is_some_and(permits) {
            return Ok(principal);
        }
        Err(workload_grant::invalid_grant())
    }
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
        claim: Value,
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
                claim,
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
                    (
                        200,
                        json!({"sub": "subject", "rtdb_workload_grant": self.claim}),
                    )
                }
            };
            HttpResponse::new(status, body, &self.memory)
        }
    }

    fn config() -> ManagedWorkloadOidcConfig {
        let grants = |prefix: &str| WorkloadGrantConfig::new("cell".into(), prefix.into()).unwrap();
        let point = Point::new("public.app_installation", "authority".into(), Timestamp(0)).unwrap();
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
            Arc::clone(&memory),
            Arc::new(FixedClock),
            create,
            assignment,
        )
        .await
        .unwrap();
        Authenticator {
            authentication: Authentication::ManagedWorkloadOidc(providers),
            memory,
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
                Arc::clone(&memory),
                Arc::new(FixedClock),
                create.clone(),
                assignment.clone(),
            )
            .await;
            assert_eq!(
                result.err().unwrap().code,
                "xtdb.authn/oidc-discovery-failed"
            );
            assert_eq!((create.requests(), assignment.requests()), (1, 0));
        }
    }
}
