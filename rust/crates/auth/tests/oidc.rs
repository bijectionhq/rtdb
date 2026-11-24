use std::collections::VecDeque;
use std::sync::{Arc, atomic::{AtomicI64, AtomicUsize, Ordering}};

use async_trait::async_trait;
use rtdb_auth::*;
use rtdb_columnar::MemoryBudget;
use rtdb_node::{Clock, SessionIdentity};
use rtdb_types::{Result, Timestamp};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

struct TestClock(AtomicI64);
impl Clock for TestClock { fn now(&self) -> Timestamp { Timestamp(self.0.load(Ordering::SeqCst)) } }

struct Script {
    memory: Arc<MemoryBudget>,
    responses: Mutex<VecDeque<(u16, Value)>>,
    forms: Mutex<Vec<Vec<(String, String)>>>,
    refreshes: AtomicUsize,
    refresh_gate: Option<Semaphore>,
}
#[async_trait]
impl HttpTransport for Script {
    async fn request(&self, request: HttpRequest) -> Result<HttpResponse> {
        if let HttpRequest::Form { fields, .. } = request {
            if fields.get("grant_type").is_some_and(|value| value.expose() == "refresh_token") {
                self.refreshes.fetch_add(1, Ordering::SeqCst);
                if let Some(gate) = &self.refresh_gate { gate.acquire().await.unwrap().forget(); }
            }
            self.forms.lock().await.push(fields.into_iter().map(|(key, value)| (key, value.expose().into())).collect());
        }
        let (status, body) = self.responses.lock().await.pop_front().expect("unexpected OIDC HTTP request");
        HttpResponse::new(status, body, &self.memory)
    }
}

async fn setup(extra: Vec<(u16, Value)>, gate: bool) -> (Arc<Authenticator>, Arc<Script>, Arc<TestClock>) {
    setup_with_workload(extra, gate, None).await
}

async fn setup_with_workload(extra: Vec<(u16, Value)>, gate: bool, workload_grants: Option<WorkloadGrantConfig>) -> (Arc<Authenticator>, Arc<Script>, Arc<TestClock>) {
    let memory = MemoryBudget::new(8 << 20);
    let mut responses = VecDeque::from([
        (200, json!({"issuer":"https://issuer.example", "token_endpoint":"https://issuer.example/token", "userinfo_endpoint":"https://issuer.example/userinfo", "device_authorization_endpoint":"https://issuer.example/device"})),
    ]);
    responses.extend(extra);
    let script = Arc::new(Script { memory: Arc::clone(&memory), responses: Mutex::new(responses), forms: Mutex::new(Vec::new()), refreshes: AtomicUsize::new(0), refresh_gate: gate.then(|| Semaphore::new(0)) });
    let clock = Arc::new(TestClock(AtomicI64::new(0)));
    let auth = Authenticator::open(AuthConfig::OpenIdConnect(OidcConfig { issuer_url: "https://issuer.example".parse().unwrap(), client_id: "configured-client".into(), client_secret: Secret::new("configured-secret"), rules: vec![MethodRule::any(Method::Password)], workload_grants }), memory, clock.clone(), script.clone()).await.unwrap();
    (auth, script, clock)
}

#[tokio::test]
async fn simultaneous_revalidations_refresh_once_and_exact_expiry_is_still_valid() {
    let (auth, script, clock) = setup(vec![
        (200, json!({"access_token":"access-0", "refresh_token":"refresh-0", "expires_in":1})),
        (200, json!({"sub":"subject"})),
        (200, json!({"access_token":"access-1", "refresh_token":"refresh-1", "expires_in":3600})),
        (200, json!({"sub":"subject"})),
    ], true).await;
    let principal = auth.verify_password("login", "secret").await.unwrap();
    assert_eq!(principal.user_id(), Some("subject"));
    principal.revalidate(Timestamp(1_000_000)).await.unwrap();
    assert_eq!(script.refreshes.load(Ordering::SeqCst), 0);
    clock.0.store(2_000_000, Ordering::SeqCst);
    let first = { let principal = principal.clone(); tokio::spawn(async move { principal.revalidate(Timestamp(2_000_000)).await }) };
    while script.refreshes.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; }
    let second = { let principal = principal.clone(); tokio::spawn(async move { principal.revalidate(Timestamp(2_000_000)).await }) };
    script.refresh_gate.as_ref().unwrap().add_permits(1);
    first.await.unwrap().unwrap(); second.await.unwrap().unwrap();
    assert_eq!(script.refreshes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn refresh_rotation_omission_and_failure_leave_a_retryable_complete_token() {
    let (auth, script, clock) = setup(vec![
        (200, json!({"access_token":"access-0", "refresh_token":"refresh-0", "expires_in":0})),
        (200, json!({"sub":"subject"})),
        (200, json!({"access_token":"access-1", "refresh_token":"refresh-1", "expires_in":0})),
        (200, json!({"sub":"subject"})),
        (400, json!({"error":"temporarily_unavailable"})),
        (200, json!({"access_token":"access-2", "expires_in":0})),
        (200, json!({"sub":"subject"})),
        (200, json!({"access_token":"access-3", "expires_in":100})),
        (200, json!({"sub":"subject"})),
    ], false).await;
    let principal = auth.verify_password("login", "secret").await.unwrap();
    clock.0.store(1_000_000, Ordering::SeqCst);
    principal.revalidate(Timestamp(1_000_000)).await.unwrap();
    clock.0.store(2_000_000, Ordering::SeqCst);
    assert_eq!(principal.revalidate(Timestamp(2_000_000)).await.unwrap_err().code, "xtdb.authn/refresh-failed");
    principal.revalidate(Timestamp(2_000_000)).await.unwrap();
    clock.0.store(3_000_000, Ordering::SeqCst);
    principal.revalidate(Timestamp(3_000_000)).await.unwrap();
    let sent = script.forms.lock().await.iter().filter_map(|fields| fields.iter().find(|(key, _)| key == "refresh_token").map(|(_, value)| value.clone())).collect::<Vec<_>>();
    assert_eq!(sent, ["refresh-0", "refresh-1", "refresh-1", "refresh-1"]);
}

#[tokio::test]
async fn client_credentials_refreshes_with_its_own_client_and_no_expiry_never_refreshes() {
    let (auth, script, clock) = setup(vec![
        (200, json!({"access_token":"access-0", "expires_in":0})), (200, json!({"sub":"service-subject"})),
        (200, json!({"access_token":"access-1"})),
        (200, json!({"sub":"service-subject"})),
    ], false).await;
    let principal = auth.verify_client_credentials("service", "service-secret").await.unwrap();
    clock.0.store(1_000_000, Ordering::SeqCst);
    principal.revalidate(Timestamp(1_000_000)).await.unwrap();
    principal.revalidate(Timestamp(i64::MAX)).await.unwrap();
    for fields in script.forms.lock().await.iter() {
        assert!(fields.contains(&("client_id".into(), "service".into())));
        assert!(fields.contains(&("client_secret".into(), "service-secret".into())));
    }
    assert!(!auth.is_superuser("xtdb"));
}

#[tokio::test]
async fn label_scope_is_bound_to_the_issuer_database_and_incarnation() {
    let incarnation = [7; 16];
    let claim = json!({"database":"catalog", "incarnation":incarnation, "read":[[1, 2]], "write":[[3]]});
    let (auth, script, _) = setup(vec![
        (200, json!({"access_token":"scoped", "expires_in":1})),
        (200, json!({"sub":"service", "rtdb_label_scope":claim})),
    ], false).await;
    let principal = auth.verify_client_credentials("client", "secret").await.unwrap();
    let scope = principal.label_scope().unwrap();
    scope.admit("catalog", [7; 16]).unwrap();
    assert!(scope.admit("catalog", [8; 16]).is_err());
    assert!(scope.admit("another", [7; 16]).is_err());
    assert!(scope.labels().read.contains(&[1, 2][..]));
    assert!(scope.labels().write.contains(&[3][..]));
    assert!(principal.authorize_database("catalog", rtdb_node::DatabaseAccess::Read).is_err());
    principal.revalidate(Timestamp(999_999)).await.unwrap();
    assert!(principal.revalidate(Timestamp(1_000_000)).await.is_err());
    assert_eq!(script.forms.lock().await.len(), 1);
}

#[tokio::test]
async fn label_scope_rejects_an_invalid_claim_or_a_token_without_expiry() {
    let incarnation = [7; 16];
    let valid = json!({"database":"catalog", "incarnation": incarnation, "read":[], "write":[]});
    let invalid = json!({"database":"catalog", "incarnation": incarnation, "read":[[]], "write":[]});
    for (grant, claim) in [(json!({"access_token":"scoped"}), valid),
        (json!({"access_token":"scoped", "expires_in":1}), invalid)]
    {
        let (auth, _, _) = setup(vec![
            (200, grant),
            (200, json!({"sub":"service", "rtdb_label_scope":claim})),
        ], false).await;
        let error = auth.verify_client_credentials("client", "secret").await.err().unwrap();
        assert_eq!(error.code, "rtdb.auth/label-scope");
    }
}

#[tokio::test]
async fn label_scope_requires_a_new_session_if_assigned_during_refresh() {
    let incarnation = [7; 16];
    let scope = json!({"database":"catalog", "incarnation":incarnation, "read":[[1]], "write":[]});
    let (auth, _, clock) = setup(vec![
        (200, json!({"access_token":"original", "expires_in":0})),
        (200, json!({"sub":"service"})),
        (200, json!({"access_token":"replacement", "expires_in":60})),
        (200, json!({"sub":"service", "rtdb_label_scope":scope})),
    ], false).await;
    let principal = auth.verify_client_credentials("client", "secret").await.unwrap();
    assert!(principal.label_scope().is_none());
    clock.0.store(1_000_000, Ordering::SeqCst);
    assert_eq!(principal.revalidate(Timestamp(1_000_000)).await.unwrap_err().code, "xtdb.authn/refresh-failed");
}

fn create_claim(grant_id: [u8; 16], expires_at_micros: i64) -> Value {
    let incarnation = [7; 16];
    json!({
        "version": 1, "audience": "cell", "database": "catalog", "incarnation": incarnation,
        "grant_id": grant_id, "expires_at_micros": expires_at_micros, "operations": ["create-deployment"],
    })
}

fn workload_config() -> WorkloadGrantConfig {
    WorkloadGrantConfig::new("cell".into(), "workload-".into()).unwrap()
}

#[tokio::test]
async fn workload_grant_binds_exact_target_and_keeps_receipts_across_reauthentication() {
    let mut responses = Vec::new();
    for (id, expiry) in [([8; 16], 1_000_000), ([8; 16], 3_000_000), ([9; 16], 3_000_000)] {
        responses.push((200, json!({"access_token":"scoped", "expires_in":2})));
        responses.push((200, json!({"sub":"stable-subject", "rtdb_workload_grant":create_claim(id, expiry)})));
    }
    let (auth, _, clock) = setup_with_workload(responses, false, Some(workload_config())).await;
    let first = auth.verify_client_credentials("workload-0", "first-secret").await.unwrap();
    let grant = first.workload_grant().unwrap();
    let operation = WorkloadOperation::CreateDeployment;
    grant.admit(operation, "catalog", [7; 16], Timestamp(999_999)).unwrap();
    assert!(grant.admit(operation, "another", [7; 16], Timestamp(0)).is_err());
    assert!(grant.admit(operation, "catalog", [9; 16], Timestamp(0)).is_err());
    assert!(first.label_scope().is_none());
    assert!(!first.delegable());
    assert!(first.authorize_database("catalog", rtdb_node::DatabaseAccess::Read).is_err());
    assert!(first.authorize_database("catalog", rtdb_node::DatabaseAccess::Write).is_err());
    clock.0.store(1_000_000, Ordering::SeqCst);
    assert!(first.revalidate(clock.now()).await.is_err());
    let renewed = auth.verify_client_credentials("workload-1", "next-secret").await.unwrap();
    assert_eq!(grant.receipt_namespace(), renewed.workload_grant().unwrap().receipt_namespace());
    assert!(renewed.revalidate(Timestamp(3_000_000)).await.is_err());
    let other = auth.verify_client_credentials("workload-2", "next-secret").await.unwrap();
    assert_ne!(grant.receipt_namespace(), other.workload_grant().unwrap().receipt_namespace());
}

#[tokio::test]
async fn workload_enrollment_refuses_missing_invalid_and_mixed_claims() {
    let valid = create_claim([8; 16], 1_000_000);
    let mut userinfo = vec![
        json!({"sub":"subject"}),
        json!({"sub":"subject", "rtdb_workload_grant":null}),
        json!({"sub":"subject", "rtdb_workload_grant":valid.clone(), "rtdb_label_scope":null}),
    ];
    let zero = [0; 16];
    for (key, wrong) in [
        ("version", json!(2)),
        ("audience", json!("another")),
        ("database", json!("*")),
        ("incarnation", json!([7])),
        ("grant_id", json!(zero)),
        ("expires_at_micros", json!(0)),
        ("expires_at_micros", json!(300_000_001)),
        ("operations", json!(["transact"])),
        ("operations", json!([])),
        ("operations", json!(["create-deployment", "create-deployment"])),
        ("extra", json!(1)),
    ] {
        let mut claim = valid.clone();
        claim[key] = wrong;
        userinfo.push(json!({"sub":"subject", "rtdb_workload_grant":claim}));
    }
    for response in userinfo {
        let (auth, _, _) = setup_with_workload(vec![
            (200, json!({"access_token":"token", "expires_in":60})),
            (200, response),
        ], false, Some(workload_config())).await;
        assert_eq!(auth.verify_client_credentials("workload-0", "secret").await.err().unwrap().code, "rtdb.auth/workload-grant");
    }
}

#[tokio::test]
async fn workload_claims_cannot_be_used_on_a_legacy_login_or_an_unbounded_token() {
    let info = json!({"sub":"subject", "rtdb_workload_grant":create_claim([8; 16], 1_000_000)});
    let token = json!({"access_token":"token", "expires_in":60});
    for (client, config) in [("legacy", Some(workload_config())), ("workload-0", None)] {
        let (auth, _, _) = setup_with_workload(vec![(200, token.clone()), (200, info.clone())], false, config).await;
        assert!(auth.verify_client_credentials(client, "secret").await.is_err());
    }
    let (auth, _, _) = setup_with_workload(vec![(200, token.clone()), (200, info)], false, Some(workload_config())).await;
    assert!(auth.verify_password("legacy-user", "secret").await.is_err());
    let (auth, _, _) = setup_with_workload(vec![], false, Some(workload_config())).await;
    assert!(auth.verify_password("workload-0", "secret").await.is_err());
    for response in [json!({"access_token":"token"}), json!({"access_token":"token", "expires_in":301})] {
        let (auth, _, _) = setup_with_workload(vec![(200, response)], false, Some(workload_config())).await;
        assert!(auth.verify_client_credentials("workload-0", "secret").await.is_err());
    }
    let (auth, _, _) = setup_with_workload(vec![(200, token), (200, json!({"sub":"legacy"}))], false, Some(workload_config())).await;
    let legacy = auth.verify_client_credentials("legacy", "secret").await.unwrap();
    assert!(legacy.workload_grant().is_none());
    legacy.authorize_database("catalog", rtdb_node::DatabaseAccess::Write).unwrap();
}

#[tokio::test]
async fn device_flow_polls_pending_then_returns_userinfo_subject_and_supports_cancellation() {
    let (auth, _, _) = setup(vec![
        (200, json!({"device_code":"device", "verification_uri_complete":"https://issuer.example/activate?code=1", "interval":0, "expires_in":60})),
        (400, json!({"error":"authorization_pending"})),
        (200, json!({"access_token":"device-token", "refresh_token":"device-refresh"})),
        (200, json!({"sub":"device-subject"})),
        (200, json!({"device_code":"other-device", "verification_uri_complete":"https://issuer.example/activate", "interval":10, "expires_in":60})),
    ], false).await;
    let device = auth.start_device_auth("ignored-login").await.unwrap();
    assert!(device.url().as_str().contains("code=1"));
    let principal = device.await_principal(&CancellationToken::new()).await.unwrap();
    assert_eq!(principal.user_id(), Some("device-subject"));
    let device = auth.start_device_auth("ignored-login").await.unwrap();
    let cancellation = CancellationToken::new(); cancellation.cancel();
    assert_eq!(device.await_principal(&cancellation).await.err().unwrap().code, "xtdb.authn/cancelled");
}

#[tokio::test]
async fn principals_and_device_requests_keep_response_payloads_charged_until_their_last_owner_drops() {
    let subject = "s".repeat(4096);
    let device_code = "d".repeat(4096);
    let (auth, script, _) = setup(vec![
        (200, json!({"access_token":"access", "refresh_token":"refresh"})),
        (200, json!({"sub":subject})),
        (200, json!({"device_code":device_code, "verification_uri_complete":"https://issuer.example/activate", "interval":1, "expires_in":60})),
    ], false).await;
    assert_eq!(script.memory.used_bytes(), 0);
    let principal = auth.verify_password("login", "secret").await.unwrap();
    assert!(script.memory.used_bytes() >= 4096);
    let clone = Arc::clone(&principal);
    drop(principal);
    assert!(script.memory.used_bytes() >= 4096);
    drop(clone);
    assert_eq!(script.memory.used_bytes(), 0);
    let device = auth.start_device_auth("login").await.unwrap();
    assert!(script.memory.used_bytes() >= 4096);
    drop(device);
    assert_eq!(script.memory.used_bytes(), 0);
}

#[tokio::test]
async fn assignment_recovery_uses_a_distinct_bounded_authority() {
    use rtdb_tx::transact::workload::{Authority, Family, Point};

    let incarnation = [7; 16];
    let original_namespace = [8; 32];
    let mut claim = json!({
        "version":2, "audience":"cell", "database":"catalog", "incarnation":incarnation,
        "grant_id":([9; 16]), "expires_at_micros":800_000,
        "authority":{"role":"receipt-recovery", "original_namespace":original_namespace, "family":"write"}
    });
    let point = Point::new("public.bj_installation", "authority".into(), Timestamp(0)).unwrap();
    let config = WorkloadGrantConfig::new("cell".into(), "workload-".into()).unwrap();
    let configured = config.clone().with_assignment(point.clone());

    let (auth, _, _) = setup_with_workload(vec![
        (200, json!({"access_token":"scoped", "expires_in":1})),
        (200, json!({"sub":"recovery", "rtdb_workload_grant":claim})),
    ], false, Some(configured.clone())).await;
    let principal = auth.verify_client_credentials("workload-recovery", "secret").await.unwrap();
    let grant = principal.workload_grant().unwrap();
    assert_eq!(grant.assignment("catalog", incarnation, Timestamp(0)).unwrap(),
        (&Authority::ReceiptRecovery { original_namespace, family: Family::Write }, &point, Timestamp(800_000)));
    assert!(grant.assignment("catalog", [6; 16], Timestamp(0)).is_err());
    assert!(grant.admit(WorkloadOperation::CreateDeployment, "catalog", incarnation, Timestamp(0)).is_err());
    assert!(principal.authorize_database("catalog", rtdb_node::DatabaseAccess::Write).is_err());
    assert!(principal.revalidate(Timestamp(800_000)).await.is_err());

    let unauthorized = claim.clone();
    claim["authority"]["relation"] = json!("public.caller_selected");
    for (claim, config) in [(unauthorized, config), (claim, configured)] {
        let (auth, _, _) = setup_with_workload(vec![
            (200, json!({"access_token":"scoped", "expires_in":1})),
            (200, json!({"sub":"recovery", "rtdb_workload_grant":claim})),
        ], false, Some(config)).await;
        assert_eq!(auth.verify_client_credentials("workload-recovery", "secret").await.err().unwrap().code,
            "rtdb.auth/workload-grant");
    }
}
