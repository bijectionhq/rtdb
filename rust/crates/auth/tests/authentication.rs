use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_auth::*;
use rtdb_columnar::MemoryBudget;
use rtdb_node::{SessionIdentity, SystemClock};
use rtdb_tx::TransactionAuthorization;
use rtdb_types::{Anomaly, Result, Timestamp};

struct NoHttp;
#[async_trait]
impl HttpTransport for NoHttp {
    async fn request(&self, _: HttpRequest) -> Result<HttpResponse> { Err(Anomaly::fault("test/http", "Unexpected HTTP request")) }
}
async fn open(config: AuthConfig) -> Arc<Authenticator> {
    Authenticator::open(config, MemoryBudget::new(128 << 20), Arc::new(SystemClock), Arc::new(NoHttp)).await.unwrap()
}

#[test]
fn routing_preserves_unknown_address_rejection_before_a_looser_rule() {
    let rules = vec![MethodRule { method: Method::Password, user: Some("alice".into()), remote_address: Some("127.0.0.1".into()) }, MethodRule::any(Method::Trust)];
    assert_eq!(method_for(&rules, Some("alice"), None), None);
    assert_eq!(method_for(&rules, Some("bob"), None), Some(Method::Trust));
    assert_eq!(method_for(&rules, Some("alice"), Some("127.0.0.1")), Some(Method::Password));
    assert_eq!(method_for(&rules, Some("alice"), Some("192.0.2.1")), Some(Method::Trust));
    assert_eq!(method_for(&[], None, None), None);
}

#[tokio::test]
async fn root_password_changes_method_without_granting_another_user_root() {
    let auth = open(AuthConfig::SingleRootUser { password: Some(Secret::new("root-password")) }).await;
    assert_eq!(auth.method_for(Some("anyone"), None), Some(Method::Password));
    assert!(auth.verify_password("alice", "root-password").await.is_err());
    assert!(auth.verify_password("xtdb", "wrong").await.is_err());
    let principal = auth.verify_password("xtdb", "root-password").await.unwrap();
    assert_eq!(principal.user_id(), Some("xtdb"));
    principal.revalidate(Timestamp(i64::MAX)).await.unwrap();
    auth.check_role_membership("xtdb", principal.user()).unwrap();
    assert_eq!(auth.check_role_membership("other", principal.user()).unwrap_err().code, "xtdb/role-membership-non-primary");
    assert_eq!(auth.check_role_membership("xtdb", None).unwrap_err().code, "xtdb/not-authorized");
    assert_eq!(auth.check_role_membership("xtdb", Some("alice")).unwrap_err().code, "xtdb/not-authorized");
    let trust = open(AuthConfig::SingleRootUser { password: None }).await;
    assert_eq!(trust.method_for(None, None), Some(Method::Trust));
    assert!(trust.verify_password("xtdb", "").await.is_err());
    assert_eq!(trust.unauthenticated().unwrap().user_id(), None);
}

#[test]
fn stored_hashes_round_trip_reject_malformed_and_keep_the_algorithm_tag() {
    let memory = MemoryBudget::new(128 << 20);
    for hash in [PasswordHash::argon2id("hunter2", &memory).unwrap(), PasswordHash::bcrypt("hunter2").unwrap()] {
        assert!(hash.verify("hunter2", &memory).unwrap());
        assert!(!hash.verify("wrong", &memory).unwrap());
        assert!(!format!("{hash:?}").contains(hash.encoded()));
    }
    assert!(!PasswordHash::Argon2id("not-a-real-hash".into()).verify("hunter2", &memory).unwrap());
    assert!(!PasswordHash::BCrypt("not-a-real-hash".into()).verify("hunter2", &memory).unwrap());
    let bcrypt = PasswordHash::bcrypt("hunter2").unwrap();
    assert!(!PasswordHash::Argon2id(bcrypt.encoded().into()).verify("hunter2", &memory).unwrap());
    let first = PasswordHash::argon2id("hunter2", &memory).unwrap();
    let second = PasswordHash::argon2id("hunter2", &memory).unwrap();
    assert_ne!(first.encoded(), second.encoded());
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn configured_users_keep_trusted_embedded_and_unsupported_oauth_contracts() {
    let auth = open(AuthConfig::UserList { users: BTreeMap::from([("alice".into(), PasswordHash::bcrypt("secret").unwrap())]), rules: vec![MethodRule::any(Method::Password)] }).await;
    assert_eq!(auth.known_users(), vec!["alice"]);
    assert_eq!(auth.verify_password("alice", "secret").await.unwrap().user_id(), Some("alice"));
    let wrong = auth.verify_password("alice", "wrong").await.err().unwrap();
    let absent = auth.verify_password("unknown", "wrong").await.err().unwrap();
    assert_eq!(wrong.category, absent.category);
    assert_eq!(wrong.code, absent.code);
    assert_eq!(auth.authenticate_trusted("other").unwrap().user_id(), Some("other"));
    assert_eq!(auth.verify_client_credentials("id", "secret").await.err().unwrap().code, "verifyClientCredentials");
    assert_eq!(auth.start_device_auth("alice").await.err().unwrap().code, "startDeviceAuth");
}
