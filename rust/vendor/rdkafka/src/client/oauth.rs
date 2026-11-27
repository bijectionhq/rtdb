use std::{
    ffi::{c_char, c_void, CString},
    fmt::Debug,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use rdkafka_sys as sys;
use rdkafka_sys::types::{RDKafka, RDKafkaEvent, RDKafkaRespErr};

use crate::{
    config::NativeClientConfig,
    error::{KafkaError, KafkaResult},
    util::ErrBuf,
};

/// A credential and its refresh deadline, expressed in Unix milliseconds.
pub struct ManagedOAuthToken {
    /// Serialized bearer token.
    pub token: String,
    /// The authenticated Kafka principal.
    pub principal: String,
    /// Token expiration, in Unix milliseconds.
    pub expires_at_ms: i64,
    /// Next refresh, in Unix milliseconds.
    pub refresh_at_ms: i64,
}

/// Supplies the initial credential and subsequent credentials for a native client.
/// Refresh runs on librdkafka's SASL background queue and must finish in bounded time.
/// Errors and the debug representation should be safe to include in logs.
pub trait OAuthTokenProvider: Debug + Send + Sync {
    /// Admits the storage held by one native client.
    /// The client retains this lease through native destruction.
    fn admit_client(&self) -> Result<Box<dyn Send + Sync>, String>;

    /// Fetches a credential or returns a diagnostic safe to expose to the caller.
    fn token(&self) -> Result<ManagedOAuthToken, String>;

    /// SASL extension keys and values, in alternating order, for this client.
    fn extensions(&self) -> &[CString];
}

pub(super) struct OAuthSession {
    provider: Arc<dyn OAuthTokenProvider>,
    initial_event: AtomicBool,
    _lease: Box<dyn Send + Sync>,
}

impl OAuthSession {
    pub(super) fn new(provider: Arc<dyn OAuthTokenProvider>) -> KafkaResult<Self> {
        let lease = provider
            .admit_client()
            .map_err(KafkaError::ClientCreation)?;
        Ok(Self {
            provider,
            initial_event: AtomicBool::new(true),
            _lease: lease,
        })
    }

    pub(super) fn configure(&self, config: &NativeClientConfig) {
        unsafe {
            sys::rd_kafka_conf_set_opaque(config.ptr(), (self as *const Self).cast_mut().cast());
            sys::rd_kafka_conf_enable_sasl_queue(config.ptr(), 1);
            sys::rd_kafka_conf_set_oauthbearer_token_refresh_cb(
                config.ptr(),
                Some(refresh_callback),
            );
            sys::rd_kafka_conf_set_background_event_cb(config.ptr(), Some(background_event));
        }
    }

    pub(super) fn initial_token(&self) -> KafkaResult<ManagedOAuthToken> {
        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .name("kafka-oauth-login".into())
                .spawn_scoped(scope, || self.provider.token())
                .map_err(|_| {
                    KafkaError::ClientCreation("Could not start Kafka OAuth login".into())
                })?
                .join()
                .map_err(|_| KafkaError::ClientCreation("Kafka OAuth login panicked".into()))?
                .map_err(KafkaError::ClientCreation)
        })
    }

    pub(super) fn start(&self, client: *mut RDKafka, token: ManagedOAuthToken) -> KafkaResult<()> {
        self.install(client, token)
            .map_err(KafkaError::ClientCreation)?;
        let error = unsafe { sys::rd_kafka_sasl_background_callbacks_enable(client) };
        if error.is_null() {
            Ok(())
        } else {
            unsafe { sys::rd_kafka_error_destroy(error) };
            Err(KafkaError::ClientCreation(
                "Could not enable the Kafka OAuth refresh queue".into(),
            ))
        }
    }

    fn install(&self, client: *mut RDKafka, token: ManagedOAuthToken) -> Result<(), String> {
        let value = CString::new(token.token).map_err(|_| "Invalid Kafka OAuth token")?;
        let principal =
            CString::new(token.principal).map_err(|_| "Invalid Kafka OAuth principal")?;
        let mut extensions: Vec<_> = self
            .provider
            .extensions()
            .iter()
            .map(|value| value.as_ptr())
            .collect();
        let mut error = ErrBuf::new();
        let code = unsafe {
            sys::rd_kafka_oauthbearer_set_token_with_refresh(
                client,
                value.as_ptr(),
                token.expires_at_ms,
                token.refresh_at_ms,
                principal.as_ptr(),
                extensions.as_mut_ptr(),
                extensions.len(),
                error.as_mut_ptr(),
                error.capacity(),
            )
        };
        if code == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
            Ok(())
        } else {
            Err("librdkafka rejected the Kafka OAuth credential".into())
        }
    }
}

unsafe extern "C" fn refresh_callback(
    client: *mut RDKafka,
    _config: *const c_char,
    opaque: *mut c_void,
) {
    let session = unsafe { &*opaque.cast::<OAuthSession>() };
    // librdkafka queues an initial refresh before the constructor installs the first token.
    if session.initial_event.swap(false, Ordering::Relaxed) {
        return;
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        session
            .provider
            .token()
            .and_then(|token| session.install(client, token))
    }));
    if let Err(message) = result.unwrap_or_else(|_| Err("Kafka OAuth refresh panicked".into())) {
        let message = CString::new(message).unwrap_or_else(|_| {
            CString::new("Kafka OAuth refresh failed").expect("static error has no nulls")
        });
        unsafe { sys::rd_kafka_oauthbearer_set_managed_token_failure(client, message.as_ptr()) };
    }
}

unsafe extern "C" fn background_event(
    client: *mut RDKafka,
    event: *mut RDKafkaEvent,
    opaque: *mut c_void,
) {
    unsafe {
        if sys::rd_kafka_event_type(event) == sys::RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH {
            refresh_callback(client, std::ptr::null(), opaque);
        }
        sys::rd_kafka_event_destroy(event);
    }
}
