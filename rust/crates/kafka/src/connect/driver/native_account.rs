use std::sync::Arc;
use rdkafka::{ClientConfig, config::NativeClientConfig, native_byte_account::NativeByteAccount};
use rtdb_columnar::MemoryBudget;
use rtdb_types::Result;
use super::{error, overflow, transport};

const FRAMING_BYTES: usize = 1 << 20;
const CONTROL_BYTES: usize = 8 << 20;

#[derive(Debug, PartialEq, Eq)]
struct Limits { fetch: usize, receive: usize, capacity: usize }

fn integer(native: &NativeClientConfig, name: &str) -> Result<usize> {
    native.get(name).map_err(transport)?.parse::<usize>().ok().filter(|value| *value > 0)
        .ok_or_else(|| error("invalid-config", "Native consumer byte limits must be positive integers"))
}

fn native_bytes(bytes: usize) -> Result<usize> {
    i32::try_from(bytes).map(|_| bytes).map_err(|_| overflow())
}

fn capacity(receive: usize) -> Result<usize> {
    receive.checked_mul(2).and_then(|bytes| bytes.checked_add(CONTROL_BYTES)).ok_or_else(overflow)
}

impl Limits {
    fn from_config(config: &ClientConfig) -> Result<Self> {
        let mut scalars = ClientConfig::new();
        for name in ["fetch.max.bytes", "message.max.bytes", "queued.max.messages.kbytes", "receive.message.max.bytes"] {
            if let Some(value) = config.get(name) { scalars.set(name, value); }
        }
        let native = scalars.create_native_config().map_err(transport)?;
        let configured_fetch = integer(&native, "fetch.max.bytes")?;
        let message = integer(&native, "message.max.bytes")?;
        let queued = native_bytes(integer(&native, "queued.max.messages.kbytes")?.checked_mul(1024).ok_or_else(overflow)?)?;
        let fetch = if config.get("fetch.max.bytes").is_some() { configured_fetch } else { configured_fetch.min(queued).max(message) };
        if fetch < message {
            return Err(error("invalid-config", "fetch.max.bytes must be at least message.max.bytes"));
        }
        let minimum_receive = native_bytes(fetch.checked_add(512).ok_or_else(overflow)?)?;
        let receive = if config.get("receive.message.max.bytes").is_some() {
            integer(&native, "receive.message.max.bytes")?
        } else {
            native_bytes(fetch.checked_add(FRAMING_BYTES).ok_or_else(overflow)?)?
        };
        if receive < minimum_receive {
            return Err(error("invalid-config", "receive.message.max.bytes must be at least fetch.max.bytes plus 512"));
        }
        Ok(Self { fetch, receive, capacity: capacity(receive)? })
    }
}

pub(super) fn install(config: &mut ClientConfig, memory: &Arc<MemoryBudget>) -> Result<()> {
    // Plugin conf-set callbacks can consume the byte-limit settings.
    if config.get("plugin.library.paths").is_some() { return Ok(()); }
    let limits = Limits::from_config(config)?;
    let reservation = memory.reserve(limits.capacity)?;
    let account = NativeByteAccount::new(limits.capacity, reservation).map_err(transport)?;
    config.set("fetch.max.bytes", limits.fetch.to_string())
        .set("receive.message.max.bytes", limits.receive.to_string())
        .set_native_byte_account_with_consumers(account);
    Ok(())
}

#[cfg(test)]
#[path = "native_account_tests.rs"]
mod tests;
