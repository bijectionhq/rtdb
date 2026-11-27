use std::{collections::BTreeMap, time::Duration};
use rtdb_types::Result;
use crate::config::{signed_integer, trim_java};
use super::{error, overflow};

pub(super) struct Settings {
    pub properties: BTreeMap<String, String>,
    pub maximum_block: Duration,
    pub delivery_timeout: Duration,
    pub maximum_record_bytes: usize,
    pub auto_flush: bool,
    buffer_bytes: usize,
    key_serializer: Serializer,
    value_serializer: Serializer,
}
enum Serializer { Bytes, NullOnly }
impl Serializer {
    fn parse(name: Option<&String>) -> Result<Self> {
        Ok(match name.map(|value| trim_java(value)).unwrap_or("org.apache.kafka.common.serialization.ByteArraySerializer") {
            "org.apache.kafka.common.serialization.ByteArraySerializer" => Self::Bytes,
            "org.apache.kafka.common.serialization.StringSerializer" | "org.apache.kafka.common.serialization.BooleanSerializer" |
            "org.apache.kafka.common.serialization.ShortSerializer" | "org.apache.kafka.common.serialization.IntegerSerializer" |
            "org.apache.kafka.common.serialization.LongSerializer" | "org.apache.kafka.common.serialization.FloatSerializer" |
            "org.apache.kafka.common.serialization.DoubleSerializer" | "org.apache.kafka.common.serialization.UUIDSerializer" |
            "org.apache.kafka.common.serialization.BytesSerializer" | "org.apache.kafka.common.serialization.ByteBufferSerializer" |
            "org.apache.kafka.common.serialization.VoidSerializer" => Self::NullOnly,
            _ => return Err(error("dlq", "Producer serializer class requires a native byte adapter")),
        })
    }
    fn accepts(&self, value: Option<&[u8]>) -> bool { matches!(self, Self::Bytes) || value.is_none() }
}

impl Settings {
    pub fn parse(options: &BTreeMap<String, String>) -> Result<Self> {
        let number = |name: &str, default: u64, maximum: u64| -> Result<u64> {
            let value = options.get(name).map(|value| {
                signed_integer(value).and_then(|value| u64::try_from(value).ok())
                    .ok_or_else(|| error("dlq", format!("Producer {name} must be nonnegative")))
            }).transpose()?.unwrap_or(default);
            if value > maximum { return Err(error("dlq", format!("Producer {name} exceeds its range"))); }
            Ok(value)
        };
        for name in ["interceptor.classes", "partitioner.class", "sasl.login.class", "sasl.login.callback.handler.class", "sasl.client.callback.handler.class"] {
            if options.get(name).is_some_and(|value| !value.trim().is_empty()) { return Err(error("dlq", format!("Producer {name} requires a native adapter"))); }
        }
        if options.get("ssl.engine.factory.class").is_some_and(|value| trim_java(value) != "org.apache.kafka.common.security.ssl.DefaultSslEngineFactory") {
            return Err(error("dlq", "Producer ssl.engine.factory.class requires a native adapter"));
        }
        let key_serializer = Serializer::parse(options.get("key.serializer"))?;
        let value_serializer = Serializer::parse(options.get("value.serializer"))?;
        let buffer_bytes = usize::try_from(number("buffer.memory", 32 << 20, i64::MAX as u64)?).map_err(|_| overflow())?;
        let maximum_request = usize::try_from(number("max.request.size", 1 << 20, i32::MAX as u64)?).map_err(|_| overflow())?;
        let maximum_block = Duration::from_millis(number("max.block.ms", i64::MAX as u64, i64::MAX as u64)?);
        let delivery_timeout = number("delivery.timeout.ms", i32::MAX as u64, i32::MAX as u64)?;
        let request_timeout = number("request.timeout.ms", 30_000, i32::MAX as u64)?;
        let linger = number("linger.ms", 5, i64::MAX as u64)?;
        if delivery_timeout < request_timeout.saturating_add(linger) { return Err(error("dlq", "delivery.timeout.ms must cover linger.ms and request.timeout.ms")); }
        if options.get("bootstrap.servers").is_none_or(|servers| servers.trim().is_empty()) { return Err(error("dlq", "No bootstrap.servers configured for the dead-letter producer")); }
        let mut properties = BTreeMap::from([
            ("enable.idempotence".into(), "false".into()), ("acks".into(), "all".into()),
            ("max.in.flight.requests.per.connection".into(), "1".into()), ("retries".into(), i32::MAX.to_string()),
            ("delivery.timeout.ms".into(), delivery_timeout.to_string()), ("request.timeout.ms".into(), request_timeout.to_string()),
            ("linger.ms".into(), linger.to_string()), ("batch.size".into(), "16384".into()),
            ("partitioner".into(), "murmur2_random".into()), ("metadata.max.age.ms".into(), "300000".into()),
            ("socket.send.buffer.bytes".into(), "131072".into()), ("socket.receive.buffer.bytes".into(), "32768".into()),
            ("reconnect.backoff.ms".into(), "50".into()), ("reconnect.backoff.max.ms".into(), "1000".into()),
            ("connections.max.idle.ms".into(), "540000".into()), ("retry.backoff.ms".into(), "100".into()),
            ("queue.buffering.max.messages".into(), "0".into()),
            ("queue.buffering.max.kbytes".into(), buffer_bytes.div_ceil(1024).max(1).to_string()),
            ("message.max.bytes".into(), maximum_request.max(1000).to_string()),
            ("receive.message.max.bytes".into(), "1048576".into()),
        ]);
        for (name, value) in options {
            let native = match name.as_str() {
                "bootstrap.servers" | "client.id" | "client.rack" | "client.dns.lookup" | "acks" | "enable.idempotence" |
                "max.in.flight.requests.per.connection" | "retries" | "compression.type" | "metadata.max.age.ms" |
                "reconnect.backoff.ms" | "reconnect.backoff.max.ms" | "retry.backoff.ms" | "retry.backoff.max.ms" |
                "connections.max.idle.ms" | "security.protocol" | "sasl.mechanism" | "sasl.jaas.config" |
                "sasl.username" | "sasl.password" | "sasl.scram.tokenauth" | "sasl.kerberos.service.name" | "sasl.kerberos.principal" |
                "transactional.id" | "transaction.timeout.ms" | "ssl.cipher.suites" | "ssl.key.password" |
                "ssl.ca.location" | "ssl.ca.pem" | "ssl.key.location" | "ssl.key.pem" |
                "ssl.certificate.location" | "ssl.certificate.pem" => name.as_str(),
                "send.buffer.bytes" | "receive.buffer.bytes" => {
                    let size = signed_integer(value).and_then(|value| i32::try_from(value).ok())
                        .filter(|size| *size >= -1).ok_or_else(|| error("dlq", format!("Producer {name} must be -1 or a nonnegative integer")))?;
                    let native = if name == "send.buffer.bytes" { "socket.send.buffer.bytes" } else { "socket.receive.buffer.bytes" };
                    properties.insert(native.into(), size.max(0).to_string());
                    continue;
                }
                "socket.connection.setup.timeout.ms" => "socket.connection.setup.timeout.ms",
                "batch.size" => { properties.insert(name.clone(), number("batch.size", 16384, i32::MAX as u64)?.max(1).to_string()); continue; }
                "ssl.endpoint.identification.algorithm" => { properties.insert(name.clone(), if value.is_empty() { "none".into() } else { value.to_ascii_lowercase() }); continue; }
                "partitioner.ignore.keys" => {
                    match value.trim().to_ascii_lowercase().as_str() {
                        "true" => { properties.insert("partitioner".into(), "random".into()); }, "false" => (),
                        _ => return Err(error("dlq", "partitioner.ignore.keys must be a boolean")),
                    }
                    continue;
                }
                _ => continue,
            };
            properties.insert(native.into(), value.clone());
        }
        let properties = crate::native_properties(&properties)?;
        Ok(Self {
            properties,
            maximum_block,
            delivery_timeout: Duration::from_millis(delivery_timeout),
            maximum_record_bytes: buffer_bytes.min(maximum_request),
            auto_flush: options.get("dlq.auto.flush").is_some_and(|value| value.eq_ignore_ascii_case("true")),
            buffer_bytes,
            key_serializer,
            value_serializer,
        })
    }
    pub fn validate_bytes(&self, key: Option<&[u8]>, value: Option<&[u8]>) -> Result<()> {
        if !self.key_serializer.accepts(key) || !self.value_serializer.accepts(value) { return Err(error("dlq", "Configured producer serializer cannot accept the original byte array")); }
        Ok(())
    }
    pub fn native_byte_capacity(&self) -> Result<usize> { self.buffer_bytes.checked_mul(3).and_then(|bytes| bytes.checked_add(8 << 20)).ok_or_else(overflow) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delegation_token_options_reach_the_native_dead_letter_producer() {
        let mut options = BTreeMap::from([
            ("bootstrap.servers".into(), "localhost:9092".into()),
            ("sasl.mechanism".into(), "SCRAM-SHA-256".into()),
            ("sasl.jaas.config".into(), "org.apache.kafka.common.security.scram.ScramLoginModule required username=token password=secret tokenauth=true debug=true;".into()),
        ]);
        assert_eq!(Settings::parse(&options).unwrap().properties["sasl.scram.tokenauth"], "true");
        options.insert("sasl.scram.tokenauth".into(), "false".into());
        assert!(Settings::parse(&options).is_err());
        options.remove("sasl.jaas.config");
        assert_eq!(Settings::parse(&options).unwrap().properties["sasl.scram.tokenauth"], "false");
    }

    #[test]
    fn dlq_defaults_and_java_producer_aliases_are_applied_without_registry_properties() {
        let settings = Settings::parse(&BTreeMap::from([
            ("bootstrap.servers".into(), "localhost:9092".into()),
            ("schema.registry.url".into(), "http://localhost:8081".into()),
            ("send.buffer.bytes".into(), "65536".into()),
            ("dlq.auto.flush".into(), "TrUe".into()),
        ])).unwrap();
        assert_eq!(settings.maximum_block, Duration::from_millis(i64::MAX as u64));
        assert_eq!(settings.maximum_record_bytes, 1 << 20);
        assert_eq!(settings.properties["enable.idempotence"], "false");
        assert_eq!(settings.properties["acks"], "all");
        assert_eq!(settings.properties["max.in.flight.requests.per.connection"], "1");
        assert_eq!(settings.properties["delivery.timeout.ms"], i32::MAX.to_string());
        assert_eq!(settings.properties["partitioner"], "murmur2_random");
        assert_eq!(settings.properties["socket.send.buffer.bytes"], "65536");
        assert!(!settings.properties.contains_key("schema.registry.url"));
        assert!(settings.auto_flush);
    }
    #[test]
    fn producer_admission_checks_numeric_ranges_and_delivery_deadline() {
        let mut options = BTreeMap::from([("bootstrap.servers".into(), "localhost:9092".into())]);
        options.insert("max.block.ms".into(), "-1".into()); assert!(Settings::parse(&options).is_err());
        options.remove("max.block.ms"); options.insert("delivery.timeout.ms".into(), "30000".into()); assert!(Settings::parse(&options).is_err());
        options.insert("linger.ms".into(), "0".into()); assert!(Settings::parse(&options).is_ok());
        options.insert("buffer.memory".into(), "0".into()); assert_eq!(Settings::parse(&options).unwrap().maximum_record_bytes, 0);
    }
    #[test]
    fn java_socket_buffer_sentinels_select_native_system_defaults() {
        let mut options = BTreeMap::from([("bootstrap.servers".into(), "localhost:9092".into())]);
        for (java, native) in [("send.buffer.bytes", "socket.send.buffer.bytes"), ("receive.buffer.bytes", "socket.receive.buffer.bytes")] {
            options.insert(java.into(), "-1".into());
            assert_eq!(Settings::parse(&options).unwrap().properties[native], "0");
            options.insert(java.into(), "0".into());
            assert_eq!(Settings::parse(&options).unwrap().properties[native], "0");
            options.insert(java.into(), "-2".into()); assert!(Settings::parse(&options).is_err());
            options.insert(java.into(), "2147483648".into()); assert!(Settings::parse(&options).is_err());
            options.remove(java);
        }
    }

    #[test]
    fn java_integer_options_preserve_bmp_digits_signed_zero_and_configdef_trimming() {
        let mut options = BTreeMap::from([
            ("bootstrap.servers".into(), "localhost:9092".into()),
            ("max.block.ms".into(), "\u{0}+\u{661}\u{662}\u{1f}".into()),
            ("max.request.size".into(), "\t\u{ff11}\u{ff10}\u{ff10}\u{ff10} ".into()),
            ("send.buffer.bytes".into(), "-\u{661}".into()),
            ("receive.buffer.bytes".into(), "\u{ff11}\u{ff12}".into()),
            ("batch.size".into(), "-0".into()),
            ("key.serializer".into(), "\torg.apache.kafka.common.serialization.ByteArraySerializer\u{1f}".into()),
        ]);
        let settings = Settings::parse(&options).unwrap();
        assert_eq!(settings.maximum_block, Duration::from_millis(12));
        assert_eq!(settings.maximum_record_bytes, 1000);
        assert_eq!(settings.properties["socket.send.buffer.bytes"], "0");
        assert_eq!(settings.properties["socket.receive.buffer.bytes"], "12");
        assert_eq!(settings.properties["batch.size"], "1");
        assert!(settings.validate_bytes(Some(b"key"), Some(b"value")).is_ok());

        for invalid in ["\u{a0}12", "\u{1d7d9}2", "9223372036854775808", "-1"] {
            options.insert("max.block.ms".into(), invalid.into());
            assert!(Settings::parse(&options).is_err());
        }
    }

    #[test]
    fn explicit_default_tls_factory_uses_the_native_java_configuration_adapter() {
        let mut options = BTreeMap::from([
            ("bootstrap.servers".into(), "localhost:9092".into()),
            ("ssl.engine.factory.class".into(), " org.apache.kafka.common.security.ssl.DefaultSslEngineFactory ".into()),
        ]);
        let settings = Settings::parse(&options).unwrap();
        assert!(!settings.properties.contains_key("ssl.engine.factory.class"));
        for factory in ["", "custom.Factory", "org.apache.kafka.common.security.ssl.CommonNameLoggingSslEngineFactory", "\u{a0}org.apache.kafka.common.security.ssl.DefaultSslEngineFactory"] {
            options.insert("ssl.engine.factory.class".into(), factory.into());
            assert!(Settings::parse(&options).is_err());
        }
    }

    #[test]
    fn native_byte_capacity_is_a_checked_ceiling_independent_of_the_record_size_limit() {
        let mut options = BTreeMap::from([("bootstrap.servers".into(), "localhost:1".into())]);
        options.insert("buffer.memory".into(), "0".into());
        let settings = Settings::parse(&options).unwrap();
        assert_eq!(settings.native_byte_capacity().unwrap(), 8 << 20);
        assert_eq!(settings.maximum_record_bytes, 0);
        options.insert("buffer.memory".into(), (1 << 20).to_string());
        let settings = Settings::parse(&options).unwrap();
        assert_eq!(settings.native_byte_capacity().unwrap(), 11 << 20);
        assert_eq!(settings.maximum_record_bytes, 1 << 20);
        options.insert("buffer.memory".into(), i64::MAX.to_string());
        let settings = Settings::parse(&options);
        if usize::BITS >= 64 { assert!(settings.unwrap().native_byte_capacity().is_err()); }
        else { assert!(settings.is_err()); }
    }
}

#[cfg(test)]
mod serializer_tests {
    use super::*;
    #[test]
    fn explicit_action_serializers_preserve_null_and_reject_wrong_runtime_byte_array_type() {
        let options = BTreeMap::from([
            ("bootstrap.servers".into(), "localhost:9092".into()),
            ("key.serializer".into(), "org.apache.kafka.common.serialization.StringSerializer".into()),
        ]);
        let settings = Settings::parse(&options).unwrap();
        assert!(settings.validate_bytes(None, Some(b"value")).is_ok());
        assert!(settings.validate_bytes(Some(b"key"), Some(b"value")).is_err());
    }
}
