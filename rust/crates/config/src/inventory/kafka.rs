use std::collections::BTreeMap;

use rtdb_auth::Secret;

use crate::{ExplicitEnvironment, QualificationReport};

const TOKEN_ENDPOINT: &str = "sasl.oauthbearer.token.endpoint.url";
const ALLOWED_URLS: &str = "RTDB_KAFKA_OAUTHBEARER_ALLOWED_URLS";

pub(crate) fn broker_properties(
    properties: &BTreeMap<String, Secret>,
    environment: &ExplicitEnvironment,
    path: &str,
    report: &mut QualificationReport,
) {
    let endpoint = properties.get(TOKEN_ENDPOINT);
    for (name, value) in properties {
        let supported = BROKER_PROPERTIES.contains(&name.as_str())
            || (endpoint.is_some() && OIDC_PROPERTIES.contains(&name.as_str()))
            || (name == "sasl.oauthbearer.method" && value.expose() == "default")
            || (name == "ssl.engine.factory.class"
                && java_trim(value.expose())
                    == "org.apache.kafka.common.security.ssl.DefaultSslEngineFactory");
        if !supported {
            let (code, message) = if name.ends_with(".class")
                || matches!(name.as_str(), "plugin.path" | "interceptor.classes")
            {
                (
                    "jvm-property-hook",
                    "This JVM class or plugin property requires a native adapter.",
                )
            } else {
                (
                    "unqualified-kafka-property",
                    "This property has no qualified offline native mapping.",
                )
            };
            report.gap(code, format!("{path}.{name}"), message);
        }
    }

    #[cfg(feature = "kafka")]
    {
        let values = properties
            .iter()
            .map(|(key, value)| (key.clone(), value.expose().to_owned()))
            .collect();
        if rtdb_kafka::native_properties(&values).is_err() {
            report.gap(
                "unsupported-kafka-auth-properties",
                path,
                "Native Kafka property translation rejected the supplied configuration.",
            );
            return;
        }
    }

    if let Some(endpoint) = endpoint {
        let endpoint = java_trim(endpoint.expose());
        let allowed = environment.get(ALLOWED_URLS).unwrap_or_default();
        if endpoint.is_empty() || !allowed.split(',').map(java_trim).any(|url| url == endpoint) {
            report.gap(
                "kafka-oauth-endpoint-not-allowed",
                format!("{path}.{TOKEN_ENDPOINT}"),
                "The Kafka OAuth endpoint must appear in the supplied RTDB_KAFKA_OAUTHBEARER_ALLOWED_URLS.",
            );
        }
    }
}

fn java_trim(value: &str) -> &str {
    value.trim_matches(|ch| ch <= '\u{20}')
}

const BROKER_PROPERTIES: &[&str] = &[
    "bootstrap.servers",
    "client.id",
    "security.protocol",
    "sasl.mechanism",
    "sasl.mechanisms",
    "sasl.username",
    "sasl.password",
    "sasl.jaas.config",
    "sasl.login.callback.handler.class",
    "sasl.scram.tokenauth",
    "sasl.kerberos.service.name",
    "sasl.kerberos.principal",
    "sasl.kerberos.keytab",
    "sasl.kerberos.kinit.cmd",
    "sasl.kerberos.min.time.before.relogin",
    "enable.sasl.oauthbearer.unsecure.jwt",
    "sasl.oauthbearer.config",
    "ssl.ca.location",
    "ssl.ca.pem",
    "ssl.certificate.location",
    "ssl.certificate.pem",
    "ssl.key.location",
    "ssl.key.pem",
    "ssl.key.password",
    "ssl.endpoint.identification.algorithm",
    "ssl.truststore.type",
    "ssl.truststore.location",
    "ssl.truststore.password",
    "ssl.truststore.certificates",
    "ssl.keystore.type",
    "ssl.keystore.location",
    "ssl.keystore.password",
    "ssl.keystore.key",
    "ssl.keystore.certificate.chain",
    "enable.idempotence",
    "acks",
    "compression.type",
    "linger.ms",
    "message.max.bytes",
    "request.timeout.ms",
    "delivery.timeout.ms",
    "socket.timeout.ms",
    "max.in.flight.requests.per.connection",
];

const OIDC_PROPERTIES: &[&str] = &[
    TOKEN_ENDPOINT,
    "sasl.oauthbearer.method",
    "sasl.oauthbearer.client.credentials.client.id",
    "sasl.oauthbearer.client.credentials.client.secret",
    "sasl.oauthbearer.client.id",
    "sasl.oauthbearer.client.secret",
    "sasl.oauthbearer.scope",
    "sasl.oauthbearer.header.urlencode",
    "sasl.oauthbearer.extensions",
    "sasl.oauthbearer.jwt.retriever.class",
    "sasl.oauthbearer.jwt.validator.class",
    "sasl.oauthbearer.sub.claim.name",
    "sasl.oauthbearer.scope.claim.name",
    "sasl.login.class",
    "sasl.login.connect.timeout.ms",
    "sasl.login.read.timeout.ms",
    "sasl.login.retry.backoff.ms",
    "sasl.login.retry.backoff.max.ms",
    "sasl.login.refresh.buffer.seconds",
    "sasl.login.refresh.min.period.seconds",
    "sasl.login.refresh.window.factor",
    "sasl.login.refresh.window.jitter",
];

#[cfg(feature = "kafka-connect")]
pub(crate) mod connect {
    use rtdb_kafka::connect::{PluginRegistry, PluginSpec, SourceConfig};

    use crate::QualificationReport;

    pub(crate) fn inspect(source: &prost_types::Any, path: &str, report: &mut QualificationReport) {
        let Ok(config) = SourceConfig::decode(source).and_then(|source| source.validate()) else {
            report.gap(
                "invalid-connect-description",
                path,
                "The production Kafka Connect parser rejected the supplied configuration.",
            );
            return;
        };

        let plugins = PluginRegistry::default();
        for (role, spec, is_key) in [
            ("key.converter", &config.key, true),
            ("value.converter", &config.value, false),
        ] {
            let path = format!("{path}.{role}");
            // Registry transport setup and token requests are deferred until record decode.
            match plugins.converter(spec, is_key) {
                Ok(_) => converter_options(spec, is_key, &path, report),
                Err(error) if error.code == "xtdb.kafka-connect-source/plugin-class-not-found" => {
                    report.gap(
                        "unsupported-connect-converter",
                        path,
                        "This converter has no native Kafka Connect implementation.",
                    )
                }
                Err(_) => report.gap(
                    "invalid-connect-converter-options",
                    path,
                    "The native converter rejected the supplied options.",
                ),
            }
        }
        if !config.transforms.is_empty() || !config.predicates.is_empty() {
            report.gap(
                "connect-plugin-options-unqualified",
                path,
                "Kafka Connect transforms and predicates require separate runtime qualification.",
            );
        }
    }

    fn converter_options(
        spec: &PluginSpec,
        is_key: bool,
        path: &str,
        report: &mut QualificationReport,
    ) {
        let role = if is_key { "key" } else { "value" };
        for (name, value) in &spec.options {
            let supported = match spec.class_name.as_str() {
                "io.confluent.connect.protobuf.ProtobufConverter" => {
                    PROTOBUF_OPTIONS.contains(&name.as_str())
                        || name
                            .strip_prefix("specific.protobuf.")
                            .and_then(|name| name.strip_suffix(".type"))
                            == Some(role)
                        || registry_option(name, value, role)
                }
                "io.confluent.connect.avro.AvroConverter"
                | "io.confluent.connect.json.JsonSchemaConverter" => {
                    registry_option(name, value, role)
                }
                "org.apache.kafka.connect.json.JsonConverter" => matches!(
                    name.as_str(),
                    "schemas.enable"
                        | "replace.null.with.default"
                        | "schema.content"
                        | "decimal.format"
                        | "schemas.cache.size"
                ),
                "org.apache.kafka.connect.storage.StringConverter" => {
                    name == "converter.encoding"
                        || name.split_once('.').is_some_and(|(side, suffix)| {
                            side == role
                                && matches!(suffix, "serializer.encoding" | "deserializer.encoding")
                        })
                }
                _ => false,
            };
            if !supported {
                report.gap(
                    "connect-plugin-options-unqualified",
                    format!("{path}.{name}"),
                    "This converter option has no qualified offline native mapping.",
                );
            }
        }
    }

    fn registry_option(name: &str, value: &str, role: &str) -> bool {
        if name.strip_suffix(".subject.name.strategy") == Some(role) {
            return matches!(
                value,
                "io.confluent.kafka.serializers.subject.TopicNameStrategy"
                    | "io.confluent.kafka.serializers.subject.RecordNameStrategy"
                    | "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"
            );
        }
        REGISTRY_OPTIONS.contains(&name)
            || REGISTRY_CLIENT_OPTIONS
                .contains(&name.strip_prefix("schema.registry.").unwrap_or(name))
            || name
                .strip_prefix("schema.registry.ssl.")
                .is_some_and(|name| REGISTRY_TLS_OPTIONS.contains(&name))
    }

    const PROTOBUF_OPTIONS: &[&str] = &[
        "derive.type",
        "scrub.invalid.names",
        "ignore.default.for.nullables",
        "schemas.cache.config",
        "enhanced.protobuf.schema.support",
        "int.for.enums",
        "optional.for.nullables",
        "optional.for.proto2",
        "wrapper.for.nullables",
        "wrapper.for.raw.primitives",
        "generate.struct.for.nulls",
        "generate.index.for.unions",
        "flatten.unions",
        "generalized.sum.type.support",
    ];

    const REGISTRY_OPTIONS: &[&str] = &[
        "schema.registry.url",
        "use.latest.version",
        "use.latest.with.metadata",
        "latest.cache.size",
        "latest.cache.ttl.sec",
        "max.schemas.per.subject",
    ];

    const REGISTRY_CLIENT_OPTIONS: &[&str] = &[
        "basic.auth.credentials.source",
        "basic.auth.user.info",
        "bearer.auth.credentials.source",
        "bearer.auth.custom.provider.class",
        "bearer.auth.token",
        "bearer.auth.logical.cluster",
        "bearer.auth.identity.pool.id",
        "bearer.auth.client.id",
        "bearer.auth.client.secret",
        "bearer.auth.issuer.endpoint.url",
        "bearer.auth.scope",
        "bearer.auth.cache.expiry.buffer.seconds",
        "bearer.auth.scope.claim.name",
        "bearer.auth.sub.claim.name",
        "sasl.jaas.config",
        "sasl.oauthbearer.token.endpoint.url",
        "sasl.oauthbearer.scope.claim.name",
        "sasl.oauthbearer.sub.claim.name",
        "http.connect.timeout.ms",
        "http.read.timeout.ms",
        "proxy.host",
        "proxy.port",
    ];

    const REGISTRY_TLS_OPTIONS: &[&str] = &[
        "protocol",
        "provider",
        "endpoint.identification.algorithm",
        "keymanager.algorithm",
        "trustmanager.algorithm",
        "secure.random.implementation",
        "truststore.type",
        "truststore.location",
        "truststore.password",
        "truststore.certificates",
        "keystore.type",
        "keystore.location",
        "keystore.password",
        "keystore.key",
        "keystore.certificate.chain",
        "key.password",
    ];
}
