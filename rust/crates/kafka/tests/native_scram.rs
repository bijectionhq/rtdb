use std::collections::BTreeMap;
use rdkafka::ClientConfig;
use rtdb_kafka::native_properties;

#[test]
fn native_scram_configuration_retains_token_authentication_across_clones() {
    assert_eq!(ClientConfig::new().create_native_config().unwrap().get("sasl.scram.tokenauth").unwrap(), "false");
    for enabled in [false, true] {
        let properties = BTreeMap::from([("sasl.jaas.config".into(), format!("org.apache.kafka.common.security.scram.ScramLoginModule required username=user password=secret tokenauth={enabled};"))]);
        let properties = native_properties(&properties).unwrap();
        let mut configuration = ClientConfig::new();
        for (key, value) in properties { configuration.set(key, value); }
        let native = configuration.create_native_config().unwrap();
        let copied = native.clone();
        drop(native); drop(configuration);
        assert_eq!(copied.get("sasl.scram.tokenauth").unwrap(), enabled.to_string());
    }
}

#[test]
fn native_scram_message_retains_the_delegation_extension_in_its_authentication_transcript() {
    assert_eq!(unsafe { unittest_scram() }, 0);
}

unsafe extern "C" { fn unittest_scram() -> std::ffi::c_int; }
