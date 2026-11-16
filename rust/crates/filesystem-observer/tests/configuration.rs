use std::collections::BTreeMap;
use rtdb_filesystem_observer::validate_configuration;

fn config(storage:&str,log:&str)->String {format!("nodeId: measured\nlog: !Local {{path: '{log}'}}\nstorage: !Local {{path: '{storage}'}}\nserver: null\nflightSql: null\n")}

#[test]
fn only_decoded_local_object_roots_are_measured() {
    let proof=validate_configuration(&config("/measured-objects","/runtime/log"),BTreeMap::new()).unwrap();
    assert_eq!(proof["databases"][0]["storage_path"],"/measured-objects");
    assert_eq!(proof["databases"][0]["log_path"],"/runtime/log");
    assert_eq!(proof["attached_databases_allowed"],false);
    assert_eq!(proof["runtime_configuration_changes_allowed"],false);
    for (storage,log) in [("/measured-objects-neighbor","/runtime/log"),("/measured-objects/../escape","/runtime/log"),("/measured-objects","/measured-objects/log"),("/measured-objects","/")] {
        assert!(validate_configuration(&config(storage,log),BTreeMap::new()).is_err());
    }
    assert!(validate_configuration("",BTreeMap::new()).is_err());
}

#[test]
fn explicit_container_environment_is_resolved_without_host_fallback() {
    let text="nodeId: measured\nlog: !Local {path: /runtime/log}\nstorage: !Local {path: !Env STORE}\nserver: null\nflightSql: null\n";
    assert!(validate_configuration(text,BTreeMap::new()).is_err());
    let proof=validate_configuration(text,BTreeMap::from([("STORE".into(),"/measured-objects/db".into())])).unwrap();
    assert_eq!(proof["databases"][0]["storage_path"],"/measured-objects/db");
    assert!(validate_configuration(&format!("{}diskCache: {{path: /measured-objects/cache}}\n",config("/measured-objects","/runtime/log")),BTreeMap::new()).is_err());
}
