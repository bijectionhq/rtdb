#[allow(dead_code)]
#[path = "../src/tzdb.rs"]
mod tzdb;

#[test]
fn packaged_tzdb_decodes_without_jvm_and_rejects_truncated_records() {
    let bytes = include_bytes!("../data/tzdb/2026e.dat");
    let database = tzdb::decode(bytes).unwrap();
    assert_eq!(database.version, "2026e");
    let paris = database.zones.iter().find(|zone| zone.name == "Europe/Paris").unwrap();
    assert!(!database.rules[paris.rules].recurring.is_empty());
    assert_eq!(database.legacy.len(), database.rules.len());
    assert!(database.zones.windows(2).all(|pair| pair[0].name < pair[1].name));
    for length in [0, 1, 7, 16, bytes.len() / 2, bytes.len() - 1] { assert!(tzdb::decode(&bytes[..length]).is_err(), "{length}"); }
    assert!(tzdb::decode(&vec![0; tzdb::MAX_BYTES + 1]).is_err());
}

fn fixture(times: &[i64],offsets: &[i32]) -> Vec<u8> {
    assert_eq!(offsets.len(),times.len() + 1);
    let mut rules = vec![1];
    rules.extend_from_slice(&0i32.to_be_bytes());rules.push(0);
    rules.extend_from_slice(&(times.len() as i32).to_be_bytes());
    for time in times { rules.push(255);rules.extend_from_slice(&time.to_be_bytes()); }
    for offset in offsets { rules.push(127);rules.extend_from_slice(&offset.to_be_bytes()); }
    rules.push(0);
    fn text(bytes: &mut Vec<u8>,value: &str) { bytes.extend_from_slice(&(value.len() as u16).to_be_bytes());bytes.extend_from_slice(value.as_bytes()); }
    let mut bytes = vec![1];text(&mut bytes,"TZDB");
    bytes.extend_from_slice(&1u16.to_be_bytes());text(&mut bytes,"test");
    bytes.extend_from_slice(&1u16.to_be_bytes());text(&mut bytes,"Test/Zone");
    bytes.extend_from_slice(&1u16.to_be_bytes());bytes.extend_from_slice(&(rules.len() as u16).to_be_bytes());bytes.extend_from_slice(&rules);
    bytes.extend_from_slice(&[0,1,0,0,0,0,0,0]);
    bytes
}

#[test]
fn wall_transition_endpoints_are_validated_before_runtime_offset_arithmetic() {
    let minimum = rtdb_types::CivilDate::MIN.epoch_day() * 86_400;
    let maximum = rtdb_types::CivilDate::MAX.epoch_day() * 86_400 + 86_399;
    for (times,offsets) in [
        (vec![i64::MIN,0],vec![-900,0,900]),(vec![i64::MAX],vec![0,900]),
        (vec![minimum],vec![-900,0]),(vec![minimum],vec![0,-900]),
        (vec![maximum],vec![900,0]),(vec![maximum],vec![0,900]),
    ] { assert!(tzdb::decode(&fixture(&times,&offsets)).is_err()); }
    for (second,offsets) in [(minimum + 900,[-900,0]),(maximum - 900,[0,900])] {
        tzdb::decode(&fixture(&[second],&offsets)).unwrap();
    }
}
