use prost::Message;
use rtdb_flight_sql::rtdb::store::v1::{KeyRange, PurgeFloorReply, PurgeFloorRequest};

#[test]
fn purge_request_keeps_zero_versions_and_exact_guard_order_on_the_wire() {
    let incarnation = [0x11; 16];
    let request = PurgeFloorRequest {
        incarnation: incarnation.to_vec(),
        since: Some(0),
        keep_from: Some(0),
        reads: vec![
            KeyRange { start: b"z".to_vec(), end: None },
            KeyRange { start: b"a".to_vec(), end: Some(b"b".to_vec()) },
        ],
    };
    let mut wire = vec![0x0a, 16];
    wire.extend(incarnation);
    wire.extend([0x10, 0x00, 0x18, 0x00]);
    wire.extend([0x22, 3, 0x0a, 1, b'z']);
    wire.extend([0x22, 6, 0x0a, 1, b'a', 0x12, 1, b'b']);
    assert_eq!(request.encode_to_vec(), wire);
    assert_eq!(PurgeFloorRequest::decode(wire.as_slice()).unwrap(), request);
}

#[test]
fn purge_reply_confirms_an_explicit_zero_minimum() {
    let incarnation = [0x11; 16];
    let reply = PurgeFloorReply { incarnation: incarnation.to_vec(), keep_from: Some(0) };
    let mut wire = vec![0x0a, 16];
    wire.extend(incarnation);
    wire.extend([0x10, 0x00]);
    assert_eq!(reply.encode_to_vec(), wire);
    assert_eq!(PurgeFloorReply::decode(wire.as_slice()).unwrap(), reply);
    assert_eq!(PurgeFloorReply::decode(&wire[..18]).unwrap().keep_from, None);
}
