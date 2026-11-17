use super::*;

struct Wire { version: i16, bytes: Vec<u8> }
impl Wire {
    fn i16(&mut self, value: i16) { self.bytes.extend(value.to_be_bytes()); }
    fn i32(&mut self, value: i32) { self.bytes.extend(value.to_be_bytes()); }
    fn varint(&mut self, mut value: u32) {
        while value >= 128 { self.bytes.push(value as u8 | 128); value >>= 7; }
        self.bytes.push(value as u8);
    }
    fn count(&mut self, value: u8) { self.array_length(i32::from(value)); }
    fn array_length(&mut self, value: i32) {
        if self.version >= 9 { self.varint(value.wrapping_add(1) as u32); } else { self.i32(value); }
    }
    fn string(&mut self, value: Option<&str>) {
        if self.version >= 9 { self.varint(value.map_or(0, |value| value.len() as u32 + 1)); }
        else { self.i16(value.map_or(-1, |value| value.len() as i16)); }
        if let Some(value) = value { self.bytes.extend(value.as_bytes()); }
    }
    fn tags(&mut self) { if self.version >= 9 { self.bytes.extend([1, 7, 2, 91, 92]); } }
}
fn response(version: i16, nullable_topic: bool) -> Vec<u8> {
    let mut w = Wire { version, bytes: Vec::new() };
    if version >= 3 { w.i32(37); }
    w.count(2);
    for id in [9, 2] {
        w.i32(id); w.string(Some("host")); w.i32(9092);
        if version >= 1 { w.string(None); }
        w.tags();
    }
    if version >= 2 { w.string(Some("cluster")); }
    if version >= 1 { w.i32(9); }
    w.count(2);
    for (name, error) in [(Some("z"), 0), (if nullable_topic { None } else { Some("a") }, 29001)] {
        w.i16(error); w.string(name);
        if version >= 10 { w.bytes.extend([43; 16]); }
        if version >= 1 { w.bytes.push(1); }
        w.count(2);
        for id in [7, 1] {
            w.i16(-123); w.i32(id); w.i32(if id == 7 { 9 } else { -1 });
            if version >= 7 { w.i32(42); }
            w.count(2); w.i32(9); w.i32(2);
            w.count(1); w.i32(2);
            if version >= 5 { w.count(1); w.i32(9); }
            w.tags();
        }
        if version >= 8 { w.i32(123); }
        w.tags();
    }
    if (8..=10).contains(&version) { w.i32(456); }
    if version >= 13 { w.i16(29002); }
    w.tags();
    w.bytes
}

#[test]
fn all_versions_preserve_order_errors_uuid_arrays_and_epoch_reliability() {
    for version in 0..=13 {
        let raw = response(version, version >= 12);
        let view = MetadataView::parse(version, &raw).unwrap();
        assert_eq!(view.raw(), raw);
        assert_eq!(view.version(), version);
        assert_eq!(view.has_reliable_leader_epochs(), version >= 9);
        assert_eq!((view.summary().brokers, view.summary().topics, view.summary().partitions), (2,2,4));
        assert_eq!(view.summary().error, if version >= 13 {29002} else {0});
        let mut brokers = Vec::new(); let mut topics = Vec::new(); let mut partitions = Vec::new();
        view.visit(|event| match event {
            Event::Broker(broker) => { brokers.push(broker.id); assert_eq!(broker.host,"host"); assert_eq!(broker.rack,None); }
            Event::Topic(topic) => {
                topics.push(topic.name); assert_eq!(topic.partitions,2); assert_eq!(topic.internal,version>=1);
                assert_eq!(topic.id.copied(),if version>=10 {Some([43;16])} else {None});
                assert_eq!(topic.error,if topics.len()==1 {0} else {29001});
            }
            Event::Partition(partition) => {
                partitions.push(partition.id); assert_eq!(partition.error,-123);
                assert_eq!(partition.leader,if partition.id==7 {9} else {-1});
                assert_eq!(partition.leader_epoch,if version>=7 {Some(42)} else {None});
                assert_eq!(partition.replicas.iter().collect::<Vec<_>>(),[9,2]);
                assert_eq!(partition.isr.iter().collect::<Vec<_>>(),[2]);
                assert_eq!(partition.offline.len(),usize::from(version>=5));
            }
            Event::TopicEnd {authorized_operations} => assert_eq!(authorized_operations,if version>=8 {Some(123)} else {None}),
            Event::End {authorized_operations,error} => { assert_eq!(authorized_operations,if (8..=10).contains(&version) {Some(456)} else {None}); assert_eq!(error,view.summary().error); }
            Event::Throttle(value) => assert_eq!(value,if version>=3 {37} else {0}),
            Event::Cluster {id,controller} => { assert_eq!(id,if version>=2 {Some("cluster")} else {None}); assert_eq!(controller,if version>=1 {9} else {-1}); }
        }).unwrap();
        assert_eq!(brokers,[9,2]); assert_eq!(partitions,[7,1,7,1]);
        assert_eq!(topics,[Some("z"),if version>=12 {None} else {Some("a")}]);
    }
}

#[test]
fn every_truncation_fails_before_any_view_can_deliver_a_visitor_event() {
    for version in 0..=13 {
        let bytes = response(version, false);
        for length in 0..bytes.len() { assert!(MetadataView::parse(version,&bytes[..length]).is_err(),"v{version} length {length}"); }
        let mut extra = bytes; extra.push(0);
        let view = MetadataView::parse(version,&extra).unwrap();
        assert_eq!(view.raw(), extra);
        assert_eq!(view.trailing_bytes(), [0]);
        extra.extend([255, 128]);
        assert_eq!(MetadataView::parse(version,&extra).unwrap().trailing_bytes(), [0, 255, 128]);
    }
}

#[test]
fn legacy_and_flexible_empty_arrays_have_exact_versioned_boundaries() {
    assert_eq!(MetadataView::parse(0,&[0;8]).unwrap().summary(),Summary::default());
    let v13 = [0,0,0,0,1,0,255,255,255,255,1,0,0,0];
    assert_eq!(MetadataView::parse(13,&v13).unwrap().summary(),Summary::default());
    assert_eq!(MetadataView::parse(-1,&[]).unwrap_err(),ProtocolError::UnsupportedVersion);
    assert_eq!(MetadataView::parse(14,&[]).unwrap_err(),ProtocolError::UnsupportedVersion);
    assert_eq!(MetadataView::parse(0,&[255;8]).unwrap_err(),ProtocolError::InvalidLength);
    assert_eq!(MetadataView::parse(9,&[0;5]).unwrap_err(),ProtocolError::InvalidLength);
    assert_eq!(MetadataView::parse(9,&[0,0,0,0,255,255,255,255,16]).unwrap_err(),ProtocolError::Truncated);
    assert_eq!(MetadataView::parse(9,&[0,0,0,0,255,255,255,255,128]).unwrap_err(),ProtocolError::InvalidVarint);
}

#[test]
fn malformed_utf8_and_nullable_topic_version_are_explicit_rejections() {
    let mut body = response(0,false);
    body[10] = 255;
    assert_eq!(MetadataView::parse(0,&body).unwrap_err(),ProtocolError::UnsupportedUtf8);
    assert!(MetadataView::parse(11,&response(11,true)).is_err());
    assert!(MetadataView::parse(12,&response(12,true)).is_ok());
}

#[test]
fn unknown_root_tags_retain_duplicate_and_decreasing_ids_in_received_order() {
    let mut body = response(13,false); body.truncate(body.len()-5);
    let prefix = body.len();
    body.extend([2,7,0,7,0]);
    assert_eq!(MetadataView::parse(13,&body).unwrap().raw(),body);
    body.truncate(prefix); body.extend([2,7,0,6,0]);
    assert_eq!(MetadataView::parse(13,&body).unwrap().raw(),body);
    body.truncate(prefix); body.extend([2,7,0,8,2,17,18]);
    assert_eq!(MetadataView::parse(13,&body).unwrap().raw(),body);
}

#[test]
fn borrowed_topic_ffi_layout_matches_native_name_and_length_fields() {
    assert_eq!(std::mem::size_of::<MetadataTopic<'_>>(),std::mem::size_of::<sys::rd_kafka_ordered_metadata_topic_t>());
    assert_eq!(std::mem::align_of::<MetadataTopic<'_>>(),std::mem::align_of::<sys::rd_kafka_ordered_metadata_topic_t>());
    let text = "topic"; let topic = MetadataTopic::new(text);
    assert_eq!(topic.raw.name,text.as_ptr().cast()); assert_eq!(topic.raw.length,text.len());
}


fn empty_response(version: i16) -> Vec<u8> {
    let mut w = Wire { version, bytes: Vec::new() };
    if version >= 3 { w.i32(0); }
    w.count(0);
    if version >= 2 { w.string(None); }
    if version >= 1 { w.i32(-1); }
    w.count(0);
    if (8..=10).contains(&version) { w.i32(0); }
    if version >= 13 { w.i16(0); }
    if version >= 9 { w.bytes.push(0); }
    w.bytes
}

fn response_with_string(version: i16, slot: usize, value: &str) -> Vec<u8> {
    let mut w = Wire { version, bytes: Vec::new() };
    if version >= 3 { w.i32(0); }
    w.count(1); w.i32(1); w.string(Some(if slot == 0 { value } else { "" })); w.i32(9092);
    w.string(Some(if slot == 1 { value } else { "" })); w.tags();
    w.string(Some(if slot == 2 { value } else { "" })); w.i32(-1);
    w.count(1); w.i16(0); w.string(Some(if slot == 3 { value } else { "" }));
    if version >= 10 { w.bytes.extend([0;16]); }
    w.bytes.push(0); w.count(0);
    if version >= 8 { w.i32(0); }
    w.tags();
    if (8..=10).contains(&version) { w.i32(0); }
    if version >= 13 { w.i16(0); }
    w.tags();
    w.bytes
}

#[test]
fn all_string_slots_enforce_the_signed_short_ceiling_before_payload_reads() {
    for version in [2, 8, 9, 12, 13] {
        for slot in 0..4 {
            let maximum = response_with_string(version, slot, &"x".repeat(32767));
            assert_eq!(MetadataView::parse(version, &maximum).unwrap().summary().string_bytes, 32767);
            if version >= 9 {
                let oversized = response_with_string(version, slot, &"x".repeat(32768));
                assert_eq!(MetadataView::parse(version, &oversized).unwrap_err(), ProtocolError::StringTooLong);
            }
        }
    }
    assert_eq!(MetadataView::parse(9, &[0,0,0,0,2,0,0,0,1,129,128,2]).unwrap_err(), ProtocolError::StringTooLong);
}

#[test]
fn nullable_strings_accept_all_signed_negative_lengths_and_wrapping_compact_lengths() {
    for length in [-1i16, -2, i16::MIN] {
        let mut body = empty_response(2);
        body[4..6].copy_from_slice(&length.to_be_bytes());
        let view = MetadataView::parse(2, &body).unwrap();
        view.visit(|event| { if let Event::Cluster {id, ..} = event { assert_eq!(id, None); } }).unwrap();
    }
    for encoded in [0u32, u32::MAX, 0x8000_0001] {
        let mut body = empty_response(13);
        let mut prefix = Wire {version: 13, bytes: body[..5].to_vec()};
        prefix.varint(encoded);
        prefix.bytes.extend_from_slice(&body[6..]);
        body = prefix.bytes;
        let view = MetadataView::parse(13, &body).unwrap();
        view.visit(|event| { if let Event::Cluster {id, ..} = event { assert_eq!(id, None); } }).unwrap();
    }
    let mut body = empty_response(13);
    drop(body.splice(5..6, [128,128,128,128,8]));
    assert_eq!(MetadataView::parse(13, &body).unwrap_err(), ProtocolError::StringTooLong);
    let mut nonnull = Wire {version: 13, bytes: vec![0,0,0,0,2,0,0,0,1]};
    nonnull.varint(u32::MAX);
    assert_eq!(MetadataView::parse(13, &nonnull.bytes).unwrap_err(), ProtocolError::InvalidLength);
}

#[test]
fn fifth_terminal_byte_truncates_to_java_int_and_negative_tag_ids_are_retained() {
    for fifth in 0u8..128 {
        let mut body = empty_response(13); body.pop();
        body.extend([128,128,128,128,fifth]);
        match fifth & 15 {
            0 => assert_eq!(MetadataView::parse(13, &body).unwrap().raw(), body),
            1..=7 => assert_eq!(MetadataView::parse(13, &body).unwrap_err(), ProtocolError::Truncated),
            _ => assert_eq!(MetadataView::parse(13, &body).unwrap_err(), ProtocolError::InvalidLength),
        }
    }
    for fifth in 128u8..=255 {
        let mut body = empty_response(13); body.pop();
        body.extend([128,128,128,128,fifth,0]);
        assert_eq!(MetadataView::parse(13, &body).unwrap_err(), ProtocolError::InvalidVarint);
    }
    for tags in [vec![1,255,255,255,255,15,0], vec![2,7,0,7,0], vec![2,7,0,6,0]] {
        let mut body = empty_response(13); body.pop(); body.extend(tags);
        assert_eq!(MetadataView::parse(13, &body).unwrap().raw(), body);
    }
    let mut body = empty_response(13); body.pop();
    body.extend([1,0,255,255,255,255,15]);
    assert_eq!(MetadataView::parse(13, &body).unwrap_err(), ProtocolError::InvalidLength);
}

fn prefix_to_array(version: i16, slot: usize) -> Wire {
    let mut w = Wire {version, bytes: Vec::new()};
    if version >= 3 { w.i32(0); }
    if slot == 0 { return w; }
    w.count(0);
    if version >= 2 { w.string(None); }
    if version >= 1 { w.i32(-1); }
    if slot == 1 { return w; }
    w.count(1); w.i16(0); w.string(Some("t"));
    if version >= 10 { w.bytes.extend([0;16]); }
    if version >= 1 { w.bytes.push(0); }
    if slot == 2 { return w; }
    w.count(1); w.i16(0); w.i32(0); w.i32(-1);
    if version >= 7 { w.i32(0); }
    for _ in 3..slot { w.count(0); }
    w
}

#[test]
fn every_array_slot_checks_remaining_bytes_before_the_million_element_cap() {
    for version in [0, 8, 13] {
        for slot in 0..if version >= 5 { 6 } else { 5 } {
            for remaining in [1_000_000, 1_000_001] {
                let mut w = prefix_to_array(version, slot);
                w.array_length(1_000_001);
                w.bytes.resize(w.bytes.len() + remaining, 0);
                assert_eq!(MetadataView::parse(version, &w.bytes).unwrap_err(),
                    if remaining == 1_000_000 { ProtocolError::Truncated } else { ProtocolError::ArrayTooLong });
            }
        }
    }
    let mut w = prefix_to_array(13, 0); w.varint(0x8000_0000);
    assert_eq!(MetadataView::parse(13, &w.bytes).unwrap_err(), ProtocolError::Truncated);
    let mut w = prefix_to_array(13, 0); w.varint(u32::MAX);
    assert_eq!(MetadataView::parse(13, &w.bytes).unwrap_err(), ProtocolError::InvalidLength);
}

#[test]
fn million_replica_ids_are_borrowed_and_the_next_count_is_rejected() {
    for version in [0, 13] {
        let mut w = prefix_to_array(version, 3);
        w.array_length(1_000_000); w.bytes.resize(w.bytes.len() + 4_000_000, 0);
        w.count(0);
        if version >= 5 { w.count(0); }
        w.tags();
        if version >= 8 { w.i32(0); }
        w.tags();
        if version >= 13 { w.i16(0); }
        w.tags();
        let view = MetadataView::parse(version, &w.bytes).unwrap();
        let mut seen = false;
        view.visit(|event| { if let Event::Partition(p) = event { seen = true; assert_eq!(p.replicas.len(),1_000_000); assert!(p.replicas.iter().all(|id| id == 0)); } }).unwrap();
        assert!(seen);
    }
}

fn prefix_to_tags(slot: usize) -> Wire {
    let mut w = Wire {version: 13, bytes: Vec::new()};
    w.i32(0); w.count(1); w.i32(1); w.string(Some("")); w.i32(1); w.string(None);
    if slot == 0 { return w; }
    w.bytes.push(0); w.string(None); w.i32(-1);
    w.count(1); w.i16(0); w.string(Some("t")); w.bytes.extend([0;16]); w.bytes.push(0);
    w.count(1); w.i16(0); w.i32(0); w.i32(-1); w.i32(0); w.count(0); w.count(0); w.count(0);
    if slot == 1 { return w; }
    w.bytes.push(0); w.i32(0);
    if slot == 2 { return w; }
    w.bytes.push(0); w.i16(0);
    w
}

#[test]
fn every_tag_list_checks_remaining_before_ten_thousand_and_negative_counts() {
    for slot in 0..4 {
        for remaining in [10_000, 10_001] {
            let mut w = prefix_to_tags(slot); w.varint(10_001);
            w.bytes.resize(w.bytes.len()+remaining, 0);
            assert_eq!(MetadataView::parse(13, &w.bytes).unwrap_err(),
                if remaining == 10_000 {ProtocolError::Truncated} else {ProtocolError::TooManyTags});
        }
        let mut w = prefix_to_tags(slot); w.varint(u32::MAX);
        assert_eq!(MetadataView::parse(13, &w.bytes).unwrap_err(), ProtocolError::InvalidLength);
    }
    let mut body = empty_response(13); body.pop();
    let mut w = Wire {version:13, bytes:body}; w.varint(10_000);
    w.bytes.resize(w.bytes.len()+20_000, 0);
    let view = MetadataView::parse(13,&w.bytes).unwrap();
    assert_eq!(view.raw(),w.bytes); assert!(view.trailing_bytes().is_empty());
}

#[test]
fn wrapper_suffix_acceptance_preserves_the_complete_body_and_visitor_boundary() {
    let body = [0u8; 9];
    let view = MetadataView::parse(0, &body).unwrap();
    assert_eq!(view.raw(), body); assert_eq!(view.trailing_bytes(), [0]);
    let mut calls = 0;
    if let Ok(view) = MetadataView::parse(0, &body[..7]) { view.visit(|_| calls += 1).unwrap(); }
    assert_eq!(calls, 0);
}


#[test]
fn raw_views_preserve_every_version_and_validate_before_visitation() {
    for version in 0..=13 {
        let body = response(version, version >= 12);
        let text = MetadataView::parse(version, &body).unwrap();
        let raw = RawMetadataView::parse(version, &body).unwrap();
        assert_eq!(raw.raw(), text.raw()); assert_eq!(raw.summary(), text.summary());
        assert_eq!(raw.version(), version); assert_eq!(raw.has_reliable_leader_epochs(), version >= 9);
        let mut ids = Vec::new();
        raw.visit(|event| if let RawEvent::Partition(p) = event { ids.push(p.id); }).unwrap();
        assert_eq!(ids, [7,1,7,1]);
        for end in 0..body.len() {
            let mut calls = 0;
            if let Ok(view) = RawMetadataView::parse(version, &body[..end]) { view.visit(|_| calls += 1).unwrap(); }
            assert_eq!(calls, 0, "v{version} length {end}");
            assert!(RawMetadataView::parse(version, &body[..end]).is_err());
        }
    }
}

#[test]
fn raw_strings_preserve_malformed_bytes_in_all_four_fields() {
    for version in [2, 8, 9, 12, 13] {
        for slot in 0..4 {
            let mut body = response_with_string(version, slot, "x");
            let offset = body.iter().position(|byte| *byte == b'x').unwrap();
            body[offset] = 0x80;
            assert_eq!(MetadataView::parse(version, &body).unwrap_err(), ProtocolError::UnsupportedUtf8);
            let view = RawMetadataView::parse(version, &body).unwrap();
            assert_eq!(view.summary().string_bytes, 1);
            let mut fields = Vec::new();
            view.visit(|event| match event {
                RawEvent::Broker(b) => { fields.push(b.host); fields.push(b.rack.unwrap()); }
                RawEvent::Cluster {id, ..} => fields.push(id.unwrap()),
                RawEvent::Topic(t) => fields.push(t.name.unwrap()),
                _ => (),
            }).unwrap();
            assert_eq!(fields[slot], [0x80]);
            assert_eq!(fields[slot].as_ptr(), body[offset..].as_ptr());
        }
    }
}

#[test]
fn raw_string_limits_apply_before_replacement_expansion() {
    for slot in 0..4 {
        let mut body = response_with_string(13, slot, &"x".repeat(32767));
        for byte in &mut body { if *byte == b'x' { *byte = 0x80; } }
        let view = RawMetadataView::parse(13, &body).unwrap();
        assert_eq!(view.summary().string_bytes, 32767);
        assert_eq!(view.raw(), body);
        assert_eq!(MetadataView::parse(13, &body).unwrap_err(), ProtocolError::UnsupportedUtf8);
        let oversized = response_with_string(13, slot, &"x".repeat(32768));
        assert_eq!(RawMetadataView::parse(13, &oversized).unwrap_err(), ProtocolError::StringTooLong);
    }
}

#[test]
fn utf8_facade_retains_string_error_priority_before_later_truncation() {
    let body = [0,0,0,1, 0,0,0,1, 0,1, 0x80];
    assert_eq!(MetadataView::parse(0, &body).unwrap_err(), ProtocolError::UnsupportedUtf8);
    assert_eq!(RawMetadataView::parse(0, &body).unwrap_err(), ProtocolError::Truncated);
}

#[test]
fn raw_suffix_tags_and_nullable_names_remain_uninterpreted() {
    let mut body = response(13, true);
    body.truncate(body.len()-5); body.extend([2,7,0,6,0,0xff,0x80]);
    let view = RawMetadataView::parse(13, &body).unwrap();
    assert_eq!(view.raw(), body); assert_eq!(view.trailing_bytes(), [0xff,0x80]);
    let mut names = Vec::new();
    view.visit(|event| if let RawEvent::Topic(topic) = event { names.push(topic.name); }).unwrap();
    assert_eq!(names, [Some(b"z".as_slice()), None]);
}
