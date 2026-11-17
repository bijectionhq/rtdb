use super::*;
use crate::connect::{Converter, PluginSpec};
use rtdb_columnar::MemoryBudget;
use serde_json::{Value as Json, json};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn reference(name: &str, subject: &str, version: Json) -> Json { json!({"name":name,"subject":subject,"version":version}) }
fn schema(text: &str, references: Vec<Json>) -> Json { json!({"schemaType":"AVRO","schema":text,"references":references}) }
fn root(references: Vec<Json>) -> RegistrySchema { serde_json::from_value(schema("\"int\"", references)).unwrap() }

async fn server(replies: Vec<Json>) -> (String, tokio::task::JoinHandle<Vec<(String, Json)>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for reply in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") { head.push(socket.read_u8().await.unwrap()); assert!(head.len() < 65_536); }
            let head = String::from_utf8(head).unwrap();
            let length = head.lines().filter_map(|line| line.split_once(':')).find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map_or(0, |(_, length)| length.trim().parse::<usize>().unwrap());
            let mut body = vec![0; length]; socket.read_exact(&mut body).await.unwrap();
            let body = if body.is_empty() { Json::Null } else { serde_json::from_slice(&body).unwrap() };
            requests.push((head.lines().next().unwrap().to_owned(), body));
            let reply = serde_json::to_vec(&reply).unwrap();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", reply.len()).as_bytes()).await.unwrap();
            socket.write_all(&reply).await.unwrap();
        }
        requests
    });
    (url, handle)
}

async fn requests(handle: tokio::task::JoinHandle<Vec<(String, Json)>>) -> Vec<(String, Json)> {
    tokio::time::timeout(Duration::from_secs(10), handle).await.unwrap().unwrap()
}
fn converter(url: String) -> RegistryConverter {
    let spec = PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([("schema.registry.url".into(), url)]) };
    RegistryConverter::open(&spec, super::super::RegistryFormat::Avro, false).unwrap()
}

#[test]
fn default_tenant_reference_qualification_preserves_original_explicit_default_context_text() {
    for (parent, child, expected) in [
        (None, "child", "child"), (None, "", ""), (None, ":.:child", "child"), (None, ":.", ""),
        (Some(":.ctx:parent"), "child", ":.ctx:child"),
        (Some(":.ctx:parent"), ":.:child", ":.ctx::.:child"),
        (Some(":.ctx:parent"), ":.other:child", ":.other:child"),
        (Some(":.ctx:parent"), "", ":.ctx:"),
        (Some("tenant_:.ctx:parent"), "child", "child"),
        (Some(":*parent"), "child", "child"), (None, ":*child", ":*child"),
        (Some(":*:parent"), "child", ":*:child"), (None, ":*:child", ":*:child"),
        (None, ":*", ":*"), (None, ":*x", ":*x"), (None, ":*:", ":*:"),
        (Some(":.ctx:parent"), ":*", ":.ctx::*"), (Some(":.ctx:parent"), ":*x", ":.ctx::*x"),
        (None, ":.context", ":.context:"), (Some(":.ctx:parent"), "é/child", ":.ctx:é/child"),
    ] { assert_eq!(qualify_subject(parent, child), expected, "{parent:?} {child:?}"); }
}

#[test]
fn reference_serialization_omits_empty_text_and_nullable_mutated_versions() {
    let parsed: super::super::Reference = serde_json::from_value(reference("", "", Json::Null)).unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), json!({}));
    for version in [0, -8, 4] {
        let reference: super::super::Reference = serde_json::from_value(reference("A", "subject", json!(version))).unwrap();
        assert_eq!(serde_json::to_value(reference).unwrap(), json!({"name":"A","subject":"subject","version":version}));
    }
}

#[test]
fn ordered_reference_identity_includes_dependency_order_and_the_retained_key_copies() {
    let mut first = ResolvedReferences::default(); first.insert("z".into(), "zero".into()); first.insert("a".into(), "alpha".into());
    let mut second = ResolvedReferences::default(); second.insert("a".into(), "alpha".into()); second.insert("z".into(), "zero".into());
    assert_eq!(first.by_name(), second.by_name()); assert_ne!(first, second);
    assert_eq!(first.texts().collect::<Vec<_>>(), ["zero", "alpha"]);
    let expected = first.order.capacity() * std::mem::size_of::<String>() + first.order.iter().map(String::capacity).sum::<usize>()
        + first.by_name.iter().map(|(name, text)| 512 + name.capacity() + text.capacity()).sum::<usize>();
    assert_eq!(first.retained_bound().unwrap(), expected);
}

#[tokio::test]
async fn reference_graph_fetches_depth_first_and_inserts_dependencies_before_parents() {
    let mut z = schema("\"zero\"", vec![reference("common", "shared", json!(1))]); z["subject"] = json!(":.ctx:z");
    let mut a = schema("\"alpha\"", vec![reference("common", ":.ctx:shared", json!(1))]); a["subject"] = json!(":.ctx:a");
    let common = schema("\"common\"", vec![]);
    let (url, handle) = server(vec![z, common, a, schema("\"ignored duplicate\"", vec![])]).await;
    let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
    let resolved = converter.resolve_references(root(vec![reference("z", "zsub", json!(1)), reference("a", "asub", json!(1))]), &mut context).await.unwrap();
    assert_eq!(resolved.references.order, ["common", "z", "a"]);
    assert_eq!(resolved.references.texts().collect::<Vec<_>>(), ["\"common\"", "\"zero\"", "\"alpha\""]);
    assert_eq!(resolved.references.by_name().keys().map(String::as_str).collect::<Vec<_>>(), ["a", "common", "z"]);
    assert_eq!(requests(handle).await.into_iter().map(|(request, _)| request).collect::<Vec<_>>(), [
        "GET /subjects/zsub/versions/1?deleted=true HTTP/1.1", "GET /subjects/:.ctx:shared/versions/1?deleted=true HTTP/1.1",
        "GET /subjects/asub/versions/1?deleted=true HTTP/1.1", "GET /subjects/:.ctx:shared/versions/1?deleted=true HTTP/1.1",
    ]);
    assert_eq!(resolved.root.references[0].subject.as_deref(), Some("zsub"));
    drop(resolved); drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn reference_cycles_fetch_the_back_edge_before_skipping_descent() {
    let a = schema("\"A\"", vec![reference("B", "b", json!(1))]);
    let b = schema("\"B\"", vec![reference("A", "a", json!(1))]);
    let (url, handle) = server(vec![a.clone(), b, a]).await;
    let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
    let resolved = converter.resolve_references(root(vec![reference("A", "a", json!(1))]), &mut context).await.unwrap();
    assert_eq!(resolved.references.order, ["B", "A"]);
    assert_eq!(requests(handle).await.len(), 3);
    drop(resolved); drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn latest_reference_versions_mutate_before_duplicate_skipping_and_preserve_original_subjects() {
    for returned in [Json::Null, json!(0), json!(-8)] {
        let mut first = schema("\"first\"", vec![]); first["version"] = json!(7);
        let mut duplicate = schema("\"ignored\"", vec![]); duplicate["version"] = returned.clone();
        let (url, handle) = server(vec![first, duplicate]).await;
        let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
        let resolved = converter.resolve_references(root(vec![reference("A", ":.:a", json!(-1)), reference("A", ":.:a", json!(-1))]), &mut context).await.unwrap();
        assert_eq!(resolved.root.references[0].version, Some(7));
        assert_eq!(resolved.root.references[1].version, returned.as_i64().map(|version| version as i32));
        assert_eq!(resolved.root.references[1].subject.as_deref(), Some(":.:a"));
        assert_eq!(resolved.references.texts().collect::<Vec<_>>(), ["\"first\""]);
        for (request, _) in requests(handle).await { assert_eq!(request, "GET /subjects/a/versions/-1?deleted=true HTTP/1.1"); }
        let encoded = serde_json::to_value(&resolved.root).unwrap();
        assert_eq!(encoded["references"][1].get("version"), (!returned.is_null()).then_some(&returned));
        drop(resolved); drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn reference_resolution_precedes_subject_lookup_and_get_only_metadata_never_enters_its_body() {
    let mut initial = schema("\"int\"", vec![reference("A", "a", json!(-1))]); initial["subject"] = json!(":.ignored:root"); initial["version"] = json!(99);
    let mut child = schema("\"long\"", vec![]); child["subject"] = json!(":.child:a"); child["version"] = json!(4);
    let (url, handle) = server(vec![initial, child, json!({"id":7,"version":2})]).await;
    let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
    let document = converter.schema("events", 7, &mut context).await.unwrap();
    assert_eq!(document.version, 2); assert_eq!(document.references.texts().collect::<Vec<_>>(), ["\"long\""]);
    let requests = requests(handle).await;
    assert_eq!(requests[0].0, "GET /schemas/ids/7 HTTP/1.1");
    assert_eq!(requests[1].0, "GET /subjects/a/versions/-1?deleted=true HTTP/1.1");
    assert_eq!(requests[2].0, "POST /subjects/events-value?normalize=false&deleted=true HTTP/1.1");
    assert!(requests[2].1.get("subject").is_none()); assert!(requests[2].1.get("version").is_none());
    assert_eq!(requests[2].1["references"][0], reference("A", "a", json!(4)));
    drop(document); drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn reference_null_validation_occurs_at_the_demanded_edge_and_context_drop_releases_failure_charges() {
    let (url, handle) = server(vec![schema("\"first\"", vec![])]).await;
    let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
    let invalid = json!({"name":"bad","subject":"bad","version":null});
    assert!(converter.resolve_references(root(vec![reference("good", "good", json!(1)), invalid]), &mut context).await.is_err());
    assert_eq!(requests(handle).await.len(), 1); assert!(memory.used_bytes() > 0);
    drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn reference_step_admission_includes_reallocation_overlap_and_rejects_overflow() {
    let per_step = step_bound("", "", None).unwrap();
    let live_frames = std::mem::size_of::<Frame>() * (16 + 32);
    assert!(17 * per_step >= live_frames);
    assert!(add(usize::MAX, 1).is_err());
}

#[tokio::test]
async fn reference_frontier_admission_fails_before_initializing_the_http_client() {
    let mut converter = converter("http://registry.invalid".into());
    let memory = MemoryBudget::new(0); let mut context = ConversionContext::with_limit(Arc::clone(&memory), usize::MAX);
    let result = converter.resolve_references(root(vec![reference("A", "a", json!(1))]), &mut context).await;
    assert_eq!(result.err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert!(converter.client.is_none()); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn reference_graphs_larger_than_128_nodes_use_budget_admission() {
    let references = (0..130).map(|index| reference(&format!("N{index}"), &format!("subject{index}"), json!(1))).collect();
    let (url, handle) = server((0..130).map(|_| schema("\"int\"", vec![])).collect()).await;
    let mut converter = converter(url); let memory = MemoryBudget::new(1 << 30); let mut context = ConversionContext::new(Arc::clone(&memory));
    let resolved = converter.resolve_references(root(references), &mut context).await.unwrap();
    assert_eq!(resolved.references.by_name().len(), 130); assert_eq!(resolved.references.order.len(), 130);
    assert_eq!(requests(handle).await.len(), 130);
    drop(resolved); drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn cancelled_reference_resolution_drops_partial_data_and_keeps_charges_until_context_drop() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") { head.push(socket.read_u8().await.unwrap()); assert!(head.len() < 65_536); }
        ready_tx.send(()).unwrap(); stop_rx.await.unwrap();
    });
    let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
    let mut pending = Box::pin(converter.resolve_references(root(vec![reference("A", "a", json!(1))]), &mut context));
    tokio::select! { result = &mut pending => panic!("unexpected reference completion: {}", result.is_ok()), ready = ready_rx => ready.unwrap() }
    drop(pending); assert!(memory.used_bytes() > 0);
    stop_tx.send(()).unwrap(); handle.await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), converter.close()).await.unwrap().unwrap();
    drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn retained_document_keeps_empty_reference_slots_and_metadata_capacity_after_context_drop() {
    let mut root = root((0..4096).map(|_| reference("", "", Json::Null)).collect());
    let mut metadata = Vec::with_capacity(1024); metadata.push(json!(true)); root.metadata = Some(Json::Array(metadata));
    let reference_slots = root.references.capacity() * std::mem::size_of::<super::super::Reference>();
    let Json::Array(metadata) = root.metadata.as_ref().unwrap() else { unreachable!() };
    let metadata_slots = metadata.capacity() * std::mem::size_of::<Json>();
    assert!(root_retained_bound(&root).unwrap() >= reference_slots + metadata_slots);
    assert_eq!(serialized_size(&root).unwrap(), serde_json::to_vec(&root).unwrap().len());
    assert!(8 * serialized_size(&root).unwrap() < reference_slots);
    let converter = converter("http://registry.invalid".into());
    let memory = MemoryBudget::new(1 << 24); let mut context = ConversionContext::new(Arc::clone(&memory));
    let document = converter.finish_document(ResolvedSchema { root, references: ResolvedReferences::default() }, "events-value".into(), 1, &mut context).unwrap();
    drop(converter); drop(context);
    assert_eq!(memory.used_bytes(), document.charge.bytes());
    assert!(document.charge.bytes() >= reference_slots + metadata_slots);
    assert_eq!(document.root.references.len(), 4096);
    drop(document); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn typed_reference_vector_admission_precedes_materialization_and_ignores_nonarray_shapes() {
    let value = schema("\"int\"", vec![json!({}); 1024]);
    let bound = 1024 * std::mem::size_of::<super::super::Reference>() * 3 + 4096;
    let memory = MemoryBudget::new(bound); let mut context = ConversionContext::with_limit(Arc::clone(&memory), bound - 1);
    assert_eq!(admit_typed_references(&value, &mut context).err().unwrap().code, "xtdb.kafka-connect-source/batch-too-large");
    assert_eq!(memory.used_bytes(), 0);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    admit_typed_references(&value, &mut context).unwrap();
    assert_eq!(memory.used_bytes(), bound);
    let materialized: RegistrySchema = serde_json::from_value(value).unwrap(); assert_eq!(materialized.references.len(), 1024);
    for value in [json!({}), json!({"references":null}), json!({"references":false})] { admit_typed_references(&value, &mut context).unwrap(); }
    assert_eq!(memory.used_bytes(), bound);
    drop(materialized); drop(context); assert_eq!(memory.used_bytes(), 0);
}
