use super::*;

fn with_list(list: Option<Json>) -> Json {
    let mut value = json!({"schemaType":"AVRO","schema":"\"int\""});
    if let Some(list) = list { value["references"] = list; }
    value
}

#[test]
fn nullable_reference_list_decoding_preserves_null_slots_and_omits_only_empty_lists() {
    for list in [None, Some(Json::Null), Some(json!([]))] {
        let value: RegistrySchema = serde_json::from_value(with_list(list)).unwrap();
        assert!(value.references.is_empty());
        assert!(serde_json::to_value(value).unwrap().get("references").is_none());
    }
    let value: RegistrySchema = serde_json::from_value(with_list(Some(json!([null, {}])))).unwrap();
    assert_eq!(value.references.len(), 2);
    assert!(value.references[0].is_none());
    assert!(value.references[1].is_some());
    assert_eq!(serde_json::to_value(value).unwrap()["references"], json!([null, {}]));
    assert!(serde_json::from_value::<RegistrySchema>(with_list(Some(json!(false)))).is_err());
}

#[tokio::test]
async fn id_response_missing_null_and_empty_reference_lists_reach_lookup_without_reference_requests() {
    for list in [None, Some(Json::Null), Some(json!([]))] {
        let (url, handle) = server(vec![with_list(list), json!({"id":7,"version":2})]).await;
        let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
        let document = converter.schema("events", 7, &mut context).await.unwrap();
        assert!(document.root.references.is_empty());
        let requests = requests(handle).await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].0, "GET /schemas/ids/7 HTTP/1.1");
        assert_eq!(requests[1].0, "POST /subjects/events-value?normalize=false&deleted=true HTTP/1.1");
        assert!(requests[1].1.get("references").is_none());
        drop(document); drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn fetched_response_missing_null_and_empty_reference_lists_complete_the_child() {
    for list in [None, Some(Json::Null), Some(json!([]))] {
        let (url, handle) = server(vec![with_list(list)]).await;
        let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
        let resolved = converter.resolve_references(root(vec![reference("A", "a", json!(1))]), &mut context).await.unwrap();
        assert_eq!(resolved.references.order, ["A"]);
        assert_eq!(resolved.references.texts().collect::<Vec<_>>(), ["\"int\""]);
        assert_eq!(requests(handle).await.len(), 1);
        drop(resolved); drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn null_entry_is_demanded_after_prior_valid_fetches_and_before_subject_lookup() {
    let initial = schema("\"int\"", vec![reference("A", "a", json!(1)), Json::Null]);
    let (url, handle) = server(vec![initial, with_list(Some(Json::Null))]).await;
    let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
    assert_eq!(converter.schema("events", 7, &mut context).await.err().unwrap(), error("schema-registry", "Schema reference must not be null"));
    let requests = requests(handle).await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].0, "GET /subjects/a/versions/1?deleted=true HTTP/1.1");
    assert!(memory.used_bytes() > 0);
    drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn fetched_child_demands_its_prior_valid_entry_before_its_later_null_member() {
    let child = schema("\"int\"", vec![reference("B", "b", json!(2)), Json::Null]);
    let (url, handle) = server(vec![child, with_list(None)]).await;
    let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
    assert_eq!(converter.resolve_references(root(vec![reference("A", "a", json!(1))]), &mut context).await.err().unwrap(), error("schema-registry", "Schema reference must not be null"));
    assert_eq!(requests(handle).await.into_iter().map(|(request, _)| request).collect::<Vec<_>>(), [
        "GET /subjects/a/versions/1?deleted=true HTTP/1.1", "GET /subjects/b/versions/2?deleted=true HTTP/1.1",
    ]);
    drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn duplicate_child_null_members_remain_undemanded_after_latest_version_replacement() {
    for returned in [Json::Null, json!(9)] {
        let first = with_list(None);
        let mut duplicate = with_list(Some(json!([null]))); duplicate["version"] = returned.clone();
        let (url, handle) = server(vec![first, duplicate]).await;
        let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
        let resolved = converter.resolve_references(root(vec![reference("A", "first", json!(1)), reference("A", "second", json!(-1))]), &mut context).await.unwrap();
        assert_eq!(resolved.references.order, ["A"]);
        let entry = resolved.root.references[1].as_ref().unwrap();
        assert_eq!(entry.version, returned.as_i64().map(|version| version as i32));
        assert_eq!(entry.subject.as_deref(), Some("second"));
        let value = serde_json::to_value(&resolved.root).unwrap();
        assert_eq!(value["references"][1].get("version"), (!returned.is_null()).then_some(&returned));
        assert_eq!(requests(handle).await.into_iter().map(|(request, _)| request).collect::<Vec<_>>(), [
            "GET /subjects/first/versions/1?deleted=true HTTP/1.1", "GET /subjects/second/versions/-1?deleted=true HTTP/1.1",
        ]);
        drop(resolved); drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn an_earlier_failed_reference_response_precedes_a_later_null_entry() {
    let (url, handle) = server(vec![Json::Null]).await;
    let mut converter = converter(url); let memory = MemoryBudget::new(1 << 27); let mut context = ConversionContext::new(Arc::clone(&memory));
    let failure = converter.resolve_references(root(vec![reference("A", "a", json!(1)), Json::Null]), &mut context).await.err().unwrap();
    assert_ne!(failure, error("schema-registry", "Schema reference must not be null"));
    assert_eq!(requests(handle).await.len(), 1);
    drop(converter); drop(context); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn typed_admission_counts_every_nullable_slot_before_vector_materialization() {
    let value = with_list(Some(Json::Array(vec![Json::Null; 1024])));
    let bound = 1024 * std::mem::size_of::<Option<super::super::super::Reference>>() * 3 + 4096;
    let memory = MemoryBudget::new(bound); let mut rejected = ConversionContext::with_limit(Arc::clone(&memory), bound - 1);
    assert!(admit_typed_references(&value, &mut rejected).is_err());
    assert_eq!(memory.used_bytes(), 0);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    admit_typed_references(&value, &mut context).unwrap();
    let parsed: RegistrySchema = serde_json::from_value(value).unwrap();
    assert_eq!(parsed.references.len(), 1024);
    assert!(parsed.references.iter().all(Option::is_none));
    assert_eq!(memory.used_bytes(), bound);
    drop(parsed); drop(context); drop(rejected); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn retained_document_charges_null_slots_and_spare_capacity_even_when_empty_lists_are_omitted() {
    for empty in [false, true] {
        let mut root = root(vec![]);
        root.references = Vec::with_capacity(4096);
        if !empty { root.references.push(None); }
        let slots = root.references.capacity() * std::mem::size_of::<Option<super::super::super::Reference>>();
        assert!(root_retained_bound(&root).unwrap() >= slots);
        assert_eq!(serialized_size(&root).unwrap(), serde_json::to_vec(&root).unwrap().len());
        let value = serde_json::to_value(&root).unwrap();
        if empty { assert!(value.get("references").is_none()); }
        else { assert_eq!(value["references"], json!([null])); }
        let converter = converter("http://registry.invalid".into());
        let memory = MemoryBudget::new(1 << 24); let mut context = ConversionContext::new(Arc::clone(&memory));
        let document = converter.finish_document(ResolvedSchema { root, references: ResolvedReferences::default() }, "events-value".into(), 1, &mut context).unwrap();
        drop(converter); drop(context);
        assert_eq!(memory.used_bytes(), document.charge.bytes());
        assert!(document.charge.bytes() >= slots);
        drop(document); assert_eq!(memory.used_bytes(), 0);
    }
}
