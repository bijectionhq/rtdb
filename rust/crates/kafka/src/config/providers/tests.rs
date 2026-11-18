use super::*;
#[cfg(unix)]
use rtdb_types::AllocationScope;

#[cfg(unix)]
fn properties(roots: &Path) -> Properties {
    BTreeMap::from([
        (PROVIDERS.into(), "file,dir,,".into()),
        ("config.providers.file.class".into(), FILE.into()),
        ("config.providers.file.param.allowed.paths".into(), roots.to_str().unwrap().into()),
        ("config.providers.dir.class".into(), DIRECTORY.into()),
        ("config.providers.dir.param.allowed.paths".into(), roots.to_str().unwrap().into()),
    ])
}

#[test]
#[cfg(unix)]
fn file_and_directory_values_resolve_once_and_keep_original_missing_placeholders() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("client.properties");
    fs::write(&file, "# UTF-8 Reader\nname: café\nquoted=first\\\r\n\tcontinued\nnext=${file:after}\n").unwrap();
    fs::write(root.path().join("secret"), "opaque\r\n").unwrap();
    fs::create_dir(root.path().join("nested")).unwrap();
    let mut original = properties(root.path());
    original.insert("setting".into(), format!(
        "${{file:{}:name}}/${{file:{}:quoted}}/${{file:{}:next}}/${{dir:{}:secret}}/${{dir:{}:nested}}/${{unused:x}}",
        file.display(), file.display(), file.display(), root.path().display(), root.path().display(),
    ));
    original.insert("shared".into(), format!("${{file:{}:name}}", file.display()));
    let memory = MemoryBudget::new(1 << 25);
    let scope = AllocationScope::new(&memory).unwrap();
    let resolved = resolve(&original, &memory, &mut |bytes| scope.reserve(bytes)).unwrap();
    assert_eq!(resolved["setting"], format!("café/firstcontinued/${{file:after}}/opaque\r\n/${{dir:{}:nested}}/${{unused:x}}", root.path().display()));
    assert_eq!(resolved["shared"], "café");
    assert!(memory.used_bytes() > 4096);
    drop(resolved);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
#[cfg(unix)]
fn lookup_paths_follow_canonical_component_roots_and_directory_keys_name_entries() {
    let root = tempfile::tempdir().unwrap();
    let allowed = root.path().join("prefix");
    let sibling = root.path().join("prefix-sibling");
    fs::create_dir(&allowed).unwrap();
    fs::create_dir(&sibling).unwrap();
    fs::write(sibling.join("target"), "name=unavailable").unwrap();
    let mut original = properties(&allowed);
    original.insert("outside".into(), format!("${{file:{}/../prefix-sibling/target:name}}", allowed.display()));
    original.insert("relative-key".into(), format!("${{dir:{}:../prefix-sibling/target}}", allowed.display()));
    let resolved = resolve_test(&original).unwrap();
    assert_eq!(resolved["outside"], original["outside"]);
    assert_eq!(resolved["relative-key"], original["relative-key"]);
}

#[test]
fn provider_classes_and_roots_are_checked_before_placeholder_demand() {
    let new = || BTreeMap::from([
        (PROVIDERS.into(), "file".into()),
        ("config.providers.file.class".into(), FILE.into()),
        ("config.providers.file.param.allowed.paths".into(), "relative-root".into()),
    ]);
    let invalid = new();
    assert_eq!(resolve_test(&invalid).unwrap_err().code, "xtdb.kafka/config-provider");
    let mut unsupported = new();
    unsupported.insert("config.providers.file.class".into(), "example.Provider".into());
    assert_eq!(resolve_test(&unsupported).unwrap_err().code, "xtdb.kafka/config-provider-class");
    let memory = MemoryBudget::new(1 << 28);
    assert_eq!(resolve(&invalid, &memory, &mut |_| Err(allocation())).unwrap_err().code, "xtdb.kafka/config-provider-allocation");
    let untouched = BTreeMap::from([("configuration".into(), "${absent:x}".into())]);
    assert!(matches!(resolve(&untouched, &memory, &mut |_| panic!("missing providers demand no allocation")).unwrap(), Cow::Borrowed(_)));
}

#[test]
fn providers_configure_in_java_alias_map_order() {
    let mut original = BTreeMap::from([
        (PROVIDERS.into(), "z,a,z".into()),
        ("config.providers.z.class".into(), FILE.into()),
        ("config.providers.z.param.allowed.paths".into(), "relative-root".into()),
        ("config.providers.a.class".into(), "example.Unregistered".into()),
    ]);
    assert_eq!(resolve_test(&original).unwrap_err().code, "xtdb.kafka/config-provider-class");
    original.remove("config.providers.a.class");
    assert_eq!(resolve_test(&original).unwrap_err().code, "xtdb.kafka/config-provider");
}

#[test]
#[cfg(unix)]
fn provider_lookup_order_includes_unknown_aliases_and_repeated_requests() {
    let root = tempfile::tempdir().unwrap();
    let malformed = root.path().join("malformed");
    let absent = root.path().join("absent");
    fs::write(&malformed, b"key=\\uXYZ1\n").unwrap();
    let mut original = BTreeMap::from([
        (PROVIDERS.into(), "a,p".into()),
        ("config.providers.a.class".into(), FILE.into()),
        ("config.providers.p.class".into(), FILE.into()),
        ("setting".into(), format!("${{a:{}:key}}${{p:{}:key}}", malformed.display(), absent.display())),
    ]);
    assert_eq!(resolve_test(&original).unwrap_err().code, "xtdb.kafka/config-provider");
    let setting = original.get_mut("setting").unwrap();
    for alias in 'b'..='l' {
        setting.push_str(&format!("${{{alias}:unused}}"));
    }
    assert_eq!(resolve_test(&original).unwrap_err().code, "xtdb.kafka/config-provider");
    original.get_mut("setting").unwrap().push_str("${a:key}");
    assert_eq!(resolve_test(&original).unwrap_err().code, "xtdb.kafka/properties-file");
}

#[test]
#[cfg(unix)]
fn paths_follow_hash_collision_order_after_extracting_options() {
    let root = tempfile::tempdir().unwrap();
    let absent = root.path().join("Aa");
    let malformed = root.path().join("BB");
    fs::write(&malformed, b"key=\\uXYZ1\n").unwrap();
    let mut original = BTreeMap::from([
        (PROVIDERS.into(), "p".into()),
        ("config.providers.p.class".into(), FILE.into()),
        ("a".into(), format!("${{p:{}:key}}", malformed.display())),
        ("p".into(), format!("${{p:{}:key}}", absent.display())),
    ]);
    assert_eq!(string_hash(absent.to_str().unwrap()), string_hash(malformed.to_str().unwrap()));
    assert_eq!(resolve_test(&original).unwrap_err().code, "xtdb.kafka/properties-file");
    let a = original.remove("a").unwrap();
    let p = original.insert("p".into(), a).unwrap();
    original.insert("a".into(), p);
    assert_eq!(resolve_test(&original).unwrap_err().code, "xtdb.kafka/config-provider");
}

#[test]
fn variable_capture_preserves_empty_fields_optional_path_and_ordinary_braces() {
    let input = "plain ${missing-colon} ${p::key} ${p:path:key:tail} ${:} ${nested${p:key} ${unfinished";
    let observed: Vec<_> = Variables::new(input).map(|var| (var.provider, var.path, var.key, &input[var.start..var.end])).collect();
    assert_eq!(observed, [
        ("p", "", "key", "${p::key}"),
        ("p", "path", "key:tail", "${p:path:key:tail}"),
        ("", "", "", "${:}"),
        ("nested${p", "", "key", "${nested${p:key}"),
    ]);
}

#[test]
#[cfg(unix)]
fn file_provider_projects_the_final_selected_property() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("client.properties");
    let mut original = properties(root.path());
    original.insert("setting".into(), format!("${{file:{}:selected}}", file.display()));
    fs::write(&file, "selected=\\uD800\nselected=ready\nignored=\\uD801\n").unwrap();
    assert_eq!(resolve_test(&original).unwrap()["setting"], "ready");
    fs::write(&file, "selected=ready\nignored=\\u+001\n").unwrap();
    assert_eq!(resolve_test(&original).unwrap_err().code, "xtdb.kafka/properties-file");
}

fn resolve_test(original: &Properties) -> Result<Cow<'_, Properties>> {
    let memory = MemoryBudget::new(1 << 28);
    resolve(original, &memory, &mut |_| Ok(()))
}

#[test]
fn environment_path_check_runs_after_allowlist_compilation() {
    let mut original = BTreeMap::from([
        (PROVIDERS.into(), "environment".into()),
        ("config.providers.environment.class".into(), ENVIRONMENT.into()),
        ("config.providers.environment.param.allowlist.pattern".into(), "[".into()),
        ("setting".into(), "${environment:folder:RTDB_TEST_VALUE}".into()),
    ]);
    assert_eq!(resolve_test(&original).unwrap_err().code, "xtdb.expression/illegal-argument-exception");
    original.remove("config.providers.environment.param.allowlist.pattern");
    assert_eq!(resolve_test(&original).unwrap_err().code, "xtdb.kafka/config-provider");
}
