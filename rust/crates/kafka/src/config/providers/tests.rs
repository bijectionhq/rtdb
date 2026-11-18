use super::*;
use rtdb_types::{AllocationScope, MemoryBudget};

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
    let memory = MemoryBudget::new(1 << 25);
    let scope = AllocationScope::new(&memory).unwrap();
    let resolved = resolve(&original, &mut |bytes| scope.reserve(bytes)).unwrap();
    assert_eq!(resolved["setting"], format!("café/firstcontinued/${{file:after}}/opaque\r\n/${{dir:{}:nested}}/${{unused:x}}", root.path().display()));
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
    let resolved = resolve(&original, &mut |_| Ok(())).unwrap();
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
    assert_eq!(resolve(&invalid, &mut |_| Ok(())).unwrap_err().code, "xtdb.kafka/config-provider");
    let mut unsupported = new();
    unsupported.insert("config.providers.file.class".into(), "example.Provider".into());
    assert_eq!(resolve(&unsupported, &mut |_| Ok(())).unwrap_err().code, "xtdb.kafka/config-provider-class");
    assert_eq!(resolve(&invalid, &mut |_| Err(allocation())).unwrap_err().code, "xtdb.kafka/config-provider-allocation");
    let untouched = BTreeMap::from([("configuration".into(), "${absent:x}".into())]);
    assert!(matches!(resolve(&untouched, &mut |_| panic!("missing providers demand no allocation")).unwrap(), Cow::Borrowed(_)));
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
