use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_types::{Result, Value};
use regex::Regex;
use super::{ConnectData, ConnectField, ConnectSchema, PluginSpec, Predicate, SchemaKind, SinkRecord, Transform, error};
mod cast;
mod flatten;
mod timestamp;

pub(super) fn predicate(spec: &PluginSpec) -> Result<Box<dyn Predicate>> {
    let predicate = match spec.class_name.as_str() {
        "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone" => BuiltinPredicate::Tombstone,
        "org.apache.kafka.connect.transforms.predicates.HasHeaderKey" => BuiltinPredicate::Header(required(&spec.options, "name")?.into()),
        "org.apache.kafka.connect.transforms.predicates.TopicNameMatches" => BuiltinPredicate::Topic(regex_full(required(&spec.options, "pattern")?)?),
        class => return Err(error("plugin-class-not-found", format!("No native predicate registered for '{class}'"))),
    };
    Ok(Box::new(predicate))
}
enum BuiltinPredicate { Tombstone, Header(String), Topic(Regex) }
impl Predicate for BuiltinPredicate {
    fn test(&mut self, record: &SinkRecord) -> Result<bool> {
        Ok(match self { Self::Tombstone => record.value.value == Value::Null, Self::Header(name) => record.headers.iter().any(|(key, _)| key == name), Self::Topic(pattern) => pattern.find(&record.topic).is_some_and(|found| found.start() == 0 && found.end() == record.topic.len()) })
    }
}
fn regex(pattern: &str) -> Result<Regex> { Regex::new(pattern).map_err(|cause| error("invalid-plugin-config", cause)) }
fn regex_full(pattern: &str) -> Result<Regex> { regex(&format!("\\A(?:{pattern})\\z")) }
fn required<'a>(options: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> { options.get(key).map(String::as_str).ok_or_else(|| error("invalid-plugin-config", format!("Missing '{key}'"))) }
fn list(options: &BTreeMap<String, String>, key: &str) -> Vec<String> { options.get(key).map_or(Vec::new(), |value| value.split(',').map(str::trim).filter(|value| !value.is_empty()).map(str::to_owned).collect()) }

pub(super) fn open(spec: &PluginSpec, environment: Arc<super::DateEnvironment>) -> Result<Box<dyn Transform>> {
    let class = spec.class_name.strip_prefix("org.apache.kafka.connect.transforms.").ok_or_else(|| error("plugin-class-not-found", &spec.class_name))?;
    let (name, key) = class.strip_suffix("$Key").map(|name| (name, true)).or_else(|| class.strip_suffix("$Value").map(|name| (name, false))).unwrap_or((class, false));
    if matches!(name, "ExtractField" | "HoistField" | "ReplaceField" | "SetSchemaMetadata" | "HeaderFrom" | "InsertField" | "Cast" | "Flatten" | "MaskField" | "TimestampConverter") && name == class { return Err(error("plugin-class-not-found", "Transform requires its concrete Key or Value class")); }
    let options = &spec.options;
    if name == "Cast" { return cast::open(options, key, environment); }
    if name == "Flatten" { return Ok(Box::new(flatten::Flatten::new(options.get("delimiter").cloned().unwrap_or_else(|| ".".into()), key))); }
    if name == "TimestampConverter" { return timestamp::converter(options, key, environment); }
    if name == "TimestampRouter" { return timestamp::router(options, environment); }
    let action = match name {
        "Filter" => Action::Filter,
        "ExtractField" => {
            let version = options.get("field.syntax.version").map_or("V1", String::as_str);
            let path = field_path(required(options, "field")?, version)?;
            Action::Extract(path, super::converter::option_bool(options, "replace.null.with.default", true)?)
        }
        "HoistField" => Action::Hoist(required(options, "field")?.into()),
        "ReplaceField" => {
            let mut renames = BTreeMap::new();
            for entry in list(options, "renames") {
                let (from, to) = entry.split_once(':').filter(|(_, to)| !to.contains(':')).ok_or_else(|| error("invalid-plugin-config", "Rename must contain one colon"))?;
                renames.insert(from.to_owned(), to.to_owned());
            }
            Action::Replace { include: list(options, "include").into_iter().collect(), exclude: list(options, "exclude").into_iter().collect(), renames }
        }
        "ValueToKey" => { let fields = list(options, "fields"); if fields.is_empty() { return Err(error("invalid-plugin-config", "ValueToKey fields must be nonempty")); } Action::ValueToKey(fields) }
        "DropHeaders" => Action::DropHeaders(list(options, "headers").into_iter().collect()),
        "InsertHeader" => Action::InsertHeader(required(options, "header")?.into(), super::header::literal_with_environment(required(options, "value.literal")?, &environment)?),
        "MaskField" => {
            let fields = list(options, "fields");
            if fields.is_empty() || fields.iter().collect::<BTreeSet<_>>().len() != fields.len() { return Err(error("invalid-plugin-config", "MaskField requires nonempty distinct field names")); }
            let replacement = options.get("replacement").cloned();
            if replacement.as_ref().is_some_and(String::is_empty) { return Err(error("invalid-plugin-config", "Mask replacement cannot be empty")); }
            Action::Mask { fields: fields.into_iter().collect(), replacement, replace: super::converter::option_bool(options, "replace.null.with.default", true)? }
        }
        "HeaderFrom" => {
            let fields = list(options, "fields"); let headers = list(options, "headers");
            if fields.is_empty() || fields.len() != headers.len() { return Err(error("invalid-plugin-config", "HeaderFrom requires matching nonempty field and header lists")); }
            let move_fields = match required(options, "operation")? { "move" => true, "copy" => false, _ => return Err(error("invalid-plugin-config", "HeaderFrom operation must be move or copy")) };
            Action::HeaderFrom { fields, headers, move_fields, replace: super::converter::option_bool(options, "replace.null.with.default", true)? }
        }
        "InsertField" => {
            let mut fields = Vec::new();
            for (key, kind) in [("topic.field", InsertValue::Topic), ("partition.field", InsertValue::Partition), ("offset.field", InsertValue::Offset), ("timestamp.field", InsertValue::Timestamp)] {
                if let Some(name) = options.get(key) { fields.push(insertion(name, kind)); }
            }
            if let Some(name) = options.get("static.field") { fields.push(insertion(name, InsertValue::Static(required(options, "static.value")?.into()))); }
            if fields.is_empty() { return Err(error("invalid-plugin-config", "InsertField requires at least one field")); }
            Action::InsertField { fields, replace: super::converter::option_bool(options, "replace.null.with.default", true)? }
        }
        "RegexRouter" => Action::RegexRouter(regex_full(required(options, "regex")?)?, required(options, "replacement")?.into()),
        "SetSchemaMetadata" => {
            let name = options.get("schema.name").cloned();
            let version = options.get("schema.version").map(|value| value.parse::<i32>().map_err(|cause| error("invalid-plugin-config", cause))).transpose()?;
            if name.is_none() && version.is_none() { return Err(error("invalid-plugin-config", "Schema name or version is required")); }
            Action::SchemaMetadata(name, version)
        }
        _ => return Err(error("plugin-class-not-found", format!("No native transform registered for '{}'", spec.class_name))),
    };
    Ok(Box::new(BuiltinTransform { key, action }))
}
fn field_path(field: &str, version: &str) -> Result<Vec<String>> {
    if version == "V1" { return Ok(vec![field.into()]); }
    if version != "V2" { return Err(error("invalid-plugin-config", "field.syntax.version must be V1 or V2")); }
    let mut parts = Vec::new(); let mut part = String::new(); let mut quoted = false; let mut characters = field.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '`' if quoted && characters.peek() == Some(&'`') => { characters.next(); part.push('`'); }
            '`' => quoted = !quoted,
            '.' if !quoted => { parts.push(std::mem::take(&mut part)); }
            character => part.push(character),
        }
    }
    if quoted { return Err(error("invalid-plugin-config", "Unclosed quoted field path")); }
    parts.push(part); Ok(parts)
}
enum Action {
    Filter, Extract(Vec<String>, bool), Hoist(String), ValueToKey(Vec<String>), DropHeaders(BTreeSet<String>), RegexRouter(Regex, String),
    Replace { include: BTreeSet<String>, exclude: BTreeSet<String>, renames: BTreeMap<String, String> },
    SchemaMetadata(Option<String>, Option<i32>),
    HeaderFrom { fields: Vec<String>, headers: Vec<String>, move_fields: bool, replace: bool },
    InsertField { fields: Vec<Insertion>, replace: bool },
    Mask { fields: BTreeSet<String>, replacement: Option<String>, replace: bool },
    InsertHeader(String, ConnectData),
}
enum InsertValue { Topic, Partition, Offset, Timestamp, Static(String) }
struct Insertion { name: String, optional: bool, value: InsertValue }
fn insertion(name: &str, value: InsertValue) -> Insertion {
    Insertion { name: name.strip_suffix(['!', '?']).unwrap_or(name).into(), optional: !name.ends_with('!'), value }
}
struct BuiltinTransform { key: bool, action: Action }
impl Transform for BuiltinTransform {
    fn apply(&mut self, mut record: SinkRecord) -> Result<Option<SinkRecord>> {
        resolve_record_schema(&mut record.key); resolve_record_schema(&mut record.value);
        match &self.action {
            Action::InsertHeader(name, value) => { record.headers.push((name.clone(), value.clone())); return Ok(Some(record)); }
            Action::Mask { fields: selected, replacement, replace } => {
                let data = if self.key { &mut record.key } else { &mut record.value };
                let mut values = fields_with_defaults(data, *replace)?;
                for name in selected {
                    if data.schema.is_some() && !values.contains_key(name) { continue; }
                    let value = values.remove(name).unwrap_or(Value::Null);
                    values.insert(name.clone(), cast::mask(value, replacement.as_deref())?);
                }
                data.value = if data.schema.is_some() { Value::Struct(values) } else { string_map(values) };
                return Ok(Some(record));
            }
            Action::Filter => return Ok(None),
            Action::DropHeaders(names) => { record.headers.retain(|(name, _)| !names.contains(name)); return Ok(Some(record)); }
            Action::RegexRouter(pattern, replacement) => {
                if let Some(captures) = pattern.captures(&record.topic) {
                    record.topic = java_replacement(pattern, &captures, replacement)?;
                }
                return Ok(Some(record));
            }
            Action::ValueToKey(names) => {
                let fields = fields(&record.value)?;
                let value = names.iter().map(|name| Ok((name.clone(), fields.get(name).cloned().unwrap_or(Value::Null)))).collect::<Result<BTreeMap<_, _>>>()?;
                let schema = record.value.schema.as_ref().map(|schema| {
                    let selected = names.iter().map(|name| schema.field(name).ok_or_else(|| error("transform-error", format!("Unknown field '{name}'")))).collect::<Result<_>>()?;
                    Ok::<_, rtdb_types::Anomaly>(Arc::new(ConnectSchema::new(SchemaKind::Struct(selected))))
                }).transpose()?;
                record.key = if schema.is_some() { ConnectData { schema, value: Value::Struct(value) } } else { ConnectData::schemaless(string_map(value)) };
                return Ok(Some(record));
            }
            Action::HeaderFrom { fields: selected, headers, move_fields, replace } => {
                let data = if self.key { &mut record.key } else { &mut record.value };
                let mut values = fields_with_defaults(data, *replace)?;
                for (name, header) in selected.iter().zip(headers) {
                    let schema = data.schema.as_ref().map(|schema| schema.field(name).map(|field| Arc::clone(&field.schema)).ok_or_else(|| error("transform-error", format!("Unknown header source field '{name}'")))).transpose()?;
                    record.headers.push((header.clone(), ConnectData { schema, value: values.get(name).cloned().unwrap_or(Value::Null) }));
                }
                if *move_fields {
                    for name in selected { values.remove(name); }
                    if let Some(schema) = &data.schema {
                        let mut updated = schema.as_ref().clone();
                        let SchemaKind::Struct(fields) = &schema.kind else { return Err(error("transform-error", "HeaderFrom requires a Struct schema")); };
                        updated.kind = SchemaKind::Struct(fields.iter().filter(|field| !selected.contains(&field.name)).cloned().collect()); updated.default = None;
                        data.schema = Some(Arc::new(updated));
                    }
                    data.value = if data.schema.is_some() { Value::Struct(values) } else { string_map(values) };
                }
                return Ok(Some(record));
            }
            Action::InsertField { fields: inserted, replace } => {
                let data = if self.key { &mut record.key } else { &mut record.value };
                if data.value == Value::Null { return Ok(Some(record)); }
                let mut values = fields_with_defaults(data, *replace)?;
                let mut schema = data.schema.as_ref().map(|schema| schema.as_ref().clone());
                for insertion in inserted {
                    let (kind, value, logical) = match &insertion.value {
                        InsertValue::Topic => (SchemaKind::String, Some(Value::Utf8(record.topic.clone())), false),
                        InsertValue::Partition => (SchemaKind::I32, Some(Value::I32(record.partition)), false),
                        InsertValue::Offset => (SchemaKind::I64, Some(Value::I64(record.offset)), false),
                        InsertValue::Timestamp => (SchemaKind::I64, record.timestamp_millis.map(|value| if schema.is_some() { Value::Timestamp { value, unit: rtdb_types::TimeUnit::Millisecond, timezone: Some("UTC".into()) } } else { Value::I64(value) }), true),
                        InsertValue::Static(value) => (SchemaKind::String, Some(Value::Utf8(value.clone())), false),
                    };
                    if let Some(schema) = &mut schema {
                        let SchemaKind::Struct(fields) = &mut schema.kind else { return Err(error("transform-error", "InsertField requires a Struct schema")); };
                        if fields.iter().any(|field| field.name == insertion.name) { return Err(error("transform-error", "InsertField duplicates an existing schema field")); }
                        let mut field = ConnectSchema::new(kind); field.optional = insertion.optional;
                        if logical { field.name = Some("org.apache.kafka.connect.data.Timestamp".into()); field.version = Some(1); }
                        fields.push(ConnectField { name: insertion.name.clone(), schema: Arc::new(field) });
                        values.insert(insertion.name.clone(), value.unwrap_or(Value::Null));
                    } else if let Some(value) = value { values.insert(insertion.name.clone(), value); }
                }
                if let Some(schema) = &mut schema { schema.default = None; }
                data.value = if schema.is_some() { Value::Struct(values) } else { string_map(values) };
                data.schema = schema.map(Arc::new);
                return Ok(Some(record));
            }
            _ => (),
        }
        let data = if self.key { &mut record.key } else { &mut record.value };
        match &self.action {
            Action::Extract(path, replace) => {
                for name in path {
                    let schema = data.schema.as_ref().map(|schema| schema.field(name).map(|field| Arc::clone(&field.schema)).ok_or_else(|| error("transform-error", format!("Unknown field '{name}'")))).transpose()?;
                    let mut value = if data.value == Value::Null { Value::Null } else { fields(data)?.remove(name).unwrap_or(Value::Null) };
                    if *replace && value == Value::Null { if let Some(default) = schema.as_ref().and_then(|schema| schema.default.as_ref()) { value = default.clone(); } }
                    *data = ConnectData { schema, value };
                }
            }
            Action::Hoist(name) => {
                let old = std::mem::replace(data, ConnectData::null());
                let schema = old.schema.map(|schema| Arc::new(ConnectSchema::new(SchemaKind::Struct(vec![ConnectField { name: name.clone(), schema }]))));
                let fields = BTreeMap::from([(name.clone(), old.value)]);
                *data = ConnectData { value: if schema.is_some() { Value::Struct(fields) } else { string_map(fields) }, schema };
            }
            Action::Replace { include, exclude, renames } if data.value != Value::Null => {
                let keep = |name: &str| !exclude.contains(name) && (include.is_empty() || include.contains(name));
                let values = fields(data)?.into_iter().filter(|(name, _)| keep(name)).map(|(name, value)| (renames.get(&name).cloned().unwrap_or(name), value)).collect::<BTreeMap<_, _>>();
                let schema = data.schema.as_ref().map(|schema| {
                    let SchemaKind::Struct(fields) = &schema.kind else { return Err(error("transform-error", "ReplaceField requires a Struct schema")); };
                    let mut result = schema.as_ref().clone(); result.kind = SchemaKind::Struct(fields.iter().filter(|field| keep(&field.name)).map(|field| ConnectField { name: renames.get(&field.name).cloned().unwrap_or_else(|| field.name.clone()), schema: Arc::clone(&field.schema) }).collect()); result.default = None;
                    Ok::<_, rtdb_types::Anomaly>(Arc::new(result))
                }).transpose()?;
                *data = ConnectData { value: if schema.is_some() { Value::Struct(values) } else { string_map(values) }, schema };
            }
            Action::SchemaMetadata(name, version) if data.value != Value::Null => {
                let mut schema = data.schema.as_ref().ok_or_else(|| error("transform-error", "SetSchemaMetadata requires a schema"))?.as_ref().clone();
                if let Some(name) = name { schema.name = Some(name.clone()); } if let Some(version) = version { schema.version = Some(*version); }
                data.schema = Some(Arc::new(schema));
            }
            _ => (),
        }
        Ok(Some(record))
    }
}
fn fields(data: &ConnectData) -> Result<BTreeMap<String, Value>> {
    match &data.value {
        Value::Struct(fields) => Ok(fields.clone()),
        Value::Map { entries, .. } => entries.iter().map(|(key, value)| match key { Value::Utf8(key) => Ok((key.clone(), value.clone())), _ => Err(error("transform-error", "Transform requires string map keys")) }).collect(),
        _ => Err(error("transform-error", "Transform requires a Struct or Map")),
    }
}
fn fields_with_defaults(data: &ConnectData, replace: bool) -> Result<BTreeMap<String, Value>> {
    let mut values = fields(data)?;
    if replace { if let Some(schema) = &data.schema { if let SchemaKind::Struct(fields) = &schema.resolved().kind {
        for field in fields { if values.get(&field.name).is_none_or(|value| *value == Value::Null) { if let Some(default) = &field.schema.default { values.insert(field.name.clone(), default.clone()); } } }
    } } }
    Ok(values)
}
fn resolve_record_schema(data: &mut ConnectData) {
    if let Some(schema) = &data.schema { if matches!(schema.kind, SchemaKind::Reference(_)) { data.schema = Some(Arc::new(schema.resolved().into_owned())); } }
}
fn string_map(fields: BTreeMap<String, Value>) -> Value { Value::Map { entries: fields.into_iter().map(|(key, value)| (Value::Utf8(key), value)).collect(), sorted: false } }

fn java_replacement(pattern: &Regex, captures: &regex::Captures<'_>, replacement: &str) -> Result<String> {
    let mut output = String::new(); let mut characters = replacement.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '\\' => output.push(characters.next().ok_or_else(|| error("transform-error", "Incomplete replacement escape"))?),
            '$' => {
                let first = characters.next().ok_or_else(|| error("transform-error", "Incomplete replacement group"))?;
                if first == '{' {
                    let mut name = String::new();
                    loop { match characters.next() { Some('}') => break, Some(character) => name.push(character), None => return Err(error("transform-error", "Unclosed replacement group")) } }
                    if !pattern.capture_names().flatten().any(|candidate| candidate == name) { return Err(error("transform-error", "Unknown named replacement group")); }
                    if let Some(value) = captures.name(&name) { output.push_str(value.as_str()); }
                } else {
                    let mut group = first.to_digit(10).ok_or_else(|| error("transform-error", "Replacement group is not a number"))? as usize;
                    if group >= captures.len() { return Err(error("transform-error", "Replacement group is out of range")); }
                    while let Some(digit) = characters.peek().and_then(|character| character.to_digit(10)) {
                        let candidate = group.saturating_mul(10).saturating_add(digit as usize);
                        if candidate >= captures.len() { break; } characters.next(); group = candidate;
                    }
                    if let Some(value) = captures.get(group) { output.push_str(value.as_str()); }
                }
            }
            character => output.push(character),
        }
    }
    Ok(output)
}
