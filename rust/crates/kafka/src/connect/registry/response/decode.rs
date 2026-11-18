use rtdb_types::java_text::JavaString;
use super::{ConversionContext, Failure, FailureKind, Result, beans::*, collections, cursor::{Cursor, Kind}, owned::Values, scalars};

struct Creator { seen: u16, all: u16, built: bool }
impl Creator {
    fn new(count: u32) -> Self { Self { seen: 0, all: (1u16 << count) - 1, built: false } }
    fn assign(&mut self, index: u32) { self.seen |= 1u16 << index; }
    fn ready(&self) -> bool { !self.built && self.seen == self.all }
}

pub(super) fn schema_string(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<SchemaString>> {
    if cursor.take_null(context)? { return Ok(None); }
    if cursor.kind(context)? == Kind::String {
        let schema = scalars::text(cursor, context)?;
        return Ok(Some(SchemaString { schema_type: Some(scalars::constant("AVRO", context)?), schema,
            references: Some(Values::new()), metadata: None, rule_set: None, schema_tags: None, max_id: None }));
    }
    cursor.begin_object(context)?;
    let mut field = cursor.field(context)?;
    let mut value = SchemaString { schema_type: Some(scalars::constant("AVRO", context)?), schema: None,
        references: Some(Values::new()), metadata: None, rule_set: None, schema_tags: None, max_id: None };
    while let Some(name) = field {
        if name.is("schemaType")? { value.schema_type = scalars::text(cursor, context)?; }
        else if name.is("schema")? { value.schema = scalars::text(cursor, context)?; }
        else if name.is("references")? { value.references = collections::list(cursor, context, reference)?; }
        else if name.is("metadata")? { value.metadata = metadata(cursor, context)?; }
        else if name.is("ruleSet")? { value.rule_set = rule_set(cursor, context)?; }
        else if name.is("schemaTags")? { value.schema_tags = collections::list(cursor, context, schema_tags)?; }
        else if name.is("maxId")? { value.max_id = scalars::integer(cursor, context)?; }
        else { cursor.skip(context)?; }
        field = cursor.field(context)?;
    }
    Ok(Some(value))
}

pub(super) fn schema(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<Schema>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_object(context)?;
    let mut value = Schema { subject: None, version: None, id: None, schema_type: None, references: None,
        metadata: None, rule_set: None, schema: None, schema_tags: None };
    let mut creator = Creator::new(9);
    let mut setters = Values::new();
    let mut field = cursor.field(context)?;
    while let Some(name) = field {
        if name.is("subject")? { value.subject = scalars::text(cursor, context)?; creator.assign(0); }
        else if name.is("version")? { value.version = scalars::integer(cursor, context)?; creator.assign(1); }
        else if name.is("id")? { value.id = scalars::integer(cursor, context)?; creator.assign(2); }
        else if name.is("schemaType")? { value.schema_type = scalars::text(cursor, context)?; creator.assign(3); }
        else if name.is("references")? { value.references = collections::list(cursor, context, reference)?; creator.assign(4); }
        else if name.is("metadata")? { value.metadata = metadata(cursor, context)?; creator.assign(5); }
        else if name.is("ruleset")? {
            if creator.built { return Err(Failure::at(FailureKind::MissingSetter, cursor.offset(), "No fallback setter/field for creator property ruleset")); }
            value.rule_set = rule_set(cursor, context)?; creator.assign(6);
        }
        else if name.is("schema")? { value.schema = scalars::text(cursor, context)?; creator.assign(7); }
        else if name.is("schemaTags")? { value.schema_tags = collections::list(cursor, context, schema_tags)?; creator.assign(8); }
        else if name.is("ruleSet")? {
            let rules = rule_set(cursor, context)?;
            if creator.built { value.rule_set = rules; } else { setters.push(rules, context)?; }
        }
        else { cursor.skip(context)?; }
        field = cursor.field(context)?;
        if creator.ready() { finish_schema(&mut value, &mut setters, context)?; creator.built = true; }
    }
    if !creator.built { finish_schema(&mut value, &mut setters, context)?; }
    Ok(Some(value))
}

fn finish_schema(value: &mut Schema, setters: &mut Values<Option<RuleSet>>, context: &mut ConversionContext) -> Result<()> {
    if value.schema_type.is_none() { value.schema_type = Some(scalars::constant("AVRO", context)?); }
    if value.references.is_none() { value.references = Some(Values::new()); }
    while let Some(rules) = setters.pop() { value.rule_set = rules; }
    Ok(())
}

fn reference(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<Reference>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_object(context)?;
    let mut value = Reference { name: None, subject: None, version: None };
    while let Some(name) = cursor.field(context)? {
        if name.is("name")? { value.name = scalars::text(cursor, context)?; }
        else if name.is("subject")? { value.subject = scalars::text(cursor, context)?; }
        else if name.is("version")? { value.version = scalars::integer(cursor, context)?; }
        else { cursor.skip(context)?; }
    }
    Ok(Some(value))
}

fn metadata(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<Metadata>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_object(context)?;
    let mut value = Metadata { tags: None, properties: None, sensitive: None };
    let mut creator = Creator::new(3);
    let mut field = cursor.field(context)?;
    while let Some(name) = field {
        if name.is("tags")? {
            let tags = collections::tags(cursor, context)?;
            if creator.built && tags.is_some() { return Err(cursor.coercion("Cannot assign Map to final SortedMap Metadata.tags")); }
            value.tags = tags; creator.assign(0);
        }
        else if name.is("properties")? {
            let properties = collections::properties(cursor, context)?;
            if creator.built && properties.is_some() { return Err(cursor.coercion("Cannot assign Map to final SortedMap Metadata.properties")); }
            value.properties = properties; creator.assign(1);
        }
        else if name.is("sensitive")? {
            let sensitive = collections::strings(cursor, context)?;
            if creator.built && sensitive.is_some() { return Err(cursor.coercion("Cannot assign Set to final SortedSet Metadata.sensitive")); }
            value.sensitive = sensitive; creator.assign(2);
        }
        else { cursor.skip(context)?; }
        field = cursor.field(context)?;
        if creator.ready() { finish_metadata(&mut value, cursor)?; creator.built = true; }
    }
    if !creator.built { finish_metadata(&mut value, cursor)?; }
    Ok(Some(value))
}

fn finish_metadata(value: &mut Metadata, cursor: &Cursor<'_>) -> Result<()> {
    let tags = value.tags.get_or_insert_with(|| Tags { values: Values::new(), sorted: true });
    collections::sort_tags(tags, cursor)?;
    let properties = value.properties.get_or_insert_with(|| Properties { values: Values::new(), sorted: true });
    collections::sort_properties(properties);
    let sensitive = value.sensitive.get_or_insert_with(|| StringSet { values: Values::new(), sorted: true });
    collections::sort_strings(sensitive, cursor)?;
    Ok(())
}

fn rule_set(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<RuleSet>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_object(context)?;
    let mut value = RuleSet { migration_rules: None, domain_rules: None };
    let mut creator = Creator::new(2);
    let mut field = cursor.field(context)?;
    while let Some(name) = field {
        if name.is("migrationRules")? { value.migration_rules = collections::list(cursor, context, rule)?; creator.assign(0); }
        else if name.is("domainRules")? { value.domain_rules = collections::list(cursor, context, rule)?; creator.assign(1); }
        else { cursor.skip(context)?; }
        field = cursor.field(context)?;
        if creator.ready() { finish_rule_set(&mut value); creator.built = true; }
    }
    if !creator.built { finish_rule_set(&mut value); }
    Ok(Some(value))
}

fn finish_rule_set(value: &mut RuleSet) {
    if value.migration_rules.is_none() { value.migration_rules = Some(Values::new()); }
    if value.domain_rules.is_none() { value.domain_rules = Some(Values::new()); }
}

fn rule(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<Rule>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_object(context)?;
    let mut value = Rule { name: None, doc: None, kind: None, mode: None, rule_type: None, tags: None,
        params: None, expr: None, on_success: None, on_failure: None, disabled: false };
    let mut creator = Creator::new(11);
    let mut field = cursor.field(context)?;
    while let Some(name) = field {
        if name.is("name")? { value.name = scalars::text(cursor, context)?; creator.assign(0); }
        else if name.is("doc")? { value.doc = scalars::text(cursor, context)?; creator.assign(1); }
        else if name.is("kind")? { value.kind = scalars::enumeration(cursor, context, &["TRANSFORM", "CONDITION"])?.map(|index| [RuleKind::Transform, RuleKind::Condition][index]); creator.assign(2); }
        else if name.is("mode")? { value.mode = scalars::enumeration(cursor, context, &["UPGRADE", "DOWNGRADE", "UPDOWN", "WRITE", "READ", "WRITEREAD"])?.map(|index| [RuleMode::Upgrade, RuleMode::Downgrade, RuleMode::UpDown, RuleMode::Write, RuleMode::Read, RuleMode::WriteRead][index]); creator.assign(3); }
        else if name.is("type")? { value.rule_type = scalars::text(cursor, context)?; creator.assign(4); }
        else if name.is("tags")? {
            let tags = collections::strings(cursor, context)?;
            if creator.built && tags.is_some() { return Err(cursor.coercion("Cannot assign Set to final SortedSet Rule.tags")); }
            value.tags = tags; creator.assign(5);
        }
        else if name.is("params")? {
            let params = collections::properties(cursor, context)?;
            if creator.built && params.is_some() { return Err(cursor.coercion("Cannot assign Map to final SortedMap Rule.params")); }
            value.params = params; creator.assign(6);
        }
        else if name.is("expr")? { value.expr = scalars::text(cursor, context)?; creator.assign(7); }
        else if name.is("onSuccess")? { value.on_success = scalars::text(cursor, context)?; creator.assign(8); }
        else if name.is("onFailure")? { value.on_failure = scalars::text(cursor, context)?; creator.assign(9); }
        else if name.is("disabled")? { value.disabled = scalars::boolean(cursor, context)?; creator.assign(10); }
        else { cursor.skip(context)?; }
        field = cursor.field(context)?;
        if creator.ready() { finish_rule(&mut value, cursor)?; creator.built = true; }
    }
    if !creator.built { finish_rule(&mut value, cursor)?; }
    Ok(Some(value))
}

fn finish_rule(value: &mut Rule, cursor: &Cursor<'_>) -> Result<()> {
    if value.kind.is_none() { value.kind = Some(RuleKind::Transform); }
    if value.mode.is_none() { value.mode = Some(RuleMode::WriteRead); }
    let tags = value.tags.get_or_insert_with(|| StringSet { values: Values::new(), sorted: true });
    collections::sort_strings(tags, cursor)?;
    let params = value.params.get_or_insert_with(|| Properties { values: Values::new(), sorted: true });
    collections::sort_properties(params);
    Ok(())
}

fn schema_tags(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<SchemaTags>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_object(context)?;
    let mut value = SchemaTags { entity: None, tags: None };
    while let Some(name) = cursor.field(context)? {
        if name.is("schemaEntity")? { value.entity = entity(cursor, context)?; }
        else if name.is("tags")? { value.tags = collections::list(cursor, context, scalars::text)?; }
        else { cursor.skip(context)?; }
    }
    Ok(Some(value))
}

fn entity(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<SchemaEntity>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_object(context)?;
    let mut path: Option<JavaString> = None;
    let mut entity_type = None;
    let mut normalized_path = None;
    let mut creator = Creator::new(2);
    let mut field = cursor.field(context)?;
    while let Some(name) = field {
        if name.is("entityPath")? { path = scalars::text(cursor, context)?; creator.assign(0); }
        else if name.is("entityType")? { entity_type = scalars::enumeration(cursor, context, &["sr_record", "sr_field"])?.map(|index| [EntityType::Record, EntityType::Field][index]); creator.assign(1); }
        else { cursor.skip(context)?; }
        field = cursor.field(context)?;
        if creator.ready() { normalized_path = Some(normalize_path(path.as_ref(), cursor, context)?); creator.built = true; }
    }
    if !creator.built { normalized_path = Some(normalize_path(path.as_ref(), cursor, context)?); }
    Ok(Some(SchemaEntity { path, entity_type, normalized_path: normalized_path.unwrap() }))
}

fn normalize_path(path: Option<&JavaString>, cursor: &Cursor<'_>, context: &mut ConversionContext) -> Result<JavaString> {
    let path = path.ok_or_else(|| cursor.constructor("SchemaEntity.entityPath is null"))?;
    if path.view().units().next() != Some(0x2e) { return Ok(path.clone()); }
    let mut units = Values::with_capacity(path.view().len_units() - 1, context)?;
    for unit in path.view().units().skip(1) { units.push(unit, context)?; }
    Ok(JavaString::copy_utf16_with_admission(units.as_slice(), |bytes| context.reserve(bytes))?)
}
