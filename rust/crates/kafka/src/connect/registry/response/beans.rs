use rtdb_types::java_text::JavaString;
use super::owned::Values;

pub(in crate::connect::registry) struct SchemaString {
    pub(in crate::connect::registry) schema_type: Option<JavaString>,
    pub(in crate::connect::registry) schema: Option<JavaString>,
    pub(in crate::connect::registry) references: Option<Values<Option<Reference>>>,
    pub(in crate::connect::registry) metadata: Option<Metadata>,
    pub(in crate::connect::registry) rule_set: Option<RuleSet>,
    pub(in crate::connect::registry) schema_tags: Option<Values<Option<SchemaTags>>>,
    pub(in crate::connect::registry) max_id: Option<i32>,
}

pub(in crate::connect::registry) struct Schema {
    pub(in crate::connect::registry) subject: Option<JavaString>,
    pub(in crate::connect::registry) version: Option<i32>,
    pub(in crate::connect::registry) id: Option<i32>,
    pub(in crate::connect::registry) schema_type: Option<JavaString>,
    pub(in crate::connect::registry) references: Option<Values<Option<Reference>>>,
    pub(in crate::connect::registry) metadata: Option<Metadata>,
    pub(in crate::connect::registry) rule_set: Option<RuleSet>,
    pub(in crate::connect::registry) schema: Option<JavaString>,
    pub(in crate::connect::registry) schema_tags: Option<Values<Option<SchemaTags>>>,
}

pub(in crate::connect::registry) struct Reference {
    pub(in crate::connect::registry) name: Option<JavaString>,
    pub(in crate::connect::registry) subject: Option<JavaString>,
    pub(in crate::connect::registry) version: Option<i32>,
}

pub(in crate::connect::registry) struct StringSet { pub(in crate::connect::registry) values: Values<Option<JavaString>>, pub(in crate::connect::registry) sorted: bool }
pub(in crate::connect::registry) struct Properties { pub(in crate::connect::registry) values: Values<(JavaString, Option<JavaString>)>, pub(in crate::connect::registry) sorted: bool }
pub(in crate::connect::registry) struct Tags { pub(in crate::connect::registry) values: Values<(JavaString, Option<StringSet>)>, pub(in crate::connect::registry) sorted: bool }

pub(in crate::connect::registry) struct Metadata {
    pub(in crate::connect::registry) tags: Option<Tags>,
    pub(in crate::connect::registry) properties: Option<Properties>,
    pub(in crate::connect::registry) sensitive: Option<StringSet>,
}

pub(in crate::connect::registry) struct RuleSet {
    pub(in crate::connect::registry) migration_rules: Option<Values<Option<Rule>>>,
    pub(in crate::connect::registry) domain_rules: Option<Values<Option<Rule>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::registry) enum RuleKind { Transform, Condition }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::registry) enum RuleMode { Upgrade, Downgrade, UpDown, Write, Read, WriteRead }

pub(in crate::connect::registry) struct Rule {
    pub(in crate::connect::registry) name: Option<JavaString>,
    pub(in crate::connect::registry) doc: Option<JavaString>,
    pub(in crate::connect::registry) kind: Option<RuleKind>,
    pub(in crate::connect::registry) mode: Option<RuleMode>,
    pub(in crate::connect::registry) rule_type: Option<JavaString>,
    pub(in crate::connect::registry) tags: Option<StringSet>,
    pub(in crate::connect::registry) params: Option<Properties>,
    pub(in crate::connect::registry) expr: Option<JavaString>,
    pub(in crate::connect::registry) on_success: Option<JavaString>,
    pub(in crate::connect::registry) on_failure: Option<JavaString>,
    pub(in crate::connect::registry) disabled: bool,
}

pub(in crate::connect::registry) struct SchemaTags {
    pub(in crate::connect::registry) entity: Option<SchemaEntity>,
    pub(in crate::connect::registry) tags: Option<Values<Option<JavaString>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::registry) enum EntityType { Record, Field }
pub(in crate::connect::registry) struct SchemaEntity {
    pub(in crate::connect::registry) path: Option<JavaString>,
    pub(in crate::connect::registry) entity_type: Option<EntityType>,
    pub(in crate::connect::registry) normalized_path: JavaString,
}
