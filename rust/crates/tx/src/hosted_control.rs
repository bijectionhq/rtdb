use rtdb_types::{
    Anomaly, EntityId, OwnedRelation, PlacedAttemptScope, Result, StructFields, TableRef,
    Timestamp, Value,
};

pub(crate) const CONTROL_TABLES: &str = "hosted_control_tables";

pub(crate) fn admits_name(name: &str) -> bool {
    name.starts_with("bj_") && !name.contains('-') && OwnedRelation::new(name).is_ok()
}

/// An assertion or checked replacement of one installed hosted control row.
#[derive(Clone, Debug)]
pub struct HostedControlWrite {
    relation: OwnedRelation,
    scope: PlacedAttemptScope,
    id: EntityId,
    expected: Option<Timestamp>,
    document: StructFields<Value>,
    check_only: bool,
}

impl HostedControlWrite {
    pub fn new(
        relation: OwnedRelation,
        scope: PlacedAttemptScope,
        expected: Option<Timestamp>,
        document: StructFields<Value>,
    ) -> Result<Self> {
        if !admits_name(relation.name())
            || document.len() > 128
            || document
                .keys()
                .any(|name| name != "_id" && name.starts_with('_'))
        {
            return Err(invalid());
        }
        let Some(Value::Utf8(id)) = document.get("_id") else {
            return Err(invalid());
        };
        if id.is_empty() || id.len() > 4096 {
            return Err(invalid());
        }
        let id = EntityId::String(id.clone());
        let result = Self {
            relation,
            scope,
            id,
            expected,
            document,
            check_only: false,
        };
        if result.retained_bytes()? > 1024 * 1024 {
            return Err(invalid());
        }
        Ok(result)
    }

    pub fn check(
        relation: OwnedRelation,
        scope: PlacedAttemptScope,
        expected: Option<Timestamp>,
        document: StructFields<Value>,
    ) -> Result<Self> {
        if document.len() != 1 {
            return Err(invalid());
        }
        let mut result = Self::new(relation, scope, expected, document)?;
        result.check_only = true;
        Ok(result)
    }

    pub(crate) fn is_check(&self) -> bool {
        self.check_only
    }

    pub(crate) fn name(&self) -> &str {
        self.relation.name()
    }

    pub fn table(&self) -> TableRef {
        TableRef::new("public", self.relation.name())
    }
    pub fn scope(&self) -> PlacedAttemptScope {
        self.scope
    }
    pub fn id(&self) -> &EntityId {
        &self.id
    }
    pub fn expected(&self) -> Option<Timestamp> {
        self.expected
    }
    pub fn replacement(&self) -> Option<&StructFields<Value>> {
        (!self.check_only).then_some(&self.document)
    }

    pub fn retained_bytes(&self) -> Result<usize> {
        Value::document_retained_bytes(&self.document)?
            .checked_add(self.id.as_value().retained_bytes()?)
            .and_then(|n| n.checked_add(self.relation.name().len() + 512))
            .ok_or_else(invalid)
    }

    pub(crate) fn body(&self) -> StructFields<Value> {
        StructFields::from([
            ("relation".into(), Value::Utf8(self.relation.name().into())),
            ("scope".into(), Value::Binary(self.scope.encode().to_vec())),
            (
                "expected".into(),
                self.expected.map_or(Value::Null, |time| Value::I64(time.0)),
            ),
            ("document".into(), Value::Struct(self.document.clone())),
        ])
    }
}

fn invalid() -> Anomaly {
    Anomaly::incorrect("rtdb.hosted/control", "Invalid hosted control replacement")
}
