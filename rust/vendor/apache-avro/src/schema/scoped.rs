use super::*;

/// Name construction and validation borrowed by one synchronous schema parse.
/// Returned names must already include their enclosing namespace, if applicable.
/// A returned `None` namespace is normalized to `Some("")` for subsequent qualification and reference-key identity.
/// Definitions retaining a `Name` serialize that namespace explicitly as an empty string.
/// Fixed definitions converted to `Uuid` or `Duration` discard their original name and namespace.
/// Name construction and validation results must be stable for the same inputs and namespace during the call.
/// The library retains no policy reference in returned schemas and does not change global validators.
/// A policy implementation may itself call global or scoped parsing APIs.
pub trait SchemaNamePolicy {
    /// Constructs an absolute name or returns a policy validation error.
    /// `namespace` is the explicit or inherited namespace supplied by the parser.
    fn name(&self, name: &str, namespace: &Namespace) -> AvroResult<Name>;

    /// Validates a record field's name; field aliases are passed through unchanged.
    fn record_field(&self, name: &str) -> AvroResult<()>;

    /// Validates one enum symbol before duplicate and default checks.
    fn enum_symbol(&self, symbol: &str) -> AvroResult<()>;

    /// Supplies a second absolute reference candidate after the primary candidate is absent.
    /// Lookup tries parsed, resolving, and input schemas for the primary before this candidate.
    /// Errors propagate; returning `None` ends lookup without another candidate.
    fn reference_fallback(&self, _name: &str, _namespace: &Namespace) -> AvroResult<Option<Name>> {
        Ok(None)
    }
}

impl Schema {
    /// Parses the JSON root `schema` and named JSON definitions from `schemata` using one borrowed name policy.
    /// Referenced record, enum, and fixed definitions may depend on each other and on themselves.
    /// As in ordinary parsing, aliases of unparsed external definitions are not preindexed.
    /// Object-valued `type` wrappers use the effective inner declaration, as in ordinary schema parsing.
    /// Returns a tuple of the parsed root and referenced schemas in their input iterator order.
    /// Duplicate policy-resolved names among referenced inputs return `Details::NameCollision`.
    ///
    /// Every dynamic declaration, type alias, reference, field, and enum symbol uses the policy.
    /// The library parser does not read or register global validators on this route; the supplied policy controls its own calls.
    /// Policy errors are returned unchanged; malformed-schema errors retain their error variants.
    /// A nonunion field-default error describes its schema with `Debug`.
    /// The call finishes on its caller and retains no policy reference in its result.
    pub fn parse_str_with_list_with_name_policy(
        schema: &str,
        schemata: impl IntoIterator<Item = impl AsRef<str>>,
        policy: &dyn SchemaNamePolicy,
    ) -> AvroResult<(Schema, Vec<Schema>)> {
        let schemata = schemata.into_iter();
        let count = schemata.size_hint().0;
        let mut parser = Parser {
            name_policy: Some(policy),
            input_schemas: HashMap::with_capacity(count),
            resolving_schemas: HashMap::default(),
            input_order: Vec::with_capacity(count),
            parsed_schemas: HashMap::with_capacity(count),
        };
        for json in schemata {
            let schema: Value = serde_json::from_str(json.as_ref()).map_err(Details::ParseSchemaJson)?;
            let Value::Object(inner) = &schema else {
                return Err(Details::GetNameField.into());
            };
            let name = parser.parse_name(inner, &None)?;
            if parser.input_schemas.insert(name.clone(), schema).is_some() {
                return Err(Details::NameCollision(name.fullname(None)).into());
            }
            parser.input_order.push(name);
        }
        parser.parse_input_schemas()?;
        let value = serde_json::from_str(schema).map_err(Details::ParseSchemaJson)?;
        let schema = parser.parse(&value, &None)?;
        let schemata = parser.parse_list()?;
        Ok((schema, schemata))
    }
}

fn absolute(mut name: Name) -> Name {
    name.namespace.get_or_insert_with(String::new);
    name
}

impl Parser<'_> {
    fn policy_name(&self, name: &str, namespace: &Namespace) -> AvroResult<Name> {
        self.name_policy
            .expect("scoped parser name policy")
            .name(name, namespace)
            .map(absolute)
    }

    fn parse_name(&self, complex: &Map<String, Value>, enclosing: &Namespace) -> AvroResult<Name> {
        if self.name_policy.is_none() {
            return Name::parse(complex, enclosing);
        }
        let complex = Self::scoped_declaration(complex)?;
        let name = complex.name().ok_or(Details::GetNameField)?;
        let namespace = complex.string("namespace").or_else(|| enclosing.clone());
        self.policy_name(&name, &namespace)
    }

    fn scoped_declaration(mut complex: &Map<String, Value>) -> AvroResult<&Map<String, Value>> {
        loop {
            match complex.get("type") {
                Some(Value::Object(inner)) => complex = inner,
                Some(Value::String(kind)) if matches!(kind.as_str(), "record" | "enum" | "fixed") => return Ok(complex),
                _ => return Err(Details::GetNameField.into()),
            }
        }
    }

    fn parse_aliases(&self, aliases: Option<Vec<String>>, namespace: &Namespace) -> AvroResult<Aliases> {
        if self.name_policy.is_none() {
            return Ok(fix_aliases_namespace(aliases, namespace));
        }
        aliases.map(|aliases| {
            aliases.into_iter().map(|alias| self.policy_name(&alias, namespace).map(Alias)).collect()
        }).transpose()
    }

    fn validate_field_name(&self, name: &str) -> AvroResult<()> {
        match self.name_policy {
            Some(policy) => policy.record_field(name),
            None => validate_record_field_name(name),
        }
    }

    fn validate_enum_symbol(&self, symbol: &str) -> AvroResult<()> {
        match self.name_policy {
            Some(policy) => policy.enum_symbol(symbol),
            None => validate_enum_symbol_name(symbol),
        }
    }

    fn already_seen_schema(&self, complex: &Map<String, Value>, namespace: &Namespace) -> AvroResult<Option<&Schema>> {
        if self.name_policy.is_none() {
            return Ok(self.get_already_seen_schema(complex, namespace));
        }
        match complex.get("type") {
            Some(Value::String(name)) => {
                let name = self.policy_name(name, namespace)?;
                Ok(self.resolving_schemas.get(&name).or_else(|| self.parsed_schemas.get(&name)))
            },
            _ => Ok(None),
        }
    }

    fn schema_type_name(&self, name: Name, value: Value, parsed: &Schema) -> AvroResult<Name> {
        if self.name_policy.is_none() {
            return Ok(get_schema_type_name(name, value));
        }
        Self::scoped_declaration(value.as_object().ok_or(Details::GetNameField)?)?;
        let actual = match parsed {
            Schema::Decimal(decimal) => decimal.inner.name(),
            _ => parsed.name(),
        };
        if actual.is_some_and(|actual| actual != &name) || actual.is_none() && !self.parsed_schemas.contains_key(&name) {
            return Err(Details::SchemaResolutionError(name).into());
        }
        Ok(name)
    }

    fn finish_scoped_schema(&mut self, complex: &Map<String, Value>, namespace: &Namespace, parsed: &Schema) -> AvroResult<()> {
        let (name, aliases) = match parsed {
            Schema::Decimal(decimal) => match decimal.inner.as_ref() {
                Schema::Fixed(fixed) => (fixed.name.clone(), fixed.aliases.clone()),
                _ => return Ok(()),
            },
            Schema::Uuid | Schema::Duration => {
                let Ok(declaration) = Self::scoped_declaration(complex) else { return Ok(()); };
                let name = self.parse_name(declaration, namespace)?;
                let aliases = self.parse_aliases(declaration.aliases(), &name.namespace)?;
                (name, aliases)
            }
            _ => return Ok(()),
        };
        self.register_parsed_schema(&name, parsed, &aliases);
        Ok(())
    }

    fn fetch_scoped_ref(&mut self, text: &str, namespace: &Namespace) -> AvroResult<Schema> {
        let primary = self.policy_name(text, namespace)?;
        if let Some(found) = self.scoped_candidate(&primary)? {
            return Ok(found);
        }
        if let Some(fallback) = self.name_policy.expect("scoped parser name policy").reference_fallback(text, namespace)? {
            let fallback = absolute(fallback);
            if let Some(found) = self.scoped_candidate(&fallback)? {
                return Ok(found);
            }
        }
        match primary.name.as_str() {
            "record" | "enum" | "fixed" => Err(Details::InvalidSchemaRecord(primary.to_string()).into()),
            _ => Err(Details::ParsePrimitive(primary.fullname(None)).into()),
        }
    }

    fn scoped_candidate(&mut self, name: &Name) -> AvroResult<Option<Schema>> {
        if let Some(parsed) = self.parsed_schemas.get(name) {
            return Ok(Some(self.scoped_reference(parsed)));
        }
        if let Some(resolving) = self.resolving_schemas.get(name) {
            return Ok(Some(resolving.clone()));
        }
        let Some(value) = self.input_schemas.remove(name) else {
            return Ok(None);
        };
        let parsed = self.parse(&value, &None)?;
        let name = self.schema_type_name(name.clone(), value, &parsed)?;
        self.parsed_schemas.insert(name, parsed.clone());
        Ok(Some(self.scoped_reference(&parsed)))
    }

    fn scoped_reference(&self, parsed: &Schema) -> Schema {
        let parsed = parsed.name().and_then(|name| self.parsed_schemas.get(name)).unwrap_or(parsed);
        match parsed {
            Schema::Record(record) => Schema::Ref { name: record.name.clone() },
            Schema::Enum(enumeration) => Schema::Ref { name: enumeration.name.clone() },
            Schema::Fixed(fixed) => Schema::Ref { name: fixed.name.clone() },
            _ => parsed.clone(),
        }
    }
}

#[cfg(test)]
mod tests;
