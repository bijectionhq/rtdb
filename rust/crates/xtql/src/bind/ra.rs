use super::*;
use crate::{Edn, EdnKind};

/// Imports a Clojure logical relation into the native plan using exact column symbols.
/// Named parameter bindings describe the caller's typed argument positions.
pub fn compile_ra_in(source: &str, catalog: &dyn Catalog, options: &CompileOptions, memory: &Arc<MemoryBudget>) -> Result<PreparedStatement> {
    let parse_bytes = source.len().checked_mul(128).and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(|| invalid("Logical relation syntax exceeds addressable memory"))?;
    let _parse = memory.reserve(parse_bytes)?;
    let form = read(source)?;
    let mut binder = Binder::default();
    let symbols = Symbols::Relational;
    let mut parameters = Parameters::new();
    let mut next_parameter = 0;
    for (name, (index, data_type)) in &options.named_parameters {
        let name = symbols.parameter(name);
        if parameters.insert(name, binder.parameter(*index, data_type.clone())).is_some() {
            return Err(invalid("Duplicate normalized logical relation parameter"));
        }
        next_parameter = next_parameter.max(index.checked_add(1).ok_or_else(|| invalid("Parameter index exceeds addressable range"))?);
    }
    let mut compiler = Compiler { catalog, binder: &mut binder, next_parameter: &mut next_parameter, timezone: &options.default_timezone, forms: FormAllocation::Tracked(memory), symbols };
    let importer = Importer { compiler: &mut compiler, parameters: &parameters, memory };
    let relation = importer.relation(&form)?;
    Ok(PreparedStatement { statement: Statement::Query(relation), parameter_types: binder.parameter_types().clone(), source: source.into(), notices: Vec::new(), settings: Default::default() })
}

struct Importer<'a, 'b, 'c> {
    compiler: &'a mut Compiler<'b, 'c>,
    parameters: &'a Parameters,
    memory: &'b Arc<MemoryBudget>,
}

enum TableRow<'a> {
    Literal(BTreeMap<&'a str, &'a Edn>),
    Argument(PExpr),
}

impl Importer<'_, '_, '_> {
    fn relation(mut self, form: &Edn) -> Result<Arc<Relation>> { self.lower(form) }

    fn lower(&mut self, form: &Edn) -> Result<Arc<Relation>> {
        let [op, options, children @ ..] = form.vector().ok_or_else(|| invalid("Expected a logical relation vector"))? else {
            return Err(invalid("Logical relation requires an operator and options"));
        };
        let op = op.keyword().ok_or_else(|| invalid("Logical relation operator must be a keyword"))?;
        if options.map().is_none() { return Err(invalid("Logical relation options must be a map")); }
        let result = match (op, children) {
            ("table", []) => self.table(options)?,
            ("scan", []) => self.scan(options)?,
            ("literal-pages", []) => self.literal_pages(options)?,
            ("select" | "σ" | "sigma", [input]) => {
                let input = self.lower(input)?;
                let predicate = self.scalar(required(options, "predicate")?, &mut bind_input(&input))?;
                input.filter(predicate)?
            }
            ("project" | "π" | "pi" | "map" | "ⲭ" | "chi", [input]) => self.project(options, input, matches!(op, "map" | "ⲭ" | "chi"))?,
            ("sort", [input]) => self.sort(options, input)?,
            ("union-all" | "∪", [left, right]) => self.union_all(left, right)?,
            ("distinct" | "δ", [input]) => {
                let input = self.lower(input)?;
                Relation::new(input.schema().clone(), RelationNode::Distinct { input })?
            }
            _ => return Err(error("rtdb.ra/unsupported-relation", format!("Unsupported logical relation operator or arity: {op}/{}", children.len()))),
        };
        Ok(Arc::new(result))
    }

    fn scalar(&mut self, form: &Edn, input: &mut Bound) -> Result<PExpr> {
        let expression = crate::ast::parse_expr_in(form, self.memory)?;
        let relation = Arc::clone(&input.relation);
        let expression = self.compiler.expr(&expression, input, self.parameters)?;
        if !Arc::ptr_eq(&relation, &input.relation) {
            return Err(invalid("Logical relation expressions require relational subqueries to be explicit operators"));
        }
        Ok(expression)
    }

    fn table(&mut self, options: &Edn) -> Result<Relation> {
        match (options.option("rows"), options.option("column"), options.option("param")) {
            (Some(rows), None, None) => self.row_table(options, rows),
            (None, Some(column), None) => self.column_table(options, column),
            (None, None, Some(parameter)) => self.argument_table(options, parameter),
            _ => Err(invalid("Logical table requires exactly one of :rows, :column, and :param")),
        }
    }

    fn scan(&mut self, options: &Edn) -> Result<Relation> {
        let EdnKind::String(database) = &required(options, "db-name")?.kind else { return Err(invalid("Scan database name must be a string")); };
        if !self.compiler.catalog.databases().contains(database) {
            return Err(error("xtdb/unknown-db", format!("Unknown database: {database}")).with_context("db-name", database.clone().into()));
        }
        let name = QualifiedTable { database: database.clone(), table: crate::ast::table_ref(required(options, "table")?, true)? };
        let table = self.compiler.catalog.table(&name)?;
        let missing = table.is_none();
        let table = table.unwrap_or(TableBinding { name, oid: None, columns: BTreeMap::new() });
        let mut columns = Vec::new();
        let mut variables = BTreeMap::new();
        let mut predicates = Vec::new();
        for column in sequence(required(options, "columns")?)? {
            let name = if let Some([(name, predicate)]) = column.map() {
                predicates.push(predicate);
                column_name(name)?
            } else { column_name(column)? };
            if variables.contains_key(name) { continue; }
            let field = self.compiler.binder.field(name, None, table.scan_column_type(name))?;
            columns.push(field.clone());
            variables.insert(name.to_owned(), field);
        }
        let mut bound = Bound { variables, ..Bound::unit() };
        let predicates = predicates.into_iter().map(|predicate| self.scalar(predicate, &mut bound)).collect::<Result<Vec<_>>>()?;
        let valid_time = self.scan_time(options.option("for-valid-time"))?;
        let system_time = self.scan_time(options.option("for-system-time"))?;
        let clamp_valid_time = match options.option("clamp-valid-time?").map(|form| &form.kind) {
            None | Some(EdnKind::Nil) => false,
            Some(EdnKind::Bool(value)) => *value,
            _ => return Err(invalid("Scan clamp flag must be boolean")),
        };
        if missing { return Ok(Relation::empty(Schema::new(columns)?)); }
        Relation::scan(Scan { table, columns, valid_time, system_time, clamp_valid_time, iids: None, predicates })
    }

    fn scan_time(&mut self, form: Option<&Edn>) -> Result<PTemporal> {
        let Some(form) = form.filter(|form| !matches!(form.kind, EdnKind::Nil)) else { return Ok(PTemporal::Default); };
        if form.keyword() == Some("all-time") { return Ok(PTemporal::All); }
        let mut endpoint = |form: &Edn| {
            let mut input = Bound::unit();
            if form.keyword() == Some("now") {
                return self.compiler.expr(&Expr::Call { function: "current-timestamp".into(), arguments: vec![] }, &mut input, self.parameters);
            }
            self.scalar(form, &mut input)
        };
        let Some([mode, endpoints @ ..]) = form.vector() else { return Err(invalid("Expected a scan period vector")); };
        let null = PExpr::Literal(Value::Null);
        Ok(match (mode.keyword(), endpoints) {
            (Some("at"), [time]) => PTemporal::At(endpoint(time)?),
            (Some("in"), [from, to]) => PTemporal::FromTo { from: endpoint(from)?, to: endpoint(to)? },
            (Some("from"), [from]) => PTemporal::FromTo { from: endpoint(from)?, to: null },
            (Some("to"), [to]) => PTemporal::FromTo { from: null, to: endpoint(to)? },
            (Some("between"), [from, to]) => PTemporal::Between { from: endpoint(from)?, to: endpoint(to)? },
            _ => return Err(invalid("Invalid scan period")),
        })
    }

    fn row_table(&mut self, options: &Edn, source: &Edn) -> Result<Relation> {
        let mut input = Bound::unit();
        let mut inferred = BTreeSet::new();
        let rows = sequence(source)?.iter().map(|row| {
            if row.symbol().is_some_and(|name| name.starts_with('?')) {
                let parameter = self.scalar(row, &mut input)?;
                inferred.extend(struct_names(&parameter.data_type()));
                return Ok(TableRow::Argument(parameter));
            }
            let mut columns = BTreeMap::new();
            for (name, cell) in row.map().ok_or_else(|| invalid("Table rows must be maps"))? {
                let name = name.symbol().or_else(|| name.keyword()).ok_or_else(|| invalid("Table field name must be a symbol or keyword"))?;
                if columns.insert(name, cell).is_some() { return Err(invalid("Duplicate logical table field")); }
            }
            inferred.extend(columns.keys().map(|name| (*name).to_owned()));
            Ok(TableRow::Literal(columns))
        }).collect::<Result<Vec<_>>>()?;
        let columns = output_names(options, inferred)?;
        let rows = rows.into_iter().map(|row| columns.iter().map(|column| match &row {
            TableRow::Literal(fields) => fields.get(column.as_str()).map(|form| self.scalar(form, &mut input))
                .transpose().map(|value| value.unwrap_or(PExpr::Literal(Value::Null))),
            TableRow::Argument(parameter) => Ok(get_field(parameter, column)),
        }).collect::<Result<Vec<_>>>()).collect::<Result<Vec<_>>>()?;
        let schema = self.row_schema(columns, std::slice::from_ref(&rows))?;
        Relation::new(schema, RelationNode::Values { rows })
    }

    fn literal_pages(&mut self, options: &Edn) -> Result<Relation> {
        let mut inferred = BTreeSet::new();
        let pages = sequence(required(options, "pages")?)?.iter().map(|page| {
            sequence(page)?.iter().map(|row| {
                let fields = row.map().ok_or_else(|| invalid("Literal page rows must be maps"))?;
                let row = fields.iter().map(|(name, value)| Ok((field_name(name)?.to_owned(), page_value(value)?)))
                    .collect::<Result<BTreeMap<_, _>>>()?;
                inferred.extend(row.keys().cloned());
                Ok(row)
            }).collect::<Result<Vec<_>>>()
        }).collect::<Result<Vec<_>>>()?;
        let columns = output_names(options, inferred)?;
        let pages = pages.into_iter().map(|page| page.into_iter().map(|mut row| {
            columns.iter().map(|column| PExpr::Literal(row.remove(column).unwrap_or(Value::Null))).collect()
        }).collect()).collect::<Vec<_>>();
        let schema = self.row_schema(columns, &pages)?;
        Relation::new(schema, RelationNode::LiteralPages { pages })
    }

    fn row_schema(&mut self, columns: Vec<String>, pages: &[Vec<Vec<PExpr>>]) -> Result<Schema> {
        let fields = columns.into_iter().enumerate().map(|(position, name)| {
            let types = pages.iter().flatten().map(|row| row[position].data_type()).collect::<Vec<_>>();
            let data_type = if types.is_empty() { LogicalType::Null } else { LogicalType::join_all(&types) };
            self.compiler.binder.field(name, None, data_type)
        }).collect::<Result<Vec<_>>>()?;
        Schema::new(fields)
    }

    fn column_table(&mut self, options: &Edn, column: &Edn) -> Result<Relation> {
        let Some([(name, form)]) = column.map() else { return Err(invalid("Table column must be a one-entry map")); };
        let name = column_name(name)?;
        let expr = self.scalar(form, &mut Bound::unit())?;
        let element = self.compiler.binder.field(name, None, list_element(&expr.data_type()))?;
        let input = Relation::new(Schema::new(vec![element.clone()])?, RelationNode::ColumnTable { expr, element: element.clone() })?;
        let Some(columns) = options.option("output-cols") else { return Ok(input); };
        let expressions = sequence(columns)?.iter().map(|column| {
            let column = column_name(column)?;
            if column == name { return Ok(NamedExpr { field: element.clone(), expr: element.expression() }); }
            Ok(NamedExpr { field: self.compiler.binder.field(column, None, LogicalType::Null)?, expr: PExpr::Literal(Value::Null) })
        }).collect::<Result<Vec<_>>>()?;
        Arc::new(input).project(expressions)
    }

    fn argument_table(&mut self, options: &Edn, parameter: &Edn) -> Result<Relation> {
        if !parameter.symbol().is_some_and(|name| name.starts_with('?')) { return Err(invalid("Table argument requires a parameter symbol")); }
        let expr = self.scalar(parameter, &mut Bound::unit())?;
        for ty in expr.data_type().legs() {
            let invalid_type = || error("illegal-param-type", "Table param must be of type struct list");
            match ty {
                LogicalType::Null | LogicalType::Nothing => (),
                LogicalType::List { kind: ListKind::List, element } => {
                    if element.legs().iter().any(|ty| !matches!(ty, LogicalType::Null | LogicalType::Nothing | LogicalType::Struct(_))) {
                        return Err(invalid_type());
                    }
                }
                _ => return Err(invalid_type()),
            }
        }
        let element_type = list_element(&expr.data_type());
        let columns = output_names(options, struct_names(&element_type))?;
        let element = self.compiler.binder.field("xt$document", None, element_type)?;
        let input = Arc::new(Relation::new(Schema::new(vec![element.clone()])?, RelationNode::ColumnTable { expr, element: element.clone() })?);
        let expressions = columns.iter().map(|name| {
            let expr = get_field(&element.expression(), name);
            Ok(NamedExpr { field: self.compiler.binder.field(name, None, expr.data_type())?, expr })
        }).collect::<Result<Vec<_>>>()?;
        input.project(expressions)
    }

    fn project(&mut self, options: &Edn, input: &Edn, is_map: bool) -> Result<Relation> {
        let input = self.lower(input)?;
        let append = match options.option("append-columns?").map(|form| &form.kind) {
            Some(EdnKind::Bool(value)) => *value || is_map,
            None => is_map,
            _ => return Err(invalid("Append flag must be boolean")),
        };
        let mut bound = bind_input(&input);
        let projections = sequence(required(options, "projections")?)?;
        let mut expressions = Vec::with_capacity(projections.len());
        for projection in projections {
            if let Some(name) = projection.symbol() {
                let field = bound.variables.get(name).ok_or_else(|| invalid(format!("Unknown logical column: {name}")))?;
                expressions.push(NamedExpr { field: field.clone(), expr: field.expression() });
                continue;
            }
            let Some([(name, form)]) = projection.map() else { return Err(invalid("Expected a projected column or a one-entry map")); };
            let name = column_name(name)?;
            let expr = self.scalar(form, &mut bound)?;
            let field = self.compiler.binder.field(name, None, expr.data_type())?;
            expressions.push(NamedExpr { field, expr });
        }
        if append { bound.relation.extend(expressions) } else { bound.relation.project(expressions) }
    }

    fn sort(&mut self, options: &Edn, input: &Edn) -> Result<Relation> {
        let input = self.lower(input)?;
        let mut bound = bind_input(&input);
        let mut order_by = Vec::new();
        for spec in options.option("order-specs").map(sequence).transpose()?.unwrap_or_default() {
            let (column, order) = match spec.vector().ok_or_else(|| invalid("Sort specification must be a vector"))? {
                [column] => (column, None),
                [column, order] if order.map().is_some() => (column, Some(order)),
                _ => return Err(invalid("Sort requires a column and optional ordering")),
            };
            let direction = order.and_then(|order| order.option("direction"));
            let descending = match direction.map(Edn::keyword) {
                Some(Some("desc")) => true,
                None | Some(Some("asc")) => false,
                _ => return Err(invalid("Expected :asc or :desc")),
            };
            let null_ordering = order.and_then(|order| order.option("null-ordering"));
            let nulls_first = match null_ordering.map(Edn::keyword) {
                Some(Some("nulls-first")) => true,
                None | Some(Some("nulls-last")) => false,
                _ => return Err(invalid("Expected :nulls-first or :nulls-last")),
            };
            let name = column_name(column)?;
            let expr = bound.variables.get(name).ok_or_else(|| invalid(format!("Unknown sort column: {name}")))?.expression();
            order_by.push(SortExpr { expr, descending, nulls_first });
        }
        let mut read_limit = |key| options.option(key).filter(|form| !matches!(form.kind, EdnKind::Nil)).map(|form| self.scalar(form, &mut bound)).transpose();
        let offset = read_limit("skip")?;
        let fetch = read_limit("limit")?;
        if order_by.is_empty() { return input.limit(offset, fetch); }
        Relation::new(input.schema().clone(), RelationNode::SortLimit { input, order_by, offset, fetch })
    }

    fn union_all(&mut self, left: &Edn, right: &Edn) -> Result<Relation> {
        let left = self.lower(left)?;
        let right = self.lower(right)?;
        let left_fields = left.schema().fields();
        let right_fields = right.schema().fields();
        let names = |fields: &[Field]| fields.iter().map(|field| field.name.clone()).collect::<BTreeSet<_>>();
        if names(left_fields) != names(right_fields) { return Err(error("union-incompatible-cols", "union incompatible cols")); }
        let mut schema = Vec::with_capacity(left_fields.len());
        let mut projections = Vec::with_capacity(left_fields.len());
        for field in left_fields {
            let right = right.schema().resolve(&[Identifier::quoted(&field.name)])?;
            let mut output = field.clone();
            output.data_type = output.data_type.join(&right.data_type);
            schema.push(output);
            projections.push(NamedExpr { field: right.clone(), expr: right.expression() });
        }
        let right = if projections.iter().map(|projection| &projection.field).eq(right_fields.iter()) { right }
            else { Arc::new(right.project(projections)?) };
        Relation::new(Schema::new(schema)?, RelationNode::Set { left, right, operator: SetOperator::Union, all: true })
    }
}

fn bind_input(relation: &Arc<Relation>) -> Bound {
    Bound { relation: Arc::clone(relation), variables: relation.schema().fields().iter().map(|field| (field.name.clone(), field.clone())).collect() }
}

fn output_names(options: &Edn, inferred: BTreeSet<String>) -> Result<Vec<String>> {
    match options.option("output-cols") {
        Some(columns) => sequence(columns)?.iter().map(|column| column_name(column).map(str::to_owned)).collect(),
        None => Ok(inferred.into_iter().collect()),
    }
}

fn struct_names(data_type: &LogicalType) -> BTreeSet<String> {
    data_type.legs().into_iter().filter_map(|ty| match ty { LogicalType::Struct(fields) => Some(fields.into_keys()), _ => None }).flatten().collect()
}

fn get_field(object: &PExpr, name: &str) -> PExpr {
    PExpr::Field { expr: Box::new(object.clone()), name: name.to_owned(), data_type: field_type(&object.data_type(), name) }
}

fn required<'a>(options: &'a Edn, name: &str) -> Result<&'a Edn> {
    options.option(name).ok_or_else(|| invalid(format!("Missing logical relation option: {name}")))
}

fn sequence(form: &Edn) -> Result<&[Edn]> {
    match &form.kind {
        EdnKind::Vector(values) | EdnKind::List(values) => Ok(values),
        _ => Err(invalid("Expected a sequence of logical relation forms")),
    }
}

fn column_name(form: &Edn) -> Result<&str> { form.symbol().ok_or_else(|| invalid("Expected a logical column symbol")) }

fn field_name(form: &Edn) -> Result<&str> {
    match &form.kind {
        EdnKind::Symbol(name) | EdnKind::Keyword(name) | EdnKind::String(name) => Ok(name),
        _ => Err(invalid("Expected a literal field name")),
    }
}

fn page_value(form: &Edn) -> Result<Value> {
    Ok(match &form.kind {
        EdnKind::List(forms) | EdnKind::Vector(forms) => Value::List(forms.iter().map(page_value).collect::<Result<_>>()?),
        EdnKind::Set(forms) => Value::Set(forms.iter().map(page_value).collect::<Result<_>>()?),
        EdnKind::Map(fields) => Value::Struct(fields.iter().map(|(name, value)| Ok((normal(field_name(name)?), page_value(value)?))).collect::<Result<_>>()?),
        _ => crate::literal::value(form)?,
    })
}

fn invalid(message: impl Into<String>) -> rtdb_types::Anomaly { error("rtdb.ra/invalid-relation", message) }
