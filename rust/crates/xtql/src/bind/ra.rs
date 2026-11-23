use rtdb_types::StructFields;
use super::*;
use crate::{Edn, EdnKind};
use rtdb_plan::{ExplainMode, MaterializedBinding, PercentileFraction, PercentileKind, WindowExpr, WindowFunction};

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
    let relations = BTreeMap::new();
    let importer = Importer { compiler: &mut compiler, parameters: &parameters, relations: &relations, memory };
    let statement = match form.vector() {
        Some([op, options, relation]) if op.keyword() == Some("explain") => {
            let mode = match options.option("mode").and_then(Edn::keyword) {
                Some("plan") => ExplainMode::Plan,
                Some("analyze") => ExplainMode::Analyze,
                _ => return Err(invalid("Logical explain requires :mode :plan or :mode :analyze")),
            };
            Statement::Explain { relation: importer.relation(relation)?, mode }
        }
        _ => Statement::Query(importer.relation(&form)?),
    };
    Ok(PreparedStatement { statement, parameter_types: binder.parameter_types().clone(), source: source.into(), notices: Vec::new(), settings: Default::default() })
}

struct Importer<'a, 'b, 'c> {
    compiler: &'a mut Compiler<'b, 'c>,
    parameters: &'a Parameters,
    relations: &'a BTreeMap<String, Arc<MaterializedBinding>>,
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
            ("list", []) => self.list(options)?,
            ("scan", []) => self.scan(options)?,
            ("let", [bound, body]) => self.let_relation(options, bound, body)?,
            ("relation", []) => self.reference(options)?,
            ("literal-pages", []) => self.literal_pages(options)?,
            ("select" | "σ" | "sigma", [input]) => {
                let input = self.lower(input)?;
                let predicate = self.scalar(required(options, "predicate")?, &mut bind_input(&input))?;
                input.filter(predicate)?
            }
            ("project" | "π" | "pi" | "map" | "ⲭ" | "chi", [input]) => self.project(options, input, matches!(op, "map" | "ⲭ" | "chi"))?,
            ("rename" | "ρ" | "rho", [input]) => self.rename(options, input)?,
            ("sort", [input]) => return self.sort(options, input),
            ("group-by" | "γ" | "gamma", [input]) => self.group_by(options, input)?,
            ("window", [input]) => return self.window(options, input),
            ("union-all" | "∪", [left, right]) => self.set(SetOperator::Union, left, right)?,
            ("intersect" | "∩", [left, right]) => self.set(SetOperator::Intersect, left, right)?,
            ("difference" | "except" | "−", [left, right]) => self.set(SetOperator::Except, left, right)?,
            ("cross-join" | "⨯", [left, right]) => self.join(JoinKind::Cross, options, left, right)?,
            ("join" | "⋈", [left, right]) => self.join(JoinKind::Inner, options, left, right)?,
            ("left-outer-join" | "⟕", [left, right]) => self.join(JoinKind::Left, options, left, right)?,
            ("full-outer-join" | "⟗", [left, right]) => self.join(JoinKind::Full, options, left, right)?,
            ("semi-join" | "⋉", [left, right]) => self.join(JoinKind::Semi, options, left, right)?,
            ("anti-join" | "▷", [left, right]) => self.join(JoinKind::Anti, options, left, right)?,
            ("single-join", [left, right]) => self.join(JoinKind::Single, options, left, right)?,
            ("mega-join", [relations]) => self.mega_join(options, relations)?,
            ("mark-join", [left, right]) => self.mark(options, left, right)?,
            ("apply", [input, dependent]) => self.apply(options, input, dependent)?,
            ("unnest" | "ω", [input]) => self.unnest(options, input)?,
            ("patch-gaps", [input]) => self.patch_gaps(options, input)?,
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

    fn list(&mut self, options: &Edn) -> Result<Relation> {
        let Some([(name, form)]) = required(options, "columns")?.map() else { return Err(invalid("List requires one output column")); };
        let name = column_name(name)?;
        let expr = self.scalar(form, &mut Bound::unit())?;
        let element = self.compiler.binder.field(name, None, list_element(&expr.data_type()))?;
        let page_rows = options.option("batch-size").map(|form| {
            let EdnKind::Integer(rows) = form.kind else { return Err(invalid("List batch size must be a positive integer")); };
            usize::try_from(rows).ok().and_then(std::num::NonZeroUsize::new)
                .ok_or_else(|| invalid("List batch size must be a positive integer"))
        }).transpose()?;
        let relation = Relation::new(Schema::new(vec![element.clone()])?, RelationNode::List { expr, element: element.clone(), page_rows })?;
        let Some(columns) = options.option("col-names") else { return Ok(relation); };
        let expressions = sequence(columns)?.iter().map(|column| {
            let name = column_name(column)?;
            if name == element.name { return Ok(NamedExpr { field: element.clone(), expr: element.expression() }); }
            let field = self.compiler.binder.field(name, None, LogicalType::Null)?;
            Ok(NamedExpr { field, expr: PExpr::Literal(Value::Null) })
        }).collect::<Result<Vec<_>>>()?;
        Arc::new(relation).project(expressions)
    }

    fn unnest(&mut self, options: &Edn, input: &Edn) -> Result<Relation> {
        let input = self.lower(input)?;
        let Some([(name, source)]) = required(options, "columns")?.map() else { return Err(invalid("Unnest requires one source and destination column")); };
        let name = column_name(name)?;
        let source = column_name(source)?;
        let expr = input.schema().fields().iter().find(|field| field.name == source)
            .ok_or_else(|| invalid(format!("Unknown unnest column: {source}")))?.expression();
        let element = self.compiler.binder.field(name, None, rtdb_plan::unnest_element_type(&expr.data_type()))?;
        let ordinality = options.option("ordinality-column").map(|column| {
            self.compiler.binder.field(column_name(column)?, None, LogicalType::Scalar(ScalarType::I32))
        }).transpose()?;
        let schema = Schema::new(input.schema().fields().iter().cloned().chain([element.clone()]).chain(ordinality.iter().cloned()).collect())?;
        Relation::new(schema, RelationNode::Unnest { input, expr, element, ordinality, outer: false })
    }

    fn patch_gaps(&mut self, options: &Edn, input: &Edn) -> Result<Relation> {
        let input = self.lower(input)?;
        let field = |name| input.schema().fields().iter().find(|field| field.name == name)
            .map(|field| field.id).ok_or_else(|| invalid(format!("Missing patch gap column: {name}")));
        let (iid, valid_from, valid_to, document) = (field("_iid")?, field("_valid_from")?, field("_valid_to")?, field("doc")?);
        let from = self.scalar(required(options, "valid-from")?, &mut Bound::unit())?;
        let to = self.scalar(required(options, "valid-to")?, &mut Bound::unit())?;
        let schema = Schema::new(input.schema().fields().iter().cloned().map(|mut field| {
            if field.id == document { field.data_type = field.data_type.maybe(); }
            field
        }).collect())?;
        Relation::new(schema, RelationNode::PatchGaps { input, iid, valid_from, valid_to, document, from, to })
    }

    fn scan(&mut self, options: &Edn) -> Result<Relation> {
        let EdnKind::String(database) = &required(options, "db-name")?.kind else { return Err(invalid("Scan database name must be a string")); };
        if !self.compiler.catalog.databases().contains(database) {
            return Err(error("xtdb/unknown-db", format!("Unknown database: {database}")).with_context("db-name", database.clone().into()));
        }
        let name = QualifiedTable { database: database.clone(), table: crate::ast::table_ref(required(options, "table")?, true)? };
        let table = self.compiler.catalog.table(&name)?;
        let missing = table.is_none();
        let table = table.unwrap_or(TableBinding { name, oid: None, columns: StructFields::new() });
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
        Relation::scan(Scan { table, columns, valid_time, system_time, clamp_valid_time, iids: None, predicates, top_k: None, ordered_top_k: None, matches: Vec::new() })
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
        let declared = options.option("types").map(|types| {
            types.map().ok_or_else(|| invalid("Literal page types must be a map"))?.iter().map(|(name, ty)| {
                Ok((column_name(name)?.to_owned(), crate::cast::data_type(ty)?))
            }).collect::<Result<BTreeMap<_, _>>>()
        }).transpose()?;
        let columns = output_names(options, declared.as_ref().map_or(inferred, |types| types.keys().cloned().collect()))?;
        let pages = pages.into_iter().map(|page| page.into_iter().map(|mut row| {
            columns.iter().map(|column| PExpr::Literal(row.remove(column).unwrap_or(Value::Null))).collect()
        }).collect()).collect::<Vec<_>>();
        let schema = match declared {
            Some(types) => Schema::new(columns.into_iter().map(|name| {
                let ty = types.get(&name).cloned().unwrap_or(LogicalType::Null);
                self.compiler.binder.field(name, None, ty)
            }).collect::<Result<_>>()?)?,
            None => self.row_schema(columns, &pages)?,
        };
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
        let mut ordinalities = BTreeMap::<rtdb_plan::OrdinalityScope, Field>::new();
        for projection in projections {
            if let Some(name) = projection.symbol() {
                let field = bound.variables.get(name).ok_or_else(|| invalid(format!("Unknown logical column: {name}")))?;
                expressions.push(NamedExpr { field: field.clone(), expr: field.expression() });
                continue;
            }
            let Some([(name, form)]) = projection.map() else { return Err(invalid("Expected a projected column or a one-entry map")); };
            let name = column_name(name)?;
            let expr = if let Some(scope) = ordinality_scope(form) {
                let field = match ordinalities.entry(scope) {
                    std::collections::btree_map::Entry::Occupied(field) => field.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let field = self.compiler.binder.field("xt$ordinality", None, LogicalType::Scalar(ScalarType::I64))?;
                        bound.relation = Arc::new(Arc::clone(&bound.relation).with_ordinality(field.clone(), scope)?);
                        entry.insert(field)
                    }
                };
                field.expression()
            } else if form.symbol() == Some("*") {
                PExpr::Struct(input.schema().fields().iter().map(|field| (field.name.clone(), field.expression())).collect())
            } else { self.scalar(form, &mut bound)? };
            let field = self.compiler.binder.field(name, None, expr.data_type())?;
            expressions.push(NamedExpr { field, expr });
        }
        if append {
            if ordinalities.is_empty() { return bound.relation.extend(expressions); }
            expressions = input.schema().fields().iter()
                .map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
                .chain(expressions).collect();
        }
        bound.relation.project(expressions)
    }

    fn rename(&mut self, options: &Edn, input: &Edn) -> Result<Relation> {
        let input = self.lower(input)?;
        let prefix = options.option("prefix").map(column_name).transpose()?;
        let aliases = options.option("columns").map(|columns| {
            columns.map().ok_or_else(|| invalid("Rename columns must be a map"))?.iter().map(|(from, to)| {
                Ok((column_name(from)?, column_name(to)?))
            }).collect::<Result<BTreeMap<_, _>>>()
        }).transpose()?.unwrap_or_default();
        let expressions = input.schema().fields().iter().map(|field| {
            let name = aliases.get(field.name.as_str()).copied().unwrap_or(&field.name);
            let name = match prefix {
                Some(prefix) => format!("{}/{}", symbol_name(prefix), symbol_name(name)),
                None => name.to_owned(),
            };
            Ok(NamedExpr { field: self.compiler.binder.field(name, None, field.data_type.clone())?, expr: field.expression() })
        }).collect::<Result<Vec<_>>>()?;
        input.project(expressions)
    }

    fn sort(&mut self, options: &Edn, input: &Edn) -> Result<Arc<Relation>> {
        let input = self.lower(input)?;
        let bound = bind_input(&input);
        let order_by = Self::sort_specs(options, |column| {
            let name = column_name(column)?;
            Ok(bound.variables.get(name).map_or(PExpr::Literal(Value::Null), Field::expression))
        })?;
        let offset = self.row_limit(options.option("skip"))?;
        let fetch = self.row_limit(options.option("limit"))?;
        if order_by.is_empty() && offset.is_none() && fetch.is_none() { return Ok(input); }
        let relation = if order_by.is_empty() { input.limit(offset, fetch)? }
            else { Relation::new(input.schema().clone(), RelationNode::SortLimit { input, order_by, offset, fetch })? };
        Ok(Arc::new(relation))
    }

    fn row_limit(&mut self, form: Option<&Edn>) -> Result<Option<PExpr>> {
        let Some(form) = form.filter(|form| !matches!(form.kind, EdnKind::Nil)) else { return Ok(None); };
        if let Some(name) = form.symbol().filter(|name| name.starts_with('?')) {
            return self.numeric_parameter(name).map(Some);
        }
        self.scalar(form, &mut Bound::unit()).map(Some)
    }

    fn numeric_parameter(&self, name: &str) -> Result<PExpr> {
        self.parameters.get(&self.compiler.symbols.parameter(name)).cloned()
            .ok_or_else(|| error("xtdb/expected-number", "Expected: number, got: null").with_context("param", name.into()))
    }

    fn sort_specs(options: &Edn, mut expression: impl FnMut(&Edn) -> Result<PExpr>) -> Result<Vec<SortExpr>> {
        options.option("order-specs").map(sequence).transpose()?.unwrap_or_default().iter()
            .map(|spec| Self::sort_spec(spec, &mut expression)).collect()
    }

    fn sort_spec(spec: &Edn, expression: impl FnOnce(&Edn) -> Result<PExpr>) -> Result<SortExpr> {
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
        Ok(SortExpr { expr: expression(column)?, descending, nulls_first })
    }

    fn group_by(&mut self, options: &Edn, input: &Edn) -> Result<Relation> {
        let input = self.lower(input)?;
        let mut bound = bind_input(&input);
        let columns = sequence(required(options, "columns")?)?;
        if columns.is_empty() { return Err(invalid("Group by requires at least one output column")); }
        let mut groups = Vec::new();
        let mut calls = Vec::new();
        for column in columns {
            if let Some([(name, call)]) = column.map() {
                calls.push((column_name(name)?, call));
            } else {
                let field = self.column_or_null(column, &mut bound)?;
                groups.push(NamedExpr { expr: field.expression(), field });
            }
        }
        let aggregates = calls.into_iter().map(|(name, call)| self.aggregate(name, call, &mut bound, !groups.is_empty()))
            .collect::<Result<Vec<_>>>()?;
        let schema = Schema::new(groups.iter().map(|group| group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect())?;
        Relation::new(schema, RelationNode::Aggregate { input: bound.relation, groups, aggregates })
    }

    fn window(&mut self, options: &Edn, input: &Edn) -> Result<Arc<Relation>> {
        let mut input = self.lower(input)?;
        let scope = bind_input(&input);
        let windows = required(options, "windows")?.map().ok_or_else(|| invalid("Named windows must be a map"))?;
        let mut grouped = BTreeMap::<&str, Vec<WindowExpr>>::new();
        for projection in sequence(required(options, "projections")?)? {
            let Some([(column, spec)]) = projection.map() else { return Err(invalid("Window projection requires one output column")); };
            let window = required(spec, "window-name")?.symbol().ok_or_else(|| invalid("Window name must be a symbol"))?;
            let function = Self::window_function(required(spec, "window-agg")?, &scope)?;
            let field = self.compiler.binder.field(column_name(column)?, None, function.result_type(input.schema())?)?;
            grouped.entry(window).or_default().push(WindowExpr { field, function });
        }
        if grouped.is_empty() { return Err(invalid("Window requires at least one projection")); }
        let mut named = BTreeSet::new();
        for (window, spec) in windows {
            let window = window.symbol().ok_or_else(|| invalid("Window name must be a symbol"))?;
            if !named.insert(window) { return Err(invalid("Duplicate window name")); }
            if spec.map().is_none() { return Err(invalid("Window specification must be a map")); }
            let Some(expressions) = grouped.remove(window) else { continue; };
            let partition_by = spec.option("partition-cols").map(sequence).transpose()?.unwrap_or_default().iter().map(|column| {
                let name = column_name(column)?;
                scope.variables.get(name).map(|field|field.id).ok_or_else(|| invalid(format!("Unknown partition column: {name}")))
            }).collect::<Result<_>>()?;
            let order_by = Self::sort_specs(spec, |column| {
                let name = column_name(column)?;
                Ok(scope.variables.get(name).map_or(PExpr::Literal(Value::Null), Field::expression))
            })?;
            let schema = Schema::new(input.schema().fields().iter().cloned().chain(expressions.iter().map(|expr|expr.field.clone())).collect())?;
            input = Arc::new(Relation::new(schema, RelationNode::Window { input, partition_by, order_by, expressions })?);
        }
        if let Some(name) = grouped.keys().next() { return Err(invalid(format!("Unknown window: {name}"))); }
        Ok(input)
    }

    fn window_function(form: &Edn, scope: &Bound) -> Result<WindowFunction> {
        let EdnKind::List(call) = &form.kind else { return Err(invalid("Window function must be a list")); };
        let [function, arguments @ ..] = call.as_slice() else { return Err(invalid("Window requires a function name")); };
        let name = function.symbol().ok_or_else(|| invalid("Window function must be a symbol"))?;
        match (normal(name).as_str(), arguments) {
            ("row_number", []) => Ok(WindowFunction::RowNumber),
            (kind @ ("lead" | "lag"), [column, offset @ ..]) => {
                let name = column_name(column)?;
                let column = scope.variables.get(name).ok_or_else(|| invalid(format!("Unknown window column: {name}")))?.id;
                let offset = match offset {
                    [] => 1,
                    [Edn { kind: EdnKind::Integer(value), .. }] if *value >= 0 => *value as u64,
                    _ => return Err(invalid("Window offset must be a non-negative integer")),
                };
                Ok(if kind == "lead" { WindowFunction::Lead { column, offset } } else { WindowFunction::Lag { column, offset } })
            }
            _ => Err(invalid("Unsupported window function or arguments")),
        }
    }

    fn aggregate(&mut self, name: &str, form: &Edn, bound: &mut Bound, grouped: bool) -> Result<AggregateExpr> {
        let EdnKind::List(call) = &form.kind else { return Err(invalid("Aggregate must be a list")); };
        let [function, arguments @ ..] = call.as_slice() else {
            return Err(invalid("Aggregate requires a function name"));
        };
        let function = function.symbol().ok_or_else(|| invalid("Aggregate function must be a symbol"))?;
        let (arguments, options) = match arguments.split_last() {
            Some((options, arguments)) if options.map().is_some() => (arguments, Some(options)),
            _ => (arguments, None),
        };
        let spelling = normal(function);
        let (function, argument, distinct, order_by) = match spelling.as_str() {
            "percentile_cont" | "percentile_disc" => {
                let kind = if spelling == "percentile_cont" { PercentileKind::Continuous } else { PercentileKind::Discrete };
                let [fraction, spec] = arguments else { return Err(invalid("Percentile requires one fraction and one sort specification")); };
                let fraction = self.percentile_fraction(fraction)?;
                let sort = Self::sort_spec(spec, |column| Ok(self.column_or_null(column, bound)?.expression()))?;
                let function = AggregateFunction::Percentile { kind, fraction, descending: sort.descending };
                (function, Some(sort.expr), false, Vec::new())
            }
            _ => {
                let (function, distinct) = aggregate_function(function).ok_or_else(|| invalid(format!("Unknown aggregate function: {function}")))?;
                let argument = match (function.canonical(), arguments) {
                    (AggregateFunction::CountStar, []) => None,
                    (AggregateFunction::CountStar, _) => return Err(invalid("Row count takes no argument")),
                    (_, [column]) => Some(self.column_or_null(column, bound)?.expression()),
                    _ => return Err(invalid("Aggregate requires one input column")),
                };
                let order_by = options.map(|options| Self::sort_specs(options, |column| Ok(self.column_or_null(column, bound)?.expression())))
                    .transpose()?.unwrap_or_default();
                (function, argument, distinct, order_by)
            }
        };
        let input_type = argument.as_ref().map(PExpr::data_type).unwrap_or(LogicalType::Nothing);
        let field = self.compiler.binder.field(name, None, aggregate_type(&function, input_type, grouped)?)?;
        let filter = options.and_then(|options| options.option("filter")).map(|predicate| self.scalar(predicate, bound)).transpose()?;
        Ok(AggregateExpr { field, function, argument, distinct, filter, order_by })
    }

    fn percentile_fraction(&self, form: &Edn) -> Result<PercentileFraction> {
        match &form.kind {
            EdnKind::Integer(value) => Ok(PercentileFraction::Integer(*value)),
            EdnKind::Float(value) => Ok(PercentileFraction::Literal(*value)),
            EdnKind::Decimal(value) | EdnKind::BigInteger(value) => value.parse::<f64>().map(PercentileFraction::Literal)
                .map_err(|_| invalid("Percentile fraction must be a number")),
            EdnKind::Symbol(name) if name.starts_with('?') => match self.numeric_parameter(name)? {
                PExpr::Parameter { index, .. } => Ok(PercentileFraction::Parameter { index }),
                _ => Err(invalid("Percentile fraction requires a parameter position")),
            },
            _ => Err(invalid("Percentile fraction must be a number or parameter")),
        }
    }

    fn column_or_null(&mut self, column: &Edn, bound: &mut Bound) -> Result<Field> {
        let name = column_name(column)?;
        if let Some(field) = bound.variables.get(name) { return Ok(field.clone()); }
        let field = self.compiler.binder.field(name, None, LogicalType::Null)?;
        let expr = PExpr::Literal(Value::Null);
        bound.relation = Arc::new(Arc::clone(&bound.relation).extend(vec![NamedExpr { field: field.clone(), expr }])?);
        bound.variables.insert(name.to_owned(), field.clone());
        Ok(field)
    }

    fn join(&mut self, kind: JoinKind, options: &Edn, left: &Edn, right: &Edn) -> Result<Relation> {
        let left = self.lower(left)?;
        let right = self.lower(right)?;
        self.join_lowered(kind, options, left, right)
    }

    /// Joins every relation and applies every condition once all are in scope, which yields the same rows as the reference's join-order search.
    fn mega_join(&mut self, options: &Edn, relations: &Edn) -> Result<Relation> {
        let relations = relations.vector().ok_or_else(|| invalid("Mega-join relations must be a vector"))?;
        let Some((first, rest)) = relations.split_first() else { return Err(invalid("Mega-join requires at least one relation")); };
        let mut joined = self.lower(first)?;
        for relation in rest {
            let right = self.lower(relation)?;
            joined = Arc::new(self.join_lowered(JoinKind::Cross, options, joined, right)?);
        }
        let mut input = bind_input(&joined);
        let conditions = required(options, "conditions")?.vector().ok_or_else(|| invalid("Join conditions must be a vector"))?;
        let predicates = conditions.iter().map(|condition| match condition.map() {
            Some([(left, right)]) => Ok(PExpr::binary(BinaryOp::Eq, self.join_key(left, &mut input)?, self.join_key(right, &mut input)?)),
            _ => self.scalar(condition, &mut input),
        }).collect::<Result<Vec<_>>>()?;
        match and(predicates) {
            Some(predicate) => joined.filter(predicate),
            None => Ok(Arc::unwrap_or_clone(joined)),
        }
    }

    fn join_lowered(&mut self, kind: JoinKind, options: &Edn, left: Arc<Relation>, right: Arc<Relation>) -> Result<Relation> {
        let mut left_input = bind_input(&left);
        let mut right_input = bind_input(&right);
        let condition = if kind == JoinKind::Cross { None }
            else { self.join_conditions(options, &mut left_input, &mut right_input)? };
        let relation = Relation::join(left_input.relation, right_input.relation, kind, condition)?;
        let mut outputs: BTreeMap<&str, NamedExpr> = BTreeMap::new();
        for field in relation.schema().fields() {
            if left.schema().get(field.id).is_none() && right.schema().get(field.id).is_none() { continue; }
            let output = outputs.entry(&field.name).or_insert_with(|| NamedExpr { field: field.clone(), expr: field.expression() });
            if output.field.id == field.id { continue; }
            if !matches!(kind, JoinKind::Left | JoinKind::Single) { output.expr = field.expression(); }
            if kind == JoinKind::Cross { output.field = field.clone(); }
            else { output.field.data_type = output.field.data_type.join(&field.data_type); }
        }
        if outputs.len() == relation.schema().len() { return Ok(relation); }
        let outputs = outputs.into_values().collect();
        Arc::new(relation).project(outputs)
    }

    fn join_conditions(&mut self, options: &Edn, left: &mut Bound, right: &mut Bound) -> Result<Option<PExpr>> {
        let mut combined = join_scope(left, right);
        let conditions = required(options, "conditions")?.vector().ok_or_else(|| invalid("Join conditions must be a vector"))?;
        let predicates = conditions.iter().map(|condition| {
            if let Some([(left_key, right_key)]) = condition.map() {
                let left = self.join_key(left_key, left)?;
                let right = self.join_key(right_key, right)?;
                Ok(PExpr::binary(BinaryOp::Eq, left, right))
            } else { self.scalar(condition, &mut combined) }
        }).collect::<Result<Vec<_>>>()?;
        Ok(and(predicates))
    }

    fn join_key(&mut self, form: &Edn, input: &mut Bound) -> Result<PExpr> {
        if form.symbol().is_some_and(|name| !name.starts_with('?')) {
            return Ok(self.column_or_null(form, input)?.expression());
        }
        self.scalar(form, input)
    }

    fn mark(&mut self, options: &Edn, left: &Edn, right: &Edn) -> Result<Relation> {
        let left = self.lower(left)?;
        let mut probe = bind_input(&left);
        let mut candidates = bind_input(&self.lower(right)?);
        let Some([(name, comparisons)]) = required(options, "mark-spec")?.map() else {
            return Err(invalid("Mark specification must be a one-entry map"));
        };
        let mut combined = join_scope(&probe, &candidates);
        let comparisons = comparisons.vector().ok_or_else(|| invalid("Mark comparisons must be a vector"))?
            .iter().map(|comparison| {
                if comparison.map().is_some() { return Err(invalid("Mark comparisons must be scalar predicates")); }
                self.scalar(comparison, &mut combined)
            }).collect::<Result<Vec<_>>>()?;
        let comparison = and(comparisons).unwrap_or(PExpr::Literal(Value::Bool(true)));
        let condition = self.join_conditions(options, &mut probe, &mut candidates)?;
        let name = column_name(name)?;
        let output = self.compiler.binder.field(name, None, LogicalType::Scalar(ScalarType::Bool).maybe())?;
        let schema = probe.relation.schema().concat(&Schema::new(vec![output.clone()])?)?;
        let relation = Relation::new(schema, RelationNode::Mark {
            input: probe.relation, dependent: candidates.relation, arguments: Vec::new(), condition, comparison, output: output.clone(),
        })?;
        if relation.schema().len() == left.schema().len() + 1 && !left.schema().fields().iter().any(|field| field.name == name) {
            return Ok(relation);
        }
        let expressions = left.schema().fields().iter().filter(|field| field.name != name)
            .map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
            .chain([NamedExpr { field: output.clone(), expr: output.expression() }]).collect();
        Arc::new(relation).project(expressions)
    }

    fn apply(&mut self, options: &Edn, input: &Edn, dependent: &Edn) -> Result<Relation> {
        let input = self.lower(input)?;
        let scope = bind_input(&input);
        let mut parameters = self.parameters.clone();
        let mut arguments = Vec::new();
        let mut names = BTreeSet::new();
        for (column, parameter) in required(options, "columns")?.map().ok_or_else(|| invalid("Apply parameter columns must be a map"))? {
            let column = column_name(column)?;
            let field = scope.variables.get(column).ok_or_else(|| {
                error("xtdb.apply/missing-column", format!("Column missing from independent relation: {column}")).with_context("column", column.into())
            })?;
            let name = parameter.symbol().filter(|name| name.starts_with('?'))
                .ok_or_else(|| invalid("Apply parameter must be a parameter symbol"))?;
            let name = self.compiler.symbols.parameter(name);
            if !names.insert(name.clone()) { return Err(invalid("Apply parameter is bound more than once")); }
            let index = self.compiler.parameter_index()?;
            parameters.insert(name, PExpr::Parameter { index, data_type: field.data_type.clone() });
            arguments.push((index, field.expression()));
        }
        let mut child = Importer { compiler: self.compiler, parameters: &parameters, relations: self.relations, memory: self.memory };
        let mut dependent = child.lower(dependent)?;
        let kind = match required(options, "mode")?.keyword() {
            Some("cross-join") => ApplyKind::Inner,
            Some("left-outer-join") => ApplyKind::Left,
            Some("semi-join") => ApplyKind::Semi,
            Some("anti-join") => ApplyKind::Anti,
            Some("single-join") => ApplyKind::Single,
            Some("mark-join") => ApplyKind::Mark,
            _ => return Err(invalid("Unknown apply join mode")),
        };
        let output = if kind == ApplyKind::Mark {
            let Some([(name, comparison)]) = required(options, "mark-join-projection")?.map() else {
                return Err(invalid("Mark apply requires one projected comparison"));
            };
            let expr = child.scalar(comparison, &mut bind_input(&dependent))?;
            let projected = child.compiler.binder.field("xt$comparison", None, expr.data_type())?;
            dependent = Arc::new(dependent.project(vec![NamedExpr { field: projected, expr }])?);
            let output = child.compiler.binder.field(column_name(name)?, None, LogicalType::Scalar(ScalarType::Bool).maybe())?;
            Some(output)
        } else { None };
        let schema = kind.result_schema(input.schema(), dependent.schema(), output.as_ref())?;
        Relation::new(schema, RelationNode::Apply { input, dependent, arguments, kind, output })
    }

    fn let_relation(&mut self, options: &Edn, bound: &Edn, body: &Edn) -> Result<Relation> {
        let name = required(options, "binding-sym")?.symbol().ok_or_else(|| invalid("Let requires a binding symbol"))?;
        let binding = Arc::new(MaterializedBinding { name: name.to_owned(), relation: self.lower(bound)? });
        let mut relations = self.relations.clone();
        relations.insert(name.to_owned(), Arc::clone(&binding));
        let mut child = Importer { compiler: self.compiler, parameters: self.parameters, relations: &relations, memory: self.memory };
        let body = child.lower(body)?;
        Relation::new(body.schema().clone(), RelationNode::Let { binding, body })
    }

    fn reference(&mut self, options: &Edn) -> Result<Relation> {
        let name = required(options, "cte-id")?.symbol().ok_or_else(|| invalid("Relation reference requires a binding symbol"))?;
        let binding = self.relations.get(name).cloned().ok_or_else(|| {
            rtdb_types::Anomaly::fault("xtdb.operator.let/missing-relation", format!("Can't find relation '{name}'"))
                .with_context("cte-id", name.into())
        })?;
        let relation = Arc::new(Relation::new(binding.relation.schema().clone(), RelationNode::Reference { binding })?);
        let expressions = relation.schema().fields().iter().map(|field| {
            Ok(NamedExpr { field: self.compiler.binder.field(&field.name, None, field.data_type.clone())?, expr: field.expression() })
        }).collect::<Result<Vec<_>>>()?;
        relation.project(expressions)
    }

    fn set(&mut self, operator: SetOperator, left: &Edn, right: &Edn) -> Result<Relation> {
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
        Relation::new(Schema::new(schema)?, RelationNode::Set { left, right, operator, all: true })
    }
}

fn bind_input(relation: &Arc<Relation>) -> Bound {
    Bound { relation: Arc::clone(relation), variables: relation.schema().fields().iter().map(|field| (field.name.clone(), field.clone())).collect() }
}

fn ordinality_scope(form: &Edn) -> Option<rtdb_plan::OrdinalityScope> {
    let EdnKind::List(values) = &form.kind else { return None; };
    let [function] = values.as_slice() else { return None; };
    match function.symbol()? {
        "row-number" => Some(rtdb_plan::OrdinalityScope::Relation),
        "local-row-number" => Some(rtdb_plan::OrdinalityScope::Page),
        _ => None,
    }
}

fn join_scope(left: &Bound, right: &Bound) -> Bound {
    let variables = left.variables.iter().chain(&right.variables)
        .map(|(name, field)| (name.clone(), field.clone())).collect();
    Bound { variables, ..Bound::unit() }
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

fn symbol_name(symbol: &str) -> &str { symbol.split_once('/').map_or(symbol, |(_, name)| name) }

fn field_name(form: &Edn) -> Result<&str> {
    match &form.kind {
        EdnKind::Symbol(name) | EdnKind::Keyword(name) | EdnKind::String(name) => Ok(name),
        _ => Err(invalid("Expected a literal field name")),
    }
}

fn page_value(form: &Edn) -> Result<Value> {
    Ok(match &form.kind {
        EdnKind::List(forms) | EdnKind::Vector(forms) => {
            let mut values = rtdb_types::ListBuilder::with_capacity(forms.len());
            for form in forms { values.push(page_value(form)?); }
            Value::List(values.finish())
        }
        EdnKind::Set(forms) => Value::Set(forms.iter().map(page_value).collect::<Result<_>>()?),
        EdnKind::Map(fields) => Value::Struct(fields.iter().map(|(name, value)| Ok((normal(field_name(name)?), page_value(value)?))).collect::<Result<_>>()?),
        _ => crate::literal::value(form)?,
    })
}

fn invalid(message: impl Into<String>) -> rtdb_types::Anomaly { error("rtdb.ra/invalid-relation", message) }
