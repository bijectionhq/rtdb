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
            ("select" | "σ" | "sigma", [input]) => {
                let input = self.lower(input)?;
                let predicate = self.scalar(required(options, "predicate")?, &mut bind_input(&input))?;
                input.filter(predicate)?
            }
            ("project" | "π" | "pi" | "map" | "ⲭ" | "chi", [input]) => self.project(options, input, matches!(op, "map" | "ⲭ" | "chi"))?,
            ("sort", [input]) => self.sort(options, input)?,
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
        let source = required(options, "rows")?;
        let rows = sequence(source)?.iter().map(|row| {
            let mut columns = BTreeMap::new();
            for (name, cell) in row.map().ok_or_else(|| invalid("Table rows must be maps"))? {
                let name = name.symbol().or_else(|| name.keyword()).ok_or_else(|| invalid("Table field name must be a symbol or keyword"))?;
                if columns.insert(name, cell).is_some() { return Err(invalid("Duplicate logical table field")); }
            }
            Ok(columns)
        }).collect::<Result<Vec<_>>>()?;
        let columns = match options.option("output-cols") {
            Some(columns) => sequence(columns)?.iter().map(column_name).collect::<Result<Vec<_>>>()?,
            None => rows.iter().flat_map(|row| row.keys().copied()).collect::<BTreeSet<_>>().into_iter().collect(),
        };
        let mut input = Bound::unit();
        let rows = rows.into_iter().map(|row| columns.iter().map(|column| {
            row.get(*column).map(|form| self.scalar(form, &mut input)).transpose().map(|value| value.unwrap_or(PExpr::Literal(Value::Null)))
        }).collect::<Result<Vec<_>>>()).collect::<Result<Vec<_>>>()?;
        let fields = columns.into_iter().enumerate().map(|(position, name)| {
            let types = rows.iter().map(|row| row[position].data_type()).collect::<Vec<_>>();
            let data_type = if types.is_empty() { LogicalType::Null } else { LogicalType::join_all(&types) };
            self.compiler.binder.field(name, None, data_type)
        }).collect::<Result<Vec<_>>>()?;
        Relation::new(Schema::new(fields)?, RelationNode::Values { rows })
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
}

fn bind_input(relation: &Arc<Relation>) -> Bound {
    Bound { relation: Arc::clone(relation), variables: relation.schema().fields().iter().map(|field| (field.name.clone(), field.clone())).collect() }
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

fn invalid(message: impl Into<String>) -> rtdb_types::Anomaly { error("rtdb.ra/invalid-relation", message) }
