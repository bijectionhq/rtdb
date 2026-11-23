use std::sync::Arc;
use rtdb_plan::{Catalog, Expr, MaterializedBinding, RegexReplacement, Relation, RelationNode, Schema, TableBinding, TemporalFilter};
use rtdb_types::{Anomaly, MemoryBudget, Result};
use super::gap;

#[cfg(test)]
fn restore_dependencies(source: &Relation, catalog: &dyn Catalog) -> Result<Arc<Relation>> {
    restore_with(source, catalog, FormAllocation::Untracked)
}

pub(super) fn restore_dependencies_in(source: &Relation, catalog: &dyn Catalog, memory: &Arc<MemoryBudget>) -> Result<Arc<Relation>> {
    restore_with(source, catalog, FormAllocation::Tracked(memory))
}

fn restore_with(source: &Relation, catalog: &dyn Catalog, forms: FormAllocation<'_>) -> Result<Arc<Relation>> {
    let tables = ResolvedTables::resolve(source, catalog)?;
    Rebuilder { bindings: tables.bindings.into_iter(), forms }.rebuild(source)
}

#[derive(Clone, Copy)]
enum FormAllocation<'a> {
    Tracked(&'a Arc<MemoryBudget>),
    #[cfg(test)]
    Untracked,
}
impl FormAllocation<'_> {
    fn copy(self, recipe: &RegexReplacement) -> Result<RegexReplacement> {
        match self {
            Self::Tracked(memory) => recipe.copy_in(memory),
            #[cfg(test)]
            Self::Untracked => recipe.copy_untracked(),
        }
    }
}

struct ResolvedTables { bindings: Vec<TableBinding> }

impl ResolvedTables {
    fn resolve(source: &Relation, catalog: &dyn Catalog) -> Result<Self> {
        let mut bindings = Vec::new();
        collect(source, catalog, &mut bindings)?;
        Ok(Self { bindings })
    }
}

fn collect(source: &Relation, catalog: &dyn Catalog, bindings: &mut Vec<TableBinding>) -> Result<()> {
    match source.node() {
        RelationNode::Values { .. } | RelationNode::LiteralPages { .. } | RelationNode::ArrowLiteralPages { .. } | RelationNode::List { .. } | RelationNode::ColumnTable { .. } => {}
        RelationNode::Scan(scan) => bindings.push(catalog.table(&scan.table.name)?
            .ok_or_else(|| gap("Prepared table disappeared from its own snapshot"))?),
        RelationNode::Project { input, .. } | RelationNode::Extend { input, .. } | RelationNode::Rename { input, .. }
        | RelationNode::Filter { input, .. } | RelationNode::Aggregate { input, .. }
        | RelationNode::Window { input, .. } | RelationNode::Sort { input, .. }
        | RelationNode::SortLimit { input, .. } | RelationNode::Limit { input, .. }
        | RelationNode::Distinct { input } | RelationNode::Unnest { input, .. }
        | RelationNode::Ordinality { input, .. } | RelationNode::PatchGaps { input, .. } => collect(input, catalog, bindings)?,
        RelationNode::Let { binding, body } => { collect(&binding.relation, catalog, bindings)?; collect(body, catalog, bindings)?; }
        RelationNode::Reference { binding } => collect(&binding.relation, catalog, bindings)?,
        RelationNode::Join { left, right, .. } | RelationNode::Set { left, right, .. } => {
            collect(left, catalog, bindings)?;
            collect(right, catalog, bindings)?;
        }
        RelationNode::Apply { input, dependent, .. } | RelationNode::Mark { input, dependent, .. } => {
            collect(input, catalog, bindings)?;
            collect(dependent, catalog, bindings)?;
        }
    }
    Ok(())
}

struct Rebuilder<'a> { bindings: std::vec::IntoIter<TableBinding>, forms: FormAllocation<'a> }

impl Rebuilder<'_> {
    fn rebuild(mut self, source: &Relation) -> Result<Arc<Relation>> {
        let relation = self.relation(source)?;
        if self.bindings.next().is_some() { return Err(occurrence_error()); }
        Ok(relation)
    }

    fn next_table(&mut self) -> Result<TableBinding> { self.bindings.next().ok_or_else(occurrence_error) }

    fn binding(&mut self, source: &MaterializedBinding) -> Result<Arc<MaterializedBinding>> {
        let name = source.name.clone();
        let relation = self.relation(&source.relation)?;
        Ok(Arc::new(MaterializedBinding { name, relation }))
    }

    fn relation(&mut self, source: &Relation) -> Result<Arc<Relation>> {
        let schema = Schema::new(source.schema().fields().to_vec()).map_err(projection_error)?;
        let mut node = source.node().clone();
        match &mut node {
            RelationNode::Values { rows } => self.expressions(rows.iter_mut().flatten())?,
            RelationNode::LiteralPages { pages } => self.expressions(pages.iter_mut().flatten().flatten())?,
            RelationNode::ArrowLiteralPages { .. } => {},
            RelationNode::List { expr, .. } | RelationNode::ColumnTable { expr, .. } => self.expression(expr)?,
            RelationNode::Scan(scan) => {
                scan.table = self.next_table()?;
                self.temporal(&mut scan.valid_time)?;
                self.temporal(&mut scan.system_time)?;
                self.expressions(&mut scan.predicates)?;
            }
            RelationNode::Project { input, expressions } | RelationNode::Extend { input, expressions } | RelationNode::Rename { input, expressions, .. } => {
                *input = self.relation(input)?;
                self.expressions(expressions.iter_mut().map(|expression| &mut expression.expr))?;
            }
            RelationNode::Filter { input, predicate } => { *input = self.relation(input)?; self.expression(predicate)?; }
            RelationNode::Aggregate { input, groups, aggregates } => {
                *input = self.relation(input)?;
                self.expressions(groups.iter_mut().map(|group| &mut group.expr))?;
                for aggregate in aggregates {
                    self.expressions(aggregate.argument.iter_mut())?;
                    self.expressions(aggregate.filter.iter_mut())?;
                    self.expressions(aggregate.order_by.iter_mut().map(|order| &mut order.expr))?;
                }
            }
            RelationNode::Window { input, order_by, .. } | RelationNode::Sort { input, order_by } => {
                *input = self.relation(input)?;
                self.expressions(order_by.iter_mut().map(|order| &mut order.expr))?;
            }
            RelationNode::SortLimit { input, order_by, offset, fetch } => {
                *input = self.relation(input)?;
                self.expressions(order_by.iter_mut().map(|order| &mut order.expr))?;
                self.expressions(offset.iter_mut())?; self.expressions(fetch.iter_mut())?;
            }
            RelationNode::Limit { input, offset, fetch } => { *input = self.relation(input)?; self.expressions(offset.iter_mut())?; self.expressions(fetch.iter_mut())?; }
            RelationNode::Distinct { input } | RelationNode::Ordinality { input, .. } => *input = self.relation(input)?,
            RelationNode::Unnest { input, expr, .. } => { *input = self.relation(input)?; self.expression(expr)?; }
            RelationNode::PatchGaps { input, from, to, .. } => { *input = self.relation(input)?; self.expression(from)?; self.expression(to)?; }
            RelationNode::Let { binding, body } => { *binding = self.binding(binding)?; *body = self.relation(body)?; }
            RelationNode::Reference { binding } => *binding = self.binding(binding)?,
            RelationNode::Join { left, right, condition, .. } => {
                *left = self.relation(left)?;
                *right = self.relation(right)?;
                self.expressions(condition.iter_mut())?;
            }
            RelationNode::Set { left, right, .. } => { *left = self.relation(left)?; *right = self.relation(right)?; }
            RelationNode::Apply { input, dependent, arguments, .. } => {
                *input = self.relation(input)?;
                *dependent = self.relation(dependent)?;
                self.expressions(arguments.iter_mut().map(|(_, expression)| expression))?;
            }
            RelationNode::Mark { input, dependent, arguments, condition, comparison, .. } => {
                *input = self.relation(input)?;
                *dependent = self.relation(dependent)?;
                self.expressions(arguments.iter_mut().map(|(_, expression)| expression))?;
                self.expressions(condition.iter_mut())?;
                self.expression(comparison)?;
            }
        }
        Ok(Arc::new(Relation::new(schema, node).map_err(projection_error)?))
    }

    fn temporal(&mut self, filter: &mut TemporalFilter) -> Result<()> {
        match filter {
            TemporalFilter::Default | TemporalFilter::All => (),
            TemporalFilter::At(at) => self.expression(at)?,
            TemporalFilter::FromTo { from, to } | TemporalFilter::Between { from, to } => { self.expression(from)?; self.expression(to)?; }
        }
        Ok(())
    }
    fn expressions<'a>(&mut self, expressions: impl IntoIterator<Item = &'a mut Expr>) -> Result<()> {
        for expression in expressions { self.expression(expression)?; }
        Ok(())
    }
    fn expression(&mut self, expression: &mut Expr) -> Result<()> {
        match expression {
            Expr::Literal(_) | Expr::DurationLiteral(_) | Expr::Column { .. } | Expr::Parameter { .. } | Expr::Correlated { .. } | Expr::RowCopy(_) => (),
            Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::Field { expr, .. } => self.expression(expr)?,
            Expr::Binary { left, right, .. } => { self.expression(left)?; self.expression(right)?; }
            Expr::Call { args, .. } | Expr::Coalesce(args) | Expr::List(args) => self.expressions(args)?,
            Expr::RegexReplace { source, recipe, .. } => { self.expression(source)?; *recipe = self.forms.copy(recipe)?; }
            Expr::Case { branches, otherwise } => { for (condition, value) in branches { self.expression(condition)?; self.expression(value)?; } self.expression(otherwise)?; }
            Expr::SimpleCase { operand, branches, otherwise } => { self.expression(operand)?; for (condition, value) in branches { self.expression(condition)?; self.expression(value)?; } self.expression(otherwise)?; }
            Expr::Between { operand, low, high, .. } => { self.expression(operand)?; self.expression(low)?; self.expression(high)?; }
            Expr::Subscript { expr, index, .. } => { self.expression(expr)?; self.expression(index)?; }
            Expr::Struct(fields) => self.expressions(fields.iter_mut().map(|(_, expression)| expression))?,
            Expr::In { expr, values, .. } => { self.expression(expr)?; self.expressions(values)?; }
            Expr::Like { expr, pattern, .. } => { self.expression(expr)?; self.expression(pattern)?; }
        }
        Ok(())
    }
}

fn projection_error(error: Anomaly) -> Anomaly {
    gap(&<serde_json::Error as serde::de::Error>::custom(error).to_string())
}

fn occurrence_error() -> Anomaly { gap("Typed dependency restoration lost its scan occurrence order") }

#[cfg(test)]
mod tests {
    use rtdb_types::StructFields;
    use super::*;
    use std::{cell::RefCell, collections::{BTreeMap, BTreeSet, VecDeque}};
    use rtdb_plan::{AggregateExpr, AggregateFunction, AggregateName, ApplyKind, BinaryOp, CastOption, CastTarget,
        CastTimezone, ColumnId, ColumnQualifier, DeferredCast, Expr, Field, JoinKind, NamedExpr, PreparedStatement,
        PercentileFraction, PercentileKind, QualifiedTable, RegexForm as F, RegexFormBuilder, RegexFormKind, RegexLiteralKind, RegexReplacement, Scan, SetOperator, SortExpr, Statement, TemporalFilter,
        TemporalKind, WindowExpr, WindowFunction};
    use rtdb_query::PreparedQuery;
    use rtdb_types::{AnomalyCategory, CalendarDate, CivilDate, DateUnit, DecimalValue, Keyword, LogicalType as L,
        MemoryBudget, ScalarType as S, TableRef, TimeUnit, Value as V};
    use serde_json::{Value as Json, json};

    fn raw_build(emit: impl FnOnce(&mut RegexFormBuilder<'_>) -> Result<()>) -> F { let mut builder = RegexFormBuilder::new_untracked(); emit(&mut builder).unwrap(); builder.finish().unwrap() }
    fn raw_literal(value: V) -> F { raw_build(|builder| builder.literal(&value)) }
    fn raw_character(value: char) -> F { raw_build(|builder| builder.character(value)) }
    fn raw_symbol(value: String) -> F { raw_build(|builder| builder.symbol(&value)) }
    fn raw_big_integer(value: String) -> F { raw_build(|builder| builder.big_integer(&value)) }
    fn raw_decimal(value: String) -> F { raw_build(|builder| builder.decimal(&value)) }
    fn raw_list(values: Vec<F>) -> F { raw_build(|builder| { builder.begin_list()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
    fn raw_vector(values: Vec<F>) -> F { raw_build(|builder| { builder.begin_vector()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
    fn raw_set(values: Vec<F>) -> F { raw_build(|builder| { builder.begin_set()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
    fn raw_map(values: Vec<(F,F)>) -> F { raw_build(|builder| { builder.begin_map()?; for (key,value) in values { builder.append_view(key.view())?; builder.append_view(value.view())?; } builder.end() }) }
    fn raw_tagged(tag: String, value: F) -> F { raw_build(|builder| { builder.begin_tagged(&tag)?; builder.append_view(value.view())?; builder.end() }) }
    fn raw_anonymous(value: F) -> F { raw_build(|builder| { builder.begin_anonymous()?; builder.append_view(value.view())?; builder.end() }) }

    #[derive(Default)]
    struct RecordingCatalog {
        calls: RefCell<Vec<String>>,
        outcomes: RefCell<VecDeque<Result<Option<TableBinding>>>>,
    }

    impl RecordingCatalog {
        fn with(outcomes: Vec<Result<Option<TableBinding>>>) -> Self {
            Self { calls: RefCell::new(Vec::new()), outcomes: RefCell::new(outcomes.into()) }
        }
        fn calls(&self) -> Vec<String> { self.calls.borrow().clone() }
    }

    impl Catalog for RecordingCatalog {
        fn default_database(&self) -> &str { "db" }
        fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["db".into()]) }
        fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
            self.calls.borrow_mut().push(name.table.table.clone());
            self.outcomes.borrow_mut().pop_front().unwrap_or_else(|| Ok(Some(table(&name.table.table))))
        }
        fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
    }

    fn field(id: u32, name: &str, data_type: L) -> Field {
        Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type }
    }
    fn table(name: &str) -> TableBinding {
        TableBinding { name: QualifiedTable { database: "db".into(), table: TableRef::new("public", name) }, oid: Some(99),
            columns: StructFields::from([("x".into(), L::Scalar(S::I64)), ("_id".into(), L::Scalar(S::Uuid))]) }
    }
    fn scan(name: &str, id: u32) -> Arc<Relation> {
        let mut binding = table(name);
        binding.oid = Some(1);
        binding.columns.remove("_id");
        Arc::new(Relation::scan(Scan { table: binding, columns: vec![field(id, "x", L::Scalar(S::I64))],
            valid_time: TemporalFilter::All, system_time: TemporalFilter::Default, clamp_valid_time: true,
            iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new() }).unwrap())
    }
    fn relation(schema: Schema, node: RelationNode) -> Arc<Relation> { Arc::new(Relation::new(schema, node).unwrap()) }
    fn cross(left: Arc<Relation>, right: Arc<Relation>) -> Arc<Relation> {
        relation(left.schema().concat(right.schema()).unwrap(), RelationNode::Join { left, right, kind: JoinKind::Cross, condition: None })
    }
    fn reference(binding: &Arc<MaterializedBinding>) -> Arc<Relation> {
        relation(binding.relation.schema().clone(), RelationNode::Reference { binding: Arc::clone(binding) })
    }
    fn binding(name: &str, relation: Arc<Relation>) -> Arc<MaterializedBinding> { Arc::new(MaterializedBinding { name: name.into(), relation }) }
    fn let_relation(binding: Arc<MaterializedBinding>, body: Arc<Relation>) -> Arc<Relation> {
        relation(body.schema().clone(), RelationNode::Let { binding, body })
    }
    fn values(expression: Expr, id: u32) -> Arc<Relation> {
        relation(Schema::new(vec![field(id, "value", expression.data_type())]).unwrap(), RelationNode::Values { rows: vec![vec![expression]] })
    }
    fn recipe_form(form: F, id: u32) -> Arc<Relation> {
        values(Expr::regex_replace(Expr::Literal(V::Null), RegexReplacement { pattern: form, ..Default::default() }), id)
    }
    fn first_expression(source: &Relation) -> &Expr {
        let RelationNode::Values { rows } = source.node() else { panic!("expected VALUES") };
        &rows[0][0]
    }
    fn recipe(source: &Relation) -> &RegexReplacement {
        let Expr::RegexReplace { recipe, .. } = first_expression(source) else { panic!("expected regex metadata") };
        recipe
    }
    fn scans(source: &Relation, output: &mut Vec<TableBinding>) {
        if let RelationNode::Scan(scan) = source.node() { output.push(scan.table.clone()); }
        if let RelationNode::Reference { binding } = source.node() { scans(&binding.relation, output); }
        for child in source.children() { scans(child, output); }
    }
    fn legacy(source: &Relation, catalog: &dyn Catalog) -> Result<Arc<Relation>> {
        fn visit(value: &mut Json, catalog: &dyn Catalog) -> Result<()> {
            if let Some(table) = value.get_mut("node").and_then(|node| node.get_mut("Scan")).and_then(|scan| scan.get_mut("table")) {
                let binding: TableBinding = serde_json::from_value(table.clone()).map_err(|error| gap(&error.to_string()))?;
                *table = serde_json::to_value(catalog.table(&binding.name)?.ok_or_else(|| gap("Prepared table disappeared from its own snapshot"))?).map_err(|error| gap(&error.to_string()))?;
            }
            match value { Json::Object(fields) => { for child in fields.values_mut() { visit(child, catalog)?; } }, Json::Array(values) => { for child in values { visit(child, catalog)?; } }, _ => {} }
            Ok(())
        }
        let mut value = serde_json::to_value(source).map_err(|error| gap(&error.to_string()))?;
        visit(&mut value, catalog)?;
        Ok(Arc::new(serde_json::from_value(value).map_err(|error| gap(&error.to_string()))?))
    }
    fn wire(source: &Relation) -> String { serde_json::to_string(source).unwrap() }
    fn assert_projection(source: &Relation) -> Arc<Relation> {
        let typed_catalog = RecordingCatalog::default();
        let legacy_catalog = RecordingCatalog::default();
        let typed = restore_dependencies(source, &typed_catalog).unwrap();
        let old = legacy(source, &legacy_catalog).unwrap();
        assert_eq!(wire(&typed), wire(&old));
        assert_eq!(typed_catalog.calls(), legacy_catalog.calls());
        typed
    }

    fn variants() -> Vec<(&'static str, Arc<Relation>)> {
        let input = scan("a", 0);
        let right = scan("b", 1);
        let output = field(2, "out", L::Scalar(S::I64));
        let named = NamedExpr { field: output.clone(), expr: input.schema().fields()[0].expression() };
        let order = SortExpr { expr: input.schema().fields()[0].expression(), descending: true, nulls_first: false };
        let definition = binding("cte", Arc::clone(&input));
        let mark = field(2, "mark", L::Scalar(S::Bool).maybe());
        let element = field(2, "element", L::Scalar(S::I64));
        let ordinal = field(3, "ordinal", L::Scalar(S::I64));
        let patch_fields = vec![field(0, "iid", L::Scalar(S::FixedSizeBinary(16))),
            field(1, "from", L::Scalar(S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) })),
            field(2, "to", L::Scalar(S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) })),
            field(3, "doc", L::Struct(StructFields::new()))];
        let patch = Arc::new(Relation::empty(Schema::new(patch_fields.clone()).unwrap()));
        let mut patch_output = patch_fields;
        patch_output[3].data_type = patch_output[3].data_type.clone().maybe();
        vec![
            ("Values", values(Expr::Literal(V::I64(7)), 0)),
            ("LiteralPages", relation(Schema::new(vec![output.clone()]).unwrap(), RelationNode::LiteralPages { pages: vec![vec![vec![Expr::Literal(V::I64(7))]], vec![]] })),
            ("Scan", Arc::clone(&input)),
            ("Project", relation(Schema::new(vec![output.clone()]).unwrap(), RelationNode::Project { input: Arc::clone(&input), expressions: vec![named.clone()] })),
            ("Extend", Arc::new(Arc::clone(&input).extend(vec![named.clone()]).unwrap())),
            ("Rename", relation(Schema::new(vec![output.clone()]).unwrap(), RelationNode::Rename { input: Arc::clone(&input), prefix: "prefix/".into(), expressions: vec![named] })),
            ("Let", let_relation(Arc::clone(&definition), reference(&definition))),
            ("Reference", reference(&definition)),
            ("Filter", relation(input.schema().clone(), RelationNode::Filter { input: Arc::clone(&input), predicate: Expr::Literal(V::Bool(true)) })),
            ("Join", cross(Arc::clone(&input), Arc::clone(&right))),
            ("Apply", relation(input.schema().concat(right.schema()).unwrap(), RelationNode::Apply { input: Arc::clone(&input), dependent: Arc::clone(&right), arguments: vec![(0, input.schema().fields()[0].expression())], kind: ApplyKind::Inner, output: None })),
            ("Mark", relation(input.schema().concat(&Schema::new(vec![mark.clone()]).unwrap()).unwrap(), RelationNode::Mark { input: Arc::clone(&input), dependent: Arc::clone(&right), arguments: vec![(0, input.schema().fields()[0].expression())], condition: Some(Expr::Literal(V::Bool(true))), comparison: Expr::binary(BinaryOp::Eq, input.schema().fields()[0].expression(), right.schema().fields()[0].expression()), output: mark })),
            ("Aggregate", relation(Schema::new(vec![output.clone()]).unwrap(), RelationNode::Aggregate { input: Arc::clone(&input), groups: Vec::new(), aggregates: vec![AggregateExpr { field: output.clone(), function: AggregateFunction::CountStar, argument: None, distinct: false, filter: None, order_by: Vec::new() }] })),
            ("Window", relation(input.schema().concat(&Schema::new(vec![output.clone()]).unwrap()).unwrap(), RelationNode::Window { input: Arc::clone(&input), partition_by: vec![ColumnId(0)], order_by: vec![order.clone()], expressions: vec![WindowExpr { field: output, function: WindowFunction::RowNumber }] })),
            ("Sort", relation(input.schema().clone(), RelationNode::Sort { input: Arc::clone(&input), order_by: vec![order.clone()] })),
            ("SortLimit", relation(input.schema().clone(), RelationNode::SortLimit { input: Arc::clone(&input), order_by: vec![order], offset: Some(Expr::Literal(V::I64(1))), fetch: Some(Expr::Parameter { index: 0, data_type: L::Scalar(S::I64) }) })),
            ("Limit", relation(input.schema().clone(), RelationNode::Limit { input: Arc::clone(&input), offset: Some(Expr::Literal(V::I64(0))), fetch: Some(Expr::Literal(V::I64(2))) })),
            ("Distinct", relation(input.schema().clone(), RelationNode::Distinct { input: Arc::clone(&input) })),
            ("Set", relation(input.schema().clone(), RelationNode::Set { left: Arc::clone(&input), right, operator: SetOperator::Except, all: false })),
            ("List", relation(Schema::new(vec![element.clone()]).unwrap(), RelationNode::List { expr: Expr::List(vec![Expr::Literal(V::I64(3))]), element: element.clone(), page_rows: std::num::NonZeroUsize::new(3) })),
            ("ColumnTable", relation(Schema::new(vec![element.clone()]).unwrap(), RelationNode::ColumnTable { expr: Expr::List(vec![Expr::Literal(V::I64(3))]), element: element.clone() })),
            ("Ordinality", Arc::new(Arc::clone(&input).with_ordinality(ordinal.clone(), rtdb_plan::OrdinalityScope::Relation).unwrap())),
            ("Unnest", relation(input.schema().concat(&Schema::new(vec![element.clone(), ordinal.clone()]).unwrap()).unwrap(), RelationNode::Unnest { input, expr: Expr::List(vec![Expr::Literal(V::I64(3))]), element, ordinality: Some(ordinal), outer: true })),
            ("PatchGaps", relation(Schema::new(patch_output).unwrap(), RelationNode::PatchGaps { input: patch, iid: ColumnId(0), valid_from: ColumnId(1), valid_to: ColumnId(2), document: ColumnId(3), from: Expr::Literal(V::Null), to: Expr::Literal(V::Null) })),
        ]
    }

    #[test]
    fn all_relation_variants_keep_their_typed_projection() {
        let cases = variants();
        assert_eq!(cases.len(), 24);
        let mut names = BTreeSet::new();
        for (name, source) in cases {
            assert!(names.insert(name));
            let before = wire(&source);
            let restored = assert_projection(&source);
            assert_eq!(restored.schema(), source.schema());
            assert!(!std::ptr::eq(restored.as_ref(), source.as_ref()));
            assert_eq!(serde_json::to_value(&*restored).unwrap()["node"].as_object().unwrap().keys().next().map(String::as_str), Some(name));
            match restored.node() {
                RelationNode::Values { rows } => assert_eq!(rows, &[vec![Expr::Literal(V::I64(7))]]),
                RelationNode::LiteralPages { pages } => assert_eq!(pages, &[vec![vec![Expr::Literal(V::I64(7))]], vec![]]),
                RelationNode::ArrowLiteralPages { .. } => unreachable!("Arrow literal pages use the legacy literal-page wire form"),
                RelationNode::List { expr, element, page_rows } => { assert_eq!(element.id, ColumnId(2)); assert_eq!(expr, &Expr::List(vec![Expr::Literal(V::I64(3))])); assert_eq!(*page_rows, std::num::NonZeroUsize::new(3)); }
                RelationNode::ColumnTable { expr, element } => { assert_eq!(element.id, ColumnId(2)); assert_eq!(expr, &Expr::List(vec![Expr::Literal(V::I64(3))])); }
                RelationNode::Ordinality { field, scope, .. } => { assert_eq!(field.id, ColumnId(3)); assert_eq!(field.data_type, L::Scalar(S::I64)); assert_eq!(*scope, rtdb_plan::OrdinalityScope::Relation); }
                RelationNode::Scan(scan) => { assert_eq!(scan.table, table("a")); assert_eq!(scan.columns[0].id, ColumnId(0)); }
                RelationNode::Project { expressions, .. } | RelationNode::Extend { expressions, .. } => { assert_eq!(expressions[0].field.id, ColumnId(2)); assert_eq!(expressions[0].expr, Expr::column(ColumnId(0), L::Scalar(S::I64))); }
                RelationNode::Rename { prefix, expressions, .. } => { assert_eq!(prefix, "prefix/"); assert_eq!(expressions[0].field.name, "out"); }
                RelationNode::Let { binding, body } => { assert_eq!(binding.name, "cte"); assert!(matches!(body.node(), RelationNode::Reference { .. })); }
                RelationNode::Reference { binding } => assert_eq!(binding.name, "cte"),
                RelationNode::Filter { predicate, .. } => assert_eq!(predicate, &Expr::Literal(V::Bool(true))),
                RelationNode::Join { kind, condition, .. } => { assert_eq!(*kind, JoinKind::Cross); assert!(condition.is_none()); }
                RelationNode::Apply { arguments, kind, output, .. } => {
                    assert_eq!(arguments.as_slice(), [(0, Expr::column(ColumnId(0), L::Scalar(S::I64)))]);
                    assert_eq!(*kind, ApplyKind::Inner); assert!(output.is_none());
                }
                RelationNode::Mark { arguments, condition, comparison, output, .. } => {
                    assert_eq!(arguments.as_slice(), [(0, Expr::column(ColumnId(0), L::Scalar(S::I64)))]);
                    assert_eq!(*condition, Some(Expr::Literal(V::Bool(true))));
                    assert_eq!(comparison, &Expr::binary(BinaryOp::Eq, Expr::column(ColumnId(0), L::Scalar(S::I64)), Expr::column(ColumnId(1), L::Scalar(S::I64))));
                    assert_eq!(output.id, ColumnId(2)); assert_eq!(output.data_type, L::Scalar(S::Bool).maybe());
                }
                RelationNode::Aggregate { groups, aggregates, .. } => { assert!(groups.is_empty()); assert_eq!(aggregates[0].function, AggregateFunction::CountStar); assert!(aggregates[0].argument.is_none()); }
                RelationNode::Window { partition_by, order_by, expressions, .. } => {
                    assert_eq!(partition_by.as_slice(), [ColumnId(0)]); assert!(order_by[0].descending); assert!(!order_by[0].nulls_first);
                    assert_eq!(expressions[0].function, WindowFunction::RowNumber); assert_eq!(expressions[0].field.id, ColumnId(2));
                }
                RelationNode::Sort { order_by, .. } => { assert!(order_by[0].descending); assert!(!order_by[0].nulls_first); }
                RelationNode::SortLimit { order_by, offset, fetch, .. } => {
                    assert!(order_by[0].descending); assert_eq!(offset, &Some(Expr::Literal(V::I64(1))));
                    assert_eq!(fetch, &Some(Expr::Parameter { index: 0, data_type: L::Scalar(S::I64) }));
                }
                RelationNode::Limit { offset, fetch, .. } => { assert_eq!(offset, &Some(Expr::Literal(V::I64(0)))); assert_eq!(fetch, &Some(Expr::Literal(V::I64(2)))); }
                RelationNode::Distinct { input } => assert_eq!(input.schema().fields()[0].id, ColumnId(0)),
                RelationNode::Set { left, right, operator, all } => {
                    assert_eq!(left.schema().fields()[0].id, ColumnId(0)); assert_eq!(right.schema().fields()[0].id, ColumnId(1));
                    assert_eq!(*operator, SetOperator::Except); assert!(!*all);
                }
                RelationNode::Unnest { expr, element, ordinality, outer, .. } => {
                    assert_eq!(expr, &Expr::List(vec![Expr::Literal(V::I64(3))])); assert_eq!(element.id, ColumnId(2));
                    assert_eq!(ordinality.as_ref().unwrap().id, ColumnId(3)); assert!(*outer);
                }
                RelationNode::PatchGaps { iid, valid_from, valid_to, document, from, to, .. } => {
                    assert_eq!([*iid, *valid_from, *valid_to, *document], [ColumnId(0), ColumnId(1), ColumnId(2), ColumnId(3)]);
                    assert_eq!(from, &Expr::Literal(V::Null)); assert_eq!(to, &Expr::Literal(V::Null));
                }
            }
            assert_eq!(wire(&source), before);
        }
    }

    #[test]
    fn restoring_hidden_dependency_columns_does_not_change_scan_output() {
        let source = scan("a", 7);
        let restored = assert_projection(&source);
        let RelationNode::Scan(old) = source.node() else { panic!() };
        let RelationNode::Scan(new) = restored.node() else { panic!() };
        assert_eq!(old.table.oid, Some(1));
        assert!(!old.table.columns.contains_key("_id"));
        assert_eq!(new.table, table("a"));
        assert_eq!(new.columns, old.columns);
        assert_eq!(new.valid_time, TemporalFilter::All);
        assert_eq!(new.system_time, TemporalFilter::Default);
        assert!(new.clamp_valid_time);
        assert_eq!(restored.schema().fields()[0].id, ColumnId(7));
        assert_eq!(restored.schema().len(), 1);
        assert_eq!(restored.schema().fields()[0].name, "x");
    }

    fn occurrence_tree() -> Arc<Relation> {
        let a = binding("a", scan("a", 0));
        let b = binding("b", scan("b", 1));
        let c = binding("c", scan("c", 2));
        cross(cross(let_relation(Arc::clone(&a), reference(&a)), let_relation(Arc::clone(&b), reference(&b))), reference(&c))
    }

    #[test]
    fn lookups_follow_declaration_order_for_every_owned_reference_occurrence() {
        let source = occurrence_tree();
        let outcomes = ["a", "a", "b", "b", "c"].into_iter().enumerate().map(|(index, name)| {
            let mut binding = table(name); binding.oid = Some(index as i32 + 10); Ok(Some(binding))
        }).collect();
        let catalog = RecordingCatalog::with(outcomes);
        let restored = restore_dependencies(&source, &catalog).unwrap();
        assert_eq!(catalog.calls(), ["a", "a", "b", "b", "c"]);
        let mut actual = Vec::new(); scans(&restored, &mut actual);
        assert_eq!(actual.iter().map(|table| table.oid).collect::<Vec<_>>(), [Some(10), Some(11), Some(12), Some(13), Some(14)]);
        assert_eq!(actual.iter().map(|table| table.name.table.table.as_str()).collect::<Vec<_>>(), ["a", "a", "b", "b", "c"]);
    }

    #[test]
    fn missing_and_catalog_errors_keep_their_exact_lookup_prefix() {
        let source = cross(cross(scan("a", 0), scan("b", 1)), scan("c", 2));
        for failed in 0..3 {
            for missing in [true, false] {
                let expected = if missing { gap("Prepared table disappeared from its own snapshot") }
                    else { Anomaly::fault("fixture/catalog", "lookup failed").with_context("ordinal", json!(failed)) };
                let mut outcomes: Vec<_> = ["a", "b", "c"][..failed].iter().map(|name| Ok(Some(table(name)))).collect();
                outcomes.push(if missing { Ok(None) } else { Err(expected.clone()) });
                let catalog = RecordingCatalog::with(outcomes.clone());
                let old_catalog = RecordingCatalog::with(outcomes);
                let error = restore_dependencies(&source, &catalog).unwrap_err();
                assert_eq!(error, expected);
                assert_eq!(legacy(&source, &old_catalog).unwrap_err(), error);
                assert_eq!(catalog.calls(), ["a", "b", "c"][..=failed]);
                assert_eq!(old_catalog.calls(), catalog.calls());
                if missing { assert_eq!(error.category, AnomalyCategory::Unsupported); }
            }
        }
        let error = Anomaly::fault("fixture/projection", "invalid");
        let projected = projection_error(error);
        assert_eq!(projected, gap("fixture/projection: invalid"));
    }

    #[test]
    fn completed_lookup_results_rebuild_without_a_catalog_and_check_occurrence_counts() {
        let source = scan("a", 0);
        let tables = {
            let catalog = RecordingCatalog::default();
            let tables = ResolvedTables::resolve(&source, &catalog).unwrap();
            assert_eq!(catalog.calls(), ["a"]);
            assert_eq!(tables.bindings, [table("a")]);
            tables
        };
        let restored = Rebuilder { bindings: tables.bindings.into_iter(), forms: FormAllocation::Untracked }.rebuild(&source).unwrap();
        let RelationNode::Scan(scan) = restored.node() else { panic!() };
        assert_eq!(scan.table, table("a"));
        for bindings in [Vec::new(), vec![table("a"), table("extra")]] {
            let error = Rebuilder { bindings: bindings.into_iter(), forms: FormAllocation::Untracked }.rebuild(&source).unwrap_err();
            assert_eq!(error, occurrence_error());
        }
    }

    #[test]
    fn nested_same_name_materializations_keep_their_lexical_definitions() {
        let outer = binding("same", scan("a", 0));
        let inner = binding("same", scan("b", 1));
        let source = let_relation(Arc::clone(&outer), cross(reference(&outer), let_relation(Arc::clone(&inner), reference(&inner))));
        let restored = assert_projection(&source);
        let RelationNode::Let { binding: new_outer, body } = restored.node() else { panic!() };
        let RelationNode::Join { left, right, .. } = body.node() else { panic!() };
        let RelationNode::Reference { binding: outer_ref } = left.node() else { panic!() };
        let RelationNode::Let { binding: new_inner, body } = right.node() else { panic!() };
        let RelationNode::Reference { binding: inner_ref } = body.node() else { panic!() };
        assert_eq!(new_outer.name, "same"); assert_eq!(new_inner.name, "same");
        assert_eq!(new_outer.relation, outer_ref.relation); assert_eq!(new_inner.relation, inner_ref.relation);
        assert_ne!(new_outer.relation, new_inner.relation);
        assert!(!Arc::ptr_eq(new_outer, outer_ref)); assert!(!Arc::ptr_eq(new_inner, inner_ref));
        assert!(!Arc::ptr_eq(new_outer, &outer)); assert!(!Arc::ptr_eq(new_inner, &inner));
    }

    #[test]
    fn aliases_projection_flags_and_column_ids_are_copied_without_normalization() {
        let mut scan = match scan("a", 31).node() { RelationNode::Scan(scan) => scan.clone(), _ => panic!() };
        scan.columns[0].qualifier = Some(ColumnQualifier::BaseTable { database: "DB".into(), schema: "Public".into(), alias: "Mixed/Name".into() });
        scan.columns[0].qualified_only = true; scan.columns[0].unnamed = true;
        let input = Arc::new(Relation::scan(scan).unwrap());
        let mut output = field(47, "Quoted-looking/name", L::Scalar(S::I64));
        output.qualifier = Some(ColumnQualifier::Alias("MiX.ed".into())); output.qualified_only = true; output.unnamed = true;
        let source = relation(Schema::new(vec![output.clone()]).unwrap(), RelationNode::Rename { input: Arc::clone(&input), prefix: "Raw/Prefix".into(), expressions: vec![NamedExpr { field: output.clone(), expr: input.schema().fields()[0].expression() }] });
        let restored = assert_projection(&source);
        assert_eq!(restored.schema().fields(), [output]);
        let RelationNode::Rename { input: new_input, prefix, expressions } = restored.node() else { panic!() };
        assert_eq!(prefix, "Raw/Prefix"); assert_eq!(expressions[0].expr, Expr::column(ColumnId(31), L::Scalar(S::I64)));
        assert_eq!(new_input.schema(), input.schema());
        assert_eq!(new_input.schema().fields()[0].qualifier, Some(ColumnQualifier::BaseTable { database: "DB".into(), schema: "Public".into(), alias: "Mixed/Name".into() }));
        assert!(restored.schema().fields().iter().all(|field| field.name != "_id"));
    }

    #[test]
    fn unsupported_host_metadata_stays_uninterpreted_with_its_raw_duplicates() {
        let pattern = raw_tagged("opaque/reader".into(), raw_anonymous(raw_list(vec![
            raw_symbol("unresolved/host".into()), raw_literal(V::Utf8("[".into())), raw_big_integer("00099".into()), raw_decimal("1.00M".into()),
        ])));
        let flags = raw_map(vec![(raw_character('i'), raw_symbol("first".into())), (raw_character('i'), raw_symbol("second".into()))]);
        let expected = RegexReplacement { pattern, replacement: raw_literal(V::Utf8("$bad".into())), flags, ..Default::default() };
        let source = values(Expr::regex_replace(Expr::Literal(V::Null), expected.clone()), 0);
        let restored = assert_projection(&source);
        assert_eq!(recipe(&restored), &expected);
        assert_eq!(serde_json::to_string(&recipe(&restored).pattern).unwrap(), r#"{"Tagged":{"tag":"opaque/reader","value":{"AnonymousFunction":{"List":[{"Symbol":"unresolved/host"},{"Literal":{"Utf8":"["}},{"BigInteger":"00099"},{"Decimal":"1.00M"}]}}}}"#);
        assert_eq!(serde_json::to_string(&recipe(&restored).flags).unwrap(), r#"{"Map":[[{"Character":"i"},{"Symbol":"first"}],[{"Character":"i"},{"Symbol":"second"}]]}"#);
    }

    #[test]
    fn nonfinite_float_payloads_and_signed_zero_keep_exact_nested_wire_bits() {
        let value = V::list(vec![V::F32(f32::from_bits(0x7fc00001)), V::F32(f32::INFINITY), V::F32(f32::NEG_INFINITY), V::F32(-0.0),
            V::F64(f64::from_bits(0x7ff8000000000001)), V::F64(f64::INFINITY), V::F64(f64::NEG_INFINITY), V::F64(-0.0)]);
        let source = recipe_form(raw_literal(value), 0);
        let restored = assert_projection(&source);
        let RegexFormKind::Literal(value) = recipe(&restored).pattern.view().kind() else { panic!() };
        let RegexLiteralKind::List(values) = value.kind() else { panic!() };
        let bits = values.iter().map(|value| {
            let RegexLiteralKind::Leaf(leaf) = value.kind() else { panic!() };
            match leaf.value().as_ref() { V::F32(value) => u64::from(value.to_bits()), V::F64(value) => value.to_bits(), _ => panic!() }
        }).collect::<Vec<_>>();
        assert_eq!(bits, [0x7fc00001, 0x7f800000, 0xff800000, 0x80000000, 0x7ff8000000000001, 0x7ff0000000000000, 0xfff0000000000000, 0x8000000000000000]);
        assert_eq!(serde_json::to_string(&recipe(&restored).pattern).unwrap(), r#"{"Literal":{"List":[{"F32":2143289345},{"F32":2139095040},{"F32":4286578688},{"F32":2147483648},{"F64":9221120237041090561},{"F64":9218868437227405312},{"F64":18442240474082181120},{"F64":9223372036854775808}]}}"#);
        for bits in [0x7ff8000000000001u64, 0x7ff0000000000000, 0xfff0000000000000, 0x8000000000000000] {
            let input = scan("a", 0);
            let output = field(2, "percentile", L::Scalar(S::F64).maybe());
            let source = relation(Schema::new(vec![output.clone()]).unwrap(), RelationNode::Aggregate { input: Arc::clone(&input), groups: Vec::new(),
                aggregates: vec![AggregateExpr { field: output, function: AggregateFunction::Percentile { kind: PercentileKind::Continuous, fraction: PercentileFraction::Literal(f64::from_bits(bits)), descending: false },
                    argument: Some(input.schema().fields()[0].expression()), distinct: false, filter: None, order_by: Vec::new() }] });
            let restored = assert_projection(&source);
            let RelationNode::Aggregate { aggregates, .. } = restored.node() else { panic!() };
            let AggregateFunction::Percentile { fraction: PercentileFraction::Literal(value), .. } = &aggregates[0].function else { panic!() };
            assert_eq!(value.to_bits(), bits);
            assert_eq!(serde_json::to_value(&*restored).unwrap()["node"]["Aggregate"]["aggregates"][0]["function"]["Percentile"]["fraction"], json!({"Literal":bits}));
        }
    }

    #[test]
    fn calendar_aggregate_cast_and_physical_collection_facts_survive_projection() {
        for (date, unit, expected) in [
            (CivilDate::MIN, DateUnit::Day, r#"{"Literal":{"Date":{"date":"-999999999-01-01","unit":"Day"}}}"#),
            (CivilDate::MAX, DateUnit::Millisecond, r#"{"Literal":{"Date":{"date":"+999999999-12-31","unit":"Millisecond"}}}"#),
        ] {
            let restored = assert_projection(&recipe_form(raw_literal(V::Date(CalendarDate::new(date, unit))), 0));
            assert_eq!(serde_json::to_string(&recipe(&restored).pattern).unwrap(), expected);
            let RegexFormKind::Literal(literal) = recipe(&restored).pattern.view().kind() else { panic!() };
            let RegexLiteralKind::Leaf(leaf) = literal.kind() else { panic!() };
            let cell = leaf.value();
            let V::Date(value) = cell.as_ref() else { panic!() };
            assert_eq!(value.date(), date); assert_eq!(value.unit(), unit);
        }
        let input = scan("a", 0);
        let output = field(2, "sum", L::Scalar(S::I64));
        let aggregate = relation(Schema::new(vec![output.clone()]).unwrap(), RelationNode::Aggregate { input: Arc::clone(&input), groups: Vec::new(),
            aggregates: vec![AggregateExpr { field: output, function: AggregateFunction::Named(AggregateName::new("SuM_AlL").unwrap()), argument: Some(input.schema().fields()[0].expression()), distinct: false, filter: None, order_by: Vec::new() }] });
        let restored = assert_projection(&aggregate);
        let RelationNode::Aggregate { aggregates, .. } = restored.node() else { panic!() };
        let AggregateFunction::Named(name) = &aggregates[0].function else { panic!() };
        assert_eq!(name.spelling(), "SuM_AlL"); assert_eq!(aggregates[0].function.canonical(), AggregateFunction::Sum);
        let cast = Expr::Cast { expr: Box::new(Expr::Literal(V::Null)), target: CastTarget::Deferred(DeferredCast::Temporal {
            kind: TemporalKind::TimestampZone(CastTimezone::Session), unit: TimeUnit::Nanosecond, precision: CastOption::Other("#opaque{:x 1}".into()),
        }) };
        let restored = assert_projection(&values(cast.clone(), 0));
        assert_eq!(first_expression(&restored), &cast);
        let physical = V::Struct(StructFields::from([("z".into(), V::Set(vec![V::I64(2), V::I64(1), V::I64(2)])),
            ("a".into(), V::Map { entries: vec![(V::Utf8("z".into()), V::I64(1)), (V::Utf8("z".into()), V::I64(2))], sorted: true })]));
        let restored = assert_projection(&recipe_form(raw_literal(physical), 0));
        assert_eq!(serde_json::to_string(&recipe(&restored).pattern).unwrap(), r#"{"Literal":{"Struct":{"a":{"Map":{"entries":[[{"Utf8":"z"},{"I64":1}],[{"Utf8":"z"},{"I64":2}]],"sorted":true}},"z":{"Set":[{"I64":2},{"I64":1},{"I64":2}]}}}}"#);
    }

    #[test]
    fn restored_values_and_bindings_outlive_the_source_and_catalog() {
        let restored = {
            let catalog = RecordingCatalog::default();
            let source = cross(scan("a", 0), recipe_form(raw_literal(V::Utf8("owned-pattern".into())), 1));
            let result = restore_dependencies(&source, &catalog).unwrap();
            assert_eq!(catalog.calls(), ["a"]);
            result
        };
        let RelationNode::Join { left, right, .. } = restored.node() else { panic!() };
        let RelationNode::Scan(scan) = left.node() else { panic!() };
        assert_eq!(scan.table, table("a"));
        assert_eq!(recipe(right).pattern, raw_literal(V::Utf8("owned-pattern".into())));
        assert_eq!(restored.schema().fields().iter().map(|field| field.id).collect::<Vec<_>>(), [ColumnId(0), ColumnId(1)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn non_direct_engine_query_uses_restoration_and_retains_empty_result_schema() {
        use super::super::{Engine, Options, Record, Running};
        let running = Running::open().await.unwrap();
        let mut engine = Engine::new(running.node.clone()).unwrap();
        let record: Record = serde_json::from_value(json!({"type":"query","sql":"SELECT 1 AS value WHERE FALSE","type_string":"I","sort":"nosort"})).unwrap();
        let options = Options { direct_sql: false, decorrelate: true, query_limit: None, max_rows: 10, max_result_bytes: 1024 * 1024 };
        let result = engine.query(&record, &BTreeMap::new(), &options).await;
        drop(engine);
        running.close().await.unwrap();
        let (result, charges) = result.unwrap();
        assert_eq!(result["rows"], 0); assert_eq!(result["values"], 0); assert_eq!(result["text"], "\n");
        assert_eq!(result["schema"].as_array().unwrap().len(), 1);
        assert_eq!(result["schema"][0]["name"], "value");
        assert!(charges.iter().map(|charge| charge.bytes()).sum::<usize>() >= 8192);
    }

    fn expression_addresses(source: &Relation, addresses: &mut BTreeSet<usize>) {
        for expression in source.expressions() { expression.visit(&mut |expression| { addresses.insert(expression as *const Expr as usize); }); }
        if let RelationNode::Reference { binding } = source.node() { expression_addresses(&binding.relation, addresses); }
        for child in source.children() { expression_addresses(child, addresses); }
    }
    fn pattern_pointer(source: &Relation) -> *const u8 {
        let RegexFormKind::Literal(value) = recipe(source).pattern.view().kind() else { panic!() };
        let RegexLiteralKind::Text(value) = value.kind() else { panic!() };
        assert!(!value.is_empty());
        value.as_ptr()
    }

    #[test]
    fn repeated_relation_occurrences_have_fresh_expression_and_raw_payload_addresses() {
        let input = recipe_form(raw_literal(V::Utf8("owned-pattern".into())), 0);
        let source = relation(input.schema().clone(), RelationNode::Set { left: Arc::clone(&input), right: Arc::clone(&input), operator: SetOperator::Union, all: true });
        let restored = assert_projection(&source);
        let RelationNode::Set { left, right, .. } = restored.node() else { panic!() };
        assert!(!Arc::ptr_eq(left, right)); assert!(!Arc::ptr_eq(left, &input)); assert!(!Arc::ptr_eq(right, &input));
        let mut before = BTreeSet::new(); expression_addresses(&source, &mut before);
        let mut after = BTreeSet::new(); expression_addresses(&restored, &mut after);
        assert_eq!(before.len(), 2); assert_eq!(after.len(), 4); assert!(before.is_disjoint(&after));
        assert_ne!(pattern_pointer(left), pattern_pointer(right));
        assert_ne!(pattern_pointer(left), pattern_pointer(&input)); assert_ne!(pattern_pointer(right), pattern_pointer(&input));
        assert_eq!(recipe(left), recipe(right));
    }

    fn assert_form_projection(form: F, expected: &str) {
        let source = recipe_form(form, 0);
        let typed = assert_projection(&source);
        assert_eq!(serde_json::to_string(&recipe(&source).pattern).unwrap(), expected);
        assert_eq!(serde_json::to_string(&recipe(&typed).pattern).unwrap(), expected);
    }

    #[test]
    fn all_raw_forms_and_physical_literals_keep_explicit_projection_wires() {
        let raw = vec![
            (raw_literal(V::Null), r#"{"Literal":"Null"}"#),
            (raw_character('😀'), r#"{"Character":"😀"}"#),
            (raw_symbol("ns/x".into()), r#"{"Symbol":"ns/x"}"#),
            (raw_big_integer("00099".into()), r#"{"BigInteger":"00099"}"#),
            (raw_decimal("1.00M".into()), r#"{"Decimal":"1.00M"}"#),
            (raw_list(vec![raw_symbol("unresolved".into())]), r#"{"List":[{"Symbol":"unresolved"}]}"#),
            (raw_vector(vec![raw_character('x')]), r#"{"Vector":[{"Character":"x"}]}"#),
            (raw_set(vec![raw_symbol("x".into()), raw_symbol("x".into())]), r#"{"Set":[{"Symbol":"x"},{"Symbol":"x"}]}"#),
            (raw_map(vec![(raw_symbol("x".into()), raw_character('a')), (raw_symbol("x".into()), raw_character('b'))]), r#"{"Map":[[{"Symbol":"x"},{"Character":"a"}],[{"Symbol":"x"},{"Character":"b"}]]}"#),
            (raw_tagged("opaque/tag".into(), raw_literal(V::Null)), r#"{"Tagged":{"tag":"opaque/tag","value":{"Literal":"Null"}}}"#),
            (raw_anonymous(raw_symbol("%".into())), r#"{"AnonymousFunction":{"Symbol":"%"}}"#),
        ];
        assert_eq!(raw.len(), 11);
        for (form, expected) in raw { assert_form_projection(form, expected); }
        let physical = vec![
            (V::Undefined, r#""Undefined""#), (V::Null, r#""Null""#), (V::Bool(true), r#"{"Bool":true}"#),
            (V::I8(-8), r#"{"I8":-8}"#), (V::I16(-16), r#"{"I16":-16}"#), (V::I32(-32), r#"{"I32":-32}"#), (V::I64(-64), r#"{"I64":-64}"#),
            (V::U8(8), r#"{"U8":8}"#), (V::U16(16), r#"{"U16":16}"#), (V::U32(32), r#"{"U32":32}"#), (V::U64(u64::MAX), r#"{"U64":18446744073709551615}"#),
            (V::F16(0x8000), r#"{"F16":32768}"#), (V::F32(f32::from_bits(0x7fc00001)), r#"{"F32":2143289345}"#), (V::F64(f64::from_bits(0x7ff8000000000001)), r#"{"F64":9221120237041090561}"#),
            (V::Utf8("é😀".into()), r#"{"Utf8":"é😀"}"#), (V::Binary(vec![0, 255]), r#"{"Binary":[0,255]}"#), (V::FixedSizeBinary(vec![9]), r#"{"FixedSizeBinary":[9]}"#),
            (V::Decimal(DecimalValue { unscaled: vec![255, 0], precision: 4, scale: -2, bit_width: 16 }), r#"{"Decimal":{"unscaled":[255,0],"precision":4,"scale":-2,"bit_width":16}}"#),
            (V::Date(CalendarDate::new(CivilDate::from_ymd(2000, 1, 2).unwrap(), DateUnit::Day)), r#"{"Date":{"date":"2000-01-02","unit":"Day"}}"#),
            (V::DateDay(-1), r#"{"DateDay":-1}"#), (V::DateMilli(-1000), r#"{"DateMilli":-1000}"#),
            (V::Time { value: 7, unit: TimeUnit::Microsecond, bit_width: 64 }, r#"{"Time":{"value":7,"unit":"Microsecond","bit_width":64}}"#),
            (V::Timestamp { value: -7, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) }, r#"{"Timestamp":{"value":-7,"unit":"Nanosecond","timezone":"UTC"}}"#),
            (V::Duration { value: 8, unit: TimeUnit::Second }, r#"{"Duration":{"value":8,"unit":"Second"}}"#), (V::IntervalYearMonth(-12), r#"{"IntervalYearMonth":-12}"#),
            (V::IntervalDayTime { days: -1, milliseconds: 3 }, r#"{"IntervalDayTime":{"days":-1,"milliseconds":3}}"#),
            (V::IntervalMonthDayNano { months: 1, days: 2, nanoseconds: 3 }, r#"{"IntervalMonthDayNano":{"months":1,"days":2,"nanoseconds":3}}"#),
            (V::IntervalMonthDayMicro { months: 4, days: 5, nanoseconds: 6 }, r#"{"IntervalMonthDayMicro":{"months":4,"days":5,"nanoseconds":6}}"#),
            (V::Keyword(Keyword::from_name(":raw/name")), r#"{"Keyword":":raw/name"}"#), (V::Uuid([0; 16]), r#"{"Uuid":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}"#),
            (V::Uri("https://example.test/é".into()), r#"{"Uri":"https://example.test/é"}"#), (V::Transit(vec![0, 255]), r#"{"Transit":[0,255]}"#),
            (V::Oid(-1), r#"{"Oid":-1}"#), (V::RegClass(2), r#"{"RegClass":2}"#), (V::RegProc(3), r#"{"RegProc":3}"#),
            (V::list(vec![V::I64(1)]), r#"{"List":[{"I64":1}]}"#), (V::Set(vec![V::Null, V::Null]), r#"{"Set":["Null","Null"]}"#),
            (V::Map { entries: vec![(V::Null, V::Bool(true)), (V::Null, V::Bool(false))], sorted: true }, r#"{"Map":{"entries":[["Null",{"Bool":true}],["Null",{"Bool":false}]],"sorted":true}}"#),
            (V::FixedSizeList(vec![V::Bool(false)].into()), r#"{"FixedSizeList":[{"Bool":false}]}"#), (V::TimestampTzRange { lower: -1, upper: 2 }, r#"{"TimestampTzRange":{"lower":-1,"upper":2}}"#),
            (V::Struct(StructFields::from([("z".into(), V::Null), ("a".into(), V::Bool(true))])), r#"{"Struct":{"a":{"Bool":true},"z":"Null"}}"#),
        ];
        assert_eq!(physical.len(), 41);
        for (value, expected) in physical { assert_form_projection(raw_literal(value), &format!(r#"{{"Literal":{expected}}}"#)); }
    }

    fn binary(source: &Relation) -> &Vec<u8> {
        let Expr::Literal(V::Binary(value)) = first_expression(source) else { panic!() };
        value
    }
    fn prepared(source: &Arc<Relation>) -> PreparedStatement {
        PreparedStatement { statement: Statement::Query(Arc::clone(source)), parameter_types: BTreeMap::new(), source: String::new(), notices: Vec::new(), settings: Default::default() }
    }

    #[test]
    fn typed_capacity_changes_native_admission_and_prepared_clones_retain_the_existing_lease() {
        let bytes = 1024 * 1024 + 1;
        let source = values(Expr::Literal(V::Binary(vec![7; bytes])), 0);
        let old = legacy(&source, &RecordingCatalog::default()).unwrap();
        let typed = restore_dependencies(&source, &RecordingCatalog::default()).unwrap();
        assert_eq!(binary(&old).as_slice(), binary(&typed).as_slice());
        assert_eq!(binary(&typed).len(), bytes); assert_eq!(binary(&typed).first(), Some(&7)); assert_eq!(binary(&typed).last(), Some(&7));
        assert_eq!(binary(&old).capacity(), 2 * 1024 * 1024); assert_eq!(binary(&typed).capacity(), bytes);
        assert_eq!(serde_json::to_vec(&*old).unwrap(), serde_json::to_vec(&*typed).unwrap());
        let memory = MemoryBudget::new(12 * 1024 * 1024);
        let error = PreparedQuery::from_prepared(prepared(&old), &memory).err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded"); assert_eq!(memory.used_bytes(), 0);
        let query = PreparedQuery::from_prepared(prepared(&typed), &memory).unwrap();
        let retained = memory.used_bytes(); assert!(retained > 0); assert!(retained < memory.limit_bytes());
        let clone = query.clone(); drop(query); assert_eq!(memory.used_bytes(), retained);
        drop(clone); assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn native_raw_copy_begins_only_after_every_catalog_occurrence_has_resolved() {
        let source = cross(recipe_form(raw_literal(V::Utf8("owned-pattern".into())), 0), scan("lookup", 1));
        let catalog = RecordingCatalog::with(vec![Err(Anomaly::incorrect("fixture/catalog-first", "lookup precedes raw copy"))]);
        let memory = MemoryBudget::new(0);
        let pointer = pattern_pointer(source.children()[0]);
        let error = restore_dependencies_in(&source, &catalog, &memory).unwrap_err();
        assert_eq!(error.code, "fixture/catalog-first");
        assert_eq!(catalog.calls(), ["lookup"]);
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(pattern_pointer(source.children()[0]), pointer);
    }

    #[test]
    fn native_raw_copy_failure_releases_destination_and_preserves_completed_lookup_order() {
        let source = cross(recipe_form(raw_literal(V::Utf8("owned-pattern".into())), 0), scan("lookup", 1));
        let catalog = RecordingCatalog::default();
        let memory = MemoryBudget::new(0);
        let pointer = pattern_pointer(source.children()[0]);
        let error = restore_dependencies_in(&source, &catalog, &memory).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(catalog.calls(), ["lookup"]);
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(pattern_pointer(source.children()[0]), pointer);
    }

    #[test]
    fn native_fresh_raw_owners_survive_prepared_publication_and_clones_independently_of_the_source() {
        let source = recipe_form(raw_literal(V::Utf8("owned-pattern".into())), 0);
        let memory = MemoryBudget::new(16 << 20);
        let restored = restore_dependencies_in(&source, &RecordingCatalog::default(), &memory).unwrap();
        assert_ne!(pattern_pointer(&restored), pattern_pointer(&source));
        let raw_retained = memory.used_bytes(); assert!(raw_retained > 0);
        let query = PreparedQuery::from_prepared(prepared(&restored), &memory).unwrap();
        drop(restored); drop(source);
        let retained = memory.used_bytes(); assert!(retained >= raw_retained);
        let clone = query.clone(); drop(query);
        assert_eq!(memory.used_bytes(), retained);
        drop(clone);
        assert_eq!(memory.used_bytes(), 0);
    }
}
