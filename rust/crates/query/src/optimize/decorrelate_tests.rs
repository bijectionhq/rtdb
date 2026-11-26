use super::*;
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{AggregateExpr, AggregateFunction, ApplyKind, Field, UnaryOp};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};

fn integer(id: u32, name: &str) -> Field {
    Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64).maybe() }
}
fn typed(id: u32, name: &str, data_type: LogicalType) -> Field { Field { data_type, ..integer(id, name) } }
fn int() -> LogicalType { LogicalType::Scalar(ScalarType::I64) }
fn bool_type() -> LogicalType { LogicalType::Scalar(ScalarType::Bool) }
fn literal(value: i64) -> Expr { Expr::Literal(Value::I64(value)) }

fn values(fields: Vec<Field>, rows: &[&[Option<i64>]]) -> Arc<Relation> {
    let rows = rows.iter().map(|row| row.iter().map(|cell| Expr::Literal(cell.map_or(Value::Null, Value::I64))).collect()).collect();
    Arc::new(Relation::new(Schema::new(fields).unwrap(), RelationNode::Values { rows }).unwrap())
}

fn customer_id() -> Field { integer(0, "id") }
fn customer_region() -> Field { integer(1, "region") }
fn order_customer() -> Field { integer(2, "customer") }
fn order_amount() -> Field { integer(3, "amount") }

/// Customers with a duplicate row and nulls in each column.
fn customers() -> Arc<Relation> {
    values(vec![customer_id(), customer_region()], &[
        &[Some(1), Some(10)], &[Some(2), Some(20)], &[Some(2), Some(20)], &[Some(3), None], &[None, Some(10)], &[Some(5), Some(30)],
    ])
}

/// Orders with several rows for one customer, rows for no customer, and nulls in each column.
fn orders() -> Arc<Relation> {
    values(vec![order_customer(), order_amount()], &[
        &[Some(1), Some(100)], &[Some(1), Some(150)], &[Some(2), None], &[None, Some(50)], &[Some(4), Some(70)], &[Some(3), Some(30)], &[Some(3), Some(30)],
    ])
}

fn parameter(index: usize, field: &Field) -> Expr { Expr::Parameter { index, data_type: field.data_type.clone() } }
fn equals(left: Expr, right: Expr) -> Expr { Expr::binary(BinaryOp::Eq, left, right) }
fn select(input: Arc<Relation>, predicate: Expr) -> Arc<Relation> { Arc::new(input.filter(predicate).unwrap()) }
fn keep(input: Arc<Relation>, fields: &[Field]) -> Arc<Relation> {
    Arc::new(input.project(fields.iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect()).unwrap())
}
fn apply(input: &Arc<Relation>, dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>, kind: ApplyKind, output: Option<Field>) -> Arc<Relation> {
    let schema = kind.result_schema(input.schema(), dependent.schema(), output.as_ref()).unwrap();
    Arc::new(Relation::new(schema, RelationNode::Apply { input: Arc::clone(input), dependent, arguments, kind, output }).unwrap())
}
fn aggregate(input: Arc<Relation>, aggregates: Vec<AggregateExpr>) -> Arc<Relation> {
    let schema = Schema::new(aggregates.iter().map(|aggregate| aggregate.field.clone()).collect()).unwrap();
    Arc::new(Relation::new(schema, RelationNode::Aggregate { input, groups: Vec::new(), aggregates }).unwrap())
}
fn counted(field: Field, function: AggregateFunction, argument: Option<Expr>, filter: Option<Expr>) -> AggregateExpr {
    AggregateExpr { field, function, argument, distinct: false, filter, order_by: Vec::new() }
}

/// The orders of the customer bound to parameter 0.
fn customer_orders() -> Arc<Relation> { select(orders(), equals(order_customer().expression(), parameter(0, &customer_id()))) }
fn by_customer() -> Vec<(usize, Expr)> { vec![(0, customer_id().expression())] }

/// `SELECT c.* FROM c WHERE [NOT] EXISTS (<dependent>)`, with `extra` conjoined to the test.
fn exists(dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>, negated: bool, extra: Option<Expr>) -> Arc<Relation> {
    let mark = typed(11, "_sq_11", bool_type());
    let applied = apply(&customers(), dependent, arguments, ApplyKind::Exists, Some(mark.clone()));
    let test = if negated { Expr::unary(UnaryOp::Not, mark.expression()) } else { mark.expression() };
    let predicate = match extra { Some(extra) => Expr::binary(BinaryOp::And, test, extra), None => test };
    keep(select(applied, predicate), &[customer_id(), customer_region()])
}

fn one(input: Arc<Relation>) -> Arc<Relation> {
    Arc::new(input.project(vec![NamedExpr { field: typed(10, "one", int()), expr: literal(1) }]).unwrap())
}

/// `c.id [NOT] IN (SELECT o.customer FROM o WHERE o.amount > c.region)` as the SQL binder plans a correlated quantified
/// comparison, selected on where `filtered`, or projected.
fn quantified(negated: bool, filtered: bool) -> Arc<Relation> {
    let candidate = integer(14, "candidate");
    let query = Arc::new(select(orders(), Expr::binary(BinaryOp::Gt, order_amount().expression(), parameter(1, &customer_region())))
        .project(vec![NamedExpr { field: candidate.clone(), expr: order_customer().expression() }]).unwrap());
    let comparison = equals(parameter(2, &customer_id()), candidate.expression());
    let compared = typed(15, "compared", comparison.data_type());
    let compared_rows = Arc::new(query.project(vec![NamedExpr { field: compared.clone(), expr: comparison }]).unwrap());
    let (truth, unknown) = (typed(16, "truth", int()), typed(17, "unknown", int()));
    let counts = aggregate(compared_rows, vec![
        counted(truth.clone(), AggregateFunction::CountStar, None, Some(Expr::unary(UnaryOp::IsTrue, compared.expression()))),
        counted(unknown.clone(), AggregateFunction::CountStar, None, Some(Expr::unary(UnaryOp::IsUnknown, compared.expression()))),
    ]);
    let case = Expr::Case { branches: vec![
        (Expr::binary(BinaryOp::Gt, truth.expression(), literal(0)), Expr::Literal(Value::Bool(true))),
        (Expr::binary(BinaryOp::Gt, unknown.expression(), literal(0)), Expr::Literal(Value::Null)),
    ], otherwise: Box::new(Expr::Literal(Value::Bool(false))) };
    let dependent = Arc::new(counts.project(vec![NamedExpr { field: typed(18, "result", bool_type().maybe()), expr: case }]).unwrap());
    let output = typed(19, "_sq_19", bool_type().maybe());
    let applied = apply(&customers(), dependent, vec![(1, customer_region().expression()), (2, customer_id().expression())], ApplyKind::Scalar, Some(output.clone()));
    if !filtered { return applied; }
    let test = if negated { Expr::unary(UnaryOp::Not, output.expression()) } else { output.expression() };
    keep(select(applied, test), &[customer_id(), customer_region()])
}

/// `SELECT c.*, (<dependent>) FROM c`.
fn scalar(dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>) -> Arc<Relation> {
    let output = Field { data_type: dependent.schema().fields()[0].data_type.clone().maybe(), ..integer(13, "_sq_13") };
    apply(&customers(), dependent, arguments, ApplyKind::Scalar, Some(output))
}

/// Each correlated query, and whether its applies are all rewritten into joins.
fn corpus() -> Vec<(&'static str, Arc<Relation>, bool)> {
    let count = typed(12, "count", int());
    let total = integer(12, "total");
    let plus_one = integer(20, "plus_one");
    let summed = aggregate(customer_orders(), vec![counted(total.clone(), AggregateFunction::Sum, Some(order_amount().expression()), None)]);
    let large = Expr::binary(BinaryOp::Gt, order_amount().expression(), parameter(1, &customer_region()));
    let distinct = Arc::new(Relation::new(Schema::new(vec![order_customer()]).unwrap(), RelationNode::Distinct { input: keep(customer_orders(), &[order_customer()]) }).unwrap());
    vec![
        ("exists", exists(one(customer_orders()), by_customer(), false, None), true),
        ("not exists", exists(one(customer_orders()), by_customer(), true, None), true),
        ("exists with a non-equality correlation", exists(one(select(orders(), Expr::binary(BinaryOp::And,
            Expr::binary(BinaryOp::Gt, order_amount().expression(), parameter(0, &customer_region())),
            Expr::unary(UnaryOp::IsNotNull, order_customer().expression())))), vec![(0, customer_region().expression())], false, None), true),
        ("exists with an outer conjunct", exists(one(customer_orders()), by_customer(), false,
            Some(Expr::binary(BinaryOp::Gt, customer_region().expression(), literal(10)))), true),
        ("exists over distinct and limit", exists(Arc::new(Arc::clone(&distinct).limit(None, Some(literal(1))).unwrap()), by_customer(), false, None), true),
        ("not exists over distinct and limit", exists(Arc::new(Arc::clone(&distinct).limit(None, Some(literal(1))).unwrap()), by_customer(), true, None), true),
        ("lateral inner", keep(apply(&customers(), customer_orders(), by_customer(), ApplyKind::Inner, None),
            &[customer_id(), customer_region(), order_customer(), order_amount()]), true),
        ("lateral left", apply(&customers(), customer_orders(), by_customer(), ApplyKind::Left, None), true),
        ("scalar count", scalar(aggregate(customer_orders(), vec![counted(count.clone(), AggregateFunction::CountStar, None, None)]), by_customer()), true),
        ("scalar sum", scalar(Arc::clone(&summed), by_customer()), true),
        ("scalar sum plus one", scalar(Arc::new(summed.project(vec![NamedExpr {
            field: plus_one.clone(), expr: Expr::binary(BinaryOp::Add, total.expression(), literal(1)),
        }]).unwrap()), by_customer()), true),
        ("scalar count with a correlated filter", scalar(aggregate(customer_orders(), vec![
            counted(count, AggregateFunction::Count, Some(order_amount().expression()), Some(large)),
        ]), vec![(0, customer_id().expression()), (1, customer_region().expression())]), true),
        ("in", quantified(false, true), true),
        ("not in", quantified(true, true), true),
        ("in as a value", quantified(false, false), true),
        ("scalar subquery of several rows", scalar(keep(customer_orders(), &[order_amount()]), by_customer()), false),
    ]
}

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 26), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}

/// The query's rows in a canonical order, or its error code.
async fn run(query: &PreparedQuery) -> std::result::Result<Vec<String>, String> {
    let context = context();
    let rows = async {
        let mut cursor = query.execute(Arc::clone(&context), vec![]).await?;
        let mut rows = Vec::new();
        while let Some(batch) = cursor.next_batch().await? {
            for row in 0..batch.batch().num_rows() {
                let values = batch.batch().schema().fields().iter().zip(batch.batch().columns())
                    .map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<Vec<Value>>>()?;
                rows.push(format!("{values:?}"));
            }
        }
        Ok::<_, rtdb_types::Anomaly>(rows)
    }.await;
    match rows {
        Ok(mut rows) => { rows.sort(); Ok(rows) }
        Err(error) => Err(error.code.to_string()),
    }
}

/// The query prepared without decorrelation and with it.
fn both_ways(relation: &Arc<Relation>) -> (PreparedQuery, PreparedQuery) {
    DECORRELATES.with(|enabled| enabled.set(false));
    let correlated = PreparedQuery::new(Arc::clone(relation));
    DECORRELATES.with(|enabled| enabled.set(true));
    (correlated.unwrap(), PreparedQuery::new(Arc::clone(relation)).unwrap())
}

fn count_nodes(relation: &Relation, matches: &impl Fn(&RelationNode) -> bool) -> usize {
    usize::from(matches(relation.node())) + relation.children().into_iter().map(|child| count_nodes(child, matches)).sum::<usize>()
}
fn applies(relation: &Relation) -> usize { count_nodes(relation, &|node: &RelationNode| matches!(node, RelationNode::Apply { .. })) }
fn joins(relation: &Relation, kind: JoinKind) -> usize {
    count_nodes(relation, &|node: &RelationNode| matches!(node, RelationNode::Join { kind: found, .. } if *found == kind))
}

#[tokio::test]
async fn correlated_queries_return_the_same_rows_and_errors_decorrelated() {
    for (name, relation, decorrelated) in corpus() {
        let (correlated, rewritten) = both_ways(&relation);
        assert_eq!(applies(correlated.relation()), applies(&relation), "{name}: the reference plan keeps its applies");
        assert_eq!(applies(rewritten.relation()) == 0, decorrelated, "{name}: {:?}", rewritten.relation());
        assert_eq!(rewritten.relation().schema(), relation.schema(), "{name}");
        assert_eq!(run(&rewritten).await, run(&correlated).await, "{name}");
    }
}

#[tokio::test]
async fn a_scalar_subquery_of_several_rows_still_fails_its_cardinality_check() {
    let relation = scalar(keep(customer_orders(), &[order_amount()]), by_customer());
    let (correlated, rewritten) = both_ways(&relation);
    let violation = crate::cardinality_violation().code.to_string();
    assert_eq!(run(&correlated).await, Err(violation.clone()));
    assert_eq!(run(&rewritten).await, Err(violation));
}

#[test]
fn a_selected_exists_becomes_a_semi_join_and_its_negation_an_anti_join() {
    let semi = prepare(exists(one(customer_orders()), by_customer(), false, None)).unwrap();
    assert_eq!((applies(&semi), joins(&semi, JoinKind::Semi)), (0, 1), "{semi:?}");
    let anti = prepare(exists(one(customer_orders()), by_customer(), true, None)).unwrap();
    assert_eq!((applies(&anti), joins(&anti, JoinKind::Anti)), (0, 1), "{anti:?}");
}

#[test]
fn a_selected_quantified_comparison_becomes_a_semi_or_anti_join() {
    let semi = prepare(quantified(false, true)).unwrap();
    assert_eq!((applies(&semi), joins(&semi, JoinKind::Semi)), (0, 1), "{semi:?}");
    let anti = prepare(quantified(true, true)).unwrap();
    assert_eq!((applies(&anti), joins(&anti, JoinKind::Anti)), (0, 1), "{anti:?}");
}

#[test]
fn a_correlated_aggregate_becomes_a_grouped_left_join_over_numbered_input_rows() {
    let count = typed(12, "count", int());
    let prepared = prepare(scalar(aggregate(customer_orders(), vec![counted(count, AggregateFunction::CountStar, None, None)]), by_customer())).unwrap();
    assert_eq!(applies(&prepared), 0, "{prepared:?}");
    assert_eq!(joins(&prepared, JoinKind::Left), 1, "{prepared:?}");
    assert_eq!(count_nodes(&prepared, &|node: &RelationNode| matches!(node, RelationNode::Ordinality { scope: rtdb_plan::OrdinalityScope::Relation, .. })), 1);
    assert_eq!(count_nodes(&prepared, &|node: &RelationNode| matches!(node, RelationNode::Aggregate { groups, .. } if groups.len() == 3)), 1,
        "grouped by both customer columns and the row number");
}

#[test]
fn a_dependent_plan_with_a_volatile_call_keeps_its_apply() {
    let random = Expr::Call { name: "random".into(), args: Vec::new(), return_type: LogicalType::Scalar(ScalarType::F64) };
    let dependent = select(orders(), Expr::binary(BinaryOp::And,
        equals(order_customer().expression(), parameter(0, &customer_id())), Expr::binary(BinaryOp::Gt, random, Expr::Literal(Value::F64(0.5)))));
    let prepared = prepare(exists(one(dependent), by_customer(), false, None)).unwrap();
    assert_eq!(applies(&prepared), 1, "{prepared:?}");
}

/// Five tables of different sizes, with duplicates and nulls, whose first `count` are crossed and selected on a chain of
/// equalities, as the SQL binder plans `FROM a, b, ... WHERE ...`.
fn chain(count: usize) -> Arc<Relation> {
    let fields = (20..30).map(|id| integer(id, &format!("c{id}"))).collect::<Vec<_>>();
    let tables = [
        values(vec![fields[0].clone(), fields[1].clone()], &[&[Some(1), Some(1)], &[Some(1), Some(2)], &[Some(2), None], &[None, Some(1)], &[Some(3), Some(3)]]),
        values(vec![fields[2].clone(), fields[3].clone()], &[&[Some(1), Some(7)], &[Some(2), Some(8)]]),
        values(vec![fields[4].clone(), fields[5].clone()], &[&[Some(7), Some(1)], &[Some(7), Some(1)], &[Some(8), Some(2)], &[None, Some(2)], &[Some(9), Some(3)], &[Some(8), None]]),
        values(vec![fields[6].clone(), fields[7].clone()], &[&[Some(1), Some(5)], &[Some(2), Some(6)], &[Some(2), Some(6)]]),
        values(vec![fields[8].clone(), fields[9].clone()], &[&[Some(5), Some(0)], &[Some(6), Some(0)], &[None, Some(0)], &[Some(6), Some(1)]]),
    ];
    let links = [(0, 2), (3, 4), (5, 6), (7, 8)].map(|(left, right): (usize, usize)| equals(fields[left].expression(), fields[right].expression()));
    let crossed = tables.into_iter().take(count).reduce(|left, right| Arc::new(Relation::join(left, right, JoinKind::Cross, None).unwrap())).unwrap();
    select(crossed, links.into_iter().take(count - 1).reduce(|left, right| Expr::binary(BinaryOp::And, left, right)).unwrap())
}

fn statement(statement: rtdb_plan::Statement) -> rtdb_plan::PreparedStatement {
    rtdb_plan::PreparedStatement { statement, parameter_types: BTreeMap::new(), source: String::new(), notices: Vec::new(), settings: rtdb_plan::QuerySettings::default() }
}

fn first_leaf(relation: &Relation) -> &Relation {
    match relation.node() {
        RelationNode::Join { left, .. } => first_leaf(left),
        RelationNode::Project { input, .. } | RelationNode::Filter { input, .. } => first_leaf(input),
        _ => relation,
    }
}

#[tokio::test]
async fn inner_join_runs_return_the_same_rows_ordered_by_row_counts() {
    let context = context();
    for count in 2..=5 {
        let relation = chain(count);
        let written = PreparedQuery::new(Arc::clone(&relation)).unwrap();
        let counted = PreparedQuery::from_prepared_in(statement(rtdb_plan::Statement::Query(Arc::clone(&relation))), &context.catalog, &context.memory).unwrap();
        assert_eq!(counted.relation().schema(), relation.schema(), "{count} tables keep their column order");
        assert!(first_leaf(counted.relation()).schema().get(ColumnId(22)).is_some(), "{count} tables start from the smallest: {:?}", counted.relation());
        assert_eq!(run(&counted).await, run(&written).await, "{count} tables");
        let explained = PreparedQuery::from_prepared_in(statement(rtdb_plan::Statement::Explain { relation: Arc::clone(&relation), mode: rtdb_plan::ExplainMode::Plan }), &context.catalog, &context.memory).unwrap();
        assert_eq!(explained.relation(), counted.relation(), "EXPLAIN describes the plan that runs");
    }
}

#[test]
fn relations_without_estimates_keep_the_written_order() {
    struct Unknown;
    impl TableStatistics for Unknown { fn row_count(&self, _: &rtdb_plan::QualifiedTable) -> Option<u64> { None } }
    let [a, b, c] = [0, 1, 2].map(|id| integer(id, &format!("c{id}")));
    let scan = |field: &Field| {
        use rtdb_plan::{QualifiedTable, Scan, TableBinding, TemporalFilter};
        let table = TableBinding { name: QualifiedTable { database: "xtdb".into(), table: rtdb_types::TableRef::new("public", "docs") }, oid: None,
            columns: [(field.name.clone(), field.data_type.clone())].into_iter().collect() };
        Arc::new(Relation::scan(Scan { table, columns: vec![field.clone()], valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default,
            clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new() }).unwrap())
    };
    let crossed = Arc::new(Relation::join(Arc::new(Relation::join(scan(&a), scan(&b), JoinKind::Cross, None).unwrap()), scan(&c), JoinKind::Cross, None).unwrap());
    let relation = select(crossed, Expr::binary(BinaryOp::And, equals(a.expression(), b.expression()), equals(b.expression(), c.expression())));
    assert_eq!(prepare_with_statistics(Arc::clone(&relation), &Unknown).unwrap(), prepare(relation).unwrap());
}
