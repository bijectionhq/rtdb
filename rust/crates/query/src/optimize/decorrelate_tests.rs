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

fn grouped(input: Arc<Relation>, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr>) -> Arc<Relation> {
    let schema = Schema::new(groups.iter().map(|group| group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect()).unwrap();
    Arc::new(Relation::new(schema, RelationNode::Aggregate { input, groups, aggregates }).unwrap())
}

fn lookup_key() -> Field { integer(30, "key") }
fn lookup_name() -> Field { integer(31, "name") }

/// A table with at most one row per key, and keys no customer has.
fn lookup() -> Arc<Relation> {
    values(vec![lookup_key(), lookup_name()], &[&[Some(1), Some(100)], &[Some(3), None], &[Some(6), Some(600)], &[None, Some(700)]])
}

/// The lookup row of the customer bound to parameter 0.
fn customer_lookup() -> Arc<Relation> { select(lookup(), equals(lookup_key().expression(), parameter(0, &customer_id()))) }

fn tags() -> Field { typed(40, "tags", LogicalType::list(int().maybe()).maybe()) }

/// Customers with a list of tags each: several, none, null, and one with a null element; one customer appears twice.
fn tagged() -> Arc<Relation> {
    let list = |elements: &[Option<i64>]| Expr::Literal(Value::list(elements.iter().map(|element| element.map_or(Value::Null, Value::I64)).collect()));
    let rows = vec![
        vec![literal(1), list(&[Some(10), Some(20)])], vec![literal(2), list(&[])], vec![literal(3), Expr::Literal(Value::Null)],
        vec![literal(4), list(&[Some(30), None])], vec![literal(4), list(&[Some(30), None])],
    ];
    Arc::new(Relation::new(Schema::new(vec![customer_id(), tags()]).unwrap(), RelationNode::Values { rows }).unwrap())
}

/// `SELECT t.*, u.* FROM t JOIN LATERAL UNNEST(t.tags) [WITH ORDINALITY] AS u ON <selected>`, as an apply over the tag
/// table, binding the tags to parameter 0 and the customer to parameter 1.
fn unnested(numbered: bool, selected: Option<Expr>) -> Arc<Relation> {
    let element = integer(41, "tag");
    let table = Arc::new(Relation::new(Schema::new(vec![element.clone()]).unwrap(), RelationNode::ColumnTable { expr: parameter(0, &tags()), element }).unwrap());
    let table = if numbered { Arc::new(table.with_ordinality(typed(42, "n", int()), rtdb_plan::OrdinalityScope::Page).unwrap()) } else { table };
    let table = match selected { Some(predicate) => select(table, predicate), None => table };
    apply(&tagged(), table, vec![(0, tags().expression()), (1, customer_id().expression())], ApplyKind::Inner, None)
}

fn owner() -> Field { integer(50, "owner") }
fn owned_tags() -> Field { typed(51, "owned_tags", LogicalType::list(int().maybe()).maybe()) }
fn owned_tag() -> Field { integer(52, "owned_tag") }

/// Tag lists by owner: two lists for one customer, an empty and a null list, a null owner, a list with a null element, and
/// an owner who is no customer.
fn tag_lists() -> Arc<Relation> {
    let list = |elements: &[Option<i64>]| Expr::Literal(Value::list(elements.iter().map(|element| element.map_or(Value::Null, Value::I64)).collect()));
    let rows = vec![
        vec![literal(1), list(&[Some(10), Some(20)])], vec![literal(1), list(&[Some(30)])], vec![literal(2), list(&[])],
        vec![literal(3), Expr::Literal(Value::Null)], vec![Expr::Literal(Value::Null), list(&[Some(40)])],
        vec![literal(5), list(&[Some(60), None])], vec![literal(9), list(&[Some(90)])],
    ];
    Arc::new(Relation::new(Schema::new(vec![owner(), owned_tags()]).unwrap(), RelationNode::Values { rows }).unwrap())
}

/// The elements of the non-null tag lists of the customer bound to parameter 0, with their positions where `numbered`, and
/// a row for each empty list where `outer`: the correlated selection is beneath the unnest.
fn customer_tags(numbered: bool, outer: bool) -> Arc<Relation> {
    let lists = select(tag_lists(), Expr::binary(BinaryOp::And,
        equals(owner().expression(), parameter(0, &customer_id())), Expr::unary(UnaryOp::IsNotNull, owned_tags().expression())));
    let ordinality = numbered.then(|| typed(53, "position", LogicalType::Scalar(ScalarType::I32).maybe()));
    let fields = lists.schema().fields().iter().cloned().chain([owned_tag()]).chain(ordinality.clone()).collect();
    Arc::new(Relation::new(Schema::new(fields).unwrap(), RelationNode::Unnest {
        input: lists, expr: owned_tags().expression(), element: owned_tag(), ordinality, outer,
    }).unwrap())
}

fn per_customer() -> Field { typed(25, "per_customer", int()) }

/// `SELECT o.customer, count(*) FROM o WHERE o.customer = ? AND o.amount > 40 GROUP BY o.customer`: the correlated
/// selection reads only the grouping column, and the uncorrelated one a column the grouping drops.
fn customer_group() -> Arc<Relation> {
    let selected = select(orders(), Expr::binary(BinaryOp::And,
        equals(order_customer().expression(), parameter(0, &customer_id())), Expr::binary(BinaryOp::Gt, order_amount().expression(), literal(40))));
    grouped(selected, vec![NamedExpr { field: integer(24, "customer_group"), expr: order_customer().expression() }],
        vec![counted(per_customer(), AggregateFunction::CountStar, None, None)])
}

/// Orders of the customer bound to parameter 0 whose amount plus one exceeds the region bound to parameter 1, selected on
/// a computed column the dependent plan does not return, as a period constructor is.
fn shifted_orders() -> Arc<Relation> {
    let shifted = integer(26, "shifted");
    let extended = Arc::new(orders().extend(vec![NamedExpr { field: shifted.clone(), expr: Expr::binary(BinaryOp::Add, order_amount().expression(), literal(1)) }]).unwrap());
    keep(select(extended, Expr::binary(BinaryOp::And,
        equals(order_customer().expression(), parameter(0, &customer_id())), Expr::binary(BinaryOp::Gt, shifted.expression(), parameter(1, &customer_region())))),
        &[order_customer(), order_amount()])
}

/// Each correlated query, and whether its applies are all rewritten into joins.
fn corpus() -> Vec<(&'static str, Arc<Relation>, bool)> {
    let count = typed(12, "count", int());
    let total = integer(12, "total");
    let plus_one = integer(20, "plus_one");
    let summed = aggregate(customer_orders(), vec![counted(total.clone(), AggregateFunction::Sum, Some(order_amount().expression()), None)]);
    let large = Expr::binary(BinaryOp::Gt, order_amount().expression(), parameter(1, &customer_region()));
    let distinct = Arc::new(Relation::new(Schema::new(vec![order_customer()]).unwrap(), RelationNode::Distinct { input: keep(customer_orders(), &[order_customer()]) }).unwrap());
    let amount_group = integer(21, "amount_group");
    let per_amount = grouped(customer_orders(), vec![NamedExpr { field: amount_group.clone(), expr: order_amount().expression() }],
        vec![counted(typed(22, "per_amount", int()), AggregateFunction::CountStar, None, None)]);
    let shifted = integer(23, "shifted");
    let per_amount_shifted = Arc::new(Arc::clone(&per_amount).project(vec![
        NamedExpr { field: amount_group.clone(), expr: amount_group.expression() },
        NamedExpr { field: shifted.clone(), expr: Expr::binary(BinaryOp::Add, typed(22, "per_amount", int()).expression(), parameter(1, &customer_region())) },
    ]).unwrap());
    let defaulted = Expr::Coalesce(vec![lookup_name().expression(), literal(0)]);
    let defaulted_name = Arc::new(customer_lookup().project(vec![NamedExpr { field: typed(32, "defaulted", defaulted.data_type()), expr: defaulted }]).unwrap());
    let correlated_lookup_value = Arc::new(customer_lookup().project(vec![
        NamedExpr { field: integer(33, "summed"), expr: Expr::binary(BinaryOp::Add, Expr::Coalesce(vec![lookup_name().expression(), literal(0)]), parameter(1, &customer_region())) },
    ]).unwrap());
    let outer_only_scalar = Arc::new(customer_lookup().project(vec![
        NamedExpr { field: integer(34, "outer_region"), expr: parameter(1, &customer_region()) },
    ]).unwrap());
    let mark_field = typed(60, "_mark_60", bool_type().maybe());
    let exists_field = typed(61, "_exists_61", bool_type());
    let empty_orders = values(vec![order_customer(), order_amount()], &[]);
    let single_row = values(vec![lookup_key(), lookup_name()], &[&[Some(9), Some(900)]]);
    let mark_dep = Arc::new(select(orders(), Expr::binary(BinaryOp::Gt, order_amount().expression(), parameter(1, &customer_region())))
        .project(vec![NamedExpr { field: typed(62, "cmp", bool_type().maybe()), expr: equals(parameter(0, &customer_id()), order_customer().expression()) }]).unwrap());
    let uncorrelated_mark_dep = Arc::new(orders().project(vec![
        NamedExpr { field: typed(63, "flag", bool_type().maybe()), expr: Expr::binary(BinaryOp::Gt, order_amount().expression(), literal(120)) },
    ]).unwrap());
    let correlated_mark_rel = Arc::new(Relation::new(
        customers().schema().concat(&Schema::new(vec![mark_field.clone()]).unwrap()).unwrap(),
        RelationNode::Mark {
            input: customers(),
            dependent: select(orders(), Expr::binary(BinaryOp::Gt, order_amount().expression(), parameter(0, &customer_region()))),
            arguments: vec![(0, customer_region().expression())],
            condition: None,
            comparison: equals(customer_id().expression(), order_customer().expression()),
            output: mark_field.clone(),
        },
    ).unwrap());
    let uncorrelated_mark_rel = Arc::new(Relation::new(
        customers().schema().concat(&Schema::new(vec![mark_field.clone()]).unwrap()).unwrap(),
        RelationNode::Mark {
            input: customers(),
            dependent: orders(),
            arguments: Vec::new(),
            condition: None,
            comparison: equals(customer_id().expression(), order_customer().expression()),
            output: mark_field.clone(),
        },
    ).unwrap());
    let left_only_orders = select(orders(), Expr::binary(BinaryOp::Gt, parameter(0, &customer_region()), literal(10)));
    let left_only_lookup = select(single_row.clone(), Expr::binary(BinaryOp::Gt, parameter(0, &customer_region()), literal(10)));
    let count_per_amount = typed(22, "per_amount", int());
    let having_uncorrelated = select(Arc::clone(&per_amount), Expr::binary(BinaryOp::Gt, count_per_amount.expression(), literal(1)));
    let having_correlated = select(Arc::clone(&per_amount), Expr::binary(BinaryOp::Lt, count_per_amount.expression(), parameter(1, &customer_region())));
    let nested_agg_project = Arc::new(Arc::new(summed.clone().project(vec![
        NamedExpr { field: integer(64, "inner_total"), expr: total.expression() },
    ]).unwrap()).project(vec![
        NamedExpr { field: integer(65, "outer_total"), expr: Expr::binary(BinaryOp::Add, integer(64, "inner_total").expression(), parameter(1, &customer_region())) },
    ]).unwrap());
    let uncorr_agg_corr_proj = Arc::new(aggregate(orders(), vec![counted(count.clone(), AggregateFunction::CountStar, None, None)])
        .project(vec![NamedExpr { field: integer(66, "shifted_count"), expr: Expr::binary(BinaryOp::Add, count.expression(), parameter(0, &customer_region())) }]).unwrap());
    let ungrouped_count = aggregate(customer_orders(), vec![counted(count.clone(), AggregateFunction::CountStar, None, None)]);
    let ungrouped_having = select(Arc::clone(&ungrouped_count), Expr::binary(BinaryOp::Gt, count.expression(), literal(0)));
    let ungrouped_array_agg = aggregate(customer_orders(), vec![
        counted(typed(67, "amounts", LogicalType::list(int().maybe()).maybe()), AggregateFunction::ArrayAgg, Some(order_amount().expression()), None),
    ]);
    let tag_element = integer(41, "tag");
    let tag_table = Arc::new(Relation::new(Schema::new(vec![tag_element.clone()]).unwrap(), RelationNode::ColumnTable { expr: parameter(0, &tags()), element: tag_element.clone() }).unwrap());
    let tag_table_ord = Arc::new(Arc::clone(&tag_table).with_ordinality(typed(42, "n", int()), rtdb_plan::OrdinalityScope::Page).unwrap());
    let scalar_unnest_count = apply(
        &tagged(),
        aggregate(Arc::clone(&tag_table), vec![counted(count.clone(), AggregateFunction::CountStar, None, None)]),
        vec![(0, tags().expression())],
        ApplyKind::Scalar,
        Some(typed(68, "_sq_68", int().maybe())),
    );
    let scalar_unnest_filtered_sum = apply(
        &tagged(),
        aggregate(select(Arc::clone(&tag_table), Expr::binary(BinaryOp::Gt, tag_element.expression(), literal(10))), vec![
            counted(total.clone(), AggregateFunction::Sum, Some(tag_element.expression()), None),
        ]),
        vec![(0, tags().expression())],
        ApplyKind::Scalar,
        Some(integer(69, "_sq_69")),
    );
    let scalar_unnest_null_filter = apply(
        &tagged(),
        aggregate(select(Arc::clone(&tag_table), Expr::unary(UnaryOp::IsNull, tag_element.expression())), vec![
            counted(count.clone(), AggregateFunction::CountStar, None, None),
        ]),
        vec![(0, tags().expression())],
        ApplyKind::Scalar,
        Some(typed(70, "_sq_70", int().maybe())),
    );
    let corr_on_left_join_lhs = Arc::new(Relation::join(customer_orders(), lookup(), JoinKind::Left, Some(equals(order_amount().expression(), lookup_name().expression()))).unwrap());
    let corr_on_semi_join_lhs = Arc::new(Relation::join(customer_orders(), lookup(), JoinKind::Semi, Some(equals(order_amount().expression(), lookup_name().expression()))).unwrap());
    let corr_on_anti_join_lhs = Arc::new(Relation::join(customer_orders(), lookup(), JoinKind::Anti, Some(equals(order_amount().expression(), lookup_name().expression()))).unwrap());
    let corr_on_right_join_rhs = Arc::new(Relation::join(lookup(), customer_orders(), JoinKind::Right, Some(equals(lookup_name().expression(), order_amount().expression()))).unwrap());
    let corr_on_mark_input = Arc::new(Relation::new(
        customer_orders().schema().concat(&Schema::new(vec![mark_field.clone()]).unwrap()).unwrap(),
        RelationNode::Mark {
            input: customer_orders(),
            dependent: lookup(),
            arguments: Vec::new(),
            condition: None,
            comparison: equals(order_amount().expression(), lookup_name().expression()),
            output: mark_field.clone(),
        },
    ).unwrap());
    // Pulling a correlated selection out of the RHS of a LEFT OUTER JOIN is unsound: unmatched LHS rows would be dropped.
    let corr_on_left_join_rhs = Arc::new(Relation::join(lookup(), customer_orders(), JoinKind::Left, Some(equals(lookup_name().expression(), order_amount().expression()))).unwrap());
    let fallible_unnest_input = values(vec![integer(72, "x")], &[&[Some(1)], &[Some(0)]]);
    let div_list = Expr::List(vec![Expr::binary(BinaryOp::Divide, literal(1), integer(72, "x").expression())]);
    let staged_div = typed(73, "_unnest_div", div_list.data_type());
    let fallible_unnest_elem = integer(74, "v");
    let fallible_unnest = Arc::new(Relation::new(
        Schema::new(vec![integer(72, "x"), staged_div.clone(), fallible_unnest_elem.clone()]).unwrap(),
        RelationNode::Unnest {
            input: Arc::new(fallible_unnest_input.extend(vec![NamedExpr { field: staged_div.clone(), expr: div_list }]).unwrap()),
            expr: staged_div.expression(),
            element: fallible_unnest_elem.clone(),
            ordinality: None,
            outer: false,
        },
    ).unwrap());
    let fallible_unnest_dep = keep(select(fallible_unnest, equals(integer(72, "x").expression(), parameter(0, &customer_id()))), &[fallible_unnest_elem]);
    let uncorr_distinct_limit_corr_proj = Arc::new(
        Arc::new(Arc::new(Relation::new(Schema::new(vec![order_amount()]).unwrap(), RelationNode::Distinct { input: keep(orders(), &[order_amount()]) }).unwrap())
            .limit(None, Some(literal(3))).unwrap())
            .project(vec![NamedExpr { field: integer(71, "shifted_amt"), expr: Expr::binary(BinaryOp::Add, order_amount().expression(), parameter(0, &customer_region())) }]).unwrap()
    );
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
        ("exists as a value", apply(&customers(), one(customer_orders()), by_customer(), ApplyKind::Exists, Some(exists_field.clone())), true),
        ("uncorrelated exists as a value", apply(&customers(), one(orders()), by_customer(), ApplyKind::Exists, Some(exists_field.clone())), true),
        ("uncorrelated exists over empty dependent", apply(&customers(), one(Arc::clone(&empty_orders)), by_customer(), ApplyKind::Exists, Some(exists_field.clone())), true),
        ("uncorrelated filtered exists", exists(one(orders()), by_customer(), false, None), true),
        ("uncorrelated filtered not exists", exists(one(Arc::clone(&empty_orders)), by_customer(), true, None), true),
        ("exists with a left-only correlation", exists(one(Arc::clone(&left_only_orders)), vec![(0, customer_region().expression())], false, None), true),
        ("not exists with a left-only correlation", exists(one(Arc::clone(&left_only_orders)), vec![(0, customer_region().expression())], true, None), true),
        ("uncorrelated left apply", apply(&customers(), lookup(), by_customer(), ApplyKind::Left, None), true),
        ("uncorrelated left apply over empty dependent", apply(&customers(), Arc::clone(&empty_orders), by_customer(), ApplyKind::Left, None), true),
        ("uncorrelated semi apply", apply(&customers(), lookup(), by_customer(), ApplyKind::Semi, None), true),
        ("uncorrelated semi apply over empty dependent", apply(&customers(), Arc::clone(&empty_orders), by_customer(), ApplyKind::Semi, None), true),
        ("uncorrelated anti apply", apply(&customers(), lookup(), by_customer(), ApplyKind::Anti, None), true),
        ("uncorrelated anti apply over empty dependent", apply(&customers(), Arc::clone(&empty_orders), by_customer(), ApplyKind::Anti, None), true),
        ("uncorrelated mark apply", apply(&customers(), uncorrelated_mark_dep, by_customer(), ApplyKind::Mark, Some(mark_field.clone())), true),
        ("correlated mark apply as a value", apply(&customers(), Arc::clone(&mark_dep), vec![(0, customer_id().expression()), (1, customer_region().expression())], ApplyKind::Mark, Some(mark_field.clone())), true),
        ("filtered correlated mark apply", keep(select(apply(&customers(), Arc::clone(&mark_dep), vec![(0, customer_id().expression()), (1, customer_region().expression())], ApplyKind::Mark, Some(mark_field.clone())), mark_field.expression()), &[customer_id(), customer_region()]), true),
        ("negated filtered correlated mark apply", keep(select(apply(&customers(), mark_dep, vec![(0, customer_id().expression()), (1, customer_region().expression())], ApplyKind::Mark, Some(mark_field.clone())), Expr::unary(UnaryOp::Not, mark_field.expression())), &[customer_id(), customer_region()]), true),
        ("correlated mark relation as a value", Arc::clone(&correlated_mark_rel), true),
        ("filtered correlated mark relation", keep(select(correlated_mark_rel, mark_field.expression()), &[customer_id(), customer_region()]), true),
        ("filtered uncorrelated mark relation", keep(select(Arc::clone(&uncorrelated_mark_rel), mark_field.expression()), &[customer_id(), customer_region()]), true),
        ("negated filtered uncorrelated mark relation", keep(select(uncorrelated_mark_rel, Expr::unary(UnaryOp::Not, mark_field.expression())), &[customer_id(), customer_region()]), true),
        ("lateral inner", keep(apply(&customers(), customer_orders(), by_customer(), ApplyKind::Inner, None),
            &[customer_id(), customer_region(), order_customer(), order_amount()]), true),
        ("lateral inner with dependent-only outer filter", select(apply(&customers(), customer_orders(), by_customer(), ApplyKind::Inner, None), Expr::binary(BinaryOp::Gt, order_amount().expression(), literal(50))), true),
        ("lateral left", apply(&customers(), customer_orders(), by_customer(), ApplyKind::Left, None), true),
        ("lateral left with smaller right input", apply(&customers(), customer_lookup(), by_customer(), ApplyKind::Left, None), true),
        ("lateral left with computed dependent column", apply(&customers(), Arc::clone(&defaulted_name), by_customer(), ApplyKind::Left, None), true),
        ("lateral left with correlated computed dependent column", apply(&customers(), Arc::clone(&correlated_lookup_value), vec![(0, customer_id().expression()), (1, customer_region().expression())], ApplyKind::Left, None), true),
        ("lateral single with correlated computed dependent column", apply(&customers(), correlated_lookup_value, vec![(0, customer_id().expression()), (1, customer_region().expression())], ApplyKind::Single, None), true),
        ("lateral left with a left-only correlation", apply(&customers(), left_only_lookup.clone(), vec![(0, customer_region().expression())], ApplyKind::Left, None), true),
        ("scalar count", scalar(aggregate(customer_orders(), vec![counted(count.clone(), AggregateFunction::CountStar, None, None)]), by_customer()), true),
        ("scalar count with a left-only correlation", scalar(aggregate(Arc::clone(&left_only_orders), vec![counted(count.clone(), AggregateFunction::CountStar, None, None)]), vec![(0, customer_region().expression())]), true),
        ("scalar sum", scalar(Arc::clone(&summed), by_customer()), true),
        ("scalar sum plus one", scalar(Arc::new(summed.project(vec![NamedExpr {
            field: plus_one.clone(), expr: Expr::binary(BinaryOp::Add, total.expression(), literal(1)),
        }]).unwrap()), by_customer()), true),
        ("scalar aggregate under nested projections", scalar(nested_agg_project, vec![(0, customer_id().expression()), (1, customer_region().expression())]), true),
        ("uncorrelated scalar aggregate with correlated projection", scalar(uncorr_agg_corr_proj, vec![(0, customer_region().expression())]), true),
        ("scalar count with a correlated filter", scalar(aggregate(customer_orders(), vec![
            counted(count, AggregateFunction::Count, Some(order_amount().expression()), Some(large)),
        ]), vec![(0, customer_id().expression()), (1, customer_region().expression())]), true),
        ("in", quantified(false, true), true),
        ("not in", quantified(true, true), true),
        ("in as a value", quantified(false, false), true),
        ("scalar subquery of several rows", scalar(keep(customer_orders(), &[order_amount()]), by_customer()), true),
        ("scalar value of at most one row", scalar(keep(customer_lookup(), &[lookup_name()]), by_customer()), true),
        ("scalar value with a left-only correlation", scalar(keep(left_only_lookup, &[lookup_name()]), vec![(0, customer_region().expression())]), true),
        ("scalar computed value", scalar(defaulted_name, by_customer()), true),
        ("scalar returning outer column stays null when unmatched", scalar(outer_only_scalar, vec![(0, customer_id().expression()), (1, customer_region().expression())]), true),
        ("lateral single", apply(&customers(), customer_lookup(), by_customer(), ApplyKind::Single, None), true),
        ("lateral grouped", apply(&customers(), Arc::clone(&per_amount), by_customer(), ApplyKind::Inner, None), true),
        ("lateral grouped with a correlated projection", apply(&customers(), per_amount_shifted,
            vec![(0, customer_id().expression()), (1, customer_region().expression())], ApplyKind::Inner, None), true),
        ("lateral grouped with uncorrelated having", apply(&customers(), having_uncorrelated, by_customer(), ApplyKind::Inner, None), true),
        ("lateral grouped with correlated having", apply(&customers(), having_correlated, vec![(0, customer_id().expression()), (1, customer_region().expression())], ApplyKind::Inner, None), true),
        ("lateral ungrouped aggregate", apply(&customers(), Arc::clone(&ungrouped_count), by_customer(), ApplyKind::Inner, None), true),
        ("lateral left ungrouped aggregate", apply(&customers(), Arc::clone(&ungrouped_count), by_customer(), ApplyKind::Left, None), true),
        ("lateral single ungrouped aggregate", apply(&customers(), ungrouped_count, by_customer(), ApplyKind::Single, None), true),
        ("lateral ungrouped aggregate with having", apply(&customers(), ungrouped_having, by_customer(), ApplyKind::Inner, None), true),
        ("lateral ungrouped collecting aggregate keeps apply", apply(&customers(), ungrouped_array_agg, by_customer(), ApplyKind::Inner, None), false),
        ("lateral correlated projection over uncorrelated distinct limit", apply(&customers(), uncorr_distinct_limit_corr_proj, vec![(0, customer_region().expression())], ApplyKind::Inner, None), true),
        ("lateral unnest", unnested(false, None), true),
        ("lateral unnest with ordinality", unnested(true, None), true),
        ("lateral unnest with a correlated selection", unnested(true, Some(Expr::binary(BinaryOp::Gt, integer(41, "tag").expression(), parameter(1, &customer_id())))), true),
        ("lateral left unnest", apply(&tagged(), Arc::clone(&tag_table), vec![(0, tags().expression())], ApplyKind::Left, None), true),
        ("lateral left unnest with ordinality", apply(&tagged(), tag_table_ord, vec![(0, tags().expression())], ApplyKind::Left, None), true),
        ("scalar count over lateral unnest", scalar_unnest_count, true),
        ("scalar filtered sum over lateral unnest", scalar_unnest_filtered_sum, true),
        ("scalar is-null count over lateral unnest", scalar_unnest_null_filter, true),
        ("lateral grouped over non-reflexive list input", apply(&tagged(), Arc::clone(&per_amount), by_customer(), ApplyKind::Inner, None), true),
        ("lateral unnest of a correlated selection", apply(&customers(), customer_tags(false, false), by_customer(), ApplyKind::Inner, None), true),
        ("lateral left outer unnest of a correlated selection with ordinality", apply(&customers(), customer_tags(true, true), by_customer(), ApplyKind::Left, None), true),
        ("exists over an unnest of a correlated selection", exists(one(customer_tags(false, false)), by_customer(), false, None), true),
        ("lateral grouped on the correlated column", apply(&customers(), customer_group(), by_customer(), ApplyKind::Inner, None), true),
        ("lateral left grouped on the correlated column", apply(&customers(), customer_group(), by_customer(), ApplyKind::Left, None), true),
        ("scalar count of a group on the correlated column", scalar(keep(customer_group(), &[per_customer()]), by_customer()), true),
        ("not exists over a group on the correlated column", exists(one(customer_group()), by_customer(), true, None), true),
        // The correlated selection reads a column the grouping drops, so it cannot move above it.
        ("lateral left grouped on another column", apply(&customers(), Arc::clone(&per_amount), by_customer(), ApplyKind::Left, None), false),
        ("lateral left over a computed column only its selection reads", apply(&customers(), shifted_orders(),
            vec![(0, customer_id().expression()), (1, customer_region().expression())], ApplyKind::Left, None), true),
        ("correlated selection on left outer join lhs", apply(&customers(), corr_on_left_join_lhs, by_customer(), ApplyKind::Left, None), true),
        ("correlated selection on semi join lhs", apply(&customers(), corr_on_semi_join_lhs, by_customer(), ApplyKind::Inner, None), true),
        ("correlated selection on anti join lhs", apply(&customers(), corr_on_anti_join_lhs, by_customer(), ApplyKind::Left, None), true),
        ("correlated selection on right outer join rhs", apply(&customers(), corr_on_right_join_rhs, by_customer(), ApplyKind::Inner, None), true),
        ("correlated selection on mark join input", apply(&customers(), corr_on_mark_input, by_customer(), ApplyKind::Left, None), true),
        ("correlated selection on left outer join rhs keeps apply", apply(&customers(), corr_on_left_join_rhs, by_customer(), ApplyKind::Inner, None), false),
        ("correlated selection guarding fallible unnest keeps apply", apply(&customers(), fallible_unnest_dep, by_customer(), ApplyKind::Left, None), false),
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
fn applies(relation: &Relation) -> usize {
    count_nodes(relation, &|node: &RelationNode| {
        matches!(node, RelationNode::Apply { .. })
            || matches!(node, RelationNode::Mark { arguments, .. } if !arguments.is_empty())
    })
}
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

#[tokio::test]
async fn a_correlated_filter_below_unnest_guards_list_evaluation() {
    let x = integer(72, "x");
    let input = values(vec![x.clone()], &[&[Some(1)], &[Some(0)]]);
    let filtered = select(input, equals(x.expression(), parameter(0, &customer_id())));
    let list = Expr::List(vec![Expr::binary(BinaryOp::Divide, literal(1), x.expression())]);
    let staged = typed(73, "_unnest_div", list.data_type());
    let elem = integer(74, "v");
    let input = Arc::new(filtered.extend(vec![NamedExpr { field: staged.clone(), expr: list }]).unwrap());
    let dependent = Arc::new(Relation::new(
        Schema::new(vec![x, staged.clone(), elem.clone()]).unwrap(),
        RelationNode::Unnest { input, expr: staged.expression(), element: elem.clone(), ordinality: None, outer: false },
    ).unwrap());
    let relation = apply(&customers(), keep(dependent, &[elem]), by_customer(), ApplyKind::Left, None);
    let (correlated, rewritten) = both_ways(&relation);
    assert_eq!(applies(rewritten.relation()), 1);
    let expected = run(&correlated).await.unwrap();
    assert_eq!(expected.len(), 6);
    assert_eq!(run(&rewritten).await.unwrap(), expected);
}

#[tokio::test]
async fn left_apply_unnests_through_a_column_projection_and_preserves_unmatched_rows() {
    let relation = unnested(true, None);
    let RelationNode::Apply { input, dependent, arguments, .. } = relation.node() else { panic!("Expected a lateral unnest"); };
    let [element, ordinal] = dependent.schema().fields() else { panic!("Expected element and ordinality"); };
    let columns = vec![
        NamedExpr { field: typed(43, "position", ordinal.data_type.clone()), expr: ordinal.expression() },
        NamedExpr { field: typed(44, "value", element.data_type.clone()), expr: element.expression() },
    ];
    let schema = Schema::new(columns.iter().map(|named| named.field.clone()).collect()).unwrap();
    let project = RelationNode::Project { input: Arc::clone(dependent), expressions: columns.clone() };
    let rename = RelationNode::Rename { input: Arc::clone(dependent), prefix: "tag".into(), expressions: columns };
    for node in [project, rename] {
        let projected = Arc::new(Relation::new(schema.clone(), node).unwrap());
        let relation = apply(input, projected, arguments.clone(), ApplyKind::Left, None);
        let (correlated, rewritten) = both_ways(&relation);
        assert_eq!(applies(rewritten.relation()), 0);
        assert_eq!(rewritten.relation().schema(), relation.schema());
        let expected = run(&correlated).await.unwrap();
        assert_eq!(expected.len(), 8);
        assert_eq!(run(&rewritten).await.unwrap(), expected);
    }
}

#[test]
fn a_selected_exists_becomes_a_semi_join_and_its_negation_an_anti_join() {
    let semi = prepare(exists(one(customer_orders()), by_customer(), false, None)).unwrap();
    assert_eq!((applies(&semi), joins(&semi, JoinKind::Semi)), (0, 1), "{semi:?}");
    let anti = prepare(exists(one(customer_orders()), by_customer(), true, None)).unwrap();
    assert_eq!((applies(&anti), joins(&anti, JoinKind::Anti)), (0, 1), "{anti:?}");
}

#[test]
fn a_volatile_selection_keeps_its_exists_conjunct() {
    let mark = typed(11, "_sq_11", bool_type());
    let applied = apply(&customers(), one(customer_orders()), by_customer(), ApplyKind::Exists, Some(mark.clone()));
    let random = Expr::Call { name: "random".into(), args: Vec::new(), return_type: LogicalType::Scalar(ScalarType::F64) };
    let predicate = Expr::binary(BinaryOp::And,
        Expr::binary(BinaryOp::Gt, random, Expr::Literal(Value::F64(0.5))), mark.expression());
    let prepared = prepare(select(applied, predicate.clone())).unwrap();
    assert_eq!(joins(&prepared, JoinKind::Semi), 0);
    assert_eq!(count_nodes(&prepared, &|node| matches!(node, RelationNode::Filter { predicate: kept, .. } if kept == &predicate)), 1);
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
fn a_correlated_scalar_value_becomes_a_single_join() {
    let prepared = prepare(scalar(keep(customer_lookup(), &[lookup_name()]), by_customer())).unwrap();
    assert_eq!((applies(&prepared), joins(&prepared, JoinKind::Single)), (0, 1), "{prepared:?}");
}

#[test]
fn a_correlated_grouped_lateral_becomes_a_grouping_over_numbered_input_rows() {
    let per_amount = grouped(customer_orders(), vec![NamedExpr { field: integer(21, "amount_group"), expr: order_amount().expression() }],
        vec![counted(typed(22, "per_amount", int()), AggregateFunction::CountStar, None, None)]);
    let prepared = prepare(apply(&customers(), per_amount, by_customer(), ApplyKind::Inner, None)).unwrap();
    assert_eq!(applies(&prepared), 0, "{prepared:?}");
    assert_eq!(count_nodes(&prepared, &|node: &RelationNode| matches!(node, RelationNode::Aggregate { groups, .. } if groups.len() == 4)), 1,
        "grouped by both customer columns, the row number and the subquery's own group");
}

#[test]
fn a_lateral_list_table_becomes_an_unnest() {
    let prepared = prepare(unnested(true, None)).unwrap();
    assert_eq!(applies(&prepared), 0, "{prepared:?}");
    assert_eq!(count_nodes(&prepared, &|node: &RelationNode| matches!(node, RelationNode::Unnest { ordinality: Some(_), outer: false, .. })), 1, "{prepared:?}");
}

#[test]
fn a_correlated_selection_on_a_grouping_column_moves_above_the_grouping() {
    let prepared = prepare(apply(&customers(), customer_group(), by_customer(), ApplyKind::Left, None)).unwrap();
    assert_eq!((applies(&prepared), joins(&prepared, JoinKind::Left)), (0, 1), "{prepared:?}");
    assert_eq!(count_nodes(&prepared, &|node: &RelationNode| matches!(node, RelationNode::Ordinality { .. })), 0, "no row numbers: {prepared:?}");
}

#[test]
fn a_correlated_selection_beneath_an_unnest_moves_above_it() {
    let prepared = prepare(apply(&customers(), customer_tags(true, true), by_customer(), ApplyKind::Left, None)).unwrap();
    assert_eq!((applies(&prepared), joins(&prepared, JoinKind::Left)), (0, 1), "{prepared:?}");
    assert_eq!(count_nodes(&prepared, &|node: &RelationNode| matches!(node, RelationNode::Unnest { outer: true, ordinality: Some(_), .. })), 1, "{prepared:?}");
}

#[test]
fn an_ungrouped_lateral_aggregate_becomes_a_grouped_left_join_unless_collecting() {
    let count = aggregate(customer_orders(), vec![counted(typed(12, "count", int()), AggregateFunction::CountStar, None, None)]);
    let prepared = prepare(apply(&customers(), count, by_customer(), ApplyKind::Inner, None)).unwrap();
    assert_eq!((applies(&prepared), joins(&prepared, JoinKind::Left)), (0, 1), "{prepared:?}");
    let collected = aggregate(customer_orders(), vec![
        counted(typed(12, "amounts", LogicalType::list(int().maybe()).maybe()), AggregateFunction::ArrayAgg, Some(order_amount().expression()), None),
    ]);
    let kept = prepare(apply(&customers(), collected, by_customer(), ApplyKind::Inner, None)).unwrap();
    assert_eq!(applies(&kept), 1, "{kept:?}");
}

#[test]
fn a_selected_mark_join_becomes_a_semi_join_and_standalone_exists_a_shared_mark() {
    let mark = typed(60, "_mark_60", bool_type().maybe());
    let marked = Arc::new(Relation::new(
        customers().schema().concat(&Schema::new(vec![mark.clone()]).unwrap()).unwrap(),
        RelationNode::Mark {
            input: customers(),
            dependent: orders(),
            arguments: Vec::new(),
            condition: None,
            comparison: equals(customer_id().expression(), order_customer().expression()),
            output: mark.clone(),
        },
    ).unwrap());
    let semi = prepare(keep(select(marked, mark.expression()), &[customer_id(), customer_region()])).unwrap();
    assert_eq!((applies(&semi), joins(&semi, JoinKind::Semi)), (0, 1), "{semi:?}");
    assert_eq!(count_nodes(&semi, &|node: &RelationNode| matches!(node, RelationNode::Mark { .. })), 0, "{semi:?}");

    let exists_field = typed(61, "_exists_61", bool_type());
    let projected = prepare(apply(&customers(), one(customer_orders()), by_customer(), ApplyKind::Exists, Some(exists_field))).unwrap();
    assert_eq!(applies(&projected), 0, "{projected:?}");
    assert_eq!(count_nodes(&projected, &|node: &RelationNode| matches!(node, RelationNode::Mark { arguments, .. } if arguments.is_empty())), 1, "{projected:?}");
}

#[test]
fn an_uncorrelated_aggregate_with_a_correlated_projection_uses_a_single_join_without_ordinality() {
    let count = typed(12, "count", int());
    let dependent = Arc::new(aggregate(orders(), vec![counted(count.clone(), AggregateFunction::CountStar, None, None)])
        .project(vec![NamedExpr { field: integer(66, "shifted"), expr: Expr::binary(BinaryOp::Add, count.expression(), parameter(0, &customer_region())) }]).unwrap());
    let prepared = prepare(scalar(dependent, vec![(0, customer_region().expression())])).unwrap();
    assert_eq!((applies(&prepared), joins(&prepared, JoinKind::Single)), (0, 1), "{prepared:?}");
    assert_eq!(count_nodes(&prepared, &|node: &RelationNode| matches!(node, RelationNode::Ordinality { .. })), 0, "{prepared:?}");
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
fn join_reordering_keeps_volatile_predicates_at_their_original_cardinality() {
    let [a, b, c] = [20, 21, 22].map(|id| integer(id, &format!("c{id}")));
    let a_rows = values(vec![a.clone()], &[&[Some(1)], &[Some(2)], &[Some(3)], &[Some(4)]]);
    let b_rows = values(vec![b.clone()], &[&[Some(1)], &[Some(2)], &[Some(3)]]);
    let c_rows = values(vec![c.clone()], &[&[Some(1)], &[Some(2)]]);
    let random = Expr::Call { name: "random".into(), args: Vec::new(), return_type: LogicalType::Scalar(ScalarType::F64) };
    let condition = Expr::binary(BinaryOp::And, equals(a.expression(), b.expression()),
        Expr::binary(BinaryOp::Gt, random.clone(), Expr::Literal(Value::F64(0.5))));
    let first = Arc::new(Relation::join(a_rows, b_rows, JoinKind::Inner, Some(condition.clone())).unwrap());
    let joined = Arc::new(Relation::join(first, c_rows, JoinKind::Cross, None).unwrap());
    let filter = Expr::binary(BinaryOp::Gt, random, a.expression());
    let relation = select(joined, filter.clone());
    let context = context();
    let query = PreparedQuery::from_prepared_in(statement(rtdb_plan::Statement::Query(relation)), &context.catalog, &context.memory).unwrap();
    let RelationNode::Filter { input, predicate } = query.relation().node() else { panic!("Expected a selection above the join run"); };
    assert_eq!(predicate, &filter);
    assert_eq!(count_nodes(input, &|node| matches!(node, RelationNode::Join { left, right, condition: Some(predicate), .. }
        if predicate == &condition && left.schema().fields() == [a.clone()] && right.schema().fields() == [b.clone()])), 1);
}

#[test]
fn relations_without_estimates_keep_the_written_order() {
    struct Unknown;
    impl TableStatistics for Unknown { fn row_count(&mut self, _: &QualifiedTable) -> Result<Option<u64>> { Ok(None) } }
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
    assert_eq!(prepare_with_statistics(Arc::clone(&relation), &mut Unknown).unwrap(), prepare(relation).unwrap());
}

#[test]
fn an_error_building_a_rewrite_fails_the_plan_rather_than_keeping_the_apply() {
    let count = typed(12, "count", int());
    let relation = scalar(aggregate(customer_orders(), vec![counted(count, AggregateFunction::CountStar, None, None)]), by_customer());
    assert_eq!(decorrelate::decorrelate(relation, &mut Ids(u32::MAX)).unwrap_err(), exhausted());
}
