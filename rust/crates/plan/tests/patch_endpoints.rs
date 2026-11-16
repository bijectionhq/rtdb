use std::sync::Arc;
use rtdb_plan::{CastTarget,ColumnId,Expr,Field,Relation,RelationNode,Schema};
use rtdb_types::{Keyword,LogicalType as L,ScalarType as S,TimeUnit as U,Value as V};

fn time(value: i64) -> Expr { Expr::Literal(V::Timestamp { value,unit: U::Microsecond,timezone: Some("UTC".into()) }) }
fn relation(from: Expr,to: Expr) -> rtdb_types::Result<Relation> {
    let mut fields = [L::Scalar(S::FixedSizeBinary(16)),time(0).data_type(),time(0).data_type().maybe(),L::Scalar(S::I64)].into_iter().enumerate().map(|(index,data_type)|Field { id: ColumnId(index as u32),name: format!("c{index}"),qualifier: None,qualified_only: false,unnamed: false,data_type }).collect::<Vec<_>>();
    let input = Arc::new(Relation::empty(Schema::new(fields.clone()).unwrap()));
    fields[3].data_type = fields[3].data_type.clone().maybe();
    Relation::new(Schema::new(fields).unwrap(),RelationNode::PatchGaps { input,iid: ColumnId(0),valid_from: ColumnId(1),valid_to: ColumnId(2),document: ColumnId(3),from,to })
}

#[test]
fn patch_relation_accepts_only_endpoint_forms_including_untyped_parameters() {
    let clock = |name: &str,args|Expr::Call { name: name.into(),args,return_type: time(0).data_type() };
    for endpoint in [Expr::Literal(V::Null),Expr::Literal(V::Keyword(Keyword::from_name("now"))),time(0),Expr::Literal(V::DateDay(0)),Expr::Literal(V::DateMilli(0)),Expr::Parameter { index: 0,data_type: L::Scalar(S::Utf8) },clock("current_timestamp",vec![]),clock("current-timestamp",vec![])] {
        relation(endpoint.clone(),endpoint).unwrap();
    }
    for endpoint in [Expr::Literal(V::Undefined),Expr::Literal(V::Bool(false)),Expr::Literal(V::Utf8("2020-01-01Z".into())),Expr::Literal(V::I64(0)),Expr::Literal(V::Time { value: 0,unit: U::Microsecond,bit_width: 64 }),Expr::Cast { expr: Box::new(time(0)),target: CastTarget::fixed(time(0).data_type()) },Expr::Coalesce(vec![time(0)]),clock("now",vec![]),clock("current_timestamp",vec![Expr::Literal(V::I64(6))])] {
        assert_eq!(relation(endpoint.clone(),time(1)).unwrap_err().code,"rtdb.plan/patch-endpoint");
        assert_eq!(relation(time(0),endpoint).unwrap_err().code,"rtdb.plan/patch-endpoint");
    }
}
