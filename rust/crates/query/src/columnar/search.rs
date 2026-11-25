use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, cast::AsArray,
    builder::{BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder},
    types::{Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type},
};
use arrow_schema::{DataType as Type, Field};
use rtdb_columnar::{TextCell, from_field, text_cell};
use rtdb_expr::Parameters;
use rtdb_plan::{Expr, Schema};
use rtdb_text::{Query, Scorer, stored_length};
use rtdb_types::{Anomaly, LogicalType, Result, Value, text_pair_fingerprint};

use super::{Column, ColumnBatch};
use crate::QueryContext;

#[derive(Clone, Copy)]
enum Cell<'a> {
    Null,
    Literal(&'a Value),
    Arrow(&'a dyn Array, &'a Field, Option<&'a LogicalType>, usize),
}

impl<'a> Cell<'a> {
    fn selected(self) -> Result<Self> {
        let Self::Arrow(array, field, logical, row) = self else { return Ok(self); };
        Ok(match rtdb_columnar::selected_cell(field, array, row)? {
            None => Self::Null,
            Some((field_selected, array, row)) => {
                let logical = std::ptr::eq(field, field_selected).then_some(logical).flatten();
                Self::Arrow(array, field_selected, logical, row)
            }
        })
    }

    fn text(self, types: &mut TextTypes<'a>) -> Result<TextCell<'a>> {
        Ok(match self {
            Self::Null | Self::Literal(Value::Null | Value::Undefined) => TextCell::Missing,
            Self::Literal(Value::Utf8(text)) => TextCell::Utf8(text),
            Self::Literal(_) => TextCell::Other,
            Self::Arrow(array, field, logical, row) => {
                let logical = match logical {
                    Some(logical) => logical,
                    None => types.infer(field)?,
                };
                text_cell(logical, field, array, row)?
            }
        })
    }

    fn str(self, types: &mut TextTypes<'a>) -> Result<Option<&'a str>> {
        Ok(match self.text(types)? {
            TextCell::Utf8(text) => Some(text),
            TextCell::Missing | TextCell::Other => None,
        })
    }

    fn null(self) -> Result<bool> {
        Ok(matches!(self.selected()?, Self::Null | Self::Literal(Value::Null | Value::Undefined)))
    }

    fn list(self) -> Result<Option<List<'a>>> {
        Ok(match self.selected()? {
            Self::Literal(Value::List(values)) => match values.view() {
                rtdb_types::ListView::Values(values) => Some(List::Literal(values)),
                _ => None,
            },
            Self::Arrow(array, field, _, row) if field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).is_none() => match field.data_type() {
                Type::List(child) => {
                    let list = array.as_list::<i32>();
                    let offsets = list.value_offsets();
                    Some(List::Arrow(list.values().as_ref(), child, offsets[row] as usize, (offsets[row + 1] - offsets[row]) as usize))
                }
                Type::FixedSizeList(child, _) => {
                    let list = array.as_fixed_size_list();
                    Some(List::Arrow(list.values().as_ref(), child, list.value_offset(row) as usize, list.value_length() as usize))
                }
                _ => None,
            },
            _ => None,
        })
    }

    fn count(self) -> Result<Option<u64>> {
        Ok(match self.selected()? {
            Self::Literal(value) => rtdb_expr::integer(value).and_then(|count| u64::try_from(count).ok()),
            Self::Arrow(array, field, _, row) if field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).is_none() => {
                let count = match field.data_type() {
                    Type::Int8 => i64::from(array.as_primitive::<Int8Type>().value(row)),
                    Type::Int16 => i64::from(array.as_primitive::<Int16Type>().value(row)),
                    Type::Int32 => i64::from(array.as_primitive::<Int32Type>().value(row)),
                    Type::Int64 => array.as_primitive::<Int64Type>().value(row),
                    Type::UInt8 => i64::from(array.as_primitive::<UInt8Type>().value(row) as i8),
                    Type::UInt16 => i64::from(array.as_primitive::<UInt16Type>().value(row) as i16),
                    Type::UInt32 => i64::from(array.as_primitive::<UInt32Type>().value(row) as i32),
                    Type::UInt64 => array.as_primitive::<UInt64Type>().value(row) as i64,
                    _ => return Ok(None),
                };
                u64::try_from(count).ok()
            }
            _ => None,
        })
    }
}

enum List<'a> {
    Literal(&'a [Value]),
    Arrow(&'a dyn Array, &'a Field, usize, usize),
}

impl<'a> List<'a> {
    fn len(&self) -> usize {
        match self { Self::Literal(values) => values.len(), Self::Arrow(_, _, _, len) => *len }
    }

    fn cell(&self, index: usize) -> Cell<'a> {
        match self {
            Self::Literal(values) => Cell::Literal(&values[index]),
            Self::Arrow(array, field, offset, _) => Cell::Arrow(*array, field, None, *offset + index),
        }
    }
}

enum Input<'a> {
    Constant(Cell<'a>),
    Column(&'a Column),
}

impl<'a> Input<'a> {
    fn row(&self, row: usize) -> Cell<'a> {
        match self {
            Self::Constant(cell) => *cell,
            Self::Column(Column::Values(values)) => Cell::Literal(&values[row]),
            Self::Column(column) => match column.arrow_cell(row).flatten() {
                Some((array, field, logical, index)) => Cell::Arrow(array, field, logical, index),
                None => Cell::Null,
            },
        }
    }
}

#[derive(Default)]
pub(crate) struct TextTypes<'a>(HashMap<usize, (&'a Field, LogicalType)>);

impl<'a> TextTypes<'a> {
    fn infer(&mut self, field: &'a Field) -> Result<&LogicalType> {
        use std::collections::hash_map::Entry;
        let entry = match self.0.entry(std::ptr::from_ref(field) as usize) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert((field, from_field(field)?)),
        };
        Ok(&entry.1)
    }

    pub(crate) fn text(&mut self, column: &'a Column, row: usize) -> Result<TextCell<'a>> {
        Input::Column(column).row(row).text(self)
    }
}

#[derive(Clone, Copy)]
enum Function {
    Match,
    Score,
    Length,
    TokenCount,
    TermMatches,
    Terms,
    Fingerprint,
}

impl Function {
    /// The function of a call and its arity without the analyzer argument.
    fn of(name: &str, arity: usize) -> Option<Self> {
        let arity = rtdb_plan::analyzer_index(name, arity).unwrap_or(arity);
        Some(match (name, arity) {
            ("match", 2) => Self::Match,
            ("search_score", 2 | 5) => Self::Score,
            ("search_length", 1) => Self::Length,
            ("search_token_count", 1) => Self::TokenCount,
            ("search_term_matches", 2) => Self::TermMatches,
            ("search_terms", 1) => Self::Terms,
            ("search_corpus_fingerprint", 2) => Self::Fingerprint,
            _ => return None,
        })
    }
}

pub(super) fn shaped(expr: &Expr) -> bool {
    matches!(expr, Expr::Call { name, args, .. } if Function::of(name, args.len()).is_some() && args.iter().all(operand))
}

fn operand(expr: &Expr) -> bool {
    match expr {
        Expr::Column { .. } | Expr::Parameter { .. } | Expr::Literal(_) => true,
        Expr::List(values) => values.iter().all(operand),
        other => super::projection::shaped(other),
    }
}

fn invariant_operand(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter { .. } => true,
        Expr::List(values) => values.iter().all(invariant_operand),
        _ => false,
    }
}

fn input<'a>(expr: &'a Expr, schema: &Schema, batch: &'a ColumnBatch, params: Parameters<'a>) -> Result<Option<Input<'a>>> {
    Ok(Some(match expr {
        Expr::Column { id, .. } => {
            let Some(position) = schema.position(*id) else { return Ok(None); };
            Input::Column(&batch.columns[position])
        }
        Expr::Literal(value) => Input::Constant(Cell::Literal(value)),
        Expr::Parameter { index, .. } => {
            if let Some(cell) = params.arrow_cell(*index) {
                Input::Constant(Cell::Arrow(cell.array, cell.field, None, cell.row))
            } else if let Some(value) = params.get(*index)? {
                Input::Constant(Cell::Literal(value))
            } else { return Ok(None); }
        }
        _ => return Ok(None),
    }))
}

fn stats(query: &Query, documents: Cell<'_>, total: Cell<'_>, frequencies: Cell<'_>) -> Result<(u64, u64, Vec<u64>)> {
    let invalid = || Anomaly::incorrect("rtdb.expr/invalid-search-corpus",
        "Search statistics require nonnegative counts and one document frequency per search term");
    let documents = documents.count()?.ok_or_else(invalid)?;
    let tokens = total.count()?.ok_or_else(invalid)?;
    let frequencies = frequencies.list()?.filter(|list| list.len() == query.terms().len()).ok_or_else(invalid)?;
    let frequencies = (0..query.terms().len()).map(|index| {
        let count = frequencies.cell(index).count()?.filter(|count| *count <= documents && *count <= tokens).ok_or_else(invalid)?;
        Ok(count)
    }).collect::<Result<_>>()?;
    Ok((documents, tokens, frequencies))
}

fn fingerprint<'a>(left: Cell<'a>, right: Cell<'a>, types: &mut TextTypes<'a>) -> Result<Option<[u8; 32]>> {
    let (Some(ids), Some(bodies)) = (left.list()?, right.list()?) else { return Ok(None); };
    let invalid = || Anomaly::incorrect("rtdb.expr/invalid-search-documents",
        "A search fingerprint requires equally sized arrays of text ids and bodies");
    if ids.len() != bodies.len() { return Err(invalid()); }
    text_pair_fingerprint((0..ids.len()).map(|index| {
        Ok((ids.cell(index).str(types)?.ok_or_else(invalid)?, bodies.cell(index).str(types)?.ok_or_else(invalid)?))
    })).map(Some)
}

fn output_column(name: &str, array: ArrayRef) -> Result<Column> {
    let field = Arc::new(Field::new(name, array.data_type().clone(), true));
    let logical = Some(from_field(&field)?);
    Ok(Column::Arrow { array, field, logical })
}

pub(super) fn column(expr: &Expr, schema: &Schema, batch: &ColumnBatch, params: Parameters<'_>, context: &QueryContext) -> Result<Option<Column>> {
    Ok(funded_column(expr, schema, batch, params, context)?.map(|(column, _)| column))
}

pub(super) fn funded_column(expr: &Expr, schema: &Schema, batch: &ColumnBatch, params: Parameters<'_>, context: &QueryContext)
    -> Result<Option<(Column, Arc<rtdb_columnar::MemoryReservation>)>> {
    let Expr::Call { name, args, .. } = expr else { return Ok(None) };
    let Some(function) = Function::of(name, args.len()) else { return Ok(None) };
    let analyzer = rtdb_plan::call_analyzer(name, args)?;
    let args = &args[..rtdb_plan::analyzer_index(name, args.len()).unwrap_or(args.len())];
    let mut retained = context.memory.reserve(4096usize.saturating_add(batch.len.saturating_mul(16)))?;
    let mut computed = Vec::with_capacity(args.len());
    for arg in args {
        if matches!(arg, Expr::Column { .. } | Expr::Parameter { .. } | Expr::Literal(_)) {
            computed.push(None);
            continue;
        }
        let value = if shaped(arg) {
            column(arg, schema, batch, params, context)?
        } else {
            super::projection::column(arg, schema, batch, params)?
                .or_else(|| crate::vector::column(arg, schema, batch, params))
        };
        let Some(value) = value else { return Ok(None); };
        let lease = Arc::get_mut(&mut retained).expect("Search projection owns its argument allocation");
        lease.grow_to(lease.bytes().saturating_add(value.retained_bytes()?))?;
        computed.push(Some(value));
    }
    let Some(inputs) = args.iter().zip(&computed).map(|(arg, computed)| {
        if let Some(column) = computed { Ok(Some(Input::Column(column))) } else { input(arg, schema, batch, params) }
    }).collect::<Result<Vec<_>>>()?.into_iter().collect::<Option<Vec<_>>>() else { return Ok(None); };
    let mut boolean = BooleanBuilder::with_capacity(batch.len);
    let mut real = Float64Builder::new();
    let mut length = Int64Builder::new();
    let mut binary = BinaryBuilder::new();
    let mut terms = ListBuilder::new(StringBuilder::new());
    let mut term_matches = ListBuilder::new(BooleanBuilder::new());
    let mut types = TextTypes::default();
    let mut parsed = None;
    let mut prepared = None;
    let constant_score = args.iter().skip(1).all(invariant_operand);
    for row in 0..batch.len {
        context.check_cancelled_at(row)?;
        let cell = |index: usize| inputs[index].row(row);
        let null = inputs.iter().try_fold(false, |null, input| Ok::<_, Anomaly>(null || input.row(row).null()?))?;
        let first = if null { None } else { cell(0).str(&mut types)? };
        match function {
            Function::Match | Function::Score | Function::TermMatches => {
                let query = if null { None } else { cell(1).str(&mut types)? };
                let outcome = query.map(|query| {
                    let _scratch = context.memory.reserve(256usize.saturating_add(query.len().saturating_mul(32))
                        .saturating_add(first.map_or(0, |text| text.len().saturating_mul(32))))?;
                    let varying;
                    let query = if matches!(inputs[1], Input::Constant(_)) {
                        parsed.get_or_insert_with(|| Query::analyzed(analyzer, query))
                    } else {
                        varying = Query::analyzed(analyzer, query);
                        &varying
                    };
                    Ok::<_, Anomaly>(match function {
                        Function::Match => (first.is_some_and(|text| query.matches_text(text)), 0.0),
                        Function::TermMatches => {
                            if let Some(text) = first {
                                let owner = Arc::get_mut(&mut retained).expect("Search projection owns its allocation");
                                owner.grow_to(owner.bytes().saturating_add(query.terms().len()).saturating_add(8))?;
                                let (_, frequencies, _) = query.text_terms(text, false);
                                for count in frequencies { term_matches.values().append_value(count != 0); }
                            }
                            (false, 0.0)
                        }
                        Function::Score => {
                            let build = || -> Result<Scorer> {
                                Ok(if inputs.len() == 5 {
                                    let (documents, tokens, frequencies) = stats(query, cell(2), cell(3), cell(4))?;
                                    Scorer::from_frequencies(query.clone(), documents, tokens, &frequencies)
                                } else {
                                    Scorer::new(query.clone(), None)
                                })
                            };
                            let varying;
                            let scorer = if constant_score {
                                if prepared.is_none() { prepared = Some(build()?); }
                                prepared.as_ref().unwrap()
                            } else {
                                varying = build()?;
                                &varying
                            };
                            (false, scorer.bm25_text(first.unwrap_or_default()))
                        }
                        _ => unreachable!(),
                    })
                }).transpose()?;
                match function {
                    Function::Match => boolean.append_option(outcome.map(|value| value.0)),
                    Function::Score => real.append_option(outcome.map(|value| value.1)),
                    Function::TermMatches => term_matches.append(outcome.is_some() && first.is_some()),
                    _ => unreachable!(),
                }
            }
            Function::Length | Function::TokenCount => {
                let count = first.map(|text| {
                    let _scratch = context.memory.reserve(256usize.saturating_add(text.len().saturating_mul(32)))?;
                    let count = analyzer.count_tokens(text).len();
                    Ok::<_, Anomaly>(if matches!(function, Function::Length) { i64::from(stored_length(count)) } else { count as i64 })
                }).transpose()?;
                length.append_option(count);
            }
            Function::Terms => {
                if let Some(text) = first {
                    let owner = Arc::get_mut(&mut retained).expect("Search projection owns its allocation");
                    owner.grow_to(owner.bytes().saturating_add(text.len().saturating_mul(16)))?;
                    let varying;
                    let query = if matches!(inputs[0], Input::Constant(_)) {
                        parsed.get_or_insert_with(|| Query::analyzed(analyzer, text))
                    } else {
                        varying = Query::analyzed(analyzer, text);
                        &varying
                    };
                    for term in query.terms() { terms.values().append_value(term); }
                }
                terms.append(first.is_some());
            }
            Function::Fingerprint => {
                let digest = if null { None } else { fingerprint(cell(0), cell(1), &mut types)? };
                binary.append_option(digest);
            }
        }
    }
    let array: ArrayRef = match function {
        Function::Match => Arc::new(boolean.finish()),
        Function::Score => Arc::new(real.finish()),
        Function::Length | Function::TokenCount => Arc::new(length.finish()),
        Function::TermMatches => Arc::new(term_matches.finish()),
        Function::Terms => Arc::new(terms.finish()),
        Function::Fingerprint => Arc::new(binary.finish()),
    };
    Ok(Some((output_column(name, array)?, retained)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{Instant, ScalarType, Timestamp};
    use tokio_util::sync::CancellationToken;

    #[test]
    fn arrow_search_scoring_borrows_arrow_text_and_produces_a_nullable_arrow_score() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 24);
        let storage = Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20));
        let catalog = crate::CatalogSnapshot::new("xtdb", std::collections::BTreeMap::from([
            ("xtdb".into(), crate::DatabaseSnapshot { partitions: Vec::new(), storage }),
        ]), false).unwrap();
        let context = QueryContext::new(catalog, Arc::clone(&memory), Instant::from(Timestamp(0)), "UTC",
            crate::QueryOptions::default(), &CancellationToken::new()).unwrap();
        let logical = LogicalType::Scalar(ScalarType::Utf8).maybe();
        let field = rtdb_plan::Field {
            id: rtdb_plan::ColumnId(0), name: "text".into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: logical.clone(),
        };
        let schema = Schema::new(vec![field.clone()]).unwrap();
        let batch = ColumnBatch::settled(vec![Column::Arrow {
            array: Arc::new(arrow_array::StringArray::from(vec![None, Some("invoice paid"), Some("invoice invoice")])),
            field: Arc::new(Field::new("text", Type::Utf8, true)), logical: None,
        }], 3, memory.reserve(2048).unwrap()).unwrap();
        let mut frequencies = ListBuilder::new(Int64Builder::new());
        frequencies.values().append_value(4);
        frequencies.append(true);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(arrow_array::StringArray::from(vec!["invoice"])),
            Arc::new(arrow_array::Int64Array::from(vec![7])),
            Arc::new(arrow_array::Int64Array::from(vec![22])),
            Arc::new(frequencies.finish()),
        ];
        let fields: Vec<_> = arrays.iter().enumerate().map(|(index, array)| Field::new(index.to_string(), array.data_type().clone(), false)).collect();
        let arguments = fields.iter().map(from_field).collect::<Result<Vec<_>>>().unwrap();
        let record = arrow_array::RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays).unwrap();
        let input = rtdb_columnar::OwnedBatch::new(record, &memory).unwrap();
        let bound = crate::BoundParameters::from_arrow(input, 0, arguments.clone(), &memory).unwrap();
        let params = bound.evaluator();
        let mut types = TextTypes::default();
        assert!(matches!(types.text(&batch.columns[0], 1).unwrap(), TextCell::Utf8("invoice paid")));
        assert!(matches!(types.text(&batch.columns[0], 2).unwrap(), TextCell::Utf8("invoice invoice")));
        assert_eq!(types.0.len(), 1);
        let score = Expr::Call {
            name: "search_score".into(), args: std::iter::once(field.expression())
                .chain(arguments.into_iter().enumerate().map(|(index, data_type)| Expr::Parameter { index, data_type })).collect(),
            return_type: LogicalType::Scalar(ScalarType::F64).maybe(),
        };
        let Some(Column::Arrow { array, .. }) = column(&score, &schema, &batch, params, &context).unwrap() else {
            panic!("Search score was not an Arrow column");
        };
        let output = array.as_any().downcast_ref::<arrow_array::Float64Array>().unwrap();
        let scorer = Scorer::from_frequencies(Query::parse("invoice"), 7, 22, &[4]);
        assert!(output.is_null(0));
        assert!((0..4).all(|index| params.known(index).is_none()));
        for (row, text) in ["invoice paid", "invoice invoice"].into_iter().enumerate() {
            assert_eq!(output.value(row + 1).to_bits(), scorer.bm25_arrow(&rtdb_text::Analyzer::Simple.analyze(text)).to_bits());
        }
    }
}
